package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/ugorji/go/codec"
	"go.uber.org/goleak"
)

type mockBatch struct {
	appendErr error
	sendErr   error
	rows      [][]any
}

func (m *mockBatch) Abort() error { return nil }
func (m *mockBatch) Append(v ...any) error {
	m.rows = append(m.rows, append([]any(nil), v...))
	return m.appendErr
}
func (m *mockBatch) AppendStruct(any) error        { return nil }
func (m *mockBatch) Column(int) driver.BatchColumn { return nil }
func (m *mockBatch) Flush() error                  { return nil }
func (m *mockBatch) Send() error                   { return m.sendErr }
func (m *mockBatch) IsSent() bool                  { return false }
func (m *mockBatch) Rows() int                     { return len(m.rows) }
func (m *mockBatch) Columns() []column.Interface   { return nil }
func (m *mockBatch) Close() error                  { return nil }

type mockConn struct {
	mu           sync.Mutex
	pingErr      error
	prepareErr   error
	closeErr     error
	pingCalls    int
	closeCalls   int
	prepareCalls int
	batch        driver.Batch
	prepareHook  func(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

func (m *mockConn) Ping(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pingCalls++
	return m.pingErr
}

func (m *mockConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	m.mu.Lock()
	m.prepareCalls++
	hook := m.prepareHook
	err := m.prepareErr
	batch := m.batch
	m.mu.Unlock()

	if hook != nil {
		return hook(ctx, query, opts...)
	}
	if err != nil {
		return nil, err
	}
	return batch, nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	return m.closeErr
}

type blockingBatch struct {
	mockBatch
	appendStarted chan struct{}
	releaseAppend chan struct{}
	appendOnce    sync.Once
}

func (b *blockingBatch) Append(v ...any) error {
	if b.appendStarted != nil {
		b.appendOnce.Do(func() { close(b.appendStarted) })
	}
	if b.releaseAppend != nil {
		<-b.releaseAppend
	}
	return b.mockBatch.Append(v...)
}

type concurrentMockBatch struct {
	appendErr error
	sendErr   error
	onSend    func() error

	mu   sync.Mutex
	rows [][]any
}

func (m *concurrentMockBatch) Abort() error { return nil }
func (m *concurrentMockBatch) Append(v ...any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = append(m.rows, append([]any(nil), v...))
	return m.appendErr
}
func (m *concurrentMockBatch) AppendStruct(any) error        { return nil }
func (m *concurrentMockBatch) Column(int) driver.BatchColumn { return nil }
func (m *concurrentMockBatch) Flush() error                  { return nil }
func (m *concurrentMockBatch) Send() error {
	if m.onSend != nil {
		if err := m.onSend(); err != nil {
			return err
		}
	}
	return m.sendErr
}
func (m *concurrentMockBatch) IsSent() bool                { return false }
func (m *concurrentMockBatch) Columns() []column.Interface { return nil }
func (m *concurrentMockBatch) Close() error                { return nil }
func (m *concurrentMockBatch) Rows() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rows)
}
func (m *concurrentMockBatch) snapshotRows() [][]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := make([][]any, len(m.rows))
	for i := range m.rows {
		rows[i] = append([]any(nil), m.rows[i]...)
	}
	return rows
}

type sendBarrier struct {
	target  int
	mu      sync.Mutex
	arrived int
	maxSeen int
	release chan struct{}
	done    bool
}

func newSendBarrier(target int) *sendBarrier {
	return &sendBarrier{
		target:  target,
		release: make(chan struct{}),
	}
}

func (b *sendBarrier) arrive() error {
	b.mu.Lock()
	b.arrived++
	if b.arrived > b.maxSeen {
		b.maxSeen = b.arrived
	}
	if b.arrived == b.target && !b.done {
		close(b.release)
		b.done = true
	}
	release := b.release
	b.mu.Unlock()

	select {
	case <-release:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("send barrier timeout")
	}
}

func (b *sendBarrier) max() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.maxSeen
}

type concurrentMockConn struct {
	batchFactory func() driver.Batch
	closeErr     error
	prepareErr   error

	mu           sync.Mutex
	closed       bool
	closeCalls   int
	prepareCalls int
	batches      []*concurrentMockBatch
}

func (m *concurrentMockConn) Ping(context.Context) error { return nil }

func (m *concurrentMockConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prepareCalls++
	if m.prepareErr != nil {
		return nil, m.prepareErr
	}
	if m.closed {
		return nil, errors.New("prepare on closed connection")
	}

	batch := m.batchFactory()
	if tracked, ok := batch.(*concurrentMockBatch); ok {
		m.batches = append(m.batches, tracked)
	}
	return batch, nil
}

func (m *concurrentMockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	m.closed = true
	return m.closeErr
}

func (m *concurrentMockConn) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *concurrentMockConn) snapshotBatches() []*concurrentMockBatch {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*concurrentMockBatch(nil), m.batches...)
}

func (m *concurrentMockConn) snapshotCloseCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCalls
}

func (m *concurrentMockConn) snapshotPrepareCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.prepareCalls
}

type blockingSendBatch struct {
	*concurrentMockBatch
	conn           *concurrentMockConn
	sendStarted    chan struct{}
	releaseSend    chan struct{}
	usedAfterClose atomic.Bool
}

func (b *blockingSendBatch) Send() error {
	select {
	case <-b.sendStarted:
	default:
		close(b.sendStarted)
	}

	<-b.releaseSend
	if b.conn.isClosed() {
		b.usedAfterClose.Store(true)
		return errors.New("send after close")
	}
	return b.concurrentMockBatch.Send()
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

type permanentNetError struct{}

func (permanentNetError) Error() string   { return "permanent network error" }
func (permanentNetError) Timeout() bool   { return false }
func (permanentNetError) Temporary() bool { return false }

func TestFLBPluginFlush_ReturnsErrorForNonCtxCalls(t *testing.T) {
	if got := FLBPluginFlush(nil, 0, nil); got != output.FLB_ERROR {
		t.Fatalf("FLBPluginFlush() = %d, want %d", got, output.FLB_ERROR)
	}
}

func TestPluginFromContextValue_RejectsNilAndWrongType(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "nil", value: nil},
		{name: "wrong type", value: "not-a-plugin"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if p, err := pluginFromContextValue(tt.value); err == nil || p != nil {
				t.Fatalf("pluginFromContextValue(%v) = (%v, %v), want error", tt.value, p, err)
			}
		})
	}
}

func TestClassifyInsertError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: output.FLB_RETRY},
		{name: "network timeout", err: timeoutError{}, want: output.FLB_RETRY},
		{name: "network reset", err: syscall.ECONNRESET, want: output.FLB_ERROR},
		{name: "network pipe", err: syscall.EPIPE, want: output.FLB_ERROR},
		{name: "permanent network error", err: permanentNetError{}, want: output.FLB_ERROR},
		{name: "retryable clickhouse code 203", err: &clickhouse.Exception{Code: 203}, want: output.FLB_RETRY},
		{name: "retryable clickhouse code 210", err: &clickhouse.Exception{Code: 210}, want: output.FLB_ERROR},
		{name: "retryable clickhouse code 236", err: &clickhouse.Exception{Code: 236}, want: output.FLB_ERROR},
		{name: "retryable clickhouse code 241", err: &clickhouse.Exception{Code: 241}, want: output.FLB_RETRY},
		{name: "retryable clickhouse code 242", err: &clickhouse.Exception{Code: 242}, want: output.FLB_RETRY},
		{name: "retryable clickhouse code 999", err: &clickhouse.Exception{Code: 999}, want: output.FLB_RETRY},
		{name: "non-retryable clickhouse code 439", err: &clickhouse.Exception{Code: 439}, want: output.FLB_ERROR},
		{name: "non-retryable clickhouse code 1000", err: &clickhouse.Exception{Code: 1000}, want: output.FLB_ERROR},
		{name: "schema clickhouse code", err: &clickhouse.Exception{Code: 62}, want: output.FLB_ERROR},
		{name: "plain error", err: errors.New("boom"), want: output.FLB_ERROR},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyInsertError(tt.err); got != tt.want {
				t.Fatalf("classifyInsertError(%v) = %d, want %d", tt.err, got, tt.want)
			}
		})
	}
}

func TestLogLevelLogger_RedactsPassword(t *testing.T) {
	plugin := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			Auth: clickhouse.Auth{Password: "super-secret"},
		},
	}
	cfg := map[string]string{"LogLevel": "debug"}
	if err := plugin.parseLogLevel(makeConfig(cfg)); err != nil {
		t.Fatalf("parseLogLevel() unexpected error: %v", err)
	}

	var buf bytes.Buffer
	oldOut := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	})

	// Test via Logger
	logger := plugin.Opt.Logger
	if logger == nil {
		t.Fatal("Logger is nil")
	}
	logger.Info("dsn info", "password", "super-secret", "token", "abc")
	got := buf.String()
	if strings.Contains(got, "super-secret") {
		t.Fatalf("log leaked password: %s", got)
	}
	// Check password is redacted
	if !strings.Contains(got, "password ***") {
		t.Fatalf("log missing password redaction: %s", got)
	}
}

func TestCoerceColumnValue_UsesDeclaredParser(t *testing.T) {
	tests := []struct {
		name   string
		parser ColumnParser
		raw    any
		want   any
	}{
		{name: "int64 to int32", parser: parseInt[int32], raw: int64(42), want: int32(42)},
		{name: "bool to bool", parser: parseBool, raw: true, want: true},
		{name: "bytes to int64", parser: parseInt[int64], raw: []byte("7"), want: int64(7)},
		{name: "string passthrough", parser: parseString, raw: "hello", want: "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := coerceColumnValue(tt.parser, tt.raw)
			if err != nil {
				t.Fatalf("coerceColumnValue(%v) unexpected error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("coerceColumnValue(%v) = %v (%T), want %v (%T)", tt.raw, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestCoerceColumnValue_ArrayInt16FromDecodedArray(t *testing.T) {
	parser, _, err := parseColumnType([]string{"Array(Int16)"})
	if err != nil {
		t.Fatalf("parseColumnType(Array(Int16)) error: %v", err)
	}

	got, err := coerceColumnValue(parser, []any{int64(1), int64(2)})
	if err != nil {
		t.Fatalf("coerceColumnValue(array) unexpected error: %v", err)
	}

	arr, ok := got.([]int16)
	if !ok {
		t.Fatalf("coerceColumnValue(array) type = %T, want []int16", got)
	}
	if !reflect.DeepEqual(arr, []int16{1, 2}) {
		t.Fatalf("coerceColumnValue(array) = %v, want %v", arr, []int16{1, 2})
	}
}

func TestCoerceColumnValue_ArrayInt16RejectsNonJSONArrayString(t *testing.T) {
	parser, _, err := parseColumnType([]string{"Array(Int16)"})
	if err != nil {
		t.Fatalf("parseColumnType(Array(Int16)) error: %v", err)
	}

	if _, err := coerceColumnValue(parser, "1,2"); err == nil {
		t.Fatal("coerceColumnValue(non-json string) expected error, got nil")
	}
}

func TestCoerceColumnValue_ArrayStringAndFloat64FromDecodedArray(t *testing.T) {
	strParser, _, err := parseColumnType([]string{"Array(String)"})
	if err != nil {
		t.Fatalf("parseColumnType(Array(String)) error: %v", err)
	}
	floatParser, _, err := parseColumnType([]string{"Array(Float64)"})
	if err != nil {
		t.Fatalf("parseColumnType(Array(Float64)) error: %v", err)
	}

	strVal, err := coerceColumnValue(strParser, []any{"a", "b"})
	if err != nil {
		t.Fatalf("coerceColumnValue(Array(String)) unexpected error: %v", err)
	}
	if !reflect.DeepEqual(strVal, []string{"a", "b"}) {
		t.Fatalf("coerceColumnValue(Array(String)) = %v, want %v", strVal, []string{"a", "b"})
	}

	floatVal, err := coerceColumnValue(floatParser, []any{float64(1.5), float64(2.25)})
	if err != nil {
		t.Fatalf("coerceColumnValue(Array(Float64)) unexpected error: %v", err)
	}
	if !reflect.DeepEqual(floatVal, []float64{1.5, 2.25}) {
		t.Fatalf("coerceColumnValue(Array(Float64)) = %v, want %v", floatVal, []float64{1.5, 2.25})
	}
}

func TestCoerceColumnValue_UUIDAndIPv4Validation(t *testing.T) {
	uuidParser, _, err := parseColumnType([]string{"UUID"})
	if err != nil {
		t.Fatalf("parseColumnType(UUID) error: %v", err)
	}
	ipv4Parser, _, err := parseColumnType([]string{"IPv4"})
	if err != nil {
		t.Fatalf("parseColumnType(IPv4) error: %v", err)
	}

	if _, err := coerceColumnValue(uuidParser, "not-uuid"); err == nil {
		t.Fatal("coerceColumnValue(UUID invalid) expected error, got nil")
	}
	if _, err := coerceColumnValue(ipv4Parser, "2001:db8::1"); err == nil {
		t.Fatal("coerceColumnValue(IPv4 invalid) expected error, got nil")
	}
}

func TestCoerceColumnValue_FixedStringStrictLength(t *testing.T) {
	parser, _, err := parseColumnType([]string{"FixedString(3)"})
	if err != nil {
		t.Fatalf("parseColumnType(FixedString(3)) error: %v", err)
	}
	if _, err := coerceColumnValue(parser, "abcd"); err == nil {
		t.Fatal("coerceColumnValue(FixedString overflow) expected error, got nil")
	}
}

func TestCoerceColumnValue_JSONFromMap(t *testing.T) {
	parser, _, err := parseColumnType([]string{"JSON"})
	if err != nil {
		t.Fatalf("parseColumnType(JSON) error: %v", err)
	}

	tests := []struct {
		name      string
		raw       any
		assertion func(t *testing.T, decoded map[string]any)
	}{
		{
			name: "map interface key",
			raw: map[interface{}]interface{}{
				"a": int64(1),
				"b": []any{int64(2), int64(3)},
			},
			assertion: func(t *testing.T, decoded map[string]any) {
				t.Helper()
				if decoded["a"] != float64(1) {
					t.Fatalf("decoded[a] = %v, want 1", decoded["a"])
				}
				arr, ok := decoded["b"].([]any)
				if !ok || len(arr) != 2 {
					t.Fatalf("decoded[b] = %T %v, want []any len=2", decoded["b"], decoded["b"])
				}
			},
		},
		{
			name: "map string key",
			raw: map[string]any{
				"k": "v",
				"n": int64(7),
			},
			assertion: func(t *testing.T, decoded map[string]any) {
				t.Helper()
				if decoded["k"] != "v" {
					t.Fatalf("decoded[k] = %v, want v", decoded["k"])
				}
				if decoded["n"] != float64(7) {
					t.Fatalf("decoded[n] = %v, want 7", decoded["n"])
				}
			},
		},
		{
			name: "nested mixed",
			raw: map[interface{}]interface{}{
				1: map[interface{}]interface{}{
					"inner": []any{
						map[string]any{"x": "y"},
						int64(9),
					},
				},
			},
			assertion: func(t *testing.T, decoded map[string]any) {
				t.Helper()
				l1, ok := decoded["1"].(map[string]any)
				if !ok {
					t.Fatalf("decoded[1] = %T %v, want map[string]any", decoded["1"], decoded["1"])
				}
				inner, ok := l1["inner"].([]any)
				if !ok || len(inner) != 2 {
					t.Fatalf("decoded inner = %T %v, want []any len=2", l1["inner"], l1["inner"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := coerceColumnValue(parser, tt.raw)
			if err != nil {
				t.Fatalf("coerceColumnValue(json map) unexpected error: %v", err)
			}
			s, ok := got.(string)
			if !ok {
				t.Fatalf("coerceColumnValue(json map) type = %T, want string", got)
			}

			var decoded map[string]any
			if err := json.Unmarshal([]byte(s), &decoded); err != nil {
				t.Fatalf("coerceColumnValue(json map) invalid json output: %v, value=%q", err, s)
			}
			tt.assertion(t, decoded)
		})
	}
}

func TestBatchInsert_UsesDeclaredColumnParsersForDecodedTypedValues(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		Columns:   []string{"i32", "u64", "u32", "flag", "f32"},
		ColType:   []ColumnParser{parseInt[int32], parseUint[uint64], parseUint[uint32], parseBool, parseFloat[float32]},
		TableName: "events",
		BatchStmt: "INSERT INTO default.events(i32,u64,u32,flag,f32)",
		Conn:      conn,
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{
		"i32":  int64(42),
		"u64":  uint64(1) << 63,
		"u32":  uint32(7),
		"flag": true,
		"f32":  float64(3.5),
	})

	if ret, _, rec := output.GetRecord(dec); ret != 0 {
		t.Fatalf("output.GetRecord() = %d, want 0", ret)
	} else {
		assertTypedValue(t, rec["i32"], int64(42))
		assertTypedValue(t, rec["u64"], uint64(1<<63))
		assertTypedValue(t, rec["u32"], int64(7))
		assertTypedValue(t, rec["flag"], true)
		assertTypedValue(t, rec["f32"], float64(3.5))
	}

	dec = mustTestDecoder(t, uint64(1710000000), map[string]any{
		"i32":  int64(42),
		"u64":  uint64(1) << 63,
		"u32":  uint32(7),
		"flag": true,
		"f32":  float64(3.5),
	})

	got := p.BatchInsert("", dec)
	if got != output.FLB_OK {
		t.Fatalf("BatchInsert() = %d, want %d", got, output.FLB_OK)
	}
	if len(batch.rows) != 1 {
		t.Fatalf("batch rows = %d, want 1", len(batch.rows))
	}

	row := batch.rows[0]
	if len(row) != 5 {
		t.Fatalf("batch row len = %d, want 5", len(row))
	}

	assertTypedValue(t, row[0], int32(42))
	assertTypedValue(t, row[1], uint64(1<<63))
	assertTypedValue(t, row[2], uint32(7))
	assertTypedValue(t, row[3], true)
	assertTypedValue(t, row[4], float32(3.5))
}

func TestBatchInsert_ArrayInt16ColumnParsesDecodedSlice(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}

	arrayParser, _, err := parseColumnType([]string{"Array(Int16)"})
	if err != nil {
		t.Fatalf("parseColumnType(Array(Int16)) error: %v", err)
	}

	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		Columns:   []string{"category"},
		ColType:   []ColumnParser{arrayParser},
		Defaults:  []any{[]int16{}},
		TableName: "events",
		BatchStmt: "INSERT INTO default.events(category)",
		Conn:      conn,
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{
		"category": []any{int64(1), int64(2), int64(3)},
	})

	got := p.BatchInsert("", dec)
	if got != output.FLB_OK {
		t.Fatalf("BatchInsert() = %d, want %d", got, output.FLB_OK)
	}
	if len(batch.rows) != 1 {
		t.Fatalf("batch rows = %d, want 1", len(batch.rows))
	}
	row := batch.rows[0]
	if len(row) != 1 {
		t.Fatalf("batch row len = %d, want 1", len(row))
	}
	arr, ok := row[0].([]int16)
	if !ok {
		t.Fatalf("batch row[0] type = %T, want []int16", row[0])
	}
	if !reflect.DeepEqual(arr, []int16{1, 2, 3}) {
		t.Fatalf("batch row[0] = %v, want %v", arr, []int16{1, 2, 3})
	}
}

func TestBatchInsert_JSONColumnParsesDecodedMap(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}

	jsonParser, _, err := parseColumnType([]string{"JSON"})
	if err != nil {
		t.Fatalf("parseColumnType(JSON) error: %v", err)
	}

	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		Columns:   []string{"detail"},
		ColType:   []ColumnParser{jsonParser},
		Defaults:  []any{"{}"},
		TableName: "events",
		BatchStmt: "INSERT INTO default.events(detail)",
		Conn:      conn,
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{
		"detail": map[string]any{"k": "v"},
	})

	got := p.BatchInsert("", dec)
	if got != output.FLB_OK {
		t.Fatalf("BatchInsert() = %d, want %d", got, output.FLB_OK)
	}
	if len(batch.rows) != 1 {
		t.Fatalf("batch rows = %d, want 1", len(batch.rows))
	}
	row := batch.rows[0]
	if len(row) != 1 {
		t.Fatalf("batch row len = %d, want 1", len(row))
	}
	s, ok := row[0].(string)
	if !ok {
		t.Fatalf("batch row[0] type = %T, want string", row[0])
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(s), &decoded); err != nil {
		t.Fatalf("batch row[0] invalid json: %v, value=%q", err, s)
	}
	if decoded["k"] != "v" {
		t.Fatalf("decoded[k] = %v, want v", decoded["k"])
	}
}

func TestBatchInsert_JSONColumnParsesNestedMap(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}

	jsonParser, _, err := parseColumnType([]string{"JSON"})
	if err != nil {
		t.Fatalf("parseColumnType(JSON) error: %v", err)
	}

	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		Columns:   []string{"detail"},
		ColType:   []ColumnParser{jsonParser},
		Defaults:  []any{"{}"},
		TableName: "events",
		BatchStmt: "INSERT INTO default.events(detail)",
		Conn:      conn,
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{
		"detail": map[string]any{
			"meta": map[string]any{
				"labels": []any{"a", "b"},
			},
			"2": "num-key",
		},
	})

	got := p.BatchInsert("", dec)
	if got != output.FLB_OK {
		t.Fatalf("BatchInsert() = %d, want %d", got, output.FLB_OK)
	}
	if len(batch.rows) != 1 {
		t.Fatalf("batch rows = %d, want 1", len(batch.rows))
	}

	s, ok := batch.rows[0][0].(string)
	if !ok {
		t.Fatalf("batch row[0] type = %T, want string", batch.rows[0][0])
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(s), &decoded); err != nil {
		t.Fatalf("batch row[0] invalid json: %v, value=%q", err, s)
	}
	if decoded["2"] != "num-key" {
		t.Fatalf("decoded[2] = %v, want num-key", decoded["2"])
	}
	meta, ok := decoded["meta"].(map[string]any)
	if !ok {
		t.Fatalf("decoded[meta] = %T %v, want map[string]any", decoded["meta"], decoded["meta"])
	}
	labels, ok := meta["labels"].([]any)
	if !ok || len(labels) != 2 {
		t.Fatalf("decoded meta.labels = %T %v, want []any len=2", meta["labels"], meta["labels"])
	}
}

func TestBatchInsert_UUIDIPv4AndArrays(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}

	uuidParser, _, err := parseColumnType([]string{"UUID"})
	if err != nil {
		t.Fatalf("parseColumnType(UUID) error: %v", err)
	}
	ipv4Parser, _, err := parseColumnType([]string{"IPv4"})
	if err != nil {
		t.Fatalf("parseColumnType(IPv4) error: %v", err)
	}
	arrStrParser, _, err := parseColumnType([]string{"Array(String)"})
	if err != nil {
		t.Fatalf("parseColumnType(Array(String)) error: %v", err)
	}
	arrFloatParser, _, err := parseColumnType([]string{"Array(Float64)"})
	if err != nil {
		t.Fatalf("parseColumnType(Array(Float64)) error: %v", err)
	}

	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		Columns:   []string{"trace_id", "client_ip", "tags", "scores"},
		ColType:   []ColumnParser{uuidParser, ipv4Parser, arrStrParser, arrFloatParser},
		Defaults:  []any{"00000000-0000-0000-0000-000000000000", "0.0.0.0", []string{}, []float64{}},
		TableName: "events",
		BatchStmt: "INSERT INTO default.events(trace_id,client_ip,tags,scores)",
		Conn:      conn,
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{
		"trace_id":  "550e8400-e29b-41d4-a716-446655440000",
		"client_ip": "10.1.2.3",
		"tags":      []any{"a", "b"},
		"scores":    []any{float64(1.2), float64(3.4)},
	})

	got := p.BatchInsert("", dec)
	if got != output.FLB_OK {
		t.Fatalf("BatchInsert() = %d, want %d", got, output.FLB_OK)
	}
	if len(batch.rows) != 1 {
		t.Fatalf("batch rows = %d, want 1", len(batch.rows))
	}
	row := batch.rows[0]
	if len(row) != 4 {
		t.Fatalf("batch row len = %d, want 4", len(row))
	}
	assertTypedValue(t, row[0], "550e8400-e29b-41d4-a716-446655440000")
	assertTypedValue(t, row[1], "10.1.2.3")
	if !reflect.DeepEqual(row[2], []string{"a", "b"}) {
		t.Fatalf("row[2] = %v, want %v", row[2], []string{"a", "b"})
	}
	if !reflect.DeepEqual(row[3], []float64{1.2, 3.4}) {
		t.Fatalf("row[3] = %v, want %v", row[3], []float64{1.2, 3.4})
	}
}

func TestBatchInsert_NullableColumn_MissingAndExplicitNil(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}

	nullableParser, nullableDef, err := parseColumnType([]string{"Nullable(String)"})
	if err != nil {
		t.Fatalf("parseColumnType(Nullable(String)) error: %v", err)
	}

	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		Columns:     []string{"nick"},
		ColType:     []ColumnParser{nullableParser},
		Defaults:    []any{nullableDef},
		ColNullable: []bool{true},
		TableName:   "events",
		BatchStmt:   "INSERT INTO default.events(nick)",
		Conn:        conn,
	}

	decMissing := mustTestDecoder(t, uint64(1710000000), map[string]any{})
	if got := p.BatchInsert("", decMissing); got != output.FLB_OK {
		t.Fatalf("BatchInsert(missing) = %d, want %d", got, output.FLB_OK)
	}

	decNil := mustTestDecoder(t, uint64(1710000001), map[string]any{"nick": nil})
	if got := p.BatchInsert("", decNil); got != output.FLB_OK {
		t.Fatalf("BatchInsert(explicit nil) = %d, want %d", got, output.FLB_OK)
	}

	if len(batch.rows) != 2 {
		t.Fatalf("batch rows = %d, want 2", len(batch.rows))
	}
	if batch.rows[0][0] != nil {
		t.Fatalf("row[0][0] = %v (%T), want nil", batch.rows[0][0], batch.rows[0][0])
	}
	if batch.rows[1][0] != nil {
		t.Fatalf("row[1][0] = %v (%T), want nil", batch.rows[1][0], batch.rows[1][0])
	}
}

func TestBatchInsert_NonNullableColumn_ExplicitNilSkipsRecord(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}

	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		Columns:     []string{"score"},
		ColType:     []ColumnParser{parseInt[int64]},
		Defaults:    []any{int64(0)},
		ColNullable: []bool{false},
		TableName:   "events",
		BatchStmt:   "INSERT INTO default.events(score)",
		Conn:        conn,
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{"score": nil})
	if got := p.BatchInsert("", dec); got != output.FLB_OK {
		t.Fatalf("BatchInsert(non-nullable nil) = %d, want %d (skip bad record)", got, output.FLB_OK)
	}
	if len(batch.rows) != 0 {
		t.Fatalf("expected 0 rows (bad record skipped), got %d", len(batch.rows))
	}
}

func TestBatchInsert_LowCardinalityStringBehavesLikeString(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}

	lowCardParser, lowCardDef, err := parseColumnType([]string{"LowCardinality(String)"})
	if err != nil {
		t.Fatalf("parseColumnType(LowCardinality(String)) error: %v", err)
	}

	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		Columns:     []string{"kind"},
		ColType:     []ColumnParser{lowCardParser},
		Defaults:    []any{lowCardDef},
		ColNullable: []bool{false},
		TableName:   "events",
		BatchStmt:   "INSERT INTO default.events(kind)",
		Conn:        conn,
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{"kind": "access"})
	if got := p.BatchInsert("", dec); got != output.FLB_OK {
		t.Fatalf("BatchInsert(lowcardinality string) = %d, want %d", got, output.FLB_OK)
	}
	if len(batch.rows) != 1 {
		t.Fatalf("batch rows = %d, want 1", len(batch.rows))
	}
	assertTypedValue(t, batch.rows[0][0], "access")
}

func TestBatchInsert_NullableLowCardinalityStringMixedBatch(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}

	nullableLowCardParser, nullableLowCardDef, err := parseColumnType([]string{"Nullable(LowCardinality(String))"})
	if err != nil {
		t.Fatalf("parseColumnType(Nullable(LowCardinality(String))) error: %v", err)
	}

	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		Columns:     []string{"name"},
		ColType:     []ColumnParser{nullableLowCardParser},
		Defaults:    []any{nullableLowCardDef},
		ColNullable: []bool{true},
		TableName:   "events",
		BatchStmt:   "INSERT INTO default.events(name)",
		Conn:        conn,
	}

	dec := mustTestDecoderRows(t, []testRecordRow{
		{ts: uint64(1710000000), record: map[string]any{"name": nil}},
		{ts: uint64(1710000001), record: map[string]any{"name": "alice"}},
		{ts: uint64(1710000002), record: map[string]any{}},
	})
	if got := p.BatchInsert("", dec); got != output.FLB_OK {
		t.Fatalf("BatchInsert(nullable lowcardinality mixed) = %d, want %d", got, output.FLB_OK)
	}

	if len(batch.rows) != 3 {
		t.Fatalf("batch rows = %d, want 3", len(batch.rows))
	}
	if batch.rows[0][0] != nil {
		t.Fatalf("row[0][0] = %v (%T), want nil", batch.rows[0][0], batch.rows[0][0])
	}
	assertTypedValue(t, batch.rows[1][0], "alice")
	if batch.rows[2][0] != nil {
		t.Fatalf("row[2][0] = %v (%T), want nil", batch.rows[2][0], batch.rows[2][0])
	}
}

func TestInit_ClosesConnectionOnPingFailure(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := &mockConn{pingErr: errors.New("no route")}
	oldOpen := openConnector
	openConnector = func(*clickhouse.Options) (Connector, error) {
		return conn, nil
	}
	t.Cleanup(func() { openConnector = oldOpen })

	p := &ClickHousePlugin{
		Opt:       &clickhouse.Options{DialTimeout: time.Second, Auth: clickhouse.Auth{Database: "default"}},
		TableName: "events",
	}

	if err := p.Init(); err == nil {
		t.Fatal("Init() expected error, got nil")
	}
	if conn.closeCalls != 1 {
		t.Fatalf("Close calls = %d, want 1", conn.closeCalls)
	}
	if p.Conn != nil {
		t.Fatal("expected p.Conn to be nil after failed Init")
	}
}

func TestInit_FailsFastInDaemonMode(t *testing.T) {
	oldDetect := detectDaemonMode
	detectDaemonMode = func() bool { return true }
	t.Cleanup(func() { detectDaemonMode = oldDetect })

	p := &ClickHousePlugin{
		Opt:       &clickhouse.Options{DialTimeout: time.Second, Auth: clickhouse.Auth{Database: "default"}},
		TableName: "events",
	}
	if err := p.Init(); err == nil {
		t.Fatal("Init() expected daemon mode error, got nil")
	}
}

func TestInit_AllowsForegroundMode(t *testing.T) {
	oldDetect := detectDaemonMode
	detectDaemonMode = func() bool { return false }
	t.Cleanup(func() { detectDaemonMode = oldDetect })

	conn := &mockConn{batch: &mockBatch{}}
	oldOpen := openConnector
	openConnector = func(*clickhouse.Options) (Connector, error) {
		return conn, nil
	}
	t.Cleanup(func() { openConnector = oldOpen })

	p := &ClickHousePlugin{
		Opt:       &clickhouse.Options{DialTimeout: time.Second, Auth: clickhouse.Auth{Database: "default"}},
		Columns:   []string{"msg"},
		Defaults:  []any{""},
		ColType:   []ColumnParser{parseString},
		TableName: "events",
	}
	if err := p.Init(); err != nil {
		t.Fatalf("Init() unexpected error in foreground mode: %v", err)
	}
	p.Exit()
}

func TestNewPlugin_TLSAndMetricsConfig(t *testing.T) {
	cfg := baseConfig()
	cfg["TLS"] = "true"
	cfg["TLSServerName"] = "clickhouse.internal"
	cfg["TLSInsecureSkipVerify"] = "true"
	cfg["MetricsAddr"] = "127.0.0.1:9090"

	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("NewPlugin() unexpected error: %v", err)
	}
	if p.Opt.TLS == nil {
		t.Fatal("expected TLS config to be set")
	}
	if p.Opt.TLS.ServerName != "clickhouse.internal" {
		t.Fatalf("TLS.ServerName = %q, want %q", p.Opt.TLS.ServerName, "clickhouse.internal")
	}
	if !p.Opt.TLS.InsecureSkipVerify {
		t.Fatal("expected TLS.InsecureSkipVerify to be true")
	}
	if p.MetricsAddr != "127.0.0.1:9090" {
		t.Fatalf("MetricsAddr = %q, want %q", p.MetricsAddr, "127.0.0.1:9090")
	}
}

func TestValidateMetricsAddr(t *testing.T) {
	if err := validateMetricsAddr("127.0.0.1:0"); err != nil {
		t.Fatalf("validateMetricsAddr() unexpected error: %v", err)
	}
	if err := validateMetricsAddr("localhost:9090"); err != nil {
		t.Fatalf("validateMetricsAddr() localhost unexpected error: %v", err)
	}
	if err := validateMetricsAddr(":9090"); err == nil {
		t.Fatal("validateMetricsAddr() expected error for empty host")
	}
	if err := validateMetricsAddr("0.0.0.0:9090"); err != nil {
		t.Fatalf("validateMetricsAddr() wildcard IPv4 unexpected error: %v", err)
	}
	if err := validateMetricsAddr("[::]:9090"); err != nil {
		t.Fatalf("validateMetricsAddr() wildcard IPv6 unexpected error: %v", err)
	}
	if err := validateMetricsAddr("10.0.0.1:9090"); err == nil {
		t.Fatal("validateMetricsAddr() expected error for non-loopback without env")
	}
	t.Setenv("CLICKHOUSE_ALLOW_PUBLIC_METRICS", "1")
	if err := validateMetricsAddr("10.0.0.1:9090"); err != nil {
		t.Fatalf("validateMetricsAddr() non-loopback with env unexpected error: %v", err)
	}
	if err := validateMetricsAddr("127.0.0.1"); err == nil {
		t.Fatal("validateMetricsAddr() expected error for missing port")
	}
	if err := validateMetricsAddr("bad_host:abc"); err == nil {
		t.Fatal("validateMetricsAddr() expected error for invalid address")
	}
}

func TestStartMetricsServer_ExposesMetrics(t *testing.T) {
	resetSharedMetrics()
	p := &ClickHousePlugin{
		MetricsAddr: "127.0.0.1:0",
		metricsLabels: prometheus.Labels{
			"table":    "test_table",
			"database": "test_db",
		},
	}
	if err := p.initMetrics(); err != nil {
		t.Fatalf("initMetrics() unexpected error: %v", err)
	}
	t.Cleanup(stopSharedMetrics)

	addr := sharedMetricsLn.Addr().String()
	m := sharedMetrics
	m.flushTotal.WithLabelValues("ok", "test_table", "test_db").Inc()
	m.flushInflight.WithLabelValues("test_table", "test_db").Inc()
	m.flushInflight.WithLabelValues("test_table", "test_db").Dec()
	m.batchRows.WithLabelValues("test_table", "test_db").Observe(10)
	m.droppedTotal.WithLabelValues("error", "decode", "test_table", "test_db").Add(2)

	if sharedMetricsSrv.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("ReadHeaderTimeout = %v, want 5s", sharedMetricsSrv.ReadHeaderTimeout)
	}
	if sharedMetricsSrv.ReadTimeout != 10*time.Second {
		t.Fatalf("ReadTimeout = %v, want 10s", sharedMetricsSrv.ReadTimeout)
	}
	if sharedMetricsSrv.IdleTimeout != 30*time.Second {
		t.Fatalf("IdleTimeout = %v, want 30s", sharedMetricsSrv.IdleTimeout)
	}

	client := &net.Dialer{Timeout: time.Second}
	conn, err := client.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("Dial metrics server failed: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("GET /metrics HTTP/1.1\r\nHost: " + addr + "\r\n\r\n")); err != nil {
		t.Fatalf("metrics request write failed: %v", err)
	}

	buf := make([]byte, 8192)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("metrics response read failed: %v", err)
	}
	body := string(buf[:n])
	if !containsAll(body, "clickhouse_flush_total", "clickhouse_flush_inflight", "clickhouse_batch_rows", "clickhouse_records_dropped_total", "plugin_info", "200 OK") {
		t.Fatalf("metrics response missing expected content: %s", body)
	}
}

func TestExit_IsIdempotentAndStopsMetrics(t *testing.T) {
	resetSharedMetrics()
	conn := &mockConn{}
	p := &ClickHousePlugin{
		Opt:         &clickhouse.Options{DialTimeout: time.Second},
		Conn:        conn,
		MetricsAddr: "127.0.0.1:0",
		metricsLabels: prometheus.Labels{
			"table":    "test",
			"database": "test",
		},
	}
	if err := p.initMetrics(); err != nil {
		t.Fatalf("initMetrics() unexpected error: %v", err)
	}

	p.Exit()
	p.Exit()

	if conn.closeCalls != 1 {
		t.Fatalf("Close calls = %d, want 1", conn.closeCalls)
	}
	if p.Conn != nil {
		t.Fatal("expected Conn to be nil after Exit")
	}
	if p.metricsLabels != nil {
		t.Fatal("expected metricsLabels to be cleared after Exit")
	}
	if sharedMetricsSrv != nil {
		// Server may still be active (other instances may exist), that's fine.
		stopSharedMetrics()
	}
}

func TestBatchInsert_ConcurrentWithExit_DoesNotPanic(t *testing.T) {
	batch := &blockingBatch{
		appendStarted: make(chan struct{}),
		releaseAppend: make(chan struct{}),
	}
	conn := &mockConn{batch: batch}
	p := &ClickHousePlugin{
		Opt:         &clickhouse.Options{DialTimeout: time.Second},
		Columns:     []string{"message"},
		Defaults:    []any{""},
		ColType:     []ColumnParser{parseString},
		TableName:   "events",
		BatchStmt:   "INSERT INTO default.events(message)",
		Conn:        conn,
		MetricsAddr: "127.0.0.1:0",
		metricsLabels: prometheus.Labels{
			"table":    "events",
			"database": "default",
		},
	}
	if err := p.initMetrics(); err != nil {
		t.Fatalf("initMetrics() unexpected error: %v", err)
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{"message": "hello"})

	insertDone := make(chan int, 1)
	go func() {
		insertDone <- p.BatchInsert("", dec)
	}()

	select {
	case <-batch.appendStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("BatchInsert did not reach Append")
	}

	exitDone := make(chan struct{})
	go func() {
		p.Exit()
		close(exitDone)
	}()

	select {
	case <-exitDone:
		t.Fatal("Exit returned before in-flight BatchInsert completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(batch.releaseAppend)

	select {
	case code := <-insertDone:
		if code != output.FLB_OK {
			t.Fatalf("BatchInsert() = %d, want %d", code, output.FLB_OK)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BatchInsert did not complete")
	}

	select {
	case <-exitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Exit did not complete")
	}

	if conn.closeCalls != 1 {
		t.Fatalf("Close calls = %d, want 1", conn.closeCalls)
	}

	dec = mustTestDecoder(t, uint64(1710000001), map[string]any{"message": "after-exit"})
	if code := p.BatchInsert("", dec); code != output.FLB_ERROR {
		t.Fatalf("BatchInsert() after Exit = %d, want %d", code, output.FLB_ERROR)
	}
}

func TestBatchInsert_AllowsConcurrentCalls(t *testing.T) {
	const workers = 8

	sendBarrier := newSendBarrier(workers)
	conn := &concurrentMockConn{
		batchFactory: func() driver.Batch {
			return &concurrentMockBatch{onSend: sendBarrier.arrive}
		},
	}
	p := &ClickHousePlugin{
		Opt:       &clickhouse.Options{DialTimeout: time.Second, Auth: clickhouse.Auth{Database: "default"}},
		Columns:   []string{"i64"},
		ColType:   []ColumnParser{parseInt[int64]},
		Defaults:  []any{int64(0)},
		TableName: "events",
		BatchStmt: "INSERT INTO default.events(i64)",
		Conn:      conn,
	}

	decs := make([]*output.FLBDecoder, workers)
	for i := 0; i < workers; i++ {
		decs[i] = mustTestDecoder(t, uint64(1710000000+i), map[string]any{"i64": int64(i)})
	}

	codes := make(chan int, workers)
	start := make(chan struct{})
	ready := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			codes <- p.BatchInsert("", decs[i])
		}(i)
	}

	for i := 0; i < workers; i++ {
		<-ready
	}
	close(start)

	wg.Wait()
	close(codes)

	for code := range codes {
		if code != output.FLB_OK {
			t.Fatalf("BatchInsert() = %d, want %d", code, output.FLB_OK)
		}
	}

	if got := conn.snapshotPrepareCalls(); got != workers {
		t.Fatalf("PrepareBatch calls = %d, want %d", got, workers)
	}

	batches := conn.snapshotBatches()
	if len(batches) != workers {
		t.Fatalf("prepared batches = %d, want %d", len(batches), workers)
	}

	seen := make(map[int64]bool, workers)
	for _, batch := range batches {
		rows := batch.snapshotRows()
		if len(rows) != 1 {
			t.Fatalf("batch rows = %d, want 1", len(rows))
		}
		if len(rows[0]) != 1 {
			t.Fatalf("row columns = %d, want 1", len(rows[0]))
		}
		val, ok := rows[0][0].(int64)
		if !ok {
			t.Fatalf("row value type = %T, want int64", rows[0][0])
		}
		seen[val] = true
	}

	for i := 0; i < workers; i++ {
		if !seen[int64(i)] {
			t.Fatalf("missing row for value %d", i)
		}
	}

	if got := sendBarrier.max(); got != workers {
		t.Fatalf("send overlap = %d, want %d", got, workers)
	}
}

func TestExit_WaitsForInFlightFlush(t *testing.T) {
	conn := &concurrentMockConn{}
	batch := &blockingSendBatch{
		concurrentMockBatch: &concurrentMockBatch{},
		conn:                conn,
		sendStarted:         make(chan struct{}),
		releaseSend:         make(chan struct{}),
	}
	conn.batchFactory = func() driver.Batch { return batch }

	p := &ClickHousePlugin{
		Opt:       &clickhouse.Options{DialTimeout: time.Second, Auth: clickhouse.Auth{Database: "default"}},
		Columns:   []string{"i64"},
		ColType:   []ColumnParser{parseInt[int64]},
		Defaults:  []any{int64(0)},
		TableName: "events",
		BatchStmt: "INSERT INTO default.events(i64)",
		Conn:      conn,
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{"i64": int64(7)})

	var flushCode int
	var flushPanic any
	flushDone := make(chan struct{})
	go func() {
		defer func() {
			flushPanic = recover()
			close(flushDone)
		}()
		flushCode = p.BatchInsert("", dec)
	}()

	<-batch.sendStarted

	var exitPanic any
	exitDone := make(chan struct{})
	go func() {
		defer func() {
			exitPanic = recover()
			close(exitDone)
		}()
		p.Exit()
	}()

	select {
	case <-exitDone:
		t.Fatal("Exit() returned before in-flight flush completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(batch.releaseSend)

	<-flushDone
	<-exitDone

	if flushPanic != nil {
		t.Fatalf("BatchInsert() panicked: %v", flushPanic)
	}
	if exitPanic != nil {
		t.Fatalf("Exit() panicked: %v", exitPanic)
	}
	if batch.usedAfterClose.Load() {
		t.Fatal("batch send observed connection closed before flush finished")
	}
	if flushCode != output.FLB_OK {
		t.Fatalf("BatchInsert() = %d, want %d", flushCode, output.FLB_OK)
	}
	if got := conn.snapshotCloseCalls(); got != 1 {
		t.Fatalf("Close calls = %d, want 1", got)
	}
	if p.Conn != nil {
		t.Fatal("expected p.Conn to be nil after Exit")
	}
}

type fakeABIContext struct {
	remoteContext unsafe.Pointer
}

type fakeABIPlugin struct {
	pad0    unsafe.Pointer
	api     unsafe.Pointer
	oIns    unsafe.Pointer
	context *fakeABIContext
}

func TestSetPluginContext_Releases(t *testing.T) {
	ctxRegistry = sync.Map{}
	plugin := &fakeABIPlugin{context: &fakeABIContext{}}
	p := &ClickHousePlugin{}

	release, err := setPluginContext(unsafe.Pointer(plugin), p)
	if err != nil {
		t.Fatalf("setPluginContext() unexpected error: %v", err)
	}
	if plugin.context.remoteContext == nil {
		t.Fatal("expected remote_context to be set")
	}
	if _, ok := getPluginContext(plugin.context.remoteContext); !ok {
		t.Fatal("expected context to be retrievable before release")
	}

	release()
	if _, ok := getPluginContext(plugin.context.remoteContext); ok {
		t.Fatal("expected context to be released")
	}
}

func TestSetPluginContext_NilProxyContextReturnsError(t *testing.T) {
	ctxRegistry = sync.Map{}
	plugin := &fakeABIPlugin{context: nil}
	p := &ClickHousePlugin{}

	release, err := setPluginContext(unsafe.Pointer(plugin), p)
	if err == nil {
		t.Fatal("setPluginContext() expected error for nil proxy context")
	}
	if release != nil {
		t.Fatal("setPluginContext() expected nil release function on error")
	}
}

func TestFLBPluginFlushCtx_ContextLifecycle(t *testing.T) {
	ctxRegistry = sync.Map{}
	plugin := &fakeABIPlugin{context: &fakeABIContext{}}
	p := &ClickHousePlugin{closed: true}
	release, err := setPluginContext(unsafe.Pointer(plugin), p)
	if err != nil {
		t.Fatalf("setPluginContext() unexpected error: %v", err)
	}

	var buf bytes.Buffer
	oldOut := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	})

	if got := FLBPluginFlushCtx(plugin.context.remoteContext, nil, 0, nil); got != output.FLB_ERROR {
		t.Fatalf("FLBPluginFlushCtx() before release = %d, want %d", got, output.FLB_ERROR)
	}
	before := buf.String()
	if strings.Contains(before, "context not found") {
		t.Fatalf("unexpected context lookup failure before release: %s", before)
	}

	buf.Reset()
	release()
	if got := FLBPluginFlushCtx(plugin.context.remoteContext, nil, 0, nil); got != output.FLB_ERROR {
		t.Fatalf("FLBPluginFlushCtx() after release = %d, want %d", got, output.FLB_ERROR)
	}
	if got := buf.String(); !strings.Contains(got, "flush failed: context not found") {
		t.Fatalf("expected context-not-found log after release, got: %s", got)
	}
}

func TestPayloadTooLarge_Boundary(t *testing.T) {
	if payloadTooLarge(maxFlushPayloadBytes) {
		t.Fatalf("payloadTooLarge(%d) = true, want false", maxFlushPayloadBytes)
	}
	if !payloadTooLarge(maxFlushPayloadBytes + 1) {
		t.Fatalf("payloadTooLarge(%d) = false, want true", maxFlushPayloadBytes+1)
	}
}

func TestFLBPluginFlushCtx_RejectsPayloadTooLarge(t *testing.T) {
	resetSharedMetrics()
	// Initialize shared metrics with a dedicated server on random port.
	initPlugin := &ClickHousePlugin{
		MetricsAddr: "127.0.0.1:0",
		metricsLabels: prometheus.Labels{
			"table":    "test",
			"database": "test",
		},
	}
	if err := initPlugin.initMetrics(); err != nil {
		t.Fatalf("initMetrics() unexpected error: %v", err)
	}
	defer stopSharedMetrics()

	ctxRegistry = sync.Map{}
	plugin := &fakeABIPlugin{context: &fakeABIContext{}}
	p := &ClickHousePlugin{
		closed: true,
		metricsLabels: prometheus.Labels{
			"table":    "test",
			"database": "test",
		},
	}
	release, err := setPluginContext(unsafe.Pointer(plugin), p)
	if err != nil {
		t.Fatalf("setPluginContext() unexpected error: %v", err)
	}
	t.Cleanup(release)

	var buf bytes.Buffer
	oldOut := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	})

	one := []byte{0}
	if got := FLBPluginFlushCtx(plugin.context.remoteContext, unsafe.Pointer(&one[0]), maxFlushPayloadBytes+1, nil); got != output.FLB_ERROR {
		t.Fatalf("FLBPluginFlushCtx() for oversize payload = %d, want %d", got, output.FLB_ERROR)
	}
	if got := buf.String(); !strings.Contains(got, "payload too large") {
		t.Fatalf("expected payload too large log, got: %s", got)
	}

	metric := &dto.Metric{}
	if err := sharedMetrics.droppedTotal.WithLabelValues("error", "payload_limit", "test", "test").Write(metric); err != nil {
		t.Fatalf("write dropped counter: %v", err)
	}
	if got := metric.GetCounter().GetValue(); got != 1 {
		t.Fatalf("droppedTotal(error,payload_limit)=%v, want 1", got)
	}
}

func TestFLBPluginExit_DrainsAllRegisteredInstances(t *testing.T) {
	ctxRegistry = sync.Map{}
	var released atomic.Int32

	conn1 := &mockConn{}
	conn2 := &mockConn{}
	p1 := &ClickHousePlugin{Opt: &clickhouse.Options{}, Conn: conn1}
	p2 := &ClickHousePlugin{Opt: &clickhouse.Options{}, Conn: conn2}
	const key1 = uintptr(0x1)
	const key2 = uintptr(0x2)
	p1.contextRelease = func() {
		released.Add(1)
		ctxRegistry.Delete(key1)
	}
	p2.contextRelease = func() {
		released.Add(1)
		ctxRegistry.Delete(key2)
	}
	ctxRegistry.Store(key1, p1)
	ctxRegistry.Store(key2, p2)

	if code := FLBPluginExit(); code != output.FLB_OK {
		t.Fatalf("FLBPluginExit() = %d, want %d", code, output.FLB_OK)
	}
	if got := conn1.closeCalls; got != 1 {
		t.Fatalf("conn1 close calls = %d, want 1", got)
	}
	if got := conn2.closeCalls; got != 1 {
		t.Fatalf("conn2 close calls = %d, want 1", got)
	}
	if got := released.Load(); got != 2 {
		t.Fatalf("released count = %d, want 2", got)
	}
	remaining := 0
	ctxRegistry.Range(func(any, any) bool {
		remaining++
		return true
	})
	if remaining != 0 {
		t.Fatalf("ctxRegistry remaining entries = %d, want 0", remaining)
	}
}

func TestContextRelease_ConcurrentExitAndExitCtx_ReleaseOnlyOnce(t *testing.T) {
	ctxRegistry = sync.Map{}
	plugin := &fakeABIPlugin{context: &fakeABIContext{}}
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{Auth: clickhouse.Auth{Database: "default"}},
	}

	rawRelease, err := setPluginContext(unsafe.Pointer(plugin), p)
	if err != nil {
		t.Fatalf("setPluginContext() unexpected error: %v", err)
	}
	var releaseCalls atomic.Int32
	p.contextRelease = func() {
		releaseCalls.Add(1)
		rawRelease()
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = FLBPluginExit()
	}()
	go func() {
		defer wg.Done()
		_ = FLBPluginExitCtx(plugin.context.remoteContext)
	}()
	wg.Wait()

	if got := releaseCalls.Load(); got != 1 {
		t.Fatalf("context release calls=%d, want 1", got)
	}
}

func TestBatchInsert_UsesWriteTimeoutForPrepareContext(t *testing.T) {
	const writeTimeout = 120 * time.Millisecond
	conn := &mockConn{
		prepareHook: func(ctx context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("expected prepare context to carry deadline")
			}
			remaining := time.Until(deadline)
			if remaining <= 0 || remaining > writeTimeout+80*time.Millisecond {
				t.Fatalf("prepare deadline remaining = %v, want <= %v", remaining, writeTimeout+80*time.Millisecond)
			}
			return &mockBatch{}, nil
		},
	}
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: 2 * time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: writeTimeout,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseString},
		Defaults:     []any{""},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         conn,
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{"msg": "hello"})
	if code := p.BatchInsert("", dec); code != output.FLB_OK {
		t.Fatalf("BatchInsert() = %d, want %d", code, output.FLB_OK)
	}
}

func TestBatchInsert_DecodeNonZeroRetReturnsRetryWithoutPrepare(t *testing.T) {
	conn := &mockConn{
		prepareHook: func(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
			t.Fatal("prepare should not be called when decode fails")
			return nil, nil
		},
	}
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			Auth: clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseString},
		Defaults:     []any{""},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         conn,
	}

	invalid := []byte{0xc1}
	dec := output.NewDecoder(unsafe.Pointer(&invalid[0]), len(invalid))
	if code := p.BatchInsert("", dec); code != output.FLB_RETRY {
		t.Fatalf("BatchInsert() = %d, want %d", code, output.FLB_RETRY)
	}
}

func TestBatchInsertPayload_EOFRecordReturnsOK(t *testing.T) {
	conn := &concurrentMockConn{
		batchFactory: func() driver.Batch {
			return &concurrentMockBatch{}
		},
	}
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseString},
		Defaults:     []any{""},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         conn,
	}

	payload := mustTestPayloadRows(t, []testRecordRow{
		{ts: uint64(1710000000), record: map[string]any{"msg": "hello"}},
	})
	if code := p.BatchInsertPayload("", payload); code != output.FLB_OK {
		t.Fatalf("BatchInsertPayload() = %d, want %d", code, output.FLB_OK)
	}
	if got := conn.snapshotPrepareCalls(); got != 1 {
		t.Fatalf("PrepareBatch calls = %d, want 1", got)
	}
}

func TestBatchInsertPayload_InvalidMsgpackReturnsRetry(t *testing.T) {
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			Auth: clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseString},
		Defaults:     []any{""},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         &mockConn{},
	}
	if code := p.BatchInsertPayload("", []byte{0xc1}); code != output.FLB_RETRY {
		t.Fatalf("BatchInsertPayload() = %d, want %d", code, output.FLB_RETRY)
	}
}

func TestBatchInsertPayload_InvalidEnvelopeReturnsError(t *testing.T) {
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			Auth: clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseString},
		Defaults:     []any{""},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         &mockConn{},
	}

	var payload []byte
	enc := codec.NewEncoderBytes(&payload, &codec.MsgpackHandle{})
	if err := enc.Encode(map[string]any{"bad": "envelope"}); err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	if code := p.BatchInsertPayload("", payload); code != output.FLB_ERROR {
		t.Fatalf("BatchInsertPayload() = %d, want %d", code, output.FLB_ERROR)
	}
}

func TestBatchInsertPayload_InvalidTimestampReturnsError(t *testing.T) {
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			Auth: clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseString},
		Defaults:     []any{""},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         &mockConn{},
	}

	var payload []byte
	enc := codec.NewEncoderBytes(&payload, &codec.MsgpackHandle{})
	if err := enc.Encode([]any{"bad-ts", map[string]any{"msg": "hello"}}); err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	if code := p.BatchInsertPayload("", payload); code != output.FLB_ERROR {
		t.Fatalf("BatchInsertPayload() = %d, want %d", code, output.FLB_ERROR)
	}
}

func TestBatchInsertPayload_InvalidRecordReturnsError(t *testing.T) {
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			Auth: clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseString},
		Defaults:     []any{""},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         &mockConn{},
	}

	var payload []byte
	enc := codec.NewEncoderBytes(&payload, &codec.MsgpackHandle{})
	if err := enc.Encode([]any{uint64(1710000000), "bad-record"}); err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	if code := p.BatchInsertPayload("", payload); code != output.FLB_ERROR {
		t.Fatalf("BatchInsertPayload() = %d, want %d", code, output.FLB_ERROR)
	}
}

func TestBatchInsertPayload_ColumnParseFailureSkipsRecord(t *testing.T) {
	batch := &mockBatch{}
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseInt[int64]},
		Defaults:     []any{int64(0)},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         &mockConn{batch: batch},
	}
	payload := mustTestPayloadRows(t, []testRecordRow{
		{ts: uint64(1710000000), record: map[string]any{"msg": "not-int"}},
	})
	if code := p.BatchInsertPayload("", payload); code != output.FLB_OK {
		t.Fatalf("BatchInsertPayload() = %d, want %d (bad record skipped)", code, output.FLB_OK)
	}
	if len(batch.rows) != 0 {
		t.Fatalf("expected 0 rows (bad record skipped), got %d", len(batch.rows))
	}
}

func TestBatchInsertPayload_ChunksByMaxRowsPerChunk(t *testing.T) {
	conn := &concurrentMockConn{
		batchFactory: func() driver.Batch {
			return &concurrentMockBatch{}
		},
	}
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseInt[int64]},
		Defaults:     []any{int64(0)},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         conn,
	}

	rows := make([]testRecordRow, maxRowsPerChunk+1)
	for i := range rows {
		rows[i] = testRecordRow{
			ts:     uint64(1710000000 + i),
			record: map[string]any{"msg": int64(i)},
		}
	}
	payload := mustTestPayloadRows(t, rows)
	if code := p.BatchInsertPayload("", payload); code != output.FLB_OK {
		t.Fatalf("BatchInsertPayload() = %d, want %d", code, output.FLB_OK)
	}
	if got := conn.snapshotPrepareCalls(); got != 2 {
		t.Fatalf("PrepareBatch calls = %d, want 2", got)
	}
	batches := conn.snapshotBatches()
	if len(batches) != 2 {
		t.Fatalf("batch count = %d, want 2", len(batches))
	}
	if got := len(batches[0].snapshotRows()); got != maxRowsPerChunk {
		t.Fatalf("first batch rows = %d, want %d", got, maxRowsPerChunk)
	}
	if got := len(batches[1].snapshotRows()); got != 1 {
		t.Fatalf("second batch rows = %d, want 1", got)
	}
}

func TestBatchInsertPayload_TailChunkSentWhenBelowLimit(t *testing.T) {
	conn := &concurrentMockConn{
		batchFactory: func() driver.Batch {
			return &concurrentMockBatch{}
		},
	}
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseInt[int64]},
		Defaults:     []any{int64(0)},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         conn,
	}

	rows := make([]testRecordRow, maxRowsPerChunk-1)
	for i := range rows {
		rows[i] = testRecordRow{
			ts:     uint64(1710000000 + i),
			record: map[string]any{"msg": int64(i)},
		}
	}
	payload := mustTestPayloadRows(t, rows)
	if code := p.BatchInsertPayload("", payload); code != output.FLB_OK {
		t.Fatalf("BatchInsertPayload() = %d, want %d", code, output.FLB_OK)
	}
	if got := conn.snapshotPrepareCalls(); got != 1 {
		t.Fatalf("PrepareBatch calls = %d, want 1", got)
	}
	batches := conn.snapshotBatches()
	if len(batches) != 1 {
		t.Fatalf("batch count = %d, want 1", len(batches))
	}
	if got := len(batches[0].snapshotRows()); got != maxRowsPerChunk-1 {
		t.Fatalf("tail batch rows = %d, want %d", got, maxRowsPerChunk-1)
	}
}

func TestBatchInsertPayload_MidChunkFailureReturnsError(t *testing.T) {
	batchSeq := 0
	conn := &concurrentMockConn{
		batchFactory: func() driver.Batch {
			batchSeq++
			b := &concurrentMockBatch{}
			if batchSeq == 2 {
				b.sendErr = errors.New("send failed")
			}
			return b
		},
	}
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: time.Second,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"msg"},
		ColType:      []ColumnParser{parseInt[int64]},
		Defaults:     []any{int64(0)},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(msg)",
		Conn:         conn,
	}

	rows := make([]testRecordRow, maxRowsPerChunk+1)
	for i := range rows {
		rows[i] = testRecordRow{
			ts:     uint64(1710000000 + i),
			record: map[string]any{"msg": int64(i)},
		}
	}
	payload := mustTestPayloadRows(t, rows)
	if code := p.BatchInsertPayload("", payload); code != output.FLB_ERROR {
		t.Fatalf("BatchInsertPayload() = %d, want %d", code, output.FLB_ERROR)
	}
	if got := conn.snapshotPrepareCalls(); got != 2 {
		t.Fatalf("PrepareBatch calls = %d, want 2", got)
	}
}

func TestBatchInsert_RecoversFromParserPanic(t *testing.T) {
	conn := &mockConn{batch: &mockBatch{}}
	p := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			Auth: clickhouse.Auth{Database: "default"},
		},
		WriteTimeout: time.Second,
		Columns:      []string{"panic_col"},
		ColType: []ColumnParser{
			func(string) (any, error) {
				panic("parser panic")
			},
		},
		Defaults:  []any{""},
		TableName: "events",
		BatchStmt: "INSERT INTO default.events(panic_col)",
		Conn:      conn,
	}

	dec := mustTestDecoder(t, uint64(1710000000), map[string]any{"panic_col": "boom"})
	if code := p.BatchInsert("", dec); code != output.FLB_ERROR {
		t.Fatalf("BatchInsert() = %d, want %d", code, output.FLB_ERROR)
	}
}

func TestSafeExportCall_RecoversPanic(t *testing.T) {
	got := safeExportCall("test", output.FLB_ERROR, func() int {
		panic("boom")
	})
	if got != output.FLB_ERROR {
		t.Fatalf("safeExportCall() = %d, want %d", got, output.FLB_ERROR)
	}
}

func TestLogPluginf_UsesInternalSinkWhenAvailable(t *testing.T) {
	oldInternal := internalLogSink
	oldStderr := stderrLogSink
	t.Cleanup(func() {
		internalLogSink = oldInternal
		stderrLogSink = oldStderr
	})

	var internalCalled bool
	var stderrCalled bool
	internalLogSink = func(instance unsafe.Pointer, level int, message string) bool {
		internalCalled = true
		return instance != nil && level == pluginLogLevelInfo && strings.Contains(message, "[INFO] [clickhouse] hello")
	}
	stderrLogSink = func(string) {
		stderrCalled = true
	}

	token := byte(1)
	logPluginfForInstance(unsafe.Pointer(&token), pluginLogLevelInfo, "INFO", "hello")
	if !internalCalled {
		t.Fatal("expected internal logger to be called")
	}
	if stderrCalled {
		t.Fatal("did not expect stderr fallback when internal logger succeeds")
	}
}

func TestLogPluginf_DebugAndTraceLevels(t *testing.T) {
	oldInternal := internalLogSink
	oldStderr := stderrLogSink
	t.Cleanup(func() {
		internalLogSink = oldInternal
		stderrLogSink = oldStderr
	})

	var seenDebug bool
	var seenTrace bool
	internalLogSink = func(instance unsafe.Pointer, level int, message string) bool {
		if instance == nil {
			return false
		}
		switch level {
		case pluginLogLevelDebug:
			if strings.Contains(message, "[DEBUG] [clickhouse] debug test") {
				seenDebug = true
			}
		case pluginLogLevelTrace:
			if strings.Contains(message, "[TRACE] [clickhouse] trace test") {
				seenTrace = true
			}
		}
		return true
	}
	stderrLogSink = func(string) {
		t.Fatal("did not expect stderr fallback when internal logger succeeds")
	}

	token := byte(1)
	logPluginfForInstance(unsafe.Pointer(&token), pluginLogLevelDebug, "DEBUG", "debug test")
	logPluginfForInstance(unsafe.Pointer(&token), pluginLogLevelTrace, "TRACE", "trace test")
	if !seenDebug {
		t.Fatal("expected debug level log to use internal sink")
	}
	if !seenTrace {
		t.Fatal("expected trace level log to use internal sink")
	}
}

func TestLogPluginf_FallsBackToStderrWhenInternalUnavailable(t *testing.T) {
	oldInternal := internalLogSink
	oldStderr := stderrLogSink
	t.Cleanup(func() {
		internalLogSink = oldInternal
		stderrLogSink = oldStderr
	})

	var stderrMsg string
	internalLogSink = func(instance unsafe.Pointer, level int, message string) bool {
		return false
	}
	stderrLogSink = func(message string) {
		stderrMsg = message
	}

	logWarnf("fallback test")
	if !strings.Contains(stderrMsg, "[WARN] [clickhouse] fallback test") {
		t.Fatalf("stderr fallback message mismatch: %q", stderrMsg)
	}
}

func TestSafeExportCall_RecoversPanic_LogsThroughInternalSink(t *testing.T) {
	oldInternal := internalLogSink
	oldStderr := stderrLogSink
	t.Cleanup(func() {
		internalLogSink = oldInternal
		stderrLogSink = oldStderr
	})

	var internalMsg string
	var stderrCalled bool
	internalLogSink = func(instance unsafe.Pointer, level int, message string) bool {
		internalMsg = message
		return true
	}
	stderrLogSink = func(string) {
		stderrCalled = true
	}

	got := safeExportCall("panic-path", output.FLB_ERROR, func() int {
		panic("boom")
	})
	if got != output.FLB_ERROR {
		t.Fatalf("safeExportCall() = %d, want %d", got, output.FLB_ERROR)
	}
	if !strings.Contains(internalMsg, "panic-path panic recovered") {
		t.Fatalf("expected panic message in internal sink, got: %q", internalMsg)
	}
	if stderrCalled {
		t.Fatal("did not expect stderr fallback when internal logger succeeds")
	}
}

func TestInstanceLogger_NoCrossInstanceContextBleed(t *testing.T) {
	oldInternal := internalLogSink
	oldStderr := stderrLogSink
	t.Cleanup(func() {
		internalLogSink = oldInternal
		stderrLogSink = oldStderr
	})

	type logEvent struct {
		instance unsafe.Pointer
		message  string
	}
	var (
		mu     sync.Mutex
		events []logEvent
	)
	internalLogSink = func(instance unsafe.Pointer, level int, message string) bool {
		mu.Lock()
		events = append(events, logEvent{instance: instance, message: message})
		mu.Unlock()
		return true
	}
	stderrLogSink = func(string) {
		t.Fatal("unexpected stderr fallback")
	}

	tokenA := byte(1)
	tokenB := byte(2)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			logPluginfForInstance(unsafe.Pointer(&tokenA), pluginLogLevelError, "ERROR", "instance-A-%d", i)
		}(i)
		go func(i int) {
			defer wg.Done()
			logPluginfForInstance(unsafe.Pointer(&tokenB), pluginLogLevelError, "ERROR", "instance-B-%d", i)
		}(i)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 200 {
		t.Fatalf("events=%d, want 200", len(events))
	}
	for _, ev := range events {
		switch ev.instance {
		case unsafe.Pointer(&tokenA):
			if !strings.Contains(ev.message, "instance-A-") {
				t.Fatalf("tokenA saw wrong message: %q", ev.message)
			}
		case unsafe.Pointer(&tokenB):
			if !strings.Contains(ev.message, "instance-B-") {
				t.Fatalf("tokenB saw wrong message: %q", ev.message)
			}
		default:
			t.Fatalf("unexpected instance pointer: %v message=%q", ev.instance, ev.message)
		}
	}
}

func TestThrottledLogger_IsolatedPerPluginInstance(t *testing.T) {
	oldInternal := internalLogSink
	oldStderr := stderrLogSink
	t.Cleanup(func() {
		internalLogSink = oldInternal
		stderrLogSink = oldStderr
	})

	type event struct {
		instance unsafe.Pointer
		message  string
	}
	var (
		mu     sync.Mutex
		events []event
	)
	internalLogSink = func(instance unsafe.Pointer, level int, message string) bool {
		mu.Lock()
		events = append(events, event{instance: instance, message: message})
		mu.Unlock()
		return true
	}
	stderrLogSink = func(string) {
		t.Fatal("unexpected stderr fallback")
	}

	tokenA := byte(11)
	tokenB := byte(22)
	p1 := &ClickHousePlugin{
		logInstance: unsafe.Pointer(&tokenA),
		errorLog:    newThrottledLogger(time.Hour),
	}
	p2 := &ClickHousePlugin{
		logInstance: unsafe.Pointer(&tokenB),
		errorLog:    newThrottledLogger(time.Hour),
	}

	p1.throttledErrorf("shared_key", "p1-first")
	p1.throttledErrorf("shared_key", "p1-second-suppressed")
	p2.throttledErrorf("shared_key", "p2-first")

	mu.Lock()
	defer mu.Unlock()
	var seenP1, seenP2 bool
	for _, ev := range events {
		switch ev.instance {
		case unsafe.Pointer(&tokenA):
			if strings.Contains(ev.message, "p1-first") {
				seenP1 = true
			}
			if strings.Contains(ev.message, "p1-second-suppressed") {
				t.Fatalf("unexpected unsuppressed p1 second log: %q", ev.message)
			}
		case unsafe.Pointer(&tokenB):
			if strings.Contains(ev.message, "p2-first") {
				seenP2 = true
			}
		}
	}
	if !seenP1 {
		t.Fatal("missing first log from plugin instance p1")
	}
	if !seenP2 {
		t.Fatal("missing first log from plugin instance p2; likely cross-instance suppression")
	}
}

func containsAll(s string, wants ...string) bool {
	for _, want := range wants {
		if !strings.Contains(s, want) {
			return false
		}
	}
	return true
}

type testRecordRow struct {
	ts     any
	record map[string]any
}

func mustTestDecoder(t *testing.T, ts any, record map[string]any) *output.FLBDecoder {
	t.Helper()

	var buf []byte
	enc := codec.NewEncoderBytes(&buf, &codec.MsgpackHandle{})
	if err := enc.Encode([]any{ts, record}); err != nil {
		t.Fatalf("encode test record: %v", err)
	}
	if len(buf) == 0 {
		t.Fatal("encoded test record is empty")
	}

	return output.NewDecoder(unsafe.Pointer(&buf[0]), len(buf))
}

func mustTestDecoderRows(t *testing.T, rows []testRecordRow) *output.FLBDecoder {
	t.Helper()
	if len(rows) == 0 {
		t.Fatal("rows must not be empty")
	}

	var buf []byte
	enc := codec.NewEncoderBytes(&buf, &codec.MsgpackHandle{})
	for _, row := range rows {
		if err := enc.Encode([]any{row.ts, row.record}); err != nil {
			t.Fatalf("encode test record row: %v", err)
		}
	}
	if len(buf) == 0 {
		t.Fatal("encoded test rows are empty")
	}
	return output.NewDecoder(unsafe.Pointer(&buf[0]), len(buf))
}

func mustTestPayloadRows(t *testing.T, rows []testRecordRow) []byte {
	t.Helper()
	if len(rows) == 0 {
		t.Fatal("rows must not be empty")
	}
	var buf []byte
	enc := codec.NewEncoderBytes(&buf, &codec.MsgpackHandle{})
	for _, row := range rows {
		if err := enc.Encode([]any{row.ts, row.record}); err != nil {
			t.Fatalf("encode test payload row: %v", err)
		}
	}
	if len(buf) == 0 {
		t.Fatal("encoded test payload rows are empty")
	}
	return buf
}

func assertTypedValue[T comparable](t *testing.T, got any, want T) {
	t.Helper()

	typed, ok := got.(T)
	if !ok {
		t.Fatalf("value type = %T, want %T", got, want)
	}
	if typed != want {
		t.Fatalf("value = %v (%T), want %v (%T)", got, got, want, want)
	}
}

func TestBatchBuffer_EmptyFlush(t *testing.T) {
	buf := &batchBuffer{
		maxRows:  5000,
		interval: time.Second,
		notify:   make(chan struct{}, 1),
		stop:     make(chan struct{}),
		p: &ClickHousePlugin{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	// flush with no rows should not panic
	buf.flush()
}

func TestBatchBuffer_AppendAndFlush(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	p := &ClickHousePlugin{
		Opt:         &clickhouse.Options{Auth: clickhouse.Auth{Database: "default"}},
		Columns:     []string{"message"},
		Defaults:    []any{""},
		ColType:     []ColumnParser{parseString},
		TableName:   "events",
		BatchStmt:   "INSERT INTO default.events(message)",
		Conn:        conn,
		WriteTimeout: time.Second,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		metricsLabels: prometheus.Labels{
			"table":    "events",
			"database": "default",
		},
	}

	buf := newBatchBuffer(p, 10, 100*time.Millisecond)

	// Append rows in batches
	buf.Append([][]any{{"hello"}, {"world"}})
	buf.Append([][]any{{"foo"}, {"bar"}})

	if len(buf.rows) != 4 {
		t.Fatalf("buffer rows = %d, want 4", len(buf.rows))
	}

	// Flush
	buf.flush()

}

func TestBatchBuffer_MaxRowsTriggersFlush(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	p := &ClickHousePlugin{
		Opt:          &clickhouse.Options{Auth: clickhouse.Auth{Database: "default"}},
		Columns:      []string{"message"},
		Defaults:     []any{""},
		ColType:      []ColumnParser{parseString},
		TableName:    "events",
		BatchStmt:    "INSERT INTO default.events(message)",
		Conn:         conn,
		WriteTimeout: time.Second,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		metricsLabels: prometheus.Labels{
			"table":    "events",
			"database": "default",
		},
	}

	buf := newBatchBuffer(p, 3, time.Hour) // long interval, won't trigger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go buf.run(ctx)

	// Append 2 rows - buffer accumulates
	buf.Append([][]any{{"a"}, {"b"}})
	time.Sleep(50 * time.Millisecond)

	if conn.prepareCalls != 0 {
		t.Errorf("prepareCalls = %d, want 0 before maxRows", conn.prepareCalls)
	}

	// Third row should trigger flush via notify channel
	buf.Append([][]any{{"c"}})
	time.Sleep(50 * time.Millisecond)

	// After flush, buffer should be empty and batch should have 3 rows
	if len(buf.rows) != 0 {
		t.Errorf("buffer rows after flush = %d, want 0", len(buf.rows))
	}
	if len(batch.rows) != 3 {
		t.Errorf("batch rows = %d, want 3", len(batch.rows))
	}
	if conn.prepareCalls != 1 {
		t.Errorf("prepareCalls = %d, want 1", conn.prepareCalls)
	}

	// Fourth row goes to new buffer
	buf.Append([][]any{{"d"}})
	time.Sleep(50 * time.Millisecond)
	if len(buf.rows) != 1 {
		t.Errorf("buffer rows after 4th append = %d, want 1", len(buf.rows))
	}
}
