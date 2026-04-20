package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/ugorji/go/codec"
)

type benchmarkConn struct {
}

func (c *benchmarkConn) Ping(context.Context) error { return nil }

func (c *benchmarkConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &benchmarkBatch{rows: make([][]any, 0, 1)}, nil
}

func (c *benchmarkConn) Close() error { return nil }

type benchmarkBatch struct {
	rows [][]any
	sent bool
}

type orderedRecord []any

func (orderedRecord) MapBySlice() {}

func (b *benchmarkBatch) Abort() error { return nil }
func (b *benchmarkBatch) Append(v ...any) error {
	row := make([]any, len(v))
	copy(row, v)
	b.rows = append(b.rows, row)
	return nil
}
func (b *benchmarkBatch) AppendStruct(any) error        { return nil }
func (b *benchmarkBatch) Column(int) driver.BatchColumn { return nil }
func (b *benchmarkBatch) Flush() error                  { return nil }
func (b *benchmarkBatch) Send() error                   { b.sent = true; return nil }
func (b *benchmarkBatch) IsSent() bool                  { return b.sent }
func (b *benchmarkBatch) Rows() int                     { return len(b.rows) }
func (b *benchmarkBatch) Columns() []column.Interface   { return nil }
func (b *benchmarkBatch) Close() error                  { return nil }

func BenchmarkBatchInsert_WideRecord(b *testing.B) {
	plugin, decBytes := benchmarkBatchInsertFixture(b, 1, 128)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		plugin.BatchInsert("", benchmarkDecoder(decBytes))
	}
}

func BenchmarkBatchInsert_LargeChunk(b *testing.B) {
	plugin, decBytes := benchmarkBatchInsertFixture(b, 512, 16)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		plugin.BatchInsert("", benchmarkDecoder(decBytes))
	}
}

func benchmarkBatchInsertFixture(b *testing.B, rows, cols int) (*ClickHousePlugin, []byte) {
	b.Helper()

	colNames := benchmarkColumnNames(cols)
	parsers := make([]ColumnParser, cols)
	defaults := make([]any, cols)
	for i := 0; i < cols; i++ {
		parsers[i] = parseInt[int64]
		defaults[i] = int64(0)
	}

	plugin := &ClickHousePlugin{
		Opt: &clickhouse.Options{
			DialTimeout: 0,
			Auth:        clickhouse.Auth{Database: "default"},
		},
		Columns:   colNames,
		ColType:   parsers,
		Defaults:  defaults,
		TableName: "events",
		BatchStmt: "INSERT INTO default.events(" + strings.Join(colNames, ",") + ")",
		Conn:      &benchmarkConn{},
	}

	encBytes := benchmarkDecoderBytes(b, rows, colNames)
	return plugin, encBytes
}

func benchmarkDecoderBytes(b *testing.B, rows int, colNames []string) []byte {
	b.Helper()

	var buf []byte
	enc := codec.NewEncoderBytes(&buf, &codec.MsgpackHandle{})

	for row := 0; row < rows; row++ {
		record := make(orderedRecord, 0, len(colNames)*2)
		for col := range colNames {
			record = append(record, colNames[col], int64(row*len(colNames)+col))
		}
		if err := enc.Encode([]any{uint64(1710000000 + row), record}); err != nil {
			b.Fatalf("encode benchmark record: %v", err)
		}
	}

	if len(buf) == 0 {
		b.Fatal("benchmark decoder bytes are empty")
	}
	return buf
}

func benchmarkDecoder(buf []byte) *output.FLBDecoder {
	return output.NewDecoder(unsafe.Pointer(&buf[0]), len(buf))
}

func benchmarkColumnNames(count int) []string {
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = fmt.Sprintf("c%d", i)
	}
	return names
}
