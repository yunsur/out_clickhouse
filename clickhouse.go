package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cespare/xxhash/v2"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ugorji/go/codec"
)

// Sentinel errors for configuration validation.
var (
	ErrNoDatabase = errors.New("clickhouse: Database is required")
	ErrNoTable    = errors.New("clickhouse: Table is required")
	ErrNoColumns  = errors.New("clickhouse: Columns is required")
)

var openConnector = func(opt *clickhouse.Options) (Connector, error) {
	return clickhouse.Open(opt)
}

var boundedSettings = map[string]int64{
	"max_memory_usage":                 10 << 30,
	"max_memory_usage_for_user":        10 << 30,
	"max_memory_usage_for_all_queries": 10 << 30,
	"max_threads":                      64,
	"max_insert_threads":               64,
	"max_partitions_per_insert_block":  1000,
	"max_insert_block_size":            1000000,
	"max_block_size":                   1000000,
}

var forbiddenHTTPHeaders = map[string]struct{}{
	"host":          {},
	"authorization": {},
}

var passwordPattern = regexp.MustCompile(`(?i)(password|pass|pwd|token|secret)([=:])[^&\s]+`)
var userinfoPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*:\/\/[^:\/\s@]+:)[^@\/\s]+@`)
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)
var pluginVersion = "dev"
var pluginCommit = "unknown"

var protectedSettings = map[string]struct{}{
	"insert_deduplication_token": {},
	"insert_deduplicate":         {},
}

var retryableCHCodes = map[int32]struct{}{
	203: {},
	209: {},
	210: {},
	236: {},
	241: {},
	242: {},
	252: {},
	519: {},
	999: {},
}

const maxRowsPerChunk = 10000

var detectDaemonMode = func() bool {
	return os.Getenv("FLB_DAEMON_MODE") == "1"
}

// ConfigError wraps a configuration parsing error with the field name.
type ConfigError struct {
	Field string
	Err   error
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("clickhouse: invalid config %s: %v", e.Field, e.Err)
}

func (e *ConfigError) Unwrap() error {
	return e.Err
}

// configErr creates a ConfigError for the given field.
func configErr(field string, err error) error {
	return &ConfigError{Field: field, Err: err}
}

// quoteIdentifier wraps a ClickHouse identifier in backticks, escaping any
// embedded backticks to prevent SQL injection via configuration values.
func quoteIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func validateIdentifier(field, value string) error {
	if !validIdentifier.MatchString(value) {
		return configErr(field, fmt.Errorf("invalid identifier: %q", value))
	}
	return nil
}

// ColumnParser converts a string value to the appropriate Go type for ClickHouse insertion.
type ColumnParser func(string) (any, error)

// Connector abstracts the ClickHouse connection for testability.
type Connector interface {
	Ping(ctx context.Context) error
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	Close() error
}

type pluginMetrics struct {
	registry      *prometheus.Registry
	flushTotal    *prometheus.CounterVec
	flushDuration prometheus.Histogram
	flushInflight prometheus.Gauge
	batchRows     prometheus.Histogram
	flushErrors   *prometheus.CounterVec
	recordsTotal  prometheus.Counter
	droppedTotal  *prometheus.CounterVec
	pluginInfo    *prometheus.GaugeVec
}

type throttledLogger struct {
	mu       sync.Mutex
	last     map[string]time.Time
	dropped  map[string]uint64
	interval time.Duration
}

func newThrottledLogger(interval time.Duration) *throttledLogger {
	return &throttledLogger{
		last:     map[string]time.Time{},
		dropped:  map[string]uint64{},
		interval: interval,
	}
}

func (t *throttledLogger) logf(logFn func(string, ...any), key, format string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	if prev, ok := t.last[key]; ok && now.Sub(prev) < t.interval {
		t.dropped[key]++
		return
	}
	if dropped := t.dropped[key]; dropped > 0 {
		logFn("suppressed %d '%s' messages", dropped, key)
		t.dropped[key] = 0
	}
	t.last[key] = now
	logFn(format, args...)
}

// ClickHousePlugin is the Fluent Bit output plugin context for ClickHouse.
type ClickHousePlugin struct {
	mu sync.RWMutex

	// Opt holds the clickhouse-go connection options.
	Opt *clickhouse.Options

	// Column configuration for batch insert.
	Columns     []string
	Defaults    []any
	ColType     []ColumnParser
	ColNullable []bool
	TableName   string
	BatchStmt   string
	MetricsAddr string

	// Conn is the active ClickHouse connection.
	Conn Connector

	closed bool

	WriteTimeout       time.Duration
	SemaphoreTimeout   time.Duration
	insertSem          chan struct{}
	inflight           sync.WaitGroup
	rootCtx            context.Context
	rootCancel         context.CancelFunc
	contextRelease     func()
	contextReleaseOnce sync.Once

	metrics         *pluginMetrics
	metricsServer   *http.Server
	metricsListener net.Listener
	logInstance     unsafe.Pointer
	errorLog        *throttledLogger

	// Logger is the unified slog logger for both plugin and clickhouse-go.
	Logger *slog.Logger

	// logLevel stores the parsed log level for level filtering.
	logLevel slog.Level
}

// NewPlugin parses the Fluent Bit plugin configuration and returns a new ClickHousePlugin.
// The get function retrieves config values by key, with optional default values.
func NewPlugin(get func(key string, defaults ...string) string) (*ClickHousePlugin, error) {
	p := &ClickHousePlugin{
		Opt:      &clickhouse.Options{},
		errorLog: newThrottledLogger(200 * time.Millisecond),
	}
	opt := p.Opt

	if err := p.parseProtocol(get, opt); err != nil {
		return nil, err
	}
	if err := p.parseAddr(get, opt); err != nil {
		return nil, err
	}

	opt.Auth.Database = get("Database")
	if opt.Auth.Database == "" {
		return nil, configErr("Database", ErrNoDatabase)
	}
	if err := validateIdentifier("Database", opt.Auth.Database); err != nil {
		return nil, err
	}

	// Default to "default" username, matching clickhouse-go defaults.
	opt.Auth.Username = get("UserName", "default")

	if val := get("Password"); val != "" {
		opt.Auth.Password = val
	}

	tableName := get("Table")
	if tableName == "" {
		return nil, ErrNoTable
	}
	if err := validateIdentifier("Table", tableName); err != nil {
		return nil, err
	}
	p.TableName = tableName

	if err := p.parseLogLevel(get); err != nil {
		return nil, err
	}
	if err := p.parsePoolConfig(get, opt); err != nil {
		return nil, err
	}
	if err := p.parseStrategy(get, opt); err != nil {
		return nil, err
	}
	if err := p.parseCompression(get, opt); err != nil {
		return nil, err
	}
	if err := p.parseBufferConfig(get, opt); err != nil {
		return nil, err
	}
	if err := p.parseClientInfo(get, opt); err != nil {
		return nil, err
	}
	if err := p.parseHTTPConfig(get, opt); err != nil {
		return nil, err
	}
	if err := p.parseTLS(get, opt); err != nil {
		return nil, err
	}
	if err := p.parseSettings(get, opt); err != nil {
		return nil, err
	}
	if err := p.parseMetricsConfig(get); err != nil {
		return nil, err
	}
	if err := p.parseJWT(get); err != nil {
		return nil, err
	}

	columnsVal := get("Columns")
	if columnsVal == "" {
		return nil, ErrNoColumns
	}
	if err := parseColumns(p, columnsVal); err != nil {
		return nil, err
	}

	return p, nil
}

const (
	pluginLogLevelInfo = iota
	pluginLogLevelWarn
	pluginLogLevelError
	pluginLogLevelDebug
	pluginLogLevelTrace
)

var internalLogSink = emitFluentBitLog

var stderrLogSink = func(message string) {
	log.Print(message)
}

func logInfof(format string, args ...any) {
	logPluginfForInstance(nil, pluginLogLevelInfo, "INFO", format, args...)
}

func logWarnf(format string, args ...any) {
	logPluginfForInstance(nil, pluginLogLevelWarn, "WARN", format, args...)
}

func logErrorf(format string, args ...any) {
	logPluginfForInstance(nil, pluginLogLevelError, "ERROR", format, args...)
}

func logDebugf(format string, args ...any) {
	logPluginfForInstance(nil, pluginLogLevelDebug, "DEBUG", format, args...)
}

func logTracef(format string, args ...any) {
	logPluginfForInstance(nil, pluginLogLevelTrace, "TRACE", format, args...)
}

func logPluginfForInstance(instance unsafe.Pointer, level int, levelText string, format string, args ...any) {
	message := fmt.Sprintf("[%s] [clickhouse] "+format, append([]any{levelText}, args...)...)
	if internalLogSink != nil && internalLogSink(instance, level, message) {
		return
	}
	stderrLogSink(message)
}

func (p *ClickHousePlugin) logInfof(format string, args ...any) {
	if p.Logger != nil {
		if p.logLevel <= slog.LevelInfo {
			p.Logger.Info(fmt.Sprintf(format, args...))
		}
	} else {
		p.logPluginf(pluginLogLevelInfo, "INFO", format, args...)
	}
}

func (p *ClickHousePlugin) logWarnf(format string, args ...any) {
	if p.Logger != nil {
		if p.logLevel <= slog.LevelWarn {
			p.Logger.Warn(fmt.Sprintf(format, args...))
		}
	} else {
		p.logPluginf(pluginLogLevelWarn, "WARN", format, args...)
	}
}

func (p *ClickHousePlugin) logErrorf(format string, args ...any) {
	if p.Logger != nil {
		if p.logLevel <= slog.LevelError {
			p.Logger.Error(fmt.Sprintf(format, args...))
		}
	} else {
		p.logPluginf(pluginLogLevelError, "ERROR", format, args...)
	}
}

func (p *ClickHousePlugin) logDebugf(format string, args ...any) {
	if p.Logger != nil {
		if p.logLevel <= slog.LevelDebug {
			p.Logger.Debug(fmt.Sprintf(format, args...))
		}
	} else {
		p.logPluginf(pluginLogLevelDebug, "DEBUG", format, args...)
	}
}

func (p *ClickHousePlugin) logPluginf(level int, levelText string, format string, args ...any) {
	var instance unsafe.Pointer
	if p != nil {
		instance = p.logInstance
	}
	logPluginfForInstance(instance, level, levelText, format, args...)
}

func (p *ClickHousePlugin) throttledErrorf(key, format string, args ...any) {
	if p == nil || p.errorLog == nil {
		p.logErrorf(format, args...)
		return
	}
	p.errorLog.logf(p.logErrorf, key, format, args...)
}

func (p *ClickHousePlugin) releaseContextOnce() {
	if p == nil {
		return
	}
	p.contextReleaseOnce.Do(func() {
		if p.contextRelease != nil {
			p.contextRelease()
			p.contextRelease = nil
		}
	})
}

// fluentBitHandler implements slog.Handler to forward clickhouse-go logs to Fluent Bit.
type fluentBitHandler struct {
	plugin   *ClickHousePlugin
	level    slog.Level
	password string
	attrs    []slog.Attr
}

func (h *fluentBitHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *fluentBitHandler) Handle(ctx context.Context, r slog.Record) error {
	// Build message with attributes
	msg := r.Message

	// Collect all attributes
	attrs := make([]any, 0, len(h.attrs)*2+r.NumAttrs()*2)
	for _, a := range h.attrs {
		attrs = append(attrs, a.Key, a.Value.Any())
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a.Key, a.Value.Any())
		return true
	})

	// Format message with attributes if present
	if len(attrs) > 0 {
		msg = fmt.Sprintf("%s %v", msg, attrs)
	}

	// Redact secrets
	msg = redactSecrets(msg, h.password)

	// Map slog level to Fluent Bit level
	var level int
	var levelText string
	switch {
	case r.Level >= slog.LevelError:
		level = pluginLogLevelError
		levelText = "ERROR"
	case r.Level >= slog.LevelWarn:
		level = pluginLogLevelWarn
		levelText = "WARN"
	case r.Level >= slog.LevelInfo:
		level = pluginLogLevelInfo
		levelText = "INFO"
	default:
		level = pluginLogLevelDebug
		levelText = "DEBUG"
	}

	h.plugin.logPluginf(level, levelText, "%s", msg)
	return nil
}

func (h *fluentBitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)
	return &fluentBitHandler{
		plugin:   h.plugin,
		level:    h.level,
		password: h.password,
		attrs:    newAttrs,
	}
}

func (h *fluentBitHandler) WithGroup(name string) slog.Handler {
	// Fluent Bit logger doesn't support groups, just return the same handler
	return h
}

func (p *ClickHousePlugin) parseLogLevel(get func(string, ...string) string) error {
	val := get("LogLevel")
	if val == "" {
		p.logLevel = slog.LevelInfo // default
	} else {
		switch strings.ToLower(strings.TrimSpace(val)) {
		case "debug":
			p.logLevel = slog.LevelDebug
		case "info":
			p.logLevel = slog.LevelInfo
		case "warn":
			p.logLevel = slog.LevelWarn
		case "error":
			p.logLevel = slog.LevelError
		default:
			return configErr("LogLevel", fmt.Errorf("unsupported value %q, want: debug, info, warn, error", val))
		}
	}

	password := p.Opt.Auth.Password
	handler := &fluentBitHandler{
		plugin:   p,
		level:    p.logLevel,
		password: password,
	}
	p.Logger = slog.New(handler)
	p.Opt.Logger = p.Logger
	return nil
}

func redactSecrets(input string, password string) string {
	if password != "" {
		input = strings.ReplaceAll(input, password, "***")
		if escaped := url.QueryEscape(password); escaped != password {
			input = strings.ReplaceAll(input, escaped, "***")
		}
	}
	input = passwordPattern.ReplaceAllString(input, "$1$2***")
	return userinfoPattern.ReplaceAllString(input, "$1***@")
}

func (p *ClickHousePlugin) parsePoolConfig(get func(string, ...string) string, opt *clickhouse.Options) error {
	cfg := get("MaxIdleConns", "5")
	nv, err := strconv.Atoi(cfg)
	if err != nil {
		return configErr("MaxIdleConns", err)
	}
	opt.MaxIdleConns = nv

	cfg = get("MaxOpenConns", strconv.Itoa(opt.MaxIdleConns+5))
	nv, err = strconv.Atoi(cfg)
	if err != nil {
		return configErr("MaxOpenConns", err)
	}
	opt.MaxOpenConns = nv

	cfg = get("DialTimeout", "30s")
	dv, err := time.ParseDuration(cfg)
	if err != nil {
		return configErr("DialTimeout", err)
	}
	opt.DialTimeout = dv

	cfg = get("ConnMaxLifetime", "1h")
	dv, err = time.ParseDuration(cfg)
	if err != nil {
		return configErr("ConnMaxLifetime", err)
	}
	opt.ConnMaxLifetime = dv

	cfg = get("ReadTimeout", "300s")
	dv, err = time.ParseDuration(cfg)
	if err != nil {
		return configErr("ReadTimeout", err)
	}
	opt.ReadTimeout = dv

	cfg = get("WriteTimeout", opt.ReadTimeout.String())
	dv, err = time.ParseDuration(cfg)
	if err != nil {
		return configErr("WriteTimeout", err)
	}
	p.WriteTimeout = dv

	cfg = get("SemaphoreTimeout", "500ms")
	dv, err = time.ParseDuration(cfg)
	if err != nil {
		return configErr("SemaphoreTimeout", err)
	}
	p.SemaphoreTimeout = dv

	cfg = get("FreeBufOnConnRelease", "false")
	bv, err := strconv.ParseBool(cfg)
	if err != nil {
		return configErr("FreeBufOnConnRelease", err)
	}
	opt.FreeBufOnConnRelease = bv

	return nil
}

func (p *ClickHousePlugin) parseProtocol(get func(string, ...string) string, opt *clickhouse.Options) error {
	cfg := strings.TrimSpace(get("Protocol"))
	switch cfg {
	case "", "native":
		opt.Protocol = clickhouse.Native
	case "http":
		opt.Protocol = clickhouse.HTTP
	default:
		return configErr("Protocol", fmt.Errorf("unsupported value %q, want: native, http", cfg))
	}
	return nil
}

func (p *ClickHousePlugin) parseAddr(get func(string, ...string) string, opt *clickhouse.Options) error {
	cfg := strings.TrimSpace(get("Addr"))
	if cfg != "" {
		opt.Addr = strings.Split(cfg, ",")
		return nil
	}
	// Default address based on protocol, matching clickhouse-go setDefaults.
	switch opt.Protocol {
	case clickhouse.HTTP:
		opt.Addr = []string{"localhost:8123"}
	default:
		opt.Addr = []string{"localhost:9000"}
	}
	return nil
}

func (p *ClickHousePlugin) parseStrategy(get func(string, ...string) string, opt *clickhouse.Options) error {
	cfg := get("ConnOpenStrategy")
	switch cfg {
	case "", "in_order":
		opt.ConnOpenStrategy = clickhouse.ConnOpenInOrder
	case "round_robin":
		opt.ConnOpenStrategy = clickhouse.ConnOpenRoundRobin
	case "random":
		opt.ConnOpenStrategy = clickhouse.ConnOpenRandom
	default:
		return configErr("ConnOpenStrategy", fmt.Errorf("unsupported value %q, want: in_order, round_robin, random", cfg))
	}
	return nil
}

func (p *ClickHousePlugin) parseCompression(get func(string, ...string) string, opt *clickhouse.Options) error {
	cfg := get("Compression")
	switch cfg {
	case "", "none":
		return nil
	case "lz4":
		opt.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	case "gzip":
		opt.Compression = &clickhouse.Compression{Method: clickhouse.CompressionGZIP}
	case "deflate":
		opt.Compression = &clickhouse.Compression{Method: clickhouse.CompressionDeflate}
	case "zstd":
		opt.Compression = &clickhouse.Compression{Method: clickhouse.CompressionZSTD, Level: 3}
	case "br":
		opt.Compression = &clickhouse.Compression{Method: clickhouse.CompressionBrotli, Level: 3}
	default:
		return configErr("Compression", fmt.Errorf("unsupported value %q, want: none, lz4, gzip, deflate, zstd, br", cfg))
	}

	if cfg == "zstd" || cfg == "br" {
		lvl := get("CompressionLevel")
		if lvl != "" {
			nv, err := strconv.Atoi(lvl)
			if err != nil {
				return configErr("CompressionLevel", err)
			}
			opt.Compression.Level = nv
		}
	}
	return nil
}

func (p *ClickHousePlugin) parseBufferConfig(get func(string, ...string) string, opt *clickhouse.Options) error {
	cfg := get("BlockBufferSize", "2")
	uv, err := strconv.ParseUint(cfg, 10, 8)
	if err != nil {
		return configErr("BlockBufferSize", err)
	}
	opt.BlockBufferSize = uint8(uv)

	cfg = get("MaxCompressionBuffer", "10485760")
	nv, err := strconv.Atoi(cfg)
	if err != nil {
		return configErr("MaxCompressionBuffer", err)
	}
	opt.MaxCompressionBuffer = nv

	return nil
}

func (p *ClickHousePlugin) parseClientInfo(get func(string, ...string) string, opt *clickhouse.Options) error {
	cfg := get("ClientInfo")
	if cfg != "" {
		for _, v := range strings.Split(cfg, ",") {
			infos := strings.Split(strings.TrimSpace(v), "/")
			if len(infos) != 2 {
				return configErr("ClientInfo", fmt.Errorf("value %q must be in name/version format", v))
			}
			opt.ClientInfo.Products = append(opt.ClientInfo.Products, struct {
				Name    string
				Version string
			}{
				Name:    strings.TrimSpace(infos[0]),
				Version: strings.TrimSpace(infos[1]),
			})
		}
	}

	cfg = get("ClientComment")
	if cfg != "" {
		for _, v := range strings.Split(cfg, ",") {
			if comment := strings.TrimSpace(v); comment != "" {
				opt.ClientInfo.Comment = append(opt.ClientInfo.Comment, comment)
			}
		}
	}
	return nil
}

func (p *ClickHousePlugin) parseHTTPConfig(get func(string, ...string) string, opt *clickhouse.Options) error {
	cfg := strings.TrimSpace(get("HTTPProxyURL"))
	if cfg != "" {
		proxyURL, err := url.Parse(cfg)
		if err != nil {
			return configErr("HTTPProxyURL", err)
		}
		opt.HTTPProxyURL = proxyURL
	}

	cfg = strings.TrimSpace(get("HttpUrlPath"))
	if cfg != "" {
		opt.HttpUrlPath = cfg
	}

	cfg = strings.TrimSpace(get("HttpHeaders"))
	if cfg != "" {
		headers, err := parseKeyValueConfig(cfg)
		if err != nil {
			return configErr("HttpHeaders", err)
		}
		if err := validateHTTPHeaders(headers); err != nil {
			return configErr("HttpHeaders", err)
		}
		opt.HttpHeaders = headers
	}

	cfg = strings.TrimSpace(get("HttpMaxConnsPerHost"))
	if cfg != "" {
		nv, err := strconv.Atoi(cfg)
		if err != nil {
			return configErr("HttpMaxConnsPerHost", err)
		}
		opt.HttpMaxConnsPerHost = nv
	}

	return nil
}

func (p *ClickHousePlugin) parseTLS(get func(string, ...string) string, opt *clickhouse.Options) error {
	cfg := strings.TrimSpace(get("TLS", "false"))
	enabled, err := strconv.ParseBool(cfg)
	if err != nil {
		return configErr("TLS", err)
	}
	if !enabled {
		return nil
	}

	tlsCfg := &tls.Config{
		ServerName: strings.TrimSpace(get("TLSServerName")),
	}

	skipVerify := strings.TrimSpace(get("TLSInsecureSkipVerify"))
	if skipVerify != "" {
		enabled, err := strconv.ParseBool(skipVerify)
		if err != nil {
			return configErr("TLSInsecureSkipVerify", err)
		}
		tlsCfg.InsecureSkipVerify = enabled
	}

	if caPath := strings.TrimSpace(get("TLSCACert")); caPath != "" {
		pemData, err := os.ReadFile(caPath)
		if err != nil {
			return configErr("TLSCACert", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemData) {
			return configErr("TLSCACert", errors.New("invalid PEM"))
		}
		tlsCfg.RootCAs = pool
	}

	certPath := strings.TrimSpace(get("TLSCert"))
	keyPath := strings.TrimSpace(get("TLSKey"))
	switch {
	case certPath == "" && keyPath == "":
	case certPath == "" || keyPath == "":
		return configErr("TLSCert", errors.New("TLSCert and TLSKey must be set together"))
	default:
		pair, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return configErr("TLSCert", err)
		}
		tlsCfg.Certificates = []tls.Certificate{pair}
	}

	opt.TLS = tlsCfg
	return nil
}

func (p *ClickHousePlugin) parseSettings(get func(string, ...string) string, opt *clickhouse.Options) error {
	cfg := strings.TrimSpace(get("Settings"))
	if cfg == "" {
		return nil
	}

	values, err := parseKeyValueConfig(cfg)
	if err != nil {
		return configErr("Settings", err)
	}

	if opt.Settings == nil {
		opt.Settings = clickhouse.Settings{}
	}
	for key, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if _, locked := protectedSettings[normalized]; locked {
			return configErr("Settings", fmt.Errorf("setting %q is managed by plugin and cannot be overridden", key))
		}
		parsed := parseSettingValue(value)
		if err := validateSettingLimit(key, parsed); err != nil {
			return configErr("Settings", err)
		}
		opt.Settings[key] = parsed
	}
	return nil
}

func (p *ClickHousePlugin) parseMetricsConfig(get func(string, ...string) string) error {
	cfg := strings.TrimSpace(get("MetricsAddr"))
	if cfg == "" {
		return nil
	}
	if err := validateMetricsAddr(cfg); err != nil {
		return configErr("MetricsAddr", err)
	}
	p.MetricsAddr = cfg
	return nil
}

func (p *ClickHousePlugin) parseJWT(get func(string, ...string) string) error {
	jwt := strings.TrimSpace(get("JWT"))
	if jwt == "" {
		return nil
	}
	p.Opt.GetJWT = func(ctx context.Context) (string, error) {
		return jwt, nil
	}
	return nil
}

func parseKeyValueConfig(cfg string) (map[string]string, error) {
	result := make(map[string]string)
	for _, item := range strings.Split(cfg, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, fmt.Errorf("empty item")
		}
		kv := strings.SplitN(item, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("value %q must be in key=value format", item)
		}
		key := strings.TrimSpace(kv[0])
		if key == "" {
			return nil, fmt.Errorf("value %q has empty key", item)
		}
		result[key] = strings.TrimSpace(kv[1])
	}
	return result, nil
}

func validateHTTPHeaders(headers map[string]string) error {
	for key := range headers {
		if _, forbidden := forbiddenHTTPHeaders[strings.ToLower(strings.TrimSpace(key))]; forbidden {
			return fmt.Errorf("header %q is not allowed", key)
		}
	}
	return nil
}

func validateSettingLimit(key string, value any) error {
	limit, ok := boundedSettings[strings.ToLower(strings.TrimSpace(key))]
	if !ok {
		return nil
	}

	iv, ok := value.(int64)
	if !ok {
		return fmt.Errorf("setting %q must be an integer <= %d", key, limit)
	}
	if iv < 0 {
		return fmt.Errorf("setting %q must be >= 0", key)
	}
	if iv > limit {
		return fmt.Errorf("setting %q must be <= %d", key, limit)
	}
	return nil
}

func parseSettingValue(raw string) any {
	lower := strings.ToLower(raw)
	if lower == "true" || lower == "false" {
		if b, err := strconv.ParseBool(raw); err == nil {
			return b
		}
	}

	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil && strings.ContainsAny(raw, ".eE") {
		return f
	}
	return raw
}

// Init opens the ClickHouse connection and prepares the batch insert statement.
func (p *ClickHousePlugin) Init() (err error) {
	if detectDaemonMode() {
		return fmt.Errorf("clickhouse: daemon mode is not supported for Go/CGo plugin; run fluent-bit in foreground")
	}

	conn, err := openConnector(p.Opt)
	if err != nil {
		return fmt.Errorf("clickhouse: open connection: %w", err)
	}
	p.Conn = conn
	defer func() {
		if err == nil {
			return
		}
		if p.rootCancel != nil {
			p.rootCancel()
			p.rootCancel = nil
			p.rootCtx = nil
		}
		if p.Conn != nil {
			_ = p.Conn.Close()
			p.Conn = nil
		}
	}()

	if err = p.Ping(); err != nil {
		return err
	}

	if columns := p.insertColumns(); len(columns) > 0 {
		quoted := make([]string, len(columns))
		for i, c := range columns {
			quoted[i] = quoteIdentifier(c)
		}
		p.BatchStmt = "INSERT INTO " + quoteIdentifier(p.Opt.Auth.Database) + "." + quoteIdentifier(p.TableName) + "(" + strings.Join(quoted, ",") + ")"
	}
	if p.WriteTimeout <= 0 {
		p.WriteTimeout = p.Opt.ReadTimeout
	}
	if p.WriteTimeout <= 0 {
		p.WriteTimeout = 5 * time.Minute
	}
	if p.Opt.MaxOpenConns > 0 {
		p.insertSem = make(chan struct{}, p.Opt.MaxOpenConns)
	}
	p.rootCtx, p.rootCancel = context.WithCancel(context.Background())
	if err = p.initMetrics(); err != nil {
		return fmt.Errorf("clickhouse: init metrics: %w", err)
	}

	return nil
}

// Ping checks the ClickHouse connection health with a timeout.
func (p *ClickHousePlugin) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.Opt.DialTimeout)
	defer cancel()
	if err := p.Conn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse: ping: %w", err)
	}
	return nil
}

// BatchInsert parses the tag, decodes records, and performs a batch insert.
func (p *ClickHousePlugin) BatchInsert(tag string, dec *output.FLBDecoder) int {
	seenRecord := false
	next := func() (any, map[interface{}]interface{}, bool, int) {
		ret, ts, record := output.GetRecord(dec)
		if ret == 0 {
			seenRecord = true
			return ts, record, false, output.FLB_OK
		}
		if ret == -1 {
			if seenRecord {
				return nil, nil, true, output.FLB_OK
			}
			return nil, nil, false, output.FLB_RETRY
		}
		return nil, nil, false, output.FLB_RETRY
	}
	return p.batchInsertWithNext(tag, next)
}

// BatchInsertPayload decodes records from raw msgpack payload and performs a batch insert.
func (p *ClickHousePlugin) BatchInsertPayload(tag string, payload []byte) int {
	next := nextRecordFromPayload(payload)
	return p.batchInsertWithNext(tag, next)
}

func (p *ClickHousePlugin) batchInsertWithNext(tag string, next func() (any, map[interface{}]interface{}, bool, int)) (code int) {
	started := time.Now()
	records := 0
	code = output.FLB_ERROR
	defer func() {
		if recovered := recover(); recovered != nil {
			p.logErrorf("batch insert panic recovered: %v\n%s", recovered, debug.Stack())
			code = output.FLB_ERROR
		}
	}()
	p.mu.RLock()
	p.inflight.Add(1)
	metrics := p.metrics
	closed := p.closed
	conn := p.Conn
	sem := p.insertSem
	baseCtx := p.rootCtx
	writeTimeout := p.WriteTimeout
	semTimeout := p.SemaphoreTimeout
	p.mu.RUnlock()
	defer p.inflight.Done()
	defer func() {
		p.observeFlush(metrics, code, records, started)
	}()
	if metrics != nil && metrics.flushInflight != nil {
		metrics.flushInflight.Inc()
		defer metrics.flushInflight.Dec()
	}

	if closed || conn == nil {
		p.logErrorf("batch insert rejected: plugin is closed")
		return output.FLB_ERROR
	}
	if sem != nil {
		if semTimeout <= 0 {
			semTimeout = 500 * time.Millisecond
		}
		timer := time.NewTimer(semTimeout)
		select {
		case sem <- struct{}{}:
			if !timer.Stop() {
				<-timer.C
			}
			defer func() { <-sem }()
		case <-timer.C:
			return output.FLB_RETRY
		}
	}
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	if writeTimeout <= 0 {
		writeTimeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(baseCtx, writeTimeout)
	defer cancel()

	code, records = p.insertBatch(ctx, conn, next, metrics)
	return code
}

func (p *ClickHousePlugin) insertBatch(ctx context.Context, conn Connector, next func() (any, map[interface{}]interface{}, bool, int), metrics *pluginMetrics) (int, int) {
	totalRows := 0
	for {
		pendingRows, dedupToken, eof, code, decodeDroppedRows := p.decodeRowsChunk(next)
		if decodeDroppedRows > 0 {
			observeDroppedRows(metrics, output.FLB_ERROR, "parse", decodeDroppedRows)
		}
		if code != output.FLB_OK {
			observeDroppedRows(metrics, code, "decode", len(pendingRows))
			return code, totalRows
		}
		if len(pendingRows) > 0 {
			sendCode := p.sendRows(ctx, conn, dedupToken, pendingRows, metrics)
			if sendCode != output.FLB_OK {
				return sendCode, totalRows
			}
			totalRows += len(pendingRows)
		}
		if eof {
			return output.FLB_OK, totalRows
		}
	}
}

func (p *ClickHousePlugin) sendRows(ctx context.Context, conn Connector, dedupToken string, pendingRows [][]any, metrics *pluginMetrics) int {
	prepareCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplication_token": dedupToken,
	}))
	batch, err := conn.PrepareBatch(prepareCtx, p.BatchStmt)
	if err != nil {
		p.throttledErrorf("prepare_batch", "prepare batch failed: table=%s err=%v", p.tableRef(), err)
		code := classifyInsertError(err)
		observeDroppedRows(metrics, code, "prepare", len(pendingRows))
		return code
	}
	defer func() {
		if err := batch.Close(); err != nil {
			p.throttledErrorf("batch_close", "batch close failed: table=%s err=%v", p.tableRef(), err)
		}
	}()

	for _, columns := range pendingRows {
		if err := batch.Append(columns...); err != nil {
			p.throttledErrorf("batch_append", "batch append failed: table=%s err=%v", p.tableRef(), err)
			code := classifyInsertError(err)
			observeDroppedRows(metrics, code, "append", len(pendingRows))
			return code
		}
	}
	if err := batch.Send(); err != nil {
		p.throttledErrorf("batch_send", "batch send failed: table=%s err=%v", p.tableRef(), err)
		code := classifyInsertError(err)
		observeDroppedRows(metrics, code, "send", len(pendingRows))
		return code
	}
	if metrics != nil && metrics.batchRows != nil {
		metrics.batchRows.Observe(float64(len(pendingRows)))
	}
	return output.FLB_OK
}

func (p *ClickHousePlugin) decodeRowsChunk(next func() (any, map[interface{}]interface{}, bool, int)) ([][]any, string, bool, int, int) {
	pendingRows := make([][]any, 0, 32)
	hasher := xxhash.New()
	droppedRows := 0
	for len(pendingRows) < maxRowsPerChunk {
		_, record, eof, code := next()
		if eof {
			if len(pendingRows) == 0 {
				return nil, "", true, output.FLB_OK, droppedRows
			}
			return pendingRows, dedupToken(hasher), true, output.FLB_OK, droppedRows
		}
		if code != output.FLB_OK {
			p.throttledErrorf("decode_corrupt", "record decode failed with code=%d", code)
			return nil, "", false, code, droppedRows + len(pendingRows) + 1
		}
		normalized := normalizeRecord(record)

		columns := make([]any, 0, len(p.Columns))

		skipRow := false
		for idx, colName := range p.Columns {

			value, ok := normalized[colName]
			if !ok {
				columns = append(columns, resolveDefaultValue(p.Defaults[idx]))
				continue
			}
			isNullable := idx < len(p.ColNullable) && p.ColNullable[idx]
			if value == nil && isNullable {
				columns = append(columns, nil)
				continue
			}
			val, err := coerceColumnValue(p.ColType[idx], value)
			if err != nil {
				p.throttledErrorf("column_parse", "column parse failed: column=%s type=%T, skipping record", colName, value)
				if p.logLevel <= slog.LevelDebug {
					p.throttledErrorf("column_parse_debug", "column parse failed (debug): column=%s err=%v", colName, err)
				}
				skipRow = true
				break
			}
			columns = append(columns, val)
		}
		if skipRow {
			droppedRows++
			continue
		}
		addDedupRow(hasher, columns)
		pendingRows = append(pendingRows, columns)
	}
	return pendingRows, dedupToken(hasher), false, output.FLB_OK, droppedRows
}

func nextRecordFromPayload(payload []byte) func() (any, map[interface{}]interface{}, bool, int) {
	handle := new(codec.MsgpackHandle)
	handle.SetBytesExt(reflect.TypeOf(output.FLBTime{}), 0, &output.FLBTime{})
	decoder := codec.NewDecoderBytes(payload, handle)

	return func() (any, map[interface{}]interface{}, bool, int) {
		var envelope any
		if err := decoder.Decode(&envelope); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil, true, output.FLB_OK
			}
			return nil, nil, false, output.FLB_RETRY
		}

		slice, ok := envelope.([]any)
		if !ok || len(slice) != 2 {
			return nil, nil, false, output.FLB_ERROR
		}
		ts, ok := extractTimestamp(slice[0])
		if !ok {
			return nil, nil, false, output.FLB_ERROR
		}

		record, ok := slice[1].(map[interface{}]interface{})
		if !ok {
			return nil, nil, false, output.FLB_ERROR
		}
		return ts, record, false, output.FLB_OK
	}
}

func extractTimestamp(raw any) (any, bool) {
	switch v := raw.(type) {
	case output.FLBTime:
		return v, true
	case uint64:
		return v, true
	case []any:
		if len(v) < 2 {
			return nil, false
		}
		switch ts := v[0].(type) {
		case output.FLBTime:
			return ts, true
		case uint64:
			return ts, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

func (p *ClickHousePlugin) insertColumns() []string {
	columns := make([]string, 0, len(p.Columns))
	for _, v := range p.Columns {
		columns = append(columns, v)
	}
	return columns
}

func (p *ClickHousePlugin) tableRef() string {
	return p.Opt.Auth.Database + "." + p.TableName
}

func normalizeRecord(record map[interface{}]interface{}) map[string]any {
	out := make(map[string]any, len(record))
	for key, value := range record {
		switch k := key.(type) {
		case string:
			out[k] = value
		case []byte:
			out[string(k)] = value
		default:
			out[fmt.Sprint(k)] = value
		}
	}
	return out
}

func addDedupRow(h hash.Hash, row []any) {
	for _, v := range row {
		_, _ = h.Write([]byte(stringifyValue(v)))
		_, _ = h.Write([]byte{0})
	}
	_, _ = h.Write([]byte{'\n'})
}

func dedupToken(h hash.Hash64) string {
	var out [8]byte
	binary.BigEndian.PutUint64(out[:], h.Sum64())
	return hex.EncodeToString(out[:])
}

func coerceColumnValue(parser ColumnParser, raw any) (any, error) {
	normalized, err := normalizeColumnRaw(raw)
	if err != nil {
		return nil, err
	}
	return parser(normalized)
}

func normalizeColumnRaw(raw any) (string, error) {
	switch v := raw.(type) {
	case nil:
		return "", nil
	case []byte:
		return string(v), nil
	case string:
		return v, nil
	}

	rv := reflect.ValueOf(raw)
	if rv.IsValid() {
		switch rv.Kind() {
		case reflect.Slice:
			// Keep []byte behavior unchanged and only JSON-normalize non-byte slices.
			if rv.Type().Elem().Kind() == reflect.Uint8 {
				return stringifyValue(raw), nil
			}
			fallthrough
		case reflect.Array, reflect.Map, reflect.Struct:
			normalized := normalizeToJSONCompatible(raw)
			buf, err := json.Marshal(normalized)
			if err != nil {
				return "", fmt.Errorf("normalize json value failed: %w", err)
			}
			return string(buf), nil
		}
	}

	return stringifyValue(raw), nil
}

func normalizeToJSONCompatible(v any) any {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case map[interface{}]interface{}:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[fmt.Sprint(k)] = normalizeToJSONCompatible(val)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = normalizeToJSONCompatible(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = normalizeToJSONCompatible(val)
		}
		return out
	default:
		rv := reflect.ValueOf(v)
		if !rv.IsValid() {
			return v
		}
		if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() != reflect.Uint8 {
			out := make([]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				out[i] = normalizeToJSONCompatible(rv.Index(i).Interface())
			}
			return out
		}
		return v
	}
}

func stringifyValue(raw any) string {
	switch v := raw.(type) {
	case nil:
		return ""
	case []byte:
		return string(v)
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case bool:
		return strconv.FormatBool(v)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

func resolveDefaultValue(def any) any {
	if fn, ok := def.(func() any); ok {
		return fn()
	}
	return def
}

func classifyInsertError(err error) int {
	if err == nil {
		return output.FLB_OK
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return output.FLB_RETRY
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return output.FLB_RETRY
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return output.FLB_RETRY
		}
		return output.FLB_ERROR
	}

	var chErr *clickhouse.Exception
	if errors.As(err, &chErr) {
		if _, ok := retryableCHCodes[chErr.Code]; ok {
			return output.FLB_RETRY
		}
		return output.FLB_ERROR
	}

	return output.FLB_ERROR
}

func validateMetricsAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return fmt.Errorf("MetricsAddr must bind loopback (e.g. 127.0.0.1:9090); got %q", addr)
	}
	if host != "" && strings.Contains(host, " ") {
		return fmt.Errorf("invalid host %q", host)
	}
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("MetricsAddr host %q must be localhost or an IP", host)
		}
		if !ip.IsLoopback() && os.Getenv("CLICKHOUSE_ALLOW_PUBLIC_METRICS") != "1" {
			return fmt.Errorf("non-loopback MetricsAddr %q requires CLICKHOUSE_ALLOW_PUBLIC_METRICS=1", host)
		}
	}
	nv, err := strconv.Atoi(port)
	if err != nil {
		return err
	}
	if nv < 0 || nv > 65535 {
		return fmt.Errorf("port out of range: %d", nv)
	}
	return nil
}

func (p *ClickHousePlugin) initMetrics() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.MetricsAddr == "" || p.metricsServer != nil {
		return nil
	}

	registry := prometheus.NewRegistry()
	metrics := &pluginMetrics{
		registry: registry,
		flushTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clickhouse_flush_total",
			Help: "Total number of ClickHouse flush attempts by result.",
		}, []string{"status"}),
		flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "clickhouse_flush_duration_seconds",
			Help:    "Duration of ClickHouse flush attempts.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120},
		}),
		flushInflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "clickhouse_flush_inflight",
			Help: "Number of in-flight flush operations.",
		}),
		batchRows: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "clickhouse_batch_rows",
			Help:    "Rows per successful ClickHouse batch.",
			Buckets: []float64{1, 10, 100, 500, 1000, 5000, 10000, 50000},
		}),
		flushErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clickhouse_flush_errors_total",
			Help: "Total number of failed ClickHouse flush attempts by result.",
		}, []string{"status"}),
		recordsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "clickhouse_records_total",
			Help: "Total number of successfully flushed ClickHouse records.",
		}),
		droppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clickhouse_records_dropped_total",
			Help: "Total number of dropped ClickHouse records by flush result and stage.",
		}, []string{"status", "stage"}),
		pluginInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "plugin_info",
			Help: "Build information for the ClickHouse output plugin.",
		}, []string{"version", "commit"}),
	}
	metrics.pluginInfo.WithLabelValues(pluginVersion, pluginCommit).Set(1)
	registry.MustRegister(
		metrics.flushTotal,
		metrics.flushDuration,
		metrics.flushInflight,
		metrics.batchRows,
		metrics.flushErrors,
		metrics.recordsTotal,
		metrics.droppedTotal,
		metrics.pluginInfo,
	)

	listener, err := net.Listen("tcp", p.MetricsAddr)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	p.metrics = metrics
	p.metricsListener = listener
	p.metricsServer = server

	go func(addr string) {
		defer func() {
			if recovered := recover(); recovered != nil {
				p.logErrorf("metrics server panicked: %v", recovered)
			}
		}()
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.logErrorf("metrics server failed: addr=%s err=%v", addr, err)
		}
	}(listener.Addr().String())

	return nil
}

func (p *ClickHousePlugin) stopMetrics() {
	p.mu.Lock()
	server, listener := p.takeMetricsLocked()
	p.mu.Unlock()
	shutdownMetrics(server, listener)
}

func (p *ClickHousePlugin) observeFlush(metrics *pluginMetrics, code int, records int, started time.Time) {
	if metrics == nil {
		return
	}

	status := "error"
	switch code {
	case output.FLB_OK:
		status = "ok"
	case output.FLB_RETRY:
		status = "retry"
	}

	metrics.flushTotal.WithLabelValues(status).Inc()
	metrics.flushDuration.Observe(time.Since(started).Seconds())
	if code == output.FLB_OK {
		metrics.recordsTotal.Add(float64(records))
		return
	}
	metrics.flushErrors.WithLabelValues(status).Inc()
}

// Exit closes the ClickHouse connection and releases resources.
func (p *ClickHousePlugin) Exit() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	rootCancel := p.rootCancel
	p.rootCancel = nil
	p.rootCtx = nil
	writeTimeout := p.WriteTimeout
	conn := p.Conn
	p.Conn = nil
	server, listener := p.takeMetricsLocked()
	p.mu.Unlock()
	if rootCancel != nil {
		rootCancel()
	}

	waitDone := make(chan struct{})
	go func() {
		p.inflight.Wait()
		close(waitDone)
	}()
	if writeTimeout <= 0 {
		writeTimeout = 5 * time.Minute
	}
	deadline := writeTimeout + time.Second
	select {
	case <-waitDone:
	case <-time.After(deadline):
		p.logWarnf("exit: in-flight flush did not finish in %v, forcing close", deadline)
	}

	shutdownMetrics(server, listener)
	if conn != nil {
		if err := conn.Close(); err != nil {
			p.logErrorf("close connection failed: %v", err)
		}
	}
}

func droppedStatus(code int) string {
	switch code {
	case output.FLB_RETRY:
		return "retry"
	default:
		return "error"
	}
}

func observeDroppedRows(metrics *pluginMetrics, code int, stage string, rows int) {
	if rows <= 0 || metrics == nil || metrics.droppedTotal == nil {
		return
	}
	metrics.droppedTotal.WithLabelValues(droppedStatus(code), stage).Add(float64(rows))
}

func (p *ClickHousePlugin) takeMetricsLocked() (*http.Server, net.Listener) {
	server := p.metricsServer
	listener := p.metricsListener
	p.metricsServer = nil
	p.metricsListener = nil
	p.metrics = nil
	return server, listener
}

func shutdownMetrics(server *http.Server, listener net.Listener) {
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}
	if listener != nil {
		_ = listener.Close()
	}
}
