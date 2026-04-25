package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/cespare/xxhash/v2"
)

// makeConfig builds a config getter from a map with optional defaults.
func makeConfig(m map[string]string) func(string, ...string) string {
	return func(key string, defaults ...string) string {
		if v, ok := m[key]; ok {
			return v
		}
		if len(defaults) > 0 {
			return defaults[0]
		}
		return ""
	}
}

// baseConfig returns minimum required config for a valid plugin.
func baseConfig() map[string]string {
	return map[string]string{
		"Database": "default",
		"Table":    "test_table",
		"Columns":  "id|Int64,name|String",
	}
}

func TestNewPlugin_RequiredFields(t *testing.T) {
	tests := []struct {
		name      string
		omitField string
		wantErr   error
	}{
		{name: "missing Database", omitField: "Database", wantErr: ErrNoDatabase},
		{name: "missing Table", omitField: "Table", wantErr: ErrNoTable},
		{name: "missing Columns", omitField: "Columns", wantErr: ErrNoColumns},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			delete(cfg, tt.omitField)
			_, err := NewPlugin(makeConfig(cfg))
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("got error %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewPlugin_DefaultValues(t *testing.T) {
	cfg := baseConfig()
	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("NewPlugin() unexpected error: %v", err)
	}
	// Verify default Addr matches clickhouse-go defaults for Native protocol.
	if len(p.Opt.Addr) != 1 || p.Opt.Addr[0] != "localhost:9000" {
		t.Errorf("Addr = %v, want [localhost:9000] (clickhouse-go default for native)", p.Opt.Addr)
	}
	// Verify default Protocol is Native.
	if p.Opt.Protocol != clickhouse.Native {
		t.Errorf("Protocol = %v, want Native (clickhouse-go default)", p.Opt.Protocol)
	}
	// Verify default Username matches clickhouse-go defaults.
	if p.Opt.Auth.Username != "default" {
		t.Errorf("Username = %q, want %q (clickhouse-go default)", p.Opt.Auth.Username, "default")
	}
	if p.Opt.Auth.Database != "default" {
		t.Errorf("Database = %q, want %q", p.Opt.Auth.Database, "default")
	}
}

func TestNewPlugin_Protocol(t *testing.T) {
	tests := []struct {
		name         string
		protocol     string
		wantProtocol clickhouse.Protocol
		wantErr      bool
	}{
		{name: "empty defaults to native", protocol: "", wantProtocol: clickhouse.Native},
		{name: "native", protocol: "native", wantProtocol: clickhouse.Native},
		{name: "http", protocol: "http", wantProtocol: clickhouse.HTTP},
		{name: "invalid", protocol: "invalid", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			if tt.protocol != "" {
				cfg["Protocol"] = tt.protocol
			}
			p, err := NewPlugin(makeConfig(cfg))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if p.Opt.Protocol != tt.wantProtocol {
				t.Errorf("Protocol = %v, want %v", p.Opt.Protocol, tt.wantProtocol)
			}
		})
	}
}

func TestNewPlugin_AddrDefaultHTTP(t *testing.T) {
	cfg := baseConfig()
	cfg["Protocol"] = "http"
	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("NewPlugin() unexpected error: %v", err)
	}
	// Verify default Addr for HTTP protocol.
	if len(p.Opt.Addr) != 1 || p.Opt.Addr[0] != "localhost:8123" {
		t.Errorf("Addr = %v, want [localhost:8123] (clickhouse-go default for http)", p.Opt.Addr)
	}
	if p.Opt.Protocol != clickhouse.HTTP {
		t.Errorf("Protocol = %v, want HTTP", p.Opt.Protocol)
	}
}

func TestNewPlugin_ExplicitAddr(t *testing.T) {
	cfg := baseConfig()
	cfg["Addr"] = "127.0.0.1:9000"
	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("NewPlugin() unexpected error: %v", err)
	}
	if len(p.Opt.Addr) != 1 || p.Opt.Addr[0] != "127.0.0.1:9000" {
		t.Errorf("Addr = %v, want [127.0.0.1:9000]", p.Opt.Addr)
	}
}

func TestNewPlugin_ExplicitUsername(t *testing.T) {
	cfg := baseConfig()
	cfg["UserName"] = "custom_user"
	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("NewPlugin() unexpected error: %v", err)
	}
	if p.Opt.Auth.Username != "custom_user" {
		t.Errorf("Username = %q, want %q", p.Opt.Auth.Username, "custom_user")
	}
}

func TestNewPlugin_MultipleAddresses(t *testing.T) {
	cfg := baseConfig()
	cfg["Addr"] = "host1:9000,host2:9000,host3:9000"

	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("NewPlugin() unexpected error: %v", err)
	}
	if len(p.Opt.Addr) != 3 {
		t.Errorf("Addr count = %d, want 3", len(p.Opt.Addr))
	}
}

func TestNewPlugin_ConnOpenStrategy(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		want   clickhouse.ConnOpenStrategy
		hasErr bool
	}{
		{name: "in_order", value: "in_order", want: clickhouse.ConnOpenInOrder},
		{name: "round_robin", value: "round_robin", want: clickhouse.ConnOpenRoundRobin},
		{name: "random", value: "random", want: clickhouse.ConnOpenRandom},
		{name: "empty defaults to in_order", value: "", want: clickhouse.ConnOpenInOrder},
		{name: "invalid", value: "invalid_strategy", hasErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			if tt.value != "" {
				cfg["ConnOpenStrategy"] = tt.value
			}
			p, err := NewPlugin(makeConfig(cfg))
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				var cfgErr *ConfigError
				if !errors.As(err, &cfgErr) {
					t.Errorf("expected ConfigError, got %T", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if p.Opt.ConnOpenStrategy != tt.want {
				t.Errorf("ConnOpenStrategy = %v, want %v", p.Opt.ConnOpenStrategy, tt.want)
			}
		})
	}
}

func TestNewPlugin_Compression(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		method clickhouse.CompressionMethod
		hasErr bool
	}{
		{name: "none", value: "none"},
		{name: "empty", value: ""},
		{name: "lz4", value: "lz4", method: clickhouse.CompressionLZ4},
		{name: "gzip", value: "gzip", method: clickhouse.CompressionGZIP},
		{name: "deflate", value: "deflate", method: clickhouse.CompressionDeflate},
		{name: "zstd", value: "zstd", method: clickhouse.CompressionZSTD},
		{name: "br", value: "br", method: clickhouse.CompressionBrotli},
		{name: "invalid", value: "snappy", hasErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			if tt.value != "" {
				cfg["Compression"] = tt.value
			}
			p, err := NewPlugin(makeConfig(cfg))
			if tt.hasErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.value == "none" || tt.value == "" {
				if p.Opt.Compression != nil {
					t.Error("expected nil Compression for none")
				}
				return
			}
			if p.Opt.Compression == nil {
				t.Fatal("expected non-nil Compression")
			}
			if p.Opt.Compression.Method != tt.method {
				t.Errorf("Compression.Method = %v, want %v", p.Opt.Compression.Method, tt.method)
			}
		})
	}
}

func TestNewPlugin_CompressionLevel(t *testing.T) {
	cfg := baseConfig()
	cfg["Compression"] = "zstd"
	cfg["CompressionLevel"] = "5"

	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Opt.Compression.Level != 5 {
		t.Errorf("CompressionLevel = %d, want 5", p.Opt.Compression.Level)
	}
}

func TestNewPlugin_PoolConfig(t *testing.T) {
	cfg := baseConfig()
	cfg["MaxOpenConns"] = "10"
	cfg["MaxIdleConns"] = "3"
	cfg["DialTimeout"] = "5s"
	cfg["ReadTimeout"] = "45s"
	cfg["WriteTimeout"] = "35s"
	cfg["ConnMaxLifetime"] = "30m"

	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Opt.MaxOpenConns != 10 {
		t.Errorf("MaxOpenConns = %d, want 10", p.Opt.MaxOpenConns)
	}
	if p.Opt.MaxIdleConns != 3 {
		t.Errorf("MaxIdleConns = %d, want 3", p.Opt.MaxIdleConns)
	}
	if p.Opt.DialTimeout != 5*time.Second {
		t.Errorf("DialTimeout = %v, want 5s", p.Opt.DialTimeout)
	}
	if p.Opt.ReadTimeout != 45*time.Second {
		t.Errorf("ReadTimeout = %v, want 45s", p.Opt.ReadTimeout)
	}
	if p.WriteTimeout != 35*time.Second {
		t.Errorf("WriteTimeout = %v, want 35s", p.WriteTimeout)
	}
	if p.Opt.ConnMaxLifetime != 30*time.Minute {
		t.Errorf("ConnMaxLifetime = %v, want 30m", p.Opt.ConnMaxLifetime)
	}
}

func TestNewPlugin_InvalidPoolConfig(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "bad MaxOpenConns", key: "MaxOpenConns", value: "abc"},
		{name: "bad MaxIdleConns", key: "MaxIdleConns", value: "abc"},
		{name: "bad DialTimeout", key: "DialTimeout", value: "not_a_duration"},
		{name: "bad ReadTimeout", key: "ReadTimeout", value: "not_a_duration"},
		{name: "bad WriteTimeout", key: "WriteTimeout", value: "not_a_duration"},
		{name: "bad ConnMaxLifetime", key: "ConnMaxLifetime", value: "xyz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg[tt.key] = tt.value
			_, err := NewPlugin(makeConfig(cfg))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			var cfgErr *ConfigError
			if !errors.As(err, &cfgErr) {
				t.Errorf("expected ConfigError, got %T: %v", err, err)
			}
		})
	}
}

func TestNewPlugin_DefaultOptionsSync(t *testing.T) {
	cfg := baseConfig()
	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.Opt.MaxIdleConns != 5 {
		t.Errorf("MaxIdleConns = %d, want 5", p.Opt.MaxIdleConns)
	}
	if p.Opt.MaxOpenConns != 10 {
		t.Errorf("MaxOpenConns = %d, want 10", p.Opt.MaxOpenConns)
	}
	if p.Opt.DialTimeout != 30*time.Second {
		t.Errorf("DialTimeout = %v, want 30s", p.Opt.DialTimeout)
	}
	if p.Opt.ReadTimeout != 5*time.Minute {
		t.Errorf("ReadTimeout = %v, want 5m", p.Opt.ReadTimeout)
	}
	if p.WriteTimeout != 5*time.Minute {
		t.Errorf("WriteTimeout = %v, want 5m", p.WriteTimeout)
	}
	if p.Opt.ConnMaxLifetime != time.Hour {
		t.Errorf("ConnMaxLifetime = %v, want 1h", p.Opt.ConnMaxLifetime)
	}
	if p.Opt.BlockBufferSize != 2 {
		t.Errorf("BlockBufferSize = %d, want 2", p.Opt.BlockBufferSize)
	}
	if p.Opt.MaxCompressionBuffer != 10485760 {
		t.Errorf("MaxCompressionBuffer = %d, want 10485760", p.Opt.MaxCompressionBuffer)
	}
}

func TestNewPlugin_MaxOpenConnsDefaultDerivedFromIdle(t *testing.T) {
	cfg := baseConfig()
	cfg["MaxIdleConns"] = "7"

	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Opt.MaxOpenConns != 12 {
		t.Errorf("MaxOpenConns = %d, want 12", p.Opt.MaxOpenConns)
	}
}

func TestNewPlugin_HTTPConfig(t *testing.T) {
	cfg := baseConfig()
	cfg["HTTPProxyURL"] = "http://proxy.local:8080"
	cfg["HttpUrlPath"] = "/clickhouse/proxy"
	cfg["HttpHeaders"] = "X-App=myapp, X-Trace=enabled"
	cfg["HttpMaxConnsPerHost"] = "50"

	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Opt.HTTPProxyURL == nil || p.Opt.HTTPProxyURL.String() != "http://proxy.local:8080" {
		t.Errorf("HTTPProxyURL = %v, want http://proxy.local:8080", p.Opt.HTTPProxyURL)
	}
	if p.Opt.HttpUrlPath != "/clickhouse/proxy" {
		t.Errorf("HttpUrlPath = %q, want /clickhouse/proxy", p.Opt.HttpUrlPath)
	}
	if p.Opt.HttpHeaders["X-App"] != "myapp" || p.Opt.HttpHeaders["X-Trace"] != "enabled" {
		t.Errorf("HttpHeaders = %v, want map with X-App and X-Trace", p.Opt.HttpHeaders)
	}
	if p.Opt.HttpMaxConnsPerHost != 50 {
		t.Errorf("HttpMaxConnsPerHost = %d, want 50", p.Opt.HttpMaxConnsPerHost)
	}
}

func TestNewPlugin_InvalidHTTPConfig(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "bad proxy URL", key: "HTTPProxyURL", value: "://proxy"},
		{name: "bad headers no equals", key: "HttpHeaders", value: "X-App"},
		{name: "bad headers empty key", key: "HttpHeaders", value: "=v"},
		{name: "bad headers empty item", key: "HttpHeaders", value: "X-A=1,,X-B=2"},
		{name: "forbidden host header", key: "HttpHeaders", value: "Host=clickhouse.internal"},
		{name: "forbidden authorization header", key: "HttpHeaders", value: "Authorization=Bearer token"},
		{name: "forbidden header case insensitive", key: "HttpHeaders", value: "hOsT=clickhouse.internal"},
		{name: "bad max conns per host", key: "HttpMaxConnsPerHost", value: "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg[tt.key] = tt.value
			_, err := NewPlugin(makeConfig(cfg))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			var cfgErr *ConfigError
			if !errors.As(err, &cfgErr) {
				t.Errorf("expected ConfigError, got %T: %v", err, err)
			} else if tt.key == "HttpHeaders" && cfgErr.Field != "HttpHeaders" {
				t.Errorf("ConfigError.Field = %q, want HttpHeaders", cfgErr.Field)
			}
		})
	}
}

func TestNewPlugin_Settings(t *testing.T) {
	cfg := baseConfig()
	cfg["Settings"] = "max_execution_time=60,wait_end_of_query=1,log_queries=true,ratio=0.5,profile=default"

	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, ok := p.Opt.Settings["max_execution_time"].(int64); !ok || got != 60 {
		t.Errorf("Settings[max_execution_time] = %#v, want int64(60)", p.Opt.Settings["max_execution_time"])
	}
	if got, ok := p.Opt.Settings["wait_end_of_query"].(int64); !ok || got != 1 {
		t.Errorf("Settings[wait_end_of_query] = %#v, want int64(1)", p.Opt.Settings["wait_end_of_query"])
	}
	if got, ok := p.Opt.Settings["log_queries"].(bool); !ok || !got {
		t.Errorf("Settings[log_queries] = %#v, want bool(true)", p.Opt.Settings["log_queries"])
	}
	if got, ok := p.Opt.Settings["ratio"].(float64); !ok || got != 0.5 {
		t.Errorf("Settings[ratio] = %#v, want float64(0.5)", p.Opt.Settings["ratio"])
	}
	if got, ok := p.Opt.Settings["profile"].(string); !ok || got != "default" {
		t.Errorf("Settings[profile] = %#v, want string(default)", p.Opt.Settings["profile"])
	}
}

func TestNewPlugin_InvalidSettings(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "missing equals", value: "max_execution_time"},
		{name: "empty key", value: "=1"},
		{name: "empty item", value: "a=1,,b=2"},
		{name: "max memory usage too large", value: "max_memory_usage=10737418241"},
		{name: "max memory usage negative", value: "max_memory_usage=-1"},
		{name: "max threads too large", value: "max_threads=65"},
		{name: "max threads negative", value: "max_threads=-1"},
		{name: "max insert threads too large", value: "max_insert_threads=65"},
		{name: "max partitions per insert block too large", value: "max_partitions_per_insert_block=1001"},
		{name: "max insert block size too large", value: "max_insert_block_size=1000001"},
		{name: "max block size too large", value: "max_block_size=1000001"},
		{name: "managed dedup token", value: "insert_deduplication_token=fixed"},
		{name: "managed dedup flag", value: "insert_deduplicate=1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg["Settings"] = tt.value
			_, err := NewPlugin(makeConfig(cfg))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			var cfgErr *ConfigError
			if !errors.As(err, &cfgErr) {
				t.Errorf("expected ConfigError, got %T: %v", err, err)
			} else if cfgErr.Field != "Settings" {
				t.Errorf("ConfigError.Field = %q, want Settings", cfgErr.Field)
			}
		})
	}
}

func TestNewPlugin_SettingsResourceLimits(t *testing.T) {
	cfg := baseConfig()
	cfg["Settings"] = "max_memory_usage=1073741824,max_threads=8,max_insert_threads=4,max_partitions_per_insert_block=128,max_insert_block_size=100000,max_block_size=65536"

	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for key, want := range map[string]int64{
		"max_memory_usage":                1073741824,
		"max_threads":                     8,
		"max_insert_threads":              4,
		"max_partitions_per_insert_block": 128,
		"max_insert_block_size":           100000,
		"max_block_size":                  65536,
	} {
		if got, ok := p.Opt.Settings[key].(int64); !ok || got != want {
			t.Errorf("Settings[%s] = %#v, want int64(%d)", key, p.Opt.Settings[key], want)
		}
	}
}

func TestNewPlugin_LogLevel(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		wantLogger bool
		wantErr    bool
	}{
		{name: "debug", value: "debug", wantLogger: true},
		{name: "info", value: "info", wantLogger: true},
		{name: "warn", value: "warn", wantLogger: true},
		{name: "error", value: "error", wantLogger: true},
		{name: "empty", value: "", wantLogger: true}, // empty now defaults to info level
		{name: "invalid", value: "trace", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg["LogLevel"] = tt.value
			p, err := NewPlugin(makeConfig(cfg))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			hasLogger := p.Opt.Logger != nil
			if hasLogger != tt.wantLogger {
				t.Errorf("Logger != nil = %v, want %v", hasLogger, tt.wantLogger)
			}
		})
	}
}

func TestRedactSecrets_URLUserinfo(t *testing.T) {
	got := redactSecrets("clickhouse://admin:MyP@ss!@host:9000/db", "MyP@ss!")
	if strings.Contains(got, "MyP@ss!") {
		t.Fatalf("redactSecrets() leaked plain password: %s", got)
	}
}

func TestRedactSecrets_URLUserinfoEscapedPassword(t *testing.T) {
	got := redactSecrets("clickhouse://admin:MyP%40ss%21@host:9000/db", "MyP@ss!")
	if strings.Contains(got, "MyP%40ss%21") {
		t.Fatalf("redactSecrets() leaked escaped password: %s", got)
	}
}

func TestRedactSecrets_KeyValueWithColon(t *testing.T) {
	got := redactSecrets("password:abc pass:abc token:abc secret:abc", "")
	for _, secret := range []string{"password:abc", "pass:abc", "token:abc", "secret:abc"} {
		if strings.Contains(strings.ToLower(got), secret) {
			t.Fatalf("redactSecrets() leaked %q: %s", secret, got)
		}
	}
}

func TestDedupToken_StableAndHex(t *testing.T) {
	h1 := xxhash.New()
	_, _ = h1.Write([]byte("row-a"))
	_, _ = h1.Write([]byte{0})
	token1 := dedupToken(h1)

	h2 := xxhash.New()
	_, _ = h2.Write([]byte("row-a"))
	_, _ = h2.Write([]byte{0})
	token2 := dedupToken(h2)

	if token1 != token2 {
		t.Fatalf("dedupToken() not deterministic: %q != %q", token1, token2)
	}
	if len(token1) != 16 {
		t.Fatalf("dedupToken() len = %d, want 16", len(token1))
	}
	for _, r := range token1 {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			t.Fatalf("dedupToken() contains non-hex rune %q in %q", r, token1)
		}
	}

	h3 := xxhash.New()
	_, _ = h3.Write([]byte("row-b"))
	_, _ = h3.Write([]byte{0})
	token3 := dedupToken(h3)
	if token3 == token1 {
		t.Fatalf("dedupToken() should differ for different input: %q", token3)
	}
}

func TestStringifyValue_FastPathAndFallback(t *testing.T) {
	tests := []struct {
		name string
		raw  any
		want string
	}{
		{name: "int", raw: int(-12), want: "-12"},
		{name: "int64", raw: int64(42), want: "42"},
		{name: "uint64", raw: uint64(7), want: "7"},
		{name: "bool true", raw: true, want: "true"},
		{name: "bool false", raw: false, want: "false"},
		{name: "float64", raw: 123.5, want: "123.5"},
		{name: "string", raw: "abc", want: "abc"},
		{name: "bytes", raw: []byte("xyz"), want: "xyz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stringifyValue(tt.raw); got != tt.want {
				t.Fatalf("stringifyValue(%v[%T]) = %q, want %q", tt.raw, tt.raw, got, tt.want)
			}
		})
	}
}

func TestNewPlugin_ClientInfo(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantN   int
		wantErr bool
	}{
		{name: "single product", value: "myapp/1.0", wantN: 1},
		{name: "multiple products", value: "myapp/1.0,module/0.1", wantN: 2},
		{name: "invalid format", value: "no_slash_here", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg["ClientInfo"] = tt.value
			p, err := NewPlugin(makeConfig(cfg))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(p.Opt.ClientInfo.Products) != tt.wantN {
				t.Errorf("Products count = %d, want %d", len(p.Opt.ClientInfo.Products), tt.wantN)
			}
		})
	}
}

func TestNewPlugin_ClientComment(t *testing.T) {
	tests := []struct {
		name  string
		value string
		wantN int
	}{
		{name: "single comment", value: "production", wantN: 1},
		{name: "multiple comments", value: "production,region-us", wantN: 2},
		{name: "empty string", value: "", wantN: 0},
		{name: "trims whitespace", value: "  production  ,  region-us  ", wantN: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			if tt.value != "" {
				cfg["ClientComment"] = tt.value
			}
			p, err := NewPlugin(makeConfig(cfg))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(p.Opt.ClientInfo.Comment) != tt.wantN {
				t.Errorf("Comment count = %d, want %d", len(p.Opt.ClientInfo.Comment), tt.wantN)
			}
		})
	}
}

func TestNewPlugin_BufferConfig(t *testing.T) {
	cfg := baseConfig()
	cfg["BlockBufferSize"] = "20"
	cfg["MaxCompressionBuffer"] = "20480"

	p, err := NewPlugin(makeConfig(cfg))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Opt.BlockBufferSize != 20 {
		t.Errorf("BlockBufferSize = %d, want 20", p.Opt.BlockBufferSize)
	}
	if p.Opt.MaxCompressionBuffer != 20480 {
		t.Errorf("MaxCompressionBuffer = %d, want 20480", p.Opt.MaxCompressionBuffer)
	}
}

func TestConfigError(t *testing.T) {
	err := configErr("TestField", errors.New("test error"))
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatal("expected ConfigError type")
	}
	if cfgErr.Field != "TestField" {
		t.Errorf("Field = %q, want %q", cfgErr.Field, "TestField")
	}
	want := "clickhouse: invalid config TestField: test error"
	if cfgErr.Error() != want {
		t.Errorf("Error() = %q, want %q", cfgErr.Error(), want)
	}
	// Test Unwrap
	inner := errors.New("test error")
	wrapped := configErr("F", inner)
	if !errors.Is(wrapped, inner) {
		t.Error("Unwrap should return inner error")
	}
}

func BenchmarkNewPlugin(b *testing.B) {
	cfg := baseConfig()
	cfg["Compression"] = "lz4"
	cfg["ClientInfo"] = "myapp/1.0"
	get := makeConfig(cfg)

	for b.Loop() {
		NewPlugin(get)
	}
}
