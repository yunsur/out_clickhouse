package main

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseInt(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    any
		wantErr bool
	}{
		{name: "int8 positive", input: "127", want: int8(127)},
		{name: "int8 negative", input: "-128", want: int8(-128)},
		{name: "int8 zero", input: "0", want: int8(0)},
		{name: "int8 invalid", input: "abc", wantErr: true},
		{name: "int16", input: "32767", want: int16(32767)},
		{name: "int32", input: "2147483647", want: int32(2147483647)},
		{name: "int64", input: "9223372036854775807", want: int64(math.MaxInt64)},
	}

	parsers := map[string]ColumnParser{
		"int8":  parseInt[int8],
		"int16": parseInt[int16],
		"int32": parseInt[int32],
		"int64": parseInt[int64],
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Determine parser from test name prefix
			var parser ColumnParser
			switch {
			case tt.name[:5] == "int64":
				parser = parsers["int64"]
			case tt.name[:5] == "int32":
				parser = parsers["int32"]
			case tt.name[:5] == "int16":
				parser = parsers["int16"]
			default:
				parser = parsers["int8"]
			}

			got, err := parser(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseInt(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseInt(%q) = %v (%T), want %v (%T)", tt.input, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestParseUint(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    any
		parser  ColumnParser
		wantErr bool
	}{
		{name: "uint8 max", input: "255", want: uint8(255), parser: parseUint[uint8]},
		{name: "uint8 zero", input: "0", want: uint8(0), parser: parseUint[uint8]},
		{name: "uint16", input: "65535", want: uint16(65535), parser: parseUint[uint16]},
		{name: "uint32", input: "4294967295", want: uint32(4294967295), parser: parseUint[uint32]},
		{name: "uint64", input: "18446744073709551615", want: uint64(math.MaxUint64), parser: parseUint[uint64]},
		{name: "invalid", input: "abc", parser: parseUint[uint8], wantErr: true},
		{name: "negative", input: "-1", parser: parseUint[uint8], wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.parser(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseUint(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseUint(%q) = %v (%T), want %v (%T)", tt.input, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestParseFloat(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    any
		parser  ColumnParser
		wantErr bool
	}{
		{name: "float32 positive", input: "3.14", want: float32(3.14), parser: parseFloat[float32]},
		{name: "float32 negative", input: "-2.5", want: float32(-2.5), parser: parseFloat[float32]},
		{name: "float64", input: "3.141592653589793", want: float64(3.141592653589793), parser: parseFloat[float64]},
		{name: "float64 zero", input: "0", want: float64(0), parser: parseFloat[float64]},
		{name: "invalid", input: "not_a_number", parser: parseFloat[float32], wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.parser(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseFloat(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseFloat(%q) = %v (%T), want %v (%T)", tt.input, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestParseBool(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    any
		wantErr bool
	}{
		{name: "true", input: "true", want: true},
		{name: "false", input: "false", want: false},
		{name: "1", input: "1", want: true},
		{name: "0", input: "0", want: false},
		{name: "invalid", input: "maybe", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBool(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseBool(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseBool(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "hello", want: "hello"},
		{input: "", want: ""},
		{input: "with spaces", want: "with spaces"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseString(tt.input)
			if err != nil {
				t.Fatalf("parseString(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("parseString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseColumnType(t *testing.T) {
	tests := []struct {
		name           string
		cfg            []string
		wantDef        any
		wantDefNil     bool
		wantDefFactory bool
		wantErr        bool
	}{
		{name: "uint8", cfg: []string{"UInt8"}, wantDef: uint8(0)},
		{name: "uint16", cfg: []string{"UInt16"}, wantDef: uint16(0)},
		{name: "uint32", cfg: []string{"UInt32"}, wantDef: uint32(0)},
		{name: "uint64", cfg: []string{"UInt64"}, wantDef: uint64(0)},
		{name: "int8", cfg: []string{"Int8"}, wantDef: int8(0)},
		{name: "int16", cfg: []string{"Int16"}, wantDef: int16(0)},
		{name: "int32", cfg: []string{"Int32"}, wantDef: int32(0)},
		{name: "int64", cfg: []string{"Int64"}, wantDef: int64(0)},
		{name: "float32", cfg: []string{"Float32"}, wantDef: float32(0)},
		{name: "float64", cfg: []string{"Float64"}, wantDef: float64(0)},
		{name: "bfloat16", cfg: []string{"BFloat16"}, wantDef: float32(0)},
		{name: "boolean", cfg: []string{"Boolean"}, wantDef: false},
		{name: "bool alias", cfg: []string{"Bool"}, wantDef: false},
		{name: "string", cfg: []string{"String"}, wantDef: ""},
		{name: "decimal", cfg: []string{"Decimal"}, wantDef: "0"},
		{name: "enum", cfg: []string{"Enum"}, wantDef: ""},
		{name: "enum8", cfg: []string{"Enum8"}, wantDef: ""},
		{name: "enum16", cfg: []string{"Enum16"}, wantDef: ""},
		{name: "uuid", cfg: []string{"UUID"}, wantDef: "00000000-0000-0000-0000-000000000000"},
		{name: "ipv4", cfg: []string{"IPv4"}, wantDef: "0.0.0.0"},
		{name: "ipv6", cfg: []string{"IPv6"}, wantDef: ""},
		{name: "json", cfg: []string{"JSON"}, wantDef: "{}"},
		{name: "fixedstring", cfg: []string{"FixedString(8)"}, wantDef: ""},
		{name: "array int8", cfg: []string{"Array(Int8)"}, wantDef: []int8{}},
		{name: "array int16", cfg: []string{"Array(Int16)"}, wantDef: []int16{}},
		{name: "array int32", cfg: []string{"Array(Int32)"}, wantDef: []int32{}},
		{name: "array int64", cfg: []string{"Array(Int64)"}, wantDef: []int64{}},
		{name: "array uint8", cfg: []string{"Array(UInt8)"}, wantDef: []uint8{}},
		{name: "array uint16", cfg: []string{"Array(UInt16)"}, wantDef: []uint16{}},
		{name: "array uint32", cfg: []string{"Array(UInt32)"}, wantDef: []uint32{}},
		{name: "array uint64", cfg: []string{"Array(UInt64)"}, wantDef: []uint64{}},
		{name: "array string", cfg: []string{"Array(String)"}, wantDef: []string{}},
		{name: "array float64", cfg: []string{"Array(Float64)"}, wantDef: []float64{}},
		{name: "nullable string", cfg: []string{"Nullable(String)"}, wantDefNil: true},
		{name: "nullable int64", cfg: []string{"Nullable(Int64)"}, wantDefNil: true},
		{name: "nullable json", cfg: []string{"Nullable(JSON)"}, wantDefNil: true},
		{name: "lowcardinality string", cfg: []string{"LowCardinality(String)"}, wantDef: ""},
		{name: "lowcardinality uuid", cfg: []string{"LowCardinality(UUID)"}, wantDef: "00000000-0000-0000-0000-000000000000"},
		{name: "nullable lowcardinality string", cfg: []string{"Nullable(LowCardinality(String))"}, wantDefNil: true},
		{name: "case insensitive", cfg: []string{"string"}, wantDef: ""},
		{name: "date", cfg: []string{"Date"}, wantDefFactory: true},
		{name: "date32", cfg: []string{"Date32"}, wantDefFactory: true},
		{name: "datetime", cfg: []string{"DateTime"}, wantDefFactory: true},
		{name: "datetime64", cfg: []string{"DateTime64"}, wantDefFactory: true},
		{name: "time", cfg: []string{"Time"}, wantDef: time.Duration(0)},
		{name: "time64", cfg: []string{"Time64"}, wantDef: time.Duration(0)},
		{name: "date with format", cfg: []string{"Date", "2006-01-02"}, wantDefFactory: true},
		{name: "datetime64 with format", cfg: []string{"DateTime64", "2006-01-02 15:04:05.000000000"}, wantDefFactory: true},
		{name: "unsupported type", cfg: []string{"Map"}, wantErr: true},
		{name: "nullable unsupported inner", cfg: []string{"Nullable(Map(String,Int32))"}, wantErr: true},
		{name: "invalid date format", cfg: []string{"Date", "2006-13-02"}, wantErr: true},
		{name: "invalid fixedstring size", cfg: []string{"FixedString(0)"}, wantErr: true},
		{name: "invalid nullable missing close", cfg: []string{"Nullable("}, wantErr: true},
		{name: "invalid lowcardinality empty", cfg: []string{"LowCardinality()"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, def, err := parseColumnType(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseColumnType(%v) error = %v, wantErr %v", tt.cfg, err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if parser == nil {
				t.Error("parseColumnType returned nil parser")
			}
			switch {
			case tt.wantDefFactory:
				if _, ok := def.(func() any); !ok {
					t.Errorf("default type = %T, want func() any", def)
				}
			case tt.wantDefNil:
				if def != nil {
					t.Errorf("default = %v (%T), want nil", def, def)
				}
			case !reflect.DeepEqual(def, tt.wantDef):
				t.Errorf("default = %v (%T), want %v (%T)", def, def, tt.wantDef, tt.wantDef)
			}
		})
	}
}

func TestParseColumnType_DateTimeParsing(t *testing.T) {
	tests := []struct {
		name    string
		cfg     []string
		input   string
		wantErr bool
	}{
		{name: "date default format", cfg: []string{"Date"}, input: "2024-01-15"},
		{name: "date32 default format", cfg: []string{"Date32"}, input: "2024-01-15"},
		{name: "datetime default format", cfg: []string{"DateTime"}, input: "2024-01-15 10:30:00"},
		{name: "datetime64 default format", cfg: []string{"DateTime64"}, input: "2024-01-15 10:30:00.123456789"},
		{name: "datetime64 custom format", cfg: []string{"DateTime64", "2006-01-02T15:04:05.000"}, input: "2024-01-15T10:30:00.123"},
		{name: "date invalid input", cfg: []string{"Date"}, input: "not-a-date", wantErr: true},
		{name: "array int16 default format", cfg: []string{"Array(Int16)"}, input: "[1,2,3]"},
		{name: "array uint32 default format", cfg: []string{"Array(UInt32)"}, input: "[1,2,3]"},
		{name: "array invalid non json", cfg: []string{"Array(Int16)"}, input: "1,2,3", wantErr: true},
		{name: "array invalid float", cfg: []string{"Array(Int16)"}, input: "[1.5]", wantErr: true},
		{name: "array invalid out of range", cfg: []string{"Array(Int16)"}, input: "[32768]", wantErr: true},
		{name: "array invalid negative unsigned", cfg: []string{"Array(UInt16)"}, input: "[-1]", wantErr: true},
		{name: "array string valid", cfg: []string{"Array(String)"}, input: "[\"a\",\"b\"]"},
		{name: "array string invalid type", cfg: []string{"Array(String)"}, input: "[1]", wantErr: true},
		{name: "array float64 valid", cfg: []string{"Array(Float64)"}, input: "[1.25,2.5]"},
		{name: "array float64 invalid type", cfg: []string{"Array(Float64)"}, input: "[\"x\"]", wantErr: true},
		{name: "json valid", cfg: []string{"JSON"}, input: "{\"a\":1}"},
		{name: "json invalid", cfg: []string{"JSON"}, input: "not-json", wantErr: true},
		{name: "uuid valid", cfg: []string{"UUID"}, input: "550e8400-e29b-41d4-a716-446655440000"},
		{name: "uuid invalid", cfg: []string{"UUID"}, input: "not-uuid", wantErr: true},
		{name: "ipv4 valid", cfg: []string{"IPv4"}, input: "192.168.1.1"},
		{name: "ipv4 invalid", cfg: []string{"IPv4"}, input: "2001:db8::1", wantErr: true},
		{name: "time valid", cfg: []string{"Time"}, input: "1h2m3s"},
		{name: "time invalid format", cfg: []string{"Time"}, input: "12:34:56", wantErr: true},
		{name: "time64 valid", cfg: []string{"Time64"}, input: "250ms"},
		{name: "fixedstring valid", cfg: []string{"FixedString(4)"}, input: "abcd"},
		{name: "fixedstring overflow", cfg: []string{"FixedString(3)"}, input: "abcd", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, _, err := parseColumnType(tt.cfg)
			if err != nil {
				t.Fatalf("parseColumnType(%v) failed: %v", tt.cfg, err)
			}

			result, err := parser(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parser(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				typeName := strings.ToLower(tt.cfg[0])
				if strings.HasPrefix(typeName, "array(") ||
					typeName == "json" ||
					typeName == "uuid" ||
					typeName == "ipv4" ||
					typeName == "decimal" ||
					typeName == "enum" ||
					typeName == "enum8" ||
					typeName == "enum16" ||
					typeName == "time" ||
					typeName == "time64" ||
					typeName == "bfloat16" ||
					strings.HasPrefix(typeName, "fixedstring(") {
					return
				}
				if _, ok := result.(time.Time); !ok {
					t.Errorf("expected time.Time, got %T", result)
				}
			}
		})
	}
}

func TestParseColumnType_WrapperDepthLimit(t *testing.T) {
	withinLimit := strings.Repeat("Nullable(", 8) + "String" + strings.Repeat(")", 8)
	if _, _, err := parseColumnType([]string{withinLimit}); err != nil {
		t.Fatalf("parseColumnType(%q) unexpected error: %v", withinLimit, err)
	}

	overLimit := strings.Repeat("Nullable(", 9) + "String" + strings.Repeat(")", 9)
	if _, _, err := parseColumnType([]string{overLimit}); err == nil {
		t.Fatalf("parseColumnType(%q) expected depth error, got nil", overLimit)
	}
}

func TestParseColumns(t *testing.T) {
	tests := []struct {
		name    string
		cfg     string
		wantN   int
		wantErr bool
	}{
		{
			name:  "single column",
			cfg:   "uid|Int64",
			wantN: 1,
		},
		{
			name:  "multiple columns",
			cfg:   "uid|Int64,event|String,count|UInt32,ok|Bool",
			wantN: 4,
		},
		{
			name:  "ipv6 column",
			cfg:   "src_ip|IPv6",
			wantN: 1,
		},
		{
			name:  "json column",
			cfg:   "detail|JSON",
			wantN: 1,
		},
		{
			name:  "uuid column",
			cfg:   "trace_id|UUID",
			wantN: 1,
		},
		{
			name:  "ipv4 column",
			cfg:   "client_ip|IPv4",
			wantN: 1,
		},
		{
			name:  "time column",
			cfg:   "cost|Time",
			wantN: 1,
		},
		{
			name:  "fixedstring column",
			cfg:   "code|FixedString(8)",
			wantN: 1,
		},
		{
			name:  "array string/float columns",
			cfg:   "tags|Array(String),scores|Array(Float64)",
			wantN: 2,
		},
		{
			name:  "nullable columns",
			cfg:   "nick|Nullable(String),score|Nullable(Int64)",
			wantN: 2,
		},
		{
			name:  "lowcardinality columns",
			cfg:   "kind|LowCardinality(String),trace|LowCardinality(UUID)",
			wantN: 2,
		},
		{
			name:  "nested wrapper column",
			cfg:   "name|Nullable(LowCardinality(String))",
			wantN: 1,
		},
		{
			name:  "array int16 column",
			cfg:   "category|Array(Int16)",
			wantN: 1,
		},
		{
			name:  "multiple array columns",
			cfg:   "a|Array(Int16),b|Array(UInt32)",
			wantN: 2,
		},
		{
			name:  "with datetime",
			cfg:   "uid|Int64,ts|DateTime64|2006-01-02 15:04:05.000000000",
			wantN: 2,
		},
		{
			name:    "empty column",
			cfg:     "uid|Int64,,event|String",
			wantErr: true,
		},
		{
			name:    "missing type",
			cfg:     "uid",
			wantErr: true,
		},
		{
			name:    "unsupported type",
			cfg:     "uid|Map",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ClickHousePlugin{}
			err := parseColumns(p, tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseColumns(%q) error = %v, wantErr %v", tt.cfg, err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(p.Columns) != tt.wantN {
				t.Errorf("parseColumns(%q) got %d columns, want %d", tt.cfg, len(p.Columns), tt.wantN)
			}
			if !tt.wantErr && len(p.ColNullable) != tt.wantN {
				t.Errorf("parseColumns(%q) nullable count = %d, want %d", tt.cfg, len(p.ColNullable), tt.wantN)
			}
		})
	}
}

func TestParseColumns_NullableFlags(t *testing.T) {
	p := &ClickHousePlugin{}
	cfg := "a|String,b|Nullable(Int64),c|LowCardinality(String),d|Nullable(LowCardinality(String))"
	if err := parseColumns(p, cfg); err != nil {
		t.Fatalf("parseColumns(%q) unexpected error: %v", cfg, err)
	}
	want := []bool{false, true, false, true}
	if !reflect.DeepEqual(p.ColNullable, want) {
		t.Fatalf("ColNullable = %v, want %v", p.ColNullable, want)
	}
}

func BenchmarkParseInt64(b *testing.B) {
	for b.Loop() {
		parseInt[int64]("9223372036854775807")
	}
}

func BenchmarkParseUint64(b *testing.B) {
	for b.Loop() {
		parseUint[uint64]("18446744073709551615")
	}
}

func BenchmarkParseFloat64(b *testing.B) {
	for b.Loop() {
		parseFloat[float64]("3.141592653589793")
	}
}

func BenchmarkParseBool(b *testing.B) {
	for b.Loop() {
		parseBool("true")
	}
}

func BenchmarkParseString(b *testing.B) {
	for b.Loop() {
		parseString("hello world")
	}
}

func BenchmarkParseColumnType(b *testing.B) {
	types := []struct {
		name string
		cfg  []string
	}{
		{"Int64", []string{"Int64"}},
		{"String", []string{"String"}},
		{"DateTime64", []string{"DateTime64"}},
		{"Float64", []string{"Float64"}},
	}

	for _, tt := range types {
		b.Run(tt.name, func(b *testing.B) {
			for b.Loop() {
				parseColumnType(tt.cfg)
			}
		})
	}
}
