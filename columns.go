package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// parseColumns parses the column configuration string.
// Format: "name|type[|timeformat],name|type,..."
func parseColumns(p *ClickHousePlugin, cfg string) error {
	arr := strings.Split(cfg, ",")
	p.Columns = make([]string, 0, len(arr))
	p.Defaults = make([]any, 0, len(arr))
	p.ColType = make([]ColumnParser, 0, len(arr))
	p.ColNullable = make([]bool, 0, len(arr))
	for _, v := range arr {
		v = strings.TrimSpace(v)
		if v == "" {
			return fmt.Errorf("clickhouse: empty column in Columns config %q", cfg)
		}
		typInfo := strings.Split(v, "|")
		if len(typInfo) < 2 {
			return fmt.Errorf("clickhouse: column %q must have format KEY|TYPE", v)
		}
		p.Columns = append(p.Columns, strings.TrimSpace(typInfo[0]))
		parser, def, nullable, err := parseColumnTypeSpec(typInfo[1], typInfo[2:])
		if err != nil {
			return fmt.Errorf("clickhouse: column %q type parse failed: %w", v, err)
		}
		p.ColType = append(p.ColType, parser)
		p.Defaults = append(p.Defaults, def)
		p.ColNullable = append(p.ColNullable, nullable)
	}
	return nil
}

// Supported column types:
// UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
// Float32, Float64, BFloat16, Boolean/Bool, String, FixedString(N)
// Decimal, Enum/Enum8/Enum16, UUID, IPv4, IPv6, JSON
// Array(Int*/UInt*/String/Float64), Date, Date32, DateTime, DateTime64, Time, Time64
// Nullable(T), LowCardinality(T) where T is any supported type above
func parseColumnType(cfg []string) (parser ColumnParser, def any, err error) {
	parser, def, _, err = parseColumnTypeSpec(cfg[0], cfg[1:])
	return
}

func parseColumnTypeSpec(typeExpr string, extras []string) (parser ColumnParser, def any, nullable bool, err error) {
	return parseColumnTypeSpecWithDepth(typeExpr, extras, 0)
}

func parseColumnTypeSpecWithDepth(typeExpr string, extras []string, depth int) (parser ColumnParser, def any, nullable bool, err error) {
	if depth > 8 {
		return nil, nil, false, fmt.Errorf("invalid Columns value[%s], type wrapper depth exceeds 8", append([]string{typeExpr}, extras...))
	}
	if inner, ok, wrapErr := unwrapType(typeExpr, "nullable"); ok {
		parser, _, _, err := parseColumnTypeSpecWithDepth(inner, extras, depth+1)
		if err != nil {
			return nil, nil, false, err
		}
		return parser, nil, true, nil
	} else if wrapErr != nil {
		return nil, nil, false, wrapErr
	}
	if inner, ok, wrapErr := unwrapType(typeExpr, "lowcardinality"); ok {
		parser, def, nullable, err = parseColumnTypeSpecWithDepth(inner, extras, depth+1)
		if err != nil {
			return nil, nil, false, err
		}
		return parser, def, nullable, nil
	} else if wrapErr != nil {
		return nil, nil, false, wrapErr
	}

	typeName := strings.ToLower(strings.TrimSpace(typeExpr))
	switch typeName {
	case "uint8":
		parser = parseUint[uint8]
		def = uint8(0)
	case "uint16":
		parser = parseUint[uint16]
		def = uint16(0)
	case "uint32":
		parser = parseUint[uint32]
		def = uint32(0)
	case "uint64":
		parser = parseUint[uint64]
		def = uint64(0)
	case "int8":
		parser = parseInt[int8]
		def = int8(0)
	case "int16":
		parser = parseInt[int16]
		def = int16(0)
	case "int32":
		parser = parseInt[int32]
		def = int32(0)
	case "int64":
		parser = parseInt[int64]
		def = int64(0)
	case "float32":
		parser = parseFloat[float32]
		def = float32(0)
	case "float64":
		parser = parseFloat[float64]
		def = float64(0)
	case "bfloat16":
		parser = parseFloat[float32]
		def = float32(0)
	case "boolean", "bool":
		parser = parseBool
		def = false
	case "string":
		parser = parseString
		def = ""
	case "decimal":
		parser = parseString
		def = "0"
	case "enum", "enum8", "enum16":
		parser = parseString
		def = ""
	case "uuid":
		parser = parseUUIDValue
		def = "00000000-0000-0000-0000-000000000000"
	case "ipv4":
		parser = parseIPv4Value
		def = "0.0.0.0"
	case "ipv6":
		parser = parseString
		def = ""
	case "json":
		parser = parseJSON
		def = "{}"
	case "array(string)":
		parser = parseJSONArrayString
		def = []string{}
	case "array(float64)":
		parser = parseJSONArrayFloat64
		def = []float64{}
	case "array(int8)":
		parser = func(s string) (any, error) {
			return parseSignedJSONArray[int8](s, -128, 127, "Int8")
		}
		def = []int8{}
	case "array(int16)":
		parser = func(s string) (any, error) {
			return parseSignedJSONArray[int16](s, -32768, 32767, "Int16")
		}
		def = []int16{}
	case "array(int32)":
		parser = func(s string) (any, error) {
			return parseSignedJSONArray[int32](s, -2147483648, 2147483647, "Int32")
		}
		def = []int32{}
	case "array(int64)":
		parser = func(s string) (any, error) {
			return parseSignedJSONArray[int64](s, -9223372036854775808, 9223372036854775807, "Int64")
		}
		def = []int64{}
	case "array(uint8)":
		parser = func(s string) (any, error) {
			return parseUnsignedJSONArray[uint8](s, 255, "UInt8")
		}
		def = []uint8{}
	case "array(uint16)":
		parser = func(s string) (any, error) {
			return parseUnsignedJSONArray[uint16](s, 65535, "UInt16")
		}
		def = []uint16{}
	case "array(uint32)":
		parser = func(s string) (any, error) {
			return parseUnsignedJSONArray[uint32](s, 4294967295, "UInt32")
		}
		def = []uint32{}
	case "array(uint64)":
		parser = func(s string) (any, error) {
			return parseUnsignedJSONArray[uint64](s, 18446744073709551615, "UInt64")
		}
		def = []uint64{}
	case "date", "date32":
		timeFormat := "2006-01-02"
		if len(extras) > 0 {
			_, err = time.Parse(extras[0], extras[0])
			if err != nil {
				err = fmt.Errorf("invalid Columns value[%s], parse time format failed,%+v", append([]string{typeExpr}, extras...), err)
				return
			}
			timeFormat = extras[0]
		}
		parser = func(s string) (interface{}, error) {
			return time.Parse(timeFormat, s)
		}
		def = func() any { return time.Now() }
	case "datetime":
		timeFormat := "2006-01-02 15:04:05"
		if len(extras) > 0 {
			_, err = time.Parse(extras[0], extras[0])
			if err != nil {
				err = fmt.Errorf("invalid Columns value[%s], parse time format failed,%+v", append([]string{typeExpr}, extras...), err)
				return
			}
			timeFormat = extras[0]
		}
		parser = func(s string) (interface{}, error) {
			return time.Parse(timeFormat, s)
		}
		def = func() any { return time.Now() }
	case "datetime64":
		timeFormat := "2006-01-02 15:04:05.000000000"
		if len(extras) > 0 {
			_, err = time.Parse(extras[0], extras[0])
			if err != nil {
				err = fmt.Errorf("invalid Columns value[%s], parse time format failed,%+v", append([]string{typeExpr}, extras...), err)
				return
			}
			timeFormat = extras[0]
		}
		parser = func(s string) (interface{}, error) {
			return time.Parse(timeFormat, s)
		}
		def = func() any { return time.Now() }
	case "time", "time64":
		parser = parseDurationValue
		def = time.Duration(0)
	default:
		fixedN, fixedErr := parseFixedStringType(typeName)
		if fixedErr == nil {
			parser = func(s string) (any, error) {
				return parseFixedStringValue(s, fixedN)
			}
			def = ""
			return
		}
		err = fmt.Errorf("invalid Columns value[%s], not support type", append([]string{typeExpr}, extras...))
	}
	return
}

func unwrapType(typeExpr string, wrapper string) (inner string, ok bool, err error) {
	raw := strings.TrimSpace(typeExpr)
	lower := strings.ToLower(raw)
	prefix := strings.ToLower(wrapper) + "("
	if !strings.HasPrefix(lower, prefix) {
		return "", false, nil
	}
	if !strings.HasSuffix(raw, ")") {
		return "", false, fmt.Errorf("invalid %s type: missing closing ')'", wrapper)
	}
	start := len(wrapper) + 1
	depth := 0
	for i := start; i < len(raw)-1; i++ {
		switch raw[i] {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return "", false, fmt.Errorf("invalid %s type: unexpected ')'", wrapper)
			}
			depth--
		}
	}
	if depth != 0 {
		return "", false, fmt.Errorf("invalid %s type: unbalanced parentheses", wrapper)
	}
	inner = strings.TrimSpace(raw[start : len(raw)-1])
	if inner == "" {
		return "", false, fmt.Errorf("invalid %s type: empty inner type", wrapper)
	}
	return inner, true, nil
}

func parseInt[T int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64](s string) (any, error) {
	nv, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return T(nv), nil
}

func parseUint[T uint | uint8 | uint16 | uint32 | uint64](s string) (any, error) {
	nv, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return T(nv), nil
}

func parseFloat[T float32 | float64](s string) (any, error) {
	nv, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return T(nv), nil
}

func parseBool(s string) (any, error) {
	nv, err := strconv.ParseBool(s)
	if err != nil {
		return false, err
	}
	return nv, nil
}

func parseString(s string) (any, error) {
	return s, nil
}

func parseJSON(s string) (any, error) {
	if !json.Valid([]byte(s)) {
		return nil, fmt.Errorf("invalid json value")
	}
	var out bytes.Buffer
	if err := json.Compact(&out, []byte(s)); err != nil {
		return nil, fmt.Errorf("invalid json value: %w", err)
	}
	return out.String(), nil
}

var fixedStringTypeRe = regexp.MustCompile(`^fixedstring\(([0-9]+)\)$`)

func parseFixedStringType(typeName string) (int, error) {
	matches := fixedStringTypeRe.FindStringSubmatch(strings.TrimSpace(typeName))
	if len(matches) != 2 {
		return 0, fmt.Errorf("not fixedstring type")
	}
	n, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("invalid fixedstring size: %w", err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("invalid fixedstring size: must be > 0")
	}
	return n, nil
}

func parseFixedStringValue(s string, n int) (any, error) {
	if len(s) > n {
		return nil, fmt.Errorf("fixedstring(%d) value too long: len=%d", n, len(s))
	}
	return s, nil
}

func parseDurationValue(s string) (any, error) {
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("invalid duration value: %w", err)
	}
	return d, nil
}

func parseUUIDValue(s string) (any, error) {
	u, err := uuid.Parse(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("invalid uuid value: %w", err)
	}
	return u.String(), nil
}

func parseIPv4Value(s string) (any, error) {
	ip, err := netip.ParseAddr(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("invalid ipv4 value: %w", err)
	}
	if !ip.Is4() {
		return nil, fmt.Errorf("invalid ipv4 value: not ipv4")
	}
	return ip.String(), nil
}

type signedInteger interface {
	~int8 | ~int16 | ~int32 | ~int64
}

type unsignedInteger interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

func parseSignedJSONArray[T signedInteger](s string, min, max int64, typeName string) (any, error) {
	values, err := parseJSONNumberArray(s)
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, len(values))
	for i, n := range values {
		v, err := strconv.ParseInt(n.String(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s array element[%d]=%q: %w", typeName, i, n.String(), err)
		}
		if v < min || v > max {
			return nil, fmt.Errorf("invalid %s array element[%d]=%d: out of range", typeName, i, v)
		}
		out = append(out, T(v))
	}
	return out, nil
}

func parseUnsignedJSONArray[T unsignedInteger](s string, max uint64, typeName string) (any, error) {
	values, err := parseJSONNumberArray(s)
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, len(values))
	for i, n := range values {
		v, err := strconv.ParseUint(n.String(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s array element[%d]=%q: %w", typeName, i, n.String(), err)
		}
		if v > max {
			return nil, fmt.Errorf("invalid %s array element[%d]=%d: out of range", typeName, i, v)
		}
		out = append(out, T(v))
	}
	return out, nil
}

func parseJSONNumberArray(s string) ([]json.Number, error) {
	valuesAny, err := parseJSONArrayAny(s)
	if err != nil {
		return nil, err
	}
	values := make([]json.Number, 0, len(valuesAny))
	for i, v := range valuesAny {
		num, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("invalid array json: element[%d] is not number", i)
		}
		values = append(values, num)
	}
	return values, nil
}

func parseJSONArrayString(s string) (any, error) {
	valuesAny, err := parseJSONArrayAny(s)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(valuesAny))
	for i, v := range valuesAny {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("invalid String array element[%d]: must be string", i)
		}
		out = append(out, str)
	}
	return out, nil
}

func parseJSONArrayFloat64(s string) (any, error) {
	valuesAny, err := parseJSONArrayAny(s)
	if err != nil {
		return nil, err
	}
	out := make([]float64, 0, len(valuesAny))
	for i, v := range valuesAny {
		num, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("invalid Float64 array element[%d]: must be number", i)
		}
		fv, err := strconv.ParseFloat(num.String(), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid Float64 array element[%d]=%q: %w", i, num.String(), err)
		}
		out = append(out, fv)
	}
	return out, nil
}

func parseJSONArrayAny(s string) ([]any, error) {
	decoder := json.NewDecoder(strings.NewReader(s))
	decoder.UseNumber()

	var values []any
	if err := decoder.Decode(&values); err != nil {
		return nil, fmt.Errorf("invalid array json: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("invalid array json: trailing data")
	}
	return values, nil
}
