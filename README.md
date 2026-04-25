# out_clickhouse

Fluent Bit output plugin for writing logs to ClickHouse via native TCP or HTTP.

**Important: daemon mode (`-D` / `--daemon`) is not supported for this Go/CGo plugin. When `FLB_DAEMON_MODE=1` is set, initialization fails by design. Run Fluent Bit in foreground mode (for example, systemd `Type=simple` with `-c`).**

## 参数速查导读

- [连接与认证](#连接与认证)
- [写入映射](#写入映射)
- [连接池与超时](#连接池与超时)
- [连接策略与压缩](#连接策略与压缩)
- [日志配置](#日志配置)
- [HTTP 扩展配置](#http-扩展配置)
- [TLS 配置](#tls-配置)
- [Metrics 配置](#metrics-配置)
- [其他限制与说明](#其他限制与说明)
- [常见误配](#常见误配)

## 配置项总览

> `Duration` 类型按 Go `time.ParseDuration` 解析，支持如 `300ms`、`30s`、`5m`、`1h`。

| Field | Type | Default | Required | Description | Example |
| --- | --- | --- | --- | --- | --- |
| Addr | string | `localhost:9000` (native) / `localhost:8123` (http) | No | ClickHouse address list, comma-separated. | `127.0.0.1:9000` |
| Protocol | enum | `native` | No | Connection protocol: `native`, `http`. | `http` |
| Database | string | - | Yes | ClickHouse database name. | `default` |
| UserName | string | `default` | No | ClickHouse username. | `default` |
| Password | string | - | No | ClickHouse password. | `secret` |
| JWT | string | - | No | JWT token for ClickHouse Cloud authentication. | `eyJhbGciOiJIUzI1NiIs...` |
| Table | string | - | Yes | Target table name. | `events` |
| Columns | string | - | Yes | Record-to-column mapping list, format: `name|type[|layout]`. | `uid|Int64,event|String,time|DateTime64|2006-01-02T15:04:05.000+08:00` |
| LogLevel | enum | `info` | No | Log level for plugin and clickhouse-go: `debug`, `info`, `warn`, `error`. | `debug` |
| MaxIdleConns | int | `5` | No | Max idle connections. | `10` |
| MaxOpenConns | int | `MaxIdleConns+5` | No | Max open connections. | `20` |
| DialTimeout | duration | `30s` | No | Dial timeout and ping timeout baseline. | `10s` |
| ReadTimeout | duration | `300s` | No | Read timeout for requests. | `1m` |
| WriteTimeout | duration | `ReadTimeout` | No | End-to-end deadline for one flush (prepare + append + send). | `2m` |
| ConnMaxLifetime | duration | `1h` | No | Max lifetime of a connection. | `30m` |
| ConnOpenStrategy | enum | `in_order` | No | Connection open strategy: `in_order`, `round_robin`, `random`. | `round_robin` |
| FreeBufOnConnRelease | bool | `false` | No | Free memory buffer when connection is released to pool. | `true` |
| Compression | enum | `none` | No | Compression method: `none`, `lz4`, `zstd`, `gzip`, `deflate`, `br`. | `zstd` |
| CompressionLevel | int | `3` | No | Level for `zstd` or `br` only. | `5` |
| BlockBufferSize | int | `2` | No | ClickHouse block buffer size. | `4` |
| MaxCompressionBuffer | int | `10485760` | No | Max compression buffer bytes. | `20971520` |
| ClientInfo | string | - | No | Client product list, format: `name/version,name/version`. | `my_app/1.0,my_module/0.1` |
| ClientComment | string | - | No | Client comment list, comma-separated. | `production,region-us` |
| Settings | string | - | No | Session settings list, format: `key=value,key=value`. | `max_execution_time=60,wait_end_of_query=1` |
| HTTPProxyURL | string | - | No | HTTP proxy URL for ClickHouse HTTP protocol. | `http://127.0.0.1:8080` |
| HttpUrlPath | string | - | No | Custom ClickHouse HTTP path. | `/clickhouse/proxy` |
| HttpHeaders | string | - | No | Custom HTTP headers, format: `k=v,k=v`. | `X-App=my_app,X-Trace=enabled` |
| HttpMaxConnsPerHost | int | `0` | No | Max idle/active conns per host in HTTP transport. | `50` |
| TLS | bool | `false` | No | Enable TLS. | `true` |
| TLSServerName | string | - | No | TLS server name (SNI). | `clickhouse.internal` |
| TLSCACert | string | - | No | CA PEM file path. | `/etc/ssl/certs/clickhouse-ca.pem` |
| TLSCert | string | - | No | Client cert file path. Must be used with `TLSKey`. | `/etc/ssl/certs/client.pem` |
| TLSKey | string | - | No | Client key file path. Must be used with `TLSCert`. | `/etc/ssl/private/client.key` |
| TLSInsecureSkipVerify | bool | `false` | No | Skip TLS certificate verification. | `false` |
| MetricsAddr | string | - | No | Expose Prometheus metrics on `http://<MetricsAddr>/metrics`. | `127.0.0.1:9090` |

## 连接与认证

- `Protocol` 默认为 `native`，可选 `http`。
- `Addr` 默认根据 `Protocol` 决定：
  - `native` → `localhost:9000`
  - `http` → `localhost:8123`
- `UserName` 默认为 `default`。
- 多地址统一使用英文逗号 `,` 分隔（`host1:9000,host2:9000`）。
- `Table`、`Columns` 为必填。
- `JWT` 用于 ClickHouse Cloud 认证，设置后将替代 `UserName`/`Password` 认证方式。

## 写入映射

### Columns

`Columns` 用于定义 record 字段到 ClickHouse 列的映射，格式为：

```text
column_name|Type[|layout],column_name|Type...
```

Supported Type:

- `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64`
- `Float32`, `Float64`, `BFloat16`
- `Boolean` / `Bool`
- `String`
- `FixedString(N)`
- `Decimal`
- `Enum`, `Enum8`, `Enum16`
- `UUID`
- `IPv4`, `IPv6`
- `JSON`
- `Array(Int8)`, `Array(Int16)`, `Array(Int32)`, `Array(Int64)`
- `Array(UInt8)`, `Array(UInt16)`, `Array(UInt32)`, `Array(UInt64)`
- `Array(String)`, `Array(Float64)`
- `Date`, `Date32`, `DateTime`, `DateTime64`
- `Time`, `Time64`
- `Nullable(T)`, `LowCardinality(T)` (`T` 需为以上已支持类型之一，可嵌套如 `Nullable(LowCardinality(String))`)

时间类型可带自定义 `layout`（Go time layout）。
`Decimal` 与 `Enum*` 当前仅支持无参数类型名（例如 `Decimal`、`Enum8`），不支持 `Decimal(p,s)` 与 `Enum8('a'=1,...)` 语法。
`Time`/`Time64` 字段值仅支持 Go duration（如 `250ms`、`1h2m3s`）。
`FixedString(N)` 对超长输入会直接报错，不做截断。
JSON 类型要求值是合法 JSON（字典对象[含嵌套对象]、数组、标量均可），字段缺失时默认写入 `{}`。
当输入是非 string key 的 map（如 `map[interface{}]interface{}`）时，key 会被规范化为字符串后再写入 JSON。
数组类型字符串输入仅支持 JSON array（如 `[1,2,3]`、`["a","b"]`），字段缺失时默认写入空数组。
`Nullable(T)` 的空值语义只认 `nil`（字段缺失或显式 `nil`），不会把空串或零值视为 `NULL`。
`LowCardinality(T)` 在插件侧仅做类型语义兼容包装，不做额外字典优化。

最小示例：

```text
uid|Int64,event|String,time|DateTime64|2006-01-02T15:04:05.000+08:00
```

## 连接池与超时

- `MaxIdleConns` / `MaxOpenConns` 控制连接池规模。
- `DialTimeout` 用于建连与 ping 超时基线。
- `ReadTimeout` 控制读超时。
- `WriteTimeout` 控制单次 flush 的整体写入超时（prepare/append/send）。
- `ConnMaxLifetime` 限制连接最大生存时间。

## 连接策略与压缩

- `ConnOpenStrategy` 可选：`in_order`、`round_robin`、`random`。
- `FreeBufOnConnRelease`：连接归还到连接池时是否清空内存缓冲区。默认 `false` 保留缓冲区以提高性能；设为 `true` 可减少内存占用，适合内存紧张或连接长时间空闲的场景。
- `Compression` 可选：`none`、`lz4`、`zstd`、`gzip`、`deflate`、`br`。
- `CompressionLevel` 仅对 `zstd` / `br` 生效。

## 日志配置

- `LogLevel`：控制插件和 clickhouse-go 的统一日志输出级别，日志通过 Fluent Bit 日志系统打印。
  - 默认为 `info`，输出 INFO/WARN/ERROR 级别日志。
  - `debug`：输出 DEBUG/INFO/WARN/ERROR（包含插件内部调试信息）
  - `info`：输出 INFO/WARN/ERROR
  - `warn`：输出 WARN/ERROR
  - `error`：仅输出 ERROR

## HTTP 扩展配置

- `HTTPProxyURL`：设置 HTTP 代理。
- `HttpUrlPath`：设置自定义 HTTP 路径。
- `HttpHeaders`：自定义请求头，格式 `k=v,k=v`。
- `HttpMaxConnsPerHost`：设置每个目标主机的连接上限。

## TLS 配置

- `TLS=true` 时启用 TLS。
- `TLSServerName` 可指定 SNI。
- `TLSCACert` 用于指定 CA PEM。
- `TLSCert` 和 `TLSKey` 必须成对配置。
- `TLSInsecureSkipVerify=true` 会跳过证书校验（仅建议测试环境）。

## Metrics 配置

- 设置 `MetricsAddr` 后，会暴露 Prometheus 指标：

```text
http://<MetricsAddr>/metrics
```

## 其他限制与说明

- 插件依赖 `FLBPluginFlushCtx`（ctx-aware output ABI），建议 Fluent Bit `>= 1.9`。
- 插件日志优先走 Fluent Bit 内部 logger（`flb_plg_info/warn/error`）；若运行环境不可用则自动 fallback 到 stderr。
- `FLBPluginFlushCtx` payload 有硬上限：`64MB`。超过上限会返回 `FLB_RETRY` 并记录 dropped 指标（stage=`payload_limit`）。
- 每个 flush 会注入 `insert_deduplication_token`；若目标表为非 Replicated 引擎，请配置 ClickHouse `non_replicated_deduplication_window > 0`。
- `BlockBufferSize` 增大时，`Append` 阶段可能出现更明显背压，表现为 flush 时延升高。
- `Settings` 支持任意 `key=value`，值会自动解析为 `int64` / `float64` / `bool` / `string`。
- 以下 `Settings` key 有资源上限限制：
  - `max_memory_usage`, `max_memory_usage_for_user`, `max_memory_usage_for_all_queries` <= `10 GiB`
  - `max_threads`, `max_insert_threads` <= `64`
  - `max_partitions_per_insert_block` <= `1000`
  - `max_insert_block_size`, `max_block_size` <= `1000000`
- `HttpHeaders` 存在 hard restriction：禁止设置 `Host` 与 `Authorization`（大小写不敏感）。
- clickhouse-go 原生 `Options` 中函数类型字段（例如 `DialContext`, `DialStrategy`, `TransportFunc`, `GetJWT`）不支持通过 Fluent Bit 文本配置直接传入。
- 插件命名约束（必须遵守）：
  - `flb-*.so`：Fluent Bit 按 C DSO 插件处理，会查找 `*_plugin` 注册结构。
  - `out_*.so`：Fluent Bit Go proxy 插件推荐命名（本插件应使用 `out_clickhouse.so`）。
  - 若把本插件命名为 `flb-out_clickhouse.so`，会报错 `registration structure is missing 'out_clickhouse_plugin'`。
- 加载示例：

```text
# CLI
fluent-bit -e /opt/out_clickhouse/out_clickhouse.so -c /etc/fluent-bit/fluent-bit.conf

# plugins.conf
[PLUGINS]
    Path /opt/out_clickhouse/out_clickhouse.so
```

参考：

- Go duration parser: <https://pkg.go.dev/time#ParseDuration>
- clickhouse-go options: <https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2#Options>

## 常见误配

1. `Addr` 未配置
- 结果：使用默认地址（native: `localhost:9000`，http: `localhost:8123`）。

2. `Columns` 类型与数据不匹配
- 例：`id|Int32` 但输入是非数字字符串。
- 结果：flush 时解析失败，该批次返回错误。

3. `HttpHeaders` 传入 `Host` 或 `Authorization`
- 结果：配置期校验失败（hard restriction）。

4. 仅配置 `TLSCert` 或仅配置 `TLSKey`
- 结果：配置期报错，必须成对设置。
