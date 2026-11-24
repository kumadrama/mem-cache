# mem-cache

Doku 服务使用的 Go 进程内缓存。模块路径为 `github.com/kumadrama/mem-cache`，包名为 `caches`，要求 Go 1.22 或更高版本。

```go
cache := caches.New(100_000).
	Name("item_info").
	ARC().
	Expiration(time.Minute).
	MLoaderContextFunc(func(ctx context.Context, keys []any) ([]any, *time.Duration, error) {
		values, err := loadItemInfo(ctx, keys)
		return values, nil, err
	}).
	Build()

values, missIndices, err := cache.MGetContext(ctx, ids)
```

四种策略 Simple、LRU、LFU、ARC 共用同一套读写与回源逻辑。容量按条目数计算，TTL 从写入时起算。单键 Get 合并同一实例同一 key 的并发回源。GetIFPresent 在缺失时立即返回，并在配置了单键 loader 时异步加载。MGet 对本次读取中缺失的 key 批量回源，按输入顺序返回值，missIndices 是首次查内存时的下标。loader 成功返回的 nil 可被缓存，回源失败时调用方仍可使用已有的命中值。缓存保存对象引用，跨请求共享的对象应按只读使用。

### 监控与日志

缓存通过 Doku 的 `dmetrics.Meter("doku.mem_cache")` 记录以下指标。公共维度为 `psm`、`ip`、`cache`、`policy`。给每种用途设置稳定的缓存名，默认名为 `default`。同一进程内复用名称和策略的缓存会合并计数，条目数取最近一次观测值。

| 指标 | 含义 |
| --- | --- |
| mem_cache_lookup_keys | 每次首次本地查询的 key 数，按 hit / miss 拆分 |
| mem_cache_loads、mem_cache_load_keys | 实际执行的回源次数及 key 数，按 operation 和 result 拆分 |
| mem_cache_load_duration | loader 加回填耗时的直方图，单位毫秒 |
| mem_cache_entries | 最近一次变更后的驻留条目数，含尚未清理的过期条目 |
| mem_cache_removals | 按 capacity、expired、remove、purge 记录清除条目数 |

回源失败通过 `dlog.Warn` 记录事件 `mem_cache.load_failed`，包含缓存名、策略、操作、key 数、耗时和错误。使用 `GetContext`、`MGetContext`、`GetIFPresentContext` 传递 Doku 请求上下文，日志可带上请求和 trace 信息。原有无 context 接口也会记录相同的缓存指标。
