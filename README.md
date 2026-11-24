# mem-cache

`mem-cache` 是 Doku 服务使用的 Go 进程内缓存，适合多个请求反复读取同一批数据的场景。服务启动时创建一个缓存实例，注入需要它的 handler 或业务组件。每个服务进程独立保存数据，重启后从空缓存开始。

## 安装

模块路径为 `github.com/kumadrama/mem-cache`，Go 包名为 `caches`，最低 Go 版本为 1.22。拉取依赖需要对 `kumadrama/mem-cache` 和 `kumadrama/doku-lib` 的 Git 读取权限。使用 Go 直连私有仓库时，确认 `go env GOPRIVATE` 包含 `github.com/kumadrama/*`。

**在已有 Go 服务中**，进入包含该服务 `go.mod` 的目录，安装包含本文 API 的提交。Go 会将解析出的版本写入 `go.mod`。

```bash
go get github.com/kumadrama/mem-cache@a56ddf4f8c8b
```

**在 doku-mono 中**，使用包含 `mem-cache` 子模块的分支，从仓库根目录执行下面的命令。第一行检出父仓固定的源码，第二行把同一提交登记为根 Go 模块的依赖。业务代码统一使用上述模块路径导入。

```bash
git submodule update --init -- mem-cache
go get "github.com/kumadrama/mem-cache@$(git rev-parse :mem-cache)"
```

## 跑通第一次查询

下面是一个完整的 `main.go`。可以放进空目录，先执行 `go mod init example.com/memcache-demo` 和上面的固定版本安装命令，再执行 `go run .`。示例用只读 map 模拟数据源；接入服务时，将两个 loader 中的数据读取换成接收请求 `ctx` 的 Store 或 RPC 调用。

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kumadrama/mem-cache"
)

func newTitleCache(titles map[string]string) caches.Cache {
	return caches.New(10_000).
		Name("item_title").
		LRU().
		Expiration(time.Minute).
		LoaderContextFunc(func(ctx context.Context, key any) (any, *time.Duration, error) {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			title, found := titles[key.(string)]
			if !found {
				ttl := 5 * time.Second
				return nil, &ttl, nil
			}
			return title, nil, nil
		}).
		MLoaderContextFunc(func(ctx context.Context, keys []any) ([]any, *time.Duration, error) {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			// 真实数据源可一次批查，再按 keys 顺序填回结果。
			values := make([]any, len(keys))
			for i, key := range keys {
				if title, found := titles[key.(string)]; found {
					values[i] = title
				}
			}
			return values, nil, nil
		}).
		Build()
}

func main() {
	ctx := context.Background()
	cache := newTitleCache(map[string]string{"a": "Alpha", "b": "Beta"})

	value, err := cache.GetContext(ctx, "a")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value)

	keys := []any{"a", "b", "missing"}
	for i := 0; i < 2; i++ {
		values, misses, err := cache.MGetContext(ctx, keys)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("values=%v misses=%v\n", values, misses)
	}
	fmt.Printf("hits=%d misses=%d rate=%.1f%%\n",
		cache.HitCount(), cache.MissCount(), 100*cache.HitRate())
}
```

预期业务输出如下。第一次单键查询回源并写入 `a`。第一次批查只加载 `b` 和 `missing`，第二次批查三个位置都命中，包括已经缓存的空值。

```text
Alpha
values=[Alpha Beta <nil>] misses=[1 2]
values=[Alpha Beta <nil>] misses=[]
hits=4 misses=3 rate=57.1%
```

示例统一使用 `string` key。业务侧也应固定 key 的 Go 类型，可以用接收 `string` 或 `int64` 的业务方法封装缓存。key 必须可作为 Go map 的键。值会被多个请求共享，指针、map 和切片应当按只读使用，需要修改时先复制。

## 选择读取方式

| 调用 | 本地命中 | 本地缺失 |
| --- | --- | --- |
| `GetContext(ctx, key)` | 返回缓存值 | 等待单键 loader 并回填。同一实例、同一 key 的并发调用共享一次在途回源。未配置单键 loader 时返回 `caches.KeyNotFoundError` |
| `GetIFPresentContext(ctx, key)` | 返回缓存值 | 通常立即返回 `caches.KeyNotFoundError`。配置了单键 loader 时发起或复用后台加载，适合本次请求可以直接降级的场景 |
| `MGetContext(ctx, keys)` | 结果位置与输入一一对应 | 将本次缺失的 key 交给批量 loader。仅配置单键 loader 时，批查仍直接返回本地结果和缺失下标 |

单键与批量 loader 独立配置。同步单键回源使用发起者的上下文，等待中的其他请求可以独立取消等待。后台加载保留上下文里的请求和 trace 信息，并解除原请求的取消与 deadline；在 loader 内给下游调用设置自己的超时。服务入口应将 handler 收到的 `ctx` 传给缓存。旧接口 `Get`、`GetIFPresent`、`MGet` 也会上报指标。

### 批量返回值和错误

- `values[i]` 对应 `keys[i]`。`misses` 是**首次查本地时**缺失的输入下标，回源成功后仍会返回这些下标。
- 批量 loader 接收缺失的 key，保留输入顺序和重复项。返回切片必须等长、同序。下游若返回 map，按传入的 key 重建结果切片。不同请求之间的批量回源各自执行。
- 来源明确确认某项不存在时，可以用 `nil` 占位并返回成功，后续读取会命中该空值。下游超时或失败应返回 `err`，这样后续请求可以重试。单键 loader 可给空值单独设置短 TTL，批量 loader 返回的 TTL 作用于整批回填值。
- 批量 loader 返回错误时，`MGetContext` 返回该错误，同时保留此前本地命中的值。允许部分降级的调用方可以处理命中位置，并用 `misses` 标记本次受影响的位置。判断命中以首次缺失下标为准，已缓存的 `nil` 也是命中。
- 空输入返回 `caches.KeysSizeError`；loader 返回的值数与缺失数不一致时返回 `caches.KeysAndValuesNotMatchError`。调用前可以直接跳过空批次。

## 容量、TTL 与失效

`New(n)` 的容量按驻留条目数计算。显式选择淘汰策略和业务能接受的 TTL，再根据单实例内存预算和实际访问模式调整。示例中的 10,000 条和 1 分钟仅用于演示。

| 配置 | 适用方式 |
| --- | --- |
| `LRU()` | 优先保留最近访问的数据，适合近期热点 |
| `LFU()` | 优先保留访问次数高的数据，适合较稳定的热点 |
| `ARC()` | 自适应平衡新访问和反复访问的数据，适合热点与新数据混合的负载 |
| `Simple()` | 默认策略。满容量且所有条目都有尚未到期的 TTL 时，写入新 key 会持锁扫描整张表。对大容量、持续新增 key 的 TTL 缓存，优先评估 LRU 或 ARC |

- `Expiration(ttl)` 配置默认写入 TTL。读取不会续期。loader 返回的 TTL 指针可覆盖默认值，返回 `nil` 沿用默认配置。过期条目在读取时清理，`Len(false)` 包含尚未清理的过期条目，`Len(true)` 统计当前有效条目。
- 主动写入用 `Set` 或 `MSet`，针对某次写入指定 TTL 用 `SetWithExpire` 或 `MSetWithExpire`。批量写入的 key 与 value 切片须等长。
- 数据源更新后可调用 `Remove(key)` 让当前实例下次读取重新加载，`Purge()` 清空整个实例。多副本服务若要求所有副本立即失效，需要向各副本传递失效事件；仅依赖 TTL 时，按各副本自己的过期时间收敛。

## Doku 指标、命中率与日志

创建实例时设置固定业务名，例如 `Name("item_title")`。默认名是 `default`。库通过 `dmetrics.Meter("doku.mem_cache")` 自动记录指标，公共维度为 `psm`、`ip`、`cache`、`policy`。同一进程内同名、同策略的实例会合并计数，条目数取最近一次观测值。本地使用进程内 reader，云环境由 Doku 的 OTLP collector 接收。

| 指标 | 类型 | 读法 |
| --- | --- | --- |
| `mem_cache_lookup_keys` | Counter | 按 `result=hit\|miss` 记录首次本地查找的 key 数，批量请求按输入位置计数 |
| `mem_cache_loads` | Counter | 实际执行的回源次数，含 `operation=get\|mget\|refresh` 和 `result=ok\|error` |
| `mem_cache_load_keys` | Counter | 实际交给 loader 的 key 数，与回源次数使用相同维度 |
| `mem_cache_load_duration` | Histogram | 执行 loader 加回填的耗时，单位毫秒 |
| `mem_cache_entries` | Gauge | 最近一次变更后实例中的驻留条目数 |
| `mem_cache_removals` | Counter | 按 `reason=capacity\|expired\|remove\|purge` 统计移除条目数 |

**看本实例的累计命中率**，使用 `cache.HitRate()`，返回 0～1，首次查询前为 0。**看服务最近 5 分钟的命中率**，先按服务、缓存名和策略汇总各实例的命中增量，再除以所有查找增量。一次批查中 32 个 key 有 24 个在本地命中，这批对命中率的贡献就是 24/32，即 75%。

下面的 PromQL 适用于将 OTel Counter 导出为 `_total` 的数据源。将 `item_title` 换成业务缓存名。只有 miss 流量时显示 0%，没有查询流量时该比例无定义。

```promql
100 * (
  sum by (psm, cache, policy) (
    rate(mem_cache_lookup_keys_total{cache="item_title",result="hit"}[5m])
  )
  or
  0 * sum by (psm, cache, policy) (
    rate(mem_cache_lookup_keys_total{cache="item_title"}[5m])
  )
) / sum by (psm, cache, policy) (
  rate(mem_cache_lookup_keys_total{cache="item_title"}[5m])
)
```

**排查回源失败**，查询 Doku Warn 日志事件 `mem_cache.load_failed`，按 `cache` 和 `operation` 筛选。日志包含 `policy`、`key_count`、`duration_ms` 和 `error`，传入带 Doku trace 的请求上下文后还可关联 `trace_id` 和 `span_id`。单键并发已合并时，一次实际回源失败产生一条该事件；排查下游压力时同时看 `mem_cache_loads` 和 `mem_cache_load_keys`。
