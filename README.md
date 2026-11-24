# mem-cache

Go 进程内缓存库，迁自 gotap/caches 的 ebf728076160 版本，仅依赖标准库。包名保留为 `caches`。

```go
package main

import (
	"fmt"
	"time"

	"github.com/kumadrama/mem-cache"
)

func main() {
	cache := caches.New(100_000).ARC().Expiration(time.Minute).Build()
	if err := cache.Set("id", "value"); err != nil {
		panic(err)
	}
	fmt.Println(cache.Get("id"))
}
```

`New(size)` 默认使用 Simple，也可选择 `LRU()`、`LFU()` 或 `ARC()`。容量单位为条目数。TTL 从写入时起算。缓存可持有指针，调用方共享这些对象。

单键 `Get` 使用 `LoaderFunc`，对同一实例、同一 key 的并发回源进行合并。`GetIFPresent` 在缺失时立即返回 `KeyNotFoundError`，若已配置单键 loader，则在后台加载。

批量 `MGet` 使用 `MLoaderFunc`，返回与输入顺序一致的值、首次查内存未命中的下标和错误。loader 应为每个传入的 key 返回一个值，允许返回 nil。并发批次可分别回源。回源失败时，调用方仍可读取本批已有的命中值。

库还提供逐条和批量 TTL、序列化与反序列化钩子、增删替换和清空回调、命中统计及可注入时钟。`HotKeys` 仅由 ARC 实现。
