# 拒绝理由：HybridLog 与 ReadCache 环形页分配状态机去重

## 观点来源
next/hlog-readcache-alloc-dedup.md

## 拒绝原因
AI 认为 `wkv/src/read_cache/append.rs` 与 `whlog/src/hlog/append.rs` 存在大量的同构代码，建议将其下沉至通用的页分配器内核。经过详细核对代码，该观点不合理，直接拒绝。具体理由如下：

1. **底层机制差异巨大**：
   - `hlog/append.rs` 负责持久化存储。在换页时需要调用 `ensure_page_ready`、处理页落盘、推进 `read_only_address` 以及写入 `Pad` 进行页面对其。
   - `read_cache/append.rs` 纯内存缓冲，无需处理落盘，但需要维护 `page_inflight` 并发计数以实现闭环。在旧页面被覆盖时会调用 `cleanse_page` 驱逐并同步修改 `HashIndex`。

2. **C# 中存在重度耦合，不宜在 Rust 中效仿**：
   - 在 C# （Tsavorite）中，的确通过底层的 `AllocatorBase` 实现了内存与磁盘的通用分配，但导致该类长达 3000 行，内部各种 if/else 以及虚函数钩子极为冗杂。
   - 在 Rust 中，若强行抽象这两者，必须引入极其复杂的 Traits（例如针对页面清空、在途记录注册、并发重置策略的独立回调或泛型参数），会极大地破坏现有的零成本抽象、严重影响核心并发分配路径的性能与可读性。

3. **代码去重收益极低**：
   - 当前的 CAS 核心分段锁和循环各自约五十行代码，各自语义清晰、逻辑自洽。强行去重的结构化开销远大于维护两份微小重复代码的成本。

**结论**：保持当前 `read_cache` 与 `hlog` 分离的架构，不对二者的环形页分配代码做去重抽象。
