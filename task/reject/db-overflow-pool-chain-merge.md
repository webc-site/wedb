拒绝原因：审查确认为现状良好——溢出池与桶链无重复，不合并不立项

来源：next/muse.db.md 条 18（溢出池与桶链 coupled 紧但无重复）。

原主张：OverflowPool 两级分块加 Treiber 栈、ChainWalker 在 chain.rs、桶 latch 在
bucket.rs，跨文件跳转多但无重复，不合，双入口保持，热路径只调 unchecked 并写明
挂载不变量。

取证（主仓 dev 当下代码）：
- wedb/windex/src/overflow_pool.rs:21 pub struct OverflowPool、:190 unsafe fn
  get_unchecked（doc 已带不变量说明）；wedb/windex/src/chain.rs:35
  pub(crate) struct ChainWalker；桶闩在 wedb/windex/src/bucket.rs。
- get / get_unchecked 双入口是「检查版/信任版」惯用形态，C# 对标
  MallocFixedPageSize.cs（garnet/libs/storage/Tsavorite/cs/src/core/Allocator/
  MallocFixedPageSize.cs）同为池 + 桶链结构，无重复实现可指认。

结论：正确确认条，无待办。热路径 unchecked 调用点的「挂载不变量写明」若个别缺失，
属 code review 微项，随相关文件认领时顺手补，不单独立项。
