# 拒绝：RMW 可变区回溯与窗口管理逻辑收敛

来源：next/wkv-rmw-traceback-unify.md

## 拒绝理由

1. **逻辑分离符合引擎架构（对标 C#）**：
   - C# 源码中 `FindRecord.cs` 明确分离了 `TryFindRecordForUpdate`（用于 Upsert/Delete 核心流程，处理复杂的锁、复活、缓存脱钩等）与 `TraceBackForKeyMatch`（仅用于 RMW 原位改写纯内存可变区的简单匹配回溯）。
   - 当前 Rust 实现 `inplace.rs` 中的回溯带有 `ephemeral` 桶锁、记录脱钩（Elision）和复活（Revivification）逻辑。而 `modify.rs` 中的 `trace_live_mutable_addr` 仅执行无锁的只读回溯探针，无需引入复杂的 Upsert 状态。这两者如果强行抽象合并，需要传入大量闭包或标志位，反而破坏代码直观性并增加开销。

2. **操作性质本质不同，不应收敛于单一实现**：
   - `inplace.rs` 执行的是**值替换（Upsert）**，调用底层 `hlog.try_update_in_place`，属于直接字节拷贝。
   - `modify.rs` 执行的是**闭包修改（RMW）**，调用底层的 `hlog.try_modify_record_in_place`（传递 `&mut [u8]` 供闭包就地计算）或 `hlog.try_grow_record_in_place`（带容量增长的就地修改）。
   - 三者的类型签名和后续通知逻辑完全不同，无法也没有必要收敛为所谓的“单一标准实现”。

3. **事实错误**：
   - 观点声称 `session/raw/write/rmw.rs` 与 `rmw_window.rs` 重复编写了回溯逻辑，但实际上 `rmw.rs` 和 `rmw_window.rs` 仅负责 TTL 裁决和排他闩（Latch）窗口管理，**完全没有任何可变区回溯代码**。

综合上述原因，强行收敛不仅会违背 `1:1 对标 C#` 的原则，还会引入不必要的过度抽象，因此拒绝此提议。
