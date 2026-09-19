拒绝原因：与 C# 行为一致且快照机制已在场；「寄存器级原子快照」属自创优化，
与 transpile SKILL「尽量 1:1 对标 C#，不要实现自己的优化」冲突

来源：next/agy.db.md 条 12（whlog AddressManager 读路径缺乏寄存器级原子快照）。

原主张：is_mutable / is_read_only / is_in_memory 每判定多次 AtomicU64 load、snapshot()
无锁顺序读 7 字段可能撕裂，应在批读循环前取局部快照、热路径用快照内寄存器值直算。

取证（主仓 dev 当下代码）：
- 各判定均 load 后委托单点真源：wedb/whlog/src/address.rs:48 is_mutable（3 load）、
  :57 is_read_only（2 load）、:65 is_in_memory（2 load）——判定体唯一真源在
  AddressSnapshot::region_mutable 等（:310-:322 const fn），无重复实现。
- 快照机制已在场且被消费：address.rs:187 pub fn snapshot() -> AddressSnapshot（7 字段
  顺序读），消费点 wedb/wkv/src/vdb.rs:733/:866/:888/:1056 等——需要跨字段一致的
  调用方已经在用快照。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs 的
  IsMutable/IsReadOnly 系同样逐字段 volatile 读（无全局一致快照、无原子包读取）；
  HybridLogConfig.cs 无快照形态。区域判定容忍字段间撕裂在 C# 与 rust 语义一致
  （边界单调推进，撕裂只致保守判定不致错判）。

结论：现状即 C# 形态；进一步做「快照缓存 + 寄存器直算」是 rust 侧自创优化，
无正确性收益，按 transpile 口径不立项。
