拒件：虚拟库号与逻辑库号裸 u64 混用，主张 LogicNs/VirtualNs 新类型强隔离

来源：next/agy.my.md 条 19。判定：不成立（架构偏好类加固，非缺口）。

拒绝原因
SKILL 数据库隔离条款明示「原生 u64 库 ID 纯寄存器标量更新，零堆分配」为设计基线；newtype 包装同为零开销，但本条无任何实证缺陷——是「防未来混淆」的风格加固，非「自定义点上下游没打通或实现错了」。实际发生的逻辑/物理域混用缺陷已各自实证立项本批三票：next/my-flush-replica-virtual-id-divergence.md（回放虚号分叉）、next/my-acl-logical-ns-compact-dead.md（ACL 逻辑 ns 直编码物理域）、next/my-vector-replay-slot-fallback.md（反查回退物理号）。全仓签名重构（wval/wkv/wnode/waof 跨四层）与在途票的合并冲突面远超收益；待上述三票落地后如仍有新增混用实例，再按实例评估局部 newtype，不做全仓预防性重构。

引证
wedb/wkv/src/vdb.rs（logic_ns/vns 裸 u64 签名全文件）；wedb/wkv/src/session/mod.rs struct StoreSession；garnet/libs/server/Databases/DatabaseManagerBase.cs（C# 侧 dbId 亦裸 int，靠命名约定区分）。
