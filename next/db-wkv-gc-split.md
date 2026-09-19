优先级：中
来源：next/agy.db.md 条 11 立项。取证基线：主仓 dev 当下代码。

问题
wkv gc.rs 841 行多维度后台回收混杂：VDB 换号物理回收、TTL 过期键扫描、日志自适应
紧缩触发、BfTree 树注销排空、GcManager 驱动循环五域同文件；文件末尾 :805 起内联
cfg(test) mod tests 测试残留。

取证
- wedb/wkv/src/gc.rs:213 pub struct GcManager<D>、:258 GcManager::spawn -> GcHandle、
  :304 drive（驱动循环）、:391 sweep_vdb（换号回收）、:450 sweep_expired（TTL 扫描）、
  :604 try_compact（紧缩触发，:657 调 store.compact）、:757 pub struct GcHandle、
  :144 spawn_bftree_reclaimer；:805 mod tests 内联。
- 后台紧缩生产链：gc.rs:657 -> wedb/wkv/src/compact.rs:265 WedbStore::compact ->
  wcompact LogCompactor::compact_with_filter（见 next/db-compact-entry-single-chain.md）。
- C# 对标：garnet/libs/server/StoreWrapper.cs ReconcilePrimaryTask（任务生命周期）、
  garnet/libs/server/Storage/Functions/GarnetRecordTriggers.cs IsDeleted（死亡判定）、
  garnet/libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs（紧缩）——
  C# 侧任务编排 / 判定 / 紧缩分属三处，无单文件混聚形态。
- 撞车警示：next/design-vdb-file-split.md 将 vdb.rs 拆出 vdb/gc.rs（GcDeadLog 等纯
  数据结构）；本票把 src/gc.rs 拆为 src/gc/ 目录——两票会新同名模块 gc，认领时
  统一规划模块路径（如本票拆为 gc/manager.rs、gc/vdb_sweep.rs、gc/ttl.rs、
  gc/compact_trigger.rs、gc/reclaim.rs），避免目录与文件同名冲突。

修法建议
按 gc/manager.rs（GcManager/GcHandle/drive）、gc/vdb_sweep.rs、gc/ttl.rs、
gc/compact_trigger.rs、gc/reclaim.rs（bftree 排空）拆解，gc/mod.rs 门面；
:805 测试剥离至 wkv/tests/；collect_expired 内核与 ExpiredKeySet 保持单点
（keyspace.rs expired_key_deletion_scan 与命令 EXPDELSCAN 共用，见
wedb/wkv/src/store/keyspace.rs:53 注释），拆文件不拆内核。
