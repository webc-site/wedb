主库检查点完成回调集群分支接线（对标 C# DatabaseManagerBase.InitiateCheckpointAsync）

来源：next/primary-checkpoint-cluster-callback.md（认领后已删）。判定：成立。

判据与 C# 依据
garnet/libs/server/Databases/DatabaseManagerBase.cs:498-542 InitiateCheckpointAsync 按 EnableCluster 二分：
- covered 取源：EnableCluster 时 StoreWrapper.clusterProvider.OnCheckpointInitiated（garnet/libs/cluster/Server/ClusterProvider.cs:192-208，PRIMARY 取尾地址 + UpdateCommitSafeAofAddress，REPLICA 取 ReplicationCheckpointStartOffset）；单机取 AOF TailAddress。
- 截断层：EnableCluster && EnableAOF 时 AddNewCheckpointEntry（ClusterProvider.cs:156-172 登记 CheckpointEntry 历史 + SafeTruncateAOF），SafeTruncateAOF（ClusterProvider.cs:175-189）PRIMARY 经 AofSyncDriverStore 按全副本最小已发位点回拉再删段；else 分支 TruncateUntil + Commit。

rust 现状（主仓 HEAD 实测）
- 内核 wedb/wnode/src/database/database_manager_base.rs:254-338 take_database_checkpoint_async：covered 无条件取 aof.tail_address（:274-287），拍后 aof.truncate_until_async(&covered) + commit_flush_async 无条件执行（:301-313），不分形态。头注 :232-254 已承诺两分形态，代码未实现。
- wedb 侧回调已在位且经测试但生产零接线：CheckpointCallbackFace::on_checkpoint_initiated（cluster_provider/traits.rs:115-128）、add_new_checkpoint_entry（:134-156，登记 + safe_truncate_aof）；add_new_checkpoint_entry 唯一生产调用挂 take_on_demand_checkpoint（cluster_provider/checkpoint.rs:132，副本 attach 链）。
- 类型面接不通：wnode 侧 ClusterProvider trait（cluster_provider.rs:17-168）无承接这两口的成员，内核持有的 Arc<dyn ClusterProvider> 无法下达。

采纳方案（一处机制，对标 C# 复杂度，不额外造轮子）
复用已在位的 CheckpointCallbackFace 实现，仅在 wnode 虚表补两口把内核接上，不新建第二套截断/登记路径。

1. wnode/src/cluster_provider.rs 的 ClusterProvider trait 补两口（形态同兄弟成员 checkpoint_version_shift_start/end，带单机默认）：
   - on_checkpoint_initiated(&self, covered: &mut AofAddress)，默认空实现（单机内核不触达）。
   - add_new_checkpoint_entry(&self, full, covered: AofAddress, store_token: u128, object_token: u128) -> Option<SlowFuture>，默认 None（异步截断口经既有 SlowFuture 擦除壳承载，与 flushall_broadcast 同形；单机内核不触达）。
   Arc<T> 转发 impl 同步补两条 Deref 转发。

2. wedb ClusterProvider 的 WnodeClusterProvider impl（cluster_provider/traits.rs）覆写两口，转发到同一 self 的 CheckpointCallbackFace：
   - on_checkpoint_initiated 直调 CheckpointCallbackFace::on_checkpoint_initiated。
   - add_new_checkpoint_entry 经 self_arc 取 owned Arc 构造 SlowFuture，内部 await CheckpointCallbackFace::add_new_checkpoint_entry（复用登记 + safe_truncate_aof，不改其内部）。

3. 内核 database_manager_base.rs take_database_checkpoint_async 按句柄在位与否二分：
   - covered 取源：cluster.get() 命中 → AofAddress::create(1,0) 后 cluster.on_checkpoint_initiated(&mut covered)；否则维持 AofAddress::create(1, aof.tail_address())。日志判定不变。
   - 截断层（aof 存在且快照已发布后）：cluster.get() 命中 → await cluster.add_new_checkpoint_entry(true, covered, token, token)（覆盖 C# EnableCluster && EnableAOF 分支，full 恒 true 对齐 rust 统一检查点模型，与 take_on_demand_checkpoint 口径一致）；否则保持 truncate_until_async + commit_flush_async（C# else 分支）。
   - publish_checkpoint_aof_address 保持在截断层前，用同一 covered（副本形态 covered 已是起始标记位点，口径自动对齐）。
   单机分支不变；不新增第二套截断。

4. 订正内核头注：把「AddNewCheckpointEntry 登记 + 安全截断 / 单机 TruncateUntil + Commit」由承诺改为已实装的描述（据代码事实）。

边界
- 与 next/replica-replay-checkpoint-end-arm.md 共用这一对回调面：本票只补主库 SAVE/BGSAVE/AOF 超限链的接线（触发口已在位），副本重放臂的触发口由该票另接，不改本票接线。先主后副。
- 截断的物理回收真身仍只有 truncate_until_async 单点（safe_truncate_aof 内部亦走它），不造第二套。

验证
worktree 内 cargo check -p wnode -p wedb（只 check，不跑 test.sh / clippy.sh）。

落地补记（2026-09-20，实施于 fix-primary-ckpt-callback，合并 8ca622b）
- 复核：票载行号在合并 dev（a4761f1）后全部成立，方案与 dev 无拓扑冲突。
- 实施中发现一处「两套机制」冲突并修正：第 3 条把 add_new_checkpoint_entry 挂上
  内核集群分支后，wedb take_on_demand_checkpoint（checkpoint.rs 拍后读 meta 再
  登记块）与内核走同一 take_checkpoint，生产一次按需检查点将双登记（重复
  CheckpointEntry + 重复 safe_truncate）。C# 登记唯一点仅 InitiateCheckpointAsync
  一处（StoreWrapper.TakeOnDemandCheckpointAsync 不重复登记），故删该 wedb 侧
  登记块，take_on_demand_checkpoint 收敛为纯内核转发；内核登记层级移出 aof 判空
  （对齐 C# EnableCluster && EnableAOF 段与 AppendOnlyFile 判空正交的层级，
  集群形态恒启 AOF，句柄在位即该合取式为真）。
- 测试对齐：on_demand_checkpoint_takes_and_registers_entry 补 attach_flush_gate
  （对齐 boot.rs 生产装配链形态位）；checkpoint_wiring 与 cluster_provider 两处
  回调直测改 UFCS 定点 CheckpointCallbackFace，杜绝 wnode 同名新口的解析漂移。
- cargo check -p wnode -p wedb 与 --tests 均通过零警告。
- 环境观察：全局 ~/.cargo/config.toml 的 target-dir=/tmp/_rs 为全部 worktree
  共享，并发代理会制造陈旧指纹假错（曾见 bulk_delete/SortedSetObject E0599
  抖动，隔离 CARGO_TARGET_DIR 后消失），主代理排查测试异常时可留意。
