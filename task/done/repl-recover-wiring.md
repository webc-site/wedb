# 复制恢复装配两条（repl-recover-wiring）

来源：next/ds.net.md（主代理预清理）。两条是同一装配链上下游，按 二（持久化接通）→ 一（位点回填）顺序做。

## 一、主端 --recover 后复制位点不回填复制域（成立）

对标核实（garnet/libs/cluster/Server/Replication/ReplicationManager.cs:537-563
RecoverCheckpointAndAOFAsync）：

1. C# 链：RecoverCheckpoint → RecoverAOF → GetRecoveredSafeAofAddress →
   InitializeIf → ReplayAOF（返回 replayedUntil = 重放后日志尾）→
   replicationOffset.SetValue(ref replayedUntil) → InitializeCheckpointStore。
   位点回填是宿主恢复链的固定尾段，gossip 广播、failover data-loss 判定、
   checkpoint covered 位点全部以此为基线。
2. rust 现状：main.rs run_async 里 open_recovered_with_aof 完成数据面恢复，
   重放结果仅 log::info（replayed 条目数）；rm.replication_offset 保持构造
   初值 kFirstValidAofAddress。--recover 重启后、首批新写入前，gossip 位点
   上告与 failover 基线全部虚假归零。
3. 回填值来源：重放后 AOF 尾地址。aof.log()（GarnetLog）与 wal 共享同一
   物理日志实例（single_log_aof 适配），GarnetLog::tail_address 返回
   waof::AofAddress，正是 rm.set_current_replication_offset 的入参形态
   （cluster_provider metrics 已同款消费 tail.diff(&offset)）。
4. C# EnableAOF 分支才回填；rust 对应 = open_recovered_with_aof 形态
   （node.recover && node.aof）才持有恢复位点，open_recovered（无 AOF）不
   回填，门控天然对齐。

改法：wnode open_recovered_with_config_and_aof 在 recover_aof 完成后取
aof.log().tail_address() 存 provider 字段 recovered_aof_tail（最小暴露面
+ getter）；main.rs 装配尾段 node.recover 时
rm.set_current_replication_offset(tail)。

## 二、复制历史恢复生产装配下空转，构造期门控偏离 C#（成立）

对标核实（ReplicationManager.cs:142-177 构造段 + ReplicationHistoryManager.cs）：

1. C# 构造：replicationConfigDevice = opts.CheckpointDir + "/cluster" 下的
   replication.conf；canRecoverReplicationHistory = fileSize > 0；
   Recover && fileSize > 0 → RecoverReplicationHistory()（读，损坏回退
   InitializeReplicationHistory），否则 InitializeReplicationHistory()
   （new + FlushConfig，即不 recover 时旧文件被新历史覆盖）；构造尾段
   SetPrimaryReplicationId()。
2. rust 现状三处偏离：
   - 生产装配 ClusterProvider::new() → ReplicationManager::new() →
     with_options(1, None)：无持久化目录，flush_config /
     recover_replication_history 空转，主端重启丢 replid 历史，副本被迫
     全量重同步。
   - with_options 在 config_dir Some 时无条件 recover_or_init，缺 --recover
     门控：不 recover 也读旧历史，偏离 C#（应 Initialize 覆盖）。
   - recover_async 里再调 recover_replication_history：C# RecoverAsync 不做
     历史恢复（构造期已做），属错位重复。
3. 装配时序：ClusterProvider::new() 在 main 早期无目录可用（C# 构造期即有
   CheckpointDir，rust 结构差异）；wire_replication_data_plane /
   set_aof / set_commit_channel 均从 cluster.replication_manager() 取句柄
   挂资产，rm 重建必须先于这些注入。装配点在 run_async 闭包 provider
   open 之后立即重建 rm（此时无连接，替换安全），目录取
   provider.checkpoint_dir.join("cluster")（对标 CheckpointDir/cluster），
   子日志数取 runtime_options.aof_physical_sublog_count（对标
   storeWrapper.serverOptions.AofPhysicalSublogCount）。
4. recover_or_init 的「读失败回退 new + flush」与 C# RecoverReplicationHistory
   的 catch → InitializeReplicationHistory（含 FlushConfig）等价，保留；
   fileSize > 0 门控在 with_options 内用 fs::metadata 判定。

改法：

1. replication_manager.rs
   - with_options 签名扩为 (sublog_count, config_dir, recover)；构造体先以
     空历史起底，随后执行 C# 构造门控：recover && 文件 size > 0 →
     recover_replication_history()，否则 initialize_replication_history()；
     尾段 set_primary_replication_id()（对标构造尾段）。
   - new()/Default 改 with_options(1, None, false)。
   - recover_async 删 recover_replication_history 调用（历史恢复归构造期
     门控），保留 is_primary → initialize_checkpoint_store；注释同步改写。
2. cluster_provider.rs
   - initialize_replication_manager 改为带参重建（sublog_count, config_dir,
     recover），无条件替换 rm（语义 = C# 构造期以 CheckpointDir 初始化
     ReplicationManager；装配期一次调用）。
3. main.rs
   - provider open 之后立即 initialize_replication_manager(sublog,
     Some(checkpoint_dir/cluster), node.recover)（先于 set_aof /
     wire_replication_data_plane）。
   - recover_async 调用前（对齐 C# SetValue 先于 InitializeCheckpointStore
     的顺序）回填位点：node.recover 且 recovered_aof_tail 在场时
     rm.set_current_replication_offset(tail)。
4. wnode service.rs
   - StorageSessionProvider 增 recovered_aof_tail: Option<AofAddress> 字段
     + getter；open_recovered_with_config_and_aof 在 recover_aof 后取
     aof.log().tail_address() 填入。

## 测试

1. replication_manager.rs 单测：
   - 门控三分支：recover=true + 旧文件 → replid 历史跨重启保留；recover=false
     + 旧文件 → 新 replid 且文件被覆盖；recover=true + 无文件 → 新历史落盘。
2. wnode recover_test.rs：recover_checkpoint_and_aof_after_restart 第二代
   provider 断言 recovered_aof_tail 与 aof.log().tail_address() 一致且大于
   初始位点（--recover 后位点回填值真实）。
3. wedb 侧装配面：replication_stream_e2e.rs / cluster_replication_session.rs
   的 initialize_replication_manager() 调用随新签名改写。

## 验收口径

1. ./clippy.sh 零警告（禁 allow）。
2. ./test.sh 全过。
3. bun ./js/check.js 无新增缺失/重复。
4. 行为口径：--recover 重启后 rm 位点 = 重放后 AOF 尾（gossip/failover 基线
   真实）；replid 历史经 checkpoint_dir/cluster/replication.conf 跨重启保留；
   非 --recover 启动按 C# 语义初始化新历史。

## 边界与冲突

- 不动位点推进时序（enqueued 语义归背景重放待办），仅宿主恢复链尾段回填。
- 并发代理在改 replication_manager.rs（异步重放）与 cluster_provider（指标），
  本任务改动集中在 with_options / recover_async / initialize_replication_
  manager / main.rs 装配段，冲突以先合并者为准。
- 预计 js/check/ignore 无新增（无删除 C# 对标符号）。

## 验证结果

1. 实现形态（commit e4252ce，8 文件 +178 -41）
   - wedb/wedb/src/server/replication/replication_manager.rs
     - with_options 签名扩为 (sublog_count, config_dir, recover)：构造体以
       空历史起底后执行 C# 构造门控——recover && replication.conf size > 0
       走 recover_replication_history，否则 initialize_replication_history
       （new + FlushConfig，不 recover 时旧文件被新历史覆盖）；尾段
       set_primary_replication_id。new()/Default 改 (1, None, false)。
     - recover_async 删错位的 recover_replication_history 调用（历史恢复归
       构造期门控，对标 C# RecoverAsync 不做历史恢复），保留 is_primary →
       initialize_checkpoint_store；差异登记注释同步改写。
     - recover_replication_history 注释登记调用方契约（仅构造期门控与测试）。
   - wedb/wedb/src/server/cluster_provider.rs
     - initialize_replication_manager 改带参重建 (sublog_count, config_dir,
       recover)，无条件替换 rm；注释登记与 C# 构造期的结构差异（rust
       ClusterProvider::new() 时数据目录未知，默认实例先行 + 装配期重建，
       须先于 set_aof / wire_replication_data_plane 等挂 rm 资产的注入）。
   - wedb/wedb/src/main.rs
     - provider open 后立即重建 rm：目录取 provider.checkpoint_dir.join(
       "cluster")（对标 C# CheckpointDir/cluster/replication.conf），子日志
       数取 runtime_options.aof_physical_sublog_count（对标
       serverOptions.AofPhysicalSublogCount），recover 传 node.recover。
     - 装配尾段 node.recover 时位点回填：rm.set_current_replication_offset(
       provider.recovered_aof_tail())（对标 replicationOffset.SetValue(
       replayedUntil)，先于 recover_async 的 InitializeCheckpointStore，对齐
       C# 顺序）；无 AOF 形态 tail 为 None 跳过（对标 EnableAOF 门控）。
     - runtime_options 投影提前单次构建，复制域装配族共用。
   - wedb/wnode/src/service.rs
     - StorageSessionProvider 增 recovered_aof_tail: Option<AofAddress> 字段
       + getter；open_recovered_with_config_and_aof 在 recover_aof 后取
       aof.log().tail_address() 填入（= C# ReplayAOF 返回的 replayedUntil）。
   - 测试
     - replication_manager.rs：test_with_options_replication_history_gate
       门控三分支（failover 轮转后 recover=true 跨重启保留 replid +
       replid2 + offset2；recover=false 新历史覆盖旧文件并复核；空目录
       recover=true 初始化落盘 replication.conf）。
     - wnode/tests/recover_test.rs：recover_checkpoint_and_aof_after_restart
       第二代断言 recovered_aof_tail == aof.log().tail_address() 且越过
       FIRST_VALID_AOF_ADDRESS（--recover 位点回填值真实）。
     - 既有 with_options / initialize_replication_manager 调用点随新签名
       改写（replication_stream_e2e / replication_pipeline /
       cluster_replication_session / replication_manager 内部测试）。
2. js/check/ignore 无需新增：无删除 C# 对标符号，check.js 保持 0 缺失/重复。
3. ./clippy.sh：0 警告（-D warnings，禁 allow 全程未用）。
4. ./test.sh：2055 过 1 跳过（既有口径）+ regress 门 2 过 2。
5. 复制链路测试：replication_pipeline 5/5、replication_stream_e2e 3/3、
   cluster_replication_session 3/3、cluster_replication 1/1、
   recover_test 4/4（含新增门控测试与位点回填断言）全过。
6. 合并：分支基于 dev HEAD 4c8658a（无并发新提交可并），主目录 merge
   w5-replrec。
7. 遗留登记（非本任务范围）：生产 bin 冒烟被既有缺陷阻塞——ServerBootstrap
   （wnode/src/server.rs:214）第 2 步在 compio runtime 之外调用
   cluster_provider.start() → gossip spawn panic（"not in a compio
   runtime"）；dev 基线 bin 同款复现，非本次引入。装配链行为由单测与
   集成测试覆盖验收。
