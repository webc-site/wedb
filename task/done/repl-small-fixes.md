# 复制域三条小修（repl-small-fixes）

来源：next/ds.net.md 条 10、15 与 next/glm.md 条 4（主代理预清理）。三条相互独立，逐条甄别，全部成立。

## 一、FastAofTruncate 断点重对齐缺失（成立，补分支）

对标核实（garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplaySession.cs:54-90）：

1. C# ProcessPrimaryStream 在 divergent 校验前有 FastAofTruncate 分支：
   currentAddress > previousAddress（跳跃帧）且跳跃无法被 enqueue 自动吸收时
   （跳跃终点非页边界，或跳跃距离 >= recordLength），SafeInitialize 把副本
   本地 AOF 地址空间重置对齐到 currentAddress，等向量操作完成后
   SetSublogReplicationOffset(currentAddress)，随后正常落盘衔接。
2. 跳跃帧的来源：主端 FastAofTruncate 截断（AofSyncDriverStore.cs:53/107/161
   UnsafeShiftBeginAddress snapToPageStart + truncateLog，checkpoint 提交后
   立即物理截断不等待副本消费），活跃推流迭代器跨被截断区段产生地址跳跃。
3. C# 帧语义（AofSyncTask.cs:Consume）：帧携带 (previousAddress,
   currentAddress, nextAddress)，previousAddress = 上一帧 nextAddress；正常流
   previousAddress == currentAddress（判定恒假跳过），跳跃帧 currentAddress >
   previousAddress 才进入重对齐判定。
4. rust 侧对应面现状：fast_aof_truncate 选项已存在（wconf 默认 false 与 C#
   GarnetServerOptions.cs:400 一致），且已在主端消费（determine_resync_strategy
   的 rep_tail < ckpt_begin 放行、garnet_append_only_file.rs:301 同款判定）；
   但副本接收面（process_primary_stream）tail != current_address 一律
   Divergent 断流——fast_aof_truncate = true 时主端截断推流必然断流，选项
   在副本侧半成品，转写不完整。
5. rust WalLog 连续字节编址无页概念：C# 「页边界 + 短跳」的 enqueue 自动吸收
   情形（跳跃终点恰为副本当前页边界，记录跨页 append 自动落到正确地址）在
   连续编址下不存在——任何 currentAddress > tail 的跳跃均无法自动衔接，统一
   显式 safe_initialize 重对齐（C# 显式分支的保守超集，行为正确）。

改法：补跳跃重对齐分支。process_primary_stream 在 cannot_stream_aof 校验后、
tail 读取前插入：provider.fast_aof_truncate() && current_address > previous_address
时 wal.safe_initialize(current_address, current_address) + 位点推进至
current_address，随后照常 divergent 校验（对齐后 tail == current_address 通过）。
C# 的 WaitForVectorOperationsToComplete：rust 向量域无重放期在途操作面，注释
登记差异。provider 增 fast_aof_truncate 原子量访问面（对标
clusterProvider.serverOptions.FastAofTruncate），装配期自 RuntimeServerOptions
注入。补断点重放回归测试。

## 二、ensure_replication 心跳刷新点偏移（成立，移挂点）

对标核实：

1. C# UpdateLastPrimarySyncTime 全部调用点（grep 全仓）：仅
   ReplicaDiskbasedSync.cs:293（TryReplicaDiskbasedRecovery，副本 checkpoint
   恢复开始）与 ReceiveCheckpointHandler.cs:53/86/107（checkpoint 接收三阶段）
   ——全部是「从主端同步数据建立」时刻。
2. C# EnsureReplication（ReplicationManager.cs:182-270）本体不刷新心跳：节流
   通过后走 PreventRoleChange + Task.Run 重连，心跳在重连链的恢复面才推进。
3. rust 现状：cluster_provider.rs ensure_replication 在节流判定通过后立即
   update_last_primary_sync_time——健康副本（有活跃复制流）只要轮询开启，
   每 poll_frequency 秒必然刷新，last_primary_sync_seconds 沦为轮询节拍，
   表征不出失联时长。
4. rust 挂点选择：checkpoint 接收/恢复面未转写，副本当前唯一「与主端同步
   建立」事件是 APPENDLOG 初始化帧握手成功（initialize_replica_replay_driver
   注册 = C# IsReplicating 置位同源事件，重连链 recover_replication 成功后
   主端建连即触发此帧）。挂此处，语义 = 自上次与主端建立同步的时长。

改法：ensure_replication 删除节流后刷新；process_append_log 初始化帧分支
注册成功后 update_last_primary_sync_time。注释登记与 C# 的差异（checkpoint
接收面落地后应追加其恢复点刷新）。既有测试 ensure_replication_gate_chain
的心跳断言随新语义改写。

## 三、副本重连超时默认值与注释双错（成立，默认改 0 + 注释修正 + 装配链接通）

对标核实：

1. C# 真实配置项：ServerOptions.ClusterReplicationReestablishmentTimeout，
   默认 0（defaults.conf:527；EnsureReplication pollFrequency == 0 直接
   return 禁用；GarnetServerConfigTests.cs:1050 断言默认 0）。
2. rust main.rs 注释引用的「C# ClusterConfig.ReplicationPollFrequencySeconds」
   全仓 grep 不存在，是伪造映射；常量值 1 亦与 C# 默认 0 不符——双错。
3. wconf 配置面已 1:1：RuntimeServerOptions.cluster_replication_reestablishment_timeout
   默认 0（runtime_server_options.rs:99，测试断言同），RuntimeServerConfig
   seed 链已播种该键。main.rs 常量 1 无条件覆盖注入，撞掉 --config 用户值
   （config-entry 刚接入 --config，配置值可达 RuntimeServerOptions 但无
   消费链把它播进 cluster provider）。

改法：删除 REPLICATION_REESTABLISHMENT_TIMEOUT_SECS 常量；main.rs 装配段改
从 node.runtime_server_options().cluster_replication_reestablishment_timeout
注入（对标 C# runtimeConfig.GetInt(ServerConfigType.
CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT) 装配语义：默认 0 = 禁用，
--config 可设）。装配链核查结论：with_runtime_server_options 只播种
slowlog 与 RuntimeServerConfig，不触碰 cluster provider，改动无撞点。

## 改动点

1. wedb/wedb/src/server/cluster_provider.rs
   - 增 fast_aof_truncate: AtomicBool + set_fast_aof_truncate /
     fast_aof_truncate（对标 serverOptions.FastAofTruncate 装配期注入）。
   - ensure_replication 删除节流后 update_last_primary_sync_time 调用与
     对应步骤注释。
2. wedb/wedb/src/server/replication/cluster_replication_session.rs
   - process_append_log 初始化帧注册成功后 update_last_primary_sync_time。
   - process_primary_stream 增 FastAofTruncate 跳跃重对齐分支（连续编址
     收敛说明 + 向量等待差异登记）。
3. wedb/wedb/src/main.rs
   - 删除 REPLICATION_REESTABLISHMENT_TIMEOUT_SECS 常量。
   - 重连轮询频率改自 RuntimeServerOptions 注入；增 fast_aof_truncate 注入。
4. 测试
   - cluster_replication.rs：ensure_replication_gate_chain 心跳断言改写为
     新语义（节流通过不刷新心跳，同步建立才刷新）。
   - cluster_replication_session.rs：补 FastAofTruncate 跳跃重对齐回归
     （跳跃帧重对齐后落盘衔接、位点推进、开关关闭时同帧仍 Divergent）。
5. js/check/ignore：预计无需新增（无删除 C# 对标符号）。

## 验收口径

1. ./clippy.sh 零警告（禁 allow）。
2. ./test.sh 全过。
3. bun ./js/check.js 无新增缺失/重复。
4. 行为口径：fast_aof_truncate 开启时跳跃帧重对齐续流不断流；关闭时保持
   Divergent 断流。ensure_replication 不再制造虚假心跳，心跳仅随同步建立
   推进。重连轮询频率默认 0（禁用）对齐 C#，--config 可覆盖。

## 验证结果

1. 实现形态（commit c0e4cc8，5 文件 +146 -24）
   - wedb/wedb/src/server/replication/cluster_replication_session.rs
     - process_primary_stream 增 FastAofTruncate 跳跃重对齐分支（C# 54-74
       行对标）：fast_aof_truncate 开启且 current_address > previous_address
       时 safe_initialize 对齐跳跃点 + 位点推进，随后 divergent 校验续流；
       连续编址无页自动吸收的差异与向量等待面缺失均注释登记。
     - process_append_log 初始化帧注册成功后 update_last_primary_sync_time
       （同步建立挂点，与 C# IsReplicating 置位同源；checkpoint 接收面落地
       后应追加其恢复点刷新）。
   - wedb/wedb/src/server/cluster_provider.rs
     - 增 fast_aof_truncate: AtomicBool + set_fast_aof_truncate /
       fast_aof_truncate（对标 serverOptions.FastAofTruncate 读取面）。
     - ensure_replication 删除节流后心跳刷新，判定链步骤号 7 归位 6，文档
       注释登记心跳口径（对标 C# EnsureReplication 本体无刷新）。
   - wedb/wedb/src/main.rs
     - 删除 REPLICATION_REESTABLISHMENT_TIMEOUT_SECS 常量（值 1 错 + 引用
       不存在的 C# 符号 ClusterConfig.ReplicationPollFrequencySeconds 双错）。
     - 重连轮询频率改自 RuntimeServerOptions 注入（默认 0 = 禁用对齐 C#
       defaults.conf:527；--config 可覆盖，config-entry 装配链已核无撞点）。
     - 增 fast_aof_truncate 装配注入。
   - 测试：cluster_replication.rs gate_chain 心跳断言改写为「重连轮询不得
     制造虚假心跳」；cluster_replication_session.rs 补
     cluster_replication_session_fast_aof_truncate_realignment 回归（稳态
     衔接 → 跳跃帧重对齐续流 → 位点推进 → 开关关闭同帧 Divergent 断流）。
2. js/check/ignore 无需新增：无删除 C# 对标符号，check.js 保持 0 缺失。
3. ./sh/clippy.sh：0 警告（含修复 doc_lazy_continuation 列表段落分隔一处），
   禁 allow 全程未用。
4. ./test.sh：2028 过 2028（1 跳过为既有口径）+ regress 门 2 过 2。
5. 复制链路测试：复制域 46/46 全过（含新增回归与改写的 gate_chain）。
6. bun ./js/check.js：exit 0，无新增缺失/重复。
7. 合并：分支内先 merge dev（07c04da，无冲突，合并后复制域 46/46 与
   clippy 复验通过），主目录 merge w4-repl-fixes 干净合入（仅 5 个目标
   文件），worktree 与分支已清理。
