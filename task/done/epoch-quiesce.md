配置纪元过渡全会话静止机制（epoch-quiesce）

甄别结论

成立。对照 garnet 逐点核实：

1. libs/cluster/Server/ClusterProvider.cs:366 BumpAndWaitForEpochTransitionAsync
   在 bump 后遍历 storeWrapper.Servers 全部活跃集群会话，自旋重试至每个
   会话 LocalCurrentEpoch 追平（entryEpoch == 0 视为空闲放行）。rust 版
   cluster_provider.rs 的同名函数只是 bump + yield_now 即返 true，无静止。

2. libs/server/Resp/RespServerSession.cs:490 批首 clusterSession?.AcquireCurrentEpoch()，
   :576 finally clusterSession?.ReleaseCurrentEpoch()；接口
   libs/server/Cluster/IClusterSession.cs:60/66/72 声明 LocalCurrentEpoch /
   AcquireCurrentEpoch / ReleaseCurrentEpoch。rust 侧
   wnode/src/cluster_session.rs 的 ClusterSessionFace 三者皆缺。

3. 命令侧四处 C# 均 AsyncUtils.BlockingWait(UnsafeBumpAndWaitForEpochTransitionAsync())：
   ReplicaOfCommand.cs:48（REPLICAOF NO ONE）、
   RespClusterSlotManagementCommands.cs:493（SETSLOT）、:593（SETSLOTSRANGE）、
   RespClusterFailoverCommands.cs:128（FAILSTOPWRITES）。rust 侧
   wedb/src/server/cluster_session.rs 对应四点只调 bump_current_epoch 不等待。

4. failover 三点（PrimaryFailoverSession.cs:114、ReplicaFailoverSession.cs:136/:161）
   rust failover_session.rs 已调用 async 版 bump_and_wait，真实现后自动获得
   静止语义，调用点无需改动。

不改的决策（对比意见原文）：

- 快照原子量用 i64 而非意见中的 u64：provider.garnet_current_epoch 已是
  AtomicI64（C# 亦为 long），全链路一处类型。
- 会话表面不在 ConsumerRegistry 上再造：该注册表刻意只存元数据镜像
  （会话体连接任务独占）。活跃集群会话弱引用表挂在 provider 上，
  create_cluster_session 工厂即注册点，一处定义（C# 的 activeHandlers
  归属服务器，rust 会话体不可跨线程直读，弱引用表是等价最小承接）。
- 不做「修正注释声明差异」下策，真实实现静止语义。

范围外仅记录不改（并发代理在改 gossip_manager.rs / migration/ / wconn/ /
replication/）：C# MigrationDriver.cs:160、MigrateSessionKeyAccess.cs:20、
ReplicaDisklessSync.cs:49、ReplicaDiskbasedSync.cs:55 也有 epoch 静止调用，
对应 rust 迁移/复制链路本任务不接线。

对标与改动点

A. wnode/src/cluster_session.rs
   - ClusterSessionFace 增三方法（对标 IClusterSession.cs，无默认实现）：
     local_current_epoch / acquire_current_epoch / release_current_epoch。
   - ClusterSessionVtable 与句柄增 acquire/release 两槽（provider 经具体
     类型读快照，local 不进虚表）。

B. wedb/wedb/src/server/cluster_session.rs
   - ClusterSession 增 local_current_epoch: AtomicI64（初值 0）。
   - 三 trait 方法对标 ClusterSession.cs:185/186 与 IClusterSession.cs:60。
   - 增 unsafe_bump_and_wait_for_epoch_transition（ClusterSession.cs:191
     UnsafeBumpAndWaitForEpochTransitionAsync 的同步批内形态：
     Release → provider 阻塞 bump_and_wait → Acquire）。
   - 四个命令调用点（network_replicaof NO ONE / setslot / setslotsrange /
     failstopwrites）改调上述方法。

C. wedb/wedb/src/server/cluster_provider.rs
   - 增 cluster_sessions: RwLock<Vec<Weak<ClusterSession>>> 活跃会话弱引用表，
     create_cluster_session 构造后注册。
   - bump_and_wait_for_epoch_transition_async 真实现：bump 取最新纪元 →
     yield_now().await 自旋轮询全部活跃会话快照（0 或 >= current 视为追平），
     cluster_node_timeout_ms 为上限，超时返 false（调用方按 C# 同款忽略
     返值放行，返值表达静止是否达成）。
   - 增同步形态 bump_and_wait_for_epoch_transition（C# 命令侧
     AsyncUtils.BlockingWait 语义）：thread::yield_now 自旋。单线程每核下
     同线程会话必处批外（快照 0），阻塞只等他核会话收尾，无死锁。
   - 增 all_sessions_caught_up：写锁下 retain 过期弱引用（会话亡即出表，
     免注销钩子），存活者快照判定。

D. wnode/src/resp/resp_server_session.rs
   - try_consume_messages 与 try_consume_pending 批首 acquire、全路径批尾
     release（对标 RespServerSession.cs:490/:576 try/finally）：原体改名
     inner，外壳统一取放。

E. wnode/src/resp/resp_server_session.rs 测试桩 StubClusterSession
   - 补三 trait 方法（AtomicI64 存取，行为真实可断言）。

验收口径

1. ./clippy.sh 零警告，禁 allow。
2. ./test.sh 全过。
3. bun ./js/check.js 无新增缺失（新符号带 libs/...:函数名 映射注释，
   预计无需新增 ignore）。
4. 行为口径：failover / setslot / setslotsrange / replicaof / failstopwrites
   触发的纪元推进，等待全部活跃集群会话当前批收尾（快照追平或清零）后
   才返回；批消费期会话持快照、批外为 0。

验证结果

1. ./sh/clippy.sh 零警告（rc=0，无 error/warning），禁 allow，全程未用。
2. ./test.sh wedb 子工作区 2002 过 2002（1 跳过为既有口径），regress 门 2 过 2。
3. bun ./js/check.js rc=0 无缺失；check.js ignoreLoadAndPrune 自动剪除
   cluster.yml 的 UnsafeBumpAndWaitForEpochTransitionAsync 与整份
   libs_cluster_Session.yml（AcquireCurrentEpoch）——实现落地后 ignore 已无必要。
4. 行为口径达成：failover（begin/takeover 两点）、SETSLOT、SETSLOTSRANGE、
   REPLICAOF NO ONE、FAILOVER STOPWRITES 触发的纪元推进均等待全部活跃
   集群会话批收尾（快照追平或清零）后才返回；消费批首取快照、批尾（含
   协议违规/致命断流路径）清零。

合并记录：分支 w2-epoch-quiesce（5 提交）先并 dev 无冲突，再并入主目录
dev（a63de75）；worktree 与分支已清理。
