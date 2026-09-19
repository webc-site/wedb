运行时配置热更消费面断链四件：replica-sync-delay / cluster-node-timeout / aof-sync-max-lag-bytes / aof-size-limit-enforce-frequency

来源：glm.net 第 2 条 + glm.design 第 4 条 a/b/c（同族四旋钮，合并一票，四事实均不同件）。
取证基线：主仓 HEAD 1b944517，行号为当下实况。

共同形态
wconf 槽位注册为运行期可 SET、CONFIG GET 回显新值，但生产消费面读的是装配期快照或任务启动期固化值，
改值「回显成功、行为不变」。同族已修先例两件可抄：aof-tail-witness-freq 票（消费循环改每轮
try_runtime_config 现取，ClusterProvider 的 runtime_config 可达面 :915 set_runtime_config /
:920 try_runtime_config 即该轮落地）与 task/ing/wkv-compaction-runtime-config-wiring 归档票
（ConfigReconcile::CompactionMaxSegments 推送引擎 GcConfig，每轮重读）。

四件现状（逐件取证）
a) replica-sync-delay
   - 槽位在位：/Users/z/git/db/wedb/wedb/wconf/src/runtime_server_config.rs:146 槽号注释、
     :414 NAME_LOOKUP 名 `replica-sync-delay`、:492 可 Set 名单、:581-582 播种值
     （默认 5ms，断言见 /Users/z/git/db/wedb/wedb/wconf/tests/garnet_server_config_tests.rs:92）。
   - 消费面硬编码：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_replay_task.rs:51
     `pub(crate) const REPLICA_SYNC_DELAY: Duration = Duration::from_millis(5)`，
     流耗尽空转两处直取常量：同文件 :141 与 :200 `sleep(REPLICA_SYNC_DELAY).await`。
   - C# 消费面现取：/Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:314
     `BulkConsumeAllAsync(this, runtimeConfig.GetInt(ServerConfigType.REPLICA_SYNC_DELAY), ...)`。
   - 全仓仅一处消费点：主端推流泵 AofSyncTask 侧与恢复侧（RecoverLogDriver.cs:205 对位形态）
     在本仓已各自改为事件驱动 / barrier 模型，不产第二消费点，改一处即闭环。
b) cluster-node-timeout
   - 可 SET：/Users/z/git/db/wedb/wedb/wconf/src/runtime_server_config.rs:410-413
     （含 `cluster-timeout` 兼容别名 :413），但该槽无 update_action，CONFIG SET 只落槽、
     不产 ConfigReconcile（枚举全集见 /Users/z/git/db/wedb/wedb/wconf/src/config_meta.rs:21-47，无该形态变体）。
   - provider 侧唯一写点是启动播种：/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs:418
     set_cluster_node_timeout_ms，全仓唯一调用 /Users/z/git/db/wedb/wedb/wedb/src/server/boot.rs:177（命令行 args）。
   - 消费面读该原子槽：/Users/z/git/db/wedb/wedb/wedb/src/server/gossip/gossip_manager.rs:141、
     /Users/z/git/db/wedb/wedb/wedb/src/server/failover/failover_manager.rs:83、
     cluster_provider.rs:732/:751、gossip/node_connection.rs:55、cluster_manager.rs:318 等，
     故 CONFIG SET 后这些臂读到的恒为启动快照（failover_manager.rs:78-79 注释还自称
     「set_cluster_node_timeout_ms 可动态改」，实为断链，属误导面）。
   - C# 每轮现取：/Users/z/git/db/wedb/garnet/libs/cluster/Server/Gossip/Gossip.cs:25、
     /Users/z/git/db/wedb/garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:90、
     /Users/z/git/db/wedb/garnet/libs/cluster/Server/Failover/FailoverManager.cs:24
     三处均 runtimeConfig.GetTimeSpan/GetInt(CLUSTER_NODE_TIMEOUT)。
c) aof-sync-max-lag-bytes
   - 写侧齐：/Users/z/git/db/wedb/wedb/wconf/src/runtime_server_config.rs:869-880
     apply_aof_sync_max_lag_update 产出 ConfigReconcile::AofSyncMaxLag（变体定义 config_meta.rs:35-36）。
   - 落点是空臂：/Users/z/git/db/wedb/wedb/wnode/src/config_owner.rs:60
     `ConfigReconcile::AofSyncMaxLag { .. } => {}`，:55-59 注释自述理由
     「桥内无日志句柄域可推送」。
   - 目标口在位却无人调：/Users/z/git/db/wedb/wedb/wnode/src/aof/aof_backpressure.rs:93 set_budget，
     除 :88 构造期 attach 外全仓零调用。
   - C#：updateAction 注册见 /Users/z/git/db/wedb/garnet/libs/server/Config/RuntimeServerConfig.cs:168-171
     （注释明写「pushes a CONFIG SET straight into the live gate — no restart, no lifecycle task」），
     实现 /Users/z/git/db/wedb/garnet/libs/server/StoreWrapper.cs:1043-1047 ApplyAofSyncMaxLagBytes
     → 逐库 `AppendOnlyFile?.backpressure?.SetBudget(newValueBytes)`。
d) aof-size-limit-enforce-frequency
   - 槽位：/Users/z/git/db/wedb/wedb/wconf/src/runtime_server_config.rs:280-287
     `ConfigMeta::runtime(... None, ...)`（运行期可 SET、无 update_action）。
   - 消费面一次性固化：/Users/z/git/db/wedb/wedb/wnode/src/service.rs:516 spawn_aof_size_limit_task
     在 :521 `let interval = Duration::from_secs(frequency_secs.max(1))` 捕获，循环体 :525-531
     只 `sleep(interval)`，永不回读槽位；freq 来源为装配期
     /Users/z/git/db/wedb/wedb/wnode/src/service.rs:1442 with_aof_size_limit(limit, node.aof_size_limit_enforce_frequency_secs)
     与惰性首启 :1404-1408。
   - C# 每轮现取：/Users/z/git/db/wedb/garnet/libs/server/StoreWrapper.cs:648-656
     `while(true){ await Task.Delay(TimeSpan.FromSeconds(runtimeConfig.GetInt(AOF_SIZE_LIMIT_ENFORCE_FREQUENCY))) ... }`，
     注册处注释钉死该语义（/Users/z/git/db/wedb/garnet/libs/server/Config/RuntimeServerConfig.cs:191-195）。

修法（逐件取其与 C# 同形的最小改道，禁造第三套配置读取机制）
1. a、d 属「循环每轮重读」型：消费循环内每轮现读配置，读句柄优先复用
   provider.try_runtime_config()（cluster_provider.rs:920，aof-tail-witness 票已铺好的可达面）；
   d 需把 frequency 从 spawn 形参改为循环内取值（`ConfigMeta` 为 INT32 秒，0/负值按 C# 同口径处理），
   常量 :51 与 `.max(1)` 降级为「无配置句柄形态」的兜底并补注释点名真值源，不留双源。
   若 a 的重放任务域拿不到 provider/runtime_config 句柄，按 aof-tail-witness 先例经
   ClusterProvider 暴露只读访问器补齐，禁新建第二张配置表。
2. b 属「推送投影」型：config_meta.rs 家族补 ConfigReconcile::ClusterNodeTimeout { ms } 变体 +
   runtime_server_config.rs 该槽 update_action（对标 aof-sync-max-lag 的 apply_* 形态），
   在 config_owner.rs 对应臂调 provider.set_cluster_node_timeout_ms（:418 现口）；
   消费面全部保持读 provider.cluster_node_timeout()（单点），从而 CONFIG SET 即时生效。
3. c 补上被 no-op 的臂：把 AofBackpressure 句柄引到可推送位（AOF 门面装配域，
   与 service.rs:1442 AOF 装配同位），config_owner.rs:60 由 `{ .. } => {}` 改为
   `ConfigReconcile::AofSyncMaxLag { max_lag_bytes } => gate.set_budget(max_lag_bytes)`；
   若复核确认桥内确实拿不到 gate 句柄，退为 provider 原子槽形态（对标
   aof_replay_max_lag_bytes 在 cluster_provider.rs 的既有实读面），并删 :55-59 的 no-op 自述注释。
4. 四件共同的错误口径注释一并订正：failover_manager.rs:78-79「可动态改」在 b 落地前为假，
   config_owner.rs:55-59 的自述在 c 落地后失效；repl 常量 :48-50 注释补真值源。

优先级
功能缺口（四旋钮定义即失效的断链，运维改值无效果且 CONFIG GET 回显误导）。

协调
- 不重开已归档/在册两票的射程：aof-tail-witness-freq（aof_sync_task.rs 脉冲节流，已落地形态即本票样板）、
  aof-size-knobs-read-side-wiring（aof_memory/page/segment 三尺寸属启动期事实，与 d 的 enforce-frequency
  是两个旋钮，勿混）、replication-timeout-knob-alignment（复制 RPC 超时取值口径，不动 node-timeout 语义）。
- b 的 0 值「无限」哨兵语义保持现状（其无界自旋风险归 cluster-slot-gate-sync-spin 票，本票不夹带）。

验收
- 四旋钮各一条「CONFIG SET 改值 → 行为出」用例：a 改 replica-sync-delay 后重放空转周期变化；
  b 改 cluster-node-timeout 后 gossip/failover 两侧超时随动（含回显与实效一致）；
  c 改 aof-sync-max-lag-bytes 后背压闸门预算即时更新；d 改 enforce-frequency 后任务轮询间隔随动。
- grep 四处消费点无遗留硬编码常量作真值源（兜底常量须点名配置槽）。
- 不新增第二套配置读取/推送机制；./js/check.js 无新增缺失项。

细化方案（fixloop f55-hot-reload，主仓 HEAD 306d32da 复核）
甄别结论：四件均成立。行号漂移：set_cluster_node_timeout_ms 现在在
cluster_provider.rs:430、set_runtime_config :928、try_runtime_config :933。
两点勘误落地时吸收：
- a 的「仅一处消费点」不实（qcode10.design 台账已更正）：主端推流泵
  assembly.rs:80 pump.start_throttle_loop(REPLICA_SYNC_DELAY) 为第二消费点，同批覆盖。
- d 另有隐藏断链：wconf/src/node_options.rs runtime_server_options() 投影段漏投
  aof_size_limit_enforce_frequency_secs（:818-826 段无该行），node 配置值从未进
  wconf 槽位（槽位恒播种 RuntimeServerOptions 默认 5），CONFIG GET 回显与装配行为
  值脱钩——d 修法必须补该投影，否则「循环现取槽位」会把行为值拽回默认 5。

a) replica-sync-delay（循环每轮重读型，两消费点）
- replica_replay_task.rs:51 常量改名 DEFAULT_REPLICA_SYNC_DELAY，注释降级为
  「无配置句柄形态兜底」，真值源点名 wconf 槽 replica-sync-delay。
- 新增 pub(crate) current_sync_delay(Option<&RuntimeServerConfig>) -> Duration
  单点取值（get_milliseconds(ReplicaSyncDelay)，负值钳 0）。
- ReplayAssets 增字段 runtime_config: Option<Arc<RuntimeServerConfig>>（构造点
  assembly.rs wire_replication_data_plane 从 cluster.try_runtime_config() 取，
  Arc 叶子无环）；run_replay_loop 两处 sleep(:141/:200) 改每轮 current_sync_delay。
- aof_replication_pump.rs start_throttle_loop(idle_delay: Duration) 改收
  Option<Arc<RuntimeServerConfig>>，防抖窗口 timeout 前每轮现取（同一辅助）。
  assembly.rs:80 传 cluster.try_runtime_config()。

b) cluster-node-timeout（推送投影型）
- wconf config_meta.rs 补 ConfigReconcile::ClusterNodeTimeout { ms: u64 }
  （0 = 无限哨兵，对齐 cluster_node_timeout() 的 None 分支）。
- runtime_server_config.rs ClusterNodeTimeout 槽(:132) update_action 挂
  apply_cluster_node_timeout_update（秒→毫秒，非正归 0），对标
  apply_aof_sync_max_lag_update 形态。
- wnode cluster_provider.rs trait 加 set_cluster_node_timeout_ms(&self, u64)
  默认 no-op + Arc<T> 转发；wedb impl WnodeClusterProvider(:1321) 转发自身原子槽
  （:430 现口，读口 cluster_node_timeout() 单点不动，gossip/failover 消费面零改动）。
- config_owner.rs apply_config_reconcile 增参 cluster: Option<&ClusterProviderHandle>，
  臂内 set_cluster_node_timeout_ms；调用点 config_commands.rs（host.cluster 在位）、
  service.rs:1557 与 tests 传 None。
- failover_manager.rs:78-79 注释订正（「可动态改」由假转真）。

c) aof-sync-max-lag-bytes（补空臂）
- SessionDependencies 增字段 aof: Option<Arc<GarnetAppendOnlyFile>>（对标 C#
  storeWrapper.appendOnlyFile 共享依赖组），service.rs:1476 构造点填 self.aof。
- RespServerSession 增同名字段 + inject_dependencies 拆包；ConfigSetHost 增
  aof 字段；apply_config_reconcile 增参 aof，:60 空臂改
  aof.backpressure().map(|g| g.set_budget(max_lag_bytes))，删 :55-59 no-op 自述。

d) aof-size-limit-enforce-frequency（循环每轮重读型）
- node_options.rs runtime_server_options() 补投影
  aof_size_limit_enforce_frequency_secs（u64 饱和收窄 i32）。
- spawn_aof_size_limit_task 删 frequency_secs 形参，改收
  Option<Arc<RuntimeServerConfig>>，循环内每轮 get_int 现取
  （.max(1) 保留：C# 0 → Task.Delay(0) 忙转、负 → 异常杀任务，rust 收敛最小 1s，
  注释点名）；无句柄兜底 DEFAULT_AOF_SIZE_LIMIT_ENFORCE_FREQUENCY_SECS。
- service.rs with_aof_size_limit 删 frequency 参数、aof_size_limit 元组只存 limit，
  try_start 传 Some(&self.runtime_config)；tests/aof_size_limit_task.rs 五处改传
  构造好的 RuntimeServerConfig（try_set 频率，贴 CONFIG SET 生产语义）。

边界：不动 aof-tail-witness-freq 已落地形态（本票样板）、不动三尺寸启动期票、
不动 replication-timeout 口径票、b 的 0 值无限哨兵语义保持。
