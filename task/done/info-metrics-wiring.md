# INFO 指标观测面接线（INFO commandstats / 复制段滞后指标 / KEYSPACE TTL 计数）

来源：next/glm.md 条 7、9、19（next/ds.net.md 条 5、next/net.md 条 4 关联面）

## 甄别结论（对照 garnet C# 逐条核实）

### 一、glm 条 7 指标接线两缺：部分成立

不成立部分：
- 两 main 未调用 metrics_sampling_frequency builder：已过时。wedb/src/main.rs:59 与
  wedb_standalone/src/main.rs:65 均已透传，server.rs 启动门已在。不做。

成立部分（本任务执行）：
- CommandStatsMonitor 选项未转写：C# Options.cs:344-360 有 commandstats-monitor 与
  latency-monitor 两个开关，wconf NodeArgs 均无。会话构造 RespServerSession.cs:266
  按 serverOptions.CommandStatsMonitor 挂表，rust 无对应字段。
- 会话主循环无 per-command 递增：C# RespServerSession.cs:683-716 三出口
  （执行后 IncrementCalls + commandErrorWritten 时 IncrementFailed、ACL 拒绝
  IncrementRejected），rust process_messages 全缺。
- dispose 归并：C# RespServerSession.cs:405 AddMetricsHistorySessionDispose 传
  commandStats，rust client_commands.rs merge_metrics_history_session_dispose 恒 None。
- monitor 聚合面：C# GarnetServerMonitor.cs:64/67 构造按 opts 开启
  trackCommandStats/latency，StoreWrapper.cs:226 的 monitor 创建条件是三开关任一；
  rust start_server_monitor 硬编码 new(freq, true, false, false)，门只看采样频率。
- INFO COMMANDSTATS 段：info_provider.rs command_stats_monitor 硬编码 false、
  command_stats() 恒空。wmetric trait 返回 (name, calls, rejected) 三元组丢了
  failed_calls，populate 输出硬编码 failed_calls=0（C# 输出真值）。
- 延迟指标 NET_RS_LAT 链路全缺：C# RespServerSession.cs:481/:591-598
  Start / opCount / StopAndSwitch(慢命令分桶) / Stop / RecordValue(bytes, ops)；
  rust try_consume_messages 全无。zero-ref-pubs-cleanup 为此保留
  wmetric stop_and_switch（明确给本任务复活）。

### 二、glm 条 9 复制段缺 5 个副本侧指标：成立

C# ClusterProvider.cs:255-259 副本分支尾部 5 字段，rust get_replication_info 缺：
- replication_offset_vector_lag = appendOnlyFile.Log.TailAddress.Diff(ReplicationOffset)
  → rust：aof.log().tail_address()（AofAddress 向量）.diff(&rm.get_current_replication_offset())
- replication_offset_acc_lag = 同上 AggregateDiff → waof AofAddress::aggregate_diff 已在
- aof_replay_max_lag_bytes = serverOptions.AofReplayMaxLagBytes → rust ClusterProvider
  无配置可达，仿 cluster_node_timeout_ms 先例注入 AtomicI32 + setter + main.rs 一行
  （RuntimeServerOptions.aof_replay_max_lag_bytes 默认 -1 已在）
- physical_sublog_max_sequence_vector / _drift_sequence_vector：rust 已有完整转写
  （wnode/aof/readconsistency/read_consistency_manager.rs:178/:193），
  aof.read_consistency_manager() None 时对齐 C# 输出 "-1" 分支

### 三、glm 条 19 KEYSPACE TTL 计数恒 0：成立

- 扫描原语缺失：C# ArrayKeyIterationFunctions.cs:382 UnifiedStoreGetKeyspaceStats
  （活键数 + 其中带 TTL 键数；口径对齐 DBSIZE）。rust array_key_iteration_functions.rs
  未实现。TTL 带位判定：rust 无 DataHeader.HasExpiration，等价为键级 TTL 记录
  存在性（wkv has_ttl_tag，纯内存单探针）。
- 输出面：C# DefaultInfo/AllInfoSet 排除 KEYSPACE（GarnetInfoMetrics.cs:25-28），
  仅显式 INFO KEYSPACE 触达；rust DEFAULT_INFO/ALL_INFO_SET 同口径已排除。
  存储扫描为 compio 异步（hlog 冷区跨 await），同步消费循环无法闭环——与
  DBSIZE/KEYS 同构，降级慢路径（Ok(false) → SlowWait → exec_slow 闭环），
  显式 keyspace 请求才触发，普通 INFO 路径零影响。

### 空段甄别（keyspace 之外的空集 trait 面）

- gossip_stats / buffer_pool_stats / checkpoint_info：wedb ClusterProvider 侧
  IClusterProvider 实现已有真数据（get_gossip_stats/get_buffer_pool_stats/
  get_checkpoint_info），缺的是会话 INFO 分派到 wedb 侧的通道——须经
  wnode ClusterSession 切面承接，而 cluster_session.rs 由并发代理占用（MIGrade），
  本轮冻结不碰。维持空集 + 注释声明，记录越界待办。
- databases()/hlog_scan_dump：存储域快照通道，另任务域，不动。

## 实施清单

1. wconf NodeArgs 加 commandstats_monitor / latency_monitor
   （--commandstats-monitor / --latency-monitor，对标 Options.cs:344-360）
2. wnode RespServerSessionOptions 加 command_stats_monitor；RespServerSession 挂
   command_stats: Option<Arc<Mutex<CommandStats>>>（单写多读：主循环递增、
   monitor 采样 / dispose 归并 / INFO 聚合读）
3. process_messages 三计数（对标 RespServerSession.cs:683-716）
4. try_consume_messages 延迟链路（对标 :481/:591-598，复活 stop_and_switch）
5. dispose 归并传 command_stats（client_commands.rs）
6. server.rs：monitor 构造传三开关、启动门对齐 StoreWrapper.cs:226、
   ServerBootstrap 加两个 builder
7. wmetric：GarnetServerMonitor 加 command_stats 聚合快照读取与 track 标志；
   InfoProvider::command_stats 扩 4 元组（+failed_calls）；populate 输出真值
8. info_provider.rs：facts 的 command_stats_monitor 投影、command_stats 聚合实现、
   keyspace_stats 注释更新
9. 两 main：builder + 会话选项装配（一行级）
10. 复制段 5 指标（wedb/src/server/cluster_provider.rs get_replication_info +
    aof_replay_max_lag_bytes 注入 + wedb main.rs 一行）
11. KEYSPACE：array_key_iteration_functions.rs 加 UnifiedStoreGetKeyspaceStats 等价；
    INFO 显式 keyspace 请求降级慢路径；exec_slow 加 Info 分支（切前缀逐库扫描，
    恢复 (ns, db)，段文本经 wmetric GarnetInfoMetrics 出——段格式一处定义）
12. 测试：主循环三计数、INFO COMMANDSTATS 段、复制段字段、KEYSPACE 计数

## 范围约束

- 避开 cluster_session.rs（并发代理占用）；wedb cluster_provider.rs 只动
  get_replication_info 与 aof_replay_max_lag_bytes 注入面
- CONFIG SET 联动不在本任务（ds.data 条 2 另有待办）
- 不实现加载 C# 模块、不实现微软认证；死代码直删不占位
- 删除的 rust 符号若曾对应 C# 函数，在 js/check/ignore 登记；bun ./js/check.js
  基线零输出，收尾须保持无新增缺失/重复

## 验证结果

合并：w4-info-metrics（7 commit，含 merge dev 一次，冲突解于
wedb_standalone/src/main.rs 双方新增元组项并集）→ dev 主目录合并成功。

验收：
- ./clippy.sh 零警告（禁 allow），--tests 亦零输出
- ./test.sh：wedb 域 2032 测试 2031 过；regress 域 2/2 过
- 唯一失败 ttl_purge_single_deterministic_entry
  （wedb_standalone/tests/service.rs，DELIFEXPIM arg1 断言 left=0 right=1）
  经 fork 基点 9372268（无本任务任何改动）复跑同样失败，为既有问题，
  与本任务改动面（wconf 开关 / wmetric 聚合 / 会话计数与延迟 / INFO /
  复制段 / KEYSPACE）无交集，按范围外只记录不修改
- bun ./js/check.js 零输出，无新增缺失/重复（中途出现 NetworkINFO 重复
  定义一次，降级门改名 try_info_keyspace_slow_path 后消除）
- 合并后主目录关键测试 8/8 过（三计数 / COMMANDSTATS 段 / KEYSPACE /
  复制段滞后指标）

落地清单：
- wconf：NodeArgs 增 latency-monitor / commandstats-monitor（对标
  Options.cs:344-360）
- wnode：RespServerSession 挂 CommandStats 表，主循环三出口计数
  （RespServerSession.cs:683-716）；NET_RS_LAT 延迟链路复活（:481/:591-598，
  stop_and_switch 启用）；dispose 归并传命令统计（:405）；ConsumerEntry
  命令统计镜像承接监视器采样（ActiveConsumers 直查等价）；server.rs
  monitor 三开关构造与启动门对齐（GarnetServerMonitor.cs:64、
  StoreWrapper.cs:226），装配与采样循环拆分（C# Start 仅频率>0 拉起）
- wmetric：CommandStats Clone、monitor 聚合读取（command_stats_aggregate /
  tracks_command_stats / tracks_latency）、InfoProvider::command_stats
  扩 4 元组输出真 failed_calls
- wedb：复制段补 5 个副本侧滞后指标（ClusterProvider.cs:255-259），含
  aof_replay_max_lag_bytes 注入面
- KEYSPACE：UnifiedStoreGetKeyspaceStats 转写（活键口径对齐 DBSIZE，
  带 TTL 经 has_ttl_tag，对应 C# HasExpiration）；纯显式 INFO KEYSPACE
  请求经 dispatch_slow Ok(false) 协议降级慢路径，切活跃库前缀逐库扫描
  后恢复，段文本经 wmetric 段填充器一处定义
- 两 main：采样频率（已有）+ 两开关 builder + 会话选项装配

范围外记录（不修改）：
- ttl_purge_single_deterministic_entry 既有失败（见上）
- gossip_stats / buffer_pool_stats / checkpoint_info 段：wedb 侧
  IClusterProvider 数据已备，会话 INFO 到 wedb 的通道须经 ClusterSession
  切面，该文件并发代理占用中，留待后续接线
- databases() / hlog_scan_dump 存储域快照通道（STORE / MEMORY / HLOGSCAN
  段真数据）另任务域
- CONFIG SET 对 commandstats / latency 开关的热更联动不在本任务
  （ds.data 条 2 另有待办）
