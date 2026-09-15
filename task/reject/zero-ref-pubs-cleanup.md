# zero-ref-pubs-cleanup 拒绝与口径修正

针对任务下达清单（next/design.md 原条目 17、18），以下意见（整体或部分）拒绝执行或改判。

一、wmetric stop_and_switch 拒绝删除，保留

意见原文将其列为零引用删除项。核实：C# 生产在用（RespServerSession.cs:591，
ProcessMessages 尾部 containsSlowCommand 分支 NET_RS_LAT → NET_RS_LAT_ADMIN 切换），
rust 会话延迟记录面当前未接（start/stop 仅测试引用）属 glm.md 条 7「指标接线两缺」
待办范围——其对标行明确引用 RespServerSession.cs:587-598（即 StopAndSwitch 所在段），
接线落地会复活该函数。按「先看 glm 条 7 再定，接线会复活则保留」的甄别口径保留。
连带说明：shared_iterations 与 get_latency_metrics_multi 不受 glm 条 7 复活
（前者 monitor_iterations 字段本身 pub，接线直接 clone 字段即可；后者的消费面
MetricsApi 多类别重载无业务调用，rust metrics_api::get_latency_metrics_all 已用
循环单类别承接），两者照删。

二、「C# 生产在用即补接线」的执行口径修正（部分拒绝）

意见总纲要求「C# 生产在用且 rust 只差一环的补接线」。以下各项 C# 生产在用，
但 rust 消费链整段未立项（缺的不是一环而是一段），补接线超出本待办范围，
一律按红线「死代码直接删 + ignore 登记」处置，立项时随链恢复：

- wresp get_serialized_record_span：消费链为 RespClusterMigrateCommands /
  RespClusterReplicationCommands 的 MIGRATE SLOTS 变体与 SEND_CKPT 检查点流
  （glm.md 条 8 / ds.net.md 条 1，均未立项）。
- wbase max_send_buffer_content_size + SEND_BUFFER_OVERHEAD_RESERVE：消费链为
  diskless 复制 ReplicationSnapshotIterator 与迁移 chunk 计算（均未立项）。
- wbftree clear_tree_handle：rust 搬迁自愈链（compact.rs PostCopyToTail 自愈 +
  checkpoint.rs mark_recovered_from_checkpoint + rebind_stub）已结构性承接句柄
  生命周期，无需按 C# 形状补挂 setter。
- wtxn is_skipping_operations：意见两选项中选「删函数 + 测试改生产等价路径」。
  C# 生产消费点是 RespServerSession 的 txnSkip 网络缓冲跳过分支，rust 批处理
  会话模型无该分支，接生产链不成立；测试断言改 state 字段直读。

三、清单项数与实际执行差异（事实说明，非拒绝）

- wconf pub_sub_page_size_bits、append_only_file_base_directory 与 wbftree
  cpr_snapshot_by_ptr 三项在作业开始前已不存在（主代理预清理），从清单剔除。
- wbase DEFAULT_BUS_PORT_OFFSET 按甄别提示排除（集群 bus 端口待办认领），未动。
- check.js 实际新增缺失仅 6 文件 15 函数（TransactionManager.Reset/
  IsSkippingOperations、ConfigNameComparer.GetHashCode、HyperLogLog
  DenseCountNonZero、RangeIndexManager.ClearTreeHandle、GarnetLatencyMetrics
  多类别重载等因既有 ignore、全局同名映射覆盖或 C# 侧非方法声明而无需登记）。
