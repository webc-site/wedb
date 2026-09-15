# waof/日志语义面七条（来源 next/glm.md 条 20、21、25、26、27、48、49，主代理预清理后下发）

## 甄别结论（七条全部成立，其中第四条走清死链路线）

一、waof_sublog memory_size_bytes 返回容量常量（P1）：成立。
C# 对标：libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:196
MaxMemorySizeBytes => allocator.MaxMemorySizeBytes（容量上限，AllocatorBase.cs:1026
= MaxAllocatedPageCount * PageSize）；:201 MemorySizeBytes =>
allocator.GetLogicalAddressOfStartOfPage(allocator.AllocatedPageCount)（当前占用）。
rust 现状：SublogBackend 单方法 memory_size_bytes（garnet_log.rs:105），WaofSublog
实现返回 config.buffer_size 容量常量（waof_sublog.rs:253）；SingleLog/ShardedLog/
GarnetLog 三层路由的 max_memory_size_bytes 与 memory_size_bytes 双方法塌缩转发
同一实现（single_log.rs:61/67、sharded_log.rs:187/196、garnet_log.rs:555/564），
InMemorySublog 返回 payload 总和（占用口径）。C# SingleLog.cs:39/41、ShardedLog.cs:148/159、
GarnetLog.cs:159/169 均为容量/占用双属性。

二、committed_begin_address 塌缩为 begin_address（P1）：成立。
C# 对标：TsavoriteLog.cs:120 CommittedBeginAddress 独立字段；:244 构造 =
FirstValidAddress；:528/:596 Initialize = beginAddress；:2696/:2874 commit 元数据
写出时 info.BeginAddress = BeginAddress 快照；:2734/:3095 恢复自 commit 记录
（recoveryInfo.BeginAddress）；:246 Reset 归 FirstValidAddress。
rust 现状：single_log.rs:49 与 sharded_log.rs:163 的 committed_begin_address 均
原样返回 begin_address；底层 SublogBackend 无该概念，消费面 wmetric
AofSnapshot.committed_begin_address（INFO persistence 段）恒 None 未接线。

三、prefetch_key_sequence_number 空实现且有生产调用（P1）：成立，选真实现路线。
C# 对标：libs/server/AOF/ReadConsistency/VirtualSublogReplayState.cs:119
PrefetchKeySequenceNumber = Sse.Prefetch0（读侧提前预热草图槽缓存行，把回放线程
跨核一致性缺失与 store 读重叠，无正确性影响纯性能提示）。
rust 现状：virtual_sublog_replay_state.rs:187 空体，生产调用点
read_consistency_manager.rs:322 verify_key_freshness。注释虽已声明降级，但调用链
保留空函数即占位，与红线冲突。改法：实现预热——stable Rust 无跨平台 prefetch
intrinsic，用 core::arch::asm!（aarch64 prfm pldl1keep / x86_64 prefetcht0，
其余架构编译期空），保留调用链，注释改真实映射。

四、sharded 多物理日志拓扑无装配点（P1）：部分成立，走清死链 + 留档路线。
C# 对照证据：AofRecover.cs:63 按 serverOptions.MultiLogEnabled 分派
MultiLogRecover / SingleLogRecover；MultiLogEnabled 默认 false
（GarnetServerOptions.cs:1243，AofPhysicalSublogCount 默认 1）。
rust 现状：multi_log_recover（aof_recover.rs:71）零生产调用，恢复点
garnet_append_only_file.rs:440 与 service.rs:421 恒 single_log_recover；生产域
唯一物理装配工厂 single_log_aof 恒单 WaofSublog（与 C# 默认关闭形态一致），
拓扑语义面（ShardedLog、地址向量、位图锁、recover driver）保留且在测试中存活。
点亮装配的硬阻断：C# MultiLogRecover 恢复上界收敛自各子日志 commit cookie
（appendOnlyFile.Log.RecoverLatestSequenceNumber），waof 承载下 WaofSublog cookie
仅进程内可见（无 commit 元数据持久化区，刻意架构差异已声明），多物理日志恢复
上界无从收敛；且复制域（provider.wal 单句柄）与 checkpoint 链按单物理日志设计。
结论：删 multi_log_recover 孤函数，ignore 登记差异并留档；物理装配点亮待 waof
commit 元数据持久化后另立任务。

五、NodeArgs.compaction_freq_secs 死旋钮（P1）：成立。
C# 对照：CompactionFrequencySecs 默认 0（GarnetServerOptions.cs:211），消费单点
StoreWrapper.cs:967（> 0 时注册 CompactionTask 周期任务）。
rust 现状：node_options.rs:112 字段（默认 60）全仓零消费；周期紧缩调度已由 wkv
GcConfig::compaction_interval_ms 单点承担（wkv/src/config.rs:66 注释明示原
compaction_freq_secs 已删，避免两条调度链对同一日志各自判定紧缩时机）。NodeArgs
层重复暴露即死旋钮，且为虚假配置面（用户传值无任何效果）。改法：删字段 +
DEFAULT_COMPACTION_FREQ_SECS 常量 + default_compaction_freq + override_explicit
列表项 + lib.rs 导出 + 测试引用。C# 无生产链可接（调度面已单点化），不接消费。

六、key 级 TTL 缺 4-bit coarse 粗化（P2）：成立。
C# 对照：ExpirationWithOption.cs:22-23 构造时 (ticks >> 4) << 4 粗化（1600ns
分辨率），key 级过期时间经此结构落存储；HFE/ZSet 成员侧 rust 已 1:1
（wresp/src/options.rs ExpirationWithOption::new），仅 wkv put_ttl 存全精度
（ttl.rs:134 TtlCodec::encode 原值直存）。改法：put_ttl 写入前粗化
((ticks >> 4) << 4)，单点收口。持久化格式 8B 大端 ticks 不变（仅值域粗化），
不需要向下兼容。连带核：AOF TtlWrite 镜像（service.rs）携带存储后的 ticks
（粗化值），复制端口径一致；GT/LT/NX/XX 条件比较在 RESP 边界换算后进行，
粗化差异 < 1600ns 与 C# 同级。

七、副本回放 PEXPIREAT 丢条件未声明（P2）：成立，注释级。
C# 对照：UnifiedStore/PrivateMethods.cs:108 WriteLogRMW 无条件置
RespInputFlags.Deterministic；KeyAdminCommands.cs:432 EXPIRE 族条件经
ExpirationWithOption word 低 4 位随 AOF 完整携带，副本端重评估条件语义。
rust 现状：AOF 条目由 service.rs TtlWrite 镜像产出（Pexpireat + 绝对毫秒 +
DETERMINISTIC 标志），不携带 ExpireOption；aof_processor.rs Pexpireat 臂
（:1187，行号已漂移）重放按 TtlOpt::NONE 无条件执行——条件丢失且无差异注释
（SET 条件族已有同款注释先例）。改法：仅在 Pexpireat 臂补差异注释（并发代理
在改 StoreRMW 分支，:1243 一带以先合并者为准，本分支只补注释避冲突）。

## 改动点

1. wedb/wnode/src/aof/garnet_log.rs
   SublogBackend 拆 max_memory_size_bytes/memory_size_bytes 双方法；新增
   committed_begin_address 读法；InMemorySublog 增 committed_begin 原子字段
   （初值 1），commit 快照推进、safe_initialize 恢复、reset 归 1；补测试。
2. wedb/wnode/src/aof/waof_sublog.rs
   WaofSublog 增 committed_begin 原子字段；max = config.buffer_size、
   memory = tail - begin（环形窗口有效字节真实水位）；commit/commit_flush_async
   快照推进、safe_initialize 恢复、reset 归 1。
3. wedb/wnode/src/aof/sublog.rs
   Sublog 分发层与 SublogBackend impl 同步双方法 + committed_begin_address。
4. wedb/wnode/src/aof/single_log.rs、sharded_log.rs
   max/memory 双方法分别转发；committed_begin_address 真实化。
5. wedb/wnode/src/aof/garnet_log.rs（路由层）
   GarnetLog::committed_begin_address 不再塌缩；max/memory 路由语义分离。
6. wedb/wnode/src/aof/readconsistency/virtual_sublog_replay_state.rs
   prefetch_key_sequence_number 实装（asm! 平台分支预热）。
7. wedb/wnode/src/aof/recover/aof_recover.rs
   删 multi_log_recover 孤函数（头文档同步收敛）。
8. wedb/wconf/src/node_options.rs + lib.rs + tests
   删 compaction_freq_secs 死旋钮全套（字段/常量/默认函数/覆盖表/导出/断言）。
9. wedb/wkv/src/ttl.rs
   put_ttl 写入前 4-bit 粗化；补粗化等价性测试。
10. wedb/wnode/src/aof/aof_processor.rs
    Pexpireat 臂补副本条件差异注释（只动注释）。
11. js/check/ignore
    AofRecover.cs:MultiLogRecover 登记差异；保证 check.js 无新增缺失。

## 测试计划

1. committed begin 恢复链：safe_initialize 后 committed_begin = begin 参数、
   commit 推进快照、reset 归 1（InMemorySublog 与 WaofSublog 双后端）。
2. 占用水位：WaofSublog memory = tail - begin 随写入推进、max 恒容量值；
   InMemorySublog 双方法口径。
3. TTL 粗化等价性：put_ttl 后 ttl_of 值低 4 位恒零、读取判定（check_expired/
   expire_at）不受粗化影响。
4. group-commit 并发语义回归：waof truncate_and_evict 既有并发测试 +
   wedb_standalone aof_domain 全量复跑，确认不动并发面。

## 验收口径

1. ./clippy.sh 零警告（禁 allow）。
2. ./test.sh 全过。
3. bun ./js/check.js 无新增缺失/重复（MultiLogRecover 经 ignore 登记不新增缺失）。
4. SublogBackend 双方法语义分离：max 恒容量、memory 随水位；
   committed_begin_address 全链不再返回 begin_address。

## 验证结果

1. 分支 w4-waof-sem 共 10 个提交（含两次 dev 合并），主目录合并至
   c7f2425 后 worktree 与分支已清理，独立 target 目录已删。
2. ./clippy.sh：分支与主目录均 3 tasks 全过，-D warnings 硬门禁零警告，
   无 allow。
3. ./test.sh：主目录合并后 HEAD 2041 passed / 1 skipped，regress 回归门禁
   2 passed。过程中的 store::crud::test_session_lifecycle_rapid_churn 与
   flush_database 首轮失败经主目录基线复跑判定为既有 flaky（LightEpoch
   会话 churn 偶发驱逐竞争，6 次基线复现 1 次，与本次改动无关）。
4. bun ./js/check.js：分支与主目录均退出码 0，无输出；multi_log_recover
   删除后 MultiLogRecover 经 js/check/ignore/libs_server_AOF_Recover.yml
   登记，无新增缺失/重复。
5. 新增测试（3 组全过）：
   - wnode garnet_log::committed_begin_snapshot_lifecycle（初值
     FirstValidAddress → commit 采样 → safe_initialize 恢复 → reset 归 1）
   - wnode garnet_log::memory_size_capacity_and_usage（内存后端容量/占用口径）
   - wedb_standalone aof_domain::waof_sublog_memory_watermark_and_committed_begin
     （max 恒环形容量、memory = tail - begin 水位、快照/恢复/重置全链）
   - wkv ttl::test_put_ttl_coarse_ticks_rounding（落盘值低 4 位恒零、
     等价性、GT 严格大于口径）
6. 粗化涟漪修正：expire_at 入口统一粗化（GT/LT 条件判定与落盘同值，对齐
   C# RESP 边界 ExpirationWithOption 粗化后编码的同源性）；flush_database/
   swap_database/ttl_purge/service 四处测试断言对齐存储值粗化口径。
7. group-commit 并发面零触碰：waof/src/log.rs 与 CommitPipelineState 无改动，
   waof_sublog 仅新增原子快照字段，并发回归（truncate_and_evict、aof_domain
   reset 并发族）全过。
