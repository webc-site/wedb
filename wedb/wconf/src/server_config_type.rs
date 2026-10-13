/// CONFIG 参数类型（对标 libs/server/Config/ServerConfigType.cs:ServerConfigType）。
///
/// 判别值仅作 `RuntimeServerConfig` 的内部槽位下标与数组寻址用，**不承载跨语言
/// 对齐语义**：CONFIG 面按名解析（见 config_meta 的 NAME_LOOKUP），线上不传判别值。
/// 因此与 C# 声明顺序并非逐项相等——本枚举刻意删除 C# 的两员
/// `COMPACTION_FORCE_DELETE`（C#=16）与 `AOF_NULL_DEVICE`（C#=37，尾员），
/// 自 `CompactionType` 起（rust=16 对 C#=17）至 `FastAofTruncate`
/// （rust=35 对 C#=36）全部成员判别值较 C# 声明顺序整体前移一位。
/// 刻意差异：C# 的 CompactionForceDelete 不移植——wedb 紧缩/移位经设备截断
/// 无条件物理回收历史段，C# forceDelete「紧缩后 commit AOF + Truncate 才真正
/// 删文件」的次序无对位需求（AOF 为独立日志，hlog 可由 checkpoint+AOF 重建），
/// 旋钮无可承接行为，按「不留可写不可用的旋钮」直接删除（台账回指
/// doc/zh/deviations.md §158 a）。
/// 刻意差异二：C# 的 AOFNullDevice 不移植——本仓无 null AOF 设备对物（wdev
/// 设备层无 NullDevice 实现），C# AllowDataLoss 派生式
/// `UseAofNullDevice || (FastAofTruncate && !OnDemandCheckpoint)` 在 rust 只
/// 余后项（见 wedb/wedb/src/server/cluster_provider/flags.rs::allow_data_loss
/// 注）；紧缩经设备截断无条件回收，旋钮无可承接行为，同 force-delete 裁决
/// 删除。CONFIG GET * 回显因此少 compaction-force-delete / aof-null-device
/// 两员、该二名单查回空列表，系已裁决删员的回显投影（登记见
/// doc/zh/deviations.md §158 a，严禁按 C# 形态复活二员）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ServerConfigType {
  None = 0,
  All = 1,
  Timeout = 2,
  Save = 3,
  AppendOnly = 4,
  SlaveReadOnly = 5,
  Databases = 6,

  // 运行时可调选项（在使用点实时读取，不对运行中服务器产生物理变更）。
  // 由 RuntimeServerConfig 的 long[] 槽位表承载，启动时自 RuntimeServerOptions
  // 播种，经 CONFIG SET 更新。时长类选项的单位由 RuntimeServerConfig 的
  // 元数据声明，其全部支持单位均可读取，故不编码在成员名中。
  ClusterNodeTimeout = 7,
  ReplicaSyncDelay = 8,
  AofReplayMaxLagBytes = 9,
  AofSyncMaxLagBytes = 10,
  AofTailWitnessFreq = 11,
  ReplDisklessSyncDelay = 12,
  ReplAttachTimeout = 13,
  ClusterReplicationReestablishmentTimeout = 14,
  CompactionMaxSegments = 15,
  CompactionType = 16,
  SlowlogLogSlowerThan = 17,
  ObjectScanCountLimit = 18,
  SgGet = 19,
  AofSizeLimitEnforceFrequency = 20,

  // 变更需对后台任务执行生命周期动作（启动 / 停止 / 重启）的运行时选项，
  // 由 ConfigMeta 的 UpdateAction 在 CONFIG SET 期间生效。
  AofCommitFreq = 21,
  ExpiredObjectCollectionFreq = 22,
  ExpiredKeyDeletionScanFreq = 23,

  // 只读的非数值参数（文件路径、套接字、物理开关）。经 CONFIG GET 的
  // 只读回落路径暴露——取值直接读启动 RuntimeServerOptions，CONFIG SET 拒绝。
  // 无 long[] 槽位。
  Dir = 24,
  Logdir = 25,
  UnixSocket = 26,
  ClusterEnabled = 27,

  // 只读 AOF 参数（物理布局 / 仅启动期开关）。经 CONFIG GET 只读回落路径
  // 暴露；CONFIG SET 拒绝因其需重启才能生效。无 long[] 槽位。
  AofMemory = 28,
  AofPageSize = 29,
  AofSegmentSize = 30,
  AofPhysicalSublogCount = 31,
  AofReplayTaskCount = 32,
  AofCommitWait = 33,
  AofSizeLimit = 34,
  FastAofTruncate = 35,
}
