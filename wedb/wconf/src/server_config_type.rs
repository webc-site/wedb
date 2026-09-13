/// CONFIG 参数类型（对标 libs/server/Config/ServerConfigType.cs:ServerConfigType）。
///
/// 判别值与 C# 枚举声明顺序严格一致，索引即 `RuntimeServerConfig` 槽位下标。
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
  // 由 RuntimeServerConfig 的 long[] 槽位表承载，启动时自 GarnetServerOptions
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
  CompactionForceDelete = 16,
  CompactionType = 17,
  SlowlogLogSlowerThan = 18,
  ObjectScanCountLimit = 19,
  SgGet = 20,
  AofSizeLimitEnforceFrequency = 21,

  // 变更需对后台任务执行生命周期动作（启动 / 停止 / 重启）的运行时选项，
  // 由 ConfigMeta 的 UpdateAction 在 CONFIG SET 期间生效。
  AofCommitFreq = 22,
  ExpiredObjectCollectionFreq = 23,
  ExpiredKeyDeletionScanFreq = 24,

  // 只读的非数值参数（文件路径、套接字、物理开关）。经 CONFIG GET 的
  // 只读回落路径暴露——取值直接读启动 GarnetServerOptions，CONFIG SET 拒绝。
  // 无 long[] 槽位。
  Dir = 25,
  Logdir = 26,
  UnixSocket = 27,
  ClusterEnabled = 28,

  // 只读 AOF 参数（物理布局 / 仅启动期开关）。经 CONFIG GET 只读回落路径
  // 暴露；CONFIG SET 拒绝因其需重启才能生效。无 long[] 槽位。
  AofMemory = 29,
  AofPageSize = 30,
  AofSegmentSize = 31,
  AofPhysicalSublogCount = 32,
  AofReplayTaskCount = 33,
  AofCommitWait = 34,
  AofSizeLimit = 35,
  FastAofTruncate = 36,
  AofNullDevice = 37,
}

impl ServerConfigType {
  /// C# 侧声明的全部成员（含 NONE/ALL），按判别值升序。
  pub const ALL_MEMBERS: [ServerConfigType; 38] = [
    Self::None,
    Self::All,
    Self::Timeout,
    Self::Save,
    Self::AppendOnly,
    Self::SlaveReadOnly,
    Self::Databases,
    Self::ClusterNodeTimeout,
    Self::ReplicaSyncDelay,
    Self::AofReplayMaxLagBytes,
    Self::AofSyncMaxLagBytes,
    Self::AofTailWitnessFreq,
    Self::ReplDisklessSyncDelay,
    Self::ReplAttachTimeout,
    Self::ClusterReplicationReestablishmentTimeout,
    Self::CompactionMaxSegments,
    Self::CompactionForceDelete,
    Self::CompactionType,
    Self::SlowlogLogSlowerThan,
    Self::ObjectScanCountLimit,
    Self::SgGet,
    Self::AofSizeLimitEnforceFrequency,
    Self::AofCommitFreq,
    Self::ExpiredObjectCollectionFreq,
    Self::ExpiredKeyDeletionScanFreq,
    Self::Dir,
    Self::Logdir,
    Self::UnixSocket,
    Self::ClusterEnabled,
    Self::AofMemory,
    Self::AofPageSize,
    Self::AofSegmentSize,
    Self::AofPhysicalSublogCount,
    Self::AofReplayTaskCount,
    Self::AofCommitWait,
    Self::AofSizeLimit,
    Self::FastAofTruncate,
    Self::AofNullDevice,
  ];
}
