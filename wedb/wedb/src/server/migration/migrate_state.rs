/// libs/cluster/Server/Migration/MigrateState.cs:MigrateState
#[derive(Debug, PartialEq, Clone)]
#[repr(u8)]
pub enum MigrateState {
  Success = 0x0,
  Fail = 1,
  Pending = 2,
  Import = 3,
  Stable = 4,
  Node = 5,
}

/// 集群槽位协议状态（SETSLOT-RANGE 语义仅此三态可达；收敛自
/// C# MigrationDriver.cs:GetSlotStateString 的非 throw 臂，非法态在类型层
/// 不可表达，不设全变体映射口）
#[derive(Clone)]
pub enum SlotStateStr {
  /// 槽位迁移中（MigrateState::Import）
  Import,
  /// 槽位稳定（MigrateState::Stable）
  Stable,
  /// 槽位节点态（MigrateState::Node）
  Node,
}

impl SlotStateStr {
  /// 映射为集群槽位协议状态字串（对标 C# MigrationDriver.cs:GetSlotStateString）
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Import => "IMPORTING",
      Self::Stable => "STABLE",
      Self::Node => "NODE",
    }
  }
}
