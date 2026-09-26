/// libs/cluster/Server/Migration/MigrateState.cs:MigrateState
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MigrateState {
  Success = 0x0,
  Fail = 1,
  Pending = 2,
  Import = 3,
  Stable = 4,
  Node = 5,
}

impl MigrateState {
  /// 映射为集群槽位协议状态字串（对标 C# MigrationDriver.cs:GetSlotStateString）
  pub const fn as_slot_state_str(&self) -> &'static str {
    match self {
      Self::Import => "IMPORTING",
      Self::Stable => "STABLE",
      Self::Node => "NODE",
      _ => panic!("Invalid MigrateState for slot state string"),
    }
  }
}
