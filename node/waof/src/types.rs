/// libs/server/AOF/AofEntryType.cs:AofEntryType
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofEntryType {
  Null = 0,
  MainStoreTxn = 1,
  ObjectStoreTxn = 2,
  TxnUncommitted = 3,
  MainStoreStoreCommand = 4,
  ObjectStoreStoreCommand = 5,
}

impl AofEntryType {
  /// 自磁盘字节校验解码（type 字节不可信；transmute 出越界判别值属即时 UB）
  #[inline]
  pub const fn from_u8(v: u8) -> Option<Self> {
    match v {
      0 => Some(Self::Null),
      1 => Some(Self::MainStoreTxn),
      2 => Some(Self::ObjectStoreTxn),
      3 => Some(Self::TxnUncommitted),
      4 => Some(Self::MainStoreStoreCommand),
      5 => Some(Self::ObjectStoreStoreCommand),
      _ => None,
    }
  }
}

/// libs/server/AOF/AofHeader.cs:AofHeader
///
/// 在 garnet 中的相对路径 libs/server/AOF/AofAddress.cs:AofAddress 的对应物
/// 已由 wserver::aof::aof_address 完整实现（多 sublog 位点向量），本 crate
/// 不再保留简化桩
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Default)]
pub struct AofHeader {
  pub op_type: u16,
  pub session_id: i32,
  pub type_: u8,
}
