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

/// libs/server/AOF/AofHeader.cs:AofHeader
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Default)]
pub struct AofHeader {
  pub op_type: u16,
  pub session_id: i32,
  pub type_: u8,
}

/// libs/server/AOF/AofAddress.cs:AofAddress
#[derive(Debug, Clone, Copy, Default)]
pub struct AofAddress {
  pub address: i64,
}
