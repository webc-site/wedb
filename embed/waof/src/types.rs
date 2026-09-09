/// garnet相对路径:garnet/libs/server/AOF/AofEntryType.cs:AofEntryType
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

/// garnet相对路径:garnet/libs/server/AOF/AofHeader.cs:AofHeader
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, Default)]
pub struct AofHeader {
  pub op_type: u16,
  pub session_id: i32,
  pub type_: u8,
}

/// garnet相对路径:garnet/libs/server/AOF/AofAddress.cs:AofAddress
#[derive(Debug, Clone, Copy)]
pub struct AofAddress {
  // In C# it's a struct with some properties
  pub address: i64,
}
