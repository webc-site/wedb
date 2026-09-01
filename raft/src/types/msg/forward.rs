use super::join::JoinRequest;
use super::leave::LeaveRequest;
use super::member::{GetMemberReply, GetMemberReq};
use crate::error::{Error, Result};
use crate::types::AppliedState;
use crate::types::LogEntry;
use crate::types::TxnReply;
use crate::types::TxnReq;
use crate::types::UpsertKV;

#[derive(bitcode::Encode, bitcode::Decode, Clone, Debug, PartialEq, Eq)]
pub struct GetKVReq {
  pub key: String,
}

pub type GetKVReply = Option<Vec<u8>>;

#[derive(bitcode::Encode, bitcode::Decode, Clone, Debug, PartialEq, Eq)]
pub struct ScanPrefixReq {
  pub prefix: Vec<u8>,
}

pub type ScanPrefixReply = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(bitcode::Encode, bitcode::Decode, Clone, Debug, PartialEq, Eq)]
pub struct BatchWriteReq {
  pub entries: Vec<UpsertKV>,
}

pub type BatchWriteReply = ();

#[derive(bitcode::Encode, bitcode::Decode, Debug, Clone, PartialEq, Eq)]
pub enum RequestPayload {
  Join(JoinRequest),
  Leave(LeaveRequest),
  GetMembers(GetMemberReq),
  Write(LogEntry),
  GetKV(GetKVReq),
  ScanPrefix(ScanPrefixReq),
  BatchWrite(BatchWriteReq),
  Txn(TxnReq),
}

#[derive(bitcode::Encode, bitcode::Decode, Debug, Clone, PartialEq, Eq)]
pub struct ForwardRequest {
  pub body: RequestPayload,
  pub hop: u8,
}

impl ForwardRequest {
  pub fn new(body: RequestPayload) -> Self {
    Self { body, hop: 0 }
  }
}

#[derive(bitcode::Encode, bitcode::Decode, Debug, Clone, PartialEq, Eq)]
pub enum ForwardResponse {
  Join(()),
  Leave(()),
  GetMembers(GetMemberReply),
  Write(AppliedState),
  GetKV(GetKVReply),
  ScanPrefix(ScanPrefixReply),
  BatchWrite(BatchWriteReply),
  Txn(TxnReply),
}

macro_rules! impl_into_resp {
    ($($fn_name:ident, $variant:ident, $ret:ty);* $(;)?) => {
        $(
            pub fn $fn_name(self) -> Result<$ret> {
                match self {
                    Self::$variant(r) => Ok(r),
                    other => Err(other.mismatch_err(stringify!($variant))),
                }
            }
        )*
    };
}

impl ForwardResponse {
  fn variant_name(&self) -> &'static str {
    match self {
      Self::Write(_) => "Write",
      Self::BatchWrite(_) => "BatchWrite",
      Self::Txn(_) => "Txn",
      Self::GetKV(_) => "GetKV",
      Self::ScanPrefix(_) => "ScanPrefix",
      Self::Join(_) => "Join",
      Self::Leave(_) => "Leave",
      Self::GetMembers(_) => "GetMembers",
    }
  }

  fn mismatch_err(&self, expected: &str) -> Error {
    let actual = self.variant_name();
    Error::internal(format!("Expected {expected} response, got {actual}"))
  }

  impl_into_resp! {
      into_write, Write, AppliedState;
      into_batch_write, BatchWrite, BatchWriteReply;
      into_txn, Txn, TxnReply;
      into_get_kv, GetKV, GetKVReply;
      into_scan_prefix, ScanPrefix, ScanPrefixReply;
      into_join, Join, ();
      into_leave, Leave, ();
      into_get_members, GetMembers, GetMemberReply;
  }
}
