use super::TxnReply;

#[derive(
  bitcode::Encode,
  bitcode::Decode,
  Debug,
  Clone,
  PartialEq,
  Eq,
  derive_more::From,
  derive_more::TryInto,
)]
pub enum AppliedState {
  #[try_into(ignore)]
  None,
  Txn(TxnReply),
}
