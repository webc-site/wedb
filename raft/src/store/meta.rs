use std::error::Error as StdError;

use zenoh_raft::ErrorSubject;
use zenoh_raft::ErrorVerb;
use zenoh_raft::StorageError;
use zenoh_raft::alias::LogIdOf;
use zenoh_raft::alias::VoteOf;
use zenoh_raft::errors::ErrorSource;
use zenoh_raft::impls::BoxedErrorSource;

use crate::types::RaftCodec;
use crate::types::TypeConfig;

pub trait StoreMeta {
  const KEY: &'static str;

  type Value: RaftCodec;

  fn subject(v: Option<&Self::Value>) -> ErrorSubject<TypeConfig>;

  fn read_err(e: impl StdError + 'static) -> StorageError<TypeConfig> {
    StorageError::new(
      Self::subject(None),
      ErrorVerb::Read,
      BoxedErrorSource::from_error(&e),
    )
  }

  fn write_err(v: &Self::Value, e: impl StdError + 'static) -> StorageError<TypeConfig> {
    StorageError::new(
      Self::subject(Some(v)),
      ErrorVerb::Write,
      BoxedErrorSource::from_error(&e),
    )
  }

  fn delete_err(e: impl StdError + 'static) -> StorageError<TypeConfig> {
    StorageError::new(
      Self::subject(None),
      ErrorVerb::Delete,
      BoxedErrorSource::from_error(&e),
    )
  }
}

pub(crate) struct LastPurged;
pub(crate) struct Vote;
pub(crate) struct Committed;

impl StoreMeta for LastPurged {
  const KEY: &'static str = "last_purged_log_id";
  type Value = LogIdOf<TypeConfig>;

  fn subject(_: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
    ErrorSubject::Store
  }
}

impl StoreMeta for Vote {
  const KEY: &'static str = "vote";
  type Value = VoteOf<TypeConfig>;

  fn subject(_: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
    ErrorSubject::Vote
  }
}

impl StoreMeta for Committed {
  const KEY: &'static str = "committed";
  type Value = LogIdOf<TypeConfig>;

  fn subject(_: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
    ErrorSubject::Store
  }
}
