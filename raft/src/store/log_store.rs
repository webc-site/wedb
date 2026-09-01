use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::io::{self, Error};
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use fjall::Keyspace;
use zenoh_raft::alias::{EntryOf, LogIdOf, VoteOf};
use zenoh_raft::entry::RaftEntry;
use zenoh_raft::storage::{IOFlushed, RaftLogStorage};
use zenoh_raft::{OptionalSend, RaftLogReader, RaftTypeConfig};

use super::StoreMeta;
use super::key::{LOG_DATA_FAMILY, LOG_META_FAMILY};
use super::meta::LastPurged;
use crate::engine::FjallEngine;
use crate::types::{
  CompactEntry, Entry, LogId, LogState, RaftCodec, TypeConfig, decode, read_logs_err,
};

pub struct FjallLogStore<C>
where
  C: RaftTypeConfig,
{
  pub(crate) engine: Arc<FjallEngine>,
  pub(crate) cf_meta: Keyspace,
  pub(crate) cf_logs: Keyspace,
  _p: PhantomData<C>,
}

impl<C: RaftTypeConfig> Clone for FjallLogStore<C> {
  fn clone(&self) -> Self {
    Self {
      engine: self.engine.clone(),
      cf_meta: self.cf_meta.clone(),
      cf_logs: self.cf_logs.clone(),
      _p: PhantomData,
    }
  }
}

impl<C: RaftTypeConfig> Debug for FjallLogStore<C> {
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    f.debug_struct("FjallLogStore").finish()
  }
}

impl FjallLogStore<TypeConfig> {
  pub fn create(engine: Arc<FjallEngine>) -> Result<Self, Error> {
    let cf_meta = engine
      .keyspace(LOG_META_FAMILY)
      .map_err(|e| Error::other(e.to_string()))?;
    let cf_logs = engine
      .keyspace(LOG_DATA_FAMILY)
      .map_err(|e| Error::other(e.to_string()))?;

    Ok(Self {
      engine,
      cf_meta,
      cf_logs,
      _p: Default::default(),
    })
  }

  fn get_meta<M: StoreMeta>(&self) -> Result<Option<M::Value>, io::Error> {
    let bytes = self
      .cf_meta
      .get(M::KEY)
      .map_err(|e| Error::other(M::read_err(e).to_string()))?;

    let Some(bytes) = bytes else {
      return Ok(None);
    };

    let entry = M::Value::decode_from(&bytes).map_err(read_logs_err)?;
    Ok(Some(entry))
  }

  fn put_meta<M: StoreMeta>(&self, value: &M::Value) -> Result<(), io::Error> {
    let encoded = value.encode_to()?;
    self
      .cf_meta
      .insert(M::KEY, encoded)
      .map_err(|e| Error::other(M::write_err(value, e).to_string()))?;
    Ok(())
  }

  fn delete_meta<M: StoreMeta>(&self) -> Result<(), io::Error> {
    self
      .cf_meta
      .remove(M::KEY)
      .map_err(|e| Error::other(M::delete_err(e).to_string()))?;
    Ok(())
  }

  pub fn get_last_purged_log_id(&self) -> Result<Option<LogId>, io::Error> {
    self.get_meta::<LastPurged>()
  }

  pub fn set_last_purged_log_id(&self, log_id: &LogId) -> Result<(), io::Error> {
    self.put_meta::<LastPurged>(log_id)
  }

  pub fn get_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
    self.get_meta::<super::meta::Vote>()
  }

  pub fn set_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
    self.put_meta::<super::meta::Vote>(vote)
  }

  pub fn set_committed(&self, committed: &Option<LogIdOf<TypeConfig>>) -> Result<(), io::Error> {
    if let Some(committed) = committed {
      self.put_meta::<super::meta::Committed>(committed)?;
    } else {
      self.delete_meta::<super::meta::Committed>()?;
    }
    Ok(())
  }

  pub fn get_committed(&self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
    self.get_meta::<super::meta::Committed>()
  }

  fn remove_range<R: RangeBounds<[u8; 8]>>(&self, range: R) -> Result<(), io::Error> {
    let total_count = super::batch_remove_guards(
      self.engine.db(),
      &self.cf_logs,
      self.cf_logs.range(range),
      5000,
    )?;
    if total_count > 0 {
      self.engine.persist().map_err(read_logs_err)?;
    }
    Ok(())
  }
}

impl RaftLogReader<TypeConfig> for FjallLogStore<TypeConfig> {
  async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
    &mut self,
    range: RB,
  ) -> Result<Vec<Entry>, io::Error> {
    let start_idx = match range.start_bound() {
      Bound::Included(&x) => x,
      Bound::Excluded(&x) => x.saturating_add(1),
      Bound::Unbounded => 0,
    };

    let start = id_to_bin(start_idx);
    let capacity = match range.end_bound() {
      Bound::Included(&end) if end >= start_idx => (end - start_idx + 1) as usize,
      Bound::Excluded(&end) if end > start_idx => (end - start_idx) as usize,
      _ => 16,
    };
    let mut entries = Vec::with_capacity(capacity.min(4096));

    for g in self.cf_logs.range(start..) {
      let (key, val) = g.into_inner().map_err(read_logs_err)?;
      let id = bin_to_id(&key)?;

      if !range.contains(&id) {
        break;
      }

      let compact: CompactEntry = decode(&val).map_err(read_logs_err)?;
      let entry: Entry = compact.into();
      entries.push(entry);
    }

    Ok(entries)
  }

  async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
    self.get_vote()
  }
}

impl RaftLogStorage<TypeConfig> for FjallLogStore<TypeConfig> {
  type LogReader = Self;

  async fn get_log_state(&mut self) -> Result<LogState, io::Error> {
    let last = self.cf_logs.iter().next_back();

    let last_log_id = match last {
      None => None,
      Some(g) => {
        let (_, val) = g.into_inner().map_err(read_logs_err)?;
        let compact: CompactEntry = decode(&val).map_err(read_logs_err)?;
        Some(compact.log_id.into())
      }
    };

    let last_purged_log_id = self.get_last_purged_log_id()?;
    let last_log_id = last_log_id.or(last_purged_log_id);

    Ok(LogState {
      last_purged_log_id,
      last_log_id,
    })
  }

  async fn save_committed(
    &mut self,
    committed: Option<LogIdOf<TypeConfig>>,
  ) -> Result<(), io::Error> {
    self.set_committed(&committed)
  }

  async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
    self.get_committed()
  }

  async fn get_log_reader(&mut self) -> Self::LogReader {
    self.clone()
  }

  async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
    self.put_meta::<super::meta::Vote>(vote)?;
    self.engine.persist().map_err(read_logs_err)?;
    Ok(())
  }

  async fn append<I>(
    &mut self,
    entries: I,
    callback: IOFlushed<TypeConfig>,
  ) -> Result<(), io::Error>
  where
    I: IntoIterator<Item = EntryOf<TypeConfig>> + Send,
  {
    let mut batch = self.engine.db().batch();
    let mut buffer = bitcode::Buffer::new();

    for entry in entries {
      let id = id_to_bin(entry.index());
      let compact = CompactEntry::from(entry);
      let data = buffer.encode(&compact);
      batch.insert(&self.cf_logs, id, data);
    }

    batch.commit().map_err(read_logs_err)?;

    callback.io_completed(Ok(()));
    Ok(())
  }

  async fn truncate_after(&mut self, log_id: Option<LogIdOf<TypeConfig>>) -> Result<(), io::Error> {
    log::debug!("truncate_after: [{log_id:?}, +oo)");

    let start_idx = match log_id {
      Some(ref id) => id.index() + 1,
      None => 0,
    };

    self.remove_range(id_to_bin(start_idx)..)
  }

  async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
    log::debug!("delete_log: [0, {log_id:?}]");

    self.set_last_purged_log_id(&log_id)?;
    self.remove_range(..=id_to_bin(log_id.index()))
  }
}

#[inline]
pub fn id_to_bin(id: u64) -> [u8; 8] {
  id.to_be_bytes()
}

#[inline]
pub fn bin_to_id(buf: &[u8]) -> Result<u64, Error> {
  buf
    .first_chunk::<8>()
    .copied()
    .map(u64::from_be_bytes)
    .ok_or_else(|| Error::other("Buffer too short for u64"))
}
