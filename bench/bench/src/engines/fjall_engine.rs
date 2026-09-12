#[cfg(feature = "fjall")]
use std::{
  path::Path,
  sync::atomic::{AtomicBool, Ordering},
};

#[cfg(feature = "fjall")]
use fjall::{
  KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};

#[cfg(feature = "fjall")]
use crate::{
  error::{Error, Result},
  traits::*,
  types::MemoryBudget,
};

#[cfg(feature = "fjall")]
const FJALL_MAX_WRITES_PER_TXN: usize = 100_000;

#[cfg(feature = "fjall")]
pub struct FjallEngine {
  db: SingleWriterTxDatabase,
  part: SingleWriterTxKeyspace,
}

#[cfg(feature = "fjall")]
impl FjallEngine {
  pub fn open(path: &Path, cache_size: usize) -> fjall::Result<Self> {
    let budget = MemoryBudget::new(cache_size);
    let db = SingleWriterTxDatabase::builder(path)
      .cache_size(budget.read_cache_bytes as u64)
      .open()?;
    let max_mem = budget.memtable_size as u64;
    let part = db.keyspace("bench", move || {
      KeyspaceCreateOptions::default().max_memtable_size(max_mem)
    })?;
    Ok(Self { db, part })
  }
}

#[cfg(feature = "fjall")]
impl BenchDatabase for FjallEngine {
  type Connection<'a> = FjallConnection<'a>;

  fn name() -> &'static str {
    "fjall"
  }

  fn connect(&self) -> Self::Connection<'_> {
    FjallConnection {
      db: &self.db,
      part: self.part.clone(),
      sync: AtomicBool::new(false),
    }
  }

  fn flush(&mut self) {
    let _ = self.db.persist(PersistMode::SyncAll);
  }

  fn compact(&mut self) -> bool {
    let _ = self.db.persist(PersistMode::SyncAll);
    true
  }
}

#[cfg(feature = "fjall")]
pub struct FjallConnection<'a> {
  db: &'a SingleWriterTxDatabase,
  part: SingleWriterTxKeyspace,
  sync: AtomicBool,
}

#[cfg(feature = "fjall")]
impl BenchDatabaseConnection for FjallConnection<'_> {
  type WriteTxn<'txn>
    = FjallWriteTxn<'txn>
  where
    Self: 'txn;
  type ReadTxn<'txn>
    = FjallReadTxn
  where
    Self: 'txn;

  fn set_sync(&mut self, sync: bool) -> bool {
    self.sync.store(sync, Ordering::Relaxed);
    true
  }

  fn write_transaction(&self) -> Self::WriteTxn<'_> {
    let txn = self.db.write_tx();
    FjallWriteTxn {
      db: self.db,
      part: &self.part,
      txn: Some(txn),
      sync: self.sync.load(Ordering::Relaxed),
      counter: 0,
    }
  }

  fn read_transaction(&self) -> Self::ReadTxn<'_> {
    let txn = self.db.read_tx();
    FjallReadTxn {
      part: self.part.clone(),
      txn,
    }
  }
}

#[cfg(feature = "fjall")]
pub struct FjallWriteTxn<'a> {
  db: &'a SingleWriterTxDatabase,
  part: &'a SingleWriterTxKeyspace,
  txn: Option<fjall::SingleWriterWriteTx<'a>>,
  sync: bool,
  counter: usize,
}

#[cfg(feature = "fjall")]
impl BenchWriteTransaction for FjallWriteTxn<'_> {
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    self.counter += 1;
    if self.counter >= FJALL_MAX_WRITES_PER_TXN {
      if let Some(old_txn) = self.txn.take() {
        old_txn.commit().map_err(|e| Error::Engine(e.to_string()))?;
      }
      self.txn = Some(self.db.write_tx());
      self.counter = 0;
    }
    if let Some(txn) = &mut self.txn {
      txn.insert(self.part, key, value);
    }
    Ok(())
  }

  fn remove(&mut self, key: &[u8]) -> Result<()> {
    self.counter += 1;
    if self.counter >= FJALL_MAX_WRITES_PER_TXN {
      if let Some(old_txn) = self.txn.take() {
        old_txn.commit().map_err(|e| Error::Engine(e.to_string()))?;
      }
      self.txn = Some(self.db.write_tx());
      self.counter = 0;
    }
    if let Some(txn) = &mut self.txn {
      txn.remove(self.part, key);
    }
    Ok(())
  }

  fn commit(mut self) -> Result<()> {
    if let Some(txn) = self.txn.take() {
      txn.commit().map_err(|e| Error::Engine(e.to_string()))?;
    }
    let mode = if self.sync {
      PersistMode::SyncAll
    } else {
      PersistMode::Buffer
    };
    self
      .db
      .persist(mode)
      .map_err(|e| Error::Engine(e.to_string()))
  }
}

#[cfg(feature = "fjall")]
pub struct FjallReadTxn {
  part: SingleWriterTxKeyspace,
  txn: fjall::Snapshot,
}

#[cfg(feature = "fjall")]
impl BenchReadTransaction for FjallReadTxn {
  fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
    self
      .txn
      .get(&self.part, key)
      .ok()
      .flatten()
      .map(|s| s.to_vec())
  }

  fn range_scan(&mut self, start_key: &[u8], count: usize) -> (usize, u64) {
    let iter = self.txn.range(&self.part, start_key..);
    let mut scanned = 0;
    let mut sum = 0u64;
    for guard in iter.take(count) {
      if let Ok((_k, v)) = guard.into_inner() {
        scanned += 1;
        sum += v.first().copied().unwrap_or(0) as u64;
      }
    }
    (scanned, sum)
  }

  fn len(&mut self) -> u64 {
    self.txn.len(&self.part).unwrap_or(0) as u64
  }
}
