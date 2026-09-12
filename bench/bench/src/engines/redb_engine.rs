#[cfg(feature = "redb")]
use std::{
  path::Path,
  sync::atomic::{AtomicBool, Ordering},
};

#[cfg(feature = "redb")]
use redb::{Database, Durability, ReadableDatabase, ReadableTableMetadata, TableDefinition};

#[cfg(feature = "redb")]
use crate::{
  error::{Error, Result},
  traits::*,
};

#[cfg(feature = "redb")]
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bench");

#[cfg(feature = "redb")]
pub struct RedbEngine {
  db: Database,
}

#[cfg(feature = "redb")]
impl RedbEngine {
  pub fn open(path: &Path, cache_size: usize) -> Result<Self> {
    let db = Database::builder()
      .set_cache_size(cache_size)
      .create(path)
      .map_err(|e| Error::Engine(e.to_string()))?;
    Ok(Self { db })
  }
}

#[cfg(feature = "redb")]
impl BenchDatabase for RedbEngine {
  type Connection<'a> = RedbConnection<'a>;

  fn name() -> &'static str {
    "redb"
  }

  fn connect(&self) -> Self::Connection<'_> {
    RedbConnection {
      db: &self.db,
      sync: AtomicBool::new(false),
    }
  }

  fn compact(&mut self) -> bool {
    self.db.compact().is_ok()
  }
}

#[cfg(feature = "redb")]
pub struct RedbConnection<'a> {
  db: &'a Database,
  sync: AtomicBool,
}

#[cfg(feature = "redb")]
impl BenchDatabaseConnection for RedbConnection<'_> {
  type WriteTxn<'txn>
    = RedbWriteTxn
  where
    Self: 'txn;
  type ReadTxn<'txn>
    = RedbReadTxn
  where
    Self: 'txn;

  fn set_sync(&mut self, sync: bool) -> bool {
    self.sync.store(sync, Ordering::Relaxed);
    true
  }

  fn write_transaction(&self) -> Self::WriteTxn<'_> {
    let mut txn = self.db.begin_write().expect("redb begin_write 失败");
    if !self.sync.load(Ordering::Relaxed) {
      let _ = txn.set_durability(Durability::None);
    }
    RedbWriteTxn { txn }
  }

  fn read_transaction(&self) -> Self::ReadTxn<'_> {
    let txn = self.db.begin_read().expect("redb begin_read 失败");
    RedbReadTxn { txn }
  }
}

#[cfg(feature = "redb")]
pub struct RedbWriteTxn {
  txn: redb::WriteTransaction,
}

#[cfg(feature = "redb")]
impl BenchWriteTransaction for RedbWriteTxn {
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    let mut table = self
      .txn
      .open_table(TABLE)
      .map_err(|e| Error::Engine(e.to_string()))?;
    table
      .insert(key, value)
      .map(|_| ())
      .map_err(|e| Error::Engine(e.to_string()))
  }

  fn remove(&mut self, key: &[u8]) -> Result<()> {
    let mut table = self
      .txn
      .open_table(TABLE)
      .map_err(|e| Error::Engine(e.to_string()))?;
    table
      .remove(key)
      .map(|_| ())
      .map_err(|e| Error::Engine(e.to_string()))
  }

  fn commit(self) -> Result<()> {
    self.txn.commit().map_err(|e| Error::Engine(e.to_string()))
  }
}

#[cfg(feature = "redb")]
pub struct RedbReadTxn {
  txn: redb::ReadTransaction,
}

#[cfg(feature = "redb")]
impl BenchReadTransaction for RedbReadTxn {
  fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
    let table = self.txn.open_table(TABLE).ok()?;
    let guard = table.get(key).ok()??;
    Some(guard.value().to_vec())
  }

  fn range_scan(&mut self, start_key: &[u8], count: usize) -> (usize, u64) {
    let Ok(table) = self.txn.open_table(TABLE) else {
      return (0, 0);
    };
    let Ok(iter) = table.range(start_key..) else {
      return (0, 0);
    };
    let mut scanned = 0;
    let mut sum = 0u64;
    for (_k, v) in iter.take(count).flatten() {
      scanned += 1;
      sum += v.value().first().copied().unwrap_or(0) as u64;
    }
    (scanned, sum)
  }

  fn len(&mut self) -> u64 {
    let Ok(table) = self.txn.open_table(TABLE) else {
      return 0;
    };
    ReadableTableMetadata::len(&table).unwrap_or(0)
  }
}
