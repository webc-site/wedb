#[cfg(feature = "sqlite")]
use std::path::{Path, PathBuf};

#[cfg(feature = "sqlite")]
use rusqlite::{Connection, OptionalExtension, Transaction};

#[cfg(feature = "sqlite")]
use crate::{
  error::{Error, Result},
  traits::*,
};

#[cfg(feature = "sqlite")]
pub struct SqliteEngine {
  path: PathBuf,
  cache_size: usize,
}

#[cfg(feature = "sqlite")]
impl SqliteEngine {
  pub fn open(path: &Path, cache_size: usize) -> Result<Self> {
    let conn = Connection::open(path).map_err(|e| Error::Engine(e.to_string()))?;
    let cache_pages = -((cache_size / 1024) as i64);
    conn
      .execute_batch(&format!(
        "PRAGMA journal_mode = WAL;
       PRAGMA synchronous = NORMAL;
       PRAGMA cache_size = {cache_pages};
       PRAGMA mmap_size = 0;
       PRAGMA temp_store = FILE;
       PRAGMA wal_autocheckpoint = 1000;
       PRAGMA auto_vacuum = INCREMENTAL;
       CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB);"
      ))
      .map_err(|e| Error::Engine(e.to_string()))?;
    Ok(Self {
      path: path.to_path_buf(),
      cache_size,
    })
  }
}

#[cfg(feature = "sqlite")]
impl BenchDatabase for SqliteEngine {
  type Connection<'a> = SqliteConnection;

  fn name() -> &'static str {
    "sqlite"
  }

  fn connect(&self) -> Self::Connection<'_> {
    let conn = Connection::open(&self.path).expect("sqlite connect 失败");
    let cache_pages = -((self.cache_size / 1024) as i64);
    let _ = conn.execute_batch(&format!(
      "PRAGMA journal_mode = WAL;
       PRAGMA synchronous = NORMAL;
       PRAGMA cache_size = {cache_pages};
       PRAGMA mmap_size = 0;
       PRAGMA temp_store = FILE;"
    ));
    SqliteConnection { conn }
  }

  fn flush(&mut self) {
    if let Ok(conn) = Connection::open(&self.path) {
      let cache_pages = -((self.cache_size / 1024) as i64);
      let _ = conn.execute_batch(&format!(
        "PRAGMA cache_size = {cache_pages};
         PRAGMA wal_checkpoint(TRUNCATE);
         PRAGMA shrink_memory;"
      ));
    }
  }

  fn compact(&mut self) -> bool {
    if let Ok(conn) = Connection::open(&self.path) {
      let cache_pages = -((self.cache_size / 1024) as i64);
      let _ = conn.execute_batch(&format!(
        "PRAGMA cache_size = {cache_pages};
         PRAGMA wal_checkpoint(TRUNCATE);
         PRAGMA incremental_vacuum;
         PRAGMA wal_checkpoint(TRUNCATE);
         PRAGMA shrink_memory;"
      ));
      true
    } else {
      false
    }
  }
}

#[cfg(feature = "sqlite")]
pub struct SqliteConnection {
  conn: Connection,
}

#[cfg(feature = "sqlite")]
impl BenchDatabaseConnection for SqliteConnection {
  type WriteTxn<'txn>
    = SqliteWriteTxn<'txn>
  where
    Self: 'txn;
  type ReadTxn<'txn>
    = SqliteReadTxn<'txn>
  where
    Self: 'txn;

  fn set_sync(&mut self, sync: bool) -> bool {
    let pragma = if sync {
      "PRAGMA synchronous = FULL;"
    } else {
      "PRAGMA synchronous = NORMAL;"
    };
    self.conn.execute(pragma, []).is_ok()
  }

  fn write_transaction(&self) -> Self::WriteTxn<'_> {
    let txn = self
      .conn
      .unchecked_transaction()
      .expect("sqlite 开启事务失败");
    SqliteWriteTxn { txn }
  }

  fn read_transaction(&self) -> Self::ReadTxn<'_> {
    SqliteReadTxn { conn: &self.conn }
  }
}

#[cfg(feature = "sqlite")]
pub struct SqliteWriteTxn<'a> {
  txn: Transaction<'a>,
}

#[cfg(feature = "sqlite")]
impl BenchWriteTransaction for SqliteWriteTxn<'_> {
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    self
      .txn
      .execute(
        "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
        [key, value],
      )
      .map(|_| ())
      .map_err(|e| Error::Engine(e.to_string()))
  }

  fn remove(&mut self, key: &[u8]) -> Result<()> {
    self
      .txn
      .execute("DELETE FROM kv WHERE key = ?", [key])
      .map(|_| ())
      .map_err(|e| Error::Engine(e.to_string()))
  }

  fn commit(self) -> Result<()> {
    self.txn.commit().map_err(|e| Error::Engine(e.to_string()))
  }
}

#[cfg(feature = "sqlite")]
pub struct SqliteReadTxn<'a> {
  conn: &'a Connection,
}

#[cfg(feature = "sqlite")]
impl BenchReadTransaction for SqliteReadTxn<'_> {
  fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
    self
      .conn
      .query_row("SELECT value FROM kv WHERE key = ?", [key], |row| {
        row.get(0)
      })
      .optional()
      .ok()
      .flatten()
  }

  fn range_scan(&mut self, start_key: &[u8], count: usize) -> (usize, u64) {
    let Ok(mut stmt) = self
      .conn
      .prepare("SELECT value FROM kv WHERE key >= ? ORDER BY key LIMIT ?")
    else {
      return (0, 0);
    };
    let Ok(rows) = stmt.query_map((start_key, count as i64), |row| row.get::<_, Vec<u8>>(0)) else {
      return (0, 0);
    };
    let mut scanned = 0;
    let mut sum = 0u64;
    for r in rows.flatten() {
      scanned += 1;
      sum += r.first().copied().unwrap_or(0) as u64;
    }
    (scanned, sum)
  }

  fn len(&mut self) -> u64 {
    self
      .conn
      .query_row("SELECT COUNT(*) FROM kv", [], |row| row.get::<_, i64>(0))
      .unwrap_or(0) as u64
  }
}
