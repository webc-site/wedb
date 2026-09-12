#[cfg(feature = "rocksdb")]
use std::{
  mem::take,
  path::Path,
  sync::atomic::{AtomicBool, Ordering},
  thread::available_parallelism,
};

#[cfg(feature = "rocksdb")]
use rocksdb::{
  BlockBasedOptions, Cache, DB, Direction, IteratorMode, Options, ReadOptions, WriteBatch,
  WriteOptions,
};

#[cfg(feature = "rocksdb")]
use crate::{
  error::{Error, Result},
  traits::*,
  types::MemoryBudget,
};

#[cfg(feature = "rocksdb")]
const ROCKSDB_MAX_WRITES_PER_TXN: usize = 100_000;

#[cfg(feature = "rocksdb")]
pub struct RocksdbEngine {
  db: DB,
}

#[cfg(feature = "rocksdb")]
impl RocksdbEngine {
  pub fn open(path: &Path, cache_size: usize) -> Result<Self> {
    let budget = MemoryBudget::new(cache_size);
    let cache = Cache::new_lru_cache(budget.read_cache_bytes);

    let mut bb = BlockBasedOptions::default();
    bb.set_block_cache(&cache);
    bb.set_bloom_filter(10.0, false);
    bb.set_cache_index_and_filter_blocks(true);
    bb.set_pin_l0_filter_and_index_blocks_in_cache(true);
    bb.set_pin_top_level_index_and_filter(true);

    let mut opts = Options::default();
    opts.set_block_based_table_factory(&bb);
    opts.set_write_buffer_size(budget.memtable_size);
    opts.set_max_write_buffer_number(budget.max_memtable_count as i32);
    opts.set_min_write_buffer_number_to_merge(1);
    opts.set_max_write_buffer_size_to_maintain(budget.write_buffer_bytes as i64);
    opts.create_if_missing(true);
    opts.increase_parallelism(available_parallelism().map_or(1, |n| n.get()) as i32);

    let db = DB::open(&opts, path).map_err(|e| Error::Engine(e.to_string()))?;
    Ok(Self { db })
  }
}

#[cfg(feature = "rocksdb")]
impl BenchDatabase for RocksdbEngine {
  type Connection<'a> = RocksdbConnection<'a>;

  fn name() -> &'static str {
    "rocksdb"
  }

  fn connect(&self) -> Self::Connection<'_> {
    RocksdbConnection {
      db: &self.db,
      sync: AtomicBool::new(false),
    }
  }

  fn flush(&mut self) {
    let _ = self.db.flush_wal(true);
  }

  fn compact(&mut self) -> bool {
    let _ = self.db.flush_wal(true);
    self.db.compact_range::<&[u8], &[u8]>(None, None);
    true
  }
}

#[cfg(feature = "rocksdb")]
pub struct RocksdbConnection<'a> {
  db: &'a DB,
  sync: AtomicBool,
}

#[cfg(feature = "rocksdb")]
impl BenchDatabaseConnection for RocksdbConnection<'_> {
  type WriteTxn<'txn>
    = RocksdbWriteTxn<'txn>
  where
    Self: 'txn;
  type ReadTxn<'txn>
    = RocksdbReadTxn<'txn>
  where
    Self: 'txn;

  fn set_sync(&mut self, sync: bool) -> bool {
    self.sync.store(sync, Ordering::Relaxed);
    true
  }

  fn write_transaction(&self) -> Self::WriteTxn<'_> {
    RocksdbWriteTxn {
      db: self.db,
      batch: WriteBatch::default(),
      sync: self.sync.load(Ordering::Relaxed),
      counter: 0,
    }
  }

  fn read_transaction(&self) -> Self::ReadTxn<'_> {
    RocksdbReadTxn { db: self.db }
  }
}

#[cfg(feature = "rocksdb")]
pub struct RocksdbWriteTxn<'a> {
  db: &'a DB,
  batch: WriteBatch,
  sync: bool,
  counter: usize,
}

#[cfg(feature = "rocksdb")]
impl BenchWriteTransaction for RocksdbWriteTxn<'_> {
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    self.counter += 1;
    self.batch.put(key, value);
    if self.counter >= ROCKSDB_MAX_WRITES_PER_TXN {
      let mut write_opt = WriteOptions::new();
      // 大批量写入中间切分批次不阻塞 fsync，保持流式吞吐
      write_opt.set_sync(false);
      let batch = take(&mut self.batch);
      self
        .db
        .write_opt(batch, &write_opt)
        .map_err(|e| Error::Engine(e.to_string()))?;
      self.counter = 0;
    }
    Ok(())
  }

  fn remove(&mut self, key: &[u8]) -> Result<()> {
    self.counter += 1;
    self.batch.delete(key);
    if self.counter >= ROCKSDB_MAX_WRITES_PER_TXN {
      let mut write_opt = WriteOptions::new();
      write_opt.set_sync(false);
      let batch = take(&mut self.batch);
      self
        .db
        .write_opt(batch, &write_opt)
        .map_err(|e| Error::Engine(e.to_string()))?;
      self.counter = 0;
    }
    Ok(())
  }

  fn commit(mut self) -> Result<()> {
    if !self.batch.is_empty() {
      let mut write_opt = WriteOptions::new();
      write_opt.set_sync(self.sync);
      let batch = take(&mut self.batch);
      self
        .db
        .write_opt(batch, &write_opt)
        .map_err(|e| Error::Engine(e.to_string()))?;
    }
    Ok(())
  }
}

#[cfg(feature = "rocksdb")]
pub struct RocksdbReadTxn<'a> {
  db: &'a DB,
}

#[cfg(feature = "rocksdb")]
impl BenchReadTransaction for RocksdbReadTxn<'_> {
  fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
    let mut read_opt = ReadOptions::default();
    read_opt.set_verify_checksums(false);
    self.db.get_opt(key, &read_opt).ok().flatten()
  }

  fn range_scan(&mut self, start_key: &[u8], count: usize) -> (usize, u64) {
    let mut read_opt = ReadOptions::default();
    read_opt.set_verify_checksums(false);
    let iter = self
      .db
      .iterator_opt(IteratorMode::From(start_key, Direction::Forward), read_opt);
    let mut scanned = 0;
    let mut sum = 0u64;
    for (_k, v) in iter.take(count).flatten() {
      scanned += 1;
      sum += v.first().copied().unwrap_or(0) as u64;
    }
    (scanned, sum)
  }

  fn len(&mut self) -> u64 {
    let mut read_opt = ReadOptions::default();
    read_opt.set_verify_checksums(false);
    self.db.iterator_opt(IteratorMode::Start, read_opt).count() as u64
  }
}
