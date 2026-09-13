#![cfg(feature = "wbftree")]

use std::{
  fs,
  panic::{AssertUnwindSafe, catch_unwind, set_hook},
  path::Path,
  sync::{
    Arc, Once,
    atomic::{AtomicU64, Ordering},
  },
};

use wbftree::{BfTreeInsertResult, BfTreeReadResult, BfTreeService, ScanReturnField};

use crate::{
  error::{Error, Result},
  traits::*,
};

static SILENT_PANIC_HOOK: Once = Once::new();

/// wbftree 评测引擎包装
pub struct WbftreeEngine {
  pub tree: Arc<BfTreeService>,
  pub len_counter: Arc<AtomicU64>,
}

impl WbftreeEngine {
  pub fn open(path: &Path, cache_size: usize) -> Result<Self> {
    SILENT_PANIC_HOOK.call_once(|| {
      set_hook(Box::new(|_| {}));
    });
    if let Some(parent) = path.parent()
      && !parent.as_os_str().is_empty()
    {
      fs::create_dir_all(parent)?;
    }
    let mut config = BfTreeService::preset_config(4);
    if cache_size > 0 {
      config.cb_size_byte(cache_size);
    }
    config.file_path(path);
    let tree = Arc::new(BfTreeService::new(config).map_err(|e| Error::Engine(e.to_string()))?);
    Ok(Self {
      tree,
      len_counter: Arc::new(AtomicU64::new(0)),
    })
  }
}

impl BenchDatabase for WbftreeEngine {
  type Connection<'a> = WbftreeConnection;

  fn name() -> &'static str {
    "wbftree"
  }

  fn connect(&self) -> Self::Connection<'_> {
    WbftreeConnection {
      tree: self.tree.clone(),
      len_counter: self.len_counter.clone(),
    }
  }

  fn compact(&mut self) -> bool {
    false
  }
}

pub struct WbftreeConnection {
  pub tree: Arc<BfTreeService>,
  pub len_counter: Arc<AtomicU64>,
}

impl BenchDatabaseConnection for WbftreeConnection {
  type WriteTxn<'txn> = WbftreeWriteTxn<'txn>;
  type ReadTxn<'txn> = WbftreeReadTxn<'txn>;

  fn set_sync(&mut self, _sync: bool) -> bool {
    false
  }

  fn write_transaction(&self) -> Self::WriteTxn<'_> {
    WbftreeWriteTxn {
      tree: &self.tree,
      len_counter: &self.len_counter,
      delta: 0,
    }
  }

  fn read_transaction(&self) -> Self::ReadTxn<'_> {
    WbftreeReadTxn {
      tree: &self.tree,
      len_counter: &self.len_counter,
    }
  }
}

pub struct WbftreeWriteTxn<'a> {
  pub tree: &'a BfTreeService,
  pub len_counter: &'a AtomicU64,
  pub delta: i64,
}

impl BenchWriteTransaction for WbftreeWriteTxn<'_> {
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    if self.tree.insert(key, value) == BfTreeInsertResult::Success {
      self.delta += 1;
      Ok(())
    } else {
      Err(Error::Engine("wbftree insert 失败".into()))
    }
  }

  fn remove(&mut self, key: &[u8]) -> Result<()> {
    self.tree.delete(key);
    self.delta -= 1;
    Ok(())
  }

  fn commit(self) -> Result<()> {
    if self.delta > 0 {
      self
        .len_counter
        .fetch_add(self.delta as u64, Ordering::Relaxed);
    } else if self.delta < 0 {
      self
        .len_counter
        .fetch_sub((-self.delta) as u64, Ordering::Relaxed);
    }
    Ok(())
  }
}

pub struct WbftreeReadTxn<'a> {
  pub tree: &'a BfTreeService,
  pub len_counter: &'a AtomicU64,
}

impl BenchReadTransaction for WbftreeReadTxn<'_> {
  fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
    let (res, val) = self.tree.read(key);
    if res == BfTreeReadResult::Found {
      val
    } else {
      None
    }
  }

  fn range_scan(&mut self, start_key: &[u8], count: usize) -> (usize, u64) {
    let mut sum = 0u64;
    let mut count_scanned = 0usize;
    let _ = catch_unwind(AssertUnwindSafe(|| {
      self.tree.scan_with_count_callback(
        start_key,
        count,
        ScanReturnField::KeyAndValue,
        |_k, val| {
          sum += val.first().copied().unwrap_or(0) as u64;
          count_scanned += 1;
          true
        },
      )
    }));
    (count_scanned, sum)
  }

  fn len(&mut self) -> u64 {
    self.len_counter.load(Ordering::Relaxed)
  }
}
