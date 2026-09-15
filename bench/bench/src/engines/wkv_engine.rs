#![cfg(feature = "wkv")]

use std::{
  cell::RefCell,
  future::Future,
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, StoreSession, WedbStore};

use crate::{
  error::{Error, Result},
  traits::*,
};

const SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

thread_local! {
  static COMPIO_RT: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

fn with_runtime<F: Future>(f: F) -> F::Output {
  COMPIO_RT.with(|cell| {
    cell
      .borrow_mut()
      .get_or_insert_with(|| Runtime::new().expect("compio runtime 初始化失败"))
      .block_on(f)
  })
}

/// wkv 评测引擎包装
pub struct WkvEngine {
  pub store: Arc<WedbStore<SegmentedDevice>>,
}

impl WkvEngine {
  pub fn open(path: &Path, cache_size: usize, elements: usize) -> Result<Self> {
    let device = Arc::new(
      SegmentedDevice::segmented(path, SEGMENT_SIZE).map_err(|e| Error::Engine(e.to_string()))?,
    );
    let config =
      StoreConfig::from_memory_budget_with_keys(cache_size as u64, Some(elements as u64));
    let store =
      Arc::new(WedbStore::open(config, device).map_err(|e| Error::Engine(e.to_string()))?);
    Ok(Self { store })
  }
}

impl BenchDatabase for WkvEngine {
  type Connection<'a> = WkvConnection;

  fn name() -> &'static str {
    "wkv"
  }

  fn connect(&self) -> Self::Connection<'_> {
    let session = self.store.new_session().expect("wkv new_session 失败");
    WkvConnection {
      store: self.store.clone(),
      session,
      sync: AtomicBool::new(false),
    }
  }

  fn flush(&mut self) {
    with_runtime(async {
      let _ = self.store.flush_and_evict_all().await;
    });
  }

  fn compact(&mut self) -> bool {
    with_runtime(async {
      let _ = self.store.flush_and_evict_all().await;
    });
    true
  }
}

pub struct WkvConnection {
  pub store: Arc<WedbStore<SegmentedDevice>>,
  pub session: StoreSession<SegmentedDevice>,
  pub sync: AtomicBool,
}

impl BenchDatabaseConnection for WkvConnection {
  type WriteTxn<'txn> = WkvWriteTxn<'txn>;
  type ReadTxn<'txn> = WkvReadTxn<'txn>;

  fn set_sync(&mut self, sync: bool) -> bool {
    self.sync.store(sync, Ordering::Relaxed);
    true
  }

  fn write_transaction(&self) -> Self::WriteTxn<'_> {
    WkvWriteTxn {
      store: &self.store,
      session: &self.session,
      sync: self.sync.load(Ordering::Relaxed),
    }
  }

  fn read_transaction(&self) -> Self::ReadTxn<'_> {
    WkvReadTxn {
      store: &self.store,
      session: &self.session,
    }
  }
}

pub struct WkvWriteTxn<'a> {
  pub store: &'a WedbStore<SegmentedDevice>,
  pub session: &'a StoreSession<SegmentedDevice>,
  pub sync: bool,
}

impl BenchWriteTransaction for WkvWriteTxn<'_> {
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    match self.session.try_upsert_raw_sync(key, value) {
      Ok(Ok(_)) => Ok(()),
      Ok(Err(_)) => with_runtime(async {
        self
          .session
          .upsert_raw(key, value)
          .await
          .map(|_| ())
          .map_err(|e| Error::Engine(e.to_string()))
      }),
      Err(e) => Err(Error::Engine(e.to_string())),
    }
  }

  fn remove(&mut self, key: &[u8]) -> Result<()> {
    match self.session.try_delete_raw_sync(key) {
      Ok(Ok(_)) => Ok(()),
      Ok(Err(_)) => with_runtime(async {
        self
          .session
          .delete_raw(key)
          .await
          .map(|_| ())
          .map_err(|e| Error::Engine(e.to_string()))
      }),
      Err(e) => Err(Error::Engine(e.to_string())),
    }
  }

  fn commit(self) -> Result<()> {
    if self.sync {
      with_runtime(async {
        self
          .store
          .flush_all()
          .await
          .map_err(|e| Error::Engine(e.to_string()))
      })?;
    }
    Ok(())
  }
}

pub struct WkvReadTxn<'a> {
  pub store: &'a WedbStore<SegmentedDevice>,
  pub session: &'a StoreSession<SegmentedDevice>,
}

impl BenchReadTransaction for WkvReadTxn<'_> {
  fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
    if let Ok(Some(res)) = self.session.try_read_raw_in_memory(key, |val| val.to_vec()) {
      return res;
    }
    with_runtime(async { self.session.read_raw(key).await.ok().flatten() })
  }

  fn range_scan(&mut self, _start_key: &[u8], _count: usize) -> (usize, u64) {
    (0, 0)
  }

  fn len(&mut self) -> u64 {
    self.store.entry_count() as u64
  }
}
