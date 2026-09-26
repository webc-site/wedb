#![cfg(feature = "wkv")]

use std::{
  cell::RefCell,
  fmt,
  future::Future,
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::{CheckpointType, CompactionType, StoreConfig, StoreSession, WedbStore};

use crate::{
  error::{Error, Result},
  traits::*,
};

const SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
/// 扇区大小取设备缺省口径（bench 工程不引 wbase，本地常量对齐 wdev 的 4096 缺省值）
const SECTOR_SIZE: usize = 4096;

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

/// wkv 错误统一转包装（禁静默吞：紧缩链任一步 Err 向上转 false，harness 诚实记 N/A）
fn engine_err(e: impl fmt::Display) -> Error {
  Error::Engine(e.to_string())
}

/// wkv 评测引擎包装
pub struct WkvEngine {
  pub store: Arc<WedbStore<SegmentedDevice>>,
  /// 检查点子目录（与段文件同处数据库目录，随 database_size 目录整体如实计数）
  checkpoint_dir: PathBuf,
}

impl WkvEngine {
  pub fn open(path: &Path, cache_size: usize, elements: usize) -> Result<Self> {
    let device = Arc::new(
      SegmentedDevice::new(path, SEGMENT_SIZE, SECTOR_SIZE)
        .map_err(|e| Error::Engine(e.to_string()))?,
    );
    let config =
      StoreConfig::from_memory_budget_with_keys(cache_size as u64, Some(elements as u64));
    let store =
      Arc::new(WedbStore::open(config, device).map_err(|e| Error::Engine(e.to_string()))?);
    let mut checkpoint_dir = path.as_os_str().to_owned();
    checkpoint_dir.push("_ckpt");
    Ok(Self {
      store,
      checkpoint_dir: PathBuf::from(checkpoint_dir),
    })
  }

  /// 真紧缩链（对标 garnet libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/
  /// LogAccessor.cs:Compact 与 wkv/tests/compact/basic.rs:57-99 标准调用形态）：
  /// flush_all 落盘封印紧缩窗 → tail 向下取整段边界在线紧缩（末段留活）→
  /// FoldOver 检查点抬升物理删段地板，触发窗下段文件删除，目录字节真实回落
  async fn compact_once(&self) -> Result<()> {
    self.store.flush_all().await.map_err(engine_err)?;
    let until = self.store.tail_address() / SEGMENT_SIZE * SEGMENT_SIZE;
    self
      .store
      .compact(until, CompactionType::Scan)
      .await
      .map_err(engine_err)?;
    self
      .store
      .create_checkpoint(&self.checkpoint_dir, CheckpointType::FoldOver)
      .await
      .map_err(engine_err)?;
    Ok(())
  }
}

impl BenchDatabase for WkvEngine {
  type Connection<'a> = WkvConnection;

  fn name() -> &'static str {
    "wkv"
  }

  fn connect(&self) -> Self::Connection<'_> {
    let session = self.store.new_session().expect("wkv new_session 失败");
    // 默认持久写契约 (commit 执行 flush_all 设备物理 sync)
    // 对标上游 redb-bench crates/redb-bench/src/lib.rs:1020 (connect 默认 sync: true)
    WkvConnection {
      store: self.store.clone(),
      session,
      sync: AtomicBool::new(true),
    }
  }

  fn flush(&mut self) {
    with_runtime(async {
      let _ = self.store.flush_and_evict_all().await;
    });
  }

  fn compact(&mut self) -> bool {
    // 紧缩窗只处理已固化区间，先 flush 后接 wkv 在线紧缩 + 检查点物理删段
    with_runtime(self.compact_once()).is_ok()
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

  fn warm_up(&mut self) {
    // 立即建立本线程 compio Runtime，替代计时窗内首次磁盘回退读时的惰性 new
    with_runtime(async {});
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
    match self.session.try_read_raw_in_memory(key, |val| val.to_vec()) {
      Ok(wkv::StoreResult::Success(val)) => return Some(val),
      Ok(wkv::StoreResult::NotFound) => return None,
      _ => {}
    }
    with_runtime(async { self.session.read_raw(key).await.ok().flatten() })
  }

  fn range_scan(&mut self, start_key: &[u8], count: usize) -> (usize, u64) {
    // wkv 哈希索引无键序迭代能力，范围前向扫按本仓 wkv 扫描单机制承接：锚定起始键
    // 记录的逻辑地址，沿 hlog 地址游标正向步进 count 条记录（对标 C#
    // libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:Scan，
    // 与 wnode array_key_iteration_functions.rs:scan_cursor 的 SCAN 游标同源同形），
    // 真实读页/内存环并累加值首字节校验和，杜绝 (0,0) 空桩
    let _gate = self.session.enter_batch();
    let Ok(Some(anchor)) = self.session.find_tag_cooperative(start_key) else {
      return (0, 0);
    };
    let until = self.store.tail_address();
    let store = self.store;
    with_runtime(async {
      let mut it = store.hlog.scan_iter(anchor, until);
      let mut scanned = 0usize;
      let mut sum = 0u64;
      while scanned < count {
        match it
          .next_ref(|item| {
            if item.rec.is_tombstone() {
              return Ok(None);
            }
            Ok(Some(item.rec.value().first().copied().unwrap_or(0) as u64))
          })
          .await
        {
          Ok(Some(Some(b))) => {
            scanned += 1;
            sum += b;
          }
          // 墓碑轮次游标已前进，继续步进；扫描窗耗尽或 IO 错误如实收口
          Ok(Some(None)) => {}
          Ok(None) | Err(_) => break,
        }
      }
      (scanned, sum)
    })
  }

  fn len(&mut self) -> u64 {
    self.store.entry_count() as u64
  }
}
