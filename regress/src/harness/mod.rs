use std::{
  cell::RefCell,
  future::Future,
  panic::set_hook,
  sync::{Arc, Once},
};

use compio::runtime::Runtime;
use itoa::Buffer;
use tempfile::TempDir;
use wbftree::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService, RangeIndexManager,
  ScanReturnField, StorageBackendType, TreeTuning,
};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, StoreResult, StoreSession, WedbStore};

/// 与 bench/ 标准对齐的默认配置参数（对齐 bench/bench/src/types.rs BenchmarkConfig::default）
pub const DEFAULT_CACHE_SIZE: usize = 128 * 1024 * 1024; // 128MB 缓存
pub const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64MB 单段文件
pub const DEFAULT_KEY_SIZE: usize = 24; // 24 字节 Key
pub const DEFAULT_VALUE_SIZE: usize = 150; // 150 字节 Value
pub const DEFAULT_BULK_ELEMENTS: usize = 6_000_000; // 600万项 (~1.04GB 原始数据)
pub const DEFAULT_NUM_READS: usize = 100_000; // 10万项点查
pub const DEFAULT_READ_ITERATIONS: usize = 3; // 3轮采样取中位数
pub const DEFAULT_NUM_SCANS: usize = 5_000; // 5000项扫描
pub const DEFAULT_SCAN_LEN: usize = 10; // 10 项范围扫描
pub const DEFAULT_SCAN_ITERATIONS: usize = 3; // 3轮采样取中位数
pub const DEFAULT_REMOVALS: usize = 50_000; // 5万项删除
pub const DEFAULT_WBF_CONCURRENCY: usize = 4; // 4 线程并发
pub const DEFAULT_RNG_SEED: u64 = 3; // 统一随机数种子

/// wbftree 基准调优 (对齐旧 preset：min=4 / max_record=4096 / key_len=512 / leaf=16KB，
/// create_bftree 以 max+1 直达引擎上限)
const TUNE_BENCH: TreeTuning = TreeTuning {
  cache_size: 0,
  min_record_size: DEFAULT_WBF_CONCURRENCY,
  max_record_size: 4096,
  max_key_len: 512,
  leaf_page_size: 16384,
};

#[inline(always)]
pub fn fill_pair(rng: &mut fastrand::Rng, key: &mut [u8], val: &mut [u8]) {
  rng.fill(key);
  rng.fill(val);
}

thread_local! {
  static COMPIO_RT: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

#[inline]
pub fn with_runtime<F: Future>(f: F) -> F::Output {
  COMPIO_RT.with(|cell| {
    cell
      .borrow_mut()
      .get_or_insert_with(|| Runtime::new().expect("compio runtime 初始化失败"))
      .block_on(f)
  })
}

/// 固定长度格式化 Key，消除运行时堆分配与 format! 格式化开销
#[inline]
pub fn make_num_key<const N: usize>(prefix: &[u8], num: usize) -> [u8; N] {
  let mut key = [b'0'; N];
  let p_len = prefix.len().min(N);
  key[..p_len].copy_from_slice(&prefix[..p_len]);
  let mut buf = Buffer::new();
  let s = buf.format(num).as_bytes();
  let s_len = s.len().min(N.saturating_sub(p_len));
  let dest_start = N - s_len;
  let src_start = s.len() - s_len;
  key[dest_start..N].copy_from_slice(&s[src_start..]);
  key
}

/// wkv 性能测试夹具
pub struct WkvHarness {
  pub store: Arc<WedbStore<SegmentedDevice>>,
  pub session: StoreSession<SegmentedDevice>,
  _temp_dir: TempDir,
}

impl WkvHarness {
  /// 对齐 bench/ 默认标准配置 (128MB 缓存, 64MB 段大小)
  pub fn default_bench(max_keys: usize) -> aok::Result<Self> {
    Self::new(DEFAULT_CACHE_SIZE, max_keys)
  }

  pub fn new(cache_size: usize, max_keys: usize) -> aok::Result<Self> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("wkv_data");
    let device = Arc::new(SegmentedDevice::segmented(&path, DEFAULT_SEGMENT_SIZE)?);
    let config =
      StoreConfig::from_memory_budget_with_keys(cache_size as u64, Some(max_keys as u64));
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;
    Ok(Self {
      store,
      session,
      _temp_dir: temp_dir,
    })
  }

  #[inline]
  pub fn upsert_sync(&self, key: &[u8], val: &[u8]) -> bool {
    match self.session.try_upsert_raw_sync(key, val) {
      Ok(Ok(_)) => true,
      Ok(Err(_page)) => with_runtime(async { self.session.upsert_raw(key, val).await.is_ok() }),
      Err(_) => false,
    }
  }

  #[inline]
  pub fn get_sync(&self, key: &[u8], out: &mut [u8]) -> Option<usize> {
    match self.session.try_read_raw_in_memory(key, |bytes| {
      let copy_len = bytes.len().min(out.len());
      out[..copy_len].copy_from_slice(&bytes[..copy_len]);
      copy_len
    }) {
      Ok(StoreResult::Success(len)) => return Some(len),
      Ok(StoreResult::NotFound) => return None,
      _ => {}
    }
    with_runtime(async {
      if let Ok(Some(vec)) = self.session.read_raw(key).await {
        let copy_len = vec.len().min(out.len());
        out[..copy_len].copy_from_slice(&vec[..copy_len]);
        Some(copy_len)
      } else {
        None
      }
    })
  }

  #[inline]
  pub fn delete_sync(&self, key: &[u8]) -> bool {
    match self.session.try_delete_raw_sync(key) {
      Ok(Ok(_)) => true,
      Ok(Err(_page)) => with_runtime(async { self.session.delete_raw(key).await.is_ok() }),
      Err(_) => false,
    }
  }

  pub fn flush_all(&self) {
    with_runtime(async {
      let _ = self.store.flush_and_evict_all().await;
    });
  }

  /// 刷盘并封区到 tail：构造「不可变区驻留」读工况（数据驻留内存但已封存，
  /// read_only_address = tail，点查走不可变区纯指针直读路径，对标生产
  /// flush 后只读区稳态；与 flush_and_evict_all 的全驱逐工况互补）
  pub fn seal_immutable(&self) {
    with_runtime(async {
      let _ = self.store.flush_all().await;
    });
    let _ = self.store.hlog.shift_read_only_to_tail();
  }
}

static SILENT_PANIC_HOOK: Once = Once::new();

#[inline]
fn init_silent_panic_hook() {
  SILENT_PANIC_HOOK.call_once(|| {
    set_hook(Box::new(|_| {}));
  });
}

/// wbftree 性能测试夹具
pub struct WbftreeHarness {
  pub tree: Arc<BfTreeService>,
  /// 托管树生命周期的管理器 (Drop 时释放全部在线树，须与 tree 同寿)
  _manager: Arc<RangeIndexManager>,
  _temp_dir: TempDir,
}

impl WbftreeHarness {
  /// 对齐 bench/ 默认标准配置 (128MB 缓存, 4 线程并发)
  pub fn default_bench() -> aok::Result<Self> {
    Self::new_disk(DEFAULT_CACHE_SIZE)
  }

  pub fn new_memory() -> aok::Result<Self> {
    init_silent_panic_hook();
    let temp_dir = TempDir::new()?;
    let manager = Arc::new(RangeIndexManager::new(
      temp_dir.path(),
      temp_dir.path().join("cpr"),
    )?);
    let tree = manager.create_bftree(b"bench", StorageBackendType::Memory, TUNE_BENCH)?;
    Ok(Self {
      tree,
      _manager: manager,
      _temp_dir: temp_dir,
    })
  }

  pub fn new_disk(cache_size: usize) -> aok::Result<Self> {
    init_silent_panic_hook();
    let temp_dir = TempDir::new()?;
    let manager = Arc::new(RangeIndexManager::new(
      temp_dir.path(),
      temp_dir.path().join("cpr"),
    )?);
    let mut tuning = TUNE_BENCH;
    tuning.cache_size = cache_size;
    let tree = manager.create_bftree(b"bench", StorageBackendType::Disk, tuning)?;
    Ok(Self {
      tree,
      _manager: manager,
      _temp_dir: temp_dir,
    })
  }

  #[inline]
  pub fn insert(&self, key: &[u8], val: &[u8]) -> bool {
    self.tree.insert(key, val) == BfTreeInsertResult::Success
  }

  #[inline]
  pub fn read(&self, key: &[u8], out: &mut [u8]) -> Option<usize> {
    let (res, len) = self.tree.read_into(key, out);
    if res == BfTreeReadResult::Found {
      Some(len)
    } else {
      None
    }
  }

  #[inline]
  pub fn scan(&self, start_key: &[u8], count: usize) -> (usize, u64) {
    let mut sum = 0u64;
    let mut scanned = 0usize;
    let _ = self.tree.scan_with_count_callback(
      start_key,
      count,
      ScanReturnField::KeyAndValue,
      |_k, val| {
        sum += val.first().copied().unwrap_or(0) as u64;
        scanned += 1;
        true
      },
    );
    (scanned, sum)
  }

  #[inline]
  pub fn delete(&self, key: &[u8]) -> bool {
    self.tree.delete(key) == BfTreeDeleteResult::Success
  }
}
