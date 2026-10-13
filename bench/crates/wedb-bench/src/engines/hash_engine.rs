//! wkv 混合日志 KV 的 redb 同构适配：把 BenchDatabase trait 家族逐一映射到
//! wkv 的 Raw 物理键面，使 redb 的 18 段 workload 能原样驱动哈希日志引擎。

use std::{
  cell::RefCell,
  future::Future,
  marker::PhantomData,
  ops::Bound,
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use whlog::ScanIterator;
use wkv::{CheckpointType, CompactionType, ScanState, StoreConfig, StoreResult, WedbStore};

use crate::{config::Workload, traits::*};

/// 段大小取 wdev 生产口径 64 MiB（紧缩窗向下取整到段边界，令整段可物理删除）
const SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
/// 扇区大小对齐 wdev 缺省 4096，bench 不直引 wdev 常量避免耦合其内部命名
const SECTOR_SIZE: usize = 4096;
thread_local! {
  /// 每线程各自持有一个 compio Runtime，桥接 wkv 异步接口为同步；
  /// 多线程读段绝不跨线程共享同一 Runtime。
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

/// wkv 混合日志 KV 引擎句柄：持 Arc 存储与同目录内的检查点子目录
pub struct HashEngine {
  store: Arc<WedbStore<SegmentedDevice>>,
  /// 检查点目录落在 path 之内，随 database_size 的目录树如实计入两侧尺寸
  checkpoint_dir: PathBuf,
}

impl HashEngine {
  /// 在 harness 给的目录内部打开引擎：数据段与检查点均落在 path 之下，
  /// 使 uncompacted / compacted 两段尺寸测量真实覆盖 wkv 的全部旁路存储。
  pub fn open(path: &Path, workload: &Workload) -> Result<Self, String> {
    // SegmentedDevice 以 `base_path.<段号>` 命名段文件（基名同级而非目录内），
    // 故基名取 path 的子前缀，令段文件实际落进 path 目录、被 walkdir 捕获。
    let device = Arc::new(
      SegmentedDevice::new(path.join("hlog"), SEGMENT_SIZE, SECTOR_SIZE)
        .map_err(|e| e.to_string())?,
    );
    let config = StoreConfig::from_memory_budget_with_keys(
      workload.cache_size as u64,
      Some(workload.loaded_elements() as u64),
    );
    let store = Arc::new(WedbStore::open(config, device).map_err(|e| e.to_string())?);
    Ok(Self {
      store,
      checkpoint_dir: path.join("cpr"),
    })
  }
}

impl BenchDatabase for HashEngine {
  type C<'db> = HashConnection;

  fn db_type_name() -> &'static str {
    "hash"
  }

  fn connect(&self) -> Self::C<'_> {
    let session = self.store.new_session().expect("wkv new_session 失败");
    // 与 redb 习惯一致默认同步提交；每连接独立持有一个会话（Participant 为 Send）
    HashConnection {
      session,
      sync: AtomicBool::new(true),
    }
  }

  /// 真紧缩链：flush_all 封印紧缩窗 → tail 向下取整段边界在线紧缩 → FoldOver
  /// 检查点抬升删段地板触发段文件物理删除；任一步失败即返回 false（诚实记 N/A）
  fn compact(&mut self) -> bool {
    let store = self.store.clone();
    let dir = self.checkpoint_dir.clone();
    with_runtime(async move {
      if store.flush_all().await.is_err() {
        return false;
      }
      let until = store.tail_address() / SEGMENT_SIZE * SEGMENT_SIZE;
      if store.compact(until, CompactionType::Scan).await.is_err() {
        return false;
      }
      if store
        .create_checkpoint(&dir, CheckpointType::FoldOver)
        .await
        .is_err()
      {
        return false;
      }
      true
    })
  }
}

/// 连接：持有跨线程可转移的会话与持久化档位开关
pub struct HashConnection {
  session: wkv::StoreSession<SegmentedDevice>,
  sync: AtomicBool,
}

impl BenchDatabaseConnection for HashConnection {
  type W<'txn> = HashWriteTxn<'txn>;
  type R<'txn> = HashReadTxn<'txn>;

  /// wkv 支持持久档位切换：true 提交时 flush_all 落盘，false 仅留在内存环
  fn set_sync(&mut self, sync: bool) -> bool {
    self.sync.store(sync, Ordering::Relaxed);
    true
  }

  fn write_transaction(&self) -> Self::W<'_> {
    HashWriteTxn {
      session: &self.session,
      sync: self.sync.load(Ordering::Relaxed),
    }
  }

  fn read_transaction(&self) -> Self::R<'_> {
    HashReadTxn {
      session: &self.session,
    }
  }
}

pub struct HashWriteTxn<'a> {
  session: &'a wkv::StoreSession<SegmentedDevice>,
  sync: bool,
}

impl BenchWriteTransaction for HashWriteTxn<'_> {
  type W<'txn>
    = HashInserter<'txn>
  where
    Self: 'txn;

  fn get_inserter(&mut self) -> Self::W<'_> {
    HashInserter {
      session: self.session,
    }
  }

  fn commit(self) -> Result<(), ()> {
    if self.sync {
      // sync 档：提交即 flush_all 落盘（redb individual writes 段的持久语义）；
      // nosync 档在此直接返回，最弱承诺为「留在内存环、依赖 OS 页缓存/后续 flush」
      let store = self.session.store().clone();
      with_runtime(async move { store.flush_all().await })
        .map(|_| ())
        .map_err(|_| ())
    } else {
      Ok(())
    }
  }
}

pub struct HashInserter<'a> {
  session: &'a wkv::StoreSession<SegmentedDevice>,
}

impl BenchInserter for HashInserter<'_> {
  type Output<'out>
    = &'out [u8]
  where
    Self: 'out;
  /// wkv 无有序条件摘除路径，extract_if 恒返回 Err，此占位类型仅满足 GAT 形状
  type ExtractIfIterator<'out, F>
    = NoRangeIter<F>
  where
    Self: 'out,
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
    // 先走无运行时的同步快路径，页未就绪才降级到 compio 追加重试
    match self.session.try_upsert_raw_sync(key, value) {
      Ok(Ok(_)) => Ok(()),
      Ok(Err(_)) => {
        let session = self.session;
        with_runtime(async move { session.upsert_raw(key, value).await })
          .map(|_| ())
          .map_err(|_| ())
      }
      Err(_) => Err(()),
    }
  }

  fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
    match self.session.try_delete_raw_sync(key) {
      Ok(Ok(_)) => Ok(()),
      Ok(Err(_)) => {
        let session = self.session;
        with_runtime(async move { session.delete_raw(key).await })
          .map(|_| ())
          .map_err(|_| ())
      }
      Err(_) => Err(()),
    }
  }

  /// 哈希日志无键序，两端有序弹出无原生路径：诚实 Err(())，harness 记 N/A
  fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
    Err(())
  }

  fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
    Err(())
  }

  /// 无按键序全表遍历并按谓词原地保留的引擎能力：Err(()) 记 N/A
  fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, _predicate: F) -> Result<u64, ()> {
    Err(())
  }

  /// 无区间条件摘除（extract_if）能力：Err(()) 记 N/A
  fn extract_if<'a, F>(
    &'a mut self,
    _range: (Bound<&[u8]>, Bound<&[u8]>),
    _predicate: F,
  ) -> BenchExtractIfResult<'a, Self, F>
  where
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a,
  {
    Err(())
  }
}

/// extract_if 不支持时的占位迭代器类型：仅承载 GAT 参数形状，永不构造
pub struct NoRangeIter<F>(PhantomData<F>);

impl<F> BenchIterator for NoRangeIter<F> {
  type Output<'out>
    = &'out [u8]
  where
    Self: 'out;

  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    None
  }
}

pub struct HashReadTxn<'a> {
  session: &'a wkv::StoreSession<SegmentedDevice>,
}

impl BenchReadTransaction for HashReadTxn<'_> {
  type T<'txn>
    = HashReader<'txn>
  where
    Self: 'txn;

  fn get_reader(&self) -> Self::T<'_> {
    HashReader {
      session: self.session,
      staging: Vec::new(),
      slots: Vec::new(),
      val_buf: Vec::new(),
    }
  }
}

pub struct HashReader<'a> {
  session: &'a wkv::StoreSession<SegmentedDevice>,
  staging: Vec<u8>,
  slots: Vec<(usize, usize, usize)>,
  val_buf: Vec<u8>,
}

impl BenchReader for HashReader<'_> {
  type Output<'out>
    = &'out [u8]
  where
    Self: 'out;
  type Iterator<'out>
    = HashRangeIter<'out>
  where
    Self: 'out;

  fn get<'b>(&'b mut self, key: &[u8]) -> Option<Self::Output<'b>> {
    self.val_buf.clear();
    let hit = match self.session.try_read_raw_in_memory(key, |val| {
      self.val_buf.extend_from_slice(val);
    }) {
      Ok(StoreResult::Success(())) => true,
      Ok(StoreResult::NotFound) => false,
      _ => {
        let session = self.session;
        let val_buf = &mut self.val_buf;
        with_runtime(async move {
          session
            .read_raw_with(key, |val| {
              val_buf.extend_from_slice(val);
            })
            .await
            .ok()
            .flatten()
        })
        .is_some()
      }
    };
    if hit { Some(&self.val_buf) } else { None }
  }

  /// 直读进调用方缓冲：内存命中零分配（值字节守卫内拷入 out），冷区降级
  /// 异步 read_raw_with 闭包直拷——与 C# 驱动 pinned slots 输出同形态，
  /// 彻底消除每 op 的 Vec 堆分配/释放
  fn get_into(&mut self, key: &[u8], out: &mut [u8]) -> Option<()> {
    match self.session.try_read_raw_in_memory(key, |val| {
      let n = val.len().min(out.len());
      out[..n].copy_from_slice(&val[..n]);
    }) {
      Ok(StoreResult::Success(())) => Some(()),
      Ok(StoreResult::NotFound) => None,
      _ => {
        let session = self.session;
        with_runtime(async move {
          session
            .read_raw_with(key, |val| {
              let n = val.len().min(out.len());
              out[..n].copy_from_slice(&val[..n]);
            })
            .await
            .ok()
            .flatten()
        })
      }
    }
  }

  /// wkv 是哈希日志引擎，无键序区间语义；range_from 退化为「锚定起始键记录的
  /// 逻辑地址、沿 hlog 正向游标逐条前探真实记录」——与旧 wkv 扫描单测同机制，
  /// 产出的是日志序条目而非键序条目，如实反映哈希引擎的扫描能力上限。
  ///
  /// 步进惰性化与零分配复用：借用 reader 内部常驻的 staging 与 slots 缓冲，
  /// 仅在游标推进时按需 refill，消除循环创建迭代器时的反复堆分配。
  fn range_from<'b>(&'b mut self, start: &'b [u8]) -> Self::Iterator<'b> {
    let session = self.session;
    let store = session.store();
    let until = store.tail_address();
    let cursor = match session.find_tag_cooperative(start).ok().flatten() {
      Some(anchor) => store.hlog.scan_iter(anchor, until),
      // 起始键不在表内：零宽区间游标，首次步进即收口
      None => store.hlog.scan_iter(until, until),
    };
    if self.staging.capacity() < RANGE_BATCH_SLOTS * 192 {
      self.staging.reserve(RANGE_BATCH_SLOTS * 192);
    }
    if self.slots.capacity() < RANGE_BATCH_SLOTS {
      self.slots.reserve(RANGE_BATCH_SLOTS);
    }
    self.staging.clear();
    self.slots.clear();
    HashRangeIter {
      cursor,
      done: false,
      staging: &mut self.staging,
      slots: &mut self.slots,
      slot_head: 0,
    }
  }

  fn len(&mut self) -> u64 {
    // 哈希日志无廉价精确计数：entry_count 是索引面计数（非活键口径），无法保证与
    // 写入台账逐条闭合，不能满足 harness 的精确断言。改以全日志存活扫描的 Live 桶
    // 求和为准——取扫描结构的数值读口 `state_count` 直读（dump 文本面只供 INFO 人
    // 读，绝不回解作数值口径），与 redb 的 O(1) 树计数不同，如实反映哈希引擎 len
    // 依赖遍历的事实（扫描失败才退回 entry_count 兜底）。
    let store = self.session.store().clone();
    let metrics = with_runtime(async move { store.hlog_scan_metrics().await.ok() });
    match metrics {
      Some(m) => m.state_count(ScanState::Live).max(0) as u64,
      None => self.session.store().entry_count() as u64,
    }
  }
}

/// range 游标批量驱动槽容量：`next` 槽空时一次 `for_each_ref` 驱动至多
/// 本槽位数进游标内 staging，纪元守卫与 `block_on` 壳按批摊销（对齐 C#
/// TsavoriteLogScanIterator TryBulkConsumeNext 的整段消费形态，
/// TsavoriteLogScanIterator.cs:519-560）。取 10 与对基消费窗（scan_len=10）
/// 同量级：每扫描恰一次 refill 且零超收集零头；槽在游标内就地消费，批界随
/// 消费窗自然对齐，不预取消费量以外的条目（8b「消费多少付多少」纪律维持）。
const RANGE_BATCH_SLOTS: usize = 10;

/// wkv 范围读惰性游标：持有借用自 reader 所属 store 的 whlog 扫描器。
/// 槽空时一次 `for_each_ref` 驱动至多 RANGE_BATCH_SLOTS 条进 staging；
/// `next` 直接从 staging 切片零拷交付（借用 `self`，消费方用完即还）。
pub struct HashRangeIter<'a> {
  cursor: ScanIterator<'a, SegmentedDevice>,
  done: bool,
  /// 批量 staging：记录键值字节连续拼接，跨批复用
  staging: &'a mut Vec<u8>,
  /// 批量槽表：(staging 偏移, 键长, 值长)，消费序与游标序一致
  slots: &'a mut Vec<(usize, usize, usize)>,
  /// 下一待消费槽下标
  slot_head: usize,
}

impl HashRangeIter<'_> {
  /// refill：一次 for_each_ref 驱动至多 RANGE_BATCH_SLOTS 条进 staging。
  /// 墓碑直接越过不占槽；槽满闭包返回 false 叫停（游标已推进越过最后
  /// 一条消费记录，续批从停点起，无跳过无重复）。
  fn refill(&mut self) {
    self.staging.clear();
    self.slots.clear();
    self.slot_head = 0;
    let cursor = &mut self.cursor;
    let staging = &mut *self.staging;
    let slots = &mut *self.slots;
    let batch = RANGE_BATCH_SLOTS;
    let stepped = with_runtime(async move {
      cursor
        .for_each_ref(|item| {
          if item.rec.is_tombstone() {
            return Ok(true);
          }
          let off = staging.len();
          staging.extend_from_slice(item.rec.key);
          staging.extend_from_slice(item.rec.value);
          slots.push((off, item.rec.key.len(), item.rec.value.len()));
          Ok(slots.len() < batch)
        })
        .await
    });
    let filled = self.slots.len();
    match stepped {
      Ok(()) if filled > 0 => {
        if filled < batch {
          self.done = true;
        }
      }
      // 扫描窗耗尽或 IO 错误：按「到此为止」收口
      _ => self.done = true,
    }
  }
}

impl BenchIterator for HashRangeIter<'_> {
  type Output<'out>
    = &'out [u8]
  where
    Self: 'out;

  /// 批量 refill + staging 切片直出：交付即游标内字节零拷。
  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    if self.slot_head >= self.slots.len() {
      if self.done {
        return None;
      }
      self.refill();
      if self.slot_head >= self.slots.len() {
        return None;
      }
    }
    let (off, klen, vlen) = self.slots[self.slot_head];
    self.slot_head += 1;
    let buf = &*self.staging;
    Some((&buf[off..off + klen], &buf[off + klen..off + klen + vlen]))
  }
}
