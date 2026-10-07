//! wbftree（bf-tree 有序 range index）适配：把 redb 同构契约接到引擎的排序游标内核上。
//!
//! 全程只挂一棵树（身份键 [`TREE_ID_KEY`]），18 段共用：wbftree 是有序索引，
//! `range_from` / `removals` / `retain` / `extract_if` / `pop_first` / `pop_last`
//! 一律走引擎原生升序扫描游标（`BfTreeService::scan_with_count_callback`），
//! 不退化成点查拼出来的伪有序遍历；只有引擎确实没有的语义才折成 N/A。

use std::{
  fs::{create_dir_all, remove_file},
  marker::PhantomData,
  ops::Bound,
  path::{Path, PathBuf},
  sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
  },
};

use arc_swap::ArcSwap;
use bf_tree::{BfTree, LeafInsertResult, LeafReadResult, ScanReturnField as BfTreeScanReturnField};
use wbftree::{
  BfTreeService, RangeIndexManager, ScanReturnField, StorageBackendType, TreeSession, TreeTuning,
};

use crate::{
  config::Workload,
  traits::{
    BenchDatabase, BenchDatabaseConnection, BenchExtractIfResult, BenchInserter, BenchIterator,
    BenchPopResult, BenchReadTransaction, BenchReader, BenchWriteTransaction, OwnedOutput,
  },
};

/// 评测树身份键：本适配器一次 `open` 只建一棵树，树身份即基准本身
const TREE_ID_KEY: &[u8] = b"wedb-bench-bftree";
/// 记录长度下限：引擎断言 `cb_min_record_size ≥ 2`，且叶页/min 不得超 2^12
/// (bf-tree 0.5.6 config.rs `validate`)，取 4 与 16KB 叶页恰落在该上界
const TUNE_MIN_RECORD_SIZE: usize = 4;
/// 记录长度上限（引擎收到 `+1`）：24B 键 + 150B 值与 32B 有序段键都远小于此，
/// 留大到 4KB 是为了读/扫描缓冲走 8KB 栈上快路径（见 wbftree ops.rs 选路阈值）
const TUNE_MAX_RECORD_SIZE: usize = 4096;
/// 键长上限（引擎收到 `+1` = 513）：容纳有序段的 32 字节键与扫描续键（键 ++ 0x00）
const TUNE_MAX_KEY_LEN: usize = 512;
/// 引擎允许的键长上限单点：`instantiate_tree` 下发 `cb_max_key_len(max_key_len + 1)`
const ENGINE_MAX_KEY_LEN: usize = TUNE_MAX_KEY_LEN + 1;
/// 叶页大小：4KB 叶页会把 174B 记录挤到每页不足 20 条，16KB 让引擎的
/// 「页填到近满才分裂」真正生效（同 4KB 也满足 `2 * leaf_page_size` 的页环下限）
const TUNE_LEAF_PAGE_SIZE: usize = 16 * 1024;
/// 引擎键序的最小起点：`scan_*` 拒绝空起点键，而任何非空键都 ≥ `[0x00]`
const MIN_START: &[u8] = &[0u8];
/// 压缩重放单片条目数：片内键值落地约 0.7 MB，与表规模无关的常数内存
const COPY_CHUNK: usize = 4096;
/// 压缩重建树的身份键前缀：注册表同名拒建，故每轮压缩换一个序号
const REBUILD_ID_PREFIX: &str = "wedb-bench-bftree-rebuild";

/// 页环容量：引擎要求 `cb_size_byte` 恰为 2 的幂且 ≥ 2 倍叶页
/// (bf-tree 0.5.6 config.rs `validate`)，而 runner 按物理内存收敛出来的预算是任意
/// 字节数，故向下取 2 的幂——向上取会越过收敛目的；`cache_size` 为 0 时取引擎默认
/// 32MiB（本身即 2 的幂），其余档位一律由 workload 决定
fn ring_capacity(cache_size: usize) -> usize {
  const MIN_RING: usize = 2 * TUNE_LEAF_PAGE_SIZE;
  if cache_size <= MIN_RING {
    return MIN_RING;
  }
  if !cache_size.is_multiple_of(2) && cache_size < 1024 * 1024 {
    // 极小且非 2 的幂的预算：交给引擎默认档，避免为凑幂次把页环压到装不下分裂
    return 32 * 1024 * 1024;
  }
  let floor = 1usize << (usize::BITS - 1 - cache_size.leading_zeros());
  floor.max(MIN_RING)
}

/// 将游标严格前推至 `key` 的严格后继键（`key ++ 0x00`）。
///
/// 引擎键序为 `[u8]` 字典序（短前缀小于长前缀，见 bf-tree leaf_node.rs 的 `cmp` 家族），
/// `key ++ [0]` 恰是「大于 key 的最小键」，用它作续扫起点既不重不漏。
/// 引擎字典序下 `key ++ [0] > cursor` 当且仅当 `key >= cursor`。
/// 超出引擎键长上限或无法严格前进时返回 false（区间收口终止）。
/// 复用 `cursor` 已有容量，零堆内存重分配。
fn advance_cursor(cursor: &mut Vec<u8>, key: &[u8]) -> bool {
  if key.len() >= ENGINE_MAX_KEY_LEN || key < cursor.as_slice() {
    return false;
  }
  cursor.clear();
  cursor.extend_from_slice(key);
  cursor.push(0u8);
  true
}

/// 引擎本体与适配层记账：`len()` 口径由适配层维护
struct Shared {
  tree: RwLock<Arc<BfTreeService>>,
  session: ArcSwap<TreeSession>,
  len: AtomicU64,
}

impl Shared {
  #[inline]
  fn session(&self) -> arc_swap::Guard<Arc<TreeSession>> {
    self.session.load()
  }

  #[inline]
  fn update_tree(&self, service: Arc<BfTreeService>) {
    let session = service.session().expect("bftree session active");
    *self
      .tree
      .write()
      .unwrap_or_else(|poisoned| poisoned.into_inner()) = service;
    self.session.store(Arc::new(session));
  }

  fn tree_snapshot(&self) -> Arc<BfTreeService> {
    let guard = self
      .tree
      .read()
      .unwrap_or_else(|poisoned| poisoned.into_inner());
    Arc::clone(&guard)
  }

  #[inline]
  fn apply_delta(&self, delta: i64) {
    if delta != 0 {
      self.len.fetch_add(delta as u64, Ordering::Relaxed);
    }
  }
}

/// 写事务内的适配层状态：条目数增量
struct Staged {
  delta: i64,
}

impl Staged {
  fn new() -> Self {
    Self { delta: 0 }
  }
}

/// wbftree 引擎句柄：持有单树注册表与数据文件路径
pub struct BftreeEngine {
  shared: Shared,
  /// 树生命周期由注册表托管（其 Drop 释放全部在线树），故必须与树同寿；
  /// 压缩重建另需经它摘除旧树条目并删除旧工作文件
  manager: Arc<RangeIndexManager>,
  data_path: PathBuf,
  /// 当前在线树在注册表中的身份键：每次压缩换一棵新树重建，故须可变
  tree_id: Vec<u8>,
  /// 重建轮序号：保证同一运行内各轮压缩的树身份互不相同（注册表同名拒建）
  rebuild_seq: u32,
  /// 建树调参一次定型：压缩重建的新树必须沿用同口径，否则两尺寸不可比
  tuning: TreeTuning,
}

impl BftreeEngine {
  /// 在 `path` 目录内开树：数据落 `{path}/bftree/<键 base32 前缀>.data.bftree`，
  /// 检查点目录 `{path}/cpr` 本基准不触发（只建在线树），故目录尺寸统计
  /// （harness 整目录 walkdir）计到的就是工作文件本身
  pub fn open(path: &Path, workload: &Workload) -> Result<Self, String> {
    create_dir_all(path).map_err(|e| format!("创建评测目录失败: {e}"))?;
    let manager = Arc::new(
      RangeIndexManager::new(path.join("bftree"), path.join("cpr"))
        .map_err(|e| format!("打开 RangeIndex 管理器失败: {e}"))?,
    );
    let tuning = TreeTuning {
      cache_size: ring_capacity(workload.cache_size),
      min_record_size: TUNE_MIN_RECORD_SIZE,
      max_record_size: TUNE_MAX_RECORD_SIZE,
      max_key_len: TUNE_MAX_KEY_LEN,
      leaf_page_size: TUNE_LEAF_PAGE_SIZE,
    };
    let tree = manager
      .create_bftree_with_snapshot_opt(TREE_ID_KEY, StorageBackendType::Disk, tuning, false)
      .map_err(|e| format!("创建 bftree 索引失败: {e}"))?;
    // 条目量级只用于决定页环是否够大（引擎无预分配面），此处按 workload 如实透传
    let _ = workload.loaded_elements();
    let data_path = tree
      .file_path()
      .map(PathBuf::from)
      .ok_or_else(|| "bftree 磁盘后端未回传数据文件路径".to_string())?;
    let session = tree
      .session()
      .ok_or_else(|| "初始化 tree session 失败".to_string())?;
    Ok(Self {
      shared: Shared {
        tree: RwLock::new(tree),
        session: ArcSwap::from_pointee(session),
        len: AtomicU64::new(0),
      },
      manager,
      data_path,
      tree_id: TREE_ID_KEY.to_vec(),
      rebuild_seq: 0,
      tuning,
    })
  }
}

impl BenchDatabase for BftreeEngine {
  type C<'db> = BftreeConnection<'db>;

  fn db_type_name() -> &'static str {
    "wbftree"
  }

  fn connect(&self) -> Self::C<'_> {
    let session = (**self.shared.session()).clone();
    BftreeConnection {
      shared: &self.shared,
      session,
    }
  }

  /// 压缩 = 全表有序重放到全新空树，再摘旧树删旧文件（与 SQLite VACUUM 同形）。
  ///
  /// 为什么不用引擎自带的 `cpr_snapshot` 固化通道：bf-tree 0.5.6 的
  /// `snapshot_page` 对每个页版本都 `alloc_offset` 另写一份，`finalize` 只在
  /// 映射表里去重、字节层面从不回收，且基页与它的 mini 页链全部落盘——实测
  /// 固化件（92.72 MiB）大于原工作文件（74.27 MiB），条目数却一条不少，
  /// 即 CPR 快照是「时间点影像」而非「密实化」，拿它冒充压缩只会虚报尺寸。
  /// 引擎也没有页池排空 / 文件截断 / 碎片整理的公开接口（`RangeIndexManager`
  /// 的 `detach_tree` 走的仍是同一条 cpr_snapshot 通道），故本段唯一诚实的
  /// 收缩手段是重放：旧树有序游标分片读出 → 升序 `bulk_load` 灌进新树
  /// （连续键集中同叶、页填到近满才分裂）→ `delete_index` 摘注册并删旧文件。
  /// 分片流式，落地内存与表规模无关；任一步失败一律回滚新树并返回 false
  /// （harness 据此记 N/A），绝不留下半棵树冒充压缩结果。
  fn compact(&mut self) -> bool {
    let old = self.shared.tree_snapshot();
    // 每轮压缩换一个新身份：注册表对同名树直接拒建
    self.rebuild_seq += 1;
    let rebuild_id = format!("{REBUILD_ID_PREFIX}-{}", self.rebuild_seq).into_bytes();
    let Ok(fresh) = self.manager.create_bftree_with_snapshot_opt(
      &rebuild_id,
      StorageBackendType::Disk,
      self.tuning,
      false,
    ) else {
      println!("wbftree: 压缩重建树创建失败，记 N/A");
      return false;
    };

    let mut cursor = MIN_START.to_vec();
    let (mut scanned, mut loaded) = (0u64, 0u64);
    let mut data_buf: Vec<u8> = Vec::with_capacity(COPY_CHUNK * 180);
    let mut ranges: Vec<(usize, usize, usize)> = Vec::with_capacity(COPY_CHUNK);
    loop {
      data_buf.clear();
      ranges.clear();
      let scan_res = old.scan_with_count_callback(
        &cursor,
        COPY_CHUNK,
        ScanReturnField::KeyAndValue,
        |key, value| {
          let k_off = data_buf.len();
          let k_len = key.len();
          data_buf.extend_from_slice(key);
          let v_len = value.len();
          data_buf.extend_from_slice(value);
          ranges.push((k_off, k_len, v_len));
          true
        },
      );
      if scan_res.is_err() {
        self.discard_tree(&rebuild_id);
        println!("wbftree: 压缩重放扫描失败，记 N/A");
        return false;
      }
      let chunk: Vec<(&[u8], &[u8])> = ranges
        .iter()
        .map(|&(k_off, k_len, v_len)| {
          let v_off = k_off + k_len;
          (&data_buf[k_off..v_off], &data_buf[v_off..v_off + v_len])
        })
        .collect();
      scanned += chunk.len() as u64;
      match fresh.bulk_load(&chunk) {
        Ok(count) => loaded += count,
        Err(_) => {
          self.discard_tree(&rebuild_id);
          println!("wbftree: 压缩重放写入失败，记 N/A");
          return false;
        }
      }
      if ranges.len() < COPY_CHUNK {
        break;
      }
      let Some(&(k_off, k_len, _)) = ranges.last() else {
        break;
      };
      let last_key = &data_buf[k_off..k_off + k_len];
      if !advance_cursor(&mut cursor, last_key) {
        break;
      }
    }

    // 旧树条目键唯一，重放条数必须逐条对上；对不上即引擎面有缺，宁报 N/A
    if loaded != scanned {
      self.discard_tree(&rebuild_id);
      println!("wbftree: 压缩重放条数不符（扫 {scanned} / 落 {loaded}），记 N/A");
      return false;
    }

    // 先释放旧引擎（关文件句柄、归还页环）再由注册表摘条目并删旧工作文件；
    // 本管理器无存储纪元，`release_detached` 走同步清理臂，故返回时旧文件已消失
    old.dispose();
    self.discard_tree(self.tree_id.as_slice());
    if self.data_path.exists() {
      let _ = remove_file(&self.data_path);
    }
    let Some(fresh_path) = fresh.file_path().map(PathBuf::from) else {
      println!("wbftree: 重建树未回传文件路径，记 N/A");
      return false;
    };
    self.shared.update_tree(fresh);
    self.data_path = fresh_path;
    self.tree_id = rebuild_id;
    // 全表重放给出了精确在册数，把适配层记账校准到该事实值（引擎无计数面）
    self.shared.len.store(scanned, Ordering::Relaxed);
    true
  }
}

impl BftreeEngine {
  /// 摘除注册表条目并删除其工作文件：压缩失败回滚与旧树收口共用同一通道
  fn discard_tree(&self, id_key: &[u8]) {
    let _ = self.manager.delete_index(id_key);
  }
}

pub struct BftreeConnection<'a> {
  shared: &'a Shared,
  session: TreeSession,
}

impl BenchDatabaseConnection for BftreeConnection<'_> {
  type W<'txn>
    = BftreeWriteTxn<'txn>
  where
    Self: 'txn;
  type R<'txn>
    = BftreeReadTxn<'txn>
  where
    Self: 'txn;

  /// wbftree 没有「提交」概念，也就没有可切的持久化档位：引擎写路径只做页粒度的
  /// pwrite 回写工作文件（bf-tree 0.5.6 fs/std_vfs.rs `write` → `write_at`），
  /// 全链路无任何 fsync 调用点（`VfsImpl::flush` 在引擎内无公开调用口），
  /// 页环尾部的 mini 页也没有排空接口。既然 sync 与 nosync 两档在引擎侧
  /// 得到逐字节相同的磁盘行为，就不存在可如实上报的档位切换 → 记 N/A。
  fn set_sync(&mut self, _sync: bool) -> bool {
    false
  }

  #[inline]
  fn write_transaction(&self) -> Self::W<'_> {
    BftreeWriteTxn {
      shared: self.shared,
      session: &self.session,
      staged: Staged::new(),
    }
  }

  #[inline]
  fn read_transaction(&self) -> Self::R<'_> {
    BftreeReadTxn {
      shared: self.shared,
      session: &self.session,
    }
  }
}

/// 写事务：wbftree 的写直接进树，本层只攒条目数增量，提交时一次性回写
pub struct BftreeWriteTxn<'a> {
  shared: &'a Shared,
  session: &'a TreeSession,
  staged: Staged,
}

impl BenchWriteTransaction for BftreeWriteTxn<'_> {
  type W<'txn>
    = BftreeInserter<'txn>
  where
    Self: 'txn;

  #[inline(always)]
  fn get_inserter(&mut self) -> Self::W<'_> {
    BftreeInserter {
      tree: self.session.raw_tree(),
      delta: 0,
      staged: &mut self.staged,
    }
  }

  fn commit(self) -> Result<(), ()> {
    self.shared.apply_delta(self.staged.delta);
    Ok(())
  }
}

pub struct BftreeInserter<'a> {
  tree: &'a BfTree,
  delta: i64,
  staged: &'a mut Staged,
}

impl Drop for BftreeInserter<'_> {
  #[inline(always)]
  fn drop(&mut self) {
    self.staged.delta += self.delta;
  }
}

impl BenchInserter for BftreeInserter<'_> {
  type Output<'out>
    = OwnedOutput
  where
    Self: 'out;
  type ExtractIfIterator<'out, F>
    = NoRangeIter<F>
  where
    Self: 'out,
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

  #[inline(always)]
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
    if matches!(self.tree.insert(key, value), LeafInsertResult::Success) {
      self.delta += 1;
      Ok(())
    } else {
      Err(())
    }
  }

  #[inline(always)]
  fn insert_sorted<'i>(
    &mut self,
    pairs: impl Iterator<Item = (&'i [u8], &'i [u8])>,
  ) -> Result<(), ()> {
    for (key, value) in pairs {
      if !matches!(self.tree.insert(key, value), LeafInsertResult::Success) {
        return Err(());
      }
      self.delta += 1;
    }
    Ok(())
  }

  #[inline(always)]
  fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
    self.tree.delete(key);
    self.delta -= 1;
    Ok(())
  }

  fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
    Err(())
  }

  fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
    Err(())
  }

  fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, _predicate: F) -> Result<u64, ()> {
    Err(())
  }

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

/// extract_if 不支持时的占位迭代器类型
pub struct NoRangeIter<F>(PhantomData<F>);

impl<F> BenchIterator for NoRangeIter<F> {
  type Output<'out>
    = OwnedOutput
  where
    Self: 'out;

  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    None
  }
}

/// 读事务：持有底层活跃会话
pub struct BftreeReadTxn<'a> {
  shared: &'a Shared,
  session: &'a TreeSession,
}

impl<'a> BenchReadTransaction for BftreeReadTxn<'a> {
  type T<'txn>
    = BftreeReader<'txn>
  where
    Self: 'txn;

  #[inline(always)]
  fn get_reader(&self) -> Self::T<'_> {
    BftreeReader {
      tree: self.session.raw_tree(),
      scan_buf: Vec::new(),
      len: &self.shared.len,
    }
  }
}

pub struct BftreeReader<'a> {
  tree: &'a BfTree,
  scan_buf: Vec<u8>,
  len: &'a AtomicU64,
}

impl BenchReader for BftreeReader<'_> {
  type Output<'out>
    = OwnedOutput
  where
    Self: 'out;
  type Iterator<'out>
    = BftreeRangeIter<'out>
  where
    Self: 'out;

  #[inline]
  fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>> {
    if self.scan_buf.len() < TUNE_MAX_RECORD_SIZE {
      self.scan_buf.resize(TUNE_MAX_RECORD_SIZE, 0);
    }
    match self.tree.read(key, &mut self.scan_buf) {
      LeafReadResult::Found(len) => Some(OwnedOutput(self.scan_buf[..len as usize].to_vec())),
      _ => None,
    }
  }

  #[inline(always)]
  fn get_into(&mut self, key: &[u8], out: &mut [u8]) -> Option<()> {
    if matches!(self.tree.read(key, out), LeafReadResult::Found(_)) {
      Some(())
    } else {
      None
    }
  }

  #[inline]
  fn range_from<'a>(&'a mut self, start: &'a [u8]) -> Self::Iterator<'a> {
    if self.scan_buf.len() < 8192 {
      self.scan_buf.resize(8192, 0);
    }
    let start_key = if start.is_empty() { MIN_START } else { start };
    let iter = self
      .tree
      .scan_with_count(start_key, usize::MAX, BfTreeScanReturnField::KeyAndValue)
      .ok();
    BftreeRangeIter {
      iter,
      scan_buf: &mut self.scan_buf,
    }
  }

  fn len(&mut self) -> u64 {
    self.len.load(Ordering::Relaxed)
  }
}

/// 升序范围游标：零拷贝直接产出底层 ScanIter 缓冲数据切片
pub struct BftreeRangeIter<'a> {
  iter: Option<bf_tree::ScanIter<'a, 'a>>,
  scan_buf: &'a mut [u8],
}

impl Drop for BftreeRangeIter<'_> {
  #[inline]
  fn drop(&mut self) {
    drop(self.iter.take());
  }
}

impl BenchIterator for BftreeRangeIter<'_> {
  type Output<'out>
    = &'out [u8]
  where
    Self: 'out;

  #[inline]
  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    let (klen, vlen) = match self.iter.as_mut()?.next(self.scan_buf) {
      Some(kv) => kv,
      None => {
        self.iter = None;
        return None;
      }
    };
    Some((&self.scan_buf[..klen], &self.scan_buf[klen..klen + vlen]))
  }
}
