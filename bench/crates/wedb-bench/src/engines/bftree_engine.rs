//! wbftree（bf-tree 有序 range index）适配：把 redb 同构契约接到引擎的排序游标内核上。
//!
//! 全程只挂一棵树（身份键 [`TREE_ID_KEY`]），18 段共用：wbftree 是有序索引，
//! `range_from` / `removals` / `retain` / `extract_if` / `pop_first` / `pop_last`
//! 一律走引擎原生升序扫描游标（`BfTreeService::scan_with_count_callback`），
//! 不退化成点查拼出来的伪有序遍历；只有引擎确实没有的语义才折成 N/A。

use std::{
  collections::VecDeque,
  ops::Bound,
  path::{Path, PathBuf},
  sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
  },
};

use wbftree::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService, RangeIndexManager,
  ScanReturnField, StorageBackendType, TreeTuning,
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
/// 降序弹出的起始首字节带（见 [`BftreeInserter::pop_last`]）
const TOP_BAND: i16 = 0xFF;
/// 有序装载批次：一次 `bulk_load` 的条目数，兼顾借用 amortize 与批次内存
const BULK_CHUNK: usize = 4096;
/// 删除类段落（`retain`）单片扫描条目数：片内只攒被弃键，片后集中删
const SCAN_CHUNK: usize = 4096;
/// `extract_if` 单片条目数：被摘条目要连值一起交还迭代器，故小于纯键扫描片
const EXTRACT_CHUNK: usize = 1024;
/// `range_from` 游标一次前探条目数：workload 的 `scan_len` 常态 10，16 条一整片覆盖
const RANGE_CHUNK: usize = 16;
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
  if cache_size % 2 != 0 && cache_size < 1024 * 1024 {
    // 极小且非 2 的幂的预算：交给引擎默认档，避免为凑幂次把页环压到装不下分裂
    return 32 * 1024 * 1024;
  }
  let floor = 1usize << (usize::BITS - 1 - cache_size.leading_zeros());
  floor.max(MIN_RING)
}

/// 严格后继键：`键 ++ 0x00`。引擎键序就是 `[u8]` 字典序（短前缀小于长前缀，
/// 见 bf-tree leaf_node.rs 的 `cmp` 家族），故 `k ++ [0]` 恰是「大于 k 的最小键」，
/// 用它作续扫起点既不重不漏。超出引擎键长上限即无后继（区间到此为止）
fn successor(key: &[u8]) -> Option<Vec<u8>> {
  if key.len() >= ENGINE_MAX_KEY_LEN {
    return None;
  }
  let mut next = Vec::with_capacity(key.len() + 1);
  next.extend_from_slice(key);
  next.push(0u8);
  Some(next)
}

/// 分片升序扫描的收片事实：访问条数、末条键、是否被回调提前收片
struct SliceScan {
  visited: usize,
  last: Option<Vec<u8>>,
  stopped: bool,
}

/// 一次分片扫描：引擎只给「回调排空」形态的扫描口，且扫描期间持叶页共享闩锁，
/// 同线程重入写同叶会在闩锁层自死锁（wbftree/src/service/ops.rs:262 的调用方契约），
/// 所以「边扫边删」一律拆成「片内攒键 → 收片后集中删 → 以严格后继键续扫」；
/// 分片同时把攒下的键压到常数级，避免整表键集落地
fn scan_slice(
  tree: &BfTreeService,
  cursor: &[u8],
  limit: usize,
  fields: ScanReturnField,
  on_record: &mut dyn FnMut(&[u8], &[u8]) -> bool,
) -> Result<SliceScan, ()> {
  let mut visited = 0usize;
  let mut last: Option<Vec<u8>> = None;
  let mut stopped = false;
  let result = tree.scan_with_count_callback(cursor, limit, fields, |key, value| {
    visited += 1;
    last = Some(key.to_vec());
    let keep = on_record(key, value);
    stopped |= !keep;
    keep
  });
  if result.is_err() {
    return Err(());
  }
  Ok(SliceScan {
    visited,
    last,
    stopped,
  })
}

/// 集中删除一片键并回写条目数记账；返回假值 = 引擎实例已释放（评测期不该出现，
/// 出现即适配器与引擎生命周期脱钩，调用方据此终止本段而非假报删除条数）
fn delete_keys(tree: &BfTreeService, staged: &mut Staged, keys: &[Vec<u8>]) -> bool {
  let mut deleted = 0usize;
  for key in keys {
    if tree.delete(key) != BfTreeDeleteResult::Success {
      break;
    }
    deleted += 1;
  }
  staged.delta -= deleted as i64;
  deleted == keys.len()
}

/// 引擎本体与适配层记账：`len()` 口径只能由适配层维护——bf-tree 0.5.6 无计数接口，
/// 只有 `bulk_load` 回报落刷条数。随机 24 字节键在 2^192 空间内不碰撞，故
/// 「一次成功 insert 记 1、一次 delete 记 -1」与表内实数逐条相等
struct Shared {
  /// 当前在线树实例：`compact` 会用固化快照换入新实例，故须内嵌可变
  tree: RwLock<Arc<BfTreeService>>,
  len: AtomicU64,
}

impl Shared {
  /// 借用当前树：每次开事务克隆一次 Arc（点读写面本就跨线程共享同一实例）
  fn tree_snapshot(&self) -> Arc<BfTreeService> {
    let guard = self
      .tree
      .read()
      .unwrap_or_else(|poisoned| poisoned.into_inner());
    Arc::clone(&guard)
  }

  fn apply_delta(&self, delta: i64) {
    if delta > 0 {
      self.len.fetch_add(delta as u64, Ordering::Relaxed);
    } else if delta < 0 {
      self.len.fetch_sub((-delta) as u64, Ordering::Relaxed);
    }
  }
}

/// 写事务内的适配层状态：条目数增量 + 两端弹出游标
struct Staged {
  delta: i64,
  /// 下一个待扫描的首字节带；负值 = 全部带已扫尽（表空）
  back_band: i16,
  /// 当前带内按升序攒下的键，`pop()` 自栈顶取即全表最大
  back_keys: Vec<Vec<u8>>,
  /// 升序弹出游标的续扫起点（严格后继键），与 `front_keys` 同属一片前探窗口
  front_cursor: Vec<u8>,
  /// 已攒下的全表最小键队列，自队首取即升序弹出
  front_keys: VecDeque<Vec<u8>>,
}

impl Staged {
  fn new() -> Self {
    Self {
      delta: 0,
      back_band: TOP_BAND,
      back_keys: Vec::new(),
      front_cursor: MIN_START.to_vec(),
      front_keys: VecDeque::new(),
    }
  }

  /// 新键既可能成为全表最小也可能成为全表最大，两端游标一律作废重建；
  /// 删除类操作不必——删除只让键集变小，游标里的陈旧键由弹出前的存在性点读兜住
  fn invalidate_edge_walks(&mut self) {
    self.back_band = TOP_BAND;
    self.back_keys.clear();
    self.front_cursor = MIN_START.to_vec();
    self.front_keys.clear();
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
    std::fs::create_dir_all(path).map_err(|e| format!("创建评测目录失败: {e}"))?;
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
      .create_bftree(TREE_ID_KEY, StorageBackendType::Disk, tuning)
      .map_err(|e| format!("创建 bftree 索引失败: {e}"))?;
    // 条目量级只用于决定页环是否够大（引擎无预分配面），此处按 workload 如实透传
    let _ = workload.loaded_elements();
    let data_path = tree
      .file_path()
      .map(PathBuf::from)
      .ok_or_else(|| "bftree 磁盘后端未回传数据文件路径".to_string())?;
    Ok(Self {
      shared: Shared {
        tree: RwLock::new(tree),
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
    BftreeConnection {
      shared: &self.shared,
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
    let Ok(fresh) = self
      .manager
      .create_bftree(&rebuild_id, StorageBackendType::Disk, self.tuning)
    else {
      println!("wbftree: 压缩重建树创建失败，记 N/A");
      return false;
    };

    let mut cursor = MIN_START.to_vec();
    let (mut scanned, mut loaded) = (0u64, 0u64);
    loop {
      let mut chunk: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(COPY_CHUNK);
      let slice = scan_slice(
        &old,
        &cursor,
        COPY_CHUNK,
        ScanReturnField::KeyAndValue,
        &mut |key, value| {
          chunk.push((key.to_vec(), value.to_vec()));
          true
        },
      );
      let Ok(slice) = slice else {
        self.discard_tree(&rebuild_id);
        println!("wbftree: 压缩重放扫描失败，记 N/A");
        return false;
      };
      scanned += chunk.len() as u64;
      match fresh.bulk_load(&chunk) {
        Ok(count) => loaded += count,
        Err(_) => {
          self.discard_tree(&rebuild_id);
          println!("wbftree: 压缩重放写入失败，记 N/A");
          return false;
        }
      }
      if slice.visited < COPY_CHUNK {
        break;
      }
      let Some(next) = slice
        .last
        .as_deref()
        .and_then(successor)
        .filter(|next| next.as_slice() > cursor.as_slice())
      else {
        break;
      };
      cursor = next;
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
      let _ = std::fs::remove_file(&self.data_path);
    }
    let Some(fresh_path) = fresh.file_path().map(PathBuf::from) else {
      println!("wbftree: 重建树未回传文件路径，记 N/A");
      return false;
    };
    *self
      .shared
      .tree
      .write()
      .unwrap_or_else(|poisoned| poisoned.into_inner()) = fresh;
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

  fn write_transaction(&self) -> Self::W<'_> {
    BftreeWriteTxn {
      shared: self.shared,
      tree: self.shared.tree_snapshot(),
      staged: Staged::new(),
    }
  }

  fn read_transaction(&self) -> Self::R<'_> {
    BftreeReadTxn {
      shared: self.shared,
      tree: self.shared.tree_snapshot(),
    }
  }
}

/// 写事务：wbftree 的写直接进树，本层只攒条目数增量，提交时一次性回写
pub struct BftreeWriteTxn<'a> {
  shared: &'a Shared,
  tree: Arc<BfTreeService>,
  staged: Staged,
}

impl BenchWriteTransaction for BftreeWriteTxn<'_> {
  type W<'txn>
    = BftreeInserter<'txn>
  where
    Self: 'txn;

  fn get_inserter(&mut self) -> Self::W<'_> {
    BftreeInserter {
      tree: &self.tree,
      staged: &mut self.staged,
    }
  }

  fn commit(self) -> Result<(), ()> {
    self.shared.apply_delta(self.staged.delta);
    Ok(())
  }
}

pub struct BftreeInserter<'a> {
  tree: &'a BfTreeService,
  staged: &'a mut Staged,
}

impl BenchInserter for BftreeInserter<'_> {
  type Output<'out>
    = OwnedOutput
  where
    Self: 'out;
  type ExtractIfIterator<'out, F>
    = BftreeExtractIf<'out, F>
  where
    Self: 'out,
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

  /// 单条写：引擎的 `insert` 就是批量内核的单元素特例（service/ops.rs:88），
  /// 全仓不存在第二条树内写路径
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
    match self.tree.insert(key, value) {
      BfTreeInsertResult::Success => {
        self.staged.delta += 1;
        self.staged.invalidate_edge_walks();
        Ok(())
      }
      _ => Err(()),
    }
  }

  /// 有序装载走引擎唯一的排序批量内核：整批一次引擎借用、键升序下刷，
  /// 连续键集中命中同一叶页，压掉逐条写的借用开销与页分裂抖动（service/bulk.rs）
  fn insert_sorted<'i>(
    &mut self,
    pairs: impl Iterator<Item = (&'i [u8], &'i [u8])>,
  ) -> Result<(), ()> {
    let mut batch: Vec<(&[u8], &[u8])> = Vec::with_capacity(BULK_CHUNK);
    for pair in pairs {
      batch.push(pair);
      if batch.len() >= BULK_CHUNK {
        self.flush_batch(&mut batch)?;
      }
    }
    if !batch.is_empty() {
      self.flush_batch(&mut batch)?;
    }
    Ok(())
  }

  /// 删除：引擎回报的只有「参数非法」（键长为 0 / 超上限 / 实例已释放），
  /// 不回报键是否存在（service/ops.rs:203），故增量按调用方语义如实记 -1——
  /// 本基准的删除序列就是装载序列的重放，命中的必是在册键
  fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
    if self.tree.delete(key) == BfTreeDeleteResult::Success {
      self.staged.delta -= 1;
      Ok(())
    } else {
      Err(())
    }
  }

  /// 弹最小键：引擎只有「从起点升序前探」的游标面，逐条弹出就得逐条从根下降
  /// 并各自付一次扫描构造（实测 86µs/次，比随机删慢两个量级）。故按片前探：
  /// 一片 `SCAN_CHUNK` 条只降一次树，键升序入队后自队首弹出，陈旧键（被对端
  /// 摘掉）由弹出前的存在性点读跳过——值必须现读，不能回吐扫描期的快照值
  fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
    loop {
      if let Some(key) = self.staged.front_keys.pop_front() {
        let (res, value) = self.tree.read(&key);
        match res {
          BfTreeReadResult::Found => {
            let Some(value) = value else {
              continue;
            };
            if self.tree.delete(&key) != BfTreeDeleteResult::Success {
              return Err(());
            }
            self.staged.delta -= 1;
            return Ok(Some((OwnedOutput(key), OwnedOutput(value))));
          }
          BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => continue,
          _ => return Err(()),
        }
      }
      let cursor = self.staged.front_cursor.clone();
      let mut keys: Vec<Vec<u8>> = Vec::with_capacity(SCAN_CHUNK);
      let slice = scan_slice(
        self.tree,
        &cursor,
        SCAN_CHUNK,
        ScanReturnField::Key,
        &mut |key, _value| {
          keys.push(key.to_vec());
          true
        },
      )?;
      if keys.is_empty() {
        // 游标已推到表尾：全表弹空（此后若有新键写入，游标随 insert 复位）
        return Ok(None);
      }
      self.staged.front_keys = keys.into();
      self.staged.front_cursor = slice
        .last
        .as_deref()
        .and_then(successor)
        .filter(|next| next.as_slice() > cursor.as_slice())
        // 无严格后继可续（已到键空间上界）：本队列排空后即结束
        .unwrap_or(cursor);
    }
  }

  /// 弹最大键：引擎只有升序游标、无逆序扫描与取末键接口（range_scan.rs 的
  /// `ScanIter` 仅向右兄弟前进），故按首字节把键空间切成 256 个带，从 0xFF 带起
  /// 逐带做一次**带内**升序扫描（带界取下一带首键，末带直到表尾），带内键升序
  /// 压栈后自栈顶弹出。总扫描量 = 表内键数 + 至多 256 次下降，即摊还 O(1)
  /// 每次弹出，而不是每次 pop 重扫全表；跨带陈旧键由弹出前的存在性点读跳过
  fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
    loop {
      if let Some(key) = self.staged.back_keys.pop() {
        let (res, value) = self.tree.read(&key);
        match res {
          BfTreeReadResult::Found => {
            let Some(value) = value else {
              continue;
            };
            if self.tree.delete(&key) != BfTreeDeleteResult::Success {
              return Err(());
            }
            self.staged.delta -= 1;
            return Ok(Some((OwnedOutput(key), OwnedOutput(value))));
          }
          BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => continue,
          _ => return Err(()),
        }
      }
      if self.staged.back_band < 0 {
        return Ok(None);
      }
      let band = self.staged.back_band as u8;
      self.staged.back_band -= 1;
      // 带界 = 下一带首键；末带（0xFF）无上界，扫到表尾自然收片
      let limit: Option<Vec<u8>> = if band == TOP_BAND as u8 {
        None
      } else {
        Some(vec![band + 1])
      };
      let mut keys: Vec<Vec<u8>> = Vec::new();
      let scanned = self.tree.scan_with_count_callback(
        &[band],
        usize::MAX,
        ScanReturnField::Key,
        |key, _value| match &limit {
          Some(edge) if key >= edge.as_slice() => false,
          _ => {
            keys.push(key.to_vec());
            true
          }
        },
      );
      if scanned.is_err() {
        return Err(());
      }
      self.staged.back_keys = keys;
    }
  }

  /// 全表按序保留：分片升序扫描攒下 predicate 判假的键，片后集中删；
  /// predicate 逐条按升序恰好命中一次，与 `BTreeMap::retain` 同形
  fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, predicate: F) -> Result<u64, ()> {
    let mut visitor = predicate;
    let mut removed = 0u64;
    let mut cursor = MIN_START.to_vec();
    loop {
      let mut doomed: Vec<Vec<u8>> = Vec::new();
      let slice = scan_slice(
        self.tree,
        &cursor,
        SCAN_CHUNK,
        ScanReturnField::KeyAndValue,
        &mut |key, value| {
          if !visitor(key, value) {
            doomed.push(key.to_vec());
          }
          true
        },
      )?;
      if !delete_keys(self.tree, self.staged, &doomed) {
        return Err(());
      }
      removed += doomed.len() as u64;
      if slice.stopped || slice.visited < SCAN_CHUNK {
        break;
      }
      let Some(next) = slice.last.as_deref().and_then(successor) else {
        break;
      };
      if next.as_slice() <= cursor.as_slice() {
        break;
      }
      cursor = next;
    }
    Ok(removed)
  }

  /// 区间摘除：同 `retain` 的分片「扫—删」通道，但把 predicate 判真的键值
  /// 就地交还迭代器（懒推进，一片用完再扫下一片），语义对齐
  /// `std::collections::BTreeMap::extract_if`
  fn extract_if<'a, F>(
    &'a mut self,
    range: (Bound<&[u8]>, Bound<&[u8]>),
    predicate: F,
  ) -> BenchExtractIfResult<'a, Self, F>
  where
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a,
  {
    let (cursor, upper) = range_bounds(range.0, range.1);
    Ok(BftreeExtractIf {
      tree: self.tree,
      staged: self.staged,
      predicate,
      cursor,
      upper,
      pending: VecDeque::new(),
    })
  }
}

impl BftreeInserter<'_> {
  /// 批次下刷：`bulk_load` 回报去重后的落刷条数，本基准的批内键互不相同，
  /// 该值即真实新增键数
  fn flush_batch(&mut self, batch: &mut Vec<(&[u8], &[u8])>) -> Result<(), ()> {
    let result = self.tree.bulk_load(batch.as_slice());
    batch.clear();
    match result {
      Ok(count) => {
        self.staged.delta += count as i64;
        self.staged.invalidate_edge_walks();
        Ok(())
      }
      Err(_) => Err(()),
    }
  }
}

/// 把 `Bound` 折成引擎能表达的「升序扫描起点 + 开区间上界」：引擎只提供含起点的
/// 升序游标与闭区间上界，故 Excluded/Included 端点都转成严格后继；
/// 起点无后继即区间为空（用 `cursor = None` 表达）
fn range_bounds(lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
  let cursor = match lower {
    Bound::Unbounded => Some(MIN_START.to_vec()),
    Bound::Included(start) if start.is_empty() => Some(MIN_START.to_vec()),
    Bound::Included(start) => Some(start.to_vec()),
    Bound::Excluded(start) => successor(start),
  };
  let exclusive_upper = match upper {
    Bound::Unbounded => None,
    Bound::Excluded(end) => Some(end.to_vec()),
    Bound::Included(end) => successor(end),
  };
  (cursor, exclusive_upper)
}

/// `extract_if` 的懒迭代器：借用写事务的树实例与记账槽位，
/// 每次交出本片已摘条目，空了再向前推一片
pub struct BftreeExtractIf<'a, F> {
  tree: &'a BfTreeService,
  staged: &'a mut Staged,
  predicate: F,
  cursor: Option<Vec<u8>>,
  upper: Option<Vec<u8>>,
  pending: VecDeque<(OwnedOutput, OwnedOutput)>,
}

impl<F> BftreeExtractIf<'_, F>
where
  F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool,
{
  /// 前推一片：片内先攒「该摘」的键与条目，收片后再集中删（扫描期不可重入写同叶），
  /// 删完把条目压进待发队列；区间到界、扫到表尾或游标无法推进即置空起点
  fn fill(&mut self)
  where
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool,
  {
    let Some(cursor) = self.cursor.clone() else {
      return;
    };
    let upper = self.upper.clone();
    let mut doomed: Vec<Vec<u8>> = Vec::new();
    let mut extracted: Vec<(OwnedOutput, OwnedOutput)> = Vec::new();
    let slice = {
      let predicate = &mut self.predicate;
      scan_slice(
        self.tree,
        &cursor,
        EXTRACT_CHUNK,
        ScanReturnField::KeyAndValue,
        &mut |key, value| {
          if let Some(limit) = &upper
            && key >= limit.as_slice()
          {
            return false;
          }
          if predicate(key, value) {
            doomed.push(key.to_vec());
            extracted.push((OwnedOutput(key.to_vec()), OwnedOutput(value.to_vec())));
          }
          true
        },
      )
    };
    let Ok(slice) = slice else {
      self.cursor = None;
      return;
    };
    delete_keys(self.tree, self.staged, &doomed);
    self.pending.extend(extracted);
    if slice.stopped || slice.visited < EXTRACT_CHUNK {
      self.cursor = None;
      return;
    }
    self.cursor = slice
      .last
      .as_deref()
      .and_then(successor)
      .filter(|next| next.as_slice() > cursor.as_slice());
  }
}

impl<F> BenchIterator for BftreeExtractIf<'_, F>
where
  F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool,
{
  type Output<'out>
    = OwnedOutput
  where
    Self: 'out;

  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    loop {
      if let Some(entry) = self.pending.pop_front() {
        return Some(entry);
      }
      if self.cursor.is_none() {
        return None;
      }
      self.fill();
    }
  }
}

/// 读事务：与写事务借用同一个树实例，条目数走适配层记账口径
pub struct BftreeReadTxn<'a> {
  shared: &'a Shared,
  tree: Arc<BfTreeService>,
}

impl BenchReadTransaction for BftreeReadTxn<'_> {
  type T<'txn>
    = BftreeReader<'txn>
  where
    Self: 'txn;

  fn get_reader(&self) -> Self::T<'_> {
    BftreeReader {
      tree: &self.tree,
      len: &self.shared.len,
    }
  }
}

pub struct BftreeReader<'a> {
  tree: &'a BfTreeService,
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

  /// 点读：引擎把值拷进线程本地/栈上缓冲后回传，适配层再原样交出（值可能含
  /// 0 字节，全程按切片处理，绝不按 C 字符串截断）
  fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>> {
    let (res, value) = self.tree.read(key);
    match res {
      BfTreeReadResult::Found => value.map(OwnedOutput),
      _ => None,
    }
  }

  fn range_from<'a>(&'a mut self, start: &'a [u8]) -> Self::Iterator<'a> {
    BftreeRangeIter::new(self.tree, start)
  }

  fn len(&mut self) -> u64 {
    self.len.load(Ordering::Relaxed)
  }
}

/// 升序范围游标：引擎的 `ScanIter` 不随 `BfTreeService` 公开（包装层只给回调排空
/// 形态），故按片前探——一片 `RANGE_CHUNK` 条覆盖住调用方的整段步进，
/// 片间用严格后继键续扫，游标本身仍是引擎原生有序遍历
pub struct BftreeRangeIter<'a> {
  tree: &'a BfTreeService,
  cursor: Vec<u8>,
  pending: VecDeque<(OwnedOutput, OwnedOutput)>,
  done: bool,
}

impl<'a> BftreeRangeIter<'a> {
  fn new(tree: &'a BfTreeService, start: &[u8]) -> Self {
    let cursor = if start.is_empty() {
      MIN_START.to_vec()
    } else {
      start.to_vec()
    };
    Self {
      tree,
      cursor,
      pending: VecDeque::new(),
      done: false,
    }
  }

  fn fill(&mut self) {
    let slice = scan_slice(
      self.tree,
      &self.cursor,
      RANGE_CHUNK,
      ScanReturnField::KeyAndValue,
      &mut |key, value| {
        self
          .pending
          .push_back((OwnedOutput(key.to_vec()), OwnedOutput(value.to_vec())));
        true
      },
    );
    // 扫描失败按「到此为止」收口：宁可少给条目，也不把错误折成虚假的成功计数
    let Ok(slice) = slice else {
      self.done = true;
      return;
    };
    let advance = slice
      .last
      .as_deref()
      .and_then(successor)
      .filter(|next| next.as_slice() > self.cursor.as_slice());
    match advance {
      Some(next) => self.cursor = next,
      None => self.done = true,
    }
  }
}

impl BenchIterator for BftreeRangeIter<'_> {
  type Output<'out>
    = OwnedOutput
  where
    Self: 'out;

  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    if self.pending.is_empty() && !self.done {
      self.fill();
    }
    self.pending.pop_front()
  }
}
