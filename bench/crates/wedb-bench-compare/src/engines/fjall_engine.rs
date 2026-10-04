//! fjall 对照列：把 redb 同构契约接到 fjall 的单写事务 + keyspace API 上。
//!
//! 语义取自 redb 上游 `redb-bench-compare` 的 fjall 适配器（common/mod.rs:684-946），
//! 再按 fjall 3.1 的实际能力重挂到本套 trait 形状上：持久化档位改走事务自己的
//! durability 决策（而不是提交后再 persist），有序装载改走 fjall 原生 ingestion 通道。

use std::{
  fs,
  ops::Bound,
  path::{Path, PathBuf},
  thread::sleep,
  time::{Duration, Instant},
};

use fjall::{
  Iter, Keyspace, KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase,
  SingleWriterTxKeyspace, SingleWriterWriteTx, Slice,
};
use wedb_bench::{
  config::Workload,
  traits::{
    BenchDatabase, BenchDatabaseConnection, BenchExtractIfResult, BenchInserter, BenchIterator,
    BenchPopResult, BenchReadTransaction, BenchReader, BenchWriteTransaction, VecBenchIterator,
  },
};

/// 契约是单表形状，键空间名固定
const KEYSPACE_NAME: &str = "bench";

/// fjall 的写事务把待写条目全留在内存 memtable（tx/write_tx.rs 的 insert 只插
/// memtable，落盘发生在 commit），redb 形状的一次 5M 条装载会把它打爆；
/// 攒到该条数就先提交再续一个事务，中间提交的成本留在本段耗时里。
const PENDING_LIMIT: usize = 100_000;

/// 压缩后等旧文件收敛的轮询间隔与上限：上限到了就如实报当前尺寸，
/// 不为凑数字一直等下去
const RECLAIM_POLL_INTERVAL: Duration = Duration::from_millis(50);
const RECLAIM_WAIT_LIMIT: Duration = Duration::from_secs(15);

/// 压缩收尾逼轮转用的墓碑键（见 `force_keyspace_rotation`）。长度 14 字节：随机键是
/// 24 字节、有序装载键是 32 字节，任何档位都不可能撞上真实数据；墓碑形态下
/// `get`/`len` 都看不见它，键集里不会多出这个键
const RECLAIM_MARKER: &[u8] = b"fjall:reclaim!";

/// `set_sync` 的落点：fjall 的提交决策是事务上的 `Option<PersistMode>`。
/// sync 档给 `SyncAll`（journal `fsync` 数据 + 元数据）；nosync 档给 `None`，
/// 即提交时完全不做 persist —— fjall 事务默认是 `Some(Buffer)`（只 flush 到内核，
/// 见 tx/single_writer/mod.rs:64），只有 `None` 才与 redb 的 `Durability::None` 同档。
fn persist_mode(sync: bool) -> Option<PersistMode> {
  sync.then_some(PersistMode::SyncAll)
}

/// trait 契约的失败通道只有 `()`，把 fjall 的 IO 错误打到 stderr 留给列日志
fn fail(op: &str, err: fjall::Error) {
  eprintln!("fjall: {op} 失败 — {err}");
}

/// 目录内文件逻辑字节总和（口径同 harness 的 walkdir）。只在压缩后判断
/// 「旧文件是否真的删干净」，不当作报告尺寸，报错的文件跳过而不是崩在这里
fn directory_bytes(path: &Path) -> u64 {
  let Ok(entries) = fs::read_dir(path) else {
    return 0;
  };
  let mut total = 0u64;
  for entry in entries.flatten() {
    let Ok(metadata) = entry.metadata() else {
      continue;
    };
    if metadata.is_dir() {
      total += directory_bytes(&entry.path());
    } else {
      total += metadata.len();
    }
  }
  total
}

/// fjall 的回收动作全挂在「非空内存表轮转」后面：`Tree::rotate_memtable()` 见到空
/// active memtable 就返回 `None`，`inner_rotate_memtable` 随即 `Ok(false)` 早退，
/// 快照 GC、版本历史 GC、journal 清理一个都不跑（keyspace/mod.rs:734-793）。
/// major compaction 只是「先写新表、再解引用旧表」，旧表还被上一个版本 pin 着，
/// 所以这里塞一条墓碑逼出一次真正的轮转，让上一步的输入文件被删掉。
fn force_keyspace_rotation(keyspace: &Keyspace) -> Result<(), fjall::Error> {
  keyspace
    .remove(RECLAIM_MARKER)
    .and_then(|()| keyspace.rotate_memtable_and_wait())
}

pub struct FjallEngine {
  db: SingleWriterTxDatabase,
  part: SingleWriterTxKeyspace,
  /// harness 给的数据目录，压缩后要在这里等旧表文件被物理删除
  path: PathBuf,
}

impl FjallEngine {
  pub fn open(path: &Path, workload: &Workload) -> Result<Self, String> {
    let cache_size =
      u64::try_from(workload.cache_size).map_err(|_| "fjall: 缓存预算超出 u64".to_string())?;
    // 库目录就是 runner 给的 path，fjall 在其内铺 keyspaces/ 与 journal，
    // 因此 harness 的整目录尺寸统计天然覆盖段文件与 WAL
    let db = SingleWriterTxDatabase::builder(path)
      .cache_size(cache_size)
      .open()
      .map_err(|e| format!("fjall 打开 {} 失败 — {e}", path.display()))?;
    // 树配置全部留在 fjall 默认（Leveled compaction、64 MiB memtable、深层 LZ4），
    // 对照列要的是「开箱 fjall」，不为某一列单独调参
    let part = db
      .keyspace(KEYSPACE_NAME, KeyspaceCreateOptions::default)
      .map_err(|e| format!("fjall 创建 keyspace 失败 — {e}"))?;
    Ok(Self {
      db,
      part,
      path: path.to_path_buf(),
    })
  }

  /// 真实回收四步：fsync journal；把内存表落成表文件（`rotate_memtable` 顺带做快照 GC、
  /// 版本历史 GC 与 journal 清理，见 keyspace/mod.rs:734-793）；一次跨层 major
  /// compaction 清掉墓碑与被覆盖版本；再逼一次非空轮转，让这次 compaction 的输入
  /// 文件真正解引用并被删除。任一步失败就返回 false，让「compacted size」记 N/A 而不是假报。
  fn compact_keyspace(&self) -> Result<(), fjall::Error> {
    let keyspace = self.part.as_ref();
    self
      .db
      .persist(PersistMode::SyncAll)
      .and_then(|()| keyspace.rotate_memtable_and_wait())
      .and_then(|()| keyspace.major_compact())
      .and_then(|()| force_keyspace_rotation(keyspace))
  }

  /// LSM 的重写是「先写新文件、再解引用旧文件」：`major_compact` 返回时旧表文件只是
  /// 被判为垃圾，物理删除还要等版本历史与后台 flush 收尾。不等收敛就交还给 harness，
  /// 量到的是新旧两份并存的临时态（实测 compacted 反而比 uncompacted 大 30%）。
  fn wait_for_files_reclaimed(&self) {
    let deadline = Instant::now() + RECLAIM_WAIT_LIMIT;
    let mut previous: Option<u64> = None;
    loop {
      let idle =
        self.db.inner().outstanding_flushes() == 0 && self.db.inner().active_compactions() == 0;
      let current = directory_bytes(&self.path);
      if idle && previous == Some(current) {
        return;
      }
      previous = Some(current);
      if Instant::now() >= deadline {
        return;
      }
      sleep(RECLAIM_POLL_INTERVAL);
    }
  }
}

impl BenchDatabase for FjallEngine {
  type C<'db>
    = FjallConnection<'db>
  where
    Self: 'db;

  fn db_type_name() -> &'static str {
    "fjall"
  }

  fn connect(&self) -> Self::C<'_> {
    FjallConnection {
      db: &self.db,
      part: self.part.clone(),
      sync: true,
    }
  }

  fn compact(&mut self) -> bool {
    match self.compact_keyspace() {
      Ok(()) => {}
      Err(e) => {
        fail("compaction", e);
        return false;
      }
    }
    self.wait_for_files_reclaimed();
    true
  }
}

pub struct FjallConnection<'a> {
  db: &'a SingleWriterTxDatabase,
  part: SingleWriterTxKeyspace,
  sync: bool,
}

impl BenchDatabaseConnection for FjallConnection<'_> {
  type W<'db>
    = FjallWriteTxn<'db>
  where
    Self: 'db;
  type R<'db>
    = FjallReadTxn
  where
    Self: 'db;

  fn set_sync(&mut self, sync: bool) -> bool {
    self.sync = sync;
    true
  }

  fn write_transaction(&self) -> Self::W<'_> {
    let txn = self.db.write_tx().durability(persist_mode(self.sync));
    FjallWriteTxn {
      db: self.db,
      part: &self.part,
      txn: Some(txn),
      pending: 0,
      sync: self.sync,
      cursor: None,
    }
  }

  fn read_transaction(&self) -> Self::R<'_> {
    FjallReadTxn {
      part: self.part.clone(),
      txn: self.db.read_tx(),
    }
  }
}

/// 读事务持有快照与键空间句柄的所有权：多线程段每个连接各自取事务，不借用连接
pub struct FjallReadTxn {
  part: SingleWriterTxKeyspace,
  txn: fjall::Snapshot,
}

impl BenchReadTransaction for FjallReadTxn {
  type T<'txn>
    = FjallReader<'txn>
  where
    Self: 'txn;

  fn get_reader(&self) -> Self::T<'_> {
    FjallReader {
      part: &self.part,
      txn: &self.txn,
    }
  }
}

pub struct FjallReader<'a> {
  part: &'a SingleWriterTxKeyspace,
  txn: &'a fjall::Snapshot,
}

impl BenchReader for FjallReader<'_> {
  /// fjall 的读结果是引用计数的 `Slice`，与快照生命周期解耦，
  /// 所以 GAT 输出可以直接用它而不必退回 `OwnedOutput`
  type Output<'out>
    = Slice
  where
    Self: 'out;
  type Iterator<'out>
    = FjallBenchIterator
  where
    Self: 'out;

  fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>> {
    self.txn.get(self.part, key).ok().flatten()
  }

  fn range_from<'a>(&'a mut self, start: &'a [u8]) -> Self::Iterator<'a> {
    FjallBenchIterator {
      iter: self.txn.range(self.part, start..),
    }
  }

  /// fjall 没有 O(1) 计数，`Readable::len` 就是全表扫描，与 redb 的 len() 同段位。
  /// 扫描途中的 IO 错误只能报成 0（契约没有错误通道），但必须把原因留在日志里，
  /// 否则 harness 的精确断言会以「凭空少算」的形式崩，看不出是扫描失败。
  fn len(&mut self) -> u64 {
    match self.txn.len(self.part) {
      Ok(count) => count as u64,
      Err(e) => {
        fail("len() 全表扫描", e);
        0
      }
    }
  }
}

pub struct FjallBenchIterator {
  iter: Iter,
}

impl BenchIterator for FjallBenchIterator {
  type Output<'out>
    = Slice
  where
    Self: 'out;

  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    self.iter.next().and_then(|guard| guard.into_inner().ok())
  }
}

/// pop 游标：fjall 的 `first_key_value`/`last_key_value` 每次都从零重建整表归并
/// 迭代器（tx/write_tx.rs:82-88 就是 `iter().next()`），逐次弹出会把已经删掉的键
/// 再扫一遍，实测 O(n²)：quick 档 5000 次要 8s，只能被 harness 当成慢速后端外推。
/// 这里改持一个双端游标，两端各自向中间步进，并把两端已弹出的键记成边界——
/// 归并视图对墓碑的即时性不保证同一键被重复吐出，越界键一律按本端耗尽处理，
/// 使语义与 `BTreeMap::pop_first`/`pop_last` 一致（同一个键绝不交出去两次）。
struct PopCursor {
  iter: Iter,
  front_bound: Option<Slice>,
  back_bound: Option<Slice>,
}

impl PopCursor {
  fn new(iter: Iter) -> Self {
    Self {
      iter,
      front_bound: None,
      back_bound: None,
    }
  }

  fn is_popped(&self, key: &[u8]) -> bool {
    let behind_front = self
      .front_bound
      .as_ref()
      .is_some_and(|bound| key <= bound.as_ref());
    let ahead_back = self
      .back_bound
      .as_ref()
      .is_some_and(|bound| key >= bound.as_ref());
    behind_front || ahead_back
  }

  fn note_popped(&mut self, key: Slice, front: bool) {
    if front {
      self.front_bound = Some(key);
    } else {
      self.back_bound = Some(key);
    }
  }
}

pub struct FjallWriteTxn<'db> {
  db: &'db SingleWriterTxDatabase,
  part: &'db SingleWriterTxKeyspace,
  txn: Option<SingleWriterWriteTx<'db>>,
  pending: usize,
  sync: bool,
  /// 只为连续 pop 保留；任何一笔普通写、任何一次分段提交都会让它失去 RYOW 时效
  cursor: Option<PopCursor>,
}

impl<'db> FjallWriteTxn<'db> {
  /// 活跃事务与键空间句柄；`None` 只可能出现在事务已提交之后，
  /// 契约没有为它留错误通道，只能让调用方记失败
  fn live(&mut self) -> Option<(&mut SingleWriterWriteTx<'db>, &'db SingleWriterTxKeyspace)> {
    let part = self.part;
    Some((self.txn.as_mut()?, part))
  }

  /// 事务上下文之外的键空间视图，有序装载用它直接写表
  fn keyspace(&self) -> &Keyspace {
    self.part.as_ref()
  }

  /// 分段提交：先落盘旧事务（同时释放单写锁）再开新事务，
  /// 新事务的快照已包含刚提交的条目，RYOW 视图因此不受影响
  fn rotate(&mut self) -> Result<(), ()> {
    self.pending = 0;
    self.cursor = None;
    let txn = self.txn.take().ok_or(())?;
    txn.commit().map_err(|e| fail("分段提交", e))?;
    self.txn = Some(self.db.write_tx().durability(persist_mode(self.sync)));
    Ok(())
  }

  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
    self.cursor = None;
    if self.pending >= PENDING_LIMIT {
      self.rotate()?;
    }
    self.pending += 1;
    let (txn, part) = self.live().ok_or(())?;
    txn.insert(part, key, value);
    Ok(())
  }

  fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
    self.cursor = None;
    if self.pending >= PENDING_LIMIT {
      self.rotate()?;
    }
    self.pending += 1;
    let (txn, part) = self.live().ok_or(())?;
    txn.remove(part, key);
    Ok(())
  }

  /// fjall 无原生 pop：在事务自己的 RYOW 视图上取端点再删除，
  /// 语义与 `BTreeMap::pop_first`/`pop_last` 一致（返回被摘掉的键值）
  fn pop(&mut self, front: bool) -> Result<Option<(Slice, Slice)>, ()> {
    if self.pending >= PENDING_LIMIT {
      self.rotate()?;
    }
    if self.cursor.is_none() {
      let txn = self.txn.as_ref().ok_or(())?;
      self.cursor = Some(PopCursor::new(txn.iter(self.part)));
    }

    let popped = {
      let cursor = self.cursor.as_mut().ok_or(())?;
      let guard = if front {
        cursor.iter.next()
      } else {
        cursor.iter.next_back()
      };
      match guard {
        Some(guard) => {
          let (key, value) = guard.into_inner().map_err(|e| fail("弹出取端点", e))?;
          if cursor.is_popped(key.as_ref()) {
            None
          } else {
            cursor.note_popped(key.clone(), front);
            Some((key, value))
          }
        }
        None => None,
      }
    };

    let Some((key, value)) = popped else {
      return Ok(None);
    };
    self.pending += 1;
    let (txn, part) = self.live().ok_or(())?;
    txn.remove(part, key.as_ref());
    Ok(Some((key, value)))
  }

  /// 先收集再删除：遍历借用事务的只读视图，删除要可变借用，两阶段不可避免
  fn retain(&mut self, mut predicate: impl FnMut(&[u8], &[u8]) -> bool) -> Result<u64, ()> {
    let mut doomed: Vec<Slice> = Vec::new();
    {
      let (txn, part) = self.live().ok_or(())?;
      for guard in txn.iter(part) {
        let (key, value) = guard.into_inner().map_err(|e| fail("retain 遍历", e))?;
        if !predicate(&key, &value) {
          doomed.push(key);
        }
      }
    }
    let removed = doomed.len();
    for key in &doomed {
      self.remove(key)?;
    }
    Ok(removed as u64)
  }

  fn extract_if<F>(
    &mut self,
    range: (Bound<&[u8]>, Bound<&[u8]>),
    mut predicate: F,
  ) -> Result<VecBenchIterator<Slice>, ()>
  where
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool,
  {
    let mut extracted: Vec<(Slice, Slice)> = Vec::new();
    {
      let (txn, part) = self.live().ok_or(())?;
      // 键参数只能取 &[u8]：RangeBounds 对 Unsized 的 T 有重叠实现，K 又要求 Sized
      for guard in txn.range::<&[u8], _>(part, range) {
        let (key, value) = guard.into_inner().map_err(|e| fail("extract_if 遍历", e))?;
        if predicate(&key, &value) {
          extracted.push((key, value));
        }
      }
    }
    for (key, _value) in &extracted {
      self.remove(key)?;
    }
    Ok(VecBenchIterator::new(extracted))
  }

  /// 有序装载走 fjall 原生 ingestion（keyspace/mod.rs:302）：绕过 memtable 直接写
  /// 表文件，内存有界，是引擎最快的升序批量通道，对应 redb 的流式 bulk 装载
  fn insert_sorted<'i>(
    &mut self,
    pairs: impl Iterator<Item = (&'i [u8], &'i [u8])>,
  ) -> Result<(), ()> {
    if self.pending > 0 {
      self.rotate()?;
    }
    let mut ingestion = self
      .keyspace()
      .start_ingestion()
      .map_err(|e| fail("有序装载启动", e))?;
    let mut last: Option<&[u8]> = None;
    for (key, value) in pairs {
      // ingestion 内部对非升序键是 assert（release 也生效），这里先挡住以保证适配器不 panic
      if let Some(prev) = last {
        if key <= prev {
          eprintln!("fjall: 有序装载的键未严格递增");
          return Err(());
        }
      }
      last = Some(key);
      ingestion
        .write(key, value)
        .map_err(|e| fail("有序装载", e))?;
    }
    ingestion.finish().map_err(|e| fail("有序装载收尾", e))
  }
}

impl<'db> BenchWriteTransaction for FjallWriteTxn<'db> {
  type W<'txn>
    = FjallInserter<'txn, 'db>
  where
    Self: 'txn;

  fn get_inserter(&mut self) -> Self::W<'_> {
    FjallInserter { wt: self }
  }

  fn commit(mut self) -> Result<(), ()> {
    // 只剩最后一段未落盘的条目；分段提交的 durability 与各事务一致
    let txn = self.txn.take().ok_or(())?;
    txn.commit().map_err(|e| fail("提交事务", e))
  }
}

pub struct FjallInserter<'txn, 'db> {
  wt: &'txn mut FjallWriteTxn<'db>,
}

impl BenchInserter for FjallInserter<'_, '_> {
  type Output<'out>
    = Slice
  where
    Self: 'out;
  type ExtractIfIterator<'out, F>
    = VecBenchIterator<Slice>
  where
    Self: 'out,
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
    self.wt.insert(key, value)
  }

  fn insert_sorted<'i>(
    &mut self,
    pairs: impl Iterator<Item = (&'i [u8], &'i [u8])>,
  ) -> Result<(), ()> {
    self.wt.insert_sorted(pairs)
  }

  fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
    self.wt.remove(key)
  }

  fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
    self.wt.pop(true)
  }

  fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
    self.wt.pop(false)
  }

  fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, predicate: F) -> Result<u64, ()> {
    self.wt.retain(predicate)
  }

  fn extract_if<'a, F>(
    &'a mut self,
    range: (Bound<&[u8]>, Bound<&[u8]>),
    predicate: F,
  ) -> BenchExtractIfResult<'a, Self, F>
  where
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a,
  {
    self.wt.extract_if(range, predicate)
  }
}
