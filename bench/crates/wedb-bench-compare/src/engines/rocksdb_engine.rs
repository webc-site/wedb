//! rocksdb 对照列：把 redb 同构契约接到 RocksDB 的乐观事务面（写）与快照面（读）。
//!
//! 形态取自 redb 上游 `redb-bench-compare` 的 rocksdb 适配器（common/mod.rs:370-682），
//! 再按 rust-rocksdb 0.25 的实际能力重挂到本套 trait 形状上：
//! - 写侧保持 `OptimisticTransactionDB` 事务：一个 harness 写事务对应一个 rocksdb 事务，
//!   提交才落 WAL，durability 由 `WriteOptions::set_sync` 决定；事务自己的迭代器能看见
//!   本事务未提交的删改（RYOW），pop / retain / extract_if 因此落在引擎真实能力上；
//! - 有序装载不走事务，改走 `SstFileWriter` + `ingest_external_file` 的原生 bulk 通道。

use std::{
  fmt, fs,
  ops::Bound,
  path::{Path, PathBuf},
  process::id,
  thread::available_parallelism,
};

use rocksdb::{
  BlockBasedOptions, Cache, DBIteratorWithThreadMode, Direction, IteratorMode,
  OptimisticTransactionDB, OptimisticTransactionOptions, Options, SnapshotWithThreadMode,
  SstFileWriter, Transaction, WriteOptions,
};
use wedb_bench::{
  config::Workload,
  traits::{
    BenchDatabase, BenchDatabaseConnection, BenchExtractIfResult, BenchInserter, BenchIterator,
    BenchPopResult, BenchReadTransaction, BenchReader, BenchWriteTransaction, VecBenchIterator,
  },
};

/// pop 摘除的键值对（clippy type_complexity 拆别名）
type RocksDbPair = (Box<[u8]>, Box<[u8]>);

/// 事务里未提交的键值对（含其 writebatch 索引）全在内存，redb 形状的一次装载会把
/// 内存打爆（上游 redb-bench 为此留了同一个常量，见 common/mod.rs:20）；攒到该条数
/// 就先提交再续一个事务，分段提交的成本留在本段耗时里。
const PENDING_LIMIT: usize = 100_000;

/// 有序装载的 SST 暂存子目录：放在库目录之内（尺寸统计与库同侧、退出时随工作目录
/// 一起清理），且 RocksDB 的 obsolete file 扫描只看库目录顶层的数字命名文件，
/// 不会把这个子目录里的中间产物误删
const STAGING_DIR: &str = "sst_staging";

/// trait 契约的失败通道只有 `()`，把 rocksdb 的错误打到 stderr 留给列日志
fn fail(op: &str, err: impl fmt::Display) {
  eprintln!("rocksdb: {op} 失败 — {err}");
}

/// 按 sync 档位开一个乐观事务：sync 档提交时 WAL fsync（redb individual writes 段的
/// 持久语义），nosync 档只写 WAL 不 fsync；事务自带快照，读面固定在其开始时
///
/// 上游在 macOS 上还会在提交后把库目录整份重刷一遍 fsync 兜 RocksDB 的持久性缺口
/// （common/mod.rs:467-481），那会把单次提交的 F_FULLFSYNC 次数放大到文件个数；
/// 本列只按 RocksDB 自己的 sync 承诺落 WAL，与其余列「一次提交一次持久化」同口径
fn begin(db: &OptimisticTransactionDB, sync: bool) -> Transaction<'_, OptimisticTransactionDB> {
  let mut write_opt = WriteOptions::new();
  write_opt.set_sync(sync);
  let mut txn_opt = OptimisticTransactionOptions::new();
  txn_opt.set_snapshot(true);
  db.transaction_opt(&write_opt, &txn_opt)
}

/// 把升序键值对写成一个大 SST：与建库同一套表选项（比较器、块格式必须一致才允许 ingest）
fn write_sorted_sst<'i>(
  opts: &Options,
  path: &Path,
  pairs: impl Iterator<Item = (&'i [u8], &'i [u8])>,
) -> Result<(), ()> {
  let mut writer = SstFileWriter::create(opts);
  writer.open(path).map_err(|e| fail("有序装载开文件", e))?;
  let mut prev: Option<&[u8]> = None;
  for (key, value) in pairs {
    // SstFileWriter 对非升序键是引擎侧硬失败，这里先挡住并留下可读原因
    if let Some(prev) = prev
      && key <= prev
    {
      eprintln!("rocksdb: 有序装载的键未严格递增");
      return Err(());
    }
    prev = Some(key);
    writer.put(key, value).map_err(|e| fail("有序装载", e))?;
  }
  writer.finish().map_err(|e| fail("有序装载收尾", e))
}

/// rocksdb 引擎句柄：库目录就是 runner 给的 path，SST、WAL、MANIFEST 全在其内，
/// 因此 harness 的整目录尺寸统计天然覆盖旁路存储
pub struct RocksdbEngine {
  db: OptimisticTransactionDB,
  /// 有序装载的 SST 写手复用建库选项；`Options::clone` 会连带把块缓存/布隆过滤的
  /// keep-alive 一起复制，故可安全长存于引擎内
  table_opts: Options,
}

impl RocksdbEngine {
  pub fn open(path: &Path, workload: &Workload) -> Result<Self, String> {
    let mut bb = BlockBasedOptions::default();
    bb.set_block_cache(&Cache::new_lru_cache(workload.cache_size));
    bb.set_bloom_filter(10.0, false);

    let mut opts = Options::default();
    opts.set_block_based_table_factory(&bb);
    opts.create_if_missing(true);
    opts.increase_parallelism(available_parallelism().map_or(1, |n| n.get()) as i32);
    // 其余全部留 RocksDB 缺省（含压缩）：本 manifest 未编 snappy，缺省压缩会退化为
    // 不压缩存块，与上游公布口径一致；评测数据是随机字节，显式换 lz4 只是白付 CPU
    let db = OptimisticTransactionDB::open(&opts, path)
      .map_err(|e| format!("rocksdb 打开 {} 失败 — {e}", path.display()))?;
    Ok(Self {
      db,
      table_opts: opts,
    })
  }
}

impl BenchDatabase for RocksdbEngine {
  type C<'db>
    = RocksdbConnection<'db>
  where
    Self: 'db;

  fn db_type_name() -> &'static str {
    "rocksdb"
  }

  fn connect(&self) -> Self::C<'_> {
    // 与 redb/hash 列同档：默认同步提交
    RocksdbConnection {
      db: &self.db,
      table_opts: &self.table_opts,
      sync: true,
    }
  }

  /// 真实回收两步：先把 memtable 落成表文件（否则尾部数据与其 WAL 永远计入尺寸），
  /// 再做全量 `compact_range` 把墓碑与被覆盖版本压到底层并重写表文件；manual
  /// compaction 是阻塞的，返回时旧文件已排队删除。flush 失败即返回 false（诚实记 N/A）
  fn compact(&mut self) -> bool {
    if let Err(e) = self.db.flush() {
      fail("压缩前 flush", e);
      return false;
    }
    self.db.compact_range::<&[u8], &[u8]>(None, None);
    // WAL 同步后只留一个空文件，避免把已落盘数据的日志尾算进 compacted 尺寸
    if let Err(e) = self.db.flush_wal(true) {
      fail("压缩后 flush_wal", e);
    }
    true
  }
}

pub struct RocksdbConnection<'a> {
  db: &'a OptimisticTransactionDB,
  table_opts: &'a Options,
  sync: bool,
}

impl BenchDatabaseConnection for RocksdbConnection<'_> {
  type W<'db>
    = RocksdbWriteTxn<'db>
  where
    Self: 'db;
  type R<'db>
    = RocksdbReadTxn<'db>
  where
    Self: 'db;

  /// RocksDB 的持久档位就是 WriteOptions 的 sync 位，逐事务可切
  fn set_sync(&mut self, sync: bool) -> bool {
    self.sync = sync;
    true
  }

  fn write_transaction(&self) -> Self::W<'_> {
    RocksdbWriteTxn {
      db: self.db,
      table_opts: self.table_opts,
      txn: Some(begin(self.db, self.sync)),
      pending: 0,
      sync: self.sync,
    }
  }

  fn read_transaction(&self) -> Self::R<'_> {
    RocksdbReadTxn {
      snapshot: self.db.snapshot(),
    }
  }
}

/// 读事务持有快照所有权：多线程段每个连接各自取事务，不借用连接
pub struct RocksdbReadTxn<'db> {
  snapshot: SnapshotWithThreadMode<'db, OptimisticTransactionDB>,
}

impl<'db> BenchReadTransaction for RocksdbReadTxn<'db> {
  type T<'txn>
    = RocksdbReader<'txn, 'db>
  where
    Self: 'txn;

  fn get_reader(&self) -> Self::T<'_> {
    RocksdbReader {
      snapshot: &self.snapshot,
    }
  }
}

pub struct RocksdbReader<'txn, 'db> {
  snapshot: &'txn SnapshotWithThreadMode<'db, OptimisticTransactionDB>,
}

impl BenchReader for RocksdbReader<'_, '_> {
  /// 读结果按值交出（`Vec<u8>`）：RocksDB 的 pinned slice 生命周期与 DB 借用绑定，
  /// 挂不进契约要求的 `Output<'out>`，退回拥有权形式
  type Output<'out>
    = Vec<u8>
  where
    Self: 'out;
  type Iterator<'out>
    = RocksdbBenchIterator<'out>
  where
    Self: 'out;

  fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>> {
    self.snapshot.get(key).ok().flatten()
  }

  /// 锚定起始键正向 seek，随后是引擎自己的有序步进（memtable + 各层表文件的归并游标）
  fn range_from<'a>(&'a mut self, start: &'a [u8]) -> Self::Iterator<'a> {
    RocksdbBenchIterator {
      iter: self
        .snapshot
        .iterator(IteratorMode::From(start, Direction::Forward)),
    }
  }

  /// RocksDB 没有廉价精确计数（`est.num-keys` 是估值，过不了 harness 的精确断言），
  /// 只能整表步进；这与 redb 的 O(1) 树计数不同，如实反映 len 依赖遍历的事实
  fn len(&mut self) -> u64 {
    self
      .snapshot
      .iterator(IteratorMode::Start)
      .filter_map(Result::ok)
      .count() as u64
  }
}

pub struct RocksdbBenchIterator<'a> {
  iter: DBIteratorWithThreadMode<'a, OptimisticTransactionDB>,
}

impl BenchIterator for RocksdbBenchIterator<'_> {
  type Output<'out>
    = Box<[u8]>
  where
    Self: 'out;

  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    self.iter.next().and_then(Result::ok)
  }
}

pub struct RocksdbWriteTxn<'db> {
  db: &'db OptimisticTransactionDB,
  table_opts: &'db Options,
  txn: Option<Transaction<'db, OptimisticTransactionDB>>,
  /// 当前事务里已积压的未提交写条数
  pending: usize,
  sync: bool,
}

impl<'db> RocksdbWriteTxn<'db> {
  /// 活跃事务；`None` 只可能出现在事务已提交之后，契约没有为它留错误通道
  fn live(&mut self) -> Option<&Transaction<'db, OptimisticTransactionDB>> {
    self.txn.as_ref()
  }

  /// 分段提交：先落盘旧事务再开新事务，新事务的快照已包含刚提交的条目，
  /// 因此 RYOW 视图不受影响；分段提交的 durability 与各事务一致
  fn rotate(&mut self) -> Result<(), ()> {
    self.pending = 0;
    let txn = self.txn.take().ok_or(())?;
    txn.commit().map_err(|e| fail("分段提交", e))?;
    self.txn = Some(begin(self.db, self.sync));
    Ok(())
  }

  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
    if self.pending >= PENDING_LIMIT {
      self.rotate()?;
    }
    self.pending += 1;
    self
      .live()
      .ok_or(())?
      .put(key, value)
      .map_err(|e| fail("插入", e))
  }

  fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
    if self.pending >= PENDING_LIMIT {
      self.rotate()?;
    }
    self.pending += 1;
    self
      .live()
      .ok_or(())?
      .delete(key)
      .map_err(|e| fail("删除", e))
  }

  /// RocksDB 事务无原生 pop：在本事务自己的 RYOW 视图上取端点再删除，
  /// 语义与 `BTreeMap::pop_first`/`pop_last` 一致（返回被摘掉的键值）
  fn pop(&mut self, front: bool) -> Result<Option<RocksDbPair>, ()> {
    if self.pending >= PENDING_LIMIT {
      self.rotate()?;
    }
    let mode = if front {
      IteratorMode::Start
    } else {
      IteratorMode::End
    };
    let entry = self
      .live()
      .ok_or(())?
      .iterator(mode)
      .next()
      .transpose()
      .map_err(|e| fail("弹出取端点", e))?;
    if let Some((key, _value)) = &entry {
      self.remove(key)?;
    }
    Ok(entry)
  }

  /// RocksDB 的事务 API 没有游标原位删除（上游 common/mod.rs:550 同样处理）：
  /// 先遍历收集要删的键，再逐条删除
  fn retain(&mut self, mut predicate: impl FnMut(&[u8], &[u8]) -> bool) -> Result<u64, ()> {
    let mut doomed: Vec<Box<[u8]>> = Vec::new();
    {
      let txn = self.live().ok_or(())?;
      for entry in txn.iterator(IteratorMode::Start) {
        let (key, value) = entry.map_err(|e| fail("retain 遍历", e))?;
        if !predicate(&key, &value) {
          doomed.push(key);
        }
      }
    }
    let removed = doomed.len() as u64;
    for key in &doomed {
      self.remove(key)?;
    }
    Ok(removed)
  }

  /// 区间条件摘除同样没有游标删除：定位到左边界，走到右边界为止，
  /// 谓词命中的先收集再删除，随后把摘掉的条目交给 harness 计数
  fn extract_if<F>(
    &mut self,
    range: (Bound<&[u8]>, Bound<&[u8]>),
    mut predicate: F,
  ) -> Result<VecBenchIterator<Box<[u8]>>, ()>
  where
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool,
  {
    let mut extracted: Vec<RocksDbPair> = Vec::new();
    {
      let mode = match range.0 {
        Bound::Included(start) | Bound::Excluded(start) => {
          IteratorMode::From(start, Direction::Forward)
        }
        Bound::Unbounded => IteratorMode::Start,
      };
      let txn = self.live().ok_or(())?;
      for entry in txn.iterator(mode) {
        let (key, value) = entry.map_err(|e| fail("extract_if 遍历", e))?;
        let key_ref: &[u8] = &key;
        // seek 落在左边界之上：Excluded 时首个等值键要跳过
        if let Bound::Excluded(start) = range.0
          && key_ref == start
        {
          continue;
        }
        let past_end = match range.1 {
          Bound::Included(end) => key_ref > end,
          Bound::Excluded(end) => key_ref >= end,
          Bound::Unbounded => false,
        };
        if past_end {
          break;
        }
        if predicate(key_ref, &value) {
          extracted.push((key, value));
        }
      }
    }
    for (key, _value) in &extracted {
      self.remove(key)?;
    }
    Ok(VecBenchIterator::new(extracted))
  }

  /// 有序装载走 RocksDB 原生 bulk ingest：`SstFileWriter` 直写 SST 再
  /// `ingest_external_file` 挂进当前层，绕过 memtable/WAL 的逐键写，是引擎对
  /// 「键严格递增」最快的通道（对应 redb 的流式 bulk 装载）
  fn insert_sorted<'i>(
    &mut self,
    pairs: impl Iterator<Item = (&'i [u8], &'i [u8])>,
  ) -> Result<(), ()> {
    // 先把积压条目落盘：ingest 的表文件序号为 0，若与本事务未提交的同区间写并存，
    // RocksDB 要先 flush memtable 再判定冲突；契约保证本段键大于全部既有键，
    // 干净地分段提交后 ingest 只做一次文件挂载
    if self.pending > 0 {
      self.rotate()?;
    }
    let db = self.db;
    let opts = self.table_opts;
    let stage_dir: PathBuf = db.path().join(STAGING_DIR);
    let stage_file = stage_dir.join(format!("sorted-{}.sst", id()));
    fs::create_dir_all(&stage_dir).map_err(|e| fail("有序装载建暂存目录", e))?;
    write_sorted_sst(opts, &stage_file, pairs)?;
    // 同目录树内 ingest 走硬链接，不复制字节；暂存文件只是同一份数据的别名，如实清掉
    let ingested = db.ingest_external_file(vec![&stage_file]);
    if let Err(e) = ingested {
      fail("有序装载 ingest", e);
      let _ = fs::remove_file(&stage_file);
      let _ = fs::remove_dir(&stage_dir);
      return Err(());
    }
    let _ = fs::remove_file(&stage_file);
    let _ = fs::remove_dir(&stage_dir);
    Ok(())
  }
}

impl<'db> BenchWriteTransaction for RocksdbWriteTxn<'db> {
  type W<'txn>
    = RocksdbInserter<'txn, 'db>
  where
    Self: 'txn;

  fn get_inserter(&mut self) -> Self::W<'_> {
    RocksdbInserter { wt: self }
  }

  fn commit(mut self) -> Result<(), ()> {
    // 只剩最后一段未落盘的条目；分段提交已按同一 sync 档位落盘
    let txn = self.txn.take().ok_or(())?;
    txn.commit().map_err(|e| fail("提交事务", e))
  }
}

pub struct RocksdbInserter<'txn, 'db> {
  wt: &'txn mut RocksdbWriteTxn<'db>,
}

impl BenchInserter for RocksdbInserter<'_, '_> {
  type Output<'out>
    = Box<[u8]>
  where
    Self: 'out;
  type ExtractIfIterator<'out, F>
    = VecBenchIterator<Box<[u8]>>
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
