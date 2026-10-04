//! sqlite 对照列：把 redb 同构契约映射到 rusqlite + bundled SQLite 的有序 BLOB 主键表。
//!
//! 形态取自 redb 上游 `redb-bench-compare` 的 sqlite 适配器（common/mod.rs:949-1278），
//! 键为二进制 BLOB 主键（COLLATE BINARY，点查与区间步进都走主键索引的 memcmp 序）。
//! 相对上游有意补了三处真实语义，均不为刷数：
//! - 库文件开成 WAL 且落在 harness 目录之内：尺寸段 walkdir 该目录，-wal/-shm
//!   与主文件一并如实计入（上游用默认 DELETE 日志，旁路文件形态与本口径不同）；
//! - `set_sync` 落到 `PRAGMA synchronous`：nosync 段逐条 commit 真的不落盘；
//! - `compact` 落到 `VACUUM`：整库重建、截断文件，compacted size 是真实缩后结果。
//! 无专用快速通道的段落（如有序装载）如实退回普通 INSERT，绝不伪造。

use std::{
  ops::Bound,
  path::{Path, PathBuf},
};

use rusqlite::{Connection, OptionalExtension, Statement, Transaction, params_from_iter};
use wedb_bench::{
  config::Workload,
  traits::{
    BenchDatabase, BenchDatabaseConnection, BenchExtractIfResult, BenchInserter, BenchIterator,
    BenchPopResult, BenchReadTransaction, BenchReader, BenchWriteTransaction, VecBenchIterator,
  },
};

/// 库文件固定名：必须落在 harness 给的目录之内，尺寸段（walkdir 整目录）才能
/// 把主文件与 -wal/-shm 一并计入；目录外的旁路存储等于漏报
const DB_FILE: &str = "bench.db";

/// 表形状与 workload 契约一致：24 字节二进制键、150 字节二进制值、按主键有序。
/// BLOB 主键本就按 memcmp 比较，显式 COLLATE BINARY 把「有序」写成契约，
/// 不随 SQLite 默认的列亲和调整漂移
const CREATE_SQL: &str =
  "CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY COLLATE BINARY, value BLOB);";

/// trait 的失败通道只有 `()`，SQLite 的真实错误不能咽下也不能伪造：
/// 打到 stderr 留给列日志，返回值照 Err(()) 走，harness 自会记 N/A 或中止该列
fn fail(op: &str, err: rusqlite::Error) {
  eprintln!("sqlite: {op} 失败 — {err}");
}

/// SQLite 引擎句柄：只持库路径与缓存预算。rusqlite 的 Connection 不是 Sync，
/// 多线程读段必须由每次 `connect` 各开各的连接，这里刻意不做连接池
pub struct SqliteEngine {
  path: PathBuf,
  cache_size: usize,
}

impl SqliteEngine {
  /// 在 harness 目录内建库：runner 传的是工作目录而非库路径，库文件为其子项
  pub fn open(path: &Path, workload: &Workload) -> Result<Self, String> {
    let db_path = path.join(DB_FILE);
    let conn = Connection::open(&db_path)
      .map_err(|e| format!("sqlite 打开 {} 失败 — {e}", db_path.display()))?;
    setup(&conn, workload.cache_size).map_err(|e| format!("sqlite 初始化失败 — {e}"))?;
    drop(conn);
    Ok(Self {
      path: db_path,
      cache_size: workload.cache_size,
    })
  }
}

/// 每个连接都要落的会话级设置。journal_mode 是库属性、持久化，重复设置幂等；
/// synchronous/cache_size/mmap 是连接属性，多线程读段的新连接同样需要，
/// 故 open 与 connect 共用这一份
fn setup(conn: &Connection, cache_size: usize) -> Result<(), rusqlite::Error> {
  // cache_size 负值按 KiB 计：直接吃 workload 给的字节预算，与其余列同一口径；
  // mmap_size=0 关内存映射读，读路径走 pager，尺寸段与内存统计都只反映真实文件
  let cache_kib = -((cache_size / 1024) as i64);
  conn.execute_batch(&format!(
    "PRAGMA journal_mode = WAL;
     PRAGMA synchronous = FULL;
     PRAGMA cache_size = {cache_kib};
     PRAGMA mmap_size = 0;
     PRAGMA temp_store = FILE;
     {CREATE_SQL}"
  ))
}

impl BenchDatabase for SqliteEngine {
  type C<'db>
    = SqliteConnection
  where
    Self: 'db;

  fn db_type_name() -> &'static str {
    "sqlite"
  }

  fn connect(&self) -> Self::C<'_> {
    let conn = Connection::open(&self.path).expect("sqlite connect 失败");
    if let Err(e) = setup(&conn, self.cache_size) {
      fail("连接初始化", e);
    }
    SqliteConnection { conn }
  }

  /// VACUUM 是 SQLite 唯一的真压缩路径：整库重写到临时文件再替换，回收释放页、
  /// 顺带把 WAL 收编。harness 在调用本段前已 drop 主连接，无锁竞争；
  /// 任一步失败返回 false，compacted size 诚实记 N/A
  fn compact(&mut self) -> bool {
    let outcome = Connection::open(&self.path)
      .map_err(|e| e.to_string())
      .and_then(|conn| conn.execute_batch("VACUUM;").map_err(|e| e.to_string()));
    match outcome {
      Ok(()) => true,
      Err(e) => {
        eprintln!("sqlite: VACUUM 失败 — {e}");
        false
      }
    }
  }
}

pub struct SqliteConnection {
  conn: Connection,
}

impl BenchDatabaseConnection for SqliteConnection {
  type W<'db>
    = SqliteWriteTxn<'db>
  where
    Self: 'db;
  type R<'db>
    = SqliteReadTxn<'db>
  where
    Self: 'db;

  /// nosync 映射 synchronous=OFF：逐条 commit 完全不 fsync WAL。不能用 NORMAL——
  /// WAL 模式下 NORMAL 仍会在 checkpoint 时落盘，与 redb 的 Durability::None 不同档
  fn set_sync(&mut self, sync: bool) -> bool {
    let pragma = if sync {
      "PRAGMA synchronous = FULL;"
    } else {
      "PRAGMA synchronous = OFF;"
    };
    match self.conn.execute(pragma, []) {
      Ok(_) => true,
      Err(e) => {
        fail("切换持久化档位", e);
        false
      }
    }
  }

  fn write_transaction(&self) -> Self::W<'_> {
    // unchecked_transaction 即 BEGIN DEFERRED：每段一个真事务，commit 时才收口，
    // individual writes 段的每键一 commit 因此带每键一次 WAL fsync（FULL 档）
    let txn = self
      .conn
      .unchecked_transaction()
      .expect("sqlite 开启写事务失败");
    SqliteWriteTxn { txn }
  }

  fn read_transaction(&self) -> Self::R<'_> {
    SqliteReadTxn { conn: &self.conn }
  }
}

pub struct SqliteWriteTxn<'db> {
  txn: Transaction<'db>,
}

impl<'db> BenchWriteTransaction for SqliteWriteTxn<'db> {
  type W<'txn>
    = SqliteInserter<'txn, 'db>
  where
    Self: 'txn;

  fn get_inserter(&mut self) -> Self::W<'_> {
    SqliteInserter { txn: &self.txn }
  }

  fn commit(self) -> Result<(), ()> {
    self.txn.commit().map_err(|_| ())
  }
}

pub struct SqliteInserter<'txn, 'db> {
  txn: &'txn Transaction<'db>,
}

type SqliteEntry = (Vec<u8>, Vec<u8>);

impl SqliteInserter<'_, '_> {
  /// 两端弹出：ORDER BY key + LIMIT 1 由主键索引直接定位端点（min/max 优化），
  /// 再按 rowid 删除，避免在 UNIQUE 索引上二次比对键；
  /// 方向拼进 SQL 的是本文件内的两个字面量，无注入面
  fn pop_ordered(&self, direction: &str) -> Result<Option<SqliteEntry>, ()> {
    let sql = format!("SELECT rowid, key, value FROM kv ORDER BY key {direction} LIMIT 1");
    let entry = self
      .txn
      .query_row(&sql, [], |row| {
        Ok((
          row.get::<_, i64>(0)?,
          row.get::<_, Vec<u8>>(1)?,
          row.get::<_, Vec<u8>>(2)?,
        ))
      })
      .optional()
      .map_err(|_| ())?;
    if let Some((rowid, key, value)) = entry {
      self
        .txn
        .execute("DELETE FROM kv WHERE rowid = ?", [rowid])
        .map_err(|_| ())?;
      return Ok(Some((key, value)));
    }
    Ok(None)
  }
}

impl BenchInserter for SqliteInserter<'_, '_> {
  type Output<'out>
    = Vec<u8>
  where
    Self: 'out;
  /// rusqlite 没有游标级条件摘除，extract_if 收集后回放，迭代器用 harness 的
  /// VecBenchIterator 包装一次性快照
  type ExtractIfIterator<'out, F>
    = VecBenchIterator<Vec<u8>>
  where
    Self: 'out,
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
    // OR REPLACE：workload 键序列近似无碰撞，但一旦碰撞 REPLACE 保住
    // 「一键一条目」的表形与 harness 的精确计数断言，不至于 UNIQUE 冲突中止整列
    self
      .txn
      .execute(
        "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
        [key, value],
      )
      .map(|_| ())
      .map_err(|_| ())
  }

  // insert_sorted 不覆写：SQLite 没有 LMDB append 式的有序专用通道，
  // 如实走默认逐条 insert 回退。升序键本就是 BLOB 主键 B-tree 的最右路径追加，
  // 是引擎在该形态下的自然快路径，不必也不应再包一层假 API

  fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
    self
      .txn
      .execute("DELETE FROM kv WHERE key = ?", [key])
      .map(|_| ())
      .map_err(|_| ())
  }

  fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
    self.pop_ordered("ASC")
  }

  fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
    self.pop_ordered("DESC")
  }

  /// 先按 key 序收集再删：SQLite 不允许在同表 SELECT 游标步进途中删当前行
  /// （游标失效，行为未定义），收集-删除两阶段是 rusqlite 形态下的必然；
  /// 遍历序固定为 memcmp 升序，满足 harness 谓词按序计数的假设
  fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, mut predicate: F) -> Result<u64, ()> {
    let mut doomed: Vec<Vec<u8>> = Vec::new();
    {
      let mut stmt = self
        .txn
        .prepare("SELECT key, value FROM kv ORDER BY key")
        .map_err(|_| ())?;
      let mut rows = stmt.query([]).map_err(|_| ())?;
      while let Some(row) = rows.next().map_err(|_| ())? {
        let key: Vec<u8> = row.get(0).map_err(|_| ())?;
        let value: Vec<u8> = row.get(1).map_err(|_| ())?;
        if !predicate(&key, &value) {
          doomed.push(key);
        }
      }
    }
    let removed = doomed.len() as u64;
    let mut delete_stmt = self
      .txn
      .prepare("DELETE FROM kv WHERE key = ?")
      .map_err(|_| ())?;
    for key in &doomed {
      delete_stmt.execute([key.as_slice()]).map_err(|_| ())?;
    }
    Ok(removed)
  }

  /// 区间上下界压进 SQL：只扫命中区间的索引段，谓词仍按 key 序在 Rust 侧评估
  fn extract_if<'a, F>(
    &'a mut self,
    range: (Bound<&[u8]>, Bound<&[u8]>),
    mut predicate: F,
  ) -> BenchExtractIfResult<'a, Self, F>
  where
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a,
  {
    let mut conds: Vec<&str> = Vec::new();
    let mut bounds: Vec<&[u8]> = Vec::new();
    match range.0 {
      Bound::Included(start) => {
        conds.push("key >= ?");
        bounds.push(start);
      }
      Bound::Excluded(start) => {
        conds.push("key > ?");
        bounds.push(start);
      }
      Bound::Unbounded => {}
    }
    match range.1 {
      Bound::Included(end) => {
        conds.push("key <= ?");
        bounds.push(end);
      }
      Bound::Excluded(end) => {
        conds.push("key < ?");
        bounds.push(end);
      }
      Bound::Unbounded => {}
    }
    let mut sql = String::from("SELECT key, value FROM kv");
    if !conds.is_empty() {
      sql.push_str(" WHERE ");
      sql.push_str(&conds.join(" AND "));
    }
    sql.push_str(" ORDER BY key");

    let mut extracted: Vec<SqliteEntry> = Vec::new();
    {
      let mut stmt = self.txn.prepare(&sql).map_err(|_| ())?;
      let mut rows = stmt
        .query(params_from_iter(bounds.iter()))
        .map_err(|_| ())?;
      while let Some(row) = rows.next().map_err(|_| ())? {
        let key: Vec<u8> = row.get(0).map_err(|_| ())?;
        let value: Vec<u8> = row.get(1).map_err(|_| ())?;
        if predicate(&key, &value) {
          extracted.push((key, value));
        }
      }
    }
    let mut delete_stmt = self
      .txn
      .prepare("DELETE FROM kv WHERE key = ?")
      .map_err(|_| ())?;
    for (key, _value) in &extracted {
      delete_stmt.execute([key.as_slice()]).map_err(|_| ())?;
    }
    Ok(VecBenchIterator::new(extracted))
  }
}

pub struct SqliteReadTxn<'db> {
  conn: &'db Connection,
}

impl<'db> BenchReadTransaction for SqliteReadTxn<'db> {
  type T<'txn>
    = SqliteReader<'db>
  where
    Self: 'txn;

  fn get_reader(&self) -> Self::T<'_> {
    // 读段单连接无并发写者（harness 读/写段串行，多线程段只读），SQLite 自动
    // 提交下每条 SELECT 自带原子快照，不必再 BEGIN 一层长事务占住 WAL 读锁。
    // 三个语句在此预编译：random reads 段要重放百万次点查，不能摊上逐次 prepare
    let get_stmt = self
      .conn
      .prepare("SELECT value FROM kv WHERE key = ?")
      .expect("sqlite 预编译点查失败");
    let range_stmt = self
      .conn
      .prepare("SELECT key, value FROM kv WHERE key >= ? ORDER BY key")
      .expect("sqlite 预编译区间查失败");
    let len_stmt = self
      .conn
      .prepare("SELECT COUNT(*) FROM kv")
      .expect("sqlite 预编译计数失败");
    SqliteReader {
      get_stmt,
      range_stmt,
      len_stmt,
    }
  }
}

pub struct SqliteReader<'db> {
  get_stmt: Statement<'db>,
  range_stmt: Statement<'db>,
  len_stmt: Statement<'db>,
}

impl BenchReader for SqliteReader<'_> {
  type Output<'out>
    = Vec<u8>
  where
    Self: 'out;
  type Iterator<'out>
    = SqliteIterator<'out>
  where
    Self: 'out;

  fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>> {
    // 缺行与 IO 错误都折 None：契约返回型没有错误通道，harness 对缺行本就 unwrap
    // 暴露，比伪造命中诚实
    self.get_stmt.query_row([key], |row| row.get(0)).ok()
  }

  /// 真游标区间：结果集挂在 range_stmt 上按 memcmp 序流式步进（非快照回放），
  /// 下一次 range_from 重置复用同一语句
  fn range_from<'a>(&'a mut self, start: &'a [u8]) -> Self::Iterator<'a> {
    let rows = self.range_stmt.query([start]).expect("sqlite 区间扫描失败");
    SqliteIterator { rows }
  }

  fn len(&mut self) -> u64 {
    // COUNT(*) 沿主键索引全扫：SQLite 没有 O(1) 行数，如实按引擎的计数成本计时
    self
      .len_stmt
      .query_row([], |row| row.get::<_, i64>(0))
      .expect("sqlite 计数失败") as u64
  }
}

pub struct SqliteIterator<'stmt> {
  rows: rusqlite::Rows<'stmt>,
}

impl BenchIterator for SqliteIterator<'_> {
  type Output<'out>
    = Vec<u8>
  where
    Self: 'out;

  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    let row = self.rows.next().expect("sqlite 区间步进失败")?;
    Some((
      row.get::<_, Vec<u8>>(0).expect("sqlite 取键失败"),
      row.get::<_, Vec<u8>>(1).expect("sqlite 取值失败"),
    ))
  }
}
