//! 引擎适配契约：与 redb-bench 的 trait 家族一一对应，
//! 使 redb 的每一段 workload 能原样驱动被测引擎。

use std::{ops::Bound, vec::IntoIter};

/// 被测数据库本体：持有存储、按需产生连接
pub trait BenchDatabase {
  type C<'db>: BenchDatabaseConnection
  where
    Self: 'db;

  /// 结果表列名
  fn db_type_name() -> &'static str;

  fn connect(&self) -> Self::C<'_>;

  /// 返回是否支持压缩；false 时「compacted size」记 N/A
  fn compact(&mut self) -> bool {
    false
  }
}

pub trait BenchDatabaseConnection: Send {
  type W<'db>: BenchWriteTransaction
  where
    Self: 'db;
  type R<'db>: BenchReadTransaction
  where
    Self: 'db;

  /// 返回是否支持切换持久化档位；false 时「nosync writes」记 N/A
  fn set_sync(&mut self, _sync: bool) -> bool {
    false
  }

  fn write_transaction(&self) -> Self::W<'_>;

  fn read_transaction(&self) -> Self::R<'_>;
}

pub trait BenchWriteTransaction {
  type W<'txn>: BenchInserter
  where
    Self: 'txn;

  fn get_inserter(&mut self) -> Self::W<'_>;

  #[allow(clippy::result_unit_err)]
  fn commit(self) -> Result<(), ()>;
}

pub type BenchPopEntry<'out, T> = (
  <T as BenchInserter>::Output<'out>,
  <T as BenchInserter>::Output<'out>,
);

pub type BenchPopResult<'out, T> = Result<Option<BenchPopEntry<'out, T>>, ()>;

pub type BenchExtractIfResult<'out, T, F> =
  Result<<T as BenchInserter>::ExtractIfIterator<'out, F>, ()>;

pub trait BenchInserter {
  type Output<'out>: AsRef<[u8]> + 'out
  where
    Self: 'out;
  type ExtractIfIterator<'out, F>: BenchIterator
  where
    Self: 'out,
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

  #[allow(clippy::result_unit_err)]
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;

  /// 键严格递增且大于表内所有既有键的批量装载，走引擎最快的有序写路径。
  /// 没有专用路径的引擎退回普通插入。
  #[allow(clippy::result_unit_err)]
  fn insert_sorted<'i>(
    &mut self,
    pairs: impl Iterator<Item = (&'i [u8], &'i [u8])>,
  ) -> Result<(), ()> {
    for (key, value) in pairs {
      self.insert(key, value)?;
    }
    Ok(())
  }

  #[allow(clippy::result_unit_err)]
  fn remove(&mut self, key: &[u8]) -> Result<(), ()>;

  #[allow(clippy::result_unit_err)]
  fn pop_first(&mut self) -> BenchPopResult<'_, Self>;

  #[allow(clippy::result_unit_err)]
  fn pop_last(&mut self) -> BenchPopResult<'_, Self>;

  /// 按序遍历全部条目，predicate 返回 false 的被删除，返回删除条数
  #[allow(clippy::result_unit_err)]
  fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, predicate: F) -> Result<u64, ()>;

  /// 对 `range` 内的条目应用 predicate，返回 true 的被摘除，
  /// 语义对齐 `std::collections::BTreeMap::extract_if`
  #[allow(clippy::result_unit_err)]
  fn extract_if<'a, F>(
    &'a mut self,
    range: (Bound<&[u8]>, Bound<&[u8]>),
    predicate: F,
  ) -> BenchExtractIfResult<'a, Self, F>
  where
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a;
}

pub trait BenchReadTransaction {
  type T<'txn>: BenchReader
  where
    Self: 'txn;

  fn get_reader(&self) -> Self::T<'_>;
}

#[allow(clippy::len_without_is_empty)]
pub trait BenchReader {
  type Output<'out>: AsRef<[u8]> + 'out
  where
    Self: 'out;
  type Iterator<'out>: BenchIterator
  where
    Self: 'out;

  fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>>;

  /// 值直读进调用方缓冲（对齐 C# 驱动 pinned slots 的零分配输出形态）：
  /// 命中把值字节拷入 `out`（长度须等于 value_size）返回 `Some(())`，
  /// 未命中返回 `None`。默认实现走 `get` 后整值拷贝——无原生直读能力的引擎
  /// 零成本回落，消费语义与 `get` 逐字节一致。
  fn get_into(&mut self, key: &[u8], out: &mut [u8]) -> Option<()> {
    let got = self.get(key)?;
    out.copy_from_slice(got.as_ref());
    Some(())
  }

  fn range_from<'a>(&'a mut self, start: &'a [u8]) -> Self::Iterator<'a>;

  fn len(&mut self) -> u64;
}

pub trait BenchIterator {
  type Output<'out>: AsRef<[u8]> + 'out
  where
    Self: 'out;

  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)>;
}

/// 供适配器把一次性快照（如无序引擎的全表扫描）包装成 `BenchIterator`
pub struct VecBenchIterator<T> {
  inner: IntoIter<(T, T)>,
}

impl<T> VecBenchIterator<T> {
  pub fn new(entries: Vec<(T, T)>) -> Self {
    Self {
      inner: entries.into_iter(),
    }
  }
}

impl<T: AsRef<[u8]>> BenchIterator for VecBenchIterator<T> {
  type Output<'out>
    = T
  where
    Self: 'out;

  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    self.inner.next()
  }
}

/// 值为 `Vec<u8>` 的输出包装：让拥有所有权的适配器满足 `AsRef<[u8]>` 契约
#[derive(Clone)]
pub struct OwnedOutput(pub Vec<u8>);

impl AsRef<[u8]> for OwnedOutput {
  fn as_ref(&self) -> &[u8] {
    &self.0
  }
}
