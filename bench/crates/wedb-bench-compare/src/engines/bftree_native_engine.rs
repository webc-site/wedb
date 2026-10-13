//! bf-tree 0.5.x（garnet BfTree 的 rust 底座，C# 经 P/Invoke 消费）的同语言
//! 白盒对基适配：同步接口直驱，无 async 桥接——与 wbftree 列（bftree_engine）
//! 构成 B+ 树同构对比。

use std::{
  fs::{create_dir_all, write},
  marker::PhantomData,
  ops::Bound,
  path::Path,
  sync::Arc,
};

use bf_tree::{BfTree, Config, LeafInsertResult, LeafReadResult, ScanReturnField};
use wedb_bench::{
  REDB_CACHE_SIZE,
  config::Workload,
  traits::{
    BenchDatabase, BenchDatabaseConnection, BenchExtractIfResult, BenchInserter, BenchIterator,
    BenchPopResult, BenchReadTransaction, BenchReader, BenchWriteTransaction, OwnedOutput,
  },
};

const MIN_START: &[u8] = &[0u8];

/// bf-tree 原生引擎（对基对象：garnet BfTree 的 rust 底座）
pub struct BfTreeNativeEngine {
  tree: Arc<BfTree>,
  value_size: usize,
}

impl BfTreeNativeEngine {
  /// 与 hash 引擎同口径：缓冲 4GiB（bf-tree CircularBuffer 预分配），
  /// max_key_len 64 覆盖 bench 24B key（默认 16 不足），leaf page 4KiB 默认
  pub fn open(path: &Path, workload: &Workload) -> Result<Self, String> {
    let dir = path.to_path_buf();
    create_dir_all(&dir).map_err(|e| format!("{e:?}"))?;
    let file = dir.join("bftree.dat");
    // bf-tree Config 字段全 pub(crate)，对外仅 new(file, cb_size) 与
    // new_with_config_file 两入口。key 24B 超默认 cb_max_key_len=16，
    // 缓冲 4GiB 同 hash 引擎口径——运行时生成 TOML 走配置文件入口
    let config_toml = dir.join("bftree.toml");
    write(
      &config_toml,
      format!(
        "cb_size_byte = {}\ncb_max_key_len = 64\nleaf_page_size = 4096\nindex_file_path = {:?}\nbackend_storage = \"disk\"\nsnapshot_version = 0\nuse_snapshot = false\n",
        REDB_CACHE_SIZE,
        file.to_string_lossy()
      ),
    )
    .map_err(|e| e.to_string())?;
    let config = Config::new_with_config_file(&config_toml);
    let tree = BfTree::with_config(config, None).map_err(|e| format!("{e:?}"))?;
    Ok(Self {
      tree: Arc::new(tree),
      value_size: workload.value_size,
    })
  }
}

pub struct BfTreeNativeConnection {
  tree: Arc<BfTree>,
  value_size: usize,
}

pub struct BfTreeNativeWriteTxn {
  tree: Arc<BfTree>,
}

pub struct BfTreeNativeInserter {
  tree: Arc<BfTree>,
}

pub struct BfTreeNativeReader {
  tree: Arc<BfTree>,
  value_size: usize,
}

/// bf-tree ScanIter 的消费缓冲要求：单条 (key, value) 连续写入 out_buffer，
/// 返回 (key_len, value_len)。容量取 max_key_len 64 + value_size 上界。
pub struct BfTreeNativeIter {
  iter: Option<bf_tree::ScanIter<'static, 'static>>,
  tree: Arc<BfTree>,
  start: Vec<u8>,
  out: Vec<u8>,
  done: bool,
}

impl Drop for BfTreeNativeIter {
  fn drop(&mut self) {
    drop(self.iter.take());
  }
}

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

impl BenchDatabase for BfTreeNativeEngine {
  type C<'db> = BfTreeNativeConnection;

  fn db_type_name() -> &'static str {
    "bftree_native"
  }

  fn connect(&self) -> Self::C<'_> {
    BfTreeNativeConnection {
      tree: Arc::clone(&self.tree),
      value_size: self.value_size,
    }
  }

  /// bf-tree 无在线紧缩 API：Drop 时落盘 + 快照承担持久化，compact 段如实 N/A
  fn compact(&mut self) -> bool {
    false
  }
}

impl BenchDatabaseConnection for BfTreeNativeConnection {
  type W<'txn> = BfTreeNativeWriteTxn;
  type R<'txn> = BfTreeNativeReader;

  /// bf-tree 写入即持久（CB 环形缓冲 + 后台落盘），无事务提交语义，恒真
  fn set_sync(&mut self, _sync: bool) -> bool {
    true
  }

  fn write_transaction(&self) -> Self::W<'_> {
    BfTreeNativeWriteTxn {
      tree: Arc::clone(&self.tree),
    }
  }

  fn read_transaction(&self) -> Self::R<'_> {
    BfTreeNativeReader {
      tree: Arc::clone(&self.tree),
      value_size: self.value_size,
    }
  }
}

impl BenchWriteTransaction for BfTreeNativeWriteTxn {
  type W<'txn> = BfTreeNativeInserter;

  fn get_inserter(&mut self) -> Self::W<'_> {
    BfTreeNativeInserter {
      tree: Arc::clone(&self.tree),
    }
  }

  /// bf-tree 写入即持久，commit 无额外语义
  fn commit(self) -> Result<(), ()> {
    Ok(())
  }
}

impl BenchInserter for BfTreeNativeInserter {
  type Output<'out>
    = OwnedOutput
  where
    Self: 'out;
  type ExtractIfIterator<'out, F>
    = NoRangeIter<F>
  where
    Self: 'out,
    F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
    if matches!(self.tree.insert(key, value), LeafInsertResult::Success) {
      Ok(())
    } else {
      Err(())
    }
  }

  fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
    self.tree.delete(key);
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

impl BenchReadTransaction for BfTreeNativeReader {
  type T<'txn> = BfTreeNativeReader;

  fn get_reader(&self) -> Self::T<'_> {
    BfTreeNativeReader {
      tree: Arc::clone(&self.tree),
      value_size: self.value_size,
    }
  }
}

impl BenchReader for BfTreeNativeReader {
  type Output<'out>
    = OwnedOutput
  where
    Self: 'out;
  type Iterator<'out>
    = BfTreeNativeIter
  where
    Self: 'out;

  fn get<'b>(&'b mut self, key: &[u8]) -> Option<Self::Output<'b>> {
    let mut out = vec![0u8; self.value_size];
    match self.tree.read(key, &mut out) {
      LeafReadResult::Found(len) => {
        out.truncate(len as usize);
        Some(OwnedOutput(out))
      }
      _ => None,
    }
  }

  /// bf-tree read 直填调用方缓冲：Found(len) 时 out[..len] 即数据
  /// （&mut [u8] 无 truncate，消费方按返回语义读前 len 字节；
  /// bench 消费环只取 value[0]，与 get 语义一致）
  fn get_into(&mut self, key: &[u8], out: &mut [u8]) -> Option<()> {
    match self.tree.read(key, out) {
      LeafReadResult::Found(_len) => Some(()),
      _ => None,
    }
  }

  fn range_from<'b>(&'b mut self, start: &'b [u8]) -> Self::Iterator<'b> {
    BfTreeNativeIter {
      iter: None,
      tree: Arc::clone(&self.tree),
      start: if start.is_empty() {
        MIN_START.to_vec()
      } else {
        start.to_vec()
      },
      out: vec![0u8; 64 + self.value_size],
      done: false,
    }
  }

  /// bf-tree 无 O(1) len：全表扫描计数（与 hash 引擎 len 扫描口径同公平）
  fn len(&mut self) -> u64 {
    let mut n = 0u64;
    if let Ok(mut it) = self
      .tree
      .scan_with_count(MIN_START, usize::MAX, ScanReturnField::Key)
    {
      let mut buf = [0u8; 64];
      while let Some((klen, _)) = it.next(&mut buf) {
        n += 1;
        let _ = klen;
      }
    }
    n
  }
}

impl BfTreeNativeIter {
  /// 'static 共享引用（同 BfTreeNativeEngine）：Arc 存活保证迭代器安全
  fn tree_ref(&self) -> &'static BfTree {
    unsafe { &*(Arc::as_ptr(&self.tree)) }
  }
}

impl BenchIterator for BfTreeNativeIter {
  type Output<'out>
    = OwnedOutput
  where
    Self: 'out;

  fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
    if self.done {
      return None;
    }
    if self.iter.is_none() {
      self.iter = Some(
        self
          .tree_ref()
          .scan_with_count(&self.start, usize::MAX, ScanReturnField::KeyAndValue)
          .ok()?,
      );
    }
    let iter = self.iter.as_mut().unwrap();
    let (klen, vlen) = iter.next(&mut self.out)?;
    Some((
      OwnedOutput(self.out[..klen].to_vec()),
      OwnedOutput(self.out[klen..klen + vlen].to_vec()),
    ))
  }
}
