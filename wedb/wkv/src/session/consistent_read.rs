//! 一致读会话上下文（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs）
//!
//! 在存储会话的基础读取操作之上包裹一致读协议钩子：
//! - 读取前：调用 `Session.functions.PreSingleKeyConsistentRead(hash)`
//! - 读取后：调用 `Session.functions.PostSingleKeyConsistentReadCallback()`
//! - 严禁写入操作：对标 Tsavorite，写路径一律拦截报错。

use std::{io, thread};

use wdev::Device;

use crate::{
  error::{Error, Result},
  session::StoreSession,
};

/// 一致读会话回调接口（对标 libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs）
pub trait ConsistentReadFunctions: Send + Sync {
  /// 单 key 一致读前置协议（对标 libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs:PreSingleKeyConsistentRead）
  fn pre_single_key_consistent_read(&self, hash: i64);

  /// 单 key 一致读后置协议（对标 libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs:PostSingleKeyConsistentReadCallback）
  fn post_single_key_consistent_read_callback(&self);

  /// 批量键一致读前半协议（对标 libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs:PreBatchKeyConsistentReadCallback）
  /// 默认实现为空操作，保留形参以匹配 ISessionFunctions 接口签名约定
  fn pre_batch_key_consistent_read_callback(&self, _keys: &[&[u8]]) {}

  /// 批量键读后校验（对标 libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs:PostBatchKeyConsistentReadCallback）
  /// 默认实现恒返回 true，保留形参以匹配 ISessionFunctions 接口签名约定
  fn post_batch_key_consistent_read_callback(&self, _key_count: usize) -> bool {
    true
  }
}

impl ConsistentReadFunctions for () {
  fn pre_single_key_consistent_read(&self, _hash: i64) {}
  fn post_single_key_consistent_read_callback(&self) {}
}

/// 一致读会话包装器（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs）
pub struct ConsistentReadContext<'a, D: Device, F: ConsistentReadFunctions + ?Sized> {
  session: &'a StoreSession<D>,
  functions: &'a F,
}

/// 拒绝写入错误文案（对标 Tsavorite ConsistentReadContext 报错常数字符串）
const ERR_WRITES_FORBIDDEN: &str = "Consistent read context does not allow writes!";

/// 批量读键切片小栈数组优化上限
const STACK_KEYS_LIMIT: usize = 32;

impl<'a, D: Device, F: ConsistentReadFunctions + ?Sized> ConsistentReadContext<'a, D, F> {
  /// 创建一致读会话包装器（对标 ConsistentReadContext 构造）
  pub fn new(session: &'a StoreSession<D>, functions: &'a F) -> Self {
    Self { session, functions }
  }

  /// 获取底层会话引用
  #[inline]
  pub fn session(&self) -> &'a StoreSession<D> {
    self.session
  }

  /// 获取一致读回调接口引用
  #[inline]
  pub fn functions(&self) -> &F {
    self.functions
  }

  /// 获取键哈希（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:GetKeyHash）
  #[inline]
  pub fn get_key_hash(&self, key: &[u8]) -> i64 {
    whasher::fast_hash(key) as i64
  }

  /// 同步执行闭包并在前后包裹一致读协议（单键读取/迭代守卫）
  #[inline]
  pub fn with_consistent_read<R>(&self, user_key: &[u8], f: impl FnOnce() -> R) -> R {
    let hash = self.get_key_hash(user_key);
    self.functions.pre_single_key_consistent_read(hash);
    let res = f();
    self.functions.post_single_key_consistent_read_callback();
    res
  }

  /// 一致读取普通字符串键（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:Read）
  #[inline]
  pub async fn read(&self, user_key: &[u8]) -> Result<Option<Vec<u8>>> {
    self.read_with(user_key, |v| v.to_vec()).await
  }

  /// 零拷贝读取普通字符串键（基于借用视图消除内存拷贝，Rust 零拷贝扩展）
  pub async fn read_with<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    let hash = self.get_key_hash(user_key);
    self.functions.pre_single_key_consistent_read(hash);
    let result = self.session.read_with(user_key, f).await;
    self.functions.post_single_key_consistent_read_callback();
    result
  }

  /// 同步内存直读一致性检查快路径（对标 ConsistentReadContext 同步读取）
  #[inline]
  pub fn try_read_sync<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    self.with_consistent_read(user_key, || self.session.try_read_sync(user_key, f))
  }

  /// 在调用方已持纪元保护下执行的同步内存直读快路径（对标 BatchStoreSession::try_read_sync 零 enter 开销）
  #[inline]
  pub fn try_read_sync_unprotected<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    self.with_consistent_read(user_key, || {
      self.session.try_read_sync_unprotected(user_key, f)
    })
  }

  /// 批量一致读（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:ReadWithPrefetch）
  ///
  /// - 栈上借用切片缓存：32 键以内免堆分配收集 `&[&[u8]]`；
  /// - 单块连续载荷缓冲：重试与分发过程消除逐项 `Vec<u8>` 分配，多轮重试容量复用。
  pub async fn read_batch_with<K, Func>(&self, keys: &[K], mut on_item: Func) -> Result<()>
  where
    K: AsRef<[u8]>,
    Func: FnMut(usize, Option<&[u8]>),
  {
    if keys.is_empty() {
      return Ok(());
    }
    let mut stack_keys = [&b""[..]; STACK_KEYS_LIMIT];
    let heap_keys;
    let key_slices: &[&[u8]] = if keys.len() <= STACK_KEYS_LIMIT {
      for (slot, k) in stack_keys.iter_mut().zip(keys.iter()) {
        *slot = k.as_ref();
      }
      &stack_keys[..keys.len()]
    } else {
      heap_keys = keys.iter().map(|k| k.as_ref()).collect::<Vec<_>>();
      &heap_keys
    };

    let mut offsets: Vec<(usize, Option<(usize, usize)>)> = Vec::with_capacity(keys.len());
    let mut payload: Vec<u8> = Vec::new();
    let mut retrying = false;
    loop {
      if retrying {
        thread::yield_now();
      }
      self
        .functions
        .pre_batch_key_consistent_read_callback(key_slices);
      offsets.clear();
      payload.clear();
      self
        .session
        .read_batch_with(keys, |idx, opt| {
          let span = opt.map(|v| {
            let start = payload.len();
            payload.extend_from_slice(v);
            (start, payload.len())
          });
          offsets.push((idx, span));
        })
        .await?;
      if self
        .functions
        .post_batch_key_consistent_read_callback(keys.len())
      {
        break;
      }
      retrying = true;
    }
    for (idx, span) in offsets {
      let val = span.map(|(start, end)| &payload[start..end]);
      on_item(idx, val);
    }
    Ok(())
  }

  /// 禁止写入：对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:Upsert
  #[inline]
  pub fn upsert_forbidden(&self) -> Result<()> {
    Err(Error::Io(io::Error::other(ERR_WRITES_FORBIDDEN)))
  }

  /// 禁止读-改-写：对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:RMW
  #[inline]
  pub fn rmw_forbidden(&self) -> Result<()> {
    Err(Error::Io(io::Error::other(ERR_WRITES_FORBIDDEN)))
  }

  /// 禁止删除：对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:Delete
  #[inline]
  pub fn delete_forbidden(&self) -> Result<()> {
    Err(Error::Io(io::Error::other(ERR_WRITES_FORBIDDEN)))
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicUsize, Ordering},
  };

  use compio::runtime::Runtime;
  use wdev::SegmentedDevice;

  use super::*;
  use crate::{StoreConfig, WedbStore};

  struct MockFunctions {
    pre_count: AtomicUsize,
    post_count: AtomicUsize,
    last_hash: AtomicI64,
    pre_batch_count: AtomicUsize,
    post_batch_count: AtomicUsize,
    retries: AtomicUsize,
  }

  impl ConsistentReadFunctions for MockFunctions {
    fn pre_single_key_consistent_read(&self, hash: i64) {
      self.pre_count.fetch_add(1, Ordering::SeqCst);
      self.last_hash.store(hash, Ordering::SeqCst);
    }

    fn post_single_key_consistent_read_callback(&self) {
      self.post_count.fetch_add(1, Ordering::SeqCst);
    }

    /// 满足 ConsistentReadCallback trait 契约，测试桩仅统计预批次调用次数
    fn pre_batch_key_consistent_read_callback(&self, _keys: &[&[u8]]) {
      self.pre_batch_count.fetch_add(1, Ordering::SeqCst);
    }

    /// 满足 ConsistentReadCallback trait 契约，测试桩仅按计数重试，无需按批次条数判断
    fn post_batch_key_consistent_read_callback(&self, _batch_size: usize) -> bool {
      self.post_batch_count.fetch_add(1, Ordering::SeqCst);
      if self.retries.load(Ordering::SeqCst) > 0 {
        self.retries.fetch_sub(1, Ordering::SeqCst);
        false
      } else {
        true
      }
    }
  }

  #[test]
  fn test_consistent_read_lifecycle() {
    Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("cr.log")).unwrap());
      let config = StoreConfig::new(64, 4096, 64, 0.5).unwrap();
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let session = store.new_session().unwrap();
      let fns = MockFunctions {
        pre_count: AtomicUsize::new(0),
        post_count: AtomicUsize::new(0),
        last_hash: AtomicI64::new(0),
        pre_batch_count: AtomicUsize::new(0),
        post_batch_count: AtomicUsize::new(0),
        retries: AtomicUsize::new(0),
      };

      let ctx = session.consistent_read(&fns);
      assert!(ctx.upsert_forbidden().is_err());
      assert!(ctx.rmw_forbidden().is_err());
      assert!(ctx.delete_forbidden().is_err());

      session.upsert(b"hello", b"world").await.unwrap();
      let val = ctx.read(b"hello").await.unwrap();
      assert_eq!(val.as_deref(), Some(b"world".as_slice()));
      assert_eq!(fns.pre_count.load(Ordering::SeqCst), 1);
      assert_eq!(fns.post_count.load(Ordering::SeqCst), 1);
      assert_eq!(
        fns.last_hash.load(Ordering::SeqCst),
        ctx.get_key_hash(b"hello")
      );

      let sync_val = ctx.try_read_sync(b"hello", |v| v.to_vec()).unwrap();
      assert_eq!(sync_val, Some(Some(b"world".to_vec())));
      assert_eq!(fns.pre_count.load(Ordering::SeqCst), 2);
      assert_eq!(fns.post_count.load(Ordering::SeqCst), 2);

      let batch = session.enter_batch();
      let unprot_val = ctx
        .try_read_sync_unprotected(b"hello", |v| v.to_vec())
        .unwrap();
      assert_eq!(unprot_val, Some(Some(b"world".to_vec())));
      assert_eq!(fns.pre_count.load(Ordering::SeqCst), 3);
      assert_eq!(fns.post_count.load(Ordering::SeqCst), 3);
      drop(batch);

      // 批量预取一致读校验（模拟 1 次重试后成功）
      session.upsert(b"k1", b"v1").await.unwrap();
      session.upsert(b"k2", b"v2").await.unwrap();
      fns.retries.store(1, Ordering::SeqCst);

      let mut collected = Vec::new();
      ctx
        .read_batch_with(&[b"k1".as_slice(), b"k2".as_slice()], |idx, opt| {
          collected.push((idx, opt.map(|v| v.to_vec())));
        })
        .await
        .unwrap();

      assert_eq!(fns.pre_batch_count.load(Ordering::SeqCst), 2);
      assert_eq!(fns.post_batch_count.load(Ordering::SeqCst), 2);
      assert_eq!(collected.len(), 2);
      assert_eq!(collected[0], (0, Some(b"v1".to_vec())));
      assert_eq!(collected[1], (1, Some(b"v2".to_vec())));
    });
  }
}
