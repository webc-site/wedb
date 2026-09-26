//! 一致读会话上下文（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs）
//!
//! - 纯只读会话：静态类型保证无写接口。
//! - 读取前：调用 `Session.functions.PreSingleKeyConsistentRead(hash)`
//! - 读取后：调用 `Session.functions.PostSingleKeyConsistentReadCallback()`

use core::future::Future;

use smallvec::SmallVec;
use wbase::future::yield_now;
use wdev::Device;
use wval::{KeyTag, TaggedKeyBuf};

use crate::{
  error::Result,
  session::{StoreResult, StoreSession},
};

/// 一致读会话回调接口（对标 libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs）
///
/// pre 族超时契约：返回 [`Err`](crate::error::Error::ConsistentReadTimeout) 时
/// 读中止上抛不静默续读（对齐 C# TimeoutException）；post 族保持不抛。
pub trait ConsistentReadFunctions: Send + Sync {
  /// 单 key 一致读前置协议（对标 libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs:PreSingleKeyConsistentRead）
  fn pre_single_key_consistent_read(&self, hash: i64) -> Result<()>;

  /// 单 key 一致读后置协议（对标 libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs:PostSingleKeyConsistentReadCallback）
  fn post_single_key_consistent_read_callback(&self);

  /// 批量键一致读前半协议（对标 libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs:PreBatchKeyConsistentReadCallback）
  /// 默认实现为空操作，保留形参以匹配 ISessionFunctions 接口签名约定
  fn pre_batch_key_consistent_read_callback(&self, _keys: &[&[u8]]) -> Result<()> {
    Ok(())
  }

  /// 批量键读后校验（对标 libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs:PostBatchKeyConsistentReadCallback）
  /// 默认实现恒返回 true，保留形参以匹配 ISessionFunctions 接口签名约定
  fn post_batch_key_consistent_read_callback(&self, _key_count: usize) -> bool {
    true
  }
}

impl ConsistentReadFunctions for () {
  fn pre_single_key_consistent_read(&self, _hash: i64) -> Result<()> {
    Ok(())
  }
  fn post_single_key_consistent_read_callback(&self) {}
}

/// 一致读会话包装器（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs）
pub struct ConsistentReadContext<'a, D: Device, F: ConsistentReadFunctions + ?Sized> {
  pub session: &'a StoreSession<D>,
  pub functions: &'a F,
}

/// 批量读键切片小栈数组优化上限
const STACK_KEYS_LIMIT: usize = 32;

/// 一致读键哈希（whasher 单一哈希域：读侧触发与回放草图**同键同哈希**，对标
/// libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:27-32
/// GetKeyHash 与 :49-57 的 ConsumeSingleKeyConsistentRead——C# 侧 store 键即
/// AOF 条目键，同一字节序列进同一哈希）
///
/// **入参是记录物理键而非用户键**（`[NsVarint][DbVarint][KeyTag][用户键]`）：
/// 本仓 wkv 以物理键做租界 / 类型带外隔离，回放侧草图入账的键是 AOF 条目键
/// （`wnode/src/service.rs` `physical_key` 单编码器产出，`wnode/src/aof/aof_processor.rs`
/// `prepare_key` 以 `GarnetLog::hash(条目键)` 落 `update_virtual_sublog_key_sequence_number`），
/// 读侧唯有同域取哈希，`verify_key_freshness` 的虚拟子日志路由与 key 序列号
/// 草图槽才可能命中写侧所落之处；用户键入参即为跨域错读（读侧命中空槽，
/// 会话序列号永不推进，跨子日志新鲜度约束形同虚设）。
///
/// 会话侧取哈希一律经 [`StoreSession::consistent_read_hash`] 单点，不自行拼接键。
#[inline]
pub fn record_key_hash(record_key: &[u8]) -> i64 {
  whasher::fast_hash_i64(record_key)
}

impl<D: Device> StoreSession<D> {
  /// 一致读触发哈希域单点：本会话 `(vns, vdb)` 前缀 + `tag` + 用户键编码为
  /// 记录物理键后取 [`record_key_hash`]（与回放侧草图入账键同源同域）
  ///
  /// `tag` 即本次读取实际触碰的记录域（String 用户数据 / ObjectEnvelope 对象
  /// 信封 / Meta 分层元记录），AOF 条目按同一标签入账，故两侧必然同键。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:GetKeyHash
  /// （C# 取用户键哈希作一致读序列号；wedb 一致读序列号取记录物理键哈希，
  /// 域含 ns/db 前缀，语义见 doc/zh/db.md 多库隔离）
  #[inline]
  pub fn consistent_read_hash(&self, tag: KeyTag, user_key: &[u8]) -> i64 {
    let prefix = self.session_prefix();
    Self::consistent_read_hash_with_prefix(prefix.as_slice(), tag, user_key)
  }

  /// 显式前缀一致读触发哈希（循环前缀外提内核，语义与
  /// [`Self::consistent_read_hash`] 全等；rust 工程优化无 c# 对应：批量遍历
  /// 单次外提 `session_prefix()` 消除逐键重读 ns/db 原子变量与重算 Varint）
  #[inline]
  pub fn consistent_read_hash_with_prefix(prefix: &[u8], tag: KeyTag, user_key: &[u8]) -> i64 {
    record_key_hash(Self::session_tag_key_with_prefix(prefix, tag, user_key).as_slice())
  }

  /// 一致读批量预检的记录键视图（`pre_batch_key_consistent_read_callback`
  /// 的键域单点：逐项编码为指定标签记录物理键，供回调内部按与回放侧同域的
  /// 哈希取键序列号；对标 C# ConsistentReadContext.cs:121-133 ReadWithPrefetch
  /// 以 store 键本体进 PreBatchKeyConsistentReadCallback）
  #[inline]
  pub fn consistent_read_record_keys<K: AsRef<[u8]>>(
    &self,
    tag: KeyTag,
    user_keys: &[K],
  ) -> Vec<TaggedKeyBuf> {
    let prefix = self.session_prefix();
    let prefix_slice = prefix.as_slice();
    user_keys
      .iter()
      .map(|k| Self::session_tag_key_with_prefix(prefix_slice, tag, k.as_ref()))
      .collect()
  }
}

/// 单键 pre/post 协议序列（触发面单点：ConsistentReadContext 与会话附着态共用；
/// pre 超时上抛中止，post 推进不抛）
#[inline]
pub(crate) fn single_key_around<F: ConsistentReadFunctions + ?Sized, R>(
  fns: &F,
  hash: i64,
  f: impl FnOnce() -> R,
) -> Result<R> {
  fns.pre_single_key_consistent_read(hash)?;
  let res = f();
  fns.post_single_key_consistent_read_callback();
  Ok(res)
}

/// 单键异步 pre/post 协议序列（触发面单点：pre 超时上抛中止，post 推进不抛）
#[inline]
pub(crate) async fn single_key_around_async<F: ConsistentReadFunctions + ?Sized, R, Fut>(
  fns: &F,
  hash: i64,
  fut: Fut,
) -> Result<R>
where
  Fut: Future<Output = Result<R>>,
{
  fns.pre_single_key_consistent_read(hash)?;
  let res = fut.await;
  fns.post_single_key_consistent_read_callback();
  res
}

impl<'a, D: Device, F: ConsistentReadFunctions + ?Sized> ConsistentReadContext<'a, D, F> {
  /// 创建一致读上下文并绑定借用生命周期
  #[inline]
  pub(crate) fn new(session: &'a StoreSession<D>, functions: &'a F) -> Self {
    Self { session, functions }
  }

  /// 同步执行闭包并在前后包裹一致读协议（单键读取/迭代守卫）；pre 超时上抛
  ///
  /// `tag` 为本次读取触碰的记录域，哈希经 [`StoreSession::consistent_read_hash`]
  /// 单点取记录物理键域
  #[inline]
  pub fn with_consistent_read<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce() -> R,
  ) -> Result<R> {
    single_key_around(
      self.functions,
      self.session.consistent_read_hash(tag, user_key),
      f,
    )
  }

  /// 异步执行 Future 并在前后包裹一致读协议；pre 超时上抛
  #[inline]
  pub async fn with_consistent_read_async<R, Fut>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    fut: Fut,
  ) -> Result<R>
  where
    Fut: Future<Output = Result<R>>,
  {
    single_key_around_async(
      self.functions,
      self.session.consistent_read_hash(tag, user_key),
      fut,
    )
    .await
  }

  /// 一致读取普通字符串键（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:Read）
  #[inline]
  pub async fn read(&self, user_key: &[u8]) -> Result<Option<Vec<u8>>> {
    self.read_with(user_key, |v| v.to_vec()).await
  }

  /// 零拷贝读取普通字符串键（基于借用视图消除内存拷贝，Rust 零拷贝扩展）
  #[inline]
  pub async fn read_with<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    self.read_tag_with(user_key, KeyTag::String, f).await
  }

  /// 零拷贝读取指定标签物理键（带标签读内核，TTL 门控按用户键裁决）
  #[inline]
  pub async fn read_tag_with<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    self
      .with_consistent_read_async(user_key, tag, self.session.read_tag_with(user_key, tag, f))
      .await
  }

  /// 同步内存直读指定标签物理键并披露记录物理尺寸（MEMORY USAGE 统计内核，
  /// [`Self::try_read_tag_sync_unprotected`] 的带尺寸对位，三态语义一致）
  #[inline]
  pub fn try_read_tag_sync_with_size<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8], usize) -> R,
  ) -> Result<StoreResult<R>> {
    self.with_consistent_read(user_key, tag, || {
      self.session.try_read_tag_sync_with_size(user_key, tag, f)
    })?
  }

  /// 零拷贝读取指定标签物理键并披露记录物理尺寸（MEMORY USAGE 统计内核，
  /// TTL 门控与 [`Self::read_tag_with`] 一致，读全路径向闭包披露物理分配尺寸）
  #[inline]
  pub async fn read_tag_with_size<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8], usize) -> R,
  ) -> Result<Option<R>> {
    self
      .with_consistent_read_async(
        user_key,
        tag,
        self.session.read_tag_with_size(user_key, tag, f),
      )
      .await
  }

  /// 同步内存直读一致性检查快路径（对标 ConsistentReadContext 同步读取）
  #[inline]
  pub fn try_read_sync<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    self.with_consistent_read(user_key, KeyTag::String, || {
      self.session.try_read_sync(user_key, f)
    })?
  }

  /// 在调用方已持纪元保护下执行的同步内存直读快路径（对标 BatchStoreSession::try_read_sync 零 enter 开销）
  #[inline]
  pub fn try_read_sync_unprotected<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    self.try_read_tag_sync_unprotected(user_key, KeyTag::String, f)
  }

  /// 在调用方已持纪元保护下执行指定标签物理键的同步内存直读快路径（零 enter 开销）
  #[inline]
  pub fn try_read_tag_sync_unprotected<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    self.with_consistent_read(user_key, tag, || {
      self.session.try_read_tag_sync_unprotected(user_key, tag, f)
    })?
  }

  /// 批量一致读（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:ReadWithPrefetch）
  ///
  /// - 预检键域经 [`StoreSession::consistent_read_record_keys`] 换成 String 域
  ///   记录物理键（与回放草图入账同键同哈希，post 校验才取到真实键序列号）；
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
    // 记录物理键持有体（编码体一次到位，借用视图指向本缓冲，重试轮复用）
    let record_keys = self
      .session
      .consistent_read_record_keys(KeyTag::String, keys);
    let key_slices: SmallVec<[&[u8]; STACK_KEYS_LIMIT]> =
      record_keys.iter().map(|k| k.as_slice()).collect();

    let mut offsets: Vec<(usize, Option<(usize, usize)>)> = Vec::with_capacity(keys.len());
    let mut payload: Vec<u8> = Vec::new();
    let mut retrying = false;
    loop {
      if retrying {
        yield_now().await;
      }
      self
        .functions
        .pre_batch_key_consistent_read_callback(&key_slices)?;
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
}
