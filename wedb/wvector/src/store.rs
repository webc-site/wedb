//! 存储回调与命名空间（对标 diskann-garnet 的 garnet.rs + diskann-garnet/VectorManager.Callbacks.cs）
//!
//! C# 侧 DiskANNService 经 C 函数指针（read/write/delete/readModifyWrite/filter）回调
//! `VectorManager.Callbacks.cs`，把向量/邻接表/量化向量/属性/元数据/ID 映射以
//! `(context | 项类型)` 命名空间持久化到 Tsavorite（完整物理键 =
//! `[命名空间字节][键字节]`，命名空间 ≤127 单字节、否则 4B LE）。
//!
//! Rust 侧无 FFI 边界：[`StoreCallbacks`] trait 即 C# 回调注入面的直接等价物，
//! 宿主（wnode）面向存储引擎实现该 trait；[`Callbacks`] 持有实现句柄，
//! 以与 diskann-garnet `garnet.rs` 同形的便捷方法（read_single_iid/write_iid/rmw_wid
//! 等）服务 [`crate::provider::WedbProvider`]。
//!
//! # 异步契约
//!
//! C# 回调栈内 `CompletePending(wait: true)` 同步收割是安全的（.NET 线程池
//! 完成回调兜底）；compio 一线程一运行时，运行时上下文内嵌套 block_on 会
//! 重入调度器（compio-executor 断言），故除下述两个例外，回调全部异步化，
//! 实现方在 async 上下文内 `.await` 闭环冷区读写：
//!
//! * [`StoreCallbacks::read`] 保持同步——webc-diskann 的
//!   `DataProvider::to_internal_id/to_external_id` 是同步 trait 方法（图
//!   inplace_delete 在 async 块内同步调用，第三方契约不可改），ID 映射读取
//!   唯经此口；实现的冷区分支以 VM 同步绑定同型的内联收割口承接
//!   （wbase::future::inline_wait，不经 `Runtime::block_on` 入口），严禁
//!   `blocking_wait`；
//! * [`StoreCallbacks::log`] 本身无存储 I/O，恒同步。
//!
//! 异步方法以 `fn -> impl Future + Send`（RPITIT）而非 `async fn`（AFIT）
//! 声明：调用面（diskann glue 的 SearchAccessor/PruneAccessor/SetElement）
//! 要求 Send future，AFIT 语法无法在 trait 级标注该约束。实现方以 `async fn`
//! 满足签名即可。

use std::{
  borrow::Borrow,
  future::Future,
  mem,
  ops::Deref,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use bytemuck::Pod;
use webc_diskann::provider::ExecutionContext;

/// 内部项类型命名空间位（对标 diskann-garnet/DiskANNService.cs 顶部的常量）。
pub mod term {
  /// 完整向量。
  pub const FULL_VECTOR: u64 = 0;
  /// 邻接表。
  pub const NEIGHBOR_LIST: u64 = 1;
  /// 量化后向量。
  pub const QUANTIZED_VECTOR: u64 = 2;
  /// 属性。
  pub const ATTRIBUTES: u64 = 3;
  /// 元数据。
  pub const METADATA: u64 = 4;
  /// 内部 id 映射（外部 id → 内部 id）。
  pub const INTERNAL_ID_MAP: u64 = 5;
  /// 外部 id 映射（内部 id → 外部 id）。
  pub const EXTERNAL_ID_MAP: u64 = 6;
}

/// 从 Context 提取项类型位的位掩码（项类型最大值 6，需 3 位）。
pub const TERM_BITMASK: u64 = (1 << 3) - 1;

/// 单字节命名空间上限（对齐 Tsavorite RecordNamespace.MaximumSingleByteNamespaceValue，
/// ExtendedNamespaceIndicatorBit = 7）。
pub const MAX_SINGLE_BYTE_NAMESPACE: u64 = (1 << 7) - 1;

/// 内部项类型（garnet.rs Term 枚举的等价承接；经 [`Context::term`] 或入 context 低位）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum Term {
  Vector = term::FULL_VECTOR,
  Neighbors = term::NEIGHBOR_LIST,
  Quantized = term::QUANTIZED_VECTOR,
  Attributes = term::ATTRIBUTES,
  Metadata = term::METADATA,
  IntMap = term::INTERNAL_ID_MAP,
  ExtMap = term::EXTERNAL_ID_MAP,
}

/// 命名空间字节数（context ≤127 单字节，否则 4B LE；
/// 对齐 C# VectorManager.ContextMetadata.cs:StoreContextInNamespace）。
#[inline]
pub fn namespace_len(context: u64) -> usize {
  if context <= MAX_SINGLE_BYTE_NAMESPACE {
    1
  } else {
    4
  }
}

/// 提取命名空间字节及长度（常数级、无堆分配）。
#[inline]
pub fn namespace_bytes(context: u64) -> (usize, [u8; 4]) {
  if context <= MAX_SINGLE_BYTE_NAMESPACE {
    (1, [context as u8, 0, 0, 0])
  } else {
    (4, (context as u32).to_le_bytes())
  }
}

/// `[4B LE 长度][载荷]` 键流切片迭代器（零分配，单一实现；
/// 对标 C# `ReadOnlySpan<PinnedSpanByte>` 的 int 长度头）。
///
/// 长度按 u32 LE 解读：载荷越界（截断/损坏流）即终止迭代，不 panic、不回绕。
#[derive(Debug, Clone, Copy)]
pub struct LengthPrefixedIter<'a> {
  rest: &'a [u8],
}

impl<'a> LengthPrefixedIter<'a> {
  /// 从键流字节创建零拷贝迭代器。
  #[inline]
  pub const fn new(bytes: &'a [u8]) -> Self {
    Self { rest: bytes }
  }
}

impl<'a> Iterator for LengthPrefixedIter<'a> {
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    let (len_bytes, payload) = self.rest.split_first_chunk::<4>()?;
    let len = u32::from_le_bytes(*len_bytes) as usize;
    if len > payload.len() {
      return None;
    }
    let (item, tail) = payload.split_at(len);
    self.rest = tail;
    Some(item)
  }
}

/// `[4B LE 长度][载荷]` 键流切片收集（按序收集全部载荷段）。
#[inline]
pub fn unpack_length_prefixed(bytes: &[u8]) -> Vec<&[u8]> {
  LengthPrefixedIter::new(bytes).collect()
}

/// 存储回调注入面（C# VectorManager.Callbacks.cs 非托管回调的直接等价物）。
///
/// 所有键均为完整物理键：`[命名空间字节][键字节]`，命名空间由 `context` 决定。
/// 单次 read/write/delete/rmw 无跨键原子协议（C# 侧由 Tsavorite 单次会话操作
/// 保证）：同键原子的保证责任在各实现——生产实现
/// `wnode::WedbVectorStoreCallbacks` 以同键条带互斥锁把 rmw 的
/// 「读旧 → 算新 → 写回」与 direct write 收进互斥窗口（对标 Tsavorite 桶独占
/// 瞬时锁），自研实现须同等保证同键并发原子性，否则图邻接追加与 FSM 位图
/// 标记必丢更新。
pub trait StoreCallbacks: Send + Sync + 'static {
  /// 批量读（对标 ReadCallbackUnmanaged + VectorReadBatch 的批量语义）。
  ///
  /// `keys` 为 `[len: u32 LE][键字节]` 对串（len 恒为键字节数）；
  /// 实现按 `namespace_len(context)` 前缀构造完整物理键后批量读取，
  /// 命中的键以 `(对下标, 值字节)` 回调 `f`。`length_hint` 为单值尺寸预估。
  ///
  /// 返回 false 表示存储读失败（冷区收割 Err，对标 C# CompletePending 失败态）：
  /// 本批交付不完整，调用方须把本次检索报错，禁止静默吞成半截结果。
  ///
  /// `f: Send`：闭包被 async future 持有跨挂起点（实现内冷批收割期间回调），
  /// 调用面的 Send future 契约（diskann glue）要求其可跨线程。
  fn read_multi<F>(
    &self,
    context: u64,
    keys: &[u8],
    length_hint: usize,
    f: F,
  ) -> impl Future<Output = bool> + Send
  where
    F: FnMut(u32, &[u8]) + Send;

  /// 单键读，值尺寸未知（对标 ReadSizeUnknown；实现物化后回调 `f`，缺失返回 false）。
  ///
  /// webc-diskann 的 `DataProvider::to_internal_id/to_external_id` 已随上游
  /// 契约 async 化（0.59.0-webc.5），ID 映射（IntMap/ExtMap）读取经本口的
  /// 异步臂收割，冷区分支不再需要内联收割。
  fn read<F>(&self, context: u64, key: &[u8], f: F) -> impl Future<Output = bool> + Send
  where
    F: FnMut(&[u8]) + Send;

  /// 写入（对标 WriteCallbackUnmanaged 的 Upsert 语义）。
  fn write(&self, context: u64, key: &[u8], value: &[u8]) -> impl Future<Output = bool> + Send;

  /// 删除（对标 DeleteCallbackUnmanaged；返回是否命中）。
  fn delete(&self, context: u64, key: &[u8]) -> impl Future<Output = bool> + Send;

  /// 读改写（对标 ReadModifyWriteCallbackUnmanaged 的 RMW 语义）。
  ///
  /// 记录缺失时以零初始化的 `write_len` 字节传入 `f`；`f` 不得 panic。
  ///
  /// 判定矩阵（同判据 `input.WriteDesiredSize != 0`，`write_len == 0` 判假）：
  ///   * 缺失键：NeedInitialUpdate 判假 → NOTFOUND，不建空值幽灵记录；
  ///   * 已存在键：NeedCopyUpdate 判假 → SUCCESS，不进 CopyUpdater、无冗余回写。
  ///
  /// 两种应答 `IsCompletedSuccessfully` 皆为真，故短路返回恒 true 且不回调 `f`。
  ///
  /// `f: Send`：闭包被 async future 持有跨挂起点（实现内读旧 → 改 → 写回
  /// 窗口跨冷读 await），调用面的 Send future 契约要求其可跨线程。
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:NeedCopyUpdate
  /// 在 garnet 中的相对路径:libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:NeedInitialUpdate
  fn rmw<F>(
    &self,
    context: u64,
    key: &[u8],
    write_len: usize,
    f: F,
  ) -> impl Future<Output = bool> + Send
  where
    F: FnMut(&mut [u8]) + Send;

  /// 内联过滤回调（对标 FilterCallbackUnmanaged → EvaluateCandidateFilter）。
  ///
  /// 按 internal_id 解析元素并求值编译后的过滤表达式；
  /// 无属性/元素缺失时返回 false（对齐 C# 缺失即排除语义）。
  fn filter(&self, context: u64, internal_id: u32) -> impl Future<Output = bool> + Send;

  /// 全量扫描并物理清除指定上下文基址的全部元素记录（drop 清扫链存储端承接）。
  ///
  /// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.Cleanup.cs:RunCleanupTaskAsync
  /// 的全日志扫描删除段（PostDropCleanupFunctions.Reader：命名空间按
  /// `ns & ~(ContextStep - 1)` 配对基址后经 vector context Delete 逐记录物理
  /// 删除）——C# 清理任务直接持有 StorageSession；rust 宿主存储会话封在回调
  /// 实现内，故以本回调下发「按上下文基址清扫物理记录」（context 为 8 步长
  /// 基址，覆盖其全部项类型子域记录）。物理墓碑语义，空间交后台 compaction 回收。
  ///
  /// 返回 false 表示清扫失败（I/O 错误）：调用方须保持上下文隔离（清理中标记）
  /// 待重扫，禁止照常归还上下文号（C# 清理循环异常即留标记重试，同语义）。
  fn purge_context(&self, context: u64) -> impl Future<Output = bool> + Send;

  /// 日志通道（按 context 项类型位圈定消息归属域）。
  fn log(&self, context: u64, msg: &str);
}

impl<S: StoreCallbacks> StoreCallbacks for Arc<S> {
  #[inline]
  fn read_multi<F>(
    &self,
    context: u64,
    keys: &[u8],
    length_hint: usize,
    f: F,
  ) -> impl Future<Output = bool> + Send
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    (**self).read_multi(context, keys, length_hint, f)
  }

  #[inline]
  fn read<F>(&self, context: u64, key: &[u8], f: F) -> impl Future<Output = bool> + Send
  where
    F: FnMut(&[u8]) + Send,
  {
    (**self).read(context, key, f)
  }

  #[inline]
  fn write(&self, context: u64, key: &[u8], value: &[u8]) -> impl Future<Output = bool> + Send {
    (**self).write(context, key, value)
  }

  #[inline]
  fn delete(&self, context: u64, key: &[u8]) -> impl Future<Output = bool> + Send {
    (**self).delete(context, key)
  }

  #[inline]
  fn rmw<F>(
    &self,
    context: u64,
    key: &[u8],
    write_len: usize,
    f: F,
  ) -> impl Future<Output = bool> + Send
  where
    F: FnMut(&mut [u8]) + Send,
  {
    (**self).rmw(context, key, write_len, f)
  }

  #[inline]
  fn filter(&self, context: u64, internal_id: u32) -> impl Future<Output = bool> + Send {
    (**self).filter(context, internal_id)
  }

  #[inline]
  fn purge_context(&self, context: u64) -> impl Future<Output = bool> + Send {
    (**self).purge_context(context)
  }

  #[inline]
  fn log(&self, context: u64, msg: &str) {
    (**self).log(context, msg);
  }
}

/// Provider 侧的存储回调句柄（garnet.rs Callbacks 的等价承接：
/// 静态分派持有存储实现，宿主在装配期一次注入）。
pub struct Callbacks<S: StoreCallbacks> {
  store: Arc<S>,
}

impl<S: StoreCallbacks> Clone for Callbacks<S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      store: Arc::clone(&self.store),
    }
  }
}

impl<S: StoreCallbacks> From<Arc<S>> for Callbacks<S> {
  #[inline]
  fn from(store: Arc<S>) -> Self {
    Self::new(store)
  }
}

impl<S: StoreCallbacks> Callbacks<S> {
  /// 以宿主提供的存储回调实现构造。
  pub fn new(store: Arc<S>) -> Self {
    Self { store }
  }

  /// 获取底层的存储回调引用。
  pub fn store(&self) -> &S {
    &self.store
  }

  /// 宽 id 项是否存在。
  pub async fn exists_wid(&self, ctx: &Context, key: u64, length_hint: usize) -> bool {
    self.read_bool(ctx, &key.to_le_bytes(), length_hint).await
  }

  /// 读单个内部 id 项（键字节 = id 小端 4B）。
  pub async fn read_single_iid<D: Pod>(&self, ctx: &Context, id: u32, value: &mut [D]) -> bool {
    self
      .read_single_raw(
        ctx,
        &id.to_le_bytes(),
        bytemuck::must_cast_slice_mut::<D, u8>(value),
      )
      .await
  }

  /// 读单个宽 id 项（键字节 = key 小端 8B）。
  pub async fn read_single_wid<D: Pod>(&self, ctx: &Context, key: u64, value: &mut [D]) -> bool {
    self
      .read_single_raw(
        ctx,
        &key.to_le_bytes(),
        bytemuck::must_cast_slice_mut::<D, u8>(value),
      )
      .await
  }

  /// 读单个外部 id 项（键字节 = 元素 id 原始字节）。
  pub async fn read_single_eid<D: Pod>(
    &self,
    ctx: &Context,
    id: &VectorSetId,
    value: &mut [D],
  ) -> bool {
    self
      .read_single_raw(ctx, id, bytemuck::must_cast_slice_mut::<D, u8>(value))
      .await
  }

  /// 读单个项，值尺寸未知，物化为 `Vec<D>`（量化状态 / 外部 id 映射读取通道）。
  pub async fn read_varsize_iid<D: Pod + Send>(&self, ctx: &Context, id: u32) -> Option<Vec<D>> {
    const { assert!(align_of::<D>() <= 8, "存储层仅保证 8 字节对齐",) }
    let mut result = None;
    self
      .read_bool_raw(ctx, &id.to_le_bytes(), |data| {
        result = match bytemuck::try_cast_slice::<u8, D>(data) {
          Ok(s) => Some(s.to_vec()),
          Err(_) => {
            let count = if mem::size_of::<D>() > 0 {
              data.len() / mem::size_of::<D>()
            } else {
              0
            };
            let valid_bytes = count * mem::size_of::<D>();
            let mut vec = vec![bytemuck::Zeroable::zeroed(); count];
            bytemuck::cast_slice_mut::<D, u8>(&mut vec).copy_from_slice(&data[..valid_bytes]);
            Some(vec)
          }
        };
      })
      .await;
    result
  }

  /// 批量读内部 id 项：`ids` 为 `[4, I1, 4, I2, ...]` 长度前缀对串
  /// （对标 garnet.rs read_multi_lpiid；下标按对计数回调）。
  ///
  /// 返回 false 表示存储读失败（语义同 [`StoreCallbacks::read_multi`]），
  /// 调用方须把本次检索报错，禁止静默吞成半截结果。
  pub async fn read_multi_lpiid<F>(
    &self,
    ctx: &Context,
    ids: &[u32],
    length_hint: usize,
    f: F,
  ) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    if ids.is_empty() {
      return true;
    }
    self
      .store
      .read_multi(
        ctx.inner,
        bytemuck::must_cast_slice::<u32, u8>(ids),
        length_hint,
        f,
      )
      .await
  }

  /// 写内部 id 项。
  pub fn write_iid<D: Pod>(
    &self,
    ctx: &Context,
    id: u32,
    value: &[D],
  ) -> impl Future<Output = bool> + Send {
    let value = bytemuck::must_cast_slice::<D, u8>(value);
    async move { self.write_raw(ctx, &id.to_le_bytes(), value).await }
  }

  /// 写外部 id 项。
  pub fn write_eid<D: Pod>(
    &self,
    ctx: &Context,
    id: &VectorSetId,
    value: &[D],
  ) -> impl Future<Output = bool> + Send {
    let value = bytemuck::must_cast_slice::<D, u8>(value);
    async move { self.write_raw(ctx, id, value).await }
  }

  /// 删除内部 id 项。
  pub async fn delete_iid(&self, ctx: &Context, id: u32) -> bool {
    self.store.delete(ctx.inner, &id.to_le_bytes()).await
  }

  /// 删除外部 id 项。
  pub async fn delete_eid(&self, ctx: &Context, id: &VectorSetId) -> bool {
    self.store.delete(ctx.inner, id).await
  }

  /// 读改写内部 id 项（记录缺失时零初始化 `write_len` 字节传入 `f`）。
  ///
  /// `write_len == 0` 纯读短路的判定矩阵与应答语义同 [`StoreCallbacks::rmw`]。
  pub fn rmw_iid<F, T>(
    &self,
    ctx: &Context,
    id: u32,
    write_len: usize,
    mut f: F,
  ) -> impl Future<Output = bool> + Send
  where
    F: FnMut(&mut [T]) + Send,
    T: Pod,
  {
    const { assert!(align_of::<T>() <= 8, "存储层仅保证 8 字节对齐",) }
    async move {
      self
        .store
        .rmw(
          ctx.inner,
          &id.to_le_bytes(),
          write_len,
          |data: &mut [u8]| f(bytemuck::cast_slice_mut::<u8, T>(data)),
        )
        .await
    }
  }

  /// 读改写宽 id 项。
  ///
  /// `write_len == 0` 纯读短路的判定矩阵与应答语义同 [`StoreCallbacks::rmw`]。
  pub fn rmw_wid<F, T>(
    &self,
    ctx: &Context,
    key: u64,
    write_len: usize,
    mut f: F,
  ) -> impl Future<Output = bool> + Send
  where
    F: FnMut(&mut [T]) + Send,
    T: Pod,
  {
    const { assert!(align_of::<T>() <= 8, "存储层仅保证 8 字节对齐",) }
    async move {
      self
        .store
        .rmw(
          ctx.inner,
          &key.to_le_bytes(),
          write_len,
          |data: &mut [u8]| f(bytemuck::cast_slice_mut::<u8, T>(data)),
        )
        .await
    }
  }

  /// 内联过滤回调（按 internal_id 求值编译后的过滤表达式）。
  pub async fn matches_filter(&self, ctx: &Context, internal_id: u32) -> bool {
    self.store.filter(ctx.inner, internal_id).await
  }

  /// drop 清扫：按上下文基址物理清除全部元素记录。
  pub async fn purge_context(&self, ctx: &Context) -> bool {
    self.store.purge_context(ctx.inner).await
  }

  /// 日志通道。
  pub fn log(&self, ctx: &Context, msg: &str) {
    self.store.log(ctx.inner, msg);
  }

  async fn read_bool(&self, ctx: &Context, key: &[u8], _length_hint: usize) -> bool {
    let mut called = false;
    self
      .store
      .read(ctx.inner, key, |_: &[u8]| called = true)
      .await;
    called
  }

  async fn read_bool_raw<F>(&self, ctx: &Context, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    self.store.read(ctx.inner, key, f).await
  }

  async fn read_single_raw(&self, ctx: &Context, key: &[u8], value: &mut [u8]) -> bool {
    let mut found = false;
    let read_ok = self
      .store
      .read(ctx.inner, key, |data: &[u8]| {
        if data.len() == value.len() {
          found = true;
          value.copy_from_slice(data);
        }
      })
      .await;
    read_ok && found
  }

  async fn write_raw(&self, ctx: &Context, key: &[u8], value: &[u8]) -> bool {
    self.store.write(ctx.inner, key, value).await
  }
}

/// Provider 执行上下文（garnet.rs Context 的等价承接）。
///
/// `inner` 承载 `(集合上下文 | 项类型)` 组合命名空间，宿主回调据此定位存储键；
/// `quantizer_ready` 为插入侧通知搜索侧"可开始量化训练"的单向信号。
#[derive(Clone, Debug)]
pub struct Context {
  inner: u64,
  quantizer_ready: Arc<AtomicBool>,
}

impl Context {
  /// 以集合上下文构造（低 3 位保留给项类型，须为 CONTEXT_STEP 整数倍）。
  pub fn new(inner: u64) -> Self {
    Self {
      inner,
      quantizer_ready: Arc::new(AtomicBool::new(false)),
    }
  }

  /// 组合项类型命名空间（`inner | term`，低 3 位承载项类型）。
  pub fn term(&self, kind: Term) -> Self {
    Self {
      inner: self.inner | (kind as u64 & TERM_BITMASK),
      quantizer_ready: Arc::clone(&self.quantizer_ready),
    }
  }

  /// 存储上下文组合基址（含项类型位；日志留痕定位归属域用）。
  #[inline]
  pub(crate) fn inner(&self) -> u64 {
    self.inner
  }

  /// 量化训练就绪信号是否已置位。
  pub fn quantizer_ready(&self) -> bool {
    self.quantizer_ready.load(Ordering::Acquire)
  }

  /// 置位量化训练就绪信号。
  pub fn set_quantizer_ready(&self) {
    self.quantizer_ready.store(true, Ordering::Release);
  }
}

impl ExecutionContext for Context {}

/// 向量集合元素外部 id（garnet.rs GarnetId 的等价承接）。
///
/// 元素 id 为变长字节串（RESP 侧 VADD 的 arg1）；
/// C# 侧因回调约定前置 4 字节垫位，Rust 侧键组装在 [`Callbacks`] 内完成，
/// 本类型即纯净的 id 字节串（零拷贝 Deref 到 `&[u8]`）。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct VectorSetId(Box<[u8]>);

impl VectorSetId {
  /// 前置键字节视图（与键组装约定一致，直接透出原始字节）。
  pub fn as_key_bytes(&self) -> &[u8] {
    &self.0
  }
}

impl From<&[u8]> for VectorSetId {
  fn from(value: &[u8]) -> Self {
    Self(value.into())
  }
}

impl From<Vec<u8>> for VectorSetId {
  fn from(value: Vec<u8>) -> Self {
    Self(value.into())
  }
}

impl From<Box<[u8]>> for VectorSetId {
  #[inline]
  fn from(value: Box<[u8]>) -> Self {
    Self(value)
  }
}

impl Borrow<[u8]> for VectorSetId {
  #[inline]
  fn borrow(&self) -> &[u8] {
    &self.0
  }
}

impl AsRef<[u8]> for VectorSetId {
  #[inline]
  fn as_ref(&self) -> &[u8] {
    &self.0
  }
}

impl Deref for VectorSetId {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn namespace_rules_match_csharp() {
    // ≤127 单字节，否则 4B LE（生产键编码单点 namespace_bytes）
    assert_eq!(namespace_len(1), 1);
    assert_eq!(namespace_len(127), 1);
    assert_eq!(namespace_len(128), 4);
    assert_eq!(namespace_len(0x1_0000_0000), 4);

    let (len, buf) = namespace_bytes(8);
    assert_eq!((len, buf[0]), (1, 8));

    let (len, buf) = namespace_bytes(0x1234_5678);
    assert_eq!((len, buf), (4, 0x1234_5678u32.to_le_bytes()));
  }

  #[test]
  fn term_or_masks_into_context() {
    let ctx = Context::new(8);
    assert_eq!(ctx.term(Term::Vector).inner, 8);
    assert_eq!(ctx.term(Term::ExtMap).inner, 8 | 6);
    assert_eq!(TERM_BITMASK, 7);
  }
}
