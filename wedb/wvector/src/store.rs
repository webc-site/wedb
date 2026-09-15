//! 存储回调与命名空间（对标 diskann-garnet 的 garnet.rs + diskann-garnet/VectorManager.Callbacks.cs）
//!
//! C# 侧 DiskANNService 经 C 函数指针（read/write/delete/readModifyWrite/filter）回调
//! `VectorManager.Callbacks.cs`，把向量/邻接表/量化向量/属性/元数据/ID 映射以
//! `(context | 项类型)` 命名空间持久化到 Tsavorite（完整物理键 =
//! `[命名空间字节][键字节]`，命名空间 ≤127 单字节、否则 4B LE）。
//!
//! Rust 侧无 FFI 边界：[`StoreCallbacks`] trait 即 C# 回调注入面的直接等价物，
//! 宿主（wnode）面向存储引擎实现该 trait；[`Callbacks`] 持有 trait 对象，
//! 以与 diskann-garnet `garnet.rs` 同形的便捷方法（read_single_iid/write_iid/rmw_wid
//! 等）服务 [`crate::provider::WedbProvider`]。

use std::{
  borrow::Borrow,
  mem,
  ops::Deref,
  ptr,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use bytemuck::Pod;
use diskann::provider::ExecutionContext;
use smallvec::SmallVec;

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

/// 写命名空间字节（`out` 至少 [`namespace_len`] 字节）。
#[inline]
pub fn write_namespace(context: u64, out: &mut [u8]) {
  if context <= MAX_SINGLE_BYTE_NAMESPACE {
    out[0] = context as u8;
  } else {
    out[..4].copy_from_slice(&(context as u32).to_le_bytes());
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

/// 构造统一格式物理键：`[命名空间字节][键字节]`（栈上预分配 64 字节，超长回退堆分配）。
#[inline]
pub fn make_physical_key(context: u64, key: &[u8]) -> SmallVec<[u8; 64]> {
  let (ns_len, ns_buf) = namespace_bytes(context);
  let mut out = SmallVec::with_capacity(ns_len + key.len());
  out.extend_from_slice(&ns_buf[..ns_len]);
  out.extend_from_slice(key);
  out
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
/// 单次 read/write/delete/rmw 无跨键原子协议：生产实现为 read+write 两步，
/// 在 compio 单线程串行执行域内调用方不交错，等效原子（C# 侧由 Tsavorite
/// 单次会话操作保证）；勿在多线程并发共享同一回调实例时假设原子性。
pub trait StoreCallbacks: Send + Sync + 'static {
  /// 批量读（对标 ReadCallbackUnmanaged + VectorReadBatch 的批量语义）。
  ///
  /// `keys` 为 `[len: u32 LE][键字节]` 对串（len 恒为键字节数）；
  /// 实现按 `namespace_len(context)` 前缀构造完整物理键后批量读取，
  /// 命中的键以 `(对下标, 值字节)` 回调 `f`。`length_hint` 为单值尺寸预估。
  fn read_multi<F>(&self, context: u64, keys: &[u8], length_hint: usize, f: F)
  where
    F: FnMut(u32, &[u8]);

  /// 单键读，值尺寸未知（对标 ReadSizeUnknown；实现物化后回调 `f`，缺失返回 false）。
  fn read<F>(&self, context: u64, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]);

  /// 写入（对标 WriteCallbackUnmanaged 的 Upsert 语义）。
  fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool;

  /// 删除（对标 DeleteCallbackUnmanaged；返回是否命中）。
  fn delete(&self, context: u64, key: &[u8]) -> bool;

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
  /// 在 garnet 中的相对路径:libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:NeedCopyUpdate
  /// 在 garnet 中的相对路径:libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:NeedInitialUpdate
  fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]);

  /// 内联过滤回调（对标 FilterCallbackUnmanaged → EvaluateCandidateFilter）。
  ///
  /// 按 internal_id 解析元素并求值编译后的过滤表达式；
  /// 无属性/元素缺失时返回 false（对齐 C# 缺失即排除语义）。
  fn filter(&self, context: u64, internal_id: u32) -> bool;

  /// 日志通道（按 context 项类型位圈定消息归属域）。
  fn log(&self, context: u64, msg: &str);
}

impl<S: StoreCallbacks> StoreCallbacks for Arc<S> {
  #[inline]
  fn read_multi<F>(&self, context: u64, keys: &[u8], length_hint: usize, f: F)
  where
    F: FnMut(u32, &[u8]),
  {
    (**self).read_multi(context, keys, length_hint, f);
  }

  #[inline]
  fn read<F>(&self, context: u64, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]),
  {
    (**self).read(context, key, f)
  }

  #[inline]
  fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    (**self).write(context, key, value)
  }

  #[inline]
  fn delete(&self, context: u64, key: &[u8]) -> bool {
    (**self).delete(context, key)
  }

  #[inline]
  fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]),
  {
    (**self).rmw(context, key, write_len, f)
  }

  #[inline]
  fn filter(&self, context: u64, internal_id: u32) -> bool {
    (**self).filter(context, internal_id)
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

  /// 内部 id 项是否存在（存在性探测：读取但忽略值）。
  pub fn exists_iid(&self, ctx: &Context, id: u32, length_hint: usize) -> bool {
    let key = [4u32, id];
    self.read_multi_bool(ctx, bytemuck::bytes_of(&key), length_hint)
  }

  /// 宽 id 项是否存在。
  pub fn exists_wid(&self, ctx: &Context, key: u64, length_hint: usize) -> bool {
    self.read_bool(ctx, &key.to_le_bytes(), length_hint)
  }

  /// 读单个内部 id 项（键字节 = id 小端 4B）。
  pub fn read_single_iid<D: Pod>(&self, ctx: &Context, id: u32, value: &mut [D]) -> bool {
    self.read_single_raw(
      ctx,
      &id.to_le_bytes(),
      bytemuck::must_cast_slice_mut::<D, u8>(value),
    )
  }

  /// 读单个宽 id 项（键字节 = key 小端 8B）。
  pub fn read_single_wid<D: Pod>(&self, ctx: &Context, key: u64, value: &mut [D]) -> bool {
    self.read_single_raw(
      ctx,
      &key.to_le_bytes(),
      bytemuck::must_cast_slice_mut::<D, u8>(value),
    )
  }

  /// 读单个外部 id 项（键字节 = 元素 id 原始字节）。
  pub fn read_single_eid<D: Pod>(&self, ctx: &Context, id: &VectorSetId, value: &mut [D]) -> bool {
    self.read_single_raw(ctx, id, bytemuck::must_cast_slice_mut::<D, u8>(value))
  }

  /// 读单个项，值尺寸未知，物化为 `Vec<D>`（量化状态 / 外部 id 映射读取通道）。
  pub fn read_varsize_iid<D: Pod>(&self, ctx: &Context, id: u32) -> Option<Vec<D>> {
    const { assert!(align_of::<D>() <= 8, "存储层仅保证 8 字节对齐",) }
    let mut result = None;
    self.read_bool_raw(ctx, &id.to_le_bytes(), |data| {
      result = match bytemuck::try_cast_slice::<u8, D>(data) {
        Ok(s) => Some(s.to_vec()),
        Err(_) => {
          let count = data.len() / mem::size_of::<D>();
          let mut vec = Vec::<D>::with_capacity(count);
          unsafe {
            ptr::copy_nonoverlapping(data.as_ptr(), vec.as_mut_ptr() as *mut u8, data.len());
            vec.set_len(count);
          }
          Some(vec)
        }
      };
    });
    result
  }

  /// 读单个项原始字节（变长属性/元数据读取专用快速通道）。
  pub fn read_varsize_bytes(&self, ctx: &Context, id: u32) -> Option<Vec<u8>> {
    let mut result = None;
    self.read_bool_raw(ctx, &id.to_le_bytes(), |data| {
      result = Some(data.to_vec());
    });
    result
  }

  /// 读变长外部 id（直接物化为 VectorSetId，避免冗余包装与容量闲置）。
  pub fn read_varsize_id(&self, ctx: &Context, id: u32) -> Option<VectorSetId> {
    let mut result = None;
    self.read_bool_raw(ctx, &id.to_le_bytes(), |data| {
      result = Some(VectorSetId::from(data));
    });
    result
  }

  /// 批量读内部 id 项：`ids` 为 `[4, I1, 4, I2, ...]` 长度前缀对串
  /// （对标 garnet.rs read_multi_lpiid；下标按对计数回调）。
  pub fn read_multi_lpiid<F>(&self, ctx: &Context, ids: &[u32], length_hint: usize, f: F)
  where
    F: FnMut(u32, &[u8]),
  {
    if ids.is_empty() {
      return;
    }
    self.store.read_multi(
      ctx.inner,
      bytemuck::must_cast_slice::<u32, u8>(ids),
      length_hint,
      f,
    );
  }

  /// 写内部 id 项。
  pub fn write_iid<D: Pod>(&self, ctx: &Context, id: u32, value: &[D]) -> bool {
    self.write_raw(
      ctx,
      &id.to_le_bytes(),
      bytemuck::must_cast_slice::<D, u8>(value),
    )
  }

  /// 写宽 id 项。
  pub fn write_wid<D: Pod>(&self, ctx: &Context, key: u64, value: &[D]) -> bool {
    self.write_raw(
      ctx,
      &key.to_le_bytes(),
      bytemuck::must_cast_slice::<D, u8>(value),
    )
  }

  /// 写外部 id 项。
  pub fn write_eid<D: Pod>(&self, ctx: &Context, id: &VectorSetId, value: &[D]) -> bool {
    self.write_raw(ctx, id, bytemuck::must_cast_slice::<D, u8>(value))
  }

  /// 删除内部 id 项。
  pub fn delete_iid(&self, ctx: &Context, id: u32) -> bool {
    self.store.delete(ctx.inner, &id.to_le_bytes())
  }

  /// 删除外部 id 项。
  pub fn delete_eid(&self, ctx: &Context, id: &VectorSetId) -> bool {
    self.store.delete(ctx.inner, id)
  }

  /// 读改写内部 id 项（记录缺失时零初始化 `write_len` 字节传入 `f`）。
  ///
  /// `write_len == 0` 纯读短路的判定矩阵与应答语义同 [`StoreCallbacks::rmw`]。
  pub fn rmw_iid<F, T>(&self, ctx: &Context, id: u32, write_len: usize, mut f: F) -> bool
  where
    F: FnMut(&mut [T]),
    T: Pod,
  {
    const { assert!(align_of::<T>() <= 8, "存储层仅保证 8 字节对齐",) }
    self.store.rmw(
      ctx.inner,
      &id.to_le_bytes(),
      write_len,
      |data: &mut [u8]| f(bytemuck::cast_slice_mut::<u8, T>(data)),
    )
  }

  /// 读改写宽 id 项。
  ///
  /// `write_len == 0` 纯读短路的判定矩阵与应答语义同 [`StoreCallbacks::rmw`]。
  pub fn rmw_wid<F, T>(&self, ctx: &Context, key: u64, write_len: usize, mut f: F) -> bool
  where
    F: FnMut(&mut [T]),
    T: Pod,
  {
    const { assert!(align_of::<T>() <= 8, "存储层仅保证 8 字节对齐",) }
    self.store.rmw(
      ctx.inner,
      &key.to_le_bytes(),
      write_len,
      |data: &mut [u8]| f(bytemuck::cast_slice_mut::<u8, T>(data)),
    )
  }

  /// 内联过滤回调（按 internal_id 求值编译后的过滤表达式）。
  pub fn matches_filter(&self, ctx: &Context, internal_id: u32) -> bool {
    self.store.filter(ctx.inner, internal_id)
  }

  /// 日志通道。
  pub fn log(&self, ctx: &Context, msg: &str) {
    self.store.log(ctx.inner, msg);
  }

  fn read_multi_bool(&self, ctx: &Context, keys: &[u8], length_hint: usize) -> bool {
    let mut called = false;
    self
      .store
      .read_multi(ctx.inner, keys, length_hint, |_: u32, _: &[u8]| {
        called = true
      });
    called
  }

  fn read_bool(&self, ctx: &Context, key: &[u8], _length_hint: usize) -> bool {
    let mut called = false;
    self.store.read(ctx.inner, key, |_: &[u8]| called = true);
    called
  }

  fn read_bool_raw<F>(&self, ctx: &Context, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]),
  {
    self.store.read(ctx.inner, key, f)
  }

  fn read_single_raw(&self, ctx: &Context, key: &[u8], value: &mut [u8]) -> bool {
    let mut found = false;
    let read_ok = self.store.read(ctx.inner, key, |data: &[u8]| {
      if data.len() == value.len() {
        found = true;
        value.copy_from_slice(data);
      }
    });
    read_ok && found
  }

  fn write_raw(&self, ctx: &Context, key: &[u8], value: &[u8]) -> bool {
    self.store.write(ctx.inner, key, value)
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

/// 存储回调失败（garnet.rs GarnetError 的等价承接）。
#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone, Copy)]
pub enum StoreError {
  #[error("store read failed")]
  Read,
  #[error("store write failed")]
  Write,
  #[error("store delete failed")]
  Delete,
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn namespace_rules_match_csharp() {
    // ≤127 单字节，否则 4B LE
    assert_eq!(namespace_len(1), 1);
    assert_eq!(namespace_len(127), 1);
    assert_eq!(namespace_len(128), 4);
    assert_eq!(namespace_len(0x1_0000_0000), 4);

    let mut out = [0u8; 4];
    write_namespace(8, &mut out);
    assert_eq!(&out[..1], &[8]);

    write_namespace(0x1234_5678, &mut out);
    assert_eq!(out, 0x1234_5678u32.to_le_bytes());
  }

  #[test]
  fn term_or_masks_into_context() {
    let ctx = Context::new(8);
    assert_eq!(ctx.term(Term::Vector).inner, 8);
    assert_eq!(ctx.term(Term::ExtMap).inner, 8 | 6);
    assert_eq!(TERM_BITMASK, 7);
  }
}
