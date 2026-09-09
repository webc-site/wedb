use std::{
  alloc::{Layout, alloc, alloc_zeroed, dealloc, handle_alloc_error},
  borrow::{Borrow, BorrowMut},
  fmt::{self, Debug, Formatter},
  mem::{ManuallyDrop, MaybeUninit},
  ops::{Deref, DerefMut},
  ptr::{NonNull, copy_nonoverlapping, eq},
  slice::{from_raw_parts, from_raw_parts_mut},
  sync::Arc,
};

use compio_buf::{IoBuf, IoBufMut, SetLen};

use crate::{
  align::{DEFAULT_SECTOR_SIZE, validate_sector_size},
  error::{Error, Result},
  pool::{BufMeta, BufferPool, CachedBuf},
};

/// 扇区对齐的堆内存缓冲区
///
/// 可由 [`BufferPool`](crate::BufferPool) 签发：携带池归属与归还清零策略，
/// RAII drop 时自动归还入池复用；否则 drop 即释放。
pub struct AlignedBuf {
  ptr: NonNull<u8>,
  len: usize,
  cap: usize,
  align: usize,
  /// 池归属与 Origin-Return 路由元数据 (None 表示独立分配，drop 直接释放)
  pooled: Option<(Arc<BufferPool>, BufMeta)>,
}

unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

/// 零容量缓冲区的对齐悬垂哨兵指针 (永不解引用，仅携带对齐元数据)
///
/// SAFETY: 对齐已校验为 >= MIN_SECTOR_SIZE 的 2 的幂，地址必非零
#[inline]
fn dangling(align: usize) -> NonNull<u8> {
  unsafe { NonNull::new_unchecked(align as *mut u8) }
}

impl AlignedBuf {
  /// 创建指定容量和对齐大小的缓冲区（对齐须为 2 的幂且 >= MIN_SECTOR_SIZE 512）
  ///
  /// 分配的内存全部通过 `alloc_zeroed` 置零，初始逻辑长度为 0
  pub fn new(cap: usize, align: usize) -> Result<Self> {
    validate_sector_size(align)?;

    if cap == 0 {
      return Ok(Self {
        ptr: dangling(align),
        len: 0,
        cap: 0,
        align,
        pooled: None,
      });
    }

    let layout = Layout::from_size_align(cap, align)?;
    let Some(ptr) = NonNull::new(unsafe { alloc_zeroed(layout) }) else {
      return Err(Error::AllocFailed(layout));
    };

    Ok(Self {
      ptr,
      len: 0,
      cap,
      align,
      pooled: None,
    })
  }

  /// 使用默认扇区大小（4096）创建指定容量的缓冲区
  #[inline]
  pub fn with_sector_size(cap: usize) -> Result<Self> {
    Self::new(cap, DEFAULT_SECTOR_SIZE)
  }

  /// 从切片数据复制创建指定对齐的缓冲区，容量与初始长度均为切片长度
  pub fn from_slice(data: &[u8], align: usize) -> Result<Self> {
    validate_sector_size(align)?;

    let len = data.len();
    if len == 0 {
      return Self::new(0, align);
    }

    let layout = Layout::from_size_align(len, align)?;
    let Some(ptr) = NonNull::new(unsafe { alloc(layout) }) else {
      return Err(Error::AllocFailed(layout));
    };

    unsafe {
      copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), len);
    }

    Ok(Self {
      ptr,
      len,
      cap: len,
      align,
      pooled: None,
    })
  }

  /// 创建指定容量和对齐大小的缓冲区，并将初始长度设为等于容量（已全置零）
  pub fn zeroed(cap: usize, align: usize) -> Result<Self> {
    let mut buf = Self::new(cap, align)?;
    buf.len = cap;
    Ok(buf)
  }

  /// 使用默认扇区大小（4096）创建全置零且长度等于容量的缓冲区
  #[inline]
  pub fn zeroed_with_sector_size(cap: usize) -> Result<Self> {
    Self::zeroed(cap, DEFAULT_SECTOR_SIZE)
  }

  /// 缓冲区当前逻辑数据长度
  #[inline]
  pub fn len(&self) -> usize {
    self.len
  }

  /// 是否持有实际分配的内存（非零容量悬垂哨兵）
  /// libs/storage/Tsavorite/cs/src/core/Allocator/BlittableFrame.cs:IsAllocated
  #[inline]
  pub fn is_allocated(&self) -> bool {
    self.cap > 0 && !eq(self.ptr.as_ptr(), dangling(self.align).as_ptr())
  }

  /// libs/storage/Tsavorite/cs/src/core/Allocator/BlittableFrame.cs:GetArrayAndUnalignedOffset
  #[inline]
  pub fn get_array_and_unaligned_offset(&self) -> (*const u8, usize) {
    (self.ptr.as_ptr(), 0)
  }

  /// 缓冲区总容量（字节数）
  #[inline]
  pub fn capacity(&self) -> usize {
    self.cap
  }

  /// 缓冲区内存对齐大小（字节）
  #[inline]
  pub fn align(&self) -> usize {
    self.align
  }

  /// 缓冲区逻辑数据是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }

  /// 清空缓冲区逻辑数据（长度重置为 0，不释放内存）
  #[inline]
  pub fn clear(&mut self) {
    self.len = 0;
  }

  /// 设置缓冲区逻辑数据长度，长度不得超过总容量
  pub fn set_len(&mut self, new_len: usize) -> Result<()> {
    if new_len > self.cap {
      return Err(Error::SetLenExceeded {
        len: new_len,
        capacity: self.cap,
      });
    }
    self.len = new_len;
    Ok(())
  }

  /// 获取当前逻辑数据的不可变切片
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    if self.len == 0 {
      &[]
    } else {
      unsafe { from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
  }

  /// 获取当前逻辑数据的可变切片
  #[inline]
  pub fn as_mut_slice(&mut self) -> &mut [u8] {
    if self.len == 0 {
      &mut []
    } else {
      unsafe { from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
  }

  /// 获取整个分配容量的不可变切片（包括超出当前逻辑长度的部分）
  #[inline]
  pub fn as_allocated_slice(&self) -> &[u8] {
    if self.cap == 0 {
      &[]
    } else {
      unsafe { from_raw_parts(self.ptr.as_ptr(), self.cap) }
    }
  }

  /// 获取整个分配容量的可变切片（包括超出当前逻辑长度的部分）
  #[inline]
  pub fn as_allocated_slice_mut(&mut self) -> &mut [u8] {
    if self.cap == 0 {
      &mut []
    } else {
      unsafe { from_raw_parts_mut(self.ptr.as_ptr(), self.cap) }
    }
  }

  /// 有效需求长度 (对标 C# `required_bytes`)：租借方本次请求的原始字节数（不做扇区取整），
  /// [`BufferPool::ensure_size`](crate::BufferPool::ensure_size) 复用路径会同步为最新请求；
  /// 非池化缓冲区无此元数据，恒等于 [`AlignedBuf::capacity`]（扇区取整后的容量）
  #[inline]
  #[must_use]
  pub fn required_len(&self) -> usize {
    self
      .pooled
      .as_ref()
      .map_or(self.cap, |(_, meta)| meta.required)
  }

  /// 更新有效需求长度 (池内部 `ensure_size` 复用路径专用，保持与签发路径一致的扇区取整约定)
  #[inline]
  pub(crate) fn set_required(&mut self, required: usize) {
    if let Some((_, meta)) = self.pooled.as_mut() {
      meta.required = required;
    }
  }

  /// 获取当前的归还清零策略 (对标 C# `SectorAlignedMemory.clearOnReturn`)
  ///
  /// 非池化独立分配的缓冲区恒返回 `true`
  #[inline]
  #[must_use]
  pub fn clear_on_return(&self) -> bool {
    self
      .pooled
      .as_ref()
      .is_none_or(|(_, meta)| meta.clear_on_return)
  }

  /// 动态调整归还清零策略 (对标 C# `SectorAlignedMemory.clearOnReturn`)
  ///
  /// 设为 `false` 适用于读目的地覆写场景，免去归还时的清零开销
  #[inline]
  pub fn set_clear_on_return(&mut self, clear: bool) {
    if let Some((_, meta)) = self.pooled.as_mut() {
      meta.clear_on_return = clear;
    }
  }

  /// 由缓冲池基于缓存节点重建 (携带 origin-return 路由)
  pub(crate) fn from_cached(node: CachedBuf, pool: Arc<BufferPool>, meta: BufMeta) -> Self {
    let node = ManuallyDrop::new(node);
    Self {
      ptr: node.ptr,
      len: 0,
      cap: node.cap,
      align: node.align,
      pooled: Some((pool, meta)),
    }
  }

  /// 将独立分配的缓冲区纳入池归属 (池签发新分配时的内部路径)
  pub(crate) fn attach(&mut self, pool: Arc<BufferPool>, meta: BufMeta) {
    self.pooled = Some((pool, meta));
  }

  /// 检查底层裸指针是否满足指定的对齐要求
  #[inline]
  pub fn is_aligned_to(&self, align: usize) -> bool {
    if align == 0 || !align.is_power_of_two() {
      return false;
    }
    ((self.ptr.as_ptr() as usize) & (align - 1)) == 0
  }

  /// 检查底层裸指针是否满足自身的对齐要求
  #[inline]
  pub fn is_ptr_aligned(&self) -> bool {
    self.is_aligned_to(self.align)
  }

  /// 获取常量裸指针
  #[inline]
  pub fn as_buf_ptr(&self) -> *const u8 {
    self.ptr.as_ptr()
  }

  /// 获取可变裸指针
  #[inline]
  pub fn as_mut_buf_ptr(&mut self) -> *mut u8 {
    self.ptr.as_ptr()
  }

  /// 显式设置已初始化逻辑长度（不执行容量上界安全检查）
  ///
  /// # Safety
  ///
  /// 调用者须保证 `len <= self.capacity()`
  #[inline]
  pub unsafe fn set_len_unchecked(&mut self, len: usize) {
    debug_assert!(len <= self.cap);
    self.len = len;
  }
}

impl Drop for AlignedBuf {
  fn drop(&mut self) {
    if self.cap == 0 {
      return;
    }
    if let Some((pool, meta)) = self.pooled.take() {
      // RAII 归还：按 Origin-Return 路由归还入池复用 (对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:ReturnOriginReturn)
      pool.return_buf(self.ptr, self.cap, self.align, meta);
      return;
    }
    unsafe {
      let layout = Layout::from_size_align_unchecked(self.cap, self.align);
      dealloc(self.ptr.as_ptr(), layout);
    }
  }
}

impl Deref for AlignedBuf {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

impl DerefMut for AlignedBuf {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.as_mut_slice()
  }
}

impl AsRef<[u8]> for AlignedBuf {
  #[inline]
  fn as_ref(&self) -> &[u8] {
    self.as_slice()
  }
}

impl AsMut<[u8]> for AlignedBuf {
  #[inline]
  fn as_mut(&mut self) -> &mut [u8] {
    self.as_mut_slice()
  }
}

impl Borrow<[u8]> for AlignedBuf {
  #[inline]
  fn borrow(&self) -> &[u8] {
    self.as_slice()
  }
}

impl BorrowMut<[u8]> for AlignedBuf {
  #[inline]
  fn borrow_mut(&mut self) -> &mut [u8] {
    self.as_mut_slice()
  }
}

impl Clone for AlignedBuf {
  fn clone(&self) -> Self {
    // 深拷贝独立分配：克隆体不入池 (池归属不可复制，避免许可双重记账)
    if self.cap == 0 {
      return Self {
        ptr: dangling(self.align),
        len: 0,
        cap: 0,
        align: self.align,
        pooled: None,
      };
    }
    let layout = unsafe { Layout::from_size_align_unchecked(self.cap, self.align) };
    let raw = unsafe { alloc_zeroed(layout) };
    let Some(ptr) = NonNull::new(raw) else {
      handle_alloc_error(layout);
    };
    if self.len > 0 {
      unsafe {
        copy_nonoverlapping(self.ptr.as_ptr(), ptr.as_ptr(), self.len);
      }
    }
    Self {
      ptr,
      len: self.len,
      cap: self.cap,
      align: self.align,
      pooled: None,
    }
  }
}

impl PartialEq for AlignedBuf {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.as_slice() == other.as_slice()
  }
}

impl Eq for AlignedBuf {}

impl PartialEq<[u8]> for AlignedBuf {
  #[inline]
  fn eq(&self, other: &[u8]) -> bool {
    self.as_slice() == other
  }
}

impl PartialEq<&[u8]> for AlignedBuf {
  #[inline]
  fn eq(&self, other: &&[u8]) -> bool {
    self.as_slice() == *other
  }
}

impl Debug for AlignedBuf {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("AlignedBuf")
      .field("len", &self.len)
      .field("cap", &self.cap)
      .field("required", &self.required_len())
      .field("align", &self.align)
      .field("pooled", &self.pooled.is_some())
      .field("ptr", &self.ptr)
      .finish()
  }
}

impl IoBuf for AlignedBuf {
  #[inline]
  fn as_init(&self) -> &[u8] {
    self.as_slice()
  }

  #[inline]
  fn buf_len(&self) -> usize {
    self.len
  }

  #[inline]
  fn buf_ptr(&self) -> *const u8 {
    self.ptr.as_ptr()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.len == 0
  }
}

impl SetLen for AlignedBuf {
  #[inline]
  unsafe fn set_len(&mut self, len: usize) {
    unsafe { self.set_len_unchecked(len) };
  }
}

impl IoBufMut for AlignedBuf {
  #[inline]
  fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
    if self.cap == 0 {
      &mut []
    } else {
      unsafe { from_raw_parts_mut(self.ptr.as_ptr() as *mut MaybeUninit<u8>, self.cap) }
    }
  }

  #[inline]
  fn buf_capacity(&mut self) -> usize {
    self.cap
  }

  #[inline]
  fn buf_mut_ptr(&mut self) -> *mut MaybeUninit<u8> {
    self.ptr.as_ptr() as *mut MaybeUninit<u8>
  }
}
