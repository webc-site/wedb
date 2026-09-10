use std::{mem::size_of, ptr::copy_nonoverlapping, slice::from_raw_parts};

/// 在 garnet 中的相对路径:Tsavorite.core/PinnedSpanByte.cs
/// We use ArgSlice to represent PinnedSpanByte.
#[derive(Debug, Clone, Copy)]
pub struct ArgSlice {
  pub ptr: *const u8,
  pub length: usize,
}

impl ArgSlice {
  #[inline]
  pub fn new(ptr: *const u8, length: usize) -> Self {
    Self { ptr, length }
  }

  #[inline]
  pub fn as_slice<'a>(&self) -> &'a [u8] {
    if self.ptr.is_null() || self.length == 0 {
      &[]
    } else {
      unsafe { from_raw_parts(self.ptr, self.length) }
    }
  }

  #[inline]
  pub fn total_size(&self) -> usize {
    self.length + size_of::<u32>()
  }

  /// Serializes this ArgSlice to a buffer, including a 4-byte length prefix.
  ///
  /// # Safety
  /// `dest` 须指向至少 `4 + length` 字节的可写缓冲；`self.ptr` 指向的
  /// `length` 字节源数据须在调用期间保持解引用有效
  pub unsafe fn serialize_to(&self, dest: *mut u8) {
    unsafe {
      let len_u32 = self.length as u32;
      copy_nonoverlapping(&len_u32 as *const u32 as *const u8, dest, 4);
      if self.length > 0 {
        copy_nonoverlapping(self.ptr, dest.add(4), self.length);
      }
    }
  }

  /// Deserializes an ArgSlice from a pointer that has a 4-byte length prefix.
  ///
  /// # Safety
  /// `src` 须指向至少 4 字节可读前缀，且长度前缀声明的 `len_u32` 字节
  /// 负载须完整可读；返回的 ArgSlice 借用该内存，调用方须保证其生命周期
  /// 内数据不被释放或改写
  pub unsafe fn from_length_prefixed_ptr(src: *const u8) -> Self {
    unsafe {
      let mut len_u32 = 0u32;
      copy_nonoverlapping(src, &mut len_u32 as *mut u32 as *mut u8, 4);
      Self {
        ptr: src.add(4),
        length: len_u32 as usize,
      }
    }
  }
}

// Make it Send/Sync since it's just a raw pointer wrapper
unsafe impl Send for ArgSlice {}
unsafe impl Sync for ArgSlice {}
