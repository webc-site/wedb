//! 追加式接收读缓冲与探测阈值（探测域）
//!
//! 在 garnet 中的相对路径: `libs/common/Networking/NetworkHandler.cs`（bytesRead 累积读取模型）

use std::mem::MaybeUninit;

use compio::buf::{IoBuf, IoBufMut, ReserveError, SetLen};

/// 读取前缓冲预留最小空闲容量阈值（不足时向前平移或扩容）
pub(super) const MIN_READ_SPACE: usize = 4096;
/// 握手识别首批最小所需字节数
pub(super) const MIN_HANDSHAKE_BYTES: usize = 4;

/// 追加式接收读缓冲（所有权进出读操作，compio 驱动要求 'static）
///
/// compio 对裸 `Vec` 的读约定为覆盖语义（读目标为整段容量、完成后长度
/// 截断为本次读取数），半包残余字节会被下一批读取破坏；此包装把读目标
/// 改为空闲容量段、完成后按追加推进长度，网络字节得以在缓冲内跨批次
/// 累积 —— C# NetworkHandler 的 bytesRead 累积读取模型等价物
pub(super) struct RecvAppend(pub(super) Vec<u8>);

impl IoBuf for RecvAppend {
  fn as_init(&self) -> &[u8] {
    &self.0
  }
}

impl IoBufMut for RecvAppend {
  fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
    // 读目标：空闲容量段（驱动自段首写入本次读取字节）
    self.0.spare_capacity_mut()
  }

  fn reserve(&mut self, len: usize) -> Result<(), ReserveError> {
    self
      .0
      .try_reserve(len)
      .map_err(|e| ReserveError::ReserveFailed(Box::new(e)))
  }
}

impl SetLen for RecvAppend {
  unsafe fn set_len(&mut self, len: usize) {
    // 契约：len 为绝对总长，[buf_len(), len) 已由驱动写入初始化字节，
    // 且 len <= buf_len() + 空闲容量，Vec::set_len 合法
    unsafe { self.0.set_len(len) };
  }

  unsafe fn advance_to(&mut self, len: usize) {
    // 驱动读完成路径（BufResultExt::map_advanced）以本次写入空闲容量段
    // 的字节数调用；追加语义下总长推进 len
    let current = self.0.len();
    unsafe { self.0.set_len(current + len) };
  }
}
