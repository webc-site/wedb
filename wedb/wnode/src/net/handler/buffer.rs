//! 追加式接收读缓冲与探测阈值（探测域）
//!
//! 在 garnet 中的相对路径: `libs/common/Networking/NetworkHandler.cs`（bytesRead 累积读取模型）

use std::{mem::MaybeUninit, slice::from_raw_parts_mut};

use compio::buf::{IoBuf, IoBufMut, ReserveError, SetLen};
use wbase::primed::{PrimeKey, PrimedRecv, prime_key};

/// 读取前缓冲预留最小空闲容量阈值（不足时向前平移或扩容）
pub(super) const MIN_READ_SPACE: usize = 4096;
/// 握手识别首批最小所需字节数
pub(super) const MIN_HANDSHAKE_BYTES: usize = 4;

/// 追加式接收读缓冲（所有权进出读操作，compio 驱动要求 'static）
///
/// compio 对裸 `Vec` 的读约定为覆盖语义（读目标为整段容量、完成后长度
/// 截断为本次读取数），半包残余字节会被下一批读取破坏；此包装把读目标
/// 改为空闲容量段、完成后按追加推进长度，网络字节得以在缓冲内跨批次
/// 累积 —— C# NetworkHandler 的 bytesRead 累积读取模型等价物。
///
/// TLS 臂经 [`PrimedRecv`] 契约交付空闲段：同一段空闲内存至多清零一次
/// （代际 [`Self::primed`] 随缓冲所有权承载，键失配即扩容/换块，自动
/// 重清），整段 memset 不随读取频率线性付费
pub(super) struct RecvAppend {
  buf: Vec<u8>,
  /// 空闲段初始化代际（`None` = 未初始化；跨读记忆经构造注入）
  primed: Option<PrimeKey>,
}

impl RecvAppend {
  /// 从裸 Vec 构造（空闲段视为未初始化）
  pub(super) fn new(buf: Vec<u8>) -> Self {
    Self { buf, primed: None }
  }

  /// 以外部代际键构造（键须取自同一缓冲的上一持有方，如会话跨读转存）
  pub(super) fn from_primed(buf: Vec<u8>, primed: Option<PrimeKey>) -> Self {
    Self { buf, primed }
  }

  /// 取出底层 Vec 并交出代际（归还缓冲所有权路径）
  pub(super) fn into_parts(self) -> (Option<PrimeKey>, Vec<u8>) {
    (self.primed, self.buf)
  }
}

impl IoBuf for RecvAppend {
  fn as_init(&self) -> &[u8] {
    &self.buf
  }
}

impl IoBufMut for RecvAppend {
  fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
    // 明文臂读目标：空闲容量段（驱动自段首写入本次读取字节），原样交驱动
    self.buf.spare_capacity_mut()
  }

  fn reserve(&mut self, len: usize) -> Result<(), ReserveError> {
    let before = self.buf.capacity();
    self
      .buf
      .try_reserve(len)
      .map_err(|e| ReserveError::ReserveFailed(Box::new(e)))?;
    // 扩容即 realloc/原位增长，新尾段未初始化，代际失效
    if self.buf.capacity() != before {
      self.primed = None;
    }
    Ok(())
  }
}

impl SetLen for RecvAppend {
  unsafe fn set_len(&mut self, len: usize) {
    // 契约：len 为绝对总长，[buf_len(), len) 已由驱动写入初始化字节，
    // 且 len <= buf_len() + 空闲容量，Vec::set_len 合法
    unsafe { self.buf.set_len(len) };
  }

  unsafe fn advance_to(&mut self, len: usize) {
    // 驱动读完成路径（BufResultExt::map_advanced）以本次写入空闲容量段
    // 的字节数调用；追加语义下总长推进 len
    let current = self.buf.len();
    unsafe { self.buf.set_len(current + len) };
  }
}

impl PrimedRecv for RecvAppend {
  fn primed_spare(&mut self) -> &mut [u8] {
    let len = self.buf.len();
    if self.primed != Some(prime_key(&self.buf)) {
      self.buf.spare_capacity_mut().fill(MaybeUninit::new(0));
      self.primed = Some(prime_key(&self.buf));
    }
    let cap = self.buf.capacity();
    // SAFETY: 代际匹配或刚整段清零，[len, cap) 全为初始化字节；
    // 指针与长度均取自缓冲本体
    unsafe { from_raw_parts_mut(self.buf.as_mut_ptr().add(len).cast::<u8>(), cap - len) }
  }
}
