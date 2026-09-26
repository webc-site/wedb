//! TLS 追加读缓冲空闲段初始化契约与记忆化清零缓冲
//!
//! 在 garnet 中的相对路径: libs/common/Networking/NetworkHandler.cs
//!（C# transportReceiveBuffer 数组分配期清零一次、sslStream.ReadAsync 逐批
//! 覆盖写，无逐读清零成本；rust 读目标须为已初始化切片，本模块把「空闲段
//! 已清零」的记忆随缓冲承载，整段 memset 从每读一次降为每缓冲一次）

use std::{
  mem::{MaybeUninit, take},
  ops::Deref,
  slice::from_raw_parts_mut,
};

use compio_buf::{IoBuf, IoBufMut, ReserveError, SetLen};

/// 空闲段初始化代际键:`(分配首指针, 容量)`
///
/// 长度增减不换键（同一段已初始化内存），realloc（指针变）与扩缩容
/// （原位扩容指针不变容量变）至少改动一元即失忆重清，杜绝跨分配误复用
pub type PrimeKey = (usize, usize);

/// 由缓冲推导代际键
#[inline]
pub fn prime_key(buf: &Vec<u8>) -> PrimeKey {
  (buf.as_ptr() as usize, buf.capacity())
}

/// TLS 追加读缓冲契约：交付空闲段的已初始化读视图
///
/// rustls 读目标为 `&mut [u8]`，空闲段必须先行初始化；实现以自有记忆保证
/// 同一段空闲内存至多清零一次（扩容/换缓冲后记忆自动失效重清）。调用方
/// `wtls::stream::tls_append_read` 逐 poll 取用，不付重复清零带宽
pub trait PrimedRecv: IoBufMut {
  /// 空闲段的已初始化视图（长度 = 容量 - 有效长度）
  fn primed_spare(&mut self) -> &mut [u8];
}

/// 记忆化零初始化追加读缓冲（裸 `Vec` 载体形态）
///
/// 供池化裸 Vec 的读泵（wconn）等所有权进出为裸 Vec 的场景承载：同一
/// Vec 实例驻留期间空闲段只清零一次；[`Self::take`] 交出缓冲即失忆
/// （异体缓冲禁复用旧代际），[`Self::clear`] 仅归零长度、记忆保持
/// （已初始化字节不因 `set_len(0)` 失效）
#[derive(Debug, Default)]
pub struct PrimedVec {
  buf: Vec<u8>,
  primed: Option<PrimeKey>,
}

impl PrimedVec {
  /// 从裸 Vec 构造（空闲段视为未初始化，首次 TLS 读前整段清零一次）
  pub fn new(buf: Vec<u8>) -> Self {
    Self { buf, primed: None }
  }

  /// 以外部代际键装配（键须取自同一缓冲的上一持有方，如池槽跨读承载）
  pub fn from_primed(buf: Vec<u8>, primed: Option<PrimeKey>) -> Self {
    Self { buf, primed }
  }

  /// 当前空闲段初始化代际（读毕由持有方转存，跨读复用记忆）
  pub fn prime_key(&self) -> Option<PrimeKey> {
    self.primed
  }

  /// 取出底层缓冲并失忆
  pub fn take(&mut self) -> Vec<u8> {
    self.primed = None;
    take(&mut self.buf)
  }

  /// 消耗取出底层缓冲并失忆
  pub fn into_inner(mut self) -> Vec<u8> {
    self.take()
  }

  /// 长度归零（记忆保持：覆盖写口径读泵每次读取前调用）
  pub fn clear(&mut self) {
    self.buf.clear();
  }

  /// 有效字节数
  pub fn len(&self) -> usize {
    self.buf.len()
  }

  /// 是否无有效字节
  pub fn is_empty(&self) -> bool {
    self.buf.is_empty()
  }
}

impl Deref for PrimedVec {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &[u8] {
    &self.buf
  }
}

impl IoBuf for PrimedVec {
  fn as_init(&self) -> &[u8] {
    &self.buf
  }
}

impl IoBufMut for PrimedVec {
  fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
    // 明文臂读目标：空闲段原样交驱动覆盖写（未初始化合法）
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

impl SetLen for PrimedVec {
  unsafe fn set_len(&mut self, len: usize) {
    // 契约：len 为绝对总长，[buf_len(), len) 已由驱动写入初始化字节
    unsafe { self.buf.set_len(len) };
  }

  unsafe fn advance_to(&mut self, len: usize) {
    // 追加读完成路径：总长推进 len
    let current = self.buf.len();
    unsafe { self.buf.set_len(current + len) };
  }
}

impl PrimedRecv for PrimedVec {
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

#[cfg(test)]
mod tests {
  use super::*;

  /// 记忆键在同一分配上稳定：清零一次后重复取用不换键
  #[test]
  fn prime_key_stable_across_len_changes() {
    let mut pv = PrimedVec::new(Vec::with_capacity(64));
    assert_eq!(pv.prime_key(), None);
    let key1 = {
      let spare = pv.primed_spare();
      assert_eq!(spare.len(), 64);
      assert!(spare.iter().all(|&b| b == 0));
      pv.prime_key()
    };
    // 推进长度后同段再取：键不变（不重清零），视图随长度收缩
    unsafe { pv.advance_to(8) };
    assert_eq!(pv.len(), 8);
    assert_eq!(pv.prime_key(), key1);
    assert_eq!(pv.primed_spare().len(), 56);
    // 归零长度：记忆保持，视图恢复全容量
    pv.clear();
    assert_eq!(pv.prime_key(), key1);
    assert_eq!(pv.primed_spare().len(), 64);
  }

  /// 扩容（realloc）与取出（换缓冲）均失忆，新段重新清零
  #[test]
  fn memo_invalidated_by_grow_and_take() {
    let mut pv = PrimedVec::new(Vec::with_capacity(16));
    let _ = pv.primed_spare();
    assert!(pv.prime_key().is_some());
    pv.reserve(4096).expect("扩容");
    assert_ne!(pv.buf.capacity(), 16);
    assert_eq!(pv.prime_key(), None, "扩容后代际必须失效");
    let _ = pv.primed_spare();
    let raw = pv.take();
    assert_eq!(pv.prime_key(), None, "取出后必须失忆");
    // 异体缓冲以外部键装配不非法，但新段须重新清零后才可作读视图
    let mut reborn = PrimedVec::from_primed(raw, Some((0, 0)));
    let spare = reborn.primed_spare();
    assert!(spare.iter().all(|&b| b == 0));
  }
}
