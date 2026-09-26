use wbase::pool::AlignedBuf;

use super::header::{RECORD_HEADER_LEN, WalFrameHeader};

/// 磁盘滑动预读窗口：覆盖判定、窗内负载探针与整体替换，恢复扫描与迭代扫描共用
///
/// 恢复主循环与磁盘读迭代器的公共骨架——窗口未覆盖所需前缀时按块
/// （`RECOVER_CHUNK_SIZE`）整体重新拉取并替换，前向推进
pub(crate) struct DiskWindow {
  buf: Option<AlignedBuf>,
  offset: u64,
}

impl DiskWindow {
  #[inline]
  pub const fn new() -> Self {
    Self {
      buf: None,
      offset: 0,
    }
  }

  /// 窗口是否完整覆盖 [offset, offset + need) 逻辑地址区间
  #[inline]
  pub fn covers(&self, offset: u64, need: usize) -> bool {
    match &self.buf {
      Some(buf) => offset >= self.offset && offset + need as u64 <= self.offset + buf.len() as u64,
      None => false,
    }
  }

  /// 整体替换窗口内容与起始逻辑地址
  #[inline]
  pub fn replace(&mut self, offset: u64, buf: AlignedBuf) {
    self.offset = offset;
    self.buf = Some(buf);
  }

  /// 窗口起始逻辑地址
  #[inline]
  pub fn offset(&self) -> u64 {
    self.offset
  }

  /// 窗口数据切片
  ///
  /// 调用方须保证先经 `replace` 装载覆盖目标地址的窗口
  #[inline]
  pub fn slice(&self) -> &[u8] {
    self.buf.as_deref().unwrap_or(&[])
  }

  /// 窗内负载探针：给定记录头逻辑地址 `addr`、帧头与该头在窗内的相对偏移
  /// `off`，整条记录（头 + 负载）完整落在窗内时返回负载借用切片（零 I/O、
  /// 零拷贝），越窗返回 None 由调用方回退单次设备读取
  ///
  /// 「窗内偏移 + 头长 + 负载长 ≤ 窗长」这条判据的全仓唯一落点：地址域下界
  /// 与长度换算均走 checked 算术，调用方不再各自手算窗内负载尾偏移、不再持裸切片
  #[inline]
  pub fn payload(&self, addr: u64, hdr: &WalFrameHeader, off: usize) -> Option<&[u8]> {
    if addr < self.offset {
      return None;
    }
    let start = off.checked_add(RECORD_HEADER_LEN)?;
    let end = start.checked_add(hdr.payload_len())?;
    self.slice().get(start..end)
  }
}
