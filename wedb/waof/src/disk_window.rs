use wram::AlignedBuf;

/// 磁盘滑动预读窗口：覆盖判定与整体替换，恢复扫描与迭代扫描共用
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
}
