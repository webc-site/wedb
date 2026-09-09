use std::sync::atomic::{AtomicU64, Ordering};

use bitcode::{Decode, Encode};

/// 64 位连续逻辑地址空间管理器
///
/// 严格对齐 Microsoft Garnet Tsavorite 的地址滑动状态机模型：
/// `0 <= begin <= safe_head <= head <= safe_read_only <= read_only <= tail`
///
/// - `[read_only, tail)`: 可变区（Mutable Region），支持原地修改（In-place update）
/// - `[head, read_only)`: 只读区（ReadOnly Region），驻留内存但只读，更新走 RCU 追加
/// - `[begin, head)`: 磁盘区（OnDisk Region），已从内存驱逐，读取走底层 Device 异步 I/O
/// - `< begin` 或 `>= tail`: 无效地址区
#[derive(Debug)]
pub struct AddressManager {
  /// 追加尾部逻辑地址（下一条记录写入位置）
  pub tail_address: AtomicU64,
  /// 只读区起始边界（该地址及以上为可变区，该地址以下为只读区）
  pub read_only_address: AtomicU64,
  /// 安全只读边界（Epoch 纪元安全确认后的只读地址）
  pub safe_read_only_address: AtomicU64,
  /// 内存驻留区起始边界（该地址及以上在内存环形页中，该地址以下在磁盘上）
  pub head_address: AtomicU64,
  /// 安全内存驻留边界（Epoch 纪元安全确认后的 Head 地址）
  pub safe_head_address: AtomicU64,
  /// 日志起始有效逻辑边界（该地址以下为已截断废弃历史记录）
  pub begin_address: AtomicU64,
  /// 已安全落盘至存储介质的最高连续逻辑边界（对照 Tsavorite FlushedUntilAddress）
  pub flushed_until_address: AtomicU64,
}

impl AddressManager {
  /// 创建新的地址管理器，所有边界初始设定为 initial_addr（通常为 64）
  pub fn new(initial_addr: u64) -> Self {
    Self {
      tail_address: AtomicU64::new(initial_addr),
      read_only_address: AtomicU64::new(initial_addr),
      safe_read_only_address: AtomicU64::new(initial_addr),
      head_address: AtomicU64::new(initial_addr),
      safe_head_address: AtomicU64::new(initial_addr),
      begin_address: AtomicU64::new(initial_addr),
      flushed_until_address: AtomicU64::new(initial_addr),
    }
  }

  /// 判断地址是否落在可变区（Mutable Region: addr >= effective_ro && addr < tail）
  #[inline]
  pub fn is_mutable(&self, addr: u64) -> bool {
    let tail = self.tail_address.load(Ordering::Acquire);
    let ro = self.read_only_address.load(Ordering::Acquire);
    let head = self.head_address.load(Ordering::Acquire);
    Self::is_mutable_snapshot(addr, head, ro, tail)
  }

  /// 基于调用方持有的边界快照判断可变区（快照变体，供扫描等热路径消除重复原子加载）
  ///
  /// 快照值只允许偏旧（单调地址只增不减），偏旧只会把可变区误判为只读区而走保守读锁路径，
  /// 绝不会把只读区误判为可变区，判定方向天然安全。
  #[inline]
  pub const fn is_mutable_snapshot(addr: u64, head: u64, read_only: u64, tail: u64) -> bool {
    let effective_ro = if read_only > head { read_only } else { head };
    addr >= effective_ro && addr < tail
  }

  /// 判断地址是否落在内存只读区（ReadOnly Region: addr >= head && addr < read_only）
  #[inline]
  pub fn is_read_only(&self, addr: u64) -> bool {
    let ro = self.read_only_address.load(Ordering::Acquire);
    let head = self.head_address.load(Ordering::Acquire);
    addr >= head && addr < ro
  }

  /// 判断地址是否驻留在内存环形缓冲区中（InMemory: addr >= head && addr < tail）
  #[inline]
  pub fn is_in_memory(&self, addr: u64) -> bool {
    let tail = self.tail_address.load(Ordering::Acquire);
    let head = self.head_address.load(Ordering::Acquire);
    addr >= head && addr < tail
  }

  /// 判断地址是否已落盘或仅存在于磁盘区（OnDisk: addr >= begin && addr < head）
  #[inline]
  pub fn is_on_disk(&self, addr: u64) -> bool {
    let head = self.head_address.load(Ordering::Acquire);
    let begin = self.begin_address.load(Ordering::Acquire);
    addr >= begin && addr < head
  }

  /// 判断地址是否在有效范围内（Valid: addr >= begin && addr < tail）
  #[inline]
  pub fn is_valid(&self, addr: u64) -> bool {
    let tail = self.tail_address.load(Ordering::Acquire);
    let begin = self.begin_address.load(Ordering::Acquire);
    addr >= begin && addr < tail
  }

  /// 单调推进 ReadOnlyAddress
  #[inline]
  pub fn shift_read_only_address(&self, new_ro: u64) -> u64 {
    self.read_only_address.fetch_max(new_ro, Ordering::AcqRel)
  }

  /// 单调推进 SafeReadOnlyAddress
  #[inline]
  pub fn shift_safe_read_only_address(&self, new_safe_ro: u64) -> u64 {
    self
      .safe_read_only_address
      .fetch_max(new_safe_ro, Ordering::AcqRel)
  }

  /// 单调推进 HeadAddress，并确保 ReadOnlyAddress 不落后于 HeadAddress
  #[inline]
  pub fn shift_head_address(&self, new_head: u64) -> u64 {
    self.shift_read_only_address(new_head);
    self.head_address.fetch_max(new_head, Ordering::AcqRel)
  }

  /// 单调推进 SafeHeadAddress，并确保 SafeReadOnlyAddress 不落后于 SafeHeadAddress
  #[inline]
  pub fn shift_safe_head_address(&self, new_safe_head: u64) -> u64 {
    self.shift_safe_read_only_address(new_safe_head);
    self
      .safe_head_address
      .fetch_max(new_safe_head, Ordering::AcqRel)
  }

  /// 单调推进 BeginAddress（纯地址状态机原语）
  ///
  /// 注意：HybridLog 的截断路径刻意不走"连带内联强推 head/safe_head"的复合推进
  ///（内联强推 safe_head 会绕过纪元排空语义，令旧页上 epoch 保护的读者失去保护），
  /// 而是经 `HybridLog::shift_head_address` 以 Epoch 延迟方式推进 head/safe_head 后，
  /// 再单独调用本方法推进 begin。
  #[inline]
  pub fn shift_begin_address(&self, new_begin: u64) -> u64 {
    self.begin_address.fetch_max(new_begin, Ordering::AcqRel)
  }

  /// 获取当前 TailAddress
  #[inline]
  pub fn tail(&self) -> u64 {
    self.tail_address.load(Ordering::Acquire)
  }

  /// 获取当前 ReadOnlyAddress
  #[inline]
  pub fn read_only(&self) -> u64 {
    self.read_only_address.load(Ordering::Acquire)
  }

  /// 获取当前 SafeReadOnlyAddress
  #[inline]
  pub fn safe_read_only(&self) -> u64 {
    self.safe_read_only_address.load(Ordering::Acquire)
  }

  /// 获取当前 HeadAddress
  #[inline]
  pub fn head(&self) -> u64 {
    self.head_address.load(Ordering::Acquire)
  }

  /// 获取当前 SafeHeadAddress
  #[inline]
  pub fn safe_head(&self) -> u64 {
    self.safe_head_address.load(Ordering::Acquire)
  }

  /// 获取当前 BeginAddress
  #[inline]
  pub fn begin(&self) -> u64 {
    self.begin_address.load(Ordering::Acquire)
  }

  /// 单调推进 FlushedUntilAddress
  #[inline]
  pub fn shift_flushed_until_address(&self, new_flushed: u64) -> u64 {
    self
      .flushed_until_address
      .fetch_max(new_flushed, Ordering::AcqRel)
  }

  /// 获取当前 FlushedUntilAddress
  #[inline]
  pub fn flushed_until(&self) -> u64 {
    self.flushed_until_address.load(Ordering::Acquire)
  }

  /// 基于持久化快照初始化地址管理器
  pub fn with_snapshot(snapshot: AddressSnapshot) -> Self {
    Self {
      tail_address: AtomicU64::new(snapshot.tail),
      read_only_address: AtomicU64::new(snapshot.read_only),
      safe_read_only_address: AtomicU64::new(snapshot.safe_read_only),
      head_address: AtomicU64::new(snapshot.head),
      safe_head_address: AtomicU64::new(snapshot.safe_head),
      begin_address: AtomicU64::new(snapshot.begin),
      flushed_until_address: AtomicU64::new(snapshot.flushed_until),
    }
  }

  /// 校验当前地址状态机是否满足单调递增不变式
  #[inline]
  pub fn validate_invariants(&self) -> bool {
    self.snapshot().validate()
  }

  /// 捕获当前所有地址状态的快照
  pub fn snapshot(&self) -> AddressSnapshot {
    AddressSnapshot {
      tail: self.tail(),
      read_only: self.read_only(),
      safe_read_only: self.safe_read_only(),
      head: self.head(),
      safe_head: self.safe_head(),
      begin: self.begin(),
      flushed_until: self.flushed_until(),
    }
  }
}

/// 地址状态快照（只读视图）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct AddressSnapshot {
  pub tail: u64,
  pub read_only: u64,
  pub safe_read_only: u64,
  pub head: u64,
  pub safe_head: u64,
  pub begin: u64,
  pub flushed_until: u64,
}

impl AddressSnapshot {
  /// 快照二进制大小（7 个 64 位无符号整数 = 56 字节）
  pub const SNAPSHOT_SIZE: usize = 56;

  /// 编码为 56 字节定长数组（小端编码，const fn，零堆分配）
  #[inline(always)]
  pub const fn to_bytes(&self) -> [u8; Self::SNAPSHOT_SIZE] {
    let b0 = self.tail.to_le_bytes();
    let b1 = self.read_only.to_le_bytes();
    let b2 = self.safe_read_only.to_le_bytes();
    let b3 = self.head.to_le_bytes();
    let b4 = self.safe_head.to_le_bytes();
    let b5 = self.begin.to_le_bytes();
    let b6 = self.flushed_until.to_le_bytes();
    [
      b0[0], b0[1], b0[2], b0[3], b0[4], b0[5], b0[6], b0[7], b1[0], b1[1], b1[2], b1[3], b1[4],
      b1[5], b1[6], b1[7], b2[0], b2[1], b2[2], b2[3], b2[4], b2[5], b2[6], b2[7], b3[0], b3[1],
      b3[2], b3[3], b3[4], b3[5], b3[6], b3[7], b4[0], b4[1], b4[2], b4[3], b4[4], b4[5], b4[6],
      b4[7], b5[0], b5[1], b5[2], b5[3], b5[4], b5[5], b5[6], b5[7], b6[0], b6[1], b6[2], b6[3],
      b6[4], b6[5], b6[6], b6[7],
    ]
  }

  /// 从 56 字节定长数组解码快照（const fn）
  #[inline(always)]
  pub const fn from_bytes(bytes: [u8; Self::SNAPSHOT_SIZE]) -> Self {
    let tail = u64::from_le_bytes([
      bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let read_only = u64::from_le_bytes([
      bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    let safe_read_only = u64::from_le_bytes([
      bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
    ]);
    let head = u64::from_le_bytes([
      bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31],
    ]);
    let safe_head = u64::from_le_bytes([
      bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39],
    ]);
    let begin = u64::from_le_bytes([
      bytes[40], bytes[41], bytes[42], bytes[43], bytes[44], bytes[45], bytes[46], bytes[47],
    ]);
    let flushed_until = u64::from_le_bytes([
      bytes[48], bytes[49], bytes[50], bytes[51], bytes[52], bytes[53], bytes[54], bytes[55],
    ]);
    Self {
      tail,
      read_only,
      safe_read_only,
      head,
      safe_head,
      begin,
      flushed_until,
    }
  }

  /// 从切片前 56 字节尝试安全解码快照（const fn，不足 56 字节返回 None）
  #[inline(always)]
  pub const fn decode_opt(src: &[u8]) -> Option<Self> {
    if let Some(bytes) = src.first_chunk::<{ Self::SNAPSHOT_SIZE }>() {
      Some(Self::from_bytes(*bytes))
    } else {
      None
    }
  }

  /// 构造新的地址快照
  #[inline]
  pub const fn new(
    begin: u64,
    safe_head: u64,
    head: u64,
    safe_read_only: u64,
    read_only: u64,
    tail: u64,
    flushed_until: u64,
  ) -> Self {
    Self {
      tail,
      read_only,
      safe_read_only,
      head,
      safe_head,
      begin,
      flushed_until,
    }
  }

  /// 基于核心边界构建快照（safe_head 与 safe_read_only 对齐各自边界）
  #[inline]
  pub const fn from_bounds(
    begin: u64,
    head: u64,
    flushed_until: u64,
    read_only: u64,
    tail: u64,
  ) -> Self {
    Self {
      tail,
      read_only,
      safe_read_only: read_only,
      head,
      safe_head: head,
      begin,
      flushed_until,
    }
  }

  /// 校验快照状态机不变式：
  /// 0 <= begin <= safe_head <= head <= safe_read_only <= read_only <= tail
  /// 且 flushed_until <= tail，head <= flushed_until（确保已从内存驱逐的数据必已落盘）
  #[inline]
  pub const fn validate(&self) -> bool {
    self.begin <= self.safe_head
      && self.safe_head <= self.head
      && self.head <= self.safe_read_only
      && self.safe_read_only <= self.read_only
      && self.read_only <= self.tail
      && self.flushed_until <= self.tail
      && self.head <= self.flushed_until
  }

  /// 判断地址是否落在可变区（Mutable Region）
  #[inline]
  pub const fn is_mutable(&self, addr: u64) -> bool {
    let effective_ro = if self.read_only > self.head {
      self.read_only
    } else {
      self.head
    };
    addr >= effective_ro && addr < self.tail
  }

  /// 判断地址是否落在内存只读区（ReadOnly Region）
  #[inline]
  pub const fn is_read_only(&self, addr: u64) -> bool {
    addr >= self.head && addr < self.read_only
  }

  /// 判断地址是否驻留在内存环形缓冲区中（InMemory）
  #[inline]
  pub const fn is_in_memory(&self, addr: u64) -> bool {
    addr >= self.head && addr < self.tail
  }

  /// 判断地址是否在磁盘区（OnDisk）
  #[inline]
  pub const fn is_on_disk(&self, addr: u64) -> bool {
    addr >= self.begin && addr < self.head
  }

  /// 判断地址是否在有效范围内（Valid）
  #[inline]
  pub const fn is_valid(&self, addr: u64) -> bool {
    addr >= self.begin && addr < self.tail
  }
}
