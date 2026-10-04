use std::sync::atomic::{AtomicU64, Ordering};

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
  /// 物理删段地板（最近已发布检查点重放窗下界；0 = 未发布检查点全禁删）
  ///
  /// 「拍检查点才真正删文件」闸门（对标 C# libs/storage/Tsavorite/cs/src/core/
  /// Index/Recovery/Checkpoint.cs:58 CleanupLogCheckpoint 的
  /// `Log.ShiftBeginAddress(beginAddress, truncateLog: true)` 与
  /// LogAccessor.cs:133 "log will be truncated after the next checkpoint" 契约）：
  /// 检查点非自含，恢复重放窗 = meta.hlog_meta 的 [begin, tail)，仅窗下段方可
  /// 物理回收。移位链删段目标钳制到地板，越窗段延后到下一检查点发布
  /// （`HybridLog::release_history_until`）补收；逻辑 begin 推进不受钳制。
  /// 属运行期状态，不进 AddressSnapshot（恢复期按恢复检查点重设）
  pub delete_floor: AtomicU64,
  /// 快照读钉水位（扇区；u64::MAX = 无在途快照读者）
  ///
  /// 复制全量下发（STORE_HLOG 直读活设备）在途读者注册/注销时由复制层**前向**
  /// 写入本原子位（whlog 不反向依赖复制层）；删段目标经
  /// [AddressManager::effective_delete_floor] 与本水位取 min，地板提升内部封顶，
  /// 保证物理删除永远追不上最老在途读者的 begin。对应 C# 转写
  /// libs/cluster/Server/Replication/CheckpointStore.cs:196-210（活日志截断钳制到最老
  /// 活跃 reader 引用条目）。
  /// 属运行期状态，不进 AddressSnapshot（重启后无在途读者，恢复 MAX）。
  reader_pin: AtomicU64,
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
      delete_floor: AtomicU64::new(0),
      reader_pin: AtomicU64::new(u64::MAX),
    }
  }

  /// 判断地址是否落在可变区（Mutable Region: addr >= effective_ro && addr < tail）
  ///
  /// 区域判定体唯一真源在 [AddressSnapshot::region_mutable]，本方法只做原子加载后委托。
  #[inline]
  pub fn is_mutable(&self, addr: u64) -> bool {
    let tail = self.tail_address.load(Ordering::Acquire);
    let ro = self.read_only_address.load(Ordering::Acquire);
    let head = self.head_address.load(Ordering::Acquire);
    AddressSnapshot::region_mutable(addr, head, ro, tail)
  }

  /// 判断地址是否落在内存只读区（ReadOnly Region: addr >= head && addr < read_only）
  #[inline]
  pub fn is_read_only(&self, addr: u64) -> bool {
    let ro = self.read_only_address.load(Ordering::Acquire);
    let head = self.head_address.load(Ordering::Acquire);
    AddressSnapshot::region_read_only(addr, head, ro)
  }

  /// 判断地址是否驻留在内存环形缓冲区中（InMemory: addr >= head && addr < tail）
  #[inline]
  pub fn is_in_memory(&self, addr: u64) -> bool {
    let tail = self.tail_address.load(Ordering::Acquire);
    let head = self.head_address.load(Ordering::Acquire);
    AddressSnapshot::region_in_memory(addr, head, tail)
  }

  /// 判断地址是否已落盘或仅存在于磁盘区（OnDisk: addr >= begin && addr < head）
  #[inline]
  pub fn is_on_disk(&self, addr: u64) -> bool {
    let head = self.head_address.load(Ordering::Acquire);
    let begin = self.begin_address.load(Ordering::Acquire);
    AddressSnapshot::region_on_disk(addr, begin, head)
  }

  /// 判断地址是否在有效范围内（Valid: addr >= begin && addr < tail）
  #[inline]
  pub fn is_valid(&self, addr: u64) -> bool {
    let tail = self.tail_address.load(Ordering::Acquire);
    let begin = self.begin_address.load(Ordering::Acquire);
    AddressSnapshot::region_valid(addr, begin, tail)
  }

  /// 单调推进 ReadOnlyAddress
  ///
  /// C# CAS 单调推进原语（ref CAS 失败取旧值重试）的 fetch_max 单点对应，
  /// 全部地址滑动（shift_safe_read_only/head/safe_head）同构：
  /// libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs:MonotonicUpdate
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

  /// 获取物理删段地板（移位链删段目标的钳制上界）
  ///
  /// 删段安全系复合判据，非单靠双向互检：发布侧「store floor（
  /// [Self::raise_delete_floor]）→ load pin」与注册侧「store pin → load floor」
  /// 两两反序，全 SeqCst 下两侧同时漏见仍可在全序中排列，互检自身不充分。
  /// 承重两臂：其一，读者漏见抬升（其 load floor 早于发布 store floor）则由
  /// 程序序其 store pin 更早，发布侧物理删段目标动作点经
  /// [Self::effective_delete_floor] 重读 pin 必见，删段目标被钳到不高于其
  /// begin；其二，读者未漏见则经地板复核退圈。本读弱序化即拆掉两臂的
  /// 可见性地基，禁降档
  #[inline]
  pub fn delete_floor(&self) -> u64 {
    self.delete_floor.load(Ordering::SeqCst)
  }

  /// 单调抬升物理删段地板（检查点发布 / 恢复装配点专用）
  ///
  /// 内部以读钉水位封顶（SeqCst）：目标值 = floor.min(当前 pin)。在途读者
  /// 尚未 store 时本方读到旧 pin=MAX 不受限——该漏见臂由删段目标动作点
  /// 重读 pin 兜底（见 [Self::delete_floor] 复合判据，动作点不得传递此处
  /// 算好的目标值）；读者 store 先于本方 load 时目标即被钳到不高于其 begin。
  #[inline]
  pub fn raise_delete_floor(&self, floor: u64) {
    let target = floor.min(self.reader_pin.load(Ordering::SeqCst));
    self.delete_floor.fetch_max(target, Ordering::SeqCst);
  }

  /// 生效删段地板：地板与读钉取 min。本读取是删段安全复合判据的兜底臂
  /// （见 [Self::delete_floor]）：物理删段目标必须在 raise 之后经此处重读
  /// pin，不得传递 raise 时算好的目标值——读者漏见抬升场景的唯一防线即
  /// 动作点重读。
  /// 全部设备级截断调用点必须经本方法取目标，不得直读 delete_floor。
  #[inline]
  pub fn effective_delete_floor(&self) -> u64 {
    let floor = self.delete_floor.load(Ordering::SeqCst);
    let pin = self.reader_pin.load(Ordering::SeqCst);
    floor.min(pin)
  }

  /// 读当前快照读钉水位（u64::MAX = 无在途快照读者）
  #[inline]
  pub fn reader_pin(&self) -> u64 {
    self.reader_pin.load(Ordering::Acquire)
  }

  /// 前向写快照读钉水位（复制层在读者注册/注销时写入重算后的聚合值）。
  /// 水位非单调（注销可回抬），故直接 store；写序 SeqCst 参与删钳复合判据
  /// （见 [Self::delete_floor]）。
  #[inline]
  pub fn set_reader_pin(&self, pin: u64) {
    self.reader_pin.store(pin, Ordering::SeqCst);
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
      delete_floor: AtomicU64::new(0),
      reader_pin: AtomicU64::new(u64::MAX),
    }
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

/// 地址状态快照（只读视图，纯内存传递；磁盘持久化由 wcpr::HlogMeta 独立结构承担）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

  /// 可变区判定核（Mutable Region: `addr >= max(head, read_only) && addr < tail`）
  ///
  /// 五个 `region_*` 核是全部区域边界的唯一真源：只吃纯边界值，[AddressManager] 的
  /// live 谓词（原子加载后委托）与本快照的 `is_*` 谓词（字段直读后委托）共用同一判定体，
  /// 边界不变式（含 `effective_ro` 对 head 的钳制）改动只需一点。
  ///
  /// 传入的边界值只允许偏旧（单调地址只增不减，热路径可自带快照消除重复 Acquire 加载）：
  /// 偏旧使 `effective_ro` 偏低，只会把只读区误判为可变区而走保守读锁路径，
  /// 绝不会把可变区误判为只读区（那将走无锁裸读遭遇在途编码撕裂），判定方向天然安全。
  #[inline]
  pub const fn region_mutable(addr: u64, head: u64, read_only: u64, tail: u64) -> bool {
    let effective_ro = if read_only > head { read_only } else { head };
    addr >= effective_ro && addr < tail
  }

  /// 内存只读区判定核（ReadOnly Region: `addr >= head && addr < read_only`）
  #[inline]
  pub const fn region_read_only(addr: u64, head: u64, read_only: u64) -> bool {
    addr >= head && addr < read_only
  }

  /// 内存驻留区判定核（InMemory: `addr >= head && addr < tail`）
  #[inline]
  pub const fn region_in_memory(addr: u64, head: u64, tail: u64) -> bool {
    addr >= head && addr < tail
  }

  /// 磁盘区判定核（OnDisk: `addr >= begin && addr < head`）
  #[inline]
  pub const fn region_on_disk(addr: u64, begin: u64, head: u64) -> bool {
    addr >= begin && addr < head
  }

  /// 有效区判定核（Valid: `addr >= begin && addr < tail`）
  #[inline]
  pub const fn region_valid(addr: u64, begin: u64, tail: u64) -> bool {
    addr >= begin && addr < tail
  }

  /// 判断地址是否落在可变区（Mutable Region）
  #[inline]
  pub const fn is_mutable(&self, addr: u64) -> bool {
    Self::region_mutable(addr, self.head, self.read_only, self.tail)
  }

  /// 判断地址是否落在内存只读区（ReadOnly Region）
  #[inline]
  pub const fn is_read_only(&self, addr: u64) -> bool {
    Self::region_read_only(addr, self.head, self.read_only)
  }

  /// 判断地址是否驻留在内存环形缓冲区中（InMemory）
  #[inline]
  pub const fn is_in_memory(&self, addr: u64) -> bool {
    Self::region_in_memory(addr, self.head, self.tail)
  }

  /// 判断地址是否在磁盘区（OnDisk）
  #[inline]
  pub const fn is_on_disk(&self, addr: u64) -> bool {
    Self::region_on_disk(addr, self.begin, self.head)
  }

  /// 判断地址是否在有效范围内（Valid）
  #[inline]
  pub const fn is_valid(&self, addr: u64) -> bool {
    Self::region_valid(addr, self.begin, self.tail)
  }
}
