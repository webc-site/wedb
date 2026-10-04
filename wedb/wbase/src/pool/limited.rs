//! 网络分级定长缓冲区池化管理
//!
//! 1:1 对标微软 Garnet LimitedFixedBufferPool（libs/common/Memory/LimitedFixedBufferPool.cs）
//!
//! 内部管理为分级队列数组：第 i 层承载容量 `min_allocation_size << i` 的定长缓冲
//! （对标 C# `queue[i] contains memory segments each of size (2^i * sectorSize)`，
//! LimitedFixedBufferPool.cs:16-21 类注释）。借出时按请求尺寸向上对齐定位层级弹出复用，
//! 归还时按缓冲实际容量精确匹配层级回池，仅越界（超最高层级或非 2 的幂次）缓冲走系统释放，
//! 使 64KB~512KB 区间内扩容与收敛的收发缓冲均能无损池化复用。
//!
//! 队列选型：C# 侧每层池为 `ConcurrentQueue<PoolEntry>`（libs/common/Memory/PoolLevel.cs:16），
//! 借出/归还是每 I/O 两次的纯非阻塞 TryDequeue/Enqueue（LimitedFixedBufferPool.cs:158/:120），
//! 竞争激烈且无需异步等待 → crossfire::flavor::Array（有界 MPMC 环，`Queue` trait 的
//! push/pop 即 TryDequeue/Enqueue 的零 CAS 重试等价物），环满裁断对应 C#
//! `Interlocked.Increment(size) <= maxEntriesPerLevel` 的入池上限（LimitedFixedBufferPool.cs:117）。
//!
//! 预算参与面（PR #2157，LimitedFixedBufferPool.cs 的 budget 形参与 Return 压力门）：
//! 池持可选 [`NetworkBufferBudget`]，借出/归还各记账一次（活跃计数随池活跃字节
//! 计数的两条路径移动）；预算绑定期间归还的超目标条目按条目自身方向对照目标，
//! 超即弃置不回池——空闲链上的超规格闲置块是活跃连接需要的钉死内存。

use std::{
  fmt,
  ops::{Deref, DerefMut},
  sync::{
    Arc,
    atomic::{AtomicI64, AtomicUsize, Ordering::Relaxed},
  },
};

use crossfire::flavor::{Array, Queue};

use super::net_budget::{BufferKind, NetworkBufferBudget};
use crate::align::CachePadded;

/// 默认基础（最低层级）网络缓冲区规格：64KB
pub const DEFAULT_BUFFER_SIZE: usize = 1 << 16;
/// 每层级最大常驻闲置缓冲数（对标 C# 构造函数 maxEntriesPerLevel 默认值 16）
pub const DEFAULT_MAX_ENTRIES_PER_LEVEL: usize = 16;
/// 默认层级数（对标 C# 构造函数 numLevels 默认值 4，即 min << 0..3 共 4 档）
pub const DEFAULT_NUM_LEVELS: usize = 4;

/// 池化缓冲区句柄（零 Arc 开销，RAII 自动归还；对标 C# `PoolEntry` 的
/// Get/Return 单一借还口）
pub struct PooledRefBuffer<'a> {
  buffer: Option<Vec<u8>>,
  /// TLS 读空闲段初始化代际（见 [`Self::prime_key`]）
  prime_key: Option<(usize, usize)>,
  /// 借出方向（预算回收对照目标用；对标 C# `PoolEntry.source` 低字节的
  /// 缓冲类型折叠）
  kind: BufferKind,
  /// 本句柄已计入 live_bytes 的字节数（借出时入账容量的镜像；块在借出期
  /// 可就地扩缩容，归还按本镜像对称出账）
  live_tag: usize,
  pool: &'a LimitedFixedBufferPool,
}

impl<'a> PooledRefBuffer<'a> {
  /// 获取底层 Vec 引用
  #[inline]
  pub fn vec_ref(&self) -> &Vec<u8> {
    self.buffer.as_ref().expect("pooled buffer active")
  }

  /// 获取底层可变 Vec 引用
  #[inline]
  pub fn vec_mut(&mut self) -> &mut Vec<u8> {
    self.buffer.as_mut().expect("pooled buffer active")
  }

  /// 临时提取底层 Vec 用于所有权转移（如异步 I/O），后续须通过 [`Self::set_buffer`] 归还
  #[inline]
  pub fn take_buffer(&mut self) -> Option<Vec<u8>> {
    self.buffer.take()
  }

  /// 归还或更新底层 Vec
  #[inline]
  pub fn set_buffer(&mut self, buf: Vec<u8>) {
    self.buffer = Some(buf);
  }

  /// TLS 读空闲段初始化代际（键随同块缓冲的借还转存：槽为本连接唯一
  /// 借还锚点，键由借出方在读毕回写、下次借出取用，同一块缓冲跨读免重清零）
  #[inline]
  pub fn prime_key(&self) -> Option<(usize, usize)> {
    self.prime_key
  }

  /// 回写初始化代际（键须描述本次借出的同一块缓冲；换块必清，键-缓冲同生命周期）
  #[inline]
  pub fn set_prime_key(&mut self, key: Option<(usize, usize)>) {
    self.prime_key = key;
  }

  /// 借出方向
  #[inline]
  pub fn kind(&self) -> BufferKind {
    self.kind
  }

  /// 块在借出期被就地扩缩容后的活跃字节重对账单点（写出段 shrink_to_base
  /// 收口的调用面）：C# 每次换块都是显式 Get/Return 记账，rust Vec 就地变容
  /// 无事务点，以此把 [`Self::live_tag`] 与当前容量重新对齐，归还出账对称
  #[inline]
  pub fn resync_live_bytes(&mut self) {
    let Some(buf) = &self.buffer else { return };
    let cap = buf.capacity();
    if cap != self.live_tag {
      let delta = cap as i64 - self.live_tag as i64;
      self.pool.live_bytes.fetch_add(delta, Relaxed);
      self.pool.update_peak_live_bytes();
      self.live_tag = cap;
    }
  }

  /// 本方向的基准规格（send 借出对照 send 地板；预算缺省即池 send 规格）
  #[inline]
  pub fn base_size(&self) -> usize {
    self
      .pool
      .base_size_for(self.kind, self.pool.send_buffer_size)
  }
}

impl<'a> Deref for PooledRefBuffer<'a> {
  type Target = Vec<u8>;
  #[inline]
  fn deref(&self) -> &Self::Target {
    self.vec_ref()
  }
}

impl<'a> DerefMut for PooledRefBuffer<'a> {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.vec_mut()
  }
}

impl<'a> Drop for PooledRefBuffer<'a> {
  fn drop(&mut self) {
    // 归还逻辑对标 C# `Return`（LimitedFixedBufferPool.cs:107-128）：
    // 按容量精确匹配层级，命中则清零回池，越界（超最高层级）就地系统释放，
    // 环满（超出 maxEntriesPerLevel）push 失败同样就地释放；
    // 提取后未归还（take_buffer 在途丢弃）仅递减借出计数。
    // C# 块容量恒为层级规格（响应缓冲从不扩容），rust Vec 扩容可产生层级
    // 域内的非 2 幂容量残留——写出口 shrink_to_base 换块失败（try_reserve
    // 失败退化）时该残留若直接弃置即净丢基准层级配额（突发蚕食形态），故
    // 层级域内残留换精确基准新块自愈后再入池；越界与换块仍失败才就地释放
    let _ = self.pool.borrowed_count.fetch_sub(1, Relaxed);
    self
      .pool
      .live_bytes
      .fetch_sub(self.live_tag as i64, Relaxed);
    if let Some(b) = self.pool.budget.as_ref() {
      b.on_buffer_released();
    }
    if let Some(mut buf) = self.buffer.take() {
      let cap = buf.capacity();
      let mut level = self.pool.position(cap);
      if level.is_none()
        && (self.pool.min_allocation_size..self.pool.max_allocation_size).contains(&cap)
      {
        let mut fresh = Vec::new();
        if fresh.try_reserve(self.pool.min_allocation_size).is_ok() {
          buf = fresh;
          level = self.pool.position(buf.capacity());
        }
      }
      if let Some(level) = level {
        // 预算绑定期间的超目标闲置条目即钉死内存：弃置不回池（对标
        // LimitedFixedBufferPool.cs Return 的 `budget.IsUnderPressure &&
        // length > TargetSizeFor(buffer.source)` 门）；非压力照常回池，
        // 短命连接的中等负载缓冲保留复用
        let over_target = self
          .pool
          .budget
          .as_ref()
          .is_some_and(|b| b.is_under_pressure() && cap > b.target_size_for(self.kind));
        // 字节上限判定以加后的新值为准（C# Add 返回新值后 `<= maxPooledBytes`）；
        // 三条不入池臂（超目标 / 超字节上限 / 环满）都不得残留记账
        if over_target {
          // 弃置不回池，无字节记账
        } else if self.pool.pooled_bytes.fetch_add(cap as i64, Relaxed) + cap as i64
          <= self.pool.max_pooled_bytes
        {
          buf.clear();
          if self.pool.levels[level].push(buf).is_err() {
            self.pool.pooled_bytes.fetch_sub(cap as i64, Relaxed);
          }
        } else {
          self.pool.pooled_bytes.fetch_sub(cap as i64, Relaxed);
        }
      }
    }
  }
}

impl<'a> fmt::Debug for PooledRefBuffer<'a> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("PooledRefBuffer")
      .field("len", &self.buffer.as_ref().map_or(0, Vec::len))
      .field(
        "capacity",
        &self.buffer.as_ref().map(|v| v.capacity()).unwrap_or(0),
      )
      .finish()
  }
}

/// 固定规格分级网络缓冲池（基于 crossfire::flavor::Array 纯原子无锁架构）
pub struct LimitedFixedBufferPool {
  /// 第 i 层承载容量 `min_allocation_size << i` 的闲置缓冲队列
  /// （对标 C# `PoolLevel[] pool`，层级预建，池级对象仅数字节环成本）
  levels: Box<[Array<Vec<u8>>]>,
  min_allocation_size: usize,
  /// send 借出基准规格（C# `NetworkBufferSettings.sendBufferSize`——send 缓冲
  /// 定长，借出与低水位收敛共用此锚点）
  send_buffer_size: usize,
  max_entries_per_level: usize,
  num_levels: usize,
  /// 池可承载的最大规格：`min_allocation_size << (num_levels - 1)`
  /// （对标 C# `maxAllocationSize`，LimitedFixedBufferPool.cs:29）
  max_allocation_size: usize,
  /// 全层级闲置字节总上限（C# `maxPooledBytes`；0 构造时按每层级条目界派生
  /// ——与环容量乘积等值即惰性）
  max_pooled_bytes: i64,
  /// 进程级活跃缓冲预算（C# 构造形参 `budget`；None = 不参与，对应 C#
  /// `NetworkBufferBudget.Disabled` 单例的惰性形态）
  budget: Option<Arc<NetworkBufferBudget>>,
  allocated_count: CachePadded<AtomicUsize>,
  /// 越界（超出最高层级）分配累计数（对标 C# `totalOutOfBoundAllocations`）
  out_of_bound_allocations: CachePadded<AtomicUsize>,
  pub(crate) borrowed_count: CachePadded<AtomicUsize>,
  /// 调用方借出中的字节数（C# `liveBytes`——随连接数伸缩的足迹部分）
  live_bytes: CachePadded<AtomicI64>,
  /// [`Self::live_bytes`] 高水位（C# `peakLiveBytes`）
  peak_live_bytes: CachePadded<AtomicI64>,
  /// 空闲链常驻字节数（C# `pooledBytes`）
  pooled_bytes: CachePadded<AtomicI64>,
}

impl fmt::Debug for LimitedFixedBufferPool {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("LimitedFixedBufferPool")
      .field("min_allocation_size", &self.min_allocation_size)
      .field("max_allocation_size", &self.max_allocation_size)
      .field("num_levels", &self.num_levels)
      .field("max_entries_per_level", &self.max_entries_per_level)
      .field("borrowed_count", &self.borrowed_count())
      .field("free_count", &self.free_count())
      .finish()
  }
}

impl LimitedFixedBufferPool {
  /// 创建分级网络缓冲池（对标 C# 构造函数
  /// `LimitedFixedBufferPool(minAllocationSize, maxEntriesPerLevel = 16, numLevels = 4)`，
  /// LimitedFixedBufferPool.cs:67；本签名对应 C# 省略默认层级参数的调用形，恒用
  /// [`DEFAULT_NUM_LEVELS`]，0 入参回退默认值；不参与预算）
  pub fn new(min_allocation_size: usize, max_entries_per_level: usize) -> Arc<Self> {
    Self::with_levels(
      min_allocation_size,
      max_entries_per_level,
      DEFAULT_NUM_LEVELS,
    )
  }

  /// 全参构造（对标 C# LimitedFixedBufferPool 构造函数显式传 numLevels 的
  /// 形态；网络规格推导层级数的建池入口见
  /// [`super::NetworkBufferSettings::create_buffer_pool`]）
  ///
  /// 在 garnet 中的相对路径:libs/common/Memory/LimitedFixedBufferPool.cs:LimitedFixedBufferPool
  fn with_levels(
    min_allocation_size: usize,
    max_entries_per_level: usize,
    num_levels: usize,
  ) -> Arc<Self> {
    Self::with_geometry(
      min_allocation_size,
      min_allocation_size,
      max_entries_per_level,
      num_levels,
      0,
      None,
    )
  }

  /// 全几何构造（send 基准、预算参与与闲置字节上限齐备的装配面；
  /// [`super::NetworkBufferSettings::create_buffer_pool`] 的唯一底座）
  pub(crate) fn with_geometry(
    min_allocation_size: usize,
    send_buffer_size: usize,
    max_entries_per_level: usize,
    num_levels: usize,
    max_pooled_bytes: i64,
    budget: Option<Arc<NetworkBufferBudget>>,
  ) -> Arc<Self> {
    // 分级体系刚性要求 2 的幂次基础规格（C# GetLevel 以 Debug.Assert(IsPow2) 前置，
    // 此处向上圆整为运行期自愈），保证层规格恒为 2 的幂、归还端等值命中
    let min = min_allocation_size.max(1).next_power_of_two();
    let entries = if max_entries_per_level == 0 {
      DEFAULT_MAX_ENTRIES_PER_LEVEL
    } else {
      max_entries_per_level
    };
    // 至少 1 级，避免 `min << (n - 1)` 在 0 级下移位饱和出无意义规格域
    let num_levels = num_levels.max(1);
    let max_allocation_size = min << (num_levels - 1);
    // 派生界（C# 构造 0 值时 `maxEntriesPerLevel × Σ 各层规格`）：与环容量
    // 乘积等值，即每层级条目界已完整表达的惰性上限
    let max_pooled_bytes = if max_pooled_bytes > 0 {
      max_pooled_bytes
    } else {
      (0..num_levels)
        .map(|i| entries as i64 * (min << i) as i64)
        .sum()
    };
    Arc::new(Self {
      levels: (0..num_levels)
        .map(|_| Array::new(entries))
        .collect::<Vec<_>>()
        .into_boxed_slice(),
      min_allocation_size: min,
      send_buffer_size: send_buffer_size.max(min),
      max_entries_per_level: entries,
      num_levels,
      max_allocation_size,
      max_pooled_bytes,
      budget,
      allocated_count: CachePadded::new(AtomicUsize::new(0)),
      out_of_bound_allocations: CachePadded::new(AtomicUsize::new(0)),
      borrowed_count: CachePadded::new(AtomicUsize::new(0)),
      live_bytes: CachePadded::new(AtomicI64::new(0)),
      peak_live_bytes: CachePadded::new(AtomicI64::new(0)),
      pooled_bytes: CachePadded::new(AtomicI64::new(0)),
    })
  }

  /// 层级规格：`min_allocation_size << level`（对标 C# `minAllocationSize << i`）
  #[inline]
  fn level_size(&self, level: usize) -> usize {
    self.min_allocation_size << level
  }

  /// 规格 → 层级索引映射（对标 C# `Position` + `GetLevel`，
  /// LimitedFixedBufferPool.cs:271-292）：仅接受不低于基础规格、不超最高层级
  /// 且为 2 的幂次的规格；层级 = 比值 2 的幂指数差，常数阶位运算
  #[inline]
  fn position(&self, size: usize) -> Option<usize> {
    if size < self.min_allocation_size || !size.is_power_of_two() {
      return None;
    }
    let level = Self::get_level(self.min_allocation_size, size);
    (level < self.num_levels).then_some(level)
  }

  /// 由基础规格与目标规格求层级（对标 C# 静态 `GetLevel`：
  /// ratio == 1 → 0，否则 Log2(ratio - 1) + 1；调用方保证双 2 的幂且 ratio >= 1）
  pub(crate) fn get_level(min_allocation_size: usize, requested_size: usize) -> usize {
    let ratio = requested_size / min_allocation_size;
    if ratio <= 1 {
      0
    } else {
      (ratio - 1).ilog2() as usize + 1
    }
  }

  #[inline]
  fn alloc_or_pop(&self, min_size: usize) -> Vec<u8> {
    let _ = self.borrowed_count.fetch_add(1, Relaxed);
    if let Some(b) = self.budget.as_ref() {
      b.on_buffer_acquired();
    }
    // 对标 C# `Get`：请求规格向上对齐至所属层级（低于基础规格并入 0 级），
    // 保证池中块容量恒为层级规格、归还端可无损复配
    let aligned = min_size.max(self.min_allocation_size).next_power_of_two();
    let buf = if aligned > self.max_allocation_size {
      // 越界分配（totalOutOfBoundAllocations 计数臂）：精确按需走系统堆，不入分级域
      self.out_of_bound_allocations.fetch_add(1, Relaxed);
      self.allocated_count.fetch_add(1, Relaxed);
      Vec::with_capacity(min_size)
    } else {
      let level = Self::get_level(self.min_allocation_size, aligned);
      match self.levels[level].pop() {
        Some(v) => {
          let _ = self
            .pooled_bytes
            .fetch_sub(self.level_size(level) as i64, Relaxed);
          v
        }
        None => {
          self.allocated_count.fetch_add(1, Relaxed);
          Vec::with_capacity(self.level_size(level))
        }
      }
    };
    // 活跃字节记账按实际容量（分配器可超额）
    self.live_bytes.fetch_add(buf.capacity() as i64, Relaxed);
    self.update_peak_live_bytes();
    buf
  }

  /// 借出一个覆盖指定最小容量需求的缓冲区句柄（对标 C# `Get`，唯一生产借出口；
  /// 方向默认 receive）
  #[inline]
  pub fn get_ref(&self, min_size: usize) -> PooledRefBuffer<'_> {
    self.get_ref_kind(min_size, BufferKind::Receive)
  }

  /// 按方向借出缓冲区句柄（对标 C# `Get(int size, PoolEntryBufferType bufferType)`）
  #[inline]
  pub fn get_ref_kind(&self, min_size: usize, kind: BufferKind) -> PooledRefBuffer<'_> {
    let buffer = self.alloc_or_pop(min_size);
    PooledRefBuffer {
      live_tag: buffer.capacity(),
      buffer: Some(buffer),
      prime_key: None,
      kind,
      pool: self,
    }
  }

  /// 高水位推进（C# `UpdatePeakLiveBytes` 的 CAS 循环在 rust 折为 fetch_max）
  #[inline]
  fn update_peak_live_bytes(&self) {
    let live = self.live_bytes.load(Relaxed);
    self.peak_live_bytes.fetch_max(live, Relaxed);
  }

  /// 清空全部分级队列中闲置缓冲区（对标 C# `Purge`，LimitedFixedBufferPool.cs:184-193）
  pub fn purge(&self) {
    for level in self.levels.iter() {
      while let Some(v) = level.pop() {
        let _ = self.pooled_bytes.fetch_sub(v.capacity() as i64, Relaxed);
      }
    }
  }

  /// 当前池中全层级闲置可用缓冲区总数
  #[inline]
  pub fn free_count(&self) -> usize {
    self.levels.iter().map(Array::len).sum()
  }

  /// 当前借出中的缓冲区数量
  #[inline]
  pub fn borrowed_count(&self) -> usize {
    self.borrowed_count.load(Relaxed)
  }

  /// 累计分配的新缓冲区总数
  #[inline]
  pub fn allocated_count(&self) -> usize {
    self.allocated_count.load(Relaxed)
  }

  /// 累计越界（超最高层级）分配数（对标 C# `totalOutOfBoundAllocations`）
  #[inline]
  pub fn out_of_bound_allocations(&self) -> usize {
    self.out_of_bound_allocations.load(Relaxed)
  }

  /// 调用方借出中的字节数（对标 C# `LiveBytes`）
  #[inline]
  pub fn live_bytes(&self) -> i64 {
    self.live_bytes.load(Relaxed)
  }

  /// 借出字节高水位（对标 C# `peakLiveBytes`）
  #[inline]
  pub fn peak_live_bytes(&self) -> i64 {
    self.peak_live_bytes.load(Relaxed)
  }

  /// 空闲链常驻字节数（对标 C# `PooledBytes`）
  #[inline]
  pub fn pooled_bytes(&self) -> i64 {
    self.pooled_bytes.load(Relaxed)
  }

  /// 参与的进程级活跃缓冲预算（None = 不参与）
  #[inline]
  pub fn budget(&self) -> Option<&Arc<NetworkBufferBudget>> {
    self.budget.as_ref()
  }

  /// send 借出基准规格（预算钳制施加面：C# `BaseSendBufferSize` =
  /// `budget.ClampSendBufferSize(configuredSendBufferSize)`，NetworkHandler.cs:53）
  #[inline]
  pub fn send_base_size(&self) -> usize {
    self.base_size_for(BufferKind::Send, self.send_buffer_size)
  }

  /// receive 基准规格（`configured` 为会话侧配置的接收基准——预算钳制施加面，
  /// C# `BaseReceiveBufferSize`；预算缺省原样返回）
  #[inline]
  pub fn recv_base_size(&self, configured: usize) -> usize {
    self.base_size_for(BufferKind::Receive, configured)
  }

  #[inline]
  fn base_size_for(&self, kind: BufferKind, configured: usize) -> usize {
    match self.budget.as_ref() {
      Some(b) => match kind {
        BufferKind::Send => b.clamp_send_buffer_size(configured),
        BufferKind::Receive => b.clamp_receive_buffer_size(configured),
      },
      None => configured,
    }
  }

  /// 池基准定长块容量（借还契约唯一锚点：网络泵从本池借出并归还的块容量
  /// 须恒等于此值，归还端据此精准配平层级）
  ///
  /// 在 garnet 中的相对路径:libs/common/Memory/LimitedFixedBufferPool.cs:MinAllocationSize
  /// （C# NetworkHandler.cs:105 即以该访问器单点读取池基准规格构造接收缓冲）
  #[inline]
  pub fn buffer_size(&self) -> usize {
    self.min_allocation_size
  }

  /// 池统计串（统计输出单点，格式串全仓仅此一处；
  /// rust format! 模板仅接受字面量，无法引用常量）
  ///
  /// 在 garnet 中的相对路径:libs/common/Memory/LimitedFixedBufferPool.cs:GetStats
  pub fn get_stats(&self) -> String {
    let mut stats = format!(
      "borrowed_buffers={} num_levels={} max_entries_per_level={} min_allocation_size={} max_allocation_size={} live_bytes={} peak_live_bytes={} pooled_bytes={} max_pooled_bytes={} total_free_buffers={} allocated_buffers={} out_of_bound_allocations={}",
      self.borrowed_count(),
      self.num_levels,
      self.max_entries_per_level,
      self.min_allocation_size,
      self.max_allocation_size,
      self.live_bytes(),
      self.peak_live_bytes(),
      self.pooled_bytes(),
      self.max_pooled_bytes,
      self.free_count(),
      self.allocated_count(),
      self.out_of_bound_allocations(),
    );
    // 各层级闲置分布（对标 C# GetStats 的 `,{items}={Format.MemoryBytes(size)}` 尾段）
    for (level, queue) in self.levels.iter().enumerate() {
      stats.push_str(&format!(
        " free_at_{}KB={}",
        self.level_size(level) >> 10,
        queue.len()
      ));
    }
    stats
  }
}
