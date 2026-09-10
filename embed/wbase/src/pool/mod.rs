//! 扇区对齐缓冲池 (对标 C# Tsavorite `SectorAlignedBufferPool.OriginReturn.cs`)
//!
//! 1:1 对标微软 Garnet 官方工业级 Origin-Return（源归还）三级缓存体系与双层预算架构：
//!
//! - **三级阶梯缓存架构 (0 锁热路径)**：
//!   1. **L1 线程本地私有栈 (Thread-Local Cache)**：对标 C# `Bucket.localHead`。
//!      本线程借还小容量缓冲区走纯指针操作，**0 锁、0 原子操作、< 1ns 极速借还**，覆盖 90%+ 热点路径；
//!   2. **L2 跨线程 MPSC 无锁单向栈 (Cross-Thread Inbox)**：对标 C# `Bucket.crossThreadHead`。
//!      异线程归还（如 compio 异步 I/O 完成 worker 线程）直接通过单次 CAS 推入属主收件箱；
//!      属主线程在 L1 耗尽时执行单次原子 `swap` **整链批量收割 (Claim)**，无 ABA 隐患；
//!   3. **L3 全局条带仓库 (Striped Depot)**：对标 C# `DepotStripe[]`。
//!      每个 class 拥有 8 个独立条带锁（28×8=224 条带），大容量缓冲（>256KB）直接走全局条带共享
//!      （避免多线程膨胀），并承载 L1/L2 溢出与线程退出时的安全回池与工作窃取；
//!      条带锁保证「关闭标志 + 推入」原子（对标 C# lock + closed 设计），`Free` 与并发归还
//!      竞态时迟到的推入必然失败并就地释放许可，绝不滞留配额。
//! - **分级容量表 1:1 对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:ClassCapacitySectors/`ClassOfSectors`**：
//!   2 个精确级 (512B/1KB) + 4 个线性级 (2KB..8KB，步长 2KB) + 22 个几何级
//!   (每倍频 2 级：1.5x/2x，8KB..16MB@512B 扇区)；超出 [`MAX_POOLED_SECTORS`] 走精确容量 bypass 分配。
//! - **归还清零策略 (对标 C# `clearOnReturn`)**：默认归还即清零；读目的地等覆写场景通过
//!   `clear_on_return = false` 免除归还清零，标记脏位由后续借方惰性清零，消除内存总线带宽瓶颈。
//! - **双层字节预算 (对标 C# small/large budget 与 `LargeTierMinBytes`)**：
//!   小缓冲（<= 256KB）与大缓冲（> 256KB）配额强隔离，`AtomicI64` 无锁原子记账。

mod aligned_buf;
mod budget;
mod depot;
mod inbox;
mod tls;

use std::{
  alloc::{Layout, dealloc},
  mem::forget,
  ptr::{self, NonNull, write_bytes},
  sync::{
    Arc,
    atomic::{
      AtomicBool, AtomicU64,
      Ordering::{AcqRel, Acquire, Relaxed},
    },
  },
};

pub use aligned_buf::AlignedBuf;
pub(crate) use budget::Budget;
pub(crate) use depot::Depot;
pub(crate) use inbox::{ChainIter, CrossThreadInbox, FreeNode};
pub(crate) use tls::TLS_POOLS;
pub use tls::current_thread_id;

use crate::error::{Error, Result};

#[inline]
pub(crate) fn validate_sector_size(size: usize) -> Result<()> {
  if !crate::align::is_valid_sector_size(size) {
    return Err(Error::InvalidAlignment(size, crate::align::MIN_SECTOR_SIZE));
  }
  Ok(())
}

/// size class 总数 = 2 精确 + 4 线性 + 2×11 几何 (对标 C# `NumClasses`)
pub const NUM_CLASSES: usize = 28;

const TINY_EXACT_CLASSES: usize = 2;
const LINEAR_STRIDE_SECTORS: usize = 4;
const STRIDE_CLASSES: usize = 4;
const LINEAR_CLASSES: usize = TINY_EXACT_CLASSES + STRIDE_CLASSES;
const LINEAR_TOP_SECTORS: usize = LINEAR_STRIDE_SECTORS * STRIDE_CLASSES;
const LOG2_LINEAR_TOP: usize = 4;
const GEOMETRIC_DOUBLINGS: usize = 11;

/// 可池化的最大扇区数 (32768 扇区；512B 扇区下即 16MB，对标 C# `MaxPooledSectors`)
pub const MAX_POOLED_SECTORS: usize = LINEAR_TOP_SECTORS << GEOMETRIC_DOUBLINGS;

/// 大额预算分层阈值：容量超过该值的 class 从 large 预算配额 (对标 C# `LargeTierMinBytes`)
pub const LARGE_TIER_MIN_BYTES: usize = 256 << 10;

/// 默认小缓冲预算 (小 class 合计可缓存字节数，32MiB；C# `ManagedBudgetBytes` 1GiB 的 1/4 为 256MiB，
/// 此处面向嵌入式场景整体缩小，隔离语义与 C# 一致)
pub const DEFAULT_SMALL_BUDGET_BYTES: i64 = 32 << 20;

/// 默认大缓冲预算 (大 class 合计可缓存字节数，128MiB)
pub const DEFAULT_LARGE_BUDGET_BYTES: i64 = 128 << 20;

/// 单 class 单线程本地最大缓存槽位数 (对标 C# ThreadShard 局部缓存；C# 以 per-thread 字节上限
/// `localByteCap` 计，此处以 per-class 槽位数计，总缓存字节仍由预算许可全局硬约束)
pub const MAX_LOCAL_PER_CLASS: usize = 64;

/// 全局条带仓库条带数 (2 的幂，对标 C# DepotStripes)
pub(crate) const DEPOT_STRIPES: usize = 8;
pub(crate) const DEPOT_STRIPE_MASK: usize = DEPOT_STRIPES - 1;

/// 全局条带单条带最大容量 (对标 C# DepotStripeCap；C# 为 1024，此处面向嵌入式场景缩小，
/// 单 class 缓存总量 = 条带数 × 容量 = 64，与 [`MAX_LOCAL_PER_CLASS`] 同量级)
pub const DEPOT_STRIPE_CAP: usize = 8;

/// 指定 class 的扇区容量 (对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:ClassCapacitySectors，1:1 算术)
///
/// 越界 class（`cls >= NUM_CLASSES`）为防御性处理：合法 class 的移位量 <= 15，
/// 越界 class 的移位量经上界守卫 + `checked_shl` 饱和为 `usize::MAX`，
/// 保证 const 求值不触发算术溢出 panic，也不因 `as u32` 截断产生伪小容量
#[inline]
#[must_use]
pub const fn class_capacity_sectors(cls: usize) -> usize {
  if cls < TINY_EXACT_CLASSES {
    cls + 1
  } else if cls < LINEAR_CLASSES {
    (cls - TINY_EXACT_CLASSES + 1) * LINEAR_STRIDE_SECTORS
  } else {
    let g = cls - LINEAR_CLASSES;
    let octave = LOG2_LINEAR_TOP.saturating_add(g >> 1);
    let (base, exp) = if g & 1 == 0 {
      (3usize, octave - 1)
    } else {
      (1usize, octave + 1)
    };
    if exp >= u32::MAX as usize {
      return usize::MAX;
    }
    match base.checked_shl(exp as u32) {
      Some(cap) => cap,
      None => usize::MAX,
    }
  }
}

/// 各 size class 容量 (扇区数) 编译期常量查找表
pub const CLASS_CAPACITIES_SECTORS: [usize; NUM_CLASSES] = {
  let mut arr = [0; NUM_CLASSES];
  let mut i = 0;
  while i < NUM_CLASSES {
    arr[i] = class_capacity_sectors(i);
    i += 1;
  }
  arr
};

/// 指定 class 在给定扇区大小下的字节容量 (越界 class 饱和不 panic)
#[inline]
#[must_use]
pub const fn class_capacity_bytes(cls: usize, sector_size: usize) -> usize {
  class_capacity_sectors(cls).saturating_mul(sector_size)
}

/// 按扇区数选择 size class；超出可池化范围返回 None (走 bypass，对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:ClassOfSectors)
#[inline]
#[must_use]
pub const fn class_of_sectors(sectors: usize) -> Option<usize> {
  let sectors = if sectors == 0 { 1 } else { sectors };
  if sectors <= TINY_EXACT_CLASSES {
    return Some(sectors - 1);
  }
  if sectors <= LINEAR_TOP_SECTORS {
    return Some(TINY_EXACT_CLASSES + (sectors - 1) / LINEAR_STRIDE_SECTORS);
  }
  if sectors > MAX_POOLED_SECTORS {
    return None;
  }
  let octave = (sectors - 1).ilog2() as usize;
  let mid = 3 << (octave - 1);
  let sub = (sectors > mid) as usize;
  Some(LINEAR_CLASSES + 2 * (octave - LOG2_LINEAR_TOP) + sub)
}

/// 缓存中的空闲缓冲描述符
///
/// 裸指针所有权语义：同一时刻恰有一个容器（本地栈 / 收件箱链 / Depot / 在途 AlignedBuf）持有，
/// 归还路由保证不重复登记，故仅需 `Send`（跨线程移交），无需 `Sync`。
pub(crate) struct CachedBuf {
  pub(crate) ptr: NonNull<u8>,
  pub(crate) cap: usize,
  pub(crate) align: usize,
  pub(crate) cacheable: bool,
  pub(crate) dirty: bool,
}

unsafe impl Send for CachedBuf {}

impl Drop for CachedBuf {
  fn drop(&mut self) {
    unsafe {
      dealloc(
        self.ptr.as_ptr(),
        Layout::from_size_align_unchecked(self.cap, self.align),
      )
    };
  }
}

/// 缓冲区的池归属路由元数据 (签发记账与 Origin-Return 归还共用)
pub(crate) struct BufMeta {
  /// 所属 size class
  pub(crate) cls: u32,
  /// 持有预算许可，归还时可入池
  pub(crate) cacheable: bool,
  /// 归还清零策略 (对标 C# `clearOnReturn`，默认 true)
  pub(crate) clear_on_return: bool,
  /// 签发时的有效需求长度 (非池化缓冲区恒等于容量)
  pub(crate) required: usize,
  /// 来源线程 ID (用于 Origin-Return 路由：同线程走 0 锁本地栈，异线程走无锁 CAS inbox)
  pub(crate) owner_tid: u64,
  /// 属主线程跨线程无锁收件箱
  pub(crate) inbox: Option<Arc<CrossThreadInbox>>,
}

/// 缓冲池运行统计快照（观测口径见各字段说明；对标 C# 无对应接口，属可观测性增强）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
  /// 当前已预留总字节数（含各线程缓存中的在借空闲，见 [`BufferPool::reserved_bytes`]）
  pub reserved_bytes: i64,
  /// 当前小缓冲已预留字节数
  pub small_reserved_bytes: i64,
  /// 当前大缓冲已预留字节数
  pub large_reserved_bytes: i64,
  /// 预算耗尽后显式直配（绕过池缓存，cacheable=false）的累计分配次数；
  /// 持续增长说明预算配小了或流量配错了大小层
  pub direct_alloc_count: u64,
  /// 预算耗尽后显式直配的累计字节数
  pub direct_alloc_bytes: u64,
  /// 超出可池化上限（或池已关闭）绕过池缓存的累计分配次数 (对标 C# `Stats.BypassAllocs`)
  pub bypass_alloc_count: u64,
  /// 绕过池缓存的累计分配字节数
  pub bypass_alloc_bytes: u64,
}

/// 扇区对齐缓冲池 (对标 C# Tsavorite `SectorAlignedBufferPool`)
///
/// C# 的静态调试开关 `Disabled`（绕池直配）与 `UnpinOnReturn`（归还解钉）无 Rust 对应：
/// 前者属诊断逃生门（预算与统计路径已可观测等效信息），后者服务 Windows 页面解钉
/// （Rust 分配器无 pinned 语义），均为刻意裁剪
pub struct BufferPool {
  /// 全局唯一池 ID (用于 TLS 路由索引)
  pub pool_id: u64,
  sector_size: usize,
  sector_shift: u32,
  small_budget: Budget,
  large_budget: Budget,
  first_large_class: usize,
  depot: Depot,
  is_closed: AtomicBool,
  /// 预算耗尽显式直配累计次数 (观测计数，无背压语义)
  direct_alloc_count: AtomicU64,
  /// 预算耗尽显式直配累计字节数
  direct_alloc_bytes: AtomicU64,
  /// 超界/关闭态绕过池缓存直配累计次数 (对标 C# `Stats.BypassAllocs`)
  bypass_alloc_count: AtomicU64,
  /// 绕过池缓存直配累计字节数
  bypass_alloc_bytes: AtomicU64,
}

impl BufferPool {
  /// 以默认预算创建缓冲池 (小 32MiB / 大 128MiB)
  pub fn new(sector_size: usize) -> Result<Arc<Self>> {
    Self::with_budgets(
      sector_size,
      DEFAULT_SMALL_BUDGET_BYTES,
      DEFAULT_LARGE_BUDGET_BYTES,
    )
  }

  /// 以指定小/大预算创建缓冲池
  pub fn with_budgets(
    sector_size: usize,
    small_budget_bytes: i64,
    large_budget_bytes: i64,
  ) -> Result<Arc<Self>> {
    validate_sector_size(sector_size)?;
    // 预算记账域非负：负预算会让 try_reserve 恒失败，池静默退化为全直配，须在创建期即拒绝
    if small_budget_bytes < 0 {
      return Err(Error::InvalidBudget(small_budget_bytes));
    }
    if large_budget_bytes < 0 {
      return Err(Error::InvalidBudget(large_budget_bytes));
    }
    // 防御性上界：最大 class 容量 (32768 扇区) 与扇区大小的乘积必须可安全用于 i64 预算记账
    if sector_size > (i64::MAX as usize) / MAX_POOLED_SECTORS {
      return Err(Error::InvalidSize(sector_size));
    }

    static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);
    let pool_id = NEXT_POOL_ID.fetch_add(1, Relaxed);
    let sector_shift = sector_size.trailing_zeros();

    Ok(Arc::new(Self {
      pool_id,
      sector_size,
      sector_shift,
      small_budget: Budget::new(small_budget_bytes),
      large_budget: Budget::new(large_budget_bytes),
      first_large_class: (0..NUM_CLASSES)
        .find(|&c| class_capacity_bytes(c, sector_size) > LARGE_TIER_MIN_BYTES)
        .unwrap_or(NUM_CLASSES),
      depot: Depot::new(),
      is_closed: AtomicBool::new(false),
      direct_alloc_count: AtomicU64::new(0),
      direct_alloc_bytes: AtomicU64::new(0),
      bypass_alloc_count: AtomicU64::new(0),
      bypass_alloc_bytes: AtomicU64::new(0),
    }))
  }

  /// 扇区物理大小
  #[inline]
  #[must_use]
  pub fn sector_size(&self) -> usize {
    self.sector_size
  }

  /// 缓冲池是否已显式关闭
  #[inline]
  #[must_use]
  pub fn is_closed(&self) -> bool {
    self.is_closed.load(Acquire)
  }

  /// 当前所有已预留的字节数 (对标 C# `ReservedBytes`)
  ///
  /// 观测口径：预算许可在分配时预留、缓冲物理释放或许可释放时归还；归还入池的
  /// 缓冲（含他线程 TLS 私有栈/收件箱中的在借空闲）不释放许可。故本值 ≥ 真实
  /// 在用字节数，差额即各线程缓存中的空闲容量——free 后并不即时回收，随线程
  /// 退出（TLS 析构溢出 Depot）或 [`Self::free`] 最终回收归零，最终一致
  #[inline]
  #[must_use]
  pub fn reserved_bytes(&self) -> i64 {
    self.small_budget.used() + self.large_budget.used()
  }

  /// 当前小缓冲已预留的字节数 (对标 C# `SmallReservedBytes`)
  #[inline]
  #[must_use]
  pub fn small_reserved_bytes(&self) -> i64 {
    self.small_budget.used()
  }

  /// 当前大缓冲已预留的字节数 (对标 C# `LargeReservedBytes`)
  #[inline]
  #[must_use]
  pub fn large_reserved_bytes(&self) -> i64 {
    self.large_budget.used()
  }

  /// 小缓冲总预算上限 (字节)
  #[inline]
  #[must_use]
  pub fn small_budget_bytes(&self) -> i64 {
    self.small_budget.total()
  }

  /// 大缓冲总预算上限 (字节)
  #[inline]
  #[must_use]
  pub fn large_budget_bytes(&self) -> i64 {
    self.large_budget.total()
  }

  /// 获取统计指标快照（容量配错诊断入口）
  ///
  /// - `direct_alloc_count/bytes` 持续增长说明预算耗尽显式直通在发生——预算许可
  ///   配小于实际工作集，超出部分绕过池缓存直配（语义不变，无背压），仅可观测
  /// - `bypass_alloc_count/bytes` 统计超出可池化上限（或池已关闭）的绕过直配
  ///   (对标 C# `Stats.BypassAllocs`)
  pub fn stats(&self) -> PoolStats {
    PoolStats {
      reserved_bytes: self.reserved_bytes(),
      small_reserved_bytes: self.small_reserved_bytes(),
      large_reserved_bytes: self.large_reserved_bytes(),
      direct_alloc_count: self.direct_alloc_count.load(Relaxed),
      direct_alloc_bytes: self.direct_alloc_bytes.load(Relaxed),
      bypass_alloc_count: self.bypass_alloc_count.load(Relaxed),
      bypass_alloc_bytes: self.bypass_alloc_bytes.load(Relaxed),
    }
  }

  /// 显式关闭并释放缓冲池中所有缓存的缓冲区 (对标 C# `SectorAlignedBufferPool.Free()`)
  ///
  /// 观测口径：仅回收调用线程可见的部分（全局 Depot + 本线程 TLS 私有栈）；
  /// 他线程 TLS 中的空闲缓冲随各线程退出（TLS 析构）或后续借还路径探测到池关闭
  /// 时释放，预算许可同步归还，`reserved_bytes` 最终一致归零而非即时归零
  pub fn free(&self) {
    if self.is_closed.swap(true, AcqRel) {
      return;
    }
    // 1. 清空全局 Depot 并释放配额 (条带在锁内原子关闭，此后一切推入必然失败并就地释放)
    self.depot.clear(|cls, cap| {
      self.budget_for(cls).release(cap as i64);
    });
    // 2. 清空当前线程 TLS 中属于本池的缓存
    self.drain_tls_self();
  }

  /// 清空当前线程 TLS 中属于本池的缓存并释放全部许可 (池关闭路径共用)
  ///
  /// `try_with` 容错跳过 TLS 析构期访问：当池的最后一个 `Arc` 在某线程 TLS 析构栈内
  /// 释放（[`TlsPoolEntry`] 升级出的临时强引用触发池 drop）时，`TLS_POOLS` 自身正处于
  /// 析构流程中，重入访问会 panic。跳过是正确的：触发析构的 entry 此刻正在
  /// `sweep(retire = true)`，缓冲已溢出至 Depot，随后的 `depot.clear` 必然释放其许可
  /// (对标 C# `Free` 与 `~ThreadShard` finalizer 经 `drainedOnce` CAS 仲裁、绝不崩溃)
  fn drain_tls_self(&self) {
    let _ = TLS_POOLS.try_with(|mgr| {
      if let Some(entry) = mgr.borrow_mut().find_mut(self.pool_id) {
        entry.drain_and_release(self);
      }
    });
  }

  /// 指定 class 当前缓存的空闲缓冲数 (包含当前线程本地缓存与全局条带仓库)
  ///
  /// 越界 class 无可池化容量，恒返回 0 (绝不触发索引越界 panic)；
  /// TLS 析构期访问按 0 处理 (同 `BufferPool::drain_tls_self` 的容错语义)
  #[must_use]
  pub fn cached_len(&self, cls: usize) -> usize {
    if cls >= NUM_CLASSES {
      return 0;
    }
    let local_len = TLS_POOLS
      .try_with(|mgr| {
        mgr
          .borrow()
          .find(self.pool_id)
          .map_or(0, |e| e.local[cls].len())
      })
      .unwrap_or(0);
    local_len + self.depot.total_cached(cls)
  }

  /// 获取缓冲区，默认归还清零策略 (对标 C# `Get(int)`)
  ///
  /// 与 C# 的刻意差异：0 字节请求直接返回空缓冲区 (C# 会签发 1 扇区池化缓冲)，
  /// 免去无意义分配；超出 [`MAX_POOLED_SECTORS`] 的请求走精确容量 bypass 直配不入池。
  pub fn get(self: &Arc<Self>, required_bytes: usize) -> Result<AlignedBuf> {
    self.get_with_policy(required_bytes, true)
  }

  /// 从给定切片数据租借并拷贝初始化的池化缓冲区
  ///
  /// 归还清零策略默认为 true，缓冲区在 drop 时自动归还入池复用
  pub fn get_from_slice(self: &Arc<Self>, slice: &[u8]) -> Result<AlignedBuf> {
    let mut buf = self.get(slice.len())?;
    buf.as_allocated_slice_mut()[..slice.len()].copy_from_slice(slice);
    buf.set_len(slice.len())?;
    Ok(buf)
  }

  /// 确保缓冲区具有足够的容量 (对标 C# `SectorAlignedBufferPool.EnsureSize`)
  ///
  /// 若当前缓冲区容量满足要求，则就地复用并同步有效需求长度为调用方原始请求字节数
  /// (对标 C# 复用分支的 `page.required_bytes = size`，不做扇区取整)；否则自动归还
  /// 旧缓冲区并租借新缓冲区。非池化缓冲区无需求长度元数据，仅参与容量判定。
  pub fn ensure_size(self: &Arc<Self>, buf: &mut AlignedBuf, size: usize) -> Result<()> {
    if buf.capacity() < size {
      *buf = self.get(size)?;
    } else {
      buf.set_required(size);
    }
    Ok(())
  }

  /// 以显式归还清零策略获取缓冲区 (对标 C# `Get(int, bool)`)
  pub fn get_with_policy(
    self: &Arc<Self>,
    required_bytes: usize,
    clear_on_return: bool,
  ) -> Result<AlignedBuf> {
    if required_bytes == 0 {
      return AlignedBuf::new(0, self.sector_size);
    }
    // 按扇区向上取整：usize 域 checked 运算，既避免加法回绕，
    // 也杜绝 32 位平台 u64 往返 as 转换的高位截断 (截断会把巨请求静默变成小缓冲)
    let required = match required_bytes.checked_add(self.sector_size - 1) {
      Some(v) => v & !(self.sector_size - 1),
      None => return Err(Error::Overflow),
    };
    let Some(cls) = class_of_sectors(required >> self.sector_shift) else {
      // 超出可池化范围：精确容量 bypass 直配，不入池 (required_len 恒等于容量)
      self.record_bypass(required);
      return AlignedBuf::new(required, self.sector_size);
    };

    if self.is_closed.load(Acquire) {
      // 池已关闭：bypass 直配 (对标 C# Disabled 分支，同样计入 BypassAllocs)
      self.drain_tls_self();
      self.record_bypass(required);
      return AlignedBuf::new(required, self.sector_size);
    }

    let tid = current_thread_id();

    // 1. 小容量 class：优先走 L1 线程私有栈与 L2 跨线程收割 (0 锁快路径)
    if cls < self.first_large_class {
      // `try_with` 容错：同线程其他 thread_local 的析构栈内触发 Get 时 TLS_POOLS 可能已析构，
      // 降级为「Depot 窃取 / 直配（无收件箱）」——对应归还路径 `return_owner`/`return_foreign`
      // 的同款容错回退 Depot，全链路无 panic (对标 C# bornSealed shard 的优雅降级)
      let (cached_opt, inbox) = TLS_POOLS
        .try_with(|mgr| {
          let mut mgr = mgr.borrow_mut();
          let entry = mgr.get_or_create(self);
          // 1a. 本地私有栈 (0 锁、0 原子操作)
          if let Some(node) = entry.local[cls].pop() {
            return (Some(node), Some(entry.inbox.clone()));
          }
          // 1b. 批量收割跨线程收件箱：单次 CAS 整链获取，零堆分配就地遍历
          let chain = entry.inbox.claim(cls);
          if !chain.is_null() {
            let mut iter = ChainIter::new(chain);
            if let Some(first) = iter.next() {
              for node in iter {
                if entry.local[cls].len() < MAX_LOCAL_PER_CLASS {
                  entry.local[cls].push(node);
                } else {
                  self.spill_to_depot(cls, node, tid);
                }
              }
              return (Some(first), Some(entry.inbox.clone()));
            }
          }
          (None, Some(entry.inbox.clone()))
        })
        .unwrap_or((None, None));

      if let Some(node) = cached_opt {
        return Ok(self.reuse_cached(node, cls, required_bytes, clear_on_return, tid, inbox));
      }

      // 1c. 全局条带仓库 (8-way 分片工作窃取)
      if let Some(node) = self.depot.pop(cls, tid) {
        return Ok(self.reuse_cached(node, cls, required_bytes, clear_on_return, tid, inbox));
      }

      // 1d. 缓存未命中：系统新分配并预留预算
      return self.issue_new(cls, required_bytes, clear_on_return, tid, inbox);
    }

    // 2. 大容量 class (>= first_large_class)：全局条带共享，不占本地栈
    if let Some(node) = self.depot.pop(cls, tid) {
      return Ok(self.reuse_cached(node, cls, required_bytes, clear_on_return, tid, None));
    }
    self.issue_new(cls, required_bytes, clear_on_return, tid, None)
  }

  /// 记录一次绕过池缓存的直配 (对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:RecordBypassAlloc：超界与关闭态路径)
  #[inline]
  fn record_bypass(&self, bytes: usize) {
    self.bypass_alloc_count.fetch_add(1, Relaxed);
    self.bypass_alloc_bytes.fetch_add(bytes as u64, Relaxed);
  }

  /// 缓存命中复用：命中节点按清零策略惰性清理后重建为池化缓冲区
  fn reuse_cached(
    self: &Arc<Self>,
    node: CachedBuf,
    cls: usize,
    required: usize,
    clear_on_return: bool,
    tid: u64,
    inbox: Option<Arc<CrossThreadInbox>>,
  ) -> AlignedBuf {
    if clear_on_return && node.dirty {
      unsafe { write_bytes(node.ptr.as_ptr(), 0, node.cap) };
    }
    let cacheable = node.cacheable;
    AlignedBuf::from_cached(
      node,
      self.clone(),
      BufMeta {
        cls: cls as u32,
        cacheable,
        clear_on_return,
        required,
        owner_tid: tid,
        inbox,
      },
    )
  }

  /// 新分配签发：预留预算后由系统分配并纳入池归属
  fn issue_new(
    self: &Arc<Self>,
    cls: usize,
    required: usize,
    clear_on_return: bool,
    tid: u64,
    inbox: Option<Arc<CrossThreadInbox>>,
  ) -> Result<AlignedBuf> {
    let cap = class_capacity_bytes(cls, self.sector_size);
    let cacheable = self.budget_for(cls).try_reserve(cap as i64);
    if !cacheable {
      // 预算耗尽显式直通：请求照常满足（调用方语义已定，不加背压），
      // 但必须可观测——配错容量时由此计数暴露（对标 C# 预算满直接分配无独立区分）
      self.direct_alloc_count.fetch_add(1, Relaxed);
      self.direct_alloc_bytes.fetch_add(cap as u64, Relaxed);
    }
    let mut buf = match AlignedBuf::new(cap, self.sector_size) {
      Ok(b) => b,
      Err(e) => {
        if cacheable {
          self.budget_for(cls).release(cap as i64);
        }
        return Err(e);
      }
    };
    buf.attach(
      self.clone(),
      BufMeta {
        cls: cls as u32,
        cacheable,
        clear_on_return,
        required,
        owner_tid: tid,
        inbox,
      },
    );
    Ok(buf)
  }

  /// 归还缓冲区 (由 [`AlignedBuf::drop`] 的 RAII 路径调用，对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:ReturnOriginReturn)
  pub(crate) fn return_buf(&self, ptr: NonNull<u8>, cap: usize, align: usize, meta: BufMeta) {
    let BufMeta {
      cls,
      cacheable,
      clear_on_return,
      owner_tid,
      inbox,
      ..
    } = meta;
    let cls = cls as usize;
    let dealloc = |ptr: NonNull<u8>| unsafe {
      dealloc(ptr.as_ptr(), Layout::from_size_align_unchecked(cap, align))
    };

    // 非池化 / 越界 class：从未持有预算许可，直接释放内存
    if !cacheable || cls >= NUM_CLASSES {
      dealloc(ptr);
      return;
    }

    // 若池已关闭，直接释放内存并交还预算许可 (对标 libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolTests.cs:ReturnOfInFlightBufferAfterFreeIsSafe)
    if self.is_closed.load(Acquire) {
      self.budget_for(cls).release(cap as i64);
      dealloc(ptr);
      return;
    }

    // 归还即清零 (对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:FinalizeForReturn)；免清零归还则标记脏位，交由后续借方惰性清零
    if clear_on_return {
      unsafe { write_bytes(ptr.as_ptr(), 0, cap) };
    }
    let node = CachedBuf {
      ptr,
      cap,
      align,
      cacheable,
      dirty: !clear_on_return,
    };

    // 1. 大容量 class：直接推入全局条带仓库，本地栈与收件箱均不承载 (对标 C# OriginReturn L581)
    let tid = current_thread_id();
    if cls >= self.first_large_class {
      self.spill_to_depot(cls, node, tid);
      return;
    }

    // 2. 小容量 class
    if tid == owner_tid {
      // 2a. 属主线程同源归还 (Owner Return)：0 锁、0 原子操作推入 TLS 私有栈
      self.return_owner(cls, node, tid);
    } else {
      // 2b. 跨线程归还 (Foreign Return)：无锁 CAS 推入属主收件箱 (对标 C# OriginReturn L607)
      self.return_foreign(cls, node, inbox, tid);
    }
  }

  /// 溢出转移：推入全局条带仓库供跨线程工作窃取复用；仓库已满则永久丢弃并释放许可
  /// (node drop 自动触发 dealloc，对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:DepotPush 失败 → DropBuffer)
  fn spill_to_depot(&self, cls: usize, node: CachedBuf, tid: u64) {
    let cap = node.cap;
    if !self.depot.push(cls, node, tid) {
      self.budget_for(cls).release(cap as i64);
    }
  }

  /// 小容量 class 属主同源归还：0 锁、0 原子操作推入 TLS 私有栈；
  /// 栈满、查无属主条目或线程 TLS 已进入析构则溢出转移至全局条带仓库
  /// (对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:PushLocal 溢出 DepotPush → DropBuffer)
  fn return_owner(&self, cls: usize, node: CachedBuf, tid: u64) {
    // `hold` 以可变借用被闭包捕获：已入本地栈则留空，否则取回溢出转移。
    // `try_with` 容错：属主线程退出窗口中缓冲随其 thread_local 析构时，TLS 已不可访问，
    // 此时推入垂死线程的私有栈毫无意义，回退 Depot 让许可仍归池核算 (池 Free 时统一释放)；
    // 切不可按值把 node 移入闭包——try_with 失败会连同闭包丢弃 node，导致许可泄漏
    let mut hold = Some(node);
    let _ = TLS_POOLS.try_with(|mgr| match mgr.borrow_mut().find_mut(self.pool_id) {
      Some(entry) if entry.local[cls].len() < MAX_LOCAL_PER_CLASS => {
        if let Some(node) = hold.take() {
          entry.local[cls].push(node);
        }
      }
      _ => {}
    });

    if let Some(node) = hold {
      self.spill_to_depot(cls, node, tid);
    }
  }

  /// 小容量 class 跨线程归还：利用缓冲区首部侵入式 [`FreeNode`] 经 CAS 推入属主收件箱；
  /// 属主已退出 (inbox sealed) 则恢复污染区置零后溢出重定向至全局条带仓库
  fn return_foreign(
    &self,
    cls: usize,
    node: CachedBuf,
    inbox: Option<Arc<CrossThreadInbox>>,
    tid: u64,
  ) {
    if let Some(inbox) = inbox {
      let node_ptr = node.ptr.as_ptr() as *mut FreeNode;
      unsafe {
        ptr::write(
          node_ptr,
          FreeNode {
            next: ptr::null_mut(),
            cap: node.cap,
            align: node.align,
            cacheable: node.cacheable,
            dirty: node.dirty,
          },
        );
      }
      if inbox.try_push(cls, node_ptr) {
        forget(node);
        return;
      }
      // 密封回退：恢复 32B FreeNode 污染区置零，保证 !dirty 缓冲复用时首部 100% 全零
      if !node.dirty {
        unsafe { write_bytes(node_ptr, 0, size_of::<FreeNode>()) };
      }
    }
    self.spill_to_depot(cls, node, tid);
  }

  /// 大容量 class 走独立大额预算 (对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:BudgetFor)
  #[inline]
  pub(crate) fn budget_for(&self, cls: usize) -> &Budget {
    if cls >= self.first_large_class {
      &self.large_budget
    } else {
      &self.small_budget
    }
  }
}

impl Drop for BufferPool {
  fn drop(&mut self) {
    self.free();
  }
}
