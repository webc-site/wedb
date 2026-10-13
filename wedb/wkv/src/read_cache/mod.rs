//! Microsoft Garnet 官方架构对标的独立只读非脏页内存日志（ReadCache）
//!
//! 生产开关：`StoreConfig::enable_read_cache`（默认关闭，对标 C# GarnetServerOptions
//! EnableReadCache 默认 false；生产命令行经 wconf `hlog.read_cache` 暴露）。
//! 地址位原语（READ_CACHE_BIT 判定/清除/打标）单点在 [`wbase::addr`]，
//! 本模块直连消费，不再设包装层。
//!
//! 模块拓扑严格对照 C# TsavoriteKV 的 partial 分文件形态：
//! - [`append`]：CAS 预留 + 编码 + 哈希索引挂载（对标
//!   libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToReadCache.cs:TryCopyToReadCache）
//! - [`cleanse`]：页关闭清洗与脱钩作废（对标 ReadCache.cs:ReadCacheEvict /
//!   ReadCacheEvictChain / ReadCacheAbandonRecord）
//! - [`window`]：地址窗口水位与读侧链路（对标 ReadCache.cs:FindInReadCache /
//!   SkipReadCache / ReadCacheNeedToWaitForEviction 与 AllocatorBase.cs
//!   地址窗口水位域 ClosedUntilAddress）
//!
//! 自研依据: 读缓存（C# 对应 libs/storage/Tsavorite/cs/src/core/…ReadCache + test.session/NativeReadCacheTests.cs）

use wbase::pool::AlignedBuf;
mod append;
mod cleanse;
mod window;

use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicU64},
};

use itoa::Buffer;
use parking_lot::Mutex;
use wbase::align::{CachePadded, DEFAULT_SECTOR_SIZE};
use wepoch::LightEpoch;
use whlog::{CircularPageBuffer, HybridLogConfig};

use crate::error::{Error, Result};

/// 页槽位关闭位（两阶段关闭协议：置位后新编码注册必被拒绝，直至换页完成重置）
#[doc(hidden)]
pub const INFLIGHT_CLOSED: u64 = 1 << 63;

/// 读缓存链走查三态结果（严格对标
/// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:FindInReadCache
/// 的「非 Invalid 才比对键、无条件沿 PreviousAddress 续链」与
/// ReadCacheNeedToWaitForEviction → RestartChain 的重启协议）
///
/// 三态的核心价值：把「记录不可判读」与「记录可读但未命中」严格分离——
/// 前者绝不可折叠成链终止产出确定性 NOTFOUND（活键误报不存在的根因），
/// 必须回链头重探
///
/// 与主日志读链判据的对位换算单点在 `session/raw/read.rs:map_rc_visit`：
/// C# FindInReadCache 与 InternalRead 共用同一 OperationStatus（判据同源不
/// 重复定义），本枚举只与走查出口枚举在此一处换算，勿在别处散落 match
#[derive(Debug)]
pub enum RcVisit<R> {
  /// 记录可读且闭包命中（键比对成功）
  Found(R),
  /// 记录可读但未命中：携带前驱地址续链。closed 作废记录对标 C# 跳过 Invalid
  /// 沿 prev 继续；0 表示链尾（pad/零头/链尽）
  Next(u64),
  /// 记录滑出环形窗口或在途换装竞态下不可判读：调用方须按
  /// UpdateRecordSourceToCurrentHashEntry 语义回链头重探（RestartChain），
  /// 绝不可按不存在降级
  Gone,
}

/// 页槽位在途编码计数掩码
#[doc(hidden)]
pub const INFLIGHT_COUNT_MASK: u64 = !INFLIGHT_CLOSED;

/// Microsoft Garnet 官方架构对标的独立只读非脏页内存日志（ReadCache）
///
/// 严格对齐 Garnet `ReadCache.cs` + `TryCopyToReadCache.cs`：
/// 1. 纯 DRAM 环形日志分配器，无任何物理磁盘持久化开销，零写放大；
/// 2. 磁盘冷数据命中回填后，挂载为哈希链首部前缀，加速后续高频读请求纳秒级命中；
/// 3. 主日志执行写操作（Upsert / RMW / Delete）时通过单次 CAS 原子脱钩整条 ReadCache 链；
/// 4. 环形覆盖自然淘汰旧页，在复用前执行 CleanseHashChain 原子解构恢复主日志链接，杜绝悬垂指针与数据丢失。
pub struct ReadCache {
  /// 环形页内存池
  buffer: CircularPageBuffer,
  /// 页面大小（字节，2 的幂）
  pub page_size: usize,
  /// 缓冲页总数（2 的幂）
  pub num_pages: usize,
  /// 活跃尾部分配地址（写热点，独占 64 字节缓存行）
  tail_address: CachePadded<AtomicU64>,
  /// 有效起始地址（滑动窗口下界，读热点，独占 64 字节缓存行）
  head_address: CachePadded<AtomicU64>,
  /// 驱逐清洗完成高水位地址（严格对标 C# AllocatorBase 的 ClosedUntilAddress 水位）
  ///
  /// 换页路径在 cleanse_page 恢复哈希链后按 MonotonicUpdate 口径单调推进；
  /// 读侧 SpinWaitUntilRecordIsClosed 以 `abs < closed_until_address` 为关闭判据
  /// （等于该值表示记录尚未关闭）。低频标记位，不加缓存行填充
  closed_until_address: AtomicU64,
  /// 纪元排空完成高水位地址（对标 C# AllocatorBase 的 SafeHeadAddress「The lowest
  /// reliable in-memory address，由 OnPagesClosed 在开始关闭区间时标定」）
  ///
  /// 旧页延迟关闭动作（[`ReadCache::pump_close_barrier`] 注册、
  /// [`ReadCache::close_pending_page`] 执行体）在全部旧纪元在途读者退出后，先于
  /// ClosedUntilAddress 单调推进至该边界；兼作同边界重复延迟动作的幂等短路判据。
  /// 低频标记位，不加缓存行填充；Arc 化仅为交付延迟动作闭包的 'static 共享句柄
  safe_head_address: Arc<AtomicU64>,
  /// 已武装、待纪元延迟关闭的驱逐边界（回绕换页方经 fetch_max 登记目标旧代页
  /// 末尾边界；关闭动作执行时据此复核冻结 tail 下的页界恒等式）
  pending_close_until: AtomicU64,
  /// 关闭屏障武装边沿标志：换页方置位，[`ReadCache::pump_close_barrier`] 以
  /// swap 单点消费，未武装时注册入口零开销直返
  close_armed: AtomicBool,
  /// 存储引擎统一纪元保护器（构造期自 WedbStore 注入，对标 C# TsavoriteKV 中
  /// 读缓存分配器与主日志共享同一全局 LightEpoch 实例的形态）：回绕换页的旧页
  /// 关闭序列经 `bump_current_epoch_action` 挂入其延迟排空队列，保证在途无锁
  /// 直读读者（window.rs `PageView::Fast` 裸切片借入 disclose 闭包）全部退出后
  /// 才清零/覆写物理槽位
  epoch: Arc<LightEpoch>,
  /// 各页槽位在途编码状态字（两阶段关闭协议：位63 [`INFLIGHT_CLOSED`] + 低 63 位在途编码计数）
  ///
  /// 闭环回写窗撕裂：CAS 预留切片与编码之间存在无锁窗口，滞留编码者（单线程
  /// 停滞横跨整环回绕）唤醒后会落入已被换装复用的页槽。本状态字令换页侧能够
  /// 等待被复用槽上全部在途编码完成（计数清零）并拒绝新注册（CLOSED 位），
  /// 对标 C# Tsavorite 页清除/驱逐的 epoch-gated 排空语义
  /// （C# 主日志 AllocatorBase 的 OnPagesClosed "wait for reader drain"）：C# 以全局纪元
  /// 排空实现同一不变式；本仓读侧驻留保护由 [`ReadCache::pump_close_barrier`]
  /// 的延迟关闭动作承接，此处粒度收敛到单槽、只排在途写者，
  /// 规避 turn_lock 等待者持旧纪元与锁内全局排空互待的死锁
  page_inflight: Box<[AtomicU64]>,
  /// 换页保护互斥锁
  turn_lock: Mutex<()>,
  /// 是否启用 ReadCache
  pub is_enabled: bool,
}

impl ReadCache {
  /// 创建新的 ReadCache 实例
  ///
  /// `epoch` 为存储引擎统一纪元保护器（对标 C# TsavoriteKV 构造期向读缓存
  /// 分配器注入同一全局 LightEpoch 实例）：回绕换页的旧页关闭序列经其延迟
  /// 排空队列执行，见 [`ReadCache::pump_close_barrier`]
  pub fn new(
    page_size: usize,
    num_pages: usize,
    is_enabled: bool,
    epoch: Arc<LightEpoch>,
  ) -> Result<Self> {
    if !page_size.is_power_of_two() || page_size == 0 {
      let mut msg = String::from("ReadCache page_size 必须为非零且为 2 的幂，当前为 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(page_size));
      return Err(Error::InvalidConfig(msg));
    }
    if !num_pages.is_power_of_two() || num_pages == 0 {
      let mut msg = String::from("ReadCache num_pages 必须为非零且为 2 的幂，当前为 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(num_pages));
      return Err(Error::InvalidConfig(msg));
    }

    let dummy_config = HybridLogConfig {
      page_size,
      num_pages,
      mutable_fraction: 1.0,
      ro_lag_num: whlog::ro_lag_num_from_fraction(1.0),
      initial_address: 0,
    };
    // 禁用态零预算占位（对标 C# 仅 EnableReadCache 时才构造读缓存分配器）：
    // 全部读 写路径首行短路 `is_enabled`，占位环（单扇区页）永不被寻址；
    // 否则大页配置（16MB）下禁用态也将常驻整环内存
    let buffer = if is_enabled {
      CircularPageBuffer::new(&dummy_config)?
    } else {
      CircularPageBuffer::new(&HybridLogConfig {
        page_size: DEFAULT_SECTOR_SIZE,
        num_pages: 1,
        mutable_fraction: 1.0,
        ro_lag_num: whlog::ro_lag_num_from_fraction(1.0),
        initial_address: 0,
      })?
    };
    buffer.clear_page(0);

    Ok(Self {
      buffer,
      page_size,
      num_pages,
      tail_address: CachePadded(AtomicU64::new(0)),
      head_address: CachePadded(AtomicU64::new(0)),
      closed_until_address: AtomicU64::new(0),
      safe_head_address: Arc::new(AtomicU64::new(0)),
      pending_close_until: AtomicU64::new(0),
      close_armed: AtomicBool::new(false),
      epoch,
      page_inflight: (0..num_pages).map(|_| AtomicU64::new(0)).collect(),
      turn_lock: Mutex::new(()),
      is_enabled,
    })
  }

  #[doc(hidden)]
  #[inline(always)]
  pub fn page_size(&self) -> usize {
    self.page_size
  }

  #[doc(hidden)]
  #[inline(always)]
  pub fn page_inflight(&self, index: usize) -> &AtomicU64 {
    &self.page_inflight[index]
  }

  #[doc(hidden)]
  #[inline(always)]
  pub fn turn_lock(&self) -> &Mutex<()> {
    &self.turn_lock
  }

  #[doc(hidden)]
  #[inline(always)]
  pub fn read_page(&self, page_id: u64) -> parking_lot::RwLockReadGuard<'_, AlignedBuf> {
    self.buffer.read_page(page_id)
  }
}
