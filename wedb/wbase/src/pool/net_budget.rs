//! 活跃网络连接缓冲的进程级自适应预算
//!
//! 在 garnet 中的相对路径:libs/common/Memory/NetworkBufferBudget.cs:NetworkBufferBudget
//!（PR #2157「每连接内存上限」新增）
//!
//! 缓冲池自身的闲置字节上限只约束空闲链表；被活跃连接借出的缓冲不受任何
//! 约束，其足迹 = 连接数 × 单连接规格。本类型以「发布目标基准规格」约束
//! 单连接因子：`target = clamp(receive_floor, ceiling, prev_pow2(budget / live_count))`。
//!
//! 两条不变量约束自适应行为：
//! 1. 目标只能下调基准规格（以配置规格为上限钳制）；按需增长永不钳制，
//!    需要大缓冲的连接仍能长到所需规格；
//! 2. 预算是目标而非硬上限——连接可按需长过基准，聚合可超预算；它约束的
//!    是每连接的起步与回落数值（随连接数伸缩的那一项）。分方向地板从下方
//!    兜住目标（到位后聚合重新随连接数增长；send 地板更高、先绑定）。
//!    `--network-connection-limit`（maxclients）才是硬准入界。
//!
//! C# `NetworkBufferBudget.Disabled` 单例（budgetBytes=0 一切操作惰性）在
//! rust 对位为 `Option<Arc<NetworkBufferBudget>>` 的 None 形态，语义等价。

use std::sync::atomic::{
  AtomicI64, AtomicUsize,
  Ordering::{AcqRel, Acquire},
};

use crate::align::CachePadded;

/// Recompute 在放弃发布前尝试的次数上限（C# `MaxRecomputeAttempts` 常量，
/// NetworkBufferBudget.cs:379）
///
/// 无兜底回退：耗尽即不发布，留下某个近期计数已证成的值。计数移动方
///（OnBufferAcquired/OnBufferReleased）先移计数再重算，令任何使已发布目标
/// 失效的线程自身必重算。
const MAX_RECOMPUTE_ATTEMPTS: usize = 8;

/// 缓冲方向（C# `PoolEntryBufferType` 按回收目标的折叠面：SaeaSendBuffer /
/// TransportSendBuffer 归 Send，其余归 Receive，PoolEntryTypes.cs）
///
/// 回收时按条目自身方向对照目标：send 缓冲对照 send 目标（地板更高），
/// 混用 receive 目标会把规格正确的 send 缓冲误判为超目标，令预算绑定期间
/// send 缓冲不可回池。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BufferKind {
  /// 接收方向（默认）：对照 receive 目标
  #[default]
  Receive,
  /// 发送方向：对照 send 目标（地板高于 receive）
  Send,
}

/// 活跃网络连接缓冲的进程级预算
///
/// 在 garnet 中的相对路径:libs/common/Memory/NetworkBufferBudget.cs:NetworkBufferBudget
pub struct NetworkBufferBudget {
  /// 活跃缓冲应共同保持的总字节数；0 = 禁用自适应
  budget_bytes: i64,
  /// 配置基准规格（目标上限，自适应只能下调）
  ceiling: usize,
  /// receive 缓冲可被钳制到的最小基准规格
  receive_floor: usize,
  /// send 缓冲可被钳制到的最小基准规格（高于 receive：过小 send 缓冲会把
  /// 超大应答推向池租借路径）
  send_floor: usize,
  /// 自预算化池借出的活跃缓冲数——恰好骑在维护池活跃字节计数的两条路径上
  ///（借出/归还），别无分号
  live_buffer_count: CachePadded<AtomicI64>,
  /// 已发布基准规格（分配点读取；陈旧读最多损失一块按旧规格分配的缓冲）
  target_buffer_size: CachePadded<AtomicUsize>,
  pressure_shrinks: AtomicI64,
  idle_shrinks: AtomicI64,
}

impl NetworkBufferBudget {
  /// 创建预算
  ///
  /// 在 garnet 中的相对路径:libs/common/Memory/NetworkBufferBudget.cs:NetworkBufferBudget
  ///
  /// * `budget_bytes`：活跃缓冲应保持的总字节数；≤ 0 禁用自适应
  /// * `ceiling`：配置基准规格（2 的幂）；目标永不超此值
  /// * `receive_floor` / `send_floor`：分方向最小基准规格（2 的幂，超 ceiling
  ///   时被压回 ceiling——抬升基准是自适应绝不允许做的事）
  pub fn new(budget_bytes: i64, ceiling: usize, receive_floor: usize, send_floor: usize) -> Self {
    debug_assert!(ceiling.is_power_of_two());
    debug_assert!(receive_floor.is_power_of_two());
    debug_assert!(send_floor.is_power_of_two());
    Self {
      budget_bytes: budget_bytes.max(0),
      ceiling,
      receive_floor: receive_floor.min(ceiling),
      send_floor: send_floor.min(ceiling),
      live_buffer_count: CachePadded::new(AtomicI64::new(0)),
      target_buffer_size: CachePadded::new(AtomicUsize::new(ceiling)),
      pressure_shrinks: AtomicI64::new(0),
      idle_shrinks: AtomicI64::new(0),
    }
  }

  /// 自适应是否启用（budget_bytes > 0）
  #[inline]
  pub fn is_enabled(&self) -> bool {
    self.budget_bytes > 0
  }

  /// 预算是否正在绑定（已发布目标被活跃缓冲数压到配置规格之下）——收缩
  /// 策略门控的压力信号
  #[inline]
  pub fn is_under_pressure(&self) -> bool {
    self.is_enabled() && self.target_buffer_size.load(Acquire) < self.ceiling
  }

  /// 配置预算字节数（禁用为 0）
  #[inline]
  pub fn budget_bytes(&self) -> i64 {
    self.budget_bytes
  }

  /// 新缓冲的已发布基准规格（分方向地板施加前）
  #[inline]
  pub fn target_buffer_size(&self) -> usize {
    self.target_buffer_size.load(Acquire)
  }

  /// 新 receive 缓冲基准规格
  #[inline]
  pub fn target_receive_buffer_size(&self) -> usize {
    self.receive_floor.max(self.target_buffer_size())
  }

  /// 新 send 缓冲基准规格
  #[inline]
  pub fn target_send_buffer_size(&self) -> usize {
    self.send_floor.max(self.target_buffer_size())
  }

  /// 把配置 receive 规格钳到已发布目标（对标 `ClampReceiveBufferSize`）
  #[inline]
  pub fn clamp_receive_buffer_size(&self, configured: usize) -> usize {
    if self.budget_bytes == 0 {
      configured
    } else {
      configured.min(
        self
          .receive_floor
          .max(self.target_buffer_size.load(Acquire)),
      )
    }
  }

  /// 把配置 send 规格钳到已发布目标（对标 `ClampSendBufferSize`）
  #[inline]
  pub fn clamp_send_buffer_size(&self, configured: usize) -> usize {
    if self.budget_bytes == 0 {
      configured
    } else {
      configured.min(self.send_floor.max(self.target_buffer_size.load(Acquire)))
    }
  }

  /// 自预算化池借出的活跃缓冲数
  #[inline]
  pub fn live_buffer_count(&self) -> i64 {
    self.live_buffer_count.load(Acquire)
  }

  /// 因预算压力收缩缓冲的次数
  #[inline]
  pub fn pressure_shrinks(&self) -> i64 {
    self.pressure_shrinks.load(Acquire)
  }

  /// 非压力、安静期后收缩缓冲的次数
  #[inline]
  pub fn idle_shrinks(&self) -> i64 {
    self.idle_shrinks.load(Acquire)
  }

  /// 记一次因预算压力的收缩（禁用时惰性：非预算化池共享 Disabled 形态，
  /// 计数会把进程级竞争写压到预算不治理的连接收缩路径上）
  #[inline]
  pub fn record_pressure_shrink(&self) {
    if self.is_enabled() {
      self.pressure_shrinks.fetch_add(1, AcqRel);
    }
  }

  /// 记一次安静期收缩（惰性理由同 [`Self::record_pressure_shrink`]）
  #[inline]
  pub fn record_idle_shrink(&self) {
    if self.is_enabled() {
      self.idle_shrinks.fetch_add(1, AcqRel);
    }
  }

  /// 记一次缓冲借出（对标 `OnBufferAcquired`）：计数先移再重算，随后归还
  #[inline]
  pub fn on_buffer_acquired(&self) {
    if self.budget_bytes == 0 {
      return;
    }
    self.live_buffer_count.fetch_add(1, AcqRel);
    self.recompute();
  }

  /// 记一次缓冲归还（无论回池还是弃置；对标 `OnBufferReleased`）
  #[inline]
  pub fn on_buffer_released(&self) {
    if self.budget_bytes == 0 {
      return;
    }
    self.live_buffer_count.fetch_sub(1, AcqRel);
    self.recompute();
  }

  /// 重算并发布 [`Self::target_buffer_size`]（对标 `Recompute`）
  ///
  /// 由借出/归还驱动：每条移动活跃数的路径都重发布，计数与由它导出的目标
  /// 永不脱节。已发布目标必须与导出它的计数一致——单次 CAS 对前值并不足
  /// 以保证（持陈旧计数的线程可能赢得交换，持新计数的可能输掉），故两种
  /// 结果都重导出：交换失败重试，交换成功后重读计数、若已移动再试。
  /// 迟滞带 `[current, 2*current)`：带宽必须不窄于激励步（2x），带内不写。
  pub fn recompute(&self) {
    if self.budget_bytes == 0 {
      return;
    }
    for _ in 0..MAX_RECOMPUTE_ATTEMPTS {
      let count = self.live_buffer_count.load(Acquire);
      let raw = self.budget_bytes / count.max(1);

      let current = self.target_buffer_size.load(Acquire);
      // 迟滞带：商一旦达到已发布目标即收缩；增长须达到其两倍。
      // 更窄的带会在相邻规格间无限振荡。
      if raw >= current as i64 && raw < 2 * current as i64 {
        return;
      }

      let next = self.compute_target(raw);
      if next == current {
        return;
      }

      if self
        .target_buffer_size
        .compare_exchange(current, next, AcqRel, Acquire)
        .is_err()
      {
        continue;
      }

      if self.live_buffer_count.load(Acquire) == count {
        return;
      }
    }
  }

  /// 单缓冲字节商允许的最大基准规格（对标 `ComputeTarget`：向下取池可回收
  /// 的规格级——2 的幂）
  fn compute_target(&self, raw: i64) -> usize {
    if raw >= self.ceiling as i64 {
      return self.ceiling;
    }
    if raw <= self.receive_floor as i64 {
      return self.receive_floor;
    }
    let rounded = 1usize << raw.ilog2();
    rounded.clamp(self.receive_floor, self.ceiling)
  }

  /// 给定活跃缓冲数将发布的目标（纯函数，不触共享状态、不施迟滞；对标
  /// `TargetForCount`——尺寸策略可脱离套接字检验与测试）
  pub fn target_for_count(&self, count: i64) -> usize {
    if self.budget_bytes == 0 {
      self.ceiling
    } else {
      self.compute_target(self.budget_bytes / count.max(1))
    }
  }

  /// 该方向条目应收敛的自适应规格（对标池内 `TargetSizeFor`：send 类对照
  /// send 目标，其余对照 receive 目标）
  #[inline]
  pub fn target_size_for(&self, kind: BufferKind) -> usize {
    match kind {
      BufferKind::Send => self.target_send_buffer_size(),
      BufferKind::Receive => self.target_receive_buffer_size(),
    }
  }

  /// INFO 统计片段
  ///
  /// 在 garnet 中的相对路径:libs/common/Memory/NetworkBufferBudget.cs:GetStats
  pub fn get_stats(&self) -> String {
    format!(
      "budget_bytes={} target_buffer_size={} target_send_buffer_size={} live_buffer_count={} pressure_shrinks={} idle_shrinks={}",
      self.budget_bytes,
      self.target_buffer_size(),
      self.target_send_buffer_size(),
      self.live_buffer_count(),
      self.pressure_shrinks(),
      self.idle_shrinks(),
    )
  }
}
