use std::{
  fmt::{self, Formatter},
  io::{self, Error, ErrorKind},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
  },
};

use parking_lot::RwLock;
use wbase::time::now_ms_i64;
use wconf::{RuntimeServerConfig, ServerConfigType};
use wnode::aof::{
  aof_backpressure::AofBackpressure, garnet_append_only_file::GarnetAppendOnlyFile,
};

use crate::server::replication::{
  driver_registry::DriverLifecycle,
  replica_wire::{AofSyncWire, ShippedState},
};

/// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:AofSyncTask
///
/// 单个物理子日志的 AOF 增量流式网络同步任务，集成背压门控与发送通道健康面
pub struct AofSyncTask {
  physical_sublog_idx: usize,
  start_address: i64,
  previous_address: AtomicI64,
  shipped_watermark_address: AtomicI64,
  last_throttle_shipped: AtomicI64,
  last_published_shipped_address: AtomicI64,
  /// 推流游标：已交给发送通道的最高 next_address（含溢流滞留在途帧；
  /// 补扫泵以此定位拉取起点，与已发送水位刻意分离）
  accepted_address: AtomicI64,
  /// 上次时间脉冲发送时刻毫秒（对标 C# lastAdvanceTimePulse，单调毫秒域）
  pub last_advance_time_pulse: AtomicI64,
  /// 上次脉冲时的物理日志尾地址快照（对标 C# pulseTailSnapshot；
  /// rust 推流拓扑单物理子日志，数组退化为单值，初值 -1 对标 Array.Fill(-1)）
  pub pulse_tail_snapshot: AtomicI64,
  is_connected: AtomicBool,
  local_node_id: u128,
  remote_node_id: u128,
  /// 副本发送通道（对标 C# garnetClient 字段；C# 构造期按端点建立，
  /// Rust 装配期经 attach_wire 注入——未注入时 consume 仅做位点记账，
  /// 见 attach_wire 文档）
  wire: RwLock<Option<AofSyncWire>>,
  /// 时间脉冲源（对标 C# 构造期捕获的 appendOnlyFile / backpressure 可达面；
  /// 构造期注入，缺席即 C# timePulseEnabled=false，脉冲链整体静默）
  pulse_source: Option<Arc<TimePulseSource>>,
}

/// 时间脉冲源（对标 C# AofSyncTask 构造期捕获的 appendOnlyFile、backpressure
/// 与 clusterProvider（经其 serverOptions 读节流频率））
pub struct TimePulseSource {
  /// AOF 门面：物理日志尾地址 + 序列号生成器读取源
  pub aof: Arc<GarnetAppendOnlyFile>,
  /// 背压闸门：停顿例外检测（C# backpressure?.AnyStalled()）
  pub backpressure: Option<Arc<AofBackpressure>>,
  /// 运行时配置（对标 C# clusterProvider.serverOptions 可达面：节流频率
  /// AofTailWitnessFreq 每轮实时读取槽值，CONFIG SET 即时生效；缺省 10 毫秒，
  /// 对标 defaults.conf:179 生效值 10；GarnetServerOptions.cs:150 字段初值 100
  /// 系被 Options.cs:932 GetServerOptions 投影覆盖的非生效初值，禁按其回改）
  pub runtime_config: Arc<RuntimeServerConfig>,
}

impl DriverLifecycle for AofSyncTask {
  #[inline]
  fn is_active(&self) -> bool {
    self.is_connected()
  }

  fn dispose(&self) {
    self.set_connected(false);
    if let Some(wire) = self.wire.write().take() {
      wire.disconnect();
    }
  }
}

/// AofSyncTask Debug 实现
impl fmt::Debug for AofSyncTask {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("AofSyncTask")
      .field("physical_sublog_idx", &self.physical_sublog_idx)
      .field("remote_node_id", &self.remote_node_id)
      .field("previous_address", &self.previous_address())
      .field("connected", &self.is_connected())
      .finish_non_exhaustive()
  }
}

impl AofSyncTask {
  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:AofSyncTask
  ///
  /// 脉冲源构造期注入（对标 C# 构造器捕获 appendOnlyFile / backpressure；
  /// 缺席传 None，即 C# timePulseEnabled=false 的脉冲链整体静默形态）
  pub fn new(
    physical_sublog_idx: usize,
    start_address: i64,
    local_node_id: u128,
    remote_node_id: u128,
    pulse_source: Option<Arc<TimePulseSource>>,
  ) -> Self {
    Self {
      physical_sublog_idx,
      start_address,
      previous_address: AtomicI64::new(start_address),
      shipped_watermark_address: AtomicI64::new(start_address),
      last_throttle_shipped: AtomicI64::new(start_address),
      last_published_shipped_address: AtomicI64::new(start_address),
      accepted_address: AtomicI64::new(start_address),
      last_advance_time_pulse: AtomicI64::new(0),
      pulse_tail_snapshot: AtomicI64::new(-1),
      is_connected: AtomicBool::new(true),
      local_node_id,
      remote_node_id,
      wire: RwLock::new(None),
      pulse_source,
    }
  }

  /// 注入副本发送通道（对标 C# 构造器内 garnetClient 建立）
  ///
  /// C# AofSyncTask 构造即持有 GarnetClientSession；Rust 装配链在驱动
  /// 入库后按副本端点建连并注入（attach 顺序保证初始化帧先于记录帧，
  /// 见 TcpSessionWire::connect）。未注入 wire 的驱动仅提供位点记账面
  ///（单测/位面观测形态，生产装配恒注入）
  pub fn attach_wire(&self, wire: impl Into<AofSyncWire>) {
    *self.wire.write() = Some(wire.into());
    // 重置脉冲节流窗口（对标 C# RunAofSyncTaskAsync init 帧成功后
    // lastAdvanceTimePulse = 单调毫秒；rust init 握手在
    // TcpSessionWire::connect 内完成，attach 即任务侧等价时点）
    self
      .last_advance_time_pulse
      .store(now_ms_i64(), Ordering::Release);
  }

  /// 发送通道是否在位（推流就绪判别位：泵扫描臂据此令无 wire 钉线驱动保持
  /// 休眠，见 [`super::aof_replication_pump`] 的 pump_backlog 判别段）
  ///
  /// 对标 C# garnetClient 字段在位形态（AofSyncTask.cs：钉线驱动构造即持
  /// 通道但唯一消费泵 RunAofSyncTaskAsync 仅由 TryConnectToReplica 启动，
  /// 钉线窗口内任务休眠）——rust 事件泵统一扫册，以本判别位承接同等休眠
  #[inline]
  pub fn has_wire(&self) -> bool {
    self.wire.read().is_some()
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:StartAddress
  #[inline]
  pub const fn start_address(&self) -> i64 {
    self.start_address
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:PreviousAddress
  ///
  /// 增量协议流游标：帧被发送通道受理（直发落网或溢流排队）即无条件
  /// 单调推进至该帧 next_address（对标 C# Consume 内无条件
  /// previousAddress = nextAddress），保证副本收到的帧序列严格满足
  /// 上一帧 next_address == 当前帧 previous_address；主端 AOF 安全截断线
  /// 取 previous_address（对标 C# SafeTruncateAof :88/:143），shipped_watermark_address 仅作背压闸门已发水位
  #[inline]
  pub fn previous_address(&self) -> i64 {
    self.previous_address.load(Ordering::Acquire)
  }

  /// 推流游标：已交给发送通道的最高位点（含溢流在途帧；补扫泵拉取定位面）
  #[inline]
  pub fn accepted_address(&self) -> i64 {
    self.accepted_address.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:ShippedWatermarkAddress
  #[inline]
  pub fn shipped_watermark_address(&self) -> i64 {
    self.shipped_watermark_address.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:IsConnected
  ///
  /// 本端标志与发送通道健康面双重判定（C# `garnetClient != null &&
  /// garnetClient.IsConnected`——对端断链由通道健康面即时感知，无需等待
  /// 下一次 Consume 失败）
  #[inline]
  pub fn is_connected(&self) -> bool {
    self.is_connected.load(Ordering::Acquire)
      && self.wire.read().as_ref().is_none_or(|w| w.is_connected())
  }

  /// 设置连接健康状态
  pub fn set_connected(&self, connected: bool) {
    self.is_connected.store(connected, Ordering::Release);
  }

  /// 远程节点 ID
  #[inline]
  pub const fn remote_node_id(&self) -> u128 {
    self.remote_node_id
  }

  /// 本地节点 ID
  #[inline]
  pub const fn local_node_id(&self) -> u128 {
    self.local_node_id
  }

  /// 落网水位推进（帧真实写入客户端通道后由发送面调用：consume 直发路径
  /// 与溢流泵搬运路径共用此单一入口；协议流游标 previous_address 的推进
  /// 归 [`Self::consume`] 独占，严禁在此耦合——主端 AOF 安全截断线取
  /// previous_address（对标 C# SafeTruncateAof :88/:143），shipped_watermark_address 仅作背压闸门已发水位）
  pub fn ratchet_shipped(&self, next_address: i64) {
    self
      .shipped_watermark_address
      .fetch_max(next_address, Ordering::AcqRel);
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:Consume
  ///
  /// 消费并分包向从节点发送 AOF 批次记录，驱动推流游标单调前进：
  /// 经发送通道转发完整记录帧（对标 C# Consume 内 garnetClient.
  /// ExecuteClusterAppendLog）；无论帧直发落网还是滞留溢流队列，协议流
  /// 游标 previous_address 均无条件单调推进至 next_address（对标 C#
  /// 无条件 previousAddress = nextAddress，溢流帧已被通道受理、泵保证
  /// 最终落网，后续帧帧头必须严格衔接本帧 next_address，否则从库误判
  /// FastAofTruncate 截断跳跃 → 重置日志地址空间损坏状态）；落网水位
  /// 仅直发帧即时推进，溢流滞留帧由泵落网后经 ratchet_shipped 补账；
  /// 主端 AOF 安全截断线取 previous_address（对标 C# SafeTruncateAof :88/:143），shipped_watermark_address 仅作背压闸门已发水位；发送失败断连并上抛（对标
  /// C# Consume 异常上抛 → RunAofSyncTaskAsync 终止 → 驱动出库）
  pub fn consume(&self, record: &[u8], current_address: i64, next_address: i64) -> io::Result<()> {
    if !self.is_connected.load(Ordering::Acquire) {
      return Err(Error::new(
        ErrorKind::NotConnected,
        format!(
          "AOF stream client disconnected! [{}]:({},{})",
          self.physical_sublog_idx,
          self.start_address,
          self.previous_address()
        ),
      ));
    }

    let wire_opt = self.wire.read().clone();
    if let Some(wire) = &wire_opt
      && !wire.is_connected()
    {
      self.set_connected(false);
      return Err(Error::new(
        ErrorKind::NotConnected,
        format!(
          "AOF stream client disconnected! [{}]:({},{})",
          self.physical_sublog_idx,
          self.start_address,
          self.previous_address()
        ),
      ));
    }

    let prev = self.previous_address.load(Ordering::Acquire);
    if current_address < prev {
      return Err(Error::new(
        ErrorKind::InvalidInput,
        format!("Current address {current_address} less than previous address {prev}"),
      ));
    }

    // 网络发送（对标 C# ExecuteClusterAppendLog：失败异常上抛，位点不推进）
    match wire_opt {
      Some(wire) => {
        match wire.append_log(
          self.local_node_id,
          self.physical_sublog_idx,
          prev,
          current_address,
          next_address,
          record,
        ) {
          // 协议流游标无条件单调推进（对标 C# previousAddress = nextAddress）：
          // 直发与溢流排队同权——帧已被通道受理，后续帧帧头必须严格衔接
          Ok(state @ (ShippedState::Shipped | ShippedState::Queued)) => {
            self
              .previous_address
              .fetch_max(next_address, Ordering::AcqRel);
            // 落网水位仅直发帧即时推进；溢流滞留帧由泵落网后经
            // ratchet_shipped 补账
            if state == ShippedState::Shipped {
              self.ratchet_shipped(next_address);
            }
          }
          Err(e) => {
            self.set_connected(false);
            return Err(e);
          }
        }
      }
      // 未注入 wire 的记账形态（单测/位面观测）：位点照常推进
      None => {
        self
          .previous_address
          .fetch_max(next_address, Ordering::AcqRel);
        self.ratchet_shipped(next_address);
      }
    }

    self
      .accepted_address
      .fetch_max(next_address, Ordering::AcqRel);

    // 成功消费即刷新脉冲节流窗口（对标 C# Consume 尾段
    // lastAdvanceTimePulse = Environment.单调毫秒）
    self
      .last_advance_time_pulse
      .store(now_ms_i64(), Ordering::Release);

    Ok(())
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:Throttle
  ///
  /// 节流探测与高水位报备检查，当产生足够增量或追平空闲时触发报备
  pub fn throttle(&self, publish_delta_bytes: i64) -> Option<i64> {
    if !self.is_connected() {
      return None;
    }

    // 背压高水位取落网水位（对标 C# shippedWatermarkAddress 折算面）：
    // 主端 AOF 安全截断线取 previous_address（对标 C# SafeTruncateAof :88/:143），shipped_watermark_address 仅作背压闸门已发水位；溢流帧落网由泵经
    // ratchet_shipped 补账，此处纯读（对标 C# Volatile.Write 折算在
    // rust 拓扑由 ratchet 单点承接）
    let target_wm = self.shipped_watermark_address.load(Ordering::Acquire);

    let last_pub = self.last_published_shipped_address.load(Ordering::Acquire);
    let pending = target_wm - last_pub;
    let last_throttle = self.last_throttle_shipped.load(Ordering::Acquire);
    let idle = target_wm == last_throttle;

    self
      .last_throttle_shipped
      .store(target_wm, Ordering::Release);

    let res = if pending > 0 && (pending >= publish_delta_bytes || idle) {
      self
        .last_published_shipped_address
        .store(target_wm, Ordering::Release);
      Some(target_wm)
    } else {
      None
    };
    self.send_advance_time_pulse();
    res
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:SendAdvanceTimePulse
  ///
  /// 带内时间脉冲真实发送（CLUSTER ADVANCE_TIME 帧，无应答、与 APPENDLOG 流量
  /// 同通道保序），促使副本推进时间戳解除跨子日志重放屏障。门控链逐条对标
  /// C#：脉冲源缺席（timePulseEnabled=false）静默 → 频率节流（C# :257 每轮
  /// 实时读 serverOptions.AofTailWitnessFreqMs，rust 经 pulse_source 的
  /// runtime_config 槽位同口径现取，CONFIG SET aof-tail-witness-freq 即时
  /// 生效）→ 物理 tail 无移动且背压无停顿则静默 → 本子日志尚有待发数据不发
  /// → 真实发送后更新 tail 快照与节流时刻
  pub fn send_advance_time_pulse(&self) {
    let Some(source) = self.pulse_source.as_ref() else {
      return;
    };

    let now = now_ms_i64();
    let last = self.last_advance_time_pulse.load(Ordering::Acquire);
    if last > 0
      && now - last
        < source
          .runtime_config
          .get_milliseconds(ServerConfigType::AofTailWitnessFreq)
    {
      return;
    }

    let tail = source.aof.log().get_tail_address(self.physical_sublog_idx);
    let tail_moved = tail != self.pulse_tail_snapshot.load(Ordering::Acquire);

    // tail 无移动通常无事可见证；例外是背压活跃停顿：追加被冻结（tail 不动）
    // 而副本可能停在重放对齐轮等待空闲子日志，须持续发心跳脉冲解锁停顿
    //（C# AnyStalled 全局 OR 同款语义）
    if !tail_moved
      && !source
        .backpressure
        .as_ref()
        .is_some_and(|bp| bp.any_stalled())
    {
      self.last_advance_time_pulse.store(now, Ordering::Release);
      return;
    }

    let sequence_number = source.aof.get_larger_than_maximum_sequence_number();

    // 本子日志尚有数据待发（对标 C# iter.NextAddress < physicalSublog.TailAddress）：
    // 记录帧随后自会到达，脉冲此刻无意义
    if self.accepted_address.load(Ordering::Acquire) < tail {
      self.last_advance_time_pulse.store(now, Ordering::Release);
      return;
    }

    let wire_opt = self.wire.read().clone();
    let Some(wire) = wire_opt else {
      self.last_advance_time_pulse.store(now, Ordering::Release);
      return;
    };
    match wire.advance_time(self.physical_sublog_idx, sequence_number) {
      Ok(()) => {
        // 发送成功才更新 tail 快照（对标 C# pulseTailSnapshot = pulseTailScratch）
        self.pulse_tail_snapshot.store(tail, Ordering::Release);
      }
      // 断链：健康面即时感知（对标 C# 异常沿 Throttle 上抛终止任务）
      Err(_) => self.set_connected(false),
    }
    self.last_advance_time_pulse.store(now, Ordering::Release);
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:Dispose
  pub fn dispose(&self) {
    DriverLifecycle::dispose(self);
  }
}
