use std::{
  fmt::{self, Formatter},
  io::{self, Error, ErrorKind},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
  },
};

use parking_lot::RwLock;
use wbase::time::now_ms;
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
  /// 上次时间脉冲发送时刻毫秒（对标 C# lastAdvanceTimePulse，TickCount64 域）
  last_advance_time_pulse: AtomicI64,
  /// 上次脉冲时的物理日志尾地址快照（对标 C# pulseTailSnapshot；
  /// rust 推流拓扑单物理子日志，数组退化为单值，初值 -1 对标 Array.Fill(-1)）
  pulse_tail_snapshot: AtomicI64,
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
  /// AofTailWitnessFreq 每轮实时读取，CONFIG SET 即时生效；缺省 100 毫秒
  /// 由 wconf 槽位播种单源承接，garnet/libs/server/Servers/
  /// GarnetServerOptions.cs:150 AofTailWitnessFreqMs）
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
  pub fn attach_wire(&self, wire: impl Into<AofSyncWire>) -> bool {
    *self.wire.write() = Some(wire.into());
    // 重置脉冲节流窗口（对标 C# RunAofSyncTaskAsync init 帧成功后
    // lastAdvanceTimePulse = TickCount64；rust init 握手在
    // TcpSessionWire::connect 内完成，attach 即任务侧等价时点）
    self
      .last_advance_time_pulse
      .store(now_ms() as i64, Ordering::Release);
    true
  }

  /// 卸载副本发送通道并主动断连
  pub fn detach_wire(&self) {
    if let Some(wire) = self.wire.write().take() {
      wire.disconnect();
    }
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:StartAddress
  #[inline]
  pub fn start_address(&self) -> i64 {
    self.start_address
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:PreviousAddress
  ///
  /// 已发送水位：仅统计真实写入客户端通道的帧（溢流滞留帧由发送泵落通道
  /// 后经 [`Self::ratchet_shipped`] 补账），主端 AOF 安全截断线取此值钳制
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

  /// 子日志索引
  #[inline]
  pub fn physical_sublog_idx(&self) -> usize {
    self.physical_sublog_idx
  }

  /// 远程节点 ID
  #[inline]
  pub fn remote_node_id(&self) -> u128 {
    self.remote_node_id
  }

  /// 本地节点 ID
  #[inline]
  pub fn local_node_id(&self) -> u128 {
    self.local_node_id
  }

  /// 已发送水位推进（帧真实写入客户端通道后由发送面调用：consume 直发路径
  /// 与溢流泵搬运路径共用此单一入口）
  pub(crate) fn ratchet_shipped(&self, next_address: i64) {
    self
      .previous_address
      .fetch_max(next_address, Ordering::AcqRel);
    self
      .shipped_watermark_address
      .fetch_max(next_address, Ordering::AcqRel);
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:Consume
  ///
  /// 消费并分包向从节点发送 AOF 批次记录，驱动推流游标单调前进：
  /// 经发送通道转发完整记录帧（对标 C# Consume 内 garnetClient.
  /// ExecuteClusterAppendLog）；帧直发落通道则同步推进已发送水位，帧滞留
  /// 溢流队列仅推进推流游标（水位由发送泵落通道后补账，未落网帧严禁计入
  /// 已发送水位）；发送失败断连并上抛（对标 C# Consume 异常上抛 →
  /// RunAofSyncTaskAsync 终止 → 驱动出库）
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
      Some(wire) => match wire.append_log(
        self.local_node_id,
        self.physical_sublog_idx,
        prev,
        current_address,
        next_address,
        record,
      ) {
        // 帧已落通道：已发送水位即时推进（对标 C# previousAddress = nextAddress）
        Ok(ShippedState::Shipped) => self.ratchet_shipped(next_address),
        // 帧滞留溢流队列：仅推进推流游标，水位由泵落通道后经 ratchet_shipped 补账
        Ok(ShippedState::Queued) => {}
        Err(e) => {
          self.set_connected(false);
          return Err(e);
        }
      },
      // 未注入 wire 的记账形态（单测/位面观测）：位点照常推进
      None => self.ratchet_shipped(next_address),
    }

    self
      .accepted_address
      .fetch_max(next_address, Ordering::AcqRel);

    // 成功消费即刷新脉冲节流窗口（对标 C# Consume 尾段
    // lastAdvanceTimePulse = Environment.TickCount64）
    self
      .last_advance_time_pulse
      .store(now_ms() as i64, Ordering::Release);

    Ok(())
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:Throttle
  ///
  /// 节流探测与高水位报备检查，当产生足够增量或追平空闲时触发报备
  pub fn throttle(&self, publish_delta_bytes: i64) -> Option<i64> {
    if !self.is_connected() {
      return None;
    }

    let shipped = self.previous_address.load(Ordering::Acquire);
    let target_wm = self
      .shipped_watermark_address
      .fetch_max(shipped, Ordering::AcqRel)
      .max(shipped);

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

    let now = now_ms() as i64;
    if now - self.last_advance_time_pulse.load(Ordering::Acquire)
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

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:LogRunAofSyncTask
  pub fn log_run_aof_sync_task(
    &self,
    physical_sublog_idx: usize,
    start_address: i64,
    previous_address: i64,
  ) {
    log::trace!(
      "physicalSublogIdx:{physical_sublog_idx}, startAddress: {start_address}, previousAddress: {previous_address}"
    );
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:RunAofSyncTaskAsync
  pub async fn run_aof_sync_task_async(&self) {
    self.log_run_aof_sync_task(
      self.physical_sublog_idx,
      self.start_address,
      self.previous_address(),
    );
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use parking_lot::Mutex;
  use wconf::RuntimeServerOptions;
  use wnode::GarnetLog;

  use super::*;
  use crate::server::replication::replica_wire::test_wire::{CallbackWire, FrameSink};

  /// 内存单物理子日志 AOF 门面（脉冲源构造）
  fn pulse_aof() -> Arc<GarnetAppendOnlyFile> {
    let options = RuntimeServerOptions::default();
    let (_dirs, backends) = wnode_test::test_sublogs("aof_sync_task", 1);
    Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
      &options,
      None,
    ))
  }

  #[test]
  fn test_aof_sync_task_lifecycle_and_throttle() {
    let task = AofSyncTask::new(0, 100, 0x10CA1, 0x2E707E, None);
    assert_eq!(task.start_address(), 100);
    assert_eq!(task.previous_address(), 100);
    assert_eq!(task.accepted_address(), 100);
    assert_eq!(task.shipped_watermark_address(), 100);
    assert!(task.is_connected());

    let dummy_data = b"SET k v";
    task.consume(dummy_data, 100, 200).expect("consume ok");
    assert_eq!(task.previous_address(), 200);

    // 节流触发报备（delta 设置为 50，已产生 100 增量）
    let pub_addr = task.throttle(50);
    assert_eq!(pub_addr, Some(200));

    // 再次节流，未产生新增量，不重复触发
    assert_eq!(task.throttle(50), None);
  }

  /// 发送通道接线：consume 经 wire 投递真实帧字节；投递失败断连且位点不推进
  #[test]
  fn test_consume_forwards_frame_via_wire() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let rec = received.clone();
    let wire = Arc::new(CallbackWire::new(rec));
    let task = AofSyncTask::new(0, 64, 0x0DE1, 0x2E707E, None);
    assert!(task.attach_wire(wire));

    task.consume(b"\x00payload", 64, 78).expect("consume ok");
    assert_eq!(task.previous_address(), 78);
    let frames = received.lock();
    assert_eq!(frames.len(), 1, "帧应经发送通道投递");
    // 整帧锁定（node_id 0x0DE1 渲染 32 字符定长小写 hex，元素依次为
    // CLUSTER APPENDLOG node_id 子日志下标 前址 现址 次址 记录负载）
    assert_eq!(
      frames[0].as_slice(),
      b"*8\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$32\r\n00000000000000000000000000000de1\r\n$1\r\n0\r\n$2\r\n64\r\n$2\r\n64\r\n$2\r\n78\r\n$8\r\n\x00payload\r\n"
    );
  }

  /// 发送失败 → 断连 + 位点不推进（对标 C# Consume 异常上抛语义）
  #[test]
  fn test_consume_send_failure_disconnects() {
    let wire = Arc::new(CallbackWire::new(FrameSink::Reject));
    let task = AofSyncTask::new(0, 64, 0x0DE1, 0x2E707E, None);
    assert!(task.attach_wire(wire));

    assert!(task.consume(b"rec", 64, 67).is_err());
    assert!(!task.is_connected());
    assert_eq!(task.previous_address(), 64, "失败路径位点不推进");
    // 断连后后续 consume 直接拒绝
    assert!(task.consume(b"rec", 64, 67).is_err());
  }

  /// 溢流滞留帧严禁计入已发送水位：Queued 仅推进推流游标，水位由
  /// ratchet_shipped 补账（对标 C# previousAddress 只在帧离开用户态后推进）
  #[test]
  fn test_consume_queued_frame_defers_watermark() {
    let queued = Arc::new(Mutex::new(Vec::new()));
    let wire = Arc::new(CallbackWire::new(FrameSink::Queue(queued.clone())));
    let task = AofSyncTask::new(0, 64, 0x0DE1, 0x2E707E, None);
    assert!(task.attach_wire(wire));

    task.consume(b"rec1", 64, 80).expect("consume ok");
    assert_eq!(task.accepted_address(), 80, "推流游标应推进");
    assert_eq!(task.previous_address(), 64, "溢流滞留帧不得计入已发送水位");
    assert_eq!(task.shipped_watermark_address(), 64);

    // 泵落通道补账：水位推进至已发面
    task.ratchet_shipped(80);
    assert_eq!(task.previous_address(), 80);
    assert_eq!(task.shipped_watermark_address(), 80);
    assert_eq!(queued.lock().len(), 1, "帧应滞留溢流队列表");
  }

  /// 脉冲源缺席（对标 C# timePulseEnabled=false）：脉冲链整体静默不发送
  #[test]
  fn test_advance_time_pulse_without_source_is_silent() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let wire = Arc::new(CallbackWire::new(received.clone()));
    let task = AofSyncTask::new(0, 0, 0x0DE1, 0x2E707E, None);
    assert!(task.attach_wire(wire));
    task.send_advance_time_pulse();
    assert!(received.lock().is_empty(), "无脉冲源不得发送");
  }

  /// 时间脉冲链：构造期注入源 + 跨越频率窗口后 tail 移动触发真实发送，
  /// 序列号取门面生成器；tail 无移动且无停顿时静默
  #[test]
  fn test_advance_time_pulse_sends_frame() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let wire = Arc::new(CallbackWire::new(received.clone()));
    let task = AofSyncTask::new(
      0,
      0,
      0x0DE1,
      0x2E707E,
      Some(Arc::new(TimePulseSource {
        aof: pulse_aof(),
        backpressure: None,
        runtime_config: Arc::new(RuntimeServerConfig::new(RuntimeServerOptions::default())),
      })),
    );
    assert!(task.attach_wire(wire));

    // 推流游标先追平日志尾（真实段设备空日志尾 = 设备首地址 0；对标 C# iter
    // 消费到 tail 后 NextAddress == TailAddress 的可发脉冲前置态）
    task.consume(b"x", 0, 0).expect("consume ok");

    // 首次脉冲：tail 快照 -1 → tail(0) 视作移动 → 发送（对标 Array.Fill(-1) 初值）
    task.last_advance_time_pulse.store(0, Ordering::Release);
    task.send_advance_time_pulse();
    {
      let frames = received.lock();
      assert_eq!(frames.len(), 2, "应为 1 记录帧 + 1 脉冲帧");
      // 整帧锁定：尾元素 = 子日志下标 0 + 序列号（单物理日志模式序列号生成器
      // 缺席恒 0，取严格大于即 1），帧名/元素数/元素值任一漂移即红
      assert_eq!(
        frames[1].as_slice(),
        b"*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n$1\r\n0\r\n$1\r\n1\r\n",
        "第二帧应为 CLUSTER ADVANCE_TIME 脉冲整帧"
      );
    }

    // 快照已入账：tail 无移动且无停顿 → 静默（跨越频率窗口后仍不发）
    assert_eq!(
      task.pulse_tail_snapshot.load(Ordering::Acquire),
      0,
      "发送后快照应入账"
    );
    task.last_advance_time_pulse.store(0, Ordering::Release);
    task.send_advance_time_pulse();
    assert_eq!(received.lock().len(), 2, "tail 无移动且无停顿应静默");
  }

  /// 脉冲帧字节面：advance_time 经内存通道投递 4 元素 CLUSTER ADVANCE_TIME 帧
  #[test]
  fn test_advance_time_frame_via_wire() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let wire = CallbackWire::new(received.clone());
    wire.advance_time(0, 42).expect("pulse ok");
    let frames = received.lock();
    assert_eq!(frames.len(), 1);
    // 整帧锁定：advance_time(0, 42) → 子日志下标 0 + 序列号 42
    assert_eq!(
      frames[0].as_slice(),
      b"*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n$1\r\n0\r\n$2\r\n42\r\n"
    );
  }

  /// 成功消费刷新脉冲节流窗口（对标 C# Consume 尾段
  /// lastAdvanceTimePulse = Environment.TickCount64）：consume 后 100ms
  /// 频率窗口重启，与 C# 节流口径一致
  #[test]
  fn test_consume_refreshes_pulse_throttle_window() {
    let task = AofSyncTask::new(0, 100, 0x10CA1, 0x2E707E, None);
    task.last_advance_time_pulse.store(0, Ordering::Release);
    task.consume(b"SET k v", 100, 200).expect("consume ok");
    assert!(
      task.last_advance_time_pulse.load(Ordering::Acquire) > 0,
      "成功消费应刷新节流窗口"
    );
  }
}
