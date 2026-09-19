//! 主端 → 副本发送通道（网络会话抽象）
//!
//! 对标 C# AofSyncTask 的 garnetClient 字段（libs/client/ClientSession/
//! GarnetClientSession.cs）——AofSyncTask 构造时按副本端点建立
//! GarnetClientSession，Consume 内逐记录调 ExecuteClusterAppendLog 写入
//! 发送缓冲，Throttle 时 CompletePending 冲刷。Rust 依赖方向反转：
//! 会话端口以 trait 注入（TCP 形态包装 wconn 客户端会话；测试形态为
//! 帧写入回调的内存通道），AofSyncTask 消费面只依赖同步发送端口。
//!
//! C# 发送缓冲满时 Send 内部自动 Flush（同步阻塞）；Rust 的推流端口
//! （WalLog::replication_sink）契约要求回调无阻塞，故 TCP 形态以
//! 「饱和 → 溢流队列 + 常驻泵搬运」承接同等背压语义：溢流上限
//! MAX_OVERFLOW_ENTRIES 硬封顶，超限即断连（通道健康面感知，
//! 对齐 C# 断链 → 写失败 → 剔除重同步的治理路径）。
//!
//! 已发送水位语义（对标 C# previousAddress 只在帧离开用户态后推进）：
//! append_log 返回 ShippedState 区分「帧已写入客户端命令通道」（Shipped，
//! 计入水位）与「帧滞留溢流队列」（Queued，严禁计入）；溢流帧由泵真实
//! 写入通道后经 ratchet_after_ship 回推任务水位（Weak 引用破环），保证
//! 主端 AOF 安全截断线永不越过未落网帧。

use std::{
  io::{self, Error, ErrorKind},
  sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{runtime::spawn, time};
use parking_lot::Mutex;
use wbase::{hex::hex_str_u128, pool::EventWorkQueue};
use wconn::session::{GarnetClientSession, encode_advance_time_frame, encode_append_log_frame};
#[cfg(feature = "tls")]
use wconn::tls::ClientTlsConfig;
use wdev::SegmentedDevice;
use wnode::MessageConsumerFace;

use crate::server::replication::{
  aof_sync_task::AofSyncTask, cluster_replication_session::ClusterReplicationSession,
};

/// 帧发送落点状态（对标 C# ExecuteClusterAppendLog 同步写网络发送缓冲
/// 成功返回的语义切分：rust 客户端通道饱和时帧滞留溢流队列，两者必须区分）
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShippedState {
  /// 帧已写入客户端命令通道（对标 C# 写入网络发送缓冲，计入已发送水位）
  Shipped,
  /// 帧滞留溢流队列待发（未落通道，严禁计入已发送水位）
  Queued,
}

/// 帧写入接收端具体枚举（消除动态分发闭包）
#[derive(Clone)]
pub enum FrameSink {
  /// 收集帧到列表
  Buffer(Arc<Mutex<Vec<Vec<u8>>>>),
  /// 恒拒绝投递（测试断连语义）
  Reject,
  /// 回调函数指针
  Fn(fn(&[u8]) -> bool),
  /// 投递到测试会话（SegmentedDevice），可选计数
  Session {
    session: Arc<Mutex<ClusterReplicationSession<SegmentedDevice>>>,
    seen: Option<Arc<Mutex<usize>>>,
  },
  /// 模拟通道饱和：帧暂存待发表（投递成功返回 Queued，测试溢流水位语义）
  Queue(Arc<Mutex<Vec<Vec<u8>>>>),
}

impl FrameSink {
  /// 投递一帧；None 即拒收（通道转断连态）
  pub fn call(&self, frame: &[u8]) -> Option<ShippedState> {
    match self {
      Self::Buffer(buf) => {
        buf.lock().push(frame.to_vec());
        Some(ShippedState::Shipped)
      }
      Self::Queue(buf) => {
        buf.lock().push(frame.to_vec());
        Some(ShippedState::Queued)
      }
      Self::Reject => None,
      Self::Fn(f) => f(frame).then_some(ShippedState::Shipped),
      Self::Session { session, seen } => {
        // 生产等价消费序（对标网络泵：帧入会话自有接收缓冲 → 唯一入口
        // 消费 → 致命断流哨兵复查）；应答落临时 scratch 复用缓冲，记录帧
        // 热路径零堆分配。致命断流（APPENDLOG 拒收 / 畸形帧）视同 sink
        // 拒收 → 内存通道转断连态，主端健康面感知（对标 C# 异常断链 →
        // 写失败 → 剔除重同步）
        let mut scratch = Vec::new();
        let mut session = session.lock();
        session.recv_buffer.extend_from_slice(frame);
        let remaining = session.try_consume_messages_into(&mut scratch);
        let fatal = session.take_fatal_disconnect();
        drop(session);
        if let Some(counter) = seen {
          *counter.lock() += 1;
        }
        (remaining == Some(0) && (scratch.is_empty() || scratch == b"+OK\r\n") && !fatal)
          .then_some(ShippedState::Shipped)
      }
    }
  }
}

impl From<Arc<Mutex<Vec<Vec<u8>>>>> for FrameSink {
  fn from(buf: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
    Self::Buffer(buf)
  }
}

impl From<fn(&[u8]) -> bool> for FrameSink {
  fn from(f: fn(&[u8]) -> bool) -> Self {
    Self::Fn(f)
  }
}

impl From<Arc<Mutex<ClusterReplicationSession<SegmentedDevice>>>> for FrameSink {
  fn from(session: Arc<Mutex<ClusterReplicationSession<SegmentedDevice>>>) -> Self {
    Self::Session {
      session,
      seen: None,
    }
  }
}

impl From<ClusterReplicationSession<SegmentedDevice>> for FrameSink {
  fn from(session: ClusterReplicationSession<SegmentedDevice>) -> Self {
    Self::Session {
      session: Arc::new(Mutex::new(session)),
      seen: None,
    }
  }
}

/// 内存通道形态：帧写入回调直连副本会话（同进程最小可测发送通道）
pub struct CallbackWire {
  sink: FrameSink,
  connected: AtomicBool,
}

impl CallbackWire {
  /// 创建内存通道（sink 接收完整 RESP 请求帧字节）
  pub fn new(sink: impl Into<FrameSink>) -> Self {
    Self {
      sink: sink.into(),
      connected: AtomicBool::new(true),
    }
  }

  /// 逐记录帧转发（payload 为完整 AOF 记录帧：8B 记录头 + 负载）
  pub fn append_log(
    &self,
    node_id: u128,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<ShippedState> {
    if !self.connected.load(Ordering::Acquire) {
      return Err(Error::new(ErrorKind::NotConnected, "memory wire closed"));
    }
    // 协议帧参数：节点 id 仅在帧编码面渲染 hex
    let frame = encode_append_log_frame(
      &hex_str_u128(node_id),
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
      payload,
    );
    self.deliver(&frame)
  }

  /// 带内 CLUSTER ADVANCE_TIME 时间脉冲帧直发（对标 SendAdvanceTimePulse 内存通道投递，无饱和面恒即时）
  pub fn advance_time(
    &self,
    physical_sublog_idx: usize,
    sequence_number: i64,
  ) -> io::Result<ShippedState> {
    if !self.connected.load(Ordering::Acquire) {
      return Err(Error::new(ErrorKind::NotConnected, "memory wire closed"));
    }
    let frame = encode_advance_time_frame(physical_sublog_idx, sequence_number);
    self.deliver(&frame)
  }

  /// 帧投递公共路径：拒收即转断连态（对标副本会话关闭语义）
  fn deliver(&self, frame: &[u8]) -> io::Result<ShippedState> {
    match self.sink.call(frame) {
      Some(state) => Ok(state),
      None => {
        self.connected.store(false, Ordering::Release);
        Err(Error::new(ErrorKind::ConnectionReset, "sink rejected"))
      }
    }
  }

  /// 连接健康面
  pub fn is_connected(&self) -> bool {
    self.connected.load(Ordering::Acquire)
  }

  /// 标记断连
  pub fn disconnect(&self) {
    self.connected.store(false, Ordering::Release);
  }
}

/// TCP 会话形态：wconn 客户端会话 + 溢流事件驱动泵
///
/// 对标 C# GarnetClientSession 的网络面组合：Fire-and-forget 帧写入
/// 客户端命令通道（try_send），通道饱和时进入有锁溢流队列，由事件驱动
/// 泵被动唤醒搬运，饱和重试经由客户端通道异步挂起消灭空转轮询。
/// 水位语义：帧真实写入客户端通道后才允许推进已发送水位（溢流滞留帧
/// 由泵落通道后经 ratchet 回推，见 [`TcpSessionWire::ratchets`]）
pub struct TcpSessionWire {
  client: GarnetClientSession,
  node_id: u128,
  overflow: Arc<EventWorkQueue<WireFrame>>,
  /// 在途帧标志：泵手上持有已出队、未入客户端通道的帧（直发禁入，保序）
  in_flight: Arc<AtomicBool>,
  pump_alive: Arc<AtomicBool>,
  /// 溢流帧落通道后的水位回推端（按物理子日志下标索引；Weak 破
  /// task → wire → task 引用环，对标 C# 同步发送无需回推的时序差异面）
  ratchets: Mutex<Vec<Weak<AofSyncTask>>>,
}

/// 溢流帧载荷（重发时逐字段编码，与首发同一路径）
enum WireFrame {
  /// AOF 记录帧（落通道后回推任务已发送水位）
  AppendLog(OverflowEntry),
  /// CLUSTER ADVANCE_TIME 时间脉冲帧（无水位语义，仅保序随流）
  AdvanceTime {
    physical_sublog_idx: usize,
    sequence_number: i64,
  },
}

/// 副本同步帧级/RPC 应答超时（映射 garnet/libs/server/Servers/
/// GarnetServerOptions.cs:420 ReplicaSyncTimeout 默认 5s，defaults.conf:355
/// 同值；C# 同一旋钮兼承建连限时（ConnectAsync(TotalMilliseconds)）与快照
/// 逐帧 / RPC 应答限时（ReplicaSyncSession.cs:140/:184 WaitAsync）；
/// rust 配置面未转写该选项，先以常量对齐默认口径）
pub(crate) const REPLICA_SYNC_TIMEOUT: Duration = Duration::from_secs(5);

/// 副本 attach 级握手/编排超时（映射 garnet/libs/server/Servers/
/// GarnetServerOptions.cs:425 ReplicaAttachTimeout 默认 60s，defaults.conf:358
/// 同值；C# 经 RuntimeServerConfig.cs:253 回填 REPL_ATTACH_TIMEOUT，用于
/// INITIATE_REPLICA_SYNC / ATTACH_SYNC 应答 WaitAsync 限时）
pub(crate) const REPL_ATTACH_TIMEOUT: Duration = Duration::from_secs(60);

/// 溢流帧字段（重发时逐字段编码，与首发同一路径；Box<[u8]> 节约 8B 容量字段）
struct OverflowEntry {
  physical_sublog_idx: usize,
  previous_address: i64,
  current_address: i64,
  next_address: i64,
  payload: Box<[u8]>,
}

/// 帧真实写入客户端通道后的水位回推（Weak 升级失败即任务已亡，静默跳过）
fn ratchet_after_ship(ratchets: &[Weak<AofSyncTask>], frame: &WireFrame) {
  let WireFrame::AppendLog(entry) = frame else {
    return; // 时间脉冲帧无水位语义
  };
  if let Some(task) = ratchets
    .get(entry.physical_sublog_idx)
    .and_then(Weak::upgrade)
  {
    task.ratchet_shipped(entry.next_address);
  }
}

/// 溢流帧写入客户端通道（AppendLog 与 AdvanceTime 同通道 FIFO 保序）
async fn ship_frame(
  client: &GarnetClientSession,
  node_id: u128,
  frame: &WireFrame,
) -> wconn::Result<()> {
  match frame {
    WireFrame::AppendLog(e) => {
      client
        .execute_cluster_append_log_async(
          // 协议帧参数：节点 id 仅在帧发送面渲染 hex
          &hex_str_u128(node_id),
          e.physical_sublog_idx,
          e.previous_address,
          e.current_address,
          e.next_address,
          &e.payload,
        )
        .await
    }
    WireFrame::AdvanceTime {
      physical_sublog_idx,
      sequence_number,
    } => client.execute_cluster_advance_time(*physical_sublog_idx, *sequence_number),
  }
}

/// 溢流帧同步写入客户端通道（贪婪快路径）
fn try_ship_frame(
  client: &GarnetClientSession,
  node_id: u128,
  frame: &WireFrame,
) -> wconn::Result<()> {
  match frame {
    WireFrame::AppendLog(e) => client.execute_cluster_append_log(
      &hex_str_u128(node_id),
      e.physical_sublog_idx,
      e.previous_address,
      e.current_address,
      e.next_address,
      &e.payload,
    ),
    WireFrame::AdvanceTime {
      physical_sublog_idx,
      sequence_number,
    } => client.execute_cluster_advance_time(*physical_sublog_idx, *sequence_number),
  }
}

impl TcpSessionWire {
  /// 最大允许溢流队列长度，防止对端假死导致内存无界膨胀
  pub const MAX_OVERFLOW_ENTRIES: usize = 10_000;

  /// 建立副本连接并发送 AOF 复制流初始化帧（等 +OK）
  ///
  /// 对标 C# AofSyncTask.RunAofSyncTaskAsync 的建连序列：ConnectAsync →
  /// ExecuteClusterAppendLogInit(-1,-1,-1)（成功返回后调用方才挂推流端口
  /// 与补扫泵，保证初始化帧先于任何记录帧）
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:InitializeIfNeeded
  ///
  /// C# InitializeIfNeeded 在 client.NeedsInitialization 时经
  /// SetClusterSyncHeader 把 LocalNodeId 写入同步头部；rust 的节点标识由
  /// 本建连序列 init 帧参数（node_id）一次性下发，无二段初始化。
  pub async fn connect(
    endpoint: &str,
    node_id: u128,
    physical_sublog_idx: usize,
    auth_username: Option<&str>,
    auth_password: Option<&str>,
    #[cfg(feature = "tls")] tls: Option<&Arc<ClientTlsConfig>>,
  ) -> io::Result<Arc<Self>> {
    let mut client = GarnetClientSession::new(
      endpoint.to_string(),
      auth_username.map(str::to_string),
      auth_password.map(str::to_string),
      Some(format!(
        "AofSyncTask-{physical_sublog_idx}:({})",
        hex_str_u128(node_id)
      )),
    );
    // 出站 TLS 单源透传（对标 AofSyncTask.cs:138 构造 GarnetClientSession 的
    // `tlsOptions: serverOptions.TlsOptions?.TlsClientOptions` 形参位）
    #[cfg(feature = "tls")]
    client.set_tls(tls.cloned());
    // 建连 + AUTH 握手整体限时（对标 C# RunAofSyncTaskAsync
    // ConnectAsync(ReplicaSyncTimeout)：对端 DROP 时免挂 OS 级分钟超时，
    // 快速失败走副本剔除重同步路径）
    time::timeout(REPLICA_SYNC_TIMEOUT, client.connect_async())
      .await
      .map_err(|_| Error::new(ErrorKind::TimedOut, "replica sync connect timed out"))?
      .map_err(|e| Error::new(ErrorKind::ConnectionRefused, e.to_string()))?;
    let resp = client
      .execute_cluster_append_log_init(
        // 协议帧参数：初始化帧节点 id 渲染 hex
        &hex_str_u128(node_id),
        physical_sublog_idx,
        -1,
        -1,
        -1,
      )
      .await
      .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e.to_string()))?;
    if resp != "OK" {
      return Err(Error::new(
        ErrorKind::ConnectionAborted,
        "Failed to initialize AofSync stream!",
      ));
    }

    let overflow = Arc::new(EventWorkQueue::new());
    let wire = Arc::new(Self {
      client,
      node_id,
      overflow: Arc::clone(&overflow),
      in_flight: Arc::new(AtomicBool::new(false)),
      pump_alive: Arc::new(AtomicBool::new(true)),
      ratchets: Mutex::new(Vec::new()),
    });
    wire.start_pump(overflow);
    Ok(wire)
  }

  /// 注入溢流落通道后的水位回推端（attach 期按任务物理子日志下标对位）
  pub fn set_ratchets(&self, tasks: Vec<Weak<AofSyncTask>>) {
    *self.ratchets.lock() = tasks;
  }

  /// 溢流搬运泵：事件驱动被动唤醒，尝试把饱和帧排入客户端命令通道（Weak 弱引用破环，防孤儿任务泄漏）
  fn start_pump(self: &Arc<Self>, overflow: Arc<EventWorkQueue<WireFrame>>) {
    let weak_wire = Arc::downgrade(self);
    let alive = Arc::clone(&self.pump_alive);
    spawn(async move {
      let mut pending_frame = None;
      while alive.load(Ordering::Acquire) {
        let frame = if let Some(p) = pending_frame.take() {
          p
        } else {
          // 等待队列有新数据或关闭
          if !overflow.wait_to_read().await {
            break; // 通道关闭（wire 释放）且已排空
          }
          if !alive.load(Ordering::Acquire) {
            break;
          }
          let Some(wire) = weak_wire.upgrade() else {
            break;
          };
          let Some(frame) = overflow.try_pop() else {
            continue;
          };
          wire.in_flight.store(true, Ordering::Release);
          frame
        };

        if !alive.load(Ordering::Acquire) {
          break;
        }

        let Some(wire) = weak_wire.upgrade() else {
          break;
        };

        if !wire.is_connected() {
          break;
        }

        // 异步推入客户端命令通道
        if ship_frame(&wire.client, wire.node_id, &frame)
          .await
          .is_err()
        {
          wire.disconnect();
          break;
        }

        // 帧已落通道：回推任务已发送水位，清除 in_flight
        ratchet_after_ship(&wire.ratchets.lock(), &frame);
        wire.in_flight.store(false, Ordering::Release);

        // 贪婪非阻塞消费
        while let Some(next) = overflow.try_pop() {
          wire.in_flight.store(true, Ordering::Release);
          if try_ship_frame(&wire.client, wire.node_id, &next).is_err() {
            // 通道饱和，暂存到 pending_frame，等待下一轮 async 重发
            pending_frame = Some(next);
            break;
          }
          // 帧已落通道：回推水位，清除 in_flight，继续消费
          ratchet_after_ship(&wire.ratchets.lock(), &next);
          wire.in_flight.store(false, Ordering::Release);
        }
      }
    })
    .detach();
  }

  /// 直发客户端通道或入溢流队列（公共饱和判定、溢流排队与超限断连保护）
  ///
  /// 若溢流队列为空且无在途帧则尝试直发客户端通道（零多余分配）；若已有积压
  /// 或通道饱和，则延迟构造帧入溢流队列保序；超限即断连并报 BrokenPipe
  #[inline]
  fn send_or_enqueue(
    &self,
    try_direct: impl FnOnce(&GarnetClientSession) -> wconn::Result<()>,
    make_frame: impl FnOnce() -> WireFrame,
  ) -> io::Result<ShippedState> {
    if !self.is_connected() {
      return Err(Error::new(ErrorKind::NotConnected, "wire session closed"));
    }
    let should_try_direct = self.overflow.is_empty() && !self.in_flight.load(Ordering::Acquire);
    if should_try_direct && try_direct(&self.client).is_ok() {
      return Ok(ShippedState::Shipped);
    }

    if self.overflow.len() >= Self::MAX_OVERFLOW_ENTRIES || !self.overflow.push(make_frame()) {
      self.disconnect();
      return Err(Error::new(
        ErrorKind::BrokenPipe,
        "Replication send buffer overflow exceeded limit",
      ));
    }
    Ok(ShippedState::Queued)
  }

  /// 单帧经客户端命令通道发出（fire-and-forget），饱和时入溢流队列
  ///
  /// 返回 Err 仅当会话已关闭或溢流超过上限；通道饱和不失败（帧暂存由
  /// 事件驱动泵重试，落通道后经 ratchet 回推水位），返回 Queued 提示
  /// 调用方该帧尚未计入已发送水位
  fn send_frame(
    &self,
    node_id: u128,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<ShippedState> {
    // 0 兜底本 wire 构造期节点 id（对位原空串缺席语义）
    let target_node_id = if node_id != 0 { node_id } else { self.node_id };

    self.send_or_enqueue(
      |client| {
        client.execute_cluster_append_log(
          // 协议帧参数：节点 id 仅在帧发送面渲染 hex
          &hex_str_u128(target_node_id),
          physical_sublog_idx,
          previous_address,
          current_address,
          next_address,
          payload,
        )
      },
      || {
        WireFrame::AppendLog(OverflowEntry {
          physical_sublog_idx,
          previous_address,
          current_address,
          next_address,
          payload: Box::from(payload),
        })
      },
    )
  }

  /// 逐记录帧转发（payload 为完整 AOF 记录帧：8B 记录头 + 负载）
  pub fn append_log(
    &self,
    node_id: u128,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<ShippedState> {
    self.send_frame(
      node_id,
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
      payload,
    )
  }

  /// 带内时间脉冲发送（对标 SendAdvanceTimePulse 网络会话投递）：溢流队列为空时直发客户端通道（与后续 APPENDLOG 帧
  /// 同通道 FIFO 保序）；通道饱和或已有积压时入溢流队列随流搬运（对标 C#
  /// pulse 与 APPENDLOG 同连接 ordered 语义）
  pub fn advance_time(&self, physical_sublog_idx: usize, sequence_number: i64) -> io::Result<()> {
    self
      .send_or_enqueue(
        |client| client.execute_cluster_advance_time(physical_sublog_idx, sequence_number),
        || WireFrame::AdvanceTime {
          physical_sublog_idx,
          sequence_number,
        },
      )
      .map(|_| ())
  }

  /// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:ExecuteAttachSyncAsync
  ///
  /// diskless 恢复握手帧经推流连接带内发送（同通道 FIFO 保序：副本必先
  /// 完成恢复对齐再收任何记录帧），应答为副本恢复位点串；对端 -ERR 经
  /// Err 透出。调用点在驱动挂载之前，推流泵尚无帧在途，直发无争用
  pub async fn attach_sync(&self, sync_metadata: &[u8]) -> wconn::Result<String> {
    self.client.execute_cluster_attach_sync(sync_metadata).await
  }

  /// 连接健康面（对标 C# GarnetClientSession.IsConnected）
  pub fn is_connected(&self) -> bool {
    self.pump_alive.load(Ordering::Acquire) && self.client.is_connected()
  }

  /// 标记断连（对标 C# GarnetClientSession.Dispose 的会话关闭面）
  pub fn disconnect(&self) {
    self.pump_alive.store(false, Ordering::Release);
  }
}

impl Drop for TcpSessionWire {
  fn drop(&mut self) {
    self.disconnect();
    // 关闭溢流通道，唤醒挂起中的泵随 wire 释放同步退出
    self.overflow.close();
  }
}

/// 主端 → 副本发送通道具体枚举（消除动态分发）
#[derive(Clone)]
pub enum AofSyncWire {
  /// TCP 会话通道（生产）
  Tcp(Arc<TcpSessionWire>),
  /// 内存回调通道（测试）
  Callback(Arc<CallbackWire>),
}

impl AofSyncWire {
  /// 逐记录帧转发；返回帧落点状态（Queued 即溢流滞留，严禁计入已发送水位）
  pub fn append_log(
    &self,
    node_id: u128,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<ShippedState> {
    match self {
      Self::Tcp(w) => w.append_log(
        node_id,
        physical_sublog_idx,
        previous_address,
        current_address,
        next_address,
        payload,
      ),
      Self::Callback(w) => w.append_log(
        node_id,
        physical_sublog_idx,
        previous_address,
        current_address,
        next_address,
        payload,
      ),
    }
  }

  /// 带内时间脉冲发送（无应答，与 APPENDLOG 流量同通道保序）
  pub fn advance_time(&self, physical_sublog_idx: usize, sequence_number: i64) -> io::Result<()> {
    match self {
      Self::Tcp(w) => w.advance_time(physical_sublog_idx, sequence_number),
      Self::Callback(w) => w
        .advance_time(physical_sublog_idx, sequence_number)
        .map(|_| ()),
    }
  }

  /// 注入溢流落通道后的水位回推端（仅 TCP 形态有溢流面，内存通道直发免注）
  pub fn set_ratchets(&self, tasks: Vec<Weak<AofSyncTask>>) {
    if let Self::Tcp(w) = self {
      w.set_ratchets(tasks);
    }
  }

  pub fn is_connected(&self) -> bool {
    match self {
      Self::Tcp(w) => w.is_connected(),
      Self::Callback(w) => w.is_connected(),
    }
  }

  pub fn disconnect(&self) {
    match self {
      Self::Tcp(w) => w.disconnect(),
      Self::Callback(w) => w.disconnect(),
    }
  }
}

impl From<Arc<TcpSessionWire>> for AofSyncWire {
  fn from(w: Arc<TcpSessionWire>) -> Self {
    Self::Tcp(w)
  }
}

impl From<Arc<CallbackWire>> for AofSyncWire {
  fn from(w: Arc<CallbackWire>) -> Self {
    Self::Callback(w)
  }
}

#[cfg(test)]
mod tests {
  use parking_lot::Mutex;
  use wconn::session::encode_append_log_init_frame;
  use wresp::frame::parse_resp_frame;

  use super::*;

  /// 内存通道帧收发：回调收到编码帧且断连后拒绝发送
  #[test]
  fn callback_wire_frame_delivery_and_disconnect() {
    let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let wire = CallbackWire::new(received.clone());

    assert!(
      wire
        .append_log(0x0000_DE11, 0, 64, 64, 128, b"\x00\xffpayload")
        .is_ok()
    );
    let frames = received.lock();
    assert_eq!(frames.len(), 1);
    // 收到的帧可被二进制数组解析器还原为 8 元素
    let (_consumed, items) = parse_resp_frame(&frames[0])
      .expect("协议合法")
      .expect("complete");
    assert_eq!(items.len(), 8);
    // 节点 id 协议面渲染 32 字符小写 hex
    assert_eq!(items[2], b"0000000000000000000000000000de11");
    assert_eq!(items[7], b"\x00\xffpayload");

    drop(frames);
    wire.disconnect();
    assert!(!wire.is_connected());
    assert!(wire.append_log(0x0DE1, 0, 64, 64, 128, b"x").is_err());
  }

  /// 回调拒绝投递 → 通道转断连态（对标副本会话关闭语义）
  #[test]
  fn callback_wire_sink_reject_disconnects() {
    let wire = CallbackWire::new(FrameSink::Reject);
    assert!(wire.append_log(0x0, 0, 0, 0, 1, b"f").is_err());
    assert!(!wire.is_connected());
  }

  /// 记录帧布局：8 元素数组头 + CLUSTER + APPENDLOG + 节点 + 三个整数 + 二进制载荷
  #[test]
  fn append_log_frame_layout() {
    let frame = encode_append_log_frame("p1", 2, -1, 100, 200, b"rec");
    let expected = b"*8\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$2\r\np1\r\n\
$1\r\n2\r\n$2\r\n-1\r\n$3\r\n100\r\n$3\r\n200\r\n$3\r\nrec\r\n";
    assert_eq!(frame, expected);
  }

  /// 初始化帧布局：7 元素数组（三方地址 -1/-1/-1），无 payload 元素
  #[test]
  fn append_log_init_frame_layout() {
    let frame = encode_append_log_init_frame("primary-1", 0, -1, -1, -1);
    let expected = b"*7\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$9\r\nprimary-1\r\n\
$1\r\n0\r\n$2\r\n-1\r\n$2\r\n-1\r\n$2\r\n-1\r\n";
    assert_eq!(frame, expected);
  }

  #[test]
  fn tcp_wire_not_connected() {
    let client = GarnetClientSession::new("127.0.0.1:0".to_string(), None, None, None);
    let wire = TcpSessionWire {
      client,
      node_id: 0x0000_DE11,
      overflow: Arc::new(EventWorkQueue::new()),
      in_flight: Arc::new(AtomicBool::new(false)),
      pump_alive: Arc::new(AtomicBool::new(true)),
      ratchets: Mutex::new(Vec::new()),
    };
    assert!(!wire.is_connected());
    let res = wire.advance_time(0, 1);
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), io::ErrorKind::NotConnected);
  }
}
