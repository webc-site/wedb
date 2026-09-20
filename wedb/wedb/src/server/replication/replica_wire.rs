//! 主端 → 副本发送通道（网络会话抽象）
//!
//! 对标 C# AofSyncTask 的 garnetClient 字段（libs/client/ClientSession/
//! GarnetClientSession.cs）——AofSyncTask 构造时按副本端点建立
//! GarnetClientSession，Consume 内逐记录调 ExecuteClusterAppendLog 写入
//! 发送缓冲，Throttle 时 CompletePending 冲刷。Rust 依赖方向反转：
//! 会话端口以 [`AofSyncWire`] 注入，生产唯一形态为 TCP（包装 wconn 客户端
//! 会话），与 C# 单一具体通道同形；帧写入回调的内存通道仅存于
//! `#[cfg(test)]` 测试支撑（[`test_wire`]），不进产线二进制。
//!
//! C# 发送缓冲满时 Send 内部自动 Flush（同步阻塞）；Rust 的推流端口
//! （WalLog::replication_sink）契约要求回调无阻塞，故 TCP 形态以
//! 「饱和 → 溢流队列 + 常驻泵搬运」承接同等背压语义：溢流按条数
//! （MAX_OVERFLOW_ENTRIES）+ 驻留字节（byte_cap，产线取 wconn
//! MAX_UNFLUSHED_SEND_BYTES，对标 C# NetworkWriter 4 页环形缓冲字节顶）
//! 双维硬封顶，任一触顶即断连（通道健康面感知，
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
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::Duration,
};

use compio::{runtime::spawn, time};
use parking_lot::Mutex;
use wbase::{
  hex::hex_str_u128,
  pool::{EventWorkQueue, LimitedFixedBufferPool},
};
#[cfg(feature = "tls")]
use wconn::tls::ClientTlsConfig;
use wconn::{session::GarnetClientSession, types::MAX_UNFLUSHED_SEND_BYTES};

use crate::server::replication::aof_sync_task::AofSyncTask;

/// 帧发送落点状态（对标 C# ExecuteClusterAppendLog 同步写网络发送缓冲
/// 成功返回的语义切分：rust 客户端通道饱和时帧滞留溢流队列，两者必须区分）
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShippedState {
  /// 帧已写入客户端命令通道（对标 C# 写入网络发送缓冲，计入已发送水位）
  Shipped,
  /// 帧滞留溢流队列待发（未落通道，严禁计入已发送水位）
  Queued,
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
  /// 预先渲染好的 32 字符小写 hex 节点 id（对标 C# AofSyncTask.cs readonly localNodeId）
  node_id_hex: Box<str>,
  overflow: Arc<EventWorkQueue<WireFrame>>,
  /// 溢流队列累计驻留字节（入队先增后 push、泵 pop 减，与 overflow 同推/弹点位）：
  /// 与 [`TcpSessionWire::MAX_OVERFLOW_ENTRIES`] 条数封顶并列的第二维闸，
  /// 慢副本 + 大记录场景把每连接未刷出内存钉在字节预算内（对标 C# 4 页环形缓冲）
  overflow_bytes: AtomicUsize,
  /// 溢流驻留字节封顶（产线 connect 恒取 [`MAX_UNFLUSHED_SEND_BYTES`]，
  /// 唯一数值源；单测可构造小值以廉价输入触达字节判据，与写泵
  /// flush_threshold_bytes 分片阈值同形承接 C# 「页数 4 × 发送页尺寸」字节顶）
  byte_cap: usize,
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

impl WireFrame {
  /// 溢流驻留字节计量：APPENDLOG 帧主导内存开销即整帧 AOF 记录载荷，
  /// ADVANCE_TIME 为恒定小帧（计 1 字节，纯脉冲洪流仍由条数封顶承接）。
  /// RESP 数组头等固定开销与帧数成正比、已被条数封顶约束，不重复计入字节闸
  fn resident_bytes(&self) -> usize {
    match self {
      Self::AppendLog(entry) => entry.payload.len(),
      Self::AdvanceTime { .. } => 1,
    }
  }
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
  node_id_hex: &str,
  frame: &WireFrame,
) -> wconn::Result<()> {
  match frame {
    WireFrame::AppendLog(e) => {
      client
        .execute_cluster_append_log_async(
          // 协议帧参数：直接复用预先渲染好的 32 字符小写 hex 节点 id
          node_id_hex,
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
  node_id_hex: &str,
  frame: &WireFrame,
) -> wconn::Result<()> {
  match frame {
    WireFrame::AppendLog(e) => client.execute_cluster_append_log(
      // 协议帧参数：直接复用预先渲染好的 32 字符小写 hex 节点 id
      node_id_hex,
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
  /// 溢流队列条数封顶（与字节封顶 [`TcpSessionWire::byte_cap`] 并列，
  /// 二者任一触顶即断连），防止对端假死导致内存无界膨胀
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
  ///
  /// `network_pool` 传复制域 manager 持有池（对标 C# AofSyncTask.cs:134
  /// 构造 GarnetClientSession 的 `replicationManager.GetNetworkPool` 形参，
  /// 同池跨连接复用）
  pub async fn connect(
    endpoint: &str,
    node_id: u128,
    physical_sublog_idx: usize,
    auth_username: Option<&str>,
    auth_password: Option<&str>,
    network_pool: Arc<LimitedFixedBufferPool>,
    #[cfg(feature = "tls")] tls: Option<&Arc<ClientTlsConfig>>,
  ) -> io::Result<Arc<Self>> {
    let node_id_hex = hex_str_u128(node_id).into_boxed_str();
    let mut client = GarnetClientSession::new(
      endpoint.to_string(),
      auth_username.map(str::to_string),
      auth_password.map(str::to_string),
      Some(format!("AofSyncTask-{physical_sublog_idx}:({node_id_hex})")),
      Some(network_pool),
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
        &node_id_hex,
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
      node_id_hex,
      overflow: Arc::clone(&overflow),
      overflow_bytes: AtomicUsize::new(0),
      byte_cap: MAX_UNFLUSHED_SEND_BYTES,
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
          // 帧离队即减字节计量（与入队先增后 push 对偶，pending_frame 续传不重复减）
          wire
            .overflow_bytes
            .fetch_sub(frame.resident_bytes(), Ordering::AcqRel);
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
        if ship_frame(&wire.client, &wire.node_id_hex, &frame)
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
          // 帧离队即减字节计量（同上）
          wire
            .overflow_bytes
            .fetch_sub(next.resident_bytes(), Ordering::AcqRel);
          wire.in_flight.store(true, Ordering::Release);
          if try_ship_frame(&wire.client, &wire.node_id_hex, &next).is_err() {
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
  /// 或通道饱和，则延迟构造帧入溢流队列保序；溢流按条数（[`Self::MAX_OVERFLOW_ENTRIES`]）
  /// 与驻留字节（[`Self::byte_cap`]）双判据封顶，任一触顶即断连并报 BrokenPipe
  /// （对标 C# NetworkWriter 页满 TryAllocate 返回 RETRY_LATER 的字节硬顶）
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

    // 先建帧计量，条数/字节双判据任一触顶即断连（错误文案区分两判据便于定位慢副本）
    let frame = make_frame();
    let resident = frame.resident_bytes();
    let over_entries = self.overflow.len() >= Self::MAX_OVERFLOW_ENTRIES;
    let over_bytes = self.overflow_bytes.load(Ordering::Acquire) >= self.byte_cap;
    if over_entries || over_bytes {
      self.disconnect();
      let limit = if over_entries { "entry" } else { "byte" };
      return Err(Error::new(
        ErrorKind::BrokenPipe,
        format!("Replication send buffer overflow exceeded {limit} limit"),
      ));
    }
    // 先增计量再入队：泵出队减账必不见负（usize 下溢会令字节闸永久触顶）
    self.overflow_bytes.fetch_add(resident, Ordering::AcqRel);
    if !self.overflow.push(frame) {
      self.overflow_bytes.fetch_sub(resident, Ordering::AcqRel);
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
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<ShippedState> {
    self.send_or_enqueue(
      |client| {
        client.execute_cluster_append_log(
          // 协议帧参数：直接复用建连期已缓存的 32 字符小写 hex 节点 id
          &self.node_id_hex,
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
    _node_id: u128,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<ShippedState> {
    self.send_frame(
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

  /// 节点 id（纯数值）
  #[inline]
  pub fn node_id(&self) -> u128 {
    self.node_id
  }

  /// 预先渲染好的 32 字符小写 hex 节点 id（对标 C# AofSyncTask.cs readonly localNodeId）
  #[inline]
  pub fn node_id_hex(&self) -> &str {
    &self.node_id_hex
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
///
/// 生产唯一形态为 TCP（与 C# 单一 GarnetClientSession 同形）；内存回调
/// 通道仅单元测试可见（`#[cfg(test)]`，不进产线二进制）
#[derive(Clone)]
pub enum AofSyncWire {
  /// TCP 会话通道（生产唯一形态）
  Tcp(Arc<TcpSessionWire>),
  /// 内存回调通道（仅单元测试）
  #[cfg(test)]
  Callback(Arc<test_wire::CallbackWire>),
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
      #[cfg(test)]
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
      #[cfg(test)]
      Self::Callback(w) => w
        .advance_time(physical_sublog_idx, sequence_number)
        .map(|_| ()),
    }
  }

  /// 注入溢流落通道后的水位回推端（仅 TCP 形态有溢流面，内存通道直发免注）
  pub fn set_ratchets(&self, tasks: Vec<Weak<AofSyncTask>>) {
    match self {
      Self::Tcp(w) => w.set_ratchets(tasks),
      #[cfg(test)]
      Self::Callback(_) => {}
    }
  }

  pub fn is_connected(&self) -> bool {
    match self {
      Self::Tcp(w) => w.is_connected(),
      #[cfg(test)]
      Self::Callback(w) => w.is_connected(),
    }
  }

  pub fn disconnect(&self) {
    match self {
      Self::Tcp(w) => w.disconnect(),
      #[cfg(test)]
      Self::Callback(w) => w.disconnect(),
    }
  }
}

impl From<Arc<TcpSessionWire>> for AofSyncWire {
  fn from(w: Arc<TcpSessionWire>) -> Self {
    Self::Tcp(w)
  }
}

/// 测试专用内存通道（`#[cfg(test)]` 收口，不进产线二进制）
///
/// 帧写入回调直投接收端（同进程最小可测发送通道），承接单元测试的
/// 发送端口契约覆盖：帧字节整帧断言、拒收断连、溢流 Queued 水位语义；
/// 集成测试一律走真 socket（`TcpSessionWire` + GarnetServer）
#[cfg(test)]
pub mod test_wire {
  use std::{
    io::{self, Error, ErrorKind},
    sync::{
      Arc,
      atomic::{AtomicBool, Ordering},
    },
  };

  use parking_lot::Mutex;
  use wbase::hex::hex_str_u128;
  use wconn::session::{encode_advance_time_frame, encode_append_log_frame};

  use super::{AofSyncWire, ShippedState};

  /// 帧写入接收端具体枚举（消除动态分发闭包）
  #[derive(Clone)]
  pub enum FrameSink {
    /// 收集帧到列表
    Buffer(Arc<Mutex<Vec<Vec<u8>>>>),
    /// 恒拒绝投递（测试断连语义）
    Reject,
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
      }
    }
  }

  impl From<Arc<Mutex<Vec<Vec<u8>>>>> for FrameSink {
    fn from(buf: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
      Self::Buffer(buf)
    }
  }

  /// 内存通道形态：帧写入回调直投接收端（同进程最小可测发送通道）
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

    /// 带内 CLUSTER ADVANCE_TIME 时间脉冲帧直发（无饱和面恒即时）
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

  impl From<Arc<CallbackWire>> for AofSyncWire {
    fn from(w: Arc<CallbackWire>) -> Self {
      Self::Callback(w)
    }
  }
}

#[cfg(test)]
mod tests {
  use compio::{net::TcpListener, runtime::Runtime};
  use wconn::session::{encode_append_log_frame, encode_append_log_init_frame};
  use wresp::frame::parse_resp_frame;

  use super::{
    test_wire::{CallbackWire, FrameSink},
    *,
  };

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
    let client = GarnetClientSession::new("127.0.0.1:0".to_string(), None, None, None, None);
    let node_id = 0x0000_DE11;
    let wire = TcpSessionWire {
      client,
      node_id,
      node_id_hex: hex_str_u128(node_id).into_boxed_str(),
      overflow: Arc::new(EventWorkQueue::new()),
      overflow_bytes: AtomicUsize::new(0),
      byte_cap: MAX_UNFLUSHED_SEND_BYTES,
      in_flight: Arc::new(AtomicBool::new(false)),
      pump_alive: Arc::new(AtomicBool::new(true)),
      ratchets: Mutex::new(Vec::new()),
    };
    assert!(!wire.is_connected());
    let res = wire.advance_time(0, 1);
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), io::ErrorKind::NotConnected);
  }

  /// 构造会话通道在位的 TcpSessionWire：连到静默 loopback 端点（无凭证
  /// 握手零往返即成），常驻泵不启动、在途帧置位封死直发臂——溢流只积不排，
  /// 以确定性形态触达条数/字节双封顶
  async fn wire_pumpless_connected(byte_cap: usize) -> TcpSessionWire {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    // 收下连接即静默（不读不写），套接字随任务滞留至测试收场
    spawn(async move {
      let mut held = Vec::new();
      while let Ok((sock, _)) = listener.accept().await {
        held.push(sock);
      }
    })
    .detach();
    let mut client = GarnetClientSession::new(addr, None, None, None, None);
    client.connect_async().await.unwrap();
    let node_id = 0x0000_DE11;
    TcpSessionWire {
      client,
      node_id,
      node_id_hex: hex_str_u128(node_id).into_boxed_str(),
      overflow: Arc::new(EventWorkQueue::new()),
      overflow_bytes: AtomicUsize::new(0),
      byte_cap,
      in_flight: Arc::new(AtomicBool::new(true)),
      pump_alive: Arc::new(AtomicBool::new(true)),
      ratchets: Mutex::new(Vec::new()),
    }
  }

  /// 字节维封顶：驻留字节触顶先于条数触顶断连（对标 C# NetworkWriter
  /// 4 页环形缓冲字节硬顶），错误文案区分字节判据，超界帧不入溢流队列
  #[test]
  fn tcp_wire_overflow_byte_cap_disconnects() {
    Runtime::new().unwrap().block_on(async {
      let wire = wire_pumpless_connected(4096).await;
      let payload = vec![7u8; 2048];
      for _ in 0..2 {
        assert_eq!(
          wire.append_log(0, 0, 0, 0, 2048, &payload).unwrap(),
          ShippedState::Queued
        );
      }
      assert_eq!(wire.overflow_bytes.load(Ordering::Acquire), 4096);
      let err = wire.append_log(0, 0, 0, 0, 4096, &payload).unwrap_err();
      assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
      assert!(
        err.to_string().contains("byte"),
        "字节判据文案应可区分: {err}"
      );
      assert!(!wire.is_connected(), "字节触顶必须转断连");
      assert_eq!(wire.overflow.len(), 2, "超界帧不得入溢流队列");
    });
  }

  /// 条数维封顶：字节远未触顶时条数触顶断连，错误文案区分条数判据
  #[test]
  fn tcp_wire_overflow_entry_cap_disconnects() {
    Runtime::new().unwrap().block_on(async {
      let wire = wire_pumpless_connected(usize::MAX).await;
      let payload = vec![7u8; 16];
      for _ in 0..TcpSessionWire::MAX_OVERFLOW_ENTRIES {
        assert_eq!(
          wire.append_log(0, 0, 0, 0, 1, &payload).unwrap(),
          ShippedState::Queued
        );
      }
      let err = wire.append_log(0, 0, 0, 0, 2, &payload).unwrap_err();
      assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
      assert!(
        err.to_string().contains("entry"),
        "条数判据文案应可区分: {err}"
      );
      assert!(!wire.is_connected(), "条数触顶必须转断连");
    });
  }

  /// 对标 C# AofSyncTask readonly localNodeId：
  /// 节点 id 在建连/构造时一次性渲染为 32 字符小写 hex 并缓存，
  /// 连续 N 次 append_log 均复用同一缓存切片，无重复堆分配与渲染开销。
  #[test]
  fn tcp_wire_node_id_hex_cached_and_reused() {
    Runtime::new().unwrap().block_on(async {
      let wire = wire_pumpless_connected(usize::MAX).await;
      let cached_hex = wire.node_id_hex();
      assert_eq!(cached_hex, "0000000000000000000000000000de11");
      let cached_ptr = cached_hex.as_ptr();

      let payload = vec![7u8; 16];
      for i in 0..10 {
        assert_eq!(
          wire.append_log(0, 0, 0, 0, i + 1, &payload).unwrap(),
          ShippedState::Queued
        );
        // 断言缓存字段指针在多次 append_log 之间稳定不变
        assert_eq!(wire.node_id_hex().as_ptr(), cached_ptr);
        assert_eq!(wire.node_id_hex(), "0000000000000000000000000000de11");
      }
    });
  }
}
