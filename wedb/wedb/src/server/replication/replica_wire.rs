//! 主端 → 副本发送通道（网络会话抽象）
//!
//! 对标 C# AofSyncTask 的 garnetClient 字段（libs/client/ClientSession/
//! GarnetClientSession.cs）——AofSyncTask 构造时按副本端点建立
//! GarnetClientSession，Consume 内逐记录调 ExecuteClusterAppendLog 写入
//! 发送缓冲，Throttle 时 CompletePending 冲刷。Rust 依赖方向反转：
//! 会话端口以 [`AofSyncWire`] 注入，生产唯一形态为 TCP（包装 wconn 客户端
//! 会话），与 C# 单一具体通道同形；帧写入回调的内存通道仅存于
//! `` 测试支撑（[`test_wire`]），不进产线二进制。
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
//! 写入通道后经 ratchet_after_ship 回推任务水位（Weak 引用破环），维护
//! 背压闸门已发水位（主端 AOF 安全截断线取 previous_address，对标 C# SafeTruncateAof :88/:143）。

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
use wconn::{session::GarnetClientSession, types::MAX_UNFLUSHED_SEND_BYTES};
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

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
  pub client: GarnetClientSession,
  pub node_id: u128,
  /// 预先渲染好的 32 字符小写 hex 节点 id（对标 C# AofSyncTask.cs readonly localNodeId）
  pub node_id_hex: Box<str>,
  pub overflow: Arc<EventWorkQueue<WireFrame>>,
  /// 溢流队列累计驻留字节（入队先增后 push、泵 pop 减，与 overflow 同推/弹点位）：
  /// 与 [`TcpSessionWire::MAX_OVERFLOW_ENTRIES`] 条数封顶并列的第二维闸，
  /// 慢副本 + 大记录场景把每连接未刷出内存钉在字节预算内（对标 C# 4 页环形缓冲）
  pub overflow_bytes: AtomicUsize,
  /// 溢流驻留字节封顶（产线 connect 恒取 [`MAX_UNFLUSHED_SEND_BYTES`]，
  /// 唯一数值源；单测可构造小值以廉价输入触达字节判据，与写泵
  /// flush_threshold_bytes 分片阈值同形承接 C# 「页数 4 × 发送页尺寸」字节顶）
  pub byte_cap: usize,
  /// 在途占位计数：泵已从溢流队列取走、尚未写入客户端通道的帧数（直发
  /// 禁入，保序）。取帧与占位合为一次原子先手——泵先 fetch_add 占位再
  /// try_pop，取空回退，落通道释放；生产者直发判据「队列空且占位为零」
  /// 在帧离队后必见非零占位，直发越队窗口闭合（旧 AtomicBool store 后置
  /// 的取帧/置位两步窗口已删，计数语义取而代之）
  pub in_flight: Arc<AtomicUsize>,
  pub pump_alive: Arc<AtomicBool>,
  /// 溢流帧落通道后的水位回推端（按物理子日志下标索引；Weak 破
  /// task → wire → task 引用环，对标 C# 同步发送无需回推的时序差异面）
  pub ratchets: Mutex<Vec<Weak<AofSyncTask>>>,
}

/// 溢流帧载荷（重发时逐字段编码，与首发同一路径）
pub enum WireFrame {
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

/// 溢流帧字段（重发时逐字段编码，与首发同一路径；Box<[u8]> 节约 8B 容量字段）
pub struct OverflowEntry {
  pub physical_sublog_idx: usize,
  pub previous_address: i64,
  pub current_address: i64,
  pub next_address: i64,
  pub payload: Box<[u8]>,
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
      in_flight: Arc::new(AtomicUsize::new(0)),
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
          // 原子先手：先占在途计数再取帧，取空回退——生产者直发判据
          //（队列空且占位为零）在帧离队后必见非零占位，直发越队窗口闭合
          wire.in_flight.fetch_add(1, Ordering::AcqRel);
          let Some(frame) = overflow.try_pop() else {
            wire.in_flight.fetch_sub(1, Ordering::AcqRel);
            continue;
          };
          // 帧离队即减字节计量（与入队先增后 push 对偶，pending_frame 续传不重复减）
          wire
            .overflow_bytes
            .fetch_sub(frame.resident_bytes(), Ordering::AcqRel);
          frame
        };

        if !alive.load(Ordering::Acquire) {
          break;
        }

        let Some(wire) = weak_wire.upgrade() else {
          break;
        };

        if !wire.is_connected() {
          // 断连终态：在途帧随断连丢弃，释放占位保持记账一致
          wire.in_flight.fetch_sub(1, Ordering::AcqRel);
          break;
        }

        // 异步推入客户端命令通道
        if ship_frame(&wire.client, &wire.node_id_hex, &frame)
          .await
          .is_err()
        {
          wire.disconnect();
          // 断连终态：在途帧随断连丢弃，释放占位保持记账一致
          wire.in_flight.fetch_sub(1, Ordering::AcqRel);
          break;
        }

        // 帧已落通道：回推任务已发送水位，释放在途占位
        ratchet_after_ship(&wire.ratchets.lock(), &frame);
        wire.in_flight.fetch_sub(1, Ordering::AcqRel);

        // 贪婪非阻塞消费（同款原子先手：占位先于取帧）
        loop {
          wire.in_flight.fetch_add(1, Ordering::AcqRel);
          let Some(next) = overflow.try_pop() else {
            wire.in_flight.fetch_sub(1, Ordering::AcqRel);
            break;
          };
          // 帧离队即减字节计量（同上）
          wire
            .overflow_bytes
            .fetch_sub(next.resident_bytes(), Ordering::AcqRel);
          if try_ship_frame(&wire.client, &wire.node_id_hex, &next).is_err() {
            // 通道饱和，暂存到 pending_frame（占位随帧保持），等待下一轮 async 重发
            pending_frame = Some(next);
            break;
          }
          // 帧已落通道：回推水位，释放在途占位，继续消费
          ratchet_after_ship(&wire.ratchets.lock(), &next);
          wire.in_flight.fetch_sub(1, Ordering::AcqRel);
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
    // 直发双检（顺序即屏障）：先查队列空再查在途占位——泵取帧先占位后
    // 出队（fetch_add 先于 try_pop），帧离队后占位必非零；本判据读到
    // 「队列空且占位为零」时不可能存在已离队未落网帧，直发不越队
    let should_try_direct = self.overflow.is_empty() && self.in_flight.load(Ordering::Acquire) == 0;
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
/// 通道仅单元测试可见（``，不进产线二进制）
#[derive(Clone)]
pub enum AofSyncWire {
  /// TCP 会话通道（生产唯一形态）
  Tcp(Arc<TcpSessionWire>),
  /// 内存回调通道（仅单元测试）
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
    match self {
      Self::Tcp(w) => w.set_ratchets(tasks),

      Self::Callback(_) => {}
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

/// 测试专用内存通道（`` 收口，不进产线二进制）
///
/// 帧写入回调直投接收端（同进程最小可测发送通道），承接单元测试的
/// 发送端口契约覆盖：帧字节整帧断言、拒收断连、溢流 Queued 水位语义；
/// 集成测试一律走真 socket（`TcpSessionWire` + GarnetServer）
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
