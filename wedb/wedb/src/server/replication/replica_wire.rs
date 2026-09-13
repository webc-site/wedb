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
//! MAX_OVERFLOW_ENTRIES，溢流积压由 ACK 超时剔除
//!（AofSyncDriverStore::prune_timed_out_replicas）与副本 throttle 水位共同治理。

use std::{
  collections::VecDeque,
  io::{self, Error, ErrorKind},
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use compio::runtime::spawn;
use event_listener::Event;
use parking_lot::Mutex;
use wconn::GarnetClientSession;
use wdev::SegmentedDevice;
use wnode::MessageConsumerFace;

use crate::server::replication::cluster_replication_session::ClusterReplicationSession;

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
}

impl FrameSink {
  pub fn call(&self, frame: &[u8]) -> bool {
    match self {
      Self::Buffer(buf) => {
        buf.lock().push(frame.to_vec());
        true
      }
      Self::Reject => false,
      Self::Fn(f) => f(frame),
      Self::Session { session, seen } => {
        let (consumed, resp) = session.lock().try_consume_messages(frame);
        if let Some(counter) = seen {
          *counter.lock() += 1;
        }
        consumed == frame.len() && (resp.is_empty() || resp == b"+OK\r\n")
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
    node_id: &str,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<()> {
    if !self.connected.load(Ordering::Acquire) {
      return Err(Error::new(ErrorKind::NotConnected, "memory wire closed"));
    }
    let frame = encode_append_log_record_frame(
      node_id,
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
      payload,
    );
    if !self.sink.call(&frame) {
      self.connected.store(false, Ordering::Release);
      return Err(Error::new(ErrorKind::ConnectionReset, "sink rejected"));
    }
    Ok(())
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

/// 溢流暂存通道（有锁连续缓冲队列 + 事件驱动挂起与唤醒）
///
/// 单生产者单消费者、常态为空（仅客户端命令通道饱和时启用）的低竞争场景，
/// 遵循 crossfire 官方实践指导采用有锁队列：无锁环形队列按容量预分配槽位
///（10_000 × OverflowEntry ≈ 600KB/连接），有锁队列零预分配按需增长，
/// 且免锁套娃与跨核缓存颠簸。
struct OverflowChannel {
  queue: Mutex<VecDeque<OverflowEntry>>,
  event: Event,
  closed: AtomicBool,
  /// 在途帧标志：泵手上持有已出队、未入客户端通道的帧（直发禁入，保序）
  in_flight: AtomicBool,
}

impl OverflowChannel {
  fn new() -> Self {
    Self {
      queue: Mutex::new(VecDeque::new()),
      event: Event::new(),
      closed: AtomicBool::new(false),
      in_flight: AtomicBool::new(false),
    }
  }

  /// 推入溢流帧；超过上限返回 false（调用方断连，防对端假死内存无界膨胀）
  fn push(&self, entry: OverflowEntry) -> bool {
    {
      let mut q = self.queue.lock();
      if q.len() >= TcpSessionWire::MAX_OVERFLOW_ENTRIES {
        return false;
      }
      q.push_back(entry);
    }
    self.event.notify(1);
    true
  }

  /// 非阻塞取出一帧（贪婪消费路径）；出队即置在途标志（保序）
  fn try_pop(&self) -> Option<OverflowEntry> {
    let entry = self.queue.lock().pop_front()?;
    self.in_flight.store(true, Ordering::Release);
    Some(entry)
  }

  /// 队列无积压且无在途帧时新帧方可直发（保序：在途帧已离开队列但未入
  /// 客户端通道，直发会插队致复制流乱序）
  fn is_empty(&self) -> bool {
    let q = self.queue.lock();
    q.is_empty() && !self.in_flight.load(Ordering::Acquire)
  }

  /// 帧退回队首（贪婪消费遇饱和时保序）；本泵随即在 [`Self::recv`] 锁内重取，无需唤醒
  fn push_front(&self, entry: OverflowEntry) {
    self.queue.lock().push_front(entry);
  }

  /// 异步挂起直至取出一帧；通道关闭且已排空时返回 None
  ///
  /// 空查与注册监听同持锁，与持锁的 [`Self::close`] 串行化，无丢唤醒窗口
  async fn recv(&self) -> Option<OverflowEntry> {
    loop {
      let listener = {
        let mut q = self.queue.lock();
        if let Some(entry) = q.pop_front() {
          // 出队即置在途：帧在泵手上、未入客户端通道期间直发会插队
          self.in_flight.store(true, Ordering::Release);
          return Some(entry);
        }
        // 上一迭代在途帧已全部入客户端通道，清除在途标志
        self.in_flight.store(false, Ordering::Release);
        if self.closed.load(Ordering::Acquire) {
          return None;
        }
        self.event.listen()
      };
      listener.await;
    }
  }

  /// 关闭通道（wire 释放时），唤醒所有等待中的泵
  ///
  /// 持锁 store + notify：与 [`Self::recv`] 的空查→注册监听互斥，
  /// 消除「notify 先于注册即丢失」的挂死窗口
  fn close(&self) {
    let q = self.queue.lock();
    self.closed.store(true, Ordering::Release);
    self.event.notify(usize::MAX);
    drop(q);
  }
}

/// TCP 会话形态：wconn 客户端会话 + 溢流事件驱动泵
///
/// 对标 C# GarnetClientSession 的网络面组合：Fire-and-forget 帧写入
/// 客户端命令通道（try_send），通道饱和时进入有锁溢流队列，由事件驱动
/// 泵被动唤醒搬运，饱和重试经由客户端通道异步挂起消灭空转轮询
pub struct TcpSessionWire {
  client: GarnetClientSession,
  node_id: String,
  /// 通道饱和帧的溢流暂存（有锁队列，零预分配按需增长）
  overflow: Arc<OverflowChannel>,
  pump_alive: Arc<AtomicBool>,
}

/// 溢流帧字段（重发时逐字段编码，与首发同一路径；Box<[u8]> 节约 8B 容量字段）
struct OverflowEntry {
  physical_sublog_idx: usize,
  previous_address: i64,
  current_address: i64,
  next_address: i64,
  payload: Box<[u8]>,
}

impl TcpSessionWire {
  /// 最大允许溢流队列长度，防止对端假死导致内存无界膨胀
  pub const MAX_OVERFLOW_ENTRIES: usize = 10_000;

  /// 建立副本连接并发送 AOF 复制流初始化帧（等 +OK）
  ///
  /// 对标 C# AofSyncTask.RunAofSyncTaskAsync 的建连序列：ConnectAsync →
  /// ExecuteClusterAppendLogInit(-1,-1,-1)（成功返回后调用方才挂推流端口
  /// 与补扫泵，保证初始化帧先于任何记录帧）
  pub async fn connect(
    endpoint: &str,
    node_id: &str,
    physical_sublog_idx: usize,
    auth_username: Option<&str>,
    auth_password: Option<&str>,
  ) -> io::Result<Arc<Self>> {
    let mut client = GarnetClientSession::new(
      endpoint.to_string(),
      auth_username.map(str::to_string),
      auth_password.map(str::to_string),
      Some(format!("AofSyncTask-{physical_sublog_idx}:({node_id})")),
    );
    client
      .connect_async()
      .await
      .map_err(|e| Error::new(ErrorKind::ConnectionRefused, e.to_string()))?;
    let resp = client
      .execute_cluster_append_log_init(node_id, physical_sublog_idx, -1, -1, -1)
      .await
      .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e.to_string()))?;
    if resp != "OK" {
      return Err(Error::new(
        ErrorKind::ConnectionAborted,
        "Failed to initialize AofSync stream!",
      ));
    }

    let overflow = Arc::new(OverflowChannel::new());
    let wire = Arc::new(Self {
      client,
      node_id: node_id.to_string(),
      overflow: Arc::clone(&overflow),
      pump_alive: Arc::new(AtomicBool::new(true)),
    });
    wire.start_pump(overflow);
    Ok(wire)
  }

  /// 溢流搬运泵：事件驱动被动唤醒，尝试把饱和帧排入客户端命令通道（Weak 弱引用破环，防孤儿任务泄漏）
  fn start_pump(self: &Arc<Self>, overflow: Arc<OverflowChannel>) {
    let weak_wire = Arc::downgrade(self);
    let alive = Arc::clone(&self.pump_alive);
    spawn(async move {
      while alive.load(Ordering::Acquire) {
        // 无挂起条目时被动挂起等待唤醒，消灭固定周期的空转轮询
        let Some(entry) = overflow.recv().await else {
          break; // 通道关闭（wire 释放）且已排空
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

        // 异步推入客户端命令通道：饱和时被动挂起等待网络泵腾出缓冲，
        // 由 crossfire 内建 Waker 精确唤醒，零轮询零 sleep（对标 C# 发送缓冲满
        // 时 Send 内部自动 Flush 的阻塞背压）
        if wire
          .client
          .execute_cluster_append_log_async(
            &wire.node_id,
            entry.physical_sublog_idx,
            entry.previous_address,
            entry.current_address,
            entry.next_address,
            &entry.payload,
          )
          .await
          .is_err()
        {
          break;
        }

        // 发送成功后尽力贪婪非阻塞消费当前已就绪的溢流条目，摊薄调度开销
        while let Some(next) = overflow.try_pop() {
          if wire
            .client
            .execute_cluster_append_log(
              &wire.node_id,
              next.physical_sublog_idx,
              next.previous_address,
              next.current_address,
              next.next_address,
              &next.payload,
            )
            .is_err()
          {
            // 通道再次饱和：帧退回队首保序，主路径经 recv().await 重取后
            // 走异步 await 挂起等待腾出缓冲
            overflow.push_front(next);
            break;
          }
        }
      }
    })
    .detach();
  }

  /// 单帧经客户端命令通道发出（fire-and-forget），饱和时入溢流队列
  ///
  /// 返回 Err 仅当会话已关闭或溢流超过上限；通道饱和不失败（溢流暂存由事件驱动泵重试）
  fn send_frame(
    &self,
    node_id: &str,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<()> {
    if !self.is_connected() {
      return Err(Error::new(ErrorKind::NotConnected, "wire session closed"));
    }
    let target_node_id = if !node_id.is_empty() {
      node_id
    } else {
      &self.node_id
    };

    // 若溢流队列为空则直发客户端通道；若已有积压或直发满则入队保序
    let should_try_direct = self.overflow.is_empty();
    if !should_try_direct
      || self
        .client
        .execute_cluster_append_log(
          target_node_id,
          physical_sublog_idx,
          previous_address,
          current_address,
          next_address,
          payload,
        )
        .is_err()
    {
      let entry = OverflowEntry {
        physical_sublog_idx,
        previous_address,
        current_address,
        next_address,
        payload: Box::from(payload),
      };
      if !self.overflow.push(entry) {
        self.disconnect();
        return Err(Error::new(
          ErrorKind::BrokenPipe,
          "Replication send buffer overflow exceeded limit",
        ));
      }
    }
    Ok(())
  }

  /// 逐记录帧转发（payload 为完整 AOF 记录帧：8B 记录头 + 负载）
  pub fn append_log(
    &self,
    node_id: &str,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<()> {
    self.send_frame(
      node_id,
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
      payload,
    )
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
  pub fn append_log(
    &self,
    node_id: &str,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<()> {
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

#[cfg(not(test))]
use wconn::encode_append_log_frame as encode_append_log_record_frame;
#[cfg(test)]
use wconn::{
  encode_append_log_frame as encode_append_log_record_frame, encode_append_log_init_frame,
  encode_cluster_append_log_frame as encode_append_log_frame,
};

#[cfg(test)]
mod tests {
  use parking_lot::Mutex;

  use super::*;

  /// 内存通道帧收发：回调收到编码帧且断连后拒绝发送
  #[test]
  fn callback_wire_frame_delivery_and_disconnect() {
    let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let wire = CallbackWire::new(received.clone());

    assert!(
      wire
        .append_log("node-1", 0, 64, 64, 128, b"\x00\xffpayload")
        .is_ok()
    );
    let frames = received.lock();
    assert_eq!(frames.len(), 1);
    // 收到的帧可被二进制数组解析器还原为 8 元素
    let (_consumed, items) = wresp::parse_resp_frame(&frames[0]).expect("complete");
    assert_eq!(items.len(), 8);
    assert_eq!(items[2], b"node-1");
    assert_eq!(items[7], b"\x00\xffpayload");

    drop(frames);
    wire.disconnect();
    assert!(!wire.is_connected());
    assert!(wire.append_log("node-1", 0, 64, 64, 128, b"x").is_err());
  }

  /// 回调拒绝投递 → 通道转断连态（对标副本会话关闭语义）
  #[test]
  fn callback_wire_sink_reject_disconnects() {
    let wire = CallbackWire::new(FrameSink::Reject);
    assert!(wire.append_log("n", 0, 0, 0, 1, b"f").is_err());
    assert!(!wire.is_connected());
  }

  #[test]
  fn append_log_frame_layout() {
    let frame = encode_append_log_frame("p1", 2, -1, 100, 200, Some(b"rec"));
    // 数组头 + CLUSTER + APPENDLOG + 节点 + 三个整数 + 二进制载荷
    let expected = b"*8\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$2\r\np1\r\n\
$1\r\n2\r\n$2\r\n-1\r\n$3\r\n100\r\n$3\r\n200\r\n$3\r\nrec\r\n";
    assert_eq!(frame, expected);

    // 7 元素初始化帧（payload == None）
    let init = encode_append_log_frame("p1", 0, -1, -1, -1, None);
    let expected_init =
      b"*7\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$2\r\np1\r\n$1\r\n0\r\n$2\r\n-1\r\n$2\r\n-1\r\n$2\r\n-1\r\n";
    assert_eq!(init, expected_init);
  }

  #[test]
  fn append_log_init_frame_layout() {
    let frame = encode_append_log_init_frame("primary-1", 0, -1, -1, -1);
    let expected = b"*7\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$9\r\nprimary-1\r\n\
$1\r\n0\r\n$2\r\n-1\r\n$2\r\n-1\r\n$2\r\n-1\r\n";
    assert_eq!(frame, expected);
  }
}
