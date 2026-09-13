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
  io::{self, Error, ErrorKind},
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use compio::runtime::spawn;
use parking_lot::Mutex;
use wbase::pool::EventWorkQueue;
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
        // 直写消费（应答落临时 scratch 复用缓冲，记录帧热路径零堆分配）
        let mut scratch = Vec::new();
        let consumed = session.lock().try_consume_messages_into(frame, &mut scratch);
        if let Some(counter) = seen {
          *counter.lock() += 1;
        }
        consumed == frame.len() && (scratch.is_empty() || scratch == b"+OK\r\n")
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

/// TCP 会话形态：wconn 客户端会话 + 溢流事件驱动泵
///
/// 对标 C# GarnetClientSession 的网络面组合：Fire-and-forget 帧写入
/// 客户端命令通道（try_send），通道饱和时进入有锁溢流队列，由事件驱动
/// 泵被动唤醒搬运，饱和重试经由客户端通道异步挂起消灭空转轮询
pub struct TcpSessionWire {
  client: GarnetClientSession,
  node_id: String,
  overflow: Arc<EventWorkQueue<OverflowEntry>>,
  /// 在途帧标志：泵手上持有已出队、未入客户端通道的帧（直发禁入，保序）
  in_flight: Arc<AtomicBool>,
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

    let overflow = Arc::new(EventWorkQueue::new());
    let wire = Arc::new(Self {
      client,
      node_id: node_id.to_string(),
      overflow: Arc::clone(&overflow),
      in_flight: Arc::new(AtomicBool::new(false)),
      pump_alive: Arc::new(AtomicBool::new(true)),
    });
    wire.start_pump(overflow);
    Ok(wire)
  }

  /// 溢流搬运泵：事件驱动被动唤醒，尝试把饱和帧排入客户端命令通道（Weak 弱引用破环，防孤儿任务泄漏）
  fn start_pump(self: &Arc<Self>, overflow: Arc<EventWorkQueue<OverflowEntry>>) {
    let weak_wire = Arc::downgrade(self);
    let alive = Arc::clone(&self.pump_alive);
    spawn(async move {
      let mut pending_entry = None;
      while alive.load(Ordering::Acquire) {
        let entry = if let Some(p) = pending_entry.take() {
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
          match overflow.try_pop() {
            Some(e) => {
              wire.in_flight.store(true, Ordering::Release);
              e
            }
            None => {
              wire.in_flight.store(false, Ordering::Release);
              continue;
            }
          }
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

        // 本次发送成功，清除 in_flight
        wire.in_flight.store(false, Ordering::Release);

        // 贪婪非阻塞消费
        while let Some(next) = overflow.try_pop() {
          wire.in_flight.store(true, Ordering::Release);
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
            // 通道饱和，暂存到 pending_entry，等待下一轮 async 重发
            pending_entry = Some(next);
            break;
          }
          // 发送成功，清除 in_flight，继续消费
          wire.in_flight.store(false, Ordering::Release);
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
    let should_try_direct = self.overflow.is_empty() && !self.in_flight.load(Ordering::Acquire);
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
