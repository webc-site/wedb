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
//! 「饱和 → 溢流队列 + 常驻泵搬运」承接同等背压语义：通道容量即缓冲
//! 上限，溢流积压由 ACK 超时剔除（AofSyncDriverStore::prune_timed_out_replicas）
//! 与副本 throttle 水位共同治理。

use std::{
  collections::VecDeque,
  io::{self, Error, ErrorKind},
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{runtime::spawn, time::sleep};
use parking_lot::Mutex;
use wconn::GarnetClientSession;

/// 发送端口同步面：AofSyncTask::consume 逐记录调用的发送通道
///
/// 对标 C# ExecuteClusterAppendLog（同步写发送缓冲）与 IsConnected/
/// Dispose；建连与流初始化（ConnectAsync + ExecuteClusterAppendLogInit
/// 等 +OK）属异步装配流程，由具体实现自带（见 [`TcpSessionWire::connect`]），
/// 不进入本端口
pub trait AofSyncWire: Send + Sync {
  /// 逐记录帧转发（payload 为完整 AOF 记录帧：8B 记录头 + 负载）
  ///
  /// 返回 Err 即发送通道断开（对标 C# Consume 异常上抛 → 后台同步任务
  /// 终止 → 驱动从仓库移除）
  fn append_log(
    &self,
    node_id: &str,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<()>;

  /// 连接健康面（对标 C# GarnetClientSession.IsConnected）
  fn is_connected(&self) -> bool;

  /// 标记断连（对标 C# GarnetClientSession.Dispose 的会话关闭面）
  fn disconnect(&self);
}

/// 帧写入回调端口（内存通道形态的投递面，供同进程测试/管道调用）
pub type FrameSinkFn = Box<dyn Fn(&[u8]) -> bool + Send + Sync>;

/// 内存通道形态：帧写入回调直连副本会话（同进程最小可测发送通道）
///
/// 回调收到编码后的完整 RESP 请求帧字节，返回 false 视为投递失败
/// （对端会话关闭），通道转入断连态；后续 append_log 返回 NotConnected
///（对标 C# 副本断链后 AofSyncTask.IsConnected == false 的 Consume 拒绝语义）
pub struct CallbackWire {
  sink: FrameSinkFn,
  connected: AtomicBool,
}

impl CallbackWire {
  /// 创建内存通道（sink 收到的是完整 RESP 请求帧字节）
  pub fn new(sink: impl Fn(&[u8]) -> bool + Send + Sync + 'static) -> Self {
    Self {
      sink: Box::new(sink),
      connected: AtomicBool::new(true),
    }
  }
}

impl AofSyncWire for CallbackWire {
  fn append_log(
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
    if !(self.sink)(&frame) {
      self.connected.store(false, Ordering::Release);
      return Err(Error::new(ErrorKind::ConnectionReset, "sink rejected"));
    }
    Ok(())
  }

  fn is_connected(&self) -> bool {
    self.connected.load(Ordering::Acquire)
  }

  fn disconnect(&self) {
    self.connected.store(false, Ordering::Release);
  }
}

/// TCP 会话形态：wconn 客户端会话 + 溢流队列常驻泵
///
/// 对标 C# GarnetClientSession 的网络面组合：Fire-and-forget 帧写入
/// 客户端命令通道（try_send），通道饱和时进入溢流队列，由常驻泵按
/// REPLICA_SYNC_DELAY 周期搬运（对标 C# BulkConsumeAllAsync 空转等待
/// 的 5ms 节拍），直至通道腾出容量或会话关闭
pub struct TcpSessionWire {
  client: GarnetClientSession,
  node_id: String,
  /// 通道饱和帧的溢流暂存（对标 C# networkSender 的待发送缓冲积压）
  overflow: Mutex<VecDeque<OverflowEntry>>,
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

/// 溢流泵空转周期（对标 C# REPLICA_SYNC_DELAY = 5ms）
const WIRE_PUMP_DELAY: Duration = Duration::from_millis(5);

impl TcpSessionWire {
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

    let wire = Arc::new(Self {
      client,
      node_id: node_id.to_string(),
      overflow: Mutex::new(VecDeque::new()),
      pump_alive: Arc::new(AtomicBool::new(true)),
    });
    wire.start_pump();
    Ok(wire)
  }

  /// 溢流搬运泵：周期尝试把饱和帧排入客户端命令通道（Weak 弱引用破环，防孤儿任务泄漏）
  fn start_pump(self: &Arc<Self>) {
    let weak_wire = Arc::downgrade(self);
    let alive = Arc::clone(&self.pump_alive);
    spawn(async move {
      while alive.load(Ordering::Acquire) {
        let Some(wire) = weak_wire.upgrade() else {
          break;
        };
        wire.drain_overflow();
        drop(wire);
        sleep(WIRE_PUMP_DELAY).await;
      }
    })
    .detach();
  }

  /// 排空溢流队列（通道满则保留剩余，下周期重试）
  fn drain_overflow(&self) {
    let mut pending = self.overflow.lock();
    while let Some(front) = pending.front() {
      let sent = self
        .client
        .execute_cluster_append_log(
          &self.node_id,
          front.physical_sublog_idx,
          front.previous_address,
          front.current_address,
          front.next_address,
          &front.payload,
        )
        .is_ok();
      if sent {
        pending.pop_front();
      } else {
        break;
      }
    }
  }

  /// 最大允许溢流队列长度，防止对端假死导致内存无界膨胀
  const MAX_OVERFLOW_ENTRIES: usize = 10_000;

  /// 单帧经客户端命令通道发出（fire-and-forget），饱和时入溢流队列
  ///
  /// 返回 Err 仅当会话已关闭或溢流超过上限；通道饱和不失败（溢流暂存由泵重试，对标
  /// C# 发送缓冲满时不失败只阻塞的差异——无阻塞契约下以暂存承接）
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
    if self
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
      let mut pending = self.overflow.lock();
      if pending.len() >= Self::MAX_OVERFLOW_ENTRIES {
        self.disconnect();
        return Err(Error::new(
          ErrorKind::BrokenPipe,
          "Replication send buffer overflow exceeded limit",
        ));
      }
      pending.push_back(OverflowEntry {
        physical_sublog_idx,
        previous_address,
        current_address,
        next_address,
        payload: Box::from(payload),
      });
    }
    Ok(())
  }
}

impl Drop for TcpSessionWire {
  fn drop(&mut self) {
    self.disconnect();
  }
}

impl AofSyncWire for TcpSessionWire {
  fn append_log(
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

  fn is_connected(&self) -> bool {
    self.pump_alive.load(Ordering::Acquire) && self.client.is_connected()
  }

  fn disconnect(&self) {
    self.pump_alive.store(false, Ordering::Release);
    self.overflow.lock().clear();
  }
}

pub use wconn::{
  encode_append_log_frame as encode_append_log_record_frame, encode_append_log_init_frame,
  encode_cluster_append_log_frame, encode_cluster_append_log_frame as encode_append_log_frame,
};

#[cfg(test)]
mod tests {
  use parking_lot::Mutex;

  use super::*;

  /// 内存通道帧收发：回调收到编码帧且断连后拒绝发送
  #[test]
  fn callback_wire_frame_delivery_and_disconnect() {
    let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let sink_recv = Arc::clone(&received);
    let wire = CallbackWire::new(move |frame: &[u8]| {
      sink_recv.lock().push(frame.to_vec());
      true
    });

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
    let wire = CallbackWire::new(|_frame| false);
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
