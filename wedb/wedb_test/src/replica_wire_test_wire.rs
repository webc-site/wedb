//! 内存回调发送通道（测试支撑，不进产线二进制）
//!
//! 帧写入回调直投接收端（同进程最小可测发送通道），承接单元测试的
//! 发送端口契约覆盖：帧字节整帧断言、拒收断连、溢流 Queued 水位语义；
//! 集成测试一律走真 socket（`TcpSessionWire` + GarnetServer）。
//!
//! 本文件原以 `#[path]` 供各集成测试二进制直挂（`pub mod test_wire;`），
//! 现已收口进本 crate：消费面经 `wedb_test::replica_wire_test_wire` 引用
//!（lib 侧仅存 [`CallbackWireFace`] 接口槽，见 wedb replica_wire.rs 文档）。
//! 勿在产线路径新增对 [`CallbackWireFace`] 的实现体。

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
use wedb::server::replication::replica_wire::{AofSyncWire, CallbackWireFace, ShippedState};

/// 帧写入接收端具体枚举（消除动态分发闭包）
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

/// 接口槽承接（固有方法为测试直调面，此处纯转接，语义见各固有方法）
impl CallbackWireFace for CallbackWire {
  fn append_log(
    &self,
    node_id: u128,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> io::Result<ShippedState> {
    Self::append_log(
      self,
      node_id,
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
      payload,
    )
  }

  fn advance_time(
    &self,
    physical_sublog_idx: usize,
    sequence_number: i64,
  ) -> io::Result<ShippedState> {
    Self::advance_time(self, physical_sublog_idx, sequence_number)
  }

  fn is_connected(&self) -> bool {
    Self::is_connected(self)
  }

  fn disconnect(&self) {
    Self::disconnect(self)
  }
}

/// 内存回调通道直构 [`AofSyncWire`]（测试二进制内 `From<Arc<CallbackWire>>`
/// 不可用——两侧皆外类型触发孤儿规则 E0117；lib 侧 cfg(test) 壳另有 From
/// 供 src 内联测试经 `attach_wire(impl Into<AofSyncWire>)` 直挂）
pub fn callback_wire(sink: impl Into<FrameSink>) -> AofSyncWire {
  AofSyncWire::Callback(Arc::new(CallbackWire::new(sink)))
}
