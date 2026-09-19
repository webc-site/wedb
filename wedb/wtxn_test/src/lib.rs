//! wtxn 测试共用夹具（`wtxn_test`）
//!
//! 跨 crate 单元测试共用的事务会话 Mock，仅以 dev-dependencies 形式消费，
//! 不进入任何生产链接面。

use itoa::Buffer;
use wtxn::{TxnSession, TxnState};

/// 用于单元测试的 Mock 事务会话
#[derive(Debug, Default)]
pub struct MockTxnSession {
  /// 会话 ID
  pub id: i32,
  /// 参数切片列表
  pub args: Vec<Vec<u8>>,
  /// 事务状态
  pub txn_state: TxnState,
  /// 读光标
  pub end_read_head: usize,
  /// 协议版本
  pub protocol_version: u8,
  /// 活跃数据库 ID
  pub active_db: u64,
  /// 是否允许 DEBUG
  pub allow_debug: bool,
  /// 响应缓冲区
  pub output: Vec<u8>,
  /// 集群槽位校验是否强制失败（TRYAGAIN 重试路径，C# 缓存裁决失败对位）
  pub cluster_slot_verify_fail: bool,
}

impl MockTxnSession {
  /// 构造空 Mock 会话
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// 以参数列表快速构造 Mock 会话
  #[must_use]
  pub fn with_args(args: &[&[u8]]) -> Self {
    Self {
      args: args.iter().map(|s| s.to_vec()).collect(),
      ..Default::default()
    }
  }
}

impl TxnSession for MockTxnSession {
  #[inline]
  fn session_id(&self) -> i32 {
    self.id
  }

  #[inline]
  fn arg_count(&self) -> usize {
    self.args.len()
  }

  #[inline]
  fn get_arg(&self, idx: usize) -> &[u8] {
    &self.args[idx]
  }

  #[inline]
  fn txn_state(&self) -> TxnState {
    self.txn_state
  }

  #[inline]
  fn set_txn_state(&mut self, state: TxnState) {
    self.txn_state = state;
  }

  #[inline]
  fn end_read_head(&self) -> usize {
    self.end_read_head
  }

  #[inline]
  fn set_end_read_head(&mut self, head: usize) {
    self.end_read_head = head;
  }

  #[inline]
  fn resp_protocol_version(&self) -> u8 {
    self.protocol_version
  }

  #[inline]
  fn active_db_id(&self) -> u64 {
    self.active_db
  }

  #[inline]
  fn can_run_debug(&self) -> bool {
    self.allow_debug
  }

  #[inline]
  fn write_ok(&mut self) {
    // libs/server/Resp/CmdStrings.cs:RESP_OK
    self.output.extend_from_slice(b"+OK\r\n");
  }

  #[inline]
  fn write_queued(&mut self) {
    self.output.extend_from_slice(b"+QUEUED\r\n");
  }

  #[inline]
  fn write_null_array(&mut self) {
    if self.protocol_version >= 3 {
      self.output.extend_from_slice(b"_\r\n");
    } else {
      self.output.extend_from_slice(b"*-1\r\n");
    }
  }

  #[inline]
  fn write_array_length(&mut self, count: usize) {
    // `*<count>\r\n`（wresp::resp_memory_writer::RespWriter::write_array_length 的本域直写形态）
    let mut buf = Buffer::new();
    self.output.push(b'*');
    self.output.extend_from_slice(buf.format(count).as_bytes());
    self.output.extend_from_slice(b"\r\n");
  }

  #[inline]
  fn write_error(&mut self, message: &str) {
    self.output.extend_from_slice(b"-");
    self.output.extend_from_slice(message.as_bytes());
    self.output.extend_from_slice(b"\r\n");
  }

  #[inline]
  fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    self
      .output
      .extend_from_slice(b"-ERR wrong number of arguments for '");
    self.output.extend_from_slice(cmd_name.as_bytes());
    self.output.extend_from_slice(b"' command\r\n");
  }

  #[inline]
  fn write_proc_param_error(&mut self, tx_id: u8, expected: i32, actual: usize) {
    let mut b0 = Buffer::new();
    let mut b1 = Buffer::new();
    let mut b2 = Buffer::new();
    let s0 = b0.format(tx_id);
    let s1 = b1.format(expected);
    let s2 = b2.format(actual);
    self
      .output
      .extend_from_slice(b"-ERR Invalid number of parameters to stored proc ");
    self.output.extend_from_slice(s0.as_bytes());
    self.output.extend_from_slice(b", expected ");
    self.output.extend_from_slice(s1.as_bytes());
    self.output.extend_from_slice(b", actual ");
    self.output.extend_from_slice(s2.as_bytes());
    self.output.extend_from_slice(b"\r\n");
  }

  #[inline]
  fn verify_cluster_txn_keys(&mut self, _keys: &[&[u8]]) -> bool {
    if self.cluster_slot_verify_fail {
      self
        .output
        .extend_from_slice(b"-TRYAGAIN Multiple keys request during rehashing of slot\r\n");
      return false;
    }
    true
  }
}
