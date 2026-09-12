use wresp::cmd_strings::RESP_OK;

use crate::TxnState;

/// 事务会话交互抽象（对标 Garnet 会话中驱动事务与解析命令所需面的最小接口）
pub trait TxnSession {
  /// 会话 ID（对应 C# session.ID）
  fn session_id(&self) -> i32;

  /// 命令参数总数（不含命令名 token）
  fn arg_count(&self) -> usize;

  /// 按下标获取参数切片
  fn get_arg(&self, idx: usize) -> &[u8];

  /// 会话事务状态
  fn txn_state(&self) -> TxnState;

  /// 设置会话事务状态
  fn set_txn_state(&mut self, state: TxnState);

  /// 读缓冲区光标位置（MULTI 命令结束点，用于 EXEC 回退重放）
  fn end_read_head(&self) -> usize;

  /// 设置读缓冲区光标位置
  fn set_end_read_head(&mut self, head: usize);

  /// RESP 协议版本（2 或 3）
  fn resp_protocol_version(&self) -> u8 {
    2
  }

  /// 活跃数据库 ID（SELECT 校验）
  fn active_db_id(&self) -> i32 {
    0
  }

  /// 是否允许执行 DEBUG 命令
  fn can_run_debug(&self) -> bool {
    false
  }

  /// 写 "+OK\r\n"
  fn write_ok(&mut self);

  /// 写 "+QUEUED\r\n"
  fn write_queued(&mut self);

  /// 写空数组（RESP2 `*-1\r\n`，RESP3 `_\r\n`）
  fn write_null_array(&mut self);

  /// 写数组头 `*<count>\r\n`
  fn write_array_len(&mut self, count: usize);

  /// 写错误应答
  fn write_error(&mut self, message: &str);

  /// 写参数数量错误
  fn abort_wrong_num_args(&mut self, cmd_name: &str);

  /// 写自定义过程参数数量错误
  fn write_proc_param_error(&mut self, tx_id: u8, expected: i32, actual: usize);
}

/// 用于单元测试的 Mock 会话实现
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
  pub active_db: i32,
  /// 是否允许 DEBUG
  pub allow_debug: bool,
  /// 响应缓冲区
  pub output: Vec<u8>,
  /// 是否写入过命令错误
  pub command_error_written: bool,
}

impl MockTxnSession {
  /// 构造空 Mock 会话
  pub fn new() -> Self {
    Self::default()
  }

  /// 以参数列表快速构造 Mock 会话
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
  fn active_db_id(&self) -> i32 {
    self.active_db
  }

  #[inline]
  fn can_run_debug(&self) -> bool {
    self.allow_debug
  }

  #[inline]
  fn write_ok(&mut self) {
    self.output.extend_from_slice(RESP_OK);
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
  fn write_array_len(&mut self, count: usize) {
    use wresp::RespVecExt;
    self.output.write_resp_array_len(count);
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
    let mut b0 = itoa::Buffer::new();
    let mut b1 = itoa::Buffer::new();
    let mut b2 = itoa::Buffer::new();
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
    self.command_error_written = true;
  }
}
