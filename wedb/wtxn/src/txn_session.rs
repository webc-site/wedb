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
  fn active_db_id(&self) -> u64 {
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

  /// 写数组头 `*<count>\r\n`（对齐 RespWriter::write_array_length 单点命名）
  fn write_array_length(&mut self, count: usize);

  /// 写错误应答
  fn write_error(&mut self, message: &str);

  /// 写参数数量错误
  fn abort_wrong_num_args(&mut self, cmd_name: &str);

  /// 写自定义过程参数数量错误
  fn write_proc_param_error(&mut self, tx_id: u8, expected: i32, actual: usize);

  /// 集群槽位校验（若返回 false 表示非 Ok，且已向输出写入了对应错误如 MOVED/ASK/TRYAGAIN）
  fn verify_cluster_txn_keys(&mut self, _keys: &[&[u8]]) -> bool {
    true
  }

  /// 重置集群槽位校验缓存（libs/server/Transaction/TxnKeyManager.cs:ResetCacheSlotVerificationResult）
  fn reset_cluster_slot_verification_result(&mut self) {}

  /// 迭代门评挂起重驱：集群切面在事务 Prepare 段逐键校验遇迁移推进
  /// Pending 时已登记等待体（C# CanOperateOnKey `Thread.Yield` 自旋等待
  /// 的 compio 挂起投影）；实现面取转等待体并置重驱标志返回 true，
  /// 调用方不落错误应答、消费循环回退游标，等待迁移推进/超时后重驱
  /// 本命令重评；无等待体返回 false 按普通失败落线
  fn park_iterative_slot_wait(&mut self) -> bool {
    false
  }
}
