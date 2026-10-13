use wval::SessionPrefixBuf;

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

  /// 会话**物理**归属前缀（`[NsVarint][DbVarint]`，锁轨种子与一切物理寻址
  /// 的键身份维度）
  ///
  /// 双轨声明（分置两单点、禁共口互染）：**版本轨=逻辑域**——WATCH 登记
  /// 分槽取 [`Self::watch_prefix`]，与 wkv 写面推进 `bump_watch_version` 的
  /// 逻辑投影同源于此，换号代际不入种子；**锁轨=物理域**——排队命令锁登记
  /// 与 EXEC WATCH 键并锁取本方法（wkv `StoreSession::session_prefix` 真值
  /// 源，含 FLUSHDB 换号态），与桶闩所在现域同源。
  /// 缺省根域 (ns 0, db 0)——与 wkv 未绑定上下文会话的实前缀恒等
  /// （`SessionPrefixBuf::ROOT`），非任意兜底值。
  fn session_prefix(&self) -> SessionPrefixBuf {
    SessionPrefixBuf::ROOT
  }

  /// 会话**逻辑**归属前缀（版本轨种子单点：WATCH 登记分槽专用）
  ///
  /// 真值源单点：会话所属 wkv `StoreSession::session_logical_prefix`
  /// （`namespace()`/`active_db()` 逻辑真值投影，不含换号虚拟代际），与写面
  /// 推进 `bump_watch_version` 同源于此——同逻辑库读写在 FLUSHDB/FLUSHNS/
  /// SWAPDB 换号前后必落同槽，改后写必 abort（对位 C# 每库版本表实例终身
  /// 持有）。锁轨不取本前缀（见 [`Self::session_prefix`] 双轨声明）。
  /// 缺省根域——无存储执行域形态的会话其逻辑域即 (0,0) 恒等。
  fn watch_prefix(&self) -> SessionPrefixBuf {
    SessionPrefixBuf::ROOT
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
  ///
  /// `read_only` 为事务本体真实只读态（`TransactionManager::is_read_only`，对标
  /// C# TransactionManager.cs:GetSlotVerificationInput 的 `keyEntries.IsReadOnly`）：
  /// 本回调发生于 `with_txn_manager` 借出窗口内，会话 `txn_manager` 槽恒为 None，
  /// 只读态必须由调用方（被借出的 TransactionManager 本体）单点求值后经形参传入，
  /// 杜绝实现面回读会话导致的恒 false 误判。
  fn verify_cluster_txn_keys(&mut self, _keys: &[&[u8]], _read_only: bool) -> bool {
    true
  }

  /// 重置集群槽位校验缓存（libs/server/Transaction/TxnKeyManager.cs:ResetCacheSlotVerificationResult）
  fn reset_cluster_slot_verification_result(&mut self) {}

  /// 外部 EXEC 取锁争用的 compio 慢臂登记（对标网络层 `LockAllKeys` 争用挂起）。
  ///
  /// 事务侧 [`crate::TransactionManager::run_exec`] 单次尝试取闩失败（返回
  /// `Contended`）时调用：宿主会话登记既有唯一慢臂——`pending_slow` 置一个仅
  /// 做一次 `yield_now` 的让步体、`pending_rearm` 置真，令消费循环回退游标至本
  /// EXEC 命令起点，让步体 resolve 后重驱本命令复入 `run_exec` 再单次尝试，直至
  /// 取锁成功。以此替换 compio 单核 worker 上会饿死同 worker 挂起持闩命令的
  /// `thread::yield_now` 同步自旋，同时保留 C# `TransactionalContext.Lock` 的
  /// 无界等待契约（不设失败上限）。缺省空实现供无网络泵的非 compio 测试会话使用。
  fn park_exec_lock_wait(&mut self) {}
}
