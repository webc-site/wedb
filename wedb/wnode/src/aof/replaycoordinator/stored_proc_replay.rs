//! AOF 存储过程回放执行面（C# `RespServerSession.RunCustomTxnProcAtReplica`
//! 与 `CustomCommandManagerSession.GetCustomTransactionProcedure` 的回放驱动
//! 投影：恢复/复制回放驱动无 RESP 会话，静态派发重建过程实例后走
//! [`wtxn::TransactionManager`] 事务三段式重放）。

use std::{sync::Arc, time::Duration};

use waof::AofHeader;
use wcustom::CustomTransactionProcedure;
use wdev::Device;
use wtxn::{
  DEFAULT_VERSION_MAP_SIZE, SlotVerifyHandle, TransactionManager, TxnLockTable, TxnProcedure,
  WatchVersionMap,
};

use crate::{
  aof::{AofReplayError, aof_processor::ReplayTarget},
  storage::session::txn_proc_view::TxnProcView,
};

/// 提取存储过程条目载荷（waof 头层 [`AofHeader::skip_header`] 单点：
/// 按头型跳过完整头后切片）。
pub fn stored_proc_payload(entry: &[u8]) -> Result<&[u8], AofReplayError> {
  let header_size = AofHeader::skip_header(entry).ok_or("存储过程条目头损坏")?;
  entry
    .get(header_size..)
    .ok_or_else(|| "存储过程条目载荷截断".to_string().into())
}

/// 存储过程输入参数序列编解码（C# `CustomProcedureInput.DeserializeFrom`
/// 参数区；布局与编解码单点在 waof `encode_arg_sequence` /
/// `decode_arg_sequence`，与 [`crate::aof::ReplayInput`] 参数区共用）。
pub mod stored_proc_args {
  /// 序列化参数序列到缓冲尾部（泛型支持切片序列或 Vec 序列）。
  pub fn encode<T: AsRef<[u8]>>(args: &[T], into: &mut Vec<u8>) {
    let len = waof::arg_sequence_len(args);
    into.reserve(len);
    let start = into.len();
    into.resize(start + len, 0);
    let written = waof::encode_arg_sequence(args, &mut into[start..]);
    debug_assert_eq!(written, len);
  }

  /// 从载荷解码参数序列；越界 / 截断即 `None`（条目损坏）。
  pub fn decode(bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
    waof::decode_arg_sequence(bytes)
  }
}

/// 存储过程重放执行器（C# `RunCustomTxnProcAtReplica` 的无会话投影：
/// 按日志 id 静态派发重建过程并以事务三段式重放，
/// `isRecovering: true` 语义——Finalize 跳过、AOF 不再落盘）。
///
/// 过程实例内的存储访问经实例自持句柄承接（C# 以 `TGarnetApi` 参数
/// 注入会话存储面；rust 侧静态派发产出过程实例）。键集锁面取构造期注入的
/// 引擎实例锁表句柄（对标 C# 重放会话经所属 store 的 `LockTable` 取锁，
/// 与在线事务同面互斥）。
pub struct StoredProcRegistryReplayer {
  /// 所属引擎实例锁表句柄（对标 `Tsavorite.cs:105` store 自持的 LockTable）
  lock_table: TxnLockTable,
}

impl StoredProcRegistryReplayer {
  /// 构造（`lock_table` = 目标引擎实例的锁表句柄，装配点单点注入）
  pub fn new(lock_table: TxnLockTable) -> Self {
    Self { lock_table }
  }

  /// 按 id 重建过程并事务重放；过程触达的键哈希追加进 `hashes`（C#
  /// `CustomProcedureKeyHashCollection.AddHash` 收集面），供回放后推进
  /// 读一致性时间戳。
  ///
  /// `target` 为回放落点（存储会话 + 存储句柄）：过程体存储视图
  /// [`TxnProcView`] 包其存储会话构造，对标 C# 恢复/复制会话经
  /// replayContext.respServerSession 注入的存储 API。
  pub fn replay<D: Device>(
    &self,
    proc_id: u8,
    session_id: i32,
    args: &[Vec<u8>],
    hashes: &mut Vec<i64>,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    // 静态派发直取过程实例（未注册即 C# GarnetException 的回放失败路径）
    let mut inner = wcustom::txn_proc(proc_id).ok_or_else(|| {
      AofReplayError::Replay(format!(
        "未注册的 AOF 存储过程 id {proc_id}，无法重建过程实例"
      ))
    })?;

    // 回放驱动持独立事务管理器：锁面经所属引擎实例锁表句柄真实取闩
    //（与在线事务同面互斥，对标 C# 重放会话取 store.LockTable）；AOF 出口缺省
    //（is_replaying 短路落盘，与 C# 恢复会话 recordToAof: false 对齐）
    let mut txn_manager = TransactionManager::new(
      self.lock_table.clone(),
      Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE)),
      None,
    );
    txn_manager.set_session_id(session_id);

    let mut collected = Vec::new();
    {
      inner.bind_args(args);
      let mut proc = HashCollectingProc {
        inner: &mut inner,
        sink: &mut collected,
      };

      let mut view = TxnProcView::new(target.session);
      let mut output = Vec::new();
      let ok = txn_manager.run_transaction_proc(&mut proc, &[], &mut output, true, None, &mut view);
      if !ok {
        return Err(AofReplayError::Replay(format!(
          "AOF 存储过程 {proc_id} 事务重放失败（prepare/lock/abort 路径）"
        )));
      }
    }

    hashes.extend(collected);
    Ok(())
  }
}

/// 哈希收集包装过程：prepare 段（C# AddKey 时机）捕获过程登记的键哈希。
/// 事务 commit 的 reset 会清空锁集，收集必须在锁存期内完成——与 C#
/// `ComputeCustomProcShardedLogAccess` 回放分支（AddHash）同点执行。
struct HashCollectingProc<'a, P: ?Sized> {
  inner: &'a mut P,
  sink: &'a mut Vec<i64>,
}

impl<P: TxnProcedure + ?Sized> TxnProcedure for HashCollectingProc<'_, P> {
  fn id(&self) -> u8 {
    self.inner.id()
  }

  fn fail_fast_on_key_lock_failure(&self) -> bool {
    self.inner.fail_fast_on_key_lock_failure()
  }

  fn key_lock_timeout(&self) -> Duration {
    self.inner.key_lock_timeout()
  }

  fn prepare(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn wtxn::TxnProcReadApi,
    verifier: Option<&SlotVerifyHandle<'_>>,
  ) -> bool {
    let ok = self.inner.prepare(txn_manager, api, verifier);
    if ok {
      self.sink.extend(txn_manager.key_entries.key_hashes());
    }
    ok
  }

  fn main(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn wtxn::TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    self.inner.main(txn_manager, api, output);
  }

  fn finalize(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn wtxn::TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    self.inner.finalize(txn_manager, api, output);
  }
}

impl<P: CustomTransactionProcedure + ?Sized> CustomTransactionProcedure
  for HashCollectingProc<'_, P>
{
  fn bind_args(&mut self, args: &[Vec<u8>]) {
    self.inner.bind_args(args);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn args_roundtrip() {
    let args = vec![b"k".to_vec(), b"v".to_vec(), vec![]];
    let mut encoded = Vec::new();
    stored_proc_args::encode(&args, &mut encoded);
    assert_eq!(stored_proc_args::decode(&encoded).unwrap(), args);
    assert!(stored_proc_args::decode(&encoded[..encoded.len() - 1]).is_none());
    assert!(stored_proc_args::decode(&[]).is_none());
  }

  // replay 执行面（DEFAULT 过程直通 / 未注册 id 拒绝）需回放落点存储会话，
  // 随集成测试 tests/aof_stored_proc_replay.rs 的真存储基建覆盖。
}
