//! AOF 存储过程回放执行面（C# `RespServerSession.RunCustomTxnProcAtReplica`
//! 与 `CustomCommandManagerSession.GetCustomTransactionProcedure` 的回放驱动
//! 投影：恢复/复制回放驱动无 RESP 会话，经注册表工厂重建过程实例后走
//! [`wtxn::TransactionManager`] 事务三段式重放）。

use std::{
  sync::Arc,
  time::Duration,
};

use parking_lot::Mutex as ParkingMutex;
use waof::AofHeader;
use wcustom::{CustomCommandManager, CustomTransactionProcedure, TxnProcFactory};
use wdatabase::DEFAULT_VERSION_MAP_SIZE;
use wtxn::{TransactionManager, TxnProcedure, WatchVersionMap};

use crate::aof::AofReplayError;

/// 提取存储过程条目载荷（C# `AofHeader.SkipHeader` 的判别形态：按头型取
/// 完整头尺寸后切片）。
pub fn stored_proc_payload(entry: &[u8]) -> Result<&[u8], AofReplayError> {
  let header = AofHeader::parse(entry).ok_or("存储过程条目头损坏")?;
  let header_size = header
    .header_type()
    .map_or(AofHeader::TOTAL_SIZE, |t| t.total_size());
  entry
    .get(header_size..)
    .ok_or_else(|| "存储过程条目载荷截断".to_string().into())
}

/// 存储过程输入参数序列编解码（C# `CustomProcedureInput.DeserializeFrom`
/// 参数区的 [`crate::aof::ReplayInput`] 同款格式：`[count u32][逐参
/// (len u32 + bytes)]`）。
pub mod stored_proc_args {
  /// 序列化参数序列到缓冲尾部。
  pub fn encode(args: &[Vec<u8>], into: &mut Vec<u8>) {
    into.reserve(4 + args.iter().map(|a| 4 + a.len()).sum::<usize>());
    into.extend_from_slice(&(args.len() as u32).to_le_bytes());
    for arg in args {
      into.extend_from_slice(&(arg.len() as u32).to_le_bytes());
      into.extend_from_slice(arg);
    }
  }

  /// 从载荷解码参数序列；越界 / 截断即 `None`（条目损坏）。
  pub fn decode(mut bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
    let count = u32::from_le_bytes(*bytes.first_chunk::<4>()?) as usize;
    bytes = &bytes[4..];
    let mut args = Vec::with_capacity(count.min(bytes.len() / 4));
    for _ in 0..count {
      let len = u32::from_le_bytes(*bytes.first_chunk::<4>()?) as usize;
      bytes = bytes.get(4..)?;
      args.push(bytes.get(..len)?.to_vec());
      bytes = &bytes[len..];
    }
    Some(args)
  }
}

/// AOF 存储过程重放执行面（C# `RunCustomTxnProcAtReplica` 的无会话投影：
/// 按 id 重建过程并以事务三段式重放，`isRecovering: true` 语义——Finalize
/// 跳过、AOF 不再落盘）。
///
/// 过程实例内的存储访问经工厂闭包自持句柄承接（C# 以 `TGarnetApi` 参数
/// 注入会话存储面；rust 侧注册工厂于装配时捕获目标存储句柄）。
pub trait StoredProcReplayer: Send + Sync {
  /// 按 id 重建过程并事务重放；过程触达的键哈希追加进 `hashes`（C#
  /// `CustomProcedureKeyHashCollection.AddHash` 收集面），供回放后推进
  /// 读一致性时间戳。
  fn replay(
    &self,
    proc_id: u8,
    session_id: i32,
    args: &[Vec<u8>],
    hashes: &mut Vec<i64>,
  ) -> Result<(), AofReplayError>;
}

/// 注册表驱动的存储过程重放执行器。
pub struct StoredProcRegistryReplayer {
  /// 自定义命令管理器（C# customCommandManagerSession 的注册表面）。
  registry: Arc<ParkingMutex<CustomCommandManager>>,
}

impl StoredProcRegistryReplayer {
  /// 以注册表构造执行器。
  pub fn new(registry: Arc<ParkingMutex<CustomCommandManager>>) -> Self {
    Self { registry }
  }

  /// 查注册的过程工厂（C# GetCustomTransactionProcedure；未注册即 C#
  /// GarnetException 的回放失败路径）。
  fn factory_of(&self, proc_id: u8) -> Result<TxnProcFactory, AofReplayError> {
    self
      .registry
      .lock()
      .try_get_custom_transaction_procedure(proc_id)
      .and_then(|txn| txn.factory)
      .ok_or_else(|| {
        AofReplayError::Replay(format!("未注册的 AOF 存储过程 id {proc_id}，无法重建过程实例"))
      })
  }
}

/// 哈希收集包装过程：prepare 段（C# AddKey 时机）捕获过程登记的键哈希。
/// 事务 commit 的 reset 会清空锁集，收集必须在锁存期内完成——与 C#
/// `ComputeCustomProcShardedLogAccess` 回放分支（AddHash）同点执行。
struct HashCollectingProc<'a> {
  inner: Box<dyn CustomTransactionProcedure>,
  sink: &'a mut Vec<i64>,
}

impl TxnProcedure for HashCollectingProc<'_> {
  fn id(&self) -> u8 {
    self.inner.id()
  }

  fn fail_fast_on_key_lock_failure(&self) -> bool {
    self.inner.fail_fast_on_key_lock_failure()
  }

  fn key_lock_timeout(&self) -> Duration {
    self.inner.key_lock_timeout()
  }

  fn prepare(&mut self, txn_manager: &mut TransactionManager) -> bool {
    let ok = self.inner.prepare(txn_manager);
    if ok {
      self.sink.extend(txn_manager.key_entries.key_hashes());
    }
    ok
  }

  fn main(&mut self, txn_manager: &mut TransactionManager, output: &mut Vec<u8>) {
    self.inner.main(txn_manager, output);
  }

  fn finalize(&mut self, txn_manager: &mut TransactionManager, output: &mut Vec<u8>) {
    self.inner.finalize(txn_manager, output);
  }
}

impl CustomTransactionProcedure for HashCollectingProc<'_> {
  fn bind_args(&mut self, args: &[Vec<u8>]) {
    self.inner.bind_args(args);
  }
}

impl StoredProcReplayer for StoredProcRegistryReplayer {
  fn replay(
    &self,
    proc_id: u8,
    session_id: i32,
    args: &[Vec<u8>],
    hashes: &mut Vec<i64>,
  ) -> Result<(), AofReplayError> {
    let factory = self.factory_of(proc_id)?;

    // 回放驱动持独立事务管理器：锁面经全局条带表真实加锁；AOF 出口缺省
    //（is_replaying 短路落盘，与 C# 恢复会话 recordToAof: false 对齐）
    let mut txn_manager = TransactionManager::new(
      Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE)),
      None,
    );
    txn_manager.set_session_id(session_id);

    let mut collected = Vec::new();
    {
      let mut proc = HashCollectingProc {
        inner: factory(),
        sink: &mut collected,
      };
      // 过程输入绑定（C# procInput 引用参数）
      proc.inner.bind_args(args);

      let mut output = Vec::new();
      let ok = txn_manager.run_transaction_proc(&mut proc, &[], &mut output, true);
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

#[cfg(test)]
mod tests {
  use std::collections::HashMap as StdHashMap;

  use super::*;
  use parking_lot::Mutex;
  use wcustom::{CustomCommandManager, CustomTransactionProcedure};
  use wtxn::TxnProcedure;

  /// 演示过程：SET 语义（arg 序列两两为 key/value），数据面经闭包自持
  ///（C# proc.Main(api) 的等价承载：工厂装配时捕获目标存储句柄）
  struct SetProc {
    id: u8,
    data: Arc<Mutex<StdHashMap<Vec<u8>, Vec<u8>>>>,
    args: Vec<Vec<u8>>,
  }

  impl TxnProcedure for SetProc {
    fn id(&self) -> u8 {
      self.id
    }

    fn prepare(&mut self, txn_manager: &mut TransactionManager) -> bool {
      // 键登记（C# AddKey）
      for key in self.args.iter().step_by(2) {
        txn_manager.save_key_entry_to_lock(key, wtxn::LockType::Exclusive);
      }
      !self.args.is_empty()
    }

    fn main(&mut self, _txn_manager: &mut TransactionManager, _output: &mut Vec<u8>) {
      let mut data = self.data.lock();
      for pair in self.args.chunks(2) {
        data.insert(pair[0].clone(), pair[1].clone());
      }
    }

    fn finalize(&mut self, _txn_manager: &mut TransactionManager, _output: &mut Vec<u8>) {}
  }

  impl CustomTransactionProcedure for SetProc {
    fn bind_args(&mut self, args: &[Vec<u8>]) {
      self.args = args.to_vec();
    }
  }

  fn replayer_with_set_proc(
    data: Arc<Mutex<StdHashMap<Vec<u8>, Vec<u8>>>>,
  ) -> StoredProcRegistryReplayer {
    let mut manager = CustomCommandManager::new();
    let id = manager
      .register_transaction(
        "SETX",
        Some(Arc::new(move || {
          Box::new(SetProc {
            id: 0,
            data: Arc::clone(&data),
            args: Vec::new(),
          })
        })),
        None,
        None,
      )
      .unwrap();
    assert_eq!(id, 0);
    StoredProcRegistryReplayer::new(Arc::new(ParkingMutex::new(manager)))
  }

  #[test]
  fn args_roundtrip() {
    let args = vec![b"k".to_vec(), b"v".to_vec(), vec![]];
    let mut encoded = Vec::new();
    stored_proc_args::encode(&args, &mut encoded);
    assert_eq!(stored_proc_args::decode(&encoded).unwrap(), args);
    assert!(stored_proc_args::decode(&encoded[..encoded.len() - 1]).is_none());
    assert!(stored_proc_args::decode(&[]).is_none());
  }

  #[test]
  fn replay_executes_proc_and_collects_key_hashes() {
    let data = Arc::new(Mutex::new(StdHashMap::new()));
    let replayer = replayer_with_set_proc(Arc::clone(&data));

    let mut hashes = Vec::new();
    replayer
      .replay(0, 7, &[b"k1".to_vec(), b"v1".to_vec()], &mut hashes)
      .unwrap();

    assert_eq!(data.lock().get(b"k1".as_slice()).unwrap(), b"v1");
    assert_eq!(hashes.len(), 1);
  }

  #[test]
  fn unregistered_proc_fails() {
    let replayer = replayer_with_set_proc(Default::default());
    let mut hashes = Vec::new();
    let err = replayer.replay(9, 0, &[], &mut hashes).unwrap_err();
    assert!(err.to_string().contains("未注册"));
  }
}
