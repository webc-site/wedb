use std::{
  fs::{self, File},
  io::{self, Write},
  path::Path,
};

use waof::AofAddress;

/// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:ReplicationHistoryVersion
pub const REPLICATION_HISTORY_VERSION: u8 = 1;

use crate::server::cluster_manager::create_hex_id;

/// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:ReplicationHistory
///
/// 记录主备复制纪元 ID 与对应截断位点历史
#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct ReplicationHistory {
  pub primary_repl_id: String,
  pub primary_repl_id2: String,
  pub replication_offset: AofAddress,
  pub replication_offset2: AofAddress,
}

impl ReplicationHistory {
  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:ReplicationHistory
  ///
  /// 初始化全新的复制历史实例
  pub fn new(aof_physical_sublog_count: usize) -> Self {
    Self {
      primary_repl_id: create_hex_id(),
      primary_repl_id2: String::new(),
      replication_offset: AofAddress::create(aof_physical_sublog_count as i32, 0),
      replication_offset2: AofAddress::create(aof_physical_sublog_count as i32, i64::MAX),
    }
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:Copy
  ///
  /// 深拷贝复制历史
  #[must_use]
  pub fn copy(&self) -> Self {
    self.clone()
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:ToByteArray
  ///
  /// 序列化为字节向量
  pub fn to_byte_array(&self) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    out.push(REPLICATION_HISTORY_VERSION);
    out.extend_from_slice(&bitcode::encode(self));
    out
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:FromByteArray
  ///
  /// 从二进制字节反序列化 ReplicationHistory
  pub fn from_byte_array(data: &[u8]) -> io::Result<Self> {
    let Some((&version, payload)) = data.split_first() else {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid ReplicationHistory payload: too short to contain a version",
      ));
    };
    if version != REPLICATION_HISTORY_VERSION {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
          "Incompatible ReplicationHistory version: expected {REPLICATION_HISTORY_VERSION}, got {version}"
        ),
      ));
    }
    bitcode::decode(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:UpdateReplicationId
  ///
  /// 更新当前主节点复制 ID
  #[must_use]
  pub fn update_replication_id(&self, primary_repl_id: &str) -> Self {
    let mut copy = self.clone();
    copy.primary_repl_id = primary_repl_id.to_string();
    copy
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:FailoverUpdate
  ///
  /// 故障转移时更新位点并轮转主节点 ID
  #[must_use]
  pub fn failover_update(&self, replication_offset2: AofAddress) -> Self {
    let mut copy = self.clone();
    copy.primary_repl_id2 = self.primary_repl_id.clone();
    copy.primary_repl_id = create_hex_id();
    copy.replication_offset2 = replication_offset2;
    copy
  }

  /// 持久化到 replication.conf 文件（原子写文件，防断电损坏）
  pub fn flush_to_file(&self, path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }
    let data = self.to_byte_array();
    let tmp_path = path.with_extension("tmp");
    {
      let mut file = File::create(&tmp_path)?;
      file.write_all(&data)?;
      file.sync_all()?;
    }
    fs::rename(&tmp_path, path)?;
    Ok(())
  }

  /// 从 replication.conf 恢复配置，若损坏或不存在则初始化新配置
  pub fn recover_or_init(path: &Path, aof_physical_sublog_count: usize) -> Self {
    if let Ok(data) = fs::read(path)
      && let Ok(history) = Self::from_byte_array(&data)
    {
      return history;
    }
    let history = Self::new(aof_physical_sublog_count);
    if let Err(err) = history.flush_to_file(path) {
      log::warn!("初始化持久化 replication.conf 失败: {err}");
    }
    history
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_replication_history_roundtrip() {
    let hist = ReplicationHistory::new(1);
    let bytes = hist.to_byte_array();
    let decoded = ReplicationHistory::from_byte_array(&bytes).unwrap();
    assert_eq!(hist, decoded);
  }

  #[test]
  fn test_failover_update() {
    let hist = ReplicationHistory::new(2);
    let orig_id = hist.primary_repl_id.clone();
    let failover_offset = AofAddress::create(2, 5000);
    let updated = hist.failover_update(failover_offset);
    assert_eq!(updated.primary_repl_id2, orig_id);
    assert_ne!(updated.primary_repl_id, orig_id);
    assert_eq!(updated.replication_offset2, failover_offset);
  }
}
