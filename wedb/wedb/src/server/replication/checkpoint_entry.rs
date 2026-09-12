use std::{
  fmt::{self, Display, Formatter},
  io::{Cursor, Read},
  sync::atomic::{AtomicI32, Ordering},
};

use waof::AofAddress;

/// libs/cluster/Server/Replication/CheckpointFileType.cs:CheckpointFileType
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointFileType {
  StoreHlog = 0,
  StoreIndex = 1,
}

/// 检查点元数据，记录快照版本号、Token 与覆盖的 AOF 地址
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointMetadata {
  pub store_version: i64,
  pub store_hlog_token: u128,
  pub store_index_token: u128,
  pub store_checkpoint_covered_aof_address: AofAddress,
  pub store_primary_repl_id: Option<String>,
}

impl CheckpointMetadata {
  pub fn new(physical_sublog_count: usize) -> Self {
    Self {
      store_version: -1,
      store_hlog_token: 0,
      store_index_token: 0,
      store_checkpoint_covered_aof_address: AofAddress::create(physical_sublog_count as i32, 0),
      store_primary_repl_id: None,
    }
  }
}

/// libs/server/Cluster/CheckpointMetadata.cs:ToString
impl Display for CheckpointMetadata {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "storeVersion={},storeHlogToken={:x},storeIndexToken={:x},storeCheckpointCoveredAofAddress={},storePrimaryReplId={}",
      self.store_version,
      self.store_hlog_token,
      self.store_index_token,
      self.store_checkpoint_covered_aof_address.to_aof_string(),
      self.store_primary_repl_id.as_deref().unwrap_or("(empty)")
    )
  }
}

/// libs/cluster/Server/Replication/CheckpointEntry.cs:ToString
impl Display for CheckpointEntry {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{},readers={}", self.metadata, self.reader_count())
  }
}

/// libs/cluster/Server/Replication/CheckpointEntry.cs:CheckpointEntry
///
/// 检查点链表节点，维护元数据、读者计数与多读者挂起防护
#[derive(Debug)]
pub struct CheckpointEntry {
  pub metadata: CheckpointMetadata,
  /// 读者计数：>= 0 表示活跃读者数量；i32::MIN 表示读者已挂起，禁止新增读者
  readers: AtomicI32,
}

impl Clone for CheckpointEntry {
  fn clone(&self) -> Self {
    Self {
      metadata: self.metadata.clone(),
      readers: AtomicI32::new(self.readers.load(Ordering::Acquire)),
    }
  }
}

impl CheckpointEntry {
  /// libs/cluster/Server/Replication/CheckpointEntry.cs:CheckpointEntry
  pub fn new(metadata: CheckpointMetadata) -> Self {
    Self {
      metadata,
      readers: AtomicI32::new(0),
    }
  }

  /// 创建带有指定子日志数的空检查点条目
  pub fn with_sublogs(physical_sublog_count: usize) -> Self {
    Self::new(CheckpointMetadata::new(physical_sublog_count))
  }

  /// libs/cluster/Server/Replication/CheckpointEntry.cs:GetMinAofCoveredAddress
  ///
  /// 获取检查点覆盖的最小有效 AOF 地址
  pub fn get_min_aof_covered_address(&self, first_valid_aof_address: i64) -> AofAddress {
    let mut min_covered = self.metadata.store_checkpoint_covered_aof_address;
    min_covered.max_exchange(first_valid_aof_address);
    min_covered
  }

  /// libs/cluster/Server/Replication/CheckpointEntry.cs:TryAddReader
  ///
  /// 增加活跃读者计数；若已挂起（< 0）则拒绝并返回 false
  pub fn try_add_reader(&self) -> bool {
    let mut cur = self.readers.load(Ordering::Acquire);
    loop {
      if cur < 0 {
        return false;
      }
      match self
        .readers
        .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
      {
        Ok(_) => return true,
        Err(actual) => cur = actual,
      }
    }
  }

  /// libs/cluster/Server/Replication/CheckpointEntry.cs:RemoveReader
  ///
  /// 移除活跃读者计数
  pub fn remove_reader(&self) {
    let mut cur = self.readers.load(Ordering::Acquire);
    loop {
      if cur <= 0 {
        // 无读者或已挂起
        break;
      }
      match self
        .readers
        .compare_exchange_weak(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire)
      {
        Ok(_) => break,
        Err(actual) => cur = actual,
      }
    }
  }

  /// libs/cluster/Server/Replication/CheckpointEntry.cs:TrySuspendReaders
  ///
  /// 挂起读者：将活跃读者数置为 i32::MIN，仅当当前活跃读者数为 0 时成功
  pub fn try_suspend_readers(&self) -> bool {
    let cur = self.readers.load(Ordering::Acquire);
    if cur == i32::MIN {
      return true;
    }
    self
      .readers
      .compare_exchange(0, i32::MIN, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
  }

  /// 获取当前活跃读者数（若挂起则返回 0）
  pub fn reader_count(&self) -> i32 {
    let cur = self.readers.load(Ordering::Acquire);
    if cur < 0 { 0 } else { cur }
  }

  /// 是否已被挂起
  pub fn is_suspended(&self) -> bool {
    self.readers.load(Ordering::Acquire) < 0
  }

  /// libs/cluster/Server/Replication/CheckpointEntry.cs:ContainsSharedToken
  ///
  /// 比较指定类型文件 Token 是否被共享
  pub fn contains_shared_token(
    &self,
    entry: &CheckpointEntry,
    file_type: CheckpointFileType,
  ) -> bool {
    match file_type {
      CheckpointFileType::StoreHlog => {
        self.metadata.store_hlog_token == entry.metadata.store_hlog_token
      }
      CheckpointFileType::StoreIndex => {
        self.metadata.store_index_token == entry.metadata.store_index_token
      }
    }
  }

  /// libs/cluster/Server/Replication/CheckpointEntry.cs:ToByteArray
  ///
  /// 序列化为字节向量
  pub fn to_byte_array(&self) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    out.extend_from_slice(&self.metadata.store_version.to_le_bytes());
    out.extend_from_slice(&self.metadata.store_hlog_token.to_le_bytes());
    out.extend_from_slice(&self.metadata.store_index_token.to_le_bytes());
    out.extend_from_slice(
      &self
        .metadata
        .store_checkpoint_covered_aof_address
        .serialize(),
    );
    if let Some(ref repl_id) = self.metadata.store_primary_repl_id {
      out.extend_from_slice(&1i32.to_le_bytes());
      let bytes = repl_id.as_bytes();
      let mut len = bytes.len();
      while len >= 0x80 {
        out.push(len as u8 | 0x80);
        len >>= 7;
      }
      out.push(len as u8);
      out.extend_from_slice(bytes);
    } else {
      out.extend_from_slice(&0i32.to_le_bytes());
    }
    out
  }

  /// libs/cluster/Server/Replication/CheckpointEntry.cs:FromByteArray
  ///
  /// 反序列化 CheckpointEntry
  pub fn from_byte_array(serialized: &[u8]) -> Option<Self> {
    if serialized.len() < 40 {
      return None;
    }
    let mut cursor = Cursor::new(serialized);
    let mut buf8 = [0u8; 8];
    cursor.read_exact(&mut buf8).ok()?;
    let store_version = i64::from_le_bytes(buf8);

    let mut buf16 = [0u8; 16];
    cursor.read_exact(&mut buf16).ok()?;
    let store_hlog_token = u128::from_le_bytes(buf16);

    cursor.read_exact(&mut buf16).ok()?;
    let store_index_token = u128::from_le_bytes(buf16);

    let pos = cursor.position() as usize;
    let rem = &serialized[pos..];
    let store_checkpoint_covered_aof_address = AofAddress::deserialize(rem);
    let span_len = store_checkpoint_covered_aof_address.span_len();
    if rem.len() < span_len {
      return None;
    }
    let rem2 = &rem[span_len..];
    if rem2.len() < 4 {
      return None;
    }
    let has_repl_id = i32::from_le_bytes(rem2[..4].try_into().unwrap()) > 0;
    let store_primary_repl_id = if has_repl_id {
      let mut str_cursor = Cursor::new(&rem2[4..]);
      let mut len = 0usize;
      let mut shift = 0;
      loop {
        let mut b = [0u8; 1];
        str_cursor.read_exact(&mut b).ok()?;
        len |= ((b[0] & 0x7f) as usize) << shift;
        if (b[0] & 0x80) == 0 {
          break;
        }
        shift += 7;
        if shift > 35 {
          return None;
        }
      }
      let mut str_bytes = vec![0u8; len];
      str_cursor.read_exact(&mut str_bytes).ok()?;
      String::from_utf8(str_bytes).ok()
    } else {
      None
    };

    Some(Self::new(CheckpointMetadata {
      store_version,
      store_hlog_token,
      store_index_token,
      store_checkpoint_covered_aof_address,
      store_primary_repl_id,
    }))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_readers_suspend_flow() {
    let entry = CheckpointEntry::with_sublogs(1);
    assert!(entry.try_add_reader());
    assert_eq!(entry.reader_count(), 1);
    assert!(!entry.try_suspend_readers());

    entry.remove_reader();
    assert_eq!(entry.reader_count(), 0);
    assert!(entry.try_suspend_readers());
    assert!(entry.is_suspended());

    // 挂起后禁止新增读者
    assert!(!entry.try_add_reader());
  }

  #[test]
  fn test_checkpoint_entry_serialization() {
    let mut meta = CheckpointMetadata::new(2);
    meta.store_version = 42;
    meta.store_hlog_token = 0x1234_5678_9abc_def0_1122_3344_5566_7788;
    meta.store_index_token = 0x8877_6655_4433_2211_0fed_cba9_8765_4321;
    meta.store_primary_repl_id = Some("abc123replid".to_string());
    meta.store_checkpoint_covered_aof_address = AofAddress::create(2, 1024);

    let entry = CheckpointEntry::new(meta);
    let bytes = entry.to_byte_array();
    let decoded = CheckpointEntry::from_byte_array(&bytes).unwrap();
    assert_eq!(entry.metadata, decoded.metadata);
  }
}
