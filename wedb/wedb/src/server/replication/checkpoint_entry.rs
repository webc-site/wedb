use std::{
  fmt::{self, Display, Formatter},
  sync::atomic::{AtomicI32, Ordering},
};

use waof::AofAddress;

/// libs/cluster/Server/Replication/CheckpointFileType.cs:CheckpointFileType
///
/// 值域 1:1 对齐 C#（协议面 type 字段按数值透传）；值 3 为 C# 移除
/// STORE_DLOG 后的保留空洞，恒不出现
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointFileType {
  /// 无类型（C# NONE）
  None = 0,
  /// 主存 HybridLog 设备文件（C# STORE_HLOG）
  StoreHlog = 1,
  /// 对象存 HybridLog 设备文件（C# STORE_HLOG_OBJ；rust 统一检查点模型无对象存设备，接收臂拒绝）
  StoreHlogObj = 2,
  /// 主存索引快照（C# STORE_INDEX）
  StoreIndex = 4,
  /// 主存快照元数据（C# STORE_SNAPSHOT；rust 承载 wcpr CheckpointMeta 单消息载荷）
  StoreSnapshot = 5,
  /// 对象存快照（C# STORE_SNAPSHOT_OBJ；rust 统一检查点模型无对象存文件，接收臂拒绝）
  StoreSnapshotObj = 6,
  /// RangeIndex 逐 flush 快照（C# STORE_RANGEINDEX_FLUSH；rust 副本 AOF 直推
  /// 自检查点覆盖位点全量重放，flush 快照无恢复面消费，主端不发送，接收臂拒绝）
  StoreRangeindexFlush = 7,
  /// RangeIndex 检查点快照（C# STORE_RANGEINDEX_SNAPSHOT；随检查点段流下发，
  /// 头帧元数据 = key_id 128 位 LE，副本落盘检查点目录 token 子目录 rangeindex/）
  StoreRangeindexSnapshot = 8,
}

impl CheckpointFileType {
  /// 协议 type 字段解析（C# `(CheckpointFileType)fileTypeInt` 的受控形态：
  /// 域外值拒绝，杜绝未定义判别值流入接收臂）
  pub fn from_protocol(value: i64) -> Option<Self> {
    Some(match value {
      0 => Self::None,
      1 => Self::StoreHlog,
      2 => Self::StoreHlogObj,
      4 => Self::StoreIndex,
      5 => Self::StoreSnapshot,
      6 => Self::StoreSnapshotObj,
      7 => Self::StoreRangeindexFlush,
      8 => Self::StoreRangeindexSnapshot,
      _ => return None,
    })
  }
}

/// libs/cluster/Server/Replication/CheckpointEntry.cs:CheckpointMetadata
///
/// 检查点元数据，记录快照版本号、Token 与覆盖的 AOF 地址
#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
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

  /// 是否已被挂起（C# CheckpointEntry 无该属性，仅白盒用例观测挂起位）
  #[cfg(test)]
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
      // C# 其余类型抛异常；rust 淘汰判定仅以 HLOG/INDEX 两个真实文件集为基准，
      // 其余变体不参与共享判定（放行）且不在淘汰链上出现
      CheckpointFileType::None
      | CheckpointFileType::StoreHlogObj
      | CheckpointFileType::StoreSnapshot
      | CheckpointFileType::StoreSnapshotObj
      | CheckpointFileType::StoreRangeindexFlush
      | CheckpointFileType::StoreRangeindexSnapshot => true,
    }
  }

  /// libs/cluster/Server/Replication/CheckpointEntry.cs:ToByteArray
  ///
  /// 序列化为字节向量
  pub fn to_byte_array(&self) -> Vec<u8> {
    bitcode::encode(&self.metadata)
  }

  /// libs/cluster/Server/Replication/CheckpointEntry.cs:FromByteArray
  ///
  /// 反序列化 CheckpointEntry
  pub fn from_byte_array(serialized: &[u8]) -> Option<Self> {
    let metadata = bitcode::decode(serialized).ok()?;
    Some(Self::new(metadata))
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
