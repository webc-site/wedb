use std::{sync::Arc, thread::yield_now};

use log::{info, trace, warn};

use crate::server::replication::checkpoint_entry::{CheckpointEntry, CheckpointFileType};

/// libs/cluster/Server/Replication/CheckpointStore.cs:CheckpointStore
///
/// 内存检查点仓库，管理复制运行时内存中的检查点链表、读者并发访问保护及过期检查点安全淘汰。
///
/// 【架构设计说明】：
/// 本结构体为纯内存并发安全容器，不持有任何存储引擎或文件系统句柄。
/// 对标 C# 原型中的磁盘扫描与 Cookie 反序列化方法（`GetLatestCheckpointEntryFromDisk`、
/// `GetLatestCheckpointFromDiskInfo`）在 wedb 中已被完全剥离：
/// 1. 存储持久化由底层的 `wcpr` 独立承担；
/// 2. 复制元数据遵循 `cookie 属复制域不落地` 原则（`wcpr/src/meta.rs:229`），由 `replication.conf` 单独持久化；
/// 3. 因此 `CheckpointStore` 仅在内存中维护全量与增量快照的读者借用生命周期，彻底杜绝虚假的跨层磁盘扫描。
#[derive(Debug)]
pub struct CheckpointStore {
  entries: Vec<Arc<CheckpointEntry>>,
  safely_remove_outdated: bool,
}

impl Default for CheckpointStore {
  fn default() -> Self {
    Self::new(true)
  }
}

impl CheckpointStore {
  /// libs/cluster/Server/Replication/CheckpointStore.cs:CheckpointStore
  pub fn new(safely_remove_outdated: bool) -> Self {
    Self {
      entries: Vec::new(),
      safely_remove_outdated,
    }
  }

  /// libs/cluster/Server/Replication/CheckpointStore.cs:Initialize
  ///
  /// 初始化检查点仓库，载入磁盘最新检查点
  pub fn initialize(&mut self, latest_disk_entry: Option<CheckpointEntry>) {
    self.entries.clear();
    if let Some(entry) = latest_disk_entry
      && entry.metadata.store_version != -1
    {
      let arc_entry = Arc::new(entry);
      self.entries.push(arc_entry.clone());
      if self.safely_remove_outdated {
        self.purge_all_checkpoints_except_entry(Some(&arc_entry));
      }
    }
  }

  /// libs/cluster/Server/Replication/CheckpointStore.cs:WaitForReplicas
  ///
  /// 等待从副本读取任务退出并挂起读者
  pub fn wait_for_replicas(&self) {
    if self.entries.len() <= 1 {
      return;
    }
    for entry in &self.entries {
      while !entry.try_suspend_readers() {
        yield_now();
      }
    }
  }

  /// libs/cluster/Server/Replication/CheckpointStore.cs:PurgeAllCheckpointsExceptEntry
  ///
  /// 淘汰除指定条目外的其它孤儿检查点
  pub fn purge_all_checkpoints_except_entry(&mut self, keep_entry: Option<&Arc<CheckpointEntry>>) {
    if let Some(keep) = keep_entry {
      self
        .entries
        .retain(|e| Arc::ptr_eq(e, keep) || e.metadata == keep.metadata);
    } else {
      self.entries.clear();
    }
  }

  /// libs/cluster/Server/Replication/CheckpointStore.cs:AddCheckpointEntry
  ///
  /// 添加新检查点条目到列表中
  pub fn add_checkpoint_entry(&mut self, mut entry: CheckpointEntry, full_checkpoint: bool) {
    if !full_checkpoint && let Some(last) = self.entries.last() {
      entry.metadata.store_index_token = last.metadata.store_index_token;
    }

    let arc_entry = Arc::new(entry);
    self.entries.push(arc_entry);

    if self.safely_remove_outdated {
      self.delete_outdated_checkpoints();
    }
  }

  /// libs/cluster/Server/Replication/CheckpointStore.cs:CanDeleteToken
  ///
  /// 检查某 token 是否可以安全删除
  fn can_delete_token(&self, idx: usize, file_type: CheckpointFileType) -> bool {
    let to_delete = &self.entries[idx];
    let tail_idx = self.entries.len().saturating_sub(1);

    for curr in &self.entries[idx + 1..tail_idx] {
      if !curr.contains_shared_token(to_delete, file_type) {
        return true;
      }
      if !curr.try_suspend_readers() {
        return false;
      }
    }

    // 检查 tail
    if let Some(tail) = self.entries.get(tail_idx) {
      !tail.contains_shared_token(to_delete, file_type)
    } else {
      true
    }
  }

  /// libs/cluster/Server/Replication/CheckpointStore.cs:DeleteOutdatedCheckpoints
  ///
  /// 安全清理并淘汰过期的检查点条目
  pub fn delete_outdated_checkpoints(&mut self) {
    if self.entries.len() <= 1 {
      return;
    }

    trace!("Try safe delete in-memory outdated checkpoints");
    let mut remove_count = 0usize;

    while remove_count + 1 < self.entries.len() {
      let curr = &self.entries[remove_count];
      if !curr.try_suspend_readers() {
        break;
      }
      if !self.can_delete_token(remove_count, CheckpointFileType::StoreHlog) {
        break;
      }
      if !self.can_delete_token(remove_count, CheckpointFileType::StoreIndex) {
        break;
      }

      warn!(
        "Deleting outdated checkpoint with version {}",
        curr.metadata.store_version
      );
      remove_count += 1;
    }

    if remove_count > 0 {
      self.entries.drain(0..remove_count);
      info!(
        "Deleted {} outdated checkpoints, remaining {}",
        remove_count,
        self.entries.len()
      );
    }
  }

  /// libs/cluster/Server/Replication/CheckpointStore.cs:TryGetLatestCheckpointEntryFromMemory
  ///
  /// 获取内存中最新检查点条目并递增读者计数
  pub fn try_get_latest_checkpoint_entry_from_memory(&self) -> Option<Arc<CheckpointEntry>> {
    let tail = self.entries.last()?;
    if tail.try_add_reader() {
      Some(tail.clone())
    } else {
      None
    }
  }

  /// 查看内存最新检查点条目（仅查询元数据，不递增读者计数）
  pub fn latest_entry(&self) -> Option<Arc<CheckpointEntry>> {
    self.entries.last().cloned()
  }

  /// libs/cluster/Server/Replication/CheckpointStore.cs:GetLatestCheckpointFromMemoryInfo
  ///
  /// 返回格式化内存最新检查点信息
  pub fn get_latest_checkpoint_from_memory_info(&self) -> String {
    if let Some(tail) = self.entries.last() {
      tail.to_string()
    } else {
      "(empty)".to_string()
    }
  }

  /// 当前内存中条目数量
  pub fn entry_count(&self) -> usize {
    self.entries.len()
  }
}

#[cfg(test)]
mod tests {
  use waof::AofAddress;

  use super::*;
  use crate::server::replication::checkpoint_entry::CheckpointMetadata;

  #[test]
  fn test_checkpoint_store_add_and_delete() {
    let mut store = CheckpointStore::new(true);
    let mut m1 = CheckpointMetadata::new(1);
    m1.store_version = 1;
    m1.store_hlog_token = 100;
    m1.store_index_token = 200;
    m1.store_checkpoint_covered_aof_address = AofAddress::create(1, 100);

    store.add_checkpoint_entry(CheckpointEntry::new(m1), true);
    assert_eq!(store.entry_count(), 1);

    let mut m2 = CheckpointMetadata::new(1);
    m2.store_version = 2;
    m2.store_hlog_token = 101;
    m2.store_index_token = 201;
    m2.store_checkpoint_covered_aof_address = AofAddress::create(1, 200);

    store.add_checkpoint_entry(CheckpointEntry::new(m2), true);
    // m1 不与 m2 共享 token 且无读者，安全淘汰 m1，剩余 m2
    assert_eq!(store.entry_count(), 1);
    let latest = store
      .try_get_latest_checkpoint_entry_from_memory()
      .expect("should get latest");
    assert_eq!(latest.metadata.store_version, 2);
    latest.remove_reader();

    // 验证从副本读取等待
    store.wait_for_replicas();

    assert!(
      store
        .get_latest_checkpoint_from_memory_info()
        .contains("storeVersion=2")
    );
  }
}
