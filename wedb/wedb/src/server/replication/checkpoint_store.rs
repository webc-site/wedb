use std::{
  path::{Path, PathBuf},
  sync::Arc,
  thread::yield_now,
};

use log::{info, trace, warn};
use wcpr::{list_checkpoints, purge_checkpoint};

use crate::server::replication::checkpoint_entry::{CheckpointEntry, CheckpointFileType};

/// 保留指定 token 集（hlog/index），清理检查点目录内其余陈旧 token
///
/// C# CheckpointStore.cs:PurgeAllCheckpointsExceptEntry 物理清理段对位
/// （:94 DeleteLogCheckpoint / :104 DeleteIndexCheckpoint）；wcpr 统一检查点
/// 模型下单 token 一套文件，`purge_checkpoint` 为 best-effort 删除，失败
/// warn 不阻断（与副本导入侧既有容错口径一致）
pub(crate) fn purge_checkpoint_files_except(dir: &Path, keep_hlog: u128, keep_index: u128) {
  if let Ok(tokens) = list_checkpoints(dir) {
    for token in tokens {
      if token != keep_hlog
        && token != keep_index
        && let Err(e) = purge_checkpoint(dir, token)
      {
        warn!("Failed purging orphan checkpoint {token:#x}: {e}");
      }
    }
  }
}

/// libs/cluster/Server/Replication/CheckpointStore.cs:CheckpointStore
///
/// 内存检查点仓库，管理复制运行时内存中的检查点链表、读者并发访问保护及过期检查点安全淘汰。
///
/// 【架构设计说明】：
/// C# 构造期注入 storeWrapper / clusterProvider，物理淘汰经
/// ReplicationLogCheckpointManager 落盘；rust 对位为注入检查点根目录
/// `checkpoint_dir`（rm 构造早于目录装配，经 `set_checkpoint_dir` 装配期
/// 一次注入，同 `set_commit_channel` 先例），淘汰与孤儿清理直接转调
/// `wcpr` 公开 API，不自建磁盘访问面。目录未注入（纯内存形态，仅测试）
/// 时淘汰退化为只裁内存链表。
///
/// 复制元数据遵循 `cookie 属复制域不落地` 原则（`wcpr/src/meta.rs:229`），
/// 由 `replication.conf` 单独持久化；磁盘扫描与最新条目构造由
/// replication_manager 的 `initialize_checkpoint_store` 承接。
#[derive(Debug)]
pub struct CheckpointStore {
  entries: Vec<Arc<CheckpointEntry>>,
  safely_remove_outdated: bool,
  checkpoint_dir: Option<PathBuf>,
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
      checkpoint_dir: None,
    }
  }

  /// 注入检查点根目录（装配期一次注入，对标 C# 构造期持
  /// ReplicationLogCheckpointManager；注入后淘汰具备物理删除能力）
  pub fn set_checkpoint_dir(&mut self, dir: PathBuf) {
    self.checkpoint_dir = Some(dir);
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
  /// 淘汰检查点目录内除指定条目 token 外的孤儿快照：纯磁盘清理，不动内存
  /// 链表（C# 实现仅遍历 GetLog/GetIndexCheckpointTokens 逐 token 删除，
  /// head/tail 不碰，内存由 Initialize 先行赋值；entry == null 时直接 return
  /// 亦不清内存）。仅由构造/Initialize 期调用，此刻必无在途读者（C# :47-53
  /// 论证），不做读者检查；目录未注入时无从清理（纯内存形态）
  pub fn purge_all_checkpoints_except_entry(&self, keep_entry: Option<&Arc<CheckpointEntry>>) {
    let Some(keep) = keep_entry else {
      return;
    };
    if let Some(dir) = &self.checkpoint_dir {
      purge_checkpoint_files_except(
        dir,
        keep.metadata.store_hlog_token,
        keep.metadata.store_index_token,
      );
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
  /// 安全清理并淘汰过期的检查点条目：读者闸门（try_suspend_readers +
  /// can_delete_token 两道共享 token 判定）通过后，内存出链且检查点目录
  /// 内对应 token 物理删除（C# :182/:186 DeleteLogCheckpoint / DeleteIndexCheckpoint
  /// 对位）。wcpr 统一检查点模型单 token 一套文件，hlog/index 判定同序
  /// 保留、删除一次 purge 全清；增量继承形态 index token 异于 hlog 时补删
  /// 一次。purge_checkpoint 为 best-effort 删除，失败 warn 不阻断
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
      if let Some(dir) = &self.checkpoint_dir {
        let hlog = curr.metadata.store_hlog_token;
        if let Err(e) = purge_checkpoint(dir, hlog) {
          warn!("Failed purging outdated hlog checkpoint {hlog:#x}: {e}");
        }
        let index = curr.metadata.store_index_token;
        if index != hlog
          && let Err(e) = purge_checkpoint(dir, index)
        {
          warn!("Failed purging outdated index checkpoint {index:#x}: {e}");
        }
      }
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

  /// 磁盘淘汰接通读者闸门：读者持有的条目其 token 文件不被 unlink，
  /// 释放读者后淘汰链推进回收（C# DeleteOutdatedCheckpoints
  /// TrySuspendReaders -> DeleteLogCheckpoint/DeleteIndexCheckpoint 对位）
  #[test]
  fn test_delete_outdated_purges_disk_except_reader_held() {
    use std::fs::{create_dir_all, write};

    use wcpr::{list_checkpoints, meta_filename};

    let dir = tempfile::tempdir().expect("tempdir");
    let ckpt_dir = dir.path().join("checkpoints");
    create_dir_all(&ckpt_dir).expect("mkdir");

    // 手工落 meta 占位文件（list/purge 的识别面，best-effort 删除即可）
    let seed = |token: u128| {
      write(ckpt_dir.join(meta_filename(token)), []).expect("seed meta");
    };

    let mut store = CheckpointStore::new(true);
    store.set_checkpoint_dir(ckpt_dir.clone());

    let entry = |version: i64, token: u128| {
      let mut m = CheckpointMetadata::new(1);
      m.store_version = version;
      m.store_hlog_token = token;
      m.store_index_token = token;
      CheckpointEntry::new(m)
    };

    // 三份磁盘快照 + 前两次登记（e1 无读者，随登记淘汰回收 t1）
    seed(1);
    seed(2);
    seed(3);
    store.add_checkpoint_entry(entry(1, 1), true);
    store.add_checkpoint_entry(entry(2, 2), true);
    assert_eq!(
      list_checkpoints(&ckpt_dir).expect("list"),
      vec![2, 3],
      "e1 无读者，随登记淘汰回收 t1"
    );

    // 传输会话持读者：下一次登记触发的淘汰停手，t2 不得 unlink
    let reader = store
      .try_get_latest_checkpoint_entry_from_memory()
      .expect("reader");
    store.add_checkpoint_entry(entry(3, 3), true);
    assert_eq!(
      list_checkpoints(&ckpt_dir).expect("list"),
      vec![2, 3],
      "读者持有期间快照 t2 不得被 unlink"
    );

    // 释放读者后淘汰链推进回收
    reader.remove_reader();
    seed(4);
    store.add_checkpoint_entry(entry(4, 4), true);
    assert_eq!(
      list_checkpoints(&ckpt_dir).expect("list"),
      vec![4],
      "释放读者后下一轮登记回收全部陈旧 token"
    );
  }

  /// purge_all_checkpoints_except_entry 物理清理段：保留 keep entry token，
  /// 其余孤儿 token 连文件一并回收（C# :94/:104 对位；Initialize 期无读者）
  #[test]
  fn test_purge_all_except_entry_cleans_orphan_files() {
    use std::fs::{create_dir_all, write};

    use wcpr::{list_checkpoints, meta_filename};

    let dir = tempfile::tempdir().expect("tempdir");
    let ckpt_dir = dir.path().join("checkpoints");
    create_dir_all(&ckpt_dir).expect("mkdir");

    let seed = |token: u128| {
      write(ckpt_dir.join(meta_filename(token)), []).expect("seed meta");
    };

    let mut store = CheckpointStore::new(true);
    store.set_checkpoint_dir(ckpt_dir.clone());

    seed(1);
    seed(2);
    seed(9);
    let mut m = CheckpointMetadata::new(1);
    m.store_version = 2;
    m.store_hlog_token = 2;
    m.store_index_token = 2;
    let keep = Arc::new(CheckpointEntry::new(m));

    store.purge_all_checkpoints_except_entry(Some(&keep));

    // 对标 C#：purge 纯磁盘清理不动内存链表（空表保持空，keep 由调用方自行入链）
    assert_eq!(store.entry_count(), 0);
    assert_eq!(
      list_checkpoints(&ckpt_dir).expect("list"),
      vec![2],
      "除 keep entry 的 token 外孤儿快照应全部回收"
    );
  }
}
