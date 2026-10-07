use std::{
  io::Error,
  path::{Path, PathBuf},
  sync::Arc,
  thread::yield_now,
};

use compio::fs::read;
use log::{info, trace, warn};
use wcpr::{CheckpointMeta, list_all_checkpoint_tokens, purge_checkpoint};

use super::replication_manager::EMPTY_CHECKPOINT_INFO;
use crate::{
  error::Result,
  server::replication::checkpoint_entry::{CheckpointEntry, CheckpointFileType},
};

/// 保留指定 token 集（hlog/index），清理检查点目录内其余陈旧 token
///
/// C# CheckpointStore.cs:PurgeAllCheckpointsExceptEntry 的物理清理段对位
/// （:82 单点转调 PurgeAllCheckpointsExceptTokens，其内 :89/:99 分别
/// GetLogCheckpointTokens/GetIndexCheckpointTokens 物理列举快照目录全部 Token，
/// :94/:104 DeleteLogCheckpoint/DeleteIndexCheckpoint 逐 token 删除）；wcpr 统一
/// 检查点模型下单 token 一套文件，`purge_checkpoint` 为 best-effort 删除，wcpr
/// 内部吞错、无失败通道（与副本导入侧既有容错口径一致）。
///
/// 枚举候选必须用 [`list_all_checkpoint_tokens`]（物理实体全量）而非
/// `list_checkpoints`（仅含完整 `.meta` 的有效快照）：全量快照分块传输在 index 文件
/// 或 RangeIndex 子目录落盘后、`.meta` 提交前遭遇网络中断/超时/断连时，磁盘上仅存
/// `index_<token>.ckpt` 与 Base32 子目录而**无** `.meta`。若按 `list_checkpoints`
/// 枚举，这类未提交孤儿 Token 无法被感知，本口对其彻底穿透跳过，`purge_checkpoint`
/// 永不调用——每次传输中断重试即在从库磁盘沉淀一份数 GB 量级的孤儿索引文件与子目录，
/// 随网络抖动累积直至 ENOSPC。C# 侧两轨枚举本就以物理清单为准，无「等 meta 才可见」
/// 约束，故 rust 清理轨对齐其口径；恢复选点轨（`find_latest_checkpoint`）仍持
/// `.meta` 完整性过滤，两轨语义分工不可互换。
pub(crate) fn purge_checkpoint_files_except(dir: &Path, keep_hlog: u128, keep_index: u128) {
  let mut purged = false;
  if let Ok(tokens) = list_all_checkpoint_tokens(dir) {
    for token in tokens {
      if token != keep_hlog && token != keep_index {
        // best-effort 清理：wcpr 内部吞错，删除失败无错误可感知
        purge_checkpoint(dir, token);
        purged = true;
      }
    }
  }
  if purged && let Err(e) = wdev::sync_dir(dir) {
    warn!("Failed syncing checkpoint dir after purging: {e}");
  }
}

/// 检查点 meta 读取 + hlog begin 扇区下对齐（replication 域单点）
///
/// 四步链收口：`meta_filename(token)` 定位 → compio 异步整读 →
/// [`CheckpointMeta::decode`] → `begin_address / sector * sector` 下对齐。
/// 主端推流读钉注册 / 滞后补删 / 快照下发段范围基准三处共用；错误经
/// [`crate::Error::Io`] 透明转发，语境化日志由调用方按各自口径落
pub(crate) async fn read_meta_aligned_begin(
  dir: &Path,
  token: u128,
  sector: u64,
) -> Result<(CheckpointMeta, u64)> {
  let meta_path = dir.join(wcpr::meta_filename(token));
  let bytes = read(&meta_path).await?;
  let meta = CheckpointMeta::decode(&bytes)
    .map_err(|e| Error::other(format!("checkpoint meta decode failed: {e}")))?;
  let begin = meta.hlog_meta.begin_address / sector * sector;
  Ok((meta, begin))
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
/// 由 `replication.toml` 单独持久化；磁盘扫描与最新条目构造由
/// replication_manager 的 `get_latest_checkpoint_entry_from_disk` 承接。
///
/// 内存链表与磁盘清理是两条独立轨（对标 C#）：链表由调用方自行组装——
/// `Initialize`（CheckpointStore.cs:38-57）自身先 :40 赋 head = tail 取最新
/// 磁盘条目，:55-56 才调 purge；副本侧 ReplicaDiskbasedSync.cs:336 purge 之后
/// 紧跟 :340 重扫盘重建。purge 内既无 retain 也无 clear，链表的裁剪只发生在
/// `delete_outdated_checkpoints`（C# DeleteOutdatedCheckpoints 对位）。
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
  /// 初始化检查点仓库，载入磁盘最新检查点：先重建内存链表（C# :40 自赋
  /// head = tail = GetLatestCheckpointEntryFromDisk，:42-45 storeVersion == -1
  /// 时置空），再清磁盘孤儿（C# :55-56），两段分立、次序不可颠倒
  pub fn initialize(&mut self, latest_disk_entry: Option<CheckpointEntry>) {
    self.entries.clear();
    if let Some(entry) = latest_disk_entry
      && entry.metadata.store_version != -1
    {
      let arc_entry = Arc::new(entry);
      self.entries.push(arc_entry.clone());
      if self.safely_remove_outdated {
        self.purge_all_checkpoints_except_entry(&arc_entry);
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
  /// 淘汰检查点目录内除 keep 条目 token 外的孤儿快照：C# 函数体只有 :82 一次
  /// PurgeAllCheckpointsExceptTokens 转调，其内 :94/:104 逐 token 物理删除，
  /// 全程不写 head/tail（内存链表的裁剪只发生在 delete_outdated_checkpoints）。
  /// C# 形参的 null 兜底（:79 扫盘取最新）rust 无该面且零调用方，故形参收为
  /// 非空 `&CheckpointEntry`，不留 Option 旧形态。仅由构造/Initialize 期调用，
  /// 此刻必无在途读者（C# :52-54 论证），不做读者检查；目录未注入时无从清理
  /// （纯内存形态）
  pub fn purge_all_checkpoints_except_entry(&self, keep: &CheckpointEntry) {
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
  /// 一次。purge_checkpoint 为 best-effort 删除，wcpr 内部吞错、不阻断
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
        // best-effort 清理：wcpr 内部吞错，删除失败无错误可感知
        purge_checkpoint(dir, hlog);
        let index = curr.metadata.store_index_token;
        if index != hlog {
          purge_checkpoint(dir, index);
        }
      }
      remove_count += 1;
    }

    if remove_count > 0 {
      if let Some(dir) = &self.checkpoint_dir
        && let Err(e) = wdev::sync_dir(dir)
      {
        warn!("Failed syncing checkpoint dir after purging outdated checkpoints: {e}");
      }
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
      EMPTY_CHECKPOINT_INFO.to_string()
    }
  }

  /// 当前内存中条目数量
  pub fn entry_count(&self) -> usize {
    self.entries.len()
  }
}
