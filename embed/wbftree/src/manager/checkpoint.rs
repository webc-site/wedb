//! 检查点屏障与全树 CPR 快照 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SetCheckpointBarrier / SnapshotAllTreesForCheckpoint)

use std::{fs, path::Path, sync::atomic::Ordering};

use wbase::time::{Duration, Instant};

use super::RangeIndexManager;
use crate::{
  error::{Error, Result},
  service::{BfTreeService, backoff},
};

/// 单树快照等待的总超时上限 (对照 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:WaitForTreeCheckpoint 纯 Thread.Yield 无超时：
/// thread-per-core 下屏障持有者卡慢 I/O 时纯自旋烧整核，超时以错误显式暴露)
const CHECKPOINT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// 检查点 RAII 守卫，确保在任何退出路径下重置 checkpoint_in_progress 并清理 snapshot_pending
struct CheckpointGuard<'a>(&'a RangeIndexManager);
impl Drop for CheckpointGuard<'_> {
  fn drop(&mut self) {
    self.0.clear_checkpoint_barrier();
  }
}

/// 快照待处理 RAII 守卫，确保单树快照结束（无论成功或异常退出）后重置 snapshot_pending
struct SnapshotPendingGuard<'a>(&'a super::TreeEntry);
impl Drop for SnapshotPendingGuard<'_> {
  fn drop(&mut self) {
    self.0.snapshot_pending.store(false, Ordering::SeqCst);
  }
}

impl RangeIndexManager {
  /// 设置全局检查点屏障 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SetCheckpointBarrier)
  ///
  /// 将所有在线与就绪条目的 snapshot_pending 设为 true，并设置 checkpoint_in_progress 为 true
  pub fn set_checkpoint_barrier(&self) {
    let pin = self.live_indexes.pin();
    for entry in pin.values() {
      entry.snapshot_pending.store(true, Ordering::SeqCst);
    }
    self.checkpoint_in_progress.store(true, Ordering::SeqCst);
  }

  /// 清除全局检查点屏障 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:ClearCheckpointBarrier)
  ///
  /// 将 checkpoint_in_progress 设为 false，并将所有条目的 snapshot_pending 设为 false
  pub fn clear_checkpoint_barrier(&self) {
    self.checkpoint_in_progress.store(false, Ordering::SeqCst);
    let pin = self.live_indexes.pin();
    for entry in pin.values() {
      entry.snapshot_pending.store(false, Ordering::SeqCst);
    }
  }

  /// 等待单树快照完成屏障 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:WaitForTreeCheckpoint)
  ///
  /// 返回 `Ok(true)` 表示该树正在被快照且已等待其完成，调用方必须重试整次操作；
  /// `Ok(false)` 表示无需等待。等待采用退避阶梯，超过
  /// [`CHECKPOINT_WAIT_TIMEOUT`](self::CHECKPOINT_WAIT_TIMEOUT) 返回
  /// [`Error::Timeout`] (C# 为纯 `Thread.Yield` 无超时；本实现同步自旋无 epoch
  /// 兜底，快照持有者卡死时显式上抛而非无限烧核)。
  pub fn wait_for_tree_checkpoint(&self, key: &[u8]) -> Result<bool> {
    if !self.checkpoint_in_progress.load(Ordering::Acquire) {
      return Ok(false);
    }
    let key_id = Self::key_id_of(key);
    let entry_opt = {
      let pin = self.live_indexes.pin();
      pin.get(&key_id).cloned()
    };
    if let Some(entry) = entry_opt
      && entry.snapshot_pending.load(Ordering::Acquire)
    {
      let deadline = Instant::now() + CHECKPOINT_WAIT_TIMEOUT;
      let mut spins = 0u32;
      while entry.snapshot_pending.load(Ordering::Acquire) {
        if Instant::now() >= deadline {
          return Err(Error::Timeout);
        }
        backoff(spins);
        spins = spins.wrapping_add(1);
      }
      return Ok(true);
    }
    Ok(false)
  }

  /// 等待全局检查点完全解除 (退避阶梯等待，无超时——检查点流程自身带
  /// [`CHECKPOINT_WAIT_TIMEOUT`](self::CHECKPOINT_WAIT_TIMEOUT) 上限，必然推进)
  #[inline]
  pub fn wait_for_global_checkpoint(&self) {
    let mut spins = 0u32;
    while self.is_checkpoint_in_progress() {
      backoff(spins);
      spins = spins.wrapping_add(1);
    }
  }

  /// 为全局检查点快照所有活跃与就绪的 BfTree (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint)
  pub fn snapshot_all_trees_for_checkpoint(&self, checkpoint_token: &str) -> Result<()> {
    self.snapshot_all_trees_to_dir(&self.cpr_dir, checkpoint_token)?;
    Ok(())
  }

  /// 为指定目标目录快照所有活跃与就绪的 BfTree
  ///
  /// 屏障时序 1:1 对标 C# 两阶段语义：未处于检查点中时就地设置屏障 (单步便捷路径)；
  /// 调用方已先行 [`set_checkpoint_barrier`](Self::set_checkpoint_barrier) 则保留原屏障
  /// 时序不重设——屏障设置与快照执行之间新注册的条目 (snapshot_pending == false)
  /// 不会被纳入本次检查点，与 C# 版本切换/FlushBegin 分离语义一致
  ///
  /// 返回实际生成快照文件的树数 (跳过的非 pending 条目与无工作文件的冷条目不计入)，
  /// 供 wcpr 检查点日志与校验使用
  pub fn snapshot_all_trees_to_dir(
    &self,
    target_dir: &Path,
    checkpoint_token: &str,
  ) -> Result<usize> {
    if !self.is_checkpoint_in_progress() {
      self.set_checkpoint_barrier();
    }
    let _guard = CheckpointGuard(self);

    let entries = self.live_entries();

    let token_snapshot_dir = target_dir.join(checkpoint_token).join("rangeindex");
    let _ = fs::create_dir_all(&token_snapshot_dir);

    let mut snapshot_count = 0;
    // 文件名缓冲提到循环外复用 (时间/分配优化)：前缀恒为 26 字符 Base32 定长编码，
    // 每轮 clear() 后重写，N 树检查点快照从 N 次 String 堆分配降为 1 次
    // (对标 C# string.Concat 每条目一次分配的 HashPrefix + ".bftree" 拼接)
    let mut dest_file_name =
      String::with_capacity(super::HASH_PREFIX_LEN + super::TREE_FILE_SUFFIX.len());
    for entry in &entries {
      // 仅快照屏障设置时已存在的条目参与检查点快照；屏障之后新注册的条目
      // snapshot_pending == false 直接跳过 (1:1 对标 C# SnapshotPending == 0 → continue)，
      // 避免把检查点 hlog 快照之外新建的幻影树写进检查点文件导致恢复时复活幽灵索引
      if !entry.snapshot_pending.load(Ordering::Acquire) {
        continue;
      }
      let _pending_guard = SnapshotPendingGuard(entry);
      let tree_opt = entry.tree.read().as_ref().cloned();
      dest_file_name.clear();
      dest_file_name.push_str(entry.hash_prefix.as_str());
      dest_file_name.push_str(super::TREE_FILE_SUFFIX);
      let token_dest = token_snapshot_dir.join(&dest_file_name);

      if let Some(tree) = tree_opt {
        entry.snapshot_under_claim(&tree, &token_dest)?;
        snapshot_count += 1;
      } else {
        // 冷树 / 待激活树：data.bftree 已在磁盘上就绪，直接复制到检查点目录。
        // 工作文件存在但复制失败属致命错误，向上传播 (1:1 对标 C# File.Copy 异常传播)
        let data_path = self.data_file_path(&entry.hash_prefix);
        if data_path.exists() {
          fs::copy(&data_path, &token_dest)?;
          snapshot_count += 1;
        }
      }
    }

    Ok(snapshot_count)
  }

  /// 在防重入快照 claim 下把指定键的在线树 CPR 快照到目标路径 (1:1 对标
  /// Garnet SnapshotForMigration，供 RENAME / 迁移流导出使用)
  ///
  /// ⚠️ 调用方须已持该键的条带互斥写锁：快照窗口内阻止同键数据写入，保证快照
  /// 之后旧键上的任何写入要么发生在调用方删除旧键之前被丢弃 (调用方契约)，要么
  /// 显式失败，绝不静默丢失。注册表已无条目时 (并发注销竞态) 直接快照——此时
  /// 无其他快照源 (flush/checkpoint 全部经条目 claim 路由)，无需防重入。
  pub fn snapshot_tree_to_path_locked(
    &self,
    key: &[u8],
    tree: &BfTreeService,
    destination_path: &Path,
  ) -> Result<()> {
    let key_id = Self::key_id_of(key);
    let entry = self.live_indexes.pin().get(&key_id).cloned();
    match entry {
      Some(entry) => entry.snapshot_under_claim(tree, destination_path),
      None => tree.cpr_snapshot(destination_path),
    }
  }
}
