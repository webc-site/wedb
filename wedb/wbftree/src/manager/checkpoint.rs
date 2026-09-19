//! 检查点屏障与全树 CPR 快照 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SetCheckpointBarrier / SnapshotAllTreesForCheckpoint)

use std::{fs, path::Path, sync::atomic::Ordering};

use coarsetime::Duration as InstantDuration;

use super::RangeIndexManager;
use crate::{error::Result, service::BfTreeService};

/// 快照 claim 自旋等待的超时上界 (对照 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotUnderClaim
/// 纯 Thread.Yield 自旋无超时：thread-per-core 下 claim 持有者卡慢 I/O 时纯自旋烧整核，
/// 超时以 [`Error::Timeout`] 显式暴露；读屏障等待不设此上界，见
/// `wait_for_tree_checkpoint_async`)
pub(crate) const CHECKPOINT_WAIT_TIMEOUT: InstantDuration = InstantDuration::from_secs(30);

/// 检查点 RAII 守卫：仅清理由本次调用就地设置的屏障
///
/// 单步便捷路径（调用方未预设屏障）在退出时清屏；调用方预设的 VersionShift 外层
/// 屏障归调用方所有——屏障须持续持有至 flush 完成后才由 wcpr 统一解除（对标 C#
/// SnapshotAllTreesForCheckpoint 在 FlushBegin 触发器内清屏的时序归属），此处绝不
/// 越权提前放行树写入，否则快照后 meta 记录与树效果将失去刚性对齐
struct CheckpointGuard<'a>(&'a RangeIndexManager, bool);
impl Drop for CheckpointGuard<'_> {
  fn drop(&mut self) {
    if self.1 {
      self.0.clear_checkpoint_barrier();
    }
  }
}

/// 快照待处理 RAII 守卫，确保单树快照结束（无论成功或异常退出）后重置 snapshot_pending 并通知就绪等待者
struct SnapshotPendingGuard<'a>(&'a super::TreeEntry);
impl Drop for SnapshotPendingGuard<'_> {
  fn drop(&mut self) {
    self.0.snapshot_pending.store(false, Ordering::SeqCst);
    self.0.checkpoint_event.notify(usize::MAX);
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
  /// 将 checkpoint_in_progress 设为 false，并将所有条目的 snapshot_pending 设为 false，触发 event_listener 通知
  pub fn clear_checkpoint_barrier(&self) {
    self.checkpoint_in_progress.store(false, Ordering::SeqCst);
    let pin = self.live_indexes.pin();
    for entry in pin.values() {
      entry.snapshot_pending.store(false, Ordering::SeqCst);
      entry.checkpoint_event.notify(usize::MAX);
    }
    self.checkpoint_event.notify(usize::MAX);
  }

  /// 等待单树快照完成屏障 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:WaitForTreeCheckpoint)
  ///
  /// 返回 `Ok(true)` 表示该树正在被快照且已等待其完成，调用方必须重试整次操作；
  /// `Ok(false)` 表示无需等待。C# 为纯 `Thread.Yield` 忙转交且无超时，此处以
  /// event_listener 无阻塞挂起承接（零轮询、零 CPU），超时语义与 C# 一致：
  /// 不设上限，屏障持有者异常时读者被动挂起直至清屏。
  pub async fn wait_for_tree_checkpoint_async(&self, key: &[u8]) -> Result<bool> {
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
      loop {
        let listener = entry.checkpoint_event.listen();
        if !entry.snapshot_pending.load(Ordering::Acquire) {
          return Ok(true);
        }
        listener.await;
      }
    }
    Ok(false)
  }

  /// 为指定目标目录快照所有活跃与就绪的 BfTree (1:1 对标
  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint，
  /// 目标目录显式传参替代 C# 构造器暂存的 cpr 目录)
  ///
  /// 屏障时序 1:1 对标 C# 两阶段语义：未处于检查点中时就地设置屏障并在退出时清屏
  /// (单步便捷路径，wbftree 测试与独立快照场景)；调用方已先行
  /// [`set_checkpoint_barrier`](Self::set_checkpoint_barrier) 则屏障归调用方所有——
  /// 保留屏障时序不重设、退出时亦不清屏，由调用方持至 flush 完成后统一解除（wcpr
  /// create_checkpoint_inner 的 VersionShift 外层屏障），屏障设置与快照执行之间新注册
  /// 的条目 (snapshot_pending == false) 不会被纳入本次检查点，与 C# 版本切换/FlushBegin
  /// 分离语义一致
  ///
  /// 返回实际生成快照文件的树数 (跳过的非 pending 条目与无工作文件的冷条目不计入)，
  /// 供 wcpr 检查点日志与校验使用
  pub fn snapshot_all_trees_to_dir(
    &self,
    target_dir: &Path,
    checkpoint_token: u128,
  ) -> Result<usize> {
    let owned = !self.is_checkpoint_in_progress();
    if owned {
      self.set_checkpoint_barrier();
    }
    let _guard = CheckpointGuard(self, owned);

    let entries = self.live_entries();

    let mut token_dest = Self::token_snapshot_dir(target_dir, checkpoint_token);
    fs::create_dir_all(&token_dest)?;

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
      let prefix = entry.hash_prefix();
      Self::write_tree_file_name(&mut dest_file_name, prefix.as_str());
      token_dest.push(&dest_file_name);

      let res = if let Some(tree) = tree_opt {
        entry.snapshot_under_claim(&tree, &token_dest).map(|()| {
          snapshot_count += 1;
        })
      } else {
        // 冷树 / 待激活树：data.bftree 已在磁盘上就绪，直接复制到检查点目录。
        // 工作文件存在但复制失败属致命错误，向上传播 (1:1 对标 C# File.Copy 异常传播)
        let data_path = self.data_file_path(&prefix);
        if data_path.exists() {
          fs::copy(&data_path, &token_dest).map(|_| {
            snapshot_count += 1;
          })?
        }
        Ok(())
      };
      token_dest.pop();
      res?;
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
