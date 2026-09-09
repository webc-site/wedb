//! 刷盘事件触发的单树 CPR 快照与存根标记
//! (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotTreeForFlush / SnapshotForFlushCold / LogOnFlushInvariantViolation)

use std::fs;

use super::RangeIndexManager;
use crate::{error::Result, stub::RangeIndexStub};

impl RangeIndexManager {
  /// 刷盘事件触发快照与存根标记 (使用防重入快照锁)
  pub fn on_flush(&self, key: &[u8], stub: &mut RangeIndexStub) -> Result<()> {
    self.on_flush_internal(key, stub, None)
  }

  /// 带有逻辑地址的刷盘事件触发快照与存根标记
  pub fn on_flush_address(
    &self,
    key: &[u8],
    stub: &mut RangeIndexStub,
    logical_address: i64,
  ) -> Result<()> {
    self.on_flush_internal(key, stub, Some(logical_address))
  }

  fn on_flush_internal(
    &self,
    key: &[u8],
    stub: &mut RangeIndexStub,
    logical_address: Option<i64>,
  ) -> Result<()> {
    // 过期源存根 no-op (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotTreeForFlush)：所有权已转移至尾部新记录时，
    // 既不快照过期视图也不置位 IsFlushed，避免把陈旧数据误标为已刷盘
    if stub.is_transferred() {
      return Ok(());
    }

    let key_id = Self::key_id_of(key);
    let hash_prefix = Self::base32_prefix_of(key);
    let flush_path = match logical_address {
      Some(addr) => {
        // 带地址刷盘文件产生，重新开启惰性恢复的地址扫描通道
        self.notice_addr_flush_files();
        self.log_flush_path(&hash_prefix, addr)
      }
      None => self.bare_flush_path(&hash_prefix),
    };

    // 热路径 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotTreeForFlush 活跃分支)：在线树直接 CPR 快照，
    // CPR 与工作线程并发安全，全程不持条带锁 (Arc 保活使快照期间树实例不可被释放)
    if let Some((entry, tree)) = self.live_tree_of(key_id) {
      entry.snapshot_under_claim(&tree, &flush_path)?;
      stub.set_flushed(true);
      return Ok(());
    }

    // 冷路径 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotForFlushCold)：持条带共享锁与 RestoreTree / 注销
    // 路径串行化，锁内复查——树可能恰在取锁前被并发恢复激活。刻意用 S 锁而非 X 锁
    // (对标 C# 冷路径死锁纪律)：claim 等待者与 CPR 快照均不依赖条带锁退出，无锁序
    // 倒置；X 锁则会让同条带全部数据操作停摆整个复制时长。
    let key_hash = Self::key_hash_of(key);
    let _stripe_lock = self.locks.read(key_hash);
    if let Some((entry, tree)) = self.live_tree_of(key_id) {
      entry.snapshot_under_claim(&tree, &flush_path)?;
      stub.set_flushed(true);
      return Ok(());
    }

    // 无在线树时 data.bftree 无并发写者，直接复制为刷盘快照；工作文件缺失属不变量
    // 破坏 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:LogOnFlushInvariantViolation)，保持未刷盘状态交由上层显式处理
    let data_path = self.data_file_path(&hash_prefix);
    if data_path.exists() {
      fs::copy(&data_path, &flush_path)?;
      stub.set_flushed(true);
    }

    Ok(())
  }
}
