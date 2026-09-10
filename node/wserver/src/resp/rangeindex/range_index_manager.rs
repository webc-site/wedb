//! 范围索引管理器会话门面（对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs 主分片）
//!
//! C# RangeIndexManager 直接持有 BfTreeService / liveIndexes / 条带锁；Rust 侧
//! 该引擎面由 wkv 重导出的 [`wkv::RangeIndexManager`]（embed/wbftree）承接。
//! 本结构是会话域的持有门面：绑定引擎实例、承接恢复期检查点令牌（C#
//! recoveredCheckpointToken）、并以键为径暴露快照防重入 claim 与文件路径族。
//! 命名差异说明：C# 文件名前缀为 32 字符十六进制（XxHash128 → Guid("N")），
//! Rust 引擎为 26 字符 Base32（同一 128 位 key_id 的定长编码），路径形态由
//! 引擎统一决定，本门面只转发。

use std::{
  path::{Path, PathBuf},
  sync::Arc,
  thread,
};

use parking_lot::Mutex;
use wkv::{BfTreeService, RangeIndexManager as EngineRangeIndexManager};

/// 刷盘快照文件条目（C# EnumerateFlushFiles 的 (Path, Name, Address) 三元组）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushFileEntry {
  /// 完整文件路径
  pub path: PathBuf,
  /// 键哈希前缀（26 字符 Base32）
  pub key_hash: String,
  /// 文件名内嵌的 HybridLog 逻辑地址
  pub address: i64,
}

/// 范围索引管理器（会话域门面）
pub struct RangeIndexManager {
  /// 引擎管理器实例（liveIndexes / 条带锁 / 快照原语的真正持有者）
  engine: Arc<EngineRangeIndexManager>,
  /// 最近一次恢复的检查点令牌（C# recoveredCheckpointToken；供
  /// RebuildFromSnapshotIfPending 类恢复路径定位检查点快照）
  recovered_checkpoint_token: Mutex<Option<String>>,
}

impl RangeIndexManager {
  /// 创建管理器（C# 构造子：riLogRoot 必建，migration-tmp 清理重建由引擎承接）
  pub fn new(ri_log_root: impl Into<PathBuf>, cpr_dir: impl Into<PathBuf>) -> Self {
    Self::from_engine(Arc::new(EngineRangeIndexManager::new(ri_log_root, cpr_dir)))
  }

  /// 绑定既有引擎实例（wkv WedbStore::range_index 的会话域包装）
  pub fn from_engine(engine: Arc<EngineRangeIndexManager>) -> Self {
    Self {
      engine,
      recovered_checkpoint_token: Mutex::new(None),
    }
  }

  /// 引擎实例引用
  #[inline]
  pub fn engine(&self) -> &Arc<EngineRangeIndexManager> {
    &self.engine
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:TryClaimSnapshot
  ///
  /// 尝试认领该键树条目的快照防重入原子（0→1 CAS）。C# 调用方持有
  /// TreeEntry 引用；本门面按键寻径，条目不存在（无在线/挂起树）返回 false
  pub fn try_claim_snapshot(&self, key: &[u8]) -> bool {
    let key_id = EngineRangeIndexManager::key_id_of(key);
    self
      .engine
      .live_indexes()
      .pin()
      .get(&key_id)
      .is_some_and(|entry| entry.try_claim_snapshot())
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:ReleaseSnapshot
  ///
  /// 释放快照防重入原子（与 [`Self::try_claim_snapshot`] 配对）
  pub fn release_snapshot(&self, key: &[u8]) {
    let key_id = EngineRangeIndexManager::key_id_of(key);
    if let Some(entry) = self.engine.live_indexes().pin().get(&key_id).cloned() {
      entry.release_snapshot();
    }
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:LogDataPath
  ///
  /// {logRoot}/&lt;hash&gt;.data.bftree（工作文件）
  pub fn log_data_path(&self, hash_prefix: &str) -> PathBuf {
    self.engine.data_file_path(hash_prefix)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:LogFlushPath
  ///
  /// {logRoot}/&lt;hash&gt;.&lt;addr&gt;.flush.bftree（逐刷盘不可变快照）
  pub fn log_flush_path(&self, hash_prefix: &str, logical_address: i64) -> PathBuf {
    self.engine.log_flush_path(hash_prefix, logical_address)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:CheckpointSnapshotPath
  ///
  /// {cprDir}/&lt;token&gt;/rangeindex/&lt;hash&gt;.bftree
  pub fn checkpoint_snapshot_path(&self, hash_prefix: &str, checkpoint_token: &str) -> PathBuf {
    self
      .engine
      .checkpoint_snapshot_path(checkpoint_token, hash_prefix)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:CheckpointSnapshotDir
  ///
  /// 持有某检查点令牌全部 RI 快照的目录
  pub fn checkpoint_snapshot_dir(&self, checkpoint_token: &str) -> PathBuf {
    self
      .engine
      .checkpoint_snapshot_path(checkpoint_token, "")
      .parent()
      .map_or_else(PathBuf::new, Path::to_path_buf)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:RoundUpToPowerOf2
  ///
  /// 标准位运算上取 2 的幂（C# 逐位或折叠的等价实现）
  #[inline]
  pub const fn round_up_to_power_of2(v: u32) -> u32 {
    let mut x = v.wrapping_sub(1);
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x.wrapping_add(1)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:DisposeBfTreeDeferred
  ///
  /// 延迟释放 BfTree：C# 经 storeEpoch.BumpCurrentEpoch 把原生 Dispose 推到
  /// 全体在途读者越过纪元屏障之后；Rust 引擎无共享纪元可挂回调，改为后台
  /// 线程执行 `dispose_quiesced`（排空在途写者后释放），达成同等
  /// 「调用方不等释放、读者先于释放完成」语义
  pub fn dispose_bf_tree_deferred(&self, tree: Arc<BfTreeService>) {
    thread::spawn(move || {
      if let Err(e) = tree.dispose_quiesced() {
        log::warn!("Deferred dispose failed: {e}");
      }
    });
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:RegisterPending
  ///
  /// 登记挂起条目：data.bftree 已就位但无在线引擎实例，待首次访问惰性恢复。
  /// C# 拆为 PreStage（刷盘文件拷贝）+ RegisterPending（纯注册）两处调用；
  /// Rust 引擎合并为单原语 `pre_stage_and_register_pending`（须给定源刷盘
  /// 地址），无地址的纯注册路径由 `recover_all_trees_from_dir` 批量承接。
  /// 返回是否完成预置登记
  pub fn register_pending(&self, key: &[u8], src_flush_address: i64) -> Result<bool, String> {
    self
      .engine
      .pre_stage_and_register_pending(key, src_flush_address)
      .map(|()| true)
      .map_err(|e| e.to_string())
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:SetRecoveredCheckpointToken
  ///
  /// 记录最近一次恢复的检查点令牌
  pub fn set_recovered_checkpoint_token(&self, token: impl Into<String>) {
    *self.recovered_checkpoint_token.lock() = Some(token.into());
  }

  /// 读取最近一次恢复的检查点令牌
  pub fn recovered_checkpoint_token(&self) -> Option<String> {
    self.recovered_checkpoint_token.lock().clone()
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:EnumerateFlushFiles
  ///
  /// 枚举日志根目录下全部 &lt;hash&gt;.&lt;addr&gt;.flush.bftree，解析并返回
  /// 内嵌逻辑地址；不合命名模式的外来文件跳过。C# 为私有产出迭代器；
  /// Rust 经引擎复制枚举原语过滤刷盘文件类目（引擎侧同样只认合法命名）
  pub fn enumerate_flush_files(&self) -> Vec<FlushFileEntry> {
    self
      .engine
      .enumerate_files_for_replication("", i64::MIN, i64::MAX)
      .unwrap_or_default()
      .into_iter()
      .filter(|entry| entry.is_flush_file)
      .map(|entry| FlushFileEntry {
        path: entry.path,
        key_hash: entry.key_hash,
        address: entry.address,
      })
      .collect()
  }
}

#[cfg(test)]
mod tests {
  use std::fs;

  use tempfile::tempdir;
  use wkv::RangeIndexManager as Engine;

  use super::*;

  /// 26 字符 Base32 合法前缀（引擎命名解析器只认此形态）
  fn prefix_of(key: &[u8]) -> String {
    Engine::hash_prefix_of(key)
  }

  #[test]
  fn round_up_matches_std_next_power_of_two() {
    // v=0：C# uint 递减回绕语义返回 0（std 语义为 1），ComputeLeafPageSize
    // 调用面永不为 0，按 C# 原样承接
    assert_eq!(RangeIndexManager::round_up_to_power_of2(0), 0);
    // v≥1 全段扫描：与标准库逐点一致（边界 v=u32::MAX 处 std 溢出 panic，
    // 本实现按 C# 回绕语义返回 0，不在 std 对照集内）
    for v in [1u32, 2, 3, 4, 5, 1023, 1024, 1025, 4095, 65536, 0x7FFF_FFFF] {
      assert_eq!(
        RangeIndexManager::round_up_to_power_of2(v),
        v.next_power_of_two()
      );
    }
    // C# ComputeLeafPageSize 依赖此原语：2049 → 2.5x=5122 → 8192
    assert_eq!(Engine::compute_leaf_page_size(2049), 8192);
    assert_eq!(Engine::compute_leaf_page_size(2048), 4096);
  }

  #[test]
  fn path_helpers_follow_layout() {
    let dir = tempdir().unwrap();
    let mgr = RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr"));

    let prefix = prefix_of(b"key-a");
    assert_eq!(
      mgr.log_data_path(&prefix),
      dir.path().join("ri").join(format!("{prefix}.data.bftree"))
    );
    // 刷盘文件名 = 前缀 + '.' + 地址段 + ".flush.bftree"，与数据文件同根不同名
    let flush = mgr.log_flush_path(&prefix, 4096);
    assert_eq!(flush.parent(), mgr.log_data_path(&prefix).parent());
    let name = flush.file_name().unwrap().to_str().unwrap();
    assert!(
      name
        .strip_prefix(&prefix)
        .is_some_and(|rest| rest.starts_with('.') && rest.ends_with(".flush.bftree"))
    );
    // 不同地址 → 不同刷盘文件（地址段进入文件名）
    assert_ne!(flush, mgr.log_flush_path(&prefix, 8192));

    // 检查点快照目录 = 路径父目录，且路径落在其下
    let dir_cp = mgr.checkpoint_snapshot_dir("token-1");
    let path_cp = mgr.checkpoint_snapshot_path(&prefix, "token-1");
    assert_eq!(path_cp.parent(), Some(dir_cp.as_path()));
    assert!(path_cp.starts_with(&dir_cp));
    assert!(dir_cp.ends_with("rangeindex"));
  }

  #[test]
  fn try_claim_release_roundtrip_without_entry() {
    let dir = tempdir().unwrap();
    let mgr = RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr"));
    // 无条目：认领失败、释放静默
    assert!(!mgr.try_claim_snapshot(b"absent"));
    mgr.release_snapshot(b"absent");
  }

  #[test]
  fn try_claim_is_mutually_exclusive_until_released() {
    let dir = tempdir().unwrap();
    let mgr = RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr"));
    // 内存后端创建即注册在线条目（同步、无需存储设备）
    mgr
      .engine
      .create_bftree(
        b"idx",
        wkv::StorageBackend::Memory,
        wkv::TreeTuning::default(),
      )
      .unwrap();

    assert!(mgr.try_claim_snapshot(b"idx"));
    // 已认领期间他人无法认领
    assert!(!mgr.try_claim_snapshot(b"idx"));
    mgr.release_snapshot(b"idx");
    // 释放后可再次认领
    assert!(mgr.try_claim_snapshot(b"idx"));
    mgr.release_snapshot(b"idx");
  }

  #[test]
  fn recovered_checkpoint_token_roundtrip() {
    let dir = tempdir().unwrap();
    let mgr = RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr"));
    assert_eq!(mgr.recovered_checkpoint_token(), None);
    mgr.set_recovered_checkpoint_token("ckpt-42");
    assert_eq!(mgr.recovered_checkpoint_token().as_deref(), Some("ckpt-42"));
    mgr.set_recovered_checkpoint_token("ckpt-43");
    assert_eq!(mgr.recovered_checkpoint_token().as_deref(), Some("ckpt-43"));
  }

  #[test]
  fn enumerate_flush_files_parses_embedded_address() {
    let dir = tempdir().unwrap();
    let mgr = RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr"));
    let p1 = prefix_of(b"k1");
    let p2 = prefix_of(b"k2");

    // 经引擎路径原语落两个合法刷盘文件 + 一个外来文件
    let f1 = mgr.log_flush_path(&p1, 0x100);
    let f2 = mgr.log_flush_path(&p2, 0x200);
    fs::write(&f1, b"x").unwrap();
    fs::write(&f2, b"y").unwrap();
    fs::write(dir.path().join("ri").join("garbage.bftree"), b"z").unwrap();

    let mut files = mgr.enumerate_flush_files();
    files.sort_by_key(|f| f.address);
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].address, 0x100);
    assert_eq!(files[0].key_hash, p1);
    assert_eq!(files[1].address, 0x200);
    assert_eq!(files[1].key_hash, p2);
  }
}
