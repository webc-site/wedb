//! 刷盘文件枚举、日志截断回收与检查点全量恢复
//! (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:EnumerateFilesForReplication / OnTruncateImpl / RecoverAllTreesFromCheckpoint)

use std::{
  fs,
  path::{Path, PathBuf},
  str,
  sync::Arc,
};

use wbase::base32::{decode_u64, decode_u128, encode_u128, is_base32};

use super::{HASH_PREFIX_LEN, RangeIndexFileEntry, RangeIndexManager, TreeEntry};
use crate::error::Result;

impl RangeIndexManager {
  /// 解析刷盘快照文件名 `{hash_prefix}.{logical_address_b32}.flush.bftree`
  ///
  /// 严格校验：前缀 26 位 Base32、地址段恰好 13 位 Base32，确保不误选/误删外来文件
  #[inline]
  pub(super) fn parse_flush_file_name(file_name: &str) -> Option<(&str, i64)> {
    let rest = file_name.strip_suffix(".flush.bftree")?;
    let (prefix, addr_str) = rest.rsplit_once('.')?;
    if prefix.len() != HASH_PREFIX_LEN || !is_base32(prefix) {
      return None;
    }
    let addr = decode_u64(addr_str)? as i64;
    Some((prefix, addr))
  }

  /// 日志截断清理：删除逻辑地址小于 new_begin_address 的历史刷盘快照文件 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:OnTruncateImpl)
  pub fn on_truncate(&self, new_begin_address: i64) -> Result<()> {
    if !self.ri_log_root.exists() {
      return Ok(());
    }

    for entry in fs::read_dir(&self.ri_log_root)? {
      let entry = entry?;
      let path = entry.path();
      if let Some(file_name) = path.file_name().and_then(|n| n.to_str())
        && let Some((_prefix, addr)) = Self::parse_flush_file_name(file_name)
        && addr < new_begin_address
      {
        let _ = fs::remove_file(&path);
      }
    }

    Ok(())
  }

  /// 收集指定检查点与 HybridLog 地址范围内需要进行主从复制的文件 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:EnumerateFilesForReplication)
  pub fn enumerate_files_for_replication(
    &self,
    checkpoint_token: &str,
    hlog_start_address: i64,
    hlog_end_address: i64,
  ) -> Result<Vec<RangeIndexFileEntry>> {
    // 结果集不做容量预估：两个来源均经 fs::read_dir 流式枚举，Unix 目录流不提供
    // 前置条目数，预估只能拍脑袋；且复制窗口内的刷盘文件受 on_truncate 按地址
    // 回收约束，规模天然有界小，Vec 倍增摊销成本可忽略 (对标 C# List{} 无容量版本)
    let mut result = Vec::new();

    // 1. 扫描 ri_log_root 下的 *.flush.bftree 文件
    if self.ri_log_root.exists() {
      for entry in fs::read_dir(&self.ri_log_root)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
          continue;
        };
        if let Some((prefix, addr)) = Self::parse_flush_file_name(name)
          && addr >= hlog_start_address
          && addr < hlog_end_address
        {
          result.push(RangeIndexFileEntry {
            path: entry.path(),
            key_hash: prefix.to_string(),
            address: addr,
            is_flush_file: true,
          });
        }
      }
    }

    // 2. 扫描 cpr_dir/<token>/rangeindex/*.bftree 检查点快照
    let snapshot_dir = self.cpr_dir.join(checkpoint_token).join("rangeindex");
    if snapshot_dir.exists() {
      for entry in fs::read_dir(&snapshot_dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
          continue;
        };
        if let Some(stem) = name.strip_suffix(".bftree")
          // 快照文件名固定为 26 位 Base32 前缀，跳过外来文件
          && stem.len() == HASH_PREFIX_LEN
          && is_base32(stem)
        {
          result.push(RangeIndexFileEntry {
            path: entry.path(),
            key_hash: stem.to_string(),
            address: 0,
            is_flush_file: false,
          });
        }
      }
    }

    Ok(result)
  }

  /// 便捷别名：获取需要进行主从复制的文件路径列表 (1:1 对标 get_replication_file_names)
  #[inline]
  pub fn get_replication_file_names(
    &self,
    checkpoint_token: &str,
    hlog_start_address: i64,
    hlog_end_address: i64,
  ) -> Result<Vec<PathBuf>> {
    let entries = self.enumerate_files_for_replication(
      checkpoint_token,
      hlog_start_address,
      hlog_end_address,
    )?;
    Ok(entries.into_iter().map(|e| e.path).collect())
  }

  /// 从指定检查点全量恢复所有 BfTree 索引 (1:1 对标 Garnet RecoverAllTreesFromCheckpoint)
  pub fn recover_all_trees_from_checkpoint(&self, checkpoint_token: &str) -> Result<()> {
    self.recover_all_trees_from_dir(&self.cpr_dir, checkpoint_token)?;
    Ok(())
  }

  /// 从指定目标目录全量恢复所有 BfTree 索引至 ri_log_root 并注册 pending 条目 (支持多候选路径容错)
  ///
  /// 与 C# RecoverAllTreesFromCheckpoint / RebuildFromSnapshotIfPending 语义对齐：
  /// 仅做文件预置 (fs::copy 覆盖 data.bftree) + 注册 tree=None 的 pending 条目，
  /// 引擎实例一律由首次访问的 get_or_open_tree 惰性恢复——急切 open 会为每棵树
  /// 分配完整环形缓冲区，RI 键规模大时启动内存与耗时不可控 (C# 同样不在此处开树)。
  ///
  /// 与 C# RecoverAllTreesFromCheckpoint 的差异：C# 恢复期由 OnRecoverySnapshotRead
  /// 逐 stub 触发 (持有原始 key，keyHash 精确派生，单文件失败可 log 后继续)；本实现
  /// 按目录枚举快照文件批量预置 (wedb 恢复流程无主日志逐 stub 回放，见 wkv
  /// checkpoint.rs)，单文件失败即整批上抛——静默跳过等于恢复后缺树运行，宁可启动
  /// 恢复显式失败。
  ///
  /// 正确性要点：
  /// - 每个文件在 stem 派生的条带写锁内以 try_insert 注册：条目已存在 (pending
  ///   预置 / get_or_open_tree 已激活 / 前一轮恢复) 时不覆盖，杜绝双开同一数据
  ///   文件的引擎实例。恢复流程契约由上层保证在服务对外前单线程执行；与运行期
  ///   get_or_open_tree (fast_hash(key) 条带) 分属不同条带时，try_insert 的
  ///   失败即拒绝语义兜底不产生覆盖。
  /// - key_hash 由 stem (key_id Base32 编码) 派生而非 fast_hash(原始 key)：恢复期
  ///   只有文件名，原始 key 不可得。条带锁只要求「同一 key 的并发路径派生同值」
  ///   ——stem 是 key_id 的确定性函数，同一 key 恒定落同一条带，锁分段成立；
  ///   键路由由 key_id (字典键) 承担，key_hash 不参与数据寻址。
  pub fn recover_all_trees_from_dir(
    &self,
    target_dir: &Path,
    checkpoint_token: &str,
  ) -> Result<usize> {
    let candidate_dirs = [
      target_dir.join(checkpoint_token).join("rangeindex"),
      target_dir.join("rangeindex"),
      self.cpr_dir.join(checkpoint_token).join("rangeindex"),
      self.cpr_dir.join("rangeindex"),
    ];

    let mut staged_count = 0;
    for snapshot_dir in &candidate_dirs {
      if !snapshot_dir.exists() {
        continue;
      }
      if let Ok(entries) = fs::read_dir(snapshot_dir) {
        // 单次 pin 贯穿目录内全部文件注册 (papaya epoch guard 一次获取，免逐文件重入)
        let pin = self.live_indexes.pin();
        for entry in entries.flatten() {
          let path = entry.path();
          if path.extension().is_some_and(|ext| ext == "bftree")
            && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
            // 快照文件名固定为 26 位 Base32 前缀，跳过外来文件避免 key_id 误注册
            && let Some(key_id) = decode_u128(stem)
          {
            let target_data_path = self.data_file_path(stem);
            // 无条件以检查点快照预置工作文件：文件名恒不同 ({stem}.bftree vs
            // {stem}.data.bftree)，`path != target` 恒真——工作文件可能仅有环形
            // 缓冲中未落盘的部分页，快照才是恢复点权威版本，存在也必须覆盖
            // (`path == target` 分支仅防御 fs::copy 自拷贝，按命名规则不可达)
            if !target_data_path.exists() || target_data_path != path {
              fs::copy(&path, &target_data_path)?;
            }

            // 持 stem 条带写锁注册，与并发恢复轮次串行化
            let key_hash = Self::key_hash_of(stem.as_bytes());
            let _stripe_lock = self.locks.write(key_hash);

            // 已注册 (pending 预置 / 已激活 / 前一轮恢复) 则不覆盖，
            // 避免同一数据文件被重复登记或双开引擎实例
            if pin.contains_key(&key_id) {
              continue;
            }
            // 仅注册 pending 条目 (tree=None)，引擎实例交给 get_or_open_tree 惰性恢复
            // (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:RebuildFromSnapshotIfPending 只预置不开树)。
            // 前缀取 stem 解码出的 key_id 再规范编码：stem 本就是 key_id 的 Base32
            // 规范编码 (检查点文件名恒为小写)，round-trip 恒等且零堆分配 (对标 C#
            // 直接截取文件名前缀 name[..HashPrefixLength])
            let tree_entry = Arc::new(TreeEntry::new(None, key_hash, key_id, encode_u128(key_id)));
            // try_insert：锁内 contains 与插入间唯一竞争方是 fast_hash(原始 key)
            // 条带的 get_or_open_tree (恢复期契约排除)，失败即拒绝兜底不覆盖
            if pin.try_insert(key_id, tree_entry).is_ok() {
              staged_count += 1;
            }
          }
        }
      }
      if staged_count > 0 {
        break;
      }
    }

    Ok(staged_count)
  }
}
