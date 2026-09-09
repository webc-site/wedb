//! 树实例生命周期：创建 / 惰性恢复 / 注册 / 注销 / 删除 / 迁移发布
//! (1:1 对标 Garnet CreateBfTree、RestoreTree、RegisterIndex、UnregisterIndex、DisposeTreeUnderLock、PublishMigratedIndex)

use std::{
  fs,
  path::{Path, PathBuf},
  sync::Arc,
};

use super::{RangeIndexManager, TreeEntry};
use crate::{
  error::{Error, Result},
  service::{BfTreeService, file_has_cpr_magic},
  stub::RangeIndexStub,
  types::{BfTreeConfig, StorageBackend, StorageBackendType, TreeTuning},
};

impl RangeIndexManager {
  /// 创建并注册全新的 BfTreeService (持有条带互斥写锁保证并发唯一性)
  pub fn create_bftree(
    &self,
    key: &[u8],
    storage_backend: StorageBackend,
    tuning: TreeTuning,
  ) -> Result<Arc<BfTreeService>> {
    let key_hash = Self::key_hash_of(key);
    let _stripe_lock = self.locks.write(key_hash);
    let key_id = Self::key_id_of(key);
    let hash_prefix = Self::base32_prefix_of(key);
    self.create_bftree_internal(key_id, key_hash, &hash_prefix, storage_backend, tuning)
  }

  /// 仅构建 BfTreeService 实例，不操作 live_indexes 字典
  fn instantiate_tree(
    &self,
    hash_prefix: &str,
    storage_backend: StorageBackend,
    tuning: TreeTuning,
  ) -> Result<Arc<BfTreeService>> {
    let mut config = BfTreeConfig::default();

    let (file_path_str, backend_type) = if storage_backend == StorageBackend::Memory {
      config.cache_only(true);
      (None, StorageBackendType::Memory)
    } else {
      // file_path 同时配置 Std 磁盘后端与数据文件路径
      let data_path = self.data_file_path(hash_prefix);
      config.file_path(&data_path);
      (
        Some(data_path.to_string_lossy().into_owned()),
        StorageBackendType::Disk,
      )
    };

    if tuning.cache_size > 0 {
      config.cb_size_byte(tuning.cache_size);
    }
    if tuning.min_record_size > 0 {
      config.cb_min_record_size(tuning.min_record_size);
    }
    if tuning.max_record_size > 0 {
      config.cb_max_record_size(tuning.max_record_size);
    }
    if tuning.max_key_len > 0 {
      config.cb_max_key_len(tuning.max_key_len);
    }

    let actual_leaf_page_size = if tuning.leaf_page_size > 0 {
      tuning.leaf_page_size
    } else if tuning.max_record_size > 0 {
      Self::compute_leaf_page_size(tuning.max_record_size)
    } else {
      0
    };
    if actual_leaf_page_size > 0 {
      config.leaf_page_size(actual_leaf_page_size);
    }
    config.use_snapshot(true);

    Ok(Arc::new(BfTreeService::new_with_backend(
      config,
      backend_type,
      file_path_str,
    )?))
  }

  fn create_bftree_internal(
    &self,
    key_id: u128,
    key_hash: u64,
    hash_prefix: &str,
    storage_backend: StorageBackend,
    tuning: TreeTuning,
  ) -> Result<Arc<BfTreeService>> {
    // 单次 pin 贯穿查重与注册：调用方已持条带写锁，同 key 并发被串行化
    let pin = self.live_indexes.pin();
    if pin.contains_key(&key_id) {
      return Err(Error::IndexExists);
    }

    let tree = self.instantiate_tree(hash_prefix, storage_backend, tuning)?;
    let entry = Arc::new(TreeEntry::new(
      Some(Arc::clone(&tree)),
      key_hash,
      key_id,
      hash_prefix.to_string(),
    ));

    pin.insert(key_id, entry);
    Ok(tree)
  }

  /// 获取或按需打开在线 BfTreeService (双重检查锁与条带锁保证并发安全性与恢复正确性)
  pub fn get_or_open_tree(&self, key: &[u8], stub: &RangeIndexStub) -> Result<Arc<BfTreeService>> {
    let key_hash = Self::key_hash_of(key);
    let _stripe_lock = self.locks.write(key_hash);
    let key_id = Self::key_id_of(key);

    {
      let pin = self.live_indexes.pin();
      if let Some(entry) = pin.get(&key_id) {
        let tree_guard = entry.tree.read();
        if let Some(t) = tree_guard.as_ref() {
          return Ok(Arc::clone(t));
        }
      }
    }

    let hash_prefix = Self::hash_prefix_of(key);
    let backend = StorageBackendType::from_u8(stub.storage_backend);
    let data_path = self.data_file_path(&hash_prefix);
    let flush_path = self.bare_flush_path(&hash_prefix);

    // 磁盘后端才存在可恢复的磁盘工件；内存后端的刷盘文件与工作文件均无意义
    //
    // 刷盘快照选择契约 (裸名与带地址两种命名对同一 hash_prefix 互斥，绝不混用)：
    // - 裸名 `{prefix}.flush.bftree` 由 [`on_flush`](super::flush) 体系产生，
    //   每次 fs::copy 截断覆盖，裸名存在即最新完整版本，直接采用；
    // - 带地址 `{prefix}.{addr:016x}.flush.bftree` 由
    //   [`on_flush_address`](super::flush) 体系产生，地址单调递增，
    //   取最大地址即最新版本 (旧文件由 on_truncate 按地址回收)。
    // 裸名优先于地址扫描并非版本偏好：裸名文件的存在本身即证明该树走 on_flush
    // 体系 (on_flush_address 体系下裸名文件绝不存在)，两者不同时出现，故不存在
    // 「裸名压过更新地址版本」的恢复错误。
    if backend == StorageBackendType::Disk {
      if flush_path.exists() {
        // 拷贝失败必须传播：静默吞掉会回退到陈旧/部分写入的 data.bftree，
        // 恢复出错误树版本 (1:1 对标 C# File.Copy 异常传播语义)
        fs::copy(&flush_path, &data_path)?;
      } else if self.addr_flush_scan_pending() {
        // O(目录条目数) 扫描被门控：常态 (无带地址刷盘文件) 下首例恢复证伪后，
        // 后续恢复走 O(1) stat 直达 data.bftree (时间复杂度优化，见字段文档)
        let mut found_flush = false;
        if let Ok(entries) = fs::read_dir(&self.ri_log_root) {
          let mut latest_candidate: Option<(i64, PathBuf)> = None;
          for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name_str) = path.file_name().and_then(|n| n.to_str())
              && let Some((prefix, addr)) = Self::parse_flush_file_name(name_str)
              && prefix == hash_prefix
              && latest_candidate
                .as_ref()
                .is_none_or(|(max_addr, _)| addr > *max_addr)
            {
              latest_candidate = Some((addr, path));
            }
          }
          if let Some((_, path)) = latest_candidate {
            fs::copy(path, &data_path)?;
            found_flush = true;
          }
        }
        if !found_flush {
          self.settle_addr_flush_scan();
        }
      }

      // 1:1 对标 C# RestoreTree：pre-stage 不变量保证 TreeHandle=0 的存根必有已预置的
      // data.bftree；缺失说明不变量被破坏（pre-stage 失败或文件被外部删除）。
      // 返回错误显式暴露数据丢失，绝不静默创建空树掩盖问题。
      if !data_path.exists() {
        let mut msg = String::from("数据文件缺失且无可用刷盘快照: ");
        msg.push_str(&data_path.display().to_string());
        return Err(Error::Recovery(msg));
      }
    }

    // 魔数预检统一走 file_has_cpr_magic：调引擎前拦截损坏文件，避免依赖 unwind。
    // 刻意不以 stub 后端门控 (1:1 对标 C# RestoreTree 一律 RecoverFromCprSnapshot)：
    // 检查点恢复流程会把 Memory 后端树的快照同样预置为 data.bftree，此时必须从
    // 快照恢复数据，而非按 Memory 语义新建空树丢失全部字段；stub 后端仅作为
    // 恢复实例的标签透传。
    let is_cpr = data_path.exists() && file_has_cpr_magic(&data_path);

    // 1:1 对标 Garnet RestoreTree: 如果磁盘上已存在数据文件且为快照，严格从快照恢复，否则以已有文件重新打开
    let tree = if is_cpr {
      Arc::new(BfTreeService::recover_from_cpr_snapshot(
        &data_path, true, backend,
      )?)
    } else {
      self.instantiate_tree(&hash_prefix, backend.into(), TreeTuning::from(stub))?
    };

    // 原地激活现有条目（如 pre_stage 或恢复阶段注册的 pending entry），或者注册全新条目
    let pin = self.live_indexes.pin();
    if let Some(entry) = pin.get(&key_id) {
      *entry.tree.write() = Some(Arc::clone(&tree));
    } else {
      let entry = Arc::new(TreeEntry::new(
        Some(Arc::clone(&tree)),
        key_hash,
        key_id,
        hash_prefix,
      ));
      pin.insert(key_id, entry);
    }
    Ok(tree)
  }

  /// 预分阶段复制并注册就绪条目 (1:1 对标 Garnet PreStageAndRegisterPending)
  ///
  /// 源刷盘快照缺失属于不变量破坏 (1:1 对标 C# 不变量 violation 处理)：绝不回退到其他
  /// 刷盘文件以免恢复出错误树版本，且不注册 pending 条目，让后续 get_or_open_tree
  /// 显式报错暴露数据丢失，而非静默恢复不正确的数据。
  pub fn pre_stage_and_register_pending(&self, key: &[u8], src_flush_address: i64) -> Result<()> {
    let key_hash = Self::key_hash_of(key);
    let _stripe_lock = self.locks.write(key_hash);
    let key_id = Self::key_id_of(key);
    let hash_prefix = Self::hash_prefix_of(key);
    let snapshot_path = self.log_flush_path(&hash_prefix, src_flush_address);
    // 复制接收端预置带地址刷盘文件，重新开启恢复扫描通道
    self.notice_addr_flush_files();
    if !snapshot_path.exists() {
      return Ok(());
    }
    let data_path = self.data_file_path(&hash_prefix);
    fs::copy(&snapshot_path, &data_path)?;

    let entry = Arc::new(TreeEntry::new(None, key_hash, key_id, hash_prefix));
    self.live_indexes.pin().insert(key_id, entry);
    Ok(())
  }

  /// 从内存字典移除条目并取走在线树实例
  ///
  /// 仅做注册表摘除 (调用方须已持条带写锁)；排空释放交由调用方在锁外执行——
  /// 排空时长取决于在途写者 (可能慢 I/O)，持锁排空会长时间阻塞同条带的
  /// 生命周期操作 (1:1 对标 C# DisposeTreeUnderLock 锁内移除、锁外 epoch 排空)。
  #[inline]
  fn remove_and_take_tree(&self, key_id: u128) -> Option<Arc<BfTreeService>> {
    let entry = self.live_indexes.pin().remove(&key_id)?.clone();
    entry.tree.write().take()
  }

  /// 注销并释放指定树条目 (1:1 对标 Garnet UnregisterIndex)
  ///
  /// 锁内摘除注册表条目 (并发恢复/快照立即不可见)，锁外屏障排空在途写者后
  /// 释放引擎实例；排空超时上抛且条目保持已移除态 (引擎句柄由 Arc 归零兜底)。
  pub fn unregister_index(&self, key: &[u8]) -> Result<bool> {
    let key_hash = Self::key_hash_of(key);
    let _stripe_lock = self.locks.write(key_hash);
    let Some(tree) = self.remove_and_take_tree(Self::key_id_of(key)) else {
      return Ok(false);
    };
    drop(_stripe_lock);
    tree.dispose_quiesced()?;
    Ok(true)
  }

  /// 删除指定索引并彻底清理磁盘文件 (1:1 对标 Garnet DisposeTreeUnderLock deleteFiles=true)
  ///
  /// 时序对标 C# DisposeAndDeleteFilesDeferred：锁内摘除条目 → 锁外屏障排空在途
  /// 写者并释放引擎实例 → 树静稳后才删除工作文件。绝不持锁删文件——那会与仍在
  /// 旧树上执行 insert 的写者撕裂 (写入已成功应答却落入正被 unlink 的 inode，
  /// 客户端收到成功而数据消失于进程内可见窗口之外的磁盘上)。刷盘快照文件保留，
  /// 由 [`on_truncate`](super::replication) 按日志地址回收 (与 C# 一致)。
  pub fn delete_index(&self, key: &[u8]) -> Result<bool> {
    let key_hash = Self::key_hash_of(key);
    let _stripe_lock = self.locks.write(key_hash);
    let Some(tree) = self.remove_and_take_tree(Self::key_id_of(key)) else {
      // 条目不存在也删残留工作文件：显式清理路径 (对标 C# TryDelete 不依赖条目存在)
      let data_path = self.data_file_path_for_key(key);
      if data_path.exists() {
        let _ = fs::remove_file(data_path);
      }
      return Ok(false);
    };
    drop(_stripe_lock);
    // 排空失败 (超时) 时保留数据文件：在途写者尚未静稳，删文件会产生撕裂写
    tree.dispose_quiesced()?;
    let data_path = self.data_file_path_for_key(key);
    if data_path.exists() {
      let _ = fs::remove_file(data_path);
    }
    Ok(true)
  }

  /// 销毁并释放指定树条目 (1:1 对标 Garnet DisposeTreeUnderLock)
  ///
  /// delete_file=true (DEL/UNLINK) 时同时删除工作文件 data.bftree (刷盘快照保留，
  /// 由 on_truncate 按日志地址回收)；false (淘汰) 时仅注销条目保留文件供惰性恢复
  pub fn dispose_tree(&self, key: &[u8], delete_file: bool) -> Result<bool> {
    if delete_file {
      self.delete_index(key)
    } else {
      self.unregister_index(key)
    }
  }

  /// 销毁并释放指定树条目，校验存根转移标志 (1:1 对标 Garnet DisposeTreeUnderLock)
  pub fn dispose_tree_under_lock(
    &self,
    key: &[u8],
    stub: &RangeIndexStub,
    delete_files: bool,
  ) -> Result<bool> {
    if !delete_files && stub.is_transferred() {
      return Ok(false);
    }
    self.dispose_tree(key, delete_files)
  }

  /// 注册已存在的 BfTreeService 实例到管理器中 (1:1 对标 Garnet RegisterIndex)
  ///
  /// 持有条带互斥写锁，与 RestoreTree / UnregisterIndex / 检查点快照等路径串行化
  pub fn register_tree(&self, key: &[u8], tree: Arc<BfTreeService>) {
    let key_hash = Self::key_hash_of(key);
    let _stripe_lock = self.locks.write(key_hash);
    let key_id = Self::key_id_of(key);
    let pin = self.live_indexes.pin();
    if let Some(existing) = pin.get(&key_id) {
      *existing.tree.write() = Some(tree);
    } else {
      let hash_prefix = Self::hash_prefix_of(key);
      let entry = Arc::new(TreeEntry::new(Some(tree), key_hash, key_id, hash_prefix));
      pin.insert(key_id, entry);
    }
  }

  /// 迁移发布：把临时快照文件原子换入数据路径，恢复引擎实例并注册 (1:1 对标
  /// Garnet PublishMigratedIndex 的文件换入/树恢复/注册表部分)
  ///
  /// ⚠️ 调用方须已持该键的条带互斥写锁 (对标 C# *UnderLock 契约)——发布全流程
  /// (存在性判定 → 旧树排空 → 文件换入 → 恢复 → 注册) 必须对同键并发发布原子，
  /// 锁由调用方持有并覆盖其后续的存根元数据落盘。
  ///
  /// 时序说明：
  /// - Unix 上 `rename` 原子替换既有数据文件，换入窗口内无「文件缺失」间隙；
  ///   rename 失败 (Windows 同名冲突/跨设备) 回退 remove+copy，仅该回退路径
  ///   存在非原子窗口，失败即上抛；
  /// - replace 时旧树在锁内排空释放：在途写者 (持条带读锁 + 写者微守卫) 不依赖
  ///   写锁退出，无死锁，至多阻塞同条带新写者一个 insert 时长；
  /// - 快照文件缺失属不变量破坏，显式上抛，绝不静默注册空树。
  pub fn publish_tree_from_snapshot_locked(
    &self,
    key: &[u8],
    snapshot_path: &Path,
    replace: bool,
  ) -> Result<Arc<BfTreeService>> {
    let key_hash = Self::key_hash_of(key);
    let key_id = Self::key_id_of(key);
    let hash_prefix = Self::hash_prefix_of(key);
    let data_path = self.data_file_path(&hash_prefix);

    // 锁内复查注册表：并发发布已被调用方条带锁串行化，此处是最终裁决点
    let exists = self.live_indexes.pin().contains_key(&key_id);
    if exists && !replace {
      return Err(Error::IndexExists);
    }
    if exists {
      // 旧树锁内摘除 + 排空释放 (remove_and_take_tree 契约：调用方持条带写锁)
      if let Some(old) = self.remove_and_take_tree(key_id) {
        old.dispose_quiesced()?;
      }
    }

    if !snapshot_path.exists() {
      let mut msg = String::from("迁移快照文件不存在: ");
      msg.push_str(&snapshot_path.display().to_string());
      return Err(Error::Recovery(msg));
    }
    if let Some(parent) = data_path.parent() {
      fs::create_dir_all(parent)?;
    }
    if fs::rename(snapshot_path, &data_path).is_err() {
      // 回退路径 (平台/跨设备)：非原子，失败即上抛保持盘上自洽
      if data_path.exists() {
        fs::remove_file(&data_path)?;
      }
      fs::copy(snapshot_path, &data_path)?;
      let _ = fs::remove_file(snapshot_path);
    }

    let tree = Arc::new(BfTreeService::recover_from_cpr_snapshot(
      &data_path,
      true,
      StorageBackendType::Disk,
    )?);
    let entry = Arc::new(TreeEntry::new(
      Some(Arc::clone(&tree)),
      key_hash,
      key_id,
      hash_prefix,
    ));
    self.live_indexes.pin().insert(key_id, entry);
    Ok(tree)
  }
}
