//! 树实例生命周期：创建 / 惰性恢复 / 注册 / 注销 / 删除 / 迁移发布
//! (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:CreateBfTree、RestoreTree、RegisterIndex、UnregisterIndex、DisposeTreeUnderLock、PublishMigratedIndex)

use std::{
  ffi::OsString,
  fs,
  path::{Path, PathBuf},
  sync::Arc,
};

use wbase::base32::Base32Buf128;

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
    self.create_bftree_internal(key_id, key_hash, hash_prefix, storage_backend, tuning)
  }

  /// 根据 max_record_size 自动推导并补齐未指定的叶子页大小
  #[inline]
  pub fn resolve_tuning(tuning: &mut TreeTuning) {
    if tuning.leaf_page_size == 0 && tuning.max_record_size > 0 {
      tuning.leaf_page_size = Self::compute_leaf_page_size(tuning.max_record_size);
    }
  }

  /// 仅构建 BfTreeService 实例，不操作 live_indexes 字典
  fn instantiate_tree(
    &self,
    hash_prefix: &str,
    storage_backend: StorageBackend,
    mut tuning: TreeTuning,
  ) -> Result<Arc<BfTreeService>> {
    Self::resolve_tuning(&mut tuning);
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
      config.cb_max_record_size(tuning.max_record_size + 1);
    }
    if tuning.max_key_len > 0 {
      config.cb_max_key_len(tuning.max_key_len + 1);
    }

    if tuning.leaf_page_size > 0 {
      config.leaf_page_size(tuning.leaf_page_size);
    }
    config.use_snapshot(true);
    config.scan_promotion_rate(0);

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
    hash_prefix: Base32Buf128,
    storage_backend: StorageBackend,
    tuning: TreeTuning,
  ) -> Result<Arc<BfTreeService>> {
    // 单次 pin 贯穿查重与注册：调用方已持条带写锁，同 key 并发被串行化
    let pin = self.live_indexes.pin();
    if pin.contains_key(&key_id) {
      return Err(Error::IndexExists);
    }

    // 清理旧世代磁盘工件：注册表无条目时数据文件与刷盘快照必无在线引擎引用
    // (条带锁内裁决，pending 条目已在上方 IndexExists 拦截)。引擎以
    // create(true)+truncate(false) 打开基文件，残留的旧内容 (崩溃残留 / 上轮
    // 删除未竟) 会被全新索引静默继承，以幻影数据暴露给新索引；同理，前缀寻址
    // 恢复 (存根无逻辑地址，与 C# 按记录地址 PreStage 不同) 无法区分世代，
    // 删除后残留的旧世代刷盘快照会被惰性恢复误选为新世代的恢复来源——两类
    // 工件一并 unlink，保证新树从干净的数据文件与恢复源起步。
    let _ = fs::remove_file(self.data_file_path(&hash_prefix));
    let _ = fs::remove_file(self.bare_flush_path(&hash_prefix));
    self.remove_addr_flush_files(key_id);

    let tree = self.instantiate_tree(&hash_prefix, storage_backend, tuning)?;
    let entry = Arc::new(TreeEntry::new(Some(Arc::clone(&tree)), key_hash, key_id));

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

    // 内联 Base32 前缀贯穿整个恢复路径：路径拼接经 Deref 走 &str，注册条目零克隆
    let hash_prefix = Self::base32_prefix_of(key);
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
    //
    // IsRecovered 存根绕过刷盘快照 (1:1 对齐 C#：recovered 存根仅经 RestoreTree
    // 打开 data.bftree——检查点恢复已把权威快照预置其中，刷盘文件仅供 IsFlushed
    // 存根经 PreStage 按地址消费)：检查点快照必然新于其之前产生的任何刷盘文件，
    // 若仍让刷盘文件覆盖，恢复会回退到检查点之前的旧世代状态。
    if backend == StorageBackendType::Disk && !stub.is_recovered() {
      if flush_path.exists() {
        // 拷贝失败必须传播：静默吞掉会回退到陈旧/部分写入的 data.bftree，
        // 恢复出错误树版本 (1:1 对标 C# File.Copy 异常传播语义)
        fs::copy(&flush_path, &data_path)?;
      } else if self.addr_flush_scan_pending() {
        // O(目录条目数) 扫描被门控：常态 (无带地址刷盘文件) 下首例恢复证伪后，
        // 后续恢复走 O(1) stat 直达 data.bftree (时间复杂度优化，见字段文档)
        let scan_token = self.addr_flush_scan_token();
        let mut found_flush = false;
        if let Ok(entries) = fs::read_dir(&self.ri_log_root) {
          // 只跟踪胜出文件名：赢家路径 join 一次，N 条目录项从 N 次 PathBuf 拼接降为 1 次
          let mut latest: Option<(u64, OsString)> = None;
          for entry in entries.flatten() {
            let name = entry.file_name();
            if let Some(name_str) = name.to_str()
              && let Some((file_key_id, addr)) = Self::parse_flush_file_name(name_str)
              && file_key_id == key_id
              && latest.as_ref().is_none_or(|(max_addr, _)| addr > *max_addr)
            {
              latest = Some((addr, name));
            }
          }
          if let Some((_, name)) = latest {
            fs::copy(self.ri_log_root.join(name), &data_path)?;
            found_flush = true;
          }
        }
        if !found_flush {
          self.settle_addr_flush_scan(scan_token);
        }
      }
    }

    // 1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:RestoreTree：pre-stage 不变量保证 TreeHandle=0 的存根必有已预置的
    // data.bftree；缺失说明不变量被破坏（pre-stage 失败或文件被外部删除）。
    // 返回错误显式暴露数据丢失，绝不静默创建空树掩盖问题 (recovered 存根同样
    // 受此约束——检查点预置失败绝不能退化为静默空树)。
    if backend == StorageBackendType::Disk && !data_path.exists() {
      use core::fmt::Write;
      let mut msg = String::with_capacity(48 + data_path.as_os_str().len());
      let _ = write!(msg, "数据文件缺失且无可用刷盘快照: {}", data_path.display());
      return Err(Error::Recovery(msg));
    }

    // 魔数预检统一走 file_has_cpr_magic：调引擎前拦截损坏文件，避免依赖 unwind。
    // 磁盘后端上 data.bftree 存在性已由上方不变量检查保证，仅内存后端需 stat 探测
    // (省一次冗余 stat)。
    // 刻意不以 stub 后端门控 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:RestoreTree 一律 RecoverFromCprSnapshot)：
    // 检查点恢复流程会把 Memory 后端树的快照同样预置为 data.bftree，此时必须从
    // 快照恢复数据，而非按 Memory 语义新建空树丢失全部字段；stub 后端仅作为
    // 恢复实例的标签透传。
    let is_cpr =
      (backend == StorageBackendType::Disk || data_path.exists()) && file_has_cpr_magic(&data_path);

    // 1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:RestoreTree: 如果磁盘上已存在数据文件且为快照，严格从快照恢复，否则以已有文件重新打开
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
      let entry = Arc::new(TreeEntry::new(Some(Arc::clone(&tree)), key_hash, key_id));
      pin.insert(key_id, entry);
    }
    Ok(tree)
  }

  /// 注册 pending 条目：磁盘上的 data.bftree 已有正确内容，但原生 BfTree 尚未打开。
  /// 由后续 RestoreTree 发起的 RegisterIndex 激活。
  ///
  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:RegisterPending
  ///
  /// try_insert 原子注册 (1:1 对标 C# `liveIndexes.TryAdd`——"Add only if no entry
  /// exists")：已存在条目 (在线激活或 pending) 保持原样，杜绝 contains+insert
  /// 窗口内并发注册的相互覆盖
  ///
  /// 返回值：是否由**本次调用**完成登记 (false = 条目已存在，原样保留)。C# 对
  /// TryAdd 结果直接丢弃 (`_ =`)，此处显式暴露供 wkv 恢复计数使用——false 不代表
  /// 失败，更不意味着‘该键未注册’
  pub fn register_pending(&self, key_bytes: &[u8]) -> bool {
    let key_id = Self::key_id_of(key_bytes);
    let key_hash = Self::key_hash_of(key_bytes);
    let pending = Arc::new(TreeEntry::new(None, key_hash, key_id));
    self.live_indexes.pin().try_insert(key_id, pending).is_ok()
  }

  /// 预分阶段复制并注册就绪条目 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:PreStageAndRegisterPending)
  ///
  /// 源刷盘快照缺失属于不变量破坏 (1:1 对标 C# 不变量 violation 处理)：绝不回退到其他
  /// 刷盘文件以免恢复出错误树版本，且不注册 pending 条目，让后续 get_or_open_tree
  /// 显式报错暴露数据丢失，而非静默恢复不正确的数据。
  ///
  /// 注册走 try_insert (1:1 对标 C# `liveIndexes.TryAdd`)：已有条目 (在线激活或
  /// pending)一律保持原样——无条件覆盖会把并发恢复激活的在线树条目换成 pending，
  /// 导致同一数据文件被二次打开引擎实例。
  ///
  /// 返回值：`Ok(())` 仅表示无 I/O 错误，**不**代表‘已完成登记’——源缺失的
  /// no-op 与条目已存在时的 TryAdd 落空同样返回 `Ok(())`，登记与否以 TryAdd 语义
  /// 为准 (C# 同样丢弃 TryAdd 结果，仅以 ERROR 日志区分源缺失)
  pub fn pre_stage_and_register_pending(&self, key: &[u8], src_flush_address: u64) -> Result<()> {
    let key_hash = Self::key_hash_of(key);
    let _stripe_lock = self.locks.write(key_hash);
    let key_id = Self::key_id_of(key);
    let hash_prefix = Self::base32_prefix_of(key);
    let snapshot_path = self.log_flush_path(&hash_prefix, src_flush_address);
    if !snapshot_path.exists() {
      return Ok(());
    }
    let data_path = self.data_file_path(&hash_prefix);
    fs::copy(&snapshot_path, &data_path)?;
    // 预置完成后重开带地址刷盘文件扫描通道。notice 后置于文件落盘 (见
    // manager 模块 addr_flush_gen 字段文档的竞态闭环)：先 notice 后拷贝会让
    // 扫描在 notice 之后、拷贝完成之前证伪封存通道，随后就绪的文件被永久跳过
    self.notice_addr_flush_files();

    let entry = Arc::new(TreeEntry::new(None, key_hash, key_id));
    if self.live_indexes.pin().try_insert(key_id, entry).is_err() {
      log::warn!("BfTree 生命周期预置快照条目已存在: key_id={key_id}");
    }
    Ok(())
  }

  /// 从内存字典移除条目并取走在线树实例
  ///
  /// 仅做注册表摘除 (调用方须已持条带写锁)；排空释放交由调用方在锁外执行——
  /// 排空时长取决于在途写者 (可能慢 I/O)，持锁排空会长时间阻塞同条带的
  /// 生命周期操作 (对应 DisposeTreeUnderLock 锁内移除、锁外 epoch 排空)。
  #[inline]
  fn remove_and_take_tree(&self, key_id: u128) -> Option<Arc<BfTreeService>> {
    let entry = self.live_indexes.pin().remove(&key_id)?.clone();
    entry.tree.write().take()
  }

  /// 延迟释放 BfTree 与删除数据文件 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeAndDeleteFilesDeferred)
  ///
  /// C# 局部函数 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:TryDelete
  /// (先 dispose 后删文件的容错单文件删除) 在此内联为 `fs::remove_file` 忽略错误
  fn dispose_and_delete_files_deferred(
    tree: Option<Arc<BfTreeService>>,
    data_path: Option<PathBuf>,
  ) {
    if let Some(tree) = tree {
      tree.dispose();
    }
    if let Some(path) = data_path
      && path.exists()
    {
      let _ = fs::remove_file(path);
    }
  }

  /// 销毁并释放指定树条目 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock)
  ///
  /// 在条带独占写锁保护下从注册表摘除条目；随后经 storeEpoch.bump_current_epoch_action 挂入
  /// 延迟清理队列 (无纪元时回退为同步清理)。读写者在纪元保护下安全退出后，底层引擎与文件方才真正释放。
  pub fn dispose_tree_under_lock(&self, key: &[u8], delete_file: bool) -> Result<bool> {
    let key_hash = Self::key_hash_of(key);
    let _stripe_lock = self.locks.write(key_hash);
    let key_id = Self::key_id_of(key);
    let tree = self.remove_and_take_tree(key_id);
    let data_path = if delete_file {
      let p = self.data_file_path_for_key(key);
      (tree.is_some() || p.exists()).then_some(p)
    } else {
      None
    };
    drop(_stripe_lock);

    if tree.is_none() && data_path.is_none() {
      return Ok(false);
    }

    if let Some(ref epoch) = self.store_epoch {
      epoch.bump_current_epoch_action(move || {
        Self::dispose_and_delete_files_deferred(tree, data_path);
      });
    } else {
      Self::dispose_and_delete_files_deferred(tree, data_path);
    }
    Ok(true)
  }

  /// 注销并释放指定树条目 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:UnregisterIndex)
  pub fn unregister_index(&self, key: &[u8]) -> Result<bool> {
    self.dispose_tree_under_lock(key, false)
  }

  /// 删除指定索引并彻底清理磁盘文件（对应 DisposeTreeUnderLock deleteFiles=true 分支）
  pub fn delete_index(&self, key: &[u8]) -> Result<bool> {
    self.dispose_tree_under_lock(key, true)
  }

  /// 注册已存在的 BfTreeService 实例到管理器中 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:RegisterIndex)
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
      let entry = Arc::new(TreeEntry::new(Some(tree), key_hash, key_id));
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
    let hash_prefix = Self::base32_prefix_of(key);
    let data_path = self.data_file_path(&hash_prefix);

    // 锁内复查注册表：并发发布已被调用方条带锁串行化，此处是最终裁决点
    let exists = self.live_indexes.pin().contains_key(&key_id);
    if exists && !replace {
      return Err(Error::IndexExists);
    }
    if exists {
      // 旧树锁内摘除 + 延迟释放 (remove_and_take_tree 契约：调用方持条带写锁)
      if let Some(old) = self.remove_and_take_tree(key_id) {
        self.dispose_bf_tree_deferred(old);
      }
    }

    if !snapshot_path.exists() {
      use core::fmt::Write;
      let mut msg = String::with_capacity(32 + snapshot_path.as_os_str().len());
      let _ = write!(msg, "迁移快照文件不存在: {}", snapshot_path.display());
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
    let entry = Arc::new(TreeEntry::new(Some(Arc::clone(&tree)), key_hash, key_id));
    self.live_indexes.pin().insert(key_id, entry);
    Ok(tree)
  }
}
