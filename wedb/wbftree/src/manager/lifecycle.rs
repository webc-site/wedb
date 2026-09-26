//! 树实例生命周期：创建 / 惰性恢复 / 注册 / 注销 / 删除 / 迁移发布
//! (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:CreateBfTree、RestoreTree、RegisterIndex、DisposeTreeUnderLock、PublishMigratedIndex)

use std::{
  fs::{self, File},
  io,
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  thread,
};

use bf_tree::{Config, StorageBackend};
use wbase::base32::Base32Buf128;
use whasher::fast_hash;

use super::{DetachedTree, RangeIndexManager, TreeEntry};
use crate::{
  error::{Error, Result},
  service::{BfTreeService, file_has_cpr_magic},
  stub::RangeIndexStub,
  types::{StorageBackendType, TreeTuning},
};

/// 发布故障注入钩子（仅测试用，一处定义）：置真后**下一次**
/// [`RangeIndexManager::publish_tree_from_snapshot_locked`] 即返回
/// [`Error::Recovery`]（一次性，消费即自动复位，生产路径恒假零开销）。供宿主
/// 验证「升阶 / 重灌换入失败 = 旧树原样在位、树内容零变更」的分级语义——
/// 分层臂 WATCH 栅栏须复位脏标记，杜绝假推进误夭折并发 WATCH 事务
#[cfg(debug_assertions)]
pub static PUBLISH_FAIL_INJECT: AtomicBool = AtomicBool::new(false);

/// 删除故障注入钩子（仅测试用，一处定义）：置真后**下一次**
/// [`RangeIndexManager::delete_index`] 即返回 [`Error::Io`]（一次性，消费即
/// 自动复位，生产路径恒假零开销）。供宿主验证「排空树注销失败时元记录墓碑
/// 已落盘 = 键已死、树物理面已变更」的分级语义——分层臂 WATCH 栅栏保持
/// 置脏真实推进
#[cfg(debug_assertions)]
pub static DELETE_INDEX_FAIL_INJECT: AtomicBool = AtomicBool::new(false);

/// 惰性恢复/重开暂停故障注入钩子（仅测试用，一处定义）：置真且目标键哈希匹配时，
/// **下一次** [`RangeIndexManager::get_or_open_tree`] 在打开并预留配额后暂停，
/// 供宿主/测试验证并发清库/换号时幽灵树回滚与预算归还契约
#[cfg(debug_assertions)]
pub static GET_OR_OPEN_PAUSE_INJECT: AtomicBool = AtomicBool::new(false);
#[cfg(debug_assertions)]
pub static GET_OR_OPEN_PAUSE_KEY_HASH: AtomicU64 = AtomicU64::new(0);
#[cfg(debug_assertions)]
pub static GET_OR_OPEN_PAUSED: AtomicBool = AtomicBool::new(false);
#[cfg(debug_assertions)]
pub static GET_OR_OPEN_RESUME: AtomicBool = AtomicBool::new(false);

impl RangeIndexManager {
  /// 创建并注册全新的 BfTreeService (持有条带互斥写锁保证并发唯一性，
  /// 对标 C# CreateBfTree 以 StorageBackendType 直达引擎构造；`id_key` 域语义见
  /// [`RangeIndexManager::key_id_of`] 身份契约)
  pub fn create_bftree(
    &self,
    id_key: &[u8],
    storage_backend: StorageBackendType,
    tuning: TreeTuning,
  ) -> Result<Arc<BfTreeService>> {
    let key_hash = fast_hash(id_key);
    let _stripe_lock = self.locks.write(key_hash);
    let key_id = Self::key_id_of(id_key);
    let hash_prefix = Self::base32_prefix_of(id_key);
    self.create_bftree_internal(key_id, key_hash, hash_prefix, storage_backend, tuning)
  }

  /// 根据 max_record_size 自动推导并补齐未指定的叶子页大小
  #[inline]
  pub fn resolve_tuning(tuning: &mut TreeTuning) {
    if tuning.leaf_page_size == 0 && tuning.max_record_size > 0 {
      tuning.leaf_page_size = Self::compute_leaf_page_size(tuning.max_record_size);
    }
  }

  /// 仅构建 BfTreeService 实例，不操作 live_indexes 字典。
  ///
  /// `data_path` 为 `Some` 即以 Std 磁盘后端在该显式路径打开工作文件（正式数据路径经
  /// [`Self::data_file_path`] 派生，升阶 scratch 树经 [`Self::derive_temp_migration_path`]
  /// 落在 migration-tmp）；为 `None` 即纯内存后端 (cache_only，零磁盘工件)。路径由
  /// 调用方给出，本构造口不内嵌命名规则——正式建树与升阶 scratch 建树共用唯一构造口。
  fn instantiate_tree(
    &self,
    data_path: Option<&Path>,
    storage_backend: StorageBackendType,
    mut tuning: TreeTuning,
  ) -> Result<Arc<BfTreeService>> {
    Self::resolve_tuning(&mut tuning);
    let mut config = Config::default();

    let (file_path_str, backend_type) = match (data_path, storage_backend) {
      (Some(data_path), StorageBackendType::Disk) => {
        // 显式配置 Std 磁盘后端与数据文件路径 (bf_tree Config::file_path 不联动后端)
        config.storage_backend(StorageBackend::Std);
        config.file_path(data_path);
        (
          Some(data_path.to_string_lossy().into_owned()),
          StorageBackendType::Disk,
        )
      }
      _ => {
        config.cache_only(true);
        (None, StorageBackendType::Memory)
      }
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
    // 读促销一并关停（与 scan_promotion_rate 同口径的引擎档位收口）：bf-tree 0.5.6
    // 的读促销把基页记录以 OpType::Cache（clean）写进 mini page，扫描命中该叶时
    // promote_or_merge_mini_page → try_merge_mini_page 以 need_actually_merge_to_disk()
    // == false 提前返回 NoSplit、未经 load_base_page 即调 load_base_page_from_buffer
    // (mini_page_op.rs:379 unwrap None) 直接 panic，上层 catch_unwind 只把整次扫描
    // 折成 Err，HGETALL/RI.SCAN 遂静默返空或截断（副本回放主从发散即此面）。快照
    // 恢复出的树基页恒为 PageLocation::Base，读促销命中率最高，故为必踩而非偶发。
    // 档位随 CPR 快照配置头持久化（bf_tree::Config::new_from_snapshot 回读
    // read_promotion_rate），建树侧一次置零即覆盖换入树与副本重放树。
    config.read_promotion_rate(0);

    Ok(Arc::new(BfTreeService::new_with_backend(
      config,
      backend_type,
      file_path_str,
      true,
    )?))
  }

  /// 调参对应的常驻页环真实容量（预算记账口径单点）：
  /// `instantiate_tree` 仅在 `cache_size > 0` 时下发 `cb_size_byte`，为 0 则
  /// 引擎取自身默认（`Config::default().get_cb_size_byte()`）整块分配。预留
  /// 与归还必须按同一事实源计量，否则「按调参 0 预留、按树 cache_bytes()
  /// 归还」即在摘除点下溢，摧毁 cache_reserved ⟺ 在线树环容量和的记账契约
  #[inline]
  fn effective_cache_bytes(tuning: &TreeTuning) -> usize {
    if tuning.cache_size > 0 {
      tuning.cache_size
    } else {
      Config::default().get_cb_size_byte()
    }
  }

  fn create_bftree_internal(
    &self,
    key_id: u128,
    key_hash: u64,
    hash_prefix: Base32Buf128,
    storage_backend: StorageBackendType,
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
    // 删除未竟) 会被全新索引静默继承，以幻影数据暴露给新索引；同理，旧世代带地址
    // 刷盘快照仍可能被旧世代的迟到预置引用命中 (并发 promote 携带的旧源地址)，
    // 不清即把新世代数据文件覆盖回旧快照——数据文件与带地址刷盘件两类工件一并
    // unlink，保证新树从干净的数据文件与恢复源起步。
    let _ = fs::remove_file(self.data_file_path(&hash_prefix));
    self.remove_addr_flush_files(key_id);

    // 预算闸（登记前预留）：新树页环整块常驻，超预算显式拒绝创建；
    // 构造失败即归还配额防泄漏（登记本身无失败面）。预留量取引擎真实环容量
    // 口径（effective_cache_bytes），与摘除点 release_cache(tree.cache_bytes())
    // 对称，杜绝 cache_size=0 调参下「按 0 预留、按默认 32MiB 归还」的下溢毁账。
    // 实例化段无 unwind 记账面：引擎 0.5.6 全构造面（with_config 直建与
    // new_from_cpr_snapshot 快照恢复，后者在 CPRSnapShotMgr::new_from_snapshot
    // 内同样 validate 前置）均先 Config::validate 后 LeafStorage::new，容量
    // 不足组合恒以 InvalidConfig 干净拒绝，CircularBuffer::new 环断言配置面
    // 不可达——此处无需 catch_unwind（曾防的断言展开路径经亲码证伪不存在）
    let reserve_bytes = Self::effective_cache_bytes(&tuning);
    if !self.try_reserve_cache(reserve_bytes) {
      return Err(Error::CacheBudgetExhausted);
    }
    let tree = match self.instantiate_tree(
      (storage_backend == StorageBackendType::Disk)
        .then_some(self.data_file_path(&hash_prefix))
        .as_deref(),
      storage_backend,
      tuning,
    ) {
      Ok(tree) => tree,
      // Err 臂归还：容量不足组合被引擎 Config::validate 显式拒绝（InvalidConfig）
      // 等，预留记账不得滞留
      Err(e) => {
        self.release_cache(reserve_bytes);
        return Err(e);
      }
    };
    let entry = Arc::new(TreeEntry::new(Some(Arc::clone(&tree)), key_hash, key_id));

    pin.insert(key_id, entry);
    Ok(tree)
  }

  /// 集合就地升阶 / 分层重灌共用建树内核：构造未注册 scratch 树、排序批量装载、
  /// CPR 快照至独立临时文件，随后销毁并删除 scratch 工作文件，
  /// 返回 (快照文件路径, 去重后落刷条数)。
  ///
  /// 产物快照交 [`Self::publish_tree_from_snapshot_locked`] 原子换入正式数据路径
  /// （首升阶 replace=false、分层重灌 replace=true）。建树全程不触注册表与目标键
  /// 数据文件：旧树在换入前完好可读，杜绝旧「先摘旧树再原位重建」形态下
  /// drain 成功、重建失败的键蒸发窗口（对标 C# 对象记录重写单日志记录原子、
  /// 无销毁重建窗——ObjectStore/VarLenInputMethods.cs:GetRMWModifiedFieldInfo）。
  /// scratch 工作文件与快照均落 migration-tmp（启动期 remove_dir_all 清扫，
  /// 见本文件发布时序注），中途失败残件无泄漏类，仅在成功换入前多一次
  /// 快照文件的顺序写。装载被拒以 [`Error::LoadRejected`] 携原始状态码上抛，
  /// 由宿主分流 RESP 错误文案。
  ///
  /// 在 garnet 中的相对路径: 无逐函数对位（C# 集合恒驻对象域无就地升阶；
  /// 本内核与 publish_tree_from_snapshot_locked 组合承接本仓分层建树 + 换入，
  /// 换入通道与副本迁移流同源，见 doc/zh/collection.md 与
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs 的 PublishMigratedIndex）
  pub fn build_collection_tree_snapshot(
    &self,
    entries: &[(Vec<u8>, Vec<u8>)],
    tuning: &TreeTuning,
  ) -> Result<(PathBuf, u64)> {
    let mut tuning = *tuning;
    Self::resolve_tuning(&mut tuning);
    // scratch 环预算闸（入口预留，出口无条件归还）：每个并发升阶各持一环
    // 整块预留，并发峰值被总预算严格封顶，不另设信号量。拒绝即上抛
    // CacheBudgetExhausted，此刻未创建任何文件与引擎实例。预留/归还同取
    // effective_cache_bytes 口径（与登记臂对称，出口释放必与入口预留同量）
    let reserve_bytes = Self::effective_cache_bytes(&tuning);
    if !self.try_reserve_cache(reserve_bytes) {
      return Err(Error::CacheBudgetExhausted);
    }
    // 出口无条件归还：装载/快照段失败上抛前配额必还，scratch 环记账零滞留。
    // 入口预留先于建树不动——它承担并发升阶峰值封顶，先实例化后预留会让 N 个
    // 并发升阶各自真实分配整块页环后才被拒，预算闸失效。装载/快照段无 unwind
    // 记账面：与 create_bftree_internal 同一依据（引擎全构造面 validate 前置，
    // 断言配置面不可达），无需 catch_unwind
    let built = self.build_snapshot_inner(entries, &tuning);
    self.release_cache(reserve_bytes);
    built
  }

  /// [`Self::build_collection_tree_snapshot`] 的预留内建树段（调用方已持 scratch
  /// 配额，成功/失败一律由调用方出口归还）
  fn build_snapshot_inner(
    &self,
    entries: &[(Vec<u8>, Vec<u8>)],
    tuning: &TreeTuning,
  ) -> Result<(PathBuf, u64)> {
    let work_path = self.derive_temp_migration_path();
    let snap_path = self.derive_temp_migration_path();
    let tree = self.instantiate_tree(Some(&work_path), StorageBackendType::Disk, *tuning)?;
    // 装载与快照任一失败即作废 scratch：bulk_load 前置校验保证失败发生于任何
    // 写入之前（口径同旧原位建树路径），cpr_snapshot 失败快照文件由引擎侧自理，
    // 两态统一在下方释放 scratch 并删除其工作文件
    let built = tree
      .bulk_load(entries)
      .map_err(Error::LoadRejected)
      .and_then(|count| {
        tree.cpr_snapshot(&snap_path)?;
        Ok(count)
      });
    tree.dispose();
    // dispose 后方删（Windows 句柄次序，同 settle_detached_release）；工作文件
    // 已被引擎创建即两态同径 unlink，残件由 migration-tmp 启动清扫兜底，
    // 删除失败仅告警不污染装载结果
    if let Err(e) = fs::remove_file(&work_path) {
      log::warn!("升阶 scratch 工作文件删除失败，待启动清扫回收: {e}");
    }
    built.map(|count| (snap_path, count))
  }

  /// 获取或按需打开在线 BfTreeService (双重检查锁与条带锁保证并发安全性与恢复正确性；
  /// `id_key` 域语义见 [`RangeIndexManager::key_id_of`] 身份契约)
  pub fn get_or_open_tree(
    &self,
    id_key: &[u8],
    stub: &RangeIndexStub,
  ) -> Result<Arc<BfTreeService>> {
    let key_hash = fast_hash(id_key);
    let _stripe_lock = self.locks.write(key_hash);
    let start_gen = self.generation.load(Ordering::Acquire);
    let key_id = Self::key_id_of(id_key);

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
    let hash_prefix = Self::base32_prefix_of(id_key);
    let backend = StorageBackendType::from_u8(stub.storage_backend);
    let data_path = self.data_file_path(&hash_prefix);

    // 磁盘后端才存在可恢复的磁盘工件；内存后端的刷盘文件与工作文件均无意义
    //
    // 预置 (pre-stage) 说明 (1:1 对标 C# RestoreTree 只 `File.Exists(workingPath)`，
    // 绝无目录扫描)：冷态待恢复的存根，其 data.bftree 正常由生命周期钩子预置
    // 就位——刷盘态存根读路径先 RIPROMOTE 提升，PostCopyUpdater 冷态以源记录地址
    // 转调 [`Self::pre_stage_and_register_pending`] 精确复制那一个刷盘件 (对位 C#
    // 「uses the exact source address」，刷盘件的权威版本由存根源地址唯一确定，
    // 不是目录里地址最大的那个)；日志复制入尾走 PostCopyToTail 冷态同一入口；
    // 启动检查点恢复由 recover_all_trees_from_dir 批量预置。
    //
    // 文件缺失非不变量破坏 (1:1 对标 C# 同臂：Debug.Assert 仅断言常态，Release
    // 模式 LogWarning + return false 由上层答 NOTFOUND)：并发删空的墓碑先于
    // 在途慢路径落地时，数据文件由延迟释放挂入纪元异步 unlink，慢路径与删除
    // 交错即合法撞空——此时降级 [`Error::NotFound`] 交 wkv 收敛为
    // RangeIndexError::NotFound，绝不以致命恢复错误上抛破坏可用性
    if backend == StorageBackendType::Disk && !data_path.exists() {
      log::warn!("RestoreTree: data.bftree 缺失: {}", data_path.display());
      return Err(Error::NotFound);
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
      self.instantiate_tree(
        (backend == StorageBackendType::Disk).then_some(data_path.as_path()),
        backend,
        TreeTuning::from(stub),
      )?
    };

    // 预算强制预留（正确性优先）：恢复/重开的树数据在盘、键已注册，拒绝即丢
    // 数据——超预算仅告警，记账如实累加（与摘除点 release_cache 成对）
    self.reserve_cache_forced(tree.cache_bytes());

    #[cfg(debug_assertions)]
    if GET_OR_OPEN_PAUSE_INJECT.load(Ordering::Acquire)
      && GET_OR_OPEN_PAUSE_KEY_HASH.load(Ordering::Acquire) == key_hash
      && GET_OR_OPEN_PAUSE_INJECT.swap(false, Ordering::AcqRel)
    {
      GET_OR_OPEN_PAUSED.store(true, Ordering::Release);
      while !GET_OR_OPEN_RESUME.load(Ordering::Acquire) {
        thread::yield_now();
      }
      GET_OR_OPEN_RESUME.store(false, Ordering::Release);
      GET_OR_OPEN_PAUSED.store(false, Ordering::Release);
    }

    // 原地激活现有条目（如 pre_stage 或恢复阶段注册的 pending entry），或者注册全新条目。
    // 世代复查：若打开/预留期间发生过清库/dispose（捕获的世代已过期），已成幽灵树，
    // 立即回滚预留配额、弃置未注册树实例（Arc 离手随 drop dispose）并返回 NotFound
    let pin = self.live_indexes.pin();
    if self.generation.load(Ordering::Acquire) != start_gen {
      self.release_cache(tree.cache_bytes());
      return Err(Error::NotFound);
    }

    if let Some(entry) = pin.get(&key_id) {
      *entry.tree.write() = Some(Arc::clone(&tree));
    } else {
      let entry = Arc::new(TreeEntry::new(Some(Arc::clone(&tree)), key_hash, key_id));
      pin.insert(key_id, entry);
    }

    if self.generation.load(Ordering::Acquire) != start_gen {
      if let Some(entry) = pin.remove(&key_id)
        && let Some(t) = entry.tree.write().take()
      {
        self.release_cache(t.cache_bytes());
        t.dispose();
      }
      return Err(Error::NotFound);
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
  pub fn register_pending(&self, id_key: &[u8]) -> bool {
    let key_id = Self::key_id_of(id_key);
    let key_hash = fast_hash(id_key);
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
  pub fn pre_stage_and_register_pending(
    &self,
    id_key: &[u8],
    src_flush_address: u64,
  ) -> Result<()> {
    let key_hash = fast_hash(id_key);
    let _stripe_lock = self.locks.write(key_hash);
    let key_id = Self::key_id_of(id_key);
    let hash_prefix = Self::base32_prefix_of(id_key);
    let snapshot_path = self.log_flush_path(&hash_prefix, src_flush_address);
    if !snapshot_path.exists() {
      return Ok(());
    }
    let data_path = self.data_file_path(&hash_prefix);
    fs::copy(&snapshot_path, &data_path)?;

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
    let tree = entry.tree.write().take();
    // 预算归还（唯一摘除点）：与登记/恢复挂点的预留成对，树清退后配额即回
    if let Some(ref t) = tree {
      self.release_cache(t.cache_bytes());
    }
    tree
  }

  /// 世代守卫下的延迟释放内核 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeAndDeleteFilesDeferred)
  ///
  /// 与 C# 裸删形态的差额即本仓自加的世代守卫，且守卫在**登记与删除两个时点
  /// 各判一次**：删除时点必须持该键条带写锁复查 live_indexes，与
  /// create_bftree_internal 的注册同锁同判据——注册在锁内、unlink 在锁外即有时序洞
  /// (排空窗口内同名重建会把新世代数据文件删成孤儿 inode，静默丢数据)。
  ///
  /// C# 局部函数 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:TryDelete
  /// (先 dispose 后删文件的容错单文件删除) 在此内联为 `fs::remove_file` 忽略错误。
  ///
  /// `defer_on_contention` 为收割线程不确定自身锁态时的退让开关：延迟动作可在
  /// 任意线程内联执行 (LightEpoch 的槽位复用 prev_action.call 与收尾 help_drain)，
  /// 该线程可能正持同一条带锁 (如 publish_tree_from_snapshot_locked 锁内
  /// dispose_bf_tree_deferred、树会话的条带读锁)，阻塞加锁即同线程自死锁；故被占
  /// 时不裁决、不删除，排队待下一轮释放驱动重投本内核。同步形态 (无纪元) 的调用
  /// 线程与 [`Self::detach_tree`] 同契约 (不持条带锁)，按 detach_tree 的既有序序
  /// 阻塞加锁。
  fn settle_detached_release(self: &Arc<Self>, detached: DetachedTree, defer_on_contention: bool) {
    let DetachedTree {
      key_id,
      key_hash,
      tree,
      data_path,
    } = detached;
    // 弃旧引擎与注册表裁决无关，且 Windows 下须先关句柄再 unlink (C# 同款次序)，
    // 故置于取锁之前；dispose 以 tree.swap(None) 实现，退让重投不会二次释放
    if let Some(tree) = tree {
      tree.dispose();
    }
    let Some(data_path) = data_path else {
      return;
    };

    let _stripe_lock = if defer_on_contention {
      let Some(guard) = self.locks.try_write(key_hash) else {
        log::debug!("BfTree 延迟释放让位条带锁: key_id={key_id}");
        self.release_retries.lock().push(DetachedTree {
          key_id,
          key_hash,
          tree: None,
          data_path: Some(data_path),
        });
        return;
      };
      guard
    } else {
      self.locks.write(key_hash)
    };

    // 锁内复查：同 key_id 已重新登记即数据文件归新世代在用，只弃旧引擎绝不 unlink
    if self.live_indexes.pin().contains_key(&key_id) {
      return;
    }
    let _ = fs::remove_file(&data_path);
  }

  /// 摘取并接管树条目 (DisposeTreeUnderLock 的同步轻半)
  ///
  /// 条带独占写锁保护下从注册表摘除条目、取走在线树实例与数据文件路径，
  /// 全程不做纪元操作与文件删除——把 C# DisposeTreeUnderLock「锁内移除、
  /// 锁外 epoch 排空」的契约拆为可跨线程投递的两段：换号回收调用方只需
  /// 同步完成摘注册 (同名重建不被 IndexExists 拦截)，物理释放延后由
  /// 后台 GC 线程经 [`Self::release_detached`] 承接。无条目可摘时返回
  /// None (语义对齐 DisposeTreeUnderLock 的 false 分支)。`id_key` 域语义见
  /// [`RangeIndexManager::key_id_of`] 身份契约。
  ///
  /// 离线自洽收口：`delete_file=false`（懒恢复场景，文件保留）且在线树在场时，
  /// 树运行期的基页落盘 (merge-to-disk) 已把工作文件 data.bftree 写长，而快照头
  /// `BfTreeMeta.file_size` 仅由 cpr_snapshot 收口——直接以工作文件恢复会撞引擎
  /// 自洽断言 (new_from_snapshot: assert 文件长度 == file_size)。摘除在条带写锁内
  /// 完成，此刻已无并发写者 (数据写臂持条带读锁)，本函数就地 CPR 快照至
  /// migration-tmp 临时件并 rename 原子覆盖 data.bftree，钉死「离线树的
  /// data.bftree 恒为自洽快照」不变量——即 C# RestoreTree 直接
  /// RecoverFromCprSnapshot(data.bftree) 所信任的 pre-stage 不变量
  /// (libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:RestoreTree) 在本
  /// 封装层的显式单点。消亡臂 (`delete_file=true`) 文件即将删除，无需收口。
  /// 快照或 rename 失败仅告警保留原文件：恢复面将如实显式报错，禁静默换树
  pub fn detach_tree(&self, id_key: &[u8], delete_file: bool) -> Option<DetachedTree> {
    let key_hash = fast_hash(id_key);
    let _stripe_lock = self.locks.write(key_hash);
    let key_id = Self::key_id_of(id_key);
    let tree = self.remove_and_take_tree(key_id);
    let data_path = if delete_file {
      let p = self.data_file_path_for_key(id_key);
      (tree.is_some() || p.exists()).then_some(p)
    } else if let Some(tree) = tree.as_ref() {
      let data_path = self.data_file_path_for_key(id_key);
      let heal_path = self.derive_temp_migration_path();
      match tree.cpr_snapshot(&heal_path) {
        Ok(()) => {
          if fs::rename(&heal_path, &data_path).is_err() {
            log::warn!("BfTree 离线收口快照换入失败: key_id={key_id}");
          }
        }
        Err(e) => log::warn!("BfTree 离线收口快照失败，保留原数据文件: key_id={key_id}, e={e:?}"),
      }
      None
    } else {
      None
    };
    drop(_stripe_lock);

    if tree.is_none() && data_path.is_none() {
      return None;
    }
    Some(DetachedTree {
      key_id,
      key_hash,
      tree,
      data_path,
    })
  }

  /// 释放已摘取树 (DisposeTreeUnderLock 的延迟重半，1:1 对标该方法内
  /// storeEpoch.BumpCurrentEpoch(DisposeAndDeleteFilesDeferred) 段)
  ///
  /// 经 storeEpoch.bump_current_epoch_action 挂入延迟清理队列 (无纪元时
  /// 回退为同步清理)；读写者在纪元保护下安全退出后，底层引擎与文件方才
  /// 真正释放。槽位耗尽或就绪收割时的自旋与磁盘删除均发生在收割本动作的
  /// 线程上——换号回收形态必须由后台 GC 线程调用，严禁回到回放/流水线
  /// 线程。同名重建世代守卫在登记与删除两个时点各判一次 (判据与内核见
  /// `Self::settle_detached_release`)：摘注册与释放跨队列投递分离后窗口是
  /// 后台轮询量级，仅登记前判一次会漏掉排空期内的同名重建。登记前的
  /// filter 是「已确认新世代在册」的快速短路，省掉无谓的纪元注册与锁竞争，
  /// 不替代删除时点的复查。
  pub fn release_detached(self: &Arc<Self>, mut detached: DetachedTree) {
    let key_id = detached.key_id;
    detached.data_path = detached
      .data_path
      .filter(|_| !self.live_indexes.pin().contains_key(&key_id));
    if detached.tree.is_none() && detached.data_path.is_none() {
      return;
    }

    if let Some(ref epoch) = self.store_epoch {
      let this = Arc::clone(self);
      epoch.bump_current_epoch_action(move || this.settle_detached_release(detached, true));
    } else {
      self.settle_detached_release(detached, false);
    }
  }

  /// 重投条带锁竞争退让的待释放批次 (收割线程让位时由 `Self::settle_detached_release`
  /// 入队，每轮至多消费 `cap` 条，剩余留待下轮)
  ///
  /// 与 [`Self::release_detached`] 同一裁决内核，仅由不持条带锁的释放驱动线程
  /// (wkv 内置 GC 轮次 / 测试手动驱动) 调用；返回本轮重投条数。
  pub fn harvest_release_retries(self: &Arc<Self>, cap: usize) -> usize {
    let batch: Vec<DetachedTree> = {
      let mut retries = self.release_retries.lock();
      let n = retries.len().min(cap);
      retries.drain(..n).collect()
    };
    let n = batch.len();
    for detached in batch {
      self.release_detached(detached);
    }
    n
  }

  /// 销毁并释放指定树条目 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock)
  ///
  /// 「[`Self::detach_tree`] 紧接 [`Self::release_detached`]」的组合别名
  /// (DEL/驱逐等用户命令面的同步路径专用；换号回收路径须两段跨线程拆分，
  /// 不得经本组合在流水线线程做纪元注册与文件删除)。`id_key` 域语义见
  /// [`RangeIndexManager::key_id_of`] 身份契约
  pub fn dispose_tree_under_lock(
    self: &Arc<Self>,
    id_key: &[u8],
    delete_file: bool,
  ) -> Result<bool> {
    match self.detach_tree(id_key, delete_file) {
      Some(detached) => {
        self.release_detached(detached);
        Ok(true)
      }
      None => Ok(false),
    }
  }

  /// 删除指定索引并彻底清理磁盘文件（对应 DisposeTreeUnderLock deleteFiles=true 分支）
  pub fn delete_index(self: &Arc<Self>, id_key: &[u8]) -> Result<bool> {
    // 故障注入门（测试钩子，一次性）：模拟树注销/文件删除 IO 失败面，与真实
    // 删除失败同型上抛，杜绝测试绕过本单点
    #[cfg(debug_assertions)]
    if DELETE_INDEX_FAIL_INJECT.swap(false, Ordering::AcqRel) {
      return Err(Error::Io(io::Error::other("删除故障注入（测试钩子）")));
    }
    self.dispose_tree_under_lock(id_key, true)
  }

  /// 迁移发布：把临时快照文件原子换入数据路径，恢复引擎实例并注册 (1:1 对标
  /// Garnet PublishMigratedIndex 的文件换入/树恢复/注册表部分)
  ///
  /// ⚠️ 调用方须已持该键的条带互斥写锁 (对标 C# *UnderLock 契约)——发布全流程
  /// (存在性判定 → 快照源校验 → 旧树排空 → 旧世代刷盘件清理 → 文件换入 →
  /// 恢复 → 注册) 必须对同键并发发布原子，锁由调用方持有并覆盖其后续的
  /// 存根元数据落盘。`id_key` 域语义见 [`RangeIndexManager::key_id_of`] 身份契约。
  ///
  /// 时序说明：
  /// - Unix 上 `rename` 原子替换既有数据文件，换入窗口内无「文件缺失」间隙；
  ///   rename 失败 (Windows 同名冲突/跨设备) 回退 remove+copy，仅该回退路径
  ///   存在非原子窗口，失败即上抛；
  /// - 换入完成后 fsync 父目录 (数据 fsync + 目录 fsync 双屏障口径见
  ///   wdev::sync_dir 文档)，掉电后换入结果持久可见；
  /// - replace 时旧树在锁内排空释放：在途写者 (持条带读锁 + 写者微守卫) 不依赖
  ///   写锁退出，无死锁，至多阻塞同条带新写者一个 insert 时长；
  /// - 快照文件缺失属不变量破坏，显式上抛，绝不静默注册空树。
  pub fn publish_tree_from_snapshot_locked(
    &self,
    id_key: &[u8],
    snapshot_path: &Path,
    replace: bool,
  ) -> Result<Arc<BfTreeService>> {
    // 故障注入门（测试钩子，一次性）：模拟换入失败面（rename/恢复 IO 失败），
    // 与真实发布失败同型上抛——此刻旧树原样在位、注册表未动，杜绝测试绕过
    // 本单点
    #[cfg(debug_assertions)]
    if PUBLISH_FAIL_INJECT.swap(false, Ordering::AcqRel) {
      return Err(Error::Recovery("发布故障注入（测试钩子）".into()));
    }
    let key_hash = fast_hash(id_key);
    let key_id = Self::key_id_of(id_key);
    let hash_prefix = Self::base32_prefix_of(id_key);
    let data_path = self.data_file_path(&hash_prefix);

    // 锁内复查注册表：并发发布已被调用方条带锁串行化，此处是最终裁决点
    let exists = self.live_indexes.pin().contains_key(&key_id);
    if exists && !replace {
      return Err(Error::IndexExists);
    }
    // 快照存在性不变量前置于摘旧树之前：缺失属调用方序违，先判死再动旧态，
    // 杜绝「旧树已摘、快照缺失上抛」的自伤窗
    if !snapshot_path.exists() {
      use core::fmt::Write;
      let mut msg = String::with_capacity(32 + snapshot_path.as_os_str().len());
      let _ = write!(msg, "迁移快照文件不存在: {}", snapshot_path.display());
      return Err(Error::Recovery(msg));
    }
    if exists {
      // 旧树锁内摘除 + 延迟释放 (remove_and_take_tree 契约：调用方持条带写锁)
      if let Some(old) = self.remove_and_take_tree(key_id) {
        self.dispose_bf_tree_deferred(old);
      }
    }
    // 换入即新世代确立：清该键全部旧世代带地址刷盘件（create_bftree_internal
    // 防重门旁同款工件回收的发布侧对位）。前代残件仍可能被前代存根记录的迟到
    // 预置引用命中 (promote 携带的旧源地址)，不清则换入进来的新树被旧世代快照
    // 回灌——先建后拆换来的换入原子性会被盘上残件击穿；键消亡臂 (delete_index)
    // 不删刷盘件、仅靠 on_truncate 按地址滞后回收，故本裁决点为换代路径的即时收口
    self.remove_addr_flush_files(key_id);

    let parent = data_path.parent();
    if let Some(parent) = parent {
      fs::create_dir_all(parent)?;
    }
    // 发布双屏障 (统一口径见 wdev::sync_dir 文档)：数据屏障已由迁移接收端
    // RangeIndexChunkedDeserializer 落盘时的 sync_all 闭合；rename 只改目录项，
    // POSIX 下掉电后不保证其可见——必须 fsync 换入目录，杜绝「上层已确认迁移 +
    // 数据文件目录项丢失 → 惰性恢复显式报错」的半途发布窗口。源目录
    // (migration-tmp) 不 fsync：崩溃后 temp 文件复活无正确性影响 (读取一律以
    // data 路径为准，且启动时 with_epoch 会 remove_dir_all 清理)。
    if fs::rename(snapshot_path, &data_path).is_ok() {
      if let Some(parent) = parent {
        wdev::sync_dir(parent)?;
      }
    } else {
      // 回退路径 (平台/跨设备)：非原子，失败即上抛保持盘上自洽
      if data_path.exists() {
        fs::remove_file(&data_path)?;
      }
      fs::copy(snapshot_path, &data_path)?;
      // fs::copy 仅写页缓存，回退路径补数据屏障 (写权限句柄口径同
      // wcpr sync_file_data：Windows 的 FlushFileBuffers 要求写句柄)
      File::options().write(true).open(&data_path)?.sync_all()?;
      if let Some(parent) = parent {
        wdev::sync_dir(parent)?;
      }
      let _ = fs::remove_file(snapshot_path);
    }

    let tree = Arc::new(BfTreeService::recover_from_cpr_snapshot(
      &data_path,
      true,
      StorageBackendType::Disk,
    )?);
    // 预算强制预留（正确性优先，同 get_or_open_tree 口径）：换入树已在盘、
    // 流块已入账，拒绝即丢数据——超预算仅告警（replace 臂的旧树配额已由
    // remove_and_take_tree 归还，此处只记新树）
    self.reserve_cache_forced(tree.cache_bytes());
    let entry = Arc::new(TreeEntry::new(Some(Arc::clone(&tree)), key_hash, key_id));
    self.live_indexes.pin().insert(key_id, entry);
    Ok(tree)
  }
}
