//! RangeIndex 管理器实现 (1:1 对标 Garnet RangeIndexManager.cs)
//!
//! 负责协调 BfTree 的生命周期、数据文件预分阶段 (Pre-Stage)、检查点 CPR 快照与全量故障恢复。
//!
//! 模块划分（对标 C# partial class 文件组织）：
//! - [`checkpoint`]：全局检查点屏障与全树快照（对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint 等）
//! - [`lifecycle`]：创建 / 惰性恢复 / 注册 / 注销 / 删除（对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:CreateBfTree、RestoreTree、DisposeTreeUnderLock 等）
//! - [`flush`]：刷盘事件触发的单树快照（对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotTreeForFlush）
//! - [`replication`]：刷盘文件枚举、截断回收与全量恢复（对标 Replication/OnTruncate/RecoverAllTrees）

use std::fs::{read_dir, remove_dir_all, remove_file};

use event_listener::Event;
mod checkpoint;
mod flush;
mod lifecycle;
mod replication;

use std::{
  fs,
  path::{Path, PathBuf},
  str,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
  },
};

#[cfg(debug_assertions)]
pub use lifecycle::{
  DELETE_INDEX_FAIL_INJECT, GET_OR_OPEN_PAUSE_INJECT, GET_OR_OPEN_PAUSE_KEY_HASH,
  GET_OR_OPEN_PAUSED, GET_OR_OPEN_RESUME, PUBLISH_FAIL_INJECT,
};
use parking_lot::{Mutex, RwLock};
use wbase::{
  backoff::backoff,
  base32::{BASE32_LEN_U64, BASE32_LEN_U128, Base32Buf128, encode_u64, encode_u128},
  map::{ConcurrentMap, new_concurrent_map},
  striped::StripedRwLock,
};
use wepoch::LightEpoch;
use whasher::hash128;

use crate::{
  error::{Error, Result},
  service::BfTreeService,
  stub::RANGE_INDEX_STUB_SIZE,
};

/// 编译期计算的 128 位哈希前缀种子密钥
const PREFIX_SEED_1: u64 = 0x27bb_2ee6_87b0_b0fd;
const PREFIX_SEED_2: u64 = 0x517c_c1b7_2722_0a95;

/// 键哈希前缀长度 (128 位哈希的 26 字符小写 Base32 编码)
pub(crate) const HASH_PREFIX_LEN: usize = BASE32_LEN_U128;

/// 数据文件标准后缀
const DATA_FILE_SUFFIX: &str = ".data.bftree";
/// 刷盘快照文件标准后缀
const FLUSH_FILE_SUFFIX: &str = ".flush.bftree";
/// 树文件通用后缀
const TREE_FILE_SUFFIX: &str = ".bftree";

/// CPR 快照文件魔数 (底层 bf-tree 快照格式头部标识)
pub(crate) const CPR_MAGIC: &[u8; 16] = b"BF-TREE-V0-BEGIN";

/// 锁条带数量 (128 分段降低热路径锁竞争；C# Garnet 按 ProcessorCount 取 2 的幂
/// 条带化 [RangeIndexManager.cs]，此处固定 128 为刻意差异，仅影响竞争粒度)
pub(crate) const NUM_LOCK_STRIPES: usize = 128;

/// 默认迁移分块大小 (256KB)：迁移流帧上限与 AOF 流重装配缓冲预期共用的
/// 分块基准，全仓单点定义（wnode 迁移/复制面与 wedb 迁移、推流及本 crate
/// 测试均直引此处）
///
/// 在 garnet 中的相对路径:libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:DefaultMigrationChunkSize
pub const DEFAULT_MIGRATION_CHUNK_SIZE: usize = 256 * 1024;
/// 索引存根字节大小 (35 字节，1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:IndexSizeBytes)
pub const INDEX_SIZE_BYTES: usize = RANGE_INDEX_STUB_SIZE;

/// 已摘取待物理释放的树 (RangeIndexManager::detach_tree 的产物)
///
/// 注册表已同步摘净、纪元排空与引擎释放、数据文件删除尚未发生；换号回收
/// 场景将其成批投递至待释放队列，由后台 GC 线程经
/// RangeIndexManager::release_detached 完成物理释放 (doc/zh/db.md
/// 主从异步屏障承诺：回放流水线只换号与投递，不阻塞)。
pub struct DetachedTree {
  /// 该树 128 位键 ID (释放时同名重建世代守卫的查询目标)
  key_id: u128,
  /// detach_tree 已算出的键哈希 (延迟释放时点据此落回同一条带写锁，与
  /// create_bftree_internal 的注册点同锁同判据)
  key_hash: u64,
  /// 已摘出的在线树实例
  tree: Option<Arc<BfTreeService>>,
  /// 纪元排空后须删除的数据文件路径 (None 表示不动文件)
  data_path: Option<PathBuf>,
}

/// 单树条目 (1:1 对标 Garnet TreeEntry)
///
/// 内部纯二进制：仅存储 128 位 `key_id: u128`，消除冗余 Base32 字段，结构体体积缩小
/// 32 字节（44% 瘦身），彻底消除 Cache Line 污染；需要文件名时由 [`Self::hash_prefix`]
/// 在栈上即时以纯位移+查表计算出 26 字符定长 Base32 栈缓冲（耗时 < 2ns，零堆分配）。
pub struct TreeEntry {
  /// 托管的在线 BfTreeService 实例
  pub tree: RwLock<Option<Arc<BfTreeService>>>,
  /// 键哈希值 (用于锁分段)
  pub key_hash: u64,
  /// 128 位唯一键 ID
  pub key_id: u128,
  /// 是否处于快照中
  pub snapshot_pending: AtomicBool,
  /// 快照防重入原子锁
  pub snapshot_in_progress: AtomicBool,
  /// 检查点完成事件通知（基于 event_listener 消除忙自旋与轮询）
  pub checkpoint_event: event_listener::Event,
}

impl TreeEntry {
  /// 创建新条目
  pub fn new(tree: Option<Arc<BfTreeService>>, key_hash: u64, key_id: u128) -> Self {
    Self {
      tree: RwLock::new(tree),
      key_hash,
      key_id,
      snapshot_pending: AtomicBool::new(false),
      snapshot_in_progress: AtomicBool::new(false),
      checkpoint_event: Event::new(),
    }
  }

  /// 26 字符 Base32 前缀 (key_id 的内联栈编码，`Deref` 至 `str`)
  #[inline(always)]
  pub fn hash_prefix(&self) -> Base32Buf128 {
    encode_u128(self.key_id)
  }

  /// 尝试获取快照原子锁 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:TryClaimSnapshot)
  #[inline]
  pub fn try_claim_snapshot(&self) -> bool {
    self
      .snapshot_in_progress
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
      .is_ok()
  }

  /// 释放快照原子锁 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:ReleaseSnapshot)
  #[inline]
  pub fn release_snapshot(&self) {
    self.snapshot_in_progress.store(false, Ordering::Release);
  }

  /// 在防重入快照锁保护下执行 CPR 快照 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotUnderClaim)
  ///
  /// 认领自旋采用退避阶梯 (spin → yield → 微睡)，与 C# 纯 `Thread.Yield` 的**无界
  /// 串行等待**语义一致：同键并发的刷盘 / 检查点 / 迁移快照在此排队，等待方绝不因
  /// 等待时长而失败——bftree 引擎对并发快照静默 no-op，claim 是唯一串行化点，认领
  /// 失败即等于放弃一次必须发生的快照。大树慢盘单次 CPR 快照落盘可达数十秒以上
  /// (doc/zh/collection.md 承诺单树千万至上亿条目，GB 级整树顺序写超 30 秒是常态)，
  /// 任何固定上界都会与容量承诺直接冲突，故不设超时；等待不占核 (第三阶为 50μs 微睡)，
  /// 烧核顾虑由退避阶梯本身消解，无需以此为由判失败。
  ///
  /// 无界等待不构成挂死泄漏：认领释放由 [`SnapshotClaimGuard`] RAII 在任何退出或
  /// panic 展开路径强制归还 (对标 C# try/finally ReleaseSnapshot)，等待方阻塞的上界
  /// 即持有者单次快照时长，持有者快照结束必然释放——全链路仅此一套串行机制，无旁路
  /// 重试、无第二重超时。
  pub fn snapshot_under_claim(&self, tree: &BfTreeService, destination_path: &Path) -> Result<()> {
    let mut spins = 0u32;
    while !self.try_claim_snapshot() {
      backoff(spins);
      spins = spins.wrapping_add(1);
    }
    let _guard = SnapshotClaimGuard(self);
    tree.cpr_snapshot(destination_path)
  }
}

/// 快照防重入 RAII 守卫，确保在任何退出或异常路径下释放 snapshot_in_progress
struct SnapshotClaimGuard<'a>(&'a TreeEntry);
impl Drop for SnapshotClaimGuard<'_> {
  fn drop(&mut self) {
    self.0.release_snapshot();
  }
}

/// RangeIndex 管理器 (1:1 对标 Garnet RangeIndexManager)
pub struct RangeIndexManager {
  /// 数据文件根目录 ({ri_log_root}/{hashPrefix}.data.bftree)
  pub(crate) ri_log_root: PathBuf,
  /// 检查点快照根目录 ({cpr_dir}/{token}/rangeindex/{hashPrefix}.bftree)
  pub(crate) cpr_dir: PathBuf,
  /// 迁移临时目录 ({ri_log_root}/migration-tmp/)
  pub(crate) migration_temp_dir: PathBuf,
  /// 在线索引字典 (按 128 位 key_id 纯整数索引，零堆分配，基于 papaya 高性能无锁并发字典与硬件向量加速 gxhash)
  pub(crate) live_indexes: ConcurrentMap<u128, Arc<TreeEntry>>,
  /// RENAME 迁移进行中的键 claim 注册表（非持久、纯内存态，键选 128 位 key_id
  /// 与 [`Self::live_indexes`] 同一标识域，值恒 `()`）
  ///
  /// 段一以内存 claim 替代旧键元记录持久墓碑封堵并发写：claim 期间元记录全程
  /// 存活，进程崩溃/掉电即随注册表消散，日志回放恢复旧键原态——杜绝旧持久墓碑
  /// 「墓碑落盘后、流块入账前」崩溃窗的整键丢失（promote「先发流后毁旧态」
  /// 不变量：持久墓碑改由段五排空单点在流块已入账 + 新键 meta 已落之后落笔）。
  /// 判点在 wkv 四入口（load_range_index_stub / load_collection_stub /
  /// refresh_tiered_meta / load_meta）：RI 面与 load_meta 维持「键暂时不可见」
  /// （与墓碑态等价，NotFound/缺席即显式拒绝）；分层路由与 RI 创建面命中显式回
  /// MigrationBusy 锁忙/重试错（禁「视同不存在」穿透——穿透会在 dst 信封域
  /// 物化重建对象或走重建路径，换一种已 ACK 丢失形）。RENAME 段一对 old/new
  /// 双键成对登记（dst 侧仅存活记录在场才登记，双键互斥 try 失败即退）
  pub(crate) migrating: ConcurrentMap<u128, ()>,
  /// 全局检查点进行中标记
  pub(crate) checkpoint_in_progress: AtomicBool,
  /// 键哈希分段读写条带锁 (wbase::striped::StripedRwLock，默认槽位 128 字节缓存行对齐消除伪共享)
  pub(crate) locks: StripedRwLock<(), NUM_LOCK_STRIPES>,
  /// 条带锁竞争退让的待释放批次：纪元收割线程不确定本线程条带锁态 (延迟动作可在
  /// 持同一条带锁的线程上内联收割)，绝不可阻塞加锁，故让位后在此排队，由下一轮
  /// 释放驱动线程经 [`RangeIndexManager::harvest_release_retries`] 重投同一内核
  pub(crate) release_retries: Mutex<Vec<DetachedTree>>,
  /// 存储纪元 (用于 BfTree 延迟销毁，1:1 对标 Garnet RangeIndexManager.cs:storeEpoch)
  pub(crate) store_epoch: Option<Arc<LightEpoch>>,
  /// 全局检查点完成事件通知（基于 event_listener 消除忙自旋与轮询）
  pub(crate) checkpoint_event: event_listener::Event,
  /// 常驻页缓存总预算（字节，0 = 不设限）：长驻树页环 + 并发升阶 scratch 环
  /// 共用的唯一总闸。本仓分层防 OOM 自定义面（C# RangeIndexManager 无预算
  /// 字段——C# 树仅 RI.CREATE 显式创建、数量用户可控；自动升阶树数无上限，
  /// doc/zh/collection.md 的防 OOM 承诺由本闸兜底）
  pub(crate) cache_budget: usize,
  /// 已预留字节计数：在线树页环（cache_bytes）与升阶 scratch 环（建树期
  /// 短持）的实时和。验收契约：与当前在线活跃树环容量和 + 在途 scratch 环
  /// 容量严格一致（见 try_reserve_cache / reserve_cache_forced / release_cache）
  pub(crate) cache_reserved: AtomicUsize,
  /// 全局生命周期世代计数（换号/清库门控）：dispose() 时自增，get_or_open_tree 注册段复查，
  /// 防止惰性恢复在清库窗口登记幽灵树与残留记账
  pub(crate) generation: AtomicU64,
}

impl RangeIndexManager {
  /// 创建管理器实例 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs 构造器；
  /// 预算不设限，预算注入走 [`Self::with_epoch_and_budget`])
  pub fn new(ri_log_root: impl Into<PathBuf>, cpr_dir: impl Into<PathBuf>) -> Result<Self> {
    Self::with_epoch_and_budget(ri_log_root, cpr_dir, None, 0)
  }

  /// 关联存储纪元创建管理器实例 (1:1 对标 Garnet RangeIndexManager(..., storeEpoch)；
  /// 预算不设限，预算注入走 [`Self::with_epoch_and_budget`])
  pub fn with_epoch(
    ri_log_root: impl Into<PathBuf>,
    cpr_dir: impl Into<PathBuf>,
    store_epoch: Option<Arc<LightEpoch>>,
  ) -> Result<Self> {
    Self::with_epoch_and_budget(ri_log_root, cpr_dir, store_epoch, 0)
  }

  /// 关联存储纪元并注入常驻页缓存总预算（字节，0 = 不设限）
  ///
  /// 宿主装配单点（wkv StoreConfig.tree_cache_budget_bytes 注入）：定额由宿主
  /// 配置面供给（wconf `tree-cache-budget` 旋钮，缺省
  /// wkv DEFAULT_TREE_CACHE_BUDGET_BYTES 256MiB 编译期常量），管理器只承载
  /// 记账与触顶裁决，不承载定额推导
  pub fn with_epoch_and_budget(
    ri_log_root: impl Into<PathBuf>,
    cpr_dir: impl Into<PathBuf>,
    store_epoch: Option<Arc<LightEpoch>>,
    cache_budget: usize,
  ) -> Result<Self> {
    let ri_log_root = ri_log_root.into();
    let cpr_dir = cpr_dir.into();

    if ri_log_root.as_os_str().is_empty() {
      return Err(Error::InvalidArgument(
        "ri_log_root 路径不能为空 (对标 C# ArgumentException)".into(),
      ));
    }

    fs::create_dir_all(&ri_log_root)?;
    fs::create_dir_all(&cpr_dir)?;

    let migration_temp_dir = ri_log_root.join("migration-tmp");
    if migration_temp_dir.exists() {
      fs::remove_dir_all(&migration_temp_dir)?;
    }
    fs::create_dir_all(&migration_temp_dir)?;

    // 清理崩溃残留的 *.recovering 临时换树工件，防止磁盘工件残留与脏恢复
    if let Ok(entries) = fs::read_dir(&ri_log_root) {
      for entry in entries.flatten() {
        if let Some(name) = entry.file_name().to_str()
          && name.ends_with(".recovering")
        {
          let _ = fs::remove_file(entry.path());
        }
      }
    }

    Ok(Self {
      ri_log_root,
      cpr_dir,
      migration_temp_dir,
      live_indexes: new_concurrent_map(),
      migrating: new_concurrent_map(),
      checkpoint_in_progress: AtomicBool::new(false),
      locks: StripedRwLock::new(),
      release_retries: Mutex::new(Vec::new()),
      store_epoch,
      checkpoint_event: Event::new(),
      cache_budget,
      cache_reserved: AtomicUsize::new(0),
      generation: AtomicU64::new(0),
    })
  }

  /// 尝试预留页缓存配额（CAS 无锁记账，true = 预留成功）
  ///
  /// 长驻树登记（create_bftree_internal）与升阶 scratch 建树闸（build_collection_tree_snapshot）
  /// 的拒绝判据：预算内成功，超限返回 false 由调用方回
  /// [`Error::CacheBudgetExhausted`]。预算 0 = 不设限，仅记账不裁决
  #[inline]
  pub fn try_reserve_cache(&self, bytes: usize) -> bool {
    if bytes == 0 {
      return true;
    }
    if self.cache_budget == 0 {
      self.cache_reserved.fetch_add(bytes, Ordering::AcqRel);
      return true;
    }
    let mut cur = self.cache_reserved.load(Ordering::Acquire);
    loop {
      // checked_add：forced 挂点可把记账推高至预算之上，此处不得下溢/回绕
      match cur.checked_add(bytes) {
        Some(next) if next <= self.cache_budget => {
          match self.cache_reserved.compare_exchange_weak(
            cur,
            next,
            Ordering::AcqRel,
            Ordering::Acquire,
          ) {
            Ok(_) => return true,
            Err(v) => cur = v,
          }
        }
        _ => return false,
      }
    }
  }

  /// 强制预留页缓存配额（正确性优先挂点：惰性恢复 / 换入恢复的树已在盘上
  /// 或在册，拒绝即丢数据——超预算仅告警不拒绝，记账如实累加）
  #[inline]
  pub fn reserve_cache_forced(&self, bytes: usize) {
    if bytes == 0 {
      return;
    }
    let prev = self.cache_reserved.fetch_add(bytes, Ordering::AcqRel);
    if self.cache_budget != 0 && prev + bytes > self.cache_budget {
      log::warn!(
        "树页缓存超预算: 已预留 {} + 本次 {} > 预算 {}（正确性优先放行，恢复/换入树拒绝即丢数据）",
        prev,
        bytes,
        self.cache_budget
      );
    }
  }

  /// 归还页缓存配额（树摘除 / scratch 建树出口的成对释放）
  #[inline]
  pub fn release_cache(&self, bytes: usize) {
    if bytes == 0 {
      return;
    }
    let prev = self.cache_reserved.fetch_sub(bytes, Ordering::AcqRel);
    debug_assert!(prev >= bytes, "页缓存配额下溢: prev={prev}, bytes={bytes}");
  }

  /// 当前已预留页缓存字节数（验收口径：与在线活跃树环容量和 + 在途 scratch
  /// 环容量一致）
  #[inline]
  pub fn cache_reserved(&self) -> usize {
    self.cache_reserved.load(Ordering::Acquire)
  }

  /// 当前生命周期世代号（换号/清库门控代数）
  #[inline]
  pub fn generation(&self) -> u64 {
    self.generation.load(Ordering::Acquire)
  }

  /// 页缓存总预算定额字节（0 = 不设限；装配期单点注入后恒定）
  #[inline]
  pub fn cache_budget(&self) -> usize {
    self.cache_budget
  }

  /// 范围索引数据根目录 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:RiLogRoot)
  #[inline]
  pub fn ri_log_root(&self) -> &Path {
    &self.ri_log_root
  }

  /// 迁移临时目录
  #[inline]
  pub fn migration_temp_dir(&self) -> &Path {
    &self.migration_temp_dir
  }

  /// 生成临时迁移文件路径 ({ri_log_root}/migration-tmp/{random_id}.bftree) (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:DeriveTempMigrationPath)
  #[inline]
  pub fn derive_temp_migration_path(&self) -> PathBuf {
    let rand_id = fastrand::u128(..);
    let b32 = encode_u128(rand_id);
    self.migration_temp_dir.join(Self::tree_file_name(&b32))
  }

  /// 释放管理器资源并释放所有在线树 (1:1 对标 Garnet IDisposable.Dispose)
  pub fn dispose(&self) {
    self.generation.fetch_add(1, Ordering::AcqRel);
    let pin = self.live_indexes.pin();
    for entry in pin.values() {
      if let Some(tree) = entry.tree.write().take() {
        // 直摘路径的配额归还（与 remove_and_take_tree 同判据）：注册表清空即
        // 配额归零，维持 cache_reserved ⟺ 在线树环容量和的记账契约
        self.release_cache(tree.cache_bytes());
        tree.dispose();
      }
    }
    pin.clear();
  }

  /// 清空全部范围索引（释放全部在线树并清理磁盘树文件与快照）
  pub fn clear_all(&self) -> Result<()> {
    self.dispose();
    if self.ri_log_root.exists() {
      for entry in read_dir(&self.ri_log_root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
          let _ = remove_file(&path);
        }
      }
    }
    if self.cpr_dir.exists() {
      for entry in read_dir(&self.cpr_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
          let _ = remove_file(&path);
        } else if path.is_dir() {
          let _ = remove_dir_all(&path);
        }
      }
    }
    Ok(())
  }

  /// 延迟释放 BfTree (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:DisposeBfTreeDeferred)
  pub fn dispose_bf_tree_deferred(&self, tree: Arc<BfTreeService>) {
    if let Some(ref epoch) = self.store_epoch {
      epoch.bump_current_epoch_action(move || {
        tree.dispose();
      });
    } else {
      tree.dispose();
    }
  }

  /// 获取指定键哈希的排他写锁（1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:AcquireExclusiveForDelete）
  ///
  /// 无界阻塞形态：仅限同步上下文（阻塞线程闭包内自取自放）使用；异步任务
  /// 内取同条带锁一律走 [`Self::try_acquire_exclusive_for_delete`] 有界档
  ///（票 zcode-r135c-lockorder 案二，compio thread-per-core 下无界 park 停核）
  #[inline]
  pub fn acquire_exclusive_for_delete(
    &self,
    key_hash: u64,
  ) -> parking_lot::RwLockWriteGuard<'_, ()> {
    self.locks.write(key_hash)
  }

  /// 获取指定键哈希的共享读锁（1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:ReadRangeIndexLock）
  ///
  /// 无界阻塞形态，使用域限制同 [`Self::acquire_exclusive_for_delete`]；异步
  /// 任务内走 [`Self::try_read_range_index_lock`] 有界档
  #[inline]
  pub fn read_range_index_lock(&self, key_hash: u64) -> parking_lot::RwLockReadGuard<'_, ()> {
    self.locks.read(key_hash)
  }

  /// 单次尝试获取指定键哈希的排他写锁（非阻塞：被占即回 None）
  ///
  /// 异步上下文（compio 任务 poll 栈内）取条带锁的专用口：调用方以
  /// try + 让核有界环承接（wkv `acquire_tree_write`），杜绝 parking_lot
  /// 无界停车停摆整核 io_uring reactor（票 zcode-r135c-lockorder 案二）
  #[inline]
  pub fn try_acquire_exclusive_for_delete(
    &self,
    key_hash: u64,
  ) -> Option<parking_lot::RwLockWriteGuard<'_, ()>> {
    self.locks.try_write(key_hash)
  }

  /// 单次尝试获取指定键哈希的共享读锁（非阻塞：被占即回 None）
  ///
  /// 使用域限制同 [`Self::try_acquire_exclusive_for_delete`]（wkv
  /// `acquire_tree_read` 有界取锁环消费）
  #[inline]
  pub fn try_read_range_index_lock(
    &self,
    key_hash: u64,
  ) -> Option<parking_lot::RwLockReadGuard<'_, ()>> {
    self.locks.try_read(key_hash)
  }

  /// 计算树身份键的 128 位唯一 ID (零堆分配，用于内存字典极速索引，1:1 对标
  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:KeyId)
  ///
  /// 身份契约（rust 自定义面，与 C# 的刻意差异）：`id_key` 是调用方在其域上下文
  /// 中派生的**树身份键**，非裸用户键。本管理器 1:1 对标的 C# RangeIndexManager
  /// 单实例单域（`KeyId = XxHash128(keyBytes)`，跨库共享 StoreWrapper.cs:237 与
  /// GarnetDatabase.cs:145），身份不含域即天然正确；rust 侧多库共享单日志、
  /// 分层集合为本仓自定义面，树身份必须 = f(物理域, 用户键)，否则跨库同名键在
  /// 树注册、升阶换入、claim 封堵、FLUSHDB 回收四面互撞。wkv 侧身份键单点为
  /// 物理 Meta 键形态 `[vns varint][vdb varint][KeyTag::Meta][user_key]`
  /// （`NamespaceDbCodec::encode_tagged_key(vns, vdb, KeyTag::Meta, key)`，与会话
  /// `session_meta_key` 同一编码内核），域前缀刚性隔离与主存物理键同源。不兼容
  /// 旧树文件（转写规范明令），直接新格式。
  #[inline]
  pub fn key_id_of(id_key: &[u8]) -> u128 {
    hash128(id_key, PREFIX_SEED_1, PREFIX_SEED_2)
  }

  /// 根据树身份键计算 26 字符 Base32 前缀 (1:1 对标
  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:HashKeyToPrefix，零堆分配栈
  /// 缓冲形态；域语义见 [`Self::key_id_of`] 身份契约)
  #[inline]
  pub fn base32_prefix_of(id_key: &[u8]) -> Base32Buf128 {
    let id = Self::key_id_of(id_key);
    encode_u128(id)
  }

  /// 向上舍入到下一个 2 的幂 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:RoundUpToPowerOf2)
  #[inline]
  pub const fn round_up_to_power_of_2(v: u32) -> u32 {
    v.next_power_of_two()
  }

  /// 根据最大记录大小动态计算叶子页面大小 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:ComputeLeafPageSize)
  ///
  /// ≤2KB → 4KB；否则 2.5 倍封顶 32KB 后向上取 2 的幂 (纯整数运算，floor(5n/2)
  /// 与 C# 浮点截断一致)。乘法饱和收口：超大 MAXRECORD 下 C# double 2.5 倍
  /// 远超 32KB 封顶同得 32768，而 usize 回绕会派生出小叶页绕穿容量守卫
  #[inline]
  pub fn compute_leaf_page_size(max_record_size: usize) -> usize {
    if max_record_size <= 2048 {
      return 4096;
    }
    let target = (max_record_size.saturating_mul(5) / 2).min(32768) as u32;
    Self::round_up_to_power_of_2(target) as usize
  }

  /// 数据文件标准路径 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:LogDataPath，
  /// {logRoot}/&lt;hash&gt;.data.bftree 工作文件)
  pub fn data_file_path(&self, hash_prefix: &str) -> PathBuf {
    Self::data_file_path_in(&self.ri_log_root, hash_prefix)
  }

  /// 数据文件标准路径的目录显式传参核（[`Self::data_file_path`] 转发至此；
  /// 恢复回退静态清场 [`Self::discard_staged_data_files`] 无实例可持，复用
  /// 同一命名规则杜绝路径拼接散落）
  fn data_file_path_in(ri_log_root: &Path, hash_prefix: &str) -> PathBuf {
    let mut file_name = String::with_capacity(hash_prefix.len() + DATA_FILE_SUFFIX.len());
    file_name.push_str(hash_prefix);
    file_name.push_str(DATA_FILE_SUFFIX);
    ri_log_root.join(file_name)
  }

  /// 根据 128 位 key_id 获取数据文件标准路径 (内部纯二进制)
  #[inline]
  pub fn data_file_path_by_id(&self, key_id: u128) -> PathBuf {
    let b32 = encode_u128(key_id);
    self.data_file_path(&b32)
  }

  /// 根据树身份键获取数据文件标准路径 (零堆分配 Base32 转换，1:1 对标
  /// libs/server/Resp/RangeIndex/RangeIndexManager.cs:LogDataPathFor；域语义见
  /// [`Self::key_id_of`] 身份契约)
  #[inline]
  pub fn data_file_path_for_key(&self, id_key: &[u8]) -> PathBuf {
    self.data_file_path_by_id(Self::key_id_of(id_key))
  }

  /// 刷盘快照文件标准路径 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:LogFlushPath，
  /// {ri_log_root}/{hash_prefix}.{logical_address_b32}.flush.bftree)
  ///
  /// 刷盘快照的**唯一**命名形态：地址段必带，C# 侧无任何无地址形态。刷盘件只由
  /// [`Self::pre_stage_and_register_pending`] 按存根源记录的精确地址单件消费
  /// (对位 C#「uses the exact source address」)，绝不被目录扫描择优；其余按目录
  /// 枚举的路径只有截断回收与换代清理两处 (见 [`super::replication`] 的 flush_files)
  pub fn log_flush_path(&self, hash_prefix: &str, logical_address: u64) -> PathBuf {
    let b32 = encode_u64(logical_address);
    let mut s =
      String::with_capacity(hash_prefix.len() + 1 + BASE32_LEN_U64 + FLUSH_FILE_SUFFIX.len());
    s.push_str(hash_prefix);
    s.push('.');
    s.push_str(&b32);
    s.push_str(FLUSH_FILE_SUFFIX);
    self.ri_log_root.join(s)
  }

  /// 获取在线索引字典引用 (用于检查点遍历与恢复注册)
  #[inline]
  pub fn live_indexes(&self) -> &ConcurrentMap<u128, Arc<TreeEntry>> {
    &self.live_indexes
  }

  /// 获取所有活跃与就绪的索引条目快照
  pub(crate) fn live_entries(&self) -> Vec<Arc<TreeEntry>> {
    let pin = self.live_indexes.pin();
    // 以 pin 时点字典长度预留容量：快照路径逐条目 Arc 克隆，预分配消除逐个
    // push 的倍增搬家 (并发注册晚于 pin 时容量仅是低估提示，不影响正确性)
    let mut entries = Vec::with_capacity(pin.len());
    entries.extend(pin.values().cloned());
    entries
  }

  /// 注册表在册判定（在线激活或 pending 皆算）：冷树回收扫描的「是否占树缓存
  /// 面」前置门 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock
  /// 的 liveIndexes.TryGetValue 缺席即 false 分支——C# 驱逐回收臂先查在册再摘除)
  #[inline]
  pub fn is_registered(&self, id_key: &[u8]) -> bool {
    self
      .live_indexes
      .pin()
      .contains_key(&Self::key_id_of(id_key))
  }

  /// 获取在线树实例（快速共享读路径，1:1 对标 Garnet liveIndexes.TryGetValue；
  /// `id_key` 域语义见 [`Self::key_id_of`] 身份契约）
  #[inline]
  pub fn get_tree(&self, id_key: &[u8]) -> Option<Arc<BfTreeService>> {
    let key_id = Self::key_id_of(id_key);
    let pin = self.live_indexes.pin();
    pin
      .get(&key_id)
      .and_then(|e| e.tree.read().as_ref().cloned())
  }

  /// 登记 RENAME 迁移 claim（段一封堵并发写/排空；返回 false = 键已在迁移中，
  /// 并发 RENAME 同源/反向双键互斥判据；old/new 双键成对登记，释放幂等。
  /// `id_key` 域语义见 [`Self::key_id_of`] 身份契约）
  pub fn try_claim_migration(&self, id_key: &[u8]) -> bool {
    self
      .migrating
      .pin()
      .insert(Self::key_id_of(id_key), ())
      .is_none()
  }

  /// 键是否处于迁移 claim 中（wkv 四入口封堵判据：键暂时不可见）
  #[inline]
  pub fn migration_claimed(&self, id_key: &[u8]) -> bool {
    self.migrating.pin().contains_key(&Self::key_id_of(id_key))
  }

  /// 释放迁移 claim（成功臂排空后与各失败臂收尾；幂等）
  pub fn release_migration_claim(&self, id_key: &[u8]) {
    self.migrating.pin().remove(&Self::key_id_of(id_key));
  }

  /// 按 key_id 查询在线树条目 (papaya 无锁读 + 条目与树双 Arc 保活)
  #[inline]
  pub(crate) fn live_tree_of(&self, key_id: u128) -> Option<(Arc<TreeEntry>, Arc<BfTreeService>)> {
    let pin = self.live_indexes.pin();
    pin
      .get(&key_id)
      .and_then(|e| e.tree.read().as_ref().cloned().map(|t| (Arc::clone(e), t)))
  }

  /// 检查点快照目录标准路径 ({target_dir}/{token_b32}/rangeindex)
  pub fn token_snapshot_dir(target_dir: &Path, token: u128) -> PathBuf {
    let b32 = encode_u128(token);
    let mut p = target_dir.to_path_buf();
    p.reserve(b32.len() + 1 + "rangeindex".len());
    p.push(b32.as_str());
    p.push("rangeindex");
    p
  }

  /// 将词干格式化为树文件名 (`{stem}.bftree`) 写入既有缓冲区 (消除热路径堆分配)
  #[inline]
  pub(crate) fn write_tree_file_name(buf: &mut String, stem: &str) {
    buf.clear();
    buf.reserve(stem.len() + TREE_FILE_SUFFIX.len());
    buf.push_str(stem);
    buf.push_str(TREE_FILE_SUFFIX);
  }

  /// 根据词干构建树文件名 (`{stem}.bftree`)
  #[inline]
  pub(crate) fn tree_file_name(stem: &str) -> String {
    let mut s = String::with_capacity(stem.len() + TREE_FILE_SUFFIX.len());
    Self::write_tree_file_name(&mut s, stem);
    s
  }

  /// 检查点快照文件标准路径 ({target_dir}/{token_b32}/rangeindex/{stem}.bftree)
  #[inline]
  pub(crate) fn snapshot_file_path(target_dir: &Path, token: u128, stem: &str) -> PathBuf {
    let mut p = Self::token_snapshot_dir(target_dir, token);
    p.push(Self::tree_file_name(stem));
    p
  }

  /// 检查点快照目录标准路径 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:CheckpointSnapshotDir，
  /// {cpr_dir}/{token_b32}/rangeindex)
  #[inline]
  pub fn checkpoint_snapshot_dir(&self, token: u128) -> PathBuf {
    Self::token_snapshot_dir(&self.cpr_dir, token)
  }

  /// 获取当前活跃与待激活索引数量 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:LiveIndexCount，
  /// internal 诊断计数，测试断言消费，与 C# RespRangeIndexTests 用途一致)
  #[inline]
  pub fn live_index_count(&self) -> usize {
    self.live_indexes.pin().len()
  }

  /// 检查是否正处于全局检查点快照中
  #[inline]
  pub fn is_checkpoint_in_progress(&self) -> bool {
    self.checkpoint_in_progress.load(Ordering::Acquire)
  }
}

impl Drop for RangeIndexManager {
  fn drop(&mut self) {
    self.dispose();
  }
}
