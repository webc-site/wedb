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
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
};

use coarsetime::Instant;
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
  /// claim 自旋采用退避阶梯 (spin → yield → 微睡)；C# 为纯 Thread.Yield。
  /// claim 持有者是 [`BfTreeService::cpr_snapshot`](BfTreeService::cpr_snapshot)
  /// 的单次 CPR 快照落盘 (引擎阶段协议与点写并发安全，无屏障排空；耗时以快照
  /// 文件 I/O 为上界)，故此处无需第二重超时。
  pub fn snapshot_under_claim(&self, tree: &BfTreeService, destination_path: &Path) -> Result<()> {
    let deadline = Instant::now() + checkpoint::CHECKPOINT_WAIT_TIMEOUT;
    let mut spins = 0u32;
    while !self.try_claim_snapshot() {
      if Instant::now() >= deadline {
        return Err(Error::Timeout);
      }
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
  /// 全局检查点进行中标记
  pub(crate) checkpoint_in_progress: AtomicBool,
  /// 带地址刷盘文件存在疑似标记 (生成计数，惰性恢复的目录扫描门控)
  ///
  /// `get_or_open_tree` 选最新带地址刷盘文件需 O(目录条目数) 扫描；未接线
  /// on_flush 的常态部署下该类文件恒不存在，逐次全目录扫描纯属浪费。生成计数
  /// 单调递增 ([`Self::notice_addr_flush_files`] 每次自增)，`settled_gen` 落后于
  /// `gen` 即表示存在未扫描的新文件。初值 gen=1 > settled=0 (保守开启，首例恢复
  /// 做一次扫描证伪)，证伪后关闭扫描通道恢复 O(1) stat 路径。
  ///
  /// 竞态闭环：notice 必须后置于刷盘文件**完整落盘之后**调用 (on_flush_address /
  /// 预分阶段均如此)。扫描以「开始到结束 gen 不变」为证伪前提——若 notice 先于
  /// 建文件，扫描可在 notice 之后、文件诞生之前完成目录枚举并证伪封存通道
  /// (gen 未变)，随后落盘的文件被永久跳过；notice 后置则任何「枚举时未见文件」
  /// 的证伪必然先于 notice 完成，notice 的 gen 自增随即重开通道，下次扫描必见
  /// 新文件。工作文件仅含引擎环形缓冲已写回的部分页，快照才是恢复点权威版本
  /// (见 [`super::replication`] 预置覆盖论证)，跳过已落盘的刷盘文件意味着从
  /// 陈旧工作文件恢复丢失已刷数据，此窗口必须闭合。
  pub(crate) addr_flush_gen: AtomicU64,
  /// 已证伪封存的扫描代号 (仅由 [`Self::settle_addr_flush_scan`] 在 gen 不变时推进)
  pub(crate) addr_flush_settled_gen: AtomicU64,
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
}

impl RangeIndexManager {
  /// 创建管理器实例 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs 构造器)
  pub fn new(ri_log_root: impl Into<PathBuf>, cpr_dir: impl Into<PathBuf>) -> Result<Self> {
    Self::with_epoch(ri_log_root, cpr_dir, None)
  }

  /// 关联存储纪元创建管理器实例 (1:1 对标 Garnet RangeIndexManager(..., storeEpoch))
  pub fn with_epoch(
    ri_log_root: impl Into<PathBuf>,
    cpr_dir: impl Into<PathBuf>,
    store_epoch: Option<Arc<LightEpoch>>,
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
      checkpoint_in_progress: AtomicBool::new(false),
      addr_flush_gen: AtomicU64::new(1),
      addr_flush_settled_gen: AtomicU64::new(0),
      locks: StripedRwLock::new(),
      release_retries: Mutex::new(Vec::new()),
      store_epoch,
      checkpoint_event: Event::new(),
    })
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

  /// 是否需要扫描带地址刷盘文件 (惰性恢复的 O(1) 门控探针)
  #[inline]
  pub(crate) fn addr_flush_scan_pending(&self) -> bool {
    self.addr_flush_gen.load(Ordering::Acquire)
      != self.addr_flush_settled_gen.load(Ordering::Acquire)
  }

  /// 读取扫描起始代号 (证伪前提：本次扫描全程生成号不变)
  #[inline]
  pub(crate) fn addr_flush_scan_token(&self) -> u64 {
    self.addr_flush_gen.load(Ordering::Acquire)
  }

  /// 全量扫描未发现任何带地址刷盘文件且扫描期间无新文件产生 (`token` 未变)，
  /// 关闭扫描通道；代号已变则放弃证伪，保持扫描通道开启等待下轮复扫
  #[inline]
  pub(crate) fn settle_addr_flush_scan(&self, token: u64) {
    if self.addr_flush_gen.load(Ordering::Acquire) == token {
      self.addr_flush_settled_gen.store(token, Ordering::Release);
    }
  }

  /// 带地址刷盘文件已完整落盘 (on_flush_address / 预分阶段)，生成号自增重新开启恢复扫描通道
  ///
  /// 调用方必须在文件创建/换入**之后**调用：扫描侧凭生成号不变证伪，notice 后置
  /// 保证「证伪完成 → notice 重开」严格有序，任何证伪后落盘的文件必被下轮扫描
  /// 捕获 (先 notice 后建文件的反序窗口内文件会被永久跳过，见字段文档竞态闭环)
  #[inline]
  pub(crate) fn notice_addr_flush_files(&self) {
    self.addr_flush_gen.fetch_add(1, Ordering::AcqRel);
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
    let pin = self.live_indexes.pin();
    for entry in pin.values() {
      if let Some(tree) = entry.tree.write().take() {
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

  /// 获取键哈希分段读写条带锁引用 (供迁移发布等外部路径持锁复用，1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:AcquireExclusiveForDelete)
  #[inline]
  pub fn locks(&self) -> &StripedRwLock<(), NUM_LOCK_STRIPES> {
    &self.locks
  }

  /// 计算键的 128 位唯一 ID (零堆分配，用于内存字典极速索引，1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:KeyId)
  #[inline]
  pub fn key_id_of(key: &[u8]) -> u128 {
    hash128(key, PREFIX_SEED_1, PREFIX_SEED_2)
  }

  /// 根据键计算 26 字符 Base32 前缀 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:HashKeyToPrefix，零堆分配栈缓冲形态)
  #[inline]
  pub fn base32_prefix_of(key: &[u8]) -> Base32Buf128 {
    let id = Self::key_id_of(key);
    encode_u128(id)
  }

  /// 向上舍入到下一个 2 的幂 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:RoundUpToPowerOf2)
  #[inline]
  pub const fn round_up_to_power_of_2(v: u32) -> u32 {
    v.next_power_of_two()
  }

  /// 根据最大记录大小动态计算叶子页面大小 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:ComputeLeafPageSize)
  ///
  /// ≤2KB → 4KB；否则 2.5 倍封顶 32KB 后向上取 2 的幂 (纯整数运算，floor(5n/2) 与 C# 浮点截断一致)
  #[inline]
  pub fn compute_leaf_page_size(max_record_size: usize) -> usize {
    if max_record_size <= 2048 {
      return 4096;
    }
    let target = ((max_record_size * 5 / 2).min(32768)) as u32;
    Self::round_up_to_power_of_2(target) as usize
  }

  /// 数据文件标准路径 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:LogDataPath，
  /// {logRoot}/&lt;hash&gt;.data.bftree 工作文件)
  pub fn data_file_path(&self, hash_prefix: &str) -> PathBuf {
    let mut file_name = String::with_capacity(hash_prefix.len() + DATA_FILE_SUFFIX.len());
    file_name.push_str(hash_prefix);
    file_name.push_str(DATA_FILE_SUFFIX);
    self.ri_log_root.join(file_name)
  }

  /// 根据 128 位 key_id 获取数据文件标准路径 (内部纯二进制)
  #[inline]
  pub fn data_file_path_by_id(&self, key_id: u128) -> PathBuf {
    let b32 = encode_u128(key_id);
    self.data_file_path(&b32)
  }

  /// 根据原始 key 获取数据文件标准路径 (零堆分配 Base32 转换，1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:LogDataPathFor)
  #[inline]
  pub fn data_file_path_for_key(&self, key: &[u8]) -> PathBuf {
    self.data_file_path_by_id(Self::key_id_of(key))
  }

  /// 刷盘快照文件标准路径 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:LogFlushPath，
  /// {ri_log_root}/{hash_prefix}.{logical_address_b32}.flush.bftree)
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

  /// 裸名刷盘快照文件路径 ({ri_log_root}/{hash_prefix}.flush.bftree)
  ///
  /// 裸名与带地址命名对同一 hash_prefix 互斥 (见 lifecycle::get_or_open_tree 的
  /// 刷盘快照选择契约)，由 on_flush 体系产生，每次 fs::copy 截断覆盖
  pub fn bare_flush_path(&self, hash_prefix: &str) -> PathBuf {
    let mut flush_name = String::with_capacity(hash_prefix.len() + FLUSH_FILE_SUFFIX.len());
    flush_name.push_str(hash_prefix);
    flush_name.push_str(FLUSH_FILE_SUFFIX);
    self.ri_log_root.join(flush_name)
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

  /// 获取在线树实例（快速共享读路径，1:1 对标 Garnet liveIndexes.TryGetValue）
  #[inline]
  pub fn get_tree(&self, key: &[u8]) -> Option<Arc<BfTreeService>> {
    let key_id = Self::key_id_of(key);
    let pin = self.live_indexes.pin();
    pin
      .get(&key_id)
      .and_then(|e| e.tree.read().as_ref().cloned())
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
