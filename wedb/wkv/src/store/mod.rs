use std::{
  env, fs,
  path::PathBuf,
  process,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
  },
};

use itoa::Buffer;
use papaya::Operation;
use parking_lot::{Mutex, RwLock};
use wbase::{
  align::DEFAULT_SECTOR_SIZE,
  group_commit::GroupCommitPipeline,
  map::{ConcurrentMap, new_concurrent_map},
  time::now_ms,
};
use wbftree::RangeIndexManager;
use wdev::Device;
use wepoch::LightEpoch;
use whlog::HybridLog;
use windex::HashIndex;
use wreviv::FreeRecordPool;

use crate::{
  config::{GcConfig, StoreConfig},
  error::{Error, Result},
  gc::GcHandle,
  read_cache::ReadCache,
  session::{SessionSlot, StoreSession},
};

pub mod addr;
pub mod event;
pub mod flush;
pub mod gc;
pub mod keyspace;

pub use event::*;

/// 集合版本映射并发字典（基于无锁高效 papaya 与硬件向量加速 GxBuildHasher）
pub type KeyIdVersionsMap = ConcurrentMap<u64, (u64, bool)>;

/// key_id 分配安全余量（恢复时在持久化水位之上预留的分配额度）
///
/// Checkpoint 仅持久化创建时点的 next_key_id 水位，其后至进程崩溃之间的新集合分配
/// 不随快照落盘；恢复时以 `持久化水位 + KEY_ID_ASSIGN_MARGIN` 作为 fetch_max 下限，
/// 保证新进程分配的 key_id 严格大于上一进程所有可能已分配的值——即使墙钟回退
/// （NTP 步进 / VM 快照回滚）使 generate_initial_key_id 落入历史区间也不会复用。
pub const KEY_ID_ASSIGN_MARGIN: u64 = 1 << 20;

/// 默认基于分段存储设备的单机存储引擎类型别名
pub type DefaultWedbStore = WedbStore<wdev::SegmentedDevice>;

/// Microsoft Garnet Tsavorite 顶层混合存储引擎
///
/// 紧凑整合无锁哈希索引（HashIndex）、混合日志环形缓冲区（HybridLog）、
/// 纪元并发保护器（LightEpoch）与底层块存储设备（Device）。
/// AOF 监听端口暂停守卫（drop 恢复；见 [`WedbStore::pause_aof_listeners`]）
pub struct AofListenerPauseGuard<D: Device> {
  pub(crate) store: Arc<WedbStore<D>>,
}

impl<D: Device> Drop for AofListenerPauseGuard<D> {
  fn drop(&mut self) {
    self
      .store
      .aof_listeners_paused
      .store(false, Ordering::Release);
  }
}

pub struct WedbStore<D: Device> {
  /// 存储引擎配置
  pub config: StoreConfig,
  /// 64 字节 Cacheline 对齐无锁哈希索引
  ///
  /// 索引定容是显式设计边界：打开时按 `config.index_size` 一次性定容，构造后不再
  /// 扩容（checkpoint 恢复亦按持久化的 index_size 原样重建）。选型依据：前台
  /// `find_or_create_tag + try_cas/try_elide` 的槽位句柄在整个操作窗口内恒指向
  /// 同一张活跃表，写路径无需任何 epoch/quiesce 或 CAS 后重查活跃表的同步开销，
  /// 快照与恢复也天然与活跃表一一对应。容量规划由调用方负责：容量不足不会丢数据，
  /// 但哈希碰撞链变长、写入延迟劣化，溢出桶池耗尽后写入将显式报错——键规模增长前
  /// 须按预估负载放大 index_size 重建索引。
  pub index: Arc<HashIndex>,
  /// 混合日志分配器（内存可变/只读/磁盘三区滑动）
  pub hlog: Arc<HybridLog<D>>,
  /// Group Commit 刷盘流水线（对标 Garnet TsavoriteLog.ongoingCommitRequests）
  pub flush_pipeline: GroupCommitPipeline,
  /// 硬件已完成 sync 持久化的最高连续逻辑地址水位
  pub synced_until: AtomicU64,
  /// 纪元并发保护管理器
  pub epoch: Arc<LightEpoch>,
  /// 底层块存储设备
  pub device: Arc<D>,
  /// 下一个集合唯一 ID 分配计数器（原子无锁递增）
  pub next_key_id: AtomicU64,
  /// 基于磁盘与内存的 RangeIndex 管理器 (1:1 对标 Garnet RangeIndexManager)
  pub range_index: Arc<wbftree::RangeIndexManager>,
  /// 内存与日志槽位复活回收池 (严格对标 Garnet Tsavorite FreeRecordPool / RevivificationManager)
  pub reviv_pool: Arc<wreviv::FreeRecordPool>,
  /// 独立只读非脏页内存日志 (严格对标 Garnet Tsavorite ReadCache)
  pub read_cache: Arc<ReadCache>,
  /// 集合 ID 到其最新版本号与存活状态的映射：key_id -> (current_version, is_alive)
  /// 用于 Fast Drop 与 GC LogCompactor 追踪已删集合的版本水位并安全回收死条目
  /// （基于 papaya 无锁并发字典消除锁竞争）
  pub key_id_versions: KeyIdVersionsMap,
  /// 内置 GC 后台循环句柄槽（`config.gc.enabled` 且在 compio 运行时内经
  /// [`Self::start_gc`] 拉起；Mutex 槽支持停止后按新配置重拉——对标 C#
  /// TaskManager 的 CancelAsync/RegisterAndRun 任务生命周期）
  pub(crate) gc: Mutex<Option<GcHandle<D>>>,
  /// 内置 GC 运行态配置共享句柄（构造时自 `config.gc` 初始化；GC 驱动循环每轮
  /// 重读，[`Self::update_gc_config`] 热更新下一轮生效，对标 Garnet
  /// RuntimeServerConfig 的 CONFIG SET 语义）
  pub(crate) gc_cfg: Arc<RwLock<GcConfig>>,
  /// 统一存储事件处理器（宿主经 [`Self::set_event_sink`] 注入 AOF 追加适配器）
  pub(crate) event_sink: OnceLock<StoreEventSink>,
  /// 当前存储版本（对标 C# TsavoriteKV.CurrentVersion：checkpoint 拍摄/恢复
  /// 推进，AOF 条目 store_version 与重放端版本基线跳过共用此源；0 = 无
  /// checkpoint 历史，全量重放）
  pub(crate) current_version: Arc<AtomicI64>,
  /// purge 链物理写镜像抑制槽：存放正处于 purge 链中的会话身份
  /// （&StoreSession 裸地址，0 = 无）。
  ///
  /// 会话级精确抑制不变式：[`crate::session::StoreSession`] 的写监听通知
  /// 仅当自身地址与本槽相等时跳过——purge_expired 窗口内本会话的 TTL 记录
  /// 与数据两条物理墓碑不再镜像；其他会话（含并发同键写、内置 GC 会话）
  /// 的写镜像绝不受影响。守卫以 save/restore 兜底（见 ttl::PurgeNotifyGuard），
  /// panic/早退路径不残留；同一活跃会话地址唯一（守卫存活期会话必被借用），
  /// 不存在地址复用误抑制。
  pub(crate) purge_suppress: AtomicUsize,
  /// AOF 监听端口全局暂停闸（重放/恢复镜像抑制，见 [`Self::pause_aof_listeners`]）
  pub(crate) aof_listeners_paused: AtomicBool,
  /// 内部创建的 RangeIndex 临时根目录（若非用户显式配置则在 Drop 时自动清理闭环）
  pub(crate) temp_range_index_dir: Option<PathBuf>,
  /// INFO KEYSPACE 专用扫描会话槽位（懒建复用，对标 Garnet GarnetDatabase
  /// `.KeyspaceScanStorageSession` + `KeyspaceScanLock`；并发调用后到者降级为
  /// 一次性临时会话，读路径无共享可变状态，无正确性风险）
  pub(crate) keyspace_scan_session: SessionSlot<D>,
}

impl<D: Device> WedbStore<D> {
  /// 生成初始集合唯一 ID（高 48 位毫秒时间戳 + 低 16 位随机数）
  #[inline]
  fn generate_initial_key_id() -> u64 {
    let now = now_ms();
    let rand = fastrand::u16(..) as u64;
    ((now << 16) | rand).max(1)
  }

  /// 初始化 RangeIndex 管理器及可能的临时目录路径 (关联存储纪元)
  fn init_range_index(
    config: &StoreConfig,
    epoch: Option<Arc<LightEpoch>>,
  ) -> Result<(Arc<wbftree::RangeIndexManager>, Option<PathBuf>)> {
    let (ri_log_root, cpr_dir, temp_range_index_dir) = if let Some(dir) = &config.range_index_dir {
      (dir.join("rangeindex"), dir.join("checkpoints"), None)
    } else {
      let mut buf = Buffer::new();
      let mut name = String::from("wedb_rangeindex_");
      name.push_str(buf.format(process::id()));
      name.push('_');
      name.push_str(buf.format(now_ms()));
      name.push('_');
      name.push_str(buf.format(fastrand::u64(..)));
      let tmp = env::temp_dir().join(name);
      (tmp.join("log"), tmp.join("cpr"), Some(tmp))
    };
    Ok((
      Arc::new(RangeIndexManager::with_epoch(ri_log_root, cpr_dir, epoch)?),
      temp_range_index_dir,
    ))
  }

  /// 恢复装配容量预检：`config.index_size` 与实际索引容量严格一致
  ///
  /// `config` 与 `index` 可能来自不同来源（如宿主自定义恢复流程对接 wcpr 恢复出
  /// 的索引快照），不一致时禁止装配——声明小表 + 实际大表会使后续 Checkpoint 写出
  /// 互斥的 meta 与快照，问题在下次恢复才于深处暴露；声明大表 + 实际小表则是静默
  /// 缩表。索引打开时定容且无在线扩容，容量不一致一律显式报
  /// [`Error::IndexSizeMismatch`](crate::Error::IndexSizeMismatch)。
  fn check_index_capacity(config: &StoreConfig, index: &HashIndex) -> Result<()> {
    if config.index_size != index.size {
      return Err(Error::IndexSizeMismatch {
        config: config.index_size,
        actual: index.size,
      });
    }
    Ok(())
  }

  /// 核心组件装配公共体（open / from_components 共享尾段）
  ///
  /// 调用方须已完成 [`StoreConfig::validate`] 预检；
  /// RangeIndex 管理器、复活池、ReadCache、GC 运行态句柄等纯派生组件在此统一装配。
  /// ReadCache 创建带降级兜底：validate 已保证 page_size/read_cache_num_pages 为
  /// 非零 2 的幂，构造失败仅可能是资源层异常，降级为禁用配置留痕运行而非中止装配。
  fn assemble(
    config: StoreConfig,
    index: Arc<HashIndex>,
    hlog: Arc<HybridLog<D>>,
    epoch: Arc<LightEpoch>,
    device: Arc<D>,
  ) -> Result<Self> {
    let (range_index, temp_range_index_dir) =
      Self::init_range_index(&config, Some(Arc::clone(&epoch)))?;
    let reviv_pool = Arc::new(FreeRecordPool::new());
    let read_cache = Arc::new(
      ReadCache::new(
        config.page_size,
        config.read_cache_num_pages,
        config.enable_read_cache,
      )
      .unwrap_or_else(|e| {
        log::warn!("ReadCache 按会话配置创建失败，降级为默认禁用配置: err={e}");
        // SAFETY: DEFAULT_SECTOR_SIZE 与 8 均为非零 2 的幂且 enable=false 关闭全部校验分支，构造恒成功
        unsafe { ReadCache::new(DEFAULT_SECTOR_SIZE, 8, false).unwrap_unchecked() }
      }),
    );
    let gc_cfg = Arc::new(RwLock::new(config.gc.clone()));
    let flush_pipeline = GroupCommitPipeline::new();
    let synced_until = hlog.flushed_until_address();
    Ok(Self {
      config,
      index,
      hlog,
      flush_pipeline,
      synced_until: AtomicU64::new(synced_until),
      epoch,
      device,
      next_key_id: AtomicU64::new(Self::generate_initial_key_id()),
      range_index,
      reviv_pool,
      read_cache,
      key_id_versions: new_concurrent_map(),
      gc: Mutex::new(None),
      gc_cfg,
      event_sink: OnceLock::new(),
      current_version: Arc::new(AtomicI64::new(0)),
      purge_suppress: AtomicUsize::new(0),
      aof_listeners_paused: AtomicBool::new(false),
      temp_range_index_dir,
      keyspace_scan_session: SessionSlot::new(),
    })
  }

  /// 打开或创建存储引擎实例
  ///
  /// 容量防线：入口先行 [`StoreConfig::validate`] 预检——`StoreConfig` 字段公开，
  /// 调用方可绕过 builder 手搓非法容量（如 index_size 非 2 的幂），必须在任何
  /// 资源分配前拦截。
  pub fn open(config: StoreConfig, device: Arc<D>) -> Result<Self> {
    config.validate()?;
    let index = Arc::new(HashIndex::new(config.index_size)?);
    let epoch = Arc::new(LightEpoch::new(config.max_sessions));
    let hlog_config = config.to_hlog_config()?;
    let hlog = Arc::new(HybridLog::new(
      hlog_config,
      Arc::clone(&device),
      Arc::clone(&epoch),
    )?);
    Self::assemble(config, index, hlog, epoch, device)
  }

  /// 从已恢复或外部构建的核心组件创建存储引擎实例（供 Checkpoint 恢复或高级定制使用）
  ///
  /// 容量防线（恢复预检）见 `Self::check_index_capacity`。
  pub fn from_components(
    config: StoreConfig,
    index: Arc<HashIndex>,
    hlog: Arc<HybridLog<D>>,
    epoch: Arc<LightEpoch>,
    device: Arc<D>,
  ) -> Result<Self> {
    config.validate()?;
    Self::check_index_capacity(&config, &index)?;
    Self::assemble(config, index, hlog, epoch, device)
  }

  /// 抬升 key_id 分配水位下限（fetch_max 单调语义，低值永不回退已推进的水位）
  ///
  /// 恢复路径必须调用：`floor = 持久化 next_key_id + KEY_ID_ASSIGN_MARGIN`。
  /// key_id 是集合元数据记录（meta 物理键对应值）的组成部分，墙钟回退时
  /// `Self::generate_initial_key_id` 可能生成与上一进程相同的 key_id，复用即
  /// 命名空间冲突；与持久化水位取最大值后，时钟正常时以时间戳为准，回退时以水位为准。
  #[inline]
  pub fn raise_key_id_floor(&self, floor: u64) {
    self.next_key_id.fetch_max(floor, Ordering::Relaxed);
  }

  /// 更新集合 key_id 的最新版本与存活状态（版本单调递增，拒绝陈旧版本覆盖与同版本幽灵复活）
  ///
  /// 基于 papaya `compute` 将守卫判定与写入合并为单次原子读改写：旧实现 check-then-insert
  /// 在并发交错下可发生高版本水位被低版本回退（insert 无条件覆盖），导致紧缩收尾
  /// 死条目回收误判存活集合。接受条件：更高版本，或同版本
  /// alive -> false 收敛；幂等（同版本同状态）与拒绝路径（陈旧版本 / 幽灵复活）以 Abort
  /// 原子中止，零写入（保持旧实现读命令常态下不触发表写扩容的快路径语义）。
  #[inline]
  pub fn update_key_id_meta(&self, key_id: u64, version: u64, is_alive: bool) {
    self
      .key_id_versions
      .pin()
      .compute(key_id, |entry| match entry {
        Some((_, &(curr_ver, curr_alive))) => {
          if version > curr_ver || (version == curr_ver && curr_alive && !is_alive) {
            Operation::Insert((version, is_alive))
          } else {
            Operation::Abort(())
          }
        }
        None => Operation::Insert((version, is_alive)),
      });
  }

  /// 获取集合 key_id 的最新元数据状态 (current_version, is_alive)（无锁读取）
  #[inline]
  pub fn get_key_id_meta(&self, key_id: u64) -> Option<(u64, bool)> {
    self.key_id_versions.pin().get(&key_id).copied()
  }

  /// 移除集合 key_id 的元数据记录（无锁删除，防止已废弃集合版本元数据无限占用内存）
  #[inline]
  pub fn remove_key_id_meta(&self, key_id: u64) {
    self.key_id_versions.pin().remove(&key_id);
  }

  /// 创建新的客户端并发会话句柄
  ///
  /// 刻意不在此处自动启动内置 GC：后台循环任务为 compio 'static 任务，必须捕获
  /// 引擎弱引用，要求 `D: 'static`；而本方法的签名被 wedb_compact 的泛型紧缩路径
  /// （`LogCompactor<D: Device>` 调 `new_session`）钉死在 `D: Device`，加界将破坏
  /// 其编译。自动启动收敛到 [`Self::open_shared`]（Arc 化入口）与 [`Self::start_gc`]。
  pub fn new_session(self: &Arc<Self>) -> Result<StoreSession<D>> {
    let participant = self.epoch.register()?;
    Ok(StoreSession::new(Arc::clone(self), participant))
  }

  /// 打开存储引擎并完成 Arc 化（`config.gc.enabled` 时自动启动内置 GC）
  ///
  /// 嵌入式内置启动的推荐入口：`open` 返回裸 `Self`（无 Arc 可供后台任务弱引用），
  /// 本方法在 Arc 化完成后按 [`GcConfig::enabled`] 幂等拉起 GC 后台循环。须在
  /// compio 运行时内调用（无运行时时 GC 留待 [`Self::start_gc`] 手动补启）。
  pub fn open_shared(config: StoreConfig, device: Arc<D>) -> Result<Arc<Self>>
  where
    D: Device + 'static,
  {
    let store = Arc::new(Self::open(config, device)?);
    store.start_gc();
    Ok(store)
  }
}

impl<D: Device> Drop for WedbStore<D> {
  fn drop(&mut self) {
    // 先停内置 GC：协作标志置位 + 任务取消兜底，防止后台循环在引擎资源拆除后
    // 继续触达 hlog/会话（句柄强引用不构成环，GC 任务本身亦持引擎弱引用）
    if let Some(h) = self.gc.get_mut() {
      h.stop();
    }
    self.range_index.dispose();
    if let Some(tmp_dir) = &self.temp_range_index_dir {
      let _ = fs::remove_dir_all(tmp_dir);
    }
  }
}
