use std::{
  env, fs,
  path::PathBuf,
  process,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
  },
};

use arc_swap::ArcSwap;
use compio::runtime::Runtime;
use itoa::Buffer;
use parking_lot::{Mutex, RwLock};
use wbase::{
  align::DEFAULT_SECTOR_SIZE, future::yield_now, group_commit::GroupCommitPipeline, time::now_ms,
};
use wbftree::{DetachedTree, RangeIndexManager};
use wdev::Device;
use wepoch::LightEpoch;
use whlog::HybridLog;
use windex::HashIndex;
use wreviv::FreeRecordPool;
use wval::{KeyTag, NamespaceDbCodec};

use self::reclaim::BftreeDomains;
use crate::{
  config::{GcConfig, StoreConfig},
  error::{Error, Result},
  gc::{GcHandle, spawn_bftree_reclaimer},
  read_cache::ReadCache,
  session::{DeleteMissHook, SessionSlot, StoreSession, WatchHook},
  vdb::{DbMetaRecord, GcDeadEntry, ROOT_VIRTUAL_ID, VirtualDbManager},
};
pub mod addr;
pub mod cpr_host;
pub mod event;
pub mod flush;
pub mod gc;
pub mod hlog_scan;
pub mod keyspace;
pub mod reclaim;
pub mod resize;
pub mod stats;
pub mod vdb_load;

pub use event::*;
pub use hlog_scan::HybridLogScanMetrics;
pub use resize::*;

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
  /// 64 字节 Cacheline 对齐无锁哈希索引 (支持在线动态扩容)
  pub index: arc_swap::ArcSwap<HashIndex>,
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
  /// 双层虚拟化数据库路由管理器
  pub vdb: Arc<VirtualDbManager>,
  /// 独立只读非脏页内存日志 (严格对标 Garnet Tsavorite ReadCache)
  pub read_cache: Arc<ReadCache>,
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
  /// WATCH 版本推进钩子（宿主经 [`crate::session::WatchHook`] 注入；写面
  /// 收口在完成实际写入后按键回调，对标 C# functionsState.watchVersionMap
  /// 与存储引擎同实例共享的装配关系。OnceLock 保首：装配期一次性注入）
  pub(crate) watch_hook: OnceLock<WatchHook>,
  /// 用户键删除缺席观测钩子（宿主经 [`crate::session::DeleteMissHook`] 注入；
  /// 用户键双域删除判未命中后回调，对标 C# GarnetRecordTriggers.cs:OnDispose
  /// 的 Deleted 臂（值域外登记态清退收口）。OnceLock 保首：装配期一次性注入）
  pub(crate) delete_miss_hook: OnceLock<DeleteMissHook>,
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
  /// 冷租户/冷库点查装载专用会话槽位（[`Self::resolve_context`] /
  /// [`Self::resolve_ns_mapping`] 的磁盘 DbMeta 点查执行域，懒建复用；
  /// 同步域外异步会话，绝不持纪元守卫跨 await 磁盘）
  pub(crate) vdb_load_session: SessionSlot<D>,
  /// 换号回收旁表：(vns, vdb) → 域内 BfTree 裸用户键集（RI 树与升阶树注册键
  /// 无域前缀、主存无键遍历 API 无法按域反查，创建面登记供 FLUSHDB/FLUSHNS
  /// 换号联动延迟销毁；登记/取数/对账内核见 [`reclaim`]）
  pub(crate) bftree_domains: BftreeDomains,
  /// 换号域待物理释放队列：detach_tree 已同步摘注册的树批次，纪元排空、
  /// 引擎释放与数据文件删除由内置 GC 轮次经
  /// [`Self::drain_bftree_release`] 承接——回放/复制流水线只投递不释放
  /// （doc/zh/db.md 主从异步屏障：GcBarrier 语义由 FlushDb/FlushNs 条目
  /// 承载，不新增条目类型；与旁表同形态的冷路径互斥容器，杜绝第二套
  /// 释放编排）
  pub(crate) bftree_release: Mutex<Vec<DetachedTree>>,
  /// 换号元数据串行锁认领位（true = 有换号事务在编排；garnet 无对应，wedb
  /// 自研换号元数据串行锁——C# 每库独立 Tsavorite 实例天然无换号撕裂竞态）
  ///
  /// 刻意不用 parking_lot/std 同步锁：compio 线程核绑运行时下同步锁跨 await
  /// 持有会挂死整核（同核两换号任务即成死锁）且守卫 !Send 无法编译；以
  /// AtomicBool 认领位加协作让渡自旋承载（先例即 barrier_enter 的 PREPARE_GROW
  /// 自旋挂起协议），获取与释放见 [`Self::lock_dbmeta`]
  pub(crate) dbmeta_lock: AtomicBool,
  /// 在线哈希索引扩容状态机运行时容器 (对标 Garnet IndexResizeTask)
  pub resize: Arc<resize::IndexResizeState>,
}

/// 换号元数据串行锁守卫（Drop 以 Release store 释放认领，`?` 早退不遗留）
pub(crate) struct DbmetaGuard<'a>(&'a AtomicBool);

impl Drop for DbmetaGuard<'_> {
  fn drop(&mut self) {
    self.0.store(false, Ordering::Release);
  }
}

impl<D: Device> WedbStore<D> {
  /// RangeIndex 管理器显式访问器（上层编排经此消费引擎 RI 域，杜绝跨层
  /// 字段直取；对标 C# storeWrapper.RangeIndexManager 可达面；返回共享句柄
  /// 引用，克隆方按需 Arc::clone）
  #[inline]
  pub fn range_index(&self) -> &Arc<wbftree::RangeIndexManager> {
    &self.range_index
  }

  /// 获取换号元数据串行锁（garnet 无对应，wedb 自研换号元数据串行锁；
  /// 认领位语义见 [`WedbStore::dbmeta_lock`] 字段注释）
  ///
  /// Acquire 认领抢占，竞争时以 [`wbase::future::yield_now`] 协作让渡自旋——
  /// 同核另一换号任务必在让渡间隙前进，绝不 park 线程；临界区仅含换号 CAS
  /// 与 DbMeta 原子批落盘（[`crate::session::StoreSession::persist_dbmeta_batch`]），
  /// 全部持有者（flush_database / flush_namespace / swap_databases）互不嵌套、
  /// 临界区内不再获取本锁，无死锁。锁只在管理命令面，用户数据热路径不触锁
  pub(crate) async fn lock_dbmeta(&self) -> DbmetaGuard<'_> {
    while self
      .dbmeta_lock
      .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
      .is_err()
    {
      yield_now().await;
    }
    DbmetaGuard(&self.dbmeta_lock)
  }

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
  /// 缩表。恢复装配时配置容量与索引快照容量不一致一律显式报
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
      index: ArcSwap::from(index),
      hlog,
      flush_pipeline,
      synced_until: AtomicU64::new(synced_until),
      epoch,
      device,
      next_key_id: AtomicU64::new(Self::generate_initial_key_id()),
      range_index,
      reviv_pool,
      vdb: Arc::new(VirtualDbManager::new()),
      read_cache,
      gc: Mutex::new(None),
      gc_cfg,
      event_sink: OnceLock::new(),
      watch_hook: OnceLock::new(),
      delete_miss_hook: OnceLock::new(),
      current_version: Arc::new(AtomicI64::new(0)),
      purge_suppress: AtomicUsize::new(0),
      aof_listeners_paused: AtomicBool::new(false),
      temp_range_index_dir,
      keyspace_scan_session: SessionSlot::new(),
      vdb_load_session: SessionSlot::new(),
      bftree_domains: reclaim::BftreeDomains::default(),
      bftree_release: Mutex::new(Vec::new()),
      dbmeta_lock: AtomicBool::new(false),
      resize: Arc::new(resize::IndexResizeState::new()),
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
  ///
  /// 启动就绪门禁：DbMeta 内存映射重建（[`Self::rebuild_vdb_async`]）在本方法
  /// 返回**之前**于当前运行时内驱动到完成，故本方法返回的句柄本身就是
  /// 「映射面已就绪」的凭据——宿主据此开端口，服务面不可能先于映射面就绪
  /// （doc/zh/db.md「重启扫描 DbMeta，重构死亡账本、分配水位与根域映射表，
  /// 无缝恢复清理任务」；时序口径对标 libs/host/GarnetServer.cs:527-533 的
  /// `Provider.RecoverAsync().AsTask().GetAwaiter().GetResult()` 先于
  /// `servers[i].Start()`，与 [`Self::recover`] / [`Self::recover_latest`]
  /// 「await 重建后才交出句柄」的恢复段完全同构，不另设第二套就绪状态位）。
  ///
  /// 历史实现的 `drop(spawn(...))` 不是 fire-and-forget 而是 fire-and-cancel：
  /// compio `JoinHandle::drop` 内建 `task.cancel(true)`（compio-executor
  /// join_handle.rs 的 Drop 实现），句柄即抛等于在任务被首次 poll 前撤销它——
  /// 冷重启路径的映射重建从未落地，内存映射恒空。
  ///
  /// 重建失败（日志扫描 I/O 错误）直接上抛：映射面残缺时开服务面只会把脏写
  /// 落到错误虚拟前缀，与 C# 恢复失败即终止启动同语义，不做降级放行。
  ///
  /// 无 compio 运行时的调用方拿不到设备 IO 驱动，本方法无法代其重建，仅告警
  /// 留痕：该形态须在开服务面前自行 `rebuild_vdb_async().await`（[`Self::recover`]
  /// 的宿主段口径）。此窗口内先行分配虚拟 ID 会与磁盘既有编号撞号（跨库串数据）；
  /// 分配水位持久化已收敛为换号批与建档批的 0x05 收尾记录（重建经
  /// [`Self::rebuild_apply_record`] 的 0x05 臂折叠，见 doc/zh/db.md
  /// 「即时原子提交」段），无第二套水位落盘口径。
  pub fn open_shared(config: StoreConfig, device: Arc<D>) -> Result<Arc<Self>>
  where
    D: Device + 'static,
  {
    let store = Arc::new(Self::open(config, device)?);
    match Runtime::try_current() {
      Some(runtime) => {
        // 门禁在 start_gc 之前：内置 GC 的过期键清退同样以虚拟域判活，
        // 映射面未就绪即扫盘会把「尚未重建」误判为「已换号死亡」
        runtime.block_on(store.rebuild_vdb_async())?;
        // 换号域物理释放后台任务：不受 gc.enabled 门禁、与 TTL 扫描解耦的
        // 常驻消费者（doc/zh/db.md 主从异步屏障，回放投递的批次由它落地）
        spawn_bftree_reclaimer(&store);
      }
      None => {
        log::warn!(
          "open_shared 无 compio 运行时：vdb 映射重建未执行，开服务面前须自行 await rebuild_vdb_async"
        );
      }
    }
    store.start_gc();
    Ok(store)
  }

  /// 扫描 DbMeta 记录重建死亡待回收账本与分配水位（冷租户条款：映射不全量装载）
  ///
  /// 恢复链入口不再调用本方法：恢复期的 DbMeta 重建已与模糊区重插、RI 桩
  /// 自愈合一为 [`Self::run_recovery_pass`] 的单趟有序扫描（对标 C# 恢复
  /// 单遍扫描派发逐记录回调），本方法保留为冷启动（非恢复）映射重建入口，
  /// 两者共用同一单条工序 [`Self::rebuild_vdb_visit`] 与同一收尾
  /// [`Self::finish_vdb_rebuild`]，不存在第二套重建口径。
  ///
  /// 冷数据全留磁盘：NS_MAP / DB_MAP 记录只抬升分配水位（杜绝新分配撞号），
  /// 绝不灌入内存映射——未访问的冷租户与冷库映射零内存常驻，首次访问经
  /// [`Self::resolve_context`] 点查磁盘装载（doc/zh/db.md「冷租户按需加载与
  /// 零全局常驻内存」）。内存仅重建根域 (0, 0)、近期即将到期的死亡账本
  /// （[`GcDeadLog`] 小根堆）与分配水位；0x06 成对换号记录仅对根域成对装载，
  /// 0x05 落盘水位与映射扫描号取大（收尾 fetch_max 折叠，消隐式落盘依赖）。
  /// 全部记录经 [`DbMetaRecord::decode`] 单点解码后由 [`Self::rebuild_apply_record`]
  /// 按变体分发，长度错位或未知子类型告警留痕、绝不静默吞掉。
  ///
  /// 调用方契约即启动门禁：本方法 `await` 完成前映射面不可信，任何会话前缀解析
  /// 都可能盲分配到磁盘已占用的虚拟 ID（见 [`Self::open_shared`] 与
  /// [`Self::run_recovery_pass`] 的时序口径）。尾端的分配水位抬升与代数推进成对，
  /// 顺序不可交换（见函数尾注释）。
  pub async fn rebuild_vdb_async(&self) -> Result<()> {
    // 强制先初始化 (0, 0) 以确保持久分配
    self.vdb.get_or_create_db(0, 0);

    let begin = self.hlog.begin_address();
    let tail = self.hlog.tail_address();
    let mut max_vid = 0u64;

    self
      .hlog
      .scan(begin, tail, |addr, rec| {
        self.rebuild_vdb_visit(addr, rec.key, rec.value, rec.is_tombstone(), &mut max_vid);
        Ok(true)
      })
      .await?;

    self.finish_vdb_rebuild(max_vid);
    Ok(())
  }

  /// DbMeta 单条记录重建工序（键值以裸切片交付，供独立重建扫描与恢复期
  /// 融合扫描内核 [`crate::store::WedbStore::run_recovery_pass`] 共用同一机制）
  pub(super) fn rebuild_vdb_visit(
    &self,
    addr: u64,
    key: &[u8],
    value: &[u8],
    is_tombstone: bool,
    max_vid: &mut u64,
  ) {
    let Ok((_ns, _db, tag, payload)) = NamespaceDbCodec::decode_tagged_key(key) else {
      return;
    };
    if tag != KeyTag::DbMeta {
      return;
    }
    // 墓碑 = GC 已落地的回收注销：按墓碑载荷注销死亡账本在册项
    // （非 GC 载荷 dead_vid_of 为 None，与既有语义一致静默跳过）
    if is_tombstone {
      if let Some(vid) = DbMetaRecord::dead_vid_of(payload) {
        self.vdb.gc_dead.remove(&vid);
      }
      return;
    }
    match DbMetaRecord::decode(payload, value) {
      Some(record) => *max_vid = (*max_vid).max(self.rebuild_apply_record(record)),
      // 布局漂移不隐身：长度错位/未知子类型须告警（幽灵键与静默丢映射
      // 的头号成因），交由布局单点修复
      None => log::warn!(
        "DbMeta 重建：记录布局错位被忽略 addr={addr} subtype={:?} payload_len={} value_len={}",
        payload.first().copied(),
        payload.len(),
        value.len()
      ),
    }
  }

  /// 重建收尾：分配水位折叠与代数推进（与 [`Self::rebuild_vdb_visit`] 配对，
  /// 独立重建与融合恢复两条入口共用同一收尾，杜绝口径漂移）
  pub(super) fn finish_vdb_rebuild(&self, max_vid: u64) {
    if max_vid > 0 {
      self
        .vdb
        .next_virtual_id
        .fetch_max(max_vid + 1, Ordering::Relaxed);
    }
    // 就绪门禁与代数推进是一对（顺序写死，不可上提也不可下移）：以上
    // 死亡账本装载与分配水位抬升全部收敛后换代，令重建期间已建立的会话
    // 在下一次
    // [`crate::session::StoreSession::session_prefix`] 慢路径重解析映射。
    // 缺这一步时，先于重建取过代的会话永不再刷新，写路径继续落在已被磁盘映射
    // 抛弃的旧物理前缀上（永远读不到的幽灵段）；反之 bump 早于账本装载生效，
    // 会话会在账本就绪前判活判死，等价于把门禁往前挪却没开门。
    self.vdb.bump_generation();
  }

  /// 重建分派：应用一条 [`DbMetaRecord`] 到内存映射/死亡账本，返回须折叠进
  /// 分配水位的虚拟号
  ///
  /// 映射变体折叠值侧新号（vns / vdb），墓碑变体折叠键侧死亡旧号
  /// （old_vns / old_vdb）——两者皆曾分配，水位必须越过。根域快照 immortal
  /// 全量装载，非根域库级路由表按 0 内存常驻条款不装载（首访经
  /// [`Self::resolve_context`] 点查回建）；映射写入口复用
  /// [`VirtualDbManager::insert_db_mapping`] 的槽位单元格单格覆盖写单点（点查
  /// 装载与重建装载共位）。
  fn rebuild_apply_record(&self, record: DbMetaRecord) -> u64 {
    match record {
      DbMetaRecord::NsMap { logic_ns, vns } => {
        // 命名空间标量映射装载（16 字节级极小基线，判活反向索引同步维护）
        self.vdb.insert_ns_mapping(logic_ns, vns);
        vns
      }
      DbMetaRecord::DbMap { vns, logic_db, vdb } => {
        if vns == ROOT_VIRTUAL_ID {
          // 根域快照 immortal 永不空闲析构，映射表必须完整常驻
          self.vdb.insert_db_mapping(vns, logic_db, vdb);
        }
        vdb
      }
      DbMetaRecord::GcDeadNs {
        expired_at,
        old_vns,
        tail_address,
      } => {
        self.vdb.gc_dead.insert(
          old_vns,
          GcDeadEntry {
            expired_at,
            tail_address,
            vns: None,
          },
        );
        old_vns
      }
      DbMetaRecord::GcDeadDb {
        expired_at,
        vns,
        old_vdb,
        tail_address,
      } => {
        self.vdb.gc_dead.insert(
          old_vdb,
          GcDeadEntry {
            expired_at,
            tail_address,
            vns: Some(vns),
          },
        );
        old_vdb
      }
      // 0x05 分配水位：以（落盘值饱和减一）折叠进 max_vid 归约，收尾
      // fetch_max(max_vid + 1) 恰等于 max(0x05 落盘水位, 映射扫描号 + 1)，
      // 消除「每个分配号都必须落盘」的隐式依赖；饱和减一防空水位回绕
      DbMetaRecord::NextId { next_virtual_id } => next_virtual_id.saturating_sub(1),
      // 0x06 SWAPDB 成对映射单记录：命中即两库映射成对生效（与后续同值
      // 0x02 记录幂等覆盖），装载口径同 DbMap 臂——仅根域快照常驻装载，
      // 非根域留待点查；双指向号并入水位归约
      DbMetaRecord::DbSwap {
        vns,
        logic_db1,
        logic_db2,
        swapped_db1,
        swapped_db2,
      } => {
        if vns == ROOT_VIRTUAL_ID {
          self.vdb.insert_db_mapping(vns, logic_db1, swapped_db1);
          self.vdb.insert_db_mapping(vns, logic_db2, swapped_db2);
        }
        swapped_db1.max(swapped_db2)
      }
    }
  }
}

impl<D: Device> Drop for WedbStore<D> {
  fn drop(&mut self) {
    // 先停内置 GC：协作标志置位 + 任务取消兜底，防止后台循环在引擎资源拆除后
    // 继续触达 hlog/会话（句柄强引用不构成环，GC 任务本身亦持引擎弱引用）
    if let Some(h) = self.gc.get_mut() {
      h.stop();
    }
    // 待释放队列末轮收割：drop 线程已无并发读者，纪元注册即就绪即收割，
    // 死域树文件随文件句柄拆除一并落地（常驻释放任务的升级在 Drop 开始后
    // 恒失败，不再触达本引擎）
    self.drain_bftree_release(usize::MAX);
    self.range_index.dispose();
    if let Some(tmp_dir) = &self.temp_range_index_dir {
      let _ = fs::remove_dir_all(tmp_dir);
    }
  }
}
