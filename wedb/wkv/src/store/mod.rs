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
use event_listener::Event;
use itoa::Buffer;
use parking_lot::{Mutex, RwLock};
use wbase::{align::DEFAULT_SECTOR_SIZE, group_commit::GroupCommitPipeline, time::now_ms};
use wbftree::RangeIndexManager;
use wdev::Device;
use wepoch::LightEpoch;
use whlog::HybridLog;
use windex::HashIndex;
use wreviv::FreeRecordPool;
use wval::{KeyTag, NamespaceDbCodec};

use crate::{
  config::{GcConfig, StoreConfig},
  error::{Error, Result},
  gc::{GcHandle, spawn_bftree_reclaimer},
  read_cache::ReadCache,
  session::{DeleteMissHook, SessionSlot, StoreSession, WatchHook},
  vdb::{
    BftreeDomains, DbMetaRecord, GcDeadEntry, PendingTreeRelease, ROOT_VIRTUAL_ID, VirtualDbManager,
  },
};
pub mod addr;
pub mod cpr_host;
pub mod event;
pub mod flush;
pub mod hlog_scan;
pub mod keyspace;
pub mod resize;
pub mod stats;
pub mod vdb_load;

pub use event::*;
pub use hlog_scan::{HybridLogScanMetrics, MetricEntry, ScanRegion, ScanState};
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
  /// 恢复来源检查点 Token（恢复装配一次性记录，冷启动恒空）：版本基线推进
  /// 与恢复后「清未用」回收以它为准（recover_latest 回退轮的选中 Token 唯一
  /// 出口——find_latest 在回退后已不代表实际恢复版本）
  pub(crate) recovered_token: OnceLock<u128>,
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
  /// 常驻回收驱动挂载位（防重复挂载幂等）：[`spawn_bftree_reclaimer`] 首次
  /// 挂载即置位，同实例多个挂载点叠加（open_shared 冷启动 + 恢复/换入收口）
  /// 只认首次——双常驻循环会双倍轮询排空同一待释放队列
  pub(crate) reclaimer_mounted: AtomicBool,
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
  /// 换号联动延迟销毁；登记/取数/对账内核见 [`crate::vdb`]）
  pub(crate) bftree_domains: BftreeDomains,
  /// 换号域待物理释放队列：detach_tree 已同步摘注册的树批次携安全纪元删除
  /// 期限（入队时刻 + db_gc_reclaim_delay_secs，与日志紧缩死亡账本同口径）
  /// 排队，消费侧 [`Self::drain_bftree_release`] 只摘已到期条目——期限内
  /// unlink 绝不发生（树文件是集合内容唯一持久副本，删除先于换号批持久化
  /// 落地即「回滚旧域而树已消失」的崩溃窗），纪元排空、引擎释放与数据文件
  /// 删除由内置 GC 轮次在到期轮次承接——回放/复制流水线只投递不释放
  /// （doc/zh/db.md 主从异步屏障：GcBarrier 语义由 FlushDb/FlushNs 条目
  /// 承载，不新增条目类型；与旁表同形态的冷路径互斥容器，杜绝第二套
  /// 释放编排）
  pub(crate) bftree_release: Mutex<Vec<PendingTreeRelease>>,
  /// 换号元数据串行锁（garnet 无对应，wedb 自研换号元数据串行锁——C# 每库
  /// 独立 Tsavorite 实例天然无换号撕裂竞态）；认领位与等待队列的形态、以及
  /// 「本锁是『锁用 parking_lot』规范的跨 await 例外」的依据，见 [`DbmetaLock`]
  /// 类型文档；获取与释放见 [`Self::lock_dbmeta`]
  pub(crate) dbmeta_lock: DbmetaLock,
  /// ACL 旁路标签（`KeyTag::Acl`）记录变更代数（引擎级单标量，与注册用户总量
  /// 脱钩；garnet 无对应——C# 用户句柄驻全局字典、改权即就地 CAS 共享句柄，
  /// 本仓句柄连接本地持有，改权只能经代数向在途会话广播）
  ///
  /// 唯一推进口 [`Self::bump_acl_generation`]，ACL 记录写删的四个出口
  /// （`wnode::resp::acl_store::AclStore` 的 write/delete、AOF 与复制回放的
  /// `KeyTag::Acl` 条目臂）共用；消费口见
  /// `wnode::resp::resp_server_session::RespServerSession::refresh_acl_mount_if_stale`
  /// （会话挂载时快照、鉴权预门比较，与 vdb 换号代数的会话前缀重解析同形态）
  acl_generation: AtomicU64,
  /// 在线哈希索引扩容状态机运行时容器 (对标 Garnet IndexResizeTask)
  pub resize: Arc<resize::IndexResizeState>,
}

/// 换号元数据串行锁本体：`AtomicBool` 认领位 + `event_listener::Event` 等待队列
///
/// 无争用时一次 Acquire CAS 即得（与改造前的快路径同形，不多付任何代价）；
/// 争用时等待方注册监听后 `.await` **真挂起**，临界区落盘期间零轮询、零 CPU、
/// 不占调度槽，释放方 Drop 精准移交队首（event_listener 队列 FIFO 公平，先注册
/// 先醒）。
///
/// 为什么不用 parking_lot——「锁用 parking_lot」规范的跨 await 例外依据，
/// 防后人按规范误改回同步锁：
///
/// `.agents/skills/rust_review/SKILL.md`「同步锁用 parking_lot」与
/// `.agents/skills/transpile/SKILL.md`「锁用 parking_lot」两条规范的适用前提
/// 是**临界区不跨 await**。本锁不满足该前提：全部持有者的临界区内都要 await
/// DbMeta 原子批落盘（[`crate::session::StoreSession::persist_dbmeta_batch`]，
/// 含设备 IO），换成同步锁即：
///
/// 1. 死锁：本仓是 Thread-per-Core 运行时（`wnode/src/server.rs` 每 worker
///    线程一个 `Runtime::new()`、会话任务永不跨核迁移）。同核两个换号任务并到
///    一处时，后到者 `lock()` 会 park 整个 OS 线程，而持锁者的落盘续算只能由
///    该线程驱动——锁永不释放，且该核上全部无关任务一并挂死；
/// 2. `parking_lot::MutexGuard` 为 `!Send`：守卫跨 await 持有会把 `Send` 从整条
///    会话 future 上抹掉。compio 的 `Runtime::spawn` 不要求 `Send`（一核一运行
///    时，任务本就可 `!Send`），故本条不是硬编译墙，只是把未来任何 Send 边界
///    （跨线程派发、`spawn_blocking` 桥接）一并封死——第 1 条才是本锁的决定性
///    依据。
///
/// C# 侧的两种形态都不可直译（本结构取第三条）：
///
/// - `MultiDatabaseManager.cs:TrySwapDatabases` 的串行化靠
///   `databasesContentLock`（`SingleWriterMultiReaderLock`），其获取口
///   `TryGetDatabasesContentWriteLock` 是「`TryWriteLock` 失败即 `Thread.Yield()`
///   重试」——.NET 线程把时间片交回调度器，故不是热自旋，但也不是队列挂起；
///   rust 直译即 `yield_now` 自旋，钉核运行时下等待任务仍逐轮占住该核调度槽，
///   正是本锁改造掉的形态；
/// - `DatabaseManagerBase.cs:FlushDatabase` 本体是同步无锁段（日志截断 + AOF
///   截断），等待全压在调用方的同步 yield 重试上——C# 一线程一会话，等待方
///   占住的是自己的线程，不牵连同他人；rust 一核多任务，「等而不占」只能是
///   任务态挂起，同步锁 park 线程即第 1 条死锁，自旋即逐轮占调度槽。
///
/// 故本结构以事件等待队列承载：语义上补齐 C# 「等待方不烧 CPU」的效果，且比
/// C# 的 yield 重试更强——等待任务在临界区整段零唤醒、零 CPU、不占调度槽。
#[derive(Default)]
pub(crate) struct DbmetaLock {
  /// 认领位（true = 有换号事务在编排）
  busy: AtomicBool,
  /// 争用等待队列：释放方 notify(1) 逐个移交
  gate: Event,
}

impl DbmetaLock {
  /// 串行获取：快路径一次 CAS 抢占；争用路径「先注册监听、再复核认领位」后
  /// 挂起——注册先于复核是丢失唤醒的唯一防线（注册前前任已 store+notify 的
  /// 窗口由复核那次 CAS 承接），顺序颠倒即可能永久睡过一次移交
  async fn acquire(&self) -> DbmetaGuard<'_> {
    loop {
      if self
        .busy
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return DbmetaGuard(self);
      }
      let listener = self.gate.listen();
      if self
        .busy
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return DbmetaGuard(self);
      }
      listener.await;
    }
  }
}

/// 换号元数据串行锁守卫（Drop 清认领位并唤醒队首等待者，`?` 早退不遗留）
pub(crate) struct DbmetaGuard<'a>(&'a DbmetaLock);

impl Drop for DbmetaGuard<'_> {
  fn drop(&mut self) {
    // Release store 先于 notify：event_listener 的 notify 自身先打 SeqCst full
    // fence（src/notify.rs 的 fence → full_fence）再摘队列，故被唤醒者随后那次
    // Acquire CAS 必能看到本次释放，不会与仍持锁的旧代撞车
    self.0.busy.store(false, Ordering::Release);
    self.0.gate.notify(1);
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

  /// 物理记录（键 + 值）能否落入当前页容量（whlog `HybridLog::record_fits`
  /// 单源转发，与写侧 RecordTooLarge 拒绝同公式）；集合信封升阶容量门消费
  #[inline]
  pub fn record_fits_page(&self, key_len: usize, val_len: usize) -> bool {
    self.hlog.record_fits(key_len, val_len)
  }

  /// 获取换号元数据串行锁（garnet 无对应，wedb 自研换号元数据串行锁；形态与
  /// 「为何不用 parking_lot」见 [`DbmetaLock`] 类型文档）
  ///
  /// 无争用一次 CAS 即得，争用在等待队列上真挂起（零自旋、零 CPU）；临界区仅
  /// 含换号 CAS 与 DbMeta 原子批落盘
  /// （[`crate::session::StoreSession::persist_dbmeta_batch`]），全部持有者
  /// （flush_database / flush_namespace / swap_databases / apply_dbmeta_record）
  /// 互不嵌套、临界区内不再获取本锁，无死锁。锁只在管理命令面，用户数据热路径
  /// 不触锁
  #[inline]
  pub(crate) async fn lock_dbmeta(&self) -> DbmetaGuard<'_> {
    self.dbmeta_lock.acquire().await
  }

  /// 当前 ACL 变更代数（会话侧缓存代数的比较源；Acquire 与
  /// [`Self::bump_acl_generation`] 的 Release 配对，令「见到新代数」的会话
  /// 必能见到推进代数前已落盘的 ACL 记录，杜绝以旧记录重建后把新代数钉死）
  #[inline]
  pub fn acl_generation(&self) -> u64 {
    self.acl_generation.load(Ordering::Acquire)
  }

  /// 推进 ACL 变更代数（ACL 记录写删的唯一 mutator，须在记录写入生效后调用）
  #[inline]
  pub fn bump_acl_generation(&self) {
    self.acl_generation.fetch_add(1, Ordering::Release);
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
    // 复活池：启用位由 StoreConfig.enable_revivification 单点注入（对标 C#
    // RevivificationManager 构造期按 EnableRevivification 决定 revivSuspendCount
    // 初值），此后 `reviv_pool.is_enabled()` 即 C# `IsEnabled`，同时承载
    // 「未启用」与「迁移暂停」两义；分桶容量与统计恒在，不开启亦零成本。
    let reviv_pool = Arc::new(FreeRecordPool::new(config.enable_revivification));
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
      recovered_token: OnceLock::new(),
      purge_suppress: AtomicUsize::new(0),
      aof_listeners_paused: AtomicBool::new(false),
      reclaimer_mounted: AtomicBool::new(false),
      temp_range_index_dir,
      keyspace_scan_session: SessionSlot::new(),
      vdb_load_session: SessionSlot::new(),
      bftree_domains: BftreeDomains::default(),
      bftree_release: Mutex::new(Vec::new()),
      dbmeta_lock: DbmetaLock::default(),
      acl_generation: AtomicU64::new(0),
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
  /// 分配水位的虚拟号（重建装载与 DbMeta 镜像回放应用
  /// [`crate::store::WedbStore::apply_dbmeta_record`] 共用本内核，映射装载
  /// 口径只此一份）
  ///
  /// 映射变体折叠值侧新号（vns / vdb），墓碑变体折叠键侧死亡旧号
  /// （old_vns / old_vdb）——两者皆曾分配，水位必须越过。根域快照 immortal
  /// 全量装载，非根域库级路由表按 0 内存常驻条款不装载（会话首访经
  /// [`Self::resolve_context`] 点查回建，回放面按条目物理域反查逻辑域前经
  /// [`Self::load_routes_of_vns`] 点查回建，同一装载原语）；映射写入口复用
  /// [`VirtualDbManager::insert_db_mapping`] 的槽位单元格单格覆盖写单点（点查
  /// 装载与重建装载共位）。
  pub(crate) fn rebuild_apply_record(&self, record: DbMetaRecord) -> u64 {
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
    // 待释放队列末轮收割：drop 线程已无并发读者，纪元注册即就绪即收割。
    // 已过安全纪元期限的死域树文件随文件句柄拆除一并落地；未到期条目与
    // 消费侧同门控不强行释放（宁可滞留磁盘）——常驻释放任务的升级在 Drop
    // 开始后恒失败不再触达本引擎，滞留残件由下次启动对账
    // （reclaim_dead_domain_bftrees）承接
    self.drain_bftree_release(usize::MAX);
    self.range_index.dispose();
    if let Some(tmp_dir) = &self.temp_range_index_dir {
      let _ = fs::remove_dir_all(tmp_dir);
    }
  }
}

#[cfg(test)]
mod tests {
  //! 换号元数据串行锁的等待形态自证：快路径一次 CAS、争用真挂起（零唤醒零
  //! CPU）、释放精准移交一位（task/ing/my-dbmeta-lock-yield-spin 的「争用下不
  //! 烧 CPU」行为面验收）。负控实测：把 `DbmetaLock::acquire` 改回
  //! compare_exchange + yield_now 自旋形态，则「等待者挂在事件队列上」（实测
  //! 队列零登记）与「挂起不得自唤醒」（实测每轮让渡自唤一次）两枚断言即转红

  use std::{
    pin::pin,
    sync::{
      Arc,
      atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
  };

  use compio::{
    runtime::{Runtime, spawn},
    time::sleep,
  };
  use wbase::future::yield_now;

  use super::DbmetaLock;

  /// 唤醒计数 waker：运行时只在任务被唤醒后才 poll 它，故等待期的 wake 次数
  /// 就是该任务占用的调度次数（不引 libc 取线程 CPU 时间，wake 数是更直接的
  /// 观测量——烧 CPU 的形态正是「自唤醒把自己反复灌进就绪队列」）
  #[derive(Default)]
  struct WakeCounter(AtomicUsize);

  impl WakeCounter {
    fn get(&self) -> usize {
      self.0.load(Ordering::Relaxed)
    }
  }

  impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
      self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
      self.0.fetch_add(1, Ordering::Relaxed);
    }
  }

  #[test]
  fn uncontended_acquire_needs_one_poll_and_no_wake() {
    let lock = DbmetaLock::default();
    let wakes = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);

    let mut acquire = pin!(lock.acquire());
    let Poll::Ready(guard) = acquire.as_mut().poll(&mut cx) else {
      panic!("无争用快路径应一次 poll 即取到锁");
    };
    assert!(lock.busy.load(Ordering::Acquire), "取锁后认领位应置位");
    assert_eq!(0, lock.gate.total_listeners(), "快路径不入等待队列");
    assert_eq!(0, wakes.get(), "快路径不应产生任何唤醒");

    drop(guard);
    assert!(!lock.busy.load(Ordering::Acquire), "守卫释放应清认领位");
    assert_eq!(0, wakes.get(), "无等待者时释放不多发通知");
  }

  /// 争用下等待者真挂起的行为面自证（本票核心诉求）
  ///
  /// 改造前形态：`while compare_exchange 失败 { yield_now().await }`——
  /// `wbase::future::YieldNow` 的首次 poll 必 `cx.waker().wake_by_ref()` 再返回
  /// Pending，故持锁临界区（DbMeta 原子批落盘，含 IO）每推进一刻，等待任务就
  /// 被自唤醒并重新 poll 一次，wake 计数随时长线性增长（钉核下还在持续占用
  /// 该核调度槽）。改造后：等待者挂进事件队列，临界区整段零唤醒，释放时恰醒一次。
  #[test]
  fn contended_waiter_sleeps_until_handoff() {
    let lock = DbmetaLock::default();
    let wakes = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);

    // 持有者：快路径取锁后进入临界区（生产形态为 await persist_dbmeta_batch）
    let mut acquire = pin!(lock.acquire());
    let Poll::Ready(holder) = acquire.as_mut().poll(&mut cx) else {
      panic!("无争用快路径应一次 poll 即取到锁");
    };

    // 等待者：首次 poll 走完「CAS 失败 → 注册监听 → 复核 CAS 失败 → 挂起」
    let mut waiter = pin!(lock.acquire());
    assert!(
      matches!(waiter.as_mut().poll(&mut cx), Poll::Pending),
      "锁被占用时等待者应挂起而非就绪"
    );
    assert_eq!(1, lock.gate.total_listeners(), "等待者应挂在事件队列上");
    assert_eq!(0, wakes.get(), "挂起不得自唤醒（自旋形态此处即开始烧 CPU）");

    // 临界区期间的重复观测：未被唤醒即不会被调度，等待任务零 CPU
    assert_eq!(0, wakes.get(), "持锁期内等待者零唤醒");
    assert_eq!(1, lock.gate.total_listeners(), "等待者应稳定挂在队列上");

    // 释放：清认领位 + 精准移交一位
    drop(holder);
    assert!(!lock.busy.load(Ordering::Acquire));
    assert_eq!(1, wakes.get(), "释放应恰好唤醒一位等待者");

    let mut waiter2 = pin!(lock.acquire());
    let Poll::Ready(second) = waiter.as_mut().poll(&mut cx) else {
      panic!("移交后等待者应取到锁");
    };
    assert!(lock.busy.load(Ordering::Acquire), "交接后认领位仍置位");
    // 队列已把移交出的名额用掉：新来者在锁未释放前挂进队列
    assert!(matches!(waiter2.as_mut().poll(&mut cx), Poll::Pending));
    assert_eq!(1, lock.gate.total_listeners());
    assert_eq!(1, wakes.get(), "第三任务挂起不产生额外唤醒");
    drop(second);
    assert_eq!(2, wakes.get(), "再一次移交恰醒一位");
    assert!(matches!(waiter2.as_mut().poll(&mut cx), Poll::Ready(_)));
  }

  /// 真运行时下的跨 await 串行性自证：八任务各四轮，临界区内必 await 一次
  /// （对位落盘让渡），断言任一时刻至多一位持有者且全部轮次推进完成（不丢
  /// 唤醒、不死锁）
  #[test]
  fn contended_tasks_serialize_across_await() {
    const TASKS: usize = 8;
    const ROUNDS: usize = 4;

    let rt = Runtime::new().expect("compio 运行时构造失败");
    rt.block_on(async {
      let lock = Arc::new(DbmetaLock::default());
      let held = Arc::new(AtomicUsize::new(0));
      let overlap = Arc::new(AtomicUsize::new(0));
      let done = Arc::new(AtomicUsize::new(0));

      let mut handles = Vec::with_capacity(TASKS);
      for _ in 0..TASKS {
        let lock = Arc::clone(&lock);
        let held = Arc::clone(&held);
        let overlap = Arc::clone(&overlap);
        let done = Arc::clone(&done);
        handles.push(spawn(async move {
          for _ in 0..ROUNDS {
            let _guard = lock.acquire().await;
            if held.fetch_add(1, Ordering::AcqRel) != 0 {
              overlap.fetch_add(1, Ordering::Relaxed);
            }
            // 临界区内跨 await：同步锁在此形态下会把整核 park 死
            sleep(Duration::from_millis(1)).await;
            yield_now().await;
            held.fetch_sub(1, Ordering::AcqRel);
            done.fetch_add(1, Ordering::Relaxed);
          }
        }));
      }
      for h in handles {
        h.await.expect("换号任务不应被取消");
      }

      assert_eq!(0, overlap.load(Ordering::Relaxed), "临界区不得重叠");
      assert_eq!(
        TASKS * ROUNDS,
        done.load(Ordering::Relaxed),
        "全部轮次应推进完成"
      );
      assert_eq!(0, held.load(Ordering::Relaxed));
      assert!(!lock.busy.load(Ordering::Acquire), "末轮释放后应无残留认领");
    });
  }
}
