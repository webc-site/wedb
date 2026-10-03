//! 自研依据: 存储会话核心（C# 对应 StorageSession 面本仓零拷贝重构）
use std::{
  env, fs,
  path::PathBuf,
  process,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
};

use arc_swap::ArcSwap;
use compio::runtime::Runtime;
use event_listener::Event;
use itoa::Buffer;
use parking_lot::{Mutex, RwLock};
use wbase::{align::DEFAULT_SECTOR_SIZE, group_commit::GroupCommitPipeline, time::now_ms};
use wbftree::RangeIndexManager;
use wcpr::CkptGateState;
use wdev::Device;
use wepoch::LightEpoch;
use whlog::HybridLog;
use windex::HashIndex;
use wreviv::{FreeRecord, FreeRecordPool};
use wval::{KeyTag, NamespaceDbCodec};

use crate::{
  config::{GcConfig, StoreConfig},
  error::{Error, Result},
  gc::{ColdBftreeObserved, GcHandle, spawn_bftree_reclaimer},
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
pub mod reviv_host;
pub mod stats;
pub mod vdb_load;

pub use event::{ObjectRmwNotification, StoreEvent, StoreEventSink, TieredCollectionNotification};
pub use hlog_scan::{HybridLogScanMetrics, ScanRegion, ScanState};
pub use resize::{IndexResizeState, ResizePhase, grow_index_blocking};

/// key_id 分配安全余量（恢复时在持久化水位之上预留的分配额度）
///
/// Checkpoint 仅持久化创建时点的 next_key_id 水位，其后至进程崩溃之间的新集合分配
/// 不随快照落盘；恢复时以 `持久化水位 + KEY_ID_ASSIGN_MARGIN` 作为 fetch_max 下限，
/// 保证新进程分配的 key_id 严格大于上一进程所有可能已分配的值——即使墙钟回退
/// （NTP 步进 / VM 快照回滚）使 generate_initial_key_id 落入历史区间也不会复用。
pub const KEY_ID_ASSIGN_MARGIN: u64 = 1 << 20;

/// ReadCache 降级兜底页数（按会话配置创建失败的 unwrap_unchecked 安全前提之一：
/// 非零 2 的幂，配合 enable=false 关闭全部校验分支，构造恒成功）
const RC_FALLBACK_NUM_PAGES: usize = 8;

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
  /// 当前存储版本合字（hlog 版本推进窗口字本体，[`whlog::VERSION_MASK`] 掩取
  /// 版本域；对标 C# TsavoriteKV.CurrentVersion：checkpoint 拍摄/恢复推进，
  /// AOF 条目 store_version 与重放端版本基线跳过共用此源；0 = 无 checkpoint
  /// 历史，全量重放）。与 hlog 窗口位单原子同源（装配期 clone
  /// [`whlog::HybridLog::version_shift_atomic`]），开窗即推版本、绝无第二套版本源
  pub(crate) current_version: Arc<AtomicU64>,
  /// 上一次成功检查点的版本号（对标 C# TsavoriteKV.lastVersion；0 = 无成功检查点历史）
  ///
  /// 仅在检查点持久化成功发布后单点登记，或恢复时由 set_current_version 同点对齐。
  /// 与 CurrentVersion 分列：检查点在途或失败轮中，CurrentVersion 已推进至新版本，
  /// 而 LastCheckpointedVersion 恒保持上一成功版本。
  pub(crate) last_checkpointed_version: AtomicU64,
  /// 检查点串行闸门（宿主实例所有权字段，1:1 对标 C# GarnetDatabase.cs:75
  /// `CheckpointingLock` per-database 实例锁）：[`Self::create_checkpoint`] 与
  /// [`Self::create_checkpoint_with_token`] 两入口交 wcpr 持闸，串行同一引擎上
  /// 的 SAVE 与周期快照；随实例生命周期析构，绝无进程级注册表的条目泄漏与
  /// 路径口径分叉（同实例经 symlink/相对绝对混用裂为两锁）
  pub ckpt_gate: wcpr::CkptGateState,
  /// 恢复来源检查点 Token（恢复装配一次性记录，冷启动恒空）：版本基线推进
  /// 与恢复后「清未用」回收以它为准（recover_latest 回退轮的选中 Token 唯一
  /// 出口——find_latest 在回退后已不代表实际恢复版本）
  pub(crate) recovered_token: OnceLock<u128>,
  /// 恢复检查点的 AOF 覆盖位点向量（恢复装配一次性记录，冷启动恒空）：恢复
  /// Token 元数据 `checkpoint_aof_address` 的读出口——AOF 重放扫描的位点
  /// 下界，按物理子日志逐位保存（快照发起时各子日志的覆盖边界，条目位点
  /// 严格小于其所属子日志下界的条目效果已物化进快照，重放必须跳过，兜住
  /// 「快照发布后、AOF 截断前」宕机的重放面；None 即元数据补写窗内崩溃，
  /// 边界未持久化，退化为空全量版本闸过滤）。绝不以子日志 0 标量广播全维
  pub(crate) recovered_aof_floor: OnceLock<Vec<u64>>,
  /// AOF 监听端口全局暂停闸（重放/恢复镜像抑制，见 [`Self::pause_aof_listeners`]）
  pub(crate) aof_listeners_paused: AtomicBool,
  /// 常驻回收驱动挂载位（防重复挂载幂等）：[`spawn_bftree_reclaimer`] 首次
  /// 挂载即置位，同实例多个挂载点叠加（open_shared 冷启动 + 恢复/换入收口）
  /// 只认首次——双常驻循环会双倍轮询排空同一待释放队列
  pub(crate) reclaimer_mounted: AtomicBool,
  /// 物理回收单飞闸：物理回收与紧缩在 GcManager 与常驻 bftree 任务间并发调度，
  /// 用此 Store 级闸门防并发执行
  ///
  /// [`doc(hidden)`] 测试专用隐藏面：仅 wkv/tests/gc_reclaimer_mount.rs 集成
  /// 测试直写（在途互斥预置与 RunGuard 复位观测），非公共 API 契约
  #[doc(hidden)]
  pub reclaim_inflight: AtomicBool,
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
  /// 换号域待 unlink 队列：detach_tree 已同步摘注册、引擎已就地 dispose 的
  /// 树批次（条目仅余 unlink 世代判据与 data_path，页环绝不随期限滞留）携
  /// 安全纪元删除期限（入队时刻 + db_gc_reclaim_delay_secs，与日志紧缩死亡
  /// 账本同口径）排队，消费侧 [`Self::drain_bftree_release`] 只摘已到期条目
  /// ——期限内 unlink 绝不发生（树文件是集合内容唯一持久副本，删除先于换号
  /// 批持久化落地即「回滚旧域而树已消失」的崩溃窗），数据文件删除由内置 GC
  /// 轮次在到期轮次承接——回放/复制流水线只投递不删除（doc/zh/db.md 主从
  /// 异步屏障：GcBarrier 语义由 FlushDb/FlushNs 条目承载，不新增条目类型；
  /// 与旁表同形态的冷路径互斥容器，杜绝第二套释放编排）
  pub(crate) bftree_release: Mutex<Vec<PendingTreeRelease>>,
  /// 冷树回收观察账：key_id → 首轮冷观察 ticks（判据与消费内核见
  /// [`crate::gc::cold_tree`]）——Meta 存根越出内存窗后迟滞计时，冷满窗口
  /// 方可摘除常驻页环，防热键释放-重开抖动
  pub(crate) cold_bftree_observed: ColdBftreeObserved,
  /// 换号元数据串行锁（garnet 无对应，wedb 自研换号元数据串行锁——C# 每库
  /// 独立 Tsavorite 实例天然无换号撕裂竞态）；认领位与等待队列的形态、以及
  /// 「本锁是『锁用 parking_lot』规范的跨 await 例外」的依据，见 [`SerialLock`]
  /// 类型文档；获取与释放见 [`Self::lock_dbmeta`]
  pub(crate) dbmeta_lock: SerialLock,
  /// ACL 管理串行锁（garnet 无对应——C# 用户句柄驻全局字典，SETUSER 读改写
  /// 靠 [`SerialLock`] 同形态的任务态串行闸；获取与释放见 [`Self::lock_acl`]。
  /// 临界区为 SETUSER 的「点查 → 复制改写 → 回写」与 DELUSER 的墓碑删除，含
  /// 冷记录落盘回读的阻塞收割窗口，与 [`Self::dbmeta_lock`] 同理必须任务态
  /// 挂起等待——对标 C# NetworkAclSetUser 的 do/while CAS 重试环
  /// （libs/server/Resp/ACLCommands.cs:181-226 + UserHandle.cs:TrySetUser 的
  /// Interlocked.CompareExchange）的「两命令操作全生效」串行化语义）
  ///
  /// 锁序册：本锁只允许被 lock_dbmeta 持有者嵌套获取（单向
  /// lock_dbmeta → lock_acl，wkv flush_all_databases 恒根域住户重挂面），
  /// 反向嵌套（持本锁再取 lock_dbmeta）严禁——SETUSER/DELUSER 临界区全程
  /// 不触换号元数据编排，反向路径不存在
  pub(crate) acl_lock: SerialLock,
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
  /// 幽灵读回归定向测试留钩（一次性，纯测试基础设施，生产恒 None）：
  /// 扩容迁移窗口内 `read_probe` 采得首地址缺席（`None` 且 `is_growing`）时，
  /// 于采样与进入读内核的间隙内回调，供定向用例确定性完成全量迁移并翻回
  /// Rest 相位，锁定「growing 期采得的陈旧 None 不得直采进内核」探针-内核
  /// 同相位不变式（对标 C# InternalRead 入口 SplitBuckets 先于 FindTag 装载）。
  /// 与 [`TEST_COLD_WINDOW_HOOK`](crate::session::TEST_COLD_WINDOW_HOOK) 同族；
  /// 逐实例挂载杜绝跨测试实例串扰
  #[cfg(debug_assertions)]
  #[doc(hidden)]
  pub test_read_gap_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

/// 任务态串行锁本体：`AtomicBool` 认领位 + `event_listener::Event` 等待队列
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
/// 是**临界区不跨 await**。本锁的两个持有面——换号元数据编排（临界区内
/// `await` DbMeta 原子批落盘
/// [`crate::session::StoreSession::persist_dbmeta_batch`]）与 ACL 管理读改写
/// （`wnode` 侧 `AclStore` 的 read/write 在冷记录/环形页翻转降级时经
/// `blocking_wait` 阻塞收割，挂起窗口内 `Runtime::block_on` 会调度同核其他
/// 任务）——都不满足该前提，换成同步锁即：
///
/// 1. 死锁：本仓是 Thread-per-Core 运行时（`wnode/src/server.rs` 每 worker
///    线程一个 `Runtime::new()`、会话任务永不跨核迁移）。同核两个持锁任务并到
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
/// - C# ACL 侧 `NetworkAclSetUser` 的 do/while CAS 重试环
///   （libs/server/Resp/ACLCommands.cs）+ `UserHandle.TrySetUser` 的
///   `Interlocked.CompareExchange`（libs/server/ACL/UserHandle.cs）——.NET
///   共享句柄天然支持逐用户 CAS 换新，rust 侧存储为唯一真源、无句柄可换，
///   串行锁是「两命令操作全生效」语义的对位承接。
///
/// 故本结构以事件等待队列承载：语义上补齐 C# 「等待方不烧 CPU」的效果，且比
/// C# 的 yield 重试更强——等待任务在临界区整段零唤醒、零 CPU、不占调度槽。
#[derive(Default)]
pub struct SerialLock {
  /// 认领位（true = 有事务在编排）
  busy: AtomicBool,
  /// 争用等待队列：释放方 notify(1) 逐个移交
  gate: Event,
}

impl SerialLock {
  /// 串行获取：快路径一次 CAS 抢占；争用路径「先注册监听、再复核认领位」后
  /// 挂起——注册先于复核是丢失唤醒的唯一防线（注册前前任已 store+notify 的
  /// 窗口由复核那次 CAS 承接），顺序颠倒即可能永久睡过一次移交
  pub async fn acquire(&self) -> SerialLockGuard<'_> {
    loop {
      if self
        .busy
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return SerialLockGuard(self);
      }
      let listener = self.gate.listen();
      if self
        .busy
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return SerialLockGuard(self);
      }
      listener.await;
    }
  }

  // 测试断言面，产线零调用
  #[doc(hidden)]
  #[inline]
  pub fn is_busy(&self) -> bool {
    self.busy.load(Ordering::Acquire)
  }

  #[doc(hidden)]
  #[inline]
  pub fn total_listeners(&self) -> usize {
    self.gate.total_listeners()
  }
}

/// 任务态串行锁守卫（Drop 清认领位并唤醒队首等待者，`?` 早退不遗留）
pub struct SerialLockGuard<'a>(&'a SerialLock);

impl Drop for SerialLockGuard<'_> {
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
  /// 「为何不用 parking_lot」见 [`SerialLock`] 类型文档）
  ///
  /// 无争用一次 CAS 即得，争用在等待队列上真挂起（零自旋、零 CPU）；临界区仅
  /// 含换号 CAS 与 DbMeta 原子批落盘
  /// （[`crate::session::StoreSession::persist_dbmeta_batch`]），全部持有者
  /// （flush_database / flush_all_databases / flush_namespace / swap_databases /
  /// apply_dbmeta_record，加 vdb_load 的 load_routes_of_vns 与 resolve_db 死值
  /// 臂重探两持闸点——后者临界区内仅 probe 纯读，swap 锁内重导走活值臂不触闸）
  /// 互不嵌套、临界区内不再获取本锁，无死锁。锁只在管理命令面与装载降级冷
  /// 路径，用户数据热路径不触锁
  #[inline]
  pub async fn lock_dbmeta(&self) -> SerialLockGuard<'_> {
    self.dbmeta_lock.acquire().await
  }

  /// 获取 ACL 管理串行锁（garnet 无对应——C# 共享句柄 CAS 换新天然串行，
  /// 见 [`SerialLock`] 类型文档；本锁为其「两命令操作全生效」语义的对位承接）
  ///
  /// 持锁面仅 SETUSER 的「点查 → 复制改写 → 回写」与 DELUSER 的墓碑删除两臂
  /// （wnode 分派段收敛获取，其余 ACL 只读命令不触锁）：跨 worker 的两条连接
  /// 并发改同一用户时后到者排队，杜绝裸读改写互相覆盖丢更新。锁只在 ACL 管理
  /// 冷路径，用户数据热路径不触锁
  #[inline]
  pub async fn lock_acl(&self) -> SerialLockGuard<'_> {
    self.acl_lock.acquire().await
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
      Arc::new(RangeIndexManager::with_epoch_and_budget(
        ri_log_root,
        cpr_dir,
        epoch,
        config.tree_cache_budget_bytes,
      )?),
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
    // 版本合字共享引用先于 hlog move 构造（同一原子本体：版本号、窗口位、
    // AOF 版本戳与恢复基线绝无第二套源）
    let version_shift = Arc::clone(hlog.version_shift_atomic());
    let (range_index, temp_range_index_dir) =
      Self::init_range_index(&config, Some(Arc::clone(&epoch)))?;
    // 复活池：启用位由 StoreConfig.enable_revivification 单点注入（对标 C#
    // RevivificationManager 构造期按 EnableRevivification 决定 revivSuspendCount
    // 初值），此后 `reviv_pool.is_enabled()` 即 C# `IsEnabled`，同时承载
    // 「未启用」与「迁移暂停」两义；分桶容量与统计恒在，不开启亦零成本。
    // oversize 复活臂（对标 C# PowerOf2Bins 预设恒附的 RecordSize = MaxRecordSize
    // 单一尾桶）：超 16 位内联尺寸词顶值、单页容量内的内联记录入臂桶仅存地址，
    // 取出经 whlog 记录头复读口 `record_block_size` 推导真实尺寸判纳；复读口为
    // 单点注入的只读闭包，臂容量即页容量（单记录硬上限，whlog 无跨页形态）；
    // 页容量容不下超限记录的配置不挂臂（对标 C# 未配置 oversize 桶形态）
    let pool = FreeRecordPool::new(config.enable_revivification);
    let reviv_pool = Arc::new(if config.page_size > FreeRecord::MAX_INLINE_SIZE as usize {
      let probe = Arc::clone(&hlog);
      pool.with_oversize(
        config.page_size.min(u32::MAX as usize) as u32,
        move |addr| probe.record_block_size(addr),
      )
    } else {
      pool
    });
    let read_cache = Arc::new(
      ReadCache::new(
        config.page_size,
        config.read_cache_num_pages,
        config.enable_read_cache,
        Arc::clone(&epoch),
      )
      .unwrap_or_else(|e| {
        log::warn!("ReadCache 按会话配置创建失败，降级为默认禁用配置: err={e}");
        // SAFETY: DEFAULT_SECTOR_SIZE 与 RC_FALLBACK_NUM_PAGES 均为非零 2 的幂且
        // enable=false 关闭全部校验分支，构造恒成功
        unsafe {
          ReadCache::new(
            DEFAULT_SECTOR_SIZE,
            RC_FALLBACK_NUM_PAGES,
            false,
            Arc::clone(&epoch),
          )
          .unwrap_unchecked()
        }
      }),
    );
    let gc_cfg = Arc::new(RwLock::new(config.gc.clone()));
    let flush_pipeline = GroupCommitPipeline::new();
    let synced_until = hlog.flushed_until_address();
    let vdb = Arc::new(VirtualDbManager::new());
    vdb
      .gc_dead
      .set_grace_delay_secs(config.gc.db_gc_reclaim_delay_secs);
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
      vdb,
      read_cache,
      gc: Mutex::new(None),
      gc_cfg,
      event_sink: OnceLock::new(),
      watch_hook: OnceLock::new(),
      delete_miss_hook: OnceLock::new(),
      current_version: version_shift,
      last_checkpointed_version: AtomicU64::new(0),
      ckpt_gate: CkptGateState::default(),
      recovered_token: OnceLock::new(),
      recovered_aof_floor: OnceLock::new(),
      aof_listeners_paused: AtomicBool::new(false),
      reclaimer_mounted: AtomicBool::new(false),
      reclaim_inflight: AtomicBool::new(false),
      temp_range_index_dir,
      keyspace_scan_session: SessionSlot::new(),
      vdb_load_session: SessionSlot::new(),
      bftree_domains: BftreeDomains::default(),
      bftree_release: Mutex::new(Vec::new()),
      cold_bftree_observed: ColdBftreeObserved::default(),
      dbmeta_lock: SerialLock::default(),
      acl_lock: SerialLock::default(),
      acl_generation: AtomicU64::new(0),
      resize: Arc::new(resize::IndexResizeState::new()),
      #[cfg(debug_assertions)]
      test_read_gap_hook: Mutex::new(None),
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
    // 末位收口条带锁让位队列（强环孤岛自愈）：dispose 摘净注册表后重投让位
    // 批次——重投动作经 release_detached 的弱引用臂再入纪元延迟队列，统一由
    // LightEpoch::drop 宿主消亡收割（或此刻无保护者时的注册内联收割）落地
    // unlink。不新建第二条删除路径，零新机制；全程同步析构上下文，无跨
    // await 持锁问题
    self.range_index.harvest_release_retries(usize::MAX);
    if let Some(tmp_dir) = &self.temp_range_index_dir {
      let _ = fs::remove_dir_all(tmp_dir);
    }
  }
}
