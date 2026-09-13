use std::{
  env, fs, io,
  path::{Path, PathBuf},
  process,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
  },
};

use compio::runtime::Runtime;
use crossfire::oneshot::{TxOneshot, oneshot};
use gxhash::{GxBuildHasher, HashSet};
use itoa::Buffer;
use papaya::Operation;
use parking_lot::{Mutex, RwLock};
use wbase::time::{now_ms, now_ticks};
use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexStub, StorageBackend, TreeTuning};
use wdev::Device;
use wepoch::LightEpoch;
use whlog::HybridLog;
use windex::{HashBucket, HashBucketEntry, HashIndex};
use wrecord::{HEADER_SIZE, RecordHeader};
use wval::{CollectionType, KeyTag, META_VALUE_SIZE, MetaValue, NamespaceDbCodec};

use crate::{
  config::{GcConfig, StoreConfig},
  error::{Error, Result},
  gc,
  read_cache::{ReadCache, absolute_address, is_read_cache_addr},
  session::{SessionSlot, StoreSession},
  ttl::{TTL_VALUE_LEN, TtlProbe},
};

/// 集合版本映射并发字典（单一来源 `wbase::map::ConcurrentMap`：papaya 无锁结构 + gxhash 构建器）
pub type KeyIdVersionsMap = wbase::map::ConcurrentMap<u64, (u64, bool)>;

/// Group Commit 挂起等待者契约（对标 Garnet TsavoriteLog.CommitTask）
struct FlushWaiter {
  target: u64,
  tx: Option<TxOneshot<Result<()>>>,
}

/// 刷盘流水线互斥状态
struct FlushState {
  is_flushing: bool,
  waiters: Vec<FlushWaiter>,
}

/// 对标 Garnet 的 Group Commit / Pipeline Flush 控制器
pub struct FlushPipeline {
  state: Mutex<FlushState>,
  /// 硬件已完成 sync 持久化的最高连续逻辑地址水位
  pub synced_until: AtomicU64,
}

impl FlushPipeline {
  pub fn new(initial_addr: u64) -> Self {
    Self {
      state: Mutex::new(FlushState {
        is_flushing: false,
        waiters: Vec::new(),
      }),
      synced_until: AtomicU64::new(initial_addr),
    }
  }

  #[inline]
  pub fn synced_until(&self) -> u64 {
    self.synced_until.load(Ordering::Acquire)
  }
}

/// 免动态分发监听端口宏：按签名生成手写分发结构体
///
/// 相比闭包装箱：调用槽为编译期单态函数指针直调，省去 trait 对象
/// 虚表双重间接寻址与内联阻断；同时通过 Arc<F> 擦除闭包具体类型以统一存放。仅作共享所有权底座（Clone 递增引用计数）。
/// 结构体 4 指针宽按值移动，构造约束 `F: Fn + Send + Sync + 'static`。
macro_rules! define_listener {
  ($(#[doc = $doc:expr])* $name:ident, ($($arg:ident: $typ:ty),*) $(;)?) => {
    $(#[doc = $doc])*
    pub struct $name {
      ptr: *const (),
      call_fn: unsafe fn(*const (), $($typ),*),
      drop_fn: unsafe fn(*const ()),
      clone_fn: unsafe fn(*const ()) -> *const (),
    }

    unsafe impl Send for $name {}
    unsafe impl Sync for $name {}

    impl $name {
      pub fn new<F>(f: F) -> Self
      where
        F: Fn($($typ),*) + Send + Sync + 'static,
      {
        Self::from(Arc::new(f))
      }

      #[inline]
      pub fn call(&self, $($arg: $typ),*) {
        unsafe { (self.call_fn)(self.ptr, $($arg),*) }
      }
    }

    impl Drop for $name {
      fn drop(&mut self) {
        // ptr 由 Arc::into_raw 固化，构造后恒非空且 Drop 仅执行一次
        unsafe { (self.drop_fn)(self.ptr) };
      }
    }

    impl Clone for $name {
      fn clone(&self) -> Self {
        Self {
          ptr: unsafe { (self.clone_fn)(self.ptr) },
          call_fn: self.call_fn,
          drop_fn: self.drop_fn,
          clone_fn: self.clone_fn,
        }
      }
    }

    impl<F> From<Arc<F>> for $name
    where
      F: Fn($($typ),*) + Send + Sync + 'static,
    {
      fn from(arc: Arc<F>) -> Self {
        unsafe fn call_fn<F: Fn($($typ),*)>(ptr: *const (), $($arg: $typ),*) {
          let f = unsafe { &*(ptr as *const F) };
          f($($arg),*);
        }
        unsafe fn drop_fn<F>(ptr: *const ()) {
          unsafe {
            drop(Arc::from_raw(ptr as *const F));
          }
        }
        unsafe fn clone_fn<F>(ptr: *const ()) -> *const () {
          unsafe {
            Arc::increment_strong_count(ptr as *const F);
            ptr
          }
        }
        Self {
          ptr: Arc::into_raw(arc) as *const (),
          call_fn: call_fn::<F>,
          drop_fn: drop_fn::<F>,
          clone_fn: clone_fn::<F>,
        }
      }
    }
  };
}

/// 宏路径化再导出：兄弟模块（如 checkpoint 的 [`VersionShiftFn`]）经
/// `use crate::store::define_listener;` 复用同一 vtable 生成器
pub(crate) use define_listener;

define_listener! {
  /// 写监听回调端口（对标 C# Tsavorite IFunctions 的 Post 回调挂点）
  ///
  /// 参数为 `(key, val, tombstone)`：记录成功写入混合日志内存后同栈触发，
  /// 服务端据此把写效果追加进 AOF 日志兑现增量持久化与复制流。
  /// 回调位于写入热路径，实现必须无阻塞、无系统调用。
  WriteListenerFn, (key: &[u8], val: &[u8], tombstone: bool);
}

define_listener! {
  /// RangeIndex 写监听回调端口
  ///
  /// 参数为 `(key, field, value, delete)`：RangeIndex 字段写成功后同栈触发，
  /// 服务端据此把 RangeIndex 写效果（独立树文件中的唯一副本）追加进 AOF。
  /// 回调位于写入热路径，实现必须无阻塞、无系统调用、绝不 panic
  /// （release 下 panic=abort 等价进程崩溃）。
  RangeIndexListenerFn, (key: &[u8], field: &[u8], value: &[u8], delete: bool);
}

define_listener! {
  /// RangeIndex 创建监听回调端口
  ///
  /// 参数为 `(key, backend, tuning)`：RangeIndex 成功创建并完成存根落盘后同栈触发，
  /// 宿主据此把 RI.CREATE 命令条目入队 WAL。
  RangeIndexCreateListenerFn, (key: &[u8], backend: &StorageBackend, tuning: TreeTuning);
}

define_listener! {
  /// RangeIndex 删除监听回调端口
  ///
  /// 参数为 `(key)`：RangeIndex 成功删除并完成整树清理后同栈触发，
  /// 宿主据此把 RiDrop 命令条目入队 WAL（对标 Garnet DEL 触发 OnDispose(Deleted) 清理 BfTree）。
  RangeIndexDropListenerFn, (key: &[u8]);
}

define_listener! {
  /// TTL 过期物理清除监听回调端口
  ///
  /// 参数为 `(ns, db, 用户键, expire_at_ticks)`：purge 链（TTL 记录 + 数据双删）成功
  /// 完成后同栈触发恰好一次，`expire_at_ticks` 为 .NET Ticks（i64，100ns 单位，
  /// 0001-01-01 纪元，与 TTL 记录存储值同域）。宿主（AOF 适配器）据此把整次过期
  /// 清除折叠为 **单条**确定性逻辑条目入 WAL——对标 Garnet `InputHeader` 的
  /// `RespInputFlags.Deterministic` 携带绝对过期时间的单条目语义（见
  /// libs/server/Storage/Functions/MainStore/PrivateMethods.cs 的 WriteLogRMW），
  /// 副本重放零漂移且流内条目数最小。
  ///
  /// 端口在场时 purge 链内两条物理墓碑不再经 [`WriteListenerFn`] 镜像（会话级
  /// 精确抑制，见 `Self::purge_suppress`）；未注册时保持现状两条物理条目
  /// （嵌入式无 AOF 场景行为不变）。回调位于清除路径，实现必须无阻塞、
  /// 无系统调用、绝不 panic（release 下 panic=abort 等价进程崩溃）。
  TtlPurgeListenerFn, (ns: u64, db: u64, key: &[u8], expire_at_ticks: i64);
}

/// 对象 RMW 增量日志通知载荷
#[derive(Debug, Clone, Copy)]
pub struct ObjectRmwNotification<'a> {
  pub key: &'a [u8],
  pub obj_type: u8,
  pub op_code: u8,
  /// 事件时间戳：真 .NET Ticks（i64，100ns 单位，0001-01-01 纪元的
  /// `DateTimeOffset.UtcNow.UtcTicks`），非 Unix 毫秒——与 Garnet 对象 RMW
  /// 输入的时间戳同域，Unix 秒/毫秒 ↔ ticks 换算见 `wbase::convert`
  pub timestamp_ticks: i64,
  pub arg1: i32,
  pub arg2: i32,
  pub args: &'a [&'a [u8]],
}

define_listener! {
  /// 对象 RMW 增量日志监听回调端口
  ///
  /// 集合操作修改成功后同栈触发，宿主（AOF 适配器）据此把增量操作命令帧入队 WAL，
  /// 消除大集合 O(N) 全量 bitcode 载荷落 WAL 的写放大。
  ObjectRmwListenerFn, (notif: &ObjectRmwNotification<'_>);
}

define_listener! {
  /// 随键 TTL 写监听回调端口
  ///
  /// 参数为 `(ns, db, 用户键, expire_at_ticks)`：TTL 旁路记录
  /// （[`wval::KeyTag::Ttl`]，value = 8 字节绝对 .NET Ticks）成功写入或删除后
  /// 同栈触发。`Some(ticks)` = 设置/更新过期；`None` = 清除过期（PERSIST）。
  /// 宿主（AOF 适配器）据此入队 PEXPIREAT / PERSIST RMW 条目——对标 C#
  /// 命令层 EXPIRE/PEXPIREAT/PERSIST 的 AOF 形态（C# 的 SET k v EX 在
  /// StoreUpsert 条目 input 中随行 expiration，rust 侧随键 TTL 为旁路记录
  /// 无命令上下文，以「值条目 + TTL RMW 条目」两跳等价）。回调位于写入热路径，
  /// 实现必须无阻塞、无系统调用、绝不 panic。
  TtlWriteListenerFn, (ns: u64, db: u64, key: &[u8], expire_at_ticks: Option<i64>);
}

/// 创建集合版本映射字典（委托 `wbase::map::new_concurrent_map` 单一实现）
#[inline]
pub fn new_key_id_versions_map() -> KeyIdVersionsMap {
  wbase::map::new_concurrent_map()
}

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
  store: Arc<WedbStore<D>>,
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
  /// Group Commit 刷盘流水线控制器（对标 Garnet TsavoriteLog.ongoingCommitRequests）
  pub flush_pipeline: FlushPipeline,
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
  /// 用于 Fast Drop 与 GC LogCompactor 快速判定历史版本子键与已删集合子键并安全物理回收
  /// （基于 papaya 无锁并发字典消除锁竞争）
  pub key_id_versions: KeyIdVersionsMap,
  /// 内置 GC 后台循环句柄（`config.gc.enabled` 且在 compio 运行时内首次
  /// [`Self::new_session`] 时自动启动，OnceLock 保证全程至多一个循环）
  gc: OnceLock<gc::GcHandle<D>>,
  /// 内置 GC 运行态配置共享句柄（构造时自 `config.gc` 初始化；GC 驱动循环每轮
  /// 重读，[`Self::update_gc_config`] 热更新下一轮生效，对标 Garnet
  /// RuntimeServerConfig 的 CONFIG SET 语义）
  pub(crate) gc_cfg: Arc<RwLock<GcConfig>>,
  /// 写监听端口（宿主经 [`Self::set_write_listener`] 注入 AOF 追加适配器；
  /// 同为端口注入，引擎不感知 AOF 存在）
  write_listener: OnceLock<WriteListenerFn>,
  /// RangeIndex 写监听端口（宿主经 [`Self::set_range_listener`] 注入）
  range_listener: OnceLock<RangeIndexListenerFn>,
  /// RangeIndex 创建监听端口（宿主经 [`Self::set_range_create_listener`] 注入）
  range_create_listener: OnceLock<RangeIndexCreateListenerFn>,
  /// RangeIndex 删除监听端口（宿主经 [`Self::set_range_drop_listener`] 注入）
  range_drop_listener: OnceLock<RangeIndexDropListenerFn>,
  /// TTL 过期物理清除监听端口（宿主经 [`Self::set_ttl_purge_listener`] 注入；
  /// 端口在场即视为 purge 链的"AOF 适配器在场"）
  ttl_purge_listener: OnceLock<TtlPurgeListenerFn>,
  /// 对象 RMW 增量日志监听端口（宿主经 [`Self::set_object_rmw_listener`] 注入）
  object_rmw_listener: OnceLock<ObjectRmwListenerFn>,
  /// 随键 TTL 写监听端口（宿主经 [`Self::set_ttl_write_listener`] 注入；
  /// TTL 旁路记录写入/删除时触发，见 [`TtlWriteListenerFn`]）
  ttl_write_listener: OnceLock<TtlWriteListenerFn>,
  /// 当前存储版本（对标 C# TsavoriteKV.CurrentVersion：checkpoint 拍摄/恢复
  /// 推进，AOF 条目 store_version 与重放端版本基线跳过共用此源；0 = 无
  /// checkpoint 历史，全量重放）
  current_version: Arc<AtomicI64>,
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
  temp_range_index_dir: Option<PathBuf>,
  /// INFO KEYSPACE 专用扫描会话槽位（懒建复用，对标 Garnet GarnetDatabase
  /// `.KeyspaceScanStorageSession` + `KeyspaceScanLock`；并发调用后到者降级为
  /// 一次性临时会话，读路径无共享可变状态，无正确性风险）
  keyspace_scan_session: SessionSlot<D>,
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
      Arc::new(wbftree::RangeIndexManager::with_epoch(
        ri_log_root,
        cpr_dir,
        epoch,
      )?),
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
    let reviv_pool = Arc::new(wreviv::FreeRecordPool::new());
    let read_cache = Arc::new(
      ReadCache::new(
        config.page_size,
        config.read_cache_num_pages,
        config.enable_read_cache,
      )
      .unwrap_or_else(|e| {
        log::warn!("ReadCache 按会话配置创建失败，降级为默认禁用配置: err={e}");
        // SAFETY: 4096/8 均为非零 2 的幂且 enable=false 关闭全部校验分支，构造恒成功
        unsafe { ReadCache::new(4096, 8, false).unwrap_unchecked() }
      }),
    );
    let gc_cfg = Arc::new(RwLock::new(config.gc.clone()));
    let flush_pipeline = FlushPipeline::new(hlog.flushed_until_address());
    Ok(Self {
      config,
      index,
      hlog,
      flush_pipeline,
      epoch,
      device,
      next_key_id: AtomicU64::new(Self::generate_initial_key_id()),
      range_index,
      reviv_pool,
      read_cache,
      key_id_versions: new_key_id_versions_map(),
      gc: OnceLock::new(),
      gc_cfg,
      write_listener: OnceLock::new(),
      range_listener: OnceLock::new(),
      range_create_listener: OnceLock::new(),
      range_drop_listener: OnceLock::new(),
      ttl_purge_listener: OnceLock::new(),
      object_rmw_listener: OnceLock::new(),
      ttl_write_listener: OnceLock::new(),
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
  /// key_id 是子键物理键（ns+tag+key_id+version+field）的组成部分，墙钟回退时
  /// `Self::generate_initial_key_id` 可能生成与上一进程相同的 key_id，复用即
  /// 命名空间冲突；与持久化水位取最大值后，时钟正常时以时间戳为准，回退时以水位为准。
  #[inline]
  pub fn raise_key_id_floor(&self, floor: u64) {
    self.next_key_id.fetch_max(floor, Ordering::Relaxed);
  }

  /// 更新集合 key_id 的最新版本与存活状态（版本单调递增，拒绝陈旧版本覆盖与同版本幽灵复活）
  ///
  /// 基于 papaya `compute` 将守卫判定与写入合并为单次原子读改写：旧实现 check-then-insert
  /// 在并发交错下可发生高版本水位被低版本回退（insert 无条件覆盖），导致 LogCompactor 依
  /// `is_stale_subkey` 误判存活子键为陈旧并物理回收。接受条件：更高版本，或同版本
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

  /// 启动内置 GC 后台循环（幂等；运行态配置 `gc_cfg.enabled` 为 false 时不启动）
  ///
  /// 返回 true 表示 GC 处于运行中。须在 compio 运行时上下文内调用（`D: 'static`
  /// 为 spawn 任务硬性要求）；引擎 Drop 时自动停止循环。
  pub fn start_gc(self: &Arc<Self>) -> bool
  where
    D: Device + 'static,
  {
    if self.gc.get().is_some() {
      return true;
    }
    if !self.gc_cfg.read().enabled || Runtime::try_current().is_none() {
      return false;
    }
    // 并发竞态败者：其句柄随 OnceLock::set 失败即 Drop，任务被立即取消
    self.gc.set(gc::GcManager::spawn(Arc::clone(self))).is_ok()
  }

  /// 热更新内置 GC 运行态配置（对标 Garnet CONFIG SET → RuntimeServerConfig）
  ///
  /// GC 驱动循环每轮重读本配置：扫描/紧缩间隔、批预算、开关修改下一轮即生效，
  /// 无需重启引擎。GC 未启动时更新同样持久生效——[`Self::start_gc`] 以此处的
  /// `enabled` 判定是否拉起循环。
  pub fn update_gc_config(&self, f: impl FnOnce(&mut GcConfig)) {
    f(&mut self.gc_cfg.write());
  }

  /// 读取内置 GC 运行态配置快照
  pub fn gc_config(&self) -> GcConfig {
    self.gc_cfg.read().clone()
  }

  /// 获取内置 GC 后台循环句柄（未启动返回 None）
  #[inline]
  pub fn gc_handle(&self) -> Option<&gc::GcHandle<D>> {
    self.gc.get()
  }

  /// 注入写监听端口（须在创建任何会话前调用；重复注入返回 false）
  pub fn set_write_listener(&self, listener: impl Into<WriteListenerFn>) -> bool {
    self.write_listener.set(listener.into()).is_ok()
  }

  /// 读取写监听端口
  #[inline]
  pub(crate) fn write_listener(&self) -> Option<&WriteListenerFn> {
    self.write_listener.get()
  }

  /// 注入 RangeIndex 写监听端口（须在产生 RangeIndex 写之前调用；重复注入返回 false）
  pub fn set_range_listener(&self, listener: impl Into<RangeIndexListenerFn>) -> bool {
    self.range_listener.set(listener.into()).is_ok()
  }

  /// 读取 RangeIndex 写监听端口
  #[inline]
  pub(crate) fn range_listener(&self) -> Option<&RangeIndexListenerFn> {
    self.range_listener.get()
  }

  /// 注入 RangeIndex 创建监听端口（须在产生 RangeIndex 创建之前调用；重复注入返回 false）
  pub fn set_range_create_listener(&self, listener: impl Into<RangeIndexCreateListenerFn>) -> bool {
    self.range_create_listener.set(listener.into()).is_ok()
  }

  /// 读取 RangeIndex 创建监听端口
  #[inline]
  pub(crate) fn range_create_listener(&self) -> Option<&RangeIndexCreateListenerFn> {
    self.range_create_listener.get()
  }

  /// 注入 RangeIndex 删除监听端口（须在产生 RangeIndex 删除之前调用；重复注入返回 false）
  pub fn set_range_drop_listener(&self, listener: impl Into<RangeIndexDropListenerFn>) -> bool {
    self.range_drop_listener.set(listener.into()).is_ok()
  }

  /// 读取 RangeIndex 删除监听端口
  #[inline]
  pub(crate) fn range_drop_listener(&self) -> Option<&RangeIndexDropListenerFn> {
    self.range_drop_listener.get()
  }

  /// 注入 TTL 过期物理清除监听端口（重复注入返回 false）
  ///
  /// 端口注册后 purge 链即进入"单条确定性逻辑条目"模式：物理墓碑镜像抑制 +
  /// 清除完成后触发一次回调，语义详见 [`TtlPurgeListenerFn`]
  pub fn set_ttl_purge_listener(&self, listener: impl Into<TtlPurgeListenerFn>) -> bool {
    self.ttl_purge_listener.set(listener.into()).is_ok()
  }

  /// 读取 TTL 过期物理清除监听端口
  #[inline]
  pub(crate) fn ttl_purge_listener(&self) -> Option<&TtlPurgeListenerFn> {
    self.ttl_purge_listener.get()
  }

  /// 注入对象 RMW 增量日志监听端口（重复注入返回 false）
  pub fn set_object_rmw_listener(&self, listener: impl Into<ObjectRmwListenerFn>) -> bool {
    self.object_rmw_listener.set(listener.into()).is_ok()
  }

  /// 注入随键 TTL 写监听端口（重复注入返回 false）
  pub fn set_ttl_write_listener(&self, listener: impl Into<TtlWriteListenerFn>) -> bool {
    self.ttl_write_listener.set(listener.into()).is_ok()
  }

  /// 读取随键 TTL 写监听端口
  #[inline]
  pub(crate) fn ttl_write_listener(&self) -> Option<&TtlWriteListenerFn> {
    self.ttl_write_listener.get()
  }

  /// 当前存储版本（对标 C# TsavoriteKV.CurrentVersion；0 = 无 checkpoint 历史）
  #[inline]
  pub fn current_version(&self) -> i64 {
    self.current_version.load(Ordering::Acquire)
  }

  /// 获取当前存储版本的共享原子引用（供外部监听器无环捕获）
  #[inline]
  pub fn current_version_atomic(&self) -> &Arc<AtomicI64> {
    &self.current_version
  }

  /// 推进当前存储版本（checkpoint 拍摄成功 / 从 checkpoint 恢复时由库管理层
  /// 调用；单调推进，勿回退）
  #[inline]
  pub fn set_current_version(&self, version: i64) {
    self.current_version.fetch_max(version, Ordering::Release);
  }

  /// 触发对象 RMW 增量日志通知（暂停闸置位期间跳过）
  #[inline]
  pub fn notify_object_rmw(&self, notif: &ObjectRmwNotification<'_>) {
    if self.aof_listeners_paused.load(Ordering::Relaxed) {
      return;
    }
    if let Some(listener) = self.object_rmw_listener.get() {
      listener.call(notif);
    }
  }

  /// 暂停全部 AOF 监听端口（写/RangeIndex/对象 RMW/TTL purge），返回恢复守卫
  ///
  /// 对标 C# AofProcessor 的重放会话（`new StoreWrapper(storeWrapper,
  /// recordToAof: false)`）：重放/恢复写入不得镜像回写本端 AOF，否则副本
  /// 重放自激放大（非幂等 RMW 条目重复应用）。守卫 drop 时自动恢复。
  pub fn pause_aof_listeners(self: &Arc<Self>) -> AofListenerPauseGuard<D> {
    self.aof_listeners_paused.store(true, Ordering::Release);
    AofListenerPauseGuard {
      store: Arc::clone(self),
    }
  }

  /// 刷盘前遍历指定页面范围内的原位记录，触发 OnFlush 事件
  /// (1:1 对标 C# ObjectAllocatorImpl.cs:FlushRecordsInRange 与 GarnetRecordTriggers.cs:OnFlush)
  pub fn on_flush_pages(&self, start_page: u64, end_page: u64) -> Result<()> {
    let page_size = self.hlog.config.page_size;
    for p in start_page..=end_page {
      if !self.hlog.buffer.is_page_loaded(p) {
        continue;
      }
      let mut page_guard = self.hlog.buffer.write_page(p);
      let page_start = self.hlog.config.page_start_address(p);
      let init_page = self.hlog.config.page_id(self.hlog.config.initial_address);
      let mut offset = if p == init_page {
        self
          .hlog
          .config
          .page_offset(self.hlog.config.initial_address)
      } else {
        0
      };

      while offset + HEADER_SIZE <= page_size {
        let header_bytes = &page_guard[offset..offset + HEADER_SIZE];
        if RecordHeader::is_zero_slice(header_bytes) {
          break;
        }
        let Ok(header) = RecordHeader::from_slice(header_bytes) else {
          break;
        };
        if header.is_pad() || header.is_null() {
          break;
        }
        let physical_size = header.physical_size();
        if physical_size == 0 || offset + physical_size > page_size {
          break;
        }
        let record_addr = page_start + offset as u64;
        let key_len = header.key_len() as usize;
        let val_len = header.val_len() as usize;
        let key_start = offset + HEADER_SIZE;
        let key_end = key_start + key_len;
        let val_end = key_end + val_len;

        if val_end <= offset + physical_size && !header.is_tombstone() {
          let (k_part, v_part) = page_guard.split_at_mut(key_end);
          let key_slice = &k_part[key_start..key_end];
          let val_slice = &mut v_part[..val_len];

          if let Some(user_key) = NamespaceDbCodec::decode_meta_user_key(key_slice)
            && val_slice.len() >= META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE
            && let Ok(meta) = MetaValue::from_slice(&val_slice[..META_VALUE_SIZE])
            && meta.collection_type == CollectionType::RangeIndex
          {
            let stub_slice =
              &mut val_slice[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE];
            if let Ok(mut stub) = RangeIndexStub::decode(stub_slice)
              && !stub.is_flushed()
              && !stub.is_transferred()
            {
              self
                .range_index
                .on_flush_address(user_key, &mut stub, record_addr)?;
              RangeIndexStub::slice_set_flushed(stub_slice, true)?;
            }
          }
        }
        offset += physical_size;
      }
    }
    Ok(())
  }

  /// 批量合并落盘指定逻辑页范围，并在落盘前原位触发 OnFlush 刷盘快照
  pub async fn flush_pages_range(&self, start_page: u64, end_page: u64) -> Result<()> {
    if start_page <= end_page {
      self.on_flush_pages(start_page, end_page)?;
      self.hlog.flush_pages_range(start_page, end_page).await?;
    }
    Ok(())
  }

  /// 将内存中所有驻留脏页异步刷盘并同步设备（严格对标 Garnet Group Commit 流水线）
  pub async fn flush_all(&self) -> Result<()> {
    let target = self.tail_address();

    // 1. 快速短路（0 I/O）：目标水位已被硬件 sync 持久化覆盖
    if target <= self.flush_pipeline.synced_until() {
      return Ok(());
    }

    // 2. 状态机协商：判定成为 Leader 还是 Follower
    let rx = {
      let mut lock = self.flush_pipeline.state.lock();

      // 双重检查防并发竞态
      if target <= self.flush_pipeline.synced_until() {
        return Ok(());
      }

      if lock.is_flushing {
        // 当前已有 Leader 在刷盘，登记为 Follower 挂起等待，绝不重复发起 I/O
        let (tx, rx) = oneshot::<Result<()>>();
        lock.waiters.push(FlushWaiter {
          target,
          tx: Some(tx),
        });
        Some(rx)
      } else {
        // 升级为 Leader，接管物理刷盘管道
        lock.is_flushing = true;
        None
      }
    };

    // 3. Follower 分支：挂起等待 Leader 批量唤醒
    if let Some(rx) = rx {
      return match rx.await {
        Ok(res) => res,
        Err(_) => Err(io::Error::other("Flush pipeline broken").into()),
      };
    }

    // 4. Leader 级联执行循环（Cascade Loop）
    self.run_flush_pipeline_leader_loop().await
  }

  /// Leader 级联刷盘驱动主循环
  async fn run_flush_pipeline_leader_loop(&self) -> Result<()> {
    loop {
      // (1) 收集当前批次目标：当前 tail 与所有挂起 Follower 的最大需求
      let batch_target = {
        let lock = self.flush_pipeline.state.lock();
        let max_waiter_target = lock.waiters.iter().map(|w| w.target).max().unwrap_or(0);
        let cur_tail = self.tail_address();
        cur_tail.max(max_waiter_target)
      };

      let synced = self.flush_pipeline.synced_until();
      let res: Result<()> = async {
        if batch_target > synced {
          // (2) 增量页刷盘：仅从当前 flushed_until 所在页刷到 batch_target 所在页
          // 彻底杜绝从 head 到 tail 的全量重复扫描与重复页写锁占用
          let flushed = self.hlog.flushed_until_address();
          if batch_target > flushed {
            let start_page = self.hlog.config.page_id(flushed);
            let end_page = self.hlog.config.page_id(batch_target.saturating_sub(1));
            if start_page <= end_page {
              self.on_flush_pages(start_page, end_page)?;
              self.hlog.flush_pages_range(start_page, end_page).await?;
            }
          }

          // (3) 物理介质持久化（硬件 fsync）：全系统单协程串行执行，彻底消除抖动争抢
          self.device.sync().await.map_err(Error::from)?;

          // (4) 推进持久化水位
          let current_flushed = self.hlog.flushed_until_address();
          let new_synced = batch_target.min(current_flushed);
          self
            .flush_pipeline
            .synced_until
            .fetch_max(new_synced, Ordering::AcqRel);
        }
        Ok(())
      }
      .await;

      // (5) 异常分发或批量成功唤醒
      let mut lock = self.flush_pipeline.state.lock();
      match res {
        Ok(()) => {
          let current_synced = self.flush_pipeline.synced_until();
          // 批量精准唤醒所有位点已覆盖的 Follower
          lock.waiters.retain_mut(|w| {
            if w.target <= current_synced {
              if let Some(tx) = w.tx.take() {
                tx.send(Ok(()));
              }
              false
            } else {
              true
            }
          });

          // (6) 级联检查：若仍有更高水位的 Follower 积压，或有新追加记录超过 synced
          let has_higher_waiters = !lock.waiters.is_empty();
          let tail_advanced = self.tail_address() > current_synced;

          if !has_higher_waiters && !tail_advanced {
            // 管道完全排空，释放 Leader 身份并退出
            lock.is_flushing = false;
            return Ok(());
          }
          // 仍有积压，Leader 继续下一轮批处理
        }
        Err(err) => {
          // 发生错误，向所有等待者广播错误，释放 Leader 身份，杜绝死锁
          for mut w in lock.waiters.drain(..) {
            if let Some(tx) = w.tx.take() {
              tx.send(Err(io::Error::other("Pipeline flush failed").into()));
            }
          }
          lock.is_flushing = false;
          return Err(err);
        }
      }
    }
  }

  /// 将内存所有页面刷盘并全部驱逐至磁盘区（对标 Tsavorite FlushAndEvict）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:FlushAndEvict
  pub async fn flush_and_evict_all(&self) -> Result<()> {
    let tail = self.tail_address();
    self.flush_all().await?;
    self.shift_read_only_address(tail);
    self.shift_head_address(tail);
    Ok(())
  }

  /// 获取临时 RangeIndex 目录路径（若未显式指定 range_index_dir）
  #[inline]
  pub fn temp_range_index_dir(&self) -> Option<&Path> {
    self.temp_range_index_dir.as_deref()
  }

  /// 获取哈希索引中已记录的有效条目总数（对标 Tsavorite GetEntryCount）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:GetEntryCount
  pub fn entry_count(&self) -> usize {
    let begin_addr = self.hlog.begin_address();
    let mut count = 0;

    for bucket in self.index.buckets.iter() {
      let mut curr_bucket = bucket;
      loop {
        for item in curr_bucket.entries.iter().take(HashBucket::DATA_ENTRIES) {
          let raw = item.load(Ordering::Acquire);
          if raw == 0 {
            continue;
          }
          let entry = HashBucketEntry::from_raw(raw);
          if entry.is_tentative() {
            continue;
          }
          let addr = entry.address();
          if is_read_cache_addr(addr) {
            let abs_addr = absolute_address(addr);
            if abs_addr >= self.read_cache.head_address()
              && abs_addr < self.read_cache.tail_address()
            {
              count += 1;
            } else {
              let real_addr = self.read_cache.skip_read_cache(addr);
              if real_addr >= begin_addr {
                count += 1;
              }
            }
          } else if addr >= begin_addr {
            count += 1;
          }
        }

        let overflow_idx = curr_bucket.overflow_index();
        if overflow_idx == 0 {
          break;
        }

        match self.index.overflow_pool.get(overflow_idx) {
          Some(next) => curr_bucket = next,
          None => break,
        }
      }
    }

    count
  }

  /// 获取当前日志分配尾部逻辑地址（TailAddress）
  #[inline]
  pub fn tail_address(&self) -> u64 {
    self.hlog.tail_address()
  }

  /// 获取安全尾部逻辑地址（SafeTailAddress，对标 C# TsavoriteLog.SafeTailAddress）
  ///
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:SafeTailAddress
  #[inline]
  pub fn safe_tail_address(&self) -> u64 {
    self.hlog.safe_tail_address()
  }

  /// 获取当前只读区分界逻辑地址（ReadOnlyAddress）
  #[inline]
  pub fn read_only_address(&self) -> u64 {
    self.hlog.read_only_address()
  }

  /// 获取安全只读区分界逻辑地址（SafeReadOnlyAddress）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:SafeReadOnlyAddress
  #[inline]
  pub fn safe_read_only_address(&self) -> u64 {
    self.hlog.safe_read_only_address()
  }

  /// 获取当前内存头逻辑地址（HeadAddress）
  #[inline]
  pub fn head_address(&self) -> u64 {
    self.hlog.head_address()
  }

  /// 获取安全内存头逻辑地址（SafeHeadAddress）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:SafeHeadAddress
  #[inline]
  pub fn safe_head_address(&self) -> u64 {
    self.hlog.safe_head_address()
  }

  /// 获取有效数据起始逻辑地址（BeginAddress）
  #[inline]
  pub fn begin_address(&self) -> u64 {
    self.hlog.begin_address()
  }

  /// 获取已安全落盘至介质的最高连续逻辑边界（FlushedUntilAddress）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:FlushedUntilAddress
  #[inline]
  pub fn flushed_until_address(&self) -> u64 {
    self.hlog.flushed_until_address()
  }

  /// 将 ReadOnlyAddress 迅速推进至当前 TailAddress（对标 ShiftReadOnlyToTail）
  #[inline]
  pub fn shift_read_only_to_tail(&self) -> u64 {
    let tail = self.tail_address();
    self.shift_read_only_address(tail);
    tail
  }

  /// 推进 ReadOnlyAddress（进入该地址之前的记录将变为只读，后续更新触发 CopyUpdate）
  ///
  /// 复活池联动：随只读线推进显式调度 `purge_below`（对标 C# RevivificationManager 随
  /// SafeReadOnlyAddress 推进的过期槽位清扫），避免已滑出可变区的死槽位滞留池内
  /// 挤占分桶容量（`take` 侧另有惰性清扫兜底，此处为主动版）。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:ShiftReadOnlyAddress
  #[inline]
  pub fn shift_read_only_address(&self, new_ro: u64) {
    self.hlog.shift_read_only_address(new_ro);
    if self.config.enable_revivification {
      self.reviv_pool.purge_below(new_ro);
    }
  }

  /// 推进 HeadAddress（进入该地址之前的记录将被逐出内存，后续读取转为磁盘异步 I/O）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:ShiftHeadAddress
  #[inline]
  pub fn shift_head_address(&self, new_head: u64) {
    self.hlog.shift_head_address(new_head);
  }

  /// 推进 BeginAddress（进入该地址之前的数据将被截断清理）
  ///
  /// 截断成功后显式调度复活池 `purge_below`：低于新截断线的槽位已物理失效，
  /// 立即清退防止复活写入命中已截断地址。
  /// 同步通知 RangeIndex 管理器回收地址低于新截断线的 flush 快照
  /// (对标 Garnet `GarnetRecordTriggers.OnTruncate`)。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:ShiftBeginAddress
  pub async fn shift_begin_address(&self, new_begin: u64) -> Result<()> {
    self
      .hlog
      .shift_begin_address(new_begin)
      .await
      .map_err(Error::from)?;
    if self.config.enable_revivification {
      self.reviv_pool.purge_below(new_begin);
    }
    if let Err(e) = self.range_index.on_truncate(new_begin) {
      log::warn!("range_index on_truncate({new_begin}) 回收快照失败: {e}");
    }
    Ok(())
  }

  /// 物理截断历史存储段文件（对标 Garnet `store.Log.Truncate()`）
  ///
  /// 破坏性操作：物理截断并删除底层设备上低于当前 `begin_address` 的所有段文件。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:Truncate
  pub async fn truncate(&self) -> Result<()> {
    let begin = self.hlog.begin_address();
    self
      .device
      .truncate_until_address(begin)
      .await
      .map_err(Error::from)?;
    if self.config.enable_revivification {
      self.reviv_pool.purge_below(begin);
    }
    if let Err(e) = self.range_index.on_truncate(begin) {
      log::warn!("range_index on_truncate({begin}) 回收快照失败: {e}");
    }
    Ok(())
  }

  /// 扫描内存区并物理删除已过期键（对标 Garnet ExpiredKeyDeletionScan / EXPDELSCAN）
  ///
  /// 参数 `db_id`: None 表示默认数据库 0，Some(db) 过滤指定数据库；
  /// 返回 `(num_expired_keys_deleted, total_records_scanned)`。
  pub async fn expired_key_deletion_scan(
    self: &Arc<Self>,
    db_id: Option<u64>,
  ) -> Result<(u64, u64)> {
    let from = self.hlog.read_only_address();
    let until = self.hlog.tail_address();
    let session = self.new_session()?;
    // 过期判定基准：.NET Ticks（与 TTL 记录值同域）
    let now = now_ticks();
    let target_db = db_id.unwrap_or(0);
    let mut scanned = 0u64;
    let mut to_expire: HashSet<(u64, Box<[u8]>)> = HashSet::with_hasher(GxBuildHasher::default());

    self
      .hlog
      .scan(from, until, |_, rec| {
        scanned += 1;
        if rec.is_tombstone() {
          return Ok(true);
        }
        if let Some((ns, db, user_key)) = StoreSession::<D>::user_key_from_ttl_key(rec.key)
          && db == target_db
          && let Ok(be) = <[u8; TTL_VALUE_LEN]>::try_from(rec.value)
          // 到期判定取严格小于（读路径口径，与 probe_ttl/check_expired 一致）
          && i64::from_be_bytes(be) < now
        {
          // 同步内存探针双检初筛：若最新态已不过期（续期）或已删除（墓碑），跳过无谓收集
          session.set_context(ns, db);
          if matches!(session.probe_ttl(user_key, now), TtlProbe::Pass) {
            return Ok(true);
          }
          to_expire.insert((ns, Box::from(user_key)));
        }
        Ok(true)
      })
      .await?;

    let mut deleted = 0u64;
    for (ns, key) in to_expire {
      session.set_context(ns, target_db);
      if session.check_expired(&key).await? {
        deleted += 1;
      }
    }
    Ok((deleted, scanned))
  }

  /// 统计 keyspace 存活键数量与其中设置 TTL 的数量（对标 Garnet
  /// `StoreWrapper.GetKeyspaceStats` / `INFO KEYSPACE` 命令）
  ///
  /// 返回 `(key_count, expire_count)`：`key_count` 为存活（未过期）用户键数，
  /// 按 `(ns, db, 用户键)` 去重（同一键多个历史版本只计一次）；`expire_count`
  /// 为其中设置了 TTL 记录的存活键数。
  ///
  /// 实现为全日志两阶段扫描（对标 Garnet `KeyspaceStats` 的哈希索引 lookup
  /// 迭代；wedb 的 windex 无按键遍历 API，索引槽位仅承载 (bucket, tag) 链头，
  /// tag 碰撞键须经 prev_address 链回溯才能枚举，退化为乱序版日志扫描，故择优
  /// 顺序扫描）：
  /// 1. 顺序扫描 `[begin_address, tail_address)` 全区间（含磁盘冷区，
  ///    [`HybridLog::scan`] 自动读盘），跳过墓碑与非用户面物理键（仅收
  ///    String/Meta 标签；集合子键与 TTL 旁路记录不作候选），收集去重候选
  ///    `(ns, db, 用户键)`；
  /// 2. 逐候选经会话读路径（哈希索引取最新态，免疫复活导致的地址乱序）复判：
  ///    字符串记录命中或集合元记录 size > 0 任一成立视为存活；是否有 TTL 以
  ///    TTL 记录最新版判定（ttl_of）；已过期键两栏均不计。探针为纯读，不触发
  ///    惰性物理清除（区别于 contains_key / check_expired），统计零写副作用。
  ///
  /// 并发防护对标 Garnet `KeyspaceScanLock`：专用扫描会话懒建复用，并发调用
  /// 后到者降级为一次性临时会话（读路径无共享可变状态，无正确性风险）。
  pub async fn keyspace_stats(self: &Arc<Self>) -> Result<(u64, u64)> {
    let session = self.keyspace_scan_session.take(self)?;
    let from = self.hlog.begin_address();
    let until = self.hlog.tail_address();
    // 过期判定基准：.NET Ticks（与 TTL 记录值同域）
    let now = now_ticks();
    let mut candidates: HashSet<(u64, u64, Box<[u8]>)> =
      HashSet::with_hasher(GxBuildHasher::default());

    self
      .hlog
      .scan(from, until, |_, rec| {
        if rec.is_tombstone() {
          return Ok(true);
        }
        if let Ok((ns, db, tag, user_key)) = NamespaceDbCodec::decode_tagged_key(rec.key)
          && matches!(tag, KeyTag::String | KeyTag::Meta)
        {
          candidates.insert((ns, db, Box::from(user_key)));
        }
        Ok(true)
      })
      .await?;

    let mut key_count = 0u64;
    let mut expire_count = 0u64;
    for (ns, db, key) in &candidates {
      session.set_context(*ns, *db);
      // 存活判定：字符串记录命中，或集合元记录存在且 size > 0（幽灵元记录不计）
      let str_k = session.session_string_key(key);
      let alive = session.read_raw_with(&str_k, |_| ()).await?.is_some();
      let alive = if alive {
        true
      } else {
        let meta_k = session.session_meta_key(key);
        match session.read_raw(&meta_k).await? {
          Some(bytes) => matches!(MetaValue::read_size(&bytes), Ok(size) if size > 0),
          None => false,
        }
      };
      if !alive {
        continue;
      }
      // 是否有 TTL 与是否过期均以 TTL 记录最新版判定；已过期键两栏均不计
      let ttl = session.ttl_of(key).await?;
      if ttl.is_some_and(|exp| exp < now) {
        continue;
      }
      key_count += 1;
      if ttl.is_some() {
        expire_count += 1;
      }
    }

    self.keyspace_scan_session.restore(session);
    Ok((key_count, expire_count))
  }

  /// 获取混合日志分配器引用
  #[inline]
  pub fn hlog(&self) -> &Arc<HybridLog<D>> {
    &self.hlog
  }
}

impl<D: Device> Drop for WedbStore<D> {
  fn drop(&mut self) {
    // 先停内置 GC：协作标志置位 + 任务取消兜底，防止后台循环在引擎资源拆除后
    // 继续触达 hlog/会话（句柄强引用不构成环，GC 任务本身亦持引擎弱引用）
    if let Some(h) = self.gc.take() {
      h.stop();
    }
    self.range_index.dispose();
    if let Some(tmp_dir) = &self.temp_range_index_dir {
      let _ = fs::remove_dir_all(tmp_dir);
    }
  }
}
