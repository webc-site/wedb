use std::{
  env, fs,
  path::{Path, PathBuf},
  process,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, AtomicUsize, Ordering},
  },
};

use compio::runtime::Runtime;
use itoa::Buffer;
use parking_lot::RwLock;
use wbase::time::now_ms;
use wdev::Device;
use wepoch::LightEpoch;
use whasher::{
  GxBuildHasher, HashSet,
  papaya::{HashMap as PapayaMap, Operation},
};
use whlog::HybridLog;
use windex::{HashBucket, HashBucketEntry, HashIndex};
use wval::{KeyTag, MetaValue, NamespaceDbCodec};

use crate::{
  config::{GcConfig, StoreConfig},
  error::{Error, Result},
  gc,
  read_cache::{ReadCache, absolute_address, is_read_cache_addr},
  session::{SessionSlot, StoreSession},
  ttl::{TTL_VALUE_LEN, TtlProbe},
};

/// 集合版本映射并发字典（基于无锁高效 papaya 与硬件向量加速 GxBuildHasher）
pub type KeyIdVersionsMap = PapayaMap<u64, (u64, bool), GxBuildHasher>;

/// 写监听回调端口（对标 C# Tsavorite IFunctions 的 Post 回调挂点）
///
/// 参数为 `(key, val, tombstone)`：记录成功写入混合日志内存后同栈触发，
/// 服务端据此把写效果追加进 AOF 日志兑现增量持久化与复制流。
/// 回调位于写入热路径，实现必须无阻塞、无系统调用。
pub type WriteListenerFn = Arc<dyn Fn(&[u8], &[u8], bool) + Send + Sync>;

/// RangeIndex 写监听回调端口
///
/// 参数为 `(key, field, value, delete)`：RangeIndex 字段写成功后同栈触发，
/// 服务端据此把 RangeIndex 写效果（独立树文件中的唯一副本）追加进 AOF。
/// 回调位于写入热路径，实现必须无阻塞、无系统调用、绝不 panic
/// （release 下 panic=abort 等价进程崩溃）。
pub type RangeIndexListenerFn = Arc<dyn Fn(&[u8], &[u8], &[u8], bool) + Send + Sync>;

/// TTL 过期物理清除监听回调端口
///
/// 参数为 `(ns, db, 用户键, expire_at_ms)`：purge 链（TTL 记录 + 数据双删）成功
/// 完成后同栈触发恰好一次，宿主（AOF 适配器）据此把整次过期清除折叠为
/// **单条**确定性逻辑条目入 WAL——对标 Garnet `InputHeader` 的
/// `RespInputFlags.Deterministic` 携带绝对过期时间的单条目语义（见
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs 的 WriteLogRMW），
/// 副本重放零漂移且流内条目数最小。
///
/// 端口在场时 purge 链内两条物理墓碑不再经 [`WriteListenerFn`] 镜像（会话级
/// 精确抑制，见 [`Self::purge_suppress`]）；未注册时保持现状两条物理条目
/// （嵌入式无 AOF 场景行为不变）。回调位于清除路径，实现必须无阻塞、
/// 无系统调用、绝不 panic（release 下 panic=abort 等价进程崩溃）。
pub type TtlPurgeListenerFn = Arc<dyn Fn(u64, u64, &[u8], u64) + Send + Sync>;

/// 创建集合版本映射字典
#[inline]
pub fn new_key_id_versions_map() -> KeyIdVersionsMap {
  PapayaMap::builder()
    .hasher(GxBuildHasher::default())
    .build()
}

/// key_id 分配安全余量（恢复时在持久化水位之上预留的分配额度）
///
/// Checkpoint 仅持久化创建时点的 next_key_id 水位，其后至进程崩溃之间的新集合分配
/// 不随快照落盘；恢复时以 `持久化水位 + KEY_ID_ASSIGN_MARGIN` 作为 fetch_max 下限，
/// 保证新进程分配的 key_id 严格大于上一进程所有可能已分配的值——即使墙钟回退
/// （NTP 步进 / VM 快照回滚）使 generate_initial_key_id 落入历史区间也不会复用。
pub const KEY_ID_ASSIGN_MARGIN: u64 = 1 << 20;

/// Microsoft Garnet Tsavorite 顶层混合存储引擎
///
/// 紧凑整合无锁哈希索引（HashIndex）、混合日志环形缓冲区（HybridLog）、
/// 纪元并发保护器（LightEpoch）与底层块存储设备（Device）。
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
  /// 纪元并发保护管理器
  pub epoch: Arc<LightEpoch>,
  /// 底层块存储设备
  pub device: Arc<D>,
  /// 下一个集合唯一 ID 分配计数器（原子无锁递增）
  pub next_key_id: AtomicU64,
  /// 基于磁盘的块级有序 BfTree 存储引擎（用于千万级 ZSet、范围查询与有序扫描）
  pub bftree: Arc<wbftree::BfTreeService>,
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
  /// TTL 过期物理清除监听端口（宿主经 [`Self::set_ttl_purge_listener`] 注入；
  /// 端口在场即视为 purge 链的"AOF 适配器在场"）
  ttl_purge_listener: OnceLock<TtlPurgeListenerFn>,
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
  /// 内部创建的 RangeIndex 临时根目录（若非用户显式配置则在 Drop 时自动清理闭环）
  temp_range_index_dir: Option<PathBuf>,
  /// 内部创建的 BfTree 临时数据文件（若非用户显式配置则在 Drop 时自动清理闭环）
  temp_bftree_path: Option<PathBuf>,
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

  /// 初始化 BfTree 服务及可能的临时文件路径
  ///
  /// 持久工作文件的两种合法形态：
  /// - CPR 快照镜像（含 `BF-TREE-V0-BEGIN` 魔数，由恢复预置拷贝写入）→ 从其恢复打开；
  /// - 不存在 → 全新 open_disk。
  ///
  /// 无魔数的残留文件只能是未被任何 Checkpoint 覆盖过的孤儿基文件
  /// （`BfTree::with_config` 恒建新树，open_disk 产物不是快照格式），直接删除重建。
  fn init_bftree(config: &StoreConfig) -> Result<(Arc<wbftree::BfTreeService>, Option<PathBuf>)> {
    if let Some(path) = &config.bftree_path {
      // 清理上一轮 recover_in_place 的 `.recovering` 残留（各崩溃点均已自洽，
      // 残留仅占空间，下次恢复会重新覆盖生成）
      let mut tmp_os = path.as_os_str().to_os_string();
      tmp_os.push(".recovering");
      let tmp_path = PathBuf::from(tmp_os);
      if tmp_path.exists() {
        let _ = fs::remove_file(&tmp_path);
      }
      // 魔数判定复用 wbftree 引擎侧同一实现（单一事实源，杜绝两处魔数漂移）
      if path.exists() && wbftree::file_has_cpr_magic(path) {
        match wbftree::BfTreeService::recover_from_cpr_snapshot(
          path,
          true,
          wbftree::StorageBackendType::Disk,
        ) {
          Ok(service) => return Ok((Arc::new(service), None)),
          Err(e) => {
            log::warn!(
              "BfTree 工作文件恢复失败，删除重建: path={}, err={e}",
              path.display()
            );
          }
        }
        fs::remove_file(path)?;
      } else if path.exists() {
        log::warn!(
          "BfTree 工作文件为无魔数孤儿基文件（未被 Checkpoint 覆盖），删除重建: path={}",
          path.display()
        );
        fs::remove_file(path)?;
      }
      Ok((Arc::new(wbftree::BfTreeService::open_disk(path, 4)?), None))
    } else {
      let mut buf = Buffer::new();
      let mut name = String::from("wbftree_");
      name.push_str(buf.format(process::id()));
      name.push('_');
      name.push_str(buf.format(now_ms()));
      name.push('_');
      name.push_str(buf.format(fastrand::u64(..)));
      name.push_str(".data");
      let tmp = env::temp_dir().join(name);
      Ok((
        Arc::new(wbftree::BfTreeService::open_disk(&tmp, 4)?),
        Some(tmp),
      ))
    }
  }

  /// 初始化 RangeIndex 管理器及可能的临时目录路径
  fn init_range_index(config: &StoreConfig) -> (Arc<wbftree::RangeIndexManager>, Option<PathBuf>) {
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
    (
      Arc::new(wbftree::RangeIndexManager::new(ri_log_root, cpr_dir)),
      temp_range_index_dir,
    )
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

  /// 核心组件装配公共体（open / from_components / from_components_with_bftree 共享尾段）
  ///
  /// 调用方须已完成 [`StoreConfig::validate`] 预检并自行决定 BfTree 来源；
  /// RangeIndex 管理器、复活池、ReadCache、GC 运行态句柄等纯派生组件在此统一装配。
  /// ReadCache 创建带降级兜底：validate 已保证 page_size/read_cache_num_pages 为
  /// 非零 2 的幂，构造失败仅可能是资源层异常，降级为禁用配置留痕运行而非中止装配。
  fn assemble(
    config: StoreConfig,
    index: Arc<HashIndex>,
    hlog: Arc<HybridLog<D>>,
    epoch: Arc<LightEpoch>,
    device: Arc<D>,
    bftree: Arc<wbftree::BfTreeService>,
    temp_bftree_path: Option<PathBuf>,
  ) -> Self {
    let (range_index, temp_range_index_dir) = Self::init_range_index(&config);
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
    Self {
      config,
      index,
      hlog,
      epoch,
      device,
      next_key_id: AtomicU64::new(Self::generate_initial_key_id()),
      bftree,
      range_index,
      reviv_pool,
      read_cache,
      key_id_versions: new_key_id_versions_map(),
      gc: OnceLock::new(),
      gc_cfg,
      write_listener: OnceLock::new(),
      range_listener: OnceLock::new(),
      ttl_purge_listener: OnceLock::new(),
      purge_suppress: AtomicUsize::new(0),
      temp_range_index_dir,
      temp_bftree_path,
      keyspace_scan_session: SessionSlot::new(),
    }
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
    let (bftree, temp_bftree_path) = Self::init_bftree(&config)?;
    Ok(Self::assemble(
      config,
      index,
      hlog,
      epoch,
      device,
      bftree,
      temp_bftree_path,
    ))
  }

  /// 从已恢复或外部构建的核心组件创建存储引擎实例（供 Checkpoint 恢复或高级定制使用）
  ///
  /// 容量防线（恢复预检）见 [`Self::check_index_capacity`]。
  pub fn from_components(
    config: StoreConfig,
    index: Arc<HashIndex>,
    hlog: Arc<HybridLog<D>>,
    epoch: Arc<LightEpoch>,
    device: Arc<D>,
  ) -> Result<Self> {
    config.validate()?;
    Self::check_index_capacity(&config, &index)?;
    let (bftree, temp_bftree_path) = Self::init_bftree(&config).unwrap_or_else(|e| {
      // 降级为内存树必须留痕：配置了持久工作文件但恢复失败时静默降级将造成数据丢失假象
      log::warn!("BfTree 初始化失败，降级为内存树（配置的持久工作文件不生效）: err={e}");
      (
        Arc::new(unsafe { wbftree::BfTreeService::open_memory(4).unwrap_unchecked() }),
        None,
      )
    });
    Ok(Self::assemble(
      config,
      index,
      hlog,
      epoch,
      device,
      bftree,
      temp_bftree_path,
    ))
  }

  /// 从已恢复或外部构建的核心组件及已有 BfTree 引擎创建存储引擎实例
  ///
  /// 容量防线同 [`Self::from_components`]：`config.index_size` 必须与
  /// `index.size` 严格一致，否则显式报错，绝不静默缩表或配置漂移。
  pub fn from_components_with_bftree(
    config: StoreConfig,
    index: Arc<HashIndex>,
    hlog: Arc<HybridLog<D>>,
    epoch: Arc<LightEpoch>,
    device: Arc<D>,
    bftree: Arc<wbftree::BfTreeService>,
  ) -> Result<Self> {
    config.validate()?;
    Self::check_index_capacity(&config, &index)?;
    Ok(Self::assemble(
      config, index, hlog, epoch, device, bftree, None,
    ))
  }

  /// 抬升 key_id 分配水位下限（fetch_max 单调语义，低值永不回退已推进的水位）
  ///
  /// 恢复路径必须调用：`floor = 持久化 next_key_id + KEY_ID_ASSIGN_MARGIN`。
  /// key_id 是子键物理键（ns+tag+key_id+version+field）的组成部分，墙钟回退时
  /// [`Self::generate_initial_key_id`] 可能生成与上一进程相同的 key_id，复用即
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
  pub fn set_write_listener(&self, listener: WriteListenerFn) -> bool {
    self.write_listener.set(listener).is_ok()
  }

  /// 读取写监听端口
  #[inline]
  pub(crate) fn write_listener(&self) -> Option<&WriteListenerFn> {
    self.write_listener.get()
  }

  /// 注入 RangeIndex 写监听端口（须在产生 RangeIndex 写之前调用；重复注入返回 false）
  pub fn set_range_listener(&self, listener: RangeIndexListenerFn) -> bool {
    self.range_listener.set(listener).is_ok()
  }

  /// 读取 RangeIndex 写监听端口
  #[inline]
  pub(crate) fn range_listener(&self) -> Option<&RangeIndexListenerFn> {
    self.range_listener.get()
  }

  /// 注入 TTL 过期物理清除监听端口（重复注入返回 false）
  ///
  /// 端口注册后 purge 链即进入"单条确定性逻辑条目"模式：物理墓碑镜像抑制 +
  /// 清除完成后触发一次回调，语义详见 [`TtlPurgeListenerFn`]
  pub fn set_ttl_purge_listener(&self, listener: TtlPurgeListenerFn) -> bool {
    self.ttl_purge_listener.set(listener).is_ok()
  }

  /// 读取 TTL 过期物理清除监听端口
  #[inline]
  pub(crate) fn ttl_purge_listener(&self) -> Option<&TtlPurgeListenerFn> {
    self.ttl_purge_listener.get()
  }

  /// 将内存中所有驻留脏页异步刷盘并同步设备
  pub async fn flush_all(&self) -> Result<()> {
    let head = self.hlog.head_address();
    let tail = self.hlog.tail_address();

    if tail > head {
      let start_page = self.hlog.config.page_id(head);
      let end_page = self.hlog.config.page_id(tail.saturating_sub(1));
      if start_page <= end_page {
        self.hlog.flush_pages_range(start_page, end_page).await?;
      }
    }
    self.device.sync().await.map_err(Error::from)?;
    Ok(())
  }

  /// 将内存所有页面刷盘并全部驱逐至磁盘区（对标 Tsavorite FlushAndEvict）
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

  /// 获取临时 BfTree 文件路径（若未显式指定 bftree_path）
  #[inline]
  pub fn temp_bftree_path(&self) -> Option<&Path> {
    self.temp_bftree_path.as_deref()
  }

  /// 获取哈希索引中已记录的有效条目总数（对标 Tsavorite GetEntryCount）
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

  /// 获取当前只读区分界逻辑地址（ReadOnlyAddress）
  #[inline]
  pub fn read_only_address(&self) -> u64 {
    self.hlog.read_only_address()
  }

  /// 获取当前内存头逻辑地址（HeadAddress）
  #[inline]
  pub fn head_address(&self) -> u64 {
    self.hlog.head_address()
  }

  /// 获取有效数据起始逻辑地址（BeginAddress）
  #[inline]
  pub fn begin_address(&self) -> u64 {
    self.hlog.begin_address()
  }

  /// 推进 ReadOnlyAddress（进入该地址之前的记录将变为只读，后续更新触发 CopyUpdate）
  ///
  /// 复活池联动：随只读线推进显式调度 `purge_below`（对标 C# RevivificationManager 随
  /// SafeReadOnlyAddress 推进的过期槽位清扫），避免已滑出可变区的死槽位滞留池内
  /// 挤占分桶容量（`take` 侧另有惰性清扫兜底，此处为主动版）。
  #[inline]
  pub fn shift_read_only_address(&self, new_ro: u64) {
    self.hlog.shift_read_only_address(new_ro);
    if self.config.enable_revivification {
      self.reviv_pool.purge_below(new_ro);
    }
  }

  /// 推进 HeadAddress（进入该地址之前的记录将被逐出内存，后续读取转为磁盘异步 I/O）
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
  pub async fn shift_begin_address(&self, new_begin: u64) -> Result<()> {
    self
      .hlog
      .shift_begin_address(new_begin)
      .await
      .map_err(Error::from)?;
    if self.config.enable_revivification {
      self.reviv_pool.purge_below(new_begin);
    }
    if let Err(e) = self.range_index.on_truncate(new_begin as i64) {
      log::warn!("range_index on_truncate({new_begin}) 回收快照失败: {e}");
    }
    Ok(())
  }

  /// 物理截断历史存储段文件（对标 Garnet `store.Log.Truncate()`）
  ///
  /// 破坏性操作：物理截断并删除底层设备上低于当前 `begin_address` 的所有段文件。
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
    if let Err(e) = self.range_index.on_truncate(begin as i64) {
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
    let now = now_ms();
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
          && u64::from_be_bytes(be) <= now
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
    let now = now_ms();
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
      if ttl.is_some_and(|exp| exp <= now) {
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

  /// 获取块级有序 BfTree 引擎引用
  #[inline]
  pub fn bftree(&self) -> &Arc<wbftree::BfTreeService> {
    &self.bftree
  }

  /// 底层 KV 有序范围扫描（闭区间 [start, end]，零拷贝回调直通内置 BfTree）
  ///
  /// 引擎底层扫描能力的最小暴露面：不经会话前缀/元数据/TTL/分数键编码等任何上层
  /// 路由，直接对原始 KV 执行字典序扫描；回调在扫描栈缓冲上原位消费 (k, v) 切片，
  /// 返回 false 提前终止。返回实际扫描记录数。
  pub fn scan_range_callback<F>(&self, start: &[u8], end: &[u8], on_record: F) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    self
      .bftree
      .scan_with_end_key_callback(start, end, wbftree::ScanReturnField::KeyAndValue, on_record)
      .map_err(Error::from)
  }
}

impl<D: Device> Drop for WedbStore<D> {
  fn drop(&mut self) {
    // 先停内置 GC：协作标志置位 + 任务取消兜底，防止后台循环在引擎资源拆除后
    // 继续触达 hlog/会话（句柄强引用不构成环，GC 任务本身亦持引擎弱引用）
    if let Some(h) = self.gc.take() {
      h.stop();
    }
    self.bftree.dispose();
    if let Some(tmp_path) = &self.temp_bftree_path {
      let _ = fs::remove_file(tmp_path);
    }
    self.range_index.dispose();
    if let Some(tmp_dir) = &self.temp_range_index_dir {
      let _ = fs::remove_dir_all(tmp_dir);
    }
  }
}
