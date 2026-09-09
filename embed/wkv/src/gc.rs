//! 内置 GC：主动过期清理与日志紧缩的后台引擎（对标 Garnet ExpiredKeyDeletionTask + CompactionTask）。
//!
//! 扫描分两段，主路径对标 Garnet `[ReadOnlyAddress, TailAddress]` 滑动窗口语义：
//! - 热区 `[read_only, tail)`：每轮无游标全量窗口扫描。纯内存指针行走、零磁盘 I/O
//!   （窗口由 hlog 内存缓冲区天然约束，与 Garnet 每 tick 全窗口同一取舍），刚写入
//!   即过期的短 TTL 键至多一个扫描间隔即被物理清除；
//! - 冷区 `[cold_cursor, read_only)`：游标增量推进，单轮受 `max_scan_records` 有界
//!   （磁盘区 I/O 有界，wedb 增强）。长 TTL 键在落盘冷区过期后不留死角，游标最终覆盖。
//!
//! 候选经最新 TTL 双检（内存探针初筛 + 删除阶段 `check_expired` 终审）后，走与用户
//! DEL 完全一致的统一删除路径；删除完成后才提交游标，取消或失败下一轮重试（幂等）。
//! 紧缩按阈值判定经 `wcompact::LogCompactor` 执行；紧缩适配器删除谓词恒假——必须
//! 保留最新 TTL 记录（TTL 与数据分开存储，若只丢弃过期 TTL 而迁移数据，会产生
//! 永不过期的键），过期清理完全交给本模块。
//!
//! 配置为运行态共享句柄（`WedbStore::gc_cfg`），驱动循环每轮重读（对标 Garnet 每
//! 轮读取 RuntimeServerConfig），`WedbStore::update_gc_config` 热更新下一轮生效；
//! Garnet CompactionTask 频率在 wedb 由 `GcConfig::compaction_interval_ms` 统一承担。
//! 扫描会话懒建复用（对标 Garnet StoreExpiredKeyDeletionDbStorageSession）。
//! 扫描观测对标 Garnet `ExpiredKeyDeletionScan` 的 `(numExpiredKeysFound,
//! totalRecordsScanned)` 双口径（见 [`GcStatsSnapshot`]）。
//!
//! 第三段：hash 字段级过期收集（对标 Garnet StoreWrapper.ObjectCollectTaskAsync
//! 按 EXPIRED_OBJECT_COLLECTION_FREQ 周期驱动 storageSession.HashCollect 收集
//! 对象内过期成员）。与 Garnet 的差异：Garnet 为独立频率配置的独立后台任务，
//! 直接遍历内存对象内的过期 field；wedb 的字段 TTL 以紧凑载荷内联存储，本引擎
//! 在统一 GC 循环内并列复用既有热区/冷区两段扫描（过滤链扩展识别带 has_expire
//! 粘性标志的 Meta 元记录产出候选，经 `collect_expired_hash_fields` 持锁双检后
//! 压缩回写），不单设 EXPIRED_OBJECT_COLLECTION_FREQ 配置项、扫描频率由
//! `scan_interval_ms` 统一承担——理由：字段级候选与 key 级候选同源于同一份日志，
//! 合并单遍扫描省一遍全量 I/O，且统一执行闸天然防两段收集互相饿活。
//!
//! 驱动分三层：[`GcManager::run_once`]（单轮纯逻辑）、[`GcManager::drive`]（强引用
//! 循环，供上层调度器接管）、[`GcManager::spawn`]（内置弱引用循环，引擎 Drop 自动
//! 退出）。

use std::{
  ops::Range,
  sync::{
    Arc, Weak,
    atomic::{
      AtomicBool, AtomicU64,
      Ordering::{Acquire, Relaxed, Release},
    },
  },
  time::Duration,
};

use compio::{
  runtime::{JoinHandle, spawn},
  time::sleep,
};
use log::{info, warn};
use parking_lot::{Mutex, RwLock};
use wbase::time::now_ms;
use wcompact::CompactionType;
use wdev::Device;
use whasher::{GxBuildHasher, HashSet};
use wval::{KeyTag, NamespaceDbCodec};

use crate::{
  config::GcConfig,
  error::Result,
  session::StoreSession,
  store::WedbStore,
  ttl::{TTL_VALUE_LEN, TtlProbe},
};

/// 后台循环扫描间隔下限毫秒（防 scan_interval_ms=0 退化为忙轮询）
const MIN_SCAN_INTERVAL_MS: u64 = 10;

/// 过期候选集：(ns, db, 用户键)。收集与删除两阶段共用（类型别名收敛复合泛型实例化）
type ExpiredKeySet = HashSet<(u64, u64, Box<[u8]>)>;

/// 内置 GC 原子计数器（Relaxed 语义：仅观测，无同步依赖）
#[derive(Default)]
struct GcStats {
  /// 累计物理删除的过期键数
  expired_deleted: AtomicU64,
  /// 累计后台字段收集中物理清除的过期 Hash 字段数（对标 Garnet object collect 可观测性）
  expired_fields_deleted: AtomicU64,
  /// 累计紧缩执行次数
  compactions: AtomicU64,
  /// 最近一轮过期扫描物理删除数
  last_scan_deleted: AtomicU64,
  /// 最近一轮后台字段收集物理清除的字段数
  last_scan_fields_deleted: AtomicU64,
  /// 最近一轮过期扫描记录数（两段扫描求和，对标 Garnet totalRecordsScanned 口径）
  last_scan_scanned: AtomicU64,
  /// 累计过期扫描记录数（各轮 last_scan_scanned 求和）
  total_scanned: AtomicU64,
  /// 最近一轮紧缩丢弃记录数
  last_compact_dropped: AtomicU64,
}

/// 内置 GC 统计快照（轻量 Copy 结构）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GcStatsSnapshot {
  /// 累计物理删除的过期键数
  pub expired_deleted: u64,
  /// 累计后台字段收集中物理清除的过期 Hash 字段数（对标 Garnet
  /// ObjectCollectTask / HashCollect 的对象收集可观测性）
  pub expired_fields_deleted: u64,
  /// 累计紧缩执行次数
  pub compactions: u64,
  /// 最近一轮过期扫描物理删除数
  pub last_scan_deleted: u64,
  /// 最近一轮后台字段收集物理清除的字段数
  pub last_scan_fields_deleted: u64,
  /// 最近一轮过期扫描记录数（热区 + 冷区两段求和，对标 Garnet
  /// `ExpiredKeyDeletionScan` 返回的 `totalRecordsScanned`）
  pub last_scan_scanned: u64,
  /// 累计过期扫描记录数（各轮求和；观测扫描吞吐与游标推进进度）
  pub total_scanned: u64,
  /// 最近一轮紧缩丢弃记录数
  pub last_compact_dropped: u64,
}

/// 内置 GC 管理器共享核（后台循环与 [`GcHandle`] 共享）
pub struct GcManager<D: Device> {
  /// 引擎弱引用：不与 WedbStore 构成强引用环，引擎 Drop 后循环自行退出
  store: Weak<WedbStore<D>>,
  /// 运行态 GC 配置共享句柄（与 `WedbStore::gc_cfg` 同源；每轮重读支持热更新）
  cfg: Arc<RwLock<GcConfig>>,
  /// 协作取消标志（compio JoinHandle 无 abort，循环每轮轮询）
  cancel: AtomicBool,
  /// 单轮执行闸：后台循环与手动 run_once 并发时跳过后到者
  pub(crate) inflight: AtomicBool,
  /// 上次紧缩判定的毫秒时间戳（0 = 尚未判定，首轮即评估）
  last_compact_ms: AtomicU64,
  /// 冷区欠账游标（已扫描到的日志地址；0 = 自 begin 起）。恒钳制在只读线以下，
  /// 热区由每轮窗口扫描覆盖；紧缩推进 begin 后经 max(begin) 自动适配
  cold_cursor: AtomicU64,
  /// 复用的扫描会话（懒建，对标 Garnet 专用扫描 StorageSession；取用-归还两段式，
  /// 不在互斥守卫内跨 await，异常路径丢弃由下轮重建）
  sweep_session: Mutex<Option<StoreSession<D>>>,
  stats: GcStats,
}

impl<D: Device> GcManager<D> {
  /// 创建 GC 管理器（共享 `store.gc_cfg` 运行态配置；不启动后台循环，供测试手动驱动）
  pub fn new(store: &Arc<WedbStore<D>>) -> Self {
    Self {
      store: Arc::downgrade(store),
      cfg: Arc::clone(&store.gc_cfg),
      cancel: AtomicBool::new(false),
      inflight: AtomicBool::new(false),
      last_compact_ms: AtomicU64::new(0),
      cold_cursor: AtomicU64::new(0),
      sweep_session: Mutex::new(None),
      stats: GcStats::default(),
    }
  }

  /// 当前扫描间隔毫秒（每轮重读运行态配置；下限钳制防 0 值忙轮询）
  fn scan_interval_ms(&self) -> u64 {
    self.cfg.read().scan_interval_ms.max(MIN_SCAN_INTERVAL_MS)
  }

  /// 派生内置 GC 后台循环（须在 compio 运行时内调用）
  ///
  /// 弱引用驱动：每间隔升级一次弱引用读配置并执行单轮，间隔内不持强引用——
  /// 引擎或句柄全部释放后循环在一个间隔内自行退出。`D: 'static` 为 compio
  /// `spawn` 的 'static 任务硬性要求（未来体捕获引擎弱引用），仅约束本方法。
  pub fn spawn(store: Arc<WedbStore<D>>) -> GcHandle<D>
  where
    D: Device + 'static,
  {
    let mgr = Arc::new(Self::new(&store));
    let weak = Arc::downgrade(&mgr);
    let join = spawn(async move {
      loop {
        // 睡前读间隔：update_gc_config 热更新下一轮生效（对标 Garnet 每轮重读
        // RuntimeServerConfig）；读后立即释放强引用保证释放语义不被任务自持阻断
        let interval_ms = match weak.upgrade() {
          None => return,
          Some(m) => {
            if m.cancel.load(Relaxed) {
              return;
            }
            let ms = m.scan_interval_ms();
            drop(m);
            ms
          }
        };
        sleep(Duration::from_millis(interval_ms)).await;
        let Some(m) = weak.upgrade() else {
          return;
        };
        // 单轮失败仅留痕不中断循环：后台 GC 的韧性优先于快速失败
        if let Err(e) = m.run_once().await {
          warn!("内置 GC 轮次失败，留待下轮: err={e}");
        }
      }
    });
    GcHandle {
      inner: mgr,
      join: Mutex::new(Some(join)),
    }
  }

  /// 强引用驱动循环：每间隔执行一次 [`Self::run_once`]，取消标志置位或引擎
  /// 释放时退出。供上层调度器以自有生命周期接管（调用方持强引用即有意常驻，
  /// 与 [`Self::spawn`] 的弱引用自退出语义互补）
  pub async fn drive(&self) {
    loop {
      sleep(Duration::from_millis(self.scan_interval_ms())).await;
      if self.cancel.load(Relaxed) {
        return;
      }
      if self.store.upgrade().is_none() {
        return;
      }
      if let Err(e) = self.run_once().await {
        warn!("内置 GC 轮次失败，留待下轮: err={e}");
      }
    }
  }

  /// 读取统计快照
  pub fn stats(&self) -> GcStatsSnapshot {
    GcStatsSnapshot {
      expired_deleted: self.stats.expired_deleted.load(Relaxed),
      expired_fields_deleted: self.stats.expired_fields_deleted.load(Relaxed),
      compactions: self.stats.compactions.load(Relaxed),
      last_scan_deleted: self.stats.last_scan_deleted.load(Relaxed),
      last_scan_fields_deleted: self.stats.last_scan_fields_deleted.load(Relaxed),
      last_scan_scanned: self.stats.last_scan_scanned.load(Relaxed),
      total_scanned: self.stats.total_scanned.load(Relaxed),
      last_compact_dropped: self.stats.last_compact_dropped.load(Relaxed),
    }
  }

  /// 当前是否正处于单轮 GC 执行中（执行闸状态）
  #[inline]
  pub fn is_inflight(&self) -> bool {
    self.inflight.load(Relaxed)
  }

  /// 尝试获取单轮执行闸（成功返回 RAII 守卫，已被占用返回 None；供测试与并发模拟）
  #[inline]
  pub fn try_acquire_gate(&self) -> Option<RunGuard<'_>> {
    if self.inflight.swap(true, Acquire) {
      None
    } else {
      Some(RunGuard(&self.inflight))
    }
  }

  /// 单轮 GC：先过期扫描，后按间隔判定紧缩（供后台循环复用，亦可手动驱动）
  pub async fn run_once(&self) -> Result<()> {
    // 单轮闸：上一轮未结束（含后台与手动并发）则跳过本轮
    if self.inflight.swap(true, Acquire) {
      return Ok(());
    }
    let _guard = RunGuard(&self.inflight);
    self.tick().await
  }

  /// 单轮内部实现（调用方须持 inflight 闸）
  async fn tick(&self) -> Result<()> {
    // 引擎已释放（弱引用悬空）：静默退出，无任何可回收对象
    let Some(store) = self.store.upgrade() else {
      return Ok(());
    };
    // 每轮重读一次运行态配置快照（GcConfig 为纯标量结构，clone 即快照）
    let cfg = self.cfg.read().clone();
    let (deleted, fields_deleted, scanned) = self.sweep_expired(&store, &cfg).await?;
    self.stats.expired_deleted.fetch_add(deleted, Relaxed);
    self
      .stats
      .expired_fields_deleted
      .fetch_add(fields_deleted, Relaxed);
    self.stats.last_scan_deleted.store(deleted, Relaxed);
    self
      .stats
      .last_scan_fields_deleted
      .store(fields_deleted, Relaxed);
    self.stats.last_scan_scanned.store(scanned, Relaxed);
    self.stats.total_scanned.fetch_add(scanned, Relaxed);
    self.try_compact(&store, &cfg).await
  }

  /// 取出（或懒建）复用的扫描会话；用毕须 [`Self::restore_sweep_session`] 归还。
  /// 以所有权取还替代在互斥守卫内跨 await，异常路径丢弃会话由下轮懒建重建
  fn take_sweep_session(&self, store: &Arc<WedbStore<D>>) -> Result<StoreSession<D>> {
    match self.sweep_session.lock().take() {
      Some(s) => Ok(s),
      None => store.new_session(),
    }
  }

  /// 归还复用的扫描会话
  fn restore_sweep_session(&self, session: StoreSession<D>) {
    *self.sweep_session.lock() = Some(session);
  }

  /// 两段式过期扫描：热区窗口优先（对标 Garnet 滑动窗口），冷区欠账用剩余删除预算。
  /// 返回 (物理删除数, 扫描记录数)，对标 Garnet `ExpiredKeyDeletionScan` 的
  /// `(numExpiredKeysFound, totalRecordsScanned)` 双口径——扫描记录数为热区与冷区
  /// 两段 `collect_expired` 的 scanned 求和。
  async fn sweep_expired(&self, store: &Arc<WedbStore<D>>, cfg: &GcConfig) -> Result<(u64, u64)> {
    let now = now_ms();
    let cap = cfg.max_batch_deletes.max(1);
    let cold_cap = cfg.max_scan_records.max(1);
    let session = self.take_sweep_session(store)?;
    let mut picked: ExpiredKeySet = HashSet::with_hasher(GxBuildHasher::default());
    let mut scanned = 0u64;

    // 段 1：热区窗口 [read_only, tail) —— 每轮无游标全量扫描（纯内存零 I/O），
    // 记录数不限（与 Garnet 全窗口扫描同一取舍，窗口由内存缓冲区约束），
    // 刚写入即过期的短 TTL 键至多一个扫描间隔即被物理清除
    let read_only = store.read_only_address();
    let tail = store.tail_address();
    if read_only < tail {
      let (n, ..) = Self::collect_expired(
        &session,
        store,
        read_only..tail,
        now,
        cap,
        u64::MAX,
        &mut picked,
      )
      .await?;
      scanned += n;
    }

    // 段 2：冷区欠账 [cold_cursor, read_only) —— 游标增量推进，单轮记录预算有界
    // （磁盘 I/O 有界）；热区候选已占满删除预算时本轮跳过（游标不动，下轮重扫，幂等）
    let mut cold_commit = None;
    if picked.len() < cap {
      let cold_from = self.cold_cursor.load(Relaxed).max(store.begin_address());
      if cold_from < read_only {
        let (n, next_addr, exhausted) = Self::collect_expired(
          &session,
          store,
          cold_from..read_only,
          now,
          cap,
          cold_cap as u64,
          &mut picked,
        )
        .await?;
        scanned += n;
        // 游标恒钳制在只读线以下（热区由段 1 覆盖，不得重复计入冷区欠账）
        cold_commit = Some(if exhausted || next_addr >= read_only {
          read_only
        } else {
          next_addr
        });
      }
    }

    // 收集完成后统一物理删除：逐键双检，走与用户 DEL 完全一致的路径
    // （check_expired 内部经 purge_expired = 删 TTL 记录 + 删数据，索引/墓碑/WAL 一致）
    let mut deleted = 0u64;
    for (ns, db, key) in &picked {
      session.set_context(*ns, *db);
      match session.check_expired(key).await {
        Ok(true) => deleted += 1,
        // 双检未过期：扫描与删除间隙内被用户续期/删除/紧缩，安全放行
        Ok(false) => {}
        Err(e) => warn!("内置 GC 过期删除失败，留待下一扫描周期重试: err={e}"),
      }
    }
    if deleted > 0 {
      info!(
        "内置 GC 过期扫描完成: 候选={}, 物理删除={deleted}",
        picked.len()
      );
    }
    // 删除完成才提交冷区游标；任务取消时重扫当前批，最新 TTL 双检保证幂等
    if let Some(addr) = cold_commit {
      self.cold_cursor.store(addr, Relaxed);
    }
    self.restore_sweep_session(session);
    Ok((deleted, scanned))
  }

  /// 单段候选收集：扫描 `[from, until)` 中至多 `max_records` 条记录，将已过期
  /// TTL 键加入 `picked`（至多 `max_picks` 个）。返回 (扫描数, 游标位置, 是否扫到线头)。
  ///
  /// 过滤链四级：墓碑位单次读取 → 非 TTL 物理键变长前缀反解跳过（零分配）→
  /// 值定长校验（非法长度按无 TTL 容错）→ 到期比较 + 最新态内存探针双检
  /// （陈旧日志版本已续期/已删时放行，防其反复占据批预算饿死存活过期键）。
  async fn collect_expired(
    session: &StoreSession<D>,
    store: &Arc<WedbStore<D>>,
    range: Range<u64>,
    now: u64,
    max_picks: usize,
    max_records: u64,
    picked: &mut ExpiredKeySet,
  ) -> Result<(u64, u64, bool)> {
    let mut scanned = 0u64;
    let mut exhausted = false;
    let mut scan = store.hlog.scan_iter(range.start, range.end);
    loop {
      if picked.len() >= max_picks || scanned >= max_records {
        break;
      }
      let next = scan
        .next_ref(|item| {
          let rec = item.rec;
          // 快路径 1：墓碑位单次读取
          if rec.is_tombstone() {
            return Ok(true);
          }
          // 快路径 2：非 TTL 物理键（变长前缀反解 + 标签比对，零分配）
          let Some((ns, db, user_key)) = StoreSession::<D>::user_key_from_ttl_key(rec.key) else {
            return Ok(true);
          };
          // 值定长校验：非法长度按无 TTL 容错跳过（与读路径口径一致）
          let Ok(be) = <[u8; TTL_VALUE_LEN]>::try_from(rec.value) else {
            return Ok(true);
          };
          if u64::from_be_bytes(be) > now {
            return Ok(true);
          }
          // 陈旧版本双检：该键最新 TTL 态已不过期（续期）或已删除（墓碑）时放行
          session.set_context(ns, db);
          if matches!(session.probe_ttl(user_key, now), TtlProbe::Pass) {
            return Ok(true);
          }
          picked.insert((ns, db, Box::from(user_key)));
          Ok(true)
        })
        .await?;
      match next {
        None => {
          exhausted = true;
          break;
        }
        Some(_) => scanned += 1,
      }
    }
    Ok((scanned, scan.current_address(), exhausted))
  }

  /// 日志紧缩调度（对标 CompactionTask / DatabaseManagerBase.cs:425）
  ///
  /// 触发条件 `read_only - begin > max_segments × segment_size`；回退量
  /// `until = read_only - segment_size × (max - n)`（n 为回退段数，钳制不超过 max），
  /// 保证 `until <= read_only` 满足紧缩器前置校验。直接集成 [`WedbStore::compact`]。
  async fn try_compact(&self, store: &Arc<WedbStore<D>>, cfg: &GcConfig) -> Result<()> {
    // 判定节流：无论是否触发均推进时间戳，判定开销每间隔至多一次
    let now = now_ms();
    let last = self.last_compact_ms.load(Relaxed);
    if last != 0 && now.saturating_sub(last) < cfg.compaction_interval_ms {
      return Ok(());
    }
    self.last_compact_ms.store(now, Relaxed);

    let begin = store.begin_address();
    let read_only = store.read_only_address();
    // segment_size：分段设备取设备段大小（物理回收单元），单文件设备回退 hlog 页大小
    let seg = store
      .device
      .segment_size()
      .unwrap_or(store.hlog.config.page_size as u64);
    let max = cfg.compaction_max_segments as u64;
    // 未超阈值（或阈值/段长为 0 视作紧缩关闭）：不动日志
    if seg == 0 || max == 0 || read_only.saturating_sub(begin) <= max.saturating_mul(seg) {
      return Ok(());
    }
    let n = (cfg.compaction_num_segments.min(cfg.compaction_max_segments)) as u64;
    let until = read_only
      .saturating_sub(seg.saturating_mul(max - n))
      .max(begin);

    let outcome = store.compact(until, CompactionType::Lookup).await?;
    self.stats.compactions.fetch_add(1, Relaxed);
    self
      .stats
      .last_compact_dropped
      .store(outcome.dead_dropped as u64, Relaxed);
    info!(
      "内置 GC 紧缩完成: until={until:#x}, 丢弃={}, 释放={}B, 新起始地址={:#x}（推进 begin 后由设备 truncate_until_address 物理回收已回收段）",
      outcome.dead_dropped, outcome.bytes_freed, outcome.new_begin_address
    );
    Ok(())
  }
}

/// 单轮 GC 执行闸守卫：Drop 时自动释放执行闸，确保任务取消时不残留死锁
pub struct RunGuard<'a>(&'a AtomicBool);

impl Drop for RunGuard<'_> {
  fn drop(&mut self) {
    self.0.store(false, Release);
  }
}

/// 内置 GC 后台循环句柄：`stop()`/Drop 取消，`stats()` 只读观测
pub struct GcHandle<D: Device> {
  inner: Arc<GcManager<D>>,
  /// 后台循环任务句柄；Drop 时内建 task cancel 兜底（协作标志之外的最后一道闸）
  join: Mutex<Option<JoinHandle<()>>>,
}

impl<D: Device> GcHandle<D> {
  /// 请求后台循环退出（协作式：至多再运行一个扫描间隔；可重复调用）
  pub fn stop(&self) {
    self.inner.cancel.store(true, Relaxed);
  }

  /// 读取统计快照
  pub fn stats(&self) -> GcStatsSnapshot {
    self.inner.stats()
  }

  /// 后台循环是否已退出（stop 后至多一个扫描间隔内变真）
  pub fn is_finished(&self) -> bool {
    self
      .join
      .lock()
      .as_ref()
      .is_none_or(JoinHandle::is_finished)
  }

  /// 手动驱动一轮 GC（与后台循环共享单轮闸，并发调用安全；供测试）
  pub async fn run_once(&self) -> Result<()> {
    self.inner.run_once().await
  }
}

impl<D: Device> Drop for GcHandle<D> {
  fn drop(&mut self) {
    self.inner.cancel.store(true, Relaxed);
    // 兜底取消：JoinHandle Drop 内建 task cancel；此刻任务至多处于 sleep 或单轮
    // 中间态，未来体在 await 点安全丢弃（与任意前台任务的取消语义一致）
    self.join.lock().take();
  }
}
