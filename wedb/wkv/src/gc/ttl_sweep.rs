//! TTL 过期键扫描域：两段式过期扫描调度（热区窗口 + 冷区欠账游标，
//! [`GcManager::sweep_expired`]）与候选收集共享内核 [`collect_expired`]
//! （EXPDELSCAN 与本引擎双入口共用，对标 C# StoreExpiredKeyDeletionScan
//! 单内核双入口）。启停与间隔谓词单点见门面件。

use std::{
  ops::Range,
  sync::{Arc, atomic::Ordering::Relaxed},
};

use log::{info, warn};
use wbase::{
  map::{GxBuildHasher, HashSet},
  time::now_ticks,
};
use wdev::Device;

use super::GcManager;
use crate::{
  config::GcConfig,
  error::Result,
  session::StoreSession,
  store::WedbStore,
  ttl::{TtlGate, is_expired},
};

/// 过期候选集：(ns, db, 用户键)。收集与删除两阶段共用（类型别名收敛复合泛型实例化）
pub(crate) type ExpiredKeySet = HashSet<(u64, u64, Box<[u8]>)>;

/// 单段扫描预算（收集内核 [`collect_expired`] 参数收敛：记录数上限与候选数上限；
/// EXPDELSCAN 全量预算双 MAX，GC 热区全量记录、冷区 `max_scan_records` 记录预算）
pub(crate) struct ScanBudget {
  /// 单段记录扫描上限（u64::MAX = 全量）
  pub max_records: u64,
  /// 候选收集上限（usize::MAX = 全量）
  pub max_picks: usize,
}

/// 单段过期候选收集共享内核（EXPDELSCAN 与内置 GC 双入口共用，对标 C# 同构单内核：
/// ArrayKeyIterationFunctions.cs:ExpiredKeysBase —— 后台 ExpiredKeyDeletionScanTaskAsync
/// 与命令 ExpiredKeyDeletionScan(dbId) 均经 DatabaseManagerBase.StoreExpiredKeyDeletionScan
/// 落到同一 Reader；调度差异只在入口，判定与收集内核一处定义）
///
/// 扫描 `[range]` 中至多 `budget.max_records` 条记录，将已过期 TTL 键（经 `db_match`
/// 逻辑库过滤）加入 `picked`（至多 `budget.max_picks` 个）。
/// 返回 (扫描数, 游标位置, 是否扫到线头)。
///
/// 过滤链四级：墓碑位单次读取 → TTL 物理键变长前缀反解（零分配，非 TTL 记录跳过）→
/// 值定长校验（非法长度按无 TTL 容错）+ 到期比较（严格小于读路径口径）+ 最新态
/// 内存探针双检（陈旧日志版本已续期/已删时放行，防其反复占据批预算饿死存活过期键）。
/// `now` 为 .NET Ticks 过期判定基准，与 TTL 记录值同域
pub(crate) async fn collect_expired<D: Device>(
  session: &StoreSession<D>,
  store: &Arc<WedbStore<D>>,
  range: Range<u64>,
  now: i64,
  budget: ScanBudget,
  picked: &mut ExpiredKeySet,
  db_match: &impl Fn(u64, u64) -> bool,
) -> Result<(u64, u64, bool)> {
  let mut scanned = 0u64;
  let mut exhausted = false;
  let mut scan = store.hlog.scan_iter(range.start, range.end);
  loop {
    if picked.len() >= budget.max_picks || scanned >= budget.max_records {
      break;
    }
    let next = scan
      .next_ref(|item| {
        let rec = item.rec;
        // 快路径 1：墓碑位单次读取
        if rec.is_tombstone() {
          return Ok(true);
        }
        // 快路径 2：TTL 物理键反解（变长前缀反解 + 标签比对，零分配），非 TTL 记录跳过
        let Some((ns, db, user_key)) = StoreSession::<D>::user_key_from_ttl_key(rec.key) else {
          return Ok(true);
        };
        // 统一过期判定：未到期或非法长度（按无 TTL 容错）放行跳过（严格小于读路径口径）
        if !is_expired(rec.value, now) || !db_match(ns, db) {
          return Ok(true);
        }
        // 陈旧版本双检：该键最新 TTL 态已不过期（续期）或已删除（墓碑）时放行
        session.set_virtual_context(ns, db);
        if matches!(session.probe_ttl(user_key, now), TtlGate::Pass) {
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

impl<D: Device> GcManager<D> {
  /// 两段式过期扫描：热区窗口优先（对标 Garnet 滑动窗口），冷区欠账用剩余删除预算。
  /// 返回 (物理删除键数, 扫描记录数)，对标 Garnet
  /// `ExpiredKeyDeletionScan` 的 `(numExpiredKeysFound, totalRecordsScanned)` 双口径；
  /// 扫描记录数为热区与冷区两段 `collect_expired` 的 scanned 求和。
  /// 候选收集走与 EXPDELSCAN 共享的 [`collect_expired`] 内核（C# 侧同为单内核：
  /// StoreExpiredKeyDeletionScan 双入口共用 ExpiredKeysBase），本入口独有热/冷两段
  /// 调度与容错删除语义（单键失败 warn 留痕，下轮双检幂等重试）。
  pub(super) async fn sweep_expired(
    &self,
    store: &Arc<WedbStore<D>>,
    cfg: &GcConfig,
  ) -> Result<(u64, u64)> {
    // 过期判定基准：.NET Ticks（与 TTL 记录值同域）
    let now = now_ticks();
    let cap = cfg.max_batch_deletes.max(1);
    let cold_cap = cfg.max_scan_records.max(1);
    let session = self.sweep_session.take(store)?;
    let mut picked = ExpiredKeySet::with_hasher(GxBuildHasher::default());
    let mut scanned = 0u64;
    let all_dbs = |_: u64, _: u64| true;

    // 段 1：热区窗口 [read_only, tail) —— 每轮无游标全量扫描（纯内存零 I/O），
    // 记录数不限（与 Garnet 全窗口扫描同一取舍，窗口由内存缓冲区约束），
    // 刚写入即过期的短 TTL 键至多一个扫描间隔即被物理清除
    let read_only = store.read_only_address();
    let tail = store.tail_address();
    if read_only < tail {
      let (n, ..) = collect_expired(
        &session,
        store,
        read_only..tail,
        now,
        ScanBudget {
          max_records: u64::MAX,
          max_picks: usize::MAX,
        },
        &mut picked,
        &all_dbs,
      )
      .await?;
      scanned += n;
    }

    // 段 2：冷区欠账 [cold_cursor, read_only) —— 游标增量推进，单轮记录预算有界
    // （磁盘 I/O 有界）；删除预算已被键级候选占满时本轮跳过（游标不动，下轮重扫，幂等）
    let mut cold_commit = None;
    if picked.len() < cap {
      let cold_from = self.cold_cursor.load(Relaxed).max(store.begin_address());
      if cold_from < read_only {
        let (n, next_addr, exhausted) = collect_expired(
          &session,
          store,
          cold_from..read_only,
          now,
          ScanBudget {
            max_records: cold_cap as u64,
            max_picks: cap.saturating_sub(picked.len()),
          },
          &mut picked,
          &all_dbs,
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
      session.set_virtual_context(*ns, *db);
      match session.check_expired(key).await {
        Ok(true) => deleted += 1,
        // 双检未过期：扫描与删除间隙内被用户续期/删除/紧缩，安全放行
        Ok(false) => {}
        Err(e) => warn!("内置 GC 过期删除失败，留待下一扫描周期重试: err={e}"),
      }
    }

    if deleted > 0 {
      info!(
        "内置 GC 过期扫描完成: 键候选={}, 物理删除键={deleted}",
        picked.len()
      );
    }
    // 删除完成才提交冷区游标；任务取消时重扫当前批，最新 TTL 双检保证幂等
    if let Some(addr) = cold_commit {
      self.cold_cursor.store(addr, Relaxed);
    }
    self.sweep_session.restore(session);
    Ok((deleted, scanned))
  }
}
