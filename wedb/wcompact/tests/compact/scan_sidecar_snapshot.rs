//! Scan 档紧缩阶段 3 快照化复查回归：宿主记录与 TTL 旁路记录（侧车）的伴生复查
//! 互扰双向锁测
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs:CompactScan
//! （C# 阶段 3 只读 iter1 定稿的 tempKv 快照、绝不回读主库现态判活，且 TTL 内嵌
//! 记录物理头与值一体随迁，结构上无「值拷回而旁路已失」形态；rust 把 TTL 拆成
//! 独立物理键并与宿主同列候选表，故阶段 3 复查必须快照化：以同一个新鲜 `now`
//! 对全体非死候选预跑复查、冻结判死位图，处置环只读冻结位图分派——
//! wcompact/src/compactor/run.rs:compact_scan 阶段 3 两子段）
//!
//! 病灶形态（处置环内边复查边处置）：前一处置把侧车槽位摘除后，后一宿主候选的
//! 复查读到的是**本轮自己造成的**删除态（侧车缺席即判活），过期值被回拷尾部永生，
//! 而其伴生分层树已被同轮清退销毁、还留下伪清理事件入账。锁测三断言：
//! ①逐键 GET 全部落空（值未永生）；②尾部无被清退候选的新帧（无永生帧）；
//! ③分层宿主清理事件入账数与本轮判死的分层宿主数逐键相等（无伪退册）。
//!
//! 互扰窗构造：宿主/侧车同批 `SETEX` 落紧缩区，到期时刻经「阶段 1 加宽钩」
//! （模拟生产长区间紧缩窗的定稿区 I/O 时长）确定性地落在阶段 1 裁决与阶段 3
//! 复查之间；侧车处置臂挂 `wkv/src/compact.rs:on_dropped` 同风格的单次暂停握手钩
//! 最大化「前一处置 → 后一复查」的互扰窗。多键批量使「侧车先于宿主」的哈希序
//! 近乎必然（单键反序角部另测）。

use std::{
  collections::HashSet,
  convert::Infallible,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
  },
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::{runtime::spawn, time::sleep};
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_MILLISECOND, time::now_ticks};
use wcompact::{CompactSession, CompactionFunctions, CompactionType, LogCompactor};

use super::support::{FixtureSession, FixtureStore, Watchdog, read_ttl_expiry, str_key, ttl_key};

/// 批量宿主/侧车对数：单对「侧车先于宿主」的哈希序约二分之一，批量后反序概率
/// 指数收敛，互扰序近乎必然
const PAIRS: usize = 32;
/// 阶段 1 加宽钩的定宽睡眠（毫秒）：把阶段 3 复查时点无条件推至易逝键到期之后，
/// 对位生产「长区间紧缩窗内到期的全体 TTL 键」触发窗
const PHASE1_WIDEN_MS: u64 = 150;
/// 易逝键到期时刻相对测试起点的偏移（毫秒）：远小于加宽窗、远大于紧缩入口开销
const EXPIRE_IN_MS: i64 = 30;
/// 存活对照键的到期偏移（秒）：跨整轮紧缩恒未到期
const CONTROL_ALIVE_SECS: i64 = 60;

/// 伴生互扰锁测的业务谓词（生产 wkv/src/compact.rs:WedbCompactionFunctions::is_deleted
/// 的 ttl:/str: 对偶形态在 fixture 域的同构复刻，外加两枚确定性窗钩）
struct SidecarInterferenceFunctions {
  /// 阶段 1 加宽钩（一次性）：首次复查谓词调用处定宽挂起，令阶段 3 复查的
  /// 新鲜 `now` 必落在易逝键到期之后（阶段 1 裁决用表入口时点，不受影响）
  widen_phase1: AtomicBool,
  /// 侧车处置暂停注入闩（一次性，`wkv/src/compact.rs` 的 ON_DROPPED_PAUSE_INJECT
  /// 同风格）：命中后侧车处置臂置位 paused 并等待 resume，最大化互扰时间窗
  pause_inject: AtomicBool,
  paused: AtomicBool,
  resume: AtomicBool,
  /// 分层宿主在册表（对位生产 range_index 树注册表：在册即宿主树在场）
  trees: Mutex<HashSet<Vec<u8>>>,
  /// 宿主树退册事件账（对位生产 on_dropped 的 RangeIndexDrop 先入账再注销：
  /// 每棵被销毁的宿主树恰记一条）
  drop_events: Mutex<Vec<Vec<u8>>>,
}

impl SidecarInterferenceFunctions {
  fn new() -> Self {
    Self {
      widen_phase1: AtomicBool::new(false),
      pause_inject: AtomicBool::new(false),
      paused: AtomicBool::new(false),
      resume: AtomicBool::new(false),
      trees: Mutex::new(HashSet::new()),
      drop_events: Mutex::new(Vec::new()),
    }
  }

  fn trees_snapshot(&self) -> HashSet<Vec<u8>> {
    self.trees.lock().unwrap_or_else(|e| e.into_inner()).clone()
  }

  fn drop_events_snapshot(&self) -> Vec<Vec<u8>> {
    self
      .drop_events
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .clone()
  }
}

impl CompactionFunctions<FixtureStore> for SidecarInterferenceFunctions {
  type Error = Infallible;

  async fn is_deleted(&self, session: &FixtureSession, key: &[u8], val: &[u8], now: i64) -> bool {
    if self.widen_phase1.swap(false, Ordering::AcqRel) {
      sleep(Duration::from_millis(PHASE1_WIDEN_MS)).await;
    }
    if let Some(user) = key.strip_prefix(b"ttl:") {
      // 生产 KeyTag::Ttl 臂同口径：侧车自身到期直接判死；未到期时宿主缺席判孤儿
      if <[u8; 8]>::try_from(val).is_ok_and(|be| i64::from_be_bytes(be) <= now) {
        return true;
      }
      let _guard = session.enter_epoch();
      return session.store.index.find_tag(&str_key(user)).is_none();
    }
    if let Some(user) = key.strip_prefix(b"str:") {
      // 生产 user-visible 臂同口径：现读伴生侧车裁决附带 TTL 是否已过期，
      // 侧车缺席即判存活（TTL 旁路缺失时的正确保守——本锁测的互扰输入面）
      return read_ttl_expiry(session, &ttl_key(user))
        .await
        .is_some_and(|exp| exp <= now);
    }
    false
  }

  async fn on_dropped(&self, session: &FixtureSession, key: &[u8]) -> Result<(), Self::Error> {
    let user = match key.strip_prefix(b"ttl:").or(key.strip_prefix(b"str:")) {
      Some(user) => user,
      None => return Ok(()),
    };
    // 侧车处置臂单次暂停握手（一次性）：对位 wkv on_dropped 入口的调试暂停钩
    if key.starts_with(b"ttl:") && self.pause_inject.swap(false, Ordering::AcqRel) {
      self.paused.store(true, Ordering::Release);
      while !self.resume.load(Ordering::Acquire) {
        sleep(Duration::from_millis(5)).await;
      }
      self.resume.store(false, Ordering::Release);
      self.paused.store(false, Ordering::Release);
    }
    // 并发安全垫（生产 on_dropped 的当下 TTL 重判对位）：侧车在场且未过期、
    // 或侧车缺席而宿主记录仍在场，均保守零副作用放行
    match read_ttl_expiry(session, &ttl_key(user)).await {
      Some(exp) => {
        if exp > now_ticks() {
          return Ok(());
        }
      }
      None => {
        let _guard = session.enter_epoch();
        if session.store.index.find_tag(&str_key(user)).is_some() {
          return Ok(());
        }
      }
    }
    // 宿主树整键退册入账（生产 get_tree 命中 → emit_event(RangeIndexDrop) →
    // delete_index 的三步在 fixture 域折叠为「在册表移除 + 事件账记名」）
    if self
      .trees
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .remove(user)
    {
      self
        .drop_events
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(user.to_vec());
    }
    Ok(())
  }
}

/// 批量写入易逝宿主/侧车对：宿主数据帧与 TTL 侧车帧同落紧缩区，到期时刻落在
/// 阶段 1 裁决（表入口时点）与阶段 3 复查（加宽后时点）之间
async fn seed_expiring_pairs(
  store: &FixtureStore,
  s: &FixtureSession,
  cf: &SidecarInterferenceFunctions,
  users: &[Vec<u8>],
  expire_at: i64,
) -> aok::Result<()> {
  for user in users {
    store.put(s, &str_key(user), b"payload").await?;
    store.put_ttl(s, user, expire_at).await?;
    cf.trees
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .insert(user.clone());
  }
  Ok(())
}

/// 存活对照对：跨整轮紧缩恒未到期，宿主与侧车必须双双判活回迁
async fn seed_control_pair(
  store: &FixtureStore,
  s: &FixtureSession,
  cf: &SidecarInterferenceFunctions,
  user: &[u8],
) -> aok::Result<()> {
  store.put(s, &str_key(user), b"ctl-payload").await?;
  store
    .put_ttl(
      s,
      user,
      now_ticks() + TICKS_PER_MILLISECOND * 1000 * CONTROL_ALIVE_SECS,
    )
    .await?;
  cf.trees
    .lock()
    .unwrap_or_else(|e| e.into_inner())
    .insert(user.to_vec());
  Ok(())
}

/// 三断言收口：①逐易逝键 GET 落空且宿主/侧车槽位双双摘净；②回迁数恰为对照对
/// （被清退候选尾部零新帧，无永生值）；③退册事件账与判死宿主逐键相等且绝不含
/// 仍存活记录（无伪退册入副本）
async fn assert_no_interference(
  store: &FixtureStore,
  s: &FixtureSession,
  cf: &SidecarInterferenceFunctions,
  users: &[Vec<u8>],
  control: &[u8],
  live_copied: usize,
  dead_dropped: usize,
) -> aok::Result<()> {
  for user in users {
    assert_eq!(
      store.get(s, &str_key(user)).await?,
      None,
      "易逝宿主被同轮伴生清退翻转判活即值永生: {user:?}"
    );
    assert!(
      store.index.lookup_candidates(&str_key(user)).is_empty(),
      "判死宿主的索引槽位必须摘净: {user:?}"
    );
    assert!(
      store.index.lookup_candidates(&ttl_key(user)).is_empty(),
      "判死侧车的索引槽位必须摘净: {user:?}"
    );
  }
  // 存活对照照常回迁可读（防「冻结位图误杀活键」的反向退化）
  assert_eq!(
    store.get(s, &str_key(control)).await?.as_deref(),
    Some(b"ctl-payload".as_slice()),
    "未到期对照宿主必须存活可读"
  );
  assert_eq!(
    live_copied, 2,
    "仅对照宿主与对照侧车允许回迁，易逝候选零迁移"
  );
  assert_eq!(
    dead_dropped,
    users.len() * 2,
    "全体易逝宿主与侧车必须判死清退"
  );

  let events = cf.drop_events_snapshot();
  assert_eq!(
    events.len(),
    users.len(),
    "退册事件条数必须与本轮判死的分层宿主数逐键相等（多一条即伪清退入副本账）"
  );
  let judged_dead: HashSet<&[u8]> = users.iter().map(|u| u.as_slice()).collect();
  for user in &events {
    assert!(
      judged_dead.contains(user.as_slice()) && !user.as_slice().eq(control),
      "仍存活记录绝不得带退册事件: {user:?}"
    );
    assert_eq!(
      store.get(s, &str_key(user)).await?,
      None,
      "带退册事件的宿主记录必须已判死清退（记录活而树毁即主从双端被毁）: {user:?}"
    );
  }
  let trees = cf.trees_snapshot();
  assert!(trees.contains(control), "对照宿主的分层树必须在册");
  for user in users {
    assert!(
      !trees.contains(user),
      "判死宿主的分层树必须已退册: {user:?}"
    );
  }
  Ok(())
}

/// 伴生次序锁测：批量易逝对 + 存活对照，侧车处置臂挂 ON_DROPPED_PAUSE_INJECT
/// 同风格暂停握手；阶段 3 快照化复查下宿主与侧车同轮同命运，三断言全部成立
#[compio::test]
async fn scan_phase3_sidecar_first_recheck_is_frozen() -> Void {
  let _watchdog = Watchdog::start(
    "scan_phase3_sidecar_first_recheck_is_frozen",
    Duration::from_secs(30),
  );
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("sidecar_snapshot.db"))?;
  let s = store.session()?;
  let cf = Arc::new(SidecarInterferenceFunctions::new());

  let users: Vec<Vec<u8>> = (0..PAIRS)
    .map(|i| format!("k{i:02}").into_bytes())
    .collect();
  seed_control_pair(&store, &s, &cf, b"ctl").await?;
  let expire_at = now_ticks() + TICKS_PER_MILLISECOND * EXPIRE_IN_MS;
  seed_expiring_pairs(&store, &s, &cf, &users, expire_at).await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let compactor = LogCompactor::new(Arc::clone(&store));
  cf.widen_phase1.store(true, Ordering::Release);
  cf.pause_inject.store(true, Ordering::Release);
  let cf_task = Arc::clone(&cf);
  let compact_task = spawn(async move {
    compactor
      .compact_with_filter(tail, CompactionType::Scan, &*cf_task)
      .await
  });

  // 暂停握手：等侧车处置臂抵达断点（互扰窗在场），随后放行
  let deadline = Instant::now() + Duration::from_secs(10);
  while !cf.paused.load(Ordering::Acquire) && Instant::now() < deadline {
    sleep(Duration::from_millis(2)).await;
  }
  assert!(
    cf.paused.load(Ordering::Acquire),
    "侧车处置暂停钩未抵达——本轮未构造出伴生清退互扰窗"
  );
  cf.resume.store(true, Ordering::Release);

  let stats = compact_task.await.expect("紧缩任务不得 panic")?;
  assert_no_interference(
    &store,
    &s,
    &cf,
    &users,
    b"ctl",
    stats.live_copied,
    stats.dead_dropped,
  )
  .await?;
  assert_eq!(stats.scanned_records, PAIRS * 2 + 2, "全体记录均须被扫描");
  assert_eq!(stats.retained, 0, "清退链健康时不得有保守保留");

  OK
}

/// 反序夹具回归（既有 scan_mode 册形态）：单对易逝宿主/侧车 + 存活对照、不挂
/// 暂停钩、内联直跑——宿主先行与侧车先行两种哈希序在快照化复查下同判同命，恒绿
#[compio::test]
async fn scan_phase3_single_pair_order_insensitive() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("sidecar_single.db"))?;
  let s = store.session()?;
  let cf = SidecarInterferenceFunctions::new();

  let users = vec![b"lonely".to_vec()];
  seed_control_pair(&store, &s, &cf, b"ctl").await?;
  let expire_at = now_ticks() + TICKS_PER_MILLISECOND * EXPIRE_IN_MS;
  seed_expiring_pairs(&store, &s, &cf, &users, expire_at).await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  cf.widen_phase1.store(true, Ordering::Release);
  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor
    .compact_with_filter(tail, CompactionType::Scan, &cf)
    .await?;

  assert_no_interference(
    &store,
    &s,
    &cf,
    &users,
    b"ctl",
    stats.live_copied,
    stats.dead_dropped,
  )
  .await?;
  assert_eq!(stats.retained, 0);

  OK
}

/// 快速通道与快照复查混合回归：阶段 1 已判死对（到期早于紧缩入口）走快速清理
/// 通道，阶段 1 判活、复查时点已到期对经冻结位图判死，存活对照照常回迁——
/// 复查语义保留、仅成账时点冻结，互不串扰
#[compio::test]
async fn scan_phase3_mixed_fast_lane_and_frozen_recheck() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("sidecar_mixed.db"))?;
  let s = store.session()?;
  let cf = SidecarInterferenceFunctions::new();

  seed_control_pair(&store, &s, &cf, b"ctl").await?;
  // 阶段 1 即判死对：到期早于紧缩入口（快速清理通道，其保守保留安全垫由在册
  // 侧车在场承接，与本票冻结位图正交）
  let stale = vec![b"stale".to_vec()];
  seed_expiring_pairs(&store, &s, &cf, &stale, now_ticks() - TICKS_PER_MILLISECOND).await?;
  // 复查窗内到期对：阶段 1 判活、阶段 3 冻结复查判死
  let fresh: Vec<Vec<u8>> = (0..4).map(|i| format!("f{i}").into_bytes()).collect();
  let expire_at = now_ticks() + TICKS_PER_MILLISECOND * EXPIRE_IN_MS;
  seed_expiring_pairs(&store, &s, &cf, &fresh, expire_at).await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  cf.widen_phase1.store(true, Ordering::Release);
  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor
    .compact_with_filter(tail, CompactionType::Scan, &cf)
    .await?;

  let mut all_dead = stale.clone();
  all_dead.extend(fresh.iter().cloned());
  assert_no_interference(
    &store,
    &s,
    &cf,
    &all_dead,
    b"ctl",
    stats.live_copied,
    stats.dead_dropped,
  )
  .await?;

  OK
}
