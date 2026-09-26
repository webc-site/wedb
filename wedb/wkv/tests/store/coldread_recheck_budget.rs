//! 磁盘冷读复检预算收窄收敛回归（票 wkv-cold-read-locktimeout-caller-no-convergence）
//!
//! 缺陷形态（修复前）：`StoreSession::read_from_disk` 复检重投谓词无链头单调下界
//! （`MemRecheck::Retry` 仅要求探针链头不在已检候选集），稳态内存驻留同桶碰撞版本
//! 的链头每轮重触 Retry、重投窗永不收窄，`MAX_DISK_RECHECKS=16` 预算必然推满上抛
//! 可重试 `Index(LockTimeout)`——向量桥冷读臂无重投环直转 -ERR（VADD 间歇报错主根
//! 因），且调用方朴素外层重投实测不收敛（32 轮预算尽且恶化），契约断链。
//!
//! 修复后判据（本文件锁定，对标 C# ContinuePending.cs:ContinuePendingRead :76-123
//! 重发收敛同形——让位序为触发链头单调越界）：
//! 1. 收窄收敛判据：链头稳定（无逐轮新增写入严格越过已见上界）的 miss 键冷读，
//!    重投触发即断供，数轮内落真实走尽 `Ok(None)` 合法 NOTFOUND 出口，绝不再假性
//!    推满预算上抛 LockTimeout（本用例即修复前后行为翻转的收敛锁）；
//! 2. 持续写窗活记录收敛判据：受控 Device 桩使冷读每个磁盘 I/O 窗口内驱动侧恰完成
//!    一次同桶碰撞推进写（链头逐轮严格前进，订正 2 注入形态的活记录滑入磁盘区），
//!    被注入键记录须在复检预算内收敛读得实值 `Some`——既不得吞为假 NOTFOUND（修复
//!    前 Ok(None) 形态），也不得退化为预算尽上抛（朴素重投恶化档形态）；
//! 3. 批量冷臂并案判据：`read_batch_with`（wnode 向量桥 `read_multi` 冷批经
//!    `read_batch_raw_with` 汇入的同一 `read_from_disk` 内核）在持续写窗内逐键收敛
//!    正确交付，零 LockTimeout 整体上抛；
//! 4. 真实走尽 NOTFOUND 出口与命中臂常态出口保持不动；
//! 5. 预算尽兜底上抛形态保留（零缺席证据纪律不变，见 read.rs 走尽臂尾注）：对抗
//!    档快写（每轮重投窗内链头均严格越过上界且手中活候选未读）16 轮推满仍回
//!    `Err(Index(LockTimeout))` 交调用方重投。单线程协作调度下确定性注入此档须
//!    逐轮写盘推进链头且轮轮重走全链（读放大指数化），本文件不另立注入用例；真实
//!    并发对抗面由 wnode 并发 VADD 溢写活性回归（vector_set_concurrent_vadd_disk_spill）
//!    承接演练。

use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicU64, Ordering},
};

use aok::{OK, Void};
use compio::runtime::spawn;
use itoa::Buffer;
use tempfile::tempdir;
use wbase::{
  align::DEFAULT_SECTOR_SIZE,
  future::yield_now,
  pool::{AlignedBuf, BufferPool},
};
use wdev::{Device, SegmentedDevice};
use windex::{HashBucketEntry, HashIndex};
use wkv::{StoreConfig, StoreSession, WedbStore};
use wval::KeyTag;

/// 搜索与 `anchor` 物理键同桶同 tag 的碰撞键（collision_chain 同款确定性搜索，
/// 返回 `want` 个互异碰撞键：写压推进键与冷读探针键分用，杜绝自写自读干扰）
fn find_collisions(
  session: &StoreSession<impl Device>,
  anchor: &[u8],
  mask: u64,
  want: usize,
) -> Vec<Vec<u8>> {
  let anchor_phys = session.session_string_key(anchor);
  let target = HashIndex::hash_key(anchor_phys.as_slice()) & mask;
  let prefix = session.session_prefix();
  let mut buf = Vec::with_capacity(64);
  buf.extend_from_slice(prefix.as_slice());
  buf.push(KeyTag::String as u8);
  let key_off = buf.len();
  buf.extend_from_slice(b"recheck-churn-");
  let base_len = buf.len();
  let mut itoa_buf = Buffer::new();
  let mut found = Vec::with_capacity(want);
  for i in 0u64.. {
    buf.truncate(base_len);
    buf.extend_from_slice(itoa_buf.format(i).as_bytes());
    if HashIndex::hash_key(&buf) & mask == target {
      found.push(buf[key_off..].to_vec());
      if found.len() == want {
        return found;
      }
    }
  }
  unreachable!("碰撞键搜索不可能耗尽");
}

/// 严格递增尺寸的碰撞键推进值：尺寸逐写必异，杜绝同长度原位覆写（原位覆写不产生
/// 新链头，收窄谓词 `first > min_address` 恒假即稳态收敛，无法演练持续写对抗窗）
fn churn_val(step: u64) -> Vec<u8> {
  vec![0x63u8; 8 + step as usize]
}

/// 冷读设备 I/O 门控桩（委托 SegmentedDevice，仅拦冷读纯设备路径
/// `read_range` 系原语 `read_raw`/`read_aligned`）：arm 开启时第 k 次读须等到
/// 驱动侧完成第 k 次推进（`steps >= k`）才放行，读侧因此恰好在每次磁盘 I/O
/// 窗口内停等一次外部并发写——复现票面「每个磁盘 I/O 窗口内槽头前移」形态，
/// 全程无 sleep、无自旋空转依赖调度让核
#[derive(Default)]
struct Gate {
  armed: AtomicBool,
  reads: AtomicU64,
  steps: AtomicU64,
}

struct GatedDevice {
  inner: SegmentedDevice,
  gate: Arc<Gate>,
}

impl Device for GatedDevice {
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  fn segment_size(&self) -> u64 {
    self.inner.segment_size()
  }

  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }

  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }

  fn end_segment(&self) -> Option<u32> {
    self.inner.end_segment()
  }

  fn capacity(&self) -> Option<u64> {
    self.inner.capacity()
  }

  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.pass_gate().await;
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.pass_gate().await;
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> wdev::Result<()> {
    self.inner.sync().await
  }

  fn get_file_size(&self, segment_id: u32) -> wdev::Result<u64> {
    self.inner.get_file_size(segment_id)
  }

  async fn remove_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.remove_segment(segment_id).await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}

impl GatedDevice {
  /// 门控放行：armed 时第 k 次冷读停等到驱动侧第 k 步推进完成
  async fn pass_gate(&self) {
    if !self.gate.armed.load(Ordering::Acquire) {
      return;
    }
    let k = self.gate.reads.fetch_add(1, Ordering::AcqRel) + 1;
    while self.gate.steps.load(Ordering::Acquire) < k {
      yield_now().await;
    }
  }
}

/// 停等读侧发出第 `want` 次门控冷读（或读侧已闭环）；返回读侧是否已提前闭环
async fn wait_read_entered(gate: &Gate, want: u64, finished: impl Fn() -> bool) -> bool {
  for _ in 0..2_000_000u64 {
    if gate.reads.load(Ordering::Acquire) >= want {
      return false;
    }
    if finished() {
      return true;
    }
    yield_now().await;
  }
  false
}

/// 撤压放行全部门控挂起：步数抬高至高位，正在 pass_gate 停等环内挂起的读侧即刻
/// 放行（仅翻 armed 位只挡新读，已入闸挂起者不复查该位会永停）；用于阶段收尾
fn release_gate(gate: &Gate) {
  gate.steps.store(u64::MAX >> 1, Ordering::Release);
  gate.armed.store(false, Ordering::Release);
}

fn config() -> aok::Result<StoreConfig> {
  // index_size=64：桶位 6 位 + tag 15 位，同桶同 tag 碰撞键期望 2^21 步内可搜得
  Ok(StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?)
}

/// 用例一（收窄收敛判据，修复前后行为翻转锁）：磁盘 miss 键 + 内存驻留同桶碰撞
/// 版本（复检探针链头恒为同一内存地址、不再前进）。修复前该稳态每轮重触 Retry，
/// 16 轮预算必推满上抛 LockTimeout（VADD 冷读臂间歇 -ERR 主形态）；修复后重投
/// 触发链头须严格越过已见上界（让位序界随轮升位），稳态链头即断供重投，数轮内
/// 落真实走尽 `Ok(None)` 出口，随后撤压/驱逐常态出口零变化
#[compio::test]
async fn test_stable_head_narrowing_converges_real_notfound() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("budget.db"))?);
  let store = Arc::new(WedbStore::open(config()?, device)?);
  let session = store.new_session()?;

  let mask = (store.index.load().mask as u64) | (0x7fffu64 << HashBucketEntry::HASH_TAG_SHIFT);
  let victim = b"budget-victim";
  let churn = find_collisions(&session, victim, mask, 1).remove(0);

  // 磁盘区基线：碰撞键 v1 落盘并全量驱逐（链上唯一磁盘候选，miss 键须走冷读）
  session.upsert(&churn, b"churn-v1").await?;
  store.flush_and_evict_all().await?;

  // 内存驻留碰撞版本：复检探针链头恒为该内存地址（不在已检磁盘候选集）且永不再
  // 前进——收窄语义下这正是「零增量信息」稳态：首轮 Retry 界升位至该链头，次轮
  // 复检链头与之持平未越过上界，断供重投落真实走尽出口
  session.upsert(&churn, b"churn-v2").await?;

  assert_eq!(
    session.read(victim).await?,
    None,
    "链头稳定的 miss 冷读须收窄收敛为真实 NOTFOUND，不得再假性推满预算上抛 LockTimeout"
  );
  // 命中臂常态回归：内存驻留碰撞版本直读命中（走尽臂收窄零影响）
  assert_eq!(
    session.read(&churn).await?.as_deref(),
    Some(b"churn-v2".as_slice()),
    "命中臂/常态热读路径零变化"
  );

  // 全量驱逐令链头落入磁盘候选：真实走尽出口与磁盘命中臂保持不动
  // （对标 ContinuePending.cs:37/:41 BeginAddress/minAddress 真实出口）
  store.flush_and_evict_all().await?;
  assert_eq!(
    session.read(victim).await?,
    None,
    "真实走尽（无未读新候选）NOTFOUND 出口不得改动"
  );
  assert_eq!(
    session.read(&churn).await?.as_deref(),
    Some(b"churn-v2".as_slice()),
    "磁盘链命中臂常态零变化"
  );
  OK
}

/// 用例二（持续写窗收敛 + 活记录不吞 + 批量冷臂并案）：受控 Device 桩把冷读设备
/// I/O 与驱动侧同桶碰撞推进写锁为逐窗交错（触发链头逐窗严格越过已见上界，重投
/// 持续不断供）。中途注入 victim 自身写入并全量驱逐令其滑入磁盘区：
/// 1. 单键冷臂：victim 活记录须在复检预算内收敛读得 Some（修复前形态 Ok(None) 假
///    NOTFOUND / 预算尽 Err(LockTimeout) 两态均判负）；
/// 2. 批量冷臂（read_batch_with → read_batch_raw_with → 同一 read_from_disk 内核，
///    wnode 向量桥 read_multi 冷批并案面）：持续写窗内 victim 收敛 Some、同桶 miss
///    键收敛 None，整批零 Err。
#[compio::test]
async fn test_final_window_live_record_not_fake_notfound() -> Void {
  let dir = tempdir()?;
  let gate = Arc::new(Gate::default());
  let device = Arc::new(GatedDevice {
    inner: SegmentedDevice::single_file(dir.path().join("gated.db"))?,
    gate: Arc::clone(&gate),
  });
  // index 1024 桶：记录规模（<4 键/桶）远不及分裂水位，排除扩容相位扰动；
  // 128 页环：冷读基线落盘后读侧仅锚定单一磁盘候选，环大小不影响轮次口径
  let cfg = StoreConfig::new(1024, DEFAULT_SECTOR_SIZE, 128, 0.5)?;
  let store = Arc::new(WedbStore::open(cfg, device)?);
  let session = store.new_session()?;

  let mask = (store.index.load().mask as u64) | (0x7fffu64 << HashBucketEntry::HASH_TAG_SHIFT);
  let victim = b"gated-victim";
  let collisions = find_collisions(&session, victim, mask, 2);
  let (churn, churn2) = (collisions[0].clone(), collisions[1].clone());
  let victim_val = b"victim-live-on-new-cand";

  // 磁盘区基线：碰撞键落盘（victim 冷读走尽臂每轮对全部同 tag 槽位候选各发一次
  // 纯设备读）+ 内存驻留链头（首轮重投触发地址）
  session.upsert(&churn, &churn_val(1)).await?;
  store.flush_and_evict_all().await?;
  session.upsert(&churn, &churn_val(2)).await?;

  // 第 8 个磁盘 I/O 窗口注入 victim 自身写入并全量驱逐令其滑入磁盘区（订正 2 形态
  // 的活候选）；此后驱动侧每放行一次冷读即追加一条更大尺寸碰撞记录（触发链头逐窗
  // 越过已见上界），复现「持续写窗内重投逐轮不断供直至注入记录收敛」的对抗节奏
  const INJECT_STEP: u64 = 8;
  const PHASE_A_CAP: u64 = 96;

  gate.armed.store(true, Ordering::Release);
  let read_sess = store.new_session()?;
  let victim_key = victim.to_vec();
  let reader = spawn(async move { read_sess.read(&victim_key).await });

  let mut step = 0u64;
  let mut injected = false;
  while step < PHASE_A_CAP {
    if wait_read_entered(&gate, step + 1, || reader.is_finished()).await {
      break;
    }
    step += 1;
    // 推进写必产生新链头：严格递增尺寸追加 + 全量驱逐令新链头落入磁盘区，
    // 下轮复检 new_cands 锚定最新磁盘再入点活候选在列，门控节奏逐轮延续
    session.upsert(&churn, &churn_val(2 + step)).await?;
    if step == INJECT_STEP {
      let a_v = session.upsert(victim, victim_val).await?;
      store.flush_and_evict_all().await?;
      assert!(
        store.hlog.is_on_disk(a_v),
        "注入窗口 victim 记录须已滑入磁盘区"
      );
      injected = true;
    } else {
      store.flush_and_evict_all().await?;
    }
    gate.steps.fetch_add(1, Ordering::AcqRel);
  }
  // 撤压收口：放行门控挂起后读侧必在有限轮内闭环（持续快写档的兜底上抛形态见本
  // 文件文档判据 5，本装配按设计以收敛 Some 出口闭环）
  release_gate(&gate);
  let mut closed = false;
  for _ in 0..2_000_000u64 {
    if reader.is_finished() {
      closed = true;
      break;
    }
    yield_now().await;
  }
  assert!(
    closed,
    "撤压后读侧未闭环（reads={}, steps={})",
    gate.reads.load(Ordering::Relaxed),
    step
  );
  let res = reader.await.expect("读侧任务须闭环");
  assert!(injected, "注入窗口未触达（读侧提前闭环，装配节奏偏离）");
  let got = res.expect("持续写窗冷读不得预算尽上抛（朴素重投恶化档形态判负）");
  assert_eq!(
    got.as_deref(),
    Some(victim_val.as_slice()),
    "持续写窗注入的活记录须收敛读得 Some——假 NOTFOUND（修复前 Ok(None) 形态）即判负"
  );

  // —— 批量冷臂并案验证（wnode 向量桥 read_multi 冷批 → read_batch_raw_with →
  // 同一 read_from_disk 内核）：重新武装门控，驱动侧按每 4 窗一次的节奏持续推进
  // 链头（限速档：保证复检轮次数受推进次数约束、预算内必收敛），断言整批 Ok 且
  // victim 交付实值、同桶从未写入 miss 键交付 None
  const PHASE_B_CAP: u64 = 40;
  // 阶段间门控计数器归零重启（A 阶段读侧已闭环await，无在途读者，归零无竞态）
  gate.reads.store(0, Ordering::Release);
  gate.steps.store(0, Ordering::Release);
  step = 0;
  gate.armed.store(true, Ordering::Release);
  let batch_sess = store.new_session()?;
  let victim_b = victim.to_vec();
  let churn2_b = churn2.clone();
  let batcher = spawn(async move {
    let mut out: Vec<(usize, Option<Vec<u8>>)> = Vec::new();
    let res = batch_sess
      .read_batch_with(&[victim_b.as_slice(), churn2_b.as_slice()], |i, v| {
        out.push((i, v.map(|b| b.to_vec())));
      })
      .await;
    (res, out)
  });
  while step < PHASE_B_CAP {
    if wait_read_entered(&gate, step + 1, || batcher.is_finished()).await {
      break;
    }
    step += 1;
    // 限速推进：触发链头越界节奏低于读窗轮次上界，撤压后复检即断越界，预算内
    // 落真实走尽/命中出口（逐窗满速推进即对抗档，判据 5 上抛兜底面）
    if step % 4 == 0 {
      session.upsert(&churn, &churn_val(2 + step)).await?;
    }
    store.flush_and_evict_all().await?;
    gate.steps.fetch_add(1, Ordering::AcqRel);
  }
  release_gate(&gate);
  let mut batch_closed = false;
  for _ in 0..2_000_000u64 {
    if batcher.is_finished() {
      batch_closed = true;
      break;
    }
    yield_now().await;
  }
  assert!(
    batch_closed,
    "撤压后批量冷臂未闭环（reads={}, steps={})",
    gate.reads.load(Ordering::Relaxed),
    step
  );
  let (res, out) = batcher.await.expect("批量读任务须闭环");
  res.expect("批量冷臂在持续写窗内须逐键收敛，禁止 LockTimeout 整体上抛（read_multi 并案面）");
  assert_eq!(
    out,
    vec![(0, Some(victim_val.to_vec())), (1, None),],
    "批量冷臂交付须逐键收敛正确（回调序恒等于键序）"
  );

  // 常态回归：撤压后单键复投收敛读活记录实值；同桶 miss 键与无关键均真实 NOTFOUND
  assert_eq!(
    session.read(victim).await?.as_deref(),
    Some(victim_val.as_slice())
  );
  assert_eq!(
    session.read(&churn2).await?,
    None,
    "同桶从未写入键须收敛真实 NOTFOUND"
  );
  assert_eq!(session.read(b"unrelated-miss-key").await?, None);
  OK
}
