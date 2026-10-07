//! 索引快照条目被并发紧缩越位致恢复静默悬挂：竞态回归（票 wcpr-index-snapshot-entry-below-compacted-begin）
//!
//! 缺陷：create_checkpoint_inner 的 begin 采样位于第 3 步 WAIT_FLUSH 入口（前票
//! wcpr-hlog-meta-late-sampling-race 上移点），仍晚于第 2 步索引快照固化。紧缩链无
//! 检查点相位互斥（wkv try_compact 无 Checkpoint 门、常驻回收轮 200ms 驱动；
//! compact_with_filter 收尾 shift_begin_address 全量推进 begin 并连带推 head）：
//! 「快照已固化条目 K→旧地址 a」之后、begin 采样之前紧缩移位越过 a，落盘 meta
//! 全程合法（begin <= tail、head <= tail，恢复校验族全过），而装载窗 [head, tail)
//! 不含 a、冷读判 addr < begin 失效、重放窗不含老地址、紧缩搬迁旁路 AOF——
//! 存活键静默永久丢失（无拒启、无回退、无告警）。
//!
//! 修复：begin 采样上移至检查点入口同步段（对标 C# HybridLogCheckpointSMTask.cs:38-39
//! PREPARE 段 beginAddress 先于一切相位捕获），构造性保证 cp_begin <= 快照扫描时点
//! begin <= 快照全部条目地址；head 维持第 3 步采样（晚采仅扩大装载窗、无害）；
//! cp_begin <= index_start 恒成立，附带消除 begin > index_start 拒启子形态。
//! 与彼票互补：彼修 meta 撕裂拒启（begin/head > tail），本修快照条目悬挂，缺一不可。
//!
//! 注入用真实原语（非假 mock）：受害键槽位以真实 RC 形态经快照解析端口会合
//! （fixture `arm_index_snapshot_shift_race`），后台紧缩线程携独立 Runtime 调真实
//! `shift_begin_address` 推 begin/head 越过已固化条目地址（物理删段受 delete_floor
//! 未发布检查点 0 全钳制、不删任何数据）；修复判据 = 落盘 meta.begin <= 快照固化
//! 地址且 recover_latest 后该键冷读可达。旧形态第 3 步采样必采到越界值、断言红。
//!
//! 两形态对应票面两子形态（同注入会合口、不同移位目标深度）：
//! 一、静默悬挂主形态（a < begin <= index_start）：移位仅越过受害条目地址、落盘
//! meta 全程合法，旧代码恢复校验族全过而键四路无补救静默永久丢失；
//! 二、具名拒启子形态（begin > index_start）：窗口内先真实追加把活跃 tail 推过
//! 检查点入口地板、再把 begin 移过该地板，旧代码第 3 步现采落盘 begin > index_start，
//! recover.rs 校验族抛 IndexStartLogicalAddress 低于 BeginAddress 具名拒启、
//! recover_latest 逐代回退叠加丢失；修复后 cp_begin 冻结于入口恒 <= index_start，
//! 校验族恒过、恢复成功且快照条目冷读可达。

use std::{
  sync::{Arc, atomic::Ordering},
  thread,
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{CheckpointType, CkptGateState, CprStore, create_checkpoint, recover_latest};
use wdev::SegmentedDevice;

use super::support::{MiniStore, PAGE_SIZE};

const K_VICTIM: &[u8] = b"inxsnap:victim";
const V_VICTIM: &[u8] = b"hang-proof";
const K_FILLER: &[u8] = b"inxsnap:filler";
const V_FILLER: &[u8] = b"filler-behind-victim";
const K2_VICTIM: &[u8] = b"inxsnap2:victim";
const V2_VICTIM: &[u8] = b"hang-proof-2";
const K2_FILLER: &[u8] = b"inxsnap2:filler";
const V2_FILLER: &[u8] = b"filler-behind-victim-2";
/// 拒启子形态窗口内真实追加的值体（4KB，与 fixture 驱逐注入同款节奏：循环 append
/// 把活跃 tail 推过检查点入口地板两整页，令移位目标严格大于该地板）
const GROW_WINDOW_VALUE: &[u8] = &[0x66; 4096];
/// 注入线程双向会合超时：超时即断言判负，绝不死等拖垮门禁
const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(10);

/// 快照条目地址被窗口内紧缩移位越过：落盘 begin 恒 <= 快照固化地址，恢复冷读可达
#[test]
fn snapshot_entry_below_concurrent_compacted_begin_stays_recoverable() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("index_snap_hang.db");
    let store = MiniStore::open(&db_path)?;
    let gate = CkptGateState::default();
    let p = store.session()?;

    let addr_victim = store.put(&p, K_VICTIM, V_VICTIM).await?;
    // 后继填充键：移位目标（= 其记录地址）严格大于受害条目地址且不超过入口 tail，
    // 落盘 meta 因此全程通过恢复校验族（本票「合法下静默悬挂」主形态）
    let shift_target = store.put(&p, K_FILLER, V_FILLER).await?;
    assert!(
      shift_target > addr_victim,
      "结构前提：移位目标必须越过受害条目地址: victim={addr_victim:#x} target={shift_target:#x}"
    );
    // 受害槽位置于真实 RC 形态：快照扫描经解析端口读入该条目（= 固化时点），
    // 亦为注入会合的触发口
    let rc_slot = store.install_read_cache_entry(&p, K_VICTIM)?;

    store.arm_index_snapshot_shift_race(rc_slot);
    // 后台紧缩线程：与快照扫描会合后以独立 Runtime 驱动真实移位（SegmentedDevice
    // 句柄每线程 TLS 独占自持，见 wdev handle.rs 契约；本线程不复用检查点 reactor，
    // 绝无 compio block_on 重入）——复现多核下紧缩于「快照固化之后、begin 采样
    // 之前」窗口内完成移位与 shift_begin_address 的真实交错
    let compact_store = Arc::clone(&store);
    let compact = thread::spawn(move || -> aok::Result<()> {
      let started = Instant::now();
      while !compact_store.snapshot_shift_parked.load(Ordering::Acquire) {
        assert!(
          started.elapsed() < RENDEZVOUS_TIMEOUT,
          "快照扫描未在超时内命中受害槽位会合：布防未生效"
        );
        thread::sleep(Duration::from_millis(1));
      }
      let compact_rt = Runtime::new()?;
      let hlog = Arc::clone(&compact_store.hlog);
      compact_rt.block_on(async move { hlog.shift_begin_address(shift_target).await })?;
      compact_store
        .snapshot_shift_done
        .store(true, Ordering::Release);
      Ok(())
    });

    let meta = create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
    compact
      .join()
      .map_err(|_| aok::anyhow!("紧缩注入线程 panic"))??;

    // 注入生效：运行时 begin/head 确已被窗口内紧缩推过受害条目地址
    // （旧第 3 步采样点读到的正是这个越界现值）
    assert!(
      store.begin_address() > addr_victim && store.head_address() > addr_victim,
      "紧缩移位必须真实越过快照固化地址: begin={:#x} head={:#x} victim={addr_victim:#x}",
      store.begin_address(),
      store.head_address(),
    );
    // 修复判据：落盘 begin 取入口 PREPARE 段快照，恒 <= 索引快照已固化条目地址，
    // 且取值冻结、拒采窗口内被紧缩推进的现值
    assert!(
      meta.hlog_meta.begin_address <= addr_victim,
      "落盘 begin 绝不允许越过快照固化条目地址: begin={:#x} victim={addr_victim:#x}",
      meta.hlog_meta.begin_address,
    );
    assert!(
      meta.hlog_meta.begin_address < store.begin_address(),
      "begin 须冻结于入口捕获，拒采窗口内紧缩推过的现值"
    );
    // 校验族恒过：begin <= head（不变式）、begin <= index_start（拒启子形态根除）、
    // head <= tail（彼票不变式维持）
    assert!(
      meta.hlog_meta.begin_address <= meta.hlog_meta.head_address
        && meta.hlog_meta.begin_address <= meta.index_start_logical_address
        && meta.hlog_meta.head_address <= meta.hlog_meta.tail_address,
      "落盘元数据地址序自洽: meta={:?}",
      meta.hlog_meta,
    );

    // 崩溃恢复：具名校验通过，受害键经快照条目冷读 [begin, head) 磁盘区可达，
    // 不再静默悬挂（旧形态此处 begin > victim，冷读 AddressOutOfRange、
    // 装载窗/重放窗/AOF 四路皆无补救）
    drop(p);
    drop(store);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let restored = recover_latest::<_, MiniStore>(&ckpt_dir, device).await?;
    let p2 = restored.session()?;
    assert_eq!(
      restored.get(&p2, K_VICTIM).await?.as_deref(),
      Some(V_VICTIM),
      "快照条目地址 >= 落盘 begin，恢复后冷读必须命中"
    );
    assert_eq!(
      restored.get(&p2, K_FILLER).await?.as_deref(),
      Some(V_FILLER),
      "移位目标及其后记录不受影响"
    );
    OK
  })
}

/// 子形态二（begin > index_start 具名拒启）：窗口内紧缩把 begin 推过检查点入口
/// 地板——旧第 3 步现采落盘 meta 触发 recover.rs「IndexStartLogicalAddress 低于
/// BeginAddress」具名拒启、recover_latest 逐代回退叠加丢失；修复后 cp_begin 冻结
/// 于入口 PREPARE 段、恒 <= index_start，校验族恒过、恢复成功且快照条目冷读可达
#[test]
fn snapshot_shift_past_index_start_recovered_without_refusal() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("index_snap_hang2.db");
    let store = MiniStore::open(&db_path)?;
    let gate = CkptGateState::default();
    let p = store.session()?;

    let addr_victim = store.put(&p, K2_VICTIM, V2_VICTIM).await?;
    store.put(&p, K2_FILLER, V2_FILLER).await?;
    let rc_slot = store.install_read_cache_entry(&p, K2_VICTIM)?;

    store.arm_index_snapshot_shift_race(rc_slot);
    // 后台紧缩线程（独立 Runtime，不复用检查点 reactor）：会合后先以真实 append
    // 把活跃 tail 推过检查点入口地板（自签发入口取闸内 tail，即本快照 index_start
    // 的下界），再以真实 shift_begin_address 顶至当前活跃 tail——落盘地板自此严格
    // 低于运行时 begin，正是旧现采形态的具名拒启输入（非假 mock）
    let compact_store = Arc::clone(&store);
    let compact = thread::spawn(move || -> aok::Result<()> {
      let started = Instant::now();
      while !compact_store.snapshot_shift_parked.load(Ordering::Acquire) {
        assert!(
          started.elapsed() < RENDEZVOUS_TIMEOUT,
          "快照扫描未在超时内命中受害槽位会合：布防未生效"
        );
        thread::sleep(Duration::from_millis(1));
      }
      let compact_rt = Runtime::new()?;
      compact_rt.block_on(async move {
        let writer = compact_store.session()?;
        let base = compact_store.hlog.tail_address();
        let target = base + 2 * PAGE_SIZE as u64;
        let mut i = 0u64;
        while compact_store.hlog.tail_address() < target {
          let key = format!("inxsnap2:grow:{i}");
          compact_store
            .append(&writer, key.as_bytes(), GROW_WINDOW_VALUE, 0, false)
            .await?;
          i += 1;
        }
        let tail = compact_store.hlog.tail_address();
        compact_store.hlog.shift_begin_address(tail).await?;
        compact_store
          .snapshot_shift_done
          .store(true, Ordering::Release);
        OK
      })
    });

    let meta = create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
    compact
      .join()
      .map_err(|_| aok::anyhow!("紧缩注入线程 panic"))??;

    // 注入生效：运行时 begin 已越过本快照 index_start 地板——旧第 3 步现采必读到
    // 该越界值，落盘 meta 触发 recover.rs 具名校验拒启
    assert!(
      store.begin_address() > meta.index_start_logical_address,
      "紧缩移位必须越过 index_start 地板: begin={:#x} index_start={:#x}",
      store.begin_address(),
      meta.index_start_logical_address,
    );
    // 修复判据：落盘 begin 冻结于入口捕获，恒 <= index_start（校验族恒过）且
    // <= 快照固化条目地址；地址序不变式 begin <= head <= tail 全自洽
    assert!(
      meta.hlog_meta.begin_address <= meta.index_start_logical_address,
      "落盘 begin 绝不允许越过 index_start 地板（旧形态此处具名拒启）: \
       begin={:#x} index_start={:#x}",
      meta.hlog_meta.begin_address,
      meta.index_start_logical_address,
    );
    assert!(
      meta.hlog_meta.begin_address <= addr_victim,
      "落盘 begin 恒 <= 快照固化条目地址: begin={:#x} victim={addr_victim:#x}",
      meta.hlog_meta.begin_address,
    );
    assert!(
      meta.hlog_meta.begin_address < store.begin_address(),
      "begin 须冻结于入口捕获，拒采窗口内紧缩推过的现值"
    );
    assert!(
      meta.hlog_meta.begin_address <= meta.hlog_meta.head_address
        && meta.hlog_meta.head_address <= meta.hlog_meta.tail_address,
      "落盘元数据地址序自洽: meta={:?}",
      meta.hlog_meta,
    );

    // 崩溃恢复：具名校验全过不再拒启，受害键经快照条目冷读可达
    drop(p);
    drop(store);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let restored = recover_latest::<_, MiniStore>(&ckpt_dir, device).await?;
    let p2 = restored.session()?;
    assert_eq!(
      restored.get(&p2, K2_VICTIM).await?.as_deref(),
      Some(V2_VICTIM),
      "快照条目地址 >= 落盘 begin，恢复后冷读必须命中"
    );
    assert_eq!(
      restored.get(&p2, K2_FILLER).await?.as_deref(),
      Some(V2_FILLER),
      "地板前基线键随一致性快照恢复可见"
    );
    OK
  })
}
