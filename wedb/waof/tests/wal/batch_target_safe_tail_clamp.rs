//! Group Commit 批次目标 → safe_tail 单点钳制回归测试
//!
//! 对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:
//! SafeTailAddress（104 行，"the largest address below which every byte has been
//! fully written"）与 CommitInternal→
//! libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:
//! ShiftReadOnlyToTail（1627-1639 行，1637 行 epoch.BumpCurrentEpoch 纪元动作）：
//! C# 刷盘动作在全部写入者退出当前纪元后才运行，落盘面恒 ≤ SafeTailAddress。
//!
//! 本票 [`waof::WalCommitStep::step`] 内 `goal = goal.min(safe_tail_address())`
//! 单点钳制须同时封堵三条越界来源：
//! ① Leader 自身登记 target（[`wbase::group_commit::GroupCommitPipeline::enter`]
//!    Lead 分支 push 到 waiters 首位，见姊妹票 7af12031）
//! ② Follower waiter target（`enter` Follow 分支挂起等待者）
//! ③ step 内 commit 元数据帧 `goal = goal.max(frame_end)` 抬升
//! 三路径各自一测试，另加一路真实多写入者并发持续不变式覆盖。
//!
//! 复现手段：外部向 `WalLogInner::inflight_slots` 手动写入低值哨兵（该字段 `pub`，
//! 语义即「某写入者已 CAS 预占但尚未释放的槽位下界」，与 pipeline.rs 的
//! acquire_inflight_slot / reserve_address 契约一致），构造 safe_tail 被在途写入
//! 压低、target 高于 safe_tail 的稳态；随后走真实 `WalCommitStep::step` 级联刷盘，
//! 用真设备与真环形缓冲验证钳制。禁止假 mock。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  thread::{sleep, spawn},
  time::Duration,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use waof::{RECORD_HEADER_LEN, WalLog};
use wdev::SegmentedDevice;

use super::support::{self, WalFixture, make_payload, reopen_single_file};

/// 让后台 Leader/Follower 走完若干轮 step + 让步（钳制下恒零推进，级联自旋
/// 等待 safe_tail 回升）；主线程用 sleep 让出真实调度时间窗，不干预机制
fn settle() {
  sleep(Duration::from_millis(80));
}

/// 在途槽位下界哨兵（模拟 pipeline.rs::reserve_address 契约：CAS 成功后、
/// 写入者尚未完成环形缓冲写入前，槽位值 = 本写入者尚未完成的首帧起点，
/// safe_tail_address 折回该下界）
fn hold_phantom_inflight(wal: &Arc<WalLog<SegmentedDevice>>, lower_bound: u64) {
  wal
    .inflight_slots
    .first()
    .expect("WalLog::new 已保底 inflight_slots ≥ 1")
    .store(lower_bound, Ordering::Release);
}

fn release_phantom_inflight(wal: &Arc<WalLog<SegmentedDevice>>) {
  wal
    .inflight_slots
    .first()
    .expect("同上")
    .store(u64::MAX, Ordering::Release);
}

/// 越界路径①：Leader 自身登记 target 越过 safe_tail 的钳制回归。
///
/// 场景：写入者 W 已 CAS 预占 [T5, T8)，环形缓冲中 [T5, T8) 已定稿但 phantom
/// 槽位把 safe_tail 折回 T5（真实在途写入者未释放槽位时的等价状态）。另一线程
/// 调 wait_for_commit(0)，target = tail_address = T8 > safe_tail = T5；升级
/// Leader 后 target 入 waiters 首位（姊妹票 7af12031），run_leader 首轮
/// batch_target = max(step.tail()=T5, waiter_max=T8) = T8。
///
/// 无钳制：step 直接把 goal=T8 交给 flush_and_sync_range，把 [T5, T8) 刷至设备
/// 并把 committed 推至 T8；恢复扫描或后续 flush 均从 T8 起，[T5, T8) 若曾半写
/// 即永远无法重刷，已确认数据丢失。
///
/// 有钳制：step 内 goal = min(T8, safe_tail=T5) = T5，flush 只到 T5；级联
/// 循环走 yield_now 让步承接差额；phantom 释放后 safe_tail 回升，下一轮补齐
/// 至 T8 + 帧尾。
#[test]
fn leader_target_overrun_clamped_by_safe_tail() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("clamp_leader_target.log", 64 * 1024)?;
    let wal = fixture.wal;

    // 段 A：真实写入环形缓冲 [begin, T5)，未刷盘
    let payload_a = make_payload(96, 0xA1);
    let addr_a = wal.enqueue(&payload_a)?;
    assert_eq!(addr_a, wal.begin_address());
    let t5 = wal.tail_address();

    // 模拟在途写入者 W：phantom 槽位锁 T5（真实 pipeline.rs reserve_address
    // 契约：CAS 成功后、写环形缓冲前，槽位值即预占下界 T5）
    hold_phantom_inflight(&wal, t5);

    // 段 B：主线程代为完成 W 后续要写入的 [T5, T8)（走真实 enqueue 路径
    // 用另一槽位，写完释放；phantom 槽位仍折 safe_tail 至 T5）
    let payload_b = make_payload(96, 0xB2);
    wal.enqueue(&payload_b)?;
    let t8 = wal.tail_address();
    assert!(t8 > t5, "B 段应推进尾地址");
    assert_eq!(
      wal.safe_tail_address(),
      t5,
      "phantom 槽位应把 safe_tail 折至在途下界 T5"
    );

    // 后台线程：wait_for_commit(0) → commit_to(target = tail = T8) → Leader
    // 级联；若 step 无钳制即把 goal 直接推给 flush_and_sync_range 越界刷 [T5, T8)
    let wal_bg = Arc::clone(&wal);
    let bg = spawn(move || {
      let rt = Runtime::new().expect("bg runtime");
      rt.block_on(async move { wal_bg.wait_for_commit(0).await })
        .expect("bg wait_for_commit 应成功")
    });

    // 观测窗口：给 Leader 若干轮 step（钳制下 committed 恒 ≤ T5，让步自旋）
    settle();
    let observed_committed = wal.committed_until_address.load(Ordering::Acquire);
    let observed_flushed = wal.flushed_until_address.load(Ordering::Acquire);
    let observed_safe = wal.safe_tail_address();
    // 核心不变式①：committed_until 恒不越过被在途压低的 safe_tail
    assert!(
      observed_committed <= observed_safe,
      "committed_until {observed_committed} 越过 safe_tail {observed_safe}：\
       step 内 goal.min(safe_tail) 单点钳制缺失，Leader target 越界刷盘"
    );
    assert!(
      observed_committed <= t5,
      "在途槽位持有期间 committed 不得越过 phantom 下界 T5 = {t5}，实测 {observed_committed}"
    );
    assert!(
      observed_flushed <= t5,
      "在途槽位持有期间 flushed 不得越过 phantom 下界 T5 = {t5}，实测 {observed_flushed}"
    );

    // 释放 phantom：safe_tail 回升至 tail，级联让步承接差额、下一轮 step 补齐
    release_phantom_inflight(&wal);

    let committed = bg.join().expect("bg thread");
    // 核心不变式③：在途帧完成后最终水位覆盖 T8（Leader target 达标才回 COMMIT_OK）
    assert!(
      committed >= t8,
      "在途帧完成后最终 committed {committed} 须覆盖 Leader target T8 = {t8}"
    );
    assert_eq!(wal.committed_until_address(), committed);
    // 收敛后不变式仍成立：committed ≤ 当下 safe_tail
    assert!(
      wal.committed_until_address() <= wal.safe_tail_address(),
      "静默后 committed_until {:?} 应 ≤ safe_tail {:?}",
      wal.committed_until_address(),
      wal.safe_tail_address()
    );

    // 落盘面一致性：重开恢复扫描完整读回 A、B 两条数据记录；
    // 若曾越界刷盘，B 帧 CRC 或半写字节会触发保守截尾丢记录
    drop(wal);
    let reopened =
      reopen_single_file(fixture.dir.path(), "clamp_leader_target.log", 64 * 1024).await?;
    let records = support::collect_data(reopened.scan_committed()).await?;
    assert_eq!(records.len(), 2, "钳制后两条数据记录均应可恢复");
    assert_eq!(records[0].payload, payload_a);
    assert_eq!(records[1].payload, payload_b);
    for rec in &records {
      rec
        .header
        .verify(&rec.payload)
        .expect("设备字节应与写入者定稿内容一致，CRC 必过");
    }

    info!("越界路径①（Leader 自身 target）→ safe_tail 单点钳制回归通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 越界路径②：Follower waiter target 越过 safe_tail 的钳制回归。
///
/// 与票面「wait_for_commit(0) 取 target=tail，含并发写入者已 CAS 预占但尚未
/// 写完环形缓冲的在途区间」原始描述直接对位——两名 bg 线程并发 commit_to(t8)，
/// 先手者升级 Leader、target 入首等待者；后手者登记为 Follower（waiters 中
/// tx=Some），target = T8 > safe_tail = T5。step 内 batch_target 由
/// `step.tail().max(max_waiter_target)` 合成，若无钳制即越界刷盘。
///
/// 断言：phantom 持有期间 committed/flushed 恒 ≤ T5（Follower target 与 Leader
/// target 同被钳制）；释放后两者均被批量唤醒，返回 committed ≥ T8。
#[test]
fn follower_target_overrun_clamped_by_safe_tail() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("clamp_follower_target.log", 64 * 1024)?;
    let wal = fixture.wal;

    // 基线：真实写入并 commit 一次，形成非零 committed 边界
    wal.enqueue(&make_payload(64, 0x5A))?;
    wal.commit().await?;

    let payload_a = make_payload(128, 0xA3);
    wal.enqueue(&payload_a)?;
    let t5 = wal.tail_address();
    hold_phantom_inflight(&wal, t5);

    let payload_b = make_payload(128, 0xB4);
    wal.enqueue(&payload_b)?;
    let t8 = wal.tail_address();
    assert_eq!(wal.safe_tail_address(), t5, "phantom 槽位应压低 safe_tail");

    // 先手 bg-A：升级 Leader，target = T8 入 waiters 首位（sister 票 7af12031）
    let wal_a = Arc::clone(&wal);
    let target_a = t8;
    let a = spawn(move || {
      let rt = Runtime::new().expect("bg-A runtime");
      rt.block_on(async move { wal_a.commit_to(target_a).await })
        .expect("bg-A commit_to")
    });

    // 让 A 完成 enter→Lead→acquire commit_lock→首轮 step 让步进入自旋；
    // 随后 B 才登记，此时 leading=true，B 走 Follow 分支挂 tx=Some
    sleep(Duration::from_millis(30));
    let wal_b = Arc::clone(&wal);
    let target_b = t8;
    let b = spawn(move || {
      let rt = Runtime::new().expect("bg-B runtime");
      rt.block_on(async move { wal_b.commit_to(target_b).await })
        .expect("bg-B commit_to")
    });

    settle();
    let committed_now = wal.committed_until_address.load(Ordering::Acquire);
    let flushed_now = wal.flushed_until_address.load(Ordering::Acquire);
    // 核心不变式：Leader target 与 Follower target 同被 step 内 single-point
    // 钳制封顶至 safe_tail；两者均未达标，committed 恒 ≤ T5
    assert!(
      committed_now <= t5,
      "phantom 持有期间 committed {committed_now} 不得越过 safe_tail T5 = {t5}"
    );
    assert!(
      flushed_now <= t5,
      "phantom 持有期间 flushed {flushed_now} 不得越过 safe_tail T5 = {t5}"
    );

    release_phantom_inflight(&wal);

    let res_a = a.join().expect("bg-A thread");
    let res_b = b.join().expect("bg-B thread");
    assert!(
      res_a >= t8,
      "Leader 唤醒位点 {res_a} 须覆盖 target T8 = {t8}"
    );
    assert!(
      res_b >= t8,
      "Follower 唤醒位点 {res_b} 须覆盖 target T8 = {t8}"
    );

    drop(wal);
    let reopened =
      reopen_single_file(fixture.dir.path(), "clamp_follower_target.log", 64 * 1024).await?;
    let records = support::collect_data(reopened.scan_committed()).await?;
    // 基线记录 + A + B
    assert_eq!(records.len(), 3);
    assert_eq!(records[1].payload, payload_a);
    assert_eq!(records[2].payload, payload_b);
    for rec in &records {
      rec.header.verify(&rec.payload).expect("CRC 必过");
    }

    info!("越界路径②（Follower waiter target）→ safe_tail 单点钳制回归通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 越界路径③：step 内 commit 元数据帧 `goal = goal.max(frame_end)` 抬升越界
/// 的钳制回归。
///
/// 与票外补充「Leader 自身路径同样越界——step 内 commit 元数据帧追加在含在途
/// 预占的 tail 之后，goal = goal.max(frame_end) 同样越过 safe_tail，无需
/// Follower waiter 在场」直接对位。
///
/// 场景：数据帧已入环形缓冲（tail = data_end），phantom 槽位把 safe_tail 折回
/// 数据帧起点 addr（帧起点，非帧末）；调用 commit_to(data_end)：step 内首行
/// `enqueue(commit_frame)` 让 tail 推进到 frame_end = data_end + 32；
/// goal = max(target=data_end, frame_end) = frame_end；无钳制即把 goal=frame_end
/// 直接刷至设备，committed 冲到 frame_end，越过 safe_tail=addr 显著距离。
///
/// 断言：phantom 持有期间 committed/flushed 恒 ≤ addr（帧尾抬升被钳制拦截）；
/// last_commit_frame 已推进到 frame_end 属帧游标事实（钳制前发生的既有步），
/// 刷盘面则必须被拦在游标之下；phantom 释放后帧与数据同批补齐。
#[test]
fn commit_frame_tail_growth_clamped_by_safe_tail() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("clamp_frame_growth.log", 64 * 1024)?;
    let wal = fixture.wal;

    let payload = make_payload(160, 0xC5);
    let addr = wal.enqueue(&payload)?;
    let data_end = addr + (RECORD_HEADER_LEN + payload.len()) as u64;
    assert_eq!(data_end, wal.tail_address());

    // phantom 槽位下界锁在数据帧起点 addr：safe_tail 折至 addr（数据帧本身
    // 也视为在途未定稿，令 step 内 enqueue 帧后 goal.max(frame_end) 显著
    // 越过 safe_tail）
    hold_phantom_inflight(&wal, addr);
    assert_eq!(wal.safe_tail_address(), addr);

    let wal_bg = Arc::clone(&wal);
    let pre_target = data_end;
    let bg = spawn(move || {
      let rt = Runtime::new().expect("bg runtime");
      rt.block_on(async move { wal_bg.commit_to(pre_target).await })
        .expect("bg commit_to")
    });

    settle();
    let observed_committed = wal.committed_until_address.load(Ordering::Acquire);
    let observed_flushed = wal.flushed_until_address.load(Ordering::Acquire);
    let frame_cursor = wal.last_commit_frame.load(Ordering::Acquire);
    // 帧游标先推进（step 内 commit 帧 enqueue 完成即 store，属钳制之前的
    // 既有事实）——本用例正是要断言刷盘面被拦在帧游标之下
    assert!(
      frame_cursor > addr,
      "step 内 commit 帧尾游标已推进是既有事实，实测 {frame_cursor} 应 > 数据帧起点 {addr}"
    );
    assert!(
      observed_committed <= addr,
      "commit 帧尾抬升不得穿透 safe_tail：committed {observed_committed} > safe_tail {addr}"
    );
    assert!(
      observed_flushed <= addr,
      "commit 帧尾抬升不得穿透 safe_tail：flushed {observed_flushed} > safe_tail {addr}"
    );

    release_phantom_inflight(&wal);

    let committed = bg.join().expect("bg thread");
    // 最终 committed 覆盖数据帧末，即 safe_tail 回升后帧与数据同批补齐落盘
    assert!(
      committed >= data_end,
      "safe_tail 回升后最终 committed {committed} 须覆盖数据帧末 {data_end}"
    );
    assert!(
      committed <= wal.safe_tail_address(),
      "静默后 committed 仍不越过 safe_tail"
    );

    drop(wal);
    let reopened =
      reopen_single_file(fixture.dir.path(), "clamp_frame_growth.log", 64 * 1024).await?;
    let records = support::collect_data(reopened.scan_committed()).await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, payload);
    records[0]
      .header
      .verify(&records[0].payload)
      .expect("设备字节 == 定稿内容，CRC 必过");

    info!("越界路径③（commit 帧 goal.max(frame_end) 抬升）→ safe_tail 钳制回归通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 真实并发写入下的持续不变式：多写入者自然竞争 + 反复 wait_for_commit(0)
/// 期间，钳制保证 committed_until 恒 ≤ 当下 safe_tail 快照。此路径不依赖
/// phantom 手动置位，验证真 enqueue_reserved 在途槽位协议 + 真 step 钳制
/// 端到端组合下的机制完整性；亦覆盖「committed_until 永不超过任一时刻的
/// safe_tail」这一票面核心断言的最强形态。
#[test]
fn concurrent_writers_committed_never_exceeds_safe_tail() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("clamp_concurrent.log", 256 * 1024)?;
    let wal = fixture.wal;

    let max_committed_seen = Arc::new(AtomicU64::new(0));

    const OPS_PER_WRITER: usize = 12;

    // 4 名写入者：每轮 enqueue 一条 128B 记录，随后 wait_for_commit(0)
    let mut writers = Vec::new();
    for w in 0..4u32 {
      let wal_clone = Arc::clone(&wal);
      let max_seen = Arc::clone(&max_committed_seen);
      writers.push(spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
          for i in 0..OPS_PER_WRITER as u32 {
            let payload = make_payload(128, (w * 80 + i) as u8);
            wal_clone.enqueue(&payload).unwrap();
            let committed = wal_clone.wait_for_commit(0).await.unwrap();
            max_seen.fetch_max(committed, Ordering::AcqRel);
            // 唤醒即采样：committed 应 ≤ 当下 safe_tail（钳制保证已刷区间
            // 恒落在被定稿的下界之内）
            let safe_now = wal_clone.safe_tail_address();
            assert!(
              committed <= safe_now,
              "并发下 committed {committed} 越过当下 safe_tail {safe_now}"
            );
          }
        });
      }));
    }

    // 观测者：全程持续采样 committed vs safe_tail 的相对关系
    let watcher = {
      let wal_clone = Arc::clone(&wal);
      let stop = Arc::new(AtomicBool::new(false));
      let stop_clone = Arc::clone(&stop);
      let handle = spawn(move || {
        while !stop_clone.load(Ordering::Relaxed) {
          let committed = wal_clone.committed_until_address.load(Ordering::Acquire);
          let safe = wal_clone.safe_tail_address();
          assert!(
            committed <= safe,
            "watcher 采样 committed {committed} > safe_tail {safe}"
          );
          sleep(Duration::from_millis(1));
        }
      });
      (handle, stop)
    };

    for h in writers {
      h.join().expect("writer thread");
    }
    watcher.1.store(true, Ordering::Relaxed);
    watcher.0.join().expect("watcher thread");

    // 全部收敛：committed 追平 tail
    let final_committed = wal.commit().await?;
    assert_eq!(final_committed, wal.tail_address());
    assert!(max_committed_seen.load(Ordering::Acquire) <= final_committed);

    // 落盘面完整：4×OPS_PER_WRITER 条数据记录 CRC 全过
    drop(wal);
    let reopened =
      reopen_single_file(fixture.dir.path(), "clamp_concurrent.log", 256 * 1024).await?;
    let records = support::collect_data(reopened.scan_committed()).await?;
    assert_eq!(records.len(), 4 * OPS_PER_WRITER);
    for rec in &records {
      rec.header.verify(&rec.payload).expect("CRC 必过");
    }

    info!("真实并发下 committed ≤ safe_tail 持续不变式回归通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
