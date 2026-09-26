//! commit 帧入队遇 safe_tail 钳制回落的持久帧尾见证闸回归测试
//!
//! 对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/
//! SerialCommitCallbackWorker:2799-2800：addr > commitInfo.UntilAddress 即
//! break——唤醒资格绑定本批 flush 完成范围覆盖帧尾，唤醒即见证是硬闸。
//!
//! 危害复现（票面 waof-commit-safetail-clamp-lost-witness）：commit 帧入队后
//! step 内单点钳制 `goal.min(safe_tail)` 把批次目标折回帧尾之下（在途写入者
//! phantom 槽位压低 safe_tail，对标 batch_target_safe_tail_clamp.rs 的槽位
//! 设定），若无见证闸，flush_and_sync_range 仍按回落目标推进 committed 并
//! 唤醒全部等待者——已确认区间 (磁盘持久帧尾, committed] 无任何磁盘提交见证，
//! 崩溃恢复经 erase_tail_after 物理擦除，客户端已收 OK 的写静默丢失。
//!
//! 断言口径：committed 恒 ≤ 磁盘最后持久 commit 帧尾（经 decode 设备字节
//! 观测，非内存游标）；帧悬空轮的等待者不得获确认（未达标不唤醒）；崩溃
//! 对拍以钳制回落轮后的真实盘面字节快照直接 recover，上一批已确认写不被
//! 擦除、无见证字节不转正。全程真实 enqueue/commit/recover，禁止假 mock。

use std::{
  fs::{copy, read_dir},
  path::Path,
  sync::{Arc, atomic::Ordering},
  thread::{JoinHandle, sleep, spawn},
  time::Duration,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use waof::{RECORD_HEADER_LEN, WalFrameHeader, WalLog};
use wdev::{Device, SegmentedDevice};

use super::support::{self, WalFixture, make_payload, reopen_single_file};

/// 让后台 Leader 走完钳制回落轮的首轮 step（帧入队 + 腾刷）并进入零推进
/// 让步自旋：有界轮询等腾刷窗口落地（主线程只读水位观测，不干预机制）；
/// 超界即放弃等待（后续断言按未完成态甄别）
fn settle(wal: &WalLog<SegmentedDevice>, flushed_target: u64) {
  for _ in 0..200 {
    if wal.flushed_until_address() >= flushed_target {
      return;
    }
    sleep(Duration::from_millis(1));
  }
}

/// 在途槽位下界哨兵（pipeline.rs reserve_address 契约：CAS 成功后、写入者
/// 尚未完成环形缓冲写入前，槽位值 = 预占下界，safe_tail_address 折回该下界）
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

/// 崩溃切面取盘：把设备真实落盘的全部分段文件复制为快照，返回复制字节总量
///
/// 为什么不能直接 copy WAL 基础名：`SegmentedDevice` 落盘名一律是
/// `<base>.<定长 Base32 段号>`（`single_file()` 只是段大小取 1GB 的分段设备，
/// 见 wdev segmented_device/handle.rs 的 `segment_path`），基础名本身在盘上不存在。
/// 目标名沿用设备自己写出的段号后缀、只替换 base 分量，故 `reopen_single_file`
/// 以同一 base_path 规则重建设备时仍能定位到快照段文件。
fn snapshot_device_segments(dir: &Path, base_name: &str, snap_name: &str) -> aok::Result<u64> {
  let mut bytes = 0u64;
  for entry in read_dir(dir)?.flatten() {
    let name = entry.file_name();
    let Some(name) = name.to_str() else {
      continue;
    };
    // 与设备自身恢复扫描同口径：base 分量之后必须是 '.' + 段号后缀才算本设备段文件
    let Some(rest) = name
      .strip_prefix(base_name)
      .and_then(|rest| rest.starts_with('.').then_some(rest))
    else {
      continue;
    };
    let len = entry.metadata()?.len();
    copy(entry.path(), dir.join(format!("{snap_name}{rest}")))?;
    bytes += len;
  }
  Ok(bytes)
}

/// decode 设备字节观测磁盘最后持久 commit 帧尾（None = 盘上无 commit 帧）
///
/// 从日志起始位点起逐帧解码记录链（读头 → 负载 → commit 魔数判据），只读
/// [begin, flushed) 的已落盘区间，帧尾 = 帧起始 + COMMIT_FRAME_TOTAL_LEN。
/// 这是「磁盘持久帧尾」的唯一直接观测位——内存游标 last_commit_frame 在钳制
/// 回落轮会先于刷盘推进，不构成见证事实
async fn disk_last_commit_frame_tail(
  wal: &WalLog<SegmentedDevice>,
  flushed: u64,
) -> aok::Result<Option<u64>> {
  let begin = wal.begin_address();
  let len = (flushed - begin) as usize;
  let bytes = wal.device().read_range(begin, len).await?;
  let bytes = bytes.as_slice();
  let mut cur = 0usize;
  let mut last = None;
  while cur + RECORD_HEADER_LEN <= bytes.len() {
    let Some(hdr) = WalFrameHeader::decode_opt(&bytes[cur..]) else {
      break;
    };
    if hdr.is_zero() {
      break;
    }
    let entry_len = hdr.payload_len();
    let payload_at = cur + RECORD_HEADER_LEN;
    if payload_at + entry_len > bytes.len() {
      break;
    }
    if waof::is_commit_frame(&bytes[payload_at..payload_at + entry_len]) {
      last = Some(begin + (payload_at + entry_len) as u64);
    }
    cur = payload_at + entry_len;
  }
  Ok(last)
}

/// 后台线程发起钳制回落轮提交：target = 批 2 数据尾（phantom 槽位压低
/// safe_tail，step 内帧 goal.max(frame_end) 抬升被钳回帧尾之下）
fn spawn_clamped_commit(wal: &Arc<WalLog<SegmentedDevice>>, target: u64) -> JoinHandle<u64> {
  let wal_bg = Arc::clone(wal);
  spawn(move || {
    let rt = Runtime::new().expect("bg runtime");
    rt.block_on(async move { wal_bg.commit_to(target).await })
      .expect("bg commit_to")
  })
}

/// 钳制回落轮确定性断言：committed 恒 ≤ 磁盘持久帧尾，等待者未达标不唤醒。
///
/// 场景：批 1 提交后磁盘帧尾 F0 为唯一见证；批 2 数据入环形缓冲后 phantom
/// 槽位把 safe_tail 折回批 2 数据尾 t2，后台 commit_to(t2) 令 step 内新帧
/// 入队至 [t2, t2+32) 再被钳回 goal = t2 < 帧尾——帧悬空。
///
/// 无见证闸：committed 被推至 t2 > F0，等待者（target = t2）即刻获 OK，
/// (F0, t2] 无见证确认写。有见证闸：committed 停在 F0（== 入口水位见证界），
/// 批 2 已定稿字节经 flush_window 腾刷只推 flushed（环满阻塞不放大），等待者
/// 保持挂起；phantom 释放后同帧随下一轮 step 补齐，见证到位才唤醒。
#[compio::test]
async fn clamped_round_committed_never_passes_disk_frame_tail() -> Void {
  let fixture = WalFixture::single_file("witness_gate_clamp.log", 64 * 1024)?;
  let wal = fixture.wal;

  // 批 1：真实提交，帧尾 F0 落盘即磁盘持久见证位
  wal.set_pending_cookie(1001);
  let payload1 = make_payload(96, 0x11);
  wal.enqueue(&payload1)?;
  let f0 = wal.commit().await?;
  let flushed1 = wal.flushed_until_address();
  assert_eq!(
    disk_last_commit_frame_tail(&wal, flushed1).await?,
    Some(f0),
    "校准观测位：批 1 提交后磁盘最后持久帧尾应恰为 committed {f0}"
  );

  // 批 2：数据入环形缓冲（未刷），phantom 槽位把 safe_tail 折回数据尾 t2
  wal.set_pending_cookie(1002);
  let payload2 = make_payload(96, 0x22);
  wal.enqueue(&payload2)?;
  let t2 = wal.tail_address();
  hold_phantom_inflight(&wal, t2);
  assert_eq!(wal.safe_tail_address(), t2);

  let bg = spawn_clamped_commit(&wal, t2);

  // 观测窗口：钳制回落轮走完（帧悬空 + 腾刷），等待者保持挂起
  settle(&wal, t2);
  let committed = wal.committed_until_address.load(Ordering::Acquire);
  let flushed = wal.flushed_until_address();
  let disk_tail = disk_last_commit_frame_tail(&wal, flushed).await?;
  // 核心不变式：committed 恒 ≤ 磁盘最后持久 commit 帧尾——推进后的已确认
  // 区间必有磁盘提交见证
  assert_eq!(
    committed, f0,
    "帧悬空轮 committed 必须折回见证界（上一持久帧尾 {f0}），实测 {committed}"
  );
  assert!(
    committed <= disk_tail.unwrap_or(0),
    "committed {committed} 越过磁盘持久帧尾 {disk_tail:?}：见证闸缺失，已确认写失去磁盘见证"
  );
  // 腾刷解耦：见证界之下已定稿字节照常刷出（只推 flushed），环满阻塞不放大
  assert_eq!(
    flushed, t2,
    "批 2 已定稿字节应经 flush_window 腾刷至数据尾 {t2}，实测 flushed {flushed}"
  );
  // 唤醒即见证：帧尾未落盘前等待者不得获确认
  assert!(
    !bg.is_finished(),
    "钳制回落轮等待者在其 target 获磁盘帧见证前不得被唤醒"
  );

  // phantom 释放：safe_tail 回升，级联让步承接差额、同帧补齐见证到位才唤醒
  release_phantom_inflight(&wal);
  let committed = bg.join().expect("bg thread");
  assert!(
    committed >= t2,
    "补齐后最终 committed {committed} 须覆盖批 2 数据尾 {t2}"
  );
  let flushed = wal.flushed_until_address();
  let disk_tail = disk_last_commit_frame_tail(&wal, flushed).await?;
  assert!(
    committed <= disk_tail.unwrap_or(0),
    "补齐后 committed {committed} 仍须 ≤ 磁盘持久帧尾 {disk_tail:?}（归纳闭合）"
  );

  // 落盘面完整：两条数据记录均可恢复
  drop(wal);
  let reopened =
    reopen_single_file(fixture.dir.path(), "witness_gate_clamp.log", 64 * 1024).await?;
  let records = support::collect_data(reopened.scan_committed()).await?;
  assert_eq!(records.len(), 2, "批 1 与批 2 数据均应可恢复");
  assert_eq!(records[0].payload, payload1);
  assert_eq!(records[1].payload, payload2);

  info!("钳制回落轮 committed ≤ 磁盘持久帧尾确定性断言通过");
  OK
}

/// 崩溃对拍：钳制回落轮后直接 recover，上一批已确认写不被 erase_tail_after
/// 擦除，无见证字节不转正。
///
/// 对拍切面 = 帧悬空轮走完后的真实盘面字节快照（批 1 数据 + 帧尾 F0 见证 +
/// 批 2 数据无见证腾刷落盘 + 悬空帧字节不在盘上）。以快照恢复即崩溃语义：
/// 恢复收敛至最后持久帧尾 F0，erase_tail_after 只擦无见证的批 2 字节；
/// 批 1 已确认写（客户端在 commit 返回时已获确认）必须完整存活。
#[compio::test]
async fn crash_after_clamped_round_keeps_confirmed_writes() -> Void {
  // 基础名与快照名同为 WAL 设备 base：两者落盘名各自带段号后缀，互不遮蔽
  let base_name = "witness_gate_crash.log";
  let fixture = WalFixture::single_file(base_name, 64 * 1024)?;
  let snap_name = "witness_gate_crash_snap.log";
  let wal = fixture.wal;

  // 批 1：提交确认（返回即客户端已获持久承诺）
  wal.set_pending_cookie(2001);
  let payload1 = make_payload(96, 0x33);
  wal.enqueue(&payload1)?;
  let f0 = wal.commit().await?;

  // 批 2：phantom 压低 safe_tail，后台提交进入钳制回落轮（帧悬空自旋）
  wal.set_pending_cookie(2002);
  let payload2 = make_payload(96, 0x44);
  wal.enqueue(&payload2)?;
  let t2 = wal.tail_address();
  hold_phantom_inflight(&wal, t2);
  let bg = spawn_clamped_commit(&wal, t2);
  settle(&wal, t2);
  assert_eq!(
    wal.committed_until_address.load(Ordering::Acquire),
    f0,
    "崩溃切面前置：帧悬空轮 committed 停在见证界 F0 = {f0}"
  );
  assert!(!bg.is_finished(), "崩溃切面前置：等待者未获见证不得被确认");

  // 为什么要求 flushed 已越过 F0：快照必须真的含「已刷盘但无见证」的字节，
  // 否则下方「无见证字节不转正」断言退化为盘上本无数据的空跑
  let flushed_at_snap = wal.flushed_until_address();
  assert!(
    flushed_at_snap > f0,
    "崩溃切面须落在批 2 无见证字节腾刷落盘之后（实测 flushed {flushed_at_snap} 未越过 F0 {f0}）"
  );

  // 崩溃切面：此刻盘面字节快照（bg 零推进自旋期无盘写，快照稳定）；
  // 环形缓冲内存随崩溃丢失，磁盘仅存已刷字节
  let snap_bytes = snapshot_device_segments(fixture.dir.path(), base_name, snap_name)?;
  assert!(
    snap_bytes >= f0,
    "快照盘面须覆盖崩溃前已见证字节（帧尾 {f0}），实测 {snap_bytes}：取盘口疑未对准真实段文件"
  );

  // 释放 phantom 让 bg 补齐退出，解锁 commit 互斥后对快照做崩溃恢复
  release_phantom_inflight(&wal);
  let committed = bg.join().expect("bg thread");
  assert!(
    committed >= t2,
    "主文件补齐 {committed} 须覆盖 {t2}（对照面）"
  );

  let reopened = reopen_single_file(fixture.dir.path(), snap_name, 64 * 1024).await?;
  // 恢复收敛位 = 最后持久帧尾 F0：悬空帧与腾刷的无见证字节均不转正
  assert_eq!(
    reopened.committed_until_address(),
    f0,
    "恢复提交上界应收敛至崩溃前最后持久帧尾 {f0}"
  );
  assert_eq!(
    reopened.recovered_cookie(),
    2001,
    "恢复 cookie 应为批 1 见证帧；批 2 帧悬空无见证不得转正"
  );
  // 上一批已确认写不被 erase_tail_after 擦除；批 2 无见证字节被物理擦除
  let records = support::collect_data(reopened.scan_committed()).await?;
  assert_eq!(records.len(), 1, "仅批 1 已确认写存活");
  assert_eq!(records[0].payload, payload1);
  records[0]
    .header
    .verify(&records[0].payload)
    .expect("存活记录 CRC 必过");

  info!("钳制回落轮后崩溃对拍：批 1 已确认写存活、无见证字节不转正");
  OK
}
