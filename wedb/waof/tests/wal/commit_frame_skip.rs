//! WAL commit 帧「跳帧降级」端到端测试（对标 libs/storage/Tsavorite/cs/src/core/
//! TsavoriteLog/TsavoriteLog.cs:TryEnqueueCommitRecord 的分配失败分支）
//!
//! C# 侧 commit 帧分配失败即 `allocator.TryAllocateRetryNow` 返回 false、
//! `TryEnqueueCommitRecord` 报假；本实现（wal/flush.rs:WalCommitStep::step 帧随批段）
//! 降级为跳帧：本批数据照常刷盘、帧游标不推进、待下轮 commit 补写、恢复侧回退上一
//! 有效帧收敛。
//!
//! 触发方式取确定性满窗口径：把环形窗口灌到上界（与 pipeline.rs:reserve_address
//! 的窗口判据同口径），随后与 commit 帧等长的入队当场 BufferFull，即 step 内的帧
//! 写入必然落进跳帧臂（非依赖时序、非并发运气）

use std::sync::atomic::Ordering;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use waof::{
  COMMIT_FRAME_TOTAL_LEN, Error, NO_COOKIE, RECORD_HEADER_LEN, WalLog, WalScanIterator,
  is_commit_frame,
};
use wbase::align::sector_bounds;
use wdev::{Device, SegmentedDevice};

use super::support::{self, WalFixture, make_pattern_payload, reopen_single_file};

/// 环形窗口容量（须为设备默认扇区 4096 的整数倍）：小到一次满窗灌入只有数条记录，
/// 大到满窗后仍能容纳下一轮数据批与补写帧
const BUF: usize = 16 * 1024;

/// 满窗灌入的单条记录负载上限（字节）
const BLOCK: usize = 2 * 1024;

type Wal = WalLog<SegmentedDevice>;

/// 环形窗口当前可预留的地址上界（对标 pipeline.rs:reserve_address 的
/// `required_end - sector_bounds(flushed, ..).0 > buffer_size` 窗口判据）
fn ring_upper(wal: &Wal) -> u64 {
  let sector = wal.device().sector_size() as u64;
  let flushed = wal.flushed_until_address();
  sector_bounds(flushed, flushed, sector).0 + wal.config().buffer_size as u64
}

/// 把环形窗口灌满至上界：commit 帧（含头 32B）在满窗上无处预留地址，
/// 提交步进即命中跳帧臂。末条按需缩窄填满余量。
///
/// 返回本批各条记录的负载长度（下标即 seq，供逐条回读校验）
fn fill_window(wal: &Wal) -> aok::Result<Vec<usize>> {
  let mut lens = Vec::new();
  loop {
    let room = ring_upper(wal) - wal.tail_address();
    if room <= RECORD_HEADER_LEN as u64 {
      break;
    }
    let len = (room - RECORD_HEADER_LEN as u64).min(BLOCK as u64) as usize;
    wal.enqueue(&make_pattern_payload(lens.len(), len))?;
    lens.push(len);
  }
  assert_eq!(
    wal.tail_address(),
    ring_upper(wal),
    "前置：窗口须灌满至上界"
  );

  // 前置实证：与 commit 帧等长的入队当场被拒，故 step 内的帧写入必走跳帧臂
  let frame_payload_len = (COMMIT_FRAME_TOTAL_LEN - RECORD_HEADER_LEN as u64) as usize;
  assert!(
    matches!(
      wal.enqueue(&make_pattern_payload(lens.len(), frame_payload_len)),
      Err(Error::BufferFull { .. })
    ),
    "前置：帧级预留须已被拒（否则本用例并未覆盖跳帧臂）"
  );
  Ok(lens)
}

/// 统计扫描区间内的 commit 元数据帧数（跳帧与补写的直接观测量）
async fn count_frames<D: Device>(iter: WalScanIterator<D>) -> aok::Result<usize> {
  let all = support::collect_iter(iter).await?;
  Ok(
    all
      .iter()
      .filter(|rec| is_commit_frame(&rec.payload))
      .count(),
  )
}

/// 不变量 1、2：跳帧后本批数据照常刷盘并推进提交位点，帧游标 last_commit_frame
/// 不推进（仍停在上一有效帧尾）
#[test]
fn test_commit_frame_skip_flushes_batch_and_holds_cursor() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("commit_frame_skip.log", BUF)?;
    let wal = fixture.wal;

    // 基线批：数据 + 随批帧，帧游标推进到帧尾（跳帧臂的参照点）
    wal.enqueue(b"baseline-record-before-skip")?;
    let base_committed = wal.commit().await?;
    assert_eq!(
      wal.last_commit_frame.load(Ordering::Acquire),
      base_committed
    );
    assert_eq!(count_frames(wal.scan(0, base_committed)).await?, 1);

    // 满窗批：帧无处预留，跳帧降级
    let lens = fill_window(&wal)?;
    let batch_tail = wal.tail_address();
    assert!(batch_tail > base_committed);

    let committed = wal.commit().await?;

    // 不变量 1：本批数据照常刷盘，提交位点即数据尾（跳帧不拖累刷盘）
    assert_eq!(committed, batch_tail);
    assert_eq!(wal.committed_until_address(), batch_tail);
    assert_eq!(wal.flushed_until_address(), batch_tail);

    // 不变量 2：帧游标不推进，仍停在基线帧尾
    assert_eq!(
      wal.last_commit_frame.load(Ordering::Acquire),
      base_committed
    );

    // 帧缺席实证：[0, tail) 内只有基线一帧，满窗批无帧
    assert_eq!(count_frames(wal.scan(0, batch_tail)).await?, 1);

    // 数据面实证：已提交区间回读出基线记录 + 满窗批全部记录，逐条 CRC 与字节一致
    let records = support::collect_data(wal.scan_committed()).await?;
    assert_eq!(records.len(), lens.len() + 1);
    assert_eq!(records[0].payload, b"baseline-record-before-skip");
    for (i, rec) in records.iter().skip(1).enumerate() {
      rec.header.verify(&rec.payload)?;
      assert_eq!(rec.payload, make_pattern_payload(i, lens[i]));
    }

    info!("commit 帧跳帧降级：本批照常刷盘与帧游标不推进测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 不变量 3：跳帧只降一级，下一轮 commit 在腾出的窗口上补写帧，帧游标随后可达
/// 提交边界覆盖上一轮（跳帧批）序号
#[test]
fn test_commit_frame_skip_next_round_backfills_frame() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("commit_frame_backfill.log", BUF)?;
    let wal = fixture.wal;

    // 跳帧批
    let lens = fill_window(&wal)?;
    let skip_tail = wal.tail_address();
    assert_eq!(wal.commit().await?, skip_tail);
    assert_eq!(count_frames(wal.scan(0, skip_tail)).await?, 0);
    assert_eq!(wal.last_commit_frame.load(Ordering::Acquire), 0);

    // 刷盘腾窗后的下一轮：新批数据 + 补写帧
    wal.set_pending_cookie(11);
    wal.enqueue(&make_pattern_payload(0, 100))?;
    let data_end = wal.tail_address();
    let committed = wal.commit().await?;

    // 补写帧落在本轮数据尾之后、覆盖至帧自身末尾，可达提交边界即帧游标
    assert_eq!(committed, data_end + COMMIT_FRAME_TOTAL_LEN);
    assert_eq!(wal.tail_address(), committed);
    assert_eq!(wal.last_commit_frame.load(Ordering::Acquire), committed);

    // 游标随后覆盖跳帧批序号：提交上界越过上一轮数据尾，区间内恰有一帧（补写的）
    assert!(committed > skip_tail);
    assert_eq!(count_frames(wal.scan(skip_tail, committed)).await?, 1);

    // 回放：跳帧批与本轮批全部数据记录均已提交
    let records = support::collect_data(wal.scan_committed()).await?;
    assert_eq!(records.len(), lens.len() + 1);
    for (i, rec) in records.iter().take(lens.len()).enumerate() {
      assert_eq!(rec.payload, make_pattern_payload(i, lens[i]));
    }
    assert_eq!(records[lens.len()].payload, make_pattern_payload(0, 100));

    info!("commit 帧跳帧降级：下一轮 commit 补写帧与游标覆盖测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 不变量 4：崩溃恢复侧缺帧回退上一有效帧并收敛——跳帧批虽已落盘不计入已提交，
/// 回放结果与已刷盘数据一致、无幻影提交；补写帧后再恢复即收敛至全量
#[test]
fn test_commit_frame_skip_recovery_falls_back_to_previous_frame() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("commit_frame_skip_recover.log", BUF)?;
    let dir = fixture.dir;
    let wal = fixture.wal;
    let file_name = "commit_frame_skip_recover.log";

    // 基线批（含帧，cookie = 7）
    wal.enqueue(b"pre-skip-committed")?;
    wal.set_pending_cookie(7);
    let prev_frame_end = wal.commit().await?;

    // 满窗批：跳帧刷盘后进程终止（该批落在「已落盘、无帧覆盖」区间）
    let lens = fill_window(&wal)?;
    let skip_tail = wal.tail_address();
    assert_eq!(wal.commit().await?, skip_tail);
    drop(wal);

    // 恢复：数据位点取扫描终点，提交上界回退到上一有效帧尾
    let wal = reopen_single_file(dir.path(), file_name, BUF).await?;
    assert_eq!(wal.tail_address(), skip_tail);
    assert_eq!(wal.flushed_until_address(), skip_tail);
    assert_eq!(wal.committed_until_address(), prev_frame_end);
    assert_eq!(
      wal.last_commit_frame.load(Ordering::Acquire),
      prev_frame_end
    );
    assert_eq!(wal.recovered_cookie(), 7);

    // 无幻影提交：已提交区间只回放基线批，跳帧批不被动转正
    let replayed = support::collect_data(wal.scan_committed()).await?;
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].payload, b"pre-skip-committed");

    // 回放结果与已刷盘数据一致：全量扫描读回基线 + 跳帧批全部记录，逐条字节精确
    let flushed_records = support::collect_data(wal.scan(0, skip_tail)).await?;
    assert_eq!(flushed_records.len(), lens.len() + 1);
    for (i, rec) in flushed_records.iter().skip(1).enumerate() {
      assert_eq!(rec.payload, make_pattern_payload(i, lens[i]));
    }

    // 恢复后补写帧：新一轮 commit 把跳帧批序号一并覆盖
    wal.set_pending_cookie(9);
    wal.enqueue(&make_pattern_payload(0, 64))?;
    let final_committed = wal.commit().await?;
    assert_eq!(final_committed, wal.tail_address());
    assert_eq!(
      wal.last_commit_frame.load(Ordering::Acquire),
      final_committed
    );
    assert_eq!(
      count_frames(wal.scan(prev_frame_end, final_committed)).await?,
      1
    );
    assert_eq!(
      support::collect_data(wal.scan_committed()).await?.len(),
      lens.len() + 2
    );
    drop(wal);

    // 二次恢复：补写帧收敛至全量，跳帧批转正、cookie 前滚
    let wal = reopen_single_file(dir.path(), file_name, BUF).await?;
    assert_eq!(wal.committed_until_address(), final_committed);
    assert_eq!(wal.recovered_cookie(), 9);
    let records = support::collect_data(wal.scan_committed()).await?;
    assert_eq!(records.len(), lens.len() + 2);
    assert_eq!(records[0].payload, b"pre-skip-committed");
    for (i, rec) in records.iter().skip(1).take(lens.len()).enumerate() {
      rec.header.verify(&rec.payload)?;
      assert_eq!(rec.payload, make_pattern_payload(i, lens[i]));
    }
    assert_eq!(records[lens.len() + 1].payload, make_pattern_payload(0, 64));

    info!("commit 帧跳帧降级：恢复侧回退上一有效帧与补写收敛测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 不变量 4 的无帧可退角：首次 commit 即跳帧后崩溃，设备上不存在任何帧，
/// 恢复按「最后一条完整记录即已提交」的兼容语义收敛——该批已由 commit 确认，
/// 回放等于已刷盘数据，既不丢也不虚高（无幻影提交）
#[test]
fn test_commit_frame_skip_first_round_without_frame_recovers_flushed_tail() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("commit_frame_skip_no_frame.log", BUF)?;
    let dir = fixture.dir;
    let wal = fixture.wal;

    let lens = fill_window(&wal)?;
    let skip_tail = wal.tail_address();
    assert_eq!(wal.commit().await?, skip_tail);
    assert_eq!(wal.last_commit_frame.load(Ordering::Acquire), 0);
    drop(wal);

    let file_name = "commit_frame_skip_no_frame.log";
    let wal = reopen_single_file(dir.path(), file_name, BUF).await?;
    assert_eq!(wal.tail_address(), skip_tail);
    assert_eq!(wal.committed_until_address(), skip_tail);
    assert_eq!(wal.last_commit_frame.load(Ordering::Acquire), skip_tail);
    assert_eq!(wal.recovered_cookie(), NO_COOKIE);

    let records = support::collect_data(wal.scan_committed()).await?;
    assert_eq!(records.len(), lens.len());
    for (i, rec) in records.iter().enumerate() {
      rec.header.verify(&rec.payload)?;
      assert_eq!(rec.payload, make_pattern_payload(i, lens[i]));
    }

    info!("commit 帧跳帧降级：无帧可退时按已刷盘尾部收敛测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
