//! WAL commit 帧「腾窗自旋重试」端到端测试（对标 libs/storage/Tsavorite/cs/src/
//! core/TsavoriteLog/TsavoriteLog.cs:CommitInternal 的
//! `while (!TryEnqueueCommitRecord(ref info)) Thread.Yield()` 重试契约）
//!
//! C# fastCommitMode 下提交记录是不可跳过的前置条件：分配失败只让核重试，绝不
//! 跳过；commit 的完成语义恒等于「commit record 已随批刷盘」，已确认写必有磁盘
//! 提交见证，重启恢复绝不丢弃。本实现（wal/flush.rs:WalCommitStep::step 帧随批
//! 段）同契约：帧入队遇环形满先腾窗刷新（只推进 flushed 水位，committed 恒 ≤
//! 持久帧尾不变式不因腾窗破缺），再让核重试入队，自旋有界收敛，界尽以原错误
//! 上抛——不存在跳帧降级路径。
//!
//! 触发方式取确定性满窗口径：把环形窗口灌到上界（与 pipeline.rs:reserve_address
//! 的窗口判据同口径），随后与 commit 帧等长的入队当场 BufferFull，即 step 内的
//! 帧写入必然落进腾窗自旋臂（非依赖时序、非并发运气）

use std::{
  fs::read_dir,
  sync::{Arc, atomic::Ordering},
  thread::{spawn, yield_now},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use waof::{
  COMMIT_FRAME_TOTAL_LEN, Error, RECORD_HEADER_LEN, WalLog, WalScanIterator, is_commit_frame,
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
/// 提交步进的帧写入必然落进腾窗自旋臂。末条按需缩窄填满余量。
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

  // 前置实证：与 commit 帧等长的入队当场被拒，故 step 内的帧写入必走腾窗自旋臂
  let frame_payload_len = (COMMIT_FRAME_TOTAL_LEN - RECORD_HEADER_LEN as u64) as usize;
  assert!(
    matches!(
      wal.enqueue(&make_pattern_payload(lens.len(), frame_payload_len)),
      Err(Error::BufferFull { .. })
    ),
    "前置：帧级预留须已被拒（否则本用例并未覆盖腾窗自旋臂）"
  );
  Ok(lens)
}

/// 统计扫描区间内的 commit 元数据帧数（腾窗补帧的直接观测量）
async fn count_frames<D: Device>(iter: WalScanIterator<D>) -> aok::Result<usize> {
  let all = support::collect_iter(iter).await?;
  Ok(
    all
      .iter()
      .filter(|rec| is_commit_frame(&rec.payload))
      .count(),
  )
}

/// 不变量 1（腾窗补帧）：满窗提交先腾窗刷出本批数据、再补写 commit 帧并随批
/// 刷盘确认——commit 返回位点 == 帧游标 == 批数据尾 + 帧全长，返回位点区间内
/// 帧计数覆盖；已确认数据全部落在提交区间（对标 C# 重试契约：提交记录不可跳过，
/// 已确认写必有磁盘提交见证）
#[compio::test]
async fn test_commit_frame_full_window_frees_and_appends_frame() -> Void {
  let fixture = WalFixture::single_file("commit_frame_tengchuang.log", BUF)?;
  let wal = fixture.wal;

  // 基线批：数据 + 随批帧，帧游标推进到帧尾（腾窗臂的参照点）
  wal.enqueue(b"baseline-record-before-full")?;
  let base_committed = wal.commit().await?;
  assert_eq!(
    wal.last_commit_frame.load(Ordering::Acquire),
    base_committed
  );
  assert_eq!(count_frames(wal.scan(0, base_committed)).await?, 1);

  // 满窗批：帧无处预留，提交步进腾窗补帧
  let lens = fill_window(&wal)?;
  let batch_tail = wal.tail_address();
  assert!(batch_tail > base_committed);

  let committed = wal.commit().await?;

  // 不变量 1：腾窗刷出本批数据 + 补写帧随批刷盘，提交位点即帧尾（批数据尾
  // + 帧全长），tail/flushed/committed 三水位与帧游标四点重合
  assert_eq!(committed, batch_tail + COMMIT_FRAME_TOTAL_LEN);
  assert_eq!(wal.tail_address(), committed);
  assert_eq!(wal.flushed_until_address(), committed);
  assert_eq!(wal.committed_until_address(), committed);
  assert_eq!(
    wal.last_commit_frame.load(Ordering::Acquire),
    committed,
    "commit 返回位点不得越过帧游标（提交见证恒在）"
  );

  // 不变量 2：返回位点区间内帧计数覆盖——全区间恰两帧（基线帧 + 腾窗补写帧），
  // 补写帧恰落满窗批数据尾之后
  assert_eq!(count_frames(wal.scan(0, committed)).await?, 2);
  assert_eq!(count_frames(wal.scan(batch_tail, committed)).await?, 1);

  // 数据面实证：已提交区间回读出基线记录 + 满窗批全部记录，逐条 CRC 与字节一致
  let records = support::collect_data(wal.scan_committed()).await?;
  assert_eq!(records.len(), lens.len() + 1);
  assert_eq!(records[0].payload, b"baseline-record-before-full");
  for (i, rec) in records.iter().skip(1).enumerate() {
    rec.header.verify(&rec.payload)?;
    assert_eq!(rec.payload, make_pattern_payload(i, lens[i]));
  }

  info!("commit 帧腾窗补帧：满窗提交先腾窗再补帧确认测试通过");
  OK
}

/// 不变量 2（无重复帧）：满窗腾窗补帧后的空转 commit 不再写帧——帧游标防级联轮
/// 重复写帧，空转提交返回位点不回退、帧计数不动（[`WalLog::wait_for_commit`]
/// 高速栅栏同语义）
#[compio::test]
async fn test_commit_frame_full_window_idle_commit_no_duplicate_frame() -> Void {
  let fixture = WalFixture::single_file("commit_frame_idle.log", BUF)?;
  let wal = fixture.wal;

  let lens = fill_window(&wal)?;
  let committed = wal.commit().await?;
  assert_eq!(committed, wal.tail_address());
  assert_eq!(count_frames(wal.scan(0, committed)).await?, 1);

  // 空转提交：无新数据、无元数据变更，返回位点不回退、不追加帧
  let idle = wal.commit().await?;
  assert_eq!(idle, committed);
  assert_eq!(wal.last_commit_frame.load(Ordering::Acquire), committed);
  assert_eq!(count_frames(wal.scan(0, committed)).await?, 1);

  // 高速提交栅栏：目标位点已被覆盖即 0 I/O 返回
  let barrier = wal.wait_for_commit(committed).await?;
  assert_eq!(barrier, committed);
  assert_eq!(count_frames(wal.scan(0, committed)).await?, 1);

  // 数据面闭环：满窗批全部记录仍在提交区间
  assert_eq!(
    support::collect_data(wal.scan_committed()).await?.len(),
    lens.len()
  );

  info!("commit 帧腾窗补帧：空转提交无重复帧与栅栏直返测试通过");
  OK
}

/// 不变量 3（恢复不回退）：满窗 commit 确认后立即崩溃重启，恢复侧以盘上 commit
/// 帧收敛提交上界——tail/flushed/committed 一律不回退（已确认写零丢失，对位 C#
/// RecoverAsync 收敛至最后 commit record 的 UntilAddress；跳帧降级时代的「回退
/// 上一帧物理擦除已确认批」语义已随腾窗补帧废除），帧 cookie 回填证明提交见证
/// 确已落盘；续写二次恢复幂等
#[compio::test]
async fn test_commit_frame_full_window_reopen_no_regression() -> Void {
  let fixture = WalFixture::single_file("commit_frame_reopen.log", BUF)?;
  let dir = fixture.dir;
  let wal = fixture.wal;
  let file_name = "commit_frame_reopen.log";

  // 满窗批带 cookie 提交：腾窗补帧后确认
  let lens = fill_window(&wal)?;
  wal.set_pending_cookie(7);
  let committed = wal.commit().await?;
  assert_eq!(committed, wal.tail_address());
  drop(wal);

  // 恢复：三水位与帧游标统一收敛至帧尾，不回退；cookie 自盘上帧回填
  let wal = reopen_single_file(dir.path(), file_name, BUF).await?;
  assert_eq!(
    wal.tail_address(),
    committed,
    "已确认批不得回退（腾窗补帧后帧恒在盘）"
  );
  assert_eq!(wal.flushed_until_address(), committed);
  assert_eq!(wal.committed_until_address(), committed);
  assert_eq!(wal.last_commit_frame.load(Ordering::Acquire), committed);
  assert_eq!(wal.recovered_cookie(), 7, "提交见证帧须自盘面回填");
  assert_eq!(wal.recover_truncation(), None);

  // 已确认记录全部可回放，逐条 CRC 与字节一致
  let replayed = support::collect_data(wal.scan_committed()).await?;
  assert_eq!(replayed.len(), lens.len());
  for (i, rec) in replayed.iter().enumerate() {
    rec.header.verify(&rec.payload)?;
    assert_eq!(rec.payload, make_pattern_payload(i, lens[i]));
  }

  // 恢复后接续：新批自提交上界无缝接续，帧随批确认
  wal.set_pending_cookie(9);
  let append_addr = wal.enqueue(&make_pattern_payload(0, 64))?;
  assert_eq!(append_addr, committed, "新 enqueue 须自帧尾接续");
  let final_committed = wal.commit().await?;
  assert_eq!(final_committed, wal.tail_address());
  assert_eq!(
    wal.last_commit_frame.load(Ordering::Acquire),
    final_committed
  );
  assert_eq!(count_frames(wal.scan(committed, final_committed)).await?, 1);
  drop(wal);

  // 二次恢复：收敛至全量，恢复干净无截尾观测
  let wal = reopen_single_file(dir.path(), file_name, BUF).await?;
  assert_eq!(wal.committed_until_address(), final_committed);
  assert_eq!(wal.tail_address(), final_committed);
  assert_eq!(wal.recovered_cookie(), 9);
  assert_eq!(wal.recover_truncation(), None, "二次恢复必幂等无观测");
  // 盘面须覆盖全部已确认字节（累加所有段文件大小）
  let disk_len: u64 = read_dir(dir.path())?
    .flatten()
    .filter(|e| {
      e.file_name()
        .to_str()
        .is_some_and(|name| name.starts_with(file_name))
    })
    .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
    .sum();
  assert!(disk_len >= final_committed, "盘面须覆盖全部已确认字节");

  info!("commit 帧腾窗补帧：恢复不回退与已确认批全量回放测试通过");
  OK
}

/// 不变量 4（并发不死锁）：并发写入叠加满窗压力下 wait_for_commit 有界收敛——
/// 每个写入者满窗背压时先提交腾窗再重试入队，提交侧腾窗自旋复用同一窗口腾出
/// 机制，全部等待者被唤醒且返回位点覆盖各自写入（对标 C# 重试契约：并发提交
/// 在提交锁上阻塞等待，绝无静默跳帧丢见证）
#[test]
fn test_commit_frame_full_window_concurrent_wait_no_deadlock() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("commit_frame_concurrent.log", BUF)?;
    let wal = fixture.wal;

    // 主线程先把窗口灌满：并发写入者的首批入队与提交即刻落入腾窗自旋臂
    let lens = fill_window(&wal)?;

    const WORKERS: u32 = 4;
    const ROUNDS: u32 = 8;

    let mut handles = Vec::new();
    for worker in 0..WORKERS {
      let wal = Arc::clone(&wal);
      handles.push(spawn(move || -> aok::Result<()> {
        let worker_rt = Runtime::new()?;
        worker_rt.block_on(async move {
          for round in 0..ROUNDS {
            for i in 0..2u32 {
              let seq = (worker * ROUNDS + round) * 2 + i;
              let addr = loop {
                match wal.enqueue(&make_pattern_payload(seq as usize, 64)) {
                  Ok(addr) => break addr,
                  // 满窗背压：先提交腾窗再重试（对标生产背压路径）
                  Err(Error::BufferFull { .. }) => {
                    wal.commit().await?;
                    yield_now();
                  }
                  Err(e) => return Err(e.into()),
                }
              };
              // 高速提交栅栏：返回位点必须覆盖自身写入尾（不死锁、不静默丢确认）
              let end = addr + RECORD_HEADER_LEN as u64 + 64;
              let committed = wal.wait_for_commit(end).await?;
              assert!(
                committed >= end,
                "唤醒位点 {committed} 须覆盖自身写入尾 {end}"
              );
            }
          }
          aok::Result::<()>::Ok(())
        })?;
        Ok(())
      }));
    }

    for handle in handles {
      handle.join().expect("并发提交线程正常结束")?;
    }

    // 静默后位点一致性：提交水位 == 刷盘水位 == 尾地址，帧游标收敛帧尾
    let tail = wal.tail_address();
    assert_eq!(wal.committed_until_address(), tail, "提交水位须追平尾地址");
    assert_eq!(wal.flushed_until_address(), tail, "刷盘水位须追平尾地址");
    assert_eq!(wal.last_commit_frame.load(Ordering::Acquire), tail);

    // 数据面闭环：全部并发写入均可回放
    let data = support::collect_data(wal.scan(wal.begin_address(), tail)).await?;
    assert_eq!(data.len(), lens.len() + (WORKERS * ROUNDS * 2) as usize);

    info!("commit 帧腾窗补帧：并发满窗 wait_for_commit 有界收敛测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
