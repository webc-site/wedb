//! 推流唤醒信号与在途槽位释放的次序回归：写入 → signal → pump 可见
//!
//! enqueue_reserved 曾以「先 try_send 唤醒、后释放在途槽位」的次序落地：
//! thread-per-core 多核下 pump 被信号唤醒即刻可在另一核运行，读到的
//! `safe_tail_address` 下界折掉本写入者未释放的槽位，扫描扑空后回 recv
//! 深度挂起；该帧信号已被消费、写者释放槽位后不再有信号，末批记录写入
//! 静默后无限期滞留（副本位点静默滞后，failover 判定即丢帧面）。
//!
//! 回归口径（跨核定向，pump 在写入进行中实时消费信号，不得事后补扫）：
//! 1. 单轮末批：写泵锁步，每轮写入恰好一条记录后立即静默（末批形态），
//!    pump 凭信号推送必须覆盖该轮记录——信号后置时逐轮确定通过；
//! 2. 多写静默收敛：多线程压力写入期间 pump 实时推送，全体静默后不补
//!    任何信号，pump 仅凭既有信号必须在限时内推平全量尾——末批信号后置
//!    保证最后释放者的信号必然覆盖全尾。
//!
//! 黑盒检测边界：信号发送与槽位释放之间原为 ns 级窗口，仅写线程恰被
//! 调度出该窗口时折损 safe_tail 才可观测，故对次序回退的检出为统计性
//! （逐轮累积暴露概率）；信号后置本身为代码可审计的确定性次序，本套
//! 测试确定性证明修复后的验收性质——写入静默后末批必被推送。
//!
//! 结构：enqueue 与 tail 读取均为同步 API，写者/监督在测试主线程，pump
//! 自持 compio runtime 独立线程实时消费信号；监督以原子量 + 轮询限时，
//! 回归命中表现为推进停格超时断言，而非挂死。
//!
//! 自研依据: 等待者唤醒信号保序（crossfire 面板，C# 对应 EnqueueAndWaitForCommit.cs/WaitForCommit.cs）

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread::{sleep, spawn},
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use crossfire::mpsc::bounded_async;
use waof::WalLog;
use wdev::SegmentedDevice;

use super::support::WalFixture;

/// 每条记录负载长度（8B 头 + 负载，控制环形窗口不覆写）
const PAYLOAD_LEN: usize = 32;
/// 单轮末批回归轮数（逐轮放大跨核竞态窗口暴露概率）
const SINGLE_BATCH_ROUNDS: u64 = 512;
/// 多写压力的写线程数与每线程写入条数
const WRITER_THREADS: u64 = 4;
const RECORDS_PER_WRITER: u64 = 512;
/// 单轮推送限时（锁步下信号必达，超时即回归命中而非挂死）
const ROUND_TIMEOUT: Duration = Duration::from_secs(5);
/// 静默后收敛限时（正常毫秒级收敛，超时即末批滞留回归命中）
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(10);

/// pump 侧扫描推进：从 cursor 扫至 safe_tail，返回（推进后游标，新增条数）
async fn pump_scan(wal: &WalLog<SegmentedDevice>, cursor: u64) -> (u64, u64) {
  let until = wal.safe_tail_address();
  if cursor >= until {
    return (cursor, 0);
  }
  let mut iter = wal.scan(cursor, until);
  let mut count = 0u64;
  while let Some(_record) = iter.next().await.expect("扫描不失败") {
    count += 1;
  }
  (until, count)
}

/// 单轮末批回归：写泵锁步——写者每轮写一条即静默并等待 pump 推完，pump
/// 凭信号读 safe_tail 推送，断言每轮信号抵达后记录必已入安全可读面。
/// 信号后置时逐轮确定通过；信号先于槽位释放（回归形态）时，pump 被唤醒
/// 读到折损 safe_tail，推进停格使锁步限时断言命中（写者恰好未被调度出
/// 该窗口时逐轮累积暴露概率）。
#[test]
fn wake_signal_covers_final_batch_per_round() -> Void {
  let fixture = WalFixture::single_file("wake_order.log", 1024 * 1024)?;
  let wal = fixture.wal;

  let (tx, rx) = bounded_async::<()>(1);
  assert!(wal.set_replication_wake(tx), "唤醒端一次性注册");

  // pump 已推完轮数：锁步放行写者下一轮
  let pumped_rounds = Arc::new(AtomicU64::new(0));

  // pump 独立线程实时消费信号：每帧信号抵达即读 safe_tail 扫描推送
  let pump = {
    let wal = Arc::clone(&wal);
    let pumped_rounds = Arc::clone(&pumped_rounds);
    spawn(move || {
      let rt = Runtime::new().unwrap();
      rt.block_on(async {
        let mut pushed = 0u64;
        loop {
          if rx.recv().await.is_err() {
            break;
          }
          let (cursor, _) = pump_scan(&wal, pushed).await;
          pushed = cursor;
          pumped_rounds.fetch_add(1, Ordering::Release);
          if pumped_rounds.load(Ordering::Acquire) == SINGLE_BATCH_ROUNDS {
            break;
          }
        }
      });
    })
  };

  // 写者（主线程）：锁步单轮末批——写一条即静默，等 pump 推完再写下一轮
  let mut expected = 0u64;
  for round in 0..SINGLE_BATCH_ROUNDS {
    let wait_deadline = Instant::now() + ROUND_TIMEOUT;
    while pumped_rounds.load(Ordering::Acquire) < round {
      assert!(
        Instant::now() < wait_deadline,
        "第 {round} 轮信号未在限时内推送（末批滞留）"
      );
      sleep(Duration::from_micros(50));
    }
    let mut payload = vec![0u8; PAYLOAD_LEN];
    payload[0] = (round % 256) as u8;
    wal.enqueue(&payload)?;
    // 写者静默：本轮信号发出后再无任何写入
    expected = wal.tail_address();
  }

  pump.join().expect("pump 线程正常结束");
  assert_eq!(wal.safe_tail_address(), expected, "全部记录推平日志尾");
  OK
}

/// 多写静默收敛：4 线程压力写入期间 pump 实时推送；全体静默后不补任何
/// 信号、不做任何手动补扫，pump 仅凭既有信号必须在限时内推平全量尾——
/// 末批信号后置保证最后释放者的信号覆盖全尾，写入静默后末批记录不得
/// 滞留（回归形态：末批信号被在泵窗口消费掉，pump 深眠无信号可收，
/// 推进停格命中断言）。
#[test]
fn silent_tail_batch_is_pushed_after_writes() -> Void {
  let fixture = WalFixture::single_file("wake_silent_tail.log", 1024 * 1024)?;
  let wal = fixture.wal;

  let (tx, rx) = bounded_async::<()>(1);
  assert!(wal.set_replication_wake(tx), "唤醒端一次性注册");

  let mut writers = Vec::new();
  for worker in 0..WRITER_THREADS {
    let wal = Arc::clone(&wal);
    writers.push(spawn(move || -> Void {
      let writer_rt = Runtime::new()?;
      writer_rt.block_on(async {
        for i in 0..RECORDS_PER_WRITER {
          let mut payload = vec![0u8; PAYLOAD_LEN];
          payload[0] = ((worker * 16 + i % 16) % 256) as u8;
          wal.enqueue(&payload)?;
        }
        wal.commit().await?;
        OK
      })
    }));
  }
  for writer in writers {
    writer.join().expect("写者线程正常结束")?;
  }
  // 全体静默：此后不得有任何写入或手动补扫
  let final_tail = wal.tail_address();

  // pump 独立线程实时消费信号，推送进度经原子量回传监督面
  let pushed = Arc::new(AtomicU64::new(0));
  let pump = {
    let wal = Arc::clone(&wal);
    let pushed = Arc::clone(&pushed);
    spawn(move || {
      let rt = Runtime::new().unwrap();
      rt.block_on(async {
        let mut cursor = 0u64;
        loop {
          if rx.recv().await.is_err() {
            break;
          }
          let (next, _) = pump_scan(&wal, cursor).await;
          cursor = next;
          pushed.store(cursor, Ordering::Release);
          if cursor >= final_tail {
            break;
          }
        }
      });
    })
  };

  // 监督：仅既有信号驱动推进，静默后不补信号不补扫，限时内须推平全尾
  let deadline = Instant::now() + CONVERGE_TIMEOUT;
  while pushed.load(Ordering::Acquire) < final_tail {
    assert!(
      Instant::now() < deadline,
      "写入静默后末批滞留：pump 推进 {} 未达日志尾 {final_tail}",
      pushed.load(Ordering::Acquire)
    );
    sleep(Duration::from_millis(5));
  }

  pump.join().expect("pump 线程正常结束");
  assert_eq!(
    pushed.load(Ordering::Acquire),
    final_tail,
    "静默后末批仍被推送，推平日志尾"
  );
  OK
}
