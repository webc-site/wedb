use std::{
  process,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
  },
  thread,
  time::{Duration, Instant},
};

use aok::{OK, Result, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{Error, HybridLog, HybridLogConfig, SECTOR_ALIGNMENT};

/// 截断屏障并发测试共享装配：建实例、批量追加、刷盘 + sync，返回 (hlog, epoch, 地址表)
async fn setup_truncate_fixture(
  dir: &tempfile::TempDir,
  tag: &str,
  n_records: usize,
) -> Result<(
  Arc<HybridLog<SegmentedDevice>>,
  Arc<LightEpoch>,
  Vec<(u64, Vec<u8>)>,
)> {
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag))?);
  let epoch = Arc::new(LightEpoch::new(32));
  let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 8, 0.5)?;
  let hlog = Arc::new(HybridLog::new(config, device, epoch.clone())?);

  let mut addrs = Vec::with_capacity(n_records);
  for i in 0..n_records {
    let key = format!("k{i:03}").into_bytes();
    let addr = hlog.append(&key, &[b'v'; 24], 0, false)?;
    addrs.push((addr, key));
  }
  hlog.flush_all().await?;
  hlog.sync().await?;
  Ok((hlog, epoch, addrs))
}

/// 并发测试看门狗：超时后周期性转储地址快照与纪元状态诊断，再 grace 期后 abort
///
/// 防自死锁/活锁时 nextest 无声挂死：abort 触发全线程栈转储，诊断先行落 stderr
fn spawn_shift_watchdog(
  label: &'static str,
  done: Arc<AtomicBool>,
  hlog: Arc<HybridLog<SegmentedDevice>>,
  deadline: Duration,
) -> thread::JoinHandle<()> {
  thread::spawn(move || {
    let start = Instant::now();
    let mut reported = false;
    while !done.load(Ordering::Acquire) {
      thread::sleep(Duration::from_millis(100));
      let elapsed = start.elapsed();
      if elapsed > deadline && !reported {
        reported = true;
        eprintln!(
          "[看门狗:{label}] 超过 {deadline:?} 未完成，转储诊断: snapshot={:?} epoch={:?}",
          hlog.addresses.snapshot(),
          hlog.epoch
        );
      }
      if elapsed > deadline + Duration::from_secs(2) {
        eprintln!("[看门狗:{label}] grace 期满仍未完成，判定挂死，abort 转储全部线程栈");
        process::abort();
      }
    }
  })
}

/// 测试: 截断前纪元排空屏障（确定性交错）——读者守卫先于 shifter 全部推进入场，
/// 屏障强制截断 happens-after 读者守卫退出；事件日志断言「守卫退出」不得晚于「截断完成」
#[test]
fn test_shift_begin_truncation_drain_barrier() -> Void {
  use std::sync::{Condvar, Mutex};

  #[derive(Debug, PartialEq)]
  enum Event {
    Dropped,
    Done,
  }

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (hlog, epoch, addrs) = setup_truncate_fixture(&dir, "barrier.db", 128).await?;

    // 读者线程先入场持守卫（TLS ProtectedScope，对标 wkv session.read_record
    // 守卫横跨磁盘 I/O 协议）。三段握手把采样点钉在 head 挪线后、begin 挪线前：
    // Ready → shifter 推进 head 发 Go → 读者采样并发起在途冷读回 Sampled →
    // shifter 才 shift_begin（屏障必须等守卫退出后才能截断）。
    // 事件经 Mutex+Condvar 记录（mpsc 多生产者不保证跨 Sender 全局 FIFO，
    // 无法用于保序断言）
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (sampled_tx, sampled_rx) = mpsc::channel::<()>();
    let log: Arc<(Mutex<Vec<Event>>, Condvar)> = Arc::new((Mutex::new(Vec::new()), Condvar::new()));

    let hlog_r = Arc::clone(&hlog);
    let epoch_r = Arc::clone(&epoch);
    let log_r = Arc::clone(&log);
    let addr_r = addrs[16].0;
    let key_r = addrs[16].1.clone();
    let reader = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        {
          let _guard = epoch_r.protected_scope();
          let _ = ready_tx.send(());
          if go_rx.recv().is_err() {
            return Err(aok::Error::msg("shifter 已退出，握手断裂"));
          }
          // 守卫持有期间采样磁盘区（截断线以下地址）
          if !hlog_r.is_on_disk(addr_r) {
            return Err(aok::Error::msg(format!(
              "采样点必须在截断线推进前: addr_r={addr_r:#x} snapshot={:?}",
              hlog_r.addresses.snapshot()
            )));
          }
          let _ = sampled_tx.send(());
          // 持守卫连续冷读拉长窗口。begin 在屏障前先行挪线（封死新采样），
          // 故挪线后的新冷读被干净拒绝（PageNotReady）；截断被屏障挡在守卫
          // 退出之后，已采样读者绝不允许设备级撕裂（NotFound/Corrupted）
          let read_res = async {
            for i in 0..64 {
              match hlog_r.read_disk_record(addr_r).await {
                Ok(out) => {
                  let key = out
                    .key()
                    .map_err(|e| aok::Error::msg(format!("第{i}次冷读键损坏: {e}")))?;
                  if key != key_r.as_slice() {
                    return Err(aok::Error::msg(format!("第{i}次冷读撕裂: 键不匹配")));
                  }
                }
                Err(Error::PageNotReady(_)) => break,
                Err(e) => return Err(aok::Error::msg(format!("第{i}次冷读设备级撕裂: {e}"))),
              }
            }
            Ok(())
          }
          .await;
          // Dropped 须在守卫存活期内记录：屏障的释放触发点就是守卫 drop 本身，
          // 退守卫后再记事件会与 shifter 的 Done 记录产生无约束竞速
          log_r.0.lock().unwrap().push(Event::Dropped);
          log_r.1.notify_all();
          read_res
        }
      })
    });

    let hlog_s = Arc::clone(&hlog);
    let log_s = Arc::clone(&log);
    let cut = addrs[32].0;
    let head_at = addrs[96].0;
    let shifter = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        if ready_rx.recv().is_err() {
          return Err(aok::Error::msg("reader 已退出，握手断裂"));
        }
        // head 推进使前半区划入磁盘候选区（内部注册 safe_head 延迟动作）
        hlog_s.shift_head_address(head_at);
        let _ = go_tx.send(());
        if sampled_rx.recv().is_err() {
          return Err(aok::Error::msg("reader 采样失败，握手断裂"));
        }
        // 屏障必须等读者守卫退出（safe_ro 越过截断线）后才截断
        hlog_s.shift_begin_address(cut).await?;
        log_s.0.lock().unwrap().push(Event::Done);
        log_s.1.notify_all();
        Ok(())
      })
    });

    let reader_res = reader.join().unwrap();
    shifter.join().unwrap()?;

    // 保序断言：Dropped（守卫退出）必须先于 Done（含截断的 shift 完成）——
    // 旧实现无屏障时 Done 先至即失败
    {
      let (m, cv) = &*log;
      let events = cv.wait_while(m.lock().unwrap(), |v| v.len() < 2).unwrap();
      assert_eq!(
        *events,
        vec![Event::Dropped, Event::Done],
        "截断屏障失效：截断先于读者守卫退出完成"
      );
    }
    reader_res?;

    // 屏障后置状态：begin 已推进，截断线以下地址被干净拒绝（绝无设备级撕裂错误）
    assert_eq!(hlog.begin_address(), cut);
    assert!(hlog.safe_read_only_address() >= cut);
    match hlog.read_disk_record(addr_r).await {
      Err(Error::PageNotReady(_)) => {}
      other => panic!("截断线以下冷读应干净返回 PageNotReady，实际: {other:?}"),
    }
    assert!(hlog.addresses.validate_invariants());

    info!("截断前纪元排空屏障测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试: 调用方自身持纪元守卫调用 shift_begin_address——TLS 守卫自动退出重入、
/// Participant 守卫自动刷新解除自钉，均不得自死锁（屏障排空调用方自身持有的旧纪元）
#[test]
fn test_shift_begin_under_caller_epoch_guard() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (hlog, epoch, addrs) = setup_truncate_fixture(&dir, "guard.db", 32).await?;

    // Case 1: TLS 保护区（ProtectedScope）内截断——屏障须逐层 suspend 退出、
    // 排空后按原深度 resume 重入（旧实现此处永久自旋）
    {
      let _guard = epoch.protected_scope();
      hlog.shift_begin_address(addrs[8].0).await?;
    }
    assert_eq!(hlog.begin_address(), addrs[8].0);
    // TLS 守卫退出重入后保护语义必须完好：守卫下内存直读仍安全
    assert!(hlog.with_memory_record(addrs[16].0, |_| Ok(()))?.is_some());

    // Case 2: Participant 句柄守卫内截断——守卫归调用方所有无法代为退出，
    // 屏障须刷新本线程公布纪元解除自钉（旧实现此处永久自旋）
    let participant = epoch.register()?;
    {
      let _guard = participant.enter();
      hlog.shift_begin_address(addrs[16].0).await?;
    }
    assert_eq!(hlog.begin_address(), addrs[16].0);
    assert!(hlog.safe_read_only_address() >= addrs[16].0);
    assert!(hlog.addresses.validate_invariants());

    info!("调用方持守卫截断自动解钉测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试: 预刷盘直入路径（flushed >= new_begin）——无条件推进只读边界保证
/// safe_ro drain action 必已注册，屏障谓词有归属，绝不挂死
#[test]
fn test_shift_begin_preflushed_barrier_liveness() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (hlog, _epoch, addrs) = setup_truncate_fixture(&dir, "preflush.db", 16).await?;

    // 不做任何先行 shift，直接截断已刷盘区间（紧缩路径常态）
    hlog.shift_begin_address(addrs[8].0).await?;

    assert_eq!(hlog.begin_address(), addrs[8].0);
    assert!(hlog.safe_read_only_address() >= addrs[8].0);
    assert!(hlog.head_address() >= addrs[8].0);
    match hlog.read_record(addrs[4].0).await {
      Err(Error::AddressOutOfRange { .. }) => {}
      other => panic!("截断线以下读取应干净返回 AddressOutOfRange，实际: {other:?}"),
    }
    assert!(hlog.addresses.validate_invariants());

    info!("预刷盘直入截断屏障活性测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试: 磁盘区在途读者与并发 shift_begin_address 压力——多轮读者冷读与
/// 递进截断并发，持守卫读者绝不观察设备级撕裂（RecordCorrupted / 短读），
/// 看门狗超时转储诊断防挂死
#[test]
fn test_concurrent_disk_read_vs_shift_begin() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (hlog, epoch, addrs) = setup_truncate_fixture(&dir, "race.db", 256).await?;
    let addrs = Arc::new(addrs);
    let n = addrs.len() as u64;

    // 前 3/4 划入磁盘候选区
    hlog.shift_head_address(addrs[192].0);

    let done = Arc::new(AtomicBool::new(false));
    let watchdog = spawn_shift_watchdog(
      "read-vs-shift",
      Arc::clone(&done),
      Arc::clone(&hlog),
      Duration::from_secs(20),
    );

    // 读者线程：守卫横跨单次冷读（对标 wkv session.read_record 协议），
    // 随机游走遍历磁盘候选区；Ok 路径键必须完整一致，其余仅容许
    // 输在采样竞速的干净拒绝（AddressOutOfRange / PageNotReady）
    let hlog_r = Arc::clone(&hlog);
    let epoch_r = Arc::clone(&epoch);
    let done_r = Arc::clone(&done);
    let addrs_r = Arc::clone(&addrs);
    let reader = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        let mut i = 0u64;
        let mut reads = 0u64;
        while !done_r.load(Ordering::Acquire) {
          i = (i + 7) % n;
          let (addr, key) = (&addrs_r[i as usize].0, &addrs_r[i as usize].1);
          let _guard = epoch_r.protected_scope();
          match hlog_r.read_record(*addr).await {
            Ok(out) => {
              reads += 1;
              assert_eq!(
                out.key()?,
                key.as_slice(),
                "在途读者读到撕裂记录: addr={addr:#x}"
              );
            }
            Err(Error::AddressOutOfRange { .. } | Error::PageNotReady(_)) => {}
            Err(e) => {
              return Err(aok::Error::msg(format!(
                "在途读者观察到底层撕裂: addr={addr:#x} err={e}"
              )));
            }
          }
        }
        info!("读者完成 {reads} 次有效冷读");
        Ok(())
      })
    });

    // 截断线程：递进推进 begin，8 轮压缩磁盘区
    let final_begin = addrs[128].0;
    let hlog_s = Arc::clone(&hlog);
    let addrs_s = Arc::clone(&addrs);
    let shifter = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        for round in 1..=8 {
          let cut = addrs_s[round * 16].0;
          hlog_s.shift_begin_address(cut).await?;
        }
        Ok(())
      })
    });

    let shift_res = shifter.join().unwrap();
    done.store(true, Ordering::Release);
    let reader_res = reader.join().unwrap();
    shift_res?;
    reader_res?;
    watchdog.join().unwrap();

    assert_eq!(hlog.begin_address(), final_begin);
    assert!(hlog.addresses.validate_invariants());

    info!("磁盘区在途读者与并发截断压力测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
