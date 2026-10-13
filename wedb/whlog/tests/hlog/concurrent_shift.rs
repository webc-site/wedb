//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.hlog/LogShiftTailStressTest.cs（并发 ShiftTail）
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

use aok::{self, Error as AokError, OK, Result, Void};
use compio::runtime::Runtime;
use log::info;
use parking_lot::{Condvar, Mutex};
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
    let (addr, _) = hlog.append(&key, &[b'v'; 24], 0, false)?;
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
            return Err(AokError::msg("shifter 已退出，握手断裂"));
          }
          // 守卫持有期间采样磁盘区（截断线以下地址）
          if !hlog_r.is_on_disk(addr_r) {
            return Err(AokError::msg(format!(
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
                    .map_err(|e| AokError::msg(format!("第{i}次冷读键损坏: {e}")))?;
                  if key != key_r.as_slice() {
                    return Err(AokError::msg(format!("第{i}次冷读撕裂: 键不匹配")));
                  }
                }
                Err(Error::PageNotReady(_)) => break,
                Err(e) => return Err(AokError::msg(format!("第{i}次冷读设备级撕裂: {e}"))),
              }
            }
            Ok(())
          }
          .await;
          // Dropped 须在守卫存活期内记录：屏障的释放触发点就是守卫 drop 本身，
          // 退守卫后再记事件会与 shifter 的 Done 记录产生无约束竞速
          log_r.0.lock().push(Event::Dropped);
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
          return Err(AokError::msg("reader 已退出，握手断裂"));
        }
        // head 推进使前半区划入磁盘候选区（内部注册 safe_head 延迟动作）
        hlog_s.shift_head_address(head_at);
        let _ = go_tx.send(());
        if sampled_rx.recv().is_err() {
          return Err(AokError::msg("reader 采样失败，握手断裂"));
        }
        // 屏障必须等读者守卫退出（safe_ro 越过截断线）后才截断
        hlog_s.shift_begin_address(cut).await?;
        log_s.0.lock().push(Event::Done);
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
      let mut events = m.lock();
      cv.wait_while(&mut events, |v| v.len() < 2);
      assert_eq!(
        events.as_slice(),
        [Event::Dropped, Event::Done],
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
              return Err(AokError::msg(format!(
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

    info!("磁盘区在途读者与并发截断压力测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试: 常规刷盘的读侧上界屏障（确定性交错）——写者在纪元守卫内发布 tail 并编码，
/// 随后强持守卫不放手；刷盘内核必须先推进 SafeReadOnlyAddress 并等该守卫排空，
/// 方可拷贝落笔。事件日志断言「守卫退出」不得晚于「刷盘完成」，并钉死
/// `flushed_until <= safe_read_only` 不变式（旧实现只读线在刷完之后才推进，
/// 越界刷立即完成 → 次序相反即失败）
#[test]
fn test_flush_sealed_behind_writer_epoch_guard() -> Void {
  #[derive(Debug, PartialEq)]
  enum Event {
    Dropped,
    Flushed,
  }

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("seal_flush.db"),
    )?);
    let epoch = Arc::new(LightEpoch::new(32));
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 8, 0.5)?;
    let hlog = Arc::new(HybridLog::new(config, device, Arc::clone(&epoch))?);

    let (guarded_tx, guarded_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let log: Arc<(Mutex<Vec<Event>>, Condvar)> = Arc::new((Mutex::new(Vec::new()), Condvar::new()));

    // 写者线程：守卫内追加 16 条（跨过页界，tail 越过安全只读线），追加完成后
    // 仍持守卫等待放行——「tail 已发布」与「所有线程一致认可该区间定稿」在这段
    // 窗口内刻意分离，正是 C# 用 SafeReadOnlyAddress 挡住刷盘的竞态窗口
    let hlog_w = Arc::clone(&hlog);
    let epoch_w = Arc::clone(&epoch);
    let log_w = Arc::clone(&log);
    let writer = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        {
          let _guard = epoch_w.protected_scope();
          for i in 0..16 {
            hlog_w.append(format!("k{i:02}").as_bytes(), &[b'v'; 24], 0, false)?;
          }
          let _ = guarded_tx.send(());
          if release_rx.recv().is_err() {
            return Err(AokError::msg("主任务已退出，握手断裂"));
          }
          // Dropped 必须在守卫存活期内记录：守卫释放即屏障释放触发点，
          // 退守卫后再记事件会与刷盘完成产生无约束竞速
          log_w.0.lock().push(Event::Dropped);
          log_w.1.notify_all();
        }
        Ok(())
      })
    });

    if guarded_rx.recv().is_err() {
      return Err(AokError::msg("写者已退出，握手断裂"));
    }
    let tail = hlog.tail_address();
    assert!(
      tail > hlog.safe_read_only_address(),
      "装配失败：写者守卫期内安全只读线未落后于 tail, snapshot={:?}",
      hlog.addresses.snapshot()
    );

    let done = Arc::new(AtomicBool::new(false));
    let watchdog = spawn_shift_watchdog(
      "flush-seal",
      Arc::clone(&done),
      Arc::clone(&hlog),
      Duration::from_secs(5),
    );

    // 刷盘线程：与守卫持有期重叠发起 flush_all（目标即 tail，越过当时 safe_ro）
    let hlog_f = Arc::clone(&hlog);
    let log_f = Arc::clone(&log);
    let flusher = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        hlog_f.flush_all().await?;
        hlog_f.sync().await?;
        log_f.0.lock().push(Event::Flushed);
        log_f.1.notify_all();
        Ok(())
      })
    });

    // 给刷盘线程足够时间入场阻塞在排空屏障上，再放行写者
    thread::sleep(Duration::from_millis(100));
    let _ = release_tx.send(());

    let flush_res = flusher.join().unwrap();
    let writer_res = writer.join().unwrap();
    done.store(true, Ordering::Release);
    watchdog.join().unwrap();
    flush_res?;
    writer_res?;

    {
      let (m, cv) = &*log;
      let mut events = m.lock();
      cv.wait_while(&mut events, |v| v.len() < 2);
      assert_eq!(
        events.as_slice(),
        [Event::Dropped, Event::Flushed],
        "越界刷：刷盘先于写者守卫退出完成，说明未先把 SafeReadOnly 封印达标就落笔"
      );
    }
    assert!(
      hlog.flushed_until_address() <= hlog.safe_read_only_address(),
      "持久化前缀越过安全只读线: snapshot={:?}",
      hlog.addresses.snapshot()
    );
    assert_eq!(
      hlog.flushed_until_address(),
      tail,
      "封印排空后刷盘须覆盖至 tail"
    );

    info!("常规刷盘读侧上界屏障测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 扫描迭代器×截断交错测试共享装配：分段设备（段大小 = 2 页）+ 4096B 扇区页，
/// 追加 n 条记录跨越多页多段，全量刷盘 + sync，返回 (hlog, 设备句柄, 地址表)。
/// 段几何（页 4096 × 段 8192 = 2 页/段，48B 记录）：页 0/1 同落段 0，截断线取
/// ≥8192 的首条记录（段 1 内），shift_begin_address 抬 begin 至该线并物理 unlink
/// 段 0 —— 暂停在页 0 末的在途扫描下一轮整页读必落已删段（危害一），或文件俱在
/// 时越界产出截断区陈旧记录（危害二），两面分别由下面两个测试钉死
async fn setup_scan_shift_fixture(
  dir: &tempfile::TempDir,
  tag: &str,
  n_records: usize,
) -> Result<(
  Arc<HybridLog<SegmentedDevice>>,
  Arc<SegmentedDevice>,
  Vec<(u64, Vec<u8>)>,
)> {
  let device = Arc::new(SegmentedDevice::new(
    dir.path().join(tag),
    2 * SECTOR_ALIGNMENT as u64,
    SECTOR_ALIGNMENT,
  )?);
  let epoch = Arc::new(LightEpoch::new(32));
  let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 8, 0.5)?;
  let hlog = Arc::new(HybridLog::new(config, Arc::clone(&device), epoch)?);

  let mut addrs = Vec::with_capacity(n_records);
  for i in 0..n_records {
    let key = format!("k{i:04}").into_bytes();
    let (addr, _) = hlog.append(&key, &[b'v'; 24], 0, false)?;
    addrs.push((addr, key));
  }
  hlog.flush_all().await?;
  hlog.sync().await?;
  Ok((hlog, device, addrs))
}

/// 从装配地址表导出本几何的交错锚点：
/// - `page0_end`：页 0 内记录条数（扫完页 0 后预取缓存停在页 0，下一轮必对页 1
///   发起整页设备读，页 1 与页 0 同落段 0）；
/// - `head_pre`：页 1 内第 3 条记录地址（扫前划入磁盘区的 head 线，保证暂停点
///   之后仍处「head 之下 → 分支 1 冷读」路径）；
/// - `cut_idx`：首条地址 ≥ 段 1 起点（8192）的记录下标（截断线，其所在段受保留，
///   其前整段可被 truncate_until_address 回收）。
///
/// 装配自证断言钉死几何前提，几何漂移即刻报装配失败而非误判修复
fn scan_shift_anchors(
  hlog: &HybridLog<SegmentedDevice>,
  addrs: &[(u64, Vec<u8>)],
) -> (usize, u64, usize) {
  let page0_end = addrs
    .iter()
    .position(|(a, _)| hlog.config.page_id(*a) == 1)
    .expect("装配失败：记录未跨出页 0");
  let cut_idx = addrs
    .iter()
    .position(|(a, _)| *a >= 2 * SECTOR_ALIGNMENT as u64)
    .expect("装配失败：记录未跨入段 1");
  assert!(
    cut_idx > page0_end + 3,
    "装配失败：截断线未落在页 0 之后（page0_end={page0_end}, cut_idx={cut_idx}）"
  );
  let head_pre = addrs[page0_end + 2].0;
  assert_eq!(
    hlog.config.page_id(head_pre),
    1,
    "装配失败：head 预推线不在页 1"
  );
  (page0_end, head_pre, cut_idx)
}

/// 测试: 在途磁盘区扫描遭遇并发截断删段——扫描暂停在页 0 末（预取缓存停在页 0、
/// 游标抵页 1）后，shift_begin_address 抬 begin 越入段 1 并物理 unlink 段 0（页 0/1
/// 所在段）；续扫的下一轮整页读目标正是已删段内的页 1。旧实现构造期单次快照 begin、
/// 主循环不复验，游标滞后新 begin 且 `curr < head` 落入分支 1 向已 unlink 的段文件
/// read_range，IO 错误（NotFound）经 `?` 上抛中断整个扫描（危害一）——摘除
/// next_ref 循环头钳位臂本测试必红于此。修复对标 C# LoadPageIfNeeded 的 BeginAddress
/// 动态钳位（garnet SpanByteScanIterator.cs:135-136 / ObjectScanIterator.cs:136-137
/// `if (currentAddress < hlogBase.BeginAddress && !assumeInMemory)
///     currentAddress = hlogBase.BeginAddress;`，本迭代器恒混合三区无 assumeInMemory
/// 档，钳位臂无条件生效）：游标整体跃迁至新鲜 begin，绕开已删段平滑续扫 [cut, tail)
/// 全部存活记录后干净终止
#[test]
fn test_scan_iterator_clamps_to_shifted_begin_after_segment_removal() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (hlog, device, addrs) = setup_scan_shift_fixture(&dir, "scan_cut.db", 256).await?;
    let tail = hlog.tail_address();
    let (page0_end, head_pre, cut_idx) = scan_shift_anchors(&hlog, &addrs);

    // 页 0 与页 1 前部划入磁盘候选区（head 之下 begin 之上走分支 1 冷读）；
    // 迭代器扫完页 0 全部记录即暂停：此刻磁盘预取缓存停在页 0
    hlog.shift_head_address(head_pre);
    let mut it = hlog.scan_iter(hlog.begin_address(), tail);
    let mut consumed = 0usize;
    while let Some((_addr, out)) = it.next().await? {
      assert_eq!(
        out.key()?,
        addrs[consumed].1.as_slice(),
        "暂停前扫描须按地址升序产出全部存活记录"
      );
      consumed += 1;
      if consumed == page0_end {
        assert_eq!(it.current_address(), hlog.config.page_start_address(1));
        break;
      }
    }
    assert_eq!(consumed, page0_end, "装配失败：页 0 记录未全部扫出");

    // 模拟检查点发布（抬删段地板至截断线）后截断：段 0 被物理 unlink，取证必真删
    let cut = addrs[cut_idx].0;
    hlog.raise_delete_floor(cut);
    hlog.shift_begin_address(cut).await?;
    assert_eq!(hlog.begin_address(), cut);
    assert!(
      device.start_segment() >= 1 && device.get_file_size(0)? == 0,
      "取证失败：截断线前段文件未被物理删除，本测试未覆盖删段危害面（危害一）"
    );

    // 续扫：修复后钳位臂跃迁至 begin，绝不触碰已删段；产出集恰为 [cut, tail) 全表
    let mut rest = Vec::new();
    while let Some((addr, out)) = it.next().await? {
      rest.push((addr, out.key()?.to_vec()));
    }
    assert_eq!(
      rest,
      addrs[cut_idx..].to_vec(),
      "钳位跃迁后须无缝续扫截断线之后全部存活记录（旧实现在此对已删段 read_range 抛 IO 错误）"
    );
    assert!(
      it.current_address() >= tail,
      "迭代器须越过终点干净终止，游标停在 {:?}",
      it.current_address()
    );

    info!("在途扫描遭遇截断删段动态钳位续扫测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试: 删段受检查点窗钳制延后（delete_floor 为 0 全禁删、段文件俱在）时的越界
/// 陈旧读防御——shift_begin_address 只推逻辑 begin 不 unlink，无钳位臂的旧迭代器
/// 照常冷读已截断区间页并把这些逻辑上不可见的陈旧记录产出给上层（危害二）。
/// 修复后钳位臂整体跳过截断区，产出集恰为 [cut, tail)。与上一测试同一钳位单点、
/// 不同危害面（一测 IO 错误中断、一测陈旧数据越界产出），故不合并
#[test]
fn test_scan_iterator_yields_no_truncated_records_under_begin() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (hlog, device, addrs) = setup_scan_shift_fixture(&dir, "scan_stale.db", 256).await?;
    let tail = hlog.tail_address();
    let (page0_end, head_pre, cut_idx) = scan_shift_anchors(&hlog, &addrs);

    hlog.shift_head_address(head_pre);
    let mut it = hlog.scan_iter(hlog.begin_address(), tail);
    let mut consumed = 0usize;
    while let Some((_addr, _out)) = it.next().await? {
      consumed += 1;
      if consumed == page0_end {
        break;
      }
    }
    assert_eq!(consumed, page0_end);

    // 不抬删段地板：begin 推进但段文件全部留存（取证钉死「逻辑截断、物理未删」面）
    let cut = addrs[cut_idx].0;
    hlog.shift_begin_address(cut).await?;
    assert_eq!(hlog.begin_address(), cut);
    assert!(
      device.start_segment() == 0 && device.get_file_size(0)? > 0,
      "装配失败：delete_floor 钳制未生效，段 0 被意外删除"
    );

    let mut rest = Vec::new();
    while let Some((addr, out)) = it.next().await? {
      rest.push((addr, out.key()?.to_vec()));
    }
    let stale: Vec<u64> = rest
      .iter()
      .filter(|(addr, _)| *addr < cut)
      .map(|(addr, _)| *addr)
      .collect();
    assert!(
      stale.is_empty(),
      "产出 begin 之下已截断陈旧记录 {stale:?}（删段延后窗口的越界读危害面）"
    );
    assert_eq!(
      rest,
      addrs[cut_idx..].to_vec(),
      "截断区必须被整体跃迁跳过，续扫产出恰为 [cut, tail)"
    );

    info!("删段延后窗口内扫描不产出截断陈旧记录测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
