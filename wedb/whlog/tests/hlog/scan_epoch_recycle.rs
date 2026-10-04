//! 只读区扫描与并发页回收竞速回归（票：wcompact-scan-missing-epoch-page-recycle-lost-records）
//!
//! 契约对标 C# `TsavoriteLogScanIterator.cs:239-296` GetNext 的 `epoch.Resume()` /
//! `epoch.Suspend()` 分段：扫描器消费内存驻留区（`PageBytes::Raw` 无锁直读）期间必须
//! 处于 `LightEpoch` 保护下——页槽位回收以 `safe_head` 经纪元排空越过旧页为门槛，
//! 持守卫期间回收无法完成。修复前 wcompact 紧缩主循环经 `ScanCursor::pull` 全程未持
//! 纪元，并发驱逐回绕 memset 可直接命中正在消费的页内存，造成跳页漏迁与键址错配。
//!
//! 本测试以双线程三段握手把回收驱动点钉死在扫描窗口内部（消费闭包 `f` 执行期）：
//! 扫描线程交付页首大记录的瞬间，回收驱动线程被唤醒并全力驱动「封只读线 → 刷盘 →
//! 推 head → 纪元排空推 safe_head → 环形回绕接管该页槽位」；
//! - 修复后：驱动线程的排空被扫描线程的纪元守卫阻隔，窗口内回收必然以超时收场（绿），
//!   窗口外照常完成（滑出内存的存活记录经磁盘冷读自愈续扫）；
//! - 摘除 `next_ref` 纪元守卫臂复跑：驱动线程在窗口内即完成槽位接管清零，
//!   大记录键值字节在消费窗口内被覆写为回绕新页记录/零洞（红，断言两处必中其一）。

use std::{
  sync::{
    Arc,
    mpsc::{self, Sender},
  },
  thread,
  time::{Duration, Instant},
};

use aok::Void;
use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{Error, HybridLog, HybridLogConfig, SECTOR_ALIGNMENT};

/// 环形缓冲页数（与页大小共同构成回绕回收窗口）
const NUM_PAGES: u64 = 8;
/// 大记录值体长度（页首独占大段，值体全 `0xA5` 模式，窗口内被覆写即可检）
const BIG_VAL_LEN: usize = 320;
/// 小记录值体
const SMALL_VAL: [u8; 24] = [0x5c; 24];
/// 窗口内回收握手等待上限：修复后驱动线程被守卫阻隔，本窗口内必超时
const WINDOW_ACK_TIMEOUT: Duration = Duration::from_secs(2);
/// 驱动线程总预算：修复后须等扫描线程全部守卫退出才能完成回绕接管
const WRITER_BUDGET: Duration = Duration::from_secs(20);

/// 回收驱动线程：全力把扫描线程正在消费的 `big_page` 槽位驱逐、回绕接管
///
/// 驱动链严格走生产回收口径（对标 wkv session 的 PageNotReady 驱逐重试）：
/// 封只读线并等排空 → `flush_all` 落盘 → `shift_head_address` 推进（登记 safe_head
/// 延迟动作）→ `bump_current_epoch` + `drain` 收割 → 追加至回绕页首（`ensure_page_ready`
/// 门槛达标后整槽 memset 并接管）。
/// 完成回绕接管即回 `true`；预算耗尽回 `false`。
fn drive_recycle(
  hlog: Arc<HybridLog<SegmentedDevice>>,
  epoch: Arc<LightEpoch>,
  big_page: u64,
  ack: Sender<bool>,
) {
  let recycled_page = big_page + NUM_PAGES;
  let drive = async {
    let start = Instant::now();
    let mut filler = 0u64;
    loop {
      match hlog.append(format!("wr{filler:05}").as_bytes(), &[0x77u8; 24], 0, false) {
        Ok((addr, _)) => {
          filler += 1;
          if hlog.config.page_id(addr) == recycled_page {
            // 回绕页首记录已接管 big_page 槽位：整槽 memset 与覆盖落笔完成
            let _ = ack.send(true);
            return;
          }
        }
        Err(Error::PageNotReady(_)) => {
          // 驱逐驱动三段：封印并排空只读线 → 落盘 → 推 head 并排空 safe_head
          let tail = hlog.addresses.tail();
          hlog.shift_read_only_address(tail);
          while hlog.safe_read_only_address() < tail && start.elapsed() < WRITER_BUDGET {
            epoch.bump_current_epoch();
            epoch.drain();
            thread::sleep(Duration::from_millis(2));
          }
          if hlog.flush_all().await.is_err() {
            let _ = ack.send(false);
            return;
          }
          let flushed = hlog.addresses.flushed_until();
          hlog.shift_head_address(flushed);
          epoch.bump_current_epoch();
          epoch.drain();
          if start.elapsed() >= WRITER_BUDGET {
            let _ = ack.send(false);
            return;
          }
          thread::sleep(Duration::from_millis(1));
        }
        Err(e) => {
          eprintln!("回收驱动线程追加失败: {e:?}");
          let _ = ack.send(false);
          return;
        }
      }
    }
  };
  match Runtime::new() {
    Ok(rt) => rt.block_on(drive),
    Err(e) => {
      eprintln!("回收驱动线程建运行时失败: {e:?}");
      let _ = ack.send(false);
    }
  }
}

/// 只读区无锁直读窗口内页槽位不得被并发回收清零/复用（纪元分段保护契约）
///
/// 断言（摘除修复臂复跑必红、修复在场必绿）：
/// 1. 大记录消费窗口内回收驱动必以超时收场（守卫阻隔 `safe_head` 排空越页）；
/// 2. 交付时同步拷贝的大记录键值字节恒保持原模式（槽位未被 memset/复用覆写）；
/// 3. 页 P 全部存活记录（大记录 + 同页小记录）在整轮扫描中一条不漏、内容完好
///    （窗口外回收照常，滑出内存者经磁盘冷读自愈续扫，绝无跳页漏迁）。
#[test]
fn scan_window_excludes_concurrent_page_recycle() -> Void {
  let dir = tempdir()?;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("epoch_recycle.db"),
    )?);
    let epoch = Arc::new(LightEpoch::new(32));
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, NUM_PAGES as usize, 0.5)?;
    let hlog = Arc::new(HybridLog::new(
      config,
      Arc::clone(&device),
      Arc::clone(&epoch),
    )?);
    let page_size = hlog.config.page_size as u64;

    // ---- 布局：把页首大记录钉在某个非首逻辑页的页首上 ----
    while hlog.addresses.tail() % page_size != 0 {
      hlog.append(b"prepad01", &SMALL_VAL, 0, false)?;
    }
    let big_page = hlog.config.page_id(hlog.addresses.tail());
    assert!(big_page >= 1, "大记录须落在非首逻辑页以规避保留区");
    let big_key = b"bigkey00"[..].to_vec();
    let big_val = vec![0xA5u8; BIG_VAL_LEN];
    let (big_addr, _) = hlog.append(&big_key, &big_val, 0, false)?;
    assert_eq!(big_addr % page_size, 0, "大记录未落在页首");

    // 同页存活小记录若干（最后一条恰越页即停，越页那条不计入本盘）
    let mut survivors: Vec<(u64, Vec<u8>, Vec<u8>)> = Vec::new();
    let mut idx = 0u64;
    loop {
      let key = format!("surv{idx:03}").into_bytes();
      let (addr, _) = hlog.append(&key, &SMALL_VAL, 0, false)?;
      if hlog.config.page_id(addr) != big_page {
        break;
      }
      survivors.push((addr, key, SMALL_VAL.to_vec()));
      idx += 1;
    }
    assert!(
      !survivors.is_empty(),
      "页内须留有同页存活记录以检验跳页漏迁面"
    );
    let scan_end = hlog.config.page_start_address(big_page + 1);

    // ---- 刷盘 + 纪元排空把 safe_read_only 封过整页 P（构造无锁直读前提）----
    hlog.flush_all().await?;
    hlog.sync().await?;
    hlog.shift_read_only_address(scan_end);
    while hlog.safe_read_only_address() < scan_end {
      epoch.bump_current_epoch();
      epoch.drain();
      thread::yield_now();
    }

    // ---- 扫描：大记录交付闭包即回收竞速窗口 ----
    let (ack_tx, ack_rx) = mpsc::channel::<bool>();
    let mut writer: Option<thread::JoinHandle<()>> = None;
    let mut recycled_in_window = false;
    let mut big_key_in_window: Option<Vec<u8>> = None;
    let mut big_val_in_window: Option<Vec<u8>> = None;
    let mut scanned: Vec<(u64, Vec<u8>, Vec<u8>)> = Vec::new();

    let mut it = hlog.scan_iter(big_addr, scan_end);
    loop {
      let out = it
        .next_ref(|item| {
          if item.addr == big_addr {
            // 三段握手第 1 段：窗口内唤醒驱动线程（修复臂在场时本线程正处于纪元守卫）
            let hlog_w = Arc::clone(&hlog);
            let epoch_w = Arc::clone(&epoch);
            let ack_w = ack_tx.clone();
            writer = Some(thread::spawn(move || {
              drive_recycle(hlog_w, epoch_w, big_page, ack_w);
            }));
            // 第 2 段：窗口内等待回收结果——修复后守卫阻隔排空，必超时；
            // 摘臂复跑时驱动线程即时接管槽位，必收到 true
            recycled_in_window = matches!(ack_rx.recv_timeout(WINDOW_ACK_TIMEOUT), Ok(true));
            // 第 3 段：当场拷贝逻辑键值——窗口内若遭 memset/复用覆写，模式即失配
            big_key_in_window = Some(item.rec.key.to_vec());
            big_val_in_window = Some(item.rec.value.to_vec());
          }
          scanned.push((item.addr, item.rec.key.to_vec(), item.rec.value.to_vec()));
          Ok(())
        })
        .await?;
      if out.is_none() {
        break;
      }
    }

    let handle = writer.take().expect("回收驱动线程须在窗口内启动");
    handle.join().expect("回收驱动线程不得 panic");

    // ---- 断言组 ----
    assert!(
      !recycled_in_window,
      "红证：纪元守卫缺失，窗口内页槽位已被并发回收接管清零（safe_head 排空未被扫描读者阻隔）"
    );
    assert_eq!(
      big_key_in_window.as_deref(),
      Some(big_key.as_slice()),
      "红证：大记录键字节在消费窗口内被回绕新页记录/零洞覆写"
    );
    assert_eq!(
      big_val_in_window.as_deref(),
      Some(big_val.as_slice()),
      "红证：大记录值字节在消费窗口内被回绕新页记录/零洞覆写"
    );

    // 整轮覆盖：大记录 + 全部同页存活记录一条不漏、键值完好
    let mut expect = vec![(big_addr, big_key, big_val)];
    expect.extend(survivors);
    assert_eq!(
      scanned.len(),
      expect.len(),
      "扫描覆盖记录数与页内存活记录链不一致（跳页漏迁面）：{scanned:?}"
    );
    for (got, exp) in scanned.iter().zip(expect.iter()) {
      assert_eq!(got, exp, "存活记录内容/次序失配（键址错配或污染面）");
    }
    Ok(())
  })
}
