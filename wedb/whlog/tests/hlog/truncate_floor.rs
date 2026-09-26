//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:TruncateUntil（删段地板）
use std::{
  sync::{Arc, mpsc},
  thread,
};

use aok::{Error as AokError, OK, Result, Void};
use compio::runtime::Runtime;
use log::info;
use parking_lot::{Condvar, Mutex};
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{Error, HybridLog, HybridLogConfig, SECTOR_ALIGNMENT};

/// 内核级 truncate 删段地板测试共享装配：分段设备（段大小 = 2 页）+ 4096B 扇区页，
/// 追加 n 条记录跨越多段，全量刷盘 + sync，返回 (hlog, 设备句柄, 地址表)。
/// 段几何（页 4096 × 段 8192 = 2 页/段，48B 记录）：段 0 容纳页 0/1，首条地址
/// ≥ 8192 的记录起落段 1——删段地板钳制与补收的取证面全在段 0 的存与删
async fn setup_truncate_floor_fixture(
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

/// 从装配地址表导出锚点：`cut_idx` 为首条地址 ≥ 段 1 起点（8192）的记录下标——
/// 既是 shift_begin 的逻辑推进线，也是检查点发布后的删段地板取证线
fn cut_anchor(addrs: &[(u64, Vec<u8>)]) -> usize {
  addrs
    .iter()
    .position(|(a, _)| *a >= 2 * SECTOR_ALIGNMENT as u64)
    .expect("装配失败：记录未跨入段 1")
}

/// 测试: 未发布检查点（delete_floor 为 0）时内核 truncate 全禁删——
/// 逻辑 begin 已推进（shift_begin_address 受地板钳制只推线不删段）的前提下调用
/// truncate()，段文件必须全部留存：活动检查点重放窗 [begin, tail) 依赖的历史段
/// 绝不允许在检查点发布前被物理拆毁（旧旁路实现直接 truncate_until_address(begin)
/// 此处必删段 0，重启恢复即损坏）
#[test]
fn test_truncate_delete_floor_zero_forbids_segment_removal() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (hlog, device, addrs) = setup_truncate_floor_fixture(&dir, "floor0.db", 256).await?;
    let cut_idx = cut_anchor(&addrs);
    let head_at = addrs[192].0;

    // 前半区划入磁盘区，begin 逻辑推进至段 1（地板 0：全程无物理删段）
    hlog.shift_head_address(head_at);
    assert_eq!(hlog.addresses.delete_floor(), 0, "装配失败：地板非零");
    hlog.shift_begin_address(addrs[cut_idx].0).await?;
    assert_eq!(hlog.begin_address(), addrs[cut_idx].0);
    assert!(
      device.start_segment() == 0 && device.get_file_size(0)? > 0,
      "装配失败：shift_begin 阶段段 0 已被意外删除"
    );

    // 内核 truncate：地板 0 钳制 → 不删任何段
    hlog.truncate().await?;

    assert_eq!(
      hlog.begin_address(),
      addrs[cut_idx].0,
      "truncate 不得挪动逻辑 begin"
    );
    assert!(
      device.start_segment() == 0 && device.get_file_size(0)? > 0,
      "未发布检查点时 truncate 删了段文件（删段地板钳制失效，恢复重放窗被拆毁）"
    );

    info!("未发布检查点 truncate 全禁删测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试: 检查点发布抬升 delete_floor 后内核 truncate 补收窗下历史段——
/// 物理删段恰好回收至地板（段 0 被物理 unlink），地板之上的段与记录完好可读，
/// 逻辑 begin 不被挪动
#[test]
fn test_truncate_reclaims_up_to_raised_delete_floor() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (hlog, device, addrs) = setup_truncate_floor_fixture(&dir, "floorup.db", 256).await?;
    let cut_idx = cut_anchor(&addrs);
    let cut = addrs[cut_idx].0;
    let head_at = addrs[192].0;
    let addr_kept = addrs[cut_idx + 1].0;
    let key_kept = addrs[cut_idx + 1].1.clone();

    hlog.shift_head_address(head_at);
    hlog.shift_begin_address(cut).await?;
    // 模拟检查点发布：抬升删段地板至检查点重放窗下界
    hlog.raise_delete_floor(cut);
    assert_eq!(hlog.addresses.delete_floor(), cut, "装配失败：地板未抬升");

    hlog.truncate().await?;

    assert_eq!(hlog.begin_address(), cut, "truncate 不得挪动逻辑 begin");
    assert!(
      device.start_segment() >= 1 && device.get_file_size(0)? == 0,
      "取证失败：地板已抬升但窗下段 0 未被物理删除，truncate 补收未生效"
    );

    // 地板之上记录完好可冷读，窗下记录被干净拒绝（绝无设备级撕裂错误）
    let out = hlog.read_disk_record(addr_kept).await?;
    assert_eq!(out.key()?, key_kept.as_slice(), "地板之上记录被误删或撕裂");
    match hlog.read_disk_record(addrs[cut_idx / 2].0).await {
      Err(Error::PageNotReady(_)) => {}
      other => panic!("窗下冷读应干净返回 PageNotReady，实际: {other:?}"),
    }

    info!("检查点发布后 truncate 补收至地板测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试: 内核 truncate 度过安全纪元排空屏障（确定性交错）——读者守卫先于
/// truncate 全部动作用入场，屏障强制物理删段 happens-after 读者守卫退出；
/// 事件日志断言「守卫退出」不得晚于「截断完成」（无屏障的旧旁路实现
/// Done 先至即失败），且删段真实落地
#[test]
fn test_truncate_waits_inflight_disk_reader_drain() -> Void {
  #[derive(Debug, PartialEq)]
  enum Event {
    Dropped,
    Done,
  }

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (hlog, device, addrs) = setup_truncate_floor_fixture(&dir, "drain.db", 256).await?;
    let cut_idx = cut_anchor(&addrs);
    let cut = addrs[cut_idx].0;
    let head_at = addrs[192].0;

    // begin 逻辑推进至段 1（地板 0 不删段），随后模拟检查点发布抬地板；
    // 读者锚点取段 1 内首条记录（≥ begin、< head，冷读全程可采）
    hlog.shift_head_address(head_at);
    hlog.shift_begin_address(cut).await?;
    hlog.raise_delete_floor(cut);
    let addr_r = addrs[cut_idx + 1].0;
    let key_r = addrs[cut_idx + 1].1.clone();

    // 三段握手把读者采样钉在 truncate 入场之前；事件经 Mutex+Condvar 记录
    // （mpsc 多生产者不保证跨 Sender 全局 FIFO，无法用于保序断言）
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (sampled_tx, sampled_rx) = mpsc::channel::<()>();
    let log: Arc<(Mutex<Vec<Event>>, Condvar)> = Arc::new((Mutex::new(Vec::new()), Condvar::new()));

    let hlog_r = Arc::clone(&hlog);
    let epoch_r = Arc::clone(&hlog.epoch);
    let log_r = Arc::clone(&log);
    let reader = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        {
          let _guard = epoch_r.protected_scope();
          let _ = ready_tx.send(());
          if go_rx.recv().is_err() {
            return Err(AokError::msg("shifter 已退出，握手断裂"));
          }
          // 守卫持有期间采样磁盘区（截断线下方仍可读的存活区）
          if !hlog_r.is_on_disk(addr_r) {
            return Err(AokError::msg(format!(
              "采样点必须在磁盘区: addr_r={addr_r:#x} snapshot={:?}",
              hlog_r.addresses.snapshot()
            )));
          }
          let _ = sampled_tx.send(());
          // 持守卫连续冷读拉长在途 I/O 窗口，键必须完整一致
          for i in 0..64 {
            let out = hlog_r
              .read_disk_record(addr_r)
              .await
              .map_err(|e| AokError::msg(format!("第{i}次冷读失败（设备级撕裂）: {e}")))?;
            if out.key()? != key_r.as_slice() {
              return Err(AokError::msg(format!("第{i}次冷读撕裂: 键不匹配")));
            }
          }
          // Dropped 须在守卫存活期内记录：屏障的释放触发点就是守卫 drop 本身
          log_r.0.lock().push(Event::Dropped);
          log_r.1.notify_all();
        }
        Ok(())
      })
    });

    let hlog_s = Arc::clone(&hlog);
    let log_s = Arc::clone(&log);
    let shifter = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        if ready_rx.recv().is_err() {
          return Err(AokError::msg("reader 已退出，握手断裂"));
        }
        let _ = go_tx.send(());
        if sampled_rx.recv().is_err() {
          return Err(AokError::msg("reader 采样失败，握手断裂"));
        }
        // 屏障必须等读者守卫退出（封口 action 排空）后方可物理删段
        hlog_s.truncate().await?;
        log_s.0.lock().push(Event::Done);
        log_s.1.notify_all();
        Ok(())
      })
    });

    let reader_res = reader.join().unwrap();
    shifter.join().unwrap()?;

    // 保序断言：Dropped（守卫退出）必须先于 Done（截断完成）——
    // 无排空屏障的实现 Done 先至即失败
    {
      let (m, cv) = &*log;
      let mut events = m.lock();
      cv.wait_while(&mut events, |v| v.len() < 2);
      assert_eq!(
        events.as_slice(),
        [Event::Dropped, Event::Done],
        "排空屏障失效：物理删段先于读者守卫退出完成"
      );
    }
    reader_res?;

    // 后置状态：begin 未挪动，删段真实落地（段 0 被回收），读者锚点记录完好
    assert_eq!(hlog.begin_address(), cut);
    assert!(
      device.start_segment() >= 1 && device.get_file_size(0)? == 0,
      "取证失败：截断完成后段 0 未被物理删除"
    );

    info!("内核 truncate 纪元排空屏障测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
