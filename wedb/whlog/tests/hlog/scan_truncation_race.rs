//! 磁盘冷读截断竞态回归（票：zcode-r18-whlog）
//!
//! 契约对标 C# 纪元定序：删段动作包在 `AllocatorBase.cs:ShiftBeginAddress` 的
//! `epoch.BumpCurrentEpoch` 排空闭包内（AllocatorBase.cs:1699-1705 一带），页读挂
//! `ScanIteratorBase.BufferAndLoad` 的 `BumpCurrentEpoch` 延迟执行——删段严格
//! happens-after 全部在途受保护页读完成，并发截断至多让扫描器在下一轮 GetNext
//! 钳位跃迁到新 BeginAddress（SpanByteScanIterator.cs:135-136），绝不会因段文件
//! 被 unlink 而报设备错误中断。rust 侧磁盘冷读刻意免纪元守卫（免慢 I/O 滞留纪元
//! 推进），补偿手段为：循环头 begin 钳位 + [`whlog::HybridLog::scan_iter`] 冷读
//! 错误臂的截断竞态复核（`ScanIterator::cold_read_page`，对标同仓读路径范式
//! wkv/src/session/raw/read.rs:read_from_disk 的 `cur < begin_address` 复核）。
//!
//! 本测试以「读闸门设备 + 三段握手」把竞态窗口钉死在确定性交错上：扫描器的首个
//! 磁盘页读被扣停在「读发起后、段文件删除前」，主线程在窗口内走完整生产删段链
//! （紧缩挪线 `shift_begin_address` → 检查点发布 `release_history_until` 物理
//! unlink 段 0），放行后底层读以真实 [`wdev::Error::SegmentNotFound`] 收场：
//! - 修复后：错误臂复验 begin 已越过游标 → 钳位跃迁收敛续扫（绿），交付记录地址
//!   恒不低于钳位后 begin，且 `[begin, tail)` 存活记录一条不漏；
//! - 摘除错误臂复核复跑：错误经 `?` 原样上抛，整场扫描以 SegmentNotFound 中断
//!   （红，断言一必中）。

use std::{
  path::PathBuf,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
  },
  thread,
  time::Duration,
};

use aok::{Error as AokError, OK, Result, Void};
use compio::runtime::Runtime;
use parking_lot::{Condvar, Mutex};
use tempfile::tempdir;
use wbase::pool::{AlignedBuf, BufferPool};
use wdev::{Device, Result as DeviceResult, SegmentedDevice};
use wepoch::LightEpoch;
use whlog::{HybridLog, HybridLogConfig, SECTOR_ALIGNMENT};

/// 闸门握手预算：扫描线程异常退出时主线程不得挂死
const GATE_WINDOW_BUDGET: Duration = Duration::from_secs(10);

/// 截断竞态闸门设备（仅测试用）：武装后的首个设备读被扣停在「下发前」，
/// 主线程借该窗口完成 begin 挪线与物理删段，放行后底层读以真实设备错误收场
///
/// 读写之外全量透传 [`SegmentedDevice`]；读闸门只拦武装后第一读（swap 清旗），
/// 恰好命中扫描器的首个冷读页，其余 I/O（含主线程删段链）零拦截。
struct TruncationGateDevice {
  inner: SegmentedDevice,
  armed: AtomicBool,
  /// 扣停标志：true = 已扣停待放行（兼作扣停上报，`Device: Sync` 由 Mutex 承接）
  gate: Mutex<bool>,
  gate_cv: Condvar,
}

impl TruncationGateDevice {
  fn new(path: PathBuf) -> Result<Self> {
    Ok(Self {
      inner: SegmentedDevice::new(path, 2 * SECTOR_ALIGNMENT as u64, SECTOR_ALIGNMENT)?,
      armed: AtomicBool::new(false),
      gate: Mutex::new(false),
      gate_cv: Condvar::new(),
    })
  }

  /// 武装/解除闸门（武装后首个设备读被扣停）
  fn set_armed(&self, armed: bool) {
    self.armed.store(armed, Ordering::Release);
  }

  /// 读闸门：武装则置扣停上报并阻塞至主线程放行（仅首读生效）
  fn read_gate(&self) {
    if self.armed.swap(false, Ordering::AcqRel) {
      let mut parked = self.gate.lock();
      *parked = true;
      // 扣停即上报：唤醒等待中的主线程进入删段窗口
      self.gate_cv.notify_all();
      self.gate_cv.wait_while(&mut parked, |parked| *parked);
    }
  }

  /// 主线程等待闸门扣停上报（带预算，扫描线程异常退出时不挂死）：
  /// 谓词 `!*parked` —— 扣停未上报期间等待，上报（parked=true）即返
  fn wait_entered(&self) -> aok::Result<()> {
    let mut parked = self.gate.lock();
    if self
      .gate_cv
      .wait_while_for(
        &mut parked,
        |parked: &mut bool| !*parked,
        GATE_WINDOW_BUDGET,
      )
      .timed_out()
    {
      Err(AokError::msg(
        "闸门未扣停扫描冷读，竞态窗口未闭合，断言落空",
      ))
    } else {
      Ok(())
    }
  }

  /// 主线程放行扣停读
  fn release(&self) {
    let mut parked = self.gate.lock();
    *parked = false;
    self.gate_cv.notify_all();
  }
}

impl Device for TruncationGateDevice {
  #[inline]
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  #[inline]
  fn segment_size(&self) -> u64 {
    self.inner.segment_size()
  }

  #[inline]
  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }

  fn get_file_size(&self, segment_id: u32) -> DeviceResult<u64> {
    self.inner.get_file_size(segment_id)
  }

  #[inline]
  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }

  #[inline]
  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    self.read_gate();
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (DeviceResult<usize>, AlignedBuf) {
    self.read_gate();
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> DeviceResult<()> {
    self.inner.sync().await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> DeviceResult<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}

/// 竞态装配（仿 truncate_floor.rs）：分段设备（段大小 = 2 页，页 4096，48B 记录），
/// 追加 `n_records` 条跨多段记录，全量刷盘 + sync。闸门设备初始不拦截，
/// 装配完成后由测试体武装。
async fn setup_gate_fixture(
  device: Arc<TruncationGateDevice>,
  n_records: usize,
) -> Result<(Arc<HybridLog<TruncationGateDevice>>, Vec<(u64, Vec<u8>)>)> {
  let epoch = Arc::new(LightEpoch::new(32));
  let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 8, 0.5)?;
  let hlog = Arc::new(HybridLog::new(config, device, epoch)?);

  let mut addrs = Vec::with_capacity(n_records);
  for i in 0..n_records {
    let key = format!("k{i:04}").into_bytes();
    let (addr, _) = hlog.append(&key, &[b'v'; 24], 0, false)?;
    addrs.push((addr, key));
  }
  hlog.flush_all().await?;
  hlog.sync().await?;
  Ok((hlog, addrs))
}

/// 从装配地址表导出锚点：首条地址 ≥ 段 1 起点（8192）的记录下标——
/// 既是 begin 挪线目标，也是 release_history_until 的删段地板（段 0 据此被回收）
fn cut_anchor(addrs: &[(u64, Vec<u8>)]) -> usize {
  addrs
    .iter()
    .position(|(a, _)| *a >= 2 * SECTOR_ALIGNMENT as u64)
    .expect("装配失败：记录未跨入段 1")
}

/// 扫描器慢速推进中并发 release_history_until 越过扫描位置物理删段：
/// 扫描必须经钳位收敛完成而非 SegmentNotFound 中断，且交付记录地址恒不低于
/// 钳位后 begin（修复前错误经 `?` 上抛，整场扫描中断——摘臂复跑必红）
#[test]
fn scan_survives_concurrent_history_release_segment_removal() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(TruncationGateDevice::new(dir.path().join("scantrunc.db"))?);

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (hlog, addrs) = setup_gate_fixture(Arc::clone(&device), 256).await?;
    let cut_idx = cut_anchor(&addrs);
    let cut = addrs[cut_idx].0;
    let head_at = addrs[192].0;
    let tail = hlog.tail_address();
    assert!(head_at > cut, "装配失败：head 锚点未越过删段线");

    // 全量滑出内存：扫描全程走磁盘冷读分支（分支 1）
    hlog.shift_head_address(head_at);

    // 武装闸门并起扫描线程：迭代器按挪线前 begin 快照构造，首个磁盘读被扣停
    device.set_armed(true);
    let (constructed_tx, constructed_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel::<Result<Vec<(u64, Vec<u8>)>, whlog::Error>>();
    let hlog_s = Arc::clone(&hlog);
    let scanner = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        let mut it = hlog_s.scan_iter(0, tail);
        let _ = constructed_tx.send(());
        let mut scanned: Vec<(u64, Vec<u8>)> = Vec::new();
        let res = loop {
          match it
            .next_ref(|item| {
              scanned.push((item.addr, item.rec.key().to_vec()));
              Ok(())
            })
            .await
          {
            Ok(Some(())) => {}
            Ok(None) => break Ok(scanned),
            Err(e) => break Err(e),
          }
        };
        let _ = result_tx.send(res);
        aok::Result::<()>::Ok(())
      })
    });

    // 三段握手 1：迭代器已按挪线前快照构造（钳位口径的快照方向安全以序保证）
    if constructed_rx.recv().is_err() {
      return Err(AokError::msg("扫描线程已退出，握手断裂"));
    }
    // 三段握手 2：扫描器首个冷读正被扣停在「读发起后、删段前」——竞态窗口闭合
    device.wait_entered()?;

    // 窗口内走完整生产删段链：紧缩挪线（地板 0 钳制不删段）→
    // 检查点发布 release_history_until 抬升地板并物理 unlink 段 0
    hlog.shift_begin_address(cut).await?;
    hlog.release_history_until(cut).await?;
    assert!(
      device.start_segment() >= 1 && device.get_file_size(0)? == 0,
      "取证失败：段 0 未被物理删除，截断竞态前置条件不成立"
    );

    // 三段握手 3：放行扣停读——底层读此刻对已删段 0 发起，真实 SegmentNotFound
    device.release();
    let scanned = result_rx
      .recv()
      .map_err(|_| AokError::msg("扫描线程结果通道断裂"))?;
    scanner.join().expect("扫描线程不得 panic")?;

    // 断言一：扫描经钳位收敛完成，而非 SegmentNotFound 中断
    //（修复前形态：read_range 错误经 `?` 原样上抛，Err(Device(SegmentNotFound))）
    let scanned =
      scanned.map_err(|e| AokError::msg(format!("扫描被截断竞态中断（修复前形态复现）: {e:?}")))?;

    // 断言二：交付记录地址恒不低于钳位后 begin（陈旧交付臂禁绝）
    let stale: Vec<(u64, Vec<u8>)> = scanned.iter().filter(|(a, _)| *a < cut).cloned().collect();
    assert!(
      stale.is_empty(),
      "交付记录低于钳位后 begin={cut:#x}: {stale:?}"
    );

    // 断言三：钳位收敛后 [begin, tail) 存活记录一条不漏、次序键值一致
    let expect: Vec<(u64, Vec<u8>)> = addrs[cut_idx..].to_vec();
    assert_eq!(
      scanned, expect,
      "钳位收敛后扫描覆盖/内容与 [begin, tail) 存活记录不符"
    );
    aok::Result::<()>::Ok(())
  })?;

  OK
}
