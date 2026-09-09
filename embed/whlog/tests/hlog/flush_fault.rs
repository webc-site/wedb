use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{Error, HybridLog, HybridLogConfig, RecordOutput, SECTOR_ALIGNMENT};

use super::support::{FaultDevice, MODE_FAIL, MODE_NORMAL, MODE_SHORT};

/// 测试 17: 环形缓冲区回绕驱逐（PageNotReady → 刷盘 + 推进 head → 重试成功 + 冷读）
#[test]
fn test_ring_wraparound_eviction() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_wrap.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    // 仅 2 页环形缓冲，迫使快速回绕
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 2, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    // 每条记录 16 + 2 + 3900 = 3918 字节
    let big = vec![b'A'; 3900];
    let addr_a = hlog.append(b"a", &big, 0, false)?;
    let addr_b = hlog.append(b"b", &big, 0, false)?;
    assert_eq!(hlog.config.page_id(addr_a), 0);
    assert_eq!(hlog.config.page_id(addr_b), 1);

    // 第 3 条将回绕复用页 0 槽位：旧页未落盘未驱逐 → PageNotReady
    let err = hlog.append(b"c", &big, 0, false).unwrap_err();
    assert!(
      matches!(err, Error::PageNotReady(2)),
      "回绕必须被拦截: {err:?}"
    );

    // 刷盘旧页并推进只读/驱逐边界
    hlog.flush_page(0).await?;
    hlog.shift_read_only_address(SECTOR_ALIGNMENT as u64);
    hlog.shift_head_address(SECTOR_ALIGNMENT as u64);
    while hlog.safe_head_address() < SECTOR_ALIGNMENT as u64 {
      hlog.epoch.bump_epoch();
    }

    let addr_c = hlog.append(b"c", &big, 0, false)?;
    assert_eq!(
      addr_c,
      2 * SECTOR_ALIGNMENT as u64,
      "重试后必须落在回绕页开头"
    );
    assert!(hlog.addresses.validate_invariants());

    // 被驱逐的旧记录 a 走磁盘冷读，页 1 记录 b 仍驻留内存
    let out_a = hlog.read_record(addr_a).await?;
    assert!(matches!(out_a, RecordOutput::Disk(_)));
    assert_eq!(out_a.key()?, b"a");
    assert!(hlog.is_in_memory(addr_b));

    info!("环形缓冲区回绕驱逐测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 28: 刷盘错误路径回归——短写不得推进 flushed_until；错误回填的陈旧区间
/// （已被持久化前缀覆盖）必须被钳制丢弃，不得吸收合并触碰已驱逐页造成永久 PageNotReady
///
/// 复现序列（2 页环形缓冲强制回绕驱逐）：
/// 1. 页 1 刷盘注入写失败 → 陈旧区间 [4096, 8192) 回填 pending_flush；
/// 2. 注入短写 → 必须报 FlushFailed 且 flushed_until 不动；
/// 3. 页 1 重试成功 → flushed_until = 8192，但重复的陈旧区间仍滞留队列；
/// 4. 换页复用页 1 槽位（页 1 驱逐出内存）；
/// 5. flush_page(0) 相邻吸收陈旧区间 → 钳制后为空，必须 Ok 丢弃（而非拷贝已驱逐页报错）；
/// 6. flush_page(2) 正常落盘，队列清空，刷盘通道不再卡死。
#[test]
fn test_flush_stale_range_and_short_write() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_fault_flush.db");
    let device = Arc::new(FaultDevice::new(SegmentedDevice::single_file(&db_path)?));
    let epoch = Arc::new(LightEpoch::new(16));

    // 2 页环形缓冲：记录 16 + 1 + 3900 = 3917 字节，每页恰好一条后触发换页
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 2, 0.5)?;
    let hlog = HybridLog::new(config, device.clone(), epoch)?;
    let big = vec![b'A'; 3900];

    let a = hlog.append(b"a", &big, 0, false)?;
    let b = hlog.append(b"b", &big, 0, false)?;
    assert_eq!(hlog.config.page_id(a), 0);
    assert_eq!(hlog.config.page_id(b), 1);

    // 页 0 正常落盘 → flushed_until = 4096，推进驱逐边界后换页复用页 0 槽位
    hlog.flush_page(0).await?;
    hlog.shift_read_only_address(SECTOR_ALIGNMENT as u64);
    hlog.shift_head_address(SECTOR_ALIGNMENT as u64);
    while hlog.safe_head_address() < SECTOR_ALIGNMENT as u64 {
      hlog.epoch.bump_epoch();
    }
    let c = hlog.append(b"c", &big, 0, false)?;
    assert_eq!(c, 2 * SECTOR_ALIGNMENT as u64);

    // 1. 注入写失败：页 1 刷盘失败 → 区间 [4096, 8192) 回填 pending_flush
    device.set_mode(MODE_FAIL);
    assert!(matches!(
      hlog.flush_page(1).await,
      Err(Error::Device(wdev::Error::ReadOnly { .. }))
    ));

    // 2. 注入短写：必须报 FlushFailed 且不得推进 flushed_until（崩溃一致性前缀承诺）
    device.set_mode(MODE_SHORT);
    assert!(matches!(
      hlog.flush_page(1).await,
      Err(Error::FlushFailed { .. })
    ));
    assert_eq!(hlog.flushed_until_address(), SECTOR_ALIGNMENT as u64);
    device.set_mode(MODE_NORMAL);

    // 3. 页 1 重试刷盘成功 → flushed_until = 8192（陈旧重复区间仍滞留队列）
    hlog.flush_page(1).await?;
    assert_eq!(hlog.flushed_until_address(), 2 * SECTOR_ALIGNMENT as u64);

    // 4. 换页复用页 1 槽位 → 页 1 驱逐出内存（此后拷贝页 1 必然 PageNotReady）
    hlog.shift_read_only_address(2 * SECTOR_ALIGNMENT as u64);
    hlog.shift_head_address(2 * SECTOR_ALIGNMENT as u64);
    while hlog.safe_head_address() < 2 * SECTOR_ALIGNMENT as u64 {
      hlog.epoch.bump_epoch();
    }
    let d = hlog.append(b"d", &big, 0, false)?;
    assert_eq!(d, 3 * SECTOR_ALIGNMENT as u64);
    assert!(!hlog.buffer.is_page_loaded(1), "页 1 必须已驱逐出内存");

    // 5. 相邻吸收陈旧区间 [4096, 8192)（已被 flushed 前缀完全覆盖）→ 钳制后为空，
    //    必须 Ok 丢弃而非触碰已驱逐页 1；队列中的毒区间就此清除
    hlog.flush_page(0).await?;

    // 6. 页 2 正常落盘，flushed_until 连续推进，刷盘通道无永久卡死
    hlog.flush_page(2).await?;
    assert_eq!(hlog.flushed_until_address(), 3 * SECTOR_ALIGNMENT as u64);
    assert!(hlog.pending_flush.is_empty(), "陈旧区间必须被钳制丢弃");
    assert!(hlog.addresses.validate_invariants());

    // 故障恢复后数据完好：驱逐页冷读 + 驻留页直读
    let out_a = hlog.read_record(a).await?;
    assert_eq!(out_a.key()?, b"a");
    let out_d = hlog.read_record(d).await?;
    assert_eq!(out_d.key()?, b"d");

    info!("刷盘陈旧区间钳制与短写防护测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
