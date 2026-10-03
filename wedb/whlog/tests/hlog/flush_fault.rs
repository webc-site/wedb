//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.hlog/LogCommitFailureTests.cs + FlakyDeviceTests.cs
use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::{self, Error as WdevError, SegmentedDevice};
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
    let (addr_a, _) = hlog.append(b"a", &big, 0, false)?;
    let (addr_b, _) = hlog.append(b"b", &big, 0, false)?;
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
      hlog.epoch.bump_current_epoch();
    }

    let (addr_c, _) = hlog.append(b"c", &big, 0, false)?;
    assert_eq!(
      addr_c,
      2 * SECTOR_ALIGNMENT as u64,
      "重试后必须落在回绕页开头"
    );

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
/// 必须被同起点相交的重试区间吸收收敛，不得触碰已驱逐页造成永久 PageNotReady
///
/// 复现序列（2 页环形缓冲强制回绕驱逐）：
/// 1. 页 1 刷盘注入写失败 → 区间 [4096, 8192) 回填 pending_flush；
/// 2. 注入短写 → 必须报 FlushFailed 且 flushed_until 不动；重试区间与回填条目
///    同起点相交吸收后重新回填，队列收敛为单条而非随重试次数累积（修复前此处
///    滞留两条同址条目，靠步骤 5 与条目 until 恰好页边界相邻才巧合清队）；
/// 3. 页 1 重试成功 → 重试区间本身即出队点，成功刷盘后队列即刻清空（真实不变式，
///    不再依赖页边界巧合）；
/// 4. 换页复用页 1 槽位（页 1 驱逐出内存）；
/// 5. flush_page(0) 区间整体低于 flushed 前缀 → 钳制后为空，必须 Ok 丢弃（独立兜底）；
/// 6. flush_page(2) 正常落盘，队列保持清空，刷盘通道不再卡死。
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

    let (a, _) = hlog.append(b"a", &big, 0, false)?;
    let (b, _) = hlog.append(b"b", &big, 0, false)?;
    assert_eq!(hlog.config.page_id(a), 0);
    assert_eq!(hlog.config.page_id(b), 1);

    // 页 0 正常落盘 → flushed_until = 4096，推进驱逐边界后换页复用页 0 槽位
    hlog.flush_page(0).await?;
    hlog.shift_read_only_address(SECTOR_ALIGNMENT as u64);
    hlog.shift_head_address(SECTOR_ALIGNMENT as u64);
    while hlog.safe_head_address() < SECTOR_ALIGNMENT as u64 {
      hlog.epoch.bump_current_epoch();
    }
    let (c, _) = hlog.append(b"c", &big, 0, false)?;
    assert_eq!(c, 2 * SECTOR_ALIGNMENT as u64);

    // 1. 注入写失败：页 1 刷盘失败 → 区间 [4096, 8192) 回填 pending_flush
    device.set_mode(MODE_FAIL);
    assert!(matches!(
      hlog.flush_page(1).await,
      Err(Error::Device(WdevError::ReadOnly { .. }))
    ));

    // 2. 注入短写：必须报 FlushFailed 且不得推进 flushed_until（崩溃一致性前缀承诺）
    device.set_mode(MODE_SHORT);
    assert!(matches!(
      hlog.flush_page(1).await,
      Err(Error::FlushFailed { .. })
    ));
    assert_eq!(hlog.flushed_until_address(), SECTOR_ALIGNMENT as u64);
    // 双失败后队列恒为单条：第二次重试先相交吸收既有条目、失败再回填一条，
    // 不随重试次数累积（修复前同起点重叠恒不吸收，此处滞留两条 [4096, 8192)）
    assert_eq!(
      hlog.pending_flush.len(),
      1,
      "持续失败下回填条目必须被重试区间吸收-重排收敛为单条"
    );
    device.set_mode(MODE_NORMAL);

    // 3. 页 1 重试刷盘成功 → 重试区间相交吸收回填条目，成功即出队，flushed_until = 8192
    //    （修复前两条陈旧区间滞留队列，此断言必红）
    hlog.flush_page(1).await?;
    assert_eq!(hlog.flushed_until_address(), 2 * SECTOR_ALIGNMENT as u64);
    assert_eq!(
      hlog.pending_flush.len(),
      0,
      "成功重试必须当场吸收回填条目出队，不得留待页边界巧合"
    );

    // 4. 换页复用页 1 槽位 → 页 1 驱逐出内存（此后拷贝页 1 必然 PageNotReady）
    hlog.shift_read_only_address(2 * SECTOR_ALIGNMENT as u64);
    hlog.shift_head_address(2 * SECTOR_ALIGNMENT as u64);
    while hlog.safe_head_address() < 2 * SECTOR_ALIGNMENT as u64 {
      hlog.epoch.bump_current_epoch();
    }
    let (d, _) = hlog.append(b"d", &big, 0, false)?;
    assert_eq!(d, 3 * SECTOR_ALIGNMENT as u64);
    assert!(!hlog.buffer.is_page_loaded(1), "页 1 必须已驱逐出内存");

    // 5. 页 0 已整体低于 flushed 前缀 → 重刷区间钳制后为空，必须 Ok 丢弃
    //    而非触碰已驱逐页 1（钳制丢弃兜底与吸收出队机制解耦，独立验证）
    hlog.flush_page(0).await?;

    // 6. 页 2 正常落盘，flushed_until 连续推进，刷盘通道无永久卡死
    hlog.flush_page(2).await?;
    assert_eq!(hlog.flushed_until_address(), 3 * SECTOR_ALIGNMENT as u64);
    assert!(hlog.pending_flush.is_empty(), "陈旧区间必须被钳制丢弃");

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

/// 测试 28b: 群提交形态——失败回填条目与重试区间同起点「重叠」而非相邻，
/// 恢复后的首次成功刷盘必须单次吸收全部残留并清零队列
///
/// 群提交 FlushStep::step（wkv store/flush.rs）的重试区间起点恒为
/// page_start(page_id(flushed))，与 requeue 钳制回填条目同 from 重叠；严格相邻
/// 判据恒不命中，恢复后 flushed 一步越过条目 until，此后任何新区间起点
/// 满足 >= flushed > 条目 until，残留永久不可达。本用例不经测试 28 的页边界巧合，
/// 直接以 flush_all 使 flushed 越过失败期条目 until：
/// 1. 连续两次 MODE_FAIL 刷页 1 → 回填条目被吸收-重排收敛为单条；
/// 2. MODE_NORMAL 恢复后追加页 2 新记录并 flush_all，重试区间 [4096, 12288)
///    与条目 [4096, 8192) 相交吸收，落盘成功、flushed 直达 tail（越过条目 until）；
/// 3. pending_flush 全空、零残留（修复前条目永不被吸收，此断言必红）。
#[test]
fn test_flush_requeue_overlap_absorbed_on_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_fault_requeue.db");
    let device = Arc::new(FaultDevice::new(SegmentedDevice::single_file(&db_path)?));
    let epoch = Arc::new(LightEpoch::new(16));

    // 2 页环形缓冲，同测试 28：换页驱逐页 0 后新记录落页 2
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 2, 0.5)?;
    let hlog = HybridLog::new(config, device.clone(), epoch)?;
    let big = vec![b'A'; 3900];

    let (a, _) = hlog.append(b"a", &big, 0, false)?;
    let (b, _) = hlog.append(b"b", &big, 0, false)?;
    assert_eq!(hlog.config.page_id(a), 0);
    assert_eq!(hlog.config.page_id(b), 1);

    hlog.flush_page(0).await?;
    hlog.shift_read_only_address(SECTOR_ALIGNMENT as u64);
    hlog.shift_head_address(SECTOR_ALIGNMENT as u64);
    while hlog.safe_head_address() < SECTOR_ALIGNMENT as u64 {
      hlog.epoch.bump_current_epoch();
    }
    let (c, _) = hlog.append(b"c", &big, 0, false)?;
    assert_eq!(c, 2 * SECTOR_ALIGNMENT as u64);

    // 1. 连续两次写失败：回填条目与重试区间同起点重叠，队列收敛为单条不累积
    device.set_mode(MODE_FAIL);
    assert!(matches!(
      hlog.flush_page(1).await,
      Err(Error::Device(WdevError::ReadOnly { .. }))
    ));
    assert!(matches!(
      hlog.flush_page(1).await,
      Err(Error::Device(WdevError::ReadOnly { .. }))
    ));
    assert_eq!(hlog.flushed_until_address(), SECTOR_ALIGNMENT as u64);
    assert_eq!(
      hlog.pending_flush.len(),
      1,
      "双失败后条目必须吸收-重排为单条"
    );
    device.set_mode(MODE_NORMAL);

    // 2. 恢复后 flush_all：重试区间 [4096, 12288) 起点与条目同址相交（非页边界
    //    相邻巧合），单次 coalesce 全部吸收并连续写落盘，flushed 直达 tail、越过条目 until
    let tail = hlog.tail_address();
    let flushed = hlog.flush_all().await?;
    assert_eq!(flushed, tail);
    assert!(flushed > 2 * SECTOR_ALIGNMENT as u64);

    // 3. 恢复后零残留：修复前条目 [4096, 8192) 永不被吸收，flushed 越过其 until
    //    后永久不可达，此断言必红
    assert!(
      hlog.pending_flush.is_empty(),
      "恢复刷盘必须当场吸收全部回填条目，零残留"
    );

    // 落盘数据完好可回读
    let out_b = hlog.read_record(b).await?;
    assert_eq!(out_b.key()?, b"b");
    let out_c = hlog.read_record(c).await?;
    assert_eq!(out_c.key()?, b"c");

    info!("失败回填重叠区间恢复吸收收敛测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
