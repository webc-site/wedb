//! 写/提交路径故障注入回归（对标 libs/storage/Tsavorite/cs/test/test.hlog/
//! LogCommitFailureTests.cs 与 test.recovery/RecoveryTests.cs::RecoveryTestFailOnSectorSize）
//!
//! C# 语义映射（whlog 无 TsavoriteLog fast-commit 提交协议，映射到等价的
//! 页刷盘提交点与恢复构造点）：
//! - CommitFailureExceptionCarriesDeviceError：提交（页刷盘）失败必须把设备级
//!   类型化错误原样送达调用方（C# 断言 InnerException 身份；Rust 断言
//!   Error::Device 内层错误变体与注入的 offset/len 身份），绝不坍缩为裸错误码；
//! - 失败不得毒化日志：tail 不动、内存数据可读、后续追加正常；
//! - 修复后重试成功并可经快照恢复（日志可继续写、恢复一致）；
//! - FastCommitRecoveryFailureFailsFastAndDoesNotPoisonLog Phase 2a：恢复期设备
//!   读故障必须构造期快停，绝不静默交出被清零毒化的空日志；
//! - CalculateReadOnlyAddressClampsOutOfRangeHeadAddress：head ≥ tail（含
//!   fast-commit 哨兵 long.MaxValue 的等价 u64::MAX）必须钳制到 tail，页算术
//!   绝不溢出为越界负值地址。

use std::{io::ErrorKind, sync::Arc};

use aok::{OK, Void};
use tempfile::tempdir;
use wdev::{self, Error as WdevError, SegmentedDevice};
use wepoch::LightEpoch;
use whlog::{
  AddressSnapshot, DEFAULT_INITIAL_ADDRESS, Error, HybridLog, HybridLogConfig, SECTOR_ALIGNMENT,
};

use super::support::{FaultDevice, MODE_FAIL, MODE_NORMAL, SectorShiftDevice, SyncThrowDevice};

/// 提交（页刷盘）写失败：设备级类型化错误原样可见、日志不毒化、修复后可继续写并恢复
///
/// libs/storage/Tsavorite/cs/test/test.hlog/LogCommitFailureTests.cs:
/// CommitFailureExceptionCarriesDeviceError（+ 恢复一致性收尾）
#[compio::test]
async fn commit_flush_failure_surfaces_device_error_and_recovers() -> Void {
  let dir = tempdir()?;
  let db_path = dir.path().join("commit_failure.db");
  let device = Arc::new(FaultDevice::new(SegmentedDevice::single_file(&db_path)?));
  let epoch = Arc::new(LightEpoch::new(16));
  let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 4, 0.5)?;
  let hlog = HybridLog::new(config.clone(), Arc::clone(&device), epoch)?;

  // 记录写入内存页（追加不触发设备写，对标 C# 注释「entries stay in the
  // current in-memory page」：失败由提交刷盘确定性触发）
  let (addr_a, _) = hlog.append(b"a", b"value-a", 0, false)?;
  let tail_before = hlog.tail_address();
  assert_eq!(
    hlog.flushed_until_address(),
    DEFAULT_INITIAL_ADDRESS,
    "未提交前不得有落盘推进"
  );

  // 武装写失败：提交刷盘失败，设备级类型化错误（ReadOnly + 注入 offset/len
  // 身份）必须原样抵达调用方——对标 C# InnerException 身份断言
  device.set_mode(MODE_FAIL);
  let err = hlog.flush_page(0).await.unwrap_err();
  assert!(
    matches!(
      err,
      Error::Device(WdevError::ReadOnly { offset: 0, len })
        if len >= SECTOR_ALIGNMENT,
    ),
    "提交失败必须携带设备级类型错误身份: {err:?}"
  );

  // 日志不毒化：tail 不动、内存数据完好、后续追加正常
  assert_eq!(hlog.tail_address(), tail_before, "失败的提交不得推进 tail");
  let out = hlog.read_record(addr_a).await?;
  assert_eq!(out.value()?, b"value-a", "失败后内存数据必须可读");
  let (addr_b, _) = hlog.append(b"b", b"value-b", addr_a, false)?;
  assert!(addr_b > addr_a, "失败后必须可继续追加");

  // 修复设备：重试提交成功，连续落盘前缀推进
  device.set_mode(MODE_NORMAL);
  hlog.flush_all().await?;
  hlog.sync().await?;
  assert_eq!(
    hlog.flushed_until_address(),
    hlog.tail_address(),
    "修复后提交必须追平 tail"
  );

  // 快照恢复：全部记录逐字节一致（对标 C# 修复后可恢复语义）
  let tail = hlog.tail_address();
  drop(hlog);
  let snapshot = AddressSnapshot::from_bounds(
    DEFAULT_INITIAL_ADDRESS,
    DEFAULT_INITIAL_ADDRESS,
    tail,
    tail,
    tail,
  );
  let recovered = HybridLog::recover(
    config,
    Arc::new(SegmentedDevice::single_file(&db_path)?),
    Arc::new(LightEpoch::new(16)),
    snapshot,
  )
  .await?;
  let out_a = recovered.read_record(addr_a).await?;
  assert_eq!(out_a.value()?, b"value-a");
  let out_b = recovered.read_record(addr_b).await?;
  assert_eq!(out_b.value()?, b"value-b");

  OK
}

/// 恢复期设备读故障必须构造期快停；设备修复后恢复成功且数据完好
///
/// libs/storage/Tsavorite/cs/test/test.hlog/LogCommitFailureTests.cs:
/// FastCommitRecoveryFailureFailsFastAndDoesNotPoisonLog Phase 2a
/// （C# TolerateDeviceFailure = false 默认臂；Phase 2b 的 CalculateReadOnlyAddress
/// 钳制语义由 clamp_read_only_address_for_poisoned_head 用例直接锁定）
#[compio::test]
async fn recovery_with_failing_device_fails_fast_and_stays_recoverable() -> Void {
  let dir = tempdir()?;
  let db_path = dir.path().join("recover_fail_fast.db");
  let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 4, 0.5)?;
  let addr_a;
  let tail;

  // 阶段 1：健康设备写入并全量落盘
  {
    let device = Arc::new(SyncThrowDevice::new(SegmentedDevice::single_file(
      &db_path,
    )?));
    let epoch = Arc::new(LightEpoch::new(16));
    let hlog = HybridLog::new(config.clone(), device, epoch)?;
    addr_a = hlog.append(b"k0", b"payload-0", 0, false)?.0;
    hlog.append(b"k1", b"payload-1", addr_a, false)?;
    hlog.flush_all().await?;
    hlog.sync().await?;
    tail = hlog.tail_address();
  }

  let snapshot = AddressSnapshot::from_bounds(
    DEFAULT_INITIAL_ADDRESS,
    DEFAULT_INITIAL_ADDRESS,
    tail,
    tail,
    tail,
  );

  // 阶段 2a：全读失败设备上的恢复必须快停报错（对标 C# 构造必须 throw），
  // 绝不静默返回被清零毒化的空日志
  {
    let failing = Arc::new(SyncThrowDevice::new(SegmentedDevice::single_file(
      &db_path,
    )?));
    failing.set_arm_read_failure(true);
    let result = HybridLog::recover(
      config.clone(),
      Arc::clone(&failing),
      Arc::new(LightEpoch::new(16)),
      snapshot,
    )
    .await
    .err()
    .expect("恢复期设备读故障必须快停报错");
    assert!(
      matches!(result, Error::Device(WdevError::Io(_))),
      "必须以 Device(Io) 快停: {result:?}"
    );
  }

  // 阶段 3：设备修复后同一快照恢复成功，数据逐字节一致（日志不毒化、可恢复）
  let recovered = HybridLog::recover(
    config,
    Arc::new(SyncThrowDevice::new(SegmentedDevice::single_file(
      &db_path,
    )?)),
    Arc::new(LightEpoch::new(16)),
    snapshot,
  )
  .await?;
  let out = recovered.read_record(addr_a).await?;
  assert_eq!(out.key()?, b"k0");
  assert_eq!(out.value()?, b"payload-0");
  assert_eq!(recovered.tail_address(), tail);

  OK
}

/// 扇区尺寸不匹配的恢复必须显式报错；兼容扇区变化恢复成功
///
/// libs/storage/Tsavorite/cs/test/test.recovery/RecoveryTests.cs:
/// RecoveryTestFailOnSectorSize（smallSector 设备几何切换语义）
#[compio::test]
async fn recovery_on_mismatched_sector_size_fails() -> Void {
  let dir = tempdir()?;
  let db_path = dir.path().join("sector_mismatch.db");
  let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 4, 0.5)?;
  let addr;
  let tail;

  // 按旧扇区几何写入并全量落盘
  {
    let device = Arc::new(SectorShiftDevice::new(SegmentedDevice::single_file(
      &db_path,
    )?));
    let epoch = Arc::new(LightEpoch::new(16));
    let hlog = HybridLog::new(config.clone(), device, epoch)?;
    addr = hlog.append(b"k", b"sector-data", 0, false)?.0;
    hlog.flush_all().await?;
    hlog.sync().await?;
    tail = hlog.tail_address();
  }

  let snapshot = AddressSnapshot::from_bounds(
    DEFAULT_INITIAL_ADDRESS,
    DEFAULT_INITIAL_ADDRESS,
    tail,
    tail,
    tail,
  );

  // 换到页容量非其整数倍的扇区几何（4096 页 % 8192 扇区 ≠ 0）→ 恢复必须报错
  {
    let shifted = Arc::new(SectorShiftDevice::new(SegmentedDevice::single_file(
      &db_path,
    )?));
    shifted.set_sector_size(2 * SECTOR_ALIGNMENT);
    let result = HybridLog::recover(
      config.clone(),
      Arc::clone(&shifted),
      Arc::new(LightEpoch::new(16)),
      snapshot,
    )
    .await
    .err()
    .expect("扇区尺寸不匹配的恢复必须显式报错");
    assert!(
      matches!(result, Error::Io(ref e) if e.kind() == ErrorKind::InvalidInput),
      "必须以 InvalidInput 显式报错: {result:?}"
    );
  }

  // 正向对照：兼容扇区几何（512，4096 页为其整数倍）恢复成功且数据一致
  let compatible = Arc::new(SectorShiftDevice::new(SegmentedDevice::single_file(
    &db_path,
  )?));
  compatible.set_sector_size(SECTOR_ALIGNMENT / 8);
  let recovered =
    HybridLog::recover(config, compatible, Arc::new(LightEpoch::new(16)), snapshot).await?;
  let out = recovered.read_record(addr).await?;
  assert_eq!(out.key()?, b"k");
  assert_eq!(out.value()?, b"sector-data");

  OK
}

/// 只读地址算术对越界 head 的钳制（对标
/// LogCommitFailureTests.cs:CalculateReadOnlyAddressClampsOutOfRangeHeadAddress）
///
/// head ≥ tail——含 fast-commit「永不驱逐」哨兵 long.MaxValue 的等价 u64::MAX——
/// 必须钳制到 tail，页算术绝不溢出为越界值（Rust 实现为 `head >= tail` 前置
/// 分支，结构性杜绝 C# 曾出现的负值只读地址冻结刷盘回归）；健康路径
/// （head < tail）不受过度钳制影响
#[test]
fn clamp_read_only_address_for_poisoned_head() -> Void {
  let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 16, 0.5)?;
  let page_size = SECTOR_ALIGNMENT as u64;

  for &tail in &[512u64, 8192, 1 << 30] {
    for &head in &[tail, tail + 1, u64::MAX] {
      let ro = config.calculate_read_only_address(head, tail);
      assert_eq!(ro, tail, "head {head:#x} ≥ tail {tail:#x} 必须钳制到 tail");
    }
  }

  // 健康路径 sanity：head 严格小于 tail 时结果必须落在 [head, tail] 且非负
  let normal_tail = 1 << 20;
  let ro = config.calculate_read_only_address(0, normal_tail);
  assert!(ro <= normal_tail, "健康路径只读地址不得越过 tail");
  let ro = config.calculate_read_only_address(page_size, normal_tail);
  assert!(
    ro >= page_size && ro <= normal_tail,
    "健康路径只读地址必须落在 [head, tail]: {ro}"
  );

  OK
}
