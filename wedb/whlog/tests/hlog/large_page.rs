//! 大页 / 大记录端到端回归（对标 C# Garnet 大值通道：ServerOptions.cs:46
//! PageSize = "16m" 默认页 + KVSettings.cs:159 DefaultMaxInlineValueSize = 1MB）
//!
//! C# 语义：值 ≤ MaxInlineValueSize（默认 min(1MB, 页容量/2)）内联存储，读回逐字节
//! 一致；whlog 无 overflow 通道，单条记录必须整体落进单页，故页容量直接决定
//! 内联上限。本组用例在 16MB 生产页容量下锁定：
//! - 1MB 基线大值与接近整页的极大记录：内存直读 / 刷盘驱逐后冷读 / 恢复重载三条
//!   路径逐字节一致；
//! - 超页记录仍被 RecordTooLarge 拦截且不推进 tail；
//! - 大页位面算术（page_bits = 24）下跨页、Pad 填充与扫描语义不回归。

use std::sync::Arc;

use aok::{OK, Void};
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{
  AddressSnapshot, DEFAULT_INITIAL_ADDRESS, DEFAULT_SERVER_PAGE_SIZE, Error, HybridLog,
  HybridLogConfig, RecordOutput,
};

/// 16MB 生产页 + 4 页环形缓冲（单实例常驻 64MB，测试可承受）
fn large_page_config() -> HybridLogConfig {
  HybridLogConfig::new(DEFAULT_SERVER_PAGE_SIZE, 4, 0.5).expect("大页配置合法")
}

/// 大值逐字节校验（长度 + 内容指纹双断言，避免 1MB 全量比较开销）
fn assert_value_eq(actual: &[u8], expected: &[u8]) {
  assert_eq!(actual.len(), expected.len(), "大值长度必须一致");
  assert_eq!(actual, expected, "大值内容必须逐字节一致");
}

/// 16MB 页下内联 1MB 基线大值：内存直读与驱逐冷读逐字节一致
///
/// 对标 C# KVSettings.MaxInlineValueSize = min(1MB, PageSize/2) 的内联读写语义
#[compio::test]
async fn large_page_inline_one_mib_value_roundtrip() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("large_page.db"),
  )?);
  let epoch = Arc::new(LightEpoch::new(16));
  let hlog = HybridLog::new(large_page_config(), device, epoch)?;

  // 1MB 基线大值（内容含位置指纹，任何错位/截断都会被逐字节比对击穿）
  let mut big = vec![0u8; 1024 * 1024];
  for (i, b) in big.iter_mut().enumerate() {
    *b = (i % 251) as u8;
  }
  let (addr_big, _) = hlog.append(b"big:1mib", &big, 0, false)?;
  // 记录整体落进页 0：16 + 8 + 1MB << 16MB 页容量
  assert_eq!(hlog.config.page_id(addr_big), 0, "1MB 大值必须完整落在页 0");
  assert!(hlog.is_in_memory(addr_big));

  // 内存直读
  let out_mem = hlog.read_record(addr_big).await?;
  assert_value_eq(out_mem.value()?, &big);

  // 同页再放一条小记录（大值不破坏同页其他记录的寻址）
  let (addr_small, _) = hlog.append(b"s", b"tiny", addr_big, false)?;
  let out_small = hlog.read_record(addr_small).await?;
  assert_eq!(out_small.value()?, b"tiny");

  // 刷盘 + 驱逐第 0 页，走磁盘冷读路径
  hlog.flush_page(0).await?;
  hlog.sync().await?;
  hlog.shift_read_only_address(hlog.tail_address());
  hlog.shift_head_address(hlog.tail_address());
  assert!(hlog.is_on_disk(addr_big), "大值记录必须已驱逐到磁盘");

  let out_cold = hlog.read_record(addr_big).await?;
  assert!(matches!(out_cold, RecordOutput::Disk(_)));
  assert_value_eq(out_cold.value()?, &big);
  assert_eq!(out_cold.key()?, b"big:1mib");

  OK
}

/// 16MB 页大值经恢复路径重载：检查点快照恢复后逐字节一致并可无缝续写
///
/// 对标 C# 检查点/重启恢复后大值可读语义（RecoveryTests 的 Populate/Read 面向
/// 大值负载的等价收窄）
#[compio::test]
async fn large_page_value_survives_recovery() -> Void {
  let dir = tempdir()?;
  let db_path = dir.path().join("large_page_recover.db");
  let config = large_page_config();
  let mut big = vec![0u8; 3 * 1024 * 1024];
  for (i, b) in big.iter_mut().enumerate() {
    *b = (i % 241) as u8;
  }
  let addr_big;
  let tail_before;

  // 阶段 1：写入 3MB 大值并全量落盘
  {
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));
    let hlog = HybridLog::new(config.clone(), device, epoch)?;
    addr_big = hlog.append(b"big:3mib", &big, 0, false)?.0;
    hlog.flush_all().await?;
    hlog.sync().await?;
    tail_before = hlog.tail_address();
  }

  // 阶段 2：从快照恢复，大值逐字节一致
  {
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));
    let snapshot = AddressSnapshot::from_bounds(
      DEFAULT_INITIAL_ADDRESS,
      DEFAULT_INITIAL_ADDRESS,
      tail_before,
      tail_before,
      tail_before,
    );
    let hlog = HybridLog::recover(config, device, epoch, snapshot).await?;
    let out = hlog.read_record(addr_big).await?;
    assert_value_eq(out.value()?, &big);
    assert_eq!(out.key()?, b"big:3mib");

    // 恢复后无缝续写新记录
    let (next, _) = hlog.append(b"after", b"resume", addr_big, false)?;
    assert_eq!(next, tail_before);
    let out_next = hlog.read_record(next).await?;
    assert_eq!(out_next.value()?, b"resume");
  }

  OK
}

/// 超页记录仍被 RecordTooLarge 拦截、tail 不动；近整页极大记录（16MB - 头 - 键）
/// 可完整内联且冷读一致（单条记录必须整体落进单页的边界契约）
#[compio::test]
async fn large_page_rejects_over_page_record_but_admits_near_full_page() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("large_edge.db"),
  )?);
  let epoch = Arc::new(LightEpoch::new(16));
  let hlog = HybridLog::new(large_page_config(), Arc::clone(&device), epoch)?;

  // 超页：值 16MB（记录 = 16 头 + 2 键 + 16MB > 页容量）必须显式报错
  let huge = vec![0u8; DEFAULT_SERVER_PAGE_SIZE];
  let err = hlog.append(b"ov", &huge, 0, false).unwrap_err();
  assert!(
    matches!(err, Error::RecordTooLarge { size, .. } if size > DEFAULT_SERVER_PAGE_SIZE),
    "超页记录必须被 RecordTooLarge 拦截: {err:?}"
  );
  assert_eq!(
    hlog.tail_address(),
    DEFAULT_INITIAL_ADDRESS,
    "被拦截的追加不得推进 tail"
  );

  // 边界内：值 = 页容量 - 起始地址(64) - 头(16) - 键(8)，记录恰好占满首页剩余空间
  let max_val = vec![b'X'; DEFAULT_SERVER_PAGE_SIZE - 64 - 16 - 8];
  let (addr_max, _) = hlog.append(b"max:page", &max_val, 0, false)?;
  assert_eq!(hlog.config.page_id(addr_max), 0);

  // 下一记录触发跨页 Pad + 新页写入（大页位面算术 page_bits = 24 下语义不变）
  let (addr_next, _) = hlog.append(b"n", b"next", addr_max, false)?;
  assert_eq!(hlog.config.page_id(addr_next), 1, "页满后必须换页写入");

  // 全量落盘后两条记录（近整页 + 新页小记录）冷读逐字节一致
  hlog.flush_all().await?;
  hlog.sync().await?;
  hlog.shift_read_only_address(hlog.tail_address());
  hlog.shift_head_address(hlog.tail_address());
  let out_max = hlog.read_record(addr_max).await?;
  assert_value_eq(out_max.value()?, &max_val);
  let out_next = hlog.read_record(addr_next).await?;
  assert_eq!(out_next.value()?, b"next");

  // 扫描恰好交付两条记录（页尾 Pad 精确越过）
  let mut keys = Vec::new();
  hlog
    .scan(0, hlog.tail_address(), |_, rec| {
      keys.push(rec.key().to_vec());
      Ok(true)
    })
    .await?;
  assert_eq!(keys, vec![b"max:page".to_vec(), b"n".to_vec()]);

  OK
}
