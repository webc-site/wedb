//! 混合日志内存分布扫描集成测试（对标 Garnet CollectHybridLogStats /
//! INFO HLOGSCAN 段语义）
//!
//! 覆盖：空日志空转储、活动/被取代/墓碑三态计数与字节聚合、区域归属
//! （小日志全落可写热区 Mutable）、多版本与删除后的桶间分布、只读线
//! 推进与 head 驱逐后的 Immutable 冷区桶计数。

use aok::{OK, Void};
use compio::runtime::Runtime;
use wrecord::record_size;
use wtest_base::open_test_store;
use wval::KeyTag;

/// 从转储文本解析指定状态的 `(Count, Size)`
fn dump_bucket_size(dump: &str, state: &str) -> (i64, i64) {
  let line = dump
    .lines()
    .find(|l| l.trim().starts_with(&format!("State: {state},")))
    .unwrap_or_else(|| panic!("转储缺状态桶 {state}: {dump}"));
  let nums: Vec<i64> = line
    .split([',', ':'])
    .filter_map(|seg| seg.trim().parse().ok())
    .collect();
  (nums[0], nums[1])
}

/// 按区域标签切出转储的单区域段（`# Region: {tag}` 行之后到下个区域头前）
fn dump_region_seg<'a>(dump: &'a str, tag: &str) -> &'a str {
  dump
    .split("# Region: ")
    .find(|s| s.starts_with(tag))
    .unwrap_or_else(|| panic!("转储缺 {tag} 区域段: {dump}"))
}

/// 键 "k1"/"k2"/"k3"（长度 2）各值长度下的物理条宽（头 16 + 键 + 值对齐 8）
const fn phys(val_len: usize) -> usize {
  record_size(2, val_len)
}

/// 测试 1：空日志转储为空串（INFO 侧呈现 Empty），写入后分布非空
#[test]
fn hlog_scan_empty_then_populated() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("hlogscan_empty")?;
    let session = store.new_session()?;

    // 空日志：无记录 → 空转储（C# DumpScanMetricsInfo 空统计返回空）
    assert!(
      store
        .hlog_scan_metrics()
        .await?
        .dump_scan_metrics_info()
        .is_empty()
    );

    session.upsert(b"k1", b"v").await?;
    let dump = store.hlog_scan_metrics().await?.dump_scan_metrics_info();
    assert!(dump.contains("# Region: Mutable\n"), "{dump}");
    assert!(dump.contains("State: Live, Count: 1"), "{dump}");

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 2：多版本覆盖产生被取代旧版、删除产生墓碑，计数与字节聚合精确
#[test]
fn hlog_scan_state_distribution() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("hlogscan_dist")?;
    let session = store.new_session()?;

    // k1 短值 → k1 长值（变长覆盖必追加新版本，旧版成被取代记录）；
    // k2、k3 各一条；再删除 k3 留墓碑
    session.upsert(b"k1", b"x").await?;
    session.upsert(b"k2", b"yy").await?;
    session.upsert(b"k3", b"zz").await?;
    session.upsert(b"k1", b"wwww").await?;
    assert!(session.delete(b"k3").await?);

    let dump = store.hlog_scan_metrics().await?.dump_scan_metrics_info();

    // 小库日志无驱逐：read_only 恒在日志起点，全部记录落可写热区
    assert!(dump.contains("# Region: Mutable\n"), "{dump}");
    assert!(!dump.contains("Immutable"), "{dump}");

    // Live：k1 新版 + k2；被取代：k1 旧版；墓碑：k3
    assert!(dump.contains("State: Live, Count: 2"), "{dump}");
    assert!(dump.contains("State: RCUdUnsealed, Count: 1"), "{dump}");
    assert!(dump.contains("State: Tombstoned, Count: 1"), "{dump}");

    // 字节聚合 = 逐记录物理条宽（头 + 键 + 值 + 松弛填充，对齐 C#
    // NextAddress - CurrentAddress 口径）。Live 桶与读路径 AllocatedSize
    // 自洽（read_tag_with_size 披露同一物理尺寸）；被取代旧版不可读，
    // 按对齐条宽下界校验；墓碑为 0 字节值盲追加记录，无松弛
    let mut live_size = 0i64;
    for k in [&b"k1"[..], b"k2"] {
      live_size += session
        .read_tag_with_size(k, KeyTag::String, |_v, size| size as i64)
        .await?
        .unwrap_or(0);
    }
    assert!(
      dump.contains(&format!("State: Live, Count: 2, Size: {live_size}\n")),
      "{dump}"
    );
    let rcud_size = dump_bucket_size(&dump, "RCUdUnsealed");
    assert_eq!(rcud_size.0, 1);
    assert!(rcud_size.1 >= phys(1) as i64, "{dump}");
    assert_eq!(dump_bucket_size(&dump, "Tombstoned"), (1, phys(0) as i64));

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 3：同键原位等长更新不产生新版本，Live 计数不虚增
#[test]
fn hlog_scan_in_place_update_stays_live() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("hlogscan_inplace")?;
    let session = store.new_session()?;

    session.upsert(b"k1", b"abc").await?;
    // 等长覆盖：原位更新，日志仍只有一条 Live 记录
    session.upsert(b"k1", b"xyz").await?;

    let dump = store.hlog_scan_metrics().await?.dump_scan_metrics_info();
    assert!(dump.contains("State: Live, Count: 1"), "{dump}");
    assert!(!dump.contains("RCUdUnsealed"), "{dump}");
    assert!(!dump.contains("Tombstoned"), "{dump}");

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 4：只读线推进 + head 驱逐后的 Immutable 冷区覆盖
///
/// 对齐 C# `[HeadAddress, TailAddress]` 扫描下界与
/// `CurrentAddress >= ReadOnlyAddress` 区域判定：head 推进把滑出内存窗口
/// 的记录排除出扫描区间（驱逐语义），区间内 read_only 线之下的记录落
/// Immutable 桶，且驱逐不影响索引链头——冷区记录仍判 Live。
#[test]
fn hlog_scan_immutable_region_after_head_shift() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("hlogscan_immutable")?;
    let session = store.new_session()?;

    // 前段记录：将被 head 推进排除出扫描区间（对标驱逐）
    session.upsert(b"evicted1", b"v1").await?;
    session.upsert(b"evicted2", b"v2").await?;
    let cut = store.tail_address();
    // 冷区记录：位于驱逐切点之上、只读线之下 → 扫描区间内的 Immutable 记录
    session.upsert(b"cold", b"v3").await?;
    // 刷盘前置：head 推进钳制在 flushed_until（驱逐不越过持久化前缀不变式）
    store.flush_all().await?;
    // 推进只读线（区域判定线）再推进 head（驱逐线），对齐 wkv 紧缩测试的
    // 现有推进基建（shift_read_only_address / shift_head_address）
    store.shift_read_only_address(store.tail_address());
    store.shift_head_address(cut);
    assert_eq!(store.head_address(), cut, "head 必须推进到驱逐切点");
    // 驱逐后热区写入 → Mutable
    session.upsert(b"hot", b"v4").await?;

    let dump = store.hlog_scan_metrics().await?.dump_scan_metrics_info();

    // 双区域桶均在：冷区段在前（扫描地址序），热区段在后
    let cold_seg = dump_region_seg(&dump, "Immutable");
    let hot_seg = dump_region_seg(&dump, "Mutable");
    assert!(
      dump.find("Immutable").unwrap() < dump.find("Mutable").unwrap(),
      "{dump}"
    );

    // Immutable：仅 cold（evicted1/evicted2 已滑出 [head, tail) 不入扫描）
    assert!(cold_seg.contains("State: Live, Count: 1"), "{dump}");
    assert!(!cold_seg.contains("RCUdUnsealed"), "{dump}");
    // 驱逐不影响索引链头：cold 仍判 Live，字节聚合与读路径物理条宽自洽
    let cold_size = session
      .read_tag_with_size(b"cold", KeyTag::String, |_v, size| size as i64)
      .await?
      .unwrap_or(0);
    assert!(
      cold_seg.contains(&format!("State: Live, Count: 1, Size: {cold_size}\n")),
      "{dump}"
    );

    // Mutable：仅 hot
    assert!(hot_seg.contains("State: Live, Count: 1"), "{dump}");

    aok::Result::<()>::Ok(())
  })?;
  OK
}
