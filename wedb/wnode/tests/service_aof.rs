//! WAL 装配域回归：自定义/缺省 wal_dir 与 aof-segment-size 物理接线
//!
//! 对标 C#（票 waof-segment-knob 真源）：
//! - libs/server/Servers/GarnetServerOptions.cs:GetAofSettings / GetAofDevice
//!   （SegmentSizeBits 投影物理段设备，rust 侧 SegmentedDevice 分段装配）
//! - libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:
//!   GetSegmentFilename（`wal.log.<段号>` 段名）
//! - libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:
//!   LogSegmentSizeBits（跨段写入与历史段物理删除）

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::{AofAddress, AofEntryType};
use wconf::RuntimeServerOptions;
use wnode::service::StorageSessionProvider;
use wnode_test::session_factory;
use wtest_base::test_store_config;
use wval::{KeyTag, NamespaceDbCodec};

/// 缺省段容量（与 wconf 缺省 aof-segment-size = 1g 同一真源）
const DEFAULT_SEGMENT_BYTES: u64 = 1024 * 1024 * 1024;
/// 段容量 32m（越段用例的最小合法段：页下限 32m）
const SEGMENT_BYTES: u64 = 32 * 1024 * 1024;
/// 单条记录负载 12m（三笔越出 32m 段界）
const RECORD_VALUE_BYTES: usize = 12 * 1024 * 1024;

fn entry_key(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 缺省段容量投影进设备 + 自定义 wal_dir 首笔写入刷盘生成段 0 文件
///（分段设备构造期不预建空文件，段文件按 `<基名>.<段号>` 落盘）
#[test]
fn open_with_config_and_aof_custom_wal_dir() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().unwrap();
  let data_path = dir.path().join("data").join("test.db");
  let custom_wal = dir.path().join("custom_wal_dir");
  // 小预算测试配置注入（生产缺省走 StoreConfig::auto，大机上规划出 GB 级索引）
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    Some(&custom_wal),
    RuntimeServerOptions::default(),
    session_factory,
  )
  .unwrap();

  assert!(provider.aof().is_some());
  let wal = Arc::clone(provider.wal().expect("AOF 点亮即有物理日志"));
  assert_eq!(
    wal.device.segment_size(),
    DEFAULT_SEGMENT_BYTES,
    "缺省 aof-segment-size（1g）须投影进设备段容量"
  );
  rt.block_on(async {
    let aof = provider.aof().unwrap();
    aof
      .enqueue_raw(
        AofEntryType::StoreUpsert,
        (1, 1),
        &entry_key(b"k"),
        &[0u8; 64],
        &[],
      )
      .unwrap();
    wal.commit().await.unwrap();
  });
  let seg0 = wal.device.segment_path(0);
  assert!(
    seg0.exists(),
    "自定义 wal_dir 下须生成分段段文件: {}",
    seg0.display()
  );
  assert!(
    !custom_wal.join("wal.log").exists(),
    "分段装配不再生成裸 wal.log 单文件"
  );
}

/// 缺省 wal_dir（`<data>/wal`）下同口径生成分段段文件
#[test]
fn open_with_config_and_aof_default_wal_dir() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().unwrap();
  let data_path = dir.path().join("data").join("test.db");
  // 小预算测试配置注入（生产缺省走 StoreConfig::auto，大机上规划出 GB 级索引）
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    RuntimeServerOptions::default(),
    session_factory,
  )
  .unwrap();

  assert!(provider.aof().is_some());
  rt.block_on(async {
    let wal = Arc::clone(provider.wal().unwrap());
    let aof = provider.aof().unwrap();
    aof
      .enqueue_raw(
        AofEntryType::StoreUpsert,
        (1, 1),
        &entry_key(b"k"),
        &[0u8; 64],
        &[],
      )
      .unwrap();
    wal.commit().await.unwrap();
  });
  let seg0 = provider.wal().unwrap().device.segment_path(0);
  assert!(
    seg0.exists(),
    "缺省情况下应在 <data>/wal/ 下生成分段段文件: {}",
    seg0.display()
  );
}

/// aof-segment-size 旋钮真值：自定义段容量实持于设备，同批 wal_config
/// 投影的常驻窗口与页容量同步生效（C# GetAofSettings 一次性投影同位）
#[test]
fn aof_segment_size_knob_projects_into_segmented_device() {
  let dir = tempdir().unwrap();
  let data_path = dir.path().join("data").join("test.db");
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    RuntimeServerOptions {
      aof_memory_size: Some("64m".into()),
      aof_page_size: Some("32m".into()),
      aof_segment_size: Some("64m".into()),
      ..RuntimeServerOptions::default()
    },
    session_factory,
  )
  .unwrap();

  let wal = provider.wal().expect("AOF 点亮即有物理日志");
  assert_eq!(
    wal.device.segment_size(),
    64 * 1024 * 1024,
    "设备实持段容量须等于 aof-segment-size 生效值"
  );
  assert_eq!(
    wal.config().buffer_size,
    64 * 1024 * 1024,
    "常驻窗口须等于 aof-memory 生效值"
  );
  assert_eq!(
    wal.config().page_size,
    32 * 1024 * 1024,
    "页容量须等于 aof-page-size 生效值"
  );
}

/// 越段写入 + 检查点截断 → 历史段文件被物理删除（磁盘回收真实发生，
/// 证伪「truncate 空转、磁盘只增不减」）
#[test]
fn truncate_after_segment_boundary_cross_deletes_history_segments() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().unwrap();
  let data_path = dir.path().join("data").join("test.db");
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    RuntimeServerOptions {
      aof_memory_size: Some("64m".into()),
      aof_page_size: Some("32m".into()),
      aof_segment_size: Some("32m".into()),
      ..RuntimeServerOptions::default()
    },
    session_factory,
  )
  .unwrap();
  let wal = Arc::clone(provider.wal().unwrap());
  let aof = Arc::clone(provider.aof().unwrap());

  rt.block_on(async move {
    assert_eq!(wal.device.segment_size(), SEGMENT_BYTES);
    let seg0 = wal.device.segment_path(0);
    let seg1 = wal.device.segment_path(1);
    // 三笔 12m 记录共 36m+，越过 32m 段界 → 段 0 与段 1 相继落盘
    let value = vec![0xA5u8; RECORD_VALUE_BYTES];
    for i in 0..3u8 {
      aof
        .enqueue_raw(
          AofEntryType::StoreUpsert,
          (1, 1),
          &entry_key(format!("k{i}").as_bytes()),
          &value,
          &[],
        )
        .unwrap();
    }
    wal.commit().await.unwrap();
    assert!(seg0.exists(), "越界前段 0 须已落盘: {}", seg0.display());
    assert!(seg1.exists(), "越界后段 1 须已落盘: {}", seg1.display());

    // 截断走生产唯一回收链（与检查点后的 safe_truncate_aof 同一入口：
    // GarnetLog::truncate_until_async → WaofSublog::truncate_until_async →
    // WalLog::truncate → 设备 truncate_until_address），对标 C# AllocatorBase.cs
    // ShiftBeginAddress 的「先平移 begin 再物理删段」次序——段 0 位于截断点
    // 之前，须被真实删除
    let committed = wal.committed_until_address();
    assert!(
      committed > SEGMENT_BYTES,
      "已提交位点须越过段界: {committed}"
    );
    let until = AofAddress::create(aof.log().size() as i32, committed as i64);
    aof.log().truncate_until_async(&until).await;
    assert!(
      !seg0.exists(),
      "历史段 0 须被物理删除（磁盘回收真实发生）: {}",
      seg0.display()
    );
    assert!(seg1.exists(), "当前段 1 须保留: {}", seg1.display());
    assert!(
      wal.begin_address() >= SEGMENT_BYTES,
      "begin 须推进至段界之后: {}",
      wal.begin_address()
    );
  });
}
