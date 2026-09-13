//! RangeIndex 流式与零拷贝范围扫描测试
//!
//! 覆盖：
//! 1. range_index_scan_stream 零拷贝遍历、早期终止、字段投影；
//! 2. range_index_range_stream 闭区间零拷贝扫描；
//! 3. range_index_scan 与 range_index_range 向后兼容性与数据一致性；
//! 4. 边界用例：count = 0、反向区间、Memory 模式报错。

use std::{fs::create_dir_all, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::{ScanReturnField, StorageBackend, TreeTuning};
use wdev::SegmentedDevice;
use wkv::{RangeIndexError, StoreConfig, WedbStore};

const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

#[test]
fn test_range_index_scan_stream_basic() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("scan_stream.db");
    let ri_dir = dir.path().join("range_indexes");
    create_dir_all(&ri_dir)?;

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?.with_range_index_dir(&ri_dir);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let idx_key = b"users_by_age";
    session
      .range_index_create(idx_key, StorageBackend::Std, TUNE)
      .await?;

    // 插入 20 条有序记录：age_00 ~ age_19
    for i in 0..20 {
      let field = format!("age_{i:02}");
      let value = format!("val_{i:02}");
      session
        .range_index_set(idx_key, field.as_bytes(), value.as_bytes())
        .await?;
    }

    // 1. 流式全量扫描验证
    let mut collected = Vec::new();
    let count = session
      .range_index_scan_stream(
        idx_key,
        b"age_00",
        20,
        ScanReturnField::KeyAndValue,
        |k, v| {
          collected.push((k.to_vec(), v.to_vec()));
          true
        },
      )
      .await?;

    assert_eq!(count, 20);
    assert_eq!(collected.len(), 20);
    for (i, (k, v)) in collected.iter().enumerate() {
      assert_eq!(k.as_slice(), format!("age_{i:02}").as_bytes());
      assert_eq!(v.as_slice(), format!("val_{i:02}").as_bytes());
    }

    // 2. 早期终止 (Early Termination) 验证：扫描 5 条后中断
    let mut stopped_at = 0;
    let count = session
      .range_index_scan_stream(
        idx_key,
        b"age_00",
        20,
        ScanReturnField::KeyAndValue,
        |_k, _v| {
          stopped_at += 1;
          stopped_at < 5
        },
      )
      .await?;
    assert_eq!(count, 5);
    assert_eq!(stopped_at, 5);

    // 3. 字段投影过滤 (ScanReturnField::Key)
    session
      .range_index_scan_stream(idx_key, b"age_00", 3, ScanReturnField::Key, |k, v| {
        assert!(!k.is_empty());
        assert!(
          v.is_empty(),
          "ScanReturnField::Key 模式下 value 切片必须为空"
        );
        true
      })
      .await?;

    // 4. 字段投影过滤 (ScanReturnField::Value)
    session
      .range_index_scan_stream(idx_key, b"age_00", 3, ScanReturnField::Value, |k, v| {
        assert!(
          k.is_empty(),
          "ScanReturnField::Value 模式下 key 切片必须为空"
        );
        assert!(!v.is_empty());
        true
      })
      .await?;

    // 5. 与向后兼容的 range_index_scan 结果严格一致性对比
    let vec_records = session
      .range_index_scan(idx_key, b"age_05", 10, ScanReturnField::KeyAndValue)
      .await?;
    let mut stream_records = Vec::new();
    let stream_count = session
      .range_index_scan_stream(
        idx_key,
        b"age_05",
        10,
        ScanReturnField::KeyAndValue,
        |k, v| {
          stream_records.push((k.to_vec(), v.to_vec()));
          true
        },
      )
      .await?;

    assert_eq!(vec_records.len(), stream_count);
    assert_eq!(vec_records.len(), 10);
    for (vec_rec, (sk, sv)) in vec_records.iter().zip(stream_records.iter()) {
      assert_eq!(&vec_rec.key, sk);
      assert_eq!(&vec_rec.value, sv);
    }

    // 6. count == 0 边界
    let zero_count = session
      .range_index_scan_stream(
        idx_key,
        b"age_00",
        0,
        ScanReturnField::KeyAndValue,
        |_, _| panic!("count == 0 时不得触发回调"),
      )
      .await?;
    assert_eq!(zero_count, 0);

    // 7. 闭区间流式范围扫描 (range_index_range_stream)
    let mut range_collected = Vec::new();
    let range_count = session
      .range_index_range_stream(
        idx_key,
        b"age_05",
        b"age_09",
        ScanReturnField::KeyAndValue,
        |k, v| {
          range_collected.push((k.to_vec(), v.to_vec()));
          true
        },
      )
      .await?;
    assert_eq!(range_count, 5);
    assert_eq!(range_collected.len(), 5);
    assert_eq!(range_collected[0].0.as_slice(), b"age_05");
    assert_eq!(range_collected[4].0.as_slice(), b"age_09");

    // 8. 闭区间反向 start > end 边界
    let empty_range = session
      .range_index_range_stream(
        idx_key,
        b"age_10",
        b"age_05",
        ScanReturnField::KeyAndValue,
        |_, _| panic!("反向区间不得触发回调"),
      )
      .await?;
    assert_eq!(empty_range, 0);

    // 9. 不存在的索引报错
    let not_found_err = session
      .range_index_scan_stream(b"non_existent", b"a", 10, ScanReturnField::Key, |_, _| true)
      .await
      .unwrap_err();
    assert_eq!(not_found_err, RangeIndexError::NotFound);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

#[test]
fn test_range_index_memory_mode_scan_rejected() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("mem_scan.db");
    let config = StoreConfig::new(512, 16 * 1024, 16, 0.5)?;
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    session
      .range_index_create(b"mem_idx", StorageBackend::Memory, TUNE)
      .await?;

    let err = session
      .range_index_scan_stream(b"mem_idx", b"a", 10, ScanReturnField::Key, |_, _| true)
      .await
      .unwrap_err();
    assert_eq!(err, RangeIndexError::MemoryModeNotSupported);

    let err2 = session
      .range_index_range_stream(b"mem_idx", b"a", b"z", ScanReturnField::Key, |_, _| true)
      .await
      .unwrap_err();
    assert_eq!(err2, RangeIndexError::MemoryModeNotSupported);

    aok::Result::<()>::Ok(())
  })?;
  OK
}
