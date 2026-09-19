//! 对象信封过引擎读写/检查点往返测试
//!
//! 分层口径：C# 侧 Tsavorite 引擎测试工程不引用 Garnet.server
//! （garnet/libs/storage/Tsavorite/cs/test），对象信封读写用例位于顶层集合测试工程
//! test/standalone/Garnet.test.collections。rust 同构：被测件虽为 wkv 引擎，但断言
//! 消费 wcol 集合对象与 wval 类型标签，故本文件由 wkv/tests 归位至同引 wkv/wcol 的
//! wnode/tests，引擎 crate 不再反向依赖上层集合与协议层。
//!
//! 对象自身序列化的成员级过期剔除不牵引擎，拆至 wcol/tests/object_serialize_expiration_tests.rs。

use std::{path::Path, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcol::{
  object_payload::{obj_decode, obj_encode},
  zset::sorted_set_object::SortedSetObject,
};
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wtest_base::test_store_config;
use wval::GarnetObjectType;

const KEY_NUM: &[u8] = &[0];

/// 对象信封类型标签（值内 `[1B 类型标签][bitcode 载荷]`，对标 C# 对象存序列化首字节）
const ZSET_TAG: GarnetObjectType = GarnetObjectType::SortedSet;

/// 以指定 db 路径开小预算库（gc 关闭保持历史语义）
///
/// 刻意不用 [`wtest_base::open_test_store`]：检查点用例须在停库后按同一物理路径重建
/// device 再 recover_latest，需要自持数据文件路径（open_test_store 只回临时目录句柄）
fn open_store(
  db_path: &Path,
) -> aok::Result<(Arc<SegmentedDevice>, Arc<WedbStore<SegmentedDevice>>)> {
  let device = Arc::new(SegmentedDevice::single_file(db_path)?);
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
  Ok((device, store))
}

/// test/standalone/Garnet.test.collections/GarnetObjectTests.cs:WriteRead
#[test]
fn write_read() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    let (_device, store) = open_store(&db_path)?;
    let session = store.new_session()?;

    let obj = SortedSetObject::new();
    let bytes = obj_encode(ZSET_TAG, &obj.serialize_to_vec());

    session.upsert(KEY_NUM, &bytes).await?;

    let output_raw = session.read(KEY_NUM).await?;
    assert!(output_raw.is_some());

    let raw = output_raw.unwrap();
    let payload = obj_decode(&raw, ZSET_TAG).unwrap();
    let output = SortedSetObject::deserialize_from_slice(payload)?;
    assert!(obj.equals(&output));
    OK
  })
}

/// test/standalone/Garnet.test.collections/GarnetObjectTests.cs:WriteCheckpointRead
#[test]
fn write_checkpoint_read() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    let cp_dir = dir.path().join("checkpoints");
    let mut obj = SortedSetObject::new();
    obj.add(b"\x0f", 10.0);

    // 1. LocalWrite & Checkpoint
    {
      let (_device, store) = open_store(&db_path)?;
      let session = store.new_session()?;
      let bytes = obj_encode(ZSET_TAG, &obj.serialize_to_vec());
      session.upsert(KEY_NUM, &bytes).await?;
      store
        .create_checkpoint(&cp_dir, CheckpointType::FoldOver)
        .await?;
    }

    // 2. Recover & LocalRead
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::<SegmentedDevice>::recover_latest(&cp_dir, device).await?);
      let session = store.new_session()?;

      let output_raw = session.read(KEY_NUM).await?;
      assert!(output_raw.is_some());

      let raw = output_raw.unwrap();
      let payload = obj_decode(&raw, ZSET_TAG).unwrap();
      let output = SortedSetObject::deserialize_from_slice(payload)?;
      assert!(obj.equals(&output));
    }
    OK
  })
}

/// test/standalone/Garnet.test.collections/GarnetObjectTests.cs:WriteCheckpointCopyUpdate
#[test]
fn write_checkpoint_copy_update() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    let cp_dir = dir.path().join("checkpoints");
    let mut obj = SortedSetObject::new();
    obj.add(b"\x0f", 10.0);

    // 1. LocalWrite & Checkpoint
    {
      let (_device, store) = open_store(&db_path)?;
      let session = store.new_session()?;
      let bytes = obj_encode(ZSET_TAG, &obj.serialize_to_vec());
      session.upsert(KEY_NUM, &bytes).await?;
      store
        .create_checkpoint(&cp_dir, CheckpointType::FoldOver)
        .await?;

      // RMW update
      obj.add(b"\x10", 20.0);
      let updated_bytes = obj_encode(ZSET_TAG, &obj.serialize_to_vec());
      session.upsert(KEY_NUM, &updated_bytes).await?;
      store
        .create_checkpoint(&cp_dir, CheckpointType::FoldOver)
        .await?;
    }

    // 2. Recover & LocalRead
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::<SegmentedDevice>::recover_latest(&cp_dir, device).await?);
      let session = store.new_session()?;

      let output_raw = session.read(KEY_NUM).await?;
      assert!(output_raw.is_some());

      let raw = output_raw.unwrap();
      let payload = obj_decode(&raw, ZSET_TAG).unwrap();
      let output = SortedSetObject::deserialize_from_slice(payload)?;
      assert!(obj.equals(&output));
    }
    OK
  })
}
