use std::{path::Path, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{CheckpointType, StoreConfig, WedbStore};
use wnode::objects::{
  sortedset::sorted_set_object::SortedSetObject,
  types::garnet_object_serializer::{GarnetObjectSerializer, GarnetObjectValue},
};

const KEY_NUM: &[u8] = &[0];

fn open_store(
  db_path: &Path,
) -> aok::Result<(Arc<SegmentedDevice>, Arc<WedbStore<SegmentedDevice>>)> {
  let device = Arc::new(SegmentedDevice::single_file(db_path)?);
  let mut config = StoreConfig::new(16384, 65536, 64, 0.5)?;
  config.gc.enabled = false;
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
    let bytes =
      GarnetObjectSerializer::serialize_bytes(&GarnetObjectValue::SortedSet(obj.clone()))?;

    session.upsert(KEY_NUM, &bytes).await?;

    let output_raw = session.read(KEY_NUM).await?;
    assert!(output_raw.is_some());

    let val = GarnetObjectSerializer::deserialize_bytes(&output_raw.unwrap())?;
    match val {
      Some(GarnetObjectValue::SortedSet(output)) => {
        assert!(obj.equals(&output));
      }
      _ => panic!("Expected SortedSet object"),
    }
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
      let bytes =
        GarnetObjectSerializer::serialize_bytes(&GarnetObjectValue::SortedSet(obj.clone()))?;
      session.upsert(KEY_NUM, &bytes).await?;
      store
        .create_checkpoint(&cp_dir, CheckpointType::FoldOver)
        .await?;
    }

    // 2. Recover & LocalRead
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::recover_latest(&cp_dir, device).await?);
      let session = store.new_session()?;

      let output_raw = session.read(KEY_NUM).await?;
      assert!(output_raw.is_some());

      let val = GarnetObjectSerializer::deserialize_bytes(&output_raw.unwrap())?;
      match val {
        Some(GarnetObjectValue::SortedSet(output)) => {
          assert!(obj.equals(&output));
        }
        _ => panic!("Expected SortedSet object"),
      }
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
      let bytes =
        GarnetObjectSerializer::serialize_bytes(&GarnetObjectValue::SortedSet(obj.clone()))?;
      session.upsert(KEY_NUM, &bytes).await?;
      store
        .create_checkpoint(&cp_dir, CheckpointType::FoldOver)
        .await?;

      // RMW update
      obj.add(b"\x10", 20.0);
      let updated_bytes =
        GarnetObjectSerializer::serialize_bytes(&GarnetObjectValue::SortedSet(obj.clone()))?;
      session.upsert(KEY_NUM, &updated_bytes).await?;
      store
        .create_checkpoint(&cp_dir, CheckpointType::FoldOver)
        .await?;
    }

    // 2. Recover & LocalRead
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::recover_latest(&cp_dir, device).await?);
      let session = store.new_session()?;

      let output_raw = session.read(KEY_NUM).await?;
      assert!(output_raw.is_some());

      let val = GarnetObjectSerializer::deserialize_bytes(&output_raw.unwrap())?;
      match val {
        Some(GarnetObjectValue::SortedSet(output)) => {
          assert!(obj.equals(&output));
        }
        _ => panic!("Expected SortedSet object"),
      }
    }
    OK
  })
}

/// test/standalone/Garnet.test.collections/GarnetObjectTests.cs:HashAndSortedSetSerializeWithSnapshotTimestamp
#[test]
fn hash_and_sorted_set_serialize_with_snapshot_timestamp() {
  use std::io::Cursor;

  use wbase::time::now_ticks;
  use wnode::objects::{
    hash::hash_object::{ExpireOption, HashObject},
    sortedset::sorted_set_object::SortedSetObject,
  };

  let now = now_ticks();

  // 1. HashObject
  let mut hash = HashObject::new();
  hash.hash.insert(b"field_live".to_vec(), b"val1".to_vec());
  hash
    .hash
    .insert(b"field_expired".to_vec(), b"val2".to_vec());
  hash.set_expiration(b"field_live", now + 1_000_000_000, ExpireOption::NONE);
  hash.set_expiration(b"field_expired", now - 1_000_000, ExpireOption::NONE);

  let mut buf = Vec::new();
  hash.serialize(&mut buf).unwrap();

  let deserialized = HashObject::deserialize(&mut Cursor::new(&buf)).unwrap();
  assert!(deserialized.hash.contains_key(b"field_live".as_slice()));
  assert!(!deserialized.hash.contains_key(b"field_expired".as_slice()));
  assert_eq!(
    deserialized
      .expiration_times
      .as_ref()
      .and_then(|t| t.get(b"field_live".as_slice())),
    Some(&(now + 1_000_000_000))
  );

  // 2. SortedSetObject
  let mut zset = SortedSetObject::new();
  zset.add(b"m_live", 1.0);
  zset.add(b"m_expired", 2.0);
  zset.set_expiration(b"m_live", now + 1_000_000_000, ExpireOption::NONE);
  zset.set_expiration(b"m_expired", now - 1_000_000, ExpireOption::NONE);

  let mut zbuf = Vec::new();
  zset.serialize(&mut zbuf).unwrap();

  let z_deserialized = SortedSetObject::deserialize(&mut Cursor::new(&zbuf)).unwrap();
  assert!(
    z_deserialized
      .sorted_set_dict
      .contains_key(b"m_live".as_slice())
  );
  assert!(
    !z_deserialized
      .sorted_set_dict
      .contains_key(b"m_expired".as_slice())
  );
  assert_eq!(
    z_deserialized
      .expiration_times
      .as_ref()
      .and_then(|t| t.get(b"m_live".as_slice())),
    Some(&(now + 1_000_000_000))
  );
}
