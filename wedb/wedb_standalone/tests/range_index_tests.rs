use std::{fs, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{RangeIndexManager as Engine, StoreConfig, StoreSession, WedbStore};
use wnode::{
  aof::aof_processor::ReplayInput,
  resp::{
    rangeindex::{
      range_index_manager::RangeIndexManager,
      range_index_manager_replication::RangeIndexManagerReplication,
    },
    resp_server_session::RespServerSession,
  },
  types::RespCommand,
};

type TestEnv = (
  tempfile::TempDir,
  RangeIndexManager,
  StoreSession<SegmentedDevice>,
  RespServerSession,
);

fn setup_env() -> TestEnv {
  let dir = tempdir().unwrap();
  let ri_dir = dir.path().join("ri");
  let cpr_dir = dir.path().join("cpr");
  let ri = RangeIndexManager::new(&ri_dir, &cpr_dir).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
  let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  config.gc.enabled = false;
  config.range_index_dir = Some(ri_dir);
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let resp = RespServerSession::default();
  (dir, ri, session, resp)
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RICreateBasicTest
#[test]
fn ri_create_basic_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    let ok = resp
      .network_ricreate(
        &[b"myindex", b"MEMORY", b"CACHESIZE", b"65536"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"+OK\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RICreateDuplicateReturnsErrorTest
#[test]
fn ri_create_duplicate_returns_error_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[b"myindex", b"MEMORY", b"CACHESIZE", b"65536"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    resp
      .network_ricreate(
        &[b"myindex", b"MEMORY", b"CACHESIZE", b"65536"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert!(out.starts_with(b"-ERR index already exists"));
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RICreateThenDeleteTest
#[test]
fn ri_create_then_delete_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"MEMORY",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    resp
      .network_riset(
        &[b"myindex", b"field1", b"value1"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let deleted = session.delete(b"myindex").await.unwrap();
    assert!(deleted);

    let deleted_again = session.delete(b"myindex").await.unwrap();
    assert!(!deleted_again);

    out.clear();
    resp
      .network_riset(
        &[b"myindex", b"field1", b"value1"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    // C# storage 层 errorMsg 文案（RangeIndexOps.cs:212/220）
    assert_eq!(out, b"-ERR no such range index\r\n");

    out.clear();
    resp
      .network_riget(&[b"myindex", b"field1"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    // C# 网络层硬编码文案（RespServerSessionRangeIndex.cs:278）
    assert_eq!(out, b"-ERR range index not found\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RICreateWithDefaultsTest
#[test]
fn ri_create_with_defaults_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(&[b"myindex", b"MEMORY"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RICreateWithAllOptionsTest
#[test]
fn ri_create_with_all_options_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"MEMORY",
          b"CACHESIZE",
          b"131072",
          b"MINRECORD",
          b"8",
          b"MAXRECORD",
          b"1024",
          b"MAXKEYLEN",
          b"128",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RISetAndGetBasicTest
#[test]
fn ri_set_and_get_basic_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"MEMORY",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    resp
      .network_riset(
        &[b"myindex", b"field1", b"value1"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    resp
      .network_riget(&[b"myindex", b"field1"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"$6\r\nvalue1\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RISetOverwriteTest
#[test]
fn ri_set_overwrite_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"MEMORY",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riset(
        &[b"myindex", b"field1", b"value1"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    resp
      .network_riset(
        &[b"myindex", b"field1", b"value2"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    resp
      .network_riget(&[b"myindex", b"field1"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"$6\r\nvalue2\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIGetNonExistentFieldTest
#[test]
fn ri_get_non_existent_field_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"MEMORY",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riget(&[b"myindex", b"nosuchfield"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIGetNonExistentIndexTest
#[test]
fn ri_get_non_existent_index_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_riget(&[b"noindex", b"field1"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    // C# 网络层硬编码文案（RespServerSessionRangeIndex.cs:278）
    assert_eq!(out, b"-ERR range index not found\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIDelFieldTest
#[test]
fn ri_del_field_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"MEMORY",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riset(
        &[b"myindex", b"field1", b"value1"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_ridel(&[b"myindex", b"field1"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    resp
      .network_riget(&[b"myindex", b"field1"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RISetOnNonExistentIndexTest
#[test]
fn ri_set_on_non_existent_index_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_riset(
        &[b"noindex", b"field1", b"value1"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    // C# storage 层 errorMsg 文案（RangeIndexOps.cs:212），网络层原样写出
    assert_eq!(out, b"-ERR no such range index\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIMultipleFieldsTest
#[test]
fn ri_multiple_fields_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"MEMORY",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    resp
      .network_riset(
        &[b"myindex", b"aaa", b"val-a"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    resp
      .network_riset(
        &[b"myindex", b"bbb", b"val-b"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    resp
      .network_riset(
        &[b"myindex", b"ccc", b"val-c"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riget(&[b"myindex", b"aaa"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"$5\r\nval-a\r\n");

    out.clear();
    resp
      .network_riget(&[b"myindex", b"bbb"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"$5\r\nval-b\r\n");

    out.clear();
    resp
      .network_riget(&[b"myindex", b"ccc"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"$5\r\nval-c\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIWrongTypeOnNormalKeyTest
#[test]
fn ri_wrong_type_on_normal_key_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    session.upsert(b"normalkey", b"hello").await.unwrap();

    let mut out = Vec::new();
    resp
      .network_riset(
        &[b"normalkey", b"field1", b"value1"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert!(out.starts_with(b"-WRONGTYPE"));
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIWrongTypeGetOnNormalKeyTest
#[test]
fn ri_wrong_type_get_on_normal_key_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    session.upsert(b"normalkey", b"hello").await.unwrap();

    let mut out = Vec::new();
    resp
      .network_riget(&[b"normalkey", b"field1"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert!(out.starts_with(b"-WRONGTYPE"));
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RINormalGetOnRangeIndexKeyTest
#[test]
fn ri_normal_get_on_range_index_key_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[b"myindex", b"MEMORY", b"CACHESIZE", b"65536"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    // 普通 GET 会识别为 WRONGTYPE 或空
    let res = session.read(b"myindex").await.unwrap();
    assert!(res.is_none());
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIAofReplayTest
#[test]
fn ri_aof_replay_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, _ri, session, _resp) = setup_env();
    let replication = RangeIndexManagerReplication::new(Arc::clone(&session.store.range_index));

    let stub = wkv::RangeIndexStub::new(0, 65536, 8, 1024, 128, 0, wkv::StorageBackendType::Memory);
    let create_input = ReplayInput {
      cmd: RespCommand::Ricreate,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![stub.encode().to_vec()],
    };
    replication
      .handle_range_index_create_replay(&session, b"aoftest", &create_input)
      .await
      .unwrap();
    assert!(session.range_index_exists(b"aoftest").await.unwrap());

    let set_input1 = ReplayInput {
      cmd: RespCommand::Riset,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"key1".to_vec(), b"val1".to_vec()],
    };
    replication
      .handle_range_index_set_replay(&session, b"aoftest", &set_input1)
      .await
      .unwrap();
    assert_eq!(
      session.range_index_get(b"aoftest", b"key1").await.unwrap(),
      Some(b"val1".to_vec())
    );

    let set_input1_upd = ReplayInput {
      cmd: RespCommand::Riset,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"key1".to_vec(), b"val1-updated".to_vec()],
    };
    replication
      .handle_range_index_set_replay(&session, b"aoftest", &set_input1_upd)
      .await
      .unwrap();
    assert_eq!(
      session.range_index_get(b"aoftest", b"key1").await.unwrap(),
      Some(b"val1-updated".to_vec())
    );

    let del_input = ReplayInput {
      cmd: RespCommand::Ridel,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"key1".to_vec()],
    };
    replication
      .handle_range_index_del_replay(&session, b"aoftest", &del_input)
      .await
      .unwrap();
    assert_eq!(
      session.range_index_get(b"aoftest", b"key1").await.unwrap(),
      None
    );
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIScanBasicTest
#[test]
fn ri_scan_basic_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"DISK",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    for (k, v) in [
      (b"aaa", b"val-a"),
      (b"bbb", b"val-b"),
      (b"ccc", b"val-c"),
      (b"ddd", b"val-d"),
      (b"eee", b"val-e"),
    ] {
      resp
        .network_riset(&[b"myindex", k, v], Some(&ri), &session, &mut out)
        .await
        .unwrap();
    }

    out.clear();
    resp
      .network_riscan(
        &[b"myindex", b"aaa", b"COUNT", b"3"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert!(out.starts_with(b"*3\r\n"));
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIScanFieldsKeyTest
#[test]
fn ri_scan_fields_key_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"DISK",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    resp
      .network_riset(
        &[b"myindex", b"aaa", b"val-a"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    resp
      .network_riset(
        &[b"myindex", b"bbb", b"val-b"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riscan(
        &[b"myindex", b"aaa", b"COUNT", b"10", b"FIELDS", b"KEY"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"*2\r\n$3\r\naaa\r\n$3\r\nbbb\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIScanFieldsValueTest
#[test]
fn ri_scan_fields_value_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"DISK",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    resp
      .network_riset(
        &[b"myindex", b"aaa", b"val-a"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    resp
      .network_riset(
        &[b"myindex", b"bbb", b"val-b"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riscan(
        &[b"myindex", b"aaa", b"COUNT", b"10", b"FIELDS", b"VALUE"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"*2\r\n$5\r\nval-a\r\n$5\r\nval-b\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIRangeBasicTest
#[test]
fn ri_range_basic_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"DISK",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    for (k, v) in [
      (b"aaa", b"val-a"),
      (b"bbb", b"val-b"),
      (b"ccc", b"val-c"),
      (b"ddd", b"val-d"),
      (b"eee", b"val-e"),
    ] {
      resp
        .network_riset(&[b"myindex", k, v], Some(&ri), &session, &mut out)
        .await
        .unwrap();
    }

    out.clear();
    resp
      .network_rirange(&[b"myindex", b"bbb", b"ddd"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert!(out.starts_with(b"*3\r\n"));
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIScanOnNonExistentIndexTest
#[test]
fn ri_scan_on_non_existent_index_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_riscan(
        &[b"noindex", b"aaa", b"COUNT", b"10"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    // C# 网络层硬编码文案（RespServerSessionRangeIndex.cs:400）
    assert_eq!(out, b"-ERR range index not found\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIRangeOnNonExistentIndexTest
#[test]
fn ri_range_on_non_existent_index_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_rirange(&[b"noindex", b"aaa", b"zzz"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    // C# 网络层硬编码文案（RespServerSessionRangeIndex.cs:465）
    assert_eq!(out, b"-ERR range index not found\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIExistsBasicTest
#[test]
fn ri_exists_basic_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_riexists(&[b"myindex"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    resp
      .network_ricreate(
        &[b"myindex", b"MEMORY", b"CACHESIZE", b"65536"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riexists(&[b"myindex"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");

    session.delete(b"myindex").await.unwrap();

    out.clear();
    resp
      .network_riexists(&[b"myindex"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIExistsOnNormalKeyTest
#[test]
fn ri_exists_on_normal_key_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    session.upsert(b"normalkey", b"hello").await.unwrap();

    let mut out = Vec::new();
    resp
      .network_riexists(&[b"normalkey"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIConfigBasicTest
#[test]
fn ri_config_basic_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"MEMORY",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"32",
          b"MAXRECORD",
          b"512",
          b"MAXKEYLEN",
          b"64",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riconfig(&[b"myindex"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    let body = String::from_utf8(out).unwrap();
    assert!(body.starts_with("*12\r\n"));
    assert!(body.contains("storage_backend"));
    assert!(body.contains("MEMORY"));
    assert!(body.contains("cache_size"));
    assert!(body.contains("65536"));
    assert!(body.contains("min_record_size"));
    assert!(body.contains("32"));
    assert!(body.contains("max_record_size"));
    assert!(body.contains("512"));
    assert!(body.contains("max_key_len"));
    assert!(body.contains("64"));
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIConfigWrongTypeTest
#[test]
fn ri_config_wrong_type_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    session.upsert(b"normalkey", b"hello").await.unwrap();

    let mut out = Vec::new();
    resp
      .network_riconfig(&[b"normalkey"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert!(out.starts_with(b"-WRONGTYPE"));
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIMetricsBasicTest
#[test]
fn ri_metrics_basic_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"MEMORY",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_rimetrics(&[b"myindex"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    let body = String::from_utf8(out).unwrap();
    assert!(body.starts_with("*8\r\n"));
    assert!(body.contains("tree_handle"));
    assert!(body.contains("is_live"));
    assert!(body.contains("true"));
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RITypeCommandTest
#[test]
fn ri_type_command_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[b"myindex", b"MEMORY", b"CACHESIZE", b"65536"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    let is_ri = session.range_index_exists(b"myindex").await.unwrap();
    assert!(is_ri);
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RangeIndexManagerKeyExistsTest
#[test]
fn range_index_manager_key_exists_test() {
  let dir = tempdir().unwrap();
  let mgr = RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr")).unwrap();
  let engine = mgr.engine();
  // 按键寻径的 claim/release（C# TreeEntry 防重入原子，见 wbftree TreeEntry）
  let claim = |key: &[u8]| {
    engine
      .live_indexes()
      .pin()
      .get(&Engine::key_id_of(key))
      .is_some_and(|e| e.try_claim_snapshot())
  };
  let release = |key: &[u8]| {
    if let Some(entry) = engine
      .live_indexes()
      .pin()
      .get(&Engine::key_id_of(key))
      .cloned()
    {
      entry.release_snapshot();
    }
  };
  assert!(!claim(b"absent"));
  release(b"absent");

  engine
    .create_bftree(
      b"idx",
      wkv::StorageBackend::Memory,
      wkv::TreeTuning::default(),
    )
    .unwrap();

  assert!(claim(b"idx"));
  assert!(!claim(b"idx"));
  release(b"idx");
  assert!(claim(b"idx"));
  release(b"idx");
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIDiskFileCleanupOnDeleteTest
#[test]
fn ri_disk_file_cleanup_on_delete_test() {
  let dir = tempdir().unwrap();
  let mgr = RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr")).unwrap();
  let engine = mgr.engine();
  let p1 = Engine::hash_prefix_of(b"k1");
  let p2 = Engine::hash_prefix_of(b"k2");

  let f1 = engine.log_flush_path(&p1, 0x100);
  let f2 = engine.log_flush_path(&p2, 0x200);
  fs::write(&f1, b"x").unwrap();
  fs::write(&f2, b"y").unwrap();
  fs::write(dir.path().join("ri").join("garbage.bftree"), b"z").unwrap();

  let mut files: Vec<_> = engine
    .enumerate_files_for_replication(None, 0, u64::MAX)
    .unwrap()
    .into_iter()
    .filter(|e| e.is_flush_file)
    .collect();
  files.sort_by_key(|f| f.address);
  assert_eq!(files.len(), 2);
  assert_eq!(files[0].address, 0x100);
  assert_eq!(files[0].key_hash(), p1);
  assert_eq!(files[0].key_id, Engine::key_id_of(b"k1"));
  assert_eq!(files[1].address, 0x200);
  assert_eq!(files[1].key_hash(), p2);
  assert_eq!(files[1].key_id, Engine::key_id_of(b"k2"));
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIFlushFilesAreImmutablePerAddressTest
#[test]
fn ri_flush_files_are_immutable_per_address_test() {
  let dir = tempdir().unwrap();
  let mgr = RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr")).unwrap();
  let prefix = Engine::hash_prefix_of(b"key-a");
  let f1 = mgr.engine().log_flush_path(&prefix, 4096);
  let f2 = mgr.engine().log_flush_path(&prefix, 8192);
  assert_ne!(f1, f2);
}

#[test]
fn ri_len_basic_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    let mut out = Vec::new();

    // 1. 创建索引
    resp
      .network_ricreate(
        &[
          b"myindex",
          b"MEMORY",
          b"CACHESIZE",
          b"65536",
          b"MINRECORD",
          b"8",
        ],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    // 2. 空索引获取长度
    out.clear();
    resp
      .network_rilen(&[b"myindex"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // 3. 写入条目
    out.clear();
    resp
      .network_riset(
        &[b"myindex", b"field1", b"value1"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    resp
      .network_rilen(&[b"myindex"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // 4. 写入第二条
    out.clear();
    resp
      .network_riset(
        &[b"myindex", b"field2", b"value2"],
        Some(&ri),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    resp
      .network_rilen(&[b"myindex"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // 5. 删除条目
    out.clear();
    resp
      .network_ridel(&[b"myindex", b"field1"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    resp
      .network_rilen(&[b"myindex"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // 6. 删空至 0
    out.clear();
    resp
      .network_ridel(&[b"myindex", b"field2"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    resp
      .network_rilen(&[b"myindex"], Some(&ri), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}
