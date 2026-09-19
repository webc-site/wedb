use std::{fs, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::{
  BfTreeInsertResult, RangeIndexManager as Engine, RangeIndexStub, ScanReturnField,
  StorageBackendType, TreeTuning,
};
use wdev::SegmentedDevice;
use wkv::{RangeIndexError, StoreSession};
use wnode::{
  aof::replay_input::ReplayInput,
  rangeindex::range_index_manager_replication::RangeIndexManagerReplication,
  resp::{
    key_admin_commands::{ExpireCmd, TtlCmd},
    resp_server_session::RespServerSession,
  },
  storage::session::storage_session::StorageSession,
};
use wnode_test::test_env;
use wresp::command::RespCommand;

type TestEnv = (
  tempfile::TempDir,
  Arc<Engine>,
  StoreSession<SegmentedDevice>,
  RespServerSession,
);

/// 统一 setup（support::test_env(true)）+ 存储域 range_index 接线
/// （与生产 garnet_api 的 `store.range_index` 单一装配入口一致）
fn setup_env() -> TestEnv {
  let (dir, session, resp) = test_env(true);
  let ri = session.store.range_index.clone();
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
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    // 完整错误帧以常量派生（resp-error-cmd-dedup 后常量为裸文案，不带 - 前缀，
    // starts_with 直接比对会漏掉 RESP 错误帧首字节）
    assert_eq!(
      out,
      wnode_test::err_frame(wbftree::ERR_INDEX_ALREADY_EXISTS),
      "重复创建同名索引应回 ERR index already exists 错误帧"
    );
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
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    // C# storage 层 errorMsg 文案（RangeIndexOps.cs:212/220）
    assert_eq!(out, b"-ERR no such range index\r\n");

    out.clear();
    resp
      .network_riget(
        &[b"myindex", b"field1"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
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
      .network_ricreate(
        &[b"myindex", b"MEMORY"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
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
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    resp
      .network_riget(
        &[b"myindex", b"field1"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riset(
        &[b"myindex", b"field1", b"value1"],
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    resp
      .network_riget(
        &[b"myindex", b"field1"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riget(
        &[b"myindex", b"nosuchfield"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
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
      .network_riget(
        &[b"noindex", b"field1"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    // C# 网络层硬编码文案（RespServerSessionRangeIndex.cs:278）
    assert_eq!(out, b"-ERR range index not found\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIDelFieldTest
///
/// 本仓语义补充：删空最后一字段即触发自愈（RI.DEL 删空臂接树态排空回收单点，
/// 见 wkv range_index/ops.rs:range_index_del），索引本体随之消亡，后续 RI.GET
/// 由「字段 null」转为「索引 not found」；字段级删除语义在非删空臂原样保留
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riset(
        &[b"myindex", b"field1", b"value1"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    resp
      .network_riset(
        &[b"myindex", b"field2", b"value2"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    // 非删空臂：删 field1 后索引存活，字段级 null 应答原样（C# 断言本体）
    out.clear();
    resp
      .network_ridel(
        &[b"myindex", b"field1"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    resp
      .network_riget(
        &[b"myindex", b"field1"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    out.clear();
    resp
      .network_riget(
        &[b"myindex", b"field2"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"$6\r\nvalue2\r\n");

    // 删空臂：计数归零自愈，整键消亡（与 RI.GET 打不存在索引同文案）
    out.clear();
    resp
      .network_ridel(
        &[b"myindex", b"field2"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    resp
      .network_riget(
        &[b"myindex", b"field2"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"-ERR range index not found\r\n");
  });
}

/// RI.DEL 删空自愈验收（.agents/skills/transpile/SKILL.md 严格删空生命周期条 /
/// doc/zh/collection.md 3.3，六类集合含 RangeIndex 同一口径）：逐字段删至计数归零
/// 后 EXISTS=0、KEYS/SCAN 不再列出、RI.COUNT 与 RI.METRICS 报 not found、树实例
/// 与 DISK 树数据文件释放、随键 TTL 旁路记录消失；非删空臂零行为回归
#[test]
fn ri_del_to_empty_self_heal_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    create_disk_index(&mut resp, ri.as_ref(), &session, b"idx").await;
    for f in [b"aa".as_slice(), b"bb".as_slice()] {
      ri_set(&mut resp, ri.as_ref(), &session, b"idx", f).await;
    }
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":2\r\n"
    );

    // 随键 TTL 挂上（自愈臂必须连旁路 TTL 一并清除，杜绝孤儿 TTL）
    let batch = session.enter_batch();
    let mut out = Vec::new();
    resp
      .network_expire(ExpireCmd::Expire, &[b"idx", b"100"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    let ss = StorageSession::new(batch);

    let data_files = || -> usize {
      fs::read_dir(ri.ri_log_root())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".data.bftree"))
        .count()
    };

    // 删空前基态：EXISTS/KEYS/SCAN 均见该键，TTL 旁路记录与 DISK 树数据文件在位
    assert_eq!(ss.db_size().await.unwrap(), 1);
    assert_eq!(ss.db_keys(b"*").await.unwrap(), vec![b"idx".to_vec()]);
    assert_eq!(
      ss.scan_cursor(b"*", true, 0, 10, None).await.unwrap().1,
      vec![b"idx".to_vec()]
    );
    assert!(session.contains_key(b"idx").await.unwrap());
    assert!(session.ttl_of(b"idx").await.unwrap().is_some());
    assert_eq!(data_files(), 1, "前置条件：DISK 树数据文件在位");
    assert!(ri.get_tree(b"idx").is_some(), "前置条件：树实例在线");

    // 非删空臂：索引存活，键面/计数/TTL/树文件一律原样（零行为回归）
    out.clear();
    resp
      .network_ridel(&[b"idx", b"aa"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":1\r\n"
    );
    assert_eq!(ss.db_keys(b"*").await.unwrap(), vec![b"idx".to_vec()]);
    assert_eq!(data_files(), 1, "非删空删除不得释放树文件");

    // 删空臂：计数归零即走树态排空回收单点，整键残留一次性收口
    out.clear();
    resp
      .network_ridel(&[b"idx", b"bb"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");

    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b"-ERR range index not found\r\n",
      "删空后计数须报 not found，绝不回 :0 幽灵索引"
    );
    out.clear();
    resp
      .network_rimetrics(&[b"idx"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"-ERR range index not found\r\n", "树句柄随键消亡");
    assert_eq!(ss.db_size().await.unwrap(), 0);
    assert!(ss.db_keys(b"*").await.unwrap().is_empty(), "KEYS 不得列出");
    assert!(
      ss.scan_cursor(b"*", true, 0, 10, None)
        .await
        .unwrap()
        .1
        .is_empty(),
      "SCAN 不得列出"
    );
    assert!(!session.contains_key(b"idx").await.unwrap(), "EXISTS 口径");
    assert!(
      session.ttl_of(b"idx").await.unwrap().is_none(),
      "随键 TTL 旁路记录须消失"
    );
    assert!(
      ri.get_tree(b"idx").is_none(),
      "树实例必须随 drain 注销（不得永驻句柄）"
    );
    // 数据文件的物理 unlink 由 drain 的跨纪元延迟回收动作执行（会话在场即
    // 让在途读者先退出），引擎级即时释放由本文件
    // ri_disk_file_cleanup_on_delete_test 权威覆盖；此处以「同名重建放行」
    // 断言注册表与换号旁表登记注销已同步完成

    // 元记录彻底消失（非 size=0 的空记录）
    assert!(session.load_meta(b"idx").await.unwrap().is_none());
    // 自愈后同名重建放行：换号旁表登记已随 drain 注销，不被 IndexExists 拦截
    create_disk_index(&mut resp, ri.as_ref(), &session, b"idx").await;
    ri_set(&mut resp, ri.as_ref(), &session, b"idx", b"aa").await;
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":1\r\n"
    );
    out.clear();
    resp
      .network_riget(&[b"idx", b"aa"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(
      out, b"$7\r\npayload\r\n",
      "自愈后同名重建的树必须实际可读写（不得撞陈旧数据文件）"
    );
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
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    resp
      .network_riset(
        &[b"myindex", b"aaa", b"val-a"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    resp
      .network_riset(
        &[b"myindex", b"bbb", b"val-b"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    resp
      .network_riset(
        &[b"myindex", b"ccc", b"val-c"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riget(&[b"myindex", b"aaa"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"$5\r\nval-a\r\n");

    out.clear();
    resp
      .network_riget(&[b"myindex", b"bbb"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"$5\r\nval-b\r\n");

    out.clear();
    resp
      .network_riget(&[b"myindex", b"ccc"], Some(ri.as_ref()), &session, &mut out)
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
        Some(ri.as_ref()),
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
      .network_riget(
        &[b"normalkey", b"field1"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
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
        Some(ri.as_ref()),
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

    let stub = RangeIndexStub::new(0, 65536, 8, 1024, 128, 0, StorageBackendType::Memory);
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

    let set_input2 = ReplayInput {
      cmd: RespCommand::Riset,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"key2".to_vec(), b"val2".to_vec()],
    };
    replication
      .handle_range_index_set_replay(&session, b"aoftest", &set_input2)
      .await
      .unwrap();

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
    // 非删空回放臂：索引存活、计数递减，行为零回归
    assert!(session.range_index_exists(b"aoftest").await.unwrap());
    assert_eq!(session.range_index_count(b"aoftest").await.unwrap(), 1);

    // 删空回放臂与主臂同源（同一 range_index_del 内核）：回放侧计数归零即自愈，
    // 整键消亡不留幽灵空元记录
    let del_last = ReplayInput {
      cmd: RespCommand::Ridel,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"key2".to_vec()],
    };
    replication
      .handle_range_index_del_replay(&session, b"aoftest", &del_last)
      .await
      .unwrap();
    assert!(!session.range_index_exists(b"aoftest").await.unwrap());
    assert!(matches!(
      session.range_index_count(b"aoftest").await,
      Err(RangeIndexError::NotFound)
    ));
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
        Some(ri.as_ref()),
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
        .network_riset(&[b"myindex", k, v], Some(ri.as_ref()), &session, &mut out)
        .await
        .unwrap();
    }

    out.clear();
    resp
      .network_riscan(
        &[b"myindex", b"aaa", b"COUNT", b"3"],
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    resp
      .network_riset(
        &[b"myindex", b"aaa", b"val-a"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    resp
      .network_riset(
        &[b"myindex", b"bbb", b"val-b"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riscan(
        &[b"myindex", b"aaa", b"COUNT", b"10", b"FIELDS", b"KEY"],
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    resp
      .network_riset(
        &[b"myindex", b"aaa", b"val-a"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    resp
      .network_riset(
        &[b"myindex", b"bbb", b"val-b"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riscan(
        &[b"myindex", b"aaa", b"COUNT", b"10", b"FIELDS", b"VALUE"],
        Some(ri.as_ref()),
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
        Some(ri.as_ref()),
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
        .network_riset(&[b"myindex", k, v], Some(ri.as_ref()), &session, &mut out)
        .await
        .unwrap();
    }

    out.clear();
    resp
      .network_rirange(
        &[b"myindex", b"bbb", b"ddd"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
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
        Some(ri.as_ref()),
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
      .network_rirange(
        &[b"noindex", b"aaa", b"zzz"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
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
      .network_riexists(&[b"myindex"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    resp
      .network_ricreate(
        &[b"myindex", b"MEMORY", b"CACHESIZE", b"65536"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riexists(&[b"myindex"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");

    session.delete(b"myindex").await.unwrap();

    out.clear();
    resp
      .network_riexists(&[b"myindex"], Some(ri.as_ref()), &session, &mut out)
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
      .network_riexists(&[b"normalkey"], Some(ri.as_ref()), &session, &mut out)
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_riconfig(&[b"myindex"], Some(ri.as_ref()), &session, &mut out)
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
      .network_riconfig(&[b"normalkey"], Some(ri.as_ref()), &session, &mut out)
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    out.clear();
    resp
      .network_rimetrics(&[b"myindex"], Some(ri.as_ref()), &session, &mut out)
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
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();

    let is_ri = session.range_index_exists(b"myindex").await.unwrap();
    assert!(is_ri);

    let batch = session.enter_batch();
    out.clear();
    resp
      .network_type(&[b"myindex"], &batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+rangeindex\r\n");

    out.clear();
    resp
      .network_exists(&[b"myindex"], &batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    resp
      .network_ttl(TtlCmd::Ttl, &[b"myindex"], &batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":-1\r\n");

    let ss = StorageSession::new(batch);

    assert_eq!(ss.db_size().await.unwrap(), 1);
    let keys = ss.db_keys(b"*").await.unwrap();
    assert_eq!(keys, vec![b"myindex".to_vec()]);

    let (next_cur, scan_keys) = ss.scan_cursor(b"*", true, 0, 10, None).await.unwrap();
    assert_eq!(next_cur, 0);
    assert_eq!(scan_keys, vec![b"myindex".to_vec()]);

    let batch = session.enter_batch();
    out.clear();
    resp
      .network_expire(ExpireCmd::Expire, &[b"myindex", b"100"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    resp
      .network_persist(&[b"myindex"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RangeIndexManagerKeyExistsTest
#[test]
fn range_index_manager_key_exists_test() {
  let dir = tempdir().unwrap();
  let engine = Engine::new(dir.path().join("ri"), dir.path().join("cpr")).unwrap();
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
    .create_bftree(b"idx", StorageBackendType::Memory, TreeTuning::default())
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
  let engine = Arc::new(Engine::new(dir.path().join("ri"), dir.path().join("cpr")).unwrap());

  // RI.CREATE DISK + RI.SET：riLogRoot 下恰有一个 <hash>.data.bftree 工作文件
  let tree = engine
    .create_bftree(b"cleanup", StorageBackendType::Disk, TreeTuning::default())
    .unwrap();
  assert_eq!(tree.insert(b"key1", b"val1"), BfTreeInsertResult::Success);
  let count_data_files = || {
    fs::read_dir(engine.ri_log_root())
      .unwrap()
      .flatten()
      .filter(|e| e.file_name().to_string_lossy().ends_with(".data.bftree"))
      .count()
  };
  assert_eq!(count_data_files(), 1, "RI.CREATE 后应恰有一个 data.bftree");

  // DEL：工作文件清理；本用例未触发 flush 事件，刷盘快照不参与断言
  // (刷盘快照的日志生命周期见 RIFlushFilesAreImmutablePerAddressTest)
  assert!(engine.delete_index(b"cleanup").unwrap());
  assert_eq!(count_data_files(), 0, "DEL 之后 data.bftree 必须删除");
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIFlushFilesAreImmutablePerAddressTest
#[test]
fn ri_flush_files_are_immutable_per_address_test() {
  let dir = tempdir().unwrap();
  let engine = Engine::new(dir.path().join("ri"), dir.path().join("cpr")).unwrap();
  let prefix = Engine::base32_prefix_of(b"key-a");
  let f1 = engine.log_flush_path(&prefix, 4096);
  let f2 = engine.log_flush_path(&prefix, 8192);
  assert_ne!(f1, f2);
}

// ==== RI.COUNT 计数面（rust 侧自定义扩展）====
//
// C# 无对应处理器与测试：libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs
// 只有 CREATE/SET/GET/DEL/SCAN/RANGE/EXISTS/CONFIG/METRICS 九个，全仓亦无
// RILEN / RICOUNT 检索命中。以下用例据 .agents/skills/transpile/SKILL.md
// 「O(1) 复杂度计数规约」与 doc/zh/collection.md 第 6 节承诺编写。

/// 建 DISK 索引（MINRECORD 8 与本文件既有 C# 移植用例同口径）
async fn create_disk_index(
  resp: &mut RespServerSession,
  ri: &Engine,
  session: &StoreSession<SegmentedDevice>,
  key: &[u8],
) {
  let mut out = Vec::new();
  resp
    .network_ricreate(
      &[key, b"DISK", b"CACHESIZE", b"65536", b"MINRECORD", b"8"],
      Some(ri),
      session,
      &mut out,
    )
    .await
    .unwrap();
  assert_eq!(out, b"+OK\r\n");
}

/// RI.SET 单字段（应答须为 +OK）
async fn ri_set(
  resp: &mut RespServerSession,
  ri: &Engine,
  session: &StoreSession<SegmentedDevice>,
  key: &[u8],
  field: &[u8],
) {
  let mut out = Vec::new();
  resp
    .network_riset(&[key, field, b"payload"], Some(ri), session, &mut out)
    .await
    .unwrap();
  assert_eq!(out, b"+OK\r\n");
}

/// RI.COUNT 应答字节
async fn ri_count(
  resp: &mut RespServerSession,
  ri: &Engine,
  session: &StoreSession<SegmentedDevice>,
  key: &[u8],
) -> Vec<u8> {
  let mut out = Vec::new();
  resp
    .network_ricount(&[key], Some(ri), session, &mut out)
    .await
    .unwrap();
  out
}

/// 取 RI.METRICS 应答中 `is_live` 字段的 bulk 值原文
fn metrics_is_live(out: &[u8]) -> Option<&'static str> {
  let live_field = b"$7\r\nis_live\r\n".as_slice();
  let at = out
    .windows(live_field.len())
    .position(|w| w == live_field)
    .map(|i| i + live_field.len())?;
  match &out[at..] {
    [b'$', b'4', b'\r', b'\n', ..] => Some("true"),
    [b'$', b'5', b'\r', b'\n', ..] => Some("false"),
    _ => None,
  }
}

/// RI.COUNT 计数语义（本仓自定义扩展，C# 无对应测试）：空索引 :0、写入递增、
/// 覆盖写不增长、RI.DEL 递减、删空自愈销毁整键后报 "ERR range index not found"
/// （与整键删除同口径，绝不残留 :0 幽灵索引）、普通字符串键报 WRONGTYPE、
/// 参数个数守卫、未启用 RI 预览时的门
#[test]
fn ri_count_basic_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    create_disk_index(&mut resp, ri.as_ref(), &session, b"idx").await;

    // 空索引：MetaValue.size 初值 0
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":0\r\n"
    );

    // 逐字段写入：计数同步增长
    for f in [b"aa".as_slice(), b"bb".as_slice(), b"cc".as_slice()] {
      ri_set(&mut resp, ri.as_ref(), &session, b"idx", f).await;
    }
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":3\r\n"
    );

    // 覆盖写不增长
    ri_set(&mut resp, ri.as_ref(), &session, b"idx", b"aa").await;
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":3\r\n"
    );

    // RI.DEL 递减
    let mut out = Vec::new();
    resp
      .network_ridel(&[b"idx", b"bb"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":1\r\n");
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":2\r\n"
    );

    // 删空：计数归零即自愈销毁整键（RI 型 MetaValue::is_live 恒活，删空判据只能
    // 取 size，见 wkv range_index/ops.rs:range_index_del 删空臂）
    for f in [b"aa".as_slice(), b"cc".as_slice()] {
      out.clear();
      resp
        .network_ridel(&[b"idx", f], Some(ri.as_ref()), &session, &mut out)
        .await
        .unwrap();
      assert_eq!(out, b":1\r\n");
    }
    // 删空后不得回 :0 的幽灵空索引——计数与 RI.EXISTS 一律报索引不存在
    //（与整键删除同文案，杜绝 EXISTS 报空而 KEYS 仍列出）
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b"-ERR range index not found\r\n"
    );
    out.clear();
    resp
      .network_riexists(&[b"idx"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b":0\r\n");
    assert!(
      !session.contains_key(b"idx").await.unwrap(),
      "删空自愈后 EXISTS 口径不得残留键"
    );
    // 自愈后整键删除幂等回 false（键已消亡，无残留可删）
    assert!(!session.delete(b"idx").await.unwrap());

    // 普通字符串键：WRONGTYPE
    session.upsert(b"plain", b"v").await.unwrap();
    let got = ri_count(&mut resp, ri.as_ref(), &session, b"plain").await;
    assert!(
      got.starts_with(b"-WRONGTYPE"),
      "WRONGTYPE expected, got {got:?}"
    );

    // 参数个数守卫（canonical 名 RI.COUNT）
    out.clear();
    resp
      .network_ricount(&[], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'RI.COUNT' command\r\n"
    );

    // RI 预览未启用（ri = None）：网络层门优先于任何计数
    out.clear();
    resp
      .network_ricount(&[b"idx"], None::<&Engine>, &session, &mut out)
      .await
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Range Index (preview) commands are not enabled\r\n"
    );
  });
}

/// RI.COUNT 的 O(1) 硬指标探针（本仓自定义扩展，C# 无对应测试）：
/// 故障注入 `RangeIndexManager::delete_index` 摘除注册表条目与树文件后，
/// 计数仍返回 MetaValue.size 原值，且紧接的 RI.METRICS 报 is_live false——
/// 计数若走 acquire_tree_read / 扫树，`get_or_open_tree` 必重建注册表条目，
/// is_live 就翻回 true（用例前后两次读同一指标，证明该探针可正可负，
/// 不是恒 false 的自证断言）
#[test]
fn ri_count_o1_never_touches_tree_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    create_disk_index(&mut resp, ri.as_ref(), &session, b"idx").await;
    for f in [
      b"aa".as_slice(),
      b"bb".as_slice(),
      b"cc".as_slice(),
      b"dd".as_slice(),
      b"ee".as_slice(),
    ] {
      ri_set(&mut resp, ri.as_ref(), &session, b"idx", f).await;
    }

    let mut out = Vec::new();
    resp
      .network_rimetrics(&[b"idx"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(metrics_is_live(&out), Some("true"));
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":5\r\n"
    );

    // 故障注入：注册表条目与数据文件双摘除（树侧彻底不可用）
    assert!(ri.delete_index(b"idx").unwrap());
    assert!(ri.get_tree(b"idx").is_none(), "前置条件：树条目已摘除");

    // 计数仍取元数据实况
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":5\r\n",
      "计数不得因树下线而失真"
    );

    // 计数未唤醒树：is_live 仍 false
    out.clear();
    resp
      .network_rimetrics(&[b"idx"], Some(ri.as_ref()), &session, &mut out)
      .await
      .unwrap();
    assert_eq!(
      metrics_is_live(&out),
      Some("false"),
      "计数不得唤醒已摘除的树（O(1) 直读元数据探针）"
    );
    assert!(ri.get_tree(b"idx").is_none());
  });
}

/// 部分区间计数走真实迭代，不经 MetaValue.size（本仓自定义扩展口径：
/// 全区间计数只有 RI.COUNT 一个入口，区间计数由 RI.SCAN / RI.RANGE 的
/// FIELDS KEY 纯键投影承担）：区间应答与存储层逐键枚举一致，且区间结果
/// 恒小于全区间元数据计数
#[test]
fn ri_partial_range_count_goes_through_key_projection_test() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, ri, session, mut resp) = setup_env();
    create_disk_index(&mut resp, ri.as_ref(), &session, b"idx").await;
    for f in [
      b"aa".as_slice(),
      b"bb".as_slice(),
      b"cc".as_slice(),
      b"dd".as_slice(),
      b"ee".as_slice(),
      b"ff".as_slice(),
    ] {
      ri_set(&mut resp, ri.as_ref(), &session, b"idx", f).await;
    }
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":6\r\n"
    );

    // 闭区间 [bb, dd] 的纯键投影
    let mut out = Vec::new();
    resp
      .network_rirange(
        &[b"idx", b"bb", b"dd", b"FIELDS", b"KEY"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"*3\r\n$2\r\nbb\r\n$2\r\ncc\r\n$2\r\ndd\r\n");

    // 与存储层独立逐键枚举逐字节一致
    let mut enumerated: Vec<Vec<u8>> = Vec::new();
    session
      .range_index_range_stream(b"idx", b"bb", b"dd", ScanReturnField::Key, |k, _| {
        enumerated.push(k.to_vec());
        true
      })
      .await
      .unwrap();
    assert_eq!(
      enumerated,
      vec![b"bb".to_vec(), b"cc".to_vec(), b"dd".to_vec()]
    );
    assert_ne!(
      enumerated.len(),
      6,
      "区间计数绝不得退化为全区间 MetaValue.size"
    );

    // RI.SCAN COUNT 2 为真实迭代截断（元数据短路会给不出 2）
    out.clear();
    resp
      .network_riscan(
        &[b"idx", b"aa", b"COUNT", b"2", b"FIELDS", b"KEY"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"*2\r\n$2\r\naa\r\n$2\r\nbb\r\n");

    // 空区间：迭代返回 0 条，而全区间计数仍为元数据 6
    out.clear();
    resp
      .network_rirange(
        &[b"idx", b"zz", b"zz", b"FIELDS", b"KEY"],
        Some(ri.as_ref()),
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(out, b"*0\r\n");
    assert_eq!(
      ri_count(&mut resp, ri.as_ref(), &session, b"idx").await,
      b":6\r\n"
    );
  });
}
