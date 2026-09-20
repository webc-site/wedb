//! StoreGarnetApi 命令分派集成测试（自 src/resp/garnet_api.rs 单元测试外迁）
//!
//! 全链路真存储读写：Vector Set 分派、Etag 族条件语义、RI 族慢路径挂起。

use std::sync::{Arc, atomic::Ordering};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, GarnetApiFace, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
  vector::{
    vector_manager::{VectorManager, VectorManagerOptions},
    vector_store_callbacks::WedbVectorStoreCallbacks,
  },
};
use wresp::command::RespCommand;
use wvector::Callbacks;

/// 小容量单文件存储（测试配置：1024 索引桶 / 64KB 内存页）
fn open_store(path: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(path)).unwrap());
  Arc::new(WedbStore::open(config, device).unwrap())
}

#[test]
fn store_garnet_api_vector_dispatch() {
  let store = open_store("v.db");
  let session = store.new_session().unwrap();

  let v_session = Arc::new(store.new_session().unwrap());
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(v_session)));
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    callbacks,
  ));
  let api = StoreGarnetApi::new(session).with_vector_manager(vm);

  let mut resp_session = RespServerSession::new(1, RespServerSessionOptions::default());

  // 1. 执行 VADD
  let v1 = [0u8; 8];
  api.exec(
    &mut resp_session,
    RespCommand::Vadd,
    &[b"mykey", b"FP32", &v1, b"ele1"],
  );
  assert_eq!(resp_session.output, b":1\r\n");

  // 2. 执行 VCARD
  resp_session.output.clear();
  api.exec(&mut resp_session, RespCommand::Vcard, &[b"mykey"]);
  assert_eq!(resp_session.output, b":1\r\n");

  // 3. 执行 VDIM
  resp_session.output.clear();
  api.exec(&mut resp_session, RespCommand::Vdim, &[b"mykey"]);
  assert_eq!(resp_session.output, b":2\r\n");
}

#[test]
fn etag_family_dispatch_via_store() {
  // Etag 族接入分派表（C# ProcessOtherCommands 段）：SETWITHETAG →
  // GETWITHETAG → DELIFGREATER 全链路真存储读写（条件语义对标
  // BasicEtagCommands.cs：应答 [etag, value] 数组与 etag 真值推进）
  let store = open_store("e.db");
  let session = store.new_session().unwrap();
  let api = StoreGarnetApi::new(session);
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());

  // SETWITHETAG key val → 应答新 etag 整数（初始 NoETag + 1 = 1）
  api.exec(&mut s, RespCommand::Setwithetag, &[b"k", b"v1"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  // GETWITHETAG key → [etag, val] 双元素数组（etag 在前）
  api.exec(&mut s, RespCommand::Getwithetag, &[b"k"]);
  assert_eq!(s.output, b"*2\r\n:1\r\n$2\r\nv1\r\n");
  s.output.clear();

  // DELIFGREATER key 1 → etag 不严格大于（1 == 1）：不删除
  api.exec(&mut s, RespCommand::Delifgreater, &[b"k", b"1"]);
  assert_eq!(s.output, b":0\r\n");
  s.output.clear();

  // DELIFGREATER key 2 → 2 > 1：真实删除
  api.exec(&mut s, RespCommand::Delifgreater, &[b"k", b"2"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  // 删除后再读 → null（etag 标签随键级联清理，不留残值）
  api.exec(&mut s, RespCommand::Getwithetag, &[b"k"]);
  assert_eq!(s.output, b"$-1\r\n");
}

#[test]
fn ri_commands_suspend_to_slow_path() {
  // RI 族经 dispatch_slow 返回 Ok(false)：同步段挂起 SlowWait 停止本批
  // 消费，网络泵 await 闭环（compio 异步存储路径的统一承接形态）
  let store = open_store("ri.db");
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());

  api.exec(&mut s, RespCommand::Riget, &[b"idx", b"f"]);
  // 应答不残留（本次无输出），挂起慢路径执行体由网络泵驱动
  assert!(s.output.is_empty());
  // 经 pub 取走口验证挂起态（等价原 pub(crate) 字段直读）
  assert!(s.take_slow_wait().is_some());
}

#[test]
fn debug_flushandevict_slow_exec() {
  let store = open_store("debug_fae.db");
  let session = store.new_session().unwrap();
  let api = Arc::new(StoreGarnetApi::new(session));
  let rt = Runtime::new().unwrap();

  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Debug,
    vec![b"FLUSHANDEVICT".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert!(out.starts_with(b"+OK head="));

  // 非法子命令回 ERR command requires asynchronous completion
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Debug,
    vec![b"UNKNOWN".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"-ERR command requires asynchronous completion\r\n");
}

#[test]
fn hcollect_star_mutex_rejects_reentry() {
  let store = open_store("hc.db");
  let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let rt = Runtime::new().unwrap();

  // 置位 HCOLLECT 在途标志：重入被拒，映射常量文案
  api.hcollect_in_progress.store(true, Ordering::SeqCst);
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Hcollect,
    vec![b"*".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(
    String::from_utf8_lossy(&out),
    "-ERR HCOLLECT scan already in progress\r\n"
  );

  // ZCOLLECT 独立锁位：HCOLLECT 在途不拦 ZCOLLECT（空库扫描闭环 +OK）
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Zcollect,
    vec![b"*".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"+OK\r\n");

  // ZCOLLECT 在途：ZCOLLECT 重入同拒
  api.zcollect_in_progress.store(true, Ordering::SeqCst);
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Zcollect,
    vec![b"*".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(
    String::from_utf8_lossy(&out),
    "-ERR ZCOLLECT scan already in progress\r\n"
  );

  // 释放后恢复正常闭环（空库扫描 +OK，标志复位）
  api.hcollect_in_progress.store(false, Ordering::SeqCst);
  api.zcollect_in_progress.store(false, Ordering::SeqCst);
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Hcollect,
    vec![b"*".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"+OK\r\n");
  assert!(!api.hcollect_in_progress.load(Ordering::SeqCst));
  assert!(!api.zcollect_in_progress.load(Ordering::SeqCst));
}

#[test]
fn expdelscan_slow_invalid_db_index_returns_integer_error() {
  let store = open_store("expdelscan_err.db");
  let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let rt = Runtime::new().unwrap();

  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Expdelscan,
    vec![b"not_an_int".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
}
