//! 端到端集成测试：INFO HLOGSCAN 段（混合日志内存分布）
//!
//! 对标 C# PopulateHlogScanInfo → storeWrapper.HybridLogDistributionScan →
//! GetSectionRespInfo("MainStoreHLogScan_DB_{dbId}") 链路：纯显式 hlogscan
//! 段请求经会话降级慢路径（SlowWait 异步闭环），经数据库管理面拉取存储域
//! 分布扫描，段文本含 `MainStore_HLog_{i}` / `ObjectStore_HLog_{i}` 条目，
//! 空转储呈现 Empty（单物理日志形态：对象存储槽恒 Empty）
use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::pump;
use wtest_base::test_store_config;

/// INFO hlogscan 慢路径 RESP 帧（RESP2 数组：INFO hlogscan）
const INFO_HLOGSCAN: &[u8] = b"*2\r\n$4\r\nINFO\r\n$8\r\nhlogscan\r\n";

/// 装配带真存储执行域 + 常驻单库管理器的会话消费者（每测试独立临时目录，
/// GC 关闭）
fn consumer() -> (Runtime, RespSessionConsumer) {
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("hlogscan.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  let session = store.new_session().unwrap();
  // 常驻单库管理器（DB 0，对标 wnode service.rs 装配形态）
  let db = Arc::new(GarnetDatabase::<SegmentedDevice>::new(
    0,
    Arc::clone(&store),
    Arc::clone(&device),
    dir.clone(),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(dir, db));
  let api = StoreGarnetApi::new(session).with_database_manager(mgr);
  (
    rt,
    RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api)),
  )
}

/// 慢命令往返：同步段消费（挂起）→ block_on 承担网络泵 await 闭环
fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// SET 若干键（值均为单字节 "v"）
fn set_keys(c: &mut RespSessionConsumer, keys: &[&str]) {
  for k in keys {
    let frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len());
    let (consumed, out) = pump(c, frame.as_bytes());
    assert_eq!(consumed, Some(0));
    assert!(out.starts_with(b"+OK"), "{:?}", from_utf8(&out));
  }
}

/// 空日志：段头 + MainStore/ObjectStore 双条目均呈现 Empty
#[test]
fn info_hlogscan_empty_log() {
  let (rt, mut c) = consumer();
  let out = slow_roundtrip(&rt, &mut c, INFO_HLOGSCAN);
  let text = from_utf8(&out).unwrap();
  assert!(text.contains("# MainStoreHLogScan_DB_0\r\n"), "{text}");
  assert!(text.contains("MainStore_HLog_0:Empty"), "{text}");
  assert!(text.contains("ObjectStore_HLog_0:Empty"), "{text}");
}

/// 写入 + 覆盖 + 删除后：MainStore 条目非 Empty 且含 Live/被取代/墓碑分布；
/// 对象存储槽保持 Empty（wedb 单物理日志统一值域）
#[test]
fn info_hlogscan_distribution_after_writes() {
  let (rt, mut c) = consumer();

  set_keys(&mut c, &["k1", "k2", "k3"]);
  // k1 变长覆盖 → 旧版成被取代记录
  let (consumed, out) = pump(&mut c, b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$4\r\nwwww\r\n");
  assert_eq!(consumed, Some(0));
  assert!(out.starts_with(b"+OK"));
  // DEL k3 → 墓碑
  let (consumed, out) = pump(&mut c, b"*2\r\n$3\r\nDEL\r\n$2\r\nk3\r\n");
  assert_eq!(consumed, Some(0));
  assert_eq!(out, b":1\r\n");

  let out = slow_roundtrip(&rt, &mut c, INFO_HLOGSCAN);
  let text = from_utf8(&out).unwrap();
  assert!(text.contains("# MainStoreHLogScan_DB_0\r\n"), "{text}");
  let main = text
    .split("\r\n")
    .find(|l| l.starts_with("MainStore_HLog_0:"))
    .unwrap_or_else(|| panic!("缺 MainStore 条目: {text}"));
  assert!(!main.ends_with("Empty"), "主存储转储不得为空: {text}");
  assert!(main.contains("State: Live, Count: 2"), "{text}");
  assert!(main.contains("State: RCUdUnsealed, Count: 1"), "{text}");
  assert!(main.contains("State: Tombstoned, Count: 1"), "{text}");
  assert!(main.contains("# Region: Mutable"), "{text}");
}

/// 混合段名请求（INFO server hlogscan）整请求降级慢路径（工单
/// zcode-r126c-infosec1 案一：all 语义 → any 语义）：hlogscan 段经扫描
/// 实填分布条目，server 段经调度点 InfoSurface 快照真值渲染
#[test]
fn info_mixed_sections_demote_to_slow_path() {
  let (rt, mut c) = consumer();
  set_keys(&mut c, &["k1"]);
  let (consumed, mut out) = pump(
    &mut c,
    b"*3\r\n$4\r\nINFO\r\n$6\r\nserver\r\n$8\r\nhlogscan\r\n",
  );
  assert_eq!(consumed, Some(0), "帧应被完整消费（应答延后属预期）");
  let slow = c.take_slow_wait().expect("混合扫描段须挂起慢路径");
  rt.block_on(async { out.extend_from_slice(&slow.resolve().await) });
  let text = from_utf8(&out).unwrap();
  assert!(text.contains("# Server\r\n"), "{text}");
  assert!(text.contains("run_id:"), "SERVER 段须真值渲染: {text}");
  assert!(text.contains("# MainStoreHLogScan_DB_0\r\n"), "{text}");
  let main = text
    .split("\r\n")
    .find(|l| l.starts_with("MainStore_HLog_0:"))
    .unwrap_or_else(|| panic!("混合段 hlogscan 须出存储域条目: {text}"));
  assert!(!main.ends_with("Empty"), "hlogscan 段须经扫描实填: {text}");
}
