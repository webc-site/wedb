//! 运行时配置端到端集成测试（CONFIG GET/SET + OBJECT_SCAN_COUNT_LIMIT 热更）
//!
//! 对标 libs/server/Config/RuntimeServerConfig.cs 的 StoreWrapper 配置管道：
//! 会话共享同一 RuntimeServerConfig 实例，CONFIG SET 即时全服务器生效，
//! HSCAN/SSCAN/ZSCAN 的 COUNT 钳制上限随热更即时变化
//! （C# SharedObjectCommands.cs:ObjectScan 的
//! runtimeConfig.GetInt(OBJECT_SCAN_COUNT_LIMIT)）。

use std::sync::Arc;

use compio::runtime::Runtime;
use wconf::{RuntimeServerConfig, ServerConfigType};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};

/// 双会话共享配置装配（验证 CONFIG SET 跨会话可见）
struct Harness {
  a: RespSessionConsumer,
  b: RespSessionConsumer,
  runtime_config: Arc<RuntimeServerConfig>,
  _dir: tempfile::TempDir,
}

impl Harness {
  fn new() -> Self {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("config.db")).unwrap());
    let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
    config.gc.enabled = false;
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let runtime_config = RuntimeServerConfig::shared_default();

    let mk = |id: u64| {
      let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
      let mut consumer = RespSessionConsumer::new(id, RespServerSessionOptions::default(), api);
      consumer.set_runtime_config(runtime_config.clone());
      consumer
    };
    Self {
      a: mk(1),
      b: mk(2),
      runtime_config,
      _dir: dir,
    }
  }
}

fn feed(consumer: &mut RespSessionConsumer, args: &[&str]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", args.len()).into_bytes();
  for a in args {
    out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
  }
  let (consumed, resp) = consumer.try_consume_messages(&out);
  assert!(consumed > 0);
  resp
}

#[test]
fn config_get_set_roundtrip_visible_across_sessions() {
  with(|h| {
    // 默认值（GarnetServerOptions.ObjectScanCountLimit = 1000）
    let resp = feed(&mut h.a, &["CONFIG", "GET", "object-scan-count-limit"]);
    assert_eq!(
      resp,
      b"*2\r\n$23\r\nobject-scan-count-limit\r\n$4\r\n1000\r\n"
    );

    // A 会话 SET → 共享实例即时生效，B 会话 GET 可见
    let resp = feed(
      &mut h.a,
      &["CONFIG", "SET", "object-scan-count-limit", "64"],
    );
    assert_eq!(resp, b"+OK\r\n");
    let resp = feed(&mut h.b, &["CONFIG", "GET", "object-scan-count-limit"]);
    assert_eq!(
      resp,
      b"*2\r\n$23\r\nobject-scan-count-limit\r\n$2\r\n64\r\n"
    );

    // 直接读配置槽位一致
    assert_eq!(
      h.runtime_config
        .get_int(ServerConfigType::ObjectScanCountLimit),
      64
    );

    // 非法值拒绝（负数越界）
    let resp = feed(
      &mut h.a,
      &["CONFIG", "SET", "object-scan-count-limit", "-1"],
    );
    assert_eq!(
      resp,
      b"-ERR Value for 'object-scan-count-limit' is out of range (0..2147483647).\r\n"
    );
  });
}

#[test]
fn hscan_count_clamped_by_runtime_limit() {
  with(|h| {
    // 10 个字段的哈希
    let mut set_args = vec!["HSET".to_string(), "big".to_string()];
    for i in 0..10 {
      set_args.push(format!("f{i}"));
      set_args.push(format!("v{i}"));
    }
    let set_args_ref: Vec<&str> = set_args.iter().map(String::as_str).collect();
    let resp = feed(&mut h.a, &set_args_ref);
    assert_eq!(resp, b":10\r\n");

    // 热更钳制上限为 3：HSCAN COUNT 100 单轮至多 3 个字段（NOVALUES）
    feed(&mut h.a, &["CONFIG", "SET", "object-scan-count-limit", "3"]);
    let resp = feed(&mut h.a, &["HSCAN", "big", "0", "COUNT", "100", "NOVALUES"]);
    let text = String::from_utf8(resp).unwrap();
    // 帧型：*2 [游标, [字段...]] → 字段数组长度被钳制为 3
    assert!(
      text.contains("\r\n*3\r\n$"),
      "单轮应被钳制到 3 个字段，实际 {text}"
    );
  });
}

fn with(f: impl FnOnce(&mut Harness)) {
  Runtime::new().unwrap().block_on(async {
    let mut h = Harness::new();
    f(&mut h);
  });
}
