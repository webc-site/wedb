//! HCOLLECT / ZCOLLECT 管理命令端到端集成测试
//!
//! 对标 libs/server/Resp/AdminCommands.cs:NetworkHCOLLECT/NetworkZCOLLECT：
//! 显式键清单逐键收集（过期成员清除 + 回写）、WRONGTYPE 汇总、缺键放行、
//! `*` 全库扫描降级异步闭环。

use std::{sync::Arc, thread, time::Duration};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};

/// 单会话测试装配
fn with_session(f: impl FnOnce(&mut RespSessionConsumer)) {
  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("collect.db")).unwrap());
    let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
    config.gc.enabled = false;
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
    let mut consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api);
    f(&mut consumer);
  });
}

/// 命令参数 → RESP 帧
fn frame(args: &[&str]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", args.len()).into_bytes();
  for a in args {
    out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
  }
  out
}

fn feed(consumer: &mut RespSessionConsumer, args: &[&str]) -> Vec<u8> {
  let (consumed, resp) = consumer.try_consume_messages(&frame(args));
  assert!(consumed > 0);
  resp
}

#[test]
fn hcollect_keeps_live_fields_and_reports_ok() {
  with_session(|c| {
    feed(c, &["HSET", "h", "f1", "v1", "f2", "v2"]);

    // 无过期字段的收集：+OK 且数据保持
    feed(c, &["HCOLLECT", "h"]);
    let resp = feed(c, &["HLEN", "h"]);
    assert_eq!(resp, b":2\r\n");
    // 未收集的键直接放行 +OK
    let resp = feed(c, &["HCOLLECT", "missing"]);
    assert_eq!(resp, b"+OK\r\n");
  });
}

#[test]
fn hcollect_clears_expired_fields() {
  with_session(|c| {
    feed(c, &["HSET", "hx", "keep", "v", "gone", "v"]);
    // 字段级 100ms 过期
    feed(c, &["HPEXPIRE", "hx", "100", "FIELDS", "1", "gone"]);

    thread::sleep(Duration::from_millis(200));
    let resp = feed(c, &["HCOLLECT", "hx"]);
    assert_eq!(resp, b"+OK\r\n");
    let resp = feed(c, &["HLEN", "hx"]);
    assert_eq!(resp, b":1\r\n");
  });
}

#[test]
fn hcollect_wrongtype_and_star_degrade() {
  with_session(|c| {
    feed(c, &["SET", "str", "x"]);

    // 任一 WRONGTYPE 键 → 汇总为 WRONGTYPE
    let resp = feed(c, &["HCOLLECT", "str"]);
    assert_eq!(
      resp,
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
    );

    // `*` 全库扫描：同步执行域不可达，挂起慢路径（同步段不残留输出），
    // 网络泵 await 后产出兜底应答（HCOLLECT 慢表接入前的 ASYNC_REQUIRED）
    let resp = feed(c, &["HCOLLECT", "*"]);
    assert!(resp.is_empty(), "同步段仅校验，不残留输出");
    let slow = c.take_slow_wait().expect("HCOLLECT * 应挂起慢路径");
    let resp = Runtime::new().unwrap().block_on(slow.resolve());
    assert_eq!(resp, b"+OK\r\n");
  });
}

#[test]
fn zcollect_reports_ok_and_wrongtype() {
  with_session(|c| {
    feed(c, &["ZADD", "z", "1", "m"]);

    let resp = feed(c, &["ZCOLLECT", "z"]);
    assert_eq!(resp, b"+OK\r\n");
    // 数据保持
    let resp = feed(c, &["ZCARD", "z"]);
    assert_eq!(resp, b":1\r\n");

    let resp = feed(c, &["ZCOLLECT", "missing"]);
    assert_eq!(resp, b"+OK\r\n");

    feed(c, &["SET", "str", "x"]);
    let resp = feed(c, &["ZCOLLECT", "str"]);
    assert_eq!(
      resp,
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
    );
  });
}
