//! geo STORE 族错误臂零副作用回归：源装载/求值报错时目标键 TTL 不得被误清
//!（票 wnode-store-cold-window-ttl-clear-outsides-critical-section B 项）
//!
//! C# 对位：dest 键排他锁跨 GET → Delete → ZADD 全程，**错误臂先于 Delete
//! 返回**（libs/server/Storage/Session/ObjectStore/SortedSetGeoOps.cs:129-133：
//! WRONGTYPE 直接 return；NOTFOUND 才 EXPIRE(destination, 0)）——rust 自票
//! zcode-r18-geo 起 geo 臂收口 `store_dest_cold` 单点后，TTL 动作只落复验通过
//! 后的持窗写临界区（`store_dest_cold_common`），源 `WRONGTYPE` / FROMMEMBER
//! 圆心缺失等错误臂先于收尾单点返回，dst TTL 与成员天然零触碰。本组在**冷域**
//! （全部落盘，错误臂必走异步装载通道）锁死该不变式，杜绝窗序回退时「零写入
//! 却已抹掉 dst TTL」复犯。

use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::{open_test_store, resp_frame as frame};

type TestStore = WedbStore<SegmentedDevice>;

/// 独立连接装配
fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 单命令往返（慢路径挂起时以 block_on 承担网络泵角色闭环）
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut out = {
    let mut scratch = c.take_recv_scratch();
    scratch.extend_from_slice(&frame(args));
    c.return_recv_scratch(scratch);
    let mut out = Vec::new();
    let _ = c.try_consume_messages_into(&mut out);
    out
  };
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async { out.extend_from_slice(&slow.resolve().await) });
  }
  out
}

/// PTTL 应答帧 → 剩余毫秒（-2 键缺失 / -1 无 TTL）
fn pttl_ms(reply: &[u8]) -> i64 {
  String::from_utf8_lossy(reply)
    .trim_start_matches(':')
    .trim_end_matches("\r\n")
    .parse()
    .unwrap_or(-2)
}

/// 错误臂场景：冷域 WRONGTYPE 源 / FROMMEMBER 圆心缺失两臂均零触碰 dst——
/// 键存活、TTL 在场、成员原样；对照组 0 命中存储臂走空结果臂整键消亡
///（C# EXPIRE(destination, 0) 对位）
#[test]
fn geostore_error_arms_keep_dest_ttl_intact() {
  let (_dir, store) = open_test_store("geo-store-err-ttl.db").unwrap();
  let rt = Runtime::new().unwrap();
  {
    let mut c = consumer_on(&store);
    // err 组：geo 集合 dst{old} + TTL；geo 源合法、字符串源 WRONGTYPE 素材
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GEOADD", b"ge:dst", b"1.0", b"1.0", b"old"]),
      b":1\r\n"
    );
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &[
          b"GEOADD",
          b"ge:src",
          b"13.361389",
          b"38.115556",
          b"palermo",
          b"15.087269",
          b"37.509",
          b"catania",
        ],
      ),
      b":2\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"ge:str", b"v"]),
      b"+OK\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"EXPIRE", b"ge:dst", b"600"]),
      b":1\r\n"
    );
    // hit 组：同构素材供 0 命中存储臂对照
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GEOADD", b"gh:dst", b"1.0", b"1.0", b"old"]),
      b":1\r\n"
    );
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &[
          b"GEOADD",
          b"gh:src",
          b"13.361389",
          b"38.115556",
          b"palermo",
          b"15.087269",
          b"37.509",
          b"catania",
        ],
      ),
      b":2\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"EXPIRE", b"gh:dst", b"600"]),
      b":1\r\n"
    );
  }
  // 冷化：STORE 收尾臂与全部目标键/TTL 旁路落盘（错误臂必走异步装载通道）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  {
    let mut c = consumer_on(&store);
    // ① WRONGTYPE 源臂：错误帧透传，dst TTL 与成员零触碰
    let reply = roundtrip(
      &rt,
      &mut c,
      &[
        b"GEOSEARCHSTORE",
        b"ge:dst",
        b"ge:str",
        b"FROMLONLAT",
        b"13.36",
        b"38.11",
        b"BYRADIUS",
        b"200",
        b"km",
      ],
    );
    assert!(
      reply.starts_with(b"-"),
      "WRONGTYPE 源应回错误帧，实际：{:?}",
      String::from_utf8_lossy(&reply)
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b"ge:dst"]), b":1\r\n");
    assert!(
      pttl_ms(&roundtrip(&rt, &mut c, &[b"PTTL", b"ge:dst"])) > 0,
      "WRONGTYPE 源臂不得误清 dst TTL"
    );

    // ② FROMMEMBER 圆心缺失臂（对象层错误透传）：dst TTL 零触碰
    let reply = roundtrip(
      &rt,
      &mut c,
      &[
        b"GEOSEARCHSTORE",
        b"ge:dst",
        b"ge:src",
        b"FROMMEMBER",
        b"missing",
        b"BYRADIUS",
        b"200",
        b"km",
      ],
    );
    assert!(
      reply.starts_with(b"-"),
      "FROMMEMBER 圆心缺失应回错误帧，实际：{:?}",
      String::from_utf8_lossy(&reply)
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b"ge:dst"]), b":1\r\n");
    assert!(
      pttl_ms(&roundtrip(&rt, &mut c, &[b"PTTL", b"ge:dst"])) > 0,
      "FROMMEMBER 错误臂不得误清 dst TTL"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"ZRANGE", b"ge:dst", b"0", b"-1"]),
      b"*1\r\n$3\r\nold\r\n",
      "错误臂后 dst 成员应原样"
    );

    // ③ 对照组：0 命中存储臂 = C# EXPIRE(destination, 0)——整键消亡（TTL 随键
    // 清除）；FROMLONLAT 0 0 半径 200km 距两成员均超 4000km，必 0 命中
    let reply = roundtrip(
      &rt,
      &mut c,
      &[
        b"GEOSEARCHSTORE",
        b"gh:dst",
        b"gh:src",
        b"FROMLONLAT",
        b"0",
        b"0",
        b"BYRADIUS",
        b"200",
        b"km",
      ],
    );
    assert_eq!(reply, b":0\r\n", "0 命中存储臂应回 :0");
    assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"gh:dst"]), b":0\r\n");
  }
}
