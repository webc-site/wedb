//! ZRANDMEMBER 成员级 TTL 存活数零守卫回归（票 zcode-r143c-spopcnt 案一）
//!
//! 缺陷形：对象臂 sorted_set_random_member 于 purge_expired_len 取存活数并钳制
//! 后无零守卫，Present 但成员全到期（存活零）的键上
//! - 负 count：头部按 |count| 声明、[`pick_k_random_indexes`] n=0 空域零产出 →
//!   头多宣，客户端按声明续读把后续命令应答吞为成员，整连接 RESP 流永久错位；
//! - 正 count（钳 0）与无 count 形：头声明条件两态皆假 → 整帧零字节，挂读。
//!
//! 族内 HRANDFIELD / SRANDMEMBER 早退守卫在位，唯 ZSet 面无（本案补齐同构守卫）。
//!
//! 常驻可达性：ZRANDMEMBER 快慢双臂皆纯读不写回（run_operate 后直返，无
//! mutated_by_ttl 升格，本仓读臂既定纪律），purge 剔除不固化 → 全到期态常驻
//! 非瞬态；分层面无 zrandmember 臂，穿透物化落同一对象臂双态同染。
//!
//! 对标 C# SortedSetObjectImpl.cs:663-706（原型同 n=0 面触 Random.Next(0) 抛断流，
//! 系 doc/zh/deviations.md §12 已登记危险面，非复刻豁免）；会话缺键形态
//! write_random_member_missing（带 count → *0，无 count → nil）为同帧形对照。

use std::{iter::once, sync::Arc, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    RespServerSessionOptions,
    garnet_api::{GarnetApi, StoreGarnetApi},
  },
};
use wtest_base::test_store_config;

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭保持惰性过期语义）
fn consumer() -> (Runtime, RespSessionConsumer) {
  let dir = tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("zrandmember.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(session));
  let c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone());
  (Runtime::new().unwrap(), c)
}

/// 组 RESP 请求数组帧（cmd + args）
fn frame(cmd: &[u8], args: &[&[u8]]) -> Vec<u8> {
  let mut f = format!("*{}\r\n", args.len() + 1).into_bytes();
  for token in once(cmd).chain(args.iter().copied()) {
    f.extend_from_slice(format!("${}\r\n", token.len()).as_bytes());
    f.extend_from_slice(token);
    f.extend_from_slice(b"\r\n");
  }
  f
}

/// 同步段消费并返回应答（慢路径挂起时按流水线顺序补帧）
fn call(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(frame);
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let remaining = c.try_consume_messages_into(&mut out);
  assert_eq!(remaining, Some(0), "帧应被完整消费: {frame:?}");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}

/// 建全成员即刻到期键：ZADD 两员 + ZPEXPIRE 50 毫秒后等待越过
fn seed_all_expired(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8]) {
  assert_eq!(
    call(rt, c, &frame(b"ZADD", &[key, b"1", b"m1", b"2", b"m2"])),
    b":2\r\n"
  );
  assert_eq!(
    call(
      rt,
      c,
      &frame(b"ZPEXPIRE", &[key, b"50", b"MEMBERS", b"2", b"m1", b"m2"])
    ),
    b"*2\r\n:1\r\n:1\r\n"
  );
  sleep(Duration::from_millis(150));
  // 纯读臂不固化剔除：到期未清退前键在场（存活零态常驻的立论前提）
  assert_eq!(call(rt, c, &frame(b"EXISTS", &[key])), b":1\r\n");
}

/// 存活零键三形态应答逐字节锁 + 流水线帧界无串位
#[test]
fn zrandmember_all_expired_members_replies_nil_and_empty_array() {
  let (rt, mut c) = consumer();
  seed_all_expired(&rt, &mut c, b"zx");

  // 无 count → nil（RESP2 $-1）
  assert_eq!(
    call(&rt, &mut c, &frame(b"ZRANDMEMBER", &[b"zx"])),
    b"$-1\r\n"
  );
  // 负 count → *0（修复前 *6 空头断流）
  assert_eq!(
    call(&rt, &mut c, &frame(b"ZRANDMEMBER", &[b"zx", b"-3"])),
    b"*0\r\n"
  );
  // 正 count（钳 0）→ *0（修复前整帧零字节挂读）
  assert_eq!(
    call(&rt, &mut c, &frame(b"ZRANDMEMBER", &[b"zx", b"5"])),
    b"*0\r\n"
  );
  // WITHSCORES 形同口收口
  assert_eq!(
    call(
      &rt,
      &mut c,
      &frame(b"ZRANDMEMBER", &[b"zx", b"-2", b"WITHSCORES"])
    ),
    b"*0\r\n"
  );

  // 流水线：三命令一次性灌入，应答拼接逐字节全等即帧界无串位
  let mut pipeline = frame(b"ZRANDMEMBER", &[b"zx"]);
  pipeline.extend_from_slice(&frame(b"ZRANDMEMBER", &[b"zx", b"-3"]));
  pipeline.extend_from_slice(&frame(b"PING", &[]));
  assert_eq!(
    call(&rt, &mut c, &pipeline),
    b"$-1\r\n*0\r\n+PONG\r\n",
    "存活零键多命令串发帧界锁定"
  );

  // 同键二次采样稳定（读臂不固化剔除，全到期态非瞬态）
  assert_eq!(
    call(&rt, &mut c, &frame(b"ZRANDMEMBER", &[b"zx", b"-1"])),
    b"*0\r\n"
  );
  assert_eq!(
    call(&rt, &mut c, &frame(b"EXISTS", &[b"zx"])),
    b":1\r\n",
    "ZRANDMEMBER 纯读不写回不删空，存活零键在场（族内读臂既定纪律）"
  );

  // 对照缺键形态（write_random_member_missing）同帧形：多路径同构
  assert_eq!(
    call(&rt, &mut c, &frame(b"ZRANDMEMBER", &[b"nokey"])),
    b"$-1\r\n"
  );
  assert_eq!(
    call(&rt, &mut c, &frame(b"ZRANDMEMBER", &[b"nokey", b"-3"])),
    b"*0\r\n"
  );
}

/// RESP3 下存活零键的 nil 走版本单点 `_\r\n`、空数组恒 `*0\r\n`
#[test]
fn zrandmember_all_expired_resp3_frames() {
  let (rt, mut c) = consumer();
  // 建键与挂期在 RESP2 段完成（应答形与上例同），随后升级会话协议版本
  seed_all_expired(&rt, &mut c, b"z3");
  c.session_mut().resp_protocol_version = 3;

  assert_eq!(
    call(&rt, &mut c, &frame(b"ZRANDMEMBER", &[b"z3"])),
    b"_\r\n"
  );
  assert_eq!(
    call(&rt, &mut c, &frame(b"ZRANDMEMBER", &[b"z3", b"-3"])),
    b"*0\r\n"
  );
  assert_eq!(
    call(&rt, &mut c, &frame(b"ZRANDMEMBER", &[b"z3", b"5"])),
    b"*0\r\n"
  );
  // 缺键对照同形
  assert_eq!(
    call(&rt, &mut c, &frame(b"ZRANDMEMBER", &[b"nokey3"])),
    b"_\r\n"
  );
}
