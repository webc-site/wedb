//! RESTORE 恰 10 字节构造载荷切片 panic 回归
//! （票 wnode-restore-10byte-payload-slice-panic，r121-triage 甄别通过 P2）
//!
//! 缺陷面：parse_restore_args（key_admin_commands/types.rs）校验序类型门 →
//! 长度门（<10 拒）→ footer 版本门 → crc64 门，随后原 `&value[1..value.len()-10]`
//! 在恰 10 字节形退化为 `&value[1..0]`（起点越终点）触发 checked panic。快臂
//! network_restore 与慢臂 C::Restore 共用本推导单源，单点双臂皆炸；生产经连接泵
//! catch_unwind（net/handler/drive.rs）隔离为「本会话断连、应答零回帧」，可重连
//! 重放重复掐断，触审查红线「生产路径严禁未受控 panic」。
//!
//! 修复：改 `value.get(1..value.len()-10)` 容错解构，None 落同族
//! ERR_DUMP_VERSION_CHECKSUM 错误帧并返回 None（应答存活、会话不断），len≥11
//! 路径逐字节零漂移。严禁复刻 C# 恰 10 字节形经 TryReadLength 放行空载荷落
//! 空值键（+OK）的残余分叉（已登记 doc/zh/deviations.md §114 宗 a）。
//!
//! 测试对标既有 RESP 会话测具（key_admin_latch_concurrency.rs 的
//! RespSessionConsumer 往返 + resp_slow_path.rs 的 SlowWait 慢臂直答），
//! 真存储、真协议帧，无 mock。

use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wbase::crc64::hash as crc64_hash;
use wnode::{
  RespSessionConsumer,
  resp::{
    TtlResume,
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
    slow_path::SlowWait,
  },
};
use wnode_test::roundtrip;
use wresp::{command::RespCommand, length::try_write_length};
use wtest_base::open_test_store;

/// 恰 10 字节形过四门后应落的同族版本/校验和错误帧（types.rs 常量同源）
const ERR_VERSION_CHECKSUM: &[u8] = b"-ERR DUMP payload version or checksum are wrong\r\n";
/// len=11 空载荷体形走的既有长度格式非法错误帧（零漂移臂）
const ERR_LENGTH_INVALID: &[u8] = b"-ERR DUMP payload length format is invalid\r\n";

/// 装配带真存储的会话消费者与慢臂执行域句柄
fn harness(tag: &str) -> (Runtime, RespSessionConsumer, GarnetApi) {
  let rt = Runtime::new().unwrap();
  let (_dir, store) = open_test_store(tag).unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone());
  (rt, c, api)
}

/// `$N\r\n…\r\n` bulk 回执解析（nil / 非 bulk 回 None）
fn reply_bulk(resp: &[u8]) -> Option<Vec<u8>> {
  let body = resp.strip_prefix(b"$")?;
  let nl = body.iter().position(|b| *b == b'\r')?;
  let len: usize = from_utf8(&body[..nl]).ok()?.parse().ok()?;
  let val = body.get(nl + 2..nl + 2 + len)?;
  if body.get(nl + 2 + len..nl + 4 + len)? != b"\r\n" {
    return None;
  }
  Some(val.to_vec())
}

/// 慢臂直答（异步降级分派路径，与降级快照投递同径；不经会话快路径）
///
/// RESTORE 慢路径快照恒带「值已提交 + TTL 待投」续跑尾参（exec 降级快照
/// 追加，[`wnode::resp::TtlResume::tail_bytes`] 契约）：直驱面同款补尾参，
/// Full 形（b'0' + 8 字节零刻度）= 整命令全量重放
fn slow_restore(rt: &Runtime, api: &GarnetApi, args: &[&[u8]]) -> Vec<u8> {
  let mut snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  snapshot.push(TtlResume::Full.tail_bytes());
  rt.block_on(async {
    SlowWait::for_command(
      api,
      RespCommand::Restore,
      snapshot,
      wconf::DEFAULT_RESP_VERSION,
    )
    .resolve()
    .await
  })
}

/// 合法 RESTORE 载荷（类型 0x00 + 长度前缀 + 值 + rdb 版本 11 + crc64；crc 口径
/// 与 network_dump 同源——类型字节起算，见 doc/zh/deviations.md §21）
fn restore_payload(val: &[u8]) -> Vec<u8> {
  let mut encoded_len = [0u8; 5];
  let written = try_write_length(val.len() as u32, &mut encoded_len).unwrap();
  let mut payload = Vec::with_capacity(1 + written + val.len() + 2 + 8);
  payload.push(0x00);
  payload.extend_from_slice(&encoded_len[..written]);
  payload.extend_from_slice(val);
  payload.extend_from_slice(&11u16.to_le_bytes());
  let crc = crc64_hash(&payload);
  payload.extend_from_slice(&crc);
  payload
}

/// 恰 10 字节构造载荷：`[0x00, 0x00]` 接对该 2 字节的 crc64（footer 即全载荷）。
/// 依次过类型门（首字节 0x00）、长度门（len==10 放行）、版本门
/// （rdb_version=0≤11）、crc 门（hash(value[..2])==footer[2..]），随后原
/// `&value[1..0]` 起点越终点 panic
fn exact_10byte_payload() -> Vec<u8> {
  let head = [0x00u8, 0x00];
  let mut payload = head.to_vec();
  payload.extend_from_slice(&crc64_hash(&head));
  debug_assert_eq!(payload.len(), 10);
  payload
}

/// len=11 空载荷体形：`[0x00, 0x00, 0x00]` 接对该 3 字节的 crc64（合计 11 字节）。
/// 过版本/crc 门后 payload_body = `value.get(1..1)` 为合法空切片，
/// try_read_length 空输入即 None → 既有 LENGTH_INVALID 臂（零漂移）
fn empty_body_11byte_payload() -> Vec<u8> {
  let head = [0x00u8, 0x00, 0x00];
  let mut payload = head.to_vec();
  payload.extend_from_slice(&crc64_hash(&head));
  debug_assert_eq!(payload.len(), 11);
  payload
}

/// 验证点 a：恰 10 字节构造载荷过四门后落同族版本/校验和错误帧、连接存活
/// （随后 PING 得 +PONG，锁死 panic 不可达——修复前本命令在消费路径 checked
/// panic，会话级测试会直接 unwind 中止，无法走到 PING）
#[test]
fn restore_exact_10byte_payload_errors_and_connection_alive() {
  let (rt, mut c, _api) = harness("restore-10b-exact.db");
  let payload = exact_10byte_payload();
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RESTORE", b"k10", b"0", &payload]),
    ERR_VERSION_CHECKSUM,
    "恰 10 字节构造载荷应落 ERR DUMP payload version or checksum are wrong（修复前 panic）"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"PING"]),
    b"+PONG\r\n",
    "错误帧后会话必存活（panic 掐断即无 +PONG）"
  );
}

/// 验证点 b：len=11 空载荷体形走既有 LENGTH_INVALID 臂，逐字节零漂移
#[test]
fn restore_len11_empty_body_length_invalid_no_drift() {
  let (rt, mut c, _api) = harness("restore-10b-len11.db");
  let payload = empty_body_11byte_payload();
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RESTORE", b"k11", b"0", &payload]),
    ERR_LENGTH_INVALID,
    "len=11 空载荷体形必落 ERR DUMP payload length format is invalid（get(1..1) 合法空切片，零漂移）"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"PING"]),
    b"+PONG\r\n",
    "len=11 臂后会话必存活"
  );
}

/// 验证点 c：合法载荷与正常 DUMP→RESTORE 往返保持通过（§21 语义锁）
#[test]
fn restore_valid_payload_and_dump_roundtrip_preserved() {
  let (rt, mut c, _api) = harness("restore-10b-valid.db");

  // 合法 RESTORE → +OK + GET 载荷值
  let payload = restore_payload(b"hello");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RESTORE", b"rk", b"0", &payload]),
    b"+OK\r\n"
  );
  assert_eq!(
    reply_bulk(&roundtrip(&rt, &mut c, &[b"GET", b"rk"])).as_deref(),
    Some(b"hello".as_slice()),
    "RESTORE 后载荷值可读回"
  );

  // 正常 DUMP→RESTORE 往返（crc 含类型字节的 rust 口径，§21 语义锁）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"src", b"world"]),
    b"+OK\r\n"
  );
  let dumped = reply_bulk(&roundtrip(&rt, &mut c, &[b"DUMP", b"src"])).expect("DUMP 应答必为 bulk");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RESTORE", b"dst", b"0", &dumped]),
    b"+OK\r\n",
    "DUMP→RESTORE 往返应放行"
  );
  assert_eq!(
    reply_bulk(&roundtrip(&rt, &mut c, &[b"GET", b"dst"])).as_deref(),
    Some(b"world".as_slice()),
    "RESTORE 复现 DUMP 原值"
  );
}

/// 验证点 d：慢臂（异步降级分派路径）同载荷经共享单源落同族错误帧、resolve
/// 完成（修复前 panic），与快臂逐字节一致；会话随后 PING 存活
#[test]
fn restore_slow_arm_matches_fast_error_frames() {
  let (rt, mut c, api) = harness("restore-10b-slow.db");

  // 恰 10 字节形：慢臂直答 == 快臂往返 == 同族错误帧（单源自动同修）
  let p10 = exact_10byte_payload();
  let slow10 = slow_restore(&rt, &api, &[b"sk10", b"0", &p10]);
  let fast10 = roundtrip(&rt, &mut c, &[b"RESTORE", b"fk10", b"0", &p10]);
  assert_eq!(
    slow10, ERR_VERSION_CHECKSUM,
    "慢臂恰 10 字节形应落版本/校验和错误帧且 resolve 完成（修复前 panic）"
  );
  assert_eq!(
    slow10, fast10,
    "快慢臂共用 parse_restore_args 单源，错误帧逐字节一致"
  );

  // len=11 空载荷体形：慢臂零漂移走 LENGTH_INVALID，与快臂一致
  let p11 = empty_body_11byte_payload();
  let slow11 = slow_restore(&rt, &api, &[b"sk11", b"0", &p11]);
  assert_eq!(slow11, ERR_LENGTH_INVALID, "慢臂 len=11 零漂移");
  assert_eq!(
    slow11,
    roundtrip(&rt, &mut c, &[b"RESTORE", b"fk11", b"0", &p11]),
    "len=11 快慢臂逐字节一致"
  );

  // 慢臂 resolve 完成即证未掐断会话；同会话 PING 存活
  assert_eq!(roundtrip(&rt, &mut c, &[b"PING"]), b"+PONG\r\n");
}
