//! 有序集合弹出内核删空自愈回归测试
//!
//! 对标 garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Operate 的
//! REMOVE_KEY 标志臂（:452-453）与 Session/ObjectStore/SortedSetOps.cs:SortedSetPop /
//! SortedSetMPop：集合成员全部到期剔空（count 归零）时，弹出内核必须原子写删空
//! 墓碑物理清退该键，杜绝幽灵空键滞留存储。
//!
//! 覆盖 ZMPOP / BZMPOP / BZPOPMIN 命中全过期键的删空自愈（阻塞族无经纪注入时
//! 落立即可取路径），验证：
//! - 弹出应答正确（无有效候选键回 nil；有后续有效键则跳过幽灵键正常出件）；
//! - 原全过期键在底层存储被彻底清退（EXISTS 返回 0，SCAN 扫描不到该键）。

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
  let device = Arc::new(SegmentedDevice::single_file(dir.join("zpop.db")).unwrap());
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

/// 同步快路径单命令往返（完整消费并返回应答）
fn call(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(frame);
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let remaining = c.try_consume_messages_into(&mut out);
  assert_eq!(remaining, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// 慢命令往返：同步段消费挂起 → 驱动慢路径 await 闭环 → 按流水线顺序补应答
fn call_slow(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut out = call(c, frame);
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// 全库扫描收尽为键名列表（游标续扫至归零）
fn scan_all(rt: &Runtime, c: &mut RespSessionConsumer) -> Vec<String> {
  let mut all = Vec::new();
  let mut cursor = b"0".to_vec();
  loop {
    let out = call_slow(rt, c, &frame(b"SCAN", &[&cursor, b"COUNT", b"1000"]));
    let (next, keys) = parse_scan_reply(&String::from_utf8_lossy(&out));
    all.extend(keys);
    if next == "0" {
      return all;
    }
    cursor = next.into_bytes();
  }
}

/// 解析 SCAN 应答 `*2\r\n$<n>\r\n<cursor>\r\n*<m>\r\n...` → (游标, 键名列表)
fn parse_scan_reply(text: &str) -> (String, Vec<String>) {
  let mut lines = text.split("\r\n");
  assert_eq!(lines.next(), Some("*2"), "SCAN 应答为二元数组: {text}");
  let cursor_len: usize = lines
    .next()
    .unwrap()
    .strip_prefix('$')
    .unwrap()
    .parse()
    .unwrap();
  let cursor_raw = lines.next().unwrap();
  assert_eq!(cursor_raw.len(), cursor_len, "游标 bulk 长度一致: {text}");
  let cursor = cursor_raw.to_string();
  let n: usize = lines
    .next()
    .unwrap()
    .strip_prefix('*')
    .unwrap()
    .parse()
    .unwrap();
  let mut keys = Vec::with_capacity(n);
  for _ in 0..n {
    let len: usize = lines
      .next()
      .unwrap()
      .strip_prefix('$')
      .unwrap()
      .parse()
      .unwrap();
    let key = lines.next().unwrap();
    assert_eq!(key.len(), len, "bulk 长度一致: {text}");
    keys.push(key.to_string());
  }
  (cursor, keys)
}

/// ZMPOP 命中全过期有序集合后删空自愈：幽灵空键被物理清退
///
/// 对标 C# SortedSetObject.Operate REMOVE_KEY 臂：全成员到期 → 弹出内核写删空墓碑，
/// EXISTS 归 0、SCAN 不可见（修复前 EXISTS=1、ZCARD=0 的幽灵键被杜绝）
#[test]
fn zmpop_expired_all_members_self_heals_empty_key() {
  let (rt, mut c) = consumer();

  // ZADD ghost 1.0 a 2.0 b
  assert_eq!(
    call(&mut c, &frame(b"ZADD", &[b"ghost", b"1", b"a", b"2", b"b"]),),
    b":2\r\n"
  );
  // ZEXPIRE ghost 1 MEMBERS 2 a b → 两成员均挂 1 秒 TTL
  let out = call(
    &mut c,
    &frame(b"ZEXPIRE", &[b"ghost", b"1", b"MEMBERS", b"2", b"a", b"b"]),
  );
  assert!(
    out.starts_with(b"*2\r\n"),
    "ZEXPIRE 应答逐成员数组: {out:?}"
  );

  // 未过期前键存活
  assert_eq!(
    call(&mut c, &frame(b"EXISTS", &[b"ghost"])),
    b":1\r\n",
    "TTL 未到前幽灵键尚未形成"
  );

  // 等待成员级 TTL 到期
  sleep(Duration::from_millis(1200));

  // ZMPOP ghost 唯一候选全过期 → 空弹出 nil（RESP2 $-1）
  assert_eq!(
    call(&mut c, &frame(b"ZMPOP", &[b"1", b"ghost", b"MIN"])),
    b"$-1\r\n",
    "无有效候选键应回 nil"
  );

  // 删空自愈：全过期空键被物理清退
  assert_eq!(
    call(&mut c, &frame(b"EXISTS", &[b"ghost"])),
    b":0\r\n",
    "弹出后幽灵空键应已删空自愈"
  );
  assert!(
    !scan_all(&rt, &mut c).iter().any(|k| k == "ghost"),
    "SCAN 不应再扫到 ghost"
  );
}

/// BZMPOP（无经纪注入 → 立即可取路径）命中全过期键后删空自愈
#[test]
fn bzmpop_expired_all_members_self_heals_empty_key() {
  let (rt, mut c) = consumer();

  assert_eq!(
    call(
      &mut c,
      &frame(b"ZADD", &[b"bghost", b"3", b"x", b"4", b"y"]),
    ),
    b":2\r\n"
  );
  call(
    &mut c,
    &frame(b"ZEXPIRE", &[b"bghost", b"1", b"MEMBERS", b"2", b"x", b"y"]),
  );
  sleep(Duration::from_millis(1200));

  // BZMPOP timeout=0 numkeys=1 bghost MIN → nil
  assert_eq!(
    call(&mut c, &frame(b"BZMPOP", &[b"0", b"1", b"bghost", b"MIN"]),),
    b"$-1\r\n",
    "无有效候选键应回 nil"
  );

  assert_eq!(
    call(&mut c, &frame(b"EXISTS", &[b"bghost"])),
    b":0\r\n",
    "BZMPOP 命中全过期键后应删空自愈"
  );
  assert!(
    !scan_all(&rt, &mut c).iter().any(|k| k == "bghost"),
    "SCAN 不应再扫到 bghost"
  );
}

/// ZMPOP 多候选：跳过全过期幽灵键并从后续有效键正常出件，幽灵键同时被清退
#[test]
fn zmpop_skips_expired_key_and_pops_next_survivor() {
  let (rt, mut c) = consumer();

  // ghost 全成员挂 1 秒 TTL；live 存活两成员
  call(
    &mut c,
    &frame(b"ZADD", &[b"ghost2", b"1", b"a", b"2", b"b"]),
  );
  call(
    &mut c,
    &frame(b"ZEXPIRE", &[b"ghost2", b"1", b"MEMBERS", b"2", b"a", b"b"]),
  );
  call(&mut c, &frame(b"ZADD", &[b"live", b"5", b"p", b"6", b"q"]));
  sleep(Duration::from_millis(1200));

  // ZMPOP 2 ghost2 live MIN → 跳过幽灵键，从 live 弹最低分 p(5)，live 余 q
  assert_eq!(
    call(
      &mut c,
      &frame(b"ZMPOP", &[b"2", b"ghost2", b"live", b"MIN"]),
    ),
    b"*2\r\n$4\r\nlive\r\n*1\r\n*2\r\n$1\r\np\r\n$1\r\n5\r\n",
    "应跳过全过期键从后续有效键出件"
  );

  // 出件后幽灵键被删空自愈；存活键弹一枚后仍在
  assert_eq!(
    call(&mut c, &frame(b"EXISTS", &[b"ghost2"])),
    b":0\r\n",
    "跳过的全过期键应删空自愈"
  );
  assert_eq!(
    call(&mut c, &frame(b"EXISTS", &[b"live"])),
    b":1\r\n",
    "仍有成员的存活键不受影响"
  );
  assert!(
    !scan_all(&rt, &mut c).iter().any(|k| k == "ghost2"),
    "SCAN 不应再扫到 ghost2"
  );
}

/// BZPOPMIN（无经纪注入 → 立即可取路径）命中全过期键后删空自愈
#[test]
fn bzpopmin_expired_all_members_self_heals_empty_key() {
  let (rt, mut c) = consumer();

  call(
    &mut c,
    &frame(b"ZADD", &[b"pghost", b"1", b"a", b"2", b"b"]),
  );
  call(
    &mut c,
    &frame(b"ZEXPIRE", &[b"pghost", b"1", b"MEMBERS", b"2", b"a", b"b"]),
  );
  sleep(Duration::from_millis(1200));

  // BZPOPMIN pghost timeout=0 → nil（唯一候选全过期）
  assert_eq!(
    call(&mut c, &frame(b"BZPOPMIN", &[b"pghost", b"0"])),
    b"$-1\r\n",
    "无有效候选键应回 nil"
  );

  assert_eq!(
    call(&mut c, &frame(b"EXISTS", &[b"pghost"])),
    b":0\r\n",
    "BZPOPMIN 命中全过期键后应删空自愈"
  );
  assert!(
    !scan_all(&rt, &mut c).iter().any(|k| k == "pghost"),
    "SCAN 不应再扫到 pghost"
  );
}
