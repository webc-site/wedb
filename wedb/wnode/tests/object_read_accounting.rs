//! OBJECT 族命中/未命中入账恒零口径回归（票 zcode-r157c-objenc 案一，P3）
//!
//! 缺陷面：同一命令三臂口径互异——热臂 `network_object` 经 `read_user_sync`
//! 传 None 恒零计（正确侧）、慢臂 `object_slow` 的 read_user 漏斗出口恰计
//! 一条、其后 ObjectEnvelope/Meta 两枚二级续探走簿记入口 `read_tag_with`
//! 各自入账。分层驻态键（Meta 命中）冷形虚报 2 条（信封缺失 notfound+1 与
//! Meta 命中 found+1），字符串/缺失冷形各虚报 1 条；C# NetworkOBJECT →
//! Read_UnifiedStore（libs/server/Storage/Session/UnifiedStore/
//! AdvancedOps.cs:12-21）任意驻态恒零计，三方失联。
//!
//! 修复形态：慢臂三处统一收编既有静默口——漏斗出口改 `read_user_quiet`
//! （getexbig 已落地的静默内核薄包装，零新机制、不起第二入账通道），信封/
//! Meta 两续探改 `read_tag_quiet`（可见性私有→pub(crate)），三臂与 C# 同为
//! 恒零；热臂/慢臂两处补防回改注记，单点真源在 user_read.rs 头注（案二）。
//!
//! 测试全真存储真协议帧，无 mock：热臂经会话生产口
//! （RespSessionConsumer::try_consume_messages_into）驱动、冷臂
//! SlowWait::for_command 直驱（夹具先例 dump_read_accounting.rs 同形），
//! 同一采样句柄下对字符串键/信封对象键/分层驻态键（Meta 命中，热树态与
//! flush_and_evict_all 换出内存环的冷树态两形）/缺失键四形断言
//! total_found/total_notfound 增量恒 0/0；分层键冷形 OBJECT ENCODING 应答
//! 逐字节等热形（$9 hashtable），钉死「磁盘候选 × 分层驻态」交叉回形；
//! 同句柄下补 GET 簿记对照臂（快慢各恰 1 found），防修复过零误伤读命令
//! 入账面；既有 GET/DUMP 入账锁测（dump_read_accounting.rs）零漂移。

use std::sync::Arc;

use compio::runtime::Runtime;
use itoa::Buffer;
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::GarnetApi, slow_path::SlowWait},
};
use wnode_test::{counts, metrics_env as env, roundtrip};
use wresp::command::RespCommand;

/// 慢臂直驱帧型版本入参（与 RESP2 热臂同版）
const RESP_V2: u8 = 2;

/// 冷臂直驱（与降级快照投递同径，不经会话快路径）
fn slow_direct(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  rt.block_on(async {
    SlowWait::for_command(api, cmd, snapshot, RESP_V2)
      .resolve()
      .await
  })
}

/// 四形键装载：SET 字符串键、小 HSET 信封对象键、超阈值升阶的分层驻态键
/// （Meta 命中，经 load_collection_stub 确证）、缺失键不装载
fn seed_keys(
  rt: &Runtime,
  c: &mut RespSessionConsumer,
  store: &Arc<wkv::WedbStore<wdev::SegmentedDevice>>,
) {
  assert_eq!(roundtrip(rt, c, &[b"SET", b"str_k", b"v"]), b"+OK\r\n");
  assert_eq!(
    roundtrip(rt, c, &[b"HSET", b"obj_k", b"f", b"v"]),
    b":1\r\n"
  );
  // 升阶灌水：hash 写入 TIERED_PROMOTE_THRESHOLD+10 字段（先例
  // tiered_cmds_align.rs 同形，按 16384 对一片组装 HSET；帧路驱命令名
  // 必随帧首 token——漏 HSET 则整片回 -ERR unknown command，键不涨、
  // 升阶前提静默失效。逐片应答强断言锁死该前提）
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = Buffer::new();
  for start in (1..=total).step_by(16384) {
    let end = (start + 16383).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((end - start + 1) * 2 + 2);
    args.push(b"HSET".to_vec());
    args.push(b"tier_k".to_vec());
    for i in start..=end {
      let n = buf.format(i).as_bytes().to_vec();
      args.push(n.clone());
      args.push(n);
    }
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    let added = format!(":{}\r\n", end - start + 1);
    assert_eq!(
      roundtrip(rt, c, &slices),
      added.as_bytes(),
      "升阶灌水片 {start}..={end} 应整片 HSET 成功入账"
    );
  }
  let sess = store.new_session().unwrap();
  assert!(
    rt.block_on(sess.load_collection_stub(b"tier_k"))
      .unwrap()
      .is_some(),
    "tier_k 应已升阶（Meta 域命中）"
  );
}

/// 热臂（会话生产口 network_object）四形入账恒 0/0，回形基线同帧捕获
#[test]
fn object_hot_arm_records_zero_for_four_shapes() {
  let (rt, mut c, _api, handle, _dir, store) = env("obj-account-hot.db");
  seed_keys(&rt, &mut c, &store);

  // 灌水面 SET/HSET 为写命令不入 found/notfound 账，句柄此刻应为基准态
  let (f0, n0) = counts(&handle);

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"OBJECT", b"ENCODING", b"str_k"]),
    b"$3\r\nraw\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"OBJECT", b"ENCODING", b"obj_k"]),
    b"$9\r\nhashtable\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"OBJECT", b"ENCODING", b"tier_k"]),
    b"$9\r\nhashtable\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"OBJECT", b"ENCODING", b"nope"]),
    b"$-1\r\n"
  );
  // 三枚附属子命令同臂抽查（存在性门控与 ENCODING 共用判据）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"OBJECT", b"REFCOUNT", b"tier_k"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"OBJECT", b"IDLETIME", b"nope"]),
    b"$-1\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f0, n0),
    "OBJECT 热臂四形恒零入账（C# Read_UnifiedStore 恒零计）"
  );
}

/// 冷臂（SlowWait 直驱 object_slow）驻留内存环四形入账恒 0/0
/// （修复前：字符串 1 found、缺失 2 notfound、信封 1 found、分层 1+1 双计）
#[test]
fn object_slow_arm_hot_resident_records_zero() {
  let (rt, mut c, api, handle, _dir, store) = env("obj-account-slow-hot.db");
  seed_keys(&rt, &mut c, &store);
  let (f0, n0) = counts(&handle);

  assert_eq!(
    slow_direct(&rt, &api, RespCommand::ObjectEncoding, &[b"str_k"]),
    b"$3\r\nraw\r\n"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::ObjectEncoding, &[b"obj_k"]),
    b"$9\r\nhashtable\r\n"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::ObjectEncoding, &[b"tier_k"]),
    b"$9\r\nhashtable\r\n"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::ObjectEncoding, &[b"nope"]),
    b"$-1\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f0, n0),
    "OBJECT 慢臂四形恒零入账（修复前分层键虚报 2 条、其余 1-2 条）"
  );
}

/// 冷臂「磁盘候选 × 分层驻态」交叉形：flush_and_evict_all 换出内存环后
/// 四形入账恒 0/0，且分层键冷形回形与热臂逐字节等（$9 hashtable）
#[test]
fn object_slow_arm_disk_candidate_zero_and_byte_parity() {
  let (rt, mut c, api, handle, _dir, store) = env("obj-account-slow-cold.db");
  seed_keys(&rt, &mut c, &store);

  // 热形基线（热臂捕获，供逐字节对拍）
  let hot_str = roundtrip(&rt, &mut c, &[b"OBJECT", b"ENCODING", b"str_k"]);
  let hot_obj = roundtrip(&rt, &mut c, &[b"OBJECT", b"ENCODING", b"obj_k"]);
  let hot_tier = roundtrip(&rt, &mut c, &[b"OBJECT", b"ENCODING", b"tier_k"]);
  assert_eq!(hot_tier, b"$9\r\nhashtable\r\n", "热树态基线");

  // Meta 记录连同全部信封换出内存环：四形一律磁盘候选
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let (f0, n0) = counts(&handle);

  assert_eq!(
    slow_direct(&rt, &api, RespCommand::ObjectEncoding, &[b"str_k"]),
    hot_str,
    "字符串冷形回形逐字节等热形"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::ObjectEncoding, &[b"obj_k"]),
    hot_obj,
    "信封冷形回形逐字节等热形"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::ObjectEncoding, &[b"tier_k"]),
    hot_tier,
    "分层驻态冷树态回形逐字节等热形（$9 hashtable，锁「磁盘候选 × 分层驻态」交叉面）"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::ObjectEncoding, &[b"nope"]),
    b"$-1\r\n",
    "缺失键冷形回 nil"
  );
  assert_eq!(
    counts(&handle),
    (f0, n0),
    "OBJECT 慢臂磁盘候选四形恒零入账（修复前虚报 1-2 条随驻态跳变）"
  );
}

/// 同句柄 GET 对照臂：读命令簿记入口零漂移（快慢各恰 1 found / 1
/// notfound），防 OBJECT 收口过零误伤 GET 族入账面
#[test]
fn get_control_arm_still_records_single() {
  let (rt, mut c, api, handle, _dir, store) = env("obj-account-get-ctrl.db");
  seed_keys(&rt, &mut c, &store);

  let (f0, n0) = counts(&handle);
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"str_k"]), b"$1\r\nv\r\n");
  assert_eq!(counts(&handle), (f0 + 1, n0), "GET 命中热臂恰 1 found");
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Get, &[b"str_k"]),
    b"$1\r\nv\r\n"
  );
  assert_eq!(counts(&handle), (f0 + 2, n0), "GET 命中冷臂恰 1 found");

  let (f1, n1) = counts(&handle);
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"nope"]), b"$-1\r\n");
  assert_eq!(counts(&handle), (f1, n1 + 1), "GET 缺失热臂恰 1 notfound");
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Get, &[b"nope"]),
    b"$-1\r\n"
  );
  assert_eq!(counts(&handle), (f1, n1 + 2), "GET 缺失冷臂恰 1 notfound");
}
