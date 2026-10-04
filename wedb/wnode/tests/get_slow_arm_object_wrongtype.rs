//! GET 慢臂（SG 批量冷读口）对象键判型回归（票
//! wnode-get-slow-arm-object-key-nil-wrongtype-fork，P2）
//!
//! 缺陷面：慢臂 C::Get 唯走批量口（wkv `session/raw/batch.rs` 恒
//! KeyTag::String 单域探针），对象信封键在 String 物理域恒 NotFound，emit
//! 闭包按 None 出 nil 帧并计 notfound；快臂 network_get 经
//! read_user_sync 三域折叠（ttl_sync.rs）对同一键回 WRONGTYPE 且零入账——
//! 同一逻辑键冷态（信封落盘驱逐）答 nil、热态答 -WRONGTYPE，应答帧与簿记
//! 双分叉（review.md 4.2 多路径行为同构违例；快臂既有锚
//! object_envelope_regression.rs「GET 集合键须回 WRONGTYPE 不是 nil」在
//! 降级面失守）。
//!
//! 修复形态：判型收口 wnode 漏斗层 [`StorageSession::read_user_batch_into`]
//! ——批量口保持 wkv 单域取值不动，String 域确认缺失的键经
//! `object_kind_alive_with_prefix`（与单键三域折叠 read_user_quiet 同一
//! 判型通道，零新机制）续探信封 / Meta 域，命中改出 WRONGTYPE 错误帧且
//! 簿记静默，nil 占位帧原位替换、N 键 N 帧序不漂移。
//!
//! 测试全真存储真协议帧，无 mock：慢臂经 SlowWait::for_command 直驱（与
//! 降级快照投递同径，先例 object_read_accounting.rs 同形）与 SG 流水线混批
//! 整批判停两路真实触达，冷化经 store.flush_and_evict_all 换出内存环
//!（信封 / Meta 记录连带落盘为磁盘候选）。

use std::sync::Arc;

use compio::runtime::Runtime;
use itoa::Buffer;
use wkv::SessionLocking;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::GarnetApi, slow_path::SlowWait},
};
use wnode_test::{counts, metrics_env as env, pump, roundtrip};
use wresp::command::RespCommand;
use wtest_base::resp_frame;

/// 慢臂直驱帧型版本入参（RESP2，与热臂同版）
const RESP_V2: u8 = 2;

/// WRONGTYPE 错误帧全帧形（write_resp_error：`-` + 消息 + CRLF）
const WRONGTYPE_FRAME: &[u8] =
  b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";

/// 慢臂直驱（与降级快照投递同径，不经会话快路径）
fn slow_direct(rt: &Runtime, api: &GarnetApi, args: &[&[u8]]) -> Vec<u8> {
  let snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  rt.block_on(async {
    SlowWait::for_command(
      api,
      RespCommand::Get,
      snapshot,
      RESP_V2,
      SessionLocking::Basic,
    )
    .resolve()
    .await
  })
}

/// 三形键装载：SET 字符串键、小 HSET 信封对象键、超阈值升阶的分层驻态键
///（Meta 域命中，经 load_collection_stub 确证，先例 object_read_accounting.rs
/// seed_keys 同形；升阶键锁「Meta 域续探」腿，信封键锁「信封域续探」腿）
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

/// 慢臂磁盘候选降级面三形应答与簿记：信封 / 升阶对象键答 WRONGTYPE 非
/// nil（帧形逐字节等快臂热形）、真缺失仍 nil、字符串命中不回归；WrongType
/// 键零入账（修复前：对象键冷态答 nil 且各虚增 1 notfound）
#[test]
fn slow_arm_object_keys_reply_wrongtype_with_silent_bookkeeping() {
  let (rt, mut c, api, handle, _dir, store) = env("get-slow-wrongtype-cold.db");
  seed_keys(&rt, &mut c, &store);

  // 热形基线（快臂三域折叠出 WRONGTYPE，object_envelope_regression.rs 既有
  // 锚「GET 集合键须回 WRONGTYPE 不是 nil」的热侧）
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"str_k"]), b"$1\r\nv\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GET", b"obj_k"]),
    WRONGTYPE_FRAME,
    "热信封对象键快臂须回 WRONGTYPE"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GET", b"tier_k"]),
    WRONGTYPE_FRAME,
    "热升阶对象键快臂须回 WRONGTYPE"
  );

  // 全库换出内存环：String / 信封 / Meta 记录一并落盘为磁盘候选
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let (f0, n0) = counts(&handle);

  // 慢臂直驱（磁盘候选降级承接）：对象键 WRONGTYPE、帧形逐字节等热形；
  // 字符串命中冷读装载不回归；真缺失仍 nil（§133 采序不受波及）
  assert_eq!(
    slow_direct(&rt, &api, &[b"str_k"]),
    b"$1\r\nv\r\n",
    "冷字符串键命中帧不回归"
  );
  assert_eq!(
    slow_direct(&rt, &api, &[b"obj_k"]),
    WRONGTYPE_FRAME,
    "冷信封对象键慢臂须回 WRONGTYPE 非 nil（修复前此处答 $-1）"
  );
  assert_eq!(
    slow_direct(&rt, &api, &[b"tier_k"]),
    WRONGTYPE_FRAME,
    "冷升阶对象键慢臂须回 WRONGTYPE 非 nil（修复前此处答 $-1）"
  );
  assert_eq!(
    slow_direct(&rt, &api, &[b"nope"]),
    b"$-1\r\n",
    "真缺失键慢臂仍答 nil"
  );
  assert_eq!(
    counts(&handle),
    (f0 + 1, n0 + 1),
    "簿记四键合计恰 1 found + 1 notfound：WrongType 两键零入账、缺失键 1 notfound"
  );
}

/// SG 混合批降级重放：热字符串 + 冷对象键 + 缺失键流水线整批判停慢臂后，
/// N 键 N 帧序不漂移，批内各键帧形与逐键应答逐字节一致（锁「同批内帧形
/// 一致」，修复前冷对象键在批内被 truncate 后改答 nil）
#[test]
fn sg_mixed_batch_replay_frames_match_per_key_replies() {
  let (rt, mut c, _api, handle, _dir, store) = env("get-slow-wrongtype-sg.db");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"obj_k", b"f", b"v"]),
    b":1\r\n"
  );

  // 冷化全部后补写热键：构造「热字符串 + 冷对象键 + 缺失键」混合批
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"hot_k", b"hot"]),
    b"+OK\r\n"
  );

  // 逐键基线：hot_k 快臂热命中；obj_k 单键冷态降级慢臂；nope_k 快臂缺失
  let base_hot = roundtrip(&rt, &mut c, &[b"GET", b"hot_k"]);
  assert_eq!(base_hot, b"$3\r\nhot\r\n");
  let base_obj = roundtrip(&rt, &mut c, &[b"GET", b"obj_k"]);
  assert_eq!(base_obj, WRONGTYPE_FRAME);
  let base_miss = roundtrip(&rt, &mut c, &[b"GET", b"nope_k"]);
  assert_eq!(base_miss, b"$-1\r\n");

  let (f0, n0) = counts(&handle);

  // 三条 GET 流水线一次投喂：快臂 SG 聚合，热键命中后续遇冷对象键（信封
  // 域磁盘候选）整批判停，sg_batched_keys 全量降级慢臂批量口重放
  let pipeline = resp_frame(&[b"GET", b"hot_k"])
    .into_iter()
    .chain(resp_frame(&[b"GET", b"obj_k"]))
    .chain(resp_frame(&[b"GET", b"nope_k"]))
    .collect::<Vec<u8>>();
  let (_, mut out) = pump(&mut c, &pipeline);
  let slow = c
    .take_slow_wait()
    .expect("热冷混合批必须整批判停降级慢臂，否则本用例未触达批量冷读口");
  out.extend(rt.block_on(async { slow.resolve().await }));

  let mut expect = base_hot;
  expect.extend_from_slice(&base_obj);
  expect.extend_from_slice(&base_miss);
  assert_eq!(
    out, expect,
    "SG 混合批慢臂重放 N 键 N 帧序不漂移、各键帧形与逐键应答逐字节一致"
  );
  assert_eq!(
    counts(&handle),
    (f0 + 1, n0 + 1),
    "混批簿记：热命中 1 found、缺失 1 notfound、WrongType 键零入账"
  );
}
