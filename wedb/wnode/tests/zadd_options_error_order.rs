//! ZADD 复合违规选项的错误帧判定序回归（票 zcode-r122c-zsetcore1）
//!
//! 对标 libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetOptions:53-77：
//! 三条互斥校验写入同一局部变量 optionsError（:58 XX&NX → :66 GT/LT/NX →
//! :71 INCR 多对），后写覆盖前写、无短路，:73-77 单点 writer.WriteError 落帧
//! ——故复合违规时「末位命中的规则」胜出。
//!
//! 修复前 Rust 单源 wedb/wcol/src/zset/sorted_set_object_impl.rs
//! :sorted_set_add_get_options 逐条提前 return（首中即返），NX XX GT 形态
//! 回 XX_NX 帧与 C# 的 GT_LT_NX 帧字节分叉。本组测试以 C# CmdStrings 字面量
//! 逐字节锁死三组合形态，并在单源的两处消费点各验一次：
//! 1. 内存态信封臂 wcol :sorted_set_add（RESP 快臂与慢臂 run_operate 同汇此点）；
//! 2. 分层树内臂 wnode tiered_collection_ops/zset.rs Zadd 臂。
//!
//! 另锁 C# 判定序的两处先行关系：互斥帧先于尾段 syntax error（:73 先于 :81）、
//! 单条违规帧不因改动而漂移。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wcol::types::member_ttl::encode_member;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;
use wval::GarnetObjectType;

type TestStore = WedbStore<SegmentedDevice>;

/// libs/server/Resp/CmdStrings.cs:RESP_ERR_XX_NX_NOT_COMPATIBLE（C# 原型字面量，
/// 不引用 wresp 常量以免 Rust 自证）
const XX_NX_ERR: &[u8] = b"-ERR XX and NX options at the same time are not compatible\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GT_LT_NX_NOT_COMPATIBLE
const GT_LT_NX_ERR: &[u8] =
  b"-ERR GT, LT, and/or NX options at the same time are not compatible\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR
const INCR_PAIR_ERR: &[u8] = b"-ERR INCR option supports a single increment-element pair\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_SYNTAX_ERROR
const SYNTAX_ERR: &[u8] = b"-ERR syntax error\r\n";
/// arity 门（C# SortedSetCommands 同表）先行于对象层
const ARITY_ERR: &[u8] = b"-ERR wrong number of arguments for 'ZADD' command\r\n";

fn open_env(tag: &str) -> (Runtime, GarnetApi, Arc<TestStore>, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 手工升阶为分层树形态（与生产 export_entries 同形 entries：成员 + encode_member）
fn promote_zset(rt: &Runtime, store: &Arc<TestStore>, key: &[u8], members: &[(&[u8], f64)]) {
  let ents: Vec<(Vec<u8>, Vec<u8>)> = members
    .iter()
    .map(|(m, s)| (m.to_vec(), encode_member(&s.to_be_bytes(), None)))
    .collect();
  let sess = store.new_session().unwrap();
  rt.block_on(sess.promote_collection_to_bftree(
    key,
    GarnetObjectType::SortedSet,
    ents,
    i64::MAX,
    false,
  ))
  .unwrap();
}

/// 六组错误帧用例（键名替换后逐字节比对）
///
/// 判定序对标 C# GetOptions 覆写序：规则一 XX&NX、规则二 GT/LT/NX、
/// 规则三 INCR 多对，复合命中末位胜出。
fn assert_frames(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  state: &str,
  key: &[u8],
) {
  let mut case = |args: Vec<&[u8]>, want: &'static [u8], desc: &str| {
    let mut full: Vec<&[u8]> = Vec::with_capacity(args.len() + 1);
    full.push(key);
    full.extend_from_slice(&args);
    let out = auto_exec(api, rt, s, RespCommand::Zadd, &full);
    assert_eq!(
      out,
      want,
      "{state} 形态 {desc} 错误帧分叉：实际 {:?}",
      String::from_utf8_lossy(&out)
    );
  };

  // ---- 复合违规：末位命中胜出（本票主案）----
  case(
    vec![b"NX", b"XX", b"GT", b"1", b"m"],
    GT_LT_NX_ERR,
    "NX XX GT（规则二覆写规则一）",
  );
  case(
    vec![b"XX", b"NX", b"LT", b"1", b"m"],
    GT_LT_NX_ERR,
    "XX NX LT（选项次序无关，覆写序固定）",
  );
  case(
    vec![b"GT", b"NX", b"XX", b"1", b"m"],
    GT_LT_NX_ERR,
    "GT NX XX（规则二胜出）",
  );
  case(
    vec![b"NX", b"XX", b"INCR", b"1", b"a", b"2", b"b"],
    INCR_PAIR_ERR,
    "NX XX INCR 双对（规则三覆写规则一）",
  );
  case(
    vec![b"GT", b"LT", b"NX", b"INCR", b"1", b"a", b"2", b"b"],
    INCR_PAIR_ERR,
    "GT LT NX INCR 双对（规则三覆写规则二）",
  );
  case(
    vec![b"XX", b"NX", b"GT", b"LT", b"INCR", b"1", b"a", b"2", b"b"],
    INCR_PAIR_ERR,
    "四互斥 + INCR 双对（规则三末位胜出）",
  );

  // ---- 单条违规回归：不因覆写化而漂移 ----
  case(vec![b"XX", b"NX", b"1", b"m"], XX_NX_ERR, "XX NX 单违规");
  case(
    vec![b"GT", b"LT", b"1", b"m"],
    GT_LT_NX_ERR,
    "GT LT 单违规（规则二）",
  );
  case(
    vec![b"GT", b"NX", b"1", b"m"],
    GT_LT_NX_ERR,
    "GT NX 单违规（规则二）",
  );
  case(
    vec![b"LT", b"NX", b"1", b"m"],
    GT_LT_NX_ERR,
    "LT NX 单违规（规则二）",
  );
  case(
    vec![b"INCR", b"1", b"a", b"2", b"b"],
    INCR_PAIR_ERR,
    "INCR 双对单违规（规则三）",
  );

  // ---- 判定序先行关系：互斥帧先于尾段 syntax error（C# :73 先于 :81）----
  case(
    vec![b"NX", b"XX", b"1"],
    XX_NX_ERR,
    "NX XX + 奇数尾（互斥帧胜出）",
  );
  case(
    vec![b"NX", b"XX", b"GT"],
    GT_LT_NX_ERR,
    "NX XX GT + 空尾段（互斥帧胜出，非 syntax error）",
  );

  // ---- 尾段与 arity 回归（既有 :279-283 案不动）----
  case(
    vec![b"XX", b"m1"],
    SYNTAX_ERR,
    "XX + 单成员奇数尾 → syntax error",
  );
  case(
    vec![b"GT", b"1", b"a", b"2"],
    SYNTAX_ERR,
    "GT + 奇数尾（无互斥命中）→ syntax error",
  );
  let out = auto_exec(api, rt, s, RespCommand::Zadd, &[key, b"XX"]);
  assert_eq!(
    out, ARITY_ERR,
    "{state} 形态仅选项 2 参先被 arity 门拦（C# 同表）"
  );
}

/// 内存态信封臂（wcol :314 消费点）：复合违规末位胜出 + 无副作用建键
#[test]
fn zadd_options_error_overwrite_order_memory() {
  let (rt, api, _store, _dir) = open_env("zadd-opt-order-mem.db");
  let mut s = session_with(&api);
  assert_frames(&api, &rt, &mut s, "内存态", b"zk");

  // 错误帧路径零写入（GetOptions 先于任何字典变更）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"zk"]),
    b":0\r\n",
    "内存态复合违规路径不得创建键"
  );

  // 正路径回归：CH 计账与 INCR 单对帧不受判定序改动影响
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"zk", b"CH", b"1", b"a", b"2", b"b"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"zk", b"INCR", b"3", b"a"]
    ),
    b"$1\r\n4\r\n"
  );
}

/// 分层树内臂（wnode zset.rs :148 消费点）：与内存态/C# 三方同帧
#[test]
fn zadd_options_error_overwrite_order_tiered() {
  let (rt, api, store, _dir) = open_env("zadd-opt-order-tiered.db");
  let mut s = session_with(&api);
  promote_zset(&rt, &store, b"zk", &[(b"seed" as &[u8], 1.0)]);

  assert_frames(&api, &rt, &mut s, "分层态", b"zk");

  // 分层臂同样零副作用：seed 成员分值不变、成员数不变
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"zk", b"seed"]),
    b"$1\r\n1\r\n",
    "分层态错误帧路径不得改值"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[b"zk"]),
    b":1\r\n",
    "分层态错误帧路径不得增成员"
  );
}
