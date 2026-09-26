//! SET 条件写（无条件 / NX / XX / KEEPTTL + GET 形态）对升阶分层集合键
//! （KeyTag::Meta 域非 RangeIndex）的契约回归，对标 C#
//! libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional:772-845：
//! 对象键 → WRONGTYPE → promote 事务 DELETE → 重试 SET_Conditional，应答按
//! 重试结果裁决（无条件 → OK；XX → nil 且键被删；NX → OK 写入；
//! KEEPTTL → OK 且集合时代键级 TTL 不回填）；GET 形态
//! （:827-845）WRONGTYPE 直接错误帧、不删不重试。
//!
//! rust 分流：快路径 `network_set_conditional` 与慢路径 `slow_set_conditional`
//! 的对象键分派臂对信封 / 升阶 Meta 双域同款匹配；Meta 域写原语
//! （try_delete_sync / try_upsert_sync）在场即降级，交异步臂
//! delete_string / upsert_string 完整树清退闭环（本测试的升阶臂全部真实
//! 穿越「快路径裁决 → 降级 → 慢路径闭环」两程）。
//!
//! 双态透明门禁（doc/zh/collection.md 第 5 节）：同一矩阵在内存信封态与
//! 升阶 Meta 态各执行一遍，五格应答逐字节全等；升阶态另以
//! `load_collection_stub` 直读底层证「清退后树存根消失、GET 形态零副作用」。

use std::sync::Arc;

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::auto_exec;
use wresp::{cmd_strings::RESP_ERR_WRONG_TYPE, command::RespCommand};

fn open_env(
  tag: &str,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
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

/// 建 hash 键：`tiered = true` 时批量 HSET 越过条目数升阶门限（确认底层
/// BfTree 存根在场），否则两条目的内存信封态
fn build_hash(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  key: &[u8],
  tiered: bool,
) -> usize {
  assert!(
    [b":1\r\n".as_slice(), b":0\r\n".as_slice()]
      .contains(&auto_exec(api, rt, s, RespCommand::Del, &[key]).as_slice()),
    "重建前 DEL 清场（首建缺席回 0，重建命中回 1）"
  );
  let total = if tiered { 8000 } else { 2 };
  let val_payload = vec![b'x'; 600];
  let mut buf = ItoaBuffer::new();
  for chunk_start in (1..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(key.to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("f{i}").into_bytes());
      if tiered {
        args.push(val_payload.clone());
      } else {
        args.push(buf.format(i).as_bytes().to_vec());
      }
    }
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(api, rt, s, RespCommand::Hset, &slices);
  }
  assert_eq!(
    auto_exec(api, rt, s, RespCommand::Hset, &[key, b"f1", b"999"]),
    b":0\r\n",
    "重复字段 HSET 计数 0（键已成型）"
  );
  assert_eq!(
    auto_exec(api, rt, s, RespCommand::Type, &[key]),
    b"+hash\r\n",
  );
  total
}

/// 升阶态底层 BfTree 存根在场断言（tiered 判据单点，Meta 域事实）
fn assert_stub(store: &Arc<WedbStore<SegmentedDevice>>, rt: &Runtime, key: &[u8], expect: bool) {
  let sess = store.new_session().unwrap();
  let stub = rt.block_on(sess.load_collection_stub(key)).unwrap();
  assert_eq!(
    stub.is_some(),
    expect,
    "升阶存根预期 {} 实际 {}",
    expect,
    stub.is_some()
  );
}

/// 五格矩阵：GET 形态 → 无条件 SET →（重建）NX →（重建）XX →（重建）KEEPTTL。
/// 返回各命令应答原文（双态逐字节比对输入）。断言内嵌：TTL / TYPE / GET 终态
fn run_matrix(
  api: &GarnetApi,
  store: &Arc<WedbStore<SegmentedDevice>>,
  rt: &Runtime,
  s: &mut RespServerSession,
  key: &[u8],
  tiered: bool,
) -> Vec<Vec<u8>> {
  let mut out = Vec::new();
  let total = build_hash(api, rt, s, key, tiered);
  if tiered {
    assert_stub(store, rt, key, true);
  }

  // 格 1：GET 形态 → WRONGTYPE 错误帧，键与树原样（C# :827-845 不删不重试）
  out.push(auto_exec(
    api,
    rt,
    s,
    RespCommand::Set,
    &[key, b"v", b"GET"],
  ));
  assert!(
    out[0].first() == Some(&b'-'),
    "GET 形态须直接错误帧: {:?}",
    String::from_utf8_lossy(&out[0])
  );
  assert_eq!(
    out[0],
    format!("-{RESP_ERR_WRONG_TYPE}\r\n").into_bytes(),
    "GET 形态错误帧文案单点"
  );
  assert_eq!(
    auto_exec(api, rt, s, RespCommand::Hlen, &[key]),
    format!(":{total}\r\n").into_bytes(),
    "GET 形态不得动集合键"
  );
  if tiered {
    assert_stub(store, rt, key, true);
  }

  // 格 2：无条件 SET 覆盖 → OK + string，无 TTL（C# DELETE 重试无条件写）
  out.push(auto_exec(api, rt, s, RespCommand::Set, &[key, b"v"]));
  out.push(auto_exec(api, rt, s, RespCommand::Get, &[key]));
  out.push(auto_exec(api, rt, s, RespCommand::Type, &[key]));
  out.push(auto_exec(api, rt, s, RespCommand::Ttl, &[key]));
  assert_eq!(out[1], b"+OK\r\n");
  assert_eq!(out[2], b"$1\r\nv\r\n");
  assert_eq!(out[3], b"+string\r\n");
  assert_eq!(out[4], b":-1\r\n", "覆盖后不得残留集合时代幽灵 TTL");
  if tiered {
    assert_stub(store, rt, key, false);
  }

  // 格 3：SET NX → 删旧键写入回 OK（C# DELETE 后重试 NX 成立）
  build_hash(api, rt, s, key, tiered);
  out.push(auto_exec(api, rt, s, RespCommand::Set, &[key, b"v", b"NX"]));
  out.push(auto_exec(api, rt, s, RespCommand::Get, &[key]));
  assert_eq!(out[5], b"+OK\r\n", "NX 对对象键 DELETE 后须写入回 OK");
  assert_eq!(out[6], b"$1\r\nv\r\n");
  if tiered {
    assert_stub(store, rt, key, false);
  }

  // 格 4：SET XX → 删键回 nil（C# DELETE 后重试必 NOTFOUND）
  build_hash(api, rt, s, key, tiered);
  out.push(auto_exec(api, rt, s, RespCommand::Set, &[key, b"v", b"XX"]));
  out.push(auto_exec(api, rt, s, RespCommand::Exists, &[key]));
  assert_eq!(out[7], b"$-1\r\n", "XX 对对象键 DELETE 后须回 nil");
  assert_eq!(out[8], b":0\r\n", "XX 格键须被删除（SCAN 不可见）");
  if tiered {
    assert_stub(store, rt, key, false);
  }

  // 格 5：SET KEEPTTL → OK + string，集合时代 TTL 不回填（幽灵 TTL 回归锁）
  build_hash(api, rt, s, key, tiered);
  assert_eq!(
    auto_exec(api, rt, s, RespCommand::Expire, &[key, b"100"]),
    b":1\r\n",
    "前置键级 TTL"
  );
  out.push(auto_exec(
    api,
    rt,
    s,
    RespCommand::Set,
    &[key, b"v", b"KEEPTTL"],
  ));
  out.push(auto_exec(api, rt, s, RespCommand::Ttl, &[key]));
  out.push(auto_exec(api, rt, s, RespCommand::Get, &[key]));
  assert_eq!(out[9], b"+OK\r\n");
  assert_eq!(out[10], b":-1\r\n", "KEEPTTL 不得回填集合时代 TTL");
  assert_eq!(out[11], b"$1\r\nv\r\n");
  if tiered {
    assert_stub(store, rt, key, false);
  }
  out
}

/// 主用例：升阶 Meta 态五格矩阵与内存信封态逐字节全等（双态透明），
/// 升阶态内嵌存根清退 / WRONGTYPE 零副作用断言
#[test]
fn set_conditional_tiered_matches_in_memory() {
  let (rt, api, store, _dir) = open_env("set-cond-tiered.db");
  let mut s = session_with(&api);

  let tiered = run_matrix(&api, &store, &rt, &mut s, b"th", true);
  let memory = run_matrix(&api, &store, &rt, &mut s, b"tm", false);
  assert_eq!(
    tiered, memory,
    "SET 条件写五格矩阵在升阶 / 内存双态应答必须逐字节全等"
  );
}
