//! GETDEL 慢臂探针零入账与 GETEX 降级重放单帧入账回归（票 zcode-r143c-getexbig）
//!
//! 案一（GETDEL）：C# GETDEL 系 RMW 前置读形态命令，全链零命中/未命中入账
//! （KeyAdminCommands.cs:288-311 三臂只写应答；MainStoreOps.cs:190-213 GETDEL
//! 无 incr_session_found/notfound，同文件 GETEX 对照臂实存）。rust 快臂
//! keys.rs:network_getdel 传 None 合纪律，慢臂 key_admin_commands/slow.rs
//! C::Getdel 探针原走 storage.read_user 簿记漏斗，磁盘候选/TTL 待裁决临界键
//! 的 GETDEL 命中/缺席各多计一帧。修复形态：探针改走既有漏斗的零入账薄包装
//! StorageSession::read_user_quiet（镜像 read_tag_quiet 先例形，不起第二套
//! 判型机制），GET/GETEX 等读命令慢臂簿记出口不动。
//!
//! 案二（GETEX）：C# GETEX 恰好入账一次（MainStoreOps.cs:155-181 单次 RMW
//! pending 就地收尾后 Found→found / IsWrongType 静默 / else→notfound 单帧）。
//! rust 快臂原 read_user_sync 传 self.session_metrics 即时落账，TTL 写遭环形
//! 页翻转回 Ok(false) 时 truncate+整命令降级慢臂重放，慢臂 read_user 漏斗再
//! 计一帧——降级 GETEX 命中净增二。修复形态：循同家 network_get_sg 守卫纪律
//! 把入账移至收尾判定单点——快臂传 None 静默、本地三态布尔仅 Hit 闭环置
//! found / Missing 置 notfound，函数尾经 fold_outcome 单规则补账恰一条；
//! 一切降级出口零入账交慢臂唯一出口收口。
//!
//! 测试全真存储真协议帧，无 mock：慢臂经 SlowWait::for_command 直驱
//! （dump_read_accounting.rs 同形先例，与快臂 Deferred 降级快照投递同径），
//! 快臂经会话消费者生产口驱动；双计取证臂复用 hyperloglog.rs degrade_env
//! 同款 16KB×4 页小环形日志压力夹具——回绕复用槽位恒遇 PageNotReady；SET
//! 灌新键 + 对百轮前已被驱逐成磁盘候选旧键的 GETEX EX 冷重放下，断言每命令
//! 恒计一帧（修复前降级笔快慢臂双计净增二为红灯；同轮新键 8B TTL 追加从不
//! 跨页翻转边界，旧形同键风暴的降级前提在该页径下结构不可达，探针实测
//! getex_deg=0）。
//!
//! 同谱扩臂（票 zcode-r147c-incrovf 案二，P4）：string-RMW 族 INCR/INCRBY/
//! DECR/DECRBY/INCRBYFLOAT/SETRANGE/APPEND 慢臂前置读同走
//! `StorageSession::read_user_quiet` 零入账口——C# `MainStoreOps.cs:Increment`
//! 全链零 incr_session_*、SETRANGE/APPEND 走 `RMW_MainStore` 口
//! （`AdvancedOps.cs:RMW_MainStore` 与 `CompletePending.cs` 均不触
//! total_found/total_notfound），rust 慢臂原走簿记漏斗 `read_user` 系本席
//! 破纪；快臂 incr.rs 传 None 合规零改动。本文件末尾两测锁该族双臂零入账
//! 与 GET 簿记对照口径。

use std::sync::Arc;

use compio::runtime::Runtime;
use itoa::Buffer;
use tempfile::TempDir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wmetric::SessionMetricsHandle;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
    slow_path::SlowWait,
  },
};
use wnode_test::{counts, metrics_env as env};
use wresp::command::RespCommand;
use wtest_base::resp_frame as frame;

/// 慢臂直驱的 RESP 协议版本入参
const RESP_V2: u8 = 2;
/// 风暴轮数（越 4 页 × 16KB 环形容量数倍，保证回绕驱逐必然发生）
const STORM_ROUNDS: usize = 500;
/// 冷重放滞回轮数：SET 500 键 × 均值 ~850B 连续灌写，回绕驱逐线距尾约
/// 32KB，百轮（~85KB）前键必已离页成磁盘候选——对旧键的 GETEX 降级重放
/// 由此为确定性事件（同轮新键的 8B TTL 追加从不跨页翻转边界，旧形
/// 「同键 SET+GETEX 即触发降级」前提在该页径下结构不可达，探针实测
/// getex_deg=0；冷代形实测 400/400 全降级）
const COLD_LAG: usize = 100;

/// 风暴键值形（i 决定字节与长度，供冷代重放方按滞后索引复算期望整值）
fn storm_val(i: usize) -> Vec<u8> {
  vec![b'a' + (i % 26) as u8; 700 + i % 7 * 90]
}

/// 环形页翻转压力执行域（hyperloglog.rs degrade_env 同款 16KB×4 页小环形
/// 日志，append 回绕遇未驱逐旧页恒现 PageNotReady）+ 同款采样句柄装配
fn storm_env(
  tag: &str,
) -> (
  Runtime,
  RespSessionConsumer,
  GarnetApi,
  Arc<SessionMetricsHandle>,
  TempDir,
) {
  let dir = tempfile::tempdir().unwrap();
  let config = StoreConfig::new(1024, 16 * 1024, 4, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let handle = Arc::new(SessionMetricsHandle::default());
  let api: GarnetApi = Arc::new(
    StoreGarnetApi::new(store.new_session().unwrap())
      .with_session_metrics(Some(Arc::clone(&handle))),
  );
  let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone());
  c.attach_session_metrics(Some(Arc::clone(&handle)));
  (Runtime::new().unwrap(), c, api, handle, dir)
}

/// RESP2 bulk string 应答帧编码（GETEX/GETDEL 命中整值应答对拍用）
fn bulk_frame(val: &[u8]) -> Vec<u8> {
  let mut buf = Buffer::new();
  let mut out = Vec::new();
  out.push(b'$');
  out.extend_from_slice(buf.format(val.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  out.extend_from_slice(val);
  out.extend_from_slice(b"\r\n");
  out
}

/// 快臂单命令往返（降级挂起体由 block_on 承接闭环），并回报本命令是否降级
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> (Vec<u8>, bool) {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = c.try_consume_messages_into(&mut resp);
  match c.take_slow_wait() {
    Some(slow) => {
      resp.extend_from_slice(&rt.block_on(slow.resolve()));
      (resp, true)
    }
    None => (resp, false),
  }
}

/// 慢臂直驱（与降级快照投递同径，不经会话快路径）
fn slow_direct(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  rt.block_on(async {
    SlowWait::for_command(api, cmd, snapshot, RESP_V2)
      .resolve()
      .await
  })
}

/// 案一主锁：GETDEL 慢臂探针零入账（C# GETDEL 全链零 incr_session_found/
/// notfound 对位），命中/缺席/对象键三形 total_found/total_notfound 恒零增量
/// （修复前簿记漏斗尾 record_outcome 各多计一帧）
#[test]
fn getdel_slow_arm_probe_records_nothing() {
  let (rt, mut c, api, handle, _dir, _store) = env("getdel-account-slow.db");

  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"k", b"v"]).0, b"+OK\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"obj", b"f", b"v"]).0,
    b":1\r\n"
  );

  // 慢臂缺席 → nil 且 0/0（修复前 notfound+1）
  let (f0, n0) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Getdel, &[b"missing"]),
    b"$-1\r\n",
    "缺席 GETDEL 慢臂应答 nil"
  );
  assert_eq!(counts(&handle), (f0, n0), "缺席 GETDEL 慢臂零入账");

  // 慢臂对象键 → WRONGTYPE 且 0/0
  let obj = slow_direct(&rt, &api, RespCommand::Getdel, &[b"obj"]);
  assert!(
    obj.starts_with(b"-WRONGTYPE"),
    "对象键 GETDEL 慢臂应答 WRONGTYPE: {obj:?}"
  );
  assert_eq!(counts(&handle), (f0, n0), "对象键 GETDEL 慢臂零入账");

  // 慢臂命中 → 整值且 0/0（修复前 found+1）
  let hit = slow_direct(&rt, &api, RespCommand::Getdel, &[b"k"]);
  assert_eq!(hit, bulk_frame(b"v"), "GETDEL 慢臂命中应答整值");
  assert_eq!(
    counts(&handle),
    (f0, n0),
    "GETDEL 慢臂命中零入账（修复前多计 found 一帧）"
  );

  // 对照组钉死口径分野：GET 慢臂命中/缺席仍各恰一条（其 C# 侧本有计）
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"g", b"w"]).0, b"+OK\r\n");
  let (f1, n1) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Get, &[b"missing"]),
    b"$-1\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f1, n1 + 1),
    "GET 慢臂缺席仍恰 1 notfound（簿记出口不动）"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Get, &[b"g"]),
    bulk_frame(b"w")
  );
  assert_eq!(
    counts(&handle),
    (f1 + 1, n1 + 1),
    "GET 慢臂命中仍恰 1 found（GETDEL/GET 口径分野钉死）"
  );
}

/// 案一副锁：GETDEL 快臂零入账不变（keys.rs:150 传 None 既有纪律、
/// resp_tests.rs getdel 应答形锁零改动）；GET 快臂命中/缺席各恰一条回归
#[test]
fn getdel_fast_arm_zero_and_get_fast_arm_parity() {
  let (rt, mut c, _api, handle, _dir, _store) = env("getdel-account-fast.db");

  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"k", b"v"]).0, b"+OK\r\n");
  let (f0, n0) = counts(&handle);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GETDEL", b"missing"]).0,
    b"$-1\r\n"
  );
  assert_eq!(counts(&handle), (f0, n0), "GETDEL 快臂缺席零入账");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GETDEL", b"k"]).0,
    bulk_frame(b"v"),
    "GETDEL 快臂应答形零改动"
  );
  assert_eq!(counts(&handle), (f0, n0), "GETDEL 快臂命中零入账");

  let (f1, n1) = counts(&handle);
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"k"]).0, b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f1, n1 + 1),
    "GET 快臂缺席恰 1 notfound（读命令簿记口径不动）"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"k", b"v"]).0, b"+OK\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"k"]).0, bulk_frame(b"v"));
  assert_eq!(counts(&handle), (f1 + 1, n1 + 1), "GET 快臂命中恰 1 found");
}

/// 案二常规形态锁：GETEX 快臂/慢臂各形恰一帧（Hit=found、Missing=notfound、
/// WrongType 静默，对标 C# MainStoreOps GETEX 三臂），应答形零改动
#[test]
fn getex_records_single_frame_per_shape_both_arms() {
  let (rt, mut c, api, handle, _dir, _store) = env("getex-account-both.db");

  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"k", b"v"]).0, b"+OK\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"obj", b"f", b"v"]).0,
    b":1\r\n"
  );

  // 快臂 Hit + EX 10：found 恰 1、notfound 0（收尾判定单点补账与漏斗
  // 即时入账的等价性锁）
  let (f0, n0) = counts(&handle);
  let (resp, degraded) = roundtrip(&rt, &mut c, &[b"GETEX", b"k", b"EX", b"10"]);
  assert_eq!(resp, bulk_frame(b"v"), "GETEX 快臂应答形零改动");
  assert!(!degraded, "常规存储域快臂 GETEX 不应降级");
  assert_eq!(counts(&handle), (f0 + 1, n0), "GETEX 快臂命中恰 1 found");

  // 快臂 Missing：notfound 恰 1
  let (f1, n1) = counts(&handle);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GETEX", b"missing"]).0,
    b"$-1\r\n"
  );
  assert_eq!(counts(&handle), (f1, n1 + 1), "GETEX 快臂缺席恰 1 notfound");

  // 快臂 WrongType：-WRONGTYPE 且静默 0/0（C# IsWrongType 臂无计数）
  let (f2, n2) = counts(&handle);
  let wt = roundtrip(&rt, &mut c, &[b"GETEX", b"obj"]).0;
  assert!(
    wt.starts_with(b"-WRONGTYPE"),
    "GETEX 快臂对象键应答 WRONGTYPE: {wt:?}"
  );
  assert_eq!(counts(&handle), (f2, n2), "GETEX 快臂对象键静默不计数");

  // 慢臂（直驱，降级重放唯一收口出口）：Hit 恰 1、Missing 恰 1、对象 0/0
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"s", b"t"]).0, b"+OK\r\n");
  let (f3, n3) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Getex, &[b"s", b"EX", b"100"]),
    bulk_frame(b"t")
  );
  assert_eq!(
    counts(&handle),
    (f3 + 1, n3),
    "GETEX 慢臂命中恰 1 found（双计已收口）"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Getex, &[b"missing2"]),
    b"$-1\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f3 + 1, n3 + 1),
    "GETEX 慢臂缺席恰 1 notfound"
  );
  let obj_slow = slow_direct(&rt, &api, RespCommand::Getex, &[b"obj"]);
  assert!(
    obj_slow.starts_with(b"-WRONGTYPE"),
    "GETEX 慢臂对象键 WRONGTYPE: {obj_slow:?}"
  );
  assert_eq!(
    counts(&handle),
    (f3 + 1, n3 + 1),
    "GETEX 慢臂对象键静默不计数"
  );
}

/// 案二双计取证主锁（票面净增二红灯 → 修复后净增一）：环形页翻转风暴下
/// SET 灌新键 + 对 COLD_LAG 轮前旧键的 GETEX EX 100 冷重放（旧键已被回绕
/// 驱逐成磁盘候选，TTL 写必遭降级慢臂重放）——GETEX 逐笔窗口恒计恰一帧、
/// notfound 恒零（快臂 Hit 入账后 TTL 写降级重放不再于慢臂漏斗重复折叠），
/// 且测试前提要求风暴内至少发生一次降级重放；降级 SET 慢臂重放的新键
/// notfound 自簿记属写命令另域口径，故以逐 GETEX 窗口切净而非全局差锁定
#[test]
fn getex_ttl_degrade_storm_counts_one_frame_per_command() {
  let (rt, mut c, _api, handle, _dir) = storm_env("getex-storm-degrade.db");

  let (f0, _n0) = counts(&handle);
  let mut degraded = 0usize;
  let mut getex_found = 0u64;
  let mut getex_notfound = 0u64;
  for i in 0..STORM_ROUNDS {
    let key = format!("gk{i}");
    let (resp, _) = roundtrip(&rt, &mut c, &[b"SET", key.as_bytes(), &storm_val(i)]);
    assert_eq!(resp, b"+OK\r\n", "风暴内 SET {key} 必回 +OK");
    // 冷代重放：i≥COLD_LAG 打向百轮前必已离页的旧键（确定性磁盘候选降级
    // 入口）；前 COLD_LAG 轮为建压热段，同键闭环读保持形锁
    let (target, want) = if i >= COLD_LAG {
      let j = i - COLD_LAG;
      (format!("gk{j}"), storm_val(j))
    } else {
      (key.clone(), storm_val(i))
    };
    // GETEX 逐笔窗口计量：本票锁域只收 GETEX 自身帧数（降级 SET 慢臂重放
    // 属写命令自簿记、另域口径，不入本锁分母，窗口切净互不污染）
    let (bf, bn) = counts(&handle);
    let (resp, was_degraded) =
      roundtrip(&rt, &mut c, &[b"GETEX", target.as_bytes(), b"EX", b"100"]);
    let (af, an) = counts(&handle);
    getex_found += af - bf;
    getex_notfound += an - bn;
    assert_eq!(
      (af - bf, an - bn),
      (1, 0),
      "风暴内 GETEX {target} 逐笔恒恰一帧 found、notfound 恒零（i={i}）"
    );
    degraded += was_degraded as usize;
    assert_eq!(
      resp,
      bulk_frame(&want),
      "GETEX {target} 命中应答整值（冷代降级笔经慢臂重放闭环，形不变）"
    );
  }
  assert!(
    degraded > 0,
    "测试前提：SET 灌压越 4 页环形容量后对 {COLD_LAG} 轮前旧键的 GETEX 冷重放\
     应至少触发一次降级重放（磁盘候选慢臂入口）"
  );
  assert_eq!(
    (getex_found, getex_notfound),
    (STORM_ROUNDS as u64, 0),
    "风暴全程每 GETEX 恰计一帧 found（修复前降级笔快慢臂双计 → found 虚增）"
  );
  let (f1, _) = counts(&handle);
  assert_eq!(
    f1 - f0,
    STORM_ROUNDS as u64,
    "全程 found 总差恰等于 GETEX 笔数（SET 臂不产 found）"
  );
}

/// 案二扩臂主锁（票 zcode-r147c-incrovf 案二）：string-RMW 族七席慢臂前置读
/// 走 `read_user_quiet` 零入账口——INCR/INCRBY/DECR/DECRBY/INCRBYFLOAT/SETRANGE/
/// APPEND 命中/缺席/对象键三形 total_found/total_notfound 恒零增量（修复前簿记
/// 漏斗尾 record_outcome 各多计一帧），应答形零改动
#[test]
fn string_rmw_slow_arm_probe_records_nothing() {
  let (rt, mut c, api, handle, _dir, _store) = env("string-rmw-account-slow.db");

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"k", b"10"]).0,
    b"+OK\r\n",
    "整数种子键"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"s", b"ab"]).0,
    b"+OK\r\n",
    "串值种子键"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"obj", b"f", b"v"]).0,
    b":1\r\n",
    "对象种子键"
  );
  let (f0, n0) = counts(&handle);

  // 命中臂：整值前置读（修复前 found+1），逐臂应答与既有慢臂锁面同形
  type CaseTuple = (RespCommand, &'static [&'static [u8]], &'static [u8]);
  let hits: &[CaseTuple] = &[
    (RespCommand::Incr, &[b"k"], b":11\r\n"),
    (RespCommand::Incrby, &[b"k", b"5"], b":16\r\n"),
    (RespCommand::Decr, &[b"k"], b":15\r\n"),
    (RespCommand::Decrby, &[b"k", b"2"], b":13\r\n"),
    (RespCommand::Incrbyfloat, &[b"k", b"0.5"], b"$4\r\n13.5\r\n"),
    (RespCommand::Setrange, &[b"s", b"1", b"XY"], b":3\r\n"),
    (RespCommand::Append, &[b"s", b"cd"], b":5\r\n"),
  ];
  for (cmd, args, expect) in hits {
    assert_eq!(
      slow_direct(&rt, &api, *cmd, args),
      *expect,
      "{cmd:?} {args:?} 慢臂前置读命中臂应答形零改动"
    );
    assert_eq!(
      counts(&handle),
      (f0, n0),
      "{cmd:?} 慢臂前置读零入账（RMW 前置读不入账纪律）"
    );
  }

  // 缺席臂：新建键（修复前 notfound+1）
  let misses: &[CaseTuple] = &[
    (RespCommand::Incr, &[b"m:cnt"], b":1\r\n"),
    (
      RespCommand::Incrbyfloat,
      &[b"m:flt", b"0.5"],
      b"$3\r\n0.5\r\n",
    ),
    (RespCommand::Setrange, &[b"m:srg", b"0", b"abc"], b":3\r\n"),
    (RespCommand::Append, &[b"m:app", b"xy"], b":2\r\n"),
  ];
  for (cmd, args, expect) in misses {
    assert_eq!(
      slow_direct(&rt, &api, *cmd, args),
      *expect,
      "{cmd:?} {args:?} 慢臂前置读缺席臂应答形零改动"
    );
    assert_eq!(counts(&handle), (f0, n0), "{cmd:?} 慢臂缺席前置读零入账");
  }

  // 对象键臂：WRONGTYPE 静默且零入账（C# RMW 判型臂不触计数器）
  let obj = slow_direct(&rt, &api, RespCommand::Incr, &[b"obj"]);
  assert!(
    obj.starts_with(b"-WRONGTYPE"),
    "INCR 慢臂对象键应答 WRONGTYPE: {obj:?}"
  );
  assert_eq!(counts(&handle), (f0, n0), "INCR 慢臂对象键零入账");

  // 对照组钉死口径分野：GET 慢臂命中/缺席仍各恰一条（其 C# 侧本有计）
  let (f1, n1) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Get, &[b"k"]),
    bulk_frame(b"13.5"),
    "GET 慢臂命中应答零改动"
  );
  assert_eq!(counts(&handle), (f1 + 1, n1), "GET 慢臂命中恰 1 found");
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Get, &[b"m:none"]),
    b"$-1\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f1 + 1, n1 + 1),
    "GET 慢臂缺席恰 1 notfound（读命令簿记口径不动）"
  );
}

/// 案二扩臂快臂侧锁（同票面实测快臂合规）：string-RMW 族七席快臂前置读经
/// `read_user_sync(…, None, …)` 恒零入账，命中/缺席两形计数恒零增量；
/// GET 快臂对照恰一条，钉住「读命令簿记 / RMW 前置读静默」分野
#[test]
fn string_rmw_fast_arm_probe_records_nothing() {
  let (rt, mut c, _api, handle, _dir, _store) = env("string-rmw-account-fast.db");

  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"k", b"10"]).0, b"+OK\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"s", b"ab"]).0, b"+OK\r\n");
  let (f0, n0) = counts(&handle);

  let cases: &[&[&[u8]]] = &[
    &[b"INCR", b"k"],
    &[b"INCRBY", b"k", b"5"],
    &[b"DECR", b"k"],
    &[b"DECRBY", b"k", b"2"],
    &[b"INCRBYFLOAT", b"k", b"0.5"],
    &[b"SETRANGE", b"s", b"1", b"XY"],
    &[b"APPEND", b"s", b"cd"],
    &[b"INCR", b"m:cnt"],
    &[b"APPEND", b"m:app", b"xy"],
  ];
  for args in cases {
    let (resp, _degraded) = roundtrip(&rt, &mut c, args);
    assert!(
      !resp.starts_with(b"-"),
      "{args:?} 快臂应答须为成功帧，实得 {resp:?}"
    );
    assert_eq!(counts(&handle), (f0, n0), "{args:?} 快臂前置读零入账");
  }

  let (f1, n1) = counts(&handle);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GET", b"k"]).0,
    bulk_frame(b"13.5")
  );
  assert_eq!(counts(&handle), (f1 + 1, n1), "GET 快臂命中恰 1 found");
}
