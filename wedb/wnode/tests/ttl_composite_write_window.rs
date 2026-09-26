//! 带 TTL 复合写族读改写窗口收口回归测试（票 zcode-r15-expire 发现一）
//!
//! 缺陷背景：rust 把 TTL 拆为独立旁路记录（KeyTag::Ttl），SETEX/SET EX/
//! SET KEEPTTL/GETEX/GETDEL 的「值写 + TTL 旁路写」收口前为无窗口两步复合
//! 写——`ttl_of_sync` 读旧、`try_upsert_sync` 清 TTL、`put_ttl_sync` 回填/
//! 续期跨调用交叠，与持窗 EXPIRE 条件族交错即丢更新且应答非可串行化
//! （KEEPTTL 回填旧值可吞掉并发 EXPIRE NX 已 :1 的续期；GETDEL 可删掉并发
//! SET 刚 ACK 的新值而答出旧值）。修复后各臂在值/TTL 操作前经
//! `BatchStoreSession::try_rmw_window`（快）/ `rmw_window`（慢）整段同窗，
//! 失闩沿既有 `Ok(false)` 降级慢路径同段持窗重放，不新增第二把锁。
//!
//! C# 对位（全族单记录 RMW ephemeral 锁内一体完成，rust 进窗即 1:1 对位）：
//! - libs/server/Resp/BasicCommands.cs:NetworkSETEX（552-559 单 SET 内嵌过期）
//! - libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional（EX/KEEPTTL
//!   RMW 单记录条件写）
//! - libs/server/Storage/Functions/MainStore/RMWMethods.cs:771-799（GETEX
//!   锁内读值 + TrySetExpiration/RemoveExpiration）
//! - libs/server/Storage/Functions/MainStore/RMWMethods.cs:763-768（GETDEL
//!   锁内读值后 ExpireAndStop 原子删）
//!
//! 断言口径与 expire_persist_latch_concurrency 同款「只统计已回执」：
//! 命令层降级转异步闭环，各用例断言串行化不变式——
//! - SET KEEPTTL 与 EXPIRE NX 持窗串行下，NX 判定恒见在场 TTL（预置 100s，
//!   KEEPTTL 原样保留）→ 恒回 :0、终态 TTL 恒 100s 量级（C# EvaluateExpire
//!   的 NX 臂 hasExpiration 即不改，SessionFunctionsUtils.cs:36-42）；「NX :1
//!   而终态 100s」（KEEPTTL 清 TTL 间隙读空放行）即收口前败 face，恒不可达；
//! - SETEX 与 EXPIRE GT 同键 RMW 闩内全序化（两命令共用同键 ephemeral 闩——
//!   InternalRMW.cs:70 与 InternalUpsert.cs:67——GT 判定读的即串行后记录
//!   版本，SessionFunctionsUtils.cs:48-55），任一全序可串行化：GT :1 ⇒
//!   SETEX 后写者胜，GT :0 ⇒ SETEX 先行落地；两全序终态 TTL 恒为 SETEX
//!   写入值 30000s 量级，「GT :1 而终态 100s」（判旧版本写迟覆盖 SETEX 新
//!   TTL）即收口前败 face，恒不可达；「EXPIRE 先行」分支由尾部顺序子步
//!   确定性覆盖（随机交叉的 :1 出现次数是调度赌注，非语义判据）；
//! - GETDEL 应答值 = 被删值（禁「答旧值删新值」）；
//! - 他会话持窗期间复合写臂不闭环，放窗后降级慢路径同段持窗重放闭环。
//!
//! 另含单线程回归面：收口后各命令应答与 TTL 终态逐字节不变。

use std::{
  str::from_utf8,
  sync::{Arc, Barrier},
  thread,
};

use compio::runtime::Runtime;
use tempfile::TempDir;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::roundtrip;
use wtest_base::{resp_frame as frame, test_store_config};

/// 交叉轮数（同键争用足以在收口前稳定复现两步交叠）
const ROUNDS: usize = 50;

fn open_store(tag: &str) -> (Arc<WedbStore<SegmentedDevice>>, TempDir) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  (store, dir)
}

fn consumer_on(store: &Arc<WedbStore<SegmentedDevice>>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// `:N\r\n` 整数回执解析（非整数回执回 None，供「已回执」判定）
fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

/// `$N\r\n…\r\n` bulk string 回执解析（nil 回 None）
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

/// 读键剩余 TTL 秒数（单线程断言面；-1 无 TTL、-2 键缺失）
fn ttl_of(rt: &Runtime, store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<i64> {
  let mut c = consumer_on(store);
  reply_int(&roundtrip(rt, &mut c, &[b"TTL", key]))
}

/// 读键值（单线程断言面；键缺失回 None）
fn get_of(rt: &Runtime, store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let mut c = consumer_on(store);
  reply_bulk(&roundtrip(rt, &mut c, &[b"GET", key]))
}

/// 他会话持本键窗口闩期间，SETEX 复合写臂不得先行输出应答；放闩后降级慢
/// 路径同段持窗重放闭环且终态 TTL 正确（结构判据：直接焊死窗口在路径上，
/// 收口前 SETEX 的「清 TTL + upsert + put_ttl」在持窗者眼皮底下原位交错
/// 完成即盲写面）
#[test]
fn foreign_window_defers_setex_until_replay() {
  let (store, _dir) = open_store("ttl-composite-setex.db");
  let rt = Runtime::new().unwrap();
  let key = b"cw:setex";
  let mut c = consumer_on(&store);

  // 外部会话先持本键窗口闩（同线程持闩：同步臂自旋预算内必失闩降级）
  let sess_w = store.new_session().unwrap();
  let batch_w = sess_w.enter_batch();
  let window = batch_w.try_rmw_window(key).expect("窗口自取本键桶排他闩");

  // 持窗期间 SETEX：同步臂失闩 Ok(false) 降级、慢路径 rmw_window 挂起等闩，
  // 不得先行输出应答（挂起的 SlowWait 先收走，不 resolve 即不闭环）
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(&[b"SETEX", key, b"5000", b"v"]));
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = c.try_consume_messages_into(&mut resp);
  let slow = c.take_slow_wait();
  assert!(
    resp.is_empty(),
    "持窗期间同步臂不得先行输出应答（收口前即两步交叠窗口）：{resp:?}"
  );

  // 放闩后慢路径重放闭环：应答 +OK 且终态 TTL 为本命令写入值
  drop(window);
  if let Some(slow) = slow {
    rt.block_on(async {
      resp.extend_from_slice(&slow.resolve().await);
    });
  }
  assert_eq!(resp, b"+OK\r\n", "放闩后降级慢路径须闭环 +OK");
  let ttl = ttl_of(&rt, &store, key).expect("SETEX 闭环后键必存活");
  assert!(
    (4500..=5000).contains(&ttl),
    "SETEX 闭环后 TTL 必为本命令写入值 5000s 量级（实际 {ttl}）"
  );
}

/// 会话 A `SET k v KEEPTTL`、会话 B `EXPIRE k NX` 同键真并发交叉，逐轮校验
/// 可串行化不变式；轮间串行预置（值 + 旧 TTL 100s）钉定可判定起点
#[test]
fn keepttl_vs_expire_nx_serializes() {
  let (store, _dir) = open_store("ttl-composite-keepttl.db");
  let key = b"cw:kttl".to_vec();
  let mut expire_acked = 0usize;

  for round in 0..ROUNDS {
    {
      let rt = Runtime::new().unwrap();
      let mut c = consumer_on(&store);
      // 预置值 + 旧 TTL（KEEPTTL 回填前提；EXPIRE NX 有在场旧值可判）
      roundtrip(&rt, &mut c, &[b"SET", &key, b"v0"]);
      assert_eq!(
        reply_int(&roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"100"])),
        Some(1),
        "第 {round} 轮预置旧 TTL 须 :1"
      );
    }
    // 两线程各自独立连接 + 独立 Runtime，真并发交叉
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [false, true]
      .into_iter()
      .map(|is_expire| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let key = key.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          let resp = if is_expire {
            roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"30000", b"NX"])
          } else {
            roundtrip(&rt, &mut c, &[b"SET", &key, b"v1", b"KEEPTTL"])
          };
          gate.wait();
          (is_expire, resp)
        })
      })
      .collect();
    let results: Vec<(bool, Vec<u8>)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let expire_acked_round = results
      .iter()
      .any(|(is_expire, resp)| *is_expire && reply_int(resp) == Some(1));
    expire_acked += expire_acked_round as usize;
    assert!(
      results
        .iter()
        .any(|(is_expire, resp)| !is_expire && resp == b"+OK\r\n"),
      "第 {round} 轮：KEEPTTL 须闭环 +OK"
    );

    let rt = Runtime::new().unwrap();
    let ttl = ttl_of(&rt, &store, &key);
    // 核心不变式：EXPIRE NX 回执 :1 ⇒ 终态 TTL 必为其写入值（30000s 量级）。
    // 收口后两步读改写一体同窗，EXPIRE NX 绝无间隙偷跑放行（expire_acked 恒为 0）；
    // 若有偶发放行，终态 TTL 亦受保护
    if expire_acked_round {
      let ttl = ttl.expect("EXPIRE NX 回执 :1 后键必存活");
      assert!(
        (25_000..=31_000).contains(&ttl),
        "第 {round} 轮：EXPIRE NX 回执 :1 而终态 TTL 是 {ttl}s（应为 30000s 量级，\
         续期被 KEEPTTL 旧值回填吞掉）"
      );
    }
    // 值不被 KEEPTTL 写丢
    assert_eq!(
      get_of(&rt, &store, &key).as_deref(),
      Some(b"v1".as_slice()),
      "第 {round} 轮：KEEPTTL 值写必须可见"
    );
  }
  assert_eq!(
    expire_acked, 0,
    "串行化下 EXPIRE NX 绝无间隙偷跑放行（实测 {expire_acked} 轮偷跑）"
  );
}

/// 会话 A `SETEX k 30000 v`、会话 B `EXPIRE k 100 GT` 同键真并发交叉（预置
/// 旧 TTL 50s 使 GT 判定可放行）：两臂同键 RMW 闩内全序化（C# 同键
/// ephemeral 闩 InternalRMW.cs:70 / InternalUpsert.cs:67），任一全序皆可
/// 串行化——EXPIRE GT 回执 :1 ⇒ EXPIRE 先行落地，SETEX 后写者胜覆盖 TTL；
/// B 回执 :0 ⇒ SETEX 先行落地，GT 判定不满足拒写。两全序终态 TTL 均为
/// SETEX 写入值 30000s 量级，逐轮统一校验。
///
/// 「EXPIRE GT 先行 ⇒ :1 ⇒ SETEX 后写者胜」分支由尾部顺序子步确定性覆盖：
/// 持窗收口后落地序 = 到达序，随机交叉在高载门禁下不能保证 EXPIRE 至少
/// 先行一次（旧判据 `expire_acked > 0` 是调度赌注，非语义判据）；C# 对位
/// （BasicCommands.cs:552-559 NetworkSETEX 单 SET 内嵌过期 + EvaluateExpire
/// GT 臂 SessionFunctionsUtils.cs:48-55）本身即闩内串行的确定性序，顺序
/// 子步 1:1 对位 EXPIRE→SETEX 全序，不损失「:1 而终态非 SETEX 值」败 face
/// 的检测力。
#[test]
fn setex_vs_expire_gt_serializes() {
  let (store, _dir) = open_store("ttl-composite-setex-gt.db");
  let key = b"cw:sxg".to_vec();

  for round in 0..ROUNDS {
    {
      let rt = Runtime::new().unwrap();
      let mut c = consumer_on(&store);
      // 预置：键在场 + 旧 TTL 50s（B 的 100 > 50 可放行）
      roundtrip(&rt, &mut c, &[b"SET", &key, b"v0"]);
      roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"50"]);
    }
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [false, true]
      .into_iter()
      .map(|is_expire| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let key = key.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          let resp = if is_expire {
            roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"100", b"GT"])
          } else {
            roundtrip(&rt, &mut c, &[b"SETEX", &key, b"30000", b"v1"])
          };
          gate.wait();
          (is_expire, resp)
        })
      })
      .collect();
    let results: Vec<(bool, Vec<u8>)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(
      results
        .iter()
        .any(|(is_expire, resp)| !is_expire && resp == b"+OK\r\n"),
      "第 {round} 轮：SETEX 须闭环 +OK"
    );

    let rt = Runtime::new().unwrap();
    // 终态不变式（两全序同判）：TTL 30000s 量级只能由 SETEX 的 put_ttl
    // 写出——:1 轮钉「后写者胜」，:0 轮钉「SETEX 先行不被 GT 拒写破坏」
    let ttl = ttl_of(&rt, &store, &key).expect("交叉后键必存活（两全序下 SETEX 均在场）");
    assert!(
      (25_000..=31_000).contains(&ttl),
      "第 {round} 轮：终态 TTL 是 {ttl}s（两全序下均应为后写者 SETEX 30000s 量级，\
       「GT :1 而终态 100s」即判旧版本写迟覆盖败 face）"
    );
  }

  // 顺序子步：EXPIRE GT 先行落地（:1）→ SETEX 后写者胜覆盖 30000
  {
    let rt = Runtime::new().unwrap();
    let mut c = consumer_on(&store);
    roundtrip(&rt, &mut c, &[b"SET", &key, b"v0"]);
    roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"50"]);
    assert_eq!(
      reply_int(&roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"100", b"GT"])),
      Some(1),
      "顺序子步：EXPIRE GT 判旧值 50s 必放行 :1"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SETEX", &key, b"30000", b"v1"]),
      b"+OK\r\n",
      "顺序子步：SETEX 须闭环 +OK"
    );
    let ttl = ttl_of(&rt, &store, &key).expect("EXPIRE GT :1 后 SETEX 后写，键必存活");
    assert!(
      (25_000..=31_000).contains(&ttl),
      "EXPIRE GT 回执 :1 而 SETEX 后写者未胜：终态 TTL {ttl}s（应 30000s 量级）"
    );
  }
}

/// 会话 A `GETDEL k`、会话 B `SET k v_new` 同键真并发交叉：GETDEL 应答值 =
/// 被删值。收口前的败 face：A 读出 old 后、删除前 B 的新值落库，A 的删除
/// 把 new 一并删掉而应答 old——「答旧值删新值」在任何串行序不可达
#[test]
fn getdel_vs_set_answers_deleted_value() {
  let (store, _dir) = open_store("ttl-composite-getdel.db");
  let key = b"cwd:gk".to_vec();
  let mut getdel_acked = 0usize;

  for round in 0..ROUNDS {
    {
      let rt = Runtime::new().unwrap();
      let mut c = consumer_on(&store);
      roundtrip(&rt, &mut c, &[b"SET", &key, b"old"]);
    }
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [false, true]
      .into_iter()
      .map(|is_getdel| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let key = key.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          let resp = if is_getdel {
            roundtrip(&rt, &mut c, &[b"GETDEL", &key])
          } else {
            roundtrip(&rt, &mut c, &[b"SET", &key, b"new"])
          };
          gate.wait();
          (is_getdel, resp)
        })
      })
      .collect();
    let results: Vec<(bool, Vec<u8>)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let answered = results
      .iter()
      .filter(|(is_getdel, _)| *is_getdel)
      .find_map(|(_, resp)| reply_bulk(resp));
    let getdel_acked_round = answered.is_some();
    getdel_acked += getdel_acked_round as usize;
    assert!(
      results
        .iter()
        .any(|(is_getdel, resp)| !is_getdel && resp == b"+OK\r\n"),
      "第 {round} 轮：SET 须闭环 +OK"
    );

    let rt = Runtime::new().unwrap();
    let left = get_of(&rt, &store, &key);
    if let Some(got) = answered {
      // 可串行化分派：GETDEL 整体先 ⇒ 答 old、终态 new；SET 整体先 ⇒
      // 答 new、终态缺失
      if got == b"old" {
        assert_eq!(
          left.as_deref(),
          Some(b"new".as_slice()),
          "第 {round} 轮：GETDEL 答 old 而终态是 {left:?}（应答值与被删值脱节——\
           新值被删而旧值被答出）"
        );
      } else {
        assert_eq!(
          got, b"new",
          "第 {round} 轮：GETDEL 应答既非 old 也非 new：{got:?}"
        );
        assert_eq!(left, None, "第 {round} 轮：GETDEL 答 new 后键必须消失");
      }
    } else {
      panic!("第 {round} 轮：GETDEL 未闭环 bulk 应答");
    }
  }
  assert!(
    getdel_acked > 0,
    "交叉覆盖不足：GETDEL 闭环共 {getdel_acked} 轮"
  );
}

/// 单线程回归：收口后各命令应答与 TTL 终态逐字节不变（对标 C# 单记录语义）
#[test]
fn single_thread_replies_unchanged() {
  let (store, _dir) = open_store("ttl-composite-replay.db");
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  let k = b"cw:reg";

  // SETEX：+OK + TTL ∈ (0,100]
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SETEX", k, b"100", b"a"]),
    b"+OK\r\n"
  );
  let ttl = ttl_of(&rt, &store, k).expect("SETEX 后键必存活");
  assert!((0..=100).contains(&ttl), "SETEX TTL 应在 (0,100]: {ttl}");

  // SET … EX：+OK + 覆写值 + TTL 折新
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", k, b"b", b"EX", b"200"]),
    b"+OK\r\n"
  );
  assert_eq!(get_of(&rt, &store, k).as_deref(), Some(b"b".as_slice()));
  let ttl = ttl_of(&rt, &store, k).expect("SET EX 后键必存活");
  assert!((150..=200).contains(&ttl), "SET EX TTL 应 ≈200: {ttl}");
  let prev_ttl = ttl;

  // SET … KEEPTTL：+OK + TTL 保留（秒级流逝容差内）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", k, b"c", b"KEEPTTL"]),
    b"+OK\r\n"
  );
  let kept = ttl_of(&rt, &store, k).expect("KEEPTTL 后键必存活");
  assert!(
    kept > 0 && (prev_ttl - kept).abs() <= 2,
    "KEEPTTL 必须保留既有 TTL（前值 {prev_ttl} 现值 {kept}）"
  );

  // SET（裸写）：+OK + TTL 清除
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", k, b"d"]), b"+OK\r\n");
  assert_eq!(ttl_of(&rt, &store, k), Some(-1), "裸 SET 必须清除 TTL");

  // GETEX EX：答值 + TTL 折新
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GETEX", k, b"EX", b"300"]),
    b"$1\r\nd\r\n"
  );
  let ttl = ttl_of(&rt, &store, k).expect("GETEX 后键必存活");
  assert!((250..=300).contains(&ttl), "GETEX EX TTL 应 ≈300: {ttl}");

  // GETEX PERSIST：答值 + TTL 清除
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GETEX", k, b"PERSIST"]),
    b"$1\r\nd\r\n"
  );
  assert_eq!(
    ttl_of(&rt, &store, k),
    Some(-1),
    "GETEX PERSIST 必须清除 TTL"
  );

  // GETDEL：答值 + 键消失
  assert_eq!(roundtrip(&rt, &mut c, &[b"GETDEL", k]), b"$1\r\nd\r\n");
  assert_eq!(ttl_of(&rt, &store, k), Some(-2), "GETDEL 后键必须消失");

  // SET … GET（GETSET 形态）：答旧值；NX 拒写时答现值
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", k, b"e"]), b"+OK\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", k, b"f", b"GET"]),
    b"$1\r\ne\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", k, b"g", b"NX", b"GET"]),
    b"$1\r\nf\r\n",
    "NX 拒写时 GET 形态答现值"
  );
}
