//! 全仓写臂读改写窗口覆盖矩阵回归测试（票 zcode-r32-rmwmatrix）
//!
//! 缺陷背景：盲写原语臂（SET 裸形态 / DEL / MSET / BITOP dest / SETNX /
//! ETag 条件写族）只持纪元门零键闩，可插入任意持窗臂的读改写间隙——持窗
//! 写回内核纯盲写无地址复验，间隙内盲写绝不被察觉：GETDEL 读 v1 后并发
//! SET v2 落间隙即「SET 已回 +OK 而 v2 被删」；DEL 盲删落入 INCR 持窗间隙
//! 即「DEL :1 而键复活」；SETNX 探测与写入两步分离即并发 SET 已确认写丢失；
//! SETIFMATCH 条件判定与写回两步分离即比较并交换语义双成功。收口后各写臂
//! 快路径经 `BatchStoreSession::try_rmw_window` 整段同窗，失闩沿既有
//! `Ok(false)` 降级慢路径同段持窗重放（对标 C# InternalUpsert.cs:67 /
//! InternalRMW.cs:70 / InternalDelete.cs:60 三写原语同取
//! FindOrCreateTagAndTryEphemeralXLock，upsert/RMW/delete 全写路径同闩域
//! 互斥），不新增第二把锁。
//!
//! 断言口径与 ttl_composite_write_window / key_admin_latch_concurrency 同款：
//! - 结构判据（外部会话持本键窗口闩）：各盲写臂同步段一律失闩降级——零应答
//!   输出 + SlowWait 挂起；放闩后慢路径重放闭环，终态与直连串行执行逐字节
//!   一致。收口前盲写臂在持窗者眼皮底下原位落笔即败 face，恒不可达；
//! - 真并发判据（双连接 Barrier 交叉，只统计已回执）：
//!   SETNX 与 SET 同键交叉，绝无「SET +OK 且 SETNX :1 且终值为 SETNX 值」
//!   组合；双连接并发 SETIFMATCH 同 etag 恰一者命中（命中应答第二元素恒
//!   nil，双写双 nil 即两步判定败 face）。
//!
//! C# 对位：libs/server/Resp/BasicCommands.cs:NetworkSET/NetworkSETNX、
//! libs/server/Resp/ArrayCommands.cs:NetworkDEL/NetworkMSET、
//! libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitOperation
//! （dest SET 记录闩内落笔）、libs/server/Resp/BasicEtagCommands.cs
//! （DEL_ETagConditional 走 DEL_Conditional 的 RMW 条件内置）。

use std::{
  str::from_utf8,
  sync::{Arc, Barrier},
  thread,
};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::roundtrip;
use wtest_base::{open_test_store, resp_frame as frame};

/// 交叉轮数（同键争用足以在收口前稳定复现两步交叠）
const ROUNDS: usize = 50;

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

/// `[etag, value]` 二元数组应答的 etag 字段（数组首元素整数）解析
fn reply_etag_field(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b"*2\r\n")?;
  let end = body.iter().position(|&b| b == b'\r')?;
  reply_int(&body[..end + 2])
}

/// 读键值（单线程断言面；键缺失回 None）
fn get_of(rt: &Runtime, store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let mut c = consumer_on(store);
  reply_bulk(&roundtrip(rt, &mut c, &[b"GET", key]))
}

/// 结构判据单命令段：外部会话持 `key` 窗口闩期间发 `args`，盲写臂同步段须
/// 失闩降级（零应答输出 + SlowWait 挂起，绝不先行落笔）；放闩后慢路径重放
/// 闭环 `expected`，`post` 校验重放后键态
fn assert_defers_until_replay(
  rt: &Runtime,
  store: &Arc<WedbStore<SegmentedDevice>>,
  key: &[u8],
  args: &[&[u8]],
  expected: &[u8],
  post: impl FnOnce(&Runtime, &Arc<WedbStore<SegmentedDevice>>),
) {
  let mut c = consumer_on(store);
  let sess_w = store.new_session().unwrap();
  let batch_w = sess_w.enter_batch();
  let window = batch_w.try_rmw_window(key).expect("窗口自取本键桶排他闩");

  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = c.try_consume_messages_into(&mut resp);
  let slow = c.take_slow_wait();
  assert!(
    resp.is_empty(),
    "持窗期间盲写臂不得先行落笔应答（收口前即间隙盲写窗口）：{args:?}"
  );

  drop(window);
  if let Some(slow) = slow {
    rt.block_on(async {
      resp.extend_from_slice(&slow.resolve().await);
    });
  }
  assert_eq!(resp, expected, "放闩后降级慢路径重放应答不符：{args:?}");
  post(rt, store);
}

/// 外部会话持窗期间，盲写族各臂（SET 裸形态 / DEL / MSET / SETNX /
/// BITOP dest / SETIFMATCH / SETWITHETAG / DELIFGREATER）同步段一律失闩
/// 降级；放闩后慢路径重放闭环，终态与直连串行执行逐字节一致
#[test]
fn blindwrite_arms_defer_under_foreign_window() {
  let (_dir, store) = open_test_store("rmw-matrix-defer.db").unwrap();
  let rt = Runtime::new().unwrap();
  let k = b"bw:defer";

  // SET 裸形态：重放 +OK 且值落库
  assert_defers_until_replay(
    &rt,
    &store,
    k,
    &[b"SET", k, b"v1"],
    b"+OK\r\n",
    |rt, store| {
      assert_eq!(get_of(rt, store, k).as_deref(), Some(b"v1".as_slice()));
    },
  );

  // MSET：重放 +OK 且值落库
  assert_defers_until_replay(
    &rt,
    &store,
    k,
    &[b"MSET", k, b"v2"],
    b"+OK\r\n",
    |rt, store| {
      assert_eq!(get_of(rt, store, k).as_deref(), Some(b"v2".as_slice()));
    },
  );

  // SETNX：键在场重放 :0 零覆写
  assert_defers_until_replay(
    &rt,
    &store,
    k,
    &[b"SETNX", k, b"v3"],
    b":0\r\n",
    |rt, store| {
      assert_eq!(get_of(rt, store, k).as_deref(), Some(b"v2".as_slice()));
    },
  );

  // BITOP dest：重放 :2（OR 自身 v2 两字节）且值不变
  assert_defers_until_replay(
    &rt,
    &store,
    k,
    &[b"BITOP", b"OR", k, k],
    b":2\r\n",
    |rt, store| {
      assert_eq!(get_of(rt, store, k).as_deref(), Some(b"v2".as_slice()));
    },
  );

  // DEL：重放 :1 且键消失
  assert_defers_until_replay(&rt, &store, k, &[b"DEL", k], b":1\r\n", |rt, store| {
    assert_eq!(get_of(rt, store, k), None);
  });

  // 键缺失域：SETIFMATCH 初写（etag = given + 1 = 3）→ [3, nil]
  assert_defers_until_replay(
    &rt,
    &store,
    k,
    &[b"SETIFMATCH", k, b"v4", b"2"],
    b"*2\r\n:3\r\n$-1\r\n",
    |rt, store| {
      assert_eq!(get_of(rt, store, k).as_deref(), Some(b"v4".as_slice()));
    },
  );

  // SETWITHETAG：existing=3 → new=4
  assert_defers_until_replay(
    &rt,
    &store,
    k,
    &[b"SETWITHETAG", k, b"v5"],
    b":4\r\n",
    |rt, store| {
      assert_eq!(get_of(rt, store, k).as_deref(), Some(b"v5".as_slice()));
    },
  );

  // DELIFGREATER：given 9 > existing 4 → :1 且键消失
  assert_defers_until_replay(
    &rt,
    &store,
    k,
    &[b"DELIFGREATER", k, b"9"],
    b":1\r\n",
    |rt, store| {
      assert_eq!(get_of(rt, store, k), None);
    },
  );
}

/// 会话 A `SETNX k a`、会话 B `SET k b` 同键真并发交叉，逐轮校验可串行化
/// 不变式（票 zcode-r32-rmwmatrix 立项三）。收口前的败 face：SETNX 判缺席
/// 后、写入前 SET 落库并回 +OK，SETNX 继续覆写为 a 并回 :1——「SET +OK 且
/// SETNX :1 且终值为 SETNX 值」在任何串行序不可达
#[test]
fn setnx_vs_set_serializes() {
  let (_dir, store) = open_test_store("rmw-matrix-setnx.db").unwrap();
  let key = b"bw:nx".to_vec();
  let mut setnx_acked = 0usize;

  for round in 0..ROUNDS {
    {
      let rt = Runtime::new().unwrap();
      let mut c = consumer_on(&store);
      // 轮间串行预置：键回缺席起点（先 SET 保 DEL :1 可断言，上一轮终态
      // 必有键，本轮删除后起点恒为空）
      assert_eq!(
        reply_int(&roundtrip(&rt, &mut c, &[b"SET", &key, b"seed"])),
        None
      );
      assert_eq!(reply_int(&roundtrip(&rt, &mut c, &[b"DEL", &key])), Some(1));
    }
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [false, true]
      .into_iter()
      .map(|is_setnx| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let key = key.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          gate.wait();
          let resp = if is_setnx {
            roundtrip(&rt, &mut c, &[b"SETNX", &key, b"a"])
          } else {
            roundtrip(&rt, &mut c, &[b"SET", &key, b"b"])
          };
          (is_setnx, resp)
        })
      })
      .collect();
    let results: Vec<(bool, Vec<u8>)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let setnx_acked_round = results
      .iter()
      .any(|(is_setnx, resp)| *is_setnx && reply_int(resp) == Some(1));
    let set_acked_round = results
      .iter()
      .any(|(is_setnx, resp)| !is_setnx && resp == b"+OK\r\n");
    if setnx_acked_round {
      setnx_acked += 1;
    }

    let rt = Runtime::new().unwrap();
    let final_val = get_of(&rt, &store, &key);
    if set_acked_round && setnx_acked_round {
      // 双成功仅在「SETNX 先写 a、SET 后写 b」串行序可达：终值必须是 SET 的
      // b（SETNX :1 而终值为 SETNX 值 a 即两步间隙丢已确认写）
      assert_eq!(
        final_val.as_deref(),
        Some(b"b".as_slice()),
        "第 {round} 轮：SET 与 SETNX 双成功而终值不是后写者 SET 的值（探测与写入\
         间隙交错，非可串行化）"
      );
    } else {
      // 单独 SETNX 成功（SET +OK 必然成立，此臂防御降级通道静默丢写）
      assert_eq!(
        final_val.as_deref(),
        if setnx_acked_round {
          Some(b"a".as_slice())
        } else {
          Some(b"b".as_slice())
        },
        "第 {round} 轮：终值与已回执写入脱节"
      );
    }
  }
  assert!(
    setnx_acked > 0,
    "交叉覆盖不足：SETNX :1 共 {setnx_acked} 轮"
  );
}

/// 双连接并发 `SETIFMATCH k v E` 同 etag 交叉（预置 etag=E=1 键在场），逐轮
/// 校验比较并交换语义（票 zcode-r32-rmwmatrix 立项二）。命中应答 `[E+1,nil]`、
/// 不命中 `[E, 旧值]`——收口前的败 face：两连接各自读得 existing=E 判命中、
/// 双写双回 `[E+1, nil]`（第二元素双 nil）；收口后条件判定与写回共一个
/// 线性化点，恒恰一者命中
#[test]
fn setifmatch_concurrent_exactly_one_hits() {
  let (_dir, store) = open_test_store("rmw-matrix-ifmatch.db").unwrap();
  let key = b"bw:ifm".to_vec();

  for round in 0..ROUNDS {
    {
      let rt = Runtime::new().unwrap();
      let mut c = consumer_on(&store);
      // 预置：键在场 etag=E=1（NoETag 0 + 1；DEL 级联清退上轮残留 etag，
      // 轮 0 键未建回 :0 同样可达）
      assert!(
        matches!(
          reply_int(&roundtrip(&rt, &mut c, &[b"DEL", &key])),
          Some(0 | 1)
        ),
        "第 {round} 轮预置删除须闭环整数回执"
      );
      assert_eq!(
        reply_int(&roundtrip(&rt, &mut c, &[b"SETWITHETAG", &key, b"v0"])),
        Some(1),
        "第 {round} 轮预置 etag 须 1"
      );
    }
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2)
      .map(|i| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let key = key.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          gate.wait();
          let val: &[u8] = if i == 0 { b"v1" } else { b"v2" };
          roundtrip(&rt, &mut c, &[b"SETIFMATCH", &key, val, b"1"])
        })
      })
      .collect();
    let replies: Vec<Vec<u8>> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // 命中 = 应答第二元素为 nil（`[E+1, nil]`）；命中者恰一
    let hits = replies
      .iter()
      .filter(|resp| resp.ends_with(b"$-1\r\n"))
      .count();
    assert_eq!(
      hits, 1,
      "第 {round} 轮：并发 SETIFMATCH 同 etag 恰一者命中（实际 {hits}，\
       双命中即条件判定与写回两步分离败 face）"
    );

    // 终态：etag 抬至 E+1=2、值为命中者之一
    let rt = Runtime::new().unwrap();
    let mut c = consumer_on(&store);
    let resp = roundtrip(&rt, &mut c, &[b"GETWITHETAG", &key]);
    assert_eq!(
      reply_etag_field(&resp),
      Some(2),
      "第 {round} 轮：终态 etag 须恰为 2（实际 {resp:?}）"
    );
  }
}

/// 外部会话持窗期间 HCOLLECT 同步臂失闩降级（票 zcode-r32-rmwmatrix 立项四：
/// 装载求值写回全程持窗，装载期旧视图尾段盲写顶掉并发 HSET 新字段的败 face
/// 恒不可达）；放闩后慢路径重放闭环
#[test]
fn hcollect_defers_under_foreign_window() {
  let (_dir, store) = open_test_store("rmw-matrix-hcollect.db").unwrap();
  let rt = Runtime::new().unwrap();
  let k = b"bw:hcol";
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"HSET", k, b"f1", b"a", b"f2", b"b"]),
      b":2\r\n"
    );
  }

  let mut c = consumer_on(&store);
  let sess_w = store.new_session().unwrap();
  let batch_w = sess_w.enter_batch();
  let window = batch_w.try_rmw_window(k).expect("窗口自取本键桶排他闩");

  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(&[b"HCOLLECT", k]));
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = c.try_consume_messages_into(&mut resp);
  let slow = c.take_slow_wait();
  assert!(
    resp.is_empty(),
    "持窗期间 HCOLLECT 不得先行装载落笔：{resp:?}"
  );

  drop(window);
  if let Some(slow) = slow {
    rt.block_on(async {
      resp.extend_from_slice(&slow.resolve().await);
    });
  }
  assert_eq!(resp, b"+OK\r\n", "放闩后降级慢路径须闭环 +OK");
  // 字段未被清除（无过期字段）：HLEN 计数不变
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HLEN", k]),
    b":2\r\n",
    "HCOLLECT 重放不得丢失存活字段"
  );
}

/// 单线程回归：盲写族各臂收口后应答与键态逐字节不变（对标 C# 单记录语义）
#[test]
fn single_thread_replies_unchanged() {
  let (_dir, store) = open_test_store("rmw-matrix-replay.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  let k = b"bw:reg";

  // SET 裸形态 / MSET / DEL
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", k, b"a"]), b"+OK\r\n");
  assert_eq!(get_of(&rt, &store, k).as_deref(), Some(b"a".as_slice()));
  assert_eq!(roundtrip(&rt, &mut c, &[b"MSET", k, b"b"]), b"+OK\r\n");
  assert_eq!(get_of(&rt, &store, k).as_deref(), Some(b"b".as_slice()));
  assert_eq!(roundtrip(&rt, &mut c, &[b"SETNX", k, b"c"]), b":0\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"DEL", k]), b":1\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"DEL", k]), b":0\r\n");

  // SETNX 缺席初写
  assert_eq!(roundtrip(&rt, &mut c, &[b"SETNX", k, b"d"]), b":1\r\n");
  assert_eq!(get_of(&rt, &store, k).as_deref(), Some(b"d".as_slice()));

  // BITOP AND（dest=src 自身）：长度应答 + 值不变
  assert_eq!(roundtrip(&rt, &mut c, &[b"BITOP", b"AND", k, k]), b":1\r\n");
  assert_eq!(get_of(&rt, &store, k).as_deref(), Some(b"d".as_slice()));

  // ETag 族：初写（NoETag 0 → 1）→ 读 → 条件命中 → 条件删除
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SETWITHETAG", k, b"e"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GETWITHETAG", k]),
    b"*2\r\n:1\r\n$1\r\ne\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SETIFMATCH", k, b"f", b"1"]),
    b"*2\r\n:2\r\n$-1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SETIFMATCH", k, b"g", b"1"]),
    b"*2\r\n:2\r\n$1\r\nf\r\n",
    "条件不命中回 [existing, 旧值]"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"DELIFGREATER", k, b"1"]),
    b":0\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"DELIFGREATER", k, b"3"]),
    b":1\r\n",
    "given 3 > existing 2 才真实删除"
  );
  assert_eq!(get_of(&rt, &store, k), None, "DELIFGREATER 命中后键须消失");

  // HCOLLECT 无过期字段：字段保全
  let h = b"bw:hreg";
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", h, b"f1", b"a"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"HCOLLECT", h]), b"+OK\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", h]), b":1\r\n");
}
