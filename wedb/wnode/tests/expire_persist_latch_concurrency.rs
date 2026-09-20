//! 同步 EXPIRE/PERSIST 快路径键闩并发回归测试（票 r6-del-sync-expire-persist-latch）
//!
//! 缺陷背景：`expire_apply_sync` / `persist_apply_sync` 收口前不取本键读改写
//! 窗口闩，「`ttl_of_sync` 读 → `put_ttl_sync`/`del_ttl_sync` 写」两步可被另一
//! 会话的同步臂任意交叠——一路原位改写新 TTL 后，另一路 `del_ttl` 把刚写入的
//! 新值一并删除，出现 EXPIRE 应答 :1 而键实际无 TTL 的用户可见异常。异步版
//! `wkv::expire_at`/`persist` 自始持闩（`wkv/src/ttl.rs`），仅同步臂失守，
//! 即「闩只覆盖异步×异步组合」的不对称。
//!
//! C# 对位 UnifiedStore/RMWMethods.cs:HandleExpireInPlaceUpdate /
//! HandlePersistInPlaceUpdate：EXPIRE/PERSIST 一律经 RMW 在 InternalRMW 的
//! ephemeral 桶独占闩内读改写，天然串行。rust 收口后同步臂经
//! `BatchStoreSession::try_rmw_window` 取同一把键闩，失闩沿 `Ok(None)`
//! 降级信号交异步持闩版收口。
//!
//! 断言口径（与 rmw_key_concurrency 同款「只统计已回执」）：命令层降级转
//! 异步闭环，慢路径臂锁忙时报错误帧（交客户端重试），故各用例只统计整数
//! 回执，断言串行化不变式：
//! - EXPIRE 回执 :1 且无 PERSIST 回执 :1 ⇒ 终态 TTL 必存在（禁「回 1 无 TTL」）；
//! - 终态 TTL 消失（-1）⇒ 必有某次 PERSIST 回执 :1（删除唯一入口已显式回执）；
//! - 他会话持闩期间同步臂绝不盲写、放闩即闭环（结构判据，直接焊死闩在路径上，
//!   收口前同步臂不取闩仍原位改写回执 :1，该组断言按构造即失败）。

use std::{
  str::from_utf8,
  sync::{Arc, Barrier},
  thread,
};

use compio::runtime::Runtime;
use tempfile::TempDir;
use wconf::DEFAULT_RESP_VERSION;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
  },
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;

/// 交叉轮数与混战每线程操作数（同键争用足以在收口前稳定复现两步交叠）
const ROUNDS: usize = 50;
const OPS: usize = 60;

/// 单 store 多会话装配（对标生产 thread-per-core：不同连接落在不同线程共享
/// 同一 store，正是同键并发的实况形态）
fn open_store(tag: &str) -> (Arc<WedbStore<SegmentedDevice>>, TempDir) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  (store, dir)
}

/// 在既有 store 上开一条独立连接（独立 StoreSession = 独立纪元参与者）
fn consumer_on(store: &Arc<WedbStore<SegmentedDevice>>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// RESP2 请求帧编码
fn frame(args: &[&[u8]]) -> Vec<u8> {
  let mut out = Vec::new();
  out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
  for arg in args {
    out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
    out.extend_from_slice(arg);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 单命令往返：同步段无输出且挂起慢路径时，以 block_on 承担网络泵角色闭环
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = c.try_consume_messages_into(&mut resp);
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      resp.extend_from_slice(&slow.resolve().await);
    });
  }
  resp
}

/// `:N\r\n` 整数回执解析（非整数回执回 None，供「已回执」判定）
fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

/// 读键剩余 TTL 秒数（单线程断言面；-1 无 TTL、-2 键缺失）
fn ttl_of(rt: &Runtime, store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<i64> {
  let mut c = consumer_on(store);
  reply_int(&roundtrip(rt, &mut c, &[b"TTL", key]))
}

/// 他会话持本键窗口闩期间，同步 EXPIRE/PERSIST 臂绝不盲写（失闩沿降级信号
/// 交异步持闩版，异步臂取不到同一把闩即锁忙错误）；放闩后同步臂立即原位
/// 闭环，异主桶键不受累
#[test]
fn foreign_latch_blocks_sync_expire_and_persist() {
  let (store, _dir) = open_store("ttl-latch.db");
  let rt = Runtime::new().unwrap();
  let key = b"ltl:k";
  let mut c = consumer_on(&store);
  roundtrip(&rt, &mut c, &[b"SET", key, b"v"]);
  assert_eq!(
    reply_int(&roundtrip(&rt, &mut c, &[b"EXPIRE", key, b"10000"])),
    Some(1),
    "基线 TTL 建立须回执 :1"
  );

  // 独立会话持本键读改写窗口闩（同线程持闩：同步臂自旋预算内必失闩降级，
  // 异步臂单次尝试即锁忙，全程无阻塞风险，判定确定）
  let sess_w = store.new_session().unwrap();
  let batch_w = sess_w.enter_batch();
  let window = batch_w.try_rmw_window(key).expect("窗口自取本键桶排他闩");

  assert_ne!(
    reply_int(&roundtrip(&rt, &mut c, &[b"EXPIRE", key, b"20000"])),
    Some(1),
    "他会话持闩期间同步 EXPIRE 臂不得盲写回执 :1（收口前即此竞态窗口）"
  );
  assert_ne!(
    reply_int(&roundtrip(&rt, &mut c, &[b"PERSIST", key])),
    Some(1),
    "他会话持闩期间同步 PERSIST 臂不得盲删回执 :1"
  );
  assert!(
    ttl_of(&rt, &store, key).is_some_and(|t| t > 9000),
    "持闩期间的被拒命令不得触碰基线 TTL"
  );
  assert!(
    reply_int(&roundtrip(
      &rt,
      &mut c,
      &[b"EXPIRE", b"ltl:other", b"20000"]
    ))
    .is_some(),
    "异主桶键的同步臂须照常整数回执（键缺失回 :0），不得被本键闩拖累（禁条带折算）"
  );

  drop(window);
  assert_eq!(
    reply_int(&roundtrip(&rt, &mut c, &[b"EXPIRE", key, b"20000"])),
    Some(1),
    "放闩后同步 EXPIRE 臂立即原位闭环 :1"
  );
  assert!(
    ttl_of(&rt, &store, key).is_some_and(|t| t > 19000),
    "EXPIRE 回执 :1 后 TTL 必存在（票面核心不变式：不回 1 却无 TTL）"
  );
  assert_eq!(
    reply_int(&roundtrip(&rt, &mut c, &[b"PERSIST", key])),
    Some(1),
    "放闩后同步 PERSIST 臂闭环 :1"
  );
  assert_eq!(
    ttl_of(&rt, &store, key),
    Some(-1),
    "PERSIST 回执 :1 后 TTL 必消失"
  );
}

/// 会话 A 同步 EXPIRE 未来时刻、会话 B 同步 PERSIST 同键交叉执行，逐轮校验
/// 串行化不变式；轮间以串行预置（SET，偶数轮再预置基线 TTL）钉定可判定起点
#[test]
fn cross_sync_expire_persist_keeps_acked_ttl() {
  let (store, _dir) = open_store("ttl-cross.db");
  let key = b"crs:k".to_vec();
  let mut expire_1 = 0usize;
  let mut persist_1 = 0usize;

  for round in 0..ROUNDS {
    {
      let rt = Runtime::new().unwrap();
      let mut c = consumer_on(&store);
      roundtrip(&rt, &mut c, &[b"SET", &key, b"v"]);
      if round % 2 == 0 {
        // 预置基线 TTL：PERSIST 有可删之物（奇数轮无 TTL，合法回执只可能是 :0）
        roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"100000"]);
      }
    }
    // 两线程各自独立连接 + 独立 Runtime，栅栏放行前各自即发起，真并发交叉
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [false, true]
      .into_iter()
      .map(|is_persist| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let key = key.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          let resp = if is_persist {
            roundtrip(&rt, &mut c, &[b"PERSIST", &key])
          } else {
            roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"200000"])
          };
          gate.wait();
          // 仅回执 :1 有判定意义（:0/错误帧记 None）
          reply_int(&resp).filter(|v| *v == 1)
        })
      })
      .collect();
    let acks: Vec<Option<i64>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let (e1, p1) = (acks[0].is_some(), acks[1].is_some());
    expire_1 += e1 as usize;
    persist_1 += p1 as usize;

    let rt = Runtime::new().unwrap();
    let ttl = ttl_of(&rt, &store, &key);
    // 核心不变式一：EXPIRE 回执 :1 而无人回执删除 ⇒ TTL 必存在
    if e1 && !p1 {
      assert!(
        ttl.is_some_and(|t| t >= 0),
        "第 {round} 轮：EXPIRE 回执 :1、PERSIST 未回执 :1，终态 TTL 却是 {ttl:?}（回 1 无 TTL）"
      );
    }
    // 核心不变式二：终态 TTL 消失（-1）⇒ 必有 PERSIST 回执 :1（删除唯一显式回执口）
    if ttl == Some(-1) {
      assert!(
        p1,
        "第 {round} 轮：终态无 TTL 但 PERSIST 未回执 :1（新 TTL 被交叠误删）"
      );
    }
  }
  assert!(
    expire_1 > 0 && persist_1 > 0,
    "交叉覆盖不足：EXPIRE 回执 :1 共 {expire_1} 轮、PERSIST 回执 :1 共 {persist_1} 轮"
  );
}

/// 同步臂与异步臂混合并发同键（两路走 RESP 全链快路径，两路直调慢路径臂
/// `exec_slow` 钉住持闩异步版），全部回执与终态仍须满足同款串行化不变式
#[test]
fn mixed_sync_async_expire_persist_keeps_serial_invariants() {
  let (store, _dir) = open_store("ttl-mix.db");
  let key = b"mix:k".to_vec();
  {
    let rt = Runtime::new().unwrap();
    let mut c = consumer_on(&store);
    roundtrip(&rt, &mut c, &[b"SET", &key, b"v"]);
    roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"100000"]);
  }
  let gate = Arc::new(Barrier::new(4));
  let joins: Vec<_> = (0..4)
    .map(|t| {
      let store = Arc::clone(&store);
      let gate = Arc::clone(&gate);
      let key = key.clone();
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut expire_1 = 0usize;
        let mut persist_1 = 0usize;
        // 通道派发：偶数连接走同步快路径臂（t=2 混发两种命令），奇数连接
        // 直调慢路径异步臂（t=1 EXPIRE、t=3 PERSIST）
        let mut run = |ack: Option<i64>, is_expire: bool| {
          if ack.is_some() {
            if is_expire {
              expire_1 += 1;
            } else {
              persist_1 += 1;
            }
          }
        };
        match t {
          0 | 2 => {
            let mut c = consumer_on(&store);
            for i in 0..OPS {
              let is_expire = t == 0 || i % 2 == 0;
              let resp = if is_expire {
                roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"300000"])
              } else {
                roundtrip(&rt, &mut c, &[b"PERSIST", &key])
              };
              run(reply_int(&resp).filter(|v| *v == 1), is_expire);
            }
          }
          _ => {
            let is_expire = t == 1;
            let api: GarnetApi = Arc::new(StoreGarnetApi::new(
              store.new_session().expect("独立会话装配"),
            ));
            for _ in 0..OPS {
              let resp = if is_expire {
                rt.block_on(Arc::clone(&api).exec_slow(
                  RespCommand::Expire,
                  vec![key.clone(), b"300000".to_vec()],
                  DEFAULT_RESP_VERSION,
                ))
              } else {
                rt.block_on(Arc::clone(&api).exec_slow(
                  RespCommand::Persist,
                  vec![key.clone()],
                  DEFAULT_RESP_VERSION,
                ))
              };
              run(reply_int(&resp).filter(|v| *v == 1), is_expire);
            }
          }
        }
        gate.wait();
        (expire_1, persist_1)
      })
    })
    .collect();
  let mut expire_1 = 0usize;
  let mut persist_1 = 0usize;
  for j in joins {
    let (e, p) = j.join().unwrap();
    expire_1 += e;
    persist_1 += p;
  }
  assert!(
    expire_1 > 0 && persist_1 > 0,
    "混合并发覆盖不足：EXPIRE :1 = {expire_1}、PERSIST :1 = {persist_1}"
  );
  // 收尾强闭环（无并发者）：EXPIRE :1 → TTL 必在；PERSIST :1 → TTL 必无——
  // 混战不得残留吞写或幽灵 TTL
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  assert_eq!(
    reply_int(&roundtrip(&rt, &mut c, &[b"EXPIRE", &key, b"400000"])),
    Some(1),
    "收尾 EXPIRE 须闭环 :1"
  );
  assert!(
    ttl_of(&rt, &store, &key).is_some_and(|t| t >= 0),
    "收尾 EXPIRE 回执 :1 后 TTL 必存在"
  );
  assert_eq!(
    reply_int(&roundtrip(&rt, &mut c, &[b"PERSIST", &key])),
    Some(1),
    "收尾 PERSIST 须闭环 :1（若混战中 TTL 已被删且键被清则回 :0——本装配键恒存活）"
  );
  assert_eq!(
    ttl_of(&rt, &store, &key),
    Some(-1),
    "收尾 PERSIST 后 TTL 必消失"
  );
}
