//! 键管理族多步键序列读改写窗口收口回归测试（票 zcode-r15-generic 发现一）
//!
//! 缺陷背景：RENAME/RENAMENX（三探旧键 → 读 TTL/ETag → NX 探测 → 写新删旧）、
//! RESTORE（probe 存活探测 → upsert 写入 → TTL 落库）、MSETNX（逐键判定 →
//! 批量写入）原为无窗口多步序列——thread-per-core 多 worker 跨核并发下，
//! 「探测/读旧值」与「写回」间隙内他 worker 会话的写入可插入，产出非可串行化
//! 终态。修复后各命令入口经 `BatchStoreSession::try_rmw_window_sorted`
//! （快）/ `rmw_window_sorted`（慢）取键组桶序排他闩（哈希升序定序防交叉
//! 死锁），闩内完成全序列；失闩沿既有 `Ok(false)` 降级慢路径同序持窗重放，
//! 不新增第二把锁。GETDEL 读删一体收口已随票 zcode-r15-expire 落地，本文件
//! 补其「双 GETDEL 至多一次交付」并发面。
//!
//! C# 对位（键组排他锁内判定与写入一体）：
//! - libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:241 RENAME
//!   （SaveKeyEntryToLock(oldKey/newKey, Exclusive) 双键事务锁覆盖全序列）
//! - libs/server/Resp/KeyAdminCommands.cs:25 NetworkRESTORE（单次
//!   SET_Conditional SETEXNX 原子条件写）
//! - libs/server/Storage/Session/MainStore/MainStoreOps.cs:349
//!   MSET_Conditional（全键排他锁内 EXISTS 判定 + 批量 SET + Commit）
//!
//! 断言口径与 ttl_composite_write_window 同款「只统计已回执」的串行化不变式：
//! 并发双方均已回执后，终态必落在某一串行序可达点上；串行序不可达的组合
//! （DEL :1 而旧键值复活于新键、双成功而值被「判定后写入者」之外的第三值
//! 覆盖、双 GETDEL 双双得值）即窗口失效。

use std::{
  str::from_utf8,
  sync::{Arc, Barrier},
  thread,
  time::Duration,
};

use compio::runtime::Runtime;
use wbase::crc64::hash as crc64_hash;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::roundtrip;
use wresp::length::try_write_length;
use wtest_base::open_test_store;

/// 交叉轮数（同键争用足以在收口前稳定复现多步交错）
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

/// 读键值（单线程断言面；键缺失回 None）
fn get_of(rt: &Runtime, store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let mut c = consumer_on(store);
  reply_bulk(&roundtrip(rt, &mut c, &[b"GET", key]))
}

/// 合法 RESTORE 载荷（类型字节 0x00 + 长度前缀 + 值 + rdb 版本 + crc64；
/// crc 口径与 network_dump 同源——类型字节起算，见 deviations.md 条目 16）
fn restore_payload(val: &[u8]) -> Vec<u8> {
  let mut encoded_len = [0u8; 5];
  let bytes_written = try_write_length(val.len() as u32, &mut encoded_len).unwrap();
  let mut payload = Vec::with_capacity(1 + bytes_written + val.len() + 2 + 8);
  payload.push(0x00);
  payload.extend_from_slice(&encoded_len[..bytes_written]);
  payload.extend_from_slice(val);
  payload.extend_from_slice(&11u16.to_le_bytes());
  let crc = crc64_hash(&payload);
  payload.extend_from_slice(&crc);
  payload
}

/// 会话 A `RENAME old new`、会话 B `DEL old` 同键真并发交叉。
///
/// 串行序可达点：DEL 先 ⇒ RENAME 读不到旧键回 NOSUCHKEY、new 不存在；
/// RENAME 先 ⇒ old 换名 new=old_val、DEL 回 :0。收口前的败 face：RENAME
/// 读得 old_val 后 DEL 落地回 :1，RENAME 继续写 new=old_val——「DEL :1 而
/// 旧键值复活于 new」在任何串行序不可达。
#[test]
fn rename_vs_del_old_serializes() {
  let (_dir, store) = open_test_store("key-admin-rename-del.db").unwrap();
  let old = b"ka:rold".to_vec();
  let new = b"ka:rnew".to_vec();
  let mut rename_acked = 0usize;
  let rt = Runtime::new().unwrap();

  for round in 0..ROUNDS {
    {
      let mut c = consumer_on(&store);
      roundtrip(&rt, &mut c, &[b"SET", &old, b"v0"]);
      roundtrip(&rt, &mut c, &[b"DEL", &new]);
    }
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [false, true]
      .into_iter()
      .map(|is_rename| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let old = old.clone();
        let new = new.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          gate.wait();
          if !is_rename && round % 2 == 1 {
            thread::sleep(Duration::from_micros(200));
          }
          let resp = if is_rename {
            roundtrip(&rt, &mut c, &[b"RENAME", &old, &new])
          } else {
            roundtrip(&rt, &mut c, &[b"DEL", &old])
          };
          (is_rename, resp)
        })
      })
      .collect();
    let results: Vec<(bool, Vec<u8>)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let del_acked = results
      .iter()
      .find(|(is_rename, _)| !is_rename)
      .and_then(|(_, resp)| reply_int(resp));
    let rename_acked_round = results
      .iter()
      .any(|(is_rename, resp)| *is_rename && resp == b"+OK\r\n");
    if rename_acked_round {
      rename_acked += 1;
    }

    let new_val = get_of(&rt, &store, &new);
    if del_acked == Some(1) {
      // DEL :1 ⇒ RENAME 在删除前未读到旧键（或整体先于删除完成）：串行序下
      // new 不得携带旧键值存在（收口前败 face：new=old_val 复活）
      assert_ne!(
        new_val.as_deref(),
        Some(b"v0".as_slice()),
        "第 {round} 轮：DEL old 回 :1 而 new 键携带旧值复活（RENAME 读旧值与写新键\
         间隙的并发删除交错，非可串行化）"
      );
    }
    if rename_acked_round {
      assert_eq!(
        new_val.as_deref(),
        Some(b"v0".as_slice()),
        "第 {round} 轮：RENAME +OK 而 new 键值不是旧值（迁移序列断裂）"
      );
      assert_eq!(
        get_of(&rt, &store, &old),
        None,
        "第 {round} 轮：RENAME +OK 后 old 键必须消失"
      );
    }
  }
  assert!(
    rename_acked > 0,
    "交叉覆盖不足：RENAME +OK 共 {rename_acked} 轮"
  );
}

/// 会话 A `RENAMENX old new`、会话 B `SET new v2` 同键真并发交叉（new 预置
/// 不存在使 NX 判定可放行）。串行序可达点：SET 先 ⇒ RENAMENX :0、终态 v2；
/// RENAMENX 先 ⇒ :1、SET 后写者胜终态 v2。收口前的败 face：RENAMENX NX 判
/// 定 new 不存在 → SET 落库 → RENAMENX 写 new=old_val 回 :1——双成功而终态
/// 是 old_val，任何串行序不可达。
#[test]
fn renamenx_vs_set_new_serializes() {
  let (_dir, store) = open_test_store("key-admin-renamenx-set.db").unwrap();
  let old = b"ka:nold".to_vec();
  let new = b"ka:nnew".to_vec();
  let mut nx_acked = 0usize;
  let rt = Runtime::new().unwrap();

  for round in 0..ROUNDS {
    {
      let mut c = consumer_on(&store);
      roundtrip(&rt, &mut c, &[b"SET", &old, b"v0"]);
      roundtrip(&rt, &mut c, &[b"DEL", &new]);
    }
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [false, true]
      .into_iter()
      .map(|is_renamenx| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let old = old.clone();
        let new = new.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          gate.wait();
          if !is_renamenx && round % 2 == 1 {
            thread::sleep(Duration::from_micros(200));
          }
          let resp = if is_renamenx {
            roundtrip(&rt, &mut c, &[b"RENAMENX", &old, &new])
          } else {
            roundtrip(&rt, &mut c, &[b"SET", &new, b"v2"])
          };
          (is_renamenx, resp)
        })
      })
      .collect();
    let results: Vec<(bool, Vec<u8>)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let nx = results
      .iter()
      .find(|(is_renamenx, _)| *is_renamenx)
      .and_then(|(_, resp)| reply_int(resp));
    let set_ok = results
      .iter()
      .any(|(is_renamenx, resp)| !is_renamenx && resp == b"+OK\r\n");
    if nx == Some(1) {
      nx_acked += 1;
    }

    let new_val = get_of(&rt, &store, &new).expect("RENAMENX/SET 任一成功后 new 必存活");
    // 收口前败 face 判据：RENAMENX :1 且 SET +OK 时终态必为 v2（SET 是
    // :1 之后的唯一成功写者；终态 v0 即 NX 判定与写入分离的交交错）
    if nx == Some(1) && set_ok {
      assert_eq!(
        new_val.as_slice(),
        b"v2",
        "第 {round} 轮：RENAMENX :1 且 SET +OK 而终态 new={new_val:?}（NX 判定\
         后 SET 落库再被迁移覆写，双成功组合下的非可串行化终态）"
      );
    }
  }
  assert!(nx_acked > 0, "交叉覆盖不足：RENAMENX :1 共 {nx_acked} 轮");
}

/// 会话 A `RESTORE key payload(v1)`、会话 B `SET key v2` 同键真并发交叉（key
/// 预置不存在使 NX 条件写可放行）。串行序可达点：SET 先 ⇒ RESTORE BUSYKEY、
/// 终态 v2；RESTORE 先 ⇒ +OK、SET 后写者胜终态 v2。收口前的败 face：
/// RESTORE probe 判不存在 → SET 落库 → RESTORE 覆写 v1 回 +OK——RESTORE +OK
/// 且终态是载荷值 v1 而 SET 亦 +OK，任何串行序不可达（BUSYKEY 契约被绕过）。
#[test]
fn restore_vs_set_serializes() {
  let (_dir, store) = open_test_store("key-admin-restore-set.db").unwrap();
  let key = b"ka:rk".to_vec();
  let payload = restore_payload(b"v1");
  let mut restore_acked = 0usize;
  let rt = Runtime::new().unwrap();

  for round in 0..ROUNDS {
    {
      let mut c = consumer_on(&store);
      roundtrip(&rt, &mut c, &[b"DEL", &key]);
    }
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [false, true]
      .into_iter()
      .map(|is_restore| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let key = key.clone();
        let payload = payload.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          gate.wait();
          if !is_restore && round % 2 == 1 {
            thread::sleep(Duration::from_micros(200));
          }
          let resp = if is_restore {
            roundtrip(&rt, &mut c, &[b"RESTORE", &key, b"0", payload.as_slice()])
          } else {
            roundtrip(&rt, &mut c, &[b"SET", &key, b"v2"])
          };
          (is_restore, resp)
        })
      })
      .collect();
    let results: Vec<(bool, Vec<u8>)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let restore_ok = results
      .iter()
      .any(|(is_restore, resp)| *is_restore && resp == b"+OK\r\n");
    let set_ok = results
      .iter()
      .any(|(is_restore, resp)| !is_restore && resp == b"+OK\r\n");
    if restore_ok {
      restore_acked += 1;
    }

    let val = get_of(&rt, &store, &key).expect("RESTORE/SET 任一成功后 key 必存活");
    if restore_ok && set_ok {
      assert_eq!(
        val.as_slice(),
        b"v2",
        "第 {round} 轮：RESTORE +OK 且 SET +OK 而终态 key={val:?}（probe 判不存在\
         后 SET 落库再被载荷覆写，BUSYKEY 契约绕过 + 并发写丢失）"
      );
    }
    assert_ne!(
      val.as_slice(),
      b"v1",
      "第 {round} 轮：终态为载荷值 v1 而 SET 已 +OK——SET 后写者必胜或 RESTORE\
       必 BUSYKEY，v1 存续即窗口交错"
    );
  }
  assert!(
    restore_acked > 0,
    "交叉覆盖不足：RESTORE +OK 共 {restore_acked} 轮"
  );
}

/// 会话 A/B `GETDEL k` 双连接真并发交叉：GETDEL 至多一次交付语义——恰一者
/// 得值（=被删值），另一者 nil。收口机制（读删一体同窗）随票
/// zcode-r15-expire 落地，本用例补双 GETDEL 交付面：收口前两步读删交错下
/// 双双读到旧值、双双回值。
#[test]
fn concurrent_getdels_deliver_at_most_once() {
  let (_dir, store) = open_test_store("key-admin-getdel-getdel.db").unwrap();
  let key = b"ka:gk".to_vec();
  let rt = Runtime::new().unwrap();
  let mut both_delivered = 0usize;

  for round in 0..ROUNDS {
    {
      let mut c = consumer_on(&store);
      roundtrip(&rt, &mut c, &[b"SET", &key, b"old"]);
    }
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2)
      .map(|_| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let key = key.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          gate.wait();
          roundtrip(&rt, &mut c, &[b"GETDEL", &key])
        })
      })
      .collect();
    let results: Vec<Vec<u8>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let delivered: Vec<Vec<u8>> = results.iter().filter_map(|r| reply_bulk(r)).collect();
    // 回执面：双连接真并发下恰一者得值（=被删值），另一者 nil；双双得值即双交付，零得值即写丢失
    assert_eq!(
      delivered.len(),
      1,
      "第 {round} 轮：双 GETDEL 竞争必须恰有一者得值（实际 {} 者）",
      delivered.len()
    );
    if delivered.len() > 1 {
      both_delivered += 1;
    }

    assert_eq!(
      get_of(&rt, &store, &key),
      None,
      "第 {round} 轮：GETDEL 竞争后键必须消失"
    );
    let got = &delivered[0];
    assert_eq!(
      got.as_slice(),
      b"old",
      "第 {round} 轮：GETDEL 得值者必须答被删值 old（实际 {got:?}）"
    );
  }
  assert_eq!(
    both_delivered, 0,
    "双 GETDEL 双交付 {both_delivered} 轮——读删一体窗口失效"
  );
}

/// 会话 A `MSETNX k1 v1 k2 v2`、会话 B `SET k2 x` 同键真并发交叉（双键预置
/// 不存在使全有或全无判定可放行）。串行序可达点：SET 先 ⇒ MSETNX 判 k2 在场
/// 回 :0、终态 x；MSETNX 先 ⇒ :1、SET 后写者胜终态 x。收口前的败 face：
/// MSETNX 逐键判定通过（k2 不存在）→ SET 落库 → 批量写 k2=v1 回 :1——双成功
/// 而终态是 v1，任何串行序不可达（全有或全无契约破坏）。
#[test]
fn msetnx_vs_set_serializes() {
  let (_dir, store) = open_test_store("key-admin-msetnx-set.db").unwrap();
  let k1 = b"ka:mk1".to_vec();
  let k2 = b"ka:mk2".to_vec();
  let mut msetnx_acked = 0usize;
  let rt = Runtime::new().unwrap();

  for round in 0..ROUNDS {
    {
      let mut c = consumer_on(&store);
      roundtrip(&rt, &mut c, &[b"DEL", &k1]);
      roundtrip(&rt, &mut c, &[b"DEL", &k2]);
    }
    let gate = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [false, true]
      .into_iter()
      .map(|is_msetnx| {
        let store = Arc::clone(&store);
        let gate = Arc::clone(&gate);
        let k1 = k1.clone();
        let k2 = k2.clone();
        thread::spawn(move || {
          let rt = Runtime::new().unwrap();
          let mut c = consumer_on(&store);
          gate.wait();
          if !is_msetnx && round % 2 == 1 {
            thread::sleep(Duration::from_micros(200));
          }
          let resp = if is_msetnx {
            roundtrip(&rt, &mut c, &[b"MSETNX", &k1, b"v1", &k2, b"v1"])
          } else {
            roundtrip(&rt, &mut c, &[b"SET", &k2, b"x"])
          };
          (is_msetnx, resp)
        })
      })
      .collect();
    let results: Vec<(bool, Vec<u8>)> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let msetnx = results
      .iter()
      .find(|(is_msetnx, _)| *is_msetnx)
      .and_then(|(_, resp)| reply_int(resp));
    let set_ok = results
      .iter()
      .any(|(is_msetnx, resp)| !is_msetnx && resp == b"+OK\r\n");
    if msetnx == Some(1) {
      msetnx_acked += 1;
    }

    if msetnx == Some(1) {
      assert_eq!(get_of(&rt, &store, &k1).as_deref(), Some(b"v1".as_slice()));
    }
    let k2_val = get_of(&rt, &store, &k2).expect("MSETNX/SET 任一成功后 k2 必存活");
    if msetnx == Some(1) && set_ok {
      assert_eq!(
        k2_val.as_slice(),
        b"x",
        "第 {round} 轮：MSETNX :1 且 SET +OK 而终态 k2={k2_val:?}（逐键判定通过后\
         SET 落库再被批量写覆写，全有或全无契约破坏）"
      );
    }
  }
  assert!(
    msetnx_acked > 0,
    "交叉覆盖不足：MSETNX :1 共 {msetnx_acked} 轮"
  );
}

/// 单线程回归：收口后 RENAME/RENAMENX/RESTORE/MSETNX 应答与终态逐字节不变
/// （对标 C# 单实现语义；窗口失灵或误降级即在此暴露）
#[test]
fn single_thread_replies_unchanged() {
  let (_dir, store) = open_test_store("key-admin-replay.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  let old = b"ka:sold";
  let new = b"ka:snew";
  let nx_new = b"ka:snx";
  let rk = b"ka:srk";

  // RENAME：+OK + 值迁移 + 旧键消失；重复 RENAME 同键早退 +OK
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", old, b"a"]), b"+OK\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"RENAME", old, new]), b"+OK\r\n");
  assert_eq!(get_of(&rt, &store, new).as_deref(), Some(b"a".as_slice()));
  assert_eq!(get_of(&rt, &store, old), None, "RENAME 后旧键必须消失");
  assert_eq!(roundtrip(&rt, &mut c, &[b"RENAME", new, new]), b"+OK\r\n");

  // RENAMENX：目标缺失 :1；目标在场 :0 且不动旧键
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RENAMENX", new, nx_new]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", new, b"a"]), b"+OK\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RENAMENX", new, nx_new]),
    b":0\r\n"
  );
  assert_eq!(get_of(&rt, &store, new).as_deref(), Some(b"a".as_slice()));
  assert_eq!(
    get_of(&rt, &store, nx_new).as_deref(),
    Some(b"a".as_slice())
  );

  // RESTORE：新键 +OK + 载荷值；已存在键 BUSYKEY 且终态不变
  let payload = restore_payload(b"rv");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RESTORE", rk, b"0", payload.as_slice()]),
    b"+OK\r\n"
  );
  assert_eq!(get_of(&rt, &store, rk).as_deref(), Some(b"rv".as_slice()));
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RESTORE", rk, b"0", payload.as_slice()]),
    b"-BUSYKEY Target key name already exists.\r\n"
  );

  // RESTORE 带 TTL：+OK + TTL 折新（口径为秒，继承 Garnet）
  let rk2 = b"ka:srk2";
  let payload_ttl = restore_payload(b"tv");
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"RESTORE", rk2, b"100", payload_ttl.as_slice()]
    ),
    b"+OK\r\n"
  );
  let ttl = reply_int(&roundtrip(&rt, &mut c, &[b"TTL", rk2])).expect("RESTORE TTL 后键必存活");
  assert!((50..=100).contains(&ttl), "RESTORE TTL 应 ≈100s: {ttl}");

  // MSETNX：全键缺失 :1；任一在场 :0（全有或全无，:0 时零写入）
  let m1 = b"ka:sm1";
  let m2 = b"ka:sm2";
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"MSETNX", m1, b"1", m2, b"2"]),
    b":1\r\n"
  );
  assert_eq!(get_of(&rt, &store, m1).as_deref(), Some(b"1".as_slice()));
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"MSETNX", m1, b"9", b"ka:sm3", b"3"]),
    b":0\r\n"
  );
  assert_eq!(get_of(&rt, &store, b"ka:sm3"), None, "MSETNX :0 必须零写入");
}
