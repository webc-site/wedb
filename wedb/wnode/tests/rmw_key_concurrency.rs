//! 同键读改写（RMW）原子窗口并发回归测试
//!
//! 缺陷背景（票 rmw-atomic-read-modify-write-window）：C# 的 INCR/APPEND/SETBIT/
//! PFADD 与全部集合族 RMW 在 Tsavorite 的 ephemeral 桶独占锁内完成「回溯读旧值 →
//! 算新值 → 写回」全程
//!（libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:FindOrCreateTagAndTryEphemeralXLock），
//! 同键并发天然串行。收口前 rust 把同一状态机拆成命令层两步（`read_user_sync`
//! 裸读 → 本地算新值 → `try_rmw_sync` 盲写绝对值），跨连接同键并发必丢更新：
//! 本文件四条用例（并发 INCR / APPEND / HSET / PFADD）在收口前按构造即失败
//!（终值 < 已回执更新数、追加段丢失、HSET 字段被整值抹掉、PFCOUNT 低于串行参照）。
//!
//! 断言口径：命令层约定 `Ok(false)` = 该命令转异步闭环（字符串族慢路径臂由
//! task/ing/slow-path-string-key-admin-arms.md 单独承接），故各用例只统计
//! **已回执**的更新，并断言「存储终态 == 已回执更新集合」——丢更新即终态缺项，
//! 与个别命令是否降级无关。
//!
//! 覆盖面（验收 1 的两型判据分置）：
//! - 前四条为暖区（内存命中）快路径的同键并发，走 RESP 全链多连接多线程；
//! - `cold_slow_*` 四条为慢路径臂（`string_slow` / `bitmap_slow` 冷读—算—写回）
//!   的同键并发：先把键刷盘冷化，再以独立会话并发直调 `exec_slow`，同一 run 缝内
//!   全部任务必先读到同一份冷旧值再各自写回，收口前按构造即丢更新；
//! - `same_key_window_*` / `transactional_window_*` / `two_session_windows_*` 三条
//!   是窗口层确定性用例：不依赖调度即可断言「同键第二窗取闩失败、异主桶键不受累、
//!   事务态让闩、持窗期内绝不被串读」。

use std::{
  str::from_utf8,
  sync::{Arc, mpsc},
  thread,
};

use compio::runtime::Runtime;
use futures_util::future::join_all;
use tempfile::tempdir;
use wconf::DEFAULT_RESP_VERSION;
use wdev::SegmentedDevice;
use wkv::{BatchStoreSession, RmwWindow, SessionLocking, StoreResult, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
  },
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;

/// 并发连接数与每连接操作数（4×250 同键争用足以在收口前稳定复现丢更新）
const THREADS: usize = 4;
const ITERS: usize = 250;

/// 单 store 多会话装配（对标生产 thread-per-core：不同连接落在不同线程共享同一
/// store，正是同键并发的实况形态）
fn open_store(tag: &str) -> (Arc<WedbStore<SegmentedDevice>>, tempfile::TempDir) {
  let dir = tempdir().unwrap();
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

/// `:N\r\n` 整数回执解析（非整数回执回 None，供「已回执更新」计数）
fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

/// `$N\r\n<payload>\r\n` 批量回执解析
fn reply_bulk(resp: &[u8]) -> Option<Vec<u8>> {
  let rest = resp.strip_prefix(b"$")?;
  // 手工定位首个 CRLF（`slice::split_once` 在稳定版仍是 unstable feature）
  let end = rest
    .windows(2)
    .position(|w| w == b"\r\n")
    .filter(|&i| i + 2 <= rest.len())?;
  let len: usize = from_utf8(&rest[..end]).ok()?.parse().ok()?;
  let payload = rest.get(end + 2..)?.get(..len)?;
  Some(payload.to_vec())
}

/// 末态读值（单线程断言面）
fn get_value(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Vec<u8> {
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(store);
  reply_bulk(&roundtrip(&rt, &mut c, &[b"GET", key])).unwrap_or_default()
}

/// 并发 INCR：终值必须等于已回执自增数（收口前每对同键并发读改写丢一次自增）
#[test]
fn concurrent_incr_loses_no_update() {
  let (store, _dir) = open_store("rmw-incr.db");
  let applied: usize = (0..THREADS)
    .map(|_| {
      let store = Arc::clone(&store);
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = consumer_on(&store);
        (0..ITERS)
          .filter(|_| reply_int(&roundtrip(&rt, &mut c, &[b"INCR", b"rmw:counter"])).is_some())
          .count()
      })
    })
    .collect::<Vec<_>>()
    .into_iter()
    .map(|h| h.join().unwrap())
    .sum();
  assert!(applied > 0, "并发窗口不得把全部自增都判为降级");

  let final_value: i64 = String::from_utf8(get_value(&store, b"rmw:counter"))
    .unwrap()
    .parse()
    .expect("INCR 终值必须是十进制整数");
  assert_eq!(
    final_value, applied as i64,
    "同键并发 INCR 丢更新：终值 {final_value} ≠ 已回执自增数 {applied}"
  );
}

/// 并发 APPEND：终值长度与逐连接段序必须完整（段间可交错，段内不得丢）
#[test]
fn concurrent_append_loses_no_segment() {
  let (store, _dir) = open_store("rmw-append.db");
  let handles: Vec<_> = (0..THREADS)
    .map(|t| {
      let store = Arc::clone(&store);
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = consumer_on(&store);
        let mut segments = Vec::new();
        for i in 0..ITERS {
          // 段标记自带连接号与序号，终态可按连接还原序列表
          let seg = format!("t{t:02}i{i:04};");
          if reply_int(&roundtrip(
            &rt,
            &mut c,
            &[b"APPEND", b"rmw:log", seg.as_bytes()],
          ))
          .is_some()
          {
            segments.push(seg);
          }
        }
        segments
      })
    })
    .collect();
  let applied: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
  let total: usize = applied.iter().map(Vec::len).sum();
  assert!(total > 0, "并发窗口不得把全部追加都判为降级");
  let expected_len: usize = applied.iter().flatten().map(String::len).sum();

  let final_value = String::from_utf8(get_value(&store, b"rmw:log")).unwrap();
  assert_eq!(
    final_value.len(),
    expected_len,
    "并发 APPEND 丢段：终值长度 {} ≠ 已回执段总长 {expected_len}（段数 {total}）",
    final_value.len()
  );
  for (t, segments) in applied.iter().enumerate() {
    let mut cursor = 0usize;
    for seg in segments {
      let found = final_value[cursor..]
        .find(seg.as_str())
        .unwrap_or_else(|| panic!("连接 {t} 的追加段 {seg} 在终值中丢失或乱序"));
      cursor += found + seg.len();
    }
  }
}

/// 并发 HSET 同键不同字段：信封整值写回臂必须被本键窗口串行（收口后字段互不抹除）
#[test]
fn concurrent_hset_loses_no_field() {
  let (store, _dir) = open_store("rmw-hset.db");
  let handles: Vec<_> = (0..THREADS)
    .map(|t| {
      let store = Arc::clone(&store);
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = consumer_on(&store);
        (0..ITERS)
          .filter(|i| {
            let field = format!("f{t:02}_{i:04}");
            reply_int(&roundtrip(
              &rt,
              &mut c,
              &[b"HSET", b"rmw:hash", field.as_bytes(), b"v"],
            )) == Some(1)
          })
          .count()
      })
    })
    .collect();
  let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
  assert!(total > 0, "并发窗口不得把全部 HSET 都判为降级");

  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  let hlen =
    reply_int(&roundtrip(&rt, &mut c, &[b"HLEN", b"rmw:hash"])).expect("HLEN 应答为整数") as usize;
  assert_eq!(
    hlen, total,
    "并发 HSET 同键不同字段被整值写回抹除：HLEN {hlen} ≠ 已回执字段数 {total}"
  );
}

/// 并发 PFADD：与「同元素集串行参照」同基数（收口前丢成员致 PFCOUNT 虚低）
#[test]
fn concurrent_pfadd_matches_serial_reference() {
  let (store, _dir) = open_store("rmw-hll.db");
  let handles: Vec<_> = (0..THREADS)
    .map(|t| {
      let store = Arc::clone(&store);
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = consumer_on(&store);
        let mut added = Vec::new();
        for i in 0..ITERS {
          // 逐连接独立元素域；已回执元素集另作串行参照，估算误差同源可比
          let elem = format!("e{t:02}_{i:04}");
          if reply_int(&roundtrip(
            &rt,
            &mut c,
            &[b"PFADD", b"rmw:hll", elem.as_bytes()],
          ))
          .is_some()
          {
            added.push(elem);
          }
        }
        added
      })
    })
    .collect();
  let applied = handles
    .into_iter()
    .map(|h| h.join().unwrap())
    .collect::<Vec<_>>();
  let elems = applied.concat();
  assert!(!elems.is_empty(), "并发窗口不得把全部 PFADD 都判为降级");

  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  let concurrent =
    reply_int(&roundtrip(&rt, &mut c, &[b"PFCOUNT", b"rmw:hll"])).expect("PFCOUNT 应答为整数");

  // 串行参照：同元素集逐条串行 PFADD 到另一 store（元素串完全相同故可比）
  let (ref_store, _ref_dir) = open_store("rmw-hll-ref.db");
  let mut ref_c = consumer_on(&ref_store);
  for elem in &elems {
    roundtrip(&rt, &mut ref_c, &[b"PFADD", b"rmw:hll", elem.as_bytes()]);
  }
  let reference =
    reply_int(&roundtrip(&rt, &mut ref_c, &[b"PFCOUNT", b"rmw:hll"])).expect("参照 PFCOUNT 整数");

  assert_eq!(
    concurrent,
    reference,
    "并发 PFADD 丢成员：{} 个已回执元素的基数 {concurrent} ≠ 串行参照 {reference}",
    elems.len()
  );
}

/// 取一对必落不同主桶的用户键（反条带判据用：条带分组会令异键折到同一把锁）
fn distinct_bucket_keys(store: &Arc<WedbStore<SegmentedDevice>>) -> (Vec<u8>, Vec<u8>) {
  let index = store.index.load();
  let first: &[u8] = b"rmw:latch:a";
  let first_idx = index.bucket_index_for_key(first);
  for i in 0..4096_u32 {
    let candidate = format!("rmw:latch:b{i}").into_bytes();
    if index.bucket_index_for_key(&candidate) != first_idx {
      return (first.to_vec(), candidate);
    }
  }
  panic!("未找到与本键不同主桶的候选键");
}

/// 窗口同步写回（写回面只挂在 [`RmwWindow`] 上，收口前该形态即「读后盲写绝对值」）
fn window_write(window: &RmwWindow<'_, '_, SegmentedDevice>, val: &[u8]) {
  assert!(
    window
      .try_rmw_sync(val)
      .expect("窗口写回不得报存储错误")
      .is_ok(),
    "窗口同步写回须内存直落（Err(u64) = 降级三态，本用例装配下不出现）"
  );
}

/// 窗口纪元内读回本键值（与 [`window_write`] 同键域：KeyTag::String 记录）
fn batch_value(batch: &BatchStoreSession<'_, SegmentedDevice>, key: &[u8]) -> Vec<u8> {
  match batch
    .try_read_sync(key, |v| v.to_vec())
    .expect("同键窗口内读旧值不得报存储错误")
  {
    StoreResult::Success(v) => v,
    StoreResult::NotFound => panic!("窗口内读旧值须命中，实际 NOTFOUND"),
    StoreResult::RecordOnDisk => panic!("窗口内读旧值须内存命中，实际 RECORD_ON_DISK"),
  }
}

/// 冷化 + 并发慢路径臂执行器：基值落盘后以 n 条独立会话并发直调 `exec_slow`
/// （同一 run 缝内全部任务必先读到同一份冷旧值，正是收口前两步式的交叠实况），
/// 回逐条应答帧；空帧/错误帧由调用方按「未回执」处置
fn cold_slow_fanout(
  store: &Arc<WedbStore<SegmentedDevice>>,
  rt: &Runtime,
  key: &[u8],
  base: &[u8],
  ops: Vec<(RespCommand, Vec<Vec<u8>>)>,
) -> Vec<Vec<u8>> {
  {
    let mut c = consumer_on(store);
    roundtrip(rt, &mut c, &[b"SET", key, base]);
  }
  rt.block_on(store.flush_and_evict_all())
    .expect("冷化刷盘成功（对标 hll_slow_ttl 慢路径装配）");
  let apis: Vec<GarnetApi> = (0..ops.len())
    .map(|_| {
      Arc::new(StoreGarnetApi::new(
        store.new_session().expect("独立会话装配"),
      )) as GarnetApi
    })
    .collect();
  rt.block_on(join_all(apis.into_iter().zip(ops).map(
    |(api, (cmd, args))| async move { api.exec_slow(cmd, args, DEFAULT_RESP_VERSION).await },
  )))
}

/// `*1\r\n:N\r\n` 单子命令整数回帧解析（BITFIELD 单写子命令的应答形态）
fn reply_array_int(resp: &[u8]) -> Option<i64> {
  reply_int(resp.strip_prefix(b"*1\r\n")?)
}

/// 同键第二窗必须取闩失败、异主桶键不受累、放闩即恢复（窗口层确定性互斥判据）
#[test]
fn same_key_window_is_exclusive_and_neighbour_key_unaffected() {
  let (store, _dir) = open_store("rmw-latch.db");
  let (key, neighbour) = distinct_bucket_keys(&store);
  let sess_a = store.new_session().unwrap();
  let sess_b = store.new_session().unwrap();
  let batch_a = sess_a.enter_batch();
  let batch_b = sess_b.enter_batch();

  let window = batch_a
    .try_rmw_window(&key)
    .expect("非事务态窗口自取本键桶排他闩");
  window_write(&window, b"v1");
  assert!(
    batch_b.try_rmw_window(&key).is_none(),
    "同键并发读改写必须互斥（收口前无此闩，读—写两步任意交错即丢更新）"
  );
  assert!(
    batch_b.try_rmw_window(&neighbour).is_some(),
    "异主桶键不得被折到同一把锁（修法四：禁条带分组）"
  );
  drop(window);
  assert!(
    batch_b.try_rmw_window(&key).is_some(),
    "窗口离开作用域即放闩（RAII 放闩，绝不留残闩）"
  );
}

/// 事务锁模式下窗口让闩（本键桶闩已在事务手上，窗口绝不自旋等自己），
/// RAII 守卫离开即还原非事务态、同键互斥恢复
#[test]
fn transactional_window_yields_latch_and_guard_restores() {
  let (store, _dir) = open_store("rmw-latch-txn.db");
  let (key, _) = distinct_bucket_keys(&store);
  let sess_a = store.new_session().unwrap();
  let sess_b = store.new_session().unwrap();
  let batch_a = sess_a.enter_batch();
  let batch_b = sess_b.enter_batch();
  let _window = batch_a.try_rmw_window(&key).expect("会话 A 持本键桶排他闩");

  assert!(
    batch_b.try_rmw_window(&key).is_none(),
    "前置判据：非事务态同键互斥"
  );
  let guard = sess_b.push_session_locking(SessionLocking::Transactional);
  assert!(
    batch_b.try_rmw_window(&key).is_some(),
    "事务态窗口让闩：同键桶闩已由本会话事务持有，绝不自旋等自己"
  );
  drop(guard);
  assert_eq!(
    sess_b.session_locking(),
    SessionLocking::Basic,
    "RAII 守卫离开即还原锁器模式位"
  );
  assert!(
    batch_b.try_rmw_window(&key).is_none(),
    "还原非事务态后同键互斥随即恢复（无位残留）"
  );
}

/// 两任务交叠的读改写：持窗未写回期间第二会话根本进不来（不串读），
/// 放闩后读到的必是对侧已写回的新值（不丢更新）
#[test]
fn two_session_windows_never_cross_read_or_lose_update() {
  let (store, _dir) = open_store("rmw-latch-rmw.db");
  let key = b"rmw:latch:counter".to_vec();
  {
    let sess = store.new_session().unwrap();
    let batch = sess.enter_batch();
    let window = batch.try_rmw_window(&key).expect("基值写回取窗");
    window_write(&window, b"0");
  }
  // 握手通道容量 0：三步（宣告持闩 / 宣告交叠已被拒 / 宣告已放闩）逐一同步会合，
  // 不经调度器赌时序，故为确定性用例
  let (held_tx, held_rx) = mpsc::sync_channel::<()>(0);
  let (refused_tx, refused_rx) = mpsc::sync_channel::<()>(0);
  let (done_tx, done_rx) = mpsc::sync_channel::<()>(0);

  let store_a = Arc::clone(&store);
  let key_a = key.clone();
  let writer = thread::spawn(move || {
    let sess = store_a.new_session().unwrap();
    let batch = sess.enter_batch();
    let window = batch.try_rmw_window(&key_a).expect("A 取本键窗口");
    let old = batch_value(&batch, &key_a);
    held_tx.send(()).expect("A 宣告持窗未写回");
    refused_rx.recv().expect("B 已确认交叠被拒");
    let next = (from_utf8(&old).unwrap().parse::<i64>().unwrap() + 1).to_string();
    window_write(&window, next.as_bytes());
    drop(window);
    let _ = done_tx.send(());
  });

  let store_b = Arc::clone(&store);
  let key_b = key.clone();
  let reader = thread::spawn(move || {
    held_rx.recv().expect("A 已持窗");
    let sess = store_b.new_session().unwrap();
    let batch = sess.enter_batch();
    assert!(
      batch.try_rmw_window(&key_b).is_none(),
      "A 持窗且尚未写回期间，B 不得进入同键读改写（串读即读到将被覆盖的旧值）"
    );
    refused_tx.send(()).expect("B 宣告交叠被拒");
    done_rx.recv().expect("A 已写回放闩");
    let window = batch.try_rmw_window(&key_b).expect("A 放闩后 B 取窗");
    let old = batch_value(&batch, &key_b);
    assert_eq!(
      old,
      b"1".to_vec(),
      "B 必须读到 A 已回执的新值（丢更新即读回 0）"
    );
    let next = (from_utf8(&old).unwrap().parse::<i64>().unwrap() + 1).to_string();
    window_write(&window, next.as_bytes());
    drop(window);
    batch_value(&batch, &key_b)
  });

  writer.join().expect("A 线程无 panic");
  assert_eq!(
    reader.join().expect("B 线程无 panic"),
    b"2".to_vec(),
    "两任务交叠的同键读改写必须串行累加到终值 2"
  );
}

/// 冷区并发 SETRANGE：互不重叠区段各自回执后必须全部存活在终值里
///（收口前六臂各以同一份冷旧值为底互覆，终值只剩最后写者的区段）
#[test]
fn cold_slow_setrange_keeps_every_acked_range() {
  const N: usize = 4;
  let (store, _dir) = open_store("rmw-cold-sr.db");
  let rt = Runtime::new().unwrap();
  let key: &[u8] = b"rmw:cold:setrange";
  let base = vec![b'.'; 32];
  let ranges: Vec<(usize, Vec<u8>)> = (0..N)
    .map(|i| (i * 4, format!("s{i:03}").into_bytes()))
    .collect();
  let replies = cold_slow_fanout(
    &store,
    &rt,
    key,
    &base,
    ranges
      .iter()
      .map(|(offset, payload)| {
        (
          RespCommand::Setrange,
          vec![
            key.to_vec(),
            offset.to_string().into_bytes(),
            payload.clone(),
          ],
        )
      })
      .collect(),
  );
  let acked: Vec<(usize, Vec<u8>)> = ranges
    .into_iter()
    .zip(replies)
    .filter(|(_, reply)| reply == b":32\r\n")
    .map(|(range, _)| range)
    .collect();
  assert!(
    acked.len() >= 2,
    "至少两路 SETRANGE 须回执，否则用例无覆盖（回执 {}/{N}）",
    acked.len()
  );
  let final_value = get_value(&store, key);
  for (offset, payload) in &acked {
    assert_eq!(
      &final_value[*offset..*offset + payload.len()],
      payload.as_slice(),
      "已回执区段在终值中被并发冷读改写覆写：偏移 {offset} 期望 {} 实际 {}",
      String::from_utf8_lossy(payload),
      String::from_utf8_lossy(&final_value[*offset..*offset + payload.len()])
    );
  }
}

/// 冷区并发 APPEND：终值长度必须等于基值长 + 已回执段总长（收口前各写者以同一
/// 冷基值覆写，终值只多一段）
#[test]
fn cold_slow_append_keeps_every_acked_segment() {
  const N: usize = 4;
  let (store, _dir) = open_store("rmw-cold-ap.db");
  let rt = Runtime::new().unwrap();
  let key: &[u8] = b"rmw:cold:append";
  let base = vec![b'b'; 32];
  let segments: Vec<Vec<u8>> = (0..N).map(|i| format!("+{i:04}+").into_bytes()).collect();
  let replies = cold_slow_fanout(
    &store,
    &rt,
    key,
    &base,
    segments
      .iter()
      .map(|seg| (RespCommand::Append, vec![key.to_vec(), seg.clone()]))
      .collect(),
  );
  let acked: Vec<Vec<u8>> = segments
    .into_iter()
    .zip(replies)
    .filter(|(_, reply)| {
      // 追加回执为新值长度，串行序不可预知，故只认「正整数回执」
      reply_int(reply).is_some_and(|len| len > base.len() as i64)
    })
    .map(|(seg, _)| seg)
    .collect();
  assert!(
    acked.len() >= 2,
    "至少两路 APPEND 须回执，否则用例无覆盖（回执 {}/{N}）",
    acked.len()
  );
  let final_value = get_value(&store, key);
  let expected_len = base.len() + acked.iter().map(Vec::len).sum::<usize>();
  assert_eq!(
    final_value.len(),
    expected_len,
    "并发冷 APPEND 丢段：终值长 {} ≠ 基值长 + 已回执段长 {expected_len}（回执 {} 段）",
    final_value.len(),
    acked.len()
  );
  for seg in &acked {
    assert!(
      final_value.windows(seg.len()).any(|w| w == seg.as_slice()),
      "已回执追加段 {} 在终值中丢失",
      String::from_utf8_lossy(seg)
    );
  }
}

/// 冷区并发 SETBIT：已回执置位的比特位在终值中全部存活（收口前以同一冷基值
/// 互覆，终值只剩一路的位）
#[test]
fn cold_slow_setbit_keeps_every_acked_bit() {
  const N: usize = 4;
  let (store, _dir) = open_store("rmw-cold-setbit.db");
  let rt = Runtime::new().unwrap();
  let key: &[u8] = b"rmw:cold:setbit";
  let base = vec![0_u8; 8];
  // 逐路一个字节边界上的低位，互不重叠且回执必为旧值 0
  let offsets: Vec<i64> = (0..N).map(|i| (i * 8) as i64).collect();
  let replies = cold_slow_fanout(
    &store,
    &rt,
    key,
    &base,
    offsets
      .iter()
      .map(|offset| {
        (
          RespCommand::Setbit,
          vec![key.to_vec(), offset.to_string().into_bytes(), b"1".to_vec()],
        )
      })
      .collect(),
  );
  let acked: Vec<i64> = offsets
    .into_iter()
    .zip(replies)
    .filter(|(_, reply)| reply == b":0\r\n")
    .map(|(offset, _)| offset)
    .collect();
  assert!(
    acked.len() >= 2,
    "至少两路 SETBIT 须回执，否则用例无覆盖（回执 {}/{N}）",
    acked.len()
  );
  let final_value = get_value(&store, key);
  for offset in &acked {
    let byte = final_value[(*offset / 8) as usize];
    assert_ne!(
      byte & 0x80,
      0,
      "已回执置位的偏移 {offset} 在终值中被并发冷读改写抹掉（该字节 {byte:#010b}）"
    );
  }
}

/// 冷区并发 BITFIELD 写子命令：已回执的每个位域写入都留在终值里
///（`window` 只在 BITFIELD 臂取、BITFIELD_RO 不取窗的本票接线判据）
#[test]
fn cold_slow_bitfield_keeps_every_acked_write() {
  const N: usize = 4;
  let (store, _dir) = open_store("rmw-cold-bf.db");
  let rt = Runtime::new().unwrap();
  let key: &[u8] = b"rmw:cold:bitfield";
  let base = vec![0_u8; 8];
  let replies = cold_slow_fanout(
    &store,
    &rt,
    key,
    &base,
    (0..N)
      .map(|i| {
        (
          RespCommand::Bitfield,
          vec![
            key.to_vec(),
            b"SET".to_vec(),
            b"u8".to_vec(),
            (i * 8).to_string().into_bytes(),
            "77".as_bytes().to_vec(),
          ],
        )
      })
      .collect(),
  );
  let acked: Vec<usize> = (0..N)
    .zip(replies)
    .filter(|(_, reply)| reply_array_int(reply) == Some(0))
    .map(|(i, _)| i)
    .collect();
  assert!(
    acked.len() >= 2,
    "至少两路 BITFIELD SET 须回执，否则用例无覆盖（回执 {}/{N}）",
    acked.len()
  );
  let final_value = get_value(&store, key);
  for i in &acked {
    assert_eq!(
      final_value[*i], 77,
      "已回执位域写入在终值中被并发冷读改写覆写（字节 {i}）"
    );
  }
}

/// 冷区并发 INCR：已回执自增值必须两两互异且终值等于回执数
///（同窗内读到同一旧值即串读，写回互覆即丢更新）
#[test]
fn cold_slow_incr_acks_distinct_values_and_final_matches_ack_count() {
  const N: usize = 4;
  let (store, _dir) = open_store("rmw-cold-incr.db");
  let rt = Runtime::new().unwrap();
  let key: &[u8] = b"rmw:cold:incr";
  let replies = cold_slow_fanout(
    &store,
    &rt,
    key,
    b"0",
    (0..N)
      .map(|_| (RespCommand::Incr, vec![key.to_vec()]))
      .collect(),
  );
  let acked: Vec<i64> = replies
    .into_iter()
    .filter_map(|reply| reply_int(&reply))
    .collect();
  assert!(
    acked.len() >= 2,
    "至少两路 INCR 须回执，否则用例无覆盖（回执 {}/{N}）",
    acked.len()
  );
  let mut distinct = acked.clone();
  distinct.sort_unstable();
  distinct.dedup();
  assert_eq!(
    distinct.len(),
    acked.len(),
    "已回执自增值出现重复（{acked:?}）：同键冷读改写窗口被串读"
  );
  let final_value: i64 = String::from_utf8(get_value(&store, key))
    .unwrap()
    .parse()
    .expect("INCR 终值必须是十进制整数");
  assert_eq!(
    final_value,
    acked.len() as i64,
    "并发冷 INCR 终值 ≠ 已回执自增数（已回执 {acked:?}）"
  );
}
