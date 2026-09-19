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

use std::{sync::Arc, thread};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
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
  std::str::from_utf8(body).ok()?.parse().ok()
}

/// `$N\r\n<payload>\r\n` 批量回执解析
fn reply_bulk(resp: &[u8]) -> Option<Vec<u8>> {
  let rest = resp.strip_prefix(b"$")?;
  // 手工定位首个 CRLF（`slice::split_once` 在稳定版仍是 unstable feature）
  let end = rest
    .windows(2)
    .position(|w| w == b"\r\n")
    .filter(|&i| i + 2 <= rest.len())?;
  let len: usize = std::str::from_utf8(&rest[..end]).ok()?.parse().ok()?;
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
