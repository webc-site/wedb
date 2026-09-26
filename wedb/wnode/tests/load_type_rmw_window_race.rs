//! 装载型写命令族 RMW 窗 + 落笔复验并发回归（票 wnode-load-type-write-bypass-rmw-window-recheck）
//!
//! 缺陷形：LTRIM/LPUSHX/SMOVE/SPOP/LMOVE 等装载型写臂裸装载 + 裸写回，不取
//! [`wkv::RmwWindow`] 用户键桶排他闩、写回前不做 obj_save_recheck 域归属复验，
//! 与 run_sync_rmw/run_async_rmw 骨架保护分叉：同键并发交错可丢已 ACK 写
//!（LTRIM 旧快照顶掉窗内 RPUSH 元素）/ 复活已删键 / 造双域并存。
//!
//! 修法（本回归锁定的机制判据）：
//! - 异步臂装载前 `rmw_window` 让核取窗跨「装载 → 求值 → 写回」全程
//!   （envelope_count_correct_race 同款可观测判据：同键第二窗取闩失败 = 臂在手）；
//! - 落笔前 obj_writeback_recheck_async 按装载态复验域归属，对面窗内
//!   DEL/SET（取物理记录键桶闩、不占本窗）交叠即拒写按存储忙
//!   （RESP_ERR_SLOW_PATH_STORAGE）交回重试，绝不盲写；
//! - 双键臂（LMOVE）按字典序双窗获取：阻塞于 min 键窗时 max 键窗必未被触碰。
//!
//! 交叠构造为确定性 poll_fn 注入（判据成立才放行对面命令，不赌调度器）：
//! 对面 RPUSH 在 victim 持窗期内至多推进两轮且必被挡（严禁耗尽让核预算），
//! victim 闭环后才补齐；对面 DEL/SET 取独立桶闩可在窗内完整 ACK。

use std::{future::poll_fn, pin::pin, sync::Arc, task::Poll};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::{feed, roundtrip};
use wtest_base::open_test_store;
use wval::KeyTag;

type TestStore = WedbStore<SegmentedDevice>;

/// 独立连接装配（生产 thread-per-core 形态：对面写者与 victim 臂各持一份会话）
fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 异步域单命令往返（注入臂专用：可 poll 的 future，绝不再起嵌套 block_on）
async fn deliver(store: Arc<TestStore>, args: Vec<Vec<u8>>) -> Vec<u8> {
  let slices: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
  let mut c = consumer_on(&store);
  let mut out = feed(&mut c, &slices);
  if let Some(slow) = c.take_slow_wait() {
    out.extend_from_slice(&slow.resolve().await);
  }
  out
}

/// 窗口在手（装载型臂已取窗）的可观测判据：同键第二窗取闩失败
///（对面 DEL/SET 按物理记录键取闩、根本不取本窗，判据只反映 victim 臂持窗事实）
fn rmw_window_held(store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("判据会话");
  let batch = sess.enter_batch();
  batch.try_rmw_window(key).is_none()
}

/// 信封物理记录是否在场（双域并存 / 键复活判据）
fn envelope_record_present(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("取证会话");
  let rec_k = sess.session_tag_key(KeyTag::ObjectEnvelope, key);
  rt.block_on(sess.read_raw(&rec_k))
    .expect("信封物理记录读取不得报存储错误")
    .is_some()
}

/// 批量 bulk 数组帧（LRANGE 精确对照）
fn bulk_array(items: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", items.len()).into_bytes();
  for item in items {
    out.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
    out.extend_from_slice(item);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// victim 慢路径臂挂起 → 持窗判据成立 → 对面命令注入 → victim 闭环 →
/// 对面补齐。`block_intruder` = true 时对面为同窗 RMW 命令（RPUSH 族），
/// 断言其在 victim 持窗期内必被挡（至多两轮 poll，不耗让核预算）；
/// false 时对面为 DEL/SET（独立桶闩），必须在 victim 闭环前完整 ACK。
/// 回 `(交叠是否成立, victim 应答帧, 对面应答帧)`
fn drive_interleaved(
  rt: &Runtime,
  store: &Arc<TestStore>,
  victim_args: &[&[u8]],
  victim_key: &[u8],
  intruder: Vec<Vec<u8>>,
  block_intruder: bool,
) -> (bool, Vec<u8>, Vec<u8>) {
  let mut c = consumer_on(store);
  let sync_out = feed(&mut c, victim_args);
  assert!(
    sync_out.is_empty(),
    "冷键 victim 应挂慢路径，实际同步段直出 {:?}",
    String::from_utf8_lossy(&sync_out)
  );
  let slow = c.take_slow_wait().expect("冷键装载必挂慢路径");
  let mut rmw = pin!(slow.resolve());
  let inject_store = Arc::clone(store);
  let mut inject = pin!(deliver(inject_store, intruder));
  let probe_store = Arc::clone(store);
  let probe_key = victim_key.to_vec();
  let mut held = false;
  let mut blocked_polls = 0usize;
  let mut inject_out: Option<Vec<u8>> = None;
  let mut victim_early: Option<Vec<u8>> = None;
  let victim_reply = rt.block_on(poll_fn(|cx| {
    loop {
      if !held {
        // 交叠判据未成立的轮次只推 victim：判据成立前对面命令绝不放行
        match rmw.as_mut().poll(cx) {
          Poll::Ready(out) => {
            victim_early = Some(out);
            return Poll::Ready(Vec::new());
          }
          Poll::Pending => {
            if rmw_window_held(&probe_store, &probe_key) {
              held = true;
            } else {
              return Poll::Pending;
            }
          }
        }
      }
      if inject_out.is_none() {
        if block_intruder {
          if blocked_polls < 2 {
            blocked_polls += 1;
            assert!(
              matches!(inject.as_mut().poll(cx), Poll::Pending),
              "对面 RMW 命令竟在 victim 持窗期内落地：窗口串行化失效"
            );
            assert!(
              rmw_window_held(&probe_store, &probe_key),
              "victim 持窗中断：窗口应在装载-写回全程在手"
            );
            continue;
          }
          // victim 闭环后再补齐对面（让核等待臂对位，不与其抢预算）
          return match rmw.as_mut().poll(cx) {
            Poll::Ready(out) => Poll::Ready(out),
            Poll::Pending => Poll::Pending,
          };
        }
        // DEL/SET 形：取独立桶闩，victim 持窗期内必须完整 ACK
        match inject.as_mut().poll(cx) {
          Poll::Ready(out) => inject_out = Some(out),
          Poll::Pending => return Poll::Pending,
        }
      }
      return match rmw.as_mut().poll(cx) {
        Poll::Ready(out) => Poll::Ready(out),
        Poll::Pending => Poll::Pending,
      };
    }
  }));
  let victim_reply = match victim_early {
    Some(out) => {
      // victim 在判据成立前即闭环：交叠不成立，应答作废由调用方炸出
      let _ = victim_reply;
      out
    }
    None => victim_reply,
  };
  let intruder_reply = match inject_out {
    Some(out) => out,
    None => rt.block_on(inject.as_mut()),
  };
  (held, victim_reply, intruder_reply)
}

/// 冷键 LTRIM 持窗 + 对面 RPUSH 被窗挡：修复机制（同锁源串行化）直判——
/// 修复前 LTRIM 臂根本不取窗（判据永不成立即炸出），其旧快照裸写回可顶掉
/// 窗内已 ACK 的 RPUSH 元素；修复后终态元素零丢失
#[test]
fn ltrim_cold_window_serializes_concurrent_rpush_no_lost_ack() {
  let (_dir, store) = open_test_store("lt-rmw-ltrim-rpush.db").unwrap();
  let rt = Runtime::new().unwrap();
  let key = b"lt:rpush:race".to_vec();
  let seed: Vec<Vec<u8>> = (1..=10).map(|i| format!("e{i:02}").into_bytes()).collect();
  {
    let mut c = consumer_on(&store);
    let mut cmd: Vec<&[u8]> = vec![b"RPUSH", &key];
    cmd.extend(seed.iter().map(Vec::as_slice));
    assert_eq!(roundtrip(&rt, &mut c, &cmd), b":10\r\n");
  }
  // 冷化：LTRIM 同步臂磁盘候选降级 → 慢路径装载臂（取窗跨异步磁盘读）
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (interleaved, ltrim_reply, rpush_reply) = drive_interleaved(
    &rt,
    &store,
    &[b"LTRIM", &key, b"0", b"-1"],
    &key,
    vec![b"RPUSH".to_vec(), key.clone(), b"e11".to_vec()],
    true,
  );
  assert!(
    interleaved,
    "交叠判据未成立：LTRIM 臂未在装载期持窗（修复前裸执行形态，用例失效须炸出）"
  );
  assert_eq!(ltrim_reply, b"+OK\r\n");
  assert_eq!(
    rpush_reply,
    b":11\r\n",
    "对面 RPUSH 应在 LTRIM 闭环后重放 ACK：{:?}",
    String::from_utf8_lossy(&rpush_reply)
  );

  // 终态不变式：已 ACK 元素零丢失 + 无覆盖/复活
  let mut c = consumer_on(&store);
  let mut want: Vec<&[u8]> = seed.iter().map(Vec::as_slice).collect();
  want.push(b"e11");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LRANGE", &key, b"0", b"-1"]),
    bulk_array(&want),
    "LTRIM 旧快照顶掉了窗内已 ACK 的 RPUSH 元素"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"LLEN", &key]), b":11\r\n");
}

/// 冷键 LTRIM 持窗 + 窗内真 DEL：装载已见既存态则落笔复验必拒写（存储忙），
/// 装载已见删除态则按缺失短路 +OK 零写回——两形皆合法，盲写复活即炸出
#[test]
fn ltrim_cold_window_concurrent_del_is_never_resurrected() {
  let (_dir, store) = open_test_store("lt-rmw-ltrim-del.db").unwrap();
  let rt = Runtime::new().unwrap();
  let key = b"lt:del:race".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"RPUSH", &key, b"a", b"b", b"c"]),
      b":3\r\n"
    );
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (interleaved, ltrim_reply, del_reply) = drive_interleaved(
    &rt,
    &store,
    &[b"LTRIM", &key, b"0", b"-1"],
    &key,
    vec![b"DEL".to_vec(), key.clone()],
    true,
  );
  assert!(interleaved, "交叠判据未成立：注入 DEL 没落进 LTRIM 窗口");
  assert_eq!(del_reply, b":1\r\n", "对面 DEL 须在 LTRIM 闭环后完整回执");
  assert_eq!(ltrim_reply, b"+OK\r\n", "LTRIM 应答走样");

  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"EXISTS", &key]),
    b":0\r\n",
    "已 ACK 删除的键被 LTRIM 旧快照盲写复活"
  );
  assert!(
    !envelope_record_present(&rt, &store, &key),
    "已 ACK 删除的信封物理记录被 LTRIM 尾段回写"
  );
}

/// 冷键 SPOP 持窗 + 窗内真 SET：同键绝不容两物理域并存（双域判据），
/// SET 同入 RMW 窗互斥，持窗期内被挡，SPOP 闭环后 SET 覆写落地
#[test]
fn spop_cold_window_concurrent_set_keeps_single_domain() {
  let (_dir, store) = open_test_store("lt-rmw-spop-set.db").unwrap();
  let rt = Runtime::new().unwrap();
  let key = b"lt:spop:set:race".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(roundtrip(&rt, &mut c, &[b"SADD", &key, b"m1"]), b":1\r\n");
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (interleaved, spop_reply, set_reply) = drive_interleaved(
    &rt,
    &store,
    &[b"SPOP", &key],
    &key,
    vec![b"SET".to_vec(), key.clone(), b"hello".to_vec()],
    true,
  );
  assert!(interleaved, "交叠判据未成立：注入 SET 没落进 SPOP 窗口");
  assert_eq!(set_reply, b"+OK\r\n", "对面 SET 须在 SPOP 闭环后完成覆写");
  assert_eq!(spop_reply, b"$2\r\nm1\r\n", "SPOP 闭环应答走样");

  assert!(
    !envelope_record_present(&rt, &store, &key),
    "SET 已清退的信封记录被 SPOP 尾段回写：同键双物理域并存"
  );
  let mut c = consumer_on(&store);
  assert_eq!(roundtrip(&rt, &mut c, &[b"TYPE", &key]), b"+string\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GET", &key]),
    b"$5\r\nhello\r\n",
    "TYPE 与 GET 结论发散（双域并存的必然后果）"
  );
}

/// LMOVE 双键臂：同步臂 min 键窗被占即整体让位（Ok(false) 异步重放，修复前
/// 裸执行直出移动应答即炸出）；异步臂按字典序取窗——阻塞于 min 键窗时
/// max 键窗必未被触碰（杜绝锁序环等待的可观测判据），放行后双窗齐备完成移动
#[test]
fn lmove_warm_holder_on_lex_min_replays_and_acquires_lexicographic_pair() {
  let (_dir, store) = open_test_store("lt-rmw-lmove-pair.db").unwrap();
  let rt = Runtime::new().unwrap();
  let src = b"lt:lmove:src".to_vec();
  let dst = b"lt:lmove:dst".to_vec();
  let min = if src < dst { &src } else { &dst };
  let max = if src < dst { &dst } else { &src };
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"RPUSH", &src, b"e0", b"e1"]),
      b":2\r\n"
    );
  }

  // 占窗会话：持 min 键 RMW 窗
  let hold_sess = store.new_session().expect("占窗会话");
  let hold_batch = hold_sess.enter_batch();
  let mut holder = Some(
    hold_batch
      .try_rmw_window(min)
      .expect("占窗会话取 min 键窗必成"),
  );

  let mut c = consumer_on(&store);
  let sync_out = feed(&mut c, &[b"LMOVE", &src, &dst, b"LEFT", b"RIGHT"]);
  assert!(
    sync_out.is_empty(),
    "min 键窗被占时 LMOVE 同步臂必须让位异步重放（修复前裸执行直出移动应答）：{:?}",
    String::from_utf8_lossy(&sync_out)
  );
  let slow = c.take_slow_wait().expect("LMOVE 让位后必挂慢路径");
  let mut rmw = pin!(slow.resolve());
  let probe_store = Arc::clone(&store);
  let min_key = min.clone();
  let max_key = max.clone();
  let mut released = false;
  let reply = rt.block_on(poll_fn(|cx| match rmw.as_mut().poll(cx) {
    Poll::Ready(out) => Poll::Ready(out),
    Poll::Pending => {
      if !released
        && rmw_window_held(&probe_store, &min_key)
        && !rmw_window_held(&probe_store, &max_key)
      {
        // 字典序先行已证：臂阻塞于 min 窗且 max 窗未被触碰，放行使齐窗
        drop(holder.take());
        released = true;
        cx.waker().wake_by_ref();
      }
      Poll::Pending
    }
  }));
  assert!(
    released,
    "LMOVE 异步臂未呈「阻塞 min 窗且 max 窗在手外」形态（双窗字典序获取失效）"
  );
  assert_eq!(reply, b"$2\r\ne0\r\n");

  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LRANGE", &src, b"0", b"-1"]),
    b"*1\r\n$2\r\ne1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LRANGE", &dst, b"0", b"-1"]),
    b"*1\r\n$2\r\ne0\r\n"
  );
}

/// 无竞争回归（单线程逐字节）：并窗+复验后全家族应答帧与修复前基态逐字节
/// 一致（含冷键 LTRIM 慢路径臂），复验绝不沦为「一律弃写」开关
#[test]
fn uncontended_load_type_family_reply_bytes_unchanged() {
  let (_dir, store) = open_test_store("lt-rmw-uncontended.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  // 列表族（同步臂）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LPUSHX", b"uc:lx", b"e"]),
    b":0\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RPUSH", b"uc:l", b"a", b"b", b"c"]),
    b":3\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LPUSHX", b"uc:l", b"x"]),
    b":4\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RPUSHX", b"uc:l", b"y"]),
    b":5\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LTRIM", b"uc:l", b"1", b"-1"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LSET", b"uc:l", b"0", b"z"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LINSERT", b"uc:l", b"BEFORE", b"b", b"b2"]),
    b":5\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LINSERT", b"uc:l", b"BEFORE", b"q", b"q"]),
    b":-1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LREM", b"uc:l", b"1", b"b2"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"LLEN", b"uc:l"]), b":4\r\n");
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"LMOVE", b"uc:l", b"uc:ldst", b"LEFT", b"RIGHT"]
    ),
    b"$1\r\nz\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LRANGE", b"uc:ldst", b"0", b"-1"]),
    b"*1\r\n$1\r\nz\r\n"
  );

  // 集合族
  assert_eq!(roundtrip(&rt, &mut c, &[b"SADD", b"uc:s", b"m"]), b":1\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"SPOP", b"uc:s"]), b"$1\r\nm\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"SPOP", b"uc:s"]), b"$-1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"uc:sf", b"m"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"uc:st", b"k"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SMOVE", b"uc:sf", b"uc:st", b"m"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"uc:src", b"m1", b"m2"]),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SINTERSTORE", b"uc:dst", b"uc:src"]),
    b":2\r\n"
  );

  // GEO 族
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"GEOADD", b"uc:g", b"15.08", b"37.30", b"catania"]
    ),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[
        b"GEOSEARCHSTORE",
        b"uc:gd",
        b"uc:g",
        b"FROMLONLAT",
        b"15.08",
        b"37.30",
        b"BYRADIUS",
        b"200",
        b"km",
        b"ASC",
      ]
    ),
    b":1\r\n"
  );

  // 阻塞族立即可取臂与 LMPOP
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RPUSH", b"uc:q", b"q1"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"BLPOP", b"uc:q", b"0"]),
    b"*2\r\n$4\r\nuc:q\r\n$2\r\nq1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RPUSH", b"uc:q2", b"w1"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LMPOP", b"1", b"uc:q2", b"LEFT"]),
    b"*2\r\n$5\r\nuc:q2\r\n*1\r\n$2\r\nw1\r\n"
  );

  // 冷键慢路径臂（异步装载 + 落笔复验放行）：LTRIM 应答与终态逐字节不变
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RPUSH", b"uc:cold", b"a", b"b", b"c"]),
    b":3\r\n"
  );
  drop(c);
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LTRIM", b"uc:cold", b"1", b"-1"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LRANGE", b"uc:cold", b"0", b"-1"]),
    b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
  );
  // 冷键 LPUSHX（存在臂）与写后热路径延续
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LPUSHX", b"uc:cold", b"x"]),
    b":3\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LRANGE", b"uc:cold", b"0", b"-1"]),
    b"*3\r\n$1\r\nx\r\n$1\r\nb\r\n$1\r\nc\r\n"
  );
}
