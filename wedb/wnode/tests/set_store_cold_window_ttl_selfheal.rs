//! set 冷态 STORE 臂 persist/清退出窗回归（票 wcol-set-cold-store-persist-in-window-self-lock）
//!
//! 缺陷形态与 zset 父票 71a9c4ce 完全同源：`set_commands/slow.rs::combine_store_cold`
//! 曾在持有目标键 rmw 窗期间调用 `persist_key`（TTL 清退）与 `retire_tiered_dest`
//! （分层残留清退）——wkv 二者均按**用户键**取索引层非重入独占桶闩，与 rmw 窗同
//! 一把闩：冷态 SUNIONSTORE/INTERSTORE/DIFFSTORE 非空结果实测必自锁
//! `IndexError::LockTimeout`，回 `-ERR slow path storage error`。
//!
//! 修法落点（本回归锁定的判据；自票
//! wnode-store-cold-window-ttl-clear-outsides-critical-section 起 TTL 清退再挪进
//! 持窗写临界区随写落笔——`obj_save_clear_ttl`，见 `store_dest_cold_common`，
//! 窗外裸清的「清退与写回之间落 TTL」次生交错随之在机制上消除）：
//! - `retire_tiered_dest` 恒在 `drop(window)` 窗释放后；
//! - 窗仍跨「存活域快照 → 落笔复验 → 信封写回·随写清 TTL/删空回收」全程，
//!   覆写保护不回退（持窗判据 = 同键第二窗取闩失败；修复前 persist 在窗内即炸
//!   storage error，修复后若误摘窗则注入段判据不成立即炸出——反向注入：临时
//!   还原旧序本文件各 ttl 用例应答即回退为 busy 帧）。
//!
//! 交叠构造沿用 zset_load_type_rmw_window_race 的确定性 poll_fn 注入风格，无 sleep。
//!
//! 自指并发夹具（票 wnode-set-store-selfref-load-outside-window，§87 同式第四缝，
//! 形制沿 geo_store_cold_window_ttl_selfheal）：S*STORE dst ∈ srcs 的「源装载完成
//! → 开窗」间隙注入对面 SADD 完整落地并确认，锁死装载型写臂先窗后装纪律——
//! 取窗点位回退至装载之后即炸出（首让出点持窗判据 + 间隙注入序自洽判据）。

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

/// 窗口在手（STORE 覆写臂已取窗）的可观测判据：同键第二窗取闩失败
fn rmw_window_held(store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("判据会话");
  let batch = sess.enter_batch();
  batch.try_rmw_window(key).is_none()
}

/// victim 慢路径臂挂起 → 持窗判据成立 → 对面同窗 RMW 命令注入（必被挡）→
/// victim 闭环 → 对面补齐。回 `(交叠是否成立, victim 应答帧, 对面应答帧)`
fn drive_interleaved(
  rt: &Runtime,
  store: &Arc<TestStore>,
  victim_args: &[&[u8]],
  victim_key: &[u8],
  intruder: Vec<Vec<u8>>,
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
  let inject_out: Option<Vec<u8>> = None;
  let mut victim_early: Option<Vec<u8>> = None;
  let victim_reply = rt.block_on(poll_fn(|cx| {
    loop {
      if !held {
        // 交叠判据未成立的轮次只推 victim：判据成立前对面命令绝不放行；
        // 未成立即让出（禁忙等活锁）——先窗后装修正后（票
        // wnode-set-store-selfref-load-outside-window）STORE 臂开窗点在源装载
        // 之前，victim 首个让出点窗即在手
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
        // 对面 SADD 为同窗 RMW 命令：victim 持窗期内必被挡（让核等待）
        if blocked_polls < 2 {
          blocked_polls += 1;
          assert!(
            matches!(inject.as_mut().poll(cx), Poll::Pending),
            "对面 SADD 竟在 victim 持窗期内落地：覆写窗串行化失效"
          );
          assert!(
            rmw_window_held(&probe_store, &probe_key),
            "victim 持窗中断：窗口应在域快照-落笔全程在手"
          );
          continue;
        }
        return match rmw.as_mut().poll(cx) {
          Poll::Ready(out) => Poll::Ready(out),
          Poll::Pending => Poll::Pending,
        };
      }
      return match rmw.as_mut().poll(cx) {
        Poll::Ready(out) => Poll::Ready(out),
        Poll::Pending => Poll::Pending,
      };
    }
  }));
  let victim_reply = match victim_early {
    Some(out) => {
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

/// 三族公共场景数据：src1={a,b}、src2={b,c}、dst={x,y}（prefix 区分 TTL/无 TTL
/// 双生子组与命令组），dst 依 seed_ttl 决定是否挂 600s TTL；返回逐命令应答流
fn seed_group(
  rt: &Runtime,
  c: &mut RespSessionConsumer,
  prefix: &str,
  seed_ttl: bool,
) -> Vec<Vec<u8>> {
  let mut replies = Vec::new();
  let k = |name: String| k(name.as_str(), prefix);
  replies.push(roundtrip(rt, c, &[b"SADD", &k("src1".into()), b"a", b"b"]));
  replies.push(roundtrip(rt, c, &[b"SADD", &k("src2".into()), b"b", b"c"]));
  replies.push(roundtrip(rt, c, &[b"SADD", &k("dst".into()), b"x", b"y"]));
  if seed_ttl {
    replies.push(roundtrip(rt, c, &[b"EXPIRE", &k("dst".into()), b"600"]));
  }
  replies
}

fn k(name: &str, prefix: &str) -> Vec<u8> {
  format!("{prefix}:{name}").into_bytes()
}

/// 冷态 *STORE 目标键带 TTL 不再生：TTL 在持窗写临界区内随写清退，应答与无 TTL
/// 基线逐字节一致且 TTL 已清；修复前 persist 落窗内同桶自锁，非空结果臂应答回退为
/// `-ERR slow path storage error`（反向注入判据）
fn cold_store_cleared_ttl_matches_no_ttl_baseline(
  store_cmd: &[u8],
  expect: &[u8],
  tag: &str,
  prefix_t: &str,
  prefix_n: &str,
) {
  let (_dir, store) = open_test_store(tag).unwrap();
  let rt = Runtime::new().unwrap();
  {
    let mut c = consumer_on(&store);
    // TTL 组与无 TTL 基线组同构种子（仅 EXPIRE 之差）
    let seed_t = seed_group(&rt, &mut c, prefix_t, true);
    let seed_n = seed_group(&rt, &mut c, prefix_n, false);
    for r in seed_t.iter().chain(seed_n.iter()) {
      assert!(
        r.as_slice() == &b":2\r\n"[..] || r.as_slice() == &b":1\r\n"[..],
        "种子帧异常（SADD :2 / EXPIRE :1 之外）：{:?}",
        String::from_utf8_lossy(r)
      );
    }
    // 冷化：STORE 臂与目标键全部落盘
    rt.block_on(store.flush_and_evict_all()).unwrap();
    // 前置件：TTL 旁路随键冷化在场（丢失则用例失效，炸出）
    let dst_t = k("dst", prefix_t);
    let pttl = roundtrip(&rt, &mut c, &[b"PTTL", &dst_t]);
    let ttl_ms = String::from_utf8_lossy(&pttl)
      .trim_start_matches(':')
      .trim_end_matches("\r\n")
      .parse::<i64>()
      .unwrap_or(-1);
    assert!(
      pttl.starts_with(b":") && ttl_ms > 0,
      "冷化后 TTL 旁路应仍在场（否则前置不成立，用例失效须炸出）：{:?}",
      String::from_utf8_lossy(&pttl)
    );
    // 冷态 STORE：带 TTL 目标键应答必须与无 TTL 基线逐字节一致
    let dst_n = k("dst", prefix_n);
    let src1_t = k("src1", prefix_t);
    let src2_t = k("src2", prefix_t);
    let src1_n = k("src1", prefix_n);
    let src2_n = k("src2", prefix_n);
    let reply_t = roundtrip(&rt, &mut c, &[store_cmd, &dst_t, &src1_t, &src2_t]);
    let reply_n = roundtrip(&rt, &mut c, &[store_cmd, &dst_n, &src1_n, &src2_n]);
    assert_eq!(
      reply_t,
      reply_n,
      "带 TTL 目标键冷态 {} 应答与无 TTL 基线不一致（修复前此处为 storage error 自锁红）：\
       ttl={:?} baseline={:?}",
      String::from_utf8_lossy(store_cmd),
      String::from_utf8_lossy(&reply_t),
      String::from_utf8_lossy(&reply_n)
    );
    assert_eq!(
      reply_t,
      expect,
      "冷态 {} 应答应为基数整数值帧",
      String::from_utf8_lossy(store_cmd)
    );
    // TTL 已清（SET 语义持窗随写清退）且成员覆写落定、与基线组逐字节一致
    assert_eq!(roundtrip(&rt, &mut c, &[b"PTTL", &dst_t]), b":-1\r\n");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SMEMBERS", &dst_t]),
      roundtrip(&rt, &mut c, &[b"SMEMBERS", &dst_n]),
      "STORE 终态成员与无 TTL 基线不一致"
    );
  }
}

#[test]
fn sunionstore_cold_dest_ttl_cleared_no_selflock() {
  cold_store_cleared_ttl_matches_no_ttl_baseline(
    b"SUNIONSTORE",
    b":3\r\n",
    "set-store-un.db",
    "u1",
    "u0",
  );
}

#[test]
fn sinterstore_cold_dest_ttl_cleared_no_selflock() {
  cold_store_cleared_ttl_matches_no_ttl_baseline(
    b"SINTERSTORE",
    b":1\r\n",
    "set-store-in.db",
    "i1",
    "i0",
  );
}

#[test]
fn sdiffstore_cold_dest_ttl_cleared_no_selflock() {
  cold_store_cleared_ttl_matches_no_ttl_baseline(
    b"SDIFFSTORE",
    b":1\r\n",
    "set-store-df.db",
    "d1",
    "d0",
  );
}

/// 冷态 SINTERSTORE 交集为空 + 目标键带 TTL：删空回收臂不受挪序影响，应答与
/// 无 TTL 基线一致且键随删空消亡（TTL 无从残留）
#[test]
fn sinterstore_cold_empty_result_with_ttl_deletes_dest_like_baseline() {
  let (_dir, store) = open_test_store("set-store-empty-ttl.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  // 交集为空的孪生组：{g1a} ∩ {g2b} = ∅
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"e:t:a", b"p"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"e:t:b", b"q"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"e:t:dst", b"x"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"EXPIRE", b"e:t:dst", b"600"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"e:n:a", b"p"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"e:n:b", b"q"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SADD", b"e:n:dst", b"x"]),
    b":1\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let reply_t = roundtrip(
    &rt,
    &mut c,
    &[b"SINTERSTORE", b"e:t:dst", b"e:t:a", b"e:t:b"],
  );
  let reply_n = roundtrip(
    &rt,
    &mut c,
    &[b"SINTERSTORE", b"e:n:dst", b"e:n:a", b"e:n:b"],
  );
  assert_eq!(reply_t, reply_n, "空结果臂带 TTL 应答应与无 TTL 基线一致");
  assert_eq!(reply_t, b":0\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"e:t:dst"]), b":0\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"e:n:dst"]), b":0\r\n");
}

/// 修复不回退覆写窗保护：persist 挪窗前、retire 挪 drop(window) 窗后之后，
/// 冷态 SUNIONSTORE 目标键覆写仍跨「域快照 → 落笔」全程持窗——对面同窗 SADD
/// 在持窗期内必被挡，闭环后重放 ACK，终态已 ACK 增量零丢失
#[test]
fn sunionstore_cold_dest_window_still_serializes_concurrent_sadd() {
  let (_dir, store) = open_test_store("set-store-window-zadd.db").unwrap();
  let rt = Runtime::new().unwrap();
  let dst = b"w:dst".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SADD", b"w:src", b"a", b"b"]),
      b":2\r\n"
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"SADD", &dst, b"x"]), b":1\r\n");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"EXPIRE", &dst, b"600"]),
      b":1\r\n"
    );
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (interleaved, store_reply, sadd_reply) = drive_interleaved(
    &rt,
    &store,
    &[b"SUNIONSTORE", &dst, b"w:src"],
    &dst,
    vec![b"SADD".to_vec(), dst.clone(), b"z".to_vec()],
  );
  assert!(
    interleaved,
    "交叠判据未成立：combine_store_cold 未在目标键落笔期持窗（挪序误摘窗即炸出）"
  );
  assert_eq!(store_reply, b":2\r\n", "SUNIONSTORE 应答应为源集合基数");
  assert_eq!(
    sadd_reply,
    b":1\r\n",
    "对面 SADD 应在 SUNIONSTORE 闭环后重放 ACK：{:?}",
    String::from_utf8_lossy(&sadd_reply)
  );
  let mut c = consumer_on(&store);
  // 终态成员 = 覆写集合 {a,b} + 窗内已 ACK 的 z（去序比对，SetObject 迭代序不作判据）
  let got = roundtrip(&rt, &mut c, &[b"SMEMBERS", &dst]);
  let mut got_members: Vec<Vec<u8>> = got
    .split(|b| *b == b'\r')
    .map(|l| match l.first() {
      Some(b'\n') => &l[1..],
      _ => l,
    })
    .filter(|l| !l.is_empty() && !l.starts_with(b"*") && !l.starts_with(b"$"))
    .map(Vec::from)
    .collect();
  got_members.sort_unstable();
  assert_eq!(
    got_members,
    vec![b"a".to_vec(), b"b".to_vec(), b"z".to_vec()],
    "SUNIONSTORE 旧覆写顶掉了窗内已 ACK 的 SADD 成员（丢已确认写入）：{:?}",
    String::from_utf8_lossy(&got)
  );
  // TTL 已清（持窗临界区内随写清退生效且无自锁）
  assert_eq!(roundtrip(&rt, &mut c, &[b"PTTL", &dst]), b":-1\r\n");
}

/// SMEMBERS 应答的成员表解析（升序归一，去序比对用）
fn smembers_sorted(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8]) -> Vec<Vec<u8>> {
  let mut members = roundtrip(rt, c, &[b"SMEMBERS", key])
    .split(|b| *b == b'\r')
    .map(|l| match l.first() {
      Some(b'\n') => &l[1..],
      _ => l,
    })
    .filter(|l| !l.is_empty() && !l.starts_with(b"*") && !l.starts_with(b"$"))
    .map(Vec::from)
    .collect::<Vec<_>>();
  members.sort_unstable();
  members
}

/// 冷键 victim 慢路径臂驱动至首个让出点（源装载的磁盘读），记录该点 dest 窗是否
/// 在手，随后放行使臂闭环。回 `(首让出点持窗, victim 应答帧)`——修复后取窗先于
/// 装载成立；修复前窗仅罩落笔段，源装载让出点窗必未在手即炸出（形制沿
/// geo_store_cold_window_ttl_selfheal 自指夹具，§87 同假设：rmw_window 无争用
/// 不让渡，确定性 poll_fn 注入，无 sleep）
fn drive_first_suspend_probe(
  rt: &Runtime,
  store: &Arc<TestStore>,
  victim_args: &[&[u8]],
  dest_key: &[u8],
) -> (bool, Vec<u8>) {
  let mut c = consumer_on(store);
  let sync_out = feed(&mut c, victim_args);
  assert!(
    sync_out.is_empty(),
    "冷键 SUNIONSTORE 应挂慢路径，实际同步段直出 {:?}",
    String::from_utf8_lossy(&sync_out)
  );
  let slow = c.take_slow_wait().expect("冷键装载必挂慢路径");
  let mut fut = pin!(slow.resolve());
  let probe_store = Arc::clone(store);
  let key = dest_key.to_vec();
  let mut held = None;
  let reply = rt.block_on(poll_fn(|cx| match fut.as_mut().poll(cx) {
    Poll::Ready(out) => Poll::Ready(out),
    Poll::Pending => {
      if held.is_none() {
        // 首个让出点即源装载磁盘读：此时窗是否在手即为机制判据
        held = Some(rmw_window_held(&probe_store, &key));
      }
      Poll::Pending
    }
  }));
  (
    held.expect("victim 须在源装载处至少让出一次（冷源磁盘读）"),
    reply,
  )
}

/// 自指并发驱动·对面优先轮次：每轮先尽推对面 SADD 再推 victim。修复前（装载
/// 先于开窗）SADD 在 victim 持窗前完整落地并 ACK（early 成立），victim 以陈旧
/// 快照整写覆写之；修复后装载即窗内，SADD 被窗挡至 victim 闭环后重放。回
/// `(victim 应答帧, 对面应答帧, 对面先于 victim 闭环落地)`
fn drive_selfref_eager(
  rt: &Runtime,
  store: &Arc<TestStore>,
  victim_args: &[&[u8]],
  intruder: Vec<Vec<u8>>,
) -> (Vec<u8>, Vec<u8>, bool) {
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
  let mut early = false;
  let mut victim_reply: Option<Vec<u8>> = None;
  let mut inject_reply: Option<Vec<u8>> = None;
  rt.block_on(poll_fn(|cx| {
    if inject_reply.is_none() {
      // 对面命令每轮优先：修复前 victim 装载全程窗未开，SADD 得完整落地 ACK
      if let Poll::Ready(out) = inject.as_mut().poll(cx) {
        inject_reply = Some(out);
        early = victim_reply.is_none();
      }
    }
    if victim_reply.is_none() {
      match rmw.as_mut().poll(cx) {
        Poll::Ready(out) => victim_reply = Some(out),
        Poll::Pending => return Poll::Pending,
      }
    } else if inject_reply.is_some() {
      let v = victim_reply.take().expect("刚置位");
      let i = inject_reply.take().expect("上分支已判");
      return Poll::Ready((v, i, early));
    }
    // victim 已闭环、对面重放未完：让出等窗释放唤醒
    Poll::Pending
  }))
}

/// 自指组种子：key = {a, b}（SUNIONSTORE k k 自指形，折叠恒等覆写）
fn seed_selfref(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8]) {
  assert_eq!(roundtrip(rt, c, &[b"SADD", key, b"a", b"b"]), b":2\r\n");
}

/// 自指形冷臂机制判据（票 wnode-set-store-selfref-load-outside-window）：
/// SUNIONSTORE k k 慢路径臂在源装载首个让出点目标键 rmw 窗已在手——取窗
/// 前移至装载之前成立；修复前窗仅罩落笔段，该点窗未在手即炸出。串行终态：
/// 恒等覆写 {a, b}，回 :2
#[test]
fn sunionstore_selfref_cold_window_covers_source_load() {
  let (_dir, store) = open_test_store("set-store-selfref-win.db").unwrap();
  let rt = Runtime::new().unwrap();
  let key = b"ss:sr:k".to_vec();
  {
    let mut c = consumer_on(&store);
    seed_selfref(&rt, &mut c, &key);
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (held_at_load, reply) =
    drive_first_suspend_probe(&rt, &store, &[b"SUNIONSTORE", &key, &key], &key);
  assert!(
    held_at_load,
    "源装载 k 时目标键 rmw 窗未在手：窗外读旧态 + 窗内整写的非可串行化形仍在位"
  );
  assert_eq!(reply, b":2\r\n", "自指恒等折叠应答应为基数");
  let mut c = consumer_on(&store);
  assert_eq!(
    smembers_sorted(&rt, &mut c, &key),
    vec![b"a".to_vec(), b"b".to_vec()],
    "自指 SUNIONSTORE 原地覆写终态错误"
  );
}

/// 自指形冷臂并发终态（票验证点 a 主判据）：「源装载完成 → 开窗」间隙对面
/// SADD k m 优先注入。修复后两序皆合法且已确认写零丢失：victim 先开窗则 SADD
/// 被挡至闭环后重放（early=false、回 :2、终态 {a,b,m}）；SADD 先开窗则 victim
/// 让核等待、装载读新态折叠（early=true、回 :3、终态 {a,b,m}）。修复前存在
/// 非法第三序：victim 窗外装载 {a,b} 后 SADD 间隙落地 ACK（early=true、回 :2），
/// 陈旧快照整写覆掉已确认的 m——early 与应答基数的自洽配对 + 终态含 m 即
/// 排除该序
#[test]
fn sunionstore_selfref_gap_injected_sadd_lands_no_confirmed_write_loss() {
  let (_dir, store) = open_test_store("set-store-selfref-eager.db").unwrap();
  let rt = Runtime::new().unwrap();
  let key = b"ss:eg:k".to_vec();
  {
    let mut c = consumer_on(&store);
    seed_selfref(&rt, &mut c, &key);
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (store_reply, sadd_reply, early) = drive_selfref_eager(
    &rt,
    &store,
    &[b"SUNIONSTORE", &key, &key],
    vec![b"SADD".to_vec(), key.clone(), b"m".to_vec()],
  );
  assert_eq!(sadd_reply, b":1\r\n", "对面 SADD 应完整落地 ACK");
  assert_eq!(
    store_reply,
    if early { b":3\r\n" } else { b":2\r\n" },
    "victim 应答基数须与交错序自洽：SADD 先行则装载读新态回 :3，SADD 殿后则回 \
     :2（early={early}）；不符即窗外装载的陈旧快照复现"
  );
  let mut c = consumer_on(&store);
  assert_eq!(
    smembers_sorted(&rt, &mut c, &key),
    vec![b"a".to_vec(), b"b".to_vec(), b"m".to_vec()],
    "已确认 SADD 成员 m 丢失（陈旧快照整写覆窗，非可串行化）"
  );
}

/// 自指形冷臂覆写窗串行化不回退：victim SUNIONSTORE k k 持窗跨装载到落笔全程，
/// 对面同窗 SADD 在持窗期必被挡、闭环后重放 ACK，终态已 ACK 增量零丢失
#[test]
fn sunionstore_selfref_cold_dest_window_serializes_concurrent_sadd() {
  let (_dir, store) = open_test_store("set-store-selfref-zadd.db").unwrap();
  let rt = Runtime::new().unwrap();
  let key = b"ss:sz:k".to_vec();
  {
    let mut c = consumer_on(&store);
    seed_selfref(&rt, &mut c, &key);
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (interleaved, store_reply, sadd_reply) = drive_interleaved(
    &rt,
    &store,
    &[b"SUNIONSTORE", &key, &key],
    &key,
    vec![b"SADD".to_vec(), key.clone(), b"newm".to_vec()],
  );
  assert!(
    interleaved,
    "交叠判据未成立：自指 SUNIONSTORE 未在源装载期持目标键窗"
  );
  assert_eq!(store_reply, b":2\r\n");
  assert_eq!(
    sadd_reply,
    b":1\r\n",
    "对面 SADD 应在 SUNIONSTORE 闭环后重放 ACK：{:?}",
    String::from_utf8_lossy(&sadd_reply)
  );
  let mut c = consumer_on(&store);
  assert_eq!(
    smembers_sorted(&rt, &mut c, &key),
    vec![b"a".to_vec(), b"b".to_vec(), b"newm".to_vec()],
    "陈旧折叠快照整写顶掉了持窗期已 ACK 的 SADD 成员（丢已确认写入）"
  );
}

/// 同步/冷双臂自指形全等（票验证点 b）：热键走同步臂（combine_store 承接
/// 装载前预取窗）、冷化孪生组走慢路径臂（store_dest_cold_common 承接预取窗），
/// RESP 应答逐字节一致、终态成员集全等，取窗前移不改串行语义
#[test]
fn sunionstore_selfref_sync_slow_arms_byte_parity() {
  let (_dir, store) = open_test_store("set-store-selfref-parity.db").unwrap();
  let rt = Runtime::new().unwrap();
  let hot = b"ss:pa:hot".to_vec();
  let cold = b"ss:pa:cold".to_vec();
  let mut c = consumer_on(&store);
  seed_selfref(&rt, &mut c, &hot);
  seed_selfref(&rt, &mut c, &cold);
  // 热组：同步臂直出（无慢路径挂起）
  let hot_reply = feed(&mut c, &[b"SUNIONSTORE", &hot, &hot]);
  assert!(
    c.take_slow_wait().is_none(),
    "热键自指 SUNIONSTORE 应走同步臂直出"
  );
  assert_eq!(hot_reply, b":2\r\n");
  // 冷组：强制降级走慢臂
  drop(c);
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let mut c = consumer_on(&store);
  let cold_reply = roundtrip(&rt, &mut c, &[b"SUNIONSTORE", &cold, &cold]);
  assert_eq!(hot_reply, cold_reply, "双臂同命令 RESP 应答不一致");
  // 终态成员集去序全等（SMEMBERS 迭代序非契约面，成员集合逐员全等）
  assert_eq!(
    smembers_sorted(&rt, &mut c, &hot),
    smembers_sorted(&rt, &mut c, &cold),
    "双臂自指覆写终态不一致"
  );
}
