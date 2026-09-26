//! geo 冷态 GEOSEARCHSTORE 臂 persist 出窗回归（票 wcol-geo-search-store-cold-persist-in-window-self-lock）
//!
//! 缺陷形态与 zset 父票 71a9c4ce、set 票（dev a9952d19）完全同源：
//! `sorted_set_geo_commands.rs` GEOSEARCHSTORE 异步臂曾在持有目标键 rmw 窗
//! 期间调用 `persist_key`（TTL 清退）——wkv `persist` 按**用户键**取索引层非重入
//! 独占桶闩（scoped 口径 `try_lock_key_hash_exclusive`，锁忙即 `IndexError::LockTimeout`），与 rmw
//! 窗同一把闩：冷态 GEOSEARCHSTORE 实测必自锁，回 `-ERR slow path storage error`。
//!
//! 修法落点（本回归锁定的判据；自票
//! wnode-store-cold-window-ttl-clear-outsides-critical-section 起 TTL 清退再挪进
//! 持窗写临界区随写落笔——`obj_save_clear_ttl`，见 `store_dest_cold_common`，
//! 窗外裸清的「清退与写回之间落 TTL」次生交错随之在机制上消除）：
//! - `retire_tiered_dest` 恒在窗释放后；
//! - 窗仍跨「存活域快照 → 落笔复验 → 信封写回·随写清 TTL/删空回收」全程，
//!   覆写保护不回退（持窗判据 = 同键第二窗取闩失败；修复前 persist 在窗内即炸
//!   storage error，修复后若误摘窗则注入段判据不成立即炸出——反向注入：临时
//!   还原旧序本文件各 ttl 用例应答即回退为 storage error 帧）。
//!
//! 交叠构造沿用 set_store_cold_window_ttl_selfheal 的确定性 poll_fn 注入风格，
//! 无 sleep。
//!
//! 自指并发夹具（票 wnode-geo-store-selfref-load-outside-window，§87 同式第三缝）：
//! GEO STORE 族 dest∈src 自指形的目标键 rmw 窗双臂皆前移至源装载/搜索求值之前
//! （对标 C# GeoSearchStore 入口即持 dst Exclusive 罩读算写全程，
//! SortedSetGeoOps.cs:127-130），机制判据 = 冷臂源装载首个让出点 dest 窗必已在手
//! （修复前窗仅罩落笔段，该点窗未在手即炸出）、快路径取窗先于源 WRONGTYPE 检出
//! （修复前同步直出错帧）；修复前两臂同命令 RESP 与终态逐字节全等。

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
        // 未成立即让出（禁忙等活锁）——STORE 臂开窗点在目标键落笔之前
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
        // 对面 ZADD 为同窗 RMW 命令：victim 持窗期内必被挡（让核等待）
        if blocked_polls < 2 {
          blocked_polls += 1;
          assert!(
            matches!(inject.as_mut().poll(cx), Poll::Pending),
            "对面 ZADD 竟在 victim 持窗期内落地：覆写窗串行化失效"
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

/// GEOSEARCHSTORE 公共帧：源为两成员 geo 集合（palermo/catania，圆心取 palermo，
/// 半径 200km 双命中）
fn store_frame(dst: &[u8], src: &[u8]) -> Vec<Vec<u8>> {
  vec![
    b"GEOSEARCHSTORE".to_vec(),
    dst.to_vec(),
    src.to_vec(),
    b"FROMLONLAT".to_vec(),
    b"13.361389".to_vec(),
    b"38.115556".to_vec(),
    b"BYRADIUS".to_vec(),
    b"200".to_vec(),
    b"km".to_vec(),
    b"ASC".to_vec(),
  ]
}

fn k(name: &str, prefix: &str) -> Vec<u8> {
  format!("{prefix}:{name}").into_bytes()
}

/// `Vec<Vec<u8>>` 帧表转 `&[&[u8]]` 出参口径
fn slices(v: &[Vec<u8>]) -> Vec<&[u8]> {
  v.iter().map(Vec::as_slice).collect()
}

/// ZRANGE 0 -1 应答的成员表解析（升序归一，去序比对用）
fn zrange_members(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8]) -> Vec<Vec<u8>> {
  roundtrip(rt, c, &[b"ZRANGE", key, b"0", b"-1"])
    .split(|b| *b == b'\r')
    .map(|l| match l.first() {
      Some(b'\n') => &l[1..],
      _ => l,
    })
    .filter(|l| !l.is_empty() && !l.starts_with(b"*") && !l.starts_with(b"$"))
    .map(Vec::from)
    .collect::<Vec<_>>()
}

/// 冷键 victim 慢路径臂驱动至首个让出点（源装载的磁盘读），记录该点 dest 窗是否
/// 在手，随后放行使臂闭环。回 `(首让出点持窗, victim 应答帧)`——修复后取窗先于
/// 装载成立；修复前窗仅罩落笔段，源装载让出点窗必未在手即炸出
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
    "冷键 GEOSEARCHSTORE 应挂慢路径，实际同步段直出 {:?}",
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

/// 自指组种子：key = {palermo, catania, faraway}（faraway 在圆心 200km 域外，
/// store_frame 折叠收缩掉它）
fn seed_selfref(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8]) {
  assert_eq!(
    roundtrip(
      rt,
      c,
      &[
        b"GEOADD",
        key,
        b"13.361389",
        b"38.115556",
        b"palermo",
        b"15.087269",
        b"37.309",
        b"catania",
        b"100.0",
        b"0.0",
        b"faraway"
      ]
    ),
    b":3\r\n"
  );
}

/// 双孪生组种子：src = {palermo, catania}、dst = {old}（prefix 区分 TTL/无 TTL
/// 组），dst 依 seed_ttl 决定是否挂 600s TTL
fn seed_group(
  rt: &Runtime,
  c: &mut RespSessionConsumer,
  prefix: &str,
  seed_ttl: bool,
) -> Vec<Vec<u8>> {
  let mut replies = Vec::new();
  let k = |name: String| k(name.as_str(), prefix);
  replies.push(roundtrip(
    rt,
    c,
    &[
      b"GEOADD",
      &k("src".into()),
      b"13.361389",
      b"38.115556",
      b"palermo",
      b"15.087269",
      b"37.309",
      b"catania",
    ],
  ));
  replies.push(roundtrip(
    rt,
    c,
    &[b"GEOADD", &k("dst".into()), b"1.0", b"1.0", b"old"],
  ));
  if seed_ttl {
    replies.push(roundtrip(rt, c, &[b"EXPIRE", &k("dst".into()), b"600"]));
  }
  replies
}

/// 冷态 GEOSEARCHSTORE 目标键带 TTL 不再生：TTL 持窗写临界区内随写清退，应答与
/// 无 TTL 基线逐字节一致且 TTL 已清；修复前 persist 落窗内同桶自锁，应答回退为
/// `-ERR slow path storage error`（反向注入判据）
#[test]
fn geosearchstore_cold_dest_ttl_cleared_no_selflock() {
  let (_dir, store) = open_test_store("geo-store-ttl.db").unwrap();
  let rt = Runtime::new().unwrap();
  {
    let mut c = consumer_on(&store);
    // TTL 组与无 TTL 基线组同构种子（仅 EXPIRE 之差）
    let seed_t = seed_group(&rt, &mut c, "g1", true);
    let seed_n = seed_group(&rt, &mut c, "g0", false);
    for r in seed_t.iter().chain(seed_n.iter()) {
      assert!(
        r.as_slice() == &b":2\r\n"[..] || r.as_slice() == &b":1\r\n"[..],
        "种子帧异常（GEOADD :2/:1 / EXPIRE :1 之外）：{:?}",
        String::from_utf8_lossy(r)
      );
    }
    // 冷化：源/目标全部落盘
    rt.block_on(store.flush_and_evict_all()).unwrap();
    // 前置件：TTL 旁路随键冷化在场（丢失则用例失效，炸出）
    let dst_t = k("dst", "g1");
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
    // 冷态 GEOSEARCHSTORE：带 TTL 目标键应答必须与无 TTL 基线逐字节一致
    let dst_n = k("dst", "g0");
    let frames_t = store_frame(&dst_t, &k("src", "g1"));
    let frames_n = store_frame(&dst_n, &k("src", "g0"));
    let reply_t = roundtrip(&rt, &mut c, &slices(&frames_t));
    let reply_n = roundtrip(&rt, &mut c, &slices(&frames_n));
    assert_eq!(
      reply_t,
      reply_n,
      "带 TTL 目标键冷态 GEOSEARCHSTORE 应答与无 TTL 基线不一致（修复前此处为 storage error 自锁红）：\
       ttl={:?} baseline={:?}",
      String::from_utf8_lossy(&reply_t),
      String::from_utf8_lossy(&reply_n)
    );
    assert_eq!(
      reply_t, b":2\r\n",
      "冷态 GEOSEARCHSTORE 应答应为命中数整数值帧"
    );
    // TTL 已清（SET 语义持窗随写清退）且成员覆写落定、与基线组逐字节一致
    assert_eq!(roundtrip(&rt, &mut c, &[b"PTTL", &dst_t]), b":-1\r\n");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"ZRANGE", &dst_t, b"0", b"-1"]),
      roundtrip(&rt, &mut c, &[b"ZRANGE", &dst_n, b"0", b"-1"]),
      "GEOSEARCHSTORE 终态成员与无 TTL 基线不一致"
    );
  }
}

/// 冷态 GEOSEARCHSTORE 命中为空 + 目标键带 TTL：删空回收臂不受挪序影响，应答与
/// 无 TTL 基线一致且键随删空消亡（TTL 无从残留）
#[test]
fn geosearchstore_cold_empty_result_with_ttl_deletes_dest_like_baseline() {
  let (_dir, store) = open_test_store("geo-store-empty-ttl.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  // 圆心取远海（100.0, 0.0）半径 1km：双组皆零命中
  let empty_frames = |p: &str| -> Vec<Vec<u8>> {
    vec![
      b"GEOSEARCHSTORE".to_vec(),
      k("dst", p),
      k("src", p),
      b"FROMLONLAT".to_vec(),
      b"100.0".to_vec(),
      b"0.0".to_vec(),
      b"BYRADIUS".to_vec(),
      b"1".to_vec(),
      b"km".to_vec(),
      b"ASC".to_vec(),
    ]
  };
  assert_eq!(
    seed_group(&rt, &mut c, "e:t", true)
      .iter()
      .chain(seed_group(&rt, &mut c, "e:n", false).iter())
      .count(),
    5,
    "种子命令数异常"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let frames_t = empty_frames("e:t");
  let frames_n = empty_frames("e:n");
  let reply_t = roundtrip(&rt, &mut c, &slices(&frames_t));
  let reply_n = roundtrip(&rt, &mut c, &slices(&frames_n));
  assert_eq!(reply_t, reply_n, "空结果臂带 TTL 应答应与无 TTL 基线一致");
  assert_eq!(reply_t, b":0\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"EXISTS", &k("dst", "e:t")]),
    b":0\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"EXISTS", &k("dst", "e:n")]),
    b":0\r\n"
  );
}

/// 修复不回退覆写窗保护：persist 挪窗前之后，冷态 GEOSEARCHSTORE 目标键覆写仍跨
/// 「域快照 → 落笔」全程持窗——对面同窗 ZADD 在持窗期内必被挡，闭环后重放 ACK，
/// 终态已 ACK 增量零丢失
#[test]
fn geosearchstore_cold_dest_window_still_serializes_concurrent_zadd() {
  let (_dir, store) = open_test_store("geo-store-window-zadd.db").unwrap();
  let rt = Runtime::new().unwrap();
  let dst = b"gw:dst".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &[
          b"GEOADD",
          b"gw:src",
          b"13.361389",
          b"38.115556",
          b"palermo",
          b"15.087269",
          b"37.309",
          b"catania"
        ]
      ),
      b":2\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GEOADD", &dst, b"1.0", b"1.0", b"old"]),
      b":1\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"EXPIRE", &dst, b"600"]),
      b":1\r\n"
    );
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let victim_frames = store_frame(&dst, b"gw:src");
  let victim_args = slices(&victim_frames);
  let (interleaved, store_reply, zadd_reply) = drive_interleaved(
    &rt,
    &store,
    &victim_args,
    &dst,
    vec![
      b"ZADD".to_vec(),
      dst.clone(),
      b"5".to_vec(),
      b"zzz".to_vec(),
    ],
  );
  assert!(
    interleaved,
    "交叠判据未成立：GEOSEARCHSTORE 未在目标键落笔期持窗（挪序误摘窗即炸出）"
  );
  assert_eq!(store_reply, b":2\r\n", "GEOSEARCHSTORE 应答应为命中数");
  assert_eq!(
    zadd_reply,
    b":1\r\n",
    "对面 ZADD 应在 GEOSEARCHSTORE 闭环后重放 ACK：{:?}",
    String::from_utf8_lossy(&zadd_reply)
  );
  let mut c = consumer_on(&store);
  // 终态成员 = 覆写集合 {catania, palermo} + 窗内已 ACK 的 zzz（去序比对）
  let got = roundtrip(&rt, &mut c, &[b"ZRANGE", &dst, b"0", b"-1"]);
  let mut got_members = zrange_members(&rt, &mut c, &dst);
  got_members.sort_unstable();
  assert_eq!(
    got_members,
    vec![b"catania".to_vec(), b"palermo".to_vec(), b"zzz".to_vec()],
    "GEOSEARCHSTORE 旧覆写顶掉了窗内已 ACK 的 ZADD 成员（丢已确认写入）：{:?}",
    String::from_utf8_lossy(&got)
  );
  // TTL 已清（窗前 persist 生效且无自锁）
  assert_eq!(roundtrip(&rt, &mut c, &[b"PTTL", &dst]), b":-1\r\n");
}

/// 自指形冷臂机制判据（票 wnode-geo-store-selfref-load-outside-window）：
/// GEOSEARCHSTORE k k 慢路径臂在源装载首个让出点目标键 rmw 窗已在手——取窗
/// 前移至装载之前成立；修复前窗仅罩落笔段，该点窗未在手即炸出。串行终态：
/// 收缩掉域外成员 faraway，回 :2
#[test]
fn geosearchstore_selfref_cold_window_covers_source_load() {
  let (_dir, store) = open_test_store("geo-store-selfref-win.db").unwrap();
  let rt = Runtime::new().unwrap();
  let key = b"gs:sr:k".to_vec();
  {
    let mut c = consumer_on(&store);
    seed_selfref(&rt, &mut c, &key);
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let frames = store_frame(&key, &key);
  let (held_at_load, reply) = drive_first_suspend_probe(&rt, &store, &slices(&frames), &key);
  assert!(
    held_at_load,
    "源装载 k 时目标键 rmw 窗未在手：窗外读旧态 + 窗内整写的非可串行化形仍在位"
  );
  assert_eq!(reply, b":2\r\n", "自指收缩应答应为命中数");
  let mut c = consumer_on(&store);
  let mut members = zrange_members(&rt, &mut c, &key);
  members.sort_unstable();
  assert_eq!(
    members,
    vec![b"catania".to_vec(), b"palermo".to_vec()],
    "自指 GEOSEARCHSTORE 原地收缩终态错误"
  );
}

/// 自指形冷臂并发终态：victim 于源装载点已持目标键窗，对面 ZADD k newm（同窗
/// RMW 命令）在持窗期必被挡、victim 闭环后重放 ACK——已确认写零丢失，终态 =
/// 收缩集合 {catania, palermo} + newm
#[test]
fn geosearchstore_selfref_cold_dest_window_serializes_concurrent_zadd() {
  let (_dir, store) = open_test_store("geo-store-selfref-zadd.db").unwrap();
  let rt = Runtime::new().unwrap();
  let key = b"gs:sz:k".to_vec();
  {
    let mut c = consumer_on(&store);
    seed_selfref(&rt, &mut c, &key);
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let frames = store_frame(&key, &key);
  let (interleaved, store_reply, zadd_reply) = drive_interleaved(
    &rt,
    &store,
    &slices(&frames),
    &key,
    vec![
      b"ZADD".to_vec(),
      key.clone(),
      b"5".to_vec(),
      b"newm".to_vec(),
    ],
  );
  assert!(
    interleaved,
    "交叠判据未成立：自指 GEOSEARCHSTORE 未在源装载期持目标键窗"
  );
  assert_eq!(store_reply, b":2\r\n");
  assert_eq!(
    zadd_reply,
    b":1\r\n",
    "对面 ZADD 应在 GEOSEARCHSTORE 闭环后重放 ACK：{:?}",
    String::from_utf8_lossy(&zadd_reply)
  );
  let mut c = consumer_on(&store);
  let mut members = zrange_members(&rt, &mut c, &key);
  members.sort_unstable();
  assert_eq!(
    members,
    vec![b"catania".to_vec(), b"newm".to_vec(), b"palermo".to_vec()],
    "陈旧搜索快照整写顶掉了持窗期已 ACK 的 ZADD 成员（丢已确认写入）"
  );
}

/// 快路径结构性判据：外部会话持 dest 窗期间，GEOSEARCHSTORE 同步臂须在源装载
/// 之前即取窗失败降级（同步段零输出），而非先装载源检出 WRONGTYPE 同步直出
/// 错误帧——后者为修复前「窗仅罩落笔段、装载先于取窗」形态。放闩后慢路径重放
/// 仍检出 WRONGTYPE，检测面不丢
#[test]
fn geosearchstore_fast_path_opens_dest_window_before_source_load() {
  let (_dir, store) = open_test_store("geo-store-selfref-fastpath.db").unwrap();
  let rt = Runtime::new().unwrap();
  let dst = b"gs:fp:dst";
  let src = b"gs:fp:src";
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"HSET", src, b"f", b"v"]),
      b":1\r\n"
    );
  }
  // 外部会话持 dest 窗
  let sess_w = store.new_session().unwrap();
  let batch_w = sess_w.enter_batch();
  let window = batch_w.try_rmw_window(dst).expect("占窗会话取 dest 窗");

  let mut c = consumer_on(&store);
  let mut out = feed(
    &mut c,
    &[
      b"GEOSEARCHSTORE",
      dst,
      src,
      b"FROMLONLAT".as_slice(),
      b"13.361389".as_slice(),
      b"38.115556".as_slice(),
      b"BYRADIUS".as_slice(),
      b"200".as_slice(),
      b"km".as_slice(),
    ],
  );
  assert!(
    out.is_empty(),
    "dest 窗被占时同步臂须先于源装载取窗失败降级（修复前装载先检出源 \
     WRONGTYPE 同步直出）：{:?}",
    String::from_utf8_lossy(&out)
  );
  drop(window);
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async { out.extend_from_slice(&slow.resolve().await) });
  }
  assert!(
    String::from_utf8_lossy(&out).contains("WRONGTYPE"),
    "放闩重放须保留对象源 WRONGTYPE 检测：{:?}",
    String::from_utf8_lossy(&out)
  );
}

/// 双臂同命令 RESP 与终态逐字节全等（票验证点 b）：热键自指形走同步臂、冷化
/// 孪生组走慢路径臂，应答与收缩终态 ZRANGE 帧逐字节一致，取窗前移不改串行语义
#[test]
fn geosearchstore_selfref_sync_slow_arms_byte_parity() {
  let (_dir, store) = open_test_store("geo-store-selfref-parity.db").unwrap();
  let rt = Runtime::new().unwrap();
  let hot = b"gs:pa:hot".to_vec();
  let cold = b"gs:pa:cold".to_vec();
  let mut c = consumer_on(&store);
  seed_selfref(&rt, &mut c, &hot);
  seed_selfref(&rt, &mut c, &cold);
  // 热组：同步臂直出（无慢路径挂起）
  let hot_frames = store_frame(&hot, &hot);
  let hot_reply = feed(&mut c, &slices(&hot_frames));
  assert!(
    c.take_slow_wait().is_none(),
    "热键自指 GEOSEARCHSTORE 应走同步臂直出"
  );
  assert_eq!(hot_reply, b":2\r\n");
  // 冷组：强制降级走慢臂
  drop(c);
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let mut c = consumer_on(&store);
  let cold_frames = store_frame(&cold, &cold);
  let cold_reply = roundtrip(&rt, &mut c, &slices(&cold_frames));
  assert_eq!(hot_reply, cold_reply, "双臂同命令 RESP 应答不一致");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGE", &hot, b"0", b"-1"]),
    roundtrip(&rt, &mut c, &[b"ZRANGE", &cold, b"0", b"-1"]),
    "双臂收缩终态逐字节不一致"
  );
}
