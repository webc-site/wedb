//! 冷态 STORE 臂 TTL 清退落点回归：清退必须落在持窗写临界区内随写执行
//!（票 wnode-store-cold-window-ttl-clear-outsides-critical-section）
//!
//! 缺陷形态：`combine_store_cold` / `store_dest_cold` 曾把 `persist_key`（TTL
//! 清退）放在开窗**之前**——清退与信封写回不共临界区，对面 `EXPIRE dst` 若在
//! 「清退完成 → 开窗」间隙落地（生产 thread-per-core 双核并行真实可达），TTL
//! 借信封 upsert 的 RMW 保留语义存活，SET 语义「写即清 TTL」被覆写带走。
//!
//! 判据（C# 对位：dest 键排他锁跨 GET → Delete → ZADD 全程，
//! libs/server/Storage/Session/ObjectStore/SortedSetGeoOps.cs:129-133）：
//! 1. 禁序判据（本票核心）：victim 执行期内绝不允许观测到「TTL 已亡而信封仍无
//!    内存新值」——修复态的清退尾随写回（`obj_save_clear_ttl` 先信封后清），
//!    TTL 死亡时刻信封必已落内存。窗外裸清旧序（persist 先行）下，persist 抹
//!    TTL 后、obj_save 写回前的域快照/复验挂起点即暴露该禁序态，必红（反向
//!    注入：临时还原 persist 窗前旧序，本判据即红，见 /tmp/ttl-window-red.log）；
//! 2. 对面 `EXPIRE dst` 在 victim 持窗期内必被同址用户键闩拒（锁忙错误帧，
//!    绝不 :1 落地）——C# RETRY_LATER 同语义；
//! 3. victim 闭环后 TTL 终态为清（SET 语义）、覆写成员落定、应答为基数整数值帧。
//!
//! 探针纪律：全部同步内存门/裸读（poll 闭包内禁嵌套 block_on）；TTL 存活判据
//! 经 `ttl_gate_mem_at`（墓碑 → Pass，纯索引探针不可用——冷键删除后墓碑条目
//! 仍在索引，只查在场会误判存活）；信封内存新值经无 TTL 门的
//! `try_read_tag_sync_unprotected`（冷化旧信封 → RecordOnDisk，victim 写回的
//! 新信封落可变区 → Success）。交叠构造沿用
//! zset_load_type_rmw_window_race 的确定性 poll_fn 注入风格，无 sleep。

use std::{future::poll_fn, pin::pin, sync::Arc, task::Poll};

use compio::runtime::Runtime;
use wbase::time::now_ticks;
use wdev::SegmentedDevice;
use wkv::{StoreResult, TtlGate, WedbStore};
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

/// 窗口在手（STORE 覆写臂已取窗）的可观测判据：同键第二窗取闩失败
fn rmw_window_held(store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("判据会话");
  let batch = sess.enter_batch();
  batch.try_rmw_window(key).is_none()
}

/// TTL 旁路记录存活判据（同步内存门裁决，poll 闭包内可安全调用）：
/// 冷化存活记录 → `Degrade`（磁盘候选）；已删除（含墓碑条目残留）→ `Pass`
fn ttl_record_alive(store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("取证会话");
  let batch = sess.enter_batch();
  !matches!(
    batch
      .ttl_gate_mem_at(key, now_ticks())
      .expect("TTL 门裁决不得报存储错误"),
    TtlGate::Pass
  )
}

/// 信封内存新值判据（无 TTL 门的同步裸读，poll 闭包内可安全调用）：
/// 冷化旧信封 → `RecordOnDisk`；victim 写回的新信封落可变区 → `Success`
fn envelope_in_memory(store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("取证会话");
  let batch = sess.enter_batch();
  matches!(
    batch
      .session
      .try_read_tag_sync_unprotected(key, KeyTag::ObjectEnvelope, |_raw| ())
      .expect("信封裸读不得报存储错误"),
    StoreResult::Success(_)
  )
}

/// victim 冷态 STORE 挂起 → 持窗判据成立 → 对面 EXPIRE 注入（必被闩拒）→
/// 禁序判据（TTL 亡 ∧ 信封无内存新值 全程不出现）→ victim 闭环。回
/// `(禁序态是否出现, victim 应答帧, 对面 EXPIRE 应答帧)`
fn drive_store_with_expire_intruder(
  rt: &Runtime,
  store: &Arc<TestStore>,
  victim_args: &[&[u8]],
  victim_key: &[u8],
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
  let mut inject = pin!(deliver(inject_store, intruder_args(victim_key)));
  let probe_store = Arc::clone(store);
  let probe_key = victim_key.to_vec();
  let mut held = false;
  let mut order_violation = false;
  let mut inject_out: Option<Vec<u8>> = None;
  let victim_reply = rt.block_on(poll_fn(|cx| {
    if !held {
      // 交叠判据未成立的轮次只推 victim：判据成立前对面命令绝不放行；
      // 未成立即让出（禁忙等活锁）——STORE 臂开窗点在源键冷读之后
      match rmw.as_mut().poll(cx) {
        Poll::Ready(out) => return Poll::Ready(out),
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
      // 对面 EXPIRE 与 STORE 臂同址用户键闩：持窗期内必被拒（锁忙错误帧，
      // 绝不 :1 落地——清退-写回之间不得插入存活 TTL）
      let reply = match inject.as_mut().poll(cx) {
        Poll::Ready(out) => out,
        Poll::Pending => {
          assert!(
            rmw_window_held(&probe_store, &probe_key),
            "victim 持窗中断：窗口应在域快照-落笔全程在手"
          );
          return Poll::Pending;
        }
      };
      inject_out = Some(reply.clone());
      assert!(
        !reply.starts_with(b":1"),
        "对面 EXPIRE 竟在 victim 持窗期内 :1 落地：覆写窗串行化失效"
      );
    }
    // 禁序判据（本票核心）：挂起点观测到「TTL 已亡 ∧ 信封无内存新值」即
    // TTL 清退先于写回独立落地（窗外裸清旧序：persist 抹 TTL 后，域快照/
    // 落笔复验挂起点恰好暴露）；修复态清退尾随写回同临界区落笔，该态不存在
    if !order_violation
      && !ttl_record_alive(&probe_store, &probe_key)
      && !envelope_in_memory(&probe_store, &probe_key)
    {
      order_violation = true;
    }
    match rmw.as_mut().poll(cx) {
      Poll::Ready(out) => Poll::Ready(out),
      Poll::Pending => Poll::Pending,
    }
  }));
  let intruder_reply = match inject_out {
    Some(out) => out,
    None => rt.block_on(inject.as_mut()),
  };
  (order_violation, victim_reply, intruder_reply)
}

/// 对面注入帧：EXPIRE dst 600
fn intruder_args(dst: &[u8]) -> Vec<Vec<u8>> {
  vec![b"EXPIRE".to_vec(), dst.to_vec(), b"600".to_vec()]
}

/// set 臂 victim（冷态 SUNIONSTORE，dst 带 TTL）：共现判据 + 对面 EXPIRE 被拒 +
/// 终态 TTL 清、覆写落定
#[test]
fn sunionstore_cold_ttl_clear_inside_critical_section() {
  let (_dir, store) = open_test_store("store-ttl-cs-set.db").unwrap();
  let rt = Runtime::new().unwrap();
  let dst = b"cs:set:dst".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SADD", &dst, b"x", b"y"]),
      b":2\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SADD", b"cs:set:src", b"a", b"b"]),
      b":2\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"EXPIRE", &dst, b"600"]),
      b":1\r\n"
    );
  }
  // 冷化：STORE 臂、目标键与 TTL 旁路全部落盘
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert!(
    ttl_record_alive(&store, &dst),
    "前置判据：冷化后 TTL 旁路应仍存活（丢失则用例失效）"
  );

  let (order_violation, store_reply, expire_reply) =
    drive_store_with_expire_intruder(&rt, &store, &[b"SUNIONSTORE", &dst, b"cs:set:src"], &dst);
  assert!(
    !order_violation,
    "观测到「TTL 已亡 ∧ 信封无内存新值」禁序态：TTL 清退先于写回独立落地    （窗外裸清旧序——persist 先行抹 TTL，对面 EXPIRE 可于「清退 → 开窗」间隙\
     借信封 upsert 保活，SET 语义清 TTL 被覆写带走）"
  );
  assert_eq!(
    store_reply, b":2\r\n",
    "SUNIONSTORE 应答应为源集合基数整数值帧"
  );
  assert!(
    expire_reply.starts_with(b"-"),
    "持窗期对面 EXPIRE 应回锁忙错误帧（C# RETRY_LATER 同语义），实际：{:?}",
    String::from_utf8_lossy(&expire_reply)
  );
  let mut c = consumer_on(&store);
  // 终态：SET 语义 TTL 清、覆写成员落定
  assert_eq!(roundtrip(&rt, &mut c, &[b"PTTL", &dst]), b":-1\r\n");
  let members = roundtrip(&rt, &mut c, &[b"SMEMBERS", &dst]);
  let mut got: Vec<Vec<u8>> = members
    .split(|b| *b == b'\r')
    .map(|l| match l.first() {
      Some(b'\n') => &l[1..],
      _ => l,
    })
    .filter(|l| !l.is_empty() && !l.starts_with(b"*") && !l.starts_with(b"$"))
    .map(Vec::from)
    .collect();
  got.sort_unstable();
  assert_eq!(
    got,
    vec![b"a".to_vec(), b"b".to_vec()],
    "SUNIONSTORE 覆写成员未落定：{:?}",
    String::from_utf8_lossy(&members)
  );
}

/// zset 臂 victim（冷态 ZUNIONSTORE，dst 带 TTL）：同判据经 zset 收尾单点
/// `store_dest_cold`（geo 臂同源转引）复验
#[test]
fn zunionstore_cold_ttl_clear_inside_critical_section() {
  let (_dir, store) = open_test_store("store-ttl-cs-zset.db").unwrap();
  let rt = Runtime::new().unwrap();
  let dst = b"cs:z:dst".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"ZADD", &dst, b"1", b"x", b"2", b"y"]),
      b":2\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"ZADD", b"cs:z:src", b"3", b"a", b"4", b"b"]),
      b":2\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"EXPIRE", &dst, b"600"]),
      b":1\r\n"
    );
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert!(
    ttl_record_alive(&store, &dst),
    "前置判据：冷化后 TTL 旁路应仍存活（丢失则用例失效）"
  );

  let (order_violation, store_reply, expire_reply) = drive_store_with_expire_intruder(
    &rt,
    &store,
    &[b"ZUNIONSTORE", &dst, b"1", b"cs:z:src"],
    &dst,
  );
  assert!(
    !order_violation,
    "观测到「TTL 已亡 ∧ 信封无内存新值」禁序态：TTL 清退先于写回独立落地（窗外裸清旧序）"
  );
  assert_eq!(
    store_reply, b":2\r\n",
    "ZUNIONSTORE 应答应为结果基数整数值帧"
  );
  assert!(
    expire_reply.starts_with(b"-"),
    "持窗期对面 EXPIRE 应回锁忙错误帧，实际：{:?}",
    String::from_utf8_lossy(&expire_reply)
  );
  let mut c = consumer_on(&store);
  assert_eq!(roundtrip(&rt, &mut c, &[b"PTTL", &dst]), b":-1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZCARD", &dst]),
    b":2\r\n",
    "ZUNIONSTORE 覆写基数未落定"
  );
}
