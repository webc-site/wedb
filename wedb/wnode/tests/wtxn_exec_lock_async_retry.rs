//! EXEC 取锁同步自旋饿死 compio 单 worker 死锁回归（对标 task/ing/
//! wtxn-exec-lock-all-keys-sync-spin-starves-runtime.md）
//!
//! 缺陷：`TxnKeyEntries::lock_all_keys` / `try_lock_all_keys` 用
//! `thread::yield_now` 无界同步自旋取桶闩。该让步只让出 OS 时间片、不归还
//! 协程调度器。compio 每 worker 单线程、会话钉死单 worker（`-t 1`/单核默认 1
//! worker）：同 worker 上一任务 BLPOP-in-MULTI 挂起（重放期持排他桶闩）时，
//! 另一任务 EXEC 若走同步自旋则霸占整个 worker，使唤醒事件永不被 poll →
//! 该 worker 及其全部连接永久死锁。C# 同循环跑在抢占式线程池上无此面。
//!
//! 本用例以「同 worker 上另一连接是否仍可进展」为唯一判据（执行模型判别，非
//! 时序竞态）：单线程 compio Runtime（等价单 worker）内并发驱动四个会话——
//!   A：MULTI / BLPOP kA 0 / EXEC —— EXEC 取闩成功后重放 BLPOP 挂起，持
//!      bucket(kA) 排他闩不放；
//!   B：MULTI / SET kB v / EXEC —— kB 与 kA 落在同一桶，EXEC 取闩争用；
//!   C：PING —— 探针：B 争用期间同 worker 其它连接能否应答；
//!   R：CLIENT UNBLOCK A ERROR —— 解除 A，令其提交放闩，B 方可完成。
//! 修复后：B 首轮取闩失败即挂既有唯一慢臂（单次 `yield_now` + 重驱本 EXEC），
//! 每轮让出执行器，C 得以应答 PONG、R 得以解除 A、A 提交放闩、B 随后完成；
//! 判据（C/B 在时限内应答）成立。修复前：B 在 EXEC 内同步自旋霸占 worker，
//! C 的 PING 与 R 的 UNBLOCK 永不被消费、A 永不放闩、B 永不完成 → 整体挂起，
//! 由主线程有界 `recv_timeout` 转为可判别的 RED（背景线程自旋泄漏随进程退出
//! 回收，测试框架 main 返回强杀所有后台线程）。

use std::{
  collections::HashMap,
  sync::{
    Arc,
    mpsc::{RecvTimeoutError, channel},
  },
  thread,
  time::Duration,
};

use compio::runtime::{Runtime, spawn};
use crossfire::oneshot::{TxOneshot, oneshot};
use wcol::itembroker::{
  collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker,
};
use wconf::RuntimeServerConfig;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  resp::{
    garnet_api::StoreGarnetApi, objects::collection_item_source::CollectionItemSource,
    resp_server_session::RespServerSessionOptions, resp_session_consumer::RespSessionConsumer,
  },
};
use wtest_base::{resp_frame_str, test_store_config};
use wtxn::{TxnKeyEntryComparison, TxnLockTable, WatchVersionMap};
use wval::SessionPrefixBuf;

/// 键在共享锁表中的桶下标（与事务侧取锁同址：`key_hash & size_mask`）
fn bucket_of(lock_table: &TxnLockTable, key: &[u8]) -> usize {
  lock_table.bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(
    SessionPrefixBuf::ROOT.as_slice(),
    key,
  ))
}

/// 暴力搜索落在同一桶的两个不同键（1024 桶默认表，鸽笼原理必在千级迭代内命中）
fn find_colliding_pair(lock_table: &TxnLockTable) -> (Vec<u8>, Vec<u8>) {
  let mut first: HashMap<usize, Vec<u8>> = HashMap::new();
  for i in 0..100_000u64 {
    let name = format!("colliding-{i}").into_bytes();
    let bucket = bucket_of(lock_table, &name);
    if let Some(prev) = first.get(&bucket) {
      return (prev.clone(), name);
    }
    first.insert(bucket, name);
  }
  panic!("未能在时限内找到同桶碰撞键对");
}

/// 单消费者泵驱至完成（drive.rs 内层循环的测试等价物：消费→阻塞挂起→慢路径
/// 挂起→再消费，直至无挂起）。首轮消费返回后触发 `on_first`（A 于此刻已持闩、
/// B 于此刻已入争用慢臂），供跨协程定序，杜绝依赖墙钟 sleep 的伪竞态。
async fn pump(
  consumer: &mut RespSessionConsumer,
  frames: &[Vec<u8>],
  mut on_first: Vec<TxOneshot<()>>,
) -> Vec<u8> {
  {
    let mut scratch = consumer.take_recv_scratch();
    for frame in frames {
      scratch.extend_from_slice(frame);
    }
    consumer.return_recv_scratch(scratch);
  }
  let mut out = Vec::new();
  let mut first = true;
  loop {
    assert!(
      consumer.try_consume_messages_into(&mut out).is_some(),
      "命令帧不应触发协议违规"
    );
    if first {
      for tx in on_first.drain(..) {
        tx.send(());
      }
      first = false;
    }
    let mut progressed = false;
    if let Some(mut blocked) = consumer.take_blocked_wait() {
      let (cmd, result) = blocked.resolve().await;
      consumer.resolve_blocked_wait_into(cmd, result, &mut out);
      progressed = true;
    }
    if let Some(slow) = consumer.take_slow_wait() {
      // 空应答（取锁争用让步体）不写线；resolve 内部 `yield_now` 让出执行器
      out.extend_from_slice(&slow.resolve().await);
      progressed = true;
    }
    if !progressed {
      break;
    }
  }
  out
}

/// 场景驱动结果：A/B/C 三连接应答字节
struct Verdict {
  a: Vec<u8>,
  b: Vec<u8>,
  c: Vec<u8>,
}

/// 在独立 OS 线程上以单线程 compio Runtime（单 worker）驱动全场景，
/// 完成后回传应答；主线程以有界 `recv_timeout` 判别，绝不让测试进程真挂。
fn run_scenario() -> Verdict {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("exec-lock.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  let runtime_config = RuntimeServerConfig::shared_default();
  // 全四会话共享同一锁表句柄（Clone 共享同一 HashIndex）与版本表——同桶碰撞即争同一把闩
  let lock_table = TxnLockTable::new();
  let watch_version_map = Arc::new(WatchVersionMap::new(1024));

  let (key_a, key_b) = find_colliding_pair(&lock_table);
  assert_eq!(
    bucket_of(&lock_table, &key_a),
    bucket_of(&lock_table, &key_b),
    "kA/kB 必落同桶方能构造争用"
  );

  let make_client = |id: u64| -> RespSessionConsumer {
    let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
    let mut consumer = RespSessionConsumer::new(id, RespServerSessionOptions::default(), api);
    consumer.set_item_broker(Arc::clone(&broker));
    consumer.set_runtime_config(Arc::clone(&runtime_config));
    consumer.attach_transaction_components(Arc::clone(&watch_version_map), lock_table.clone());
    consumer
  };

  // A 持闩令：MULTI / BLPOP kA 0 / EXEC（超时 0 = 无限挂起，对标工单验证点）
  let key_a_str = String::from_utf8(key_a.clone()).unwrap();
  let key_b_str = String::from_utf8(key_b.clone()).unwrap();
  let frames_a = vec![
    resp_frame_str(&["MULTI"]),
    resp_frame_str(&["BLPOP", &key_a_str, "0"]),
    resp_frame_str(&["EXEC"]),
  ];
  // B 争用：MULTI / SET kB v / EXEC（kB 与 kA 同桶）
  let frames_b = vec![
    resp_frame_str(&["MULTI"]),
    resp_frame_str(&["SET", &key_b_str, "v"]),
    resp_frame_str(&["EXEC"]),
  ];
  let frames_c = vec![resp_frame_str(&["PING"])];
  // R 解除 A（A 会话 id = 11），令其提交放闩
  let frames_r = vec![resp_frame_str(&["CLIENT", "UNBLOCK", "11", "ERROR"])];

  let rt = Runtime::new().unwrap();
  rt.block_on(async move {
    let mut client_a = make_client(11);
    let mut client_b = make_client(12);
    let mut client_c = make_client(13);
    let mut client_r = make_client(14);

    // 定序信号：A 已持闩 → 启动 B；B 已入争用 → 启动 C、R
    let (a_parked_tx, a_parked_rx) = oneshot();
    let (b_contended_to_c_tx, b_contended_to_c_rx) = oneshot();
    let (b_contended_to_r_tx, b_contended_to_r_rx) = oneshot();

    // A：首轮消费即取闩成功、重放 BLPOP 挂起（持 bucket(kA)）
    let handle_a = spawn(async move { pump(&mut client_a, &frames_a, vec![a_parked_tx]).await });
    // 待 A 确实持闩后再放行 B，令争用为必然（非竞态）
    let _ = a_parked_rx.await;

    let handle_b = spawn(async move {
      pump(
        &mut client_b,
        &frames_b,
        vec![b_contended_to_c_tx, b_contended_to_r_tx],
      )
      .await
    });

    // C：探针。B 争用信号后发 PING——修复后 B 每轮让出执行器，PING 得应答；
    // 修复前 B 同步自旋霸占 worker，本协程乃至其消费永不被 poll。
    let handle_c = spawn(async move {
      let _ = b_contended_to_c_rx.await;
      pump(&mut client_c, &frames_c, Vec::new()).await
    });

    // R：解除 A，令其提交放闩，B 方能在后续让步轮取闩成功
    let handle_r = spawn(async move {
      let _ = b_contended_to_r_rx.await;
      pump(&mut client_r, &frames_r, Vec::new()).await
    });

    let a = handle_a.await.expect("A 协程完成");
    let b = handle_b.await.expect("B 协程完成");
    let c = handle_c.await.expect("C 协程完成");
    let _ = handle_r.await;

    Verdict { a, b, c }
  })
}

#[test]
fn exec_lock_contention_with_parked_blpop_does_not_starve_worker() {
  let (verdict_tx, verdict_rx) = channel::<Verdict>();
  // 后台线程承载单 worker Runtime；主线程有界等待，将死锁挂起转可判别 RED
  let worker = thread::spawn(move || {
    let verdict = run_scenario();
    // 回传失败仅因主线程已判超时，忽略即可
    let _ = verdict_tx.send(verdict);
  });

  match verdict_rx.recv_timeout(Duration::from_secs(8)) {
    Ok(verdict) => {
      assert!(
        verdict.c == b"+PONG\r\n",
        "B 取锁争用期间同 worker 其它连接必须仍可应答（worker 未被饿死），实际 C 应答 {:?}",
        String::from_utf8_lossy(&verdict.c)
      );
      assert!(
        verdict.b.starts_with(b"+OK\r\n+QUEUED\r\n"),
        "B 事务应答前缀异常: {:?}",
        String::from_utf8_lossy(&verdict.b)
      );
      assert_eq!(
        verdict.b,
        b"+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n",
        "争用后 B 的 EXEC 必须完整成功（键集未丢、SET 生效）：+OK +QUEUED *1 +OK，实际 {:?}",
        String::from_utf8_lossy(&verdict.b)
      );
      assert!(
        verdict.a.starts_with(b"+OK\r\n+QUEUED\r\n"),
        "A 事务应答前缀异常: {:?}",
        String::from_utf8_lossy(&verdict.a)
      );
      // 收敛后回收后台线程（此时场景已完成，不会再自旋）
      let _ = worker.join();
    }
    Err(RecvTimeoutError::Timeout) => {
      panic!(
        "死锁：单 worker 上 EXEC 取锁同步自旋饿死同 worker —— BLPOP-in-MULTI 挂起持闩时，\
         同桶 MULTI/EXEC 自旋霸占协程调度器，PING 探针与 CLIENT UNBLOCK 解除永不获调度，\
         场景在 8s 时限内无法收敛"
      );
    }
    Err(RecvTimeoutError::Disconnected) => {
      panic!("后台场景线程 panic/崩溃（详见测试输出中的 panic 回溯）");
    }
  }
}
