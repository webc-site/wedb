#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 事务重放段降级慢臂事务锁模式跨段界下传集成测试（票 wnode-exec-replay-slow-body-relatch-outside-transactional-window-self-latch-collision-locktimeout）
//!
//! 断言矩阵：
//! (a) `VADD vk 1:2` → `MULTI` → `RENAME vk vk2` → `EXEC`：该元素非错误帧、vk 消失 vk2 在场；
//! (b) 事务内非向量冷记录降级写（`flush_and_evict_all` 构造 `RecordOnDisk`）：APPEND 成功且值正确；
//! (c) 自撞直读：`RMW_PLAN_ACQUIRE_MISS` 确证 EXEC 段内降级慢臂不产生失闩争用计数增量；
//! (d) 反向夹：事务外（非 MULTI）同一命令慢臂仍按 Basic 自取排他闩，并发持闩下必见争用计数或超时。

use std::sync::{Arc, atomic::Ordering};

use compio::runtime::Runtime;
use parking_lot::Mutex;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{RMW_PLAN_ACQUIRE_MISS, StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::{nested_frame as frame, vector_manager_of};
use wtxn::{TxnLockTable, WatchVersionMap};

fn test_env(
  tag: &str,
) -> (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  RespSessionConsumer,
  TxnLockTable,
) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let store = wnode_test::store_open(&dir, tag, config);
  let vm = vector_manager_of(&store);
  let api = Arc::new(
    StoreGarnetApi::new(store.new_session().unwrap()).with_vector_manager(Arc::clone(&vm)),
  );
  let mut consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api);
  let index_store = Arc::clone(&store);
  let lock_table = TxnLockTable::from_loader(move || index_store.index.load_full());
  consumer
    .session_mut()
    .attach_transaction_components(Arc::new(WatchVersionMap::new(64)), lock_table.clone());
  (dir, store, consumer, lock_table)
}

fn fp32_vec(seed: f32) -> Vec<u8> {
  [seed; 4].iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
  haystack.windows(needle.len()).any(|w| w == needle)
}

async fn pump(consumer: &mut RespSessionConsumer, bytes: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(bytes);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  loop {
    let remaining = consumer.try_consume_messages_into(&mut resp);
    if let Some(slow) = consumer.take_slow_wait() {
      let reply = slow.resolve().await;
      consumer.resolve_slow_wait_into(&reply, &mut resp);
      continue;
    }
    assert_eq!(remaining, Some(0), "帧应完整消费");
    break;
  }
  resp
}

static TEST_LOCK: parking_lot::Mutex<()> = Mutex::new(());

/// (a) + (c) 向量键 RENAME 在事务重放段降级慢臂后让闩复用，无 LockTimeout 错误元素且零失闩争用
#[test]
fn test_txn_vector_rename_slow_degrade_no_selflatch_collision() {
  let _lock = TEST_LOCK.lock();
  Runtime::new().unwrap().block_on(async {
    let (_dir, _store, mut consumer, _lock_table) = test_env("txn_vector_rename.db");

    // 1. 建立向量键 vk
    let vec_data = fp32_vec(1.0);
    let out = pump(
      &mut consumer,
      &frame(&[&[b"VADD", b"vk", b"FP32", &vec_data, b"e1", b"NOQUANT"]]),
    )
    .await;
    assert_eq!(out, b":1\r\n", "VADD 应成功写入");

    // 2. 清零争用计数口
    RMW_PLAN_ACQUIRE_MISS.store(0, Ordering::Relaxed);

    // 3. 事务内执行 RENAME vk vk2
    let txn_frame = frame(&[&[b"MULTI"], &[b"RENAME", b"vk", b"vk2"], &[b"EXEC"]]);
    let out = pump(&mut consumer, &txn_frame).await;

    // 4. 断言 (a): EXEC 回送非错误帧且包含 +OK
    assert!(
      contains_bytes(&out, b"+OK\r\n"),
      "EXEC 中 RENAME 元素应为 +OK，实得: {out:?}"
    );
    assert!(
      !contains_bytes(&out, b"-ERR"),
      "EXEC 应无错误帧，实得: {out:?}"
    );

    // 5. 断言 (c): 自撞直读——慢臂继承 Transactional 让闩，不产生取闩失手轮次计数增量
    let misses = RMW_PLAN_ACQUIRE_MISS.load(Ordering::Relaxed);
    assert_eq!(
      misses, 0,
      "EXEC 段内降级慢臂不应与自身事务持闩相撞产生 RMW_PLAN_ACQUIRE_MISS 计数"
    );

    // 6. 验证键状态：旧键消失，新键在场
    let check = pump(
      &mut consumer,
      &frame(&[&[b"EXISTS", b"vk"], &[b"EXISTS", b"vk2"]]),
    )
    .await;
    assert_eq!(check, b":0\r\n:1\r\n", "vk 应消失且 vk2 应在场");
  });
}

/// (b) 事务内非向量冷记录降级写（RecordOnDisk）：慢臂继承 Transactional，成功写入且零失闩自撞
#[test]
fn test_txn_cold_record_slow_degrade_no_selflatch_collision() {
  let _lock = TEST_LOCK.lock();
  Runtime::new().unwrap().block_on(async {
    let (_dir, store, mut consumer, _lock_table) = test_env("txn_cold_record.db");

    // 1. 写入普通字符串键
    let out = pump(&mut consumer, &frame(&[&[b"SET", b"cold_k", b"hello"]])).await;
    assert_eq!(out, b"+OK\r\n");

    // 2. 刷盘换出内存，令 cold_k 变为冷记录 (RecordOnDisk)
    store.flush_and_evict_all().await.unwrap();

    // 3. 清零争用计数口
    RMW_PLAN_ACQUIRE_MISS.store(0, Ordering::Relaxed);

    // 4. 事务内追加写入（APPEND 同步臂遇 RecordOnDisk 纯降级，交慢臂异步闭环）
    let txn_frame = frame(&[&[b"MULTI"], &[b"APPEND", b"cold_k", b"_world"], &[b"EXEC"]]);
    let out = pump(&mut consumer, &txn_frame).await;

    // 5. 断言 (b): 成功追加并返回新长度 11 ("hello_world")
    assert!(
      contains_bytes(&out, b":11\r\n"),
      "EXEC 中 APPEND 元素应返回新长度 11，实得: {out:?}"
    );
    assert!(!contains_bytes(&out, b"-ERR"), "EXEC 应无错误帧: {out:?}");

    // 6. 验证最终读取值
    let val = pump(&mut consumer, &frame(&[&[b"GET", b"cold_k"]])).await;
    assert_eq!(val, b"$11\r\nhello_world\r\n");

    // 7. 零争用失手计数
    assert_eq!(
      RMW_PLAN_ACQUIRE_MISS.load(Ordering::Relaxed),
      0,
      "冷记录慢臂不应自撞失闩"
    );
  });
}

/// (d) 反向夹：事务外（非 MULTI）同一命令慢臂仍按 Basic 自取排他闩，并发持闩下必见争用
#[test]
fn test_non_txn_slow_arm_keeps_basic_locking_contended() {
  let _lock = TEST_LOCK.lock();
  Runtime::new().unwrap().block_on(async {
    let (_dir, store, mut consumer, _lock_table) = test_env("non_txn_basic.db");

    // 1. 建立向量键 vk_solo
    let vec_data = fp32_vec(2.0);
    let out = pump(
      &mut consumer,
      &frame(&[&[b"VADD", b"vk_solo", b"FP32", &vec_data, b"e1", b"NOQUANT"]]),
    )
    .await;
    assert_eq!(out, b":1\r\n");

    // 2. 模拟另一独立会话持住 vk_solo 的主桶排他闩
    let sess2 = store.new_session().unwrap();
    let batch2 = sess2.enter_batch();
    let window2 = batch2
      .try_rmw_window(b"vk_solo")
      .expect("sess2 应成功获取 vk_solo 的 RMW 窗口");

    // 3. 清零争用计数口
    RMW_PLAN_ACQUIRE_MISS.store(0, Ordering::Relaxed);

    // 4. 在非事务模式下执行 RENAME vk_solo vk_solo2
    // 由于非事务，locking 模式为 Basic，慢臂将自取桶排他闩
    // 但 vk_solo 此时被 window2 持有，故必发生争用（失闩）并最终由于超时失败
    let out = pump(
      &mut consumer,
      &frame(&[&[b"RENAME", b"vk_solo", b"vk_solo2"]]),
    )
    .await;

    // 释放 sess2 的窗口
    drop(window2);
    drop(batch2);
    drop(sess2);

    // 5. 断言 (d):
    // 发生了取闩失手轮次（证明 Basic 模式确实进行了真取闩，绝非无条件 Transactional 让闩）
    let misses = RMW_PLAN_ACQUIRE_MISS.load(Ordering::Relaxed);
    assert!(
      misses > 0,
      "非事务下慢臂必须按 Basic 真取排他闩，被占用时必产生 RMW_PLAN_ACQUIRE_MISS 计数"
    );
    // 并且超时回显错误帧
    assert!(
      out.starts_with(b"-ERR"),
      "非事务争用超时应返回错误帧，实得: {out:?}"
    );
  });
}
