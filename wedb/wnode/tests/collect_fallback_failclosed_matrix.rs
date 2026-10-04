//! 收集族 / RMW 收尾「同步降级绝不转异步盲写」fail-closed 契约回归
//!（票 wnode-collect-fallback-blind-write-after-recheck）
//!
//! 缺陷形：`obj_save_or_gc` / `obj_save_custom_notified` 同步快路径页翻转 /
//! TTL 磁盘候选降级返回 `Ok(false)` 后，三条臂改走异步 fallback 盲写——
//! 落笔前复验（`obj_writeback_recheck_sync` / `obj_save_recheck_async`）先于
//! 同步写发生，fallback 闭环（`delete_string` / `obj_save` → `upsert_tag`
//! 异步 I/O 窗，无记录闩）跨 await 无再裁决，窗口期对面 DEL/SET 交叠即复活
//! 已删键 / 信封与 String 双域并存 / 空集陈旧墓碑误删刚 ACK 的新值。收口后：
//! 正身收集臂（collect_hash_key / collect_sorted_set_key）`Ok(false)` 弃写
//! 留待下一轮周期收集（与复验不过臂同款）；同型二 custom_object_rmw_async
//! 弃写按存储忙交回客户端重试；同型三 run_async_rmw→apply_rmw_post_operate
//! 收尾面页翻转瞬态走「复验 → 同步快写」原地重试闭环（C# InternalRMW pending
//! → CompletePending 驱动 evict → 同条 RMW 重试对位，瞬态等待不外显错误帧；
//! 门禁契约 store_dest_cold_upgrade 同源），重试窗复验不过仍存储忙。
//!
//! 判据（全部确定性，零调度赌注，严禁编造跨 await 间隙的时序赌注）：
//! [`OBJ_SAVE_PAGESWAP_INJECT`] 页翻转桩（debug 档一次性）强制同步快路径判
//! 降级（零真实写入），逐面断言：
//! 1. 弃写零写入：注入轮信封字节不变（HLEN / ZCARD / R.GETBIT 维持收集前态）；
//! 2. 三向终态语义面：不复活（弃写轮后 DEL 正常闭环、键不复活）/ 不双域
//!    （弃写轮后 SET 单域 String）/ 不误删（无桩空集收集删空自愈照常）；
//! 3. 收敛：清桩后下一轮收集固化成功（弃写不丢收集能力，下轮 disk_count
//!    与收集后计数仍不等，门控不短路）；
//! 4. 同型二：自定义对象 R.SETBIT 冷键慢臂降级按存储忙拒绝且位图零写入；
//! 5. 同型三：冷键 HSET 收尾降级原地重试静默闭环成功（桩判据被消费 + 应答
//!    :0 + 值 = 本轮求值结果，无陈旧快照盲写、无错误帧）。
//!
//! C# 契约（判据来源）：对象求值与写回同记录锁内（HashOps.cs:HashCollect →
//! :601 RMWObjectStoreOperation；SortedSetOps.cs:SortedSetCollect 同形；
//! RMWMethods.cs InitialUpdater / CopyUpdater），「装载 → 写回」间隙结构性
//! 不存在，降级盲写形态在 C# 无对应物；页翻转瞬态的 C# 对位是 pending 重试
//! 循环（成功回基数，不外显存储错误）。

#![cfg(debug_assertions)]

use std::{sync::atomic::Ordering, time::Duration};

use compio::{runtime::Runtime, time::sleep};
use parking_lot::Mutex;
use wnode::{MessageConsumerFace, RespSessionConsumer, storage::OBJ_SAVE_PAGESWAP_INJECT};
use wnode_test::consumer_on;
use wtest_base::{open_test_store, resp_frame as frame};

/// 桩 [`OBJ_SAVE_PAGESWAP_INJECT`] 是进程级一次性 static，而本目标各用例经
/// `cargo test` 默认多线程并行——并发用例（flush_and_evict_all 慢用例尤甚）
/// 会在置桩用例的慢路径窗内抢消费桩，把「下一写判降级」判据偷走（表现为
/// async_rmw 用例间歇冒答成功）。全局互斥把同文件用例串行化，桩的消费序
/// 由构造封闭（对标 delempty_parity_locks 同款进程级共享态串行先例）
static SERIAL: Mutex<()> = Mutex::new(());

/// 字段/成员级短 TTL（object_collect_task 同款量级）
const FIELD_TTL_MS: &str = "150";
/// 过期等待（> TTL，runtime 内 await 驱动 timer）
const EXPIRY_WAIT_MS: u64 = 250;

/// 单命令往返（慢路径 SlowWait 泵，collect_arm_rmw_window_scope 同款形态）
fn send(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  assert!(
    c.try_consume_messages_into(&mut out).is_some(),
    "帧须被消费循环消化: {args:?}"
  );
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async { out.extend_from_slice(&slow.resolve().await) });
  }
  out
}

/// 预置带一个短 TTL 字段（f2）与一个常驻字段（f1）的 hash
async fn seed_hash_with_expiry(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8]) {
  assert_eq!(
    send(rt, c, &[b"HSET", key, b"f1", b"v1", b"f2", b"v2"]),
    b":2\r\n"
  );
  assert_eq!(
    send(
      rt,
      c,
      &[
        b"HPEXPIRE",
        key,
        FIELD_TTL_MS.as_bytes(),
        b"FIELDS",
        b"1",
        b"f2"
      ]
    ),
    b"*1\r\n:1\r\n"
  );
  sleep(Duration::from_millis(EXPIRY_WAIT_MS)).await;
}

/// HCOLLECT 注入轮弃写零写入 + 三向终态 + 下一轮收敛 + 删空自愈回归
///
/// 「零写入」判据：注入轮后桩被消费（同步快写降级判据命中、零落笔复位）且
/// 键存活原态——中途禁触 HLEN/HGET 等读面（水位越线信封的物化矫正/过期升格
/// 写回会自行固化信封，污染弃写观测），只走 EXISTS/TYPE 域探针
#[test]
fn hash_collect_pageswap_degrade_discards_write() {
  let _serial = SERIAL.lock();
  let (_dir, store) = open_test_store("collect-fallback-hash-failclosed.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  rt.block_on(async {
    seed_hash_with_expiry(&rt, &mut c, b"fb:hash").await;

    // 注入轮：页翻转桩强制同步快路径降级 → 弃写零写入
    OBJ_SAVE_PAGESWAP_INJECT.store(true, Ordering::SeqCst);
    assert_eq!(
      send(&rt, &mut c, &[b"HCOLLECT", b"fb:hash"]),
      b"+OK\r\n",
      "收集臂弃写应答须恒 +OK（与复验不过臂同款，留待下一轮）"
    );
    assert!(
      !OBJ_SAVE_PAGESWAP_INJECT.load(Ordering::SeqCst),
      "弃写证据：信封同步快写口的降级判据须被命中并复位（零落笔）"
    );
    assert_eq!(
      send(&rt, &mut c, &[b"EXISTS", b"fb:hash"]),
      b":1\r\n",
      "弃写零写入：键存活原态（修复前 fallback await 盲写固化/复活面）"
    );
    assert_eq!(
      send(&rt, &mut c, &[b"TYPE", b"fb:hash"]),
      b"+hash\r\n",
      "不双域：注入轮后仍单域信封"
    );

    // 不复活：弃写轮（零写入）后 DEL 正常闭环，键消亡态保持
    assert_eq!(send(&rt, &mut c, &[b"DEL", b"fb:hash"]), b":1\r\n");
    assert_eq!(
      send(&rt, &mut c, &[b"EXISTS", b"fb:hash"]),
      b":0\r\n",
      "不复活：弃写轮后 DEL 闭环，键不得因信封残留复活"
    );

    // 收敛：桩一次性已消费，下一轮收集固化成功（弃写不丢收集能力）
    let mut c2 = consumer_on(&store);
    rt.block_on(async {
      seed_hash_with_expiry(&rt, &mut c2, b"fb:hash2").await;
      OBJ_SAVE_PAGESWAP_INJECT.store(true, Ordering::SeqCst);
      assert_eq!(send(&rt, &mut c2, &[b"HCOLLECT", b"fb:hash2"]), b"+OK\r\n");
      assert!(
        !OBJ_SAVE_PAGESWAP_INJECT.load(Ordering::SeqCst),
        "注入轮弃写零写入（桩判据命中即复位）"
      );
      assert_eq!(
        send(&rt, &mut c2, &[b"HCOLLECT", b"fb:hash2"]),
        b"+OK\r\n",
        "清桩后下一轮收集固化"
      );
      // 固化后再走读面观测（信封已无过期字段，读面无升格写回干扰）
      assert_eq!(
        send(&rt, &mut c2, &[b"HLEN", b"fb:hash2"]),
        b":1\r\n",
        "下一轮门控不短路（disk_count=2 ≠ 收集后 1），固化收敛"
      );
      assert_eq!(
        send(&rt, &mut c2, &[b"HGET", b"fb:hash2", b"f1"]),
        b"$2\r\nv1\r\n"
      );
      assert_eq!(
        send(&rt, &mut c2, &[b"HGET", b"fb:hash2", b"f2"]),
        b"$-1\r\n"
      );
    });
  });
}

/// 不双域 + 不误删面：弃写轮后 SET 单域 String；无桩空集收集删空自愈照常
#[test]
fn hash_collect_degrade_then_set_stays_single_domain() {
  let _serial = SERIAL.lock();
  let (_dir, store) = open_test_store("collect-fallback-dual-domain.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  rt.block_on(async {
    seed_hash_with_expiry(&rt, &mut c, b"fb:dual").await;
    OBJ_SAVE_PAGESWAP_INJECT.store(true, Ordering::SeqCst);
    assert_eq!(send(&rt, &mut c, &[b"HCOLLECT", b"fb:dual"]), b"+OK\r\n");
    // 对面 SET（String 域覆写，自带信封清退）：弃写轮零信封写入，SET 后单域
    assert_eq!(send(&rt, &mut c, &[b"SET", b"fb:dual", b"str"]), b"+OK\r\n");
    assert_eq!(
      send(&rt, &mut c, &[b"GET", b"fb:dual"]),
      b"$3\r\nstr\r\n",
      "不双域：SET 新值须原样可读"
    );
    assert_eq!(
      send(&rt, &mut c, &[b"TYPE", b"fb:dual"]),
      b"+string\r\n",
      "不双域：域探针单域 String，信封不得残留（复活/双域面）"
    );
    assert_eq!(send(&rt, &mut c, &[b"DEL", b"fb:dual"]), b":1\r\n");

    // 不误删面回归：空集收集（无桩）删空自愈照常——键整键回收不留幽灵空信封
    assert_eq!(
      send(&rt, &mut c, &[b"HSET", b"fb:empty", b"only", b"v"]),
      b":1\r\n"
    );
    assert_eq!(
      send(
        &rt,
        &mut c,
        &[
          b"HPEXPIRE",
          b"fb:empty",
          FIELD_TTL_MS.as_bytes(),
          b"FIELDS",
          b"1",
          b"only"
        ]
      ),
      b"*1\r\n:1\r\n"
    );
    sleep(Duration::from_millis(EXPIRY_WAIT_MS)).await;
    assert_eq!(send(&rt, &mut c, &[b"HCOLLECT", b"fb:empty"]), b"+OK\r\n");
    assert_eq!(
      send(&rt, &mut c, &[b"EXISTS", b"fb:empty"]),
      b":0\r\n",
      "空集收集删空自愈（整键回收）须照常闭环"
    );
  });
}

/// ZCOLLECT 注入轮弃写零写入 + 收敛（与 hash 收集臂同款 fail-closed）
///
/// 观测纪律同 hash 面：注入轮后禁触 ZCARD/ZSCORE 等读面（过期升格写回会自行
/// 固化信封），收敛断言置于清桩固化之后
#[test]
fn zset_collect_pageswap_degrade_discards_write() {
  let _serial = SERIAL.lock();
  let (_dir, store) = open_test_store("collect-fallback-zset-failclosed.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  rt.block_on(async {
    assert_eq!(
      send(
        &rt,
        &mut c,
        &[b"ZADD", b"fb:zset", b"1", b"m1", b"2", b"m2"]
      ),
      b":2\r\n"
    );
    assert_eq!(
      send(
        &rt,
        &mut c,
        &[
          b"ZPEXPIRE",
          b"fb:zset",
          FIELD_TTL_MS.as_bytes(),
          b"MEMBERS",
          b"1",
          b"m2"
        ]
      ),
      b"*1\r\n:1\r\n"
    );
    sleep(Duration::from_millis(EXPIRY_WAIT_MS)).await;

    OBJ_SAVE_PAGESWAP_INJECT.store(true, Ordering::SeqCst);
    assert_eq!(send(&rt, &mut c, &[b"ZCOLLECT", b"fb:zset"]), b"+OK\r\n");
    assert!(
      !OBJ_SAVE_PAGESWAP_INJECT.load(Ordering::SeqCst),
      "弃写证据：信封同步快写口的降级判据须被命中并复位（零落笔）"
    );
    assert_eq!(
      send(&rt, &mut c, &[b"EXISTS", b"fb:zset"]),
      b":1\r\n",
      "弃写零写入：键存活原态"
    );
    assert_eq!(send(&rt, &mut c, &[b"TYPE", b"fb:zset"]), b"+zset\r\n");
    assert_eq!(
      send(&rt, &mut c, &[b"ZCOLLECT", b"fb:zset"]),
      b"+OK\r\n",
      "清桩后下一轮收集固化"
    );
    assert_eq!(
      send(&rt, &mut c, &[b"ZCARD", b"fb:zset"]),
      b":1\r\n",
      "下一轮门控不短路（disk_count=2 ≠ 收集后 1），固化收敛"
    );
    assert_eq!(
      send(&rt, &mut c, &[b"ZSCORE", b"fb:zset", b"m1"]),
      b"$1\r\n1\r\n"
    );
  });
}

/// 同型二：R.SETBIT 冷键慢臂（custom_object_rmw_async）同步写页翻转降级弃写
/// 按存储忙拒绝，位图零写入（修复前 upsert_tag await 盲写生效且冒答成功）
#[test]
fn custom_rmw_pageswap_degrade_rejects_busy_without_write() {
  let _serial = SERIAL.lock();
  let (_dir, store) = open_test_store("collect-fallback-custom-rmw.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  rt.block_on(async {
    // 建位图：offset 0 置 1（原位 0 → :0）
    assert_eq!(
      send(&rt, &mut c, &[b"R.SETBIT", b"fb:roar", b"0", b"1"]),
      b":0\r\n"
    );
    // 冷化落盘 → R.SETBIT 快路径磁盘候选降级，慢臂承接
    rt.block_on(store.flush_and_evict_all()).unwrap();

    OBJ_SAVE_PAGESWAP_INJECT.store(true, Ordering::SeqCst);
    let reply = send(&rt, &mut c, &[b"R.SETBIT", b"fb:roar", b"1", b"1"]);
    assert_eq!(
      reply, b"-ERR slow path storage error\r\n",
      "降级弃写须按存储忙交回客户端重试（绝不冒答成功）"
    );
    assert_eq!(
      send(&rt, &mut c, &[b"R.GETBIT", b"fb:roar", b"1"]),
      b":0\r\n",
      "弃写零写入：offset 1 不得被盲写置位"
    );
    assert_eq!(
      send(&rt, &mut c, &[b"R.GETBIT", b"fb:roar", b"0"]),
      b":1\r\n",
      "既有位数据不得被降级轮触碰"
    );
  });
}

/// 同型三：冷键 HSET 慢路径 run_async_rmw → apply_rmw_post_operate 收尾
/// obj_save 页翻转降级走原地重试闭环静默成功（C# InternalRMW pending →
/// CompletePending 驱动 evict → 同条 RMW 重试对位，门禁契约 store_dest_cold_
/// upgrade 同源：瞬态等待不外显错误帧）。三向洞封堵不回退的结构判据在实现面：
/// 重试每轮「复验（obj_save_recheck_async 单点）→ 同步快写」零让核，同步落笔
/// 与复验终判之间无插入窗；复验不过仍存储忙，绝不复辟「复验终判 → 异步
/// upsert_tag 落笔」的 await 间隙盲写闭环
#[test]
fn async_rmw_post_operate_pageswap_degrade_replays_inplace_to_success() {
  let _serial = SERIAL.lock();
  let (_dir, store) = open_test_store("collect-fallback-async-rmw.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  rt.block_on(async {
    assert_eq!(
      send(&rt, &mut c, &[b"HSET", b"fb:rmw", b"f", b"v1"]),
      b":1\r\n"
    );
    rt.block_on(store.flush_and_evict_all()).unwrap();

    // 页翻转桩强制收尾首写降级（重试循环首发命中即消费复位）→ 瞬态等待后
    // 重试闭环成功，命令正常回执绝不冒错误帧
    OBJ_SAVE_PAGESWAP_INJECT.store(true, Ordering::SeqCst);
    assert_eq!(
      send(&rt, &mut c, &[b"HSET", b"fb:rmw", b"f", b"v2"]),
      b":0\r\n",
      "页翻转瞬态须原地重试静默成功（C# 成功回基数对位，不得外显存储忙错误帧）"
    );
    assert!(
      !OBJ_SAVE_PAGESWAP_INJECT.load(Ordering::SeqCst),
      "降级判据须被命中（首发 obj_save 确实走了页翻转降级路径）"
    );
    assert_eq!(
      send(&rt, &mut c, &[b"HGET", b"fb:rmw", b"f"]),
      b"$2\r\nv2\r\n",
      "重试闭环落笔 = 本轮求值结果 v2（重放按当前态装载求值，无陈旧快照盲写）"
    );
  });
}
