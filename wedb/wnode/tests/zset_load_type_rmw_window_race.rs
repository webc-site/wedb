#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! zset 装载型写臂 RMW 窗 + 落笔复验并发回归（票 wnode-zset-load-type-write-bypass-rmw-window-residual）
//!
//! 父票（load-type 9e7ec24c）修复 list/set/hash/geo 十二命名臂并把判定核收敛到
//! [`rmw_helpers`] 单一面，zset 侧同形裸臂为其留尾：冷态 `store_dest_cold` /
//! `combine_store_cold` / `zrangestore_cold`、BZPOPMIN/BZPOPMAX 与 ZMPOP 冷态取件臂、
//! 同步档 STORE 族与立即可取臂、经纪出件 `zset_outcome` 皆裸装载裸写回，与
//! run_sync_rmw/run_async_rmw 骨架保护分叉：同键并发交错丢已 ACK 写 / 复活已删键 /
//! 造双域并存。
//!
//! 修法（本回归锁定的机制判据，逐臂复用父件套件、零新裁决）：
//! - 异步臂装载前 `rmw_window` 让核取窗跨「装载 → 求值/弹出 → 写回」全程
//!   （判据与 envelope_count_correct_race / load_type_rmw_window_race 同规格：
//!   同键第二窗取闩失败 = 臂在手；修复前臂根本不取窗，判据永不成立即炸出）；
//! - 落笔前 obj_writeback_recheck_async（取件族，按装载既存态）/
//!   obj_current_domain_async + obj_save_recheck_async（STORE 覆写族，按开窗时刻
//!   存活域）复验域归属，对面窗内 DEL/SET 交叠即拒写按存储忙交回重试；
//! - 同步档取窗失败沿用既有 Ok(false) 异步重放通道（占窗会话在位时同步臂必让位，
//!   修复前直出移动/写回应答即炸出）。
//!
//! 交叠构造为确定性 poll_fn 注入（判据成立才放行对面命令，不赌调度器、无 sleep）。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::MessageConsumerFace;
use wnode_test::{
  DrivePlan, PendingReadDevice, bulk_array, consumer_on, envelope_record_present, feed, roundtrip,
};
use wtest_base::test_store_config;

type TestStore = WedbStore<PendingReadDevice<SegmentedDevice>>;

fn open_store(tag: &str) -> (Arc<TestStore>, TempDir) {
  let dir = tempdir().unwrap();
  let device = Arc::new(PendingReadDevice::new(Arc::new(
    SegmentedDevice::single_file(dir.path().join(tag)).unwrap(),
  )));
  (
    Arc::new(WedbStore::open(test_store_config(), device).unwrap()),
    dir,
  )
}

/// 冷键 BZPOPMIN 持窗 + 对面 ZADD 被窗挡：取件臂修复机制（同锁源串行化）直判——
/// 修复前逐键裸装载裸写回（判据永不成立即炸出），旧快照可顶掉窗内已 ACK 成员；
/// 修复后终态已 ACK 元素零丢失
#[test]
fn bzpopmin_cold_window_serializes_concurrent_zadd_no_lost_ack() {
  let (store, _dir) = open_store("zt-rmw-bzpop-zadd.db");
  let rt = Runtime::new().unwrap();
  let key = b"zt:pop:race".to_vec();
  let members: Vec<Vec<u8>> = (1..=10).map(|i| format!("e{i:02}").into_bytes()).collect();
  {
    let mut c = consumer_on(&store);
    let scores: Vec<Vec<u8>> = (1..=10).map(|i| i.to_string().into_bytes()).collect();
    let mut cmd: Vec<&[u8]> = vec![b"ZADD", &key];
    for (m, s) in members.iter().zip(scores.iter()) {
      cmd.push(s.as_slice());
      cmd.push(m.as_slice());
    }
    assert_eq!(roundtrip(&rt, &mut c, &cmd), b":10\r\n");
  }
  // 冷化：BZPOPMIN 同步臂立即可取路径预探降级 → 慢路径逐键取窗装载臂
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (interleaved, pop_reply, zadd_reply) = wnode_test::drive_interleaved(
    &rt,
    &store,
    DrivePlan::load_arm(
      &[b"BZPOPMIN", &key, b"0"],
      &key,
      vec![
        b"ZADD".to_vec(),
        key.clone(),
        b"11".to_vec(),
        b"e11".to_vec(),
      ],
      true,
    ),
  );
  assert!(
    interleaved,
    "交叠判据未成立：BZPOPMIN 取件臂未在装载期持窗（修复前裸执行形态，用例失效须炸出）"
  );
  assert!(
    pop_reply.starts_with(b"*3\r\n$11\r\nzt:pop:race\r\n$3\r\ne01\r\n"),
    "BZPOPMIN 应答帧走样：{:?}",
    String::from_utf8_lossy(&pop_reply)
  );
  assert_eq!(
    zadd_reply,
    b":1\r\n",
    "对面 ZADD 应在 BZPOPMIN 闭环后重放 ACK：{:?}",
    String::from_utf8_lossy(&zadd_reply)
  );

  // 终态不变式：已 ACK 元素零丢失 + 弹出元素不复返
  let mut c = consumer_on(&store);
  let mut want: Vec<&[u8]> = members.iter().skip(1).map(Vec::as_slice).collect();
  want.push(b"e11");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGE", &key, b"0", b"-1"]),
    bulk_array(&want),
    "BZPOPMIN 旧快照顶掉了窗内已 ACK 的 ZADD 元素"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", &key]), b":10\r\n");
}

/// 冷键 ZMPOP 持窗 + 窗内真 DEL：装载已见既存态则落笔复验必拒写（存储忙），
/// 装载已见删除态则短路 nil 零写回——两形皆合法，盲写复活即炸出
#[test]
fn zmpop_cold_window_concurrent_del_is_never_resurrected() {
  let (store, _dir) = open_store("zt-rmw-zmpop-del.db");
  let rt = Runtime::new().unwrap();
  let key = b"zt:del:race".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &[b"ZADD", &key, b"1", b"a", b"2", b"b", b"3", b"c"]
      ),
      b":3\r\n"
    );
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (interleaved, pop_reply, del_reply) = wnode_test::drive_interleaved(
    &rt,
    &store,
    DrivePlan::load_arm(
      &[b"ZMPOP", b"1", &key, b"MIN"],
      &key,
      vec![b"DEL".to_vec(), key.clone()],
      true,
    ),
  );
  assert!(interleaved, "交叠判据未成立：注入 DEL 没落进 ZMPOP 窗口");
  assert_eq!(del_reply, b":1\r\n", "对面 DEL 须在 ZMPOP 闭环后完整回执");
  assert!(
    pop_reply.starts_with(b"*2\r\n"),
    "ZMPOP 闭环应答走样：{:?}",
    String::from_utf8_lossy(&pop_reply)
  );

  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"EXISTS", &key]),
    b":0\r\n",
    "已 ACK 删除的键被 ZMPOP 旧快照盲写复活"
  );
  assert!(
    !envelope_record_present(&rt, &store, &key),
    "已 ACK 删除的信封物理记录被 ZMPOP 尾段回写"
  );
}

/// 冷键 ZDIFFSTORE 目标键持窗 + 对面 ZADD dst 被窗挡：STORE 覆写臂修复机制直判——
/// 修复前 store_dest_cold 裸 obj_save 覆写整个目标键（判据永不成立即炸出），
/// 窗内已 ACK 的 ZADD 成员被整值抹掉；修复后覆写与增量写严格串行，终态零丢失
#[test]
fn zdiffstore_cold_dest_window_serializes_concurrent_zadd_no_lost_ack() {
  let (store, _dir) = open_store("zt-rmw-diffstore-zadd.db");
  let rt = Runtime::new().unwrap();
  let src = b"zt:diff:src".to_vec();
  let dst = b"zt:diff:dst".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"ZADD", &src, b"1", b"a", b"2", b"b"]),
      b":2\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"ZADD", &dst, b"9", b"z"]),
      b":1\r\n"
    );
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (interleaved, store_reply, zadd_reply) = wnode_test::drive_interleaved(
    &rt,
    &store,
    DrivePlan::load_arm(
      &[b"ZDIFFSTORE", &dst, b"1", &src],
      &dst,
      vec![b"ZADD".to_vec(), dst.clone(), b"5".to_vec(), b"c".to_vec()],
      true,
    ),
  );
  assert!(
    interleaved,
    "交叠判据未成立：store_dest_cold 未在目标键落笔期持窗（修复前裸覆写形态）"
  );
  assert_eq!(store_reply, b":2\r\n", "ZDIFFSTORE 应答基数应为源集合基数");
  assert_eq!(
    zadd_reply,
    b":1\r\n",
    "对面 ZADD 应在 ZDIFFSTORE 闭环后重放 ACK：{:?}",
    String::from_utf8_lossy(&zadd_reply)
  );

  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGE", &dst, b"0", b"-1"]),
    bulk_array(&[b"a", b"b", b"c"]),
    "ZDIFFSTORE 旧覆写顶掉了窗内已 ACK 的 ZADD 成员（丢已确认写入）"
  );
}

/// 同步档 STORE 覆写臂让位判据：目标键窗被占会话持有，暖态 ZRANGESTORE 同步臂
/// 必整体让位走既有 Ok(false) 异步重放通道（修复前裸执行直出 `:2` 即炸出），
/// 放行使后覆写正常闭环
#[test]
fn zrangestore_sync_arm_yields_to_dest_window_holder_then_replays() {
  let (store, _dir) = open_store("zt-rmw-rangestore-yield.db");
  let rt = Runtime::new().unwrap();
  let src = b"zt:rs:src".to_vec();
  let dst = b"zt:rs:dst".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"ZADD", &src, b"1", b"a", b"2", b"b"]),
      b":2\r\n"
    );
  }

  // 占窗会话：持目标键 dst RMW 窗
  let hold_sess = store.new_session().expect("占窗会话");
  let hold_batch = hold_sess.enter_batch();
  let mut holder = Some(
    hold_batch
      .try_rmw_window(&dst)
      .expect("占窗会话取 dst 键窗必成"),
  );

  let mut c = consumer_on(&store);
  let sync_out = feed(&mut c, &[b"ZRANGESTORE", &dst, &src, b"0", b"-1"]);
  assert!(
    sync_out.is_empty(),
    "dst 键窗被占时 ZRANGESTORE 同步臂必须让位异步重放（修复前裸执行直出移动应答）：{:?}",
    String::from_utf8_lossy(&sync_out)
  );
  let slow = c.take_slow_wait().expect("ZRANGESTORE 让位后必挂慢路径");
  drop(holder.take());
  let reply = rt.block_on(slow.resolve());
  assert_eq!(reply, b":2\r\n");

  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGE", &dst, b"0", b"-1"]),
    bulk_array(&[b"a", b"b"])
  );
}

/// 同步档取件臂让位判据 + 跨域让位：暖态 BZPOPMIN 逐键臂在键窗被占时必让位
///（修复前裸装载裸写回直出三员组即炸出）；放行使后正常出件且对面已 ACK 增量不丢
#[test]
fn bzpopmin_sync_arm_yields_to_key_window_holder_then_replays() {
  let (store, _dir) = open_store("zt-rmw-bzpop-sync-yield.db");
  let rt = Runtime::new().unwrap();
  let key = b"zt:bzsync".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"ZADD", &key, b"1", b"m1", b"2", b"m2"]),
      b":2\r\n"
    );
  }

  let hold_sess = store.new_session().expect("占窗会话");
  let hold_batch = hold_sess.enter_batch();
  let mut holder = Some(hold_batch.try_rmw_window(&key).expect("占窗会话取键窗必成"));

  let mut c = consumer_on(&store);
  let sync_out = feed(&mut c, &[b"BZPOPMIN", &key, b"0"]);
  assert!(
    sync_out.is_empty(),
    "键窗被占时 BZPOPMIN 同步取件臂必须让位异步重放（修复前裸执行直出弹出应答）：{:?}",
    String::from_utf8_lossy(&sync_out)
  );
  let slow = c.take_slow_wait().expect("BZPOPMIN 让位后必挂慢路径");
  drop(holder.take());
  let reply = rt.block_on(slow.resolve());
  assert!(
    reply.starts_with(b"*3\r\n$9\r\nzt:bzsync\r\n$2\r\nm1\r\n"),
    "放行后 BZPOPMIN 应正常出件：{:?}",
    String::from_utf8_lossy(&reply)
  );

  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGE", &key, b"0", b"-1"]),
    bulk_array(&[b"m2"])
  );
}

/// 无竞争回归（单线程逐字节）：并窗+复验后 zset 装载型写全家族应答帧与修复前
/// 基态逐字节一致（暖态同步臂 + 冷键慢路径臂两侧），复验绝不沦为「一律弃写」开关
#[test]
fn uncontended_zset_load_type_family_reply_bytes_unchanged() {
  let (store, _dir) = open_store("zt-rmw-uncontended.db");
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  // 聚合与覆写族（同步档）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"uc:a", b"1", b"a", b"2", b"b"]),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"uc:b", b"1", b"b", b"3", b"c"]),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZDIFFSTORE", b"uc:d", b"1", b"uc:a"]),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"ZINTERSTORE", b"uc:i", b"2", b"uc:a", b"uc:b"]
    ),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"ZUNIONSTORE", b"uc:u", b"2", b"uc:a", b"uc:b"]
    ),
    b":3\r\n"
  );
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"ZRANGESTORE", b"uc:g", b"uc:a", b"0", b"-1"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZDIFFSTORE", b"uc:g", b"1", b"uc:none"]),
    b":0\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"uc:g"]), b":0\r\n");

  // 取件族（同步档立即可取路径，无经纪注入）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"uc:p", b"1", b"p1", b"2", b"p2"]),
    b":2\r\n"
  );
  assert!(
    roundtrip(&rt, &mut c, &[b"BZPOPMIN", b"uc:p", b"0"])
      .starts_with(b"*3\r\n$4\r\nuc:p\r\n$2\r\np1\r\n")
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZMPOP", b"1", b"uc:p", b"MAX"]),
    b"*2\r\n$4\r\nuc:p\r\n*1\r\n*2\r\n$2\r\np2\r\n$1\r\n2\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZMPOP", b"1", b"uc:p", b"MIN"]),
    b"$-1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"BZMPOP", b"0", b"1", b"uc:none", b"MIN"]),
    b"$-1\r\n"
  );

  // 冷键慢路径臂（异步取窗 + 落笔复验放行）：全家族应答逐字节不变
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"uc:cold", b"1", b"x", b"2", b"y"]),
    b":2\r\n"
  );
  drop(c);
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"ZRANGESTORE", b"uc:cd", b"uc:cold", b"0", b"-1"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZDIFFSTORE", b"uc:ci", b"1", b"uc:cold"]),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"ZINTERSTORE", b"uc:cj", b"2", b"uc:cold", b"uc:cd"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGE", b"uc:cj", b"0", b"-1"]),
    bulk_array(&[b"x", b"y"])
  );
  assert!(
    roundtrip(&rt, &mut c, &[b"BZPOPMIN", b"uc:cold", b"0"])
      .starts_with(b"*3\r\n$7\r\nuc:cold\r\n$1\r\nx\r\n")
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZMPOP", b"1", b"uc:cold", b"MAX"]),
    b"*2\r\n$7\r\nuc:cold\r\n*1\r\n*2\r\n$1\r\ny\r\n$1\r\n2\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b"uc:cold"]), b":0\r\n");
  // 弹出至空后信封整键回收，写热路径延续自洽
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"uc:cold", b"7", b"z"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGE", b"uc:cold", b"0", b"-1"]),
    bulk_array(&[b"z"])
  );
  // 冷键 BZMPOP 批量臂（COUNT 越界钳至基数）出件后整键回收
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"BZMPOP", b"0", b"1", b"uc:cold", b"MIN", b"COUNT", b"5"]
    ),
    b"*2\r\n$7\r\nuc:cold\r\n*1\r\n*2\r\n$1\r\nz\r\n$1\r\n7\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"uc:cold"]), b":0\r\n");
  // 字符串域短路臂：WRONGTYPE 错误帧形态不变
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"uc:str", b"v"]),
    b"+OK\r\n"
  );
  let wrong_type = roundtrip(&rt, &mut c, &[b"BZPOPMIN", b"uc:str", b"0"]);
  assert!(
    wrong_type.starts_with(b"-WRONGTYPE"),
    "BZPOPMIN 命中字符串域应报 WRONGTYPE：{:?}",
    String::from_utf8_lossy(&wrong_type)
  );
  let store_wrong_type = roundtrip(&rt, &mut c, &[b"ZDIFFSTORE", b"uc:dst2", b"1", b"uc:str"]);
  assert!(
    store_wrong_type.starts_with(b"-WRONGTYPE"),
    "ZDIFFSTORE 源命中字符串域应报 WRONGTYPE：{:?}",
    String::from_utf8_lossy(&store_wrong_type)
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"uc:str"]), b"$1\r\nv\r\n");
}
