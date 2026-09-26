//! 信封计数矫正臂（HLEN/ZCARD 慢路径 envelope_length_correct）写回面并发回归
//!（票 wnode-envelope-length-correct-unwindowed-writeback）
//!
//! 缺陷形：矫正臂裸装载裸写回——obj_load_typed 前无 [`wkv::RmwWindow`] 用户键桶
//! 排他闩、obj_writeback_tiered 前无 obj_save_recheck_async 复验，与同步矫正臂
//!（HLEN 经 hash_length 降级由 envelope_length_correct 异步域承接，与 ZCARD 同步
//! 矫正臂 sorted_set_length_purged 经 run_sync_rmw 持
//! try_rmw_window + obj_save_recheck_sync 同一锁源）及 run_async_rmw 让核等待臂保护分叉：
//! - 已 ACK 写丢失：窗口期并发 HSET 已回执的字段被矫正臂以旧快照整值顶替；
//! - 已 ACK 键被误删 / 复活：剔空臂整键删除可连带窗口期并发新建，盲写可复活
//!   已 ACK 删除的键；
//! - 双域并存：窗口期并发 SET 清退信封写字符串后，盲写信封令同键两物理域并存。
//!
//! C# 无此面：HashLength / SortedSetLength 走 ReadObjectStoreOperation 纯读零写回
//!（HashOps.cs:449、SortedSetCommands.cs:100），rust「物化剔除 + 矫正写回」是
//! 本仓自研改良（collection.md §6 计数规约），其写回面必须与 RMW 骨架同一套
//! 窗口与复验判定（rmw_helpers.rs 模块头注登记），禁第二套裁决。
//!
//! 修法：装载前取 rmw_window（run_async_rmw 让核等待臂同款）跨「装载 → purge →
//! 写回」全程持闩 + 落笔前 obj_save_recheck_async 复验域归属，复验不过按存储忙
//! 交回客户端重试（RESP_ERR_SLOW_PATH_STORAGE 既有出口）。
//!
//! 交叠构造口径（确定性，不赌调度器、不留虚断言）：矫正臂持窗为让核等待臂，
//! 以「同键第二窗取闩失败」为窗口已在手的可观测判据，判据成立才放行对面命令
//! 完整落地；对面 HSET 自身取窗（被矫正臂挡到窗后重放），DEL/SET 取物理记录键
//! 桶闩可在窗内完整 ACK——三形分别对应三危害。无并发写者的两条回归用例为反空
//! 跑对照：水位越线矫正应答精确、剔空自愈行为不变，复验绝不沦为「一律弃写」。

use std::{future::poll_fn, pin::pin, sync::Arc, task::Poll, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::{err_frame, feed, roundtrip};
use wresp::cmd_strings::{RESP_ERR_SLOW_PATH_STORAGE, RESP_ERR_WRONG_TYPE};
use wtest_base::open_test_store;
use wval::KeyTag;

type TestStore = WedbStore<SegmentedDevice>;

/// 弃写臂应答帧（矫正臂复验不过按存储忙交回客户端重试）
fn busy_frame() -> Vec<u8> {
  err_frame(RESP_ERR_SLOW_PATH_STORAGE)
}

/// 独立连接装配（生产 thread-per-core 形态：对面写者与本臂各持一份会话）
fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 异步域单命令往返（融合轮询注入臂专用：可 poll 的 future，绝不再起嵌套 block_on）
async fn deliver(store: Arc<TestStore>, args: Vec<Vec<u8>>) -> Vec<u8> {
  let slices: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
  let mut c = consumer_on(&store);
  let mut out = feed(&mut c, &slices);
  if let Some(slow) = c.take_slow_wait() {
    out.extend_from_slice(&slow.resolve().await);
  }
  out
}

/// 窗口已在手的可观测判据：同键第二窗取闩失败（对面 DEL/SET 按物理记录键取闩、
/// 根本不取本窗，故本判据只反映矫正臂的持窗事实）
fn rmw_window_held(store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("判据会话");
  let batch = sess.enter_batch();
  batch.try_rmw_window(key).is_none()
}

/// HEXPIRE 单字段成功回执帧：C# 对位 `HashCommands.cs:582 HashExpire` 恒为逐字段
/// 状态数组（NOTFOUND 路径 :651-657 `TryWriteArrayLength(numFields)` + 循环写 -2），
/// 绝非裸整数——单 FIELDS 成功即 `*1\r\n:1\r\n`
const HEXPIRE_ONE_OK: &[u8] = b"*1\r\n:1\r\n";

/// 信封物理记录是否在场（双域并存 / 键复活判据：读侧按记录类型分类，DEL 墓碑
/// 记录即判缺）
fn envelope_record_present(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("取证会话");
  let rec_k = sess.session_tag_key(KeyTag::ObjectEnvelope, key);
  rt.block_on(sess.read_raw(&rec_k))
    .expect("信封物理记录读取不得报存储错误")
    .is_some()
}

/// 暖态信封基态：一存活字段 + 一短 TTL 到期字段（信封头部水位越线，HLEN/ZCARD
/// 慢路径矫正臂的常态触发面——携带字段级 TTL 且有到期成员的键）
fn seed_crossed_watermark(store: &Arc<TestStore>, key: &[u8], live: &[u8], exp: &[u8]) {
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", key, live, b"v1"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", key, exp, b"x"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HEXPIRE", key, b"1", b"FIELDS", b"1", exp]),
    HEXPIRE_ONE_OK,
    "前置判据：到期字段 HEXPIRE 应回执"
  );
  sleep(Duration::from_millis(1200));
}

/// 驱动水位越线 HLEN 慢路径（矫正臂）与对面命令的确定性交叠：以「同键第二窗
/// 取闩失败」为矫正臂持窗判据，判据成立才放行对面命令完整落地，再闭环矫正臂。
/// 回 `(交叠是否成立, HLEN 应答帧, 对面命令应答帧)`
fn drive_correct_with_intruder(
  store: &Arc<TestStore>,
  key: &[u8],
  intruder: Vec<Vec<u8>>,
) -> (bool, Vec<u8>, Vec<u8>) {
  let rt = Runtime::new().unwrap();
  // 冷化：暖态水位越线 HLEN 走同步物化矫正臂直出（该臂反空跑对照已由
  // uncontended 用例覆盖）；本 helper 交叠驱动的对象必为冷键慢路径装载矫正臂
  //（逐字节同 zset_load_type_rmw_window_race 冷化口径）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let mut c = consumer_on(store);
  let sync_out = feed(&mut c, &[b"HLEN", key]);
  assert!(
    sync_out.is_empty(),
    "水位越线 HLEN 应挂慢路径，实际同步段直出 {:?}",
    String::from_utf8_lossy(&sync_out)
  );
  let slow = c.take_slow_wait().expect("信封矫正臂必挂慢路径");

  let mut rmw = pin!(slow.resolve());
  let inject_store = Arc::clone(store);
  let mut inject = pin!(deliver(inject_store, intruder));
  let probe_store = Arc::clone(store);
  let probe_key = key.to_vec();
  let mut window_held = false;
  let mut inject_out: Option<Vec<u8>> = None;
  let hlen_reply = rt.block_on(poll_fn(|cx| {
    if !window_held {
      // 交叠判据未成立的轮次只推矫正臂：判据成立前对面命令绝不放行
      //（否则注入落在取窗之前，交叠根本不成立）
      return match rmw.as_mut().poll(cx) {
        Poll::Ready(out) => Poll::Ready(out),
        Poll::Pending => {
          window_held = rmw_window_held(&probe_store, &probe_key);
          Poll::Pending
        }
      };
    }
    // 窗口在手：先推进对面命令（DEL/SET 取物理记录键桶闩可完整 ACK；HSET 取
    // 本窗被挡到窗后），再推矫正臂闭环
    if inject_out.is_none()
      && let Poll::Ready(out) = inject.as_mut().poll(cx)
    {
      inject_out = Some(out);
    }
    match rmw.as_mut().poll(cx) {
      Poll::Ready(out) => Poll::Ready(out),
      Poll::Pending => Poll::Pending,
    }
  }));
  // HSET 形的对面闭环在矫正臂释放窗之后（让核等待臂），轮询退出后补齐
  let intruder_reply = match inject_out {
    Some(out) => out,
    None => rt.block_on(inject.as_mut()),
  };
  (window_held, hlen_reply, intruder_reply)
}

/// 反空跑回归（无并发写者）：水位越线 HLEN 矫正应答精确存活数，矫正物理发生
///（到期字段惰性剔除 + 升格写回，其后头部水位前移回归 O(1) 直读），复验对
/// 「状态未被触碰」放行写回，绝不沦为「一律弃写」开关
#[test]
fn uncontended_watermark_cross_hlen_corrects_exact_and_stays() {
  let (_dir, store) = open_test_store("env-count-cross.db").unwrap();
  let key = b"env:count:cross".to_vec();
  seed_crossed_watermark(&store, &key, b"f1", b"fx");

  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HLEN", &key]),
    b":1\r\n",
    "水位越线矫正应答 = 存活字段数（剔除到期 fx）"
  );
  // 矫正后：再次 HLEN 头部直读恒精确，到期字段读路径视同不存在
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", &key]), b":1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HEXISTS", &key, b"fx"]),
    b":0\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HEXISTS", &key, b"f1"]),
    b":1\r\n"
  );
  // 矫正写回后新写入照常（复验放行的正向对照延伸）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", &key, b"f2", b"v2"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", &key]), b":2\r\n");
}

/// 反空跑回归（无并发写者）：全成员到期剔空自愈——HLEN 应答 :0 且整键回收，
/// 与同步矫正臂删空语义一致
#[test]
fn uncontended_all_expired_hlen_selfheals_to_zero() {
  let (_dir, store) = open_test_store("env-count-selfheal.db").unwrap();
  let key = b"env:count:selfheal".to_vec();
  seed_crossed_watermark(&store, &key, b"f1", b"fx");
  // 存活字段也短过期：整键全到期
  let rt = Runtime::new().unwrap();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &[b"HEXPIRE", &key, b"1", b"FIELDS", b"1", b"f1"]
      ),
      HEXPIRE_ONE_OK
    );
  }
  sleep(Duration::from_millis(1200));

  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HLEN", &key]),
    b":0\r\n",
    "全成员到期矫正应答 :0（剔空自愈）"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"EXISTS", &key]),
    b":0\r\n",
    "剔空自愈应整键回收"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", &key]), b":0\r\n");
}

/// 矫正臂持窗 + 窗内并发 HSET：已 ACK 的新字段绝不被矫正旧快照整值顶替
///（危害形一——修复前裸写回臂与 HSET 写臂交错即丢字段）；HSET 被窗口挡到
/// 矫正闭环后重放，终态含矫正剔除的到期字段与 ACK 新字段，计数精确
#[test]
fn correct_window_concurrent_hset_acked_field_survives() {
  let (_dir, store) = open_test_store("env-count-hset-race.db").unwrap();
  let key = b"env:count:hset:race".to_vec();
  seed_crossed_watermark(&store, &key, b"f1", b"fx");

  let (interleaved, hlen_reply, hset_reply) = drive_correct_with_intruder(
    &store,
    &key,
    vec![
      b"HSET".to_vec(),
      key.clone(),
      b"f2".to_vec(),
      b"v2".to_vec(),
    ],
  );
  assert!(
    interleaved,
    "交叠判据未成立：矫正臂在取窗之前即闭环，注入 HSET 根本没落进窗口（用例失效须炸出）"
  );
  assert_eq!(
    hlen_reply,
    b":1\r\n",
    "矫正臂应答 = 存活数（剔 fx 存 f1）：{:?}",
    String::from_utf8_lossy(&hlen_reply)
  );
  assert_eq!(
    hset_reply,
    b":1\r\n",
    "窗内并发 HSET 应在矫正闭环后重放 ACK：{:?}",
    String::from_utf8_lossy(&hset_reply)
  );

  // 终态不变式：ACK 字段存活、到期字段已剔、计数精确（已 ACK 写丢失即此处红）
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HEXISTS", &key, b"f2"]),
    b":1\r\n",
    "已 ACK 的 HSET 字段被矫正臂旧快照整值顶替"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HEXISTS", &key, b"f1"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HEXISTS", &key, b"fx"]),
    b":0\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", &key]), b":2\r\n");
}

/// 矫正臂持窗 + 窗内真 DEL：已 ACK 删除的键绝不被矫正旧快照盲写复活（危害形二）
#[test]
fn correct_window_concurrent_del_is_never_resurrected() {
  let (_dir, store) = open_test_store("env-count-del-race.db").unwrap();
  let key = b"env:count:del:race".to_vec();
  seed_crossed_watermark(&store, &key, b"f1", b"fx");

  let (interleaved, hlen_reply, del_reply) =
    drive_correct_with_intruder(&store, &key, vec![b"DEL".to_vec(), key.clone()]);
  assert!(
    interleaved,
    "交叠判据未成立：注入 DEL 没落进窗口（用例失效须炸出）"
  );
  assert_eq!(
    del_reply,
    b":1\r\n",
    "前置判据：对面 DEL 须在矫正臂持窗期内完整回执：{:?}",
    String::from_utf8_lossy(&del_reply)
  );
  // DEL 进窗互斥：若 DEL 挡到窗后，矫正臂先回存活数 :1，随后 DEL 落库删除；
  // 若窗内遇装载已见删除态则回 :0，旧视图复验拒绝则回存储忙。终态键绝不复活
  assert!(
    hlen_reply == b":0\r\n".to_vec()
      || hlen_reply == busy_frame()
      || hlen_reply == b":1\r\n".to_vec(),
    "DEL 后矫正臂应答走样：{:?}",
    String::from_utf8_lossy(&hlen_reply)
  );

  // 键不复活：存活判缺 + 信封物理记录缺席
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"EXISTS", &key]),
    b":0\r\n",
    "已 ACK 删除的键被矫正臂尾段盲写复活"
  );
  assert!(
    !envelope_record_present(&rt, &store, &key),
    "已 ACK 删除的信封物理记录被矫正臂回写"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", &key]), b":0\r\n");
}

/// 矫正臂持窗 + 窗内真 SET：同键绝不容两物理域并存（危害形三），TYPE 与 GET
/// 结论一致
#[test]
fn correct_window_concurrent_set_keeps_single_domain() {
  let (_dir, store) = open_test_store("env-count-set-race.db").unwrap();
  let key = b"env:count:set:race".to_vec();
  seed_crossed_watermark(&store, &key, b"f1", b"fx");

  let (interleaved, hlen_reply, set_reply) = drive_correct_with_intruder(
    &store,
    &key,
    vec![b"SET".to_vec(), key.clone(), b"hello".to_vec()],
  );
  assert!(
    interleaved,
    "交叠判据未成立：注入 SET 没落进窗口（用例失效须炸出）"
  );
  assert_eq!(
    set_reply,
    b"+OK\r\n",
    "前置判据：对面 SET 须在矫正臂持窗期内完成覆写：{:?}",
    String::from_utf8_lossy(&set_reply)
  );
  // SET 进窗互斥：若 SET 挡到窗后，矫正臂先回存活数 :1，随后 SET 覆写；
  // 若窗内旧视图复验拒绝则回存储忙。终态同键绝不容两物理域并存
  assert!(
    hlen_reply == b":0\r\n".to_vec()
      || hlen_reply == busy_frame()
      || hlen_reply == b":1\r\n".to_vec(),
    "SET 覆写后矫正臂应答走样：{:?}",
    String::from_utf8_lossy(&hlen_reply)
  );

  // 只存一个物理域：存活域判 String，信封物理记录缺席（双域并存即此处红）
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  assert!(
    !envelope_record_present(&rt, &store, &key),
    "SET 已清退的信封记录被矫正臂尾段回写：同键双物理域并存"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"TYPE", &key]), b"+string\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GET", &key]),
    b"$5\r\nhello\r\n",
    "TYPE 与 GET 结论发散（双域并存的必然后果）"
  );
  // 键已归字符串域（信封物理记录缺席已断言）：HLEN 对字符串键按引擎探测树
  // String 反探单点（step_string_domain）回 WRONGTYPE，绝非 :0——:0 只有
  // 信封域存活时可达，此处恰是双域并存被禁的反证
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HLEN", &key]),
    err_frame(RESP_ERR_WRONG_TYPE)
  );
}
