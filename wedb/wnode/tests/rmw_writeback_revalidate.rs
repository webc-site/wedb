//! 对象 RMW 写回前终态复验并发回归（票 r6-del-rmw-writeback-revalidate）
//!
//! 缺陷形一（键复活）：集合族 RMW 的 [`wkv::RmwWindow`] 只持**用户键**桶排他闩，
//! 对面 DEL 只取**信封物理记录键**（`prefix|KeyTag::ObjectEnvelope|user_key`）的记录
//! 桶闩（garnet 单份哈希表故同键同桶，wedb 同键各域各自成键故两个不同基），窗口期内
//! DEL 可整删信封并回执 `:1`，RMW 尾段以装载时旧视图盲写即令已 ACK 删除的键复活。
//! 缺陷形二（双域并存）：同上交叠面下 SET 先清退信封再写字符串，RMW 尾段盲写信封即令
//! 同键同时驻留 String 与 ObjectEnvelope 两物理域，打破「用户键同一时刻至多驻留一个
//! 物理域」不变式（字符串面与对象面各按自家探测序作答，TYPE / GET 与集合命令结论发散）。
//! C# 无此二形：对象求值与写回全在记录 X 锁内
//!（libs/server/Storage/Functions/ObjectStore/RMWMethods.cs 的 CopyUpdater /
//! InPlaceUpdaterWorker），同键覆写与删除互斥于同一条记录锁
//!（MainStore/UpsertMethods.cs:InPlaceWriter、MainStore/DeleteMethods.cs:InitialDeleter），
//!「装载 → 写回」间隙根本不存在。
//! 修法：落笔前终态复验（[`wnode::resp::objects::object_store_utils::obj_save_recheck_sync`]
//! 与同名异步档）——装载时刻存活域 ≠ 落笔时刻存活域即弃写，按命令语义走既有降级 /
//! 存储忙信号，绝不尾段盲写；DEL/SET 两入口照旧不取本窗（双锁序死锁面）。
//!
//! 交叠构造口径（确定性，不赌调度器、不留虚断言）：
//! - 同步臂 [`run_sync_rmw`] 全程无让核点，故以零容量通道在 `run_op` 闭包内会合——
//!   该闭包恰被生产骨架在「装载在手、写回未落」的窗内时刻调用，对面线程得以完整落地
//!   一条真 DEL / 真 SET；
//! - 异步臂 `run_async_rmw` 跨读内核 await，故单线程融合轮询：以「同键第二窗取闩失败」
//!   为窗口已在手的可观测判据，判据成立才放行注入臂完整落地，再闭环 RMW；判据不成立
//!   即交叠未成立，用例直接炸出（绝不允许静默空跑）；
//! - `uncontended_*` 两条为反空跑对照：无对面写者时复验必须放行写回，杜绝把修法写成
//!   「一律弃写」的虚设实现。

use std::{
  future::poll_fn,
  pin::pin,
  sync::{
    Arc,
    mpsc::{self, Receiver, SyncSender},
  },
  task::Poll,
  thread,
};

use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wcol::{
  ObjectOutput,
  hash::hash_object::{HashObject, HashOperation},
  object_payload::{GarnetObjectPayload, ObjLoad},
};
use wconf::DEFAULT_RESP_VERSION;
use wdev::SegmentedDevice;
use wkv::{BatchStoreSession, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::StoreGarnetApi,
    objects::object_store_utils::{RespRmwDone, SyncRmwCmd, SyncRmwHandlers, run_sync_rmw},
    resp_server_session::RespServerSessionOptions,
  },
  storage::session::common::ttl_sync::probe_alive_domain,
};
use wnode_test::err_frame;
use wresp::cmd_strings::{RESP_ERR_SLOW_PATH_STORAGE, RESP_ERR_WRONG_TYPE};
use wtest_base::test_store_config;
use wval::{GarnetObjectType, KeyTag};

type TestStore = WedbStore<SegmentedDevice>;

/// 弃写臂应答帧（异步臂已是终态重放面，弃写即按存储忙交回客户端重试）
fn busy_frame() -> Vec<u8> {
  err_frame(RESP_ERR_SLOW_PATH_STORAGE)
}

fn open_store(tag: &str) -> (Arc<TestStore>, TempDir) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  (
    Arc::new(WedbStore::open(test_store_config(), device).unwrap()),
    dir,
  )
}

/// 独立连接装配（生产 thread-per-core 形态：对面写者与本臂各持一份会话）
fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
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

/// 喂一帧并取同步段即时输出（慢路径挂起态由调用方处置）
fn feed(c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let _ = c.try_consume_messages_into(&mut out);
  out
}

/// 单命令往返：同步段无输出且挂起慢路径时，以 block_on 承担网络泵角色闭环
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut out = feed(c, args);
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async { out.extend_from_slice(&slow.resolve().await) });
  }
  out
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
/// 根本不取本窗，故本判据只反映 RMW 方的持窗事实，与「交叠是否已成立」一一对应）
fn rmw_window_held(store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("判据会话");
  let batch = sess.enter_batch();
  batch.try_rmw_window(key).is_none()
}

/// 信封物理记录是否在场（双域并存判据：读侧按记录类型分类，DEL 墓碑记录即判缺）
fn envelope_record_present(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("取证会话");
  let rec_k = sess.session_tag_key(KeyTag::ObjectEnvelope, key);
  rt.block_on(sess.read_raw(&rec_k))
    .expect("信封物理记录读取不得报存储错误")
    .is_some()
}

/// 哈希族真 DEL（对面写者线程与主线程共用同一帧装配口径）
fn del_frame(key: &[u8]) -> Vec<Vec<u8>> {
  vec![b"DEL".to_vec(), key.to_vec()]
}

/// 生产同步骨架直调（与哈希族壳体逐参同形）：交叠闸门设在 `run_op` 闭包内
fn hash_rmw_with_gate(
  store: &BatchStoreSession<'_, SegmentedDevice>,
  key: &[u8],
  field: &[u8],
  value: &[u8],
  held: SyncSender<()>,
  acked: Receiver<()>,
) -> ObjLoad<RespRmwDone> {
  let args: Vec<&[u8]> = vec![field, value];
  run_sync_rmw(
    store,
    SyncRmwCmd {
      key,
      tag: GarnetObjectType::Hash,
      op: HashOperation::Hset,
      args: &args,
      arg1: 0,
      arg2: 0,
    },
    // 本形态负载由外层按 result1 补整数，骨架只在 operate 臂直写输出尾段
    &mut Vec::new(),
    SyncRmwHandlers::new(
      HashObject::from_blob,
      HashObject::new,
      |o: &HashObject| o.is_empty(),
      |o: &HashObject| o.to_blob(),
      move |obj: &mut HashObject, op: HashOperation, args: &[&[u8]], output: &mut Vec<u8>| {
        // 「装载在手、写回未落」的窗内时刻：先宣告持窗，再等对面命令完整回执
        held.send(()).expect("持窗宣告送达");
        acked.recv().expect("对面命令已回执");
        let mut obj_out = ObjectOutput::mount(output);
        obj.operate(op as u8, args, 0, 0, &mut obj_out, DEFAULT_RESP_VERSION);
        obj_out
      },
      // 写回面恒取：本用例唯一的弃写判点就是落笔前复验，不掺 should_write_back 面
      |_, _, _, _| true,
    ),
  )
}

/// 无对面写者的正向对照（同步臂）：复验必须放行正常写回，绝不得沦为「一律弃写」
#[test]
fn uncontended_sync_window_still_writes_back() {
  let (store, _dir) = open_store("reval-ctl-sync.db");
  let key = b"reval:ctl:sync".to_vec();
  let rt = Runtime::new().unwrap();
  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", &key, b"f1", b"v1"]),
    b":1\r\n"
  );

  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  let (held_tx, held_rx) = mpsc::sync_channel::<()>(0);
  let (acked_tx, acked_rx) = mpsc::sync_channel::<()>(0);
  // 会合照旧，只是对面不做任何写：复验在「状态未被触碰」时必须放行
  let watcher = thread::spawn(move || {
    held_rx.recv().expect("RMW 已持窗装载");
    acked_tx.send(()).expect("空对面回执送达");
  });
  let got = hash_rmw_with_gate(&batch, &key, b"f2", b"v2", held_tx, acked_rx);
  watcher.join().expect("对面线程无 panic");
  assert!(
    matches!(got, ObjLoad::Present(_)),
    "无并发覆写时复验误弃写（复验不得成为一律降级开关）：{got:?}"
  );
  assert_eq!(
    probe_alive_domain(&batch, &key).expect("存活域探针不得报存储错误"),
    Some(Some(KeyTag::ObjectEnvelope))
  );
  drop(batch);
  let mut c = consumer_on(&store);
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", &key]), b":2\r\n");
}

/// 无对面写者的正向对照（异步臂）：冷键慢路径重放照常闭环写回
#[test]
fn uncontended_async_window_still_writes_back() {
  let (store, _dir) = open_store("reval-ctl-async.db");
  let key = b"reval:ctl:async".to_vec();
  let (interleaved, reply) = drive_rmw_with_intruder(&store, &key, None);
  assert!(!interleaved, "本对照不注入对面写者，交叠判据不得成立");
  assert_eq!(reply, b":1\r\n", "冷键异步臂正常写回被误弃");
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", &key]), b":2\r\n");
}

/// 同步臂 + 窗内真 DEL：已 ACK 删除的键绝不被旧视图写回复活
#[test]
fn sync_window_concurrent_del_is_never_resurrected() {
  let (store, _dir) = open_store("reval-sync-del.db");
  let key = b"reval:sync:del".to_vec();
  let rt = Runtime::new().unwrap();
  // 基态：信封 {f1} 常驻内存（暖态，命令必走生产 run_sync_rmw）
  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", &key, b"f1", b"v1"]),
    b":1\r\n"
  );

  // 零容量通道两步会合：A 宣告持窗已装载 → B 落一条真 DEL → B 宣告已回执
  let (held_tx, held_rx) = mpsc::sync_channel::<()>(0);
  let (acked_tx, acked_rx) = mpsc::sync_channel::<()>(0);
  let del_store = Arc::clone(&store);
  let del_key = key.clone();
  let deleter = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    held_rx.recv().expect("RMW 已持窗装载");
    let mut c = consumer_on(&del_store);
    let reply = roundtrip(&rt, &mut c, &[b"DEL", &del_key]);
    acked_tx.send(()).expect("DEL 回执宣告送达");
    reply
  });

  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  let got = hash_rmw_with_gate(&batch, &key, b"f2", b"v2", held_tx, acked_rx);
  let del_reply = deleter.join().expect("DEL 线程无 panic");
  assert_eq!(
    del_reply, b":1\r\n",
    "前置判据：对面 DEL 必在 RMW 持窗期内取到信封记录并回执（本窗不挡对面即本票前提）"
  );
  assert!(
    matches!(got, ObjLoad::Degrade),
    "窗内已 ACK 的 DEL 之后，装载旧视图绝不得写回（应弃写交既有降级信号转异步重放），实际 {got:?}"
  );

  // 键不复活：存活域探针判缺 + 信封物理记录缺席
  assert_eq!(
    probe_alive_domain(&batch, &key).expect("存活域探针不得报存储错误"),
    Some(None),
    "已 ACK 删除的键被 RMW 尾段盲写复活"
  );
  assert!(
    !envelope_record_present(&rt, &store, &key),
    "已 ACK 删除的信封物理记录被 RMW 尾段回写"
  );
  drop(batch);

  // 复验不得把正常写回一并堵死：降级信号交回的异步重放按当前态重建，
  // 只含 f2，已删字段 f1 绝不回归
  let mut c = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", &key, b"f2", b"v2"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"HLEN", &key]), b":1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HEXISTS", &key, b"f1"]),
    b":0\r\n",
    "已 ACK 删除的字段在重放写回中复活"
  );
}

/// 同步臂 + 窗内真 SET：同键绝不容两物理域并存，TYPE 与 GET 结论必须一致
#[test]
fn sync_window_concurrent_set_keeps_single_domain() {
  let (store, _dir) = open_store("reval-sync-set.db");
  let key = b"reval:sync:set".to_vec();
  let rt = Runtime::new().unwrap();
  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", &key, b"f1", b"v1"]),
    b":1\r\n"
  );

  let (held_tx, held_rx) = mpsc::sync_channel::<()>(0);
  let (acked_tx, acked_rx) = mpsc::sync_channel::<()>(0);
  let set_store = Arc::clone(&store);
  let set_key = key.clone();
  let setter = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    held_rx.recv().expect("RMW 已持窗装载");
    let mut c = consumer_on(&set_store);
    let reply = roundtrip(&rt, &mut c, &[b"SET", &set_key, b"hello"]);
    acked_tx.send(()).expect("SET 回执宣告送达");
    reply
  });

  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  let got = hash_rmw_with_gate(&batch, &key, b"f2", b"v2", held_tx, acked_rx);
  let set_reply = setter.join().expect("SET 线程无 panic");
  assert_eq!(
    set_reply, b"+OK\r\n",
    "前置判据：对面 SET 须在 RMW 持窗期内完成覆写"
  );
  assert!(
    matches!(got, ObjLoad::Degrade),
    "窗内已 ACK 的 SET 之后，装载时的信封旧视图绝不得写回，实际 {got:?}"
  );

  // 只存一个物理域：存活域判 String，信封物理记录缺席（双域并存即此处红）
  assert_eq!(
    probe_alive_domain(&batch, &key).expect("存活域探针不得报存储错误"),
    Some(Some(KeyTag::String)),
    "SET 覆写后对象信封域仍在场：同键双物理域并存"
  );
  assert!(
    !envelope_record_present(&rt, &store, &key),
    "SET 已清退的信封记录被 RMW 尾段回写：同键双物理域并存"
  );
  drop(batch);

  // TYPE 与 GET 结论一致，且对象写回臂按字符串在场判 WRONGTYPE（类型面不发散）
  let mut c = consumer_on(&store);
  assert_eq!(roundtrip(&rt, &mut c, &[b"TYPE", &key]), b"+string\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", &key]), b"$5\r\nhello\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", &key, b"f2", b"v2"]),
    err_frame(RESP_ERR_WRONG_TYPE),
    "字符串在场时对象写回臂未过类型门：两域并存或类型面发散"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"TYPE", &key]),
    b"+string\r\n",
    "被拒对象写回不得改动已 ACK 的覆写态"
  );
}

/// 异步臂 + 窗内真 DEL（冷信封，装载跨读内核 await）：旧视图一律弃写
#[test]
fn async_window_concurrent_del_is_never_resurrected() {
  let (store, _dir) = open_store("reval-async-del.db");
  let key = b"reval:async:del".to_vec();
  let (interleaved, reply) = drive_rmw_with_intruder(&store, &key, Some(del_frame(&key)));
  assert!(
    interleaved,
    "交叠闸门未成立：RMW 在取窗之前即闭环，注入 DEL 根本没落进窗口（用例失效须炸出）"
  );
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  // 弃写臂回存储忙交回重试；若异步装载已见删除后终态则按新建语义回执——两态皆
  // 合法，但已 ACK 删除的字段 f1 绝不允许出现在终值里，也不得新增错误形态
  assert!(
    reply == busy_frame() || reply == b":1\r\n",
    "异步臂弃写应答形态走样：{:?}",
    String::from_utf8_lossy(&reply)
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HEXISTS", &key, b"f1"]),
    b":0\r\n",
    "窗内已 ACK 删除的字段被异步臂尾段盲写复活"
  );
  if reply == busy_frame() {
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"EXISTS", &key]),
      b":0\r\n",
      "弃写臂留下半程写入：已 ACK 删除的键被复活"
    );
    assert!(!envelope_record_present(&rt, &store, &key));
  }
}

/// 异步臂 + 窗内真 SET：终态只存一个物理域且 TYPE 与 GET 结论一致
#[test]
fn async_window_concurrent_set_keeps_single_domain() {
  let (store, _dir) = open_store("reval-async-set.db");
  let key = b"reval:async:set".to_vec();
  let (interleaved, reply) = drive_rmw_with_intruder(
    &store,
    &key,
    Some(vec![b"SET".to_vec(), key.clone(), b"hello".to_vec()]),
  );
  assert!(
    interleaved,
    "交叠闸门未成立：RMW 在取窗之前即闭环，注入 SET 根本没落进窗口（用例失效须炸出）"
  );
  assert!(
    reply == busy_frame() || reply == b":1\r\n" || reply == err_frame(RESP_ERR_WRONG_TYPE),
    "异步臂弃写应答形态走样：{:?}",
    String::from_utf8_lossy(&reply)
  );

  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  // 只存一个物理域：SET 在场即以 String 为唯一存活域，信封记录绝不得回写
  assert!(
    !envelope_record_present(&rt, &store, &key),
    "同键双物理域并存：SET 已清退的信封记录被异步臂尾段回写"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", &key]), b":1\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"TYPE", &key]), b"+string\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GET", &key]),
    b"$5\r\nhello\r\n",
    "TYPE 与 GET 结论发散（双域并存的必然后果）"
  );
}

/// 冷信封 → 驱动真 HSET 慢路径（生产异步臂）→ 持窗判据成立后放行注入臂完整落地
/// → 闭环 RMW。回 `(交叠是否成立, 应答帧)`；`intruder == None` 即正向对照（不注入）
fn drive_rmw_with_intruder(
  store: &Arc<TestStore>,
  key: &[u8],
  intruder: Option<Vec<Vec<u8>>>,
) -> (bool, Vec<u8>) {
  let rt = Runtime::new().unwrap();
  // 基态信封 {f1} 落盘冷化：磁盘候选令同步臂必 Degrade，命令交异步臂承接
  {
    let mut seed = consumer_on(store);
    assert_eq!(
      roundtrip(&rt, &mut seed, &[b"HSET", key, b"f1", b"v1"]),
      b":1\r\n"
    );
  }
  rt.block_on(store.flush_and_evict_all())
    .expect("冷化刷盘不得报错");

  let mut c = consumer_on(store);
  let sync_out = feed(&mut c, &[b"HSET", key, b"f2", b"v2"]);
  assert!(
    sync_out.is_empty(),
    "冷键 HSET 应挂异步慢路径，实际同步段直出 {:?}",
    String::from_utf8_lossy(&sync_out)
  );
  let slow = c.take_slow_wait().expect("冷信封键的 HSET 必挂慢路径");

  let mut rmw = pin!(slow.resolve());
  // 正向对照位：不注入对面写者时交叠判据恒不成立
  let no_intruder = intruder.is_none();
  let inject_store = Arc::clone(store);
  let mut inject = pin!(deliver(inject_store, intruder.unwrap_or_default()));
  let probe_store = Arc::clone(store);
  let probe_key = key.to_vec();
  let mut window_held = false;
  rt.block_on(poll_fn(|cx| {
    if no_intruder {
      // 正向对照：对面写者缺席，直接推进 RMW 至闭环
      return rmw.as_mut().poll(cx).map(|out| (false, out));
    }
    if !window_held {
      return match rmw.as_mut().poll(cx) {
        Poll::Ready(out) => Poll::Ready((false, out)),
        Poll::Pending => {
          // 窗口已在手才放行对面命令（否则注入落在取窗之前，交叠根本不成立）
          window_held = rmw_window_held(&probe_store, &probe_key);
          Poll::Pending
        }
      };
    }
    if inject.as_mut().poll(cx).is_ready() {
      return rmw.as_mut().poll(cx).map(|out| (true, out));
    }
    Poll::Pending
  }))
}
