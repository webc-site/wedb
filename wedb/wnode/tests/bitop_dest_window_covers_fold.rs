//! BITOP 目的键读改写窗口罩逐源折叠回归（票 wnode-bitop-source-read-outside-dest-window）
//!
//! 缺陷形：BITOP 快慢双臂的目的键 `dest` 读改写窗口只罩「落笔段」，逐源折叠读
//! 游离窗外。dest∈srcs 自指形（BITOP OR/AND/NOT dst dst）下，折叠读 dest 与后续
//! 盲写之间无本键排他窗保护，并发持窗写者的已提交写会被旧折叠视图的尾段盲写
//! 顶掉（非可串行化）；对标 C# `BitmapOps.cs:StringBitOperation` keys[0] 的
//! Exclusive 记录闩自任何读发生前即建立、罩至 dest SET 全程（:88-98）。
//!
//! 修法（本回归锁定的机制判据）：双臂 dest 窗取闩前移至逐源折叠循环之前、跨
//! 「折叠读 → 求值 → 落笔」全程持有，失闩沿既有 `Ok(false)`／await 降级通道
//! 走慢路径同段持窗重放，不新建第二张锁表。
//!
//! 交叠构造为确定性 poll_fn 注入（判据成立才推进，不赌调度器）：冷键折叠读的
//! 首个让出点即逐源 `read_user_with_prefix().await` 的磁盘读——修复后该点 dest
//! 窗必已在手（取闩先于折叠），修复前窗仅在折叠完成后的落笔段短暂在手，故首个
//! 折叠让出点窗必未在手。以「同键第二窗取闩失败」为持窗可观测判据。

use std::{future::poll_fn, pin::pin, str::from_utf8, sync::Arc, task::Poll};

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

/// 独立连接装配（生产 thread-per-core 形态）
fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// `:N\r\n` 整数回执解析
fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

/// `$N\r\n…\r\n` bulk string 回执解析（nil 回 None）
fn reply_bulk(resp: &[u8]) -> Option<Vec<u8>> {
  let body = resp.strip_prefix(b"$")?;
  let nl = body.iter().position(|b| *b == b'\r')?;
  let len: usize = from_utf8(&body[..nl]).ok()?.parse().ok()?;
  let val = body.get(nl + 2..nl + 2 + len)?;
  Some(val.to_vec())
}

/// 读键值（单线程断言面；键缺失回 None）
fn get_of(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) -> Option<Vec<u8>> {
  let mut c = consumer_on(store);
  reply_bulk(&roundtrip(rt, &mut c, &[b"GET", key]))
}

/// dest 窗在手（臂已取窗）的可观测判据：同键第二窗取闩失败
fn rmw_window_held(store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("判据会话");
  let batch = sess.enter_batch();
  batch.try_rmw_window(key).is_none()
}

/// 冷键 victim 慢路径臂驱动至首个折叠让出点，记录该点 dest 窗是否在手，
/// 随后放行使臂闭环，回 `(首让出点持窗, victim 应答帧)`
fn drive_fold_first_suspend(
  rt: &Runtime,
  store: &Arc<TestStore>,
  victim_args: &[&[u8]],
  dest_key: &[u8],
) -> (bool, Vec<u8>) {
  let mut c = consumer_on(store);
  let sync_out = feed(&mut c, victim_args);
  assert!(
    sync_out.is_empty(),
    "冷键 BITOP 应挂慢路径，实际同步段直出 {:?}",
    String::from_utf8_lossy(&sync_out)
  );
  let slow = c.take_slow_wait().expect("冷键 BITOP 必挂慢路径");
  let mut fut = pin!(slow.resolve());
  let probe_store = Arc::clone(store);
  let key = dest_key.to_vec();
  let mut held = None;
  let reply = rt.block_on(poll_fn(|cx| match fut.as_mut().poll(cx) {
    Poll::Ready(out) => Poll::Ready(out),
    Poll::Pending => {
      if held.is_none() {
        // 首个让出点即逐源折叠的磁盘读：此时窗是否在手即为机制判据
        held = Some(rmw_window_held(&probe_store, &key));
      }
      Poll::Pending
    }
  }));
  (
    held.expect("victim 须在折叠读处至少让出一次（冷源磁盘读）"),
    reply,
  )
}

/// dest∈srcs 自指形（BITOP OR dst dst）：折叠读 dst 的首个让出点 dest 窗已在手
/// ——修复后取闩先于折叠成立；修复前窗仅罩落笔段，折叠读点窗未在手即炸出
#[test]
fn bitop_self_ref_fold_reads_inside_dest_window() {
  let (_dir, store) = open_test_store("bitop-win-selfref.db").unwrap();
  let rt = Runtime::new().unwrap();
  let dst = b"bw:sr:dst".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", &dst, b"\x0F\xF0"]),
      b"+OK\r\n"
    );
  }
  // 冷化：折叠读 dest 走磁盘异步读 → 慢路径首个让出点即折叠读
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (held_at_fold, reply) =
    drive_fold_first_suspend(&rt, &store, &[b"BITOP", b"OR", &dst, &dst], &dst);
  assert!(
    held_at_fold,
    "折叠读 dest 时 dest 读改写窗口未在手：窗外读旧值 + 窗内盲写的非可串行化形仍在位"
  );
  assert_eq!(
    reply, b":2\r\n",
    "BITOP OR dst dst 应答长度须为 2（实际 {reply:?}）"
  );
  // 自指 OR 恒等：终值与读前逐字节一致
  assert_eq!(
    get_of(&rt, &store, &dst).as_deref(),
    Some(b"\x0F\xF0".as_slice()),
    "自指 OR 折叠落笔不得改变 dest 内容"
  );
}

/// 多源形（BITOP OR dst a b）：首源折叠让出点 dest 窗已在手，且 dest 全程
/// 持窗罩住逐源折叠（对位 C# keys[0] Exclusive 自读起罩至 dest SET）
#[test]
fn bitop_multi_source_fold_reads_inside_dest_window() {
  let (_dir, store) = open_test_store("bitop-win-multisrc.db").unwrap();
  let rt = Runtime::new().unwrap();
  let dst = b"bw:ms:dst".to_vec();
  let a = b"bw:ms:a".to_vec();
  let b = b"bw:ms:b".to_vec();
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", &a, b"\xF0\x0F"]),
      b"+OK\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", &b, b"\x0F\xF0"]),
      b"+OK\r\n"
    );
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let (held_at_fold, reply) =
    drive_fold_first_suspend(&rt, &store, &[b"BITOP", b"OR", &dst, &a, &b], &dst);
  assert!(
    held_at_fold,
    "折叠读首源时 dest 读改写窗口未在手：dest 窗未罩逐源折叠全程"
  );
  assert_eq!(reply, b":2\r\n");
  // 0xF0|0x0F = 0xFF，0x0F|0xF0 = 0xFF
  assert_eq!(
    get_of(&rt, &store, &dst).as_deref(),
    Some(b"\xFF\xFF".as_slice()),
    "多源 OR 折叠落笔结果错误"
  );
}

/// 快路径结构性判据：外部会话持 dest 窗期间，BITOP 快路径须在逐源折叠之前即
/// 取窗失败降级（同步段零输出），而非先折叠检出源键 WRONGTYPE 再落笔——后者
/// 为修复前「窗仅罩落笔段、折叠读先于取窗」形态。放闩后慢路径重放仍检出
/// WRONGTYPE，检测面不丢（对位 C# keys[0] Exclusive 自读前建立）
#[test]
fn fast_path_acquires_dest_window_before_fold() {
  let (_dir, store) = open_test_store("bitop-win-fastpath.db").unwrap();
  let rt = Runtime::new().unwrap();
  let dst = b"bw:fp:dst";
  let obj = b"bw:fp:obj";
  {
    let mut c = consumer_on(&store);
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"HSET", obj, b"f", b"v"]),
      b":1\r\n"
    );
  }
  // 外部会话持 dest 窗
  let sess_w = store.new_session().unwrap();
  let batch_w = sess_w.enter_batch();
  let window = batch_w.try_rmw_window(dst).expect("占窗会话取 dest 窗");

  let mut c = consumer_on(&store);
  let mut out = feed(&mut c, &[b"BITOP", b"OR", dst, obj]);
  assert!(
    out.is_empty(),
    "dest 窗被占时 BITOP 快路径须先于折叠取窗失败降级（修复前折叠先检出源 \
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

/// 无竞争回归（单线程逐字节）：dest∈srcs 自指形各运算应答与键态与修复前
/// 基态一致，取闩前移绝不改变串行语义
#[test]
fn uncontended_bitop_self_ref_unchanged() {
  let (_dir, store) = open_test_store("bitop-win-uncontended.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);
  let k = b"bw:uc:k";

  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", k, b"d"]), b"+OK\r\n");
  // BITOP AND k k（dest=src 自身）：长度应答 1 + 值不变
  assert_eq!(roundtrip(&rt, &mut c, &[b"BITOP", b"AND", k, k]), b":1\r\n");
  assert_eq!(get_of(&rt, &store, k).as_deref(), Some(b"d".as_slice()));
  // BITOP OR k k：恒等
  assert_eq!(roundtrip(&rt, &mut c, &[b"BITOP", b"OR", k, k]), b":1\r\n");
  assert_eq!(get_of(&rt, &store, k).as_deref(), Some(b"d".as_slice()));
  // BITOP NOT k k：取反
  let d_not = !(b"d"[0]);
  assert_eq!(roundtrip(&rt, &mut c, &[b"BITOP", b"NOT", k, k]), b":1\r\n");
  assert_eq!(get_of(&rt, &store, k), Some(vec![d_not]));

  // 冷化后自指形仍串行（走慢路径折叠）
  drop(c);
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let mut c = consumer_on(&store);
  assert_eq!(
    reply_int(&roundtrip(&rt, &mut c, &[b"BITOP", b"OR", k, k])),
    Some(1)
  );
  assert_eq!(get_of(&rt, &store, k).as_deref(), Some([d_not].as_slice()));
}
