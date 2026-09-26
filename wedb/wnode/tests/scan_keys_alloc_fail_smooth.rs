//! KEYS / SCAN 应答物化分配失败的单会话平滑降级回归（票 zcode-r55-alloc）
//!
//! 票面命题：C# 堆分配失败以 `OutOfMemoryException` 形态被会话主循环
//! `catch (Exception)` 兜住（RespServerSession.cs:566-572 单会话 Dispose 收场，
//! 进程与其余连接存活）；rust std 容器分配失败走 `handle_alloc_error` → abort，
//! 不展开栈、不受 unwind/catch_unwind 兜底覆盖——单会话一次超额分配炸全进程。
//! 收口：scan/keys 应答物化点 `try_reserve` 前置（对齐 RecvAppend 既有平滑轨
//! 先例，单套机制），失败沿既有 whlog/wkv 错误通道上抛，慢路径统一降级
//! `RESP_ERR_SLOW_PATH_STORAGE` 错误帧，单会话收场进程存活。
//!
//! 故障注入为「大额分配失败」定向分配器（仅本测试二进制）：开关开启后
//! `>= THRESHOLD` 的分配返回空（模拟堆触顶），小额分配照常——正是「大额物化
//! 失败平滑、进程其余面存活」命题的直接对位；nextest 逐用例独立进程，注入器
//! 不跨用例泄漏，`InjectGuard` 兜底兜住同进程串行（`cargo test`）残留。

use std::{
  alloc::{GlobalAlloc, Layout, System},
  ptr,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  resp::{
    RespSessionConsumer,
    garnet_api::StoreGarnetApi,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::{OwnedActiveVectorSession, WedbVectorStoreCallbacks},
    },
  },
};
use wnode_test::{complete_len, err_frame, test_env};
use wresp::cmd_strings::RESP_ERR_SLOW_PATH_STORAGE;
use wtest_base::{resp_frame, test_store_config};
use wvector::Callbacks;

/// 大额分配阈值：注入器仅对 >= 该值的分配返空（64KB——覆盖键名快照倍增扩容
/// 臂与 SCAN 页预算门，不伤及引擎页级/索引级常规分配与运行时小额开销）
const THRESHOLD: usize = 64 * 1024;

/// 注入开关（关闭时分配器全通；store 装配与灌键必须在关闭态完成）
static INJECT: AtomicBool = AtomicBool::new(false);

/// 大额失败定向分配器：开关开启后 >= THRESHOLD 的 alloc 返空，其余原样转调
struct FailLarge;

// SAFETY: 开关关闭时逐参转调 System；开启后仅对 >= THRESHOLD 的分配返回空
// 指针（std 容器对该返回的处置即本票平滑收口的触发面），dealloc 逐参转调
unsafe impl GlobalAlloc for FailLarge {
  unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
    if INJECT.load(Ordering::Relaxed) && layout.size() >= THRESHOLD {
      return ptr::null_mut();
    }
    unsafe { System.alloc(layout) }
  }

  unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
    unsafe { System.dealloc(ptr, layout) }
  }
}

#[global_allocator]
static FAIL_LARGE: FailLarge = FailLarge;

use parking_lot::Mutex;

static SERIAL_LOCK: Mutex<()> = Mutex::new(());

/// 注入开关 RAII 兜底：同进程串行执行（非 nextest）时防护残留开关污染后续用例
struct InjectGuard;

impl Drop for InjectGuard {
  fn drop(&mut self) {
    INJECT.store(false, Ordering::Relaxed);
  }
}

/// 驱动一轮消费并回线面应答字节，返回 `(应答字节, 是否走慢路径)`
///
/// 慢路径应答经产线冲出口 [`RespServerSession::resolve_slow_wait_into`] 并入，
/// 与真实网络泵同一写出面（mget_slow_path_storage_error 同款泵夹具）
async fn pump(session: &mut RespServerSession, input: &[u8]) -> (Vec<u8>, bool) {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(input);
  session.bytes_read = input.len();
  session.read_head = 0;
  session.end_read_head = 0;
  session.output.clear();
  assert_eq!(
    session.try_consume_messages(),
    Some(0),
    "输入命令应整段消费完毕"
  );

  let mut wire = Vec::new();
  match session.take_slow_wait() {
    Some(slow) => {
      let reply = slow.resolve().await;
      session.resolve_slow_wait_into(&reply, &mut wire);
      (wire, true)
    }
    None => {
      session.take_output_into(&mut wire);
      (wire, false)
    }
  }
}

/// 装配 `n` 个热区字符串键的会话（内存域直扫，不触冷读页），并返回第二会话
/// 供「其余连接存活」断言
async fn keys_env(n: usize) -> (tempfile::TempDir, RespServerSession, RespServerSession) {
  let (dir, session, mut s) = test_env(false);
  {
    let batch = session.enter_batch();
    for i in 0..n {
      let key = format!("k:{i:07}");
      batch
        .try_upsert_sync(key.as_bytes(), b"v")
        .unwrap()
        .unwrap();
    }
  }
  // 第二会话在主会话 move 前自同一 store 开出（其余连接存活断言面）
  let session2 = session.store.new_session().unwrap();
  let mut s2 = RespServerSession::default();
  s2.set_garnet_api(Arc::new(StoreGarnetApi::new(session2)));
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));
  (dir, s, s2)
}

/// 键数：快照容器以每键 24B（`Vec`）记账，8000 键的倍增扩容请求量
///（`24 * len * 2`）越过 64KB 注入阈值，保证 KEYS 物化臂必踩注入
const KEY_COUNT: usize = 8000;

/// KEYS 全量物化分配失败：单会话回一条完整存储错误帧，连接与本进程全部存活
#[test]
fn keys_materialize_alloc_fail_degrades_to_error_frame() -> aok::Result<()> {
  let _serial = SERIAL_LOCK.lock();
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, mut s, mut s2) = keys_env(KEY_COUNT).await;
    let _guard = InjectGuard;
    INJECT.store(true, Ordering::Relaxed);

    let (wire, went_slow) = pump(&mut s, &resp_frame(&[b"KEYS", b"*"])).await;
    assert!(
      went_slow,
      "KEYS 须降级慢路径全量扫描，否则本用例不触达物化失败臂"
    );
    // 单条完整错误帧：物化失败以 RESP_ERR_SLOW_PATH_STORAGE 平滑降级，
    // 绝非 abort（进程能走到断言即已自证存活）
    assert_eq!(wire, err_frame(RESP_ERR_SLOW_PATH_STORAGE));
    assert_eq!(complete_len(&wire), Some(wire.len()));

    // 本会话续用无应答错位：纯内存快路径（PING / SET）照常服务
    let (ping, _) = pump(&mut s, &resp_frame(&[b"PING"])).await;
    assert_eq!(ping, b"+PONG\r\n");
    let (ok, went_slow) = pump(&mut s, &resp_frame(&[b"SET", b"after", b"v"])).await;
    assert!(!went_slow, "SET 走快路径");
    assert_eq!(ok, b"+OK\r\n");

    // 其余连接存活（C# 单会话 Dispose 对位）：第二会话 PING 正常应答
    let (pong, _) = pump(&mut s2, &resp_frame(&[b"PING"])).await;
    assert_eq!(pong, b"+PONG\r\n");
    Ok(())
  })
}

/// SCAN 页预算门分配失败：入口 `try_reserve` 平滑降级错误帧，连接续用存活
#[test]
fn scan_page_budget_alloc_fail_degrades_to_error_frame() -> aok::Result<()> {
  let _serial = SERIAL_LOCK.lock();
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, mut s, mut s2) = keys_env(KEY_COUNT).await;
    let _guard = InjectGuard;
    INJECT.store(true, Ordering::Relaxed);

    // COUNT 5000 的页预算 5000*24B = 120KB，越过 64KB 注入阈值
    let (wire, went_slow) = pump(&mut s, &resp_frame(&[b"SCAN", b"0", b"COUNT", b"5000"])).await;
    assert!(went_slow, "SCAN 须降级慢路径游标扫描");
    assert_eq!(wire, err_frame(RESP_ERR_SLOW_PATH_STORAGE));
    assert_eq!(complete_len(&wire), Some(wire.len()));

    // 连接续用：错误帧后游标命令照常服务（小额 COUNT 页不触注入）
    let (scan, went_slow) = pump(&mut s, &resp_frame(&[b"SCAN", b"0", b"COUNT", b"8"])).await;
    assert!(went_slow, "SCAN 恒降级慢路径");
    assert!(
      scan.starts_with(b"*2\r\n"),
      "小额页须正常回游标数组帧，实测 {scan:?}"
    );

    // 其余连接存活
    let (pong, _) = pump(&mut s2, &resp_frame(&[b"PING"])).await;
    assert_eq!(pong, b"+PONG\r\n");
    Ok(())
  })
}

/// 对照组：注入关闭时同一规模 KEYS 正常回全量数组（证明错误帧根因即大额
/// 分配失败注入，而非扫描本身缺陷）
#[test]
fn keys_materialize_succeeds_without_injection() -> aok::Result<()> {
  let _serial = SERIAL_LOCK.lock();
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, mut s, _s2) = keys_env(KEY_COUNT).await;

    let (wire, went_slow) = pump(&mut s, &resp_frame(&[b"KEYS", b"*"])).await;
    assert!(went_slow, "KEYS 降级慢路径全量扫描");
    // 数组头 + KEY_COUNT 个 bulk 键，帧结构完整
    let head = format!("*{KEY_COUNT}\r\n");
    assert!(
      wire.starts_with(head.as_bytes()),
      "全量数组头须为 *{KEY_COUNT}，实测前 32 字节 {:?}",
      &wire[..32.min(wire.len())]
    );
    assert_eq!(complete_len(&wire), Some(wire.len()));

    let (ping, _) = pump(&mut s, &resp_frame(&[b"PING"])).await;
    assert_eq!(ping, b"+PONG\r\n");
    Ok(())
  })
}

// ---- 巨 COUNT 入口预留封顶语义锁（票 wnode-scan-count-entry-reserve-unbounded）：
// C# NetworkSCAN/DbScan/AllocatorScan 对 COUNT 全程零按 count 分配，合法巨
// COUNT 照常一轮扫尽正常应答；本仓入口预算门预留额封顶后不得单方面拒服务 ----

/// 饱和 COUNT（i64::MAX，strict_i64 收下的合法最大值）：无封顶时 `as usize`
/// 后元素数乘 24B 于 Layout 构造乘法溢出 → CapacityOverflow 恒错误帧
const SATURATED_COUNT: &str = "9223372036854775807";

/// 次饱和 COUNT（数千亿级）：无封顶时直驱 24B×count 的数 GB 级真实预留
const OVER_CAP_COUNT: &str = "400000000000";

/// 空库巨 COUNT 回游标 0 终态正常帧而非错误帧（与 C#/Redis「巨 COUNT 一轮
/// 扫尽正常应答」对账一致；注入关闭态常态分配，专锁入口门封顶应答面）
#[test]
fn scan_huge_count_empty_db_returns_normal_cursor_frame() -> aok::Result<()> {
  let _serial = SERIAL_LOCK.lock();
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, mut s, _s2) = keys_env(0).await;
    for count in [OVER_CAP_COUNT, SATURATED_COUNT] {
      let (wire, went_slow) = pump(
        &mut s,
        &resp_frame(&[b"SCAN", b"0", b"COUNT", count.as_bytes()]),
      )
      .await;
      assert!(went_slow, "SCAN 恒降级慢路径游标扫描");
      assert_ne!(
        wire,
        err_frame(RESP_ERR_SLOW_PATH_STORAGE),
        "合法巨 COUNT {count} 不得恒错误帧拒服务"
      );
      assert_eq!(wire, b"*2\r\n$1\r\n0\r\n*0\r\n");
      assert_eq!(complete_len(&wire), Some(wire.len()));
    }
    Ok(())
  })
}

/// 小额键库（10 键）巨 COUNT 一轮扫尽：游标 0 终态 + 全量 10 键的
/// write_output_for_scan 正常 *2 游标数组帧，且后续常规小额页不受影响
#[test]
fn scan_huge_count_small_db_scans_all_in_one_round() -> aok::Result<()> {
  let _serial = SERIAL_LOCK.lock();
  const N: usize = 10;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, mut s, _s2) = keys_env(N).await;
    for count in [OVER_CAP_COUNT, SATURATED_COUNT] {
      let (wire, went_slow) = pump(
        &mut s,
        &resp_frame(&[b"SCAN", b"0", b"COUNT", count.as_bytes()]),
      )
      .await;
      assert!(went_slow, "SCAN 恒降级慢路径游标扫描");
      assert_ne!(
        wire,
        err_frame(RESP_ERR_SLOW_PATH_STORAGE),
        "合法巨 COUNT {count} 不得恒错误帧拒服务"
      );
      let head = format!("*2\r\n$1\r\n0\r\n*{N}\r\n");
      assert!(
        wire.starts_with(head.as_bytes()),
        "巨 COUNT {count} 须游标 0 终态一轮扫尽 {N} 键，实测前 32 字节 {:?}",
        &wire[..32.min(wire.len())]
      );
      assert_eq!(complete_len(&wire), Some(wire.len()));
    }

    // 续用无错位：封顶后常规小额页照常分页应答
    let (scan, _) = pump(&mut s, &resp_frame(&[b"SCAN", b"0", b"COUNT", b"8"])).await;
    assert!(
      scan.starts_with(b"*2\r\n"),
      "小额 COUNT 页须正常回游标数组帧，实测 {scan:?}"
    );
    Ok(())
  })
}

// ---- 案二：向量登记表并页臂的页上界与分配平滑单源（票 zcode-r147c-hscanmt
// 案二）----
//
// 收敛口径（doc/zh/db.md 剥前缀段与 deviations 第 75 条互引）：SCAN 首页
// （客户端游标 0 且无 TYPE）向量并页归 COUNT 页上界单源，只补投剩余额度、
// 满页截断不加码——向量登记表域不经游标递进，跨页续投不可无损达成，禁为
// 续投另立第二套游标位段；KEYS 恒全量投影（剩余额度无界）。分配面逐键经
// try_push_key 平滑单源，失败折既有 RESP_ERR_SLOW_PATH_STORAGE 单会话错误
// 帧漏斗，杜绝旧形裸 push 扩容触顶 handle_alloc_error abort 全进程（该
// 红形由 vector_domain_alloc_fail_smooth 显式回归对位）。

/// 消费面命令驱动一轮并回线面应答（快路径直出 + 慢路径 resolve 并入，
/// vector_key_domain_ops 同款夹具）
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, req: &[u8]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(req);
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let consumed = c.try_consume_messages_into(&mut out);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {req:?}");
  if let Some(slow) = c.take_slow_wait() {
    out.extend_from_slice(&rt.block_on(slow.resolve()));
  }
  out
}

/// 同装 `n_store` 个热区字符串键 + `n_vec` 个向量集合键（VADD 走登记表，
/// 全体装配在注入关闭态完成；向量执行域会话与登记表持至用例结束）
fn vec_env(
  n_store: usize,
  n_vec: usize,
  rt: &Runtime,
) -> (
  tempfile::TempDir,
  RespSessionConsumer,
  Arc<VectorManager>,
  OwnedActiveVectorSession<SegmentedDevice>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("vecmt.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let session = store.new_session().unwrap();
  let vector_domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new())),
  ));
  let api = StoreGarnetApi::new(session).with_vector_manager(Arc::clone(&vm));
  let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api));
  for i in 0..n_store {
    let key = format!("k:{i:07}");
    assert_eq!(
      roundtrip(rt, &mut c, &resp_frame(&[b"SET", key.as_bytes(), b"v"])),
      b"+OK\r\n",
      "字符串键 {key} 装配失败"
    );
  }
  let vec3: Vec<u8> = [1.0f32, 2.0, 3.0]
    .iter()
    .flat_map(|v| v.to_le_bytes())
    .collect();
  for i in 0..n_vec {
    let key = format!("vk:{i:07}");
    assert_eq!(
      roundtrip(
        rt,
        &mut c,
        &resp_frame(&[b"VADD", key.as_bytes(), b"FP32", &vec3, b"elem1"])
      ),
      b":1\r\n",
      "VADD 创建向量集 {key} 失败"
    );
  }
  (dir, c, vm, vector_domain)
}

/// 解析 SCAN 应答帧 → (游标, 条目字节集)
fn parse_scan_page(frame: &[u8]) -> (i64, Vec<Vec<u8>>) {
  let text = String::from_utf8_lossy(frame);
  let mut parts = text.split("\r\n");
  assert_eq!(parts.next(), Some("*2"), "SCAN 应答外层应为 *2 帧: {text}");
  let cursor_hdr = parts.next().unwrap();
  assert!(cursor_hdr.starts_with('$'), "游标 bulk 头: {cursor_hdr}");
  let cursor: i64 = parts.next().unwrap().parse().unwrap();
  let arr_hdr = parts.next().unwrap();
  assert!(arr_hdr.starts_with('*'), "条目数组头: {arr_hdr}");
  let n: usize = arr_hdr[1..].parse().unwrap();
  let mut items = Vec::with_capacity(n);
  for _ in 0..n {
    let hdr = parts.next().unwrap();
    let len: usize = hdr[1..].parse().unwrap();
    let val = parts.next().unwrap().as_bytes().to_vec();
    assert_eq!(val.len(), len);
    items.push(val);
  }
  (cursor, items)
}

/// 前缀计数（键类目甄别：k: 存储域 / vk: 向量登记域）
fn count_prefix(items: &[Vec<u8>], prefix: &[u8]) -> usize {
  items.iter().filter(|k| k.starts_with(prefix)).count()
}

/// 向量并页臂归 COUNT 页上界单源：首页只补投剩余额度、满额截断不加码、
/// 非首页与 TYPE 臂不补投、KEYS 恒全量（旧形首页无视 COUNT 全量倾泻即破）
#[test]
fn vector_domain_scan_page_bound_lock() -> aok::Result<()> {
  let _serial = SERIAL_LOCK.lock();
  let rt = Runtime::new()?;
  rt.block_on(async {
    // 象一：纯向量域 20 键（0 存储键）——首页产出恒 ≤ 页上界
    {
      let (_dir, mut c, _vm, _vd) = vec_env(0, 20, &rt);
      let (cursor, items) = parse_scan_page(&roundtrip(&rt, &mut c, &resp_frame(&[b"SCAN", b"0"])));
      assert_eq!(cursor, 0, "存储域耗尽游标应归零");
      assert_eq!(
        items.len(),
        10,
        "默认页上界 COUNT=10：向量补投至满额即止（旧形 20 全量倾泻即破此锁）"
      );
      assert_eq!(count_prefix(&items, b"vk:"), 10);

      let (cursor, items) = parse_scan_page(&roundtrip(
        &rt,
        &mut c,
        &resp_frame(&[b"SCAN", b"0", b"COUNT", b"5"]),
      ));
      assert_eq!(cursor, 0);
      assert_eq!(items.len(), 5, "COUNT 5 页向量臂截断恰满额");
      assert_eq!(count_prefix(&items, b"vk:"), 5);

      let (cursor, items) = parse_scan_page(&roundtrip(
        &rt,
        &mut c,
        &resp_frame(&[b"SCAN", b"0", b"COUNT", b"1"]),
      ));
      assert_eq!(cursor, 0);
      assert_eq!(items.len(), 1, "COUNT 1 极小页同样满额截断");

      let (cursor, items) = parse_scan_page(&roundtrip(
        &rt,
        &mut c,
        &resp_frame(&[b"SCAN", b"0", b"COUNT", b"100"]),
      ));
      assert_eq!(cursor, 0);
      assert_eq!(
        items.len(),
        20,
        "额度富余时 20 向量键应一次全投不重不漏（不截断亦不加码）"
      );

      let (_, items) = parse_scan_page(&roundtrip(
        &rt,
        &mut c,
        &resp_frame(&[b"SCAN", b"0", b"COUNT", b"5", b"MATCH", b"vk:*"]),
      ));
      assert_eq!(items.len(), 5, "MATCH 命中形下同样满额截断");
      let (_, items) = parse_scan_page(&roundtrip(
        &rt,
        &mut c,
        &resp_frame(&[b"SCAN", b"0", b"COUNT", b"5", b"MATCH", b"zz*"]),
      ));
      assert!(items.is_empty(), "MATCH 全不命中向量臂零产出");

      // KEYS 恒全量投影（无分页语义，剩余额度无界）
      let wire = roundtrip(&rt, &mut c, &resp_frame(&[b"KEYS", b"vk:*"]));
      assert!(
        wire.starts_with(b"*20\r\n"),
        "KEYS 向量域恒全量 20 键，实测前 32 字节 {:?}",
        &wire[..wire.len().min(32)]
      );
    }
    // 象二：3 存储 + 20 向量——首页剩余额度 5-3=2 只补投 2，满页恰 5
    {
      let (_dir, mut c, _vm, _vd) = vec_env(3, 20, &rt);
      let (cursor, items) = parse_scan_page(&roundtrip(
        &rt,
        &mut c,
        &resp_frame(&[b"SCAN", b"0", b"COUNT", b"5"]),
      ));
      assert_eq!(cursor, 0, "存储域 3 键 < 页上界，扫尽归零");
      assert_eq!(items.len(), 5, "存储 3 + 向量补投 2 = 恰满额");
      assert_eq!(count_prefix(&items, b"k:"), 3);
      assert_eq!(count_prefix(&items, b"vk:"), 2);

      // TYPE 给定臂不触向量并页（gate !type_given，向量域键型恒非 string）
      let (cursor, items) = parse_scan_page(&roundtrip(
        &rt,
        &mut c,
        &resp_frame(&[b"SCAN", b"0", b"COUNT", b"100", b"TYPE", b"string"]),
      ));
      assert_eq!(cursor, 0);
      assert_eq!(items.len(), 3, "TYPE string 臂应排除向量域");
      assert_eq!(count_prefix(&items, b"vk:"), 0);
    }
    // 象三：8 存储 + 20 向量，COUNT 5 全遍历——首页存储即满额，向量补投
    // 额度归零；非首页不设补投亦不为续投另立游标位段（截断不加码口径）
    {
      let (_dir, mut c, _vm, _vd) = vec_env(8, 20, &rt);
      let (cursor1, page1) = parse_scan_page(&roundtrip(
        &rt,
        &mut c,
        &resp_frame(&[b"SCAN", b"0", b"COUNT", b"5"]),
      ));
      assert_eq!(page1.len(), 5, "首页恰页上界满额");
      assert_eq!(count_prefix(&page1, b"vk:"), 0, "首页存储满额，向量零补投");
      assert_ne!(cursor1, 0, "存储域未尽，游标非零递进");
      let cur = cursor1.to_string();
      let (cursor2, page2) = parse_scan_page(&roundtrip(
        &rt,
        &mut c,
        &resp_frame(&[b"SCAN", cur.as_bytes(), b"COUNT", b"5"]),
      ));
      assert_eq!(cursor2, 0, "次页扫尽归零");
      assert_eq!(page2.len(), 3, "尾页 3 存储键收尾");
      assert_eq!(
        count_prefix(&page2, b"vk:"),
        0,
        "非首页不承载向量续投（禁第二游标位段口径）"
      );
      assert_eq!(
        count_prefix(&page1, b"k:") + count_prefix(&page2, b"k:"),
        8,
        "存储域全遍历 8 键不重不漏"
      );
    }
    Ok(())
  })
}

/// 向量并页臂分配收口 try_push_key 平滑单源：KEYS 全量臂大额扩容触顶折
/// 单会话错误帧（旧形裸 push 于此 handle_alloc_error abort 全进程=红形），
/// 进程与小额度页存活
#[test]
fn vector_domain_alloc_fail_smooth() -> aok::Result<()> {
  let _serial = SERIAL_LOCK.lock();
  let rt = Runtime::new()?;
  rt.block_on(async {
    // 2500 向量键、0 存储键：Vec<Vec<u8>> 24B/槽，第 2049 次 push 倍增
    // 扩容请求 4096×24=98304B ≥ 64KB 注入阈值，必踩合并臂
    let (_dir, mut c, _vm, _vd) = vec_env(0, 2500, &rt);
    // 对照组（注入关闭）：KEYS 恒全量投影 2500 键
    let wire = roundtrip(&rt, &mut c, &resp_frame(&[b"KEYS", b"*"]));
    assert!(
      wire.starts_with(b"*2500\r\n"),
      "对照组应全量投影 2500 向量键，实测前 32 字节 {:?}",
      &wire[..wire.len().min(32)]
    );
    assert_eq!(complete_len(&wire), Some(wire.len()));

    let _guard = InjectGuard;
    INJECT.store(true, Ordering::Relaxed);
    let wire = roundtrip(&rt, &mut c, &resp_frame(&[b"KEYS", b"*"]));
    assert_eq!(
      wire,
      err_frame(RESP_ERR_SLOW_PATH_STORAGE),
      "向量并页臂扩容触顶须折单会话存储错误帧，绝非 abort（进程达此断言即自证存活）"
    );
    assert_eq!(complete_len(&wire), Some(wire.len()));

    // 失败后连接续用无错位：小额度页分配远未触顶，照常服务
    assert_eq!(
      roundtrip(&rt, &mut c, &resp_frame(&[b"PING"])),
      b"+PONG\r\n"
    );
    let (cursor, items) = parse_scan_page(&roundtrip(
      &rt,
      &mut c,
      &resp_frame(&[b"SCAN", b"0", b"COUNT", b"5"]),
    ));
    assert_eq!(cursor, 0);
    assert_eq!(items.len(), 5, "注入开启态小额向量补投页照常满额截断");
    assert_eq!(count_prefix(&items, b"vk:"), 5);

    // 注入解除后全量恢复
    INJECT.store(false, Ordering::Relaxed);
    let wire = roundtrip(&rt, &mut c, &resp_frame(&[b"KEYS", b"*"]));
    assert!(
      wire.starts_with(b"*2500\r\n"),
      "解除注入后 KEYS 全量恢复，实测前 32 字节 {:?}",
      &wire[..wire.len().min(32)]
    );
    Ok(())
  })
}
