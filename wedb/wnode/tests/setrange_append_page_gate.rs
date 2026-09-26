//! SETRANGE/APPEND 增长臂两案回归（票 zcode-r143c-setrangex）
//!
//! 案一：增长臂的引擎单页容量前置门（review.md 板块4「单页容纳性前置校验」，
//! 判据单源 `WedbStore::record_fits_page` ⇄ 写侧 `RecordTooLarge` 同公式，与
//! 对象域 envelope_overflow 同族）。败窗（页容量, 512MB] 内旧形态先付整值回读
//! 加至多近 512MB 零填充物化加注定失败写回整轮才以通用错误帧收口；现形态在
//! 取窗物化前同帧形收口。C# 对位：BasicCommands.cs:NetworkSetRange :460-463
//! 协议闸止损动机（超页引擎硬抛连累断连，AllocatorBase.cs:TryAllocate
//! "Entry does not fit on page"）。
//! 案二：APPEND 空载荷经等长原位发布臂免复制回长（对位 MainStore/
//! RMWMethods.cs:800「If nothing to append, can avoid copy update」），内存
//! 驻位大值上不再整值物化回读加整值写回；磁盘候选态仍走整值重建臂，与 C#
//! CopyUpdater APPEND 臂同构；缺失键仍落 InitialUpdater 建空键形态。
//!
//! 分配探针形态沿用 wnode/tests/hll_alloc_probe.rs：本二进制独占一枚包裹
//! `System` 的计数分配器，窗口内登记单次最大申请与累计申请，断言败窗/空追加
//! 路径上不见值级大块；窗口只在单用例内开合（nextest 逐用例独立进程）。
//! 磁盘候选夹具与慢路径驱动沿用 wnode/tests/resp_slow_path.rs 装配惯例。

use core::str::from_utf8;
use std::{
  alloc::{GlobalAlloc, Layout, System},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
  },
};

use compio::runtime::Runtime;
use parking_lot::Mutex;
use wconf::DEFAULT_RESP_VERSION;
use wdev::SegmentedDevice;
use wkv::{StoreEvent, StoreEventSink, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    slow_path::SlowWait,
  },
};
use wnode_test::{Batch, err_frame, with_batch};
use wresp::{cmd_strings::RESP_ERR_GENERIC, command::RespCommand};
use wtest_base::{resp_frame, test_store_config};
use wval::KeyTag;

/// 测试装配（16MB 预算）页容量 256KB 之上的超窗尺寸；64KB 为
/// 「不见值级大块」地板（页容量下界，以内视为读漏斗与输出缓冲自身开销，
/// 与 hll_alloc_probe 同口径）
const OVER_PAGE: usize = 300_000;
const COPY_FLOOR: u64 = 64 << 10;

/// 现终态通用错误帧（write_resp_error 对无大写前缀裸消息统一冠 `ERR `，
/// 与 string_in_place_grow.rs 超页锁测同一对拍口径）
fn generic_frame() -> Vec<u8> {
  err_frame(&format!("ERR {RESP_ERR_GENERIC}"))
}

// ---------------------------------------------------------------------------
// 分配计数探针
// ---------------------------------------------------------------------------

static ARMED: AtomicBool = AtomicBool::new(false);
static MAX_LEN: AtomicU64 = AtomicU64::new(0);
static TOTAL: AtomicU64 = AtomicU64::new(0);

struct Counting;

impl Counting {
  #[inline]
  fn record(len: usize) {
    let len = len as u64;
    TOTAL.fetch_add(len, Relaxed);
    MAX_LEN.fetch_max(len, Relaxed);
  }
}

// SAFETY: 四对方法逐一转发 `System` 同法，只在转发前读一次开关位并累加原子量；
// `record` 不申请内存、不加锁，故重入安全。
unsafe impl GlobalAlloc for Counting {
  unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
    if ARMED.load(Relaxed) {
      Self::record(layout.size());
    }
    unsafe { System.alloc(layout) }
  }

  unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
    if ARMED.load(Relaxed) {
      Self::record(layout.size());
    }
    unsafe { System.alloc_zeroed(layout) }
  }

  unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
    // 缩容原地即可，不产生新内存；只有增长计一次
    if ARMED.load(Relaxed) && new_size > layout.size() {
      Self::record(new_size);
    }
    unsafe { System.realloc(ptr, layout, new_size) }
  }

  unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
    unsafe { System.dealloc(ptr, layout) }
  }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// 一次计数窗口的快照
#[derive(Debug, Clone, Copy)]
struct Snap {
  max_len: u64,
  total: u64,
}

/// 在计数窗口内执行 `f`，返回其结果与窗口快照（窗口外分配一律不计）
fn measured<R>(f: impl FnOnce() -> R) -> (R, Snap) {
  MAX_LEN.store(0, Relaxed);
  TOTAL.store(0, Relaxed);
  ARMED.store(true, Relaxed);
  let r = f();
  let snap = Snap {
    max_len: MAX_LEN.load(Relaxed),
    total: TOTAL.load(Relaxed),
  };
  ARMED.store(false, Relaxed);
  (r, snap)
}

/// 探针自检（正向对照）：窗口内显式申请 `size` 级单块必须被看见，否则
/// 「看不见即零物化」的断言不过是虚过
fn probe_self_check(size: usize) {
  let (kept, probe) = measured(|| vec![0u8; size].len());
  assert_eq!(kept, size);
  assert!(
    probe.max_len >= size as u64,
    "计数探针失效（窗口内看不见 {size}B 申请），下方断言无意义：{probe:?}"
  );
}

// ---------------------------------------------------------------------------
// RESP 会话面最小夹具（string_in_place_grow.rs 同型）
// ---------------------------------------------------------------------------

fn resp_int(out: &[u8]) -> i64 {
  assert_eq!(out[0], b':', "应答须为整数帧：{out:?}");
  from_utf8(&out[1..out.len() - 2]).unwrap().parse().unwrap()
}

fn set(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8], val: &[u8]) {
  let mut out = Vec::new();
  s.network_set(&[key, val], batch, None, &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");
}

/// GET 原样应答（miss 为 $-1 帧，供「键未落」断言）
fn get_raw(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8]) -> Vec<u8> {
  let mut out = Vec::new();
  s.network_get(&[key], batch, &mut out).unwrap();
  out
}

fn get(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8]) -> Vec<u8> {
  let out = get_raw(s, batch, key);
  assert_eq!(out[0], b'$', "读回须命中：{out:?}");
  let head_end = out.iter().position(|&c| c == b'\r').unwrap();
  let len: i64 = from_utf8(&out[1..head_end]).unwrap().parse().unwrap();
  assert!(len >= 0, "读回须命中：{out:?}");
  out[head_end + 2..head_end + 2 + len as usize].to_vec()
}

fn usage(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8]) -> i64 {
  let mut out = Vec::new();
  s.network_memory_usage(&[key], batch, None, &mut out)
    .unwrap();
  resp_int(&out)
}

/// SETRANGE 原样应答帧（成功整数 / 败窗错误帧皆经此出口）
fn set_range_frame(
  s: &mut RespServerSession,
  batch: &Batch<'_>,
  key: &[u8],
  offset: usize,
  val: &[u8],
) -> Vec<u8> {
  let mut out = Vec::new();
  let off = offset.to_string();
  let handled = s
    .network_set_range(&[key, off.as_bytes(), val], batch, &mut out)
    .unwrap();
  assert!(handled, "门内败局须同步段自断，不得甩给异步闭环");
  out
}

/// APPEND 原样应答帧
fn append_frame(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8], val: &[u8]) -> Vec<u8> {
  let mut out = Vec::new();
  let handled = s.network_append(&[key, val], batch, &mut out).unwrap();
  assert!(handled, "门内败局须同步段自断，不得甩给异步闭环");
  out
}

// ---------------------------------------------------------------------------
// 写通知旁路（零镜像断言，装配惯例同 string_in_place_grow.rs）
// ---------------------------------------------------------------------------

type NoticeLog = Arc<Mutex<usize>>;

fn write_notice_sink(log: NoticeLog) -> StoreEventSink {
  StoreEventSink::new(log, |log, _ver, _aof_session_id, event| {
    if matches!(event, StoreEvent::Write { .. }) {
      *log.lock() += 1;
    }
    Ok(())
  })
}

/// 案一（a）：超窗 SETRANGE 在取窗物化前以现终态同帧形收口——分配探针证
/// 「无 vec 物化轮次」（修复前 Hit 臂 resize / Missing 臂 `vec![0u8;
/// offset + len]` 必见 ≥300KB 单块）、写通知零新增证「零镜像零新增条目」、
/// 旧值槽位无损与该键后续可写沿用 string_in_place_grow.rs 超页锁测断言骨架
#[test]
fn set_range_beyond_page_gate_rejects_before_materialization() {
  with_batch(|s, batch| {
    let log: NoticeLog = Arc::new(Mutex::new(0));
    assert!(
      batch
        .store()
        .set_event_sink(write_notice_sink(Arc::clone(&log))),
      "本测试独占存储事件订阅位，注入须成功"
    );

    let key = b"gate-sr";
    set(s, batch, key, b"1234567890");
    let usage_before = usage(s, batch, key);
    assert_eq!(
      *log.lock(),
      1,
      "播种 SET 恰入账一条通知、MEMORY USAGE 纯读零入账（口径同 string_in_place_grow.rs 每 SET 恰一条），基线自洽"
    );

    probe_self_check(OVER_PAGE + 1);

    // 预热一轮：首轮触达页钉住与输出缓冲增长，不计入断言
    assert_eq!(
      set_range_frame(s, batch, key, OVER_PAGE, b"x"),
      generic_frame()
    );
    let base = *log.lock();

    let (frame, snap) = measured(|| set_range_frame(s, batch, key, OVER_PAGE, b"x"));
    assert_eq!(frame, generic_frame(), "超窗 SETRANGE 须整笔显式报错");
    assert!(
      snap.max_len < COPY_FLOOR && snap.total < COPY_FLOOR,
      "超窗 SETRANGE 已在物化前收口，路径上不得见值级大块：{snap:?}"
    );
    assert_eq!(*log.lock(), base, "败窗命令零镜像：写通知不得新增");

    assert_eq!(get(s, batch, key), b"1234567890", "被拒写入不得伤及旧值");
    assert_eq!(usage(s, batch, key), usage_before, "被拒写入不得换槽位");
    assert_eq!(resp_int(&append_frame(s, batch, key, b"ZZ")), 12);
  });
}

/// 案一（a）APPEND 面：缺键臂与回落 Hit 臂超窗皆以同帧形收口，键不落、
/// 旧值槽位无损、零镜像（整值回读本身系 C# CopyUpdater 同构成本，不入射程）
#[test]
fn append_oversized_arms_reject_with_same_frame() {
  with_batch(|s, batch| {
    let log: NoticeLog = Arc::new(Mutex::new(0));
    assert!(
      batch
        .store()
        .set_event_sink(write_notice_sink(Arc::clone(&log)))
    );

    // 缺键臂：超窗载荷 APPEND 不落键
    let missing = b"gate-ap-miss";
    let huge = vec![b'H'; OVER_PAGE];
    let base = *log.lock();
    assert_eq!(
      append_frame(s, batch, missing, &huge),
      generic_frame(),
      "缺键超窗 APPEND 须同帧形收口"
    );
    assert_eq!(get_raw(s, batch, missing), b"$-1\r\n", "被拒写入不得落键");
    assert_eq!(*log.lock(), base, "败窗 APPEND 零镜像（缺键臂）");

    // 回落 Hit 臂：超窗终值免走注定失败的引擎写回往返
    let hit = b"gate-ap-hit";
    set(s, batch, hit, b"1234567890");
    let usage_before = usage(s, batch, hit);
    let base = *log.lock();
    assert_eq!(append_frame(s, batch, hit, &huge), generic_frame());
    assert_eq!(get(s, batch, hit), b"1234567890");
    assert_eq!(usage(s, batch, hit), usage_before);
    assert_eq!(*log.lock(), base, "败窗 APPEND 零镜像（Hit 回落臂）");

    // 超限失败后该键仍可正常写（超页锁测骨架）
    assert_eq!(resp_int(&append_frame(s, batch, hit, b"ZZ")), 12);
  });
}

/// 在 [0, 512KB] 上以 `fits` 谓词二分求「仍被接纳的最大终值长」（接纳性随
/// 终值长单调，页容量钳 [64KB,16MB] 且测试装配 256KB，上界必落拒绝域）
fn max_fitting_len(mut fits: impl FnMut(usize) -> bool) -> usize {
  let mut lo = 0usize; // 页容量 ≥ 64KB，0 恒容纳
  let mut hi = 512 * 1024;
  assert!(!fits(hi), "二分上界须落在拒绝域");
  while lo + 1 < hi {
    let mid = lo + (hi - lo) / 2;
    if fits(mid) {
      lo = mid;
    } else {
      hi = mid;
    }
  }
  lo
}

/// 案一（c）窗内临界：offset+len 恰容纳上限时双臂成功路径零回归，恰越界即
/// 与既有锁测逐字节同帧形
#[test]
fn page_gate_boundary_success_and_frame_identity() {
  with_batch(|s, batch| {
    let pl = batch.session_tag_key(KeyTag::String, b"bd-0").len();
    let store = batch.store();
    let fit_max = max_fitting_len(|n| store.record_fits_page(pl, n));

    // SETRANGE 恰容：缺键纯补零延长到 fit_max 成功
    assert_eq!(
      resp_int(&set_range_frame(s, batch, b"bd-0", fit_max - 1, b"z")),
      fit_max as i64
    );
    // SETRANGE 恰越界：同帧形收口且键不落
    assert_eq!(
      set_range_frame(s, batch, b"bd-1", fit_max, b"z"),
      generic_frame()
    );
    assert_eq!(get_raw(s, batch, b"bd-1"), b"$-1\r\n");

    // APPEND 缺键臂恰容成功 / 恰越界同帧形
    let whole = vec![b'V'; fit_max];
    assert_eq!(
      resp_int(&append_frame(s, batch, b"bd-2", &whole)),
      fit_max as i64
    );
    let over = vec![b'V'; fit_max + 1];
    assert_eq!(append_frame(s, batch, b"bd-3", &over), generic_frame());

    // 回落 Hit 臂恰越界：既有值不动
    assert_eq!(append_frame(s, batch, b"bd-2", b"Z"), generic_frame());
    assert_eq!(get(s, batch, b"bd-2"), whole, "回落臂败窗不得伤及既有值");
  });
}

/// 案一（d）判据源一致性钉测：同一物理键构形（session_tag_key_with_prefix
/// KeyTag::String）上 `record_fits_page` 真值表等于写臂实际 RecordTooLarge
/// 受理界，杜绝公式漂移
#[test]
fn page_gate_truth_table_matches_write_arm_boundary() {
  with_batch(|_s, batch| {
    // 定宽键名保证各探针物理键长一致
    let mut probes = 0u32;
    let pl = batch.session_tag_key(KeyTag::String, b"gate-tt-001").len();
    let store = batch.store();

    // 引擎写臂实测受理界：绕开 RESP 门直调 try_rmw_sync
    let write_accepts = |n: usize| {
      probes += 1;
      let key = format!("gate-tt-{probes:03}");
      let val = vec![0u8; n];
      let window = batch
        .try_rmw_window(key.as_bytes())
        .expect("新键桶闩必可得");
      matches!(window.try_rmw_sync(&val), Ok(Ok(_)))
    };
    let write_max = max_fitting_len(write_accepts);
    let gate_max = max_fitting_len(|n| store.record_fits_page(pl, n));
    assert_eq!(
      write_max, gate_max,
      "前置门真值表与写臂 RecordTooLarge 受理界分叉（公式漂移）：写臂界 \
       {write_max}，门界 {gate_max}"
    );
  });
}

// ---------------------------------------------------------------------------
// 磁盘候选（驱逐后）夹具：consumer 装配与慢路径驱动沿用 resp_slow_path.rs 惯例
// ---------------------------------------------------------------------------

fn consumer_with_api() -> (
  RespSessionConsumer,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
) {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("gate.db")).unwrap());
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(session));
  (
    RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone()),
    api,
    store,
  )
}

fn pump(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(frame);
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = c.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应被完整消费: {frame:?}");
  resp
}

fn roundtrip(c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let out = pump(c, &resp_frame(args));
  assert!(
    !out.is_empty(),
    "同步段须直接应答（应挂起慢路径的命令走 slow_roundtrip）"
  );
  out
}

fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut out = pump(c, &resp_frame(args));
  if let Some(slow) = c.take_slow_wait() {
    out.extend_from_slice(&rt.block_on(slow.resolve()));
  }
  out
}

/// 慢臂直答（不经会话快路径，钉次级落点帧形）
fn slow_reply(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  rt.block_on(async {
    SlowWait::for_command(
      api,
      cmd,
      args.iter().map(|a| a.to_vec()).collect(),
      DEFAULT_RESP_VERSION,
    )
    .resolve()
    .await
  })
}

/// 案一（b）磁盘候选态：超窗 SETRANGE 以快臂 generic 同帧形同步收口且零冷读
/// 降级重放（修复前 Deferred → Ok(false) 挂慢臂整值回读）；慢臂直答同帧形，
/// 钉死 RecordTooLarge 经 ? 上抛的次级落点与快臂帧形的潜在分叉收口
#[test]
fn oversized_arms_on_cold_key_reject_with_converged_frames() {
  let rt = Runtime::new().unwrap();
  let (mut c, api, store) = consumer_with_api();
  assert_eq!(roundtrip(&mut c, &[b"SET", b"ck", b"v"]), b"+OK\r\n");
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 快臂：门在取窗物化前，同步段直接落错误帧，绝不挂慢路径
  let out = pump(&mut c, &resp_frame(&[b"SETRANGE", b"ck", b"300000", b"x"]));
  assert_eq!(out, generic_frame());
  assert!(
    c.take_slow_wait().is_none(),
    "超窗 SETRANGE 须门前收口，不得降级慢臂整值冷读回"
  );
  // 旧值无损（冷读仍可用，命令面零伤及）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &[b"GET", b"ck"]),
    b"$1\r\nv\r\n"
  );

  // 慢臂直答同帧形（快臂缺席时的次级落点收口钉测）
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Setrange, &[b"ck", b"300000", b"x"]),
    generic_frame(),
    "慢臂超窗 SETRANGE 收口帧形须与快臂逐字节一致"
  );
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Append,
      &[b"ck2", &vec![b'H'; OVER_PAGE]]
    ),
    generic_frame(),
    "慢臂超窗 APPEND（缺键臂）收口帧形须与快臂逐字节一致"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &[b"GET", b"ck"]),
    b"$1\r\nv\r\n",
    "超窗败局不得伤及磁盘候选旧值"
  );
  assert_eq!(
    roundtrip(&mut c, &[b"EXISTS", b"ck2"]),
    b":0\r\n",
    "败局不得落键"
  );
}

/// 案二（b）引擎层区分测：内存驻位大值上 APPEND 空载荷命中等长原位发布臂，
/// 分配探针证「零值级复制」——修复前空载荷被旁路出原位臂，回落臂
/// read_user_sync 整值物化加整值写回必见 ≥128KB 大块（「物化整值再覆写」
/// 回潮的可观测位）；应答与读回逐字节不变
#[test]
fn empty_append_on_large_in_memory_value_copies_nothing() {
  const PAYLOAD: usize = 128 << 10; // 页容量 256KB 窗内的大值
  with_batch(|s, batch| {
    let key = b"ea-big";
    set(s, batch, key, &[b'V'; PAYLOAD]);
    let usage_before = usage(s, batch, key);

    probe_self_check(PAYLOAD);

    // 预热一轮后计数：等长原位发布臂应答 :PAYLOAD，路径上零值级分配
    let warm = append_frame(s, batch, key, b"");
    assert_eq!(warm, format!(":{PAYLOAD}\r\n").into_bytes());
    let (frame, snap) = measured(|| append_frame(s, batch, key, b""));
    assert_eq!(
      frame,
      format!(":{PAYLOAD}\r\n").into_bytes(),
      "空 APPEND 回当前值长"
    );
    assert!(
      snap.max_len < COPY_FLOOR && snap.total < COPY_FLOOR,
      "空 APPEND 经等长原位发布臂免复制回长，不得整值物化/写回：{snap:?}"
    );

    assert_eq!(
      get(s, batch, key),
      vec![b'V'; PAYLOAD],
      "空 APPEND 不得改动值"
    );
    assert_eq!(usage(s, batch, key), usage_before, "空 APPEND 不得换槽位");
  });
}

/// 案二（d）缺失键与磁盘候选两形态对位 C# InitialUpdater / CopyUpdater：
/// 缺失键空 APPEND 仍建空键回 :0；磁盘候选大值空 APPEND 走整值重建臂，
/// 应答与读回同值（无短路义务）
#[test]
fn empty_append_missing_and_cold_arms_keep_csharp_parity() {
  // 缺失键臂（RESP 会话面）
  with_batch(|s, batch| {
    assert_eq!(append_frame(s, batch, b"ea-miss", b""), b":0\r\n");
    assert_eq!(get_raw(s, batch, b"ea-miss"), b"$0\r\n\r\n");
    assert_eq!(resp_int(&append_frame(s, batch, b"ea-miss", b"abc")), 3);
  });

  // 磁盘候选臂（驱逐后经慢路径整值重建）
  const PAYLOAD: usize = 128 << 10;
  let rt = Runtime::new().unwrap();
  let (mut c, _api, store) = consumer_with_api();
  assert_eq!(
    roundtrip(&mut c, &[b"SET", b"ec", &[b'V'; PAYLOAD][..]]),
    b"+OK\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &[b"APPEND", b"ec", b""]),
    format!(":{PAYLOAD}\r\n").into_bytes(),
    "磁盘候选空 APPEND 经整值重建臂回同值"
  );
  let mut expected = format!("${PAYLOAD}\r\n").into_bytes();
  expected.extend_from_slice(&[b'V'; PAYLOAD]);
  expected.extend_from_slice(b"\r\n");
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &[b"GET", b"ec"]),
    expected,
    "重建臂读回须逐字节同值"
  );
}
