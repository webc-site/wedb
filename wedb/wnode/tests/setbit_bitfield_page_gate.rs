//! SETBIT/BITFIELD 写臂超页败窗拦截回归（票 zcode-r163c-bitopdst）
//!
//! 单页容量前置门接线位图族写臂：SETBIT 快臂 network_string_set_bit、慢臂
//! bitmap_slow SETBIT 分支与 BITFIELD 快臂 string_bit_field_action、慢臂
//! Bitfield 分支，取闩前接入 r143c 既有判据源 `string_record_fits_page`
//! （判据单源 `WedbStore::record_fits_page` ⇄ 写侧 `RecordTooLarge` 同公式），
//! 败窗（页容量, 512MB] 内旧形态先持键级排他闩盲目 `Vec::with_capacity(need)`
//! 加至多近 512MB 零填充物化、注定失败写回整轮才收口；现形态在取窗物化前
//! 同帧形收口。C# 对位：BitmapCommands.cs:NetworkStringSetBit 收 512MB 协议闸
//! 后引擎侧 AllocatorBase.cs:TryAllocate "Entry does fit on page" 硬抛的止损
//! 前移（BasicCommands.cs:NetworkSetRange :460-466 显式前置拒同族动机）。
//! BITOP 面 dest 窗罩折叠全程已闭环（r87 复核），不入本票射程。
//!
//! 分配探针形态沿用 wnode/tests/hll_alloc_probe.rs 与 setrange_append_page_gate.rs：
//! 本二进制独占一枚包裹 `System` 的计数分配器，窗口内登记单次最大申请与
//! 累计申请，断言败窗路径上不见值级大块；窗口只在单用例内开合（nextest
//! 逐用例独立进程）。磁盘候选夹具与慢路径驱动沿用 resp_slow_path.rs 装配惯例。

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

/// 测试装配（16MB 预算）页容量 256KB 之上的超窗尺寸（need 字节数口径）；
/// 64KB 为「不见值级大块」地板（页容量下界，以内视为读漏斗与输出缓冲自身
/// 开销，与 setrange_append_page_gate.rs 同口径）
const OVER_PAGE: usize = 300_000;
const COPY_FLOOR: u64 = 64 << 10;

/// SETBIT 覆盖 need = OVER_PAGE 的位偏移（(OVER_PAGE-1)*8 的最高位落在
/// 第 OVER_PAGE 字节内）
const OVER_OFFSET: i64 = (OVER_PAGE as i64 - 1) * 8;
/// BITFIELD u8 写子命令终值长 300_001（> OVER_PAGE > 页容量）的位偏移
const BF_OVER_OFFSET: i64 = 2_400_000;

/// 现终态通用错误帧（write_resp_error 对无大写前缀裸消息统一冠 `ERR `，
/// 与 setrange_append_page_gate.rs 同一对拍口径）
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

/// 整二进制序列化锁：计数探针为独占静态量，`cargo test` 共享进程线程并发下
/// 他案值级分配会污染本案计数窗（nextest 逐用例独立进程天然免疫）；全部
/// 用例整体持锁，保证两种 runner 下断言同形
static SERIES: Mutex<()> = Mutex::new(());

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
// RESP 会话面最小夹具（setrange_append_page_gate.rs 同型）
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

fn offset_text(offset: i64) -> String {
  offset.to_string()
}

/// SETBIT 原样应答帧（成功整数 / 败窗错误帧皆经此出口）
fn set_bit_frame(
  s: &mut RespServerSession,
  batch: &Batch<'_>,
  key: &[u8],
  offset: i64,
  bit: &[u8],
) -> Vec<u8> {
  let mut out = Vec::new();
  let off = offset_text(offset);
  let handled = s
    .network_string_set_bit(&[key, off.as_bytes(), bit], batch, &mut out)
    .unwrap();
  assert!(handled, "门内败局须同步段自断，不得甩给异步闭环");
  out
}

/// BITFIELD 原样应答帧（args 为 key 之后的子命令词元序列）
fn bit_field_frame(
  s: &mut RespServerSession,
  batch: &Batch<'_>,
  key: &[u8],
  args: &[&[u8]],
) -> Vec<u8> {
  let mut out = Vec::new();
  let mut parse_state = vec![key];
  parse_state.extend_from_slice(args);
  let handled = s.string_bit_field(&parse_state, batch, &mut out).unwrap();
  assert!(handled, "门内败局须同步段自断，不得甩给异步闭环");
  out
}

/// SETBIT 超窗帧（bit 恒 1）
fn set_bit_oversized(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8]) -> Vec<u8> {
  set_bit_frame(s, batch, key, OVER_OFFSET, b"1")
}

/// BITFIELD SET u8 超窗帧（写子命令终值长 300_001 > 页容量）
fn bit_field_oversized(s: &mut RespServerSession, batch: &Batch<'_>, key: &[u8]) -> Vec<u8> {
  let off = offset_text(BF_OVER_OFFSET);
  bit_field_frame(s, batch, key, &[b"SET", b"u8", off.as_bytes(), b"1"])
}

// ---------------------------------------------------------------------------
// 写通知旁路（零镜像断言，装配惯例同 setrange_append_page_gate.rs）
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

/// 案一（a）：超页 SETBIT 在取窗物化前以现终态同帧形收口——分配探针证
/// 「无 vec 物化轮次」（修复前回落臂 `Vec::with_capacity(need)` + resize 必见
/// ≥300KB 单块）、写通知零新增证「零镜像零新增条目」、旧值槽位无损与该键
/// 后续可写沿用 setrange 超页锁测断言骨架
#[test]
fn setbit_beyond_page_gate_rejects_before_materialization() {
  // 探针独占：整二进制序列化（见 [`SERIES`]）
  let _ser = SERIES.lock();
  with_batch(|s, batch| {
    let log: NoticeLog = Arc::new(Mutex::new(0));
    assert!(
      batch
        .store()
        .set_event_sink(write_notice_sink(Arc::clone(&log))),
      "本测试独占存储事件订阅位，注入须成功"
    );

    let key = b"gate-sb";
    set(s, batch, key, b"1234567890");
    let usage_before = usage(s, batch, key);
    assert_eq!(
      *log.lock(),
      1,
      "播种 SET 恰入账一条通知、MEMORY USAGE 纯读零入账，基线自洽"
    );

    probe_self_check(OVER_PAGE);

    // 预热一轮：首轮触达页钉住与输出缓冲增长，不计入断言
    assert_eq!(set_bit_oversized(s, batch, key), generic_frame());
    let base = *log.lock();

    let (frame, snap) = measured(|| set_bit_oversized(s, batch, key));
    assert_eq!(frame, generic_frame(), "超页 SETBIT 须整笔显式报错");
    assert!(
      snap.max_len < COPY_FLOOR && snap.total < COPY_FLOOR,
      "超页 SETBIT 已在取窗前收口，路径上不得见值级大块：{snap:?}"
    );
    assert_eq!(*log.lock(), base, "败窗 SETBIT 零镜像：写通知不得新增");

    assert_eq!(get(s, batch, key), b"1234567890", "被拒写入不得伤及旧值");
    assert_eq!(usage(s, batch, key), usage_before, "被拒写入不得换槽位");
    // 超限失败后该键仍可正常写（超页锁测骨架）：'1' = 0x31 首位为 0
    assert_eq!(resp_int(&set_bit_frame(s, batch, key, 0, b"1")), 0);
  });
}

/// 案一（a）缺键臂：超页 SETBIT 不落键、零镜像（修复前 InitialUpdater 盲写
/// 臂先物化近 512MB 零填充再吃 RecordTooLarge）
#[test]
fn setbit_missing_key_oversized_does_not_create_key() {
  // 探针独占：整二进制序列化（见 [`SERIES`]）
  let _ser = SERIES.lock();
  with_batch(|s, batch| {
    let log: NoticeLog = Arc::new(Mutex::new(0));
    assert!(
      batch
        .store()
        .set_event_sink(write_notice_sink(Arc::clone(&log)))
    );

    let missing = b"gate-sb-miss";
    let base = *log.lock();
    assert_eq!(
      set_bit_oversized(s, batch, missing),
      generic_frame(),
      "缺键超页 SETBIT 须同帧形收口"
    );
    assert_eq!(get_raw(s, batch, missing), b"$-1\r\n", "被拒写入不得落键");
    assert_eq!(*log.lock(), base, "败窗 SETBIT 零镜像（缺键臂）");
  });
}

/// 案一（b）：超页 BITFIELD 写子命令在取窗前以同帧形整条收口——纯 SET 形与
/// GET+SET 混合形皆只落错误帧（修复前混合形会先写数组头再逐子命令物化，
/// 门位前移后数组头都不落）；旧值无损、零镜像、后续可写
#[test]
fn bitfield_write_beyond_page_gate_rejects_whole_command() {
  // 探针独占：整二进制序列化（见 [`SERIES`]）
  let _ser = SERIES.lock();
  with_batch(|s, batch| {
    let log: NoticeLog = Arc::new(Mutex::new(0));
    assert!(
      batch
        .store()
        .set_event_sink(write_notice_sink(Arc::clone(&log))),
      "本测试独占存储事件订阅位，注入须成功"
    );

    let key = b"gate-bf";
    set(s, batch, key, b"1234567890");
    let usage_before = usage(s, batch, key);
    assert_eq!(*log.lock(), 1, "播种 SET 恰入账一条通知，基线自洽");

    probe_self_check(OVER_PAGE);

    // 纯 SET 形：预热一轮后计数断言
    assert_eq!(bit_field_oversized(s, batch, key), generic_frame());
    let base = *log.lock();
    let (frame, snap) = measured(|| bit_field_oversized(s, batch, key));
    assert_eq!(frame, generic_frame(), "超页 BITFIELD SET 须整条显式报错");
    assert!(
      snap.max_len < COPY_FLOOR && snap.total < COPY_FLOOR,
      "超页 BITFIELD 已在取窗前收口，路径上不得见值级大块：{snap:?}"
    );

    // GET+SET 混合形：败局在数组长度写出之前，应答恰为单错误帧
    let off = offset_text(BF_OVER_OFFSET);
    let mixed = bit_field_frame(
      s,
      batch,
      key,
      &[b"GET", b"u8", b"0", b"SET", b"u8", off.as_bytes(), b"1"],
    );
    assert_eq!(
      mixed,
      generic_frame(),
      "含超页写子命令的混合形须整条收口，不得先落数组头"
    );

    assert_eq!(*log.lock(), base, "败窗 BITFIELD 零镜像：写通知不得新增");
    assert_eq!(get(s, batch, key), b"1234567890", "被拒写入不得伤及旧值");
    assert_eq!(usage(s, batch, key), usage_before, "被拒写入不得换槽位");
    // 超限失败后该键仍可正常写：SET u8 0 值 200 覆写首字节，回旧值 0x31
    assert_eq!(
      bit_field_frame(s, batch, key, &[b"SET", b"u8", b"0", b"200"]),
      b"*1\r\n:49\r\n",
      "窗内 BITFIELD SET 正常路径零回归"
    );
  });
}

// ---------------------------------------------------------------------------
// 磁盘候选（驱逐后）夹具：consumer 装配与慢路径驱动沿用 setrange 测惯例
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

/// 案一（c）磁盘候选态：超页 SETBIT/BITFIELD 皆以快臂 generic 同帧形同步收口
/// 且零冷读降级重放（修复前 Deferred → Ok(false) 挂慢臂整值回读加盲目分配）；
/// 慢臂直答同帧形，钉死 RecordTooLarge 经 ? 上抛的次级落点与快臂帧形的
/// 分叉收口（旧慢臂终帧系 RESP_ERR_SLOW_PATH_STORAGE，门前移后两臂逐字节一致）
#[test]
fn oversized_bit_writes_on_cold_key_reject_with_converged_frames() {
  // 探针独占：整二进制序列化（见 [`SERIES`]）
  let _ser = SERIES.lock();
  let rt = Runtime::new().unwrap();
  let (mut c, api, store) = consumer_with_api();
  assert_eq!(roundtrip(&mut c, &[b"SET", b"ck", b"v"]), b"+OK\r\n");
  rt.block_on(store.flush_and_evict_all()).unwrap();

  let over_off = offset_text(OVER_OFFSET);
  let bf_off = offset_text(BF_OVER_OFFSET);

  // 快臂门前置：冷键超页仍同步段直接落错误帧，绝不挂慢路径
  let out = pump(
    &mut c,
    &resp_frame(&[b"SETBIT", b"ck", over_off.as_bytes(), b"1"]),
  );
  assert_eq!(out, generic_frame());
  assert!(
    c.take_slow_wait().is_none(),
    "超页 SETBIT 须门前收口，不得降级慢臂整值冷读回"
  );
  let out = pump(
    &mut c,
    &resp_frame(&[b"BITFIELD", b"ck", b"SET", b"u8", bf_off.as_bytes(), b"1"]),
  );
  assert_eq!(out, generic_frame());
  assert!(
    c.take_slow_wait().is_none(),
    "超页 BITFIELD 须门前收口，不得降级慢臂整值冷读回"
  );

  // 旧值无损（冷读仍可用，命令面零伤及）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &[b"GET", b"ck"]),
    b"$1\r\nv\r\n"
  );

  // 慢臂直答同帧形（快臂缺席时的次级落点收口钉测）
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Setbit,
      &[b"ck", over_off.as_bytes(), b"1"]
    ),
    generic_frame(),
    "慢臂超页 SETBIT 收口帧形须与快臂逐字节一致"
  );
  assert_eq!(
    slow_reply(
      &rt,
      &api,
      RespCommand::Bitfield,
      &[b"ck", b"SET", b"u8", bf_off.as_bytes(), b"1"]
    ),
    generic_frame(),
    "慢臂超页 BITFIELD 收口帧形须与快臂逐字节一致"
  );

  // 败局不落键
  assert_eq!(
    slow_roundtrip(&rt, &mut c, &[b"SETBIT", b"ck2", over_off.as_bytes(), b"1"]),
    generic_frame()
  );
  assert_eq!(roundtrip(&mut c, &[b"EXISTS", b"ck2"]), b":0\r\n");
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

/// 案一（d）窗内临界：need 恰容纳上限时 SETBIT/BITFIELD 写臂成功路径零回归，
/// 恰越界即与既有锁测逐字节同帧形且键不落
#[test]
fn page_gate_boundary_success_and_frame_identity() {
  // 探针独占：整二进制序列化（见 [`SERIES`]）
  let _ser = SERIES.lock();
  with_batch(|s, batch| {
    let pl = batch.session_tag_key(KeyTag::String, b"bd-0").len();
    let store = batch.store();
    let fit_max = max_fitting_len(|n| store.record_fits_page(pl, n));

    // SETBIT 恰容：末字节的最高位成功置入（缺键回旧 bit 0）
    assert_eq!(
      set_bit_frame(s, batch, b"bd-0", (fit_max as i64 - 1) * 8, b"1"),
      b":0\r\n"
    );

    // SETBIT 恰越界：同帧形收口且键不落
    assert_eq!(
      set_bit_frame(s, batch, b"bd-1", fit_max as i64 * 8, b"1"),
      generic_frame()
    );
    assert_eq!(get_raw(s, batch, b"bd-1"), b"$-1\r\n");

    // BITFIELD SET u8 恰容：offset 使域末端位恰落于第 fit_max 字节
    assert_eq!(
      bit_field_frame(
        s,
        batch,
        b"bd-2",
        &[
          b"SET",
          b"u8",
          offset_text((fit_max as i64 - 1) * 8).as_bytes(),
          b"1"
        ]
      ),
      b"*1\r\n:0\r\n"
    );
    // BITFIELD SET u8 恰越界（need = fit_max + 1）：同帧形收口且键不落
    assert_eq!(
      bit_field_frame(
        s,
        batch,
        b"bd-3",
        &[
          b"SET",
          b"u8",
          offset_text(fit_max as i64 * 8 - 7).as_bytes(),
          b"1"
        ]
      ),
      generic_frame()
    );
    assert_eq!(get_raw(s, batch, b"bd-3"), b"$-1\r\n");
  });
}

/// 纯读面零波及门控：GETBIT/BITFIELD_RO/全 GET 的 BITFIELD 大偏移形态
/// 不入单页门射程（无增长义务），恒按 NOTFOUND → :0 应答
#[test]
fn read_arms_are_not_gated() {
  // 探针独占：整二进制序列化（见 [`SERIES`]）
  let _ser = SERIES.lock();
  with_batch(|s, batch| {
    // GETBIT 顶格偏移（4294967295 = 512MB-1 字节域末位）：协议闸内恒 :0
    let mut out = Vec::new();
    s.network_string_get_bit(&[b"ra-gb", b"4294967295"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // BITFIELD_RO 超页偏移 GET：数组帧逐子命令 :0，不落错误帧
    let mut out = Vec::new();
    s.string_bit_field_read_only(
      &[
        b"ra-ro",
        b"GET",
        b"u8",
        offset_text(BF_OVER_OFFSET).as_bytes(),
      ],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n");

    // BITFIELD 全 GET 形超页偏移：同上零门控
    assert_eq!(
      bit_field_frame(
        s,
        batch,
        b"ra-get",
        &[b"GET", b"u8", offset_text(BF_OVER_OFFSET).as_bytes()]
      ),
      b"*1\r\n:0\r\n"
    );
  });
}
