//! HLL 读路径两处堆开销的分配探针回归（task/done/hll-load-validate-and-dense-scratch.md）
//!
//! 问题一：`load_hll` / `load_hll_cold` 旧先 `to_vec` 整值物化才判 HYLL，几百 MB
//! 普通字符串键发 PFADD 要先复制几百 MB 再回 WRONGTYPE。现口径对标
//! garnet libs/server/Storage/Functions/MainStore/RMWMethods.cs:655-676——
//! `IsValidHYLL(PinnedValuePointer, valueLen)` 在钉住记录上前置判定，非法零拷贝即拒。
//!
//! 问题二：多键 PFCOUNT 旧每键 `hll_dense_view` 新分配 12KB 稠密视图，N 键即
//! N 次申请/释放。现口径对标
//! garnet libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:118-172 的
//! `sectorAlignedMemoryHll1/2 ??=` 两块缓冲复用：稠密缓冲按命令复用而非按键分配。
//!
//! 探针形态：`GlobalAlloc` 一枚二进制只能装一次，故本文件独占一枚包裹 `System`
//! 的计数分配器，只在显式窗口内登记三项指纹——
//! ①单次最大分配（值整拷贝的唯一特征：载荷多大就申请多大）；
//! ②窗口累计分配字节；
//! ③申请尺寸恰为 `dense_bytes`（12304）的次数——稠密缓冲的指纹，合法稀疏载荷
//! ≤ SPARSE_SIZE_MAX_CAP（4096），其物化副本永不及此尺寸，故可与「仅合法分支
//! 物化载荷」的那次 owned 副本区分计数。
//!
//! 计数窗口靠 nextest 逐用例独立进程执行才纯净（与 wcol 的 LPOS 分配探针
//! 同一判据口径），故本文件的窗口只在单用例内开合，不额外串锁。
//!
//! 冷路径（`load_hll_cold`）不另设分配探针：磁盘候选读必先取页缓冲，64KB 级
//! 申请盖过载荷拷贝信号，测不出可归因的差值；其校验前置与热路径共用
//! `valid_hyll_payload` 单入口（结构上无从只改一侧），应答语义由
//! `hll_cold_degrade.rs` 的伪造载荷两支用例守护。

use core::str::from_utf8;
use std::{
  alloc::{GlobalAlloc, Layout, System},
  sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
};

use whyperlog::HyperLogLog;
use wnode::resp::resp_server_session::RespServerSession;
use wnode_test::{Batch, err_frame, with_batch};
use wresp::cmd_strings::RESP_ERR_WRONG_TYPE_HLL;

/// 稠密缓冲指纹尺寸（开表时由 `HyperLogLog::dense_bytes` 写入；0 = 未启用计数）
static DENSE_BYTES: AtomicU64 = AtomicU64::new(0);
/// 计数窗口开关
static ARMED: AtomicBool = AtomicBool::new(false);
/// 窗口内尺寸 == dense_bytes 的申请次数
static DENSE_ALLOCS: AtomicU64 = AtomicU64::new(0);
/// 窗口内单次最大申请字节
static MAX_LEN: AtomicU64 = AtomicU64::new(0);
/// 窗口内累计申请字节
static TOTAL: AtomicU64 = AtomicU64::new(0);

/// 包裹 `System` 的计数分配器（探针自身零分配：只动原子量）
struct Counting;

impl Counting {
  #[inline]
  fn record(len: usize) {
    let len = len as u64;
    TOTAL.fetch_add(len, Relaxed);
    MAX_LEN.fetch_max(len, Relaxed);
    if len == DENSE_BYTES.load(Relaxed) {
      DENSE_ALLOCS.fetch_add(1, Relaxed);
    }
  }
}

// SAFETY: 三对方法逐一转发 `System` 同法，只在转发前读一次开关位并累加原子量；
// `record` 不申请内存、不加锁，故重入安全。
unsafe impl GlobalAlloc for Counting {
  unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
    if ARMED.load(Relaxed) {
      Self::record(layout.size());
    }
    unsafe { System.alloc(layout) }
  }

  // 稠密缓冲经 `vec![0; dense_bytes]` 走 alloc_zeroed，必须同轨登记
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
  dense_allocs: u64,
  max_len: u64,
  total: u64,
}

/// 在计数窗口内执行 `f`，返回其结果与窗口快照（窗口外分配一律不计）
fn measured<R>(f: impl FnOnce() -> R) -> (R, Snap) {
  DENSE_BYTES.store(HyperLogLog::new().dense_bytes() as u64, Relaxed);
  DENSE_ALLOCS.store(0, Relaxed);
  MAX_LEN.store(0, Relaxed);
  TOTAL.store(0, Relaxed);
  ARMED.store(true, Relaxed);

  let r = f();
  let snap = Snap {
    dense_allocs: DENSE_ALLOCS.load(Relaxed),
    max_len: MAX_LEN.load(Relaxed),
    total: TOTAL.load(Relaxed),
  };
  ARMED.store(false, Relaxed);
  (r, snap)
}

fn parse_int(out: &[u8]) -> i64 {
  let text = from_utf8(&out[1..out.len() - 2]).unwrap();
  text.parse().unwrap()
}

/// 单键 PFADD 并回其应答帧
fn pfadd(sess: &mut RespServerSession, batch: &Batch, key: &[u8], elements: &[&[u8]]) -> Vec<u8> {
  let mut args = Vec::with_capacity(elements.len() + 1);
  args.push(key);
  args.extend_from_slice(elements);
  let mut out = Vec::new();
  sess.hyper_log_log_add(&args, batch, &mut out).unwrap();
  out
}

/// 问题一验收 1+2：超大非法载荷在校验位即拒，路径上不产生值拷贝，
/// 应答仍逐字节为 WRONGTYPE（快路径钉住记录口径；冷区语义由
/// hll_cold_degrade.rs 的伪造载荷用例继续守护）
#[test]
fn oversized_invalid_payload_copies_no_value() {
  // 单记录上限受页宽约束（测试装配 page_size 256KB），取 128KB 已远超任何
  // 合法 HLL 载荷（稠密 12304B / 稀疏 ≤ 4096B）一个量级
  const PAYLOAD: usize = 128 << 10;
  // 修复前 `read_user_sync(.., |v| v.to_vec())` 无条件整值物化，窗口内必见
  // PAYLOAD 级单块；64KB 以内视为存储读面与输出缓冲自身开销
  const COPY_FLOOR: u64 = 64 << 10;

  let plain = vec![0xAB_u8; PAYLOAD];
  // 伪造 HYLL 魔数 + 稠密编码而长度为 8MB（稠密须恰 dense_bytes）：魔数臂
  // 放行、长度臂拒绝，校验做到底仍以零拷贝收口
  let mut forged = vec![0_u8; PAYLOAD];
  forged[3] = 1;
  forged[4..8].copy_from_slice(&0x4859_4C4C_u32.to_le_bytes());

  with_batch(|sess, batch| {
    // 探针自检（正向对照）：同一窗口内显式申请 PAYLOAD 级单块必须被看见，
    // 否则下方「看不见即零拷贝」的断言不过是虚过
    let (kept, probe) = measured(|| vec![0xAB_u8; PAYLOAD].len());
    assert_eq!(kept, PAYLOAD);
    assert!(
      probe.max_len >= PAYLOAD as u64,
      "计数探针失效（窗口内看不见 {PAYLOAD}B 申请），下方断言无意义：{probe:?}"
    );

    batch
      .try_upsert_sync(b"hll:plain-big", &plain)
      .unwrap()
      .unwrap();
    batch
      .try_upsert_sync(b"hll:hyll-big", &forged)
      .unwrap()
      .unwrap();

    for key in [b"hll:plain-big".as_slice(), b"hll:hyll-big".as_slice()] {
      // 预热一轮：首轮触达页钉住与输出缓冲增长，不计入断言
      assert_eq!(
        pfadd(sess, batch, key, &[b"e"]),
        err_frame(RESP_ERR_WRONG_TYPE_HLL),
        "非法载荷须答 HLL WRONGTYPE"
      );

      let (_, snap) = measured(|| {
        assert_eq!(
          pfadd(sess, batch, key, &[b"e"]),
          err_frame(RESP_ERR_WRONG_TYPE_HLL)
        );
      });
      assert!(
        snap.max_len < COPY_FLOOR,
        "{key:?} 非法载荷 {PAYLOAD}B 在钉住记录上判毕即拒，不得整值物化：{snap:?}"
      );
      assert!(
        snap.total < COPY_FLOOR,
        "{key:?} 非法载荷路径累计分配应远小于载荷：{snap:?}"
      );
    }
  });
}

/// 问题二验收 3：多键 PFCOUNT 的稠密缓冲申请次数为常数（不随键数增长），
/// 稀疏首键整条命令至多展开一次（修复前为每键一次 12KB）
#[test]
fn multikey_pfcount_dense_scratch_is_constant() {
  with_batch(|sess, batch| {
    // 6 把小基数稀疏 HLL：每把 3 个互异元素
    let keys: Vec<Vec<u8>> = (0..6).map(|i| format!("hll:u{i}").into_bytes()).collect();
    for (i, key) in keys.iter().enumerate() {
      let elems: Vec<Vec<u8>> = (0..3).map(|j| format!("e{i}-{j}").into_bytes()).collect();
      let slices: Vec<&[u8]> = elems.iter().map(Vec::as_slice).collect();
      assert_eq!(pfadd(sess, batch, key, &slices), b":1\r\n");
    }
    let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

    let mut counts = Vec::new();
    for n in [2_usize, 4, 6] {
      let mut out = Vec::new();
      // 预热同键数一轮
      sess
        .hyper_log_log_length(&refs[..n], batch, &mut out)
        .unwrap();
      let (_, snap) = measured(|| {
        out.clear();
        sess
          .hyper_log_log_length(&refs[..n], batch, &mut out)
          .unwrap();
      });
      assert_eq!(parse_int(&out), (3 * n) as i64, "{n} 键虚拟并集基数");
      assert!(
        snap.dense_allocs <= 1,
        "{n} 键 PFCOUNT 稠密缓冲应至多展开一次：{snap:?}"
      );
      counts.push((n, snap.dense_allocs));
    }
    assert!(
      counts.iter().all(|(_, c)| *c == counts[0].1),
      "稠密缓冲申请次数须与键数无关（2/4/6 键同值）：{counts:?}"
    );
  });
}

/// 问题二验收 3+4：首键稠密时虚拟并集零稠密化申请——尺寸指纹分配恰为
/// 首键载荷的那一次 owned 副本（修复前为「副本 + 每键一份视图」共 N+1 次）；
/// 单键短路臂行为不变
#[test]
fn multikey_pfcount_dense_first_zero_scratch() {
  with_batch(|sess, batch| {
    // 首键稠密：单次 PFADD 3000 元素即越稀疏上限（sparse_initial_length 直接稠密）
    let dense_elems: Vec<Vec<u8>> = (0..3000).map(|i| format!("d{i}").into_bytes()).collect();
    let dense_slices: Vec<&[u8]> = dense_elems.iter().map(Vec::as_slice).collect();
    assert_eq!(pfadd(sess, batch, b"hll:dense", &dense_slices), b":1\r\n");

    let mut sparse_args: Vec<Vec<u8>> = Vec::new();
    for i in 0..3 {
      let key = format!("hll:s{i}").into_bytes();
      let elems: Vec<Vec<u8>> = (0..3).map(|j| format!("s{i}-{j}").into_bytes()).collect();
      let slices: Vec<&[u8]> = elems.iter().map(Vec::as_slice).collect();
      assert_eq!(pfadd(sess, batch, &key, &slices), b":1\r\n");
      sparse_args.push(key);
    }

    let mut args: Vec<&[u8]> = vec![b"hll:dense"];
    args.extend(sparse_args.iter().map(Vec::as_slice));

    let mut out = Vec::new();
    sess.hyper_log_log_length(&args, batch, &mut out).unwrap();
    let (_, snap) = measured(|| {
      out.clear();
      sess.hyper_log_log_length(&args, batch, &mut out).unwrap();
    });
    assert!(
      (parse_int(&out) - 3009).abs() < 100,
      "稠密首键 + 3 稀疏键并集基数异常：out={}",
      String::from_utf8_lossy(&out)
    );
    assert_eq!(
      snap.dense_allocs, 1,
      "稠密首键直接当累加器，本命令稠密化申请应为 0（唯一 dense_bytes 尺寸分配 = \
       首键载荷的 owned 副本）：{snap:?}"
    );

    // 单键短路臂（验收 4）：稠密单键亦只此一次载荷副本，无稠密视图
    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[b"hll:dense"], batch, &mut out)
      .unwrap();
    let (_, snap) = measured(|| {
      out.clear();
      sess
        .hyper_log_log_length(&[b"hll:dense"], batch, &mut out)
        .unwrap();
    });
    assert_eq!(snap.dense_allocs, 1, "单键短路臂无稠密化分配：{snap:?}");
  });
}
