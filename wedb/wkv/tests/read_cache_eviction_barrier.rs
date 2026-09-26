//! 环形换页驱逐等待屏障回归：ClosedUntilAddress 推进值与发布时序契约
//!
//! 对标 C# 原型：
//! - libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:IssueShiftAddress
//!   与 ShiftHeadAddress（MonotonicUpdate(ref HeadAddress, newHeadAddress) 后经
//!   epoch 触发 OnPagesClosed）
//! - AllocatorBase.cs:OnPagesClosedWorkerCore（逐页
//!   MonotonicUpdate(ref ClosedUntilAddress, end)，end 恒为该被关页页界且
//!   <= HeadAddress；核心不变量 ClosedUntilAddress <= HeadAddress <= TailAddress）
//! - Index/Tsavorite/Implementation/ReadCache.cs:ReadCacheNeedToWaitForEviction
//!   与 EpochOperations.cs:SpinWaitUntilRecordIsClosed（门控 `abs < HeadAddress`，
//!   关闭判据 `abs < ClosedUntilAddress`，至少续转一轮 ProtectAndDrain）
//!
//! 期望水位一律由几何公式推导：环形缓冲 page_size × num_pages，等长记录逐页
//! 填充；第 e 次换页事件（跳向逻辑页 e）复用槽位承载的旧代页为
//! e - num_pages，其页末边界 = (e - num_pages + 1) * page_size；正向首轮
//! （e < num_pages）无驱逐，水位恒 0。
//!
//! 自研依据: 读缓存驱逐屏障（C# 换页面语义对标 libs/storage/Tsavorite/cs/test/test.session/ReadCacheChainTests.cs）

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed},
  },
  thread::scope,
};

use wbase::addr::{is_read_cache, to_absolute};
use wepoch::LightEpoch;
use windex::{HashBucketEntry, HashIndex};
use wkv::ReadCache;
use wrecord::record_size;

/// 测试几何：512B 页（2 的幂）；单条记录 = 16B 头 + 5B 键 + 1B 值，8B 对齐
/// 隐式填充后 24B；每页恰容纳 21 条（512 = 21*24 + 8，页余量 8 < 24 触发换页）
const PAGE: usize = 512;

/// 主日志哨兵地址（append 前预挂条目，cleanse 脱钩后索引恢复至此）
const MAIN_ADDR: u64 = 12345;

/// 已从 windex 生产导出面收敛掉的 key 版 `insert`：测试灌数按唯一免查重追加
/// 写入口 [`HashIndex::insert_to_bucket`] 等价复现（与 src 内测试同一口径）
trait HashIndexTestOps {
  fn insert(&self, key: &[u8], address: u64) -> windex::Result<()>;
}

impl HashIndexTestOps for HashIndex {
  fn insert(&self, key: &[u8], address: u64) -> windex::Result<()> {
    let hash = HashIndex::hash_key(key);
    let tag = HashBucketEntry::tag_from_hash(hash);
    self.insert_to_bucket(self.bucket_index_for_hash(hash), tag, address)
  }
}

/// 定宽 5 字节键（保证记录尺寸恒 24B，页几何公式成立）
fn key(prefix: char, i: u64) -> String {
  format!("{prefix}{i:04}")
}

/// 每页记录数：页大小对记录尺寸取整；前置校验页尾余量小于单记录，
/// 保证每次换页事件恰发生在页末、每页灌入条数恒定
fn records_per_page() -> usize {
  let rec = record_size(5, 1);
  assert_eq!(rec, 24, "16B 头 + 5B 键 + 1B 值按 8B 对齐");
  assert!(
    PAGE % rec < rec,
    "页尾余量 {} 必须小于记录尺寸，几何公式才成立",
    PAGE % rec
  );
  PAGE / rec
}

/// 第 e 次换页事件（跳向逻辑页 e）完成后的 head/closed 期望水位（见模块头
/// 几何推导；严格对标 OnPagesClosedWorkerCore 中 `end` 取被关页页界）
fn expected_watermark(event: u64, num_pages: usize) -> u64 {
  event.saturating_sub(num_pages as u64 - 1) * PAGE as u64
}

/// 多页连续回绕换页：单线程等长追加，逐条断言 closed <= head <= tail 水位序，
/// 且 head 与 closed 每轮均精确等于几何公式给出的被驱逐旧代页末尾
/// （旧缺陷双算式：首轮回绕 head 推进归 0、closed 直推 next_page_start 超前
/// 整环容量，此处两式同时归位）
#[test]
fn multi_lap_turn_watermarks_match_evicted_page_end() {
  for num_pages in [1usize, 2, 4] {
    let rc = Arc::new(ReadCache::new(PAGE, num_pages, true, Arc::new(LightEpoch::new(8))).unwrap());
    let index = Arc::new(HashIndex::new(num_pages * 512).unwrap());
    let per_page = records_per_page();
    let rec = record_size(5, 1) as u64;

    let mut events = 0u64; // 已完成的换页事件序号 e（第 e 次跳向逻辑页 e）
    let mut appends = 0u64; // 成功追加总数 k
    let mut prev_tail = 0u64;
    // 灌满 num_pages + 3 页：num_pages=2 触发 5 次驱逐、4 触发 4 次驱逐
    let total = (num_pages as u64 + 3) * per_page as u64;
    for i in 0..total {
      let key = key('q', i);
      index.insert(key.as_bytes(), MAIN_ADDR).unwrap();
      // 回绕换页武装拍返回 None（旧页关闭已挂入纪元延迟队列）：本线程处于
      // 出借期安全点（无任何借用），泵入注册即同步收割执行，重试同条即落新页
      let addr = loop {
        if let Some(a) = rc.append(key.as_bytes(), b"v", &index, 0) {
          break a;
        }
        rc.pump_close_barrier(&index, None);
      };
      assert!(is_read_cache(addr), "返回值必须带 READ_CACHE 标记位");
      appends += 1;

      let tail = rc.tail_address();
      if tail - prev_tail > rec {
        // 页末跳跃：本次换页事件完成，新页首条记录恰落在页界后一记录处
        events += 1;
        assert_eq!(
          tail % PAGE as u64,
          rec,
          "num_pages={num_pages} 事件 {events}：换页后首条记录必须落在新页页界"
        );
      }
      prev_tail = tail;

      // tail 精确几何式：e 个页界 + 页内第 k - e*per_page 条
      assert_eq!(
        tail,
        events * PAGE as u64 + (appends - events * per_page as u64) * rec,
        "num_pages={num_pages} 第 {appends} 条：tail 几何式被打破"
      );

      let head = rc.head_address();
      let closed = rc.closed_until_address();
      // C# 核心不变量：ClosedUntilAddress <= HeadAddress <= TailAddress
      assert!(
        closed <= head && head <= tail,
        "num_pages={num_pages} 第 {appends} 条：水位序 {closed} <= {head} <= {tail} 被打破"
      );
      // 两道水位均须精确等于第 e 次换页所驱逐旧代页的页末：head 在置目标槽
      // CLOSED 前排至该边界，closed 在 cleanse 完成后追至同一边界
      let want = expected_watermark(events, num_pages);
      assert_eq!(
        head, want,
        "num_pages={num_pages} 事件 {events} 后 head 偏差"
      );
      assert_eq!(
        closed, want,
        "num_pages={num_pages} 事件 {events} 后 closed 偏差"
      );
    }
    assert!(
      events > num_pages as u64,
      "num_pages={num_pages}：应发生多轮连续回绕驱逐，实际事件 {events}"
    );
  }
}

/// 屏障解除口径：首轮回绕（事件 e=2 驱逐页 0）后，页 0 内已驱逐地址的
/// need_to_wait_for_eviction 按 SpinWaitUntilRecordIsClosed「至少
/// ProtectAndDrain 一轮」契约恰好续转一轮即解除（closed 已越过该地址）；
/// 窗口内地址与非 RC 地址一律 false 且零轮次
#[test]
fn eviction_gate_unblocks_after_closed_published() {
  let rc = Arc::new(ReadCache::new(PAGE, 2, true, Arc::new(LightEpoch::new(8))).unwrap());
  let index = Arc::new(HashIndex::new(1024).unwrap());
  let per_page = records_per_page();

  // 武装拍 None → 安全点泵入延迟关闭 → 重试（单线程无借用，注册即同步收割）
  let append1 = |key: String| -> u64 {
    loop {
      if let Some(a) = rc.append(key.as_bytes(), b"v", &index, 0) {
        return a;
      }
      rc.pump_close_barrier(&index, None);
    }
  };
  let mut first_addr = None; // 页 0 内首个 RC 记录（带标记虚拟地址）
  // 灌 2*per_page + 2 条：跨过两次换页（e=1 跳页 1 无驱逐，e=2 跳页 2 驱逐页 0）
  for i in 0..(2 * per_page as u64 + 2) {
    let key = key('g', i);
    index.insert(key.as_bytes(), MAIN_ADDR).unwrap();
    let addr = append1(key.clone());
    if i == 0 {
      first_addr = Some(addr);
    }
  }
  let want = expected_watermark(2, 2);
  assert_eq!(
    rc.closed_until_address(),
    want,
    "首轮回绕后 closed 应为页 0 页末"
  );
  assert_eq!(rc.head_address(), want, "首轮回绕后 head 应为页 0 页末");

  let evicted = first_addr.expect("首条记录必在页 0");
  assert!(to_absolute(evicted) < rc.head_address(), "首条应已滑出窗口");
  let rounds = AtomicU32::new(0);
  assert!(rc.need_to_wait_for_eviction(evicted, || {
    rounds.fetch_add(1, Relaxed);
  }));
  assert_eq!(
    rounds.load(Relaxed),
    1,
    "closed 已越过该地址：自旋臂必须恰好续转一轮即解除（C# 至少一轮语义）"
  );

  // 窗口内最新记录：门控直落 false，零轮次
  let key = key('g', 2 * per_page as u64 + 2);
  index.insert(key.as_bytes(), MAIN_ADDR).unwrap();
  let fresh = append1(key);
  let rounds2 = AtomicU32::new(0);
  assert!(!rc.need_to_wait_for_eviction(fresh, || {
    rounds2.fetch_add(1, Relaxed);
  }));
  assert_eq!(rounds2.load(Relaxed), 0, "窗口内地址不得进入自旋");
}

/// 并发驱逐屏障压力回归：3 追加线程持续跨多轮回绕换页（≈57 个换页事件）并
/// 周期性对早期地址走驱逐等待协议，1 采样线程持续核对水位序——先读 closed
/// 后读 head，两水位皆单调，观测序即真实违反序（c > h 观测到 ⇔ 存在瞬时
/// closed > head）。全程零违例、协议零活锁、终态 closed <= head <= tail。
/// 旧缺陷下 closed 恒超前 head 整环容量，采样第一拍即违例
#[test]
fn concurrent_wraps_never_publish_closed_ahead_of_head() {
  const APPENDERS: usize = 3;
  const EACH: u64 = 400; // 3*400 条 ≈ 57 个换页事件，其中回绕驱逐 56 次

  let rc = Arc::new(ReadCache::new(PAGE, 2, true, Arc::new(LightEpoch::new(8))).unwrap());
  let index = Arc::new(HashIndex::new(4096).unwrap());
  let stop = Arc::new(AtomicBool::new(false));
  let violations = Arc::new(AtomicU64::new(0));
  let barrier_waits = Arc::new(AtomicU64::new(0));

  scope(|s| {
    // 先 collect 全部句柄再统一 join（禁止逐个 join 造成采样依赖饥饿）
    let mut appender_handles = Vec::with_capacity(APPENDERS);
    for t in 0..APPENDERS {
      let rc = Arc::clone(&rc);
      let index = Arc::clone(&index);
      let barrier_waits = Arc::clone(&barrier_waits);
      appender_handles.push(s.spawn(move || {
        let mut mounted: Vec<u64> = Vec::new();
        for i in 0..EACH {
          let key = format!("t{t}x{i:04}");
          index.insert(key.as_bytes(), MAIN_ADDR).unwrap();
          if let Some(addr) = rc.append(key.as_bytes(), b"v", &index, 0) {
            mounted.push(addr);
          }
          // 回绕换页武装后于出借期安全点泵入纪元延迟关闭（未武装时零开销直返）
          rc.pump_close_barrier(&index, None);
          // 周期性对链上早期地址走一遍驱逐等待协议：若该页换页进行中，
          // 自旋必须真实阻塞至 cleanse 发布页界后解除（协议有界）
          if i % 64 == 63
            && let Some(old) = mounted.first()
          {
            let rounds = AtomicU32::new(0);
            if rc.need_to_wait_for_eviction(*old, || {
              rounds.fetch_add(1, Relaxed);
            }) {
              assert!(rounds.load(Relaxed) >= 1, "自旋臂至少续转一轮");
              barrier_waits.fetch_add(1, Relaxed);
            }
          }
        }
      }));
    }
    // 采样线程：closed 先读、head 后读（单调序推导见函数头）
    let rc2 = Arc::clone(&rc);
    let stop2 = Arc::clone(&stop);
    let violations2 = Arc::clone(&violations);
    let sampler = s.spawn(move || {
      while !stop2.load(Relaxed) {
        let closed = rc2.closed_until_address();
        let head = rc2.head_address();
        if closed > head {
          violations2.fetch_add(1, Relaxed);
        }
      }
    });

    for h in appender_handles {
      h.join().unwrap();
    }
    stop.store(true, Relaxed);
    sampler.join().unwrap();
  });

  assert_eq!(
    violations.load(Relaxed),
    0,
    "多轮回绕换页中观测到 closed 超前 head 即屏障不变量违例"
  );
  assert!(
    rc.closed_until_address() > 0,
    "压力轮次必须真实发生回绕驱逐"
  );
  assert!(
    rc.closed_until_address() <= rc.head_address() && rc.head_address() <= rc.tail_address(),
    "终态水位序 closed <= head <= tail 必须成立"
  );
  assert!(
    barrier_waits.load(Relaxed) > 0,
    "压力下驱逐等待协议必须真实触发过（自旋解除闭环）"
  );
}
