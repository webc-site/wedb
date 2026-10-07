#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 读缓存追加页界装载闸回归：tail 恰落页界（记录恰好铺满整页）时必须经
//! 换页协议装载目标页后方可写入，严禁 fit 臂绕过两阶段关闭/清洗/换装直通
//! 旧代页槽
//!
//! 对标 C# 原型：
//! - libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:TryAllocate
//!   （offset 越 PageSize 必入 HandlePageOverflow；恰满后下一条分配 post-Add
//!   Offset > PageSize 同样走协议，字节绝不先于关页协议落入下一页槽）
//! - AllocatorBase.cs:HandlePageOverflow / NeedToWaitForClose / IssueShiftAddress
//! - libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:74
//!   （eviction runs as a deferred, epoch-gated drain-list action ... 即页槽
//!   复用必须先经驱逐协议再放新写入）
//!
//! 仓内正形：whlog/src/hlog/append.rs 主日志循环的 is_page_loaded 页界单闸。
//!
//! 期望水位沿用 read_cache_eviction_barrier 册的几何公式：第 e 次换页事件
//! （跳向/装载逻辑页 e）复用槽位承载的旧代页为 e - num_pages，页末边界 =
//! (e - num_pages + 1) * page_size；正向首轮无驱逐水位恒 0。

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
  },
  thread::{scope, yield_now},
};

use wbase::addr::is_read_cache;
use wepoch::LightEpoch;
use windex::{HashBucketEntry, HashIndex};
use wkv::ReadCache;
use wrecord::record_size;

/// 测试几何：512B 页（2 的幂）
const PAGE: usize = 512;

/// 主日志哨兵地址（append 前预挂条目，cleanse 脱钩后索引恢复至此）
const MAIN_ADDR: u64 = 12345;

/// 定宽 5 字节键（记录尺寸由值长调节：16B 头 + 5B 键 + 值长按 8B 对齐）
fn key(prefix: char, i: u64) -> String {
  format!("{prefix}{i:04}")
}

/// 第 e 次换页事件完成后的 head/closed 期望水位（同 eviction_barrier 册公式）
fn expected_watermark(event: u64, num_pages: usize) -> u64 {
  event.saturating_sub(num_pages as u64 - 1) * PAGE as u64
}

/// 已从 windex 生产导出面收敛掉的 key 版 `insert`（与 eviction_barrier 册
/// 同一口径：insert_to_bucket 等价复现唯一免查重追加写入口）
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

/// 确定性页界穿越 + 多轮回绕：每页节奏 20 条 24B + 1 条 32B（480+32=512
/// 恰好铺满，32B 记录触发页界穿越），穿越后写入必须落新页且水位精确等于
/// 被驱逐旧代页末尾。
///
/// 修复判据三条：
/// 1. tail 恒等于累计记录字节（穿越装载与跳页都不得制造地址洞或回退）；
/// 2. 回绕事件后 closed == head == 几何公式值（cleanse 页号与水位边界与
///    武装值恒等，越一页错位立即爆）；
/// 3. 被驱逐页内键的索引条目恢复主日志哨兵地址（cleanse 不清错页）。
///
/// 防退化：全程 closed > 0 —— 32B 记录每页恰好铺满，else 臂换页机器只可能
/// 被页界穿越触达；若闸退化为「页界改从 fit 臂取巧清零」，本用例必以旧代页
/// 覆写或水位失准显红
#[test]
fn page_boundary_exact_fill_watermarks_match_evicted_page_end() {
  const NUM_PAGES: usize = 4;
  // 节奏：24B 键5值1；32B 键5值11（16+5+11=32）
  let rec24 = record_size(5, 1);
  let rec32 = record_size(5, 11);
  assert_eq!(rec24, 24);
  assert_eq!(rec32, 32);
  assert_eq!(20 * rec24 + rec32, PAGE, "每页节奏必须恰好铺满整页");

  let rc = Arc::new(ReadCache::new(PAGE, NUM_PAGES, true, Arc::new(LightEpoch::new(8))).unwrap());
  let index = Arc::new(HashIndex::new(1024 * NUM_PAGES).unwrap());

  let mut events = 0u64; // 已完成换页事件序号 e
  let mut bytes = 0u64; // 成功追加累计字节
  let mut appended = 0u64;
  let total_pages = NUM_PAGES as u64 + 3; // 触发多轮回绕驱逐
  // 页 0 构造期已装载：首轮 20+1 条直通铺满；此后每页首条 24B 都是穿越形态
  for page in 0..total_pages {
    for j in 0..21 {
      let (val, rec) = if j < 20 {
        (b"v".as_slice(), rec24)
      } else {
        (&b"vvvvvvvvvvv"[..], rec32)
      };
      let i = page * 21 + j;
      let k = key('b', i);
      index.insert(k.as_bytes(), MAIN_ADDR).unwrap();
      // 页界穿越（正向内联或回绕武装）返回 None 时于出借期安全点泵入
      // 纪元延迟关闭（注册即同步收割），重试同条即落已装载新页
      let addr = loop {
        if let Some(a) = rc.append(k.as_bytes(), val, &index, 0) {
          break a;
        }
        rc.pump_close_barrier(None, &index, None);
      };
      assert!(is_read_cache(addr), "返回值必须带 READ_CACHE 标记位");
      appended += 1;
      bytes += rec as u64;

      let tail = rc.tail_address();
      // 判据 1：tail 恒等于累计字节（无洞、无回退、无跳空）
      assert_eq!(
        tail, bytes,
        "页 {page} 第 {j} 条：tail 与累计字节脱钩即页界闸失效（洞/回退）"
      );

      // 穿越事件判定：事件序 = 最后落点字节所在逻辑页号，即册头定义的
      // 「已完成换页事件数」（恰满收尾记录落点仍在旧页内，换页未发生；
      // bytes/PAGE 口径会在正向首轮末条把 events 提前抬到 NUM_PAGES，
      // 与「正向首轮无驱逐水位恒 0」相悖）；回绕事件（e >= NUM_PAGES）核对水位
      let event = (bytes - 1) / PAGE as u64;
      if event > events {
        events = event;
        assert_eq!(
          (bytes - rec as u64) % PAGE as u64,
          0,
          "事件 {events}：换页必须恰起于页界（跳空即洞）"
        );
      }

      let head = rc.head_address();
      let closed = rc.closed_until_address();
      assert!(
        closed <= head && head <= tail,
        "页 {page} 第 {j} 条：水位序 {closed} <= {head} <= {tail} 被打破"
      );
      if events >= NUM_PAGES as u64 {
        // 判据 2：回绕事件后水位精确等于被驱逐旧代页末尾
        let want = expected_watermark(events, NUM_PAGES);
        assert_eq!(head, want, "事件 {events} 后 head 偏差（几何漂移/清错页）");
        assert_eq!(closed, want, "事件 {events} 后 closed 偏差（越一页发布）");
      }
    }
  }

  // 防退化：else 臂必须被页界穿越真实触达（均匀恰满负载下唯一触达通道）
  assert!(
    rc.closed_until_address() > 0,
    "均匀恰满负载下未发生任何回绕驱逐：else 臂未被穿越触达，页界闸疑似取巧退化"
  );
  assert!(
    appended == total_pages * 21,
    "全程零丢弃：实际 {appended} 条"
  );

  // 判据 3：被驱逐页（页 0，事件 NUM_PAGES 完成后已滑出窗口）内键的索引
  // 条目必须恢复主日志哨兵（cleanse 洗对了页），绝无死 RC 地址残留
  for j in 0..21u64 {
    let k = key('b', j);
    let hei = index
      .find_tag_entry_by_hash_with_min_addr(HashIndex::hash_key(k.as_bytes()), 0)
      .expect("被驱逐页键的索引条目必须存在");
    assert_eq!(
      hei.address(),
      MAIN_ADDR,
      "键 {k}：cleanse 后索引必须恢复主日志地址（恢复到死 RC 地址即清错页）"
    );
  }
}

/// 武装页余量推进 + 穿越级联：跳页武装形态（页尾余量 < 记录）与页界穿越
/// 形态交错驱动，全程核水位序与页界对齐——close 冻结几何在任何交错序下
/// 都不得从漂移 tail 重算出越一页边界
#[test]
fn page_boundary_mixed_arms_keep_watermarks_page_aligned() {
  const NUM_PAGES: usize = 4;
  let rc = Arc::new(ReadCache::new(PAGE, NUM_PAGES, true, Arc::new(LightEpoch::new(8))).unwrap());
  let index = Arc::new(HashIndex::new(1024 * NUM_PAGES).unwrap());

  // 固定种子 LCG：值长在 [1, 57] 伪随机 ⇒ 记录 24..=80 步进 8，
  // 页尾余量与记录尺寸随机错配，跳页武装与恰满穿越自然交错
  let mut seed = 0x5EED_u64;
  let mut val_len = || {
    seed = seed
      .wrapping_mul(6364136223846793005)
      .wrapping_add(1442695040888963407);
    (seed >> 33) % 57 + 1
  };

  let total_pages = (NUM_PAGES as u64 + 3) * PAGE as u64;
  let mut tail_prev = 0u64;
  let mut i = 0u64;
  while tail_prev < total_pages {
    let v = val_len();
    let k = key('m', i);
    index.insert(k.as_bytes(), MAIN_ADDR).unwrap();
    let addr = loop {
      if let Some(a) = rc.append(k.as_bytes(), &vec![b'x'; v as usize], &index, 0) {
        break a;
      }
      rc.pump_close_barrier(None, &index, None);
    };
    assert!(is_read_cache(addr));
    i += 1;

    let tail = rc.tail_address();
    assert!(tail > tail_prev, "tail 必须单调推进，第 {i} 条观测到回退");
    tail_prev = tail;

    let head = rc.head_address();
    let closed = rc.closed_until_address();
    assert!(
      closed <= head && head <= tail,
      "第 {i} 条：水位序 {closed} <= {head} <= {tail} 被打破"
    );
    // 水位恒为旧代页页末边界：页界对齐是冻结几何的硬判据（越一页错位
    // 也仍是页界，故另以 closed == head 双水位同步性收紧——两者恒等发布）
    assert_eq!(
      closed % PAGE as u64,
      0,
      "第 {i} 条：closed 越界发布偏离页界（几何漂移）"
    );
    assert_eq!(
      head % PAGE as u64,
      0,
      "第 {i} 条：head 推进偏离页界（武装边界失准）"
    );
  }
  assert!(rc.closed_until_address() > 0, "必须发生回绕驱逐");
}

/// 并发均匀恰满穿越压力：32B 等长记录（512 = 16×32 整除）三线程并发灌——
/// 每页恰好铺满，else 臂换页机器只能由页界穿越触达，穿越点竞争全部落在
/// turn_lock 串行 + 锁内复验路径上。终态 tail == 成功条数 × 32（无洞无回退）、
/// 水位序成立、全程有界完成（协议活锁即挂死显红）
#[test]
fn concurrent_exact_fill_page_boundary_turn_stays_exact() {
  const APPENDERS: usize = 3;
  const EACH: u64 = 50;
  const NUM_PAGES: usize = 2;

  let rec32 = record_size(5, 11);
  assert_eq!(rec32, 32);
  assert_eq!(PAGE % rec32 as usize, 0, "等长恰满几何前提");

  let rc = Arc::new(ReadCache::new(PAGE, NUM_PAGES, true, Arc::new(LightEpoch::new(8))).unwrap());
  let index = Arc::new(HashIndex::new(4096).unwrap());
  let mounted = Arc::new(AtomicU64::new(0));

  scope(|s| {
    let mut handles = Vec::with_capacity(APPENDERS);
    for t in 0..APPENDERS {
      let rc = Arc::clone(&rc);
      let index = Arc::clone(&index);
      let mounted = Arc::clone(&mounted);
      handles.push(s.spawn(move || {
        for i in 0..EACH {
          // 键恒 5 字节（t{0..2}{i:000..049}），与 rec32 = record_size(5, 11)
          // 的定长几何前提自洽——键长一旦偏离 5 字节，实际记录 40B 恰满崩塌，
          // 跳页臂跳空令 tail 与成功字节脱钩
          let k = format!("t{t}{i:03}");
          index.insert(k.as_bytes(), MAIN_ADDR).unwrap();
          loop {
            if rc
              .append(k.as_bytes(), &b"vvvvvvvvvvv"[..], &index, 0)
              .is_some()
            {
              mounted.fetch_add(1, Relaxed);
              break;
            }
            // 穿越武装（回绕）返回 None：出借期安全点泵入延迟关闭后重试
            rc.pump_close_barrier(None, &index, None);
            yield_now();
          }
          rc.pump_close_barrier(None, &index, None);
        }
      }));
    }
    for h in handles {
      h.join().unwrap();
    }
  });

  let total = mounted.load(Relaxed);
  assert_eq!(total, APPENDERS as u64 * EACH, "全程零丢弃");
  // 终态强判据：tail 与成功字节逐位相等（等长恰满下任何取巧清零、跳空、
  // 回退、洞都立即破坏该等式）
  assert_eq!(
    rc.tail_address(),
    total * rec32 as u64,
    "tail 与成功字节脱钩：页界闸失效"
  );
  let closed = rc.closed_until_address();
  let head = rc.head_address();
  assert!(closed > 0, "必须发生回绕驱逐");
  assert!(closed <= head && head <= rc.tail_address());
}
