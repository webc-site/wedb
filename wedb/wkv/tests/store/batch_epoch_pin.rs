//! 批处理纪元守卫长周期钉死回归：冷读 I/O 窗挂起与批间纪元让步
//!
//! 对标 C# 原型：
//! - libs/server/Storage/Session/MainStore/MainStoreOps.cs:ReadWithUnsafeContext
//!   （status.IsPending 先 context.EndUnsafe() 退出纪元保护，再驱动磁盘 I/O，
//!   完成后才 context.BeginUnsafe() 重入；I/O 期间 head 前移即 epochChanged 重探）
//! - libs/storage/Tsavorite/cs/src/core/Async/CompletePendingAsync.cs:43-56
//!   （UnsafeSuspendThread 包裹 I/O 等待窗）
//! - 重放会话逐记录常规 context（Tsavorite 重放不经 UnsafeContext 持整轮保护）
//!
//! 危害链（票 zcode-r19-wepoch 发现 1）：批守卫（enter_batch）跨 await 长磁盘
//! I/O 持有，重入臂只递增计数不刷新公布纪元，会话槽位公布纪元钉死入场值 E，
//! compute_safe_to_reclaim 取 min 恒 ≤ E，safe_head 排空屏障整轮停摆 → 前台写
//! 环形回绕 evict 等待（wait_safe_head_drained）连锁停摆。
//!
//! 自研依据: 批量接口单次获取条带写锁与进入纪元（transpile 契约 Batching API）

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  thread::{scope, sleep},
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::runtime::Runtime;

use crate::support::{config, open_store, pad};

/// 环几何：4KB 扇区对齐页 × 8 页 = 32KB 环；≈48B 记录每页 ≈85 条，环容
/// ≈680 条。预灌与回绕写入量按环容量倍数推导，保证回绕驱逐高频真实发生。
const PAGE: usize = 4096;
const PAGES: usize = 8;

/// 预灌 12 圈：最早一批键滑出 [head, tail) 内存窗（旧页 flush 进磁盘区，冷读成立）
const PRELOAD: u64 = 8192;

/// 冷读集：预灌最早 32 键（地址远低于 head，读必走 read_from_disk 磁盘 I/O 窗）
const COLD_KEYS: u64 = 32;

/// 回绕写入 6 圈：环形驱逐 evict 等待高频触发
const WRITES: u64 = 4096;

/// 停摆判定预算：修复后写线程/排空等待在毫秒级完成，超预算即判批守卫钉死。
/// 超预算路径先停对端线程解卡再 join，杜绝回归态测试挂死。
const STALL_BUDGET: Duration = Duration::from_secs(15);

/// 批会话冷读期间并发驱逐写不停摆、批读无撕裂（票验证点 1 直译：复刻
/// MainStoreOps epochChanged 场景——冷读进行中并发写回绕推进 safe_head，
/// 断言写路径等待不超时、冷读全部读回原值）
///
/// 读线程整段持批守卫循环冷读磁盘区键（C# 铁律所禁形态；修复后每次冷读磁盘
/// I/O 窗经 EpochSuspendGuard 按重入深度挂起，零纪元占用）。写线程普通会话
/// 回绕写，evict 等待面直刺批守卫钉死危害：修复前批守卫把会话槽位公布纪元
/// 钉死入场值，safe_head 排空屏障整轮停摆，写线程首圈回绕即卡死至读线程
/// 退出；修复后冷读 I/O 窗挂起，排空逐窗推进，写线程顺畅完成。
#[test]
fn batch_cold_read_does_not_stall_concurrent_eviction_writer() -> Void {
  let env = open_store(
    "batch_epoch_pin_cold_read.db",
    config(8192, PAGE, PAGES)?.with_max_sessions(16)?,
  )?;
  let store = env.store;

  // 预灌 8 圈：最早 COLD_KEYS 键的旧页 flush 进磁盘区（冷读路径成立）
  {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let session = store.new_session()?;
      for i in 0..PRELOAD {
        let key = format!("cold:{}", pad(i, 5));
        session
          .upsert(key.as_bytes(), format!("v{i}").as_bytes())
          .await?;
      }
      aok::Result::<()>::Ok(())
    })?;
  }

  let done_writes = AtomicU64::new(0);
  let stop_reader = AtomicBool::new(false);
  let torn = AtomicU64::new(0);

  scope(|s| -> aok::Result<()> {
    // 冷读线程：批会话整段持批守卫，循环冷读磁盘区键并逐次核对原值（撕裂断言）
    let store2 = Arc::clone(&store);
    let stop_reader_ref = &stop_reader;
    let torn_ref = &torn;
    let reader = s.spawn(move || -> Void {
      let rt = Runtime::new()?;
      rt.block_on(async {
        let session = store2.new_session()?;
        let batch = session.enter_batch();
        let keys: Vec<String> = (0..COLD_KEYS)
          .map(|i| format!("cold:{}", pad(i, 5)))
          .collect();
        let wants: Vec<Vec<u8>> = (0..COLD_KEYS)
          .map(|i| format!("v{i}").into_bytes())
          .collect();
        while !stop_reader_ref.load(Ordering::Relaxed) {
          for (i, k) in keys.iter().enumerate() {
            if batch.read(k.as_bytes()).await?.as_deref() != Some(wants[i].as_slice()) {
              torn_ref.fetch_add(1, Ordering::Relaxed);
            }
          }
        }
        aok::Result::<()>::Ok(())
      })?;
      OK
    });

    // 写线程：普通会话回绕写（新键纯追加），环形驱逐 evict 等待高频触发
    let done_ref = &done_writes;
    let store2 = Arc::clone(&store);
    let writer = s.spawn(move || -> Void {
      let rt = Runtime::new()?;
      rt.block_on(async {
        let session = store2.new_session()?;
        for i in 0..WRITES {
          let key = format!("hot:{}", pad(i, 5));
          session
            .upsert(key.as_bytes(), format!("h{i}").as_bytes())
            .await?;
          done_ref.fetch_add(1, Ordering::Relaxed);
        }
        aok::Result::<()>::Ok(())
      })?;
      OK
    });

    // 预算观测：超预算判停摆（先停读线程解卡，杜绝回归态挂死）
    let start = Instant::now();
    let mut stalled = false;
    while done_writes.load(Ordering::Relaxed) < WRITES {
      if start.elapsed() > STALL_BUDGET {
        stalled = true;
        break;
      }
      sleep(Duration::from_millis(5));
    }
    stop_reader.store(true, Ordering::Relaxed);
    reader.join().expect("冷读线程正常结束")?;
    writer.join().expect("写线程正常结束")?;

    assert!(
      !stalled,
      "批会话冷读期间写线程 {STALL_BUDGET:?} 内未完成 {WRITES} 条：纪元排空屏障被批守卫钉死（票 zcode-r19-wepoch 发现 1）"
    );
    assert_eq!(done_writes.load(Ordering::Relaxed), WRITES);
    assert_eq!(
      torn.load(Ordering::Relaxed),
      0,
      "批会话冷读观测到撕裂读（重入后快照复检协议失效）"
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 批间纪元让步解除排空等待（票验证点 2 直译：长周期批守卫在场，并发排空
/// 屏障等待无停摆——重放逐记录 epoch_yield 让步的微观等价物）
///
/// 批守卫线程整段持批窗并周期性 epoch_yield（瞬时挂起窗：槽位逐层退出保护
/// 区即重入，重入首层 CAS 公布最新纪元）。排空等待线程反复 bump_and_wait
/// （对标 evict/检查点 fence 的排空屏障收尾套路，忙等采样）：修复前批守卫
/// 钉死公布纪元，目标纪元永不被 safe 追平，首轮即卡死至批守卫线程退出；
/// 修复后每轮等待在让步窗内解除（重入公布的最新纪元 > 目标纪元）。
#[test]
fn batch_epoch_yield_unblocks_safe_epoch_drain() -> Void {
  let env = open_store(
    "batch_epoch_pin_yield.db",
    config(8192, PAGE, PAGES)?.with_max_sessions(8)?,
  )?;
  let store = env.store;
  let epoch = Arc::clone(&store.epoch);

  const DRAIN_ROUNDS: u64 = 8;
  let done = AtomicU64::new(0);
  let stop_holder = AtomicBool::new(false);

  scope(|s| -> aok::Result<()> {
    // 批守卫线程：enter_batch 全程在场（同步域，无需运行时），周期让步
    let stop_holder_ref = &stop_holder;
    let holder = s.spawn(move || -> Void {
      let session = store.new_session()?;
      let batch = session.enter_batch();
      while !stop_holder_ref.load(Ordering::Relaxed) {
        batch.epoch_yield();
        sleep(Duration::from_millis(2));
      }
      OK
    });

    // 排空等待线程：反复 bump_and_wait（自身无保护，契约满足）
    let done_ref = &done;
    let epoch2 = Arc::clone(&epoch);
    let waiter = s.spawn(move || -> Void {
      for _ in 0..DRAIN_ROUNDS {
        let target = epoch2.current_epoch();
        epoch2.bump_and_wait(target);
        done_ref.fetch_add(1, Ordering::Relaxed);
      }
      OK
    });

    // 预算观测：超预算判停摆（先停批守卫线程解卡，杜绝回归态挂死）
    let start = Instant::now();
    let mut stalled = false;
    while done.load(Ordering::Relaxed) < DRAIN_ROUNDS {
      if start.elapsed() > STALL_BUDGET {
        stalled = true;
        break;
      }
      sleep(Duration::from_millis(5));
    }
    stop_holder.store(true, Ordering::Relaxed);
    holder.join().expect("批守卫线程正常结束")?;
    waiter.join().expect("排空等待线程正常结束")?;

    assert!(
      !stalled,
      "批守卫在场时 {DRAIN_ROUNDS} 轮排空等待 {STALL_BUDGET:?} 内未逐轮解除：epoch_yield 让步失效（票 zcode-r19-wepoch 执行方案 1/2）"
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}
