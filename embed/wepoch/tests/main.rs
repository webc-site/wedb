//! LightEpoch 底层结构体、内存对齐与 TLS 生命周期测试

use std::{
  mem::{align_of, size_of},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  thread,
};

use aok::{OK, Void};
use log::info;
use wepoch::{EpochEntry, Error, LightEpoch};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// 验证 EpochEntry 与 LightEpoch 控制字段严格 64 字节 Cacheline 对齐与内存排布
#[test]
fn cacheline_alignment() -> Void {
  info!("验证 EpochEntry 严格 64 字节 Cacheline 对齐与内存排布");

  const CACHELINE_SIZE: usize = 64;
  const ENTRY_COUNT: usize = 16;

  // 验证结构体自身大小与对齐要求严格为 64 字节
  assert_eq!(size_of::<EpochEntry>(), CACHELINE_SIZE);
  assert_eq!(align_of::<EpochEntry>(), CACHELINE_SIZE);

  // 验证 LightEpoch 结构体级 64 字节对齐
  assert_eq!(align_of::<LightEpoch>(), CACHELINE_SIZE);
  let epoch = LightEpoch::new(ENTRY_COUNT);
  assert_eq!(
    &epoch as *const LightEpoch as usize % CACHELINE_SIZE,
    0,
    "LightEpoch 实例基址必须 64 字节对齐"
  );
  assert_eq!(epoch.entries.len(), ENTRY_COUNT);

  // 验证堆上分配的每一个条目的起始物理地址均 64 字节对齐
  for (i, entry) in epoch.entries.iter().enumerate() {
    let ptr = entry as *const EpochEntry as usize;
    assert_eq!(
      ptr % CACHELINE_SIZE,
      0,
      "条目 {i} 的内存地址 {ptr:#x} 未满足 64 字节对齐"
    );
  }

  // 验证相邻条目之间的步长严格等于 64 字节，彻底杜绝 CPU 缓存行伪共享
  for pair in epoch.entries.windows(2) {
    let ptr1 = &pair[0] as *const EpochEntry as usize;
    let ptr2 = &pair[1] as *const EpochEntry as usize;
    assert_eq!(ptr2 - ptr1, CACHELINE_SIZE);
  }

  // 验证 LightEpoch 控制字段缓存行隔离：current_epoch、safe_to_reclaim_epoch、drain_count 各占独立缓存行
  let line_of = |ptr: *const u8| ptr as usize / CACHELINE_SIZE;
  let cur_line = line_of(&epoch.current_epoch as *const _ as *const u8);
  let safe_line = line_of(&epoch.safe_to_reclaim_epoch as *const _ as *const u8);
  let count_line = line_of(&epoch.drain_count as *const _ as *const u8);
  let mask_line = line_of(&epoch.user_word_mask as *const _ as *const u8);
  assert_ne!(
    cur_line, safe_line,
    "current_epoch 与 safe_to_reclaim_epoch 不得共享缓存行"
  );
  assert_ne!(
    cur_line, count_line,
    "current_epoch 与 drain_count 不得共享缓存行"
  );
  assert_ne!(
    safe_line, count_line,
    "safe_to_reclaim_epoch 与 drain_count 不得共享缓存行"
  );
  assert_eq!(
    count_line, mask_line,
    "drain_count 与 user_word_mask 应同属一行"
  );

  OK
}

/// 验证 Participant 参与者容量上限与 Drop 自动释放槽位复用
#[test]
fn participant_capacity_and_slot_recycling() -> Void {
  info!("验证参与者容量上限与 Drop 自动释放槽位复用");

  let max_threads = 3;
  let epoch = Arc::new(LightEpoch::new(max_threads));

  let p0 = epoch.register()?;
  let p1 = epoch.register()?;
  let p2 = epoch.register()?;
  assert_eq!(p0.entry_idx(), 0);
  assert_eq!(p1.entry_idx(), 1);
  assert_eq!(p2.entry_idx(), 2);

  // 槽位已满，再次注册应报错
  assert!(matches!(
    epoch.register(),
    Err(Error::ExceededMaxThreads(cap)) if cap == max_threads
  ));

  // Drop 释放中间槽位 p1
  drop(p1);

  // 重新注册，应复用空出的槽位 1
  let p_new = epoch.register()?;
  assert_eq!(p_new.entry_idx(), 1);

  OK
}

/// 验证 Participant 会话句柄的 refresh 刷新机制
#[test]
fn participant_refresh() -> Void {
  info!("验证 Participant 会话句柄的 refresh 刷新机制");

  let epoch = Arc::new(LightEpoch::new(8));
  let p = epoch.register()?;

  let drained = Arc::new(AtomicBool::new(false));
  let drained_clone = Arc::clone(&drained);

  let guard = p.enter();
  assert_eq!(guard.protected_epoch(), 1);

  // 推进纪元并挂接延迟动作。对照 C# BumpCurrentEpoch(Action) 尾部 ProtectAndDrain
  // （"may execute the action we just added"）：注册路径统一刷新本线程公布纪元并
  // 收割就绪动作，Participant 持有的旧纪元 1 在挂接时即被刷新至 2，动作随之触发。
  // 修复前 help_drain 仅识别 TLS 机制，Participant 长期守卫自钉回收进度，
  // 耗尽 drain_list 后 append 页翻转注册路径永久自旋（活锁）
  epoch.bump_current_epoch_action(move || {
    drained_clone.store(true, Ordering::SeqCst);
  });

  // 公布纪元已刷新至最新，旧纪元 1 安全，延迟动作被成功触发
  assert_eq!(p.protected_epoch(), 2);
  assert!(drained.load(Ordering::Acquire));

  drop(guard);
  OK
}

/// 回归测试：Participant 长期守卫下连续注册超过 drain_list 容量（16 槽）的延迟动作
///
/// 修复前：help_drain 看不见 Participant 保护 → 公布纪元自钉 → safe 永不推进 →
/// 动作注册路径在列表耗尽后永久自旋；修复后每次注册即刷新即收割，全程无阻塞
#[test]
fn participant_long_guard_drain_list_exhaustion() -> Void {
  info!("验证 Participant 长期守卫下 drain_list 满载注册不活锁");

  let epoch = Arc::new(LightEpoch::new(8));
  let p = epoch.register()?;

  let fired = Arc::new(AtomicUsize::new(0));
  // 2 倍容量：修复前必活锁
  const ACTIONS: usize = 2 * wepoch::DRAIN_LIST_SIZE;

  let _guard = p.enter();
  for _ in 0..ACTIONS {
    let f = Arc::clone(&fired);
    epoch.bump_current_epoch_action(move || {
      f.fetch_add(1, Ordering::SeqCst);
    });
  }

  assert_eq!(fired.load(Ordering::Acquire), ACTIONS);
  OK
}

/// 回归测试：同一线程 TLS 与 Participant 双机制并存时注册延迟动作不活锁
///
/// 修复前 help_drain 仅刷新 TLS 优先命中的单条保护条目，Participant 槽公布的旧纪元
/// 自钉 safe_to_reclaim_epoch，drain_list 耗尽后 bump_current_epoch_action 注册路径
/// 永久自旋（活锁）；修复后 help_drain 全量刷新本线程以任一机制持有的保护条目
#[test]
fn mixed_tls_participant_drain_no_livelock() -> Void {
  info!("验证同线程 TLS+Participant 双机制并存时注册延迟动作不活锁");

  let epoch = Arc::new(LightEpoch::new(8));
  let p = epoch.register()?;
  // Participant 槽公布纪元 1（长期会话守卫）
  let _pg = p.enter();

  // 2 倍 drain_list 容量：修复前第二轮注册必活锁
  const ACTIONS: usize = 2 * wepoch::DRAIN_LIST_SIZE;
  let fired = Arc::new(AtomicUsize::new(0));

  {
    // TLS 槽与 Participant 槽并存，两条公布纪元均为 1
    let _tls = epoch.protected_scope();
    for _ in 0..ACTIONS {
      let f = Arc::clone(&fired);
      epoch.bump_current_epoch_action(move || {
        f.fetch_add(1, Ordering::SeqCst);
      });
    }
  }

  assert_eq!(fired.load(Ordering::Acquire), ACTIONS);
  OK
}

/// 验证线程退出未释放槽位时的 TLS 自动兜底回收 (Drop / TLS cleanup)
#[test]
fn thread_exit_tls_cleanup() -> Void {
  info!("验证线程退出未释放槽位时的 TLS 自动兜底回收");

  let epoch = Arc::new(LightEpoch::new(8));
  let slot_holder = Arc::new(AtomicUsize::new(0));

  let ep = Arc::clone(&epoch);
  let sh = Arc::clone(&slot_holder);

  let handle = thread::spawn(move || {
    ep.resume();
    let slot = ep.test_hook_this_thread_entry();
    assert_ne!(slot, 0);
    sh.store(slot, Ordering::SeqCst);
    // 故意不调用 ep.suspend()，模拟线程异常退出或遗漏 suspend
  });

  handle.join().unwrap();

  let slot = slot_holder.load(Ordering::SeqCst);
  assert_ne!(slot, 0);

  // 线程退出后，TLS 的 LocalEpochEntries::drop 应已兜底释放该槽位
  assert_eq!(
    epoch.test_hook_announced_epoch_at(slot),
    0,
    "线程析构后，遗留槽位的 announced_epoch 必须已被 TLS drop 清零"
  );
  assert_eq!(
    epoch.test_hook_thread_id_at(slot),
    0,
    "线程析构后，遗留槽位的 thread_id 必须已被 TLS drop 清零"
  );

  // 全局推进纪元并验证 safe_to_reclaim 可以顺利推进至最新纪元，绝不因死线程卡死
  epoch.bump_epoch();
  assert_eq!(epoch.current_epoch(), 2);
  assert!(
    epoch.is_safe_to_reclaim(1),
    "遗留死线程已被清理，纪元 1 必须判定为可安全回收"
  );

  OK
}

/// 验证长寿线程在大量瞬态 LightEpoch 下 TLS Weak 清理与 Arc 零泄漏
#[test]
fn transient_epoch_instance_lifecycle_and_tls_weak_cleanup() -> Void {
  info!("验证长寿线程在大量瞬态 LightEpoch 下 TLS Weak 清理与 Arc 零泄漏");

  const ITERATIONS: usize = 32;
  const CAPACITY: usize = 4;

  // 在同一线程上连续创建、使用并销毁 32 个不同的 LightEpoch 实例
  for _ in 0..ITERATIONS {
    let epoch = LightEpoch::new(CAPACITY);
    let weak = Arc::downgrade(&epoch.entries);
    assert_eq!(Arc::strong_count(&epoch.entries), 1);

    // 进入保护区并退出
    epoch.resume();
    assert!(epoch.this_instance_protected());
    epoch.suspend();
    assert!(!epoch.this_instance_protected());

    // 丢弃 epoch 实例
    drop(epoch);

    // 验证 Arc 彻底释放，强引用计数归零，TLS 仅持有 Weak，绝对无循环引用或内存泄露
    assert_eq!(
      weak.strong_count(),
      0,
      "LightEpoch 销毁后 entries 强引用必须为 0"
    );
  }

  OK
}

/// 验证 wram 与 wepoch 的 current_thread_id 严格一致且多线程唯一非零
#[test]
fn unified_current_thread_id_consistency() -> Void {
  info!("验证 wram 与 wepoch 线程标识统一原语");

  let wepoch_tid = wepoch::current_thread_id();
  let wram_tid = wram::current_thread_id();
  assert_eq!(
    wepoch_tid, wram_tid,
    "同一线程在 wepoch 与 wram 中必须获得完全相同的 thread_id"
  );
  assert!(wepoch_tid > 0, "thread_id 必须大于 0");

  let mut handles = Vec::new();
  for _ in 0..8 {
    handles.push(thread::spawn(|| {
      let t1 = wepoch::current_thread_id();
      let t2 = wram::current_thread_id();
      assert_eq!(t1, t2);
      assert!(t1 > 0);
      t1
    }));
  }

  let mut ids = vec![wepoch_tid];
  for h in handles {
    ids.push(h.join().unwrap());
  }

  let total = ids.len();
  ids.sort_unstable();
  ids.dedup();
  assert_eq!(
    ids.len(),
    total,
    "所有并发线程获取的 thread_id 必须全局唯一"
  );

  OK
}
