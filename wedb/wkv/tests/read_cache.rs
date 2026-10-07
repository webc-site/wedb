#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 读缓存内存布局、驱逐等待屏障与换页纪元协议集成测试（自 src/read_cache/mod.rs 外迁）
//!
//! 覆盖：
//! 1. gate_is_noop_for_non_rc_in_window_and_disabled: 门控快路径；
//! 2. page_turn_publishes_closed_until: 环形回绕驱逐单调发布 ClosedUntilAddress，被驱逐页旧地址门控仅一轮 refresh；
//! 3. turn_waits_for_inflight_registration: 撕裂窗闭环（滞留门控）；
//! 4. reader_spins_until_cleanse_publishes_page_end: 读线程自旋至旧页页末清洗完毕（ClosedUntil 未发布期真实多轮续转 + 发布后即退，等待协议两相全覆盖）；
//! 5. double_registered_close_actions_recheck_under_turn_lock: 纪元双注册锁内复核短路；
//! 6. closed_slot_rejects_stale_registration: 槽位被置 CLOSED 后拒绝滞留注册；
//! 7. cleansed_page_restores_index_without_pad: 清洗恢复索引指向主日志，零松弛区无断链。

use std::{
  sync::{
    Arc,
    atomic::{
      AtomicBool, AtomicU32,
      Ordering::{self, Acquire, Relaxed, Release},
    },
    mpsc,
  },
  thread::{scope, sleep, spawn, yield_now},
  time::{Duration, Instant},
};

use wbase::addr::{is_read_cache, to_absolute, with_read_cache};
use wepoch::LightEpoch;
use windex::{HashBucketEntry, HashIndex};
use wkv::{INFLIGHT_CLOSED, RcVisit, ReadCache, Result};
use wrecord::{HEADER_SIZE, RecordHeader, record_size};

/// 已从 windex 生产导出面收敛掉的 key 版 `insert`：测试灌数按唯一免查重追加
/// 写入口 [`HashIndex::insert_to_bucket`] 等价复现（桶下标与 Tag 在此显式换算）
trait HashIndexTestOps {
  fn insert(&self, key: &[u8], address: u64) -> windex::Result<()>;
}

impl HashIndexTestOps for HashIndex {
  #[inline]
  fn insert(&self, key: &[u8], address: u64) -> windex::Result<()> {
    let hash = HashIndex::hash_key(key);
    let tag = HashBucketEntry::tag_from_hash(hash);
    self.insert_to_bucket(self.bucket_index_for_hash(hash), tag, address)
  }
}

/// 门控：非 RC 地址 / 窗口内 / 未启用一律不等待（对标 ReadCacheNeedToWaitForEviction 快路径）
#[test]
fn gate_is_noop_for_non_rc_in_window_and_disabled() -> Result<()> {
  let rc = ReadCache::new(4096, 4, true, Arc::new(LightEpoch::new(8)))?;
  let index = Arc::new(HashIndex::new(16)?);
  // append 内部 CAS 挂载（对标 hei.TryCAS）：先挂主日志地址条目再追加
  index.insert(b"k", 12345)?;
  let rc_addr = rc.append(b"k", b"v", &index, 0).expect("append 应成功");

  assert!(!rc.need_to_wait_for_eviction(123, || ()));
  assert!(!rc.need_to_wait_for_eviction(rc_addr, || ()));

  let off = ReadCache::new(4096, 4, false, Arc::new(LightEpoch::new(8)))?;
  assert!(!off.need_to_wait_for_eviction(with_read_cache(0), || ()));
  Ok(())
}

/// 真实环形回绕驱逐：cleanse_page 完成后 ClosedUntilAddress 单调发布到被驱逐页末尾，
/// 被驱逐页旧地址门控触发且仅一轮 refresh 即返回（回链头重探）
#[test]
fn page_turn_publishes_closed_until() -> Result<()> {
  let rc = Arc::new(ReadCache::new(4096, 2, true, Arc::new(LightEpoch::new(8)))?);
  let index = Arc::new(HashIndex::new(16)?);

  let mut first_rc_addr = None;
  for i in 0..4096u32 {
    let key = format!("k{i}");
    index.insert(key.as_bytes(), 12345)?;
    if let Some(addr) = rc.append(key.as_bytes(), b"v", &index, 0)
      && first_rc_addr.is_none()
    {
      first_rc_addr = Some(addr);
    }
    // 回绕换页武装后由安全点泵入纪元延迟关闭（本线程无在途借用，注册尾随
    // help_drain 即同步收割执行）
    rc.pump_close_barrier(None, &index, None);
    if rc.closed_until_address() > 0 {
      break;
    }
  }
  assert!(
    rc.closed_until_address() > 0,
    "环形回绕应发布 ClosedUntilAddress"
  );

  let evicted = first_rc_addr.expect("首条 RC 记录应存在");
  let rounds = AtomicU32::new(0);
  assert!(rc.need_to_wait_for_eviction(evicted, || {
    rounds.fetch_add(1, Relaxed);
  }));
  assert_eq!(rounds.load(Relaxed), 1);
  Ok(())
}

/// 撕裂窗闭环（滞留门）：被复用槽存在在途注册（模拟停滞横跨整环的滞留编码者）
/// 时，换页侧必须阻塞至其退订后才清洗 + 换装 + 推进 tail
#[test]
fn turn_waits_for_inflight_registration() -> Result<()> {
  let rc = Arc::new(ReadCache::new(512, 2, true, Arc::new(LightEpoch::new(8)))?);
  let index = Arc::new(HashIndex::new(16)?);

  // 填满页 0（512B 页，单记录 24B，页界分支将翻向页 1）
  let mut i = 0u32;
  while rc.tail_address() < 512 {
    let key = format!("a{i}");
    index.insert(key.as_bytes(), 12345)?;
    assert!(rc.append(key.as_bytes(), b"v", &index, 0).is_some());
    i += 1;
  }

  // 模拟滞留编码者：已注册在途（页 2 回绕复用页 0 的槽 0）但尚未完成编码
  rc.page_inflight(0).store(1, Release);

  scope(|s| {
    s.spawn(|| {
      // 持续 append 填满页 1 并触发翻页 2（回绕驱逐页 0）；换页方武装后经
      // 安全点泵入纪元延迟关闭——本线程即收割执行线程，两阶段关闭的写者
      // 排空自旋停在本动作内，滞留注册存在时换页阻塞
      let mut j = 0u32;
      while rc.closed_until_address() == 0 {
        let key = format!("b{j}");
        let _ = index.insert(key.as_bytes(), 12345);
        let _ = rc.append(key.as_bytes(), b"v", &index, 0);
        rc.pump_close_barrier(None, &index, None);
        j += 1;
      }
    });

    // 滞留注册未退订：换页不得推进（ClosedUntilAddress 未发布、tail 不越过页 1）
    sleep(Duration::from_millis(50));
    assert_eq!(rc.closed_until_address(), 0, "在途注册存在时换页必须阻塞");
    assert!(rc.tail_address() < 1024, "tail 不得越过未清洗的页边界");

    // 滞留编码者完成（退订）：换页放行，清洗 + 换装 + ClosedUntilAddress 发布
    rc.page_inflight(0).fetch_sub(1, Ordering::Release);
  });
  // 首个被驱逐页为页 0（next_page_id=2, num_pages=2），ClosedUntilAddress
  // 精确推进至其页末边界 (2-2+1)*512 = 512，绝不超前 head（旧断言 >= 1024
  // 正是 closed_until 超前整环容量缺陷的假阳性固化）
  assert_eq!(rc.closed_until_address(), 512);
  assert!(rc.closed_until_address() <= rc.head_address());
  Ok(())
}

/// 驱逐等待屏障闭环（真实自旋）：head 先于置 CLOSED 推进，滞留注册门控期间
/// 构成 head 已越过旧页页末而 ClosedUntilAddress 尚未发布的窗口——被驱逐页内
/// 地址的 need_to_wait_for_eviction 必须真实自旋阻塞（对标
/// ReadCache.cs:ReadCacheNeedToWaitForEviction →
/// EpochOperations.cs:SpinWaitUntilRecordIsClosed，解除条件恒为
/// `abs < ClosedUntilAddress`），仅在换页方完成清洗并发布页界后方可解除，
/// 且全程 closed <= head 单调不变量成立
#[test]
fn reader_spins_until_cleanse_publishes_page_end() -> Result<()> {
  let rc = Arc::new(ReadCache::new(512, 2, true, Arc::new(LightEpoch::new(8)))?);
  let index = Arc::new(HashIndex::new(1024)?);

  // 填满页 0（首轮回绕驱逐页 0 后页界 = (2-2+1)*512 = 512），取页内第二条
  // 记录地址为探针（首条 abs=0 恒不小于 head，不可作等待判据）
  let mut page0 = Vec::new();
  let mut i = 0u32;
  while rc.tail_address() < 512 {
    let key = format!("e{i}");
    index.insert(key.as_bytes(), 12345)?;
    page0.push(
      rc.append(key.as_bytes(), b"v", &index, 0)
        .expect("append 应成功"),
    );
    i += 1;
  }
  let probe = page0[1];
  assert!(to_absolute(probe) > 0);

  // 滞留编码者门控：页 2 回绕复用槽 0 的两阶段关闭排空被阻塞，
  // 换页方停在「head 已推进、closed 未发布」的屏障窗口内
  rc.page_inflight(0).store(1, Release);

  let rounds = AtomicU32::new(0);
  let reader_done = AtomicBool::new(false);

  // 先 collect 全部句柄，再统一 join（避免串行 join 饿死协作线程）
  scope(|s| {
    let mut handles = Vec::new();

    let turn = s.spawn(|| {
      // 换页方：灌满页 1 并触发跳页 2（回绕驱逐页 0），直至 ClosedUntil 发布；
      // 武装后经安全点泵入纪元延迟关闭，本线程即收割执行线程，两阶段关闭
      // 的写者排空自旋停在该动作内（滞留门控未解除前 closed 不发布）
      let mut j = 0u32;
      while rc.closed_until_address() == 0 {
        let key = format!("f{j}");
        let _ = index.insert(key.as_bytes(), 12345);
        let _ = rc.append(key.as_bytes(), b"v", &index, 0);
        rc.pump_close_barrier(None, &index, None);
        j += 1;
      }
    });
    handles.push(turn);

    // 屏障窗口确证：head 已推进至被驱逐页页末 512，closed 仍被门控压在 0
    let deadline = Instant::now() + Duration::from_secs(5);
    while rc.head_address() < 512 {
      assert!(Instant::now() < deadline, "换页方应推进 head 至旧页页末");
      yield_now();
    }
    assert_eq!(
      rc.closed_until_address(),
      0,
      "门控期间 ClosedUntil 不得发布"
    );
    assert!(
      rc.closed_until_address() <= rc.head_address(),
      "任何时刻 closed <= head 不变量必须成立"
    );

    let reader = s.spawn(|| {
      let waited = rc.need_to_wait_for_eviction(probe, || {
        rounds.fetch_add(1, Relaxed);
      });
      assert!(waited, "探针地址已滑出窗口，必须走驱逐等待协议");
      reader_done.store(true, Release);
    });
    handles.push(reader);

    // 门控仍闭死：读线程必须仍在真实自旋（旧缺陷下 closed 超前虚标，
    // 此处会瞬间穿透返回）
    sleep(Duration::from_millis(50));
    assert!(
      !reader_done.load(Acquire),
      "cleanse 未发布时读线程不得解除阻塞"
    );
    assert!(rounds.load(Relaxed) > 1, "等待协议必须真实多轮续转");

    // 放行换页方：排空 → 清洗 → 发布页界，读线程方可解除自旋
    rc.page_inflight(0).fetch_sub(1, Release);

    for h in handles {
      h.join().unwrap();
    }
  });

  assert!(reader_done.load(Acquire));
  assert_eq!(
    rc.closed_until_address(),
    512,
    "closed 精确停在被清洗页页末"
  );
  assert!(rc.closed_until_address() <= rc.head_address());
  Ok(())
}

/// 纪元双注册锁内复核反证锁测：close_armed 边沿在冻结窗内被每次页满重试
/// 重武装（append.rs 武装臂），纪元 drain 为槽级 CAS 认领无全局收割锁，
/// 同边界两个延迟动作可被两线程并发收割——复用本族
/// `turn_waits_for_inflight_registration` 的 page_inflight 门控与泵入注册
/// 手法造确定性交错：主线程持 turn_lock 停车两收割线程（safe_head 发布点
/// 恒在该临界区内 ⇒ 停车窗内两动作的锁外幂等检查必以陈旧读数放行）；放锁
/// 后先入者持锁走至滞留排空自旋（CLOSED 位现形确证已标定 safe_head），放行
/// 门控使其全套落定；后入者唤醒后经锁内复核短路无害。回退修复即后入者以
/// 已推进的 tail 重算几何重跑整段换页关闭：closed/safe_head/head 越界一页
/// 至 1024、tail 跳页发布至 1536、存活在窗探针页被越界清洗回主日志——
/// 本组断言全数转红。
#[test]
fn double_registered_close_actions_recheck_under_turn_lock() -> Result<()> {
  // 几何（与 rc_epoch_drain 族同口径）：单记录恒 24B（16B 头 + 5B 键 + 1B 值
  // 8B 对齐），512B 页每页恰 21 条，页尾余量 8 < 24 恒触发换页分支
  assert_eq!(record_size(5, 1), 24);
  let epoch = Arc::new(LightEpoch::new(8));
  let rc = Arc::new(ReadCache::new(512, 2, true, Arc::clone(&epoch))?);
  let index = Arc::new(HashIndex::new(1024)?);

  // 灌满页 0 与页 1（正向首轮内联换装，无驱逐），页 1 首条为存活在窗探针
  for i in 0..42u64 {
    let k = format!("a{i:04}");
    index.insert(k.as_bytes(), 12345)?;
    assert!(rc.append(k.as_bytes(), b"v", &index, 0).is_some());
  }
  let probe = b"a0021";

  // 真实武装边界：回绕换页武装拍作废本条晋升，head 推至页 0 页末 512，
  // tail 冻结于页 1 页末余量处（关闭序列放行前不换页）
  index.insert(b"c0000", 12345)?;
  assert!(
    rc.append(b"c0000", b"v", &index, 0).is_none(),
    "回绕换页武装拍必须作废本条晋升"
  );
  assert_eq!(rc.head_address(), 512);
  assert_eq!(rc.tail_address(), 1016, "武装后 tail 冻结等待关闭序列发布");
  assert_eq!(rc.closed_until_address(), 0, "未收割落定前不得有关闭发布");

  // 滞留编码者门控：把首个进入关闭的动作钉死在 turn_lock 内的目标槽
  // 写者排空自旋点（safe_head 标定之后、清洗之前）
  rc.page_inflight(0).store(1, Release);

  let (pin_tx, pin_rx) = mpsc::channel::<()>();
  let (arm_tx, arm_rx) = mpsc::channel::<()>();
  let (reg2_tx, reg2_rx) = mpsc::channel::<()>();
  let (go1_tx, go1_rx) = mpsc::channel::<()>();
  let (drain1_tx, drain1_rx) = mpsc::channel::<()>();
  let (go2_tx, go2_rx) = mpsc::channel::<()>();
  let (ready2_tx, ready2_rx) = mpsc::channel::<()>();

  // T1：钉旧纪元（守卫在场时注册尾随 help_drain 不收割，同纪元滞留测试手法）
  // → 冻结窗内重试 append 重武装 + 第二次泵入 → 释守卫入收割认领其一
  let rc1 = Arc::clone(&rc);
  let index1 = Arc::clone(&index);
  let epoch1 = Arc::clone(&epoch);
  let t1 = spawn(move || {
    let participant = epoch1.register().expect("注册纪元参与者");
    let guard = participant.enter();
    epoch1.bump_current_epoch();
    epoch1.bump_current_epoch();
    pin_tx.send(()).unwrap();
    arm_rx.recv().unwrap();
    // close_armed 每次页满臂无条件重武装（双注册入口）：第二次泵入再消费
    // 边沿，注册同边界第二个延迟动作
    index1.insert(b"c0001", 12345).unwrap();
    assert!(rc1.append(b"c0001", b"v", &index1, 0).is_none());
    rc1.pump_close_barrier(None, &index1, None);
    reg2_tx.send(()).unwrap();
    go1_rx.recv().unwrap();
    // 先示警再释守卫：守卫退场尾随收割也可能就地认领动作并停驻于 turn_lock
    drain1_tx.send(()).unwrap();
    drop(guard);
    epoch1.drain();
  });

  // 主线程：第一次泵入消费边沿注册动作 A（守卫钉旧纪元致滞留队列）
  pin_rx.recv().unwrap();
  rc.pump_close_barrier(None, &index, None);
  assert!(
    epoch.has_pending_drain(),
    "守卫钉旧纪元时首次泵入的动作必须滞留"
  );
  arm_tx.send(()).unwrap();
  reg2_rx.recv().unwrap();
  assert!(
    epoch.has_pending_drain(),
    "同边界第二次泵入的双注册动作必须滞留"
  );

  // 主线程持 turn_lock 停车：safe_head 发布恒在该临界区内 ⇒ 停车窗内任何
  // 锁外幂等检查必读到陈旧 0，两动作均以陈旧通过（危害前态确定性构造）
  let turn_guard = rc.turn_lock().lock();
  go1_tx.send(()).unwrap();
  drain1_rx.recv().unwrap();
  // T2：第二收割线程认领另一动作（槽级 CAS 认领，各线程首个被认领动作
  // 停驻于 turn_lock ⇒ 两线程各认领其一，无重认领竞态）
  let epoch2 = Arc::clone(&epoch);
  let t2 = spawn(move || {
    go2_rx.recv().unwrap();
    ready2_tx.send(()).unwrap();
    epoch2.drain();
  });
  go2_tx.send(()).unwrap();
  ready2_rx.recv().unwrap();
  // 停车窗确证（本族门控期同款口径）：两动作已入锁等待，水位分毫未动
  sleep(Duration::from_millis(50));
  assert_eq!(rc.closed_until_address(), 0, "停车窗内关闭序列不得分毫推进");
  assert_eq!(rc.safe_head_address(), 0, "停车窗内 safe_head 不得标定");
  drop(turn_guard);

  // 先入者已入锁：safe_head 标定先于排空自旋，CLOSED 位置起即锁内越过
  // 发布点（有界观测，超时即场景失败直红，不放行防悬挂）
  let deadline = Instant::now() + Duration::from_secs(5);
  while rc.page_inflight(0).load(Acquire) & INFLIGHT_CLOSED == 0 {
    assert!(
      Instant::now() < deadline,
      "放锁后先入动作应进入排空自旋（CLOSED 位现形）"
    );
    yield_now();
  }
  assert_eq!(
    rc.safe_head_address(),
    512,
    "先入动作已在锁内标定 SafeHead 至本边界"
  );
  assert_eq!(rc.closed_until_address(), 0, "门控期内 closed 不得发布");
  // 放行滞留门控：先入者完成清洗/换装/水位发布并放锁，后入者此刻才被唤醒
  rc.page_inflight(0).fetch_sub(1, Release);

  t1.join().expect("第一收割线程零 panic");
  t2.join().expect("第二收割线程零 panic");

  // 锁内复核短路判据：后入动作零副作用——水位与几何零越界（回退修复即
  // 重跑关闭：closed/safe_head/head 跳至 1024、tail 跳页发布至 1536）
  assert_eq!(
    rc.closed_until_address(),
    512,
    "closed 必须精确停在首个被驱逐页页末，不得越界一页"
  );
  assert_eq!(rc.safe_head_address(), 512, "safe_head 不得越界一页发布");
  assert_eq!(rc.head_address(), 512, "head 不得越界一页推进");
  assert_eq!(
    rc.tail_address(),
    1024,
    "tail 不得跳页发布至先入者新页起点之后"
  );
  assert!(
    rc.closed_until_address() <= rc.head_address(),
    "closed <= head 单调不变量必须成立"
  );
  assert_eq!(
    rc.page_inflight(0).load(Acquire),
    0,
    "换页完成后目标槽状态字重置"
  );
  assert!(
    !epoch.has_pending_drain(),
    "双注册两动作必须都已收割执行完毕"
  );

  // 存活在窗页零伤害：探针键索引仍指 RC 记录且缓存命中（回退修复即被
  // 越界清洗恢复主日志地址、物理槽清零整页丢弃）
  let slot = index.find_tag(probe).expect("探针键必须可寻址");
  assert!(
    is_read_cache(slot),
    "存活在窗页哈希索引不得被越界清洗恢复至主日志地址"
  );
  let hit = rc.with_record(slot, |k, _v| (k == probe).then_some(()));
  assert!(
    matches!(hit, RcVisit::Found(())),
    "存活在窗页缓存必须仍可命中，实际 {hit:?}"
  );
  Ok(())
}

/// 撕裂窗闭环（关闭拒绝）：槽位被置 CLOSED 后，旧快照注册必被拒绝退订重试，
/// 状态字重置后恢复注册并成功编码
#[test]
fn closed_slot_rejects_stale_registration() -> Result<()> {
  let rc = ReadCache::new(512, 2, true, Arc::new(LightEpoch::new(8)))?;
  let index = Arc::new(HashIndex::new(16)?);

  // 填满页 0 并翻向页 1，使 tail 落在页 1 开头
  let mut i = 0u32;
  while rc.tail_address() < 512 {
    let key = format!("c{i}");
    index.insert(key.as_bytes(), 12345)?;
    assert!(rc.append(key.as_bytes(), b"v", &index, 0).is_some());
    i += 1;
  }
  index.insert(b"first-on-page1", 12345)?;
  assert!(rc.append(b"first-on-page1", b"v", &index, 0).is_some());

  // 模拟换页侧两阶段关闭的第一阶段：关闭页 1 槽位
  rc.page_inflight(1)
    .fetch_or(INFLIGHT_CLOSED, Ordering::AcqRel);

  scope(|s| {
    // 40ms 后重置（模拟换页完成）
    s.spawn(|| {
      sleep(Duration::from_millis(40));
      rc.page_inflight(1).store(0, Ordering::Release);
    });

    // 期间 append 不断重试，重置后成功且地址落在页 1
    let deadline = Instant::now() + Duration::from_secs(5);
    let _ = index.insert(b"after-close", 12345);
    let mut addr = None;
    while addr.is_none() {
      assert!(Instant::now() < deadline, "CLOSED 重置后 append 应成功");
      addr = rc.append(b"after-close", b"v", &index, 0);
    }
    let abs = to_absolute(addr.unwrap());
    assert!((512..1024).contains(&abs), "重试成功后应落在页 1");
  });
  Ok(())
}

/// 撕裂窗闭环（清洗完整性）：删除页尾 pad 头后，回绕换页的被驱逐页内容恒为
/// 完整记录序列 + 零松弛区（无 pad 头），cleanse 全量恢复索引指向主日志，
/// 无悬垂 RC 指向、无断链
#[test]
fn cleansed_page_restores_index_without_pad() -> Result<()> {
  let rc = Arc::new(ReadCache::new(512, 2, true, Arc::new(LightEpoch::new(8)))?);
  let index = Arc::new(HashIndex::new(16)?);

  let mut mounted = Vec::new();
  for i in 0..512u32 {
    let key = format!("d{i:03}");
    // 先挂主日志地址 42，append 内部 CAS 挂载 RC 地址（对标 hei.TryCAS）；
    // 清洗后应恢复主日志地址 42
    index.insert(key.as_bytes(), 42)?;
    if let Some(rc_addr) = rc.append(key.as_bytes(), b"v", &index, 0) {
      mounted.push((key.into_bytes(), rc_addr));
    }
    // 回绕换页武装后泵入纪元延迟关闭（本线程无借用，注册即同步收割执行）
    rc.pump_close_barrier(None, &index, None);
    if rc.closed_until_address() > 0 {
      break;
    }
  }
  assert!(rc.closed_until_address() > 0, "回绕应发生");

  // 被驱逐页（页 0）物理内容：无 pad 头，记录完整可解析且前驱为主日志地址
  let page0 = rc.read_page(0);
  let mut offset = 0;
  while offset + HEADER_SIZE <= rc.page_size() {
    let Some(header) = RecordHeader::decode_opt(&page0[offset..]) else {
      break;
    };
    assert!(
      !header.is_pad(),
      "页尾只允许完整记录或零松弛区，不得有 pad 头"
    );
    if header.is_null() {
      break; // 零松弛区起点，其后必全零
    }
    let Some(rec_size) = header.checked_physical_size() else {
      break;
    };
    assert_eq!(header.address(), 42, "记录前驱必须是主日志地址");
    let key = &page0[offset + HEADER_SIZE..offset + HEADER_SIZE + header.key_len() as usize];
    assert!(key.starts_with(b"d"), "键必须完整无撕裂");
    offset += rec_size;
  }

  // 全量挂载键：索引要么仍指向窗口内完整 RC 记录，要么已被清洗恢复主日志地址
  for (key, rc_addr) in &mounted {
    let slot = index.find_tag(key).expect("已挂载键必须可寻址");
    if is_read_cache(slot) {
      assert_eq!(slot, *rc_addr, "未清洗键的 RC 指向不得漂移");
      let parsed = match rc.with_record(slot, |k, _| Some(k.to_vec())) {
        RcVisit::Found(v) => Some(v),
        _ => None,
      };
      assert_eq!(
        parsed.as_deref(),
        Some(key.as_slice()),
        "窗口内记录必须完整可解析"
      );
    } else {
      assert_eq!(slot, 42, "清洗必须将索引恢复至主日志地址，不得断链");
    }
  }
  Ok(())
}
