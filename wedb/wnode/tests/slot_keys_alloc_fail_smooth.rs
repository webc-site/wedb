//! CLUSTER 槽位枚举族应答物化分配失败的单会话平滑降级回归（票 zcode-r125c-clucount1）
//!
//! 票面命题：同文件 [`reserve_fail`] 单机制（scan/keys 侧已消费，锁见
//! `scan_keys_alloc_fail_smooth.rs`）在槽位枚举族五处逐键物化点被裸
//! `to_vec`+`push` 绕开——count_keys_in_slot 冷区 Degrade 候选入列、
//! get_keys_in_slot_with 逐候选键拷贝、get_keys_in_slot(+excluding) collect
//! 臂、delete_slot_keys 删除枚举、db_size 冷区候选入列。分配触顶走
//! `handle_alloc_error` → abort 全进程，而 C# 契约是 `OutOfMemoryException`
//! 被会话主循环 `catch (Exception)` 兜住的单会话 Dispose 收场
//! （RespServerSession.cs:566）。本锁验证补齐后全族与 SCAN/KEYS 同轨：
//! 失败携 [`reserve_fail`] 自述文案沿 whlog/wkv 既有错误通道上抛（消费面
//! 折叠 `RESP_ERR_SLOW_PATH_STORAGE` 错误帧，RESP 线面锁见
//! wedb/tests/cluster_slot_keys_alloc_fail_smooth.rs），进程存活、其余路径照常。
//!
//! 故障注入为「大额分配失败」定向分配器（仅本测试二进制），与
//! scan_keys_alloc_fail_smooth.rs 同一阈值模式：开关开启后 >= THRESHOLD 的
//! 分配返空（模拟堆触顶），小额照常。容器面触发真实性：`Vec<Vec<u8>>`
//! 倍增扩容跳档至 32768×24B=786KB 时越过 512KB 注入阈值——灌 20000 键即
//! 踩物化失败臂。阈值上抬至 512KB 的原因：冷区扫描的 256KB 日志页读分配
//! （引擎面既有平滑轨）必须照常放行以证因唯一点落本臂；而 ≥ 阈值的巨键名
//! 记录会先撞上 whlog 记录重组的非平滑内部分配（实测 abort），单键巨值面
//! 在本注入器形态下不可安全触达，逐候选 `try_key_vec` 与其 collect 消费面
//! 同函数同通道，由 collect 臂锁承接。nextest 逐用例独立进程，`InjectGuard`
//! 兜底同进程串行残留。

use std::{
  alloc::{GlobalAlloc, Layout, System},
  ptr,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread::yield_now,
};

use wbase::{convert::TICKS_PER_SECOND, hash_slot::slot_of, time::now_ticks};
use wnode::storage::session::{
  common::ttl_sync::put_ttl_sync,
  storage_session::{StorageSession, version_map_watch_hook},
};
use wtest_base::open_test_store;
use wtxn::{DEFAULT_VERSION_MAP_SIZE, WatchVersionMap};

/// 大额分配阈值：严格高于 16MB 测试预算推导的 256KB 日志页容量，冷区扫描
/// 的页读分配照常放行（引擎面），唯物化容器倍增扩容（24B×2C）越阈踩注入
const THRESHOLD: usize = 512 * 1024;

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

static SERIAL_LOCK: AtomicBool = AtomicBool::new(false);

struct SerialGuard;

impl Drop for SerialGuard {
  fn drop(&mut self) {
    SERIAL_LOCK.store(false, Ordering::Release);
  }
}

fn lock_serial() -> SerialGuard {
  while SERIAL_LOCK
    .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
    .is_err()
  {
    yield_now();
  }
  SerialGuard
}

/// 注入开关 RAII 兜底：同进程串行执行（非 nextest）时防护残留开关污染后续用例
struct InjectGuard;

impl Drop for InjectGuard {
  fn drop(&mut self) {
    INJECT.store(false, Ordering::Relaxed);
  }
}

/// 会话库级槽位（库级定槽：默认 (0,0) 库，键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);

/// 灌键数：快照容器按每键 24B（`Vec`）记账，倍增扩容跳档 8192→16384（393KB，
/// 低于阈值放行）后，16384 满容的下一次 `try_reserve(1)` 请求 32768×24B=786KB
/// 越过 512KB 注入阈值——20000 键保证物化臂必踩注入
const KEY_COUNT: usize = 20000;

fn storage_session<'s, D: wdev::Device>(
  session: &'s wkv::StoreSession<D>,
) -> StorageSession<'s, D> {
  let version_map = Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE));
  let _ = session
    .store
    .set_watch_hook(version_map_watch_hook(Arc::clone(&version_map)));
  StorageSession::new(session.enter_batch())
}

/// 灌 `n` 个热区字符串键（注入关闭态装配）并返回存储会话
async fn seed(ss: &StorageSession<'_, wdev::SegmentedDevice>, n: usize) {
  for i in 0..n {
    let key = format!("k:{i:07}");
    ss.upsert_string(key.as_bytes(), b"v").await.unwrap();
  }
}

/// 给全部 `k:%07` 键挂未来 TTL（存活键，冷区候选语义源）
async fn seed_future_ttl(session: &wkv::StoreSession<wdev::SegmentedDevice>, n: usize) {
  let batch = session.enter_batch();
  for i in 0..n {
    let key = format!("k:{i:07}");
    put_ttl_sync(
      &batch,
      key.as_bytes(),
      now_ticks() + 3600 * TICKS_PER_SECOND,
    )
    .unwrap();
  }
}

/// 平滑收口错误判据：错误链须携带 [`reserve_fail`] 的自述文案
/// （「扫描应答物化内存预留失败」），证因唯一点落本臂物化收口——
/// 既排除引擎侧其他分配失败混入，也锁定沿 whlog/wkv 既有错误通道上抛
fn is_reserve_fail(err: &wkv::Error) -> bool {
  err.to_string().contains("扫描应答物化内存预留失败")
}

/// delete_slot_keys 删除枚举臂：KEY_COUNT 键倍增扩容注入下平滑上抛，进程存活，
/// 关注入后续删无碍（C# 单会话 Dispose 对位）
#[compio::test]
async fn delete_slot_keys_alloc_fail_smooth() -> aok::Void {
  let _serial = lock_serial();
  let (_dir, store) = open_test_store("slot_del_oom.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);
  seed(&ss, KEY_COUNT).await;

  let _guard = InjectGuard;
  INJECT.store(true, Ordering::Relaxed);
  let res = ss.delete_slot_keys(&[SLOT0]).await;
  // 断言前先关注入：失败态 panic 格式化不得再踩注入器（防嵌套分配挂死）
  INJECT.store(false, Ordering::Relaxed);
  let err = res.expect_err("大额分配失败注入下删除枚举物化须平滑上抛，绝不得 abort");
  assert!(
    is_reserve_fail(&err),
    "须收口为 reserve_fail 单机制: {err:?}"
  );

  // 进程存活面：关注入后同命令照常执行且键集收敛为零
  assert_eq!(ss.delete_slot_keys(&[SLOT0]).await?, KEY_COUNT as u64);
  assert_eq!(ss.db_size().await?, 0);
  Ok(())
}

/// count_keys_in_slot 冷区 Degrade 候选入列臂：KEY_COUNT 个落冷区的 TTL 存活键
/// 在注入下平滑上抛，关注入后计数与活键口径一致
#[compio::test]
async fn count_keys_in_slot_degrade_alloc_fail_smooth() -> aok::Void {
  let _serial = lock_serial();
  let (_dir, store) = open_test_store("slot_count_oom.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);
  seed(&ss, KEY_COUNT).await;
  seed_future_ttl(&session, KEY_COUNT).await;
  // 刷盘驱逐使 TTL 旁路记录落入冷区（RecordOnDisk → TtlGate::Degrade），
  // 计数面全量键入 degrade_keys 候选列——本臂的最大暴露形态
  store.flush_and_evict_all().await?;

  let _guard = InjectGuard;
  INJECT.store(true, Ordering::Relaxed);
  let res = ss.count_keys_in_slot(SLOT0).await;
  // 断言前先关注入：失败态 panic 格式化不得再踩注入器（防嵌套分配挂死）
  INJECT.store(false, Ordering::Relaxed);
  let err = res.expect_err("冷区候选入列须走 reserve_fail 单机制，不得 abort");
  assert!(
    is_reserve_fail(&err),
    "须收口为 reserve_fail 单机制: {err:?}"
  );

  // 进程存活面：关注入后计数照常（Degrade 异步复判全部存活）
  assert_eq!(ss.count_keys_in_slot(SLOT0).await?, KEY_COUNT);
  Ok(())
}

/// get_keys_in_slot collect 臂（复制快照 usize::MAX 全量收集的最大暴露面）：
/// 注入下倍增扩容平滑上抛，关注入后全量收集与 COUNT 口径一致
#[compio::test]
async fn get_keys_in_slot_collect_alloc_fail_smooth() -> aok::Void {
  let _serial = lock_serial();
  let (_dir, store) = open_test_store("slot_get_oom.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);
  seed(&ss, KEY_COUNT).await;

  let _guard = InjectGuard;
  INJECT.store(true, Ordering::Relaxed);
  let res = ss.get_keys_in_slot(SLOT0, usize::MAX).await;
  // 断言前先关注入：失败态 panic 格式化不得再踩注入器（防嵌套分配挂死）
  INJECT.store(false, Ordering::Relaxed);
  let err = res.expect_err("复制快照全量收集须走 collect 平滑单轨，不得 abort");
  assert!(
    is_reserve_fail(&err),
    "须收口为 reserve_fail 单机制: {err:?}"
  );

  // 进程存活面：关注入后全量收集成功且与 COUNT 恒等（口径分叉锁）
  let all = ss.get_keys_in_slot(SLOT0, usize::MAX).await?;
  assert_eq!(all.len(), KEY_COUNT);
  assert_eq!(ss.count_keys_in_slot(SLOT0).await?, all.len());
  Ok(())
}

/// get_keys_in_slot_with 流式臂注入零误伤锁：逐候选 `try_key_vec` 为小额
/// 键拷贝（低于阈值照常放行），全窗扫描在注入开启态照常逐个交付 emit 且
/// 早停语义不受扰（本臂补齐后与 collect 侧同轨；≥ 阈值的单巨键面因 whlog
/// 记录重组内部分配非平滑轨先触顶，注入器形态下不可安全触达，见文件头注记）
#[compio::test]
async fn get_keys_in_slot_with_unaffected_by_inject() -> aok::Void {
  let _serial = lock_serial();
  const HALF: usize = KEY_COUNT / 2;
  let (_dir, store) = open_test_store("slot_with_oom.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);
  seed(&ss, KEY_COUNT).await;

  let _guard = InjectGuard;
  INJECT.store(true, Ordering::Relaxed);
  let mut emitted = 0usize;
  let res = ss
    .get_keys_in_slot_with(SLOT0, HALF, |_| {
      emitted += 1;
      emitted < HALF
    })
    .await;
  // 断言前先关注入：失败态 panic 格式化不得再踩注入器（防嵌套分配挂死）
  INJECT.store(false, Ordering::Relaxed);
  res?;
  assert_eq!(
    emitted, HALF,
    "流式臂小额物化在注入窗口内照常放行并按时早停"
  );
  Ok(())
}

/// get_keys_in_slot_excluding 票执行注记①锁：唯一消费面迁移分批以
/// MAX_MIGRATION_BATCH_COUNT=64 有界预付容量，注入开启态下真实形态（64 名额
/// + 槽头排除键）零增长分配照常产出，杜绝族内双轨与误伤豁免面
#[compio::test]
async fn get_keys_in_slot_excluding_bounded_under_inject() -> aok::Void {
  let _serial = lock_serial();
  const BATCH: usize = 64;
  let (_dir, store) = open_test_store("slot_excl_oom.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);
  seed(&ss, KEY_COUNT).await;
  // 槽头排除键面：排除键不占名额持续推进（有界窗内零增长分配）
  let head = b"k:0000000".to_vec();

  let _guard = InjectGuard;
  INJECT.store(true, Ordering::Relaxed);
  let res = ss
    .get_keys_in_slot_excluding(SLOT0, BATCH, |k| k == head)
    .await;
  // 断言前先关注入：失败态 panic 格式化不得再踩注入器（防嵌套分配挂死）
  INJECT.store(false, Ordering::Relaxed);
  let keys = res.expect("有界 64 名额 + 排除臂在注入开启态须照常产出（预付容量不触顶）");
  assert_eq!(keys.len(), BATCH);
  assert!(!keys.iter().any(|k| k == &head), "排除键不得占名额");
  Ok(())
}

/// db_size 同机制侧邻臂（票 2.c 顺扫点）：冷区 Degrade 候选入列在注入下
/// 平滑上抛，关注入后与 len(KEYS) 恒等口径零漂移
#[compio::test]
async fn db_size_degrade_alloc_fail_smooth() -> aok::Void {
  let _serial = lock_serial();
  let (_dir, store) = open_test_store("slot_dbsize_oom.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);
  seed(&ss, KEY_COUNT).await;
  seed_future_ttl(&session, KEY_COUNT).await;
  store.flush_and_evict_all().await?;

  let _guard = InjectGuard;
  INJECT.store(true, Ordering::Relaxed);
  let res = ss.db_size().await;
  // 断言前先关注入：失败态 panic 格式化不得再踩注入器（防嵌套分配挂死）
  INJECT.store(false, Ordering::Relaxed);
  let err = res.expect_err("DBSIZE 侧邻候选入列同走 reserve_fail 单机制，不得 abort");
  assert!(
    is_reserve_fail(&err),
    "须收口为 reserve_fail 单机制: {err:?}"
  );

  // 进程存活面 + 既有一致性口径零漂移
  assert_eq!(ss.db_size().await?, KEY_COUNT);
  assert_eq!(ss.db_keys(b"*").await?.len(), ss.db_size().await?);
  Ok(())
}
