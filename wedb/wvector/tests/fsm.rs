#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 空闲空间映射集成测试（自 src/fsm.rs 外迁）

use std::sync::{Arc, atomic::Ordering};

use wvector::{
  Callbacks, Context,
  error::FsmError,
  fsm::{BLOCK_SIZE_IDS, FAST_SIZE, FreeSpaceMap, bit_used},
};
use wvector_test::MemStore;

fn callbacks() -> Callbacks<MemStore> {
  Callbacks::new(Arc::new(MemStore::new()))
}

#[compio::test]
async fn fresh_and_next_id() {
  let ctx = Context::new(8);
  let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true)
    .await
    .unwrap();
  assert!(!fsm.has_free_ids.load(Ordering::Acquire));
  assert_eq!(fsm.next_id(&ctx).await.unwrap().id(), 0);
  assert_eq!(fsm.next_id(&ctx).await.unwrap().id(), 1);
  assert_eq!(fsm.total_used(), 2);
}

#[compio::test]
async fn mark_free_out_of_range() {
  let ctx = Context::new(8);
  let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true)
    .await
    .unwrap();
  assert_eq!(fsm.mark_free(&ctx, 0).await, Err(FsmError::IdOutOfRange(0)));
}

#[compio::test]
async fn delete_and_reuse() {
  let ctx = Context::new(8);
  let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true)
    .await
    .unwrap();

  for _ in 0u32..64 {
    let _ = fsm.next_id(&ctx).await.unwrap();
  }
  assert_eq!(fsm.next_id(&ctx).await.unwrap().id(), 64);

  fsm.mark_free(&ctx, 37).await.unwrap();
  assert_eq!(fsm.next_id(&ctx).await.unwrap().id(), 37);
  assert_eq!(fsm.next_id(&ctx).await.unwrap().id(), 65);
  assert!(!fsm.has_free_ids.load(Ordering::Acquire));
}

#[compio::test]
async fn recovery_from_store() {
  let ctx = Context::new(8);
  let cbs = callbacks();
  let fsm = FreeSpaceMap::new(&ctx, cbs.clone(), false, true)
    .await
    .unwrap();

  for _ in 0u32..64 {
    let _ = fsm.next_id(&ctx).await.unwrap();
  }
  fsm.mark_free(&ctx, 37).await.unwrap();

  // 同一存储重建，状态全量恢复
  let fsm = FreeSpaceMap::new(&ctx, cbs, false, true).await.unwrap();
  assert_eq!(fsm.max_id() + 1, 64);
  assert!(fsm.has_free_ids.load(Ordering::Acquire));
  assert_eq!(fsm.fast_free_list.len(), 1);
  assert_eq!(fsm.next_id(&ctx).await.unwrap().id(), 37);
}

#[compio::test]
async fn block_expansion() {
  let ctx = Context::new(8);
  let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true)
    .await
    .unwrap();
  for _ in 0u32..BLOCK_SIZE_IDS as u32 + 1 {
    let _ = fsm.next_id(&ctx).await.unwrap();
  }
  assert_eq!(fsm.max_id(), BLOCK_SIZE_IDS as u32);
  assert_eq!(fsm.total_used(), BLOCK_SIZE_IDS + 1);
}

/// 重填臂交付回归（票：wvector-fsm-refill-reuse-arm-drops-marked-id）：
/// 队列耗尽、位图尚有空闲位时，`next_id` 重填后必须交付已置占用位的
/// 已删 id——占用置位与交付同单原子对称，不得吞丢转铸新 id。
#[compio::test]
async fn refill_reuse_delivers_marked_id() {
  let ctx = Context::new(8);
  let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true)
    .await
    .unwrap();

  // 铸造 0..=1099
  for _ in 0u32..1100 {
    let _ = fsm.next_id(&ctx).await.unwrap();
  }
  // 溢出删除 50..=1099：前 1024 枚（50..=1073）入快速队列，其余 26 枚
  //（1074..=1099）仅位图清位不入队
  for id in 50u32..1100 {
    fsm.mark_free(&ctx, id).await.unwrap();
  }
  // 排空快速队列（第一臂复用）
  for _ in 0..FAST_SIZE {
    let id = fsm.next_id(&ctx).await.unwrap().id();
    assert!(
      (50..1074).contains(&id),
      "排空应复用队列内已删 id, got {id}"
    );
  }
  assert!(fsm.fast_free_list.is_empty());

  // 队列空、位图有空闲位：命中重填臂，交付已置占用位的已删 id
  let id = fsm.next_id(&ctx).await.unwrap().id();
  assert!(
    (1074..1100).contains(&id),
    "重填复用应交付已删 id 而非转铸新 id, got {id}"
  );
  assert_eq!(fsm.max_id(), 1099, "重填复用不得推进铸造水位");

  // total_used 与位图实占恒等（无占位无主的幽灵 id）
  let mut occupied = 0usize;
  fsm
    .visit_used(&ctx, |_| {
      occupied += 1;
      true
    })
    .await
    .unwrap();
  assert_eq!(fsm.total_used(), occupied, "total_used 与实占失恒");
  assert_eq!(occupied, 1100 - 26 + 1, "实占 = 1100 − 空闲 25");
}

#[compio::test]
async fn visit_used_and_bit_helpers() {
  let ctx = Context::new(8);
  let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true)
    .await
    .unwrap();
  for _ in 0u32..8 {
    let _ = fsm.next_id(&ctx).await.unwrap();
  }
  fsm.mark_free(&ctx, 5).await.unwrap();

  let mut seen = Vec::new();
  fsm
    .visit_used(&ctx, |id| {
      seen.push(id);
      true
    })
    .await
    .unwrap();
  assert_eq!(seen, vec![0, 1, 2, 3, 4, 6, 7]);

  assert!(bit_used(0b1000_0000, 0));
  assert!(!bit_used(0b1000_0000, 1));
}
