//! 清理工作集合单元测试（对标 test/standalone/Garnet.test.vectorset/VectorSetCleanupWorkSetTests.cs）
//!
//! C# VectorSetCleanupWorkSet<TValue> 位于 libs/server/Resp/Vector/Cleanup，
//! rust 转写落点为 wnode::resp::vector::cleanup。

use std::sync::Arc;

use wnode::resp::vector::cleanup::vector_set_cleanup_work_set::VectorSetCleanupWorkSet;

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// C# VectorSetCleanupWorkSetTests.cs:AddedWorkIsPendingUntilCompleted
#[test]
fn added_work_is_pending_until_completed() {
  let work_set = VectorSetCleanupWorkSet::<i32>::new();

  assert!(!work_set.contains(b"a"));
  assert!(!work_set.try_complete(b"a"));

  assert!(work_set.try_add(b"a".to_vec(), 1));
  assert!(work_set.contains(b"a"));
  // 重复登记同键被拒绝
  assert!(!work_set.try_add(b"a".to_vec(), 2));

  assert!(work_set.try_complete(b"a"));
  assert!(!work_set.contains(b"a"));
}

/// C# VectorSetCleanupWorkSetTests.cs:EntriesCanBeEnumerated
#[test]
fn entries_can_be_enumerated() {
  let work_set = VectorSetCleanupWorkSet::<i32>::new();

  assert!(work_set.try_add(b"a".to_vec(), 1));
  assert!(work_set.try_add(b"b".to_vec(), 2));

  // C# foreach 枚举：rust 以快照承接（条目集合等价比较）
  let mut values: Vec<i32> = work_set.snapshot().into_iter().map(|(_, v)| v).collect();
  values.sort_unstable();
  assert_eq!(values, vec![1, 2]);
}

/// C# VectorSetCleanupWorkSetTests.cs:WaitForCompletionBlocksUntilTheEntryIsCompleted
#[test]
fn wait_for_completion_blocks_until_entry_completed() {
  let work_set = Arc::new(VectorSetCleanupWorkSet::<i32>::new());

  assert!(work_set.try_add(b"a".to_vec(), 1));

  // 等待者线程：条目未完成前不得返回（对标 C# Task.Run + IsCompleted 检查）
  let waiter = {
    let work_set = Arc::clone(&work_set);
    std::thread::spawn(move || work_set.wait_for_completion(b"a"))
  };
  assert!(!waiter.is_finished());

  assert!(work_set.try_complete(b"a"));
  waiter.join().unwrap();
}
