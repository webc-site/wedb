use std::rc::Rc;

use compio::runtime::Runtime;
use parking_lot::Mutex;
use wnode::task::{TaskManager, TaskPlacementCategory, TaskType};

/// test/standalone/Garnet.test/TaskManagerTests.cs:TestBasicRegisterAndRunAsync
#[test]
fn test_basic_register_and_run_async() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let task_manager = TaskManager::new();
    let ran = Rc::new(Mutex::new(false));
    let ran_clone = Rc::clone(&ran);

    assert!(task_manager.register_and_run(
      TaskType::IndexAutoGrowTask,
      move |_token| async move {
        *ran_clone.lock() = true;
      },
      false,
    ));

    assert!(task_manager.wait_async(TaskType::IndexAutoGrowTask).await);
    assert!(*ran.lock());
  });
}

/// test/standalone/Garnet.test/TaskManagerTests.cs:TestDoubleRegistrationAsync
#[test]
fn test_double_registration_async() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let task_manager = TaskManager::new();
    let started_counter = Rc::new(Mutex::new(0));
    let started_counter_clone = Rc::clone(&started_counter);

    assert!(task_manager.register_and_run(
      TaskType::IndexAutoGrowTask,
      move |_token| async move {
        *started_counter_clone.lock() += 1;
      },
      false,
    ));
    assert!(!task_manager.register_and_run(TaskType::IndexAutoGrowTask, |_| async {}, false));

    assert!(task_manager.wait_async(TaskType::IndexAutoGrowTask).await);
    assert_eq!(*started_counter.lock(), 1);
  });
}

/// test/standalone/Garnet.test/TaskManagerTests.cs:TestTaskPlacementCategoryCancellation
#[test]
fn test_task_placement_category_cancellation() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for category in [
      TaskPlacementCategory::PRIMARY,
      TaskPlacementCategory::ALL,
      TaskPlacementCategory::REPLICA,
    ] {
      let task_manager = TaskManager::new();
      task_manager.register_and_run(
        TaskType::AofSizeLimitTask,
        |token| async move { token.wait().await },
        false,
      );
      task_manager.register_and_run(
        TaskType::IndexAutoGrowTask,
        |token| async move { token.wait().await },
        false,
      );

      if category == TaskPlacementCategory::REPLICA {
        task_manager.cancel_category_async(category).await;
        assert!(task_manager.is_running(TaskType::AofSizeLimitTask));
        task_manager
          .cancel_category_async(TaskPlacementCategory::ALL)
          .await;
      } else {
        task_manager.cancel_category_async(category).await;
      }

      assert!(!task_manager.wait_async(TaskType::AofSizeLimitTask).await);
      assert!(!task_manager.wait_async(TaskType::IndexAutoGrowTask).await);
    }
  });
}

/// test/standalone/Garnet.test/TaskManagerTests.cs:TestCleanupOnCompletion
#[test]
fn test_cleanup_on_completion() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let task_manager = TaskManager::new();
    let ran = Rc::new(Mutex::new(false));
    let ran_clone = Rc::clone(&ran);

    assert!(task_manager.register_and_run(
      TaskType::IndexAutoGrowTask,
      move |_token| async move {
        *ran_clone.lock() = true;
      },
      true,
    ));

    assert!(task_manager.wait_async(TaskType::IndexAutoGrowTask).await);
    assert!(*ran.lock());
    assert!(!task_manager.is_registered(TaskType::IndexAutoGrowTask));
    assert!(!task_manager.is_running(TaskType::IndexAutoGrowTask));
  });
}
