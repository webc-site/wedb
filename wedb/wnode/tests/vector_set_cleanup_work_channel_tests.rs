//! 清理工作通道单元测试（对标 test/standalone/Garnet.test.vectorset/VectorSetCleanupWorkChannelTests.cs）
//!
//! C# VectorSetCleanupWorkChannel<T> 位于 libs/server/Resp/Vector/Cleanup，
//! rust 转写落点为 wnode::resp::vector::cleanup。

use std::sync::Arc;

use compio::runtime::Runtime;
use wnode::resp::vector::cleanup::vector_set_cleanup_work_channel::VectorSetCleanupWorkChannel;

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// C# VectorSetCleanupWorkChannelTests.cs:PublishedItemIsReadable
#[test]
fn published_item_is_readable() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let channel = VectorSetCleanupWorkChannel::<i32>::new();

    assert!(!channel.has_pending());
    assert!(channel.try_publish(7));

    assert!(channel.has_pending());
    assert!(channel.wait_to_read().await);

    assert_eq!(channel.try_read(), Some(7));

    assert!(!channel.has_pending());
    assert_eq!(channel.try_read(), None);
  });
}

/// C# VectorSetCleanupWorkChannelTests.cs:CompletedChannelRejectsPublishesAndReleasesWaiters
#[test]
fn completed_channel_rejects_publishes_and_releases_waiters() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let channel = VectorSetCleanupWorkChannel::<i32>::new();
    // C# CompleteAndWaitForConsumerTask(Task.CompletedTask)：完成 + 等待空消费者
    channel.complete();

    assert!(!channel.try_publish(7));
    assert!(!channel.wait_to_read().await);
  });
}

/// C# VectorSetCleanupWorkChannelTests.cs:CompleteAndWaitForConsumerTaskDrainsBeforeReturning
#[test]
fn complete_drains_before_consumer_finishes() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let channel = Arc::new(VectorSetCleanupWorkChannel::<i32>::new());
    let consumed = Arc::new(std::sync::atomic::AtomicU32::new(0));

    // 消费者任务：对标 C# Task.Run 的 WaitToReadAsync/TryRead 排空循环
    let consumer = rt.spawn({
      let channel = Arc::clone(&channel);
      let consumed = Arc::clone(&consumed);
      async move {
        while channel.wait_to_read().await {
          while channel.try_read().is_some() {
            consumed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          }
        }
      }
    });

    assert!(channel.try_publish(1));
    assert!(channel.try_publish(2));

    // C# CompleteAndWaitForConsumerTask(consumer)：完成后消费者必须排空才返回
    channel.complete();
    let _ = consumer.await;

    assert_eq!(consumed.load(std::sync::atomic::Ordering::Relaxed), 2);
  });
}
