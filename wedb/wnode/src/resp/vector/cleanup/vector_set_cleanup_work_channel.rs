//! 无界单消费者清理工作通道（对标 libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs）
//!
//! 基于 `wbase::pool::EventWorkQueue<T>`。

use wbase::pool::EventWorkQueue;

pub type VectorSetCleanupWorkChannel<T> = EventWorkQueue<T>;
