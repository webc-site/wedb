//! 无界单消费者清理工作通道（对标 libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs）
//!
//! 基于 `wbase::pool::EventWorkQueue<T>`。

pub type VectorSetCleanupWorkChannel<T> = wbase::pool::EventWorkQueue<T>;
