#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
pub use error::{Error, Result};

pub mod active_worker_monitor;
pub mod cooperative_dispose_guard;
pub mod counting_event_slim;
pub mod double_turnstile_barrier;
pub mod leader_barrier;
pub mod read_optimized_lock;
pub mod semaphore;
pub mod single_writer_multi_reader_lock;

pub use active_worker_monitor::ActiveWorkerMonitor;
pub use cooperative_dispose_guard::{CooperativeDisposeGuard, DisposeResult};
pub use counting_event_slim::CountingEventSlim;
pub use double_turnstile_barrier::DoubleTurnstileBarrier;
pub use leader_barrier::LeaderBarrier;
pub use read_optimized_lock::{LockToken, LockType, ReadOptimizedLock};
pub use semaphore::Semaphore;
pub use single_writer_multi_reader_lock::SingleWriterMultiReaderLock;
