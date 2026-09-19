//! 物理层：WAL 定长环形窗口日志（TsavoriteLog 对标；8B WalFrameHeader 物理帧 +
//! 位点原子 + 环形缓冲 + 磁盘段恢复）

pub mod commit;

pub mod config;

mod flush;

pub mod header;

pub mod iterator;

pub mod log;

mod pipeline;

pub mod record;

pub mod ring_buffer;

pub mod sequence_number_generator;

mod disk_window;
