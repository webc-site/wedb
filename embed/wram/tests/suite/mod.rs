//! 集成测试子模块聚合（由 main.rs 以 `mod suite;` 挂载，避免各文件被 Cargo 识别为独立测试二进制）
pub mod align;
pub mod aligned_buf;
pub mod direct_vm;
pub mod pool_budget;
pub mod pool_cross_thread;
pub mod pool_get_return;
pub mod pool_ladder;
pub mod pool_stress;
