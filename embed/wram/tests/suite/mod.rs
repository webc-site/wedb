//! 集成测试子模块聚合（由 main.rs 以 `mod suite;` 挂载，避免各文件被 Cargo 识别为独立测试二进制）
pub mod align;
pub mod direct_vm;
