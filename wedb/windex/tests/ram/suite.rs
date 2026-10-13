//! 集成测试子模块聚合（由 main.rs 以 `mod suite;` 挂载，避免各文件被 Cargo 识别为独立测试二进制）
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/NativeHashIndexTests.cs（内存哈希索引面）
pub mod direct_vm;
