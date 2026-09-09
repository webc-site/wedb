//! wram 集成测试唯一入口（单一测试二进制）
//!
//! 模块划分与各自对标的 C# 测试文件见各模块文件头部中文注释：
//! - `align`：扇区对齐算术与 SectorRange（对标 Tsavorite Allocator 对齐数学）
//! - `direct_vm`：直接虚拟内存（对标 NativeAllocatorTests.cs）
//!
//! BufferPool / AlignedBuf 全套测试已随本体下沉 wutil（Utilities 层归位）。
mod suite;

/// 全套件唯一的日志初始化入口（各模块不得重复定义 ctor）
#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}
