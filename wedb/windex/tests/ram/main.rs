//! windex ram（原生内存）集成测试唯一入口（单一测试二进制）
//!
//! 模块划分与各自对标的 C# 测试文件见各模块文件头部中文注释：
//! - `direct_vm`：直接虚拟内存（对标 NativeAllocatorTests.cs）
//!
//! 扇区对齐算术 / BufferPool / AlignedBuf 全套测试已随本体下沉 wbase。
mod suite;

/// 全套件唯一的日志初始化入口（各模块不得重复定义 ctor）
#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}
