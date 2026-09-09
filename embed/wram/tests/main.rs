//! wram 集成测试唯一入口（单一测试二进制）
//!
//! 模块划分与各自对标的 C# 测试文件见各模块文件头部中文注释：
//! - `align`：扇区对齐算术与 SectorRange（对标 Tsavorite Allocator 对齐数学）
//! - `aligned_buf`：AlignedBuf 缓冲区本体（对标 SectorAlignedMemory 语义）
//! - `direct_vm`：直接虚拟内存（对标 NativeAllocatorTests.cs）
//! - `pool_ladder`：size class 阶梯算术（对标 SectorAlignedBufferPoolTests.cs 阶梯部分）
//! - `pool_get_return`：Get/Return 基础语义与归还清零策略
//! - `pool_cross_thread`：跨线程 Origin-Return 路由
//! - `pool_budget`：字节预算、bypass 与池关闭拆除
//! - `pool_stress`：并发压力与生命周期竞态（对标 SectorAlignedBufferPoolStressTests.cs 轻量化）
mod suite;

/// 全套件唯一的日志初始化入口（各模块不得重复定义 ctor）
#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}
