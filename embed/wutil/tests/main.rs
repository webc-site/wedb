//! wutil 集成测试唯一入口（单一测试二进制）
//!
//! 模块划分与各自对标的 C# 测试文件见各模块文件头部中文注释：
//! - `aligned_buf`：AlignedBuf 缓冲区本体（对标 SectorAlignedMemory 语义）
//! - `pool_ladder`：size class 阶梯算术（对标 SectorAlignedBufferPoolTests.cs 阶梯部分）
//! - `pool_get_return`：Get/Return 基础语义与归还清零策略
//! - `pool_cross_thread`：跨线程 Origin-Return 路由
//! - `pool_budget`：字节预算、bypass 与池关闭拆除
//! - `pool_stress`：并发压力与生命周期竞态（对标 SectorAlignedBufferPoolStressTests.cs 轻量化）
//!
//! 本套件随 BufferPool / AlignedBuf 自 wram 下沉至 wutil（Utilities 层归位，
//! 对标 C# Tsavorite `core/Utilities` 位于依赖图最底层的拓扑），测试内容不变。
mod suite;

/// 全套件唯一的日志初始化入口（各模块不得重复定义 ctor）
#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}
