//! 设备层集成测试，按语义划分模块。
//!
//! 各模块顶部以中文注释标注对标的 C# 测试文件路径（只读参考仓库：
//! `garnet/libs/storage/Tsavorite/cs/test/`）。

mod alignment;
mod boundary;
mod capacity;
mod lifecycle;
mod null;
mod parallel;
mod recovery;
mod round_trip;
mod sync_contract;
mod truncate;
