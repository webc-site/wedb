//! `wdev` 集成测试唯一入口（整个 crate 仅此一个测试二进制）。
//!
//! 被测实现对标 C# Garnet Tsavorite 设备层：`libs/storage/Tsavorite/cs/src/devices/`。
//! 用例按语义拆分至 `device/` 子模块，各模块顶部标注对标的 C# 测试文件路径与方法名。

mod device;
mod support;

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}
