//! Wedb 集成测试共用装配 (`wedb_test`)
//!
//! 跨 crate 测试共用的底层测试工具：存储引擎测试装配、日志初始化、
//! 统一服务器起服、RESP 帧级客户端、临时目录开库。仅以 dev-dependencies
//! 形式消费，不进入任何生产链接面。

mod config;
mod net;
mod store;

pub use config::test_store_config;
pub use net::{SilentNode, wait_for};
pub use store::open_test_store;

/// 测试日志初始化（对标 C# TestBase/OneTimeSetUp 的一次性日志装配）：
/// 链接本 crate 的测试二进制经 ctor 自动执行，测试文件无需各自再写
/// ctor 入口（log_init::init 内部 Once 保护，重复调用幂等）
#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// RESP 数组帧编码（`*N\r\n` 头 + N 个 `$len\r\npayload\r\n` 批量字符串）
#[must_use]
pub fn resp_frame(parts: &[&[u8]]) -> Vec<u8> {
  let mut out = Vec::new();
  out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
  for p in parts {
    out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
    out.extend_from_slice(p);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// RESP 数组帧编码（字符串切片入参）
#[must_use]
pub fn resp_frame_str(parts: &[&str]) -> Vec<u8> {
  let bytes: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
  resp_frame(&bytes)
}
