//! 底层测试装配（`wtest_base`）：小预算存储配置、临时目录开库、RESP 帧级
//! 客户端基建、假端点与轮询等待、测试日志一次性装配
//!
//! 分层口径：本 crate 只依赖 wdev / wkv / compio 一侧的底层，供任何层级
//! crate 的测试消费（wnode / wkv 等），不触顶层集群门面 `wedb`；
//! 集群形态装配（NodeAssembly / start_node / cluster_decorate）只在
//! `wedb_test`，任何更下层 crate 的测试不得引更上层支撑件（对标 C#
//! garnet/test/standalone 与 garnet/test/cluster 顶层测试工程、libs 不
//! 引用测试工程的单向拓扑）。仅以 dev-dependencies 形式消费，不进入
//! 任何生产链接面。

mod config;
mod net;
mod store;

pub use config::{test_store_config, test_store_config_with_budget};
pub use net::{FailoverNode, GossipNode, SilentNode, StopWritesNode, wait_for};
pub use store::{open_test_store, open_test_store_with_budget};

/// 测试日志初始化（对标 C# TestBase/OneTimeSetUp 的一次性日志装配）：
/// 链接本 crate 的测试二进制经 ctor 自动执行，测试文件无需各自再写
/// ctor 入口（log_init::init 内部 Once 保护，重复调用幂等）
#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// RESP 命令参数切片构造（测试专用语法糖）：字面量表达到 `&[&[u8]]`
///
/// `a![b"key", b"val"]` ≡ `&[b"key" as &[u8], b"val" as &[u8]]`
#[macro_export]
macro_rules! a {
  ($($x:expr),* $(,)?) => {
    &[$($x as &[u8]),*]
  };
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
