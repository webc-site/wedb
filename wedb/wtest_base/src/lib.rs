//! 底层测试装配（`wtest_base`）：小预算存储配置、临时目录开库、RESP 帧级
//! 客户端基建、假端点与轮询等待、测试日志一次性装配
//!
//! 分层口径：本 crate 只依赖 wdev / wkv / wresp / compio 一侧的底层，供任何层级
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
use wresp::ext::RespVecExt;

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
  array_frame(
    parts.len(),
    parts.iter().map(|p| p.len()).sum(),
    parts.iter().copied(),
  )
}

/// RESP 数组帧编码（字符串切片入参）
#[must_use]
pub fn resp_frame_str(parts: &[&str]) -> Vec<u8> {
  array_frame(
    parts.len(),
    parts.iter().map(|p| p.len()).sum(),
    parts.iter().map(|p| p.as_bytes()),
  )
}

/// 测试帧装配单点：帧头与逐项 bulk string 一律出 wresp 写出面（itoa 栈上缓冲），
/// 本 crate 不再自拼第二套 RESP 编码；`payload_bytes` 只用于预留容量，
/// 不参与成帧
fn array_frame<'a>(
  count: usize,
  payload_bytes: usize,
  items: impl Iterator<Item = &'a [u8]>,
) -> Vec<u8> {
  let mut out = Vec::with_capacity(payload_bytes + count * 16 + 16);
  let mut writer = out.resp_writer2();
  writer.write_array_length(count);
  for item in items {
    writer.write_bulk_string(item);
  }
  out
}

#[cfg(test)]
mod tests {
  use super::{resp_frame, resp_frame_str};

  #[test]
  fn resp_frame_empty_and_simple() {
    assert_eq!(resp_frame(&[]), b"*0\r\n".to_vec());
    assert_eq!(resp_frame_str(&[]), b"*0\r\n".to_vec());
    assert_eq!(
      resp_frame(a![b"SET", b"k", b"v"]),
      b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".to_vec()
    );
  }

  #[test]
  fn resp_frame_binary_payload_not_escaped() {
    // 含 CRLF 与 NUL 的二进制载荷：只按字节长度成帧，不转义不分片
    assert_eq!(
      resp_frame(a![b"a\r\nb\0c"]),
      b"*1\r\n$6\r\na\r\nb\0c\r\n".to_vec()
    );
  }

  #[test]
  fn resp_frame_str_utf8_matches_byte_frame() {
    // 多字节 UTF-8：长度取字节数而非字符数，与 &[u8] 入口逐字节等价
    let mut expected = Vec::new();
    expected.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$8\r\n");
    expected.extend_from_slice("ключ".as_bytes());
    expected.extend_from_slice(b"\r\n");
    assert_eq!(resp_frame_str(&["GET", "ключ"]), expected);
    assert_eq!(
      resp_frame_str(&["GET", "ключ", "a\r\nb"]),
      resp_frame(a![b"GET", "ключ".as_bytes(), b"a\r\nb"])
    );
  }
}
