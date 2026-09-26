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
//!
//! 自研依据: 测试基建（测试存储装配单点化）

use itoa::Buffer;
mod config;
mod log_capture;
mod net;
mod store;

pub use config::{test_store_config, test_store_config_with_budget};
pub use log_capture::{log_capture_mark, log_capture_records_since};
pub use net::{
  DEFAULT_WAIT_STEP, DEFAULT_WAIT_TIMEOUT, FailoverNode, GossipNode, IntoDuration, SilentNode,
  StopWritesNode, parse_duration_token, parse_frame, parse_frame_slices, try_parse_duration_token,
  try_parse_frame, wait_assert_sync, wait_for, wait_for_step, wait_for_step_sync, wait_until_sync,
  wait_yield_sync,
};
pub use store::{open_test_store, open_test_store_with_budget};
use wresp::ext::RespVecExt;

/// 测试日志初始化（对标 C# TestBase/OneTimeSetUp 的一次性日志装配）：
/// 链接本 crate 的测试二进制经 ctor 自动执行，测试文件无需各自再写
/// ctor 入口。装配的是捕获+stdout 双面 logger（见 log_capture 模块头）——
/// 测试内再装第三方全局 logger 恒失败，留痕断言一律走
/// `log_capture_mark` / `log_capture_records_since`
#[ctor::ctor(unsafe)]
fn _log_init() {
  log_capture::install();
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

/// RESP 数组帧编码宏
///
/// 支持数组字面量或可变参数切片，自动将各项通过 `AsRef<[u8]>` 转为字节切片：
/// - `resp_frame!(["SET", key, val])`
/// - `resp_frame!("SET", key, val)`
/// - `resp_frame!([])`
#[macro_export]
macro_rules! resp_frame {
  ([ $($elem:expr),* $(,)? ]) => {
    $crate::resp_frame(&[ $(::std::convert::AsRef::<[u8]>::as_ref(&$elem)),* ])
  };
  ($($elem:expr),* $(,)? ) => {
    $crate::resp_frame(&[ $(::std::convert::AsRef::<[u8]>::as_ref(&$elem)),* ])
  };
}

/// 断言响应为 RESP 简单字符串 `+OK\r\n`
#[macro_export]
macro_rules! assert_resp_ok {
  ($resp:expr) => {{
    let resp = ::std::convert::AsRef::<[u8]>::as_ref(&$resp);
    assert_eq!(
      resp,
      b"+OK\r\n",
      "expected RESP +OK, got: {:?}",
      ::std::str::from_utf8(resp).unwrap_or("<binary>")
    );
  }};
  ($resp:expr, $($arg:tt)+) => {{
    let resp = ::std::convert::AsRef::<[u8]>::as_ref(&$resp);
    assert_eq!(
      resp,
      b"+OK\r\n",
      $($arg)+
    );
  }};
}

/// 断言响应为 RESP 整数 `:<expected>\r\n`（栈上格式化，零堆分配）
#[macro_export]
macro_rules! assert_resp_int {
  ($resp:expr, $expected:expr) => {{
    let resp = ::std::convert::AsRef::<[u8]>::as_ref(&$resp);
    let expected_val: i64 = ($expected) as i64;
    let mut buf = [0u8; 32];
    let expected_frame = $crate::format_resp_int(expected_val, &mut buf);
    assert_eq!(
      resp,
      expected_frame,
      "expected RESP int :{}, got: {:?}",
      expected_val,
      ::std::str::from_utf8(resp).unwrap_or("<binary>")
    );
  }};
  ($resp:expr, $expected:expr, $($arg:tt)+) => {{
    let resp = ::std::convert::AsRef::<[u8]>::as_ref(&$resp);
    let expected_val: i64 = ($expected) as i64;
    let mut buf = [0u8; 32];
    let expected_frame = $crate::format_resp_int(expected_val, &mut buf);
    assert_eq!(
      resp,
      expected_frame,
      $($arg)+
    );
  }};
}

#[macro_export]
#[doc(hidden)]
macro_rules! __parse_test_duration {
  ($n:literal s) => {
    ::std::time::Duration::from_secs($n)
  };
  ($n:literal ms) => {
    ::std::time::Duration::from_millis($n)
  };
  ($n:literal us) => {
    ::std::time::Duration::from_micros($n)
  };
  ($n:literal ns) => {
    ::std::time::Duration::from_nanos($n)
  };
  ($n:literal m) => {
    ::std::time::Duration::from_secs(($n as u64) * 60)
  };
  ($i:ident) => {
    $crate::IntoDuration::into_duration($i)
  };
  ($t:tt) => {
    $crate::parse_duration_token(stringify!($t))
  };
  ($($e:tt)+) => {
    $crate::IntoDuration::into_duration($($e)+)
  };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __wait_until_exec {
  ($cond:expr, $timeout:expr, $step:expr, $($arg:tt)+) => {{
    let timeout = $timeout;
    let step = $step;
    if !$crate::wait_for_step(|| ($cond), timeout, step).await {
      panic!($($arg)+);
    }
  }};
  ($cond:expr, $timeout:expr, $step:expr) => {{
    let timeout = $timeout;
    let step = $step;
    if !$crate::wait_for_step(|| ($cond), timeout, step).await {
      panic!("wait_until! timed out after {:?}: {}", timeout, stringify!($cond));
    }
  }};
}

/// 轮询等待条件成立宏（彻底淘汰测试中手写的 while + Instant::now() 循环）
///
/// 语法示例：
/// - `wait_until!(condition)`
/// - `wait_until!(condition, "失败说明")`
/// - `wait_until!(condition, timeout: 5s)`
/// - `wait_until!(condition, timeout: 5s, "失败说明")`
/// - `wait_until!(condition, timeout: 5s, step: 5ms)`
/// - `wait_until!(condition, timeout: 5s, step: 5ms, "失败说明")`
/// - `wait_until!(condition, step: 5ms, timeout: 5s, "失败说明: {}", arg)`
#[macro_export]
macro_rules! wait_until {
  // 1. timeout + step + msg
  ($cond:expr, timeout: $t:tt, step: $s:tt, $($arg:tt)+) => {
    $crate::__wait_until_exec!($cond, $crate::__parse_test_duration!($t), $crate::__parse_test_duration!($s), $($arg)+)
  };
  ($cond:expr, timeout: $t:expr, step: $s:expr, $($arg:tt)+) => {
    $crate::__wait_until_exec!($cond, $crate::IntoDuration::into_duration($t), $crate::IntoDuration::into_duration($s), $($arg)+)
  };
  // 2. timeout + step
  ($cond:expr, timeout: $t:tt, step: $s:tt) => {
    $crate::__wait_until_exec!($cond, $crate::__parse_test_duration!($t), $crate::__parse_test_duration!($s))
  };
  ($cond:expr, timeout: $t:expr, step: $s:expr) => {
    $crate::__wait_until_exec!($cond, $crate::IntoDuration::into_duration($t), $crate::IntoDuration::into_duration($s))
  };
  // 3. step + timeout + msg
  ($cond:expr, step: $s:tt, timeout: $t:tt, $($arg:tt)+) => {
    $crate::__wait_until_exec!($cond, $crate::__parse_test_duration!($t), $crate::__parse_test_duration!($s), $($arg)+)
  };
  ($cond:expr, step: $s:expr, timeout: $t:expr, $($arg:tt)+) => {
    $crate::__wait_until_exec!($cond, $crate::IntoDuration::into_duration($t), $crate::IntoDuration::into_duration($s), $($arg)+)
  };
  // 4. step + timeout
  ($cond:expr, step: $s:tt, timeout: $t:tt) => {
    $crate::__wait_until_exec!($cond, $crate::__parse_test_duration!($t), $crate::__parse_test_duration!($s))
  };
  ($cond:expr, step: $s:expr, timeout: $t:expr) => {
    $crate::__wait_until_exec!($cond, $crate::IntoDuration::into_duration($t), $crate::IntoDuration::into_duration($s))
  };
  // 5. timeout + msg
  ($cond:expr, timeout: $t:tt, $($arg:tt)+) => {
    $crate::__wait_until_exec!($cond, $crate::__parse_test_duration!($t), $crate::DEFAULT_WAIT_STEP, $($arg)+)
  };
  ($cond:expr, timeout: $t:expr, $($arg:tt)+) => {
    $crate::__wait_until_exec!($cond, $crate::IntoDuration::into_duration($t), $crate::DEFAULT_WAIT_STEP, $($arg)+)
  };
  // 6. timeout only
  ($cond:expr, timeout: $t:tt) => {
    $crate::__wait_until_exec!($cond, $crate::__parse_test_duration!($t), $crate::DEFAULT_WAIT_STEP)
  };
  ($cond:expr, timeout: $t:expr) => {
    $crate::__wait_until_exec!($cond, $crate::IntoDuration::into_duration($t), $crate::DEFAULT_WAIT_STEP)
  };
  // 7. step + msg
  ($cond:expr, step: $s:tt, $($arg:tt)+) => {
    $crate::__wait_until_exec!($cond, $crate::DEFAULT_WAIT_TIMEOUT, $crate::__parse_test_duration!($s), $($arg)+)
  };
  ($cond:expr, step: $s:expr, $($arg:tt)+) => {
    $crate::__wait_until_exec!($cond, $crate::DEFAULT_WAIT_TIMEOUT, $crate::IntoDuration::into_duration($s), $($arg)+)
  };
  // 8. step only
  ($cond:expr, step: $s:tt) => {
    $crate::__wait_until_exec!($cond, $crate::DEFAULT_WAIT_TIMEOUT, $crate::__parse_test_duration!($s))
  };
  ($cond:expr, step: $s:expr) => {
    $crate::__wait_until_exec!($cond, $crate::DEFAULT_WAIT_TIMEOUT, $crate::IntoDuration::into_duration($s))
  };
  // 9. cond + msg
  ($cond:expr, $($arg:tt)+) => {
    $crate::__wait_until_exec!($cond, $crate::DEFAULT_WAIT_TIMEOUT, $crate::DEFAULT_WAIT_STEP, $($arg)+)
  };
  // 10. cond only
  ($cond:expr) => {
    $crate::__wait_until_exec!($cond, $crate::DEFAULT_WAIT_TIMEOUT, $crate::DEFAULT_WAIT_STEP)
  };
}

/// 栈上格式化 RESP 整数帧（`:<val>\r\n`，零堆分配）
#[inline]
pub fn format_resp_int(val: i64, buf: &mut [u8; 32]) -> &[u8] {
  buf[0] = b':';
  let mut itoa_buf = Buffer::new();
  let digits = itoa_buf.format(val).as_bytes();
  let len = 1 + digits.len();
  buf[1..len].copy_from_slice(digits);
  buf[len] = b'\r';
  buf[len + 1] = b'\n';
  &buf[..len + 2]
}

/// RESP 数组帧编码（`*N\r\n` 头 + N 个 `$len\r\npayload\r\n` 批量字符串）
#[must_use]
pub fn resp_frame(parts: &[&[u8]]) -> Vec<u8> {
  let count = parts.len();
  let payload_bytes: usize = parts.iter().map(|p| p.len()).sum();
  let mut out =
    Vec::with_capacity(payload_bytes.saturating_add(count.saturating_mul(16).saturating_add(16)));
  let mut writer = out.resp_writer2();
  writer.write_array_length(count);
  for &item in parts {
    writer.write_bulk_string(item);
  }
  out
}

/// RESP 数组帧编码（字符串切片入参）
#[must_use]
pub fn resp_frame_str(parts: &[&str]) -> Vec<u8> {
  let count = parts.len();
  let payload_bytes: usize = parts.iter().map(|p| p.len()).sum();
  let mut out =
    Vec::with_capacity(payload_bytes.saturating_add(count.saturating_mul(16).saturating_add(16)));
  let mut writer = out.resp_writer2();
  writer.write_array_length(count);
  for item in parts {
    writer.write_bulk_string(item.as_bytes());
  }
  out
}

/// 构造 RESP 整数帧（`:<val>\r\n`）
#[must_use]
pub fn resp_int_frame(val: i64) -> Vec<u8> {
  let mut buf = [0u8; 32];
  format_resp_int(val, &mut buf).to_vec()
}
