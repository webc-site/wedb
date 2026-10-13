//! RESP 读侧会话与 bulk 读值单源（GET/EVAL 读断言面）
//!
//! 收口 diskless 系五册逐字同形的 GET 读值解析：`$-1` nil 返 None，
//! `$len\r\nbody` 按长度截取；`what` 参数进失败断言文案（"GET 应答帧异常" /
//! "EVAL GET 应答帧异常" 各册原样保留）。消费面经 `wedb_test::resp_value` 引用（原 common/ 直挂面
//! 已收口进本 crate）；泵单源见 `wedb_test::resp_pump_scratch`（crate 内
//! 路径引用）。

use std::{str::from_utf8, sync::Arc};

use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::resp_frame;

use crate::resp_pump_scratch::pump;

/// bulk 读值解析（应答偏差即断言文案点名的应答面变化，现场失败）
pub fn parse_bulk(out: Vec<u8>, what: &str) -> Option<Vec<u8>> {
  let text = from_utf8(&out).unwrap();
  if text.starts_with("$-1") {
    return None;
  }
  let (head, body) = text
    .split_once("\r\n")
    .unwrap_or_else(|| panic!("{what} 应答帧异常: {text:?}"));
  let len: usize = head
    .strip_prefix('$')
    .and_then(|n| n.parse().ok())
    .unwrap_or_else(|| panic!("{what} 应答帧异常: {text:?}"));
  let bytes = body
    .as_bytes()
    .get(..len)
    .unwrap_or_else(|| panic!("{what} 应答帧长度异常: {text:?}"));
  Some(bytes.to_vec())
}

/// GET 读值（nil 返 None）
pub fn get_value(consumer: &mut RespSessionConsumer, key: &[u8]) -> Option<Vec<u8>> {
  parse_bulk(pump(consumer, &resp_frame(&[b"GET", key])), "GET")
}

/// 整数应答解析（`:N\r\n` 整数帧与 `$len\r\nN\r\n` 批量帧两形态兼容）
pub fn parse_int_reply(reply: &[u8]) -> i64 {
  let text = from_utf8(reply).unwrap_or_else(|e| panic!("应答不是合法 UTF-8: {e}"));
  if let Some(rest) = text.strip_prefix(':') {
    return rest
      .trim_end()
      .parse::<i64>()
      .unwrap_or_else(|e| panic!("整数应答异常: {text:?} {e}"));
  }
  if let Some(err) = text.strip_prefix('-') {
    panic!("RESP 错误应答: {err}");
  }
  let (head, body) = text
    .split_once("\r\n")
    .unwrap_or_else(|| panic!("非整数或批量应答: {text:?}"));
  let len: usize = head
    .strip_prefix('$')
    .and_then(|n| n.parse().ok())
    .unwrap_or_else(|| panic!("非整数或批量应答: {text:?}"));
  body
    .get(..len)
    .unwrap_or_else(|| panic!("批量应答长度异常: {text:?}"))
    .parse::<i64>()
    .unwrap_or_else(|e| panic!("批量应答非数值: {text:?} {e}"))
}

/// 读侧会话（非集群直连执行域，主从共用；`options` 参数化承接 Lua 启用形）
pub fn store_reader(
  store: &Arc<WedbStore<SegmentedDevice>>,
  id: u64,
  options: RespServerSessionOptions,
) -> RespSessionConsumer {
  RespSessionConsumer::new(
    id,
    options,
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}
