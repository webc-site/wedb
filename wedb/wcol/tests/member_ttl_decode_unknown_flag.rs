#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! decode_member 未知旗标宽松放行直测。
//!
//! 记录首字节形态旗标仅 0/1 合法（0 = 裸载荷、1 = 带 8B 过期刻度）；旗标
//! 非 0/1 仅出现于外部破坏，宽松解码按裸载荷整段放行（与树引擎透传字节流
//! 口径一致，载荷首字节语义由各数据类型自解释）。此前该臂零测试覆盖。

use wcol::types::decode_member;

#[test]
fn decode_member_unknown_flag_passthrough() {
  // 旗标 0x02（非法）：无 TTL、载荷原样整段返回（不剥首字节）
  let raw = [0x02, 0xff, 0xfe, 0xfd];
  let (expiry, payload) = decode_member(&raw);
  assert_eq!(expiry, None);
  assert_eq!(payload, raw.as_slice());

  // 单字节非法旗标：载荷即旗标自身，空切片不越界
  let raw = [0x7f];
  let (expiry, payload) = decode_member(&raw);
  assert_eq!(expiry, None);
  assert_eq!(payload, raw.as_slice());
}
