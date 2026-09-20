//! 分层树成员记录 TTL 编解码（唯一 codec，供升阶导出 / 物化还原 / 树内读写臂共用）
//!
//! 分层态（wbftree）成员记录在线格式 `值 + 可选 8B 过期刻度` 上对标 C#
//! 对象序列化位掩码思路（libs/server/Objects/Hash/HashObject.cs:DoSerialize 与
//! SortedSetObject.cs 构造函数的 ExpirationBitMask——键长高位表带 TTL，其后附
//! 8B UTC ticks，装载时过滤已过期）：记录首字节为形态旗标，`0` = 裸载荷，
//! `1` = 后随 8B 大端 .NET Ticks 过期刻度再接载荷；仅 Hash / SortedSet 承载
//! 字段级 TTL（C# 同），Set / List 记录恒为裸载荷。解码借用切片零拷贝，
//! 已过期成员由各消费面按 C# 口径剔除（升阶导出防续命、物化还原防复活）。

/// 裸载荷记录旗标
const FLAG_PLAIN: u8 = 0;
/// 带过期刻度记录旗标（后随 8B 大端 ticks）
const FLAG_WITH_EXPIRY: u8 = 1;
/// 带过期刻度记录的头部长度（1B 旗标 + 8B ticks）
const EXPIRY_HEADER_LEN: usize = 9;

/// 解码分层树成员记录为 `(过期刻度, 载荷)` 借用视图
///
/// `Some(ticks)` = 成员挂字段 TTL（未判到期）；`None` = 无 TTL。
/// 未知旗标按裸载荷放行（载荷首字节语义由各数据类型自解释，旗标非 0/1
/// 仅出现于外部破坏，宽松解码与树引擎透传字节流口径一致）
#[inline]
pub fn decode_member(raw: &[u8]) -> (Option<i64>, &[u8]) {
  match raw.first() {
    Some(&FLAG_WITH_EXPIRY) if raw.len() >= EXPIRY_HEADER_LEN => {
      let ticks = i64::from_be_bytes(raw[1..9].try_into().unwrap());
      (Some(ticks), &raw[EXPIRY_HEADER_LEN..])
    }
    Some(&FLAG_PLAIN) => (None, &raw[1..]),
    _ => (None, raw),
  }
}

/// 编码分层树成员记录进目标缓冲（`expiry = None` 落裸载荷形态）
#[inline]
pub fn encode_member_into(payload: &[u8], expiry: Option<i64>, out: &mut Vec<u8>) {
  match expiry {
    None => {
      out.reserve(payload.len() + 1);
      out.push(FLAG_PLAIN);
      out.extend_from_slice(payload);
    }
    Some(ticks) => {
      out.reserve(payload.len() + EXPIRY_HEADER_LEN);
      out.push(FLAG_WITH_EXPIRY);
      out.extend_from_slice(&ticks.to_be_bytes());
      out.extend_from_slice(payload);
    }
  }
}

/// 编码后记录的线上长度（与 [`encode_member_into`] 输出单点同源）：
/// 供写路径容量预留与长度契约预校验换算，禁止各消费面另抄 1B/9B 头长
#[inline]
pub const fn encoded_len(payload_len: usize, expiry: Option<i64>) -> usize {
  payload_len
    + if expiry.is_some() {
      EXPIRY_HEADER_LEN
    } else {
      1
    }
}

/// 编码分层树成员记录为独立缓冲
#[inline]
pub fn encode_member(payload: &[u8], expiry: Option<i64>) -> Vec<u8> {
  let mut out = Vec::with_capacity(encoded_len(payload.len(), expiry));
  encode_member_into(payload, expiry, &mut out);
  out
}

/// 成员在给定时刻是否已到期（仅挂 TTL 记录可到期）
#[inline]
pub fn member_expired_at(raw: &[u8], now: i64) -> bool {
  let (expiry, _) = decode_member(raw);
  expiry.is_some_and(|ticks| ticks < now)
}
