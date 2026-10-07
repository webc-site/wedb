//! RESP 解析/编码端到端微基准夹具：对标 C# garnet/benchmark/Resp.benchmark（端到端
//! RESP 吞吐）与 BDN.benchmark/Parsing（命令解析、RESP 编码微基准）。wresp 是关键协议
//! 路径，现仓零基准守护，本件补建解析（`parse_resp_frame`）与编码（`RespWriter`）两端。

use itoa::Buffer;
use wresp::{
  frame::parse_resp_frame,
  resp_memory_writer::{Resp2, RespWriter},
};

/// 固定命令体长度（$3），SET 命令名恒为 3 字节
const CMD_NAME: &[u8] = b"SET";

/// 写一段 RESP bulk string（`$<len>\r\n<payload>\r\n`）到字节缓冲
#[inline]
fn push_bulk(buf: &mut Vec<u8>, payload: &[u8]) {
  buf.push(b'$');
  let mut ib = Buffer::new();
  buf.extend_from_slice(ib.format(payload.len()).as_bytes());
  buf.extend_from_slice(b"\r\n");
  buf.extend_from_slice(payload);
  buf.extend_from_slice(b"\r\n");
}

/// 生成第 i 个键/值：末尾嵌入序号数字，保证同长度且互异（validate 可复算）
#[inline]
fn indexed_payload(i: usize, len: usize) -> Vec<u8> {
  let mut s = vec![b'v'; len];
  let mut ib = Buffer::new();
  let num = ib.format(i).as_bytes();
  let take = num.len().min(len);
  s[len - take..].copy_from_slice(&num[num.len() - take..]);
  s
}

/// 构造 n 条流水线 `*3\r\n$3\r\nSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<val>\r\n` 命令缓冲
/// （对标 Resp.benchmark 批量下发、KV.benchmark `--batch-size` 折叠深度）
pub fn build_set_pipeline(n: usize, key_len: usize, val_len: usize) -> Vec<u8> {
  let mut buf = Vec::with_capacity(n * (key_len + val_len + 24));
  for i in 0..n {
    buf.extend_from_slice(b"*3\r\n$3\r\n");
    buf.extend_from_slice(CMD_NAME);
    buf.extend_from_slice(b"\r\n");
    push_bulk(&mut buf, &indexed_payload(i, key_len));
    push_bulk(&mut buf, &indexed_payload(i, val_len));
  }
  buf
}

/// 顺序解析整段流水线，返回 (命令数, 参数总字节数)。后者供 black_box，杜绝解析被优化掉
pub fn parse_pipeline(buf: &[u8]) -> (usize, usize) {
  let mut ptr = buf;
  let mut commands = 0usize;
  let mut arg_bytes = 0usize;
  while !ptr.is_empty() {
    match parse_resp_frame(ptr) {
      Ok(Some((consumed, args))) => {
        for a in &args {
          arg_bytes += a.len();
        }
        ptr = &ptr[consumed..];
        commands += 1;
      }
      _ => break,
    }
  }
  (commands, arg_bytes)
}

/// 编码 n 条 bulk string 应答（GET 值回包），返回编码字节数（复用 writer，clear 不重分配）
pub fn encode_bulks(reps: usize, val_len: usize) -> usize {
  let mut writer: RespWriter<Vec<u8>, Resp2> = RespWriter::with_capacity(reps * (val_len + 8));
  for i in 0..reps {
    writer.write_bulk_string(&indexed_payload(i, val_len));
  }
  writer.into_inner().len()
}

/// 工况预校验：解析端逐命令须还原 `SET` 命令名与三参数、命令数吻合；编码端结构长度须精确
/// （对标 C# RespReadUtils 往返断言口径，杜绝基准测到空解析/错编码）
pub fn validate(n: usize, key_len: usize, val_len: usize) {
  let buf = build_set_pipeline(n, key_len, val_len);
  // 逐帧校验首命令名与参数个数,并核对整段解析命令数
  let mut ptr = &buf[..];
  let mut seen = 0usize;
  while !ptr.is_empty() {
    let (consumed, args) = parse_resp_frame(ptr)
      .expect("RESP 解析不应协议违规")
      .expect("构造的应是完整帧");
    assert_eq!(args.len(), 3, "RESP 解析参数数失真: 第 {seen} 命令");
    assert_eq!(args[0], CMD_NAME, "RESP 解析命令名失真: 第 {seen} 命令");
    assert_eq!(args[1].len(), key_len, "RESP 解析键长失真: 第 {seen} 命令");
    assert_eq!(args[2].len(), val_len, "RESP 解析值长失真: 第 {seen} 命令");
    ptr = &ptr[consumed..];
    seen += 1;
  }
  assert_eq!(seen, n, "RESP 流水线命令数与构造不符");
  // 编码端: 单条 val_len 字节 bulk 应答 = '$' + 十进制长度 + CRLF + payload + CRLF
  let encoded = encode_bulks(1, val_len);
  let digits = (val_len as f64).log10().floor() as usize + 1;
  assert_eq!(
    encoded,
    1 + digits + 2 + val_len + 2,
    "RESP 编码长度失真: val_len={val_len}"
  );
}
