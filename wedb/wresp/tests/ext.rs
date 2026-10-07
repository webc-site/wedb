#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::fmt;

use wresp::{
  cmd_strings::{RESP_ERR_WRONG_TYPE, write_error_raw, write_map_len, write_set_len},
  ext::{
    RESP_FRAME_HEAD_RESERVED, RespSliceExt, RespVecExt, backfill_resp_frame_head,
    reserve_resp_frame_head, resp_frame_head_len, sanitize_error_bytes, sanitize_error_str,
  },
  resp_memory_writer::RespWriter,
};

#[test]
fn try_parse_i64_strict() {
  assert_eq!(b"42".try_parse_i64(), Some(42));
  assert_eq!(b"-7".try_parse_i64(), Some(-7));
  assert_eq!(b"+3".try_parse_i64(), Some(3));
  assert_eq!(b"0".try_parse_i64(), Some(0));
  assert_eq!(b"-0".try_parse_i64(), Some(0));
  assert_eq!(b"9223372036854775807".try_parse_i64(), Some(i64::MAX));
  assert_eq!(b"-9223372036854775808".try_parse_i64(), Some(i64::MIN));
  assert_eq!(b"007".try_parse_i64(), None);
  assert_eq!(b"-007".try_parse_i64(), None);
  assert_eq!(b"abc".try_parse_i64(), None);
  assert_eq!(b"".try_parse_i64(), None);
  assert_eq!(b"1 2".try_parse_i64(), None);
  assert_eq!(b" 1".try_parse_i64(), None);
  assert_eq!(b"5 ".try_parse_i64(), None);
  assert_eq!(b"1x".try_parse_i64(), None);
  assert_eq!(b"9223372036854775808".try_parse_i64(), None);
  assert_eq!(b"-9223372036854775809".try_parse_i64(), None);
}

#[test]
fn vec_ext_formatting() {
  let mut buf = Vec::new();
  buf.write_resp_int(100);
  buf.write_resp_simple_string("OK");
  buf.write_resp_bulk_string(b"foo");
  buf.write_resp_array_len(2);
  buf.write_resp_null_ver(2);
  buf.write_resp_error("something failed");
  assert_eq!(
    buf,
    b":100\r\n+OK\r\n$3\r\nfoo\r\n*2\r\n$-1\r\n-ERR something failed\r\n"
  );

  let mut buf2 = Vec::new();
  buf2.write_resp_error("ERR already has prefix");
  assert_eq!(buf2, b"-ERR already has prefix\r\n");

  let mut buf3 = Vec::new();
  buf3.write_resp_error(RESP_ERR_WRONG_TYPE);
  let mut want = Vec::new();
  write_error_raw(&mut want, RESP_ERR_WRONG_TYPE);
  assert_eq!(buf3, want);
}

#[test]
fn vec_ext_versioned_null_writers() {
  let mut buf = Vec::new();
  buf.write_resp_null_ver(2);
  buf.write_resp_null_ver(3);
  buf.write_resp_null_array_ver(2);
  buf.write_resp_null_array_ver(3);
  assert_eq!(buf, b"$-1\r\n_\r\n*-1\r\n_\r\n");
}

/// 整型批量串出口（itoa 栈上格式化）与旧 `to_string().as_bytes()` 写法逐位等帧：
/// 零/负数/位宽边界全覆，且收敛到 [`RespWriter::write_integer_as_bulk_string`] 单点。
#[test]
fn int_as_bulk_string_is_to_string_byte_equivalent() {
  fn old_frame(v: impl fmt::Display) -> Vec<u8> {
    let s = v.to_string();
    let mut out = Vec::new();
    out.write_resp_bulk_string(s.as_bytes());
    out
  }
  fn new_frame_i64(v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    RespWriter::new_ref(&mut out).write_integer_as_bulk_string(v);
    out
  }

  for v in [0i64, 1, 7, 65_536, i64::MAX, i64::MIN, -7] {
    assert_eq!(new_frame_i64(v), old_frame(v), "i64 {v} 帧型漂移");
  }
  for v in [0i32, -128, i32::MAX, i32::MIN] {
    assert_eq!(
      new_frame_i64(i64::from(v)),
      old_frame(v),
      "i32 {v} 帧型漂移"
    );
  }
  for v in [0u32, 65_536, u32::MAX] {
    assert_eq!(
      new_frame_i64(i64::from(v)),
      old_frame(v),
      "u32 {v} 帧型漂移"
    );
  }
  for v in [0u64, 1_099_511_627_776, u64::MAX] {
    let mut out = Vec::new();
    RespWriter::new_ref(&mut out).write_integer_as_bulk_string(v);
    assert_eq!(out, old_frame(v), "u64 {v} 帧型漂移");
  }

  assert_eq!(new_frame_i64(1_024), b"$4\r\n1024\r\n");
}

/// 错误帧净化单点：字节域核心与 `&str` 门面同源（CRLF 切断 + 长度帽），
/// 门面只是把长度帽回退到 UTF-8 字符边界，不是第二套清洗。
#[test]
fn sanitize_error_bytes_and_str_share_one_mechanism() {
  assert_eq!(
    sanitize_error_bytes(b"boom\r\n:4242", 512),
    b"boom".as_slice()
  );
  assert_eq!(
    sanitize_error_bytes(b"boom\n:4242", 512),
    b"boom".as_slice()
  );
  assert_eq!(sanitize_error_bytes(b"plain", 512), b"plain".as_slice());
  assert_eq!(sanitize_error_bytes(b"abcdef", 3), b"abc".as_slice());
  assert_eq!(
    sanitize_error_bytes("é中".as_bytes(), 3),
    [0xC3u8, 0xA9, 0xE4].as_slice()
  );

  assert_eq!(sanitize_error_str("boom\r\n:4242", 512), "boom");
  assert_eq!(sanitize_error_str("abcdef", 3), "abc");
  assert_eq!(sanitize_error_str("ééé", 5), "éé");
}

fn frame_head_body(items: &[&[u8]]) -> Vec<u8> {
  let mut buf = Vec::new();
  for item in items {
    buf.write_resp_bulk_string(item);
  }
  buf
}

fn assert_backfill_matches_direct(
  label: &str,
  count: usize,
  items: &[&[u8]],
  write_head: impl Fn(&mut Vec<u8>, usize) + Copy,
) {
  let mut want = Vec::new();
  write_head(&mut want, count);
  want.extend_from_slice(&frame_head_body(items));

  for hint in [count, count + 1_000_000, count / 2] {
    let mut out = Vec::new();
    let reserved = resp_frame_head_len(hint, write_head);
    let base = reserve_resp_frame_head(&mut out, reserved);
    out.extend_from_slice(&frame_head_body(items));
    backfill_resp_frame_head(&mut out, base, reserved, count, write_head);
    assert_eq!(out, want, "{label} 计数 {count} 预留按 {hint} 估算");
  }
}

#[test]
fn frame_head_backfill_is_byte_identical_across_width_boundaries() {
  for count in [0usize, 1, 8, 9, 10, 98, 99, 100, 101, 999, 1000] {
    let items: Vec<Vec<u8>> = (0..count.max(1))
      .map(|i| i.to_string().into_bytes())
      .collect();
    let refs: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
    let body_refs = if count == 0 { &[][..] } else { &refs[..count] };
    for ver in [2u8, 3] {
      assert_backfill_matches_direct("array", count, body_refs, |buf, n| {
        buf.write_resp_array_len(n)
      });
      assert_backfill_matches_direct("set", count, body_refs, |buf, n| write_set_len(buf, n, ver));
      assert_backfill_matches_direct("map", count, body_refs, |buf, n| write_map_len(buf, n, ver));
    }
  }
}

#[test]
fn frame_head_backfill_preserves_prefix_and_moves_only_body() {
  let mut out = b"*2\r\n".to_vec();
  let reserved = resp_frame_head_len(30, |buf, n| buf.write_resp_array_len(n));
  let base = reserve_resp_frame_head(&mut out, reserved);
  out.extend_from_slice(&frame_head_body(&[b"a", b"b", b"c"]));
  backfill_resp_frame_head(&mut out, base, reserved, 3, |buf, n| {
    buf.write_resp_array_len(n)
  });
  assert_eq!(out, b"*2\r\n*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
}

#[test]
fn frame_head_reserved_width_follows_head_form() {
  let resp3 = resp_frame_head_len(50, |buf, n| write_map_len(buf, n, 3));
  let resp2 = resp_frame_head_len(50, |buf, n| write_map_len(buf, n, 2));
  assert_eq!(resp3, 5);
  assert_eq!(resp2, 6);
  assert_eq!(
    resp_frame_head_len(99, |buf, n| buf.write_resp_array_len(n)),
    RESP_FRAME_HEAD_RESERVED
  );
  assert_eq!(
    resp_frame_head_len(100, |buf, n| buf.write_resp_array_len(n)),
    RESP_FRAME_HEAD_RESERVED + 1
  );
}

#[test]
fn frame_head_truncate_rolls_back_reserved_head() {
  let mut out = Vec::new();
  let reserved = resp_frame_head_len(9, |buf, n| buf.write_resp_array_len(n));
  let base = reserve_resp_frame_head(&mut out, reserved);
  out.extend_from_slice(&frame_head_body(&[b"partial"]));
  out.truncate(base);
  assert!(out.is_empty(), "撤帧后不得残留预留头或部分实体");
}
