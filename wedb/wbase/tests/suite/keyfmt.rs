use wbase::keyfmt::{LOG_KEY_MAX, log_key};

#[test]
fn short_key_borrows_verbatim() {
  assert_eq!(log_key(b"plain-key"), "plain-key");
  assert_eq!(log_key(&[0xFF, 0xFE]), "\u{FFFD}\u{FFFD}");
}

#[test]
fn long_key_truncated_with_length_hint() {
  let key = vec![b'a'; 4096];
  assert_eq!(log_key(&key), format!("{}…(len=4096)", "a".repeat(64)));
  assert!(log_key(&key).len() < 96, "键段长度恒有界");
}

#[test]
fn truncation_lands_on_char_boundary() {
  // 40 个三字节汉字 = 120 字节，64 落在字符中段须回退到边界
  let key = "汉".repeat(40);
  let view = log_key(key.as_bytes());
  assert_eq!(view, format!("{}…(len=120)", "汉".repeat(21)));
  assert_eq!(LOG_KEY_MAX, 64);
}

#[test]
fn degenerate_continuation_stream_no_underflow() {
  let key = vec![0x80u8; 100];
  assert_eq!(log_key(&key), "…(len=100)");
}
