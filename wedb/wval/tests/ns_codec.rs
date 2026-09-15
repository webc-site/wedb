use wval::{Error, KeyTag, NamespaceDbCodec, SessionPrefixBuf};

#[test]
fn test_session_prefix_parse_defenses() {
  // 空切片：BufferTooShort（错误面映射自 wbase::varint::decode_u64）
  assert_eq!(
    SessionPrefixBuf::from_slice(&[]),
    Err(Error::BufferTooShort {
      expected: 1,
      actual: 0,
    })
  );

  // 首字节 0x80 声明 2 字节但仅 1 字节：截断防御
  assert_eq!(
    SessionPrefixBuf::from_slice(&[0x80]),
    Err(Error::BufferTooShort {
      expected: 2,
      actual: 1,
    })
  );

  // 非法前缀字节 0xF0..=0xFE：NonCanonical
  for bad_byte in 0xF0..=0xFE {
    assert_eq!(
      SessionPrefixBuf::from_slice(&[bad_byte, 0]),
      Err(Error::NonCanonicalEncoding)
    );
  }

  // 9 字节形式但数值小于 270_549_120：非规范编码拦截
  let mut bad_9b = [0u8; 9];
  bad_9b[0] = 0xFF;
  bad_9b[1..9].copy_from_slice(&100u64.to_be_bytes());
  assert_eq!(
    SessionPrefixBuf::from_slice(&bad_9b),
    Err(Error::NonCanonicalEncoding)
  );

  // decode_tagged_key 同一错误面：截断的 3 字节变长前缀
  assert_eq!(
    NamespaceDbCodec::decode_tagged_key(&[0xC0, 0x01]),
    Err(Error::BufferTooShort {
      expected: 3,
      actual: 2,
    })
  );

  // 慢路径前缀（ns/db 跨单字节域）编码-解析往返
  let prefix = SessionPrefixBuf::new(1000, 2000);
  assert_eq!(prefix.decode(), Ok((1000, 2000)));
  assert_eq!(
    SessionPrefixBuf::from_slice(prefix.as_slice())
      .unwrap()
      .as_slice(),
    prefix.as_slice()
  );
}

#[test]
fn test_plan_a_tagged_key_encoding_and_roundtrip() {
  let ns = 1u64;
  let db = 0u64;
  let user_key = b"user:1001";

  // 1. String Key: KeyTag::String (0x00)
  let string_buf = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::String, user_key);
  // ns=1 (0x01), db=0 (0x00), tag=0x00, user_key
  assert_eq!(
    string_buf.as_slice(),
    &[
      0x01, 0x00, 0x00, b'u', b's', b'e', b'r', b':', b'1', b'0', b'0', b'1'
    ]
  );

  let (dec_ns, dec_db, dec_tag, dec_payload) =
    NamespaceDbCodec::decode_tagged_key(string_buf.as_slice()).expect("解码应成功");
  assert_eq!(dec_ns, ns);
  assert_eq!(dec_db, db);
  assert_eq!(dec_tag, KeyTag::String);
  assert_eq!(dec_payload, user_key);

  // 2. Meta Key: KeyTag::Meta (0x01)
  let meta_buf = NamespaceDbCodec::encode_meta_key(ns, db, b"my_hash");
  assert_eq!(
    meta_buf.as_slice(),
    &[0x01, 0x00, 0x01, b'm', b'y', b'_', b'h', b'a', b's', b'h']
  );

  let (dec_ns, dec_db, dec_tag, dec_payload) =
    NamespaceDbCodec::decode_tagged_key(meta_buf.as_slice()).expect("解码应成功");
  assert_eq!(dec_ns, ns);
  assert_eq!(dec_db, db);
  assert_eq!(dec_tag, KeyTag::Meta);
  assert_eq!(dec_payload, b"my_hash");

  // 3. Ttl 旁路键: KeyTag::Ttl (0x09)
  let ttl_buf = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::Ttl, b"ttl_probe");
  assert_eq!(
    ttl_buf.as_slice(),
    &[
      0x01, 0x00, 0x09, b't', b't', b'l', b'_', b'p', b'r', b'o', b'b', b'e'
    ]
  );
}

#[test]
fn test_session_prefix_and_live_user_key_filtering() {
  let session_ns = 1u64;
  let session_db = 0u64;
  let session_prefix = SessionPrefixBuf::new(session_ns, session_db);
  assert_eq!(session_prefix.as_slice(), &[0x01, 0x00]);

  // 属于当前会话的 String 键
  let str_key = NamespaceDbCodec::encode_tagged_key(session_ns, session_db, KeyTag::String, b"foo");
  let live_str = NamespaceDbCodec::extract_live_user_key(str_key.as_slice(), &session_prefix);
  assert_eq!(live_str, Some((KeyTag::String, b"foo".as_slice())));

  // 属于当前会话的 Meta 键
  let meta_key = NamespaceDbCodec::encode_meta_key(session_ns, session_db, b"hash1");
  let live_meta = NamespaceDbCodec::extract_live_user_key(meta_key.as_slice(), &session_prefix);
  assert_eq!(live_meta, Some((KeyTag::Meta, b"hash1".as_slice())));

  // 属于当前会话的 Ttl 旁路记录键 -> live_user_key 必须过滤掉 (None)
  let ttl_key =
    NamespaceDbCodec::encode_tagged_key(session_ns, session_db, KeyTag::Ttl, b"ttl_val");
  assert_eq!(
    NamespaceDbCodec::extract_live_user_key(ttl_key.as_slice(), &session_prefix),
    None
  );

  // 属于另一个命名空间 (ns=2) 的键 -> 即使在 db 0 也必须被过滤 (None)
  let other_ns_key = NamespaceDbCodec::encode_tagged_key(2, session_db, KeyTag::String, b"foo");
  assert_eq!(
    NamespaceDbCodec::extract_live_user_key(other_ns_key.as_slice(), &session_prefix),
    None
  );

  // 属于同一个命名空间但不同 db (db=1) 的键 -> 必须被过滤 (None)
  let other_db_key = NamespaceDbCodec::encode_tagged_key(session_ns, 1, KeyTag::String, b"foo");
  assert_eq!(
    NamespaceDbCodec::extract_live_user_key(other_db_key.as_slice(), &session_prefix),
    None
  );
}

#[test]
fn test_strip_tag_helpers() {
  let session = SessionPrefixBuf::new(1, 0);

  // 1. String 键提取
  let str_key = NamespaceDbCodec::encode_tagged_key(1, 0, KeyTag::String, b"my_string");
  assert_eq!(
    NamespaceDbCodec::strip_string_key(str_key.as_slice(), &session),
    Some(b"my_string".as_slice())
  );
  assert_eq!(
    NamespaceDbCodec::strip_meta_key(str_key.as_slice(), &session),
    None
  );

  // 2. Meta 键提取
  let meta_key = NamespaceDbCodec::encode_meta_key(1, 0, b"my_hash");
  assert_eq!(
    NamespaceDbCodec::strip_meta_key(meta_key.as_slice(), &session),
    Some(b"my_hash".as_slice())
  );
  assert_eq!(
    NamespaceDbCodec::strip_string_key(meta_key.as_slice(), &session),
    None
  );
}

#[test]
fn test_meta_user_key_and_prefix_len() {
  let ns = 100u64;
  let db = 5u64;

  // 1. 测试 decode_meta_user_key
  let meta_key = NamespaceDbCodec::encode_meta_key(ns, db, b"my_collection");
  assert_eq!(
    NamespaceDbCodec::decode_meta_user_key(meta_key.as_slice()),
    Some(b"my_collection".as_slice())
  );
  assert_eq!(
    NamespaceDbCodec::decode_tag(meta_key.as_slice()),
    Some(KeyTag::Meta)
  );
  // String 键不应被误判为 Meta
  let str_key = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::String, b"my_string");
  assert_eq!(
    NamespaceDbCodec::decode_meta_user_key(str_key.as_slice()),
    None
  );
  assert_eq!(
    NamespaceDbCodec::decode_tag(str_key.as_slice()),
    Some(KeyTag::String)
  );

  // 2. 测试 session_prefix_len_from_slice
  assert_eq!(
    NamespaceDbCodec::session_prefix_len_from_slice(meta_key.as_slice()),
    Some(NamespaceDbCodec::session_prefix_len(ns, db))
  );
  assert_eq!(NamespaceDbCodec::session_prefix_len_from_slice(b""), None);
  assert_eq!(
    NamespaceDbCodec::session_prefix_len_from_slice(&[0x80]), // 2B 头部仅有 1 字节
    None
  );
}

#[test]
fn test_fast_path_and_boundary_decoding() {
  // 1. 常规快速路径 (ns < 128 && db < 128)
  let ns_fast = 0u64;
  let db_fast = 15u64;

  let str_buf = NamespaceDbCodec::encode_tagged_key(ns_fast, db_fast, KeyTag::String, b"k1");
  let meta_buf = NamespaceDbCodec::encode_meta_key(ns_fast, db_fast, b"m1");

  // decode_tagged_key 快路径
  let (ns, db, tag, payload) =
    NamespaceDbCodec::decode_tagged_key(str_buf.as_slice()).expect("decode str");
  assert_eq!(
    (ns, db, tag, payload),
    (ns_fast, db_fast, KeyTag::String, b"k1".as_slice())
  );

  // decode_meta_user_key 快路径与误判拦截
  assert_eq!(
    NamespaceDbCodec::decode_meta_user_key(meta_buf.as_slice()),
    Some(b"m1".as_slice())
  );
  assert_eq!(
    NamespaceDbCodec::decode_meta_user_key(str_buf.as_slice()),
    None
  );

  // 2. 跨阈值大数值慢路径 (ns >= 128 || db >= 128)
  let ns_slow = 1000u64;
  let db_slow = 2000u64;
  let str_slow = NamespaceDbCodec::encode_tagged_key(ns_slow, db_slow, KeyTag::String, b"elem1");

  let (ns, db, tag, payload) =
    NamespaceDbCodec::decode_tagged_key(str_slow.as_slice()).expect("decode str slow");
  assert_eq!(
    (ns, db, tag, payload),
    (ns_slow, db_slow, KeyTag::String, b"elem1".as_slice())
  );
}

#[test]
fn test_with_prefix_encoding_helpers() {
  let ns = 7u64;
  let db = 3u64;

  // Meta 键编码
  let meta = NamespaceDbCodec::encode_meta_key(ns, db, b"h1");
  assert_eq!(meta.as_slice(), &[7, 3, 0x01, b'h', b'1']);

  // 基于预计算前缀构造
  let prefix = SessionPrefixBuf::new(ns, db);
  let payload =
    NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::String, b"payload");
  assert_eq!(
    payload.as_slice(),
    &[7, 3, 0x00, b'p', b'a', b'y', b'l', b'o', b'a', b'd']
  );

  // 通用 tagged 键编码
  let tagged = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::Ttl, &[1, 2]);
  assert_eq!(tagged.as_slice(), &[7, 3, 0x09, 1, 2]);

  // 闭包读取与非闭包编码产物逐字节一致
  let direct = NamespaceDbCodec::encode_meta_key(ns, db, b"h1");
  assert_eq!(meta.as_slice(), direct.as_slice());
}

#[test]
fn test_ttl_codec() {
  use wval::{TTL_VAL_LEN, TtlCodec};

  // 1. TtlCodec 编解码测试
  let ts = 1_700_000_000_123i64;
  let enc = TtlCodec::encode(ts);
  assert_eq!(enc.len(), TTL_VAL_LEN);
  assert_eq!(TtlCodec::decode(&enc), Some(ts));
  assert_eq!(TtlCodec::decode(&enc[..7]), None);
}
