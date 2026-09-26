//! 自研依据: doc/zh/db.md 物理键前缀刚性隔离 [NsVarint]+[DbVarint]+[KeyTag]+[Payload] 编解码
use wval::{Error, KeyTag, NamespaceDbCodec, STACK_KEY_CAP, SessionPrefixBuf, TaggedKeyBuf};

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
  let meta_buf = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::Meta, b"my_hash");
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
  let meta_key =
    NamespaceDbCodec::encode_tagged_key(session_ns, session_db, KeyTag::Meta, b"hash1");
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
    NamespaceDbCodec::strip_session_prefix_with_tag(str_key.as_slice(), &session, KeyTag::String),
    Some(b"my_string".as_slice())
  );
  assert_eq!(
    NamespaceDbCodec::strip_session_prefix_with_tag(str_key.as_slice(), &session, KeyTag::Meta),
    None
  );

  // 2. Meta 键提取
  let meta_key = NamespaceDbCodec::encode_tagged_key(1, 0, KeyTag::Meta, b"my_hash");
  assert_eq!(
    NamespaceDbCodec::strip_session_prefix_with_tag(meta_key.as_slice(), &session, KeyTag::Meta),
    Some(b"my_hash".as_slice())
  );
  assert_eq!(
    NamespaceDbCodec::strip_session_prefix_with_tag(meta_key.as_slice(), &session, KeyTag::String),
    None
  );
}

#[test]
fn test_meta_user_key_and_prefix_len() {
  let ns = 100u64;
  let db = 5u64;

  // 1. 测试 decode_meta_user_key
  let meta_key = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::Meta, b"my_collection");
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
  let meta_buf = NamespaceDbCodec::encode_tagged_key(ns_fast, db_fast, KeyTag::Meta, b"m1");

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
  let meta = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::Meta, b"h1");
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
  let direct = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::Meta, b"h1");
  assert_eq!(meta.as_slice(), direct.as_slice());
}

#[test]
fn test_ttl_codec() {
  use wval::{I64_VAL_LEN, I64Codec};

  // 1. I64Codec 编解码测试
  let ts = 1_700_000_000_123i64;
  let enc = I64Codec::encode(ts);
  assert_eq!(enc.len(), I64_VAL_LEN);
  assert_eq!(I64Codec::decode(&enc), Some(ts));
  assert_eq!(I64Codec::decode(&enc[..7]), None);
}

#[test]
fn test_stack_heap_allocation_boundary_pin() {
  // 62B 零堆分配关键路径回归钉锁定
  // 验证在单字节变长编码下（ns < 128, db < 128，前缀占用 2 字节）栈态与堆态的严格边界
  let ns = 1u64;
  let db = 0u64;
  let prefix = SessionPrefixBuf::new(ns, db);
  assert_eq!(prefix.len(), 2);

  // 1. encode_with_session_prefix / encode_tagged_key 边界回归钉
  // 总长 = prefix(2) + tag(1) + payload:
  // 当 payload.len() == 59 时，总长 = 62 == STACK_KEY_CAP，必须是栈态 (is_stack() == true)
  // 当 payload.len() == 60 时，总长 = 63 > STACK_KEY_CAP，必须回退为堆态 (is_heap() == true)
  let payload_59 = [b'x'; 59];
  let key_stack =
    NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::String, &payload_59);
  assert_eq!(key_stack.len(), STACK_KEY_CAP);
  assert!(key_stack.is_stack());
  assert!(!key_stack.is_heap());

  let key_stack_tagged = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::String, &payload_59);
  assert_eq!(key_stack_tagged.len(), STACK_KEY_CAP);
  assert!(key_stack_tagged.is_stack());
  assert_eq!(key_stack_tagged.as_slice(), key_stack.as_slice());

  let (dec_ns, dec_db, dec_tag, dec_payload) =
    NamespaceDbCodec::decode_tagged_key(key_stack.as_slice()).expect("栈态解码成功");
  assert_eq!(dec_ns, ns);
  assert_eq!(dec_db, db);
  assert_eq!(dec_tag, KeyTag::String);
  assert_eq!(dec_payload, &payload_59[..]);

  let meta_stack = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::Meta, &payload_59);
  assert_eq!(meta_stack.len(), STACK_KEY_CAP);
  assert!(meta_stack.is_stack());

  // 超出 62B (总长 63B 与 100B)，断言为堆态且 roundtrip 一致
  let payload_60 = [b'y'; 60];
  let key_heap_63 =
    NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::String, &payload_60);
  assert_eq!(key_heap_63.len(), 63);
  assert!(key_heap_63.is_heap());
  assert!(!key_heap_63.is_stack());

  let key_tagged_heap_63 = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::String, &payload_60);
  assert_eq!(key_tagged_heap_63.len(), 63);
  assert!(key_tagged_heap_63.is_heap());

  let (dec_ns, dec_db, dec_tag, dec_payload) =
    NamespaceDbCodec::decode_tagged_key(key_heap_63.as_slice()).expect("堆态解码成功");
  assert_eq!(dec_ns, ns);
  assert_eq!(dec_db, db);
  assert_eq!(dec_tag, KeyTag::String);
  assert_eq!(dec_payload, &payload_60[..]);

  let payload_large = [b'z'; 128];
  let key_heap_large = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::Meta, &payload_large);
  assert_eq!(key_heap_large.len(), 2 + 1 + 128);
  assert!(key_heap_large.is_heap());
  let (dec_ns, dec_db, dec_tag, dec_payload) =
    NamespaceDbCodec::decode_tagged_key(key_heap_large.as_slice()).expect("大键堆态解码成功");
  assert_eq!(dec_ns, ns);
  assert_eq!(dec_db, db);
  assert_eq!(dec_tag, KeyTag::Meta);
  assert_eq!(dec_payload, &payload_large[..]);

  // 2. replace_tag_at 边界与栈堆态保持
  // 原键为 62B 栈态
  let replaced_stack = NamespaceDbCodec::replace_tag_at(key_stack.as_slice(), 2, KeyTag::Meta);
  assert_eq!(replaced_stack.len(), STACK_KEY_CAP);
  assert!(replaced_stack.is_stack());
  assert_eq!(replaced_stack.as_slice()[2], KeyTag::Meta.as_u8());
  assert_eq!(&replaced_stack.as_slice()[3..], &payload_59[..]);

  // 原键为 63B 堆态
  let replaced_heap = NamespaceDbCodec::replace_tag_at(key_heap_63.as_slice(), 2, KeyTag::Meta);
  assert_eq!(replaced_heap.len(), 63);
  assert!(replaced_heap.is_heap());
  assert_eq!(replaced_heap.as_slice()[2], KeyTag::Meta.as_u8());
  assert_eq!(&replaced_heap.as_slice()[3..], &payload_60[..]);

  // 3. set_tag_at 原位标签轮换：栈态原位变更不落堆
  let mut mutable_stack_key = key_stack.clone();
  assert!(mutable_stack_key.is_stack());
  mutable_stack_key.set_tag_at(2, KeyTag::Ttl);
  assert!(mutable_stack_key.is_stack(), "原位轮换标签后必须保持栈态");
  assert_eq!(mutable_stack_key.as_slice()[2], KeyTag::Ttl.as_u8());
  assert_eq!(&mutable_stack_key.as_slice()[3..], &payload_59[..]);

  let mut mutable_heap_key = key_heap_63.clone();
  assert!(mutable_heap_key.is_heap());
  mutable_heap_key.set_tag_at(2, KeyTag::Ttl);
  assert!(mutable_heap_key.is_heap());
  assert_eq!(mutable_heap_key.as_slice()[2], KeyTag::Ttl.as_u8());
  assert_eq!(&mutable_heap_key.as_slice()[3..], &payload_60[..]);

  // 4. encode_vector_key_with_prefix 与 decode_tagged_key 向量帧往返
  // 向量键总长 = prefix(2) + tag(1) + context(8) + key
  // 当 key.len() == 51 时，总长 = 11 + 51 = 62 == STACK_KEY_CAP，必须是栈态
  // 当 key.len() == 52 时，总长 = 11 + 52 = 63 > STACK_KEY_CAP，必须是堆态
  let ctx = 0x0102030405060708u64;
  let vec_key_51 = [b'v'; 51];
  let vector_stack =
    NamespaceDbCodec::encode_vector_key_with_prefix(prefix.as_slice(), ctx, &vec_key_51);
  assert_eq!(vector_stack.len(), STACK_KEY_CAP);
  assert!(vector_stack.is_stack());
  let (v_ns, v_db, v_tag, v_payload) =
    NamespaceDbCodec::decode_tagged_key(vector_stack.as_slice()).expect("向量栈态解码成功");
  assert_eq!((v_ns, v_db, v_tag), (ns, db, KeyTag::Vector));
  let (v_ctx, v_k) = v_payload.split_at(8);
  assert_eq!(u64::from_be_bytes(v_ctx.try_into().unwrap()), ctx);
  assert_eq!(v_k, &vec_key_51[..]);

  let vec_key_52 = [b'w'; 52];
  let vector_heap =
    NamespaceDbCodec::encode_vector_key_with_prefix(prefix.as_slice(), ctx, &vec_key_52);
  assert_eq!(vector_heap.len(), 63);
  assert!(vector_heap.is_heap());
  let (v_ns, v_db, v_tag, v_payload) =
    NamespaceDbCodec::decode_tagged_key(vector_heap.as_slice()).expect("向量堆态解码成功");
  assert_eq!((v_ns, v_db, v_tag), (ns, db, KeyTag::Vector));
  let (v_ctx, v_k) = v_payload.split_at(8);
  assert_eq!(u64::from_be_bytes(v_ctx.try_into().unwrap()), ctx);
  assert_eq!(v_k, &vec_key_52[..]);

  // 5. TaggedKeyBuf::from 切片直接构造边界钉与 into_vec 所有权转移
  let slice_stack = TaggedKeyBuf::from(&[b'a'; STACK_KEY_CAP][..]);
  assert_eq!(slice_stack.len(), STACK_KEY_CAP);
  assert!(slice_stack.is_stack());
  assert_eq!(slice_stack.into_vec().len(), STACK_KEY_CAP);

  let slice_heap = TaggedKeyBuf::from(&[b'b'; STACK_KEY_CAP + 1][..]);
  assert_eq!(slice_heap.len(), STACK_KEY_CAP + 1);
  assert!(slice_heap.is_heap());
  assert_eq!(slice_heap.into_vec().len(), STACK_KEY_CAP + 1);

  // 6. 慢路径变长前缀边界钉 (ns=128 为 2B varint, db=0 为 1B varint, 前缀总长 3B)
  // 当 payload.len() == 58 时，总长 = 3 + 1 + 58 = 62 == STACK_KEY_CAP，仍为栈态
  // 当 payload.len() == 59 时，总长 = 3 + 1 + 59 = 63 > STACK_KEY_CAP，回退堆态
  let slow_ns = 128u64;
  let slow_db = 0u64;
  let slow_prefix = SessionPrefixBuf::new(slow_ns, slow_db);
  assert_eq!(slow_prefix.len(), 3);

  let slow_payload_58 = [b's'; 58];
  let slow_key_stack = NamespaceDbCodec::encode_with_session_prefix(
    slow_prefix.as_slice(),
    KeyTag::String,
    &slow_payload_58,
  );
  assert_eq!(slow_key_stack.len(), STACK_KEY_CAP);
  assert!(slow_key_stack.is_stack());

  let slow_payload_59 = [b't'; 59];
  let slow_key_heap = NamespaceDbCodec::encode_with_session_prefix(
    slow_prefix.as_slice(),
    KeyTag::String,
    &slow_payload_59,
  );
  assert_eq!(slow_key_heap.len(), 63);
  assert!(slow_key_heap.is_heap());
}
