use std::{
  cmp::Ordering,
  mem::{align_of, size_of},
};

use wval::{Error, KeyBufRepr, KeyTag, NamespaceDbCodec, SessionPrefixBuf, TaggedKeyBuf};

#[test]
fn test_tagged_key_buf_memory_layout() {
  // 核心架构验证：TaggedKeyBuf 必须严格对齐 L1 Cache Line (64 Bytes)
  assert_eq!(align_of::<TaggedKeyBuf>(), 64);
  assert_eq!(size_of::<TaggedKeyBuf>(), 64);

  // SessionPrefixBuf 紧凑性验证
  assert_eq!(size_of::<SessionPrefixBuf>(), 19);
}

#[test]
fn test_oppv_varint_roundtrip_and_boundaries() {
  let test_values = [
    0u64,
    1,
    64,
    127,
    128,
    129,
    1000,
    16_511,
    16_512,
    16_513,
    500_000,
    2_113_663,
    2_113_664,
    2_113_665,
    100_000_000,
    270_549_119,
    270_549_120,
    270_549_121,
    1_000_000_000_000,
    u64::MAX - 1,
    u64::MAX,
  ];

  let mut buf = [0u8; 16];
  for &val in &test_values {
    let expected_len = NamespaceDbCodec::varint_len(val);
    let written = NamespaceDbCodec::encode_varint(val, &mut buf);
    assert_eq!(written, expected_len, "varint_len 与 encode 写入长度不一致");

    let (decoded, consumed) =
      NamespaceDbCodec::decode_varint(&buf[..written]).expect("正常编码应成功解码");
    assert_eq!(decoded, val, "解码值与原值不符: val={val}");
    assert_eq!(consumed, expected_len, "消耗长度与编码长度不符");
  }
}

#[test]
fn test_oppv_monotonic_order_preserving() {
  // 严格大端字典序保序测试：对于任意 a < b，恒有 bytes(a) < bytes(b)
  let ordered_values = [
    0u64,
    1,
    127,
    128,
    129,
    16_511,
    16_512,
    2_113_663,
    2_113_664,
    270_549_119,
    270_549_120,
    1_000_000_000,
    u64::MAX,
  ];

  let mut encoded_list = Vec::new();
  for &val in &ordered_values {
    let mut buf = [0u8; 9];
    let len = NamespaceDbCodec::encode_varint(val, &mut buf);
    encoded_list.push(buf[..len].to_vec());
  }

  for i in 0..encoded_list.len() - 1 {
    let prev = &encoded_list[i];
    let next = &encoded_list[i + 1];
    assert!(
      prev < next,
      "字典序保序违背: values[{}]: {:?} 应该小于 values[{}]: {:?}",
      i,
      prev,
      i + 1,
      next
    );
  }
}

#[test]
fn test_oppv_non_canonical_and_corrupted_defenses() {
  // 1. 9 字节编码若解出值小于 270_549_120，应拦截并返回 NonCanonicalEncoding
  let mut bad_9b = [0u8; 9];
  bad_9b[0] = 0xFF;
  bad_9b[1..9].copy_from_slice(&100u64.to_be_bytes());
  let err = NamespaceDbCodec::decode_varint(&bad_9b);
  assert_eq!(err, Err(Error::NonCanonicalEncoding));

  // 2. 非法前缀字节 0xF0..=0xFE 应拒绝
  for bad_byte in 0xF0..=0xFE {
    let slice = [bad_byte, 0, 0, 0];
    assert_eq!(
      NamespaceDbCodec::decode_varint(&slice),
      Err(Error::NonCanonicalEncoding)
    );
  }

  // 3. 截断数据防御
  assert_eq!(
    NamespaceDbCodec::decode_varint(&[]),
    Err(Error::BufferTooShort {
      expected: 1,
      actual: 0
    })
  );

  let mut buf_2b = [0u8; 2];
  NamespaceDbCodec::encode_varint(1000, &mut buf_2b);
  assert_eq!(
    NamespaceDbCodec::decode_varint(&buf_2b[..1]),
    Err(Error::BufferTooShort {
      expected: 2,
      actual: 1
    })
  );
}

#[test]
fn test_plan_a_tagged_key_encoding_and_roundtrip() {
  let ns = 1u64;
  let db = 0u64;
  let user_key = b"user:1001";

  // 1. String Key: KeyTag::String (0x00)
  let string_buf = NamespaceDbCodec::encode_string_key(ns, db, user_key);
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

  // 3. Subkey: KeyTag::Hash (0x02)
  let subkey_payload = b"subkey_test";
  let subkey_buf = NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::Hash, subkey_payload);
  assert_eq!(
    subkey_buf.as_slice(),
    &[
      0x01, 0x00, 0x02, b's', b'u', b'b', b'k', b'e', b'y', b'_', b't', b'e', b's', b't'
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
  let str_key = NamespaceDbCodec::encode_string_key(session_ns, session_db, b"foo");
  let live_str = NamespaceDbCodec::extract_live_user_key(str_key.as_slice(), &session_prefix);
  assert_eq!(live_str, Some((KeyTag::String, b"foo".as_slice())));

  // 属于当前会话的 Meta 键
  let meta_key = NamespaceDbCodec::encode_meta_key(session_ns, session_db, b"hash1");
  let live_meta = NamespaceDbCodec::extract_live_user_key(meta_key.as_slice(), &session_prefix);
  assert_eq!(live_meta, Some((KeyTag::Meta, b"hash1".as_slice())));

  // 属于当前会话的 HashField 子键 -> live_user_key 必须过滤掉 (None)
  let hash_sub =
    NamespaceDbCodec::encode_tagged_key(session_ns, session_db, KeyTag::Hash, b"field_val");
  assert_eq!(
    NamespaceDbCodec::extract_live_user_key(hash_sub.as_slice(), &session_prefix),
    None
  );

  // 属于另一个命名空间 (ns=2) 的键 -> 即使在 db 0 也必须被过滤 (None)
  let other_ns_key = NamespaceDbCodec::encode_string_key(2, session_db, b"foo");
  assert_eq!(
    NamespaceDbCodec::extract_live_user_key(other_ns_key.as_slice(), &session_prefix),
    None
  );

  // 属于同一个命名空间但不同 db (db=1) 的键 -> 必须被过滤 (None)
  let other_db_key = NamespaceDbCodec::encode_string_key(session_ns, 1, b"foo");
  assert_eq!(
    NamespaceDbCodec::extract_live_user_key(other_db_key.as_slice(), &session_prefix),
    None
  );
}

#[test]
fn test_stack_and_heap_switching() {
  let ns = 1u64;
  let db = 0u64;

  // 1. 短键 (总长 <= 62 字节走 Stack)
  let short_key = vec![b'a'; 50];
  let buf_stack = NamespaceDbCodec::encode_string_key(ns, db, &short_key);
  assert!(matches!(buf_stack.inner, KeyBufRepr::Stack(..)));
  assert_eq!(buf_stack.len(), 1 + 1 + 1 + 50);

  // 2. 长键 (总长 > 62 字节走 Heap)
  let long_key = vec![b'b'; 128];
  let buf_heap = NamespaceDbCodec::encode_string_key(ns, db, &long_key);
  assert!(matches!(buf_heap.inner, KeyBufRepr::Heap(..)));
  assert_eq!(buf_heap.len(), 1 + 1 + 1 + 128);

  // 3. with_tagged_key 闭包验证
  let mut executed = false;
  NamespaceDbCodec::with_string_key(ns, db, b"hello", |slice| {
    assert_eq!(slice, &[0x01, 0x00, 0x00, b'h', b'e', b'l', b'l', b'o']);
    executed = true;
  });
  assert!(executed);

  // 4. encode_to_slice 验证
  let mut out = [0u8; 32];
  let written =
    NamespaceDbCodec::encode_to_slice(ns, db, KeyTag::String, b"hi", &mut out).expect("写入应成功");
  assert_eq!(written, 5);
  assert_eq!(&out[..5], &[0x01, 0x00, 0x00, b'h', b'i']);

  // 空间不足报错
  let err = NamespaceDbCodec::encode_to_slice(ns, db, KeyTag::String, b"hi", &mut out[..4]);
  assert_eq!(
    err,
    Err(Error::BufferTooShort {
      expected: 5,
      actual: 4
    })
  );
}

#[test]
fn test_const_fn_capabilities() {
  // 编译期静态构造常量会话前缀
  const CONST_SESSION_ZERO: SessionPrefixBuf = SessionPrefixBuf::new(0, 0);
  const CONST_SESSION_CUSTOM: SessionPrefixBuf = SessionPrefixBuf::new(100, 200);

  assert_eq!(CONST_SESSION_ZERO.as_slice(), &[0x00, 0x00]);
  assert_eq!(CONST_SESSION_ZERO.len(), 2);
  assert!(!CONST_SESSION_ZERO.is_empty());

  // 编译期静态计算变长长度与数组
  const VARINT_LEN_1: usize = NamespaceDbCodec::varint_len(50);
  const VARINT_LEN_2: usize = NamespaceDbCodec::varint_len(1000);
  const KEY_LEN: usize = NamespaceDbCodec::key_len(1, 1, 10);
  assert_eq!(VARINT_LEN_1, 1);
  assert_eq!(VARINT_LEN_2, 2);
  assert_eq!(KEY_LEN, 1 + 1 + 1 + 10);

  const ARR: ([u8; 9], usize) = NamespaceDbCodec::encode_varint_to_array(128);
  assert_eq!(ARR.1, 2);
  assert_eq!(&ARR.0[..2], &[0x80, 0x00]);

  // SessionPrefixBuf::decode
  let (dec_ns, dec_db) = CONST_SESSION_CUSTOM.decode().expect("解码应成功");
  assert_eq!(dec_ns, 100);
  assert_eq!(dec_db, 200);

  // SessionPrefixBuf::from_slice
  let from_slice_buf = SessionPrefixBuf::from_slice(&[0x01, 0x00]).expect("合法前缀应解析成功");
  assert_eq!(from_slice_buf.as_slice(), &[0x01, 0x00]);

  // 非法前缀拦截
  assert!(SessionPrefixBuf::from_slice(&[]).is_err());
  assert!(SessionPrefixBuf::from_slice(&[0xFF, 0x01]).is_err());
}

#[test]
fn test_tagged_key_buf_cross_variant_equality_and_traits() {
  use whasher::HashSet;

  let ns = 1u64;
  let db = 0u64;
  let payload = b"test_equality_key";

  // 1. 同一键通过 Stack 构造与通过 Heap 构造必须强相等 (PartialEq/Eq 基于内容切片)
  let mut stack_buf = [0u8; 62];
  let full_slice = NamespaceDbCodec::encode_string_key(ns, db, payload);
  stack_buf[..full_slice.len()].copy_from_slice(full_slice.as_slice());
  // 在 unused 后缀故意填充脏数据，验证 PartialEq 仅比对有效长度
  stack_buf[full_slice.len()..].fill(0xAA);

  let key_stack = TaggedKeyBuf::from_stack(stack_buf, full_slice.len() as u8);
  let key_heap = TaggedKeyBuf::from_heap(full_slice.as_slice().to_vec());

  assert_eq!(
    key_stack, key_heap,
    "Stack 形式与 Heap 形式相同内容的键必须判定为相等"
  );
  assert_eq!(key_stack, full_slice.as_slice());
  assert_eq!(key_heap, full_slice.as_slice());

  // 2. Hash 一致性与 HashSet 查重
  let mut set = HashSet::default();
  set.insert(key_stack.clone());
  assert!(
    set.contains(&key_heap),
    "HashSet 必须能用等价的 Heap 键查找到 Stack 键"
  );

  // 3. Ord / PartialOrd
  assert_eq!(key_stack.cmp(&key_heap), Ordering::Equal);

  // 4. From 转换
  let from_vec: TaggedKeyBuf = full_slice.as_slice().to_vec().into();
  assert!(from_vec.is_stack());
  assert_eq!(from_vec, key_stack);

  let from_slice: TaggedKeyBuf = full_slice.as_slice().into();
  assert!(from_slice.is_stack());
  assert_eq!(from_slice, key_stack);

  let into_vec: Vec<u8> = from_slice.into();
  assert_eq!(into_vec.as_slice(), full_slice.as_slice());

  // 5. Default 验证
  let default_buf = TaggedKeyBuf::default();
  assert!(default_buf.is_empty());
  assert_eq!(default_buf.len(), 0);
}

#[test]
fn test_strip_tag_helpers() {
  let session = SessionPrefixBuf::new(1, 0);

  // 1. String 键提取
  let str_key = NamespaceDbCodec::encode_string_key(1, 0, b"my_string");
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
fn test_sub_key_and_chunk_key_codecs() {
  let ns = 100u64;
  let db = 5u64;
  let key_id = 9876543210u64;
  let version = 42u64;
  let field = b"user_email_address";

  // 1. 测试打平子键编码与解码
  let sub_key = NamespaceDbCodec::encode_sub_key(ns, db, KeyTag::Hash, key_id, version, field);
  assert!(sub_key.is_stack(), "常规子键必须优先栈分配");

  // 单指令/零回溯提取 tag
  assert_eq!(
    NamespaceDbCodec::decode_tag(sub_key.as_slice()),
    Some(KeyTag::Hash)
  );
  assert_eq!(
    NamespaceDbCodec::decode_meta_user_key(sub_key.as_slice()),
    None,
    "子键不可被识别为 Meta 用户键"
  );

  let (dec_ns, dec_db, dec_tag, dec_id, dec_ver, dec_field) =
    NamespaceDbCodec::decode_sub_key(sub_key.as_slice()).expect("方案 A 子键解码应成功");
  assert_eq!(dec_ns, ns);
  assert_eq!(dec_db, db);
  assert_eq!(dec_tag, KeyTag::Hash);
  assert_eq!(dec_id, key_id);
  assert_eq!(dec_ver, version);
  assert_eq!(dec_field, field);

  // 2. 测试分块子键编码与解码 (HashChunk & SetChunk)
  let chunk_id = 1024u32;
  let chunk_key =
    NamespaceDbCodec::encode_chunk_key(ns, db, KeyTag::HashChunk, key_id, version, chunk_id);
  assert!(chunk_key.is_stack());
  assert_eq!(
    NamespaceDbCodec::decode_tag(chunk_key.as_slice()),
    Some(KeyTag::HashChunk)
  );

  let (c_ns, c_db, c_tag, c_id, c_ver, c_chunk_id) =
    NamespaceDbCodec::decode_chunk_key(chunk_key.as_slice()).expect("方案 A 分块子键解码应成功");
  assert_eq!(c_ns, ns);
  assert_eq!(c_db, db);
  assert_eq!(c_tag, KeyTag::HashChunk);
  assert_eq!(c_id, key_id);
  assert_eq!(c_ver, version);
  assert_eq!(c_chunk_id, chunk_id);

  // 3. 测试 decode_meta_user_key
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
  let str_key = NamespaceDbCodec::encode_string_key(ns, db, b"my_string");
  assert_eq!(
    NamespaceDbCodec::decode_meta_user_key(str_key.as_slice()),
    None
  );
  assert_eq!(
    NamespaceDbCodec::decode_tag(str_key.as_slice()),
    Some(KeyTag::String)
  );

  // 4. 测试 decode_subkey_id_version 极速子键判定
  let sub_meta = NamespaceDbCodec::decode_subkey_id_version(sub_key.as_slice());
  assert_eq!(sub_meta, Some((KeyTag::Hash, key_id, version)));
  // 非 subkey 标签必须返回 None
  assert_eq!(
    NamespaceDbCodec::decode_subkey_id_version(meta_key.as_slice()),
    None
  );
  assert_eq!(
    NamespaceDbCodec::decode_subkey_id_version(str_key.as_slice()),
    None
  );

  // 5. 测试 session_prefix_len_from_slice
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
  use wval::{MIN_CHUNK_KEY_LEN, MIN_SUBKEY_LEN};

  assert_eq!(MIN_SUBKEY_LEN, 19);
  assert_eq!(MIN_CHUNK_KEY_LEN, 23);

  // 1. 常规快速路径 (ns < 128 && db < 128)
  let ns_fast = 0u64;
  let db_fast = 15u64;
  let key_id = 0x1122334455667788u64;
  let version = 0xAABBCCDDEEFF0011u64;
  let field = b"fast_field";

  let str_buf = NamespaceDbCodec::encode_string_key(ns_fast, db_fast, b"k1");
  let meta_buf = NamespaceDbCodec::encode_meta_key(ns_fast, db_fast, b"m1");
  let sub_buf =
    NamespaceDbCodec::encode_sub_key(ns_fast, db_fast, KeyTag::Hash, key_id, version, field);
  let chunk_buf =
    NamespaceDbCodec::encode_chunk_key(ns_fast, db_fast, KeyTag::HashChunk, key_id, version, 42);

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
  assert_eq!(
    NamespaceDbCodec::decode_meta_user_key(sub_buf.as_slice()),
    None
  );

  // decode_subkey_id_version 快路径与长度截断门限
  assert_eq!(
    NamespaceDbCodec::decode_subkey_id_version(sub_buf.as_slice()),
    Some((KeyTag::Hash, key_id, version))
  );
  assert_eq!(
    NamespaceDbCodec::decode_subkey_id_version(chunk_buf.as_slice()),
    Some((KeyTag::HashChunk, key_id, version))
  );
  assert_eq!(
    NamespaceDbCodec::decode_subkey_id_version(str_buf.as_slice()),
    None
  );
  assert_eq!(
    NamespaceDbCodec::decode_subkey_id_version(meta_buf.as_slice()),
    None
  );
  // 短于 19 字节的键，直接拦截返回 None
  for len in 0..19 {
    if len <= sub_buf.len() {
      assert_eq!(
        NamespaceDbCodec::decode_subkey_id_version(&sub_buf[..len]),
        None
      );
    }
  }

  // decode_sub_key 快路径
  let (ns, db, tag, k_id, ver, sub_p) =
    NamespaceDbCodec::decode_sub_key(sub_buf.as_slice()).expect("decode sub_key");
  assert_eq!(
    (ns, db, tag, k_id, ver, sub_p),
    (
      ns_fast,
      db_fast,
      KeyTag::Hash,
      key_id,
      version,
      field.as_slice()
    )
  );

  // decode_chunk_key 快路径
  let (ns, db, tag, k_id, ver, chunk_id) =
    NamespaceDbCodec::decode_chunk_key(chunk_buf.as_slice()).expect("decode chunk_key");
  assert_eq!(
    (ns, db, tag, k_id, ver, chunk_id),
    (ns_fast, db_fast, KeyTag::HashChunk, key_id, version, 42)
  );

  // 2. 跨阈值大数值慢路径 (ns >= 128 || db >= 128)
  let ns_slow = 1000u64;
  let db_slow = 2000u64;
  let sub_slow =
    NamespaceDbCodec::encode_sub_key(ns_slow, db_slow, KeyTag::Set, key_id, version, b"elem1");
  let chunk_slow =
    NamespaceDbCodec::encode_chunk_key(ns_slow, db_slow, KeyTag::SetChunk, key_id, version, 999);

  assert_eq!(
    NamespaceDbCodec::decode_subkey_id_version(sub_slow.as_slice()),
    Some((KeyTag::Set, key_id, version))
  );
  let (ns, db, tag, k_id, ver, sub_p) =
    NamespaceDbCodec::decode_sub_key(sub_slow.as_slice()).expect("decode sub_key slow");
  assert_eq!(
    (ns, db, tag, k_id, ver, sub_p),
    (
      ns_slow,
      db_slow,
      KeyTag::Set,
      key_id,
      version,
      b"elem1".as_slice()
    )
  );

  let (ns, db, tag, k_id, ver, chunk_id) =
    NamespaceDbCodec::decode_chunk_key(chunk_slow.as_slice()).expect("decode chunk_key slow");
  assert_eq!(
    (ns, db, tag, k_id, ver, chunk_id),
    (ns_slow, db_slow, KeyTag::SetChunk, key_id, version, 999)
  );
}

#[test]
fn test_with_prefix_closure_helpers() {
  let ns = 7u64;
  let db = 3u64;

  // with_meta_key 闭包零堆分配路径
  let mut meta_ran = false;
  NamespaceDbCodec::with_meta_key(ns, db, b"h1", |k| {
    assert_eq!(k, &[7, 3, 0x01, b'h', b'1']);
    meta_ran = true;
  });
  assert!(meta_ran);

  // with_session_prefix 基于预计算前缀构造
  let prefix = SessionPrefixBuf::new(ns, db);
  let mut pref_ran = false;
  NamespaceDbCodec::with_session_prefix(prefix.as_slice(), KeyTag::String, b"payload", |k| {
    assert_eq!(k, &[7, 3, 0x00, b'p', b'a', b'y', b'l', b'o', b'a', b'd']);
    pref_ran = true;
  });
  assert!(pref_ran);

  // with_tagged_key 通用闭包
  let mut tagged_ran = false;
  NamespaceDbCodec::with_tagged_key(ns, db, KeyTag::Hash, &[1, 2], |k| {
    assert_eq!(k, &[7, 3, 0x02, 1, 2]);
    tagged_ran = true;
  });
  assert!(tagged_ran);

  // 与非闭包编码产物逐字节一致
  let direct = NamespaceDbCodec::encode_meta_key(ns, db, b"h1");
  NamespaceDbCodec::with_meta_key(ns, db, b"h1", |k| assert_eq!(k, direct.as_slice()));
}

#[test]
fn test_varint_len_lut_and_fast_dispatch() {
  use wval::VARINT_LEN_LUT;

  for b in 0u8..=255 {
    let fast_len = NamespaceDbCodec::varint_len_fast(b);
    let opt_len = NamespaceDbCodec::varint_len_from_byte(b);
    assert_eq!(VARINT_LEN_LUT[b as usize] as usize, fast_len);
    if fast_len == 0 {
      assert_eq!(opt_len, None);
    } else {
      assert_eq!(opt_len, Some(fast_len));
    }
    match b {
      0..=0x7F => assert_eq!(fast_len, 1),
      0x80..=0xBF => assert_eq!(fast_len, 2),
      0xC0..=0xDF => assert_eq!(fast_len, 3),
      0xE0..=0xEF => assert_eq!(fast_len, 4),
      0xFF => assert_eq!(fast_len, 9),
      0xF0..=0xFE => assert_eq!(fast_len, 0),
    }
  }
}

#[test]
fn test_replace_tag_and_ttl_codec() {
  use wval::{KeyTag, NamespaceDbCodec, TTL_VAL_LEN, TtlCodec};

  // 1. TtlCodec 编解码测试
  let ts = 1_700_000_000_123u64;
  let enc = TtlCodec::encode(ts);
  assert_eq!(enc.len(), TTL_VAL_LEN);
  assert_eq!(TtlCodec::decode(&enc), Some(ts));
  assert_eq!(TtlCodec::decode(&enc[..7]), None);

  // 2. NamespaceDbCodec::replace_tag 与 replace_tag_at 测试
  let str_key = NamespaceDbCodec::encode_tagged_key(1, 2, KeyTag::String, b"my_key");
  let ttl_key = NamespaceDbCodec::replace_tag(&str_key, KeyTag::Ttl).unwrap();
  let (_ns, _db, tag, payload) = NamespaceDbCodec::decode_tagged_key(&ttl_key).unwrap();
  assert_eq!(tag, KeyTag::Ttl);
  assert_eq!(payload, b"my_key");

  // replace_tag_at 直接调用验证
  let meta_key = NamespaceDbCodec::replace_tag_at(&str_key, 2, KeyTag::Meta);
  let (_, _, meta_tag, meta_p) = NamespaceDbCodec::decode_tagged_key(&meta_key).unwrap();
  assert_eq!(meta_tag, KeyTag::Meta);
  assert_eq!(meta_p, b"my_key");

  // 3. 多字节 varint 会话前缀替换验证（慢路径）
  let multi_varint_key =
    NamespaceDbCodec::encode_tagged_key(300, 500, KeyTag::String, b"big_session");
  let multi_ttl_key = NamespaceDbCodec::replace_tag(&multi_varint_key, KeyTag::Ttl).unwrap();
  let (ns_m, db_m, tag_m, p_m) = NamespaceDbCodec::decode_tagged_key(&multi_ttl_key).unwrap();
  assert_eq!(ns_m, 300);
  assert_eq!(db_m, 500);
  assert_eq!(tag_m, KeyTag::Ttl);
  assert_eq!(p_m, b"big_session");

  // 4. 长键堆分配分支验证
  let long_payload = vec![b'x'; 100];
  let long_str_key = NamespaceDbCodec::encode_tagged_key(1, 2, KeyTag::String, &long_payload);
  let long_ttl_key = NamespaceDbCodec::replace_tag(&long_str_key, KeyTag::Ttl).unwrap();
  let (_, _, long_tag, long_p) = NamespaceDbCodec::decode_tagged_key(&long_ttl_key).unwrap();
  assert_eq!(long_tag, KeyTag::Ttl);
  assert_eq!(long_p, long_payload.as_slice());

  // 5. 异常键防护
  assert!(NamespaceDbCodec::replace_tag(&[], KeyTag::Ttl).is_none());
  assert!(NamespaceDbCodec::replace_tag(&[1, 2], KeyTag::Ttl).is_none());
}
