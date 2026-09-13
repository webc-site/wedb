use aok::{OK, Void};
use log::info;
use wval::{GarnetObjectType, KeyTag};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

#[test]
fn test_key_tag_roundtrip_and_properties() -> Void {
  info!("测试 KeyTag 往返与属性转换");

  let tags = [
    (KeyTag::String, 0x00, "String"),
    (KeyTag::Meta, 0x01, "Meta"),
    (KeyTag::Hash, 0x02, "Hash"),
    (KeyTag::Set, 0x03, "Set"),
    (KeyTag::ZSetChunk, 0x04, "ZSetChunk"),
    (KeyTag::ZSetM2s, 0x05, "ZSetM2s"),
    (KeyTag::ListChunk, 0x06, "ListChunk"),
    (KeyTag::HashChunk, 0x07, "HashChunk"),
    (KeyTag::SetChunk, 0x08, "SetChunk"),
    (KeyTag::Ttl, 0x09, "Ttl"),
    (KeyTag::Vector, 0x0a, "Vector"),
  ];

  for (tag, byte_val, name) in tags {
    assert_eq!(tag.as_u8(), byte_val);
    assert_eq!(u8::from(tag), byte_val);
    assert_eq!(KeyTag::from_u8(byte_val), Some(tag));
    assert_eq!(KeyTag::try_from(byte_val)?, tag);
    assert_eq!(KeyTag::from_repr(byte_val), Some(tag));
    assert_eq!(tag.to_string(), name);
    assert_eq!(tag.as_ref(), name);
    assert_eq!(tag.as_str(), name);
    assert_eq!(<&'static str>::from(tag), name);
    assert_eq!(tag.prefix(), [byte_val]);

    let test_key = [byte_val, 1, 2, 3];
    assert_eq!(tag.strip_prefix(&test_key), Some(&[1, 2, 3][..]));
  }

  // 非法 tag 校验（0x0a 已分配给 Vector，0x0b 起空闲）
  assert_eq!(KeyTag::from_u8(0x0b), None);
  assert_eq!(KeyTag::from_repr(0x0b), None);
  assert_eq!(KeyTag::from_u8(0xff), None);
  assert!(KeyTag::try_from(0x99).is_err());
  assert_eq!(KeyTag::String.strip_prefix(&[0x01, 2, 3]), None);
  assert_eq!(KeyTag::String.strip_prefix(&[]), None);

  OK
}

#[test]
fn test_collection_type_roundtrip_and_properties() -> Void {
  info!("测试 GarnetObjectType 往返与属性转换");

  let types = [
    (GarnetObjectType::SortedSet, 1, "zset"),
    (GarnetObjectType::List, 2, "list"),
    (GarnetObjectType::Hash, 3, "hash"),
    (GarnetObjectType::Set, 4, "set"),
    (GarnetObjectType::RangeIndex, 5, "rangeindex"),
    (GarnetObjectType::All, 0xfb, "all"),
  ];

  for (col_type, byte_val, redis_name) in types {
    assert_eq!(col_type.as_u8(), byte_val);
    assert_eq!(u8::from(col_type), byte_val);
    assert_eq!(GarnetObjectType::from_u8(byte_val), Some(col_type));
    assert_eq!(GarnetObjectType::try_from(byte_val)?, col_type);
    assert_eq!(GarnetObjectType::from_repr(byte_val), Some(col_type));
    assert_eq!(col_type.as_str(), redis_name);
    assert_eq!(col_type.as_ref(), redis_name);
    assert_eq!(col_type.to_string(), redis_name);
    assert_eq!(<&'static str>::from(col_type), redis_name);
  }

  // 非法 collection type 校验 (0 为 Null 对象，6 未分配)
  assert_eq!(GarnetObjectType::from_u8(0), Some(GarnetObjectType::Null));
  assert_eq!(GarnetObjectType::from_u8(6), None);
  assert_eq!(GarnetObjectType::from_repr(6), None);
  assert_eq!(GarnetObjectType::from_u8(0xfc), None);
  assert!(GarnetObjectType::try_from(6).is_err());

  OK
}

#[test]
fn test_vector_key_binary_safety_and_anti_penetration() -> Void {
  info!("测试 Vector 物理键定长刚性帧隔离与二进制安全");

  use wval::NamespaceDbCodec;

  let contexts = [0u64, 1, 255, 256, u64::MAX];
  let test_keys: &[&[u8]] = &[
    b"",
    b"\x00",
    b"\x00\x00\x00\x00\x00\x00\x00\x00",
    b"user:vector:123",
    b"\xff\xfe\xfd\x00\x01\x02\x03\x04\x05\x06\x07\x08",
    &[0x42; 128], // 超长键走堆分配分支
  ];

  let prefix = [0x00, 0x00]; // ns=0, db=0

  for &ctx in &contexts {
    for &key in test_keys {
      let encoded = NamespaceDbCodec::encode_vector_key_with_prefix(&prefix, ctx, key);

      // 验证定长刚性帧：prefix(2B) + KeyTag::Vector(1B) + context(8B) = 11B
      assert_eq!(encoded.as_slice()[2], KeyTag::Vector as u8);
      assert_eq!(&encoded.as_slice()[3..11], &ctx.to_be_bytes()[..]);
      assert_eq!(&encoded.as_slice()[11..], key);

      // 逆解往返无损
      let decoded = NamespaceDbCodec::decode_vector_key(encoded.as_slice());
      assert!(decoded.is_some());
      let (ns, db, dec_ctx, dec_key) = decoded.unwrap();
      assert_eq!(ns, 0);
      assert_eq!(db, 0);
      assert_eq!(dec_ctx, ctx);
      assert_eq!(dec_key, key);
    }
  }

  // 跨 KeyTag 防穿透验证：将 tag 改为 String 或 Meta 时，decode_vector_key 必须安全拒绝
  let enc = NamespaceDbCodec::encode_vector_key_with_prefix(&prefix, 42, b"test_member");
  let mut corrupted = enc.as_slice().to_vec();
  corrupted[2] = KeyTag::String as u8;
  assert!(NamespaceDbCodec::decode_vector_key(&corrupted).is_none());
  corrupted[2] = KeyTag::Meta as u8;
  assert!(NamespaceDbCodec::decode_vector_key(&corrupted).is_none());
  corrupted[2] = KeyTag::Hash as u8;
  assert!(NamespaceDbCodec::decode_vector_key(&corrupted).is_none());

  OK
}
