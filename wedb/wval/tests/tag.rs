use aok::{OK, Void};
use log::info;
use wval::{CollectionType, KeyTag};

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

  // 非法 tag 校验（0x09 已分配给 key 级 TTL 记录，0x0a 起空闲）
  assert_eq!(KeyTag::from_u8(0x0a), None);
  assert_eq!(KeyTag::from_repr(0x0a), None);
  assert_eq!(KeyTag::from_u8(0xff), None);
  assert!(KeyTag::try_from(0x99).is_err());
  assert_eq!(KeyTag::String.strip_prefix(&[0x01, 2, 3]), None);
  assert_eq!(KeyTag::String.strip_prefix(&[]), None);

  OK
}

#[test]
fn test_collection_type_roundtrip_and_properties() -> Void {
  info!("测试 CollectionType 往返与属性转换");

  let types = [
    (CollectionType::SortedSet, 1, "zset"),
    (CollectionType::List, 2, "list"),
    (CollectionType::Hash, 3, "hash"),
    (CollectionType::Set, 4, "set"),
    (CollectionType::RangeIndex, 5, "rangeindex"),
    (CollectionType::All, 0xfb, "all"),
  ];

  for (col_type, byte_val, redis_name) in types {
    assert_eq!(col_type.as_u8(), byte_val);
    assert_eq!(u8::from(col_type), byte_val);
    assert_eq!(CollectionType::from_u8(byte_val), Some(col_type));
    assert_eq!(CollectionType::try_from(byte_val)?, col_type);
    assert_eq!(CollectionType::from_repr(byte_val), Some(col_type));
    assert_eq!(col_type.as_str(), redis_name);
    assert_eq!(col_type.as_ref(), redis_name);
    assert_eq!(col_type.to_string(), redis_name);
    assert_eq!(<&'static str>::from(col_type), redis_name);
  }

  // 非法 collection type 校验 (0 为 Null 对象，6 未分配)
  assert_eq!(CollectionType::from_u8(0), Some(CollectionType::Null));
  assert_eq!(CollectionType::from_u8(6), None);
  assert_eq!(CollectionType::from_repr(6), None);
  assert_eq!(CollectionType::from_u8(0xfc), None);
  assert!(CollectionType::try_from(6).is_err());

  OK
}
