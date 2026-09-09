use aok::{OK, Void};
use log::info;
use wrecord::{RecordRef, try_encode_to_vec};
use wval::{CollectionType, KeyTag, RecordValueExt};

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
    (CollectionType::Hash, 1, "hash"),
    (CollectionType::Set, 2, "set"),
    (CollectionType::ZSet, 3, "zset"),
    (CollectionType::List, 4, "list"),
    (CollectionType::RangeIndex, 5, "rangeindex"),
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

  // 非法 collection type 校验
  assert_eq!(CollectionType::from_u8(0), None);
  assert_eq!(CollectionType::from_repr(0), None);
  assert_eq!(CollectionType::from_u8(6), None);
  assert_eq!(CollectionType::from_repr(6), None);
  assert!(CollectionType::try_from(0).is_err());

  OK
}

#[test]
fn test_record_ref_tag_extraction() -> Void {
  info!("测试从 RecordRef 中提取 KeyTag");

  let mut key = vec![KeyTag::Hash.as_u8()];
  key.extend_from_slice(b"1001:v1:user_name");

  let val = b"Alice";
  let encoded = try_encode_to_vec(0, &key, val, false)?;
  let rec_ref = RecordRef::from_slice(&encoded)?;

  assert_eq!(rec_ref.tag(), Some(KeyTag::Hash));

  // 测试空键
  let empty_encoded = try_encode_to_vec(0, b"", val, false)?;
  let empty_ref = RecordRef::from_slice(&empty_encoded)?;
  assert_eq!(empty_ref.tag(), None);

  // 测试未定义 tag 首字节
  let unknown_encoded = try_encode_to_vec(0, &[0xfe, 1, 2], val, false)?;
  let unknown_ref = RecordRef::from_slice(&unknown_encoded)?;
  assert_eq!(unknown_ref.tag(), None);

  OK
}

#[test]
fn test_bftag_roundtrip_and_properties() -> Void {
  info!("测试 BfTag 往返与属性转换");
  use wval::BfTag;

  let tags = [
    (BfTag::ZMember, 0, "ZMember"),
    (BfTag::ZScore, 1, "ZScore"),
    (BfTag::NextNamespace, 32, "NextNamespace"),
    (BfTag::AclUser, 33, "AclUser"),
    (BfTag::AclMeta, 34, "AclMeta"),
    (BfTag::ClusterMeta, 35, "ClusterMeta"),
    (BfTag::ReplMeta, 36, "ReplMeta"),
  ];

  for (tag, byte_val, name) in tags {
    assert_eq!(tag.as_u8(), byte_val);
    assert_eq!(u8::from(tag), byte_val);
    assert_eq!(BfTag::from_u8(byte_val), Some(tag));
    assert_eq!(BfTag::try_from(byte_val)?, tag);
    assert_eq!(BfTag::from_repr(byte_val), Some(tag));
    assert_eq!(tag.to_string(), name);
    assert_eq!(tag.as_ref(), name);
    assert_eq!(tag.as_str(), name);
    assert_eq!(<&'static str>::from(tag), name);
    assert_eq!(tag.prefix(), [byte_val]);
  }

  // 校验系统与业务分类
  assert!(BfTag::NextNamespace.is_system());
  assert!(BfTag::AclUser.is_system());
  assert!(BfTag::AclMeta.is_system());
  assert!(BfTag::ClusterMeta.is_system());
  assert!(BfTag::ReplMeta.is_system());
  assert!(!BfTag::ZMember.is_system());
  assert!(!BfTag::ZScore.is_system());

  assert!(BfTag::ZMember.is_business());
  assert!(BfTag::ZScore.is_business());
  assert!(!BfTag::AclUser.is_business());
  assert!(!BfTag::NextNamespace.is_business());

  assert!(BfTag::ZMember.is_zset());
  assert!(BfTag::ZScore.is_zset());
  assert!(!BfTag::AclUser.is_zset());

  // 校验常量边界
  assert_eq!(BfTag::TAG_LEN, 1);
  assert_eq!(BfTag::BUSINESS_TAG_MAX, 31);
  assert_eq!(BfTag::SYSTEM_TAG_BASE, 32);
  assert_eq!(BfTag::STACK_KEY_CAP, 64);

  // 校验 const fn strip_prefix 编译期能力
  const CONST_STRIP: Option<&[u8]> = BfTag::AclUser.strip_prefix(&[33, b'o', b'k']);
  assert_eq!(CONST_STRIP, Some(b"ok".as_slice()));

  // 校验 const fn strip_prefix 语义（手工拼键）
  let user_key = [33u8, b'a', b'd', b'm', b'i', b'n'];
  assert_eq!(
    BfTag::AclUser.strip_prefix(&user_key),
    Some(b"admin".as_slice())
  );
  assert_eq!(BfTag::AclMeta.strip_prefix(&user_key), None);
  assert_eq!(BfTag::AclUser.strip_prefix(&[]), None);

  // 非法 tag 校验
  assert_eq!(BfTag::from_u8(2), None);
  assert_eq!(BfTag::from_u8(31), None);
  assert_eq!(BfTag::from_u8(37), None);
  assert_eq!(BfTag::from_u8(0xff), None);
  assert!(BfTag::try_from(0x99).is_err());

  OK
}
