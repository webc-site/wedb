use std::mem::{align_of, size_of};

use aok::{OK, Void};
use log::info;
use wrecord::{RecordMut, RecordRef, try_encode_to_vec};
use wval::{
  COMPACT_META_VALUE_SIZE, CollectionType, CompactMetaValue, Error, KeyTag, META_VALUE_SIZE,
  MetaValue, NamespaceDbCodec, RecordValueExt, RecordValueMutExt, Result, SUBKEY_HEADER_SIZE,
  StorageEncoding, SubKeyBuf, SubKeyCodec, SubKeyRef,
};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

#[test]
fn test_meta_value_memory_layout() -> Void {
  info!("测试 MetaValue 内存布局与对齐");

  // 严格断言定长 32 字节（2^5，每条 64 字节缓存行恰好容纳 2 个）
  assert_eq!(size_of::<MetaValue>(), 32);
  assert_eq!(size_of::<MetaValue>(), META_VALUE_SIZE);
  // 严格断言 8 字节对齐（保证所有 u64 字段天然对齐，具备单总线周期原子访问能力）
  assert_eq!(align_of::<MetaValue>(), 8);

  let meta = MetaValue::new(1001, CollectionType::Hash, 1, 100);
  assert_eq!(meta.key_id, 1001);
  assert_eq!(meta.collection_type, CollectionType::Hash);
  assert_eq!(meta.version, 1);
  assert_eq!(meta.size, 100);
  assert_eq!(meta.reserved, [0u8; 7]);

  OK
}

#[test]
fn test_meta_value_roundtrip_and_serialization() -> Void {
  info!("测试 MetaValue 二进制序列化与往返恢复");

  let original = MetaValue::new(
    0x0123_4567_89ab_cdef,
    CollectionType::ZSet,
    0x0000_0000_0000_002a,
    10_000_000,
  );

  let bytes = original.to_bytes();
  assert_eq!(bytes.len(), 32);

  // 验证大端序字节排布
  assert_eq!(&bytes[0..8], &0x0123_4567_89ab_cdef_u64.to_be_bytes());
  assert_eq!(bytes[8], CollectionType::ZSet.as_u8());
  assert_eq!(&bytes[9..16], &[0u8; 7]);
  assert_eq!(&bytes[16..24], &42u64.to_be_bytes());
  assert_eq!(&bytes[24..32], &10_000_000u64.to_be_bytes());

  // 从定长字节反序列化
  let restored = MetaValue::from_bytes(bytes)?;
  assert_eq!(restored, original);

  // 从切片反序列化
  let from_slice = MetaValue::from_slice(&bytes)?;
  assert_eq!(from_slice, original);

  // 原位切片写入
  let mut dst = [0u8; 32];
  original.write_to_slice(&mut dst)?;
  assert_eq!(dst, bytes);

  // 防御性拦截：缓冲区不足
  assert!(matches!(
    MetaValue::from_slice(&bytes[..31]),
    Err(Error::BufferTooShort {
      expected: 32,
      actual: 31
    })
  ));

  let mut short_dst = [0u8; 30];
  assert!(matches!(
    original.write_to_slice(&mut short_dst),
    Err(Error::BufferTooShort {
      expected: 32,
      actual: 30
    })
  ));

  // 非法 CollectionType 拦截
  let mut corrupt_bytes = bytes;
  corrupt_bytes[8] = 0xff; // 非法类型
  assert!(matches!(
    MetaValue::from_bytes(corrupt_bytes),
    Err(Error::InvalidCollectionType(0xff))
  ));

  OK
}

#[test]
fn test_meta_value_state_transitions() -> Void {
  info!("测试 MetaValue 状态机单调递增与容量修改");

  let mut meta = MetaValue::new(8888, CollectionType::Set, 1, 0);
  assert_eq!(meta.version, 1);
  assert_eq!(meta.size, 0);

  // 版本单调递增
  assert_eq!(meta.bump_version(), 2);
  assert_eq!(meta.version, 2);
  assert_eq!(meta.bump_version(), 3);
  assert_eq!(meta.version, 3);

  // 容量加减
  meta.inc_size(5);
  assert_eq!(meta.size, 5);
  meta.dec_size(2);
  assert_eq!(meta.size, 3);
  meta.dec_size(10); // 饱和减
  assert_eq!(meta.size, 0);

  OK
}

#[test]
fn test_subkey_codec_and_ref() -> Void {
  info!("测试 SubKey 17 字节定长前缀编码与零拷贝视图");

  assert_eq!(SUBKEY_HEADER_SIZE, 17);

  let key_id = 0x1000_2000_3000_4000_u64;
  let version = 42_u64;
  let payload = b"user:timeline:comment:999";

  // 1. 编码到 Vec
  let encoded = SubKeyCodec::try_encode_to_vec(KeyTag::Hash, key_id, version, payload)?;
  assert_eq!(encoded.len(), SUBKEY_HEADER_SIZE + payload.len());

  // 2. 零拷贝从切片解析
  let sub_ref = SubKeyRef::from_slice(&encoded)?;
  assert_eq!(sub_ref.tag, KeyTag::Hash);
  assert_eq!(sub_ref.key_id, key_id);
  assert_eq!(sub_ref.version, version);
  assert_eq!(sub_ref.payload, payload);

  // 3. 原地切片编码
  let mut buf = vec![0u8; 100];
  let written = SubKeyCodec::encode_to_slice(KeyTag::Set, key_id, version, payload, &mut buf)?;
  assert_eq!(written, SUBKEY_HEADER_SIZE + payload.len());

  let sub_ref2 = SubKeyRef::from_slice(&buf[..written])?;
  assert_eq!(sub_ref2.tag, KeyTag::Set);
  assert_eq!(sub_ref2.key_id, key_id);
  assert_eq!(sub_ref2.version, version);
  assert_eq!(sub_ref2.payload, payload);

  OK
}

#[test]
fn test_subkey_boundary_edge_cases() -> Void {
  info!("测试 SubKey 极端与空边界场景");

  let key_id = 999;
  let version = 1;

  // 空 Payload 场景（如 Set 键或空字段名）
  let empty_encoded = SubKeyCodec::try_encode_to_vec(KeyTag::Set, key_id, version, b"")?;
  assert_eq!(empty_encoded.len(), SUBKEY_HEADER_SIZE);

  let sub_ref = SubKeyRef::from_slice(&empty_encoded)?;
  assert_eq!(sub_ref.tag, KeyTag::Set);
  assert_eq!(sub_ref.key_id, key_id);
  assert_eq!(sub_ref.version, version);
  assert_eq!(sub_ref.payload, b"");

  // 单字节场景
  let single_encoded = SubKeyCodec::try_encode_to_vec(KeyTag::ZSetChunk, key_id, version, b"X")?;
  let sub_ref_single = SubKeyRef::from_slice(&single_encoded)?;
  assert_eq!(sub_ref_single.payload, b"X");

  // 包含换行与不可见二进制字节场景
  let binary_payload = [0x00, 0xff, 0xfe, 0x01, 0x02, 0x03];
  let bin_encoded =
    SubKeyCodec::try_encode_to_vec(KeyTag::ZSetM2s, key_id, version, &binary_payload)?;
  let sub_ref_bin = SubKeyRef::from_slice(&bin_encoded)?;
  assert_eq!(sub_ref_bin.payload, &binary_payload);

  OK
}

#[test]
fn test_subkey_defense_and_errors() {
  // 切片长度小于 17 字节（首字节用合法子键标签，专测长度截断分支）
  let short_slice = [KeyTag::Hash.as_u8(); 16];
  assert!(matches!(
    SubKeyRef::from_slice(&short_slice),
    Err(Error::BufferTooShort {
      expected: 17,
      actual: 16
    })
  ));

  // 首字节为非法 Tag
  let mut corrupt_header = [0u8; 17];
  corrupt_header[0] = 0xee; // 未定义 tag
  assert!(matches!(
    SubKeyRef::from_slice(&corrupt_header),
    Err(Error::InvalidKeyTag(0xee))
  ));

  // 首字节为合法但非子键的 Tag（String 载荷为用户键原文，禁止按子键头解析）
  let mut string_header = [0u8; 17];
  string_header[0] = KeyTag::String.as_u8();
  assert!(matches!(
    SubKeyRef::from_slice(&string_header),
    Err(Error::InvalidKeyTag(0x00))
  ));
}

/// 类型封闭性：非子键标签（String/Meta/Ttl 载荷为用户键原文）绝不可
/// 被当作 (key_id, version) 子键布局编解码，杜绝类型穿透
#[test]
fn test_subkey_tag_closure() -> Void {
  info!("测试 SubKeyCodec 对非子键标签的编码与解析双向拒绝");

  for tag in [KeyTag::String, KeyTag::Meta, KeyTag::Ttl] {
    // 编码侧拒绝
    assert!(matches!(
      SubKeyCodec::try_encode_to_vec(tag, 1, 1, b"payload"),
      Err(Error::InvalidKeyTag(t)) if t == tag.as_u8()
    ));
    assert!(matches!(
      SubKeyBuf::encode(tag, 1, 1, b"payload"),
      Err(Error::InvalidKeyTag(t)) if t == tag.as_u8()
    ));
    let mut dst = [0u8; 32];
    assert!(matches!(
      SubKeyCodec::encode_to_slice(tag, 1, 1, b"payload", &mut dst),
      Err(Error::InvalidKeyTag(t)) if t == tag.as_u8()
    ));

    // 解析侧拒绝：手工拼出非子键标签开头的 17 字节头
    let mut raw = [0u8; 17];
    raw[0] = tag.as_u8();
    assert!(matches!(
      SubKeyRef::from_slice(&raw),
      Err(Error::InvalidKeyTag(t)) if t == tag.as_u8()
    ));
    assert!(matches!(
      SubKeyCodec::decode_header(&raw),
      Err(Error::InvalidKeyTag(t)) if t == tag.as_u8()
    ));
    // NamespaceDbCodec 子键解码与极速判定同步拒绝
    assert_eq!(NamespaceDbCodec::decode_subkey_id_version(&raw), None);
    let mut namespaced = vec![0x01, 0x00];
    namespaced.extend_from_slice(&raw);
    assert!(matches!(
      NamespaceDbCodec::decode_sub_key(&namespaced),
      Err(Error::InvalidKeyTag(t)) if t == tag.as_u8()
    ));
  }

  // 子键族标签 (Hash..=SetChunk) 全部放行
  for tag in [
    KeyTag::Hash,
    KeyTag::Set,
    KeyTag::ZSetChunk,
    KeyTag::ZSetM2s,
    KeyTag::ListChunk,
    KeyTag::HashChunk,
    KeyTag::SetChunk,
  ] {
    let encoded = SubKeyCodec::try_encode_to_vec(tag, 7, 3, b"p")?;
    assert_eq!(SubKeyRef::from_slice(&encoded)?.tag, tag);
  }

  OK
}

#[test]
fn test_meta_value_fast_field_readers() -> Void {
  info!("测试 MetaValue 快速字段读取方法与边界防御");

  let meta = MetaValue::new(9999, CollectionType::List, 77, 4321);
  let bytes = meta.to_bytes();

  // 1. 快速读取
  assert_eq!(MetaValue::read_version(&bytes)?, 77);
  assert_eq!(MetaValue::read_size(&bytes)?, 4321);
  assert_eq!(
    MetaValue::read_collection_type(&bytes)?,
    CollectionType::List
  );

  // 2. 边界截断防御
  assert!(matches!(
    MetaValue::read_version(&bytes[..23]),
    Err(Error::BufferTooShort {
      expected: 24,
      actual: 23
    })
  ));
  assert!(matches!(
    MetaValue::read_size(&bytes[..31]),
    Err(Error::BufferTooShort {
      expected: 32,
      actual: 31
    })
  ));
  assert!(matches!(
    MetaValue::read_collection_type(&bytes[..8]),
    Err(Error::BufferTooShort {
      expected: 9,
      actual: 8
    })
  ));

  // 3. 非法类型防御
  let mut bad_type = bytes;
  bad_type[8] = 0xee;
  assert!(matches!(
    MetaValue::read_collection_type(&bad_type),
    Err(Error::InvalidCollectionType(0xee))
  ));

  OK
}

#[test]
fn test_subkey_header_codec_and_helpers() -> Void {
  info!("测试 SubKeyCodec 头部编解码与 SubKeyRef 辅助方法");

  let tag = KeyTag::Hash;
  let key_id = 0xdead_beef_cafe_babe_u64;
  let version = 12345_u64;
  let payload = b"field_name_xyz";

  // 1. 独立头部编码与快速解码
  let header = SubKeyCodec::encode_header(tag, key_id, version);
  assert_eq!(header.len(), SUBKEY_HEADER_SIZE);

  let (dec_tag, dec_id, dec_ver) = SubKeyCodec::decode_header(&header)?;
  assert_eq!(dec_tag, tag);
  assert_eq!(dec_id, key_id);
  assert_eq!(dec_ver, version);

  // 2. SubKeyRef 辅助方法
  let encoded = SubKeyCodec::try_encode_to_vec(tag, key_id, version, payload)?;
  let sub_ref = SubKeyCodec::decode(&encoded)?;

  assert_eq!(sub_ref.header(), header);
  assert_eq!(sub_ref.encoded_len(), SUBKEY_HEADER_SIZE + payload.len());
  assert_eq!(sub_ref.to_vec(), encoded);

  let mut buf = vec![0u8; sub_ref.encoded_len()];
  let written = sub_ref.write_to_slice(&mut buf)?;
  assert_eq!(written, sub_ref.encoded_len());
  assert_eq!(buf, encoded);

  // 3. try_to_vec / try_encode_to_vec 精准容量单次分配路径
  assert_eq!(sub_ref.try_to_vec()?, encoded);
  assert_eq!(
    SubKeyCodec::try_encode_to_vec(tag, key_id, version, payload)?,
    encoded
  );

  OK
}

#[test]
fn test_record_ref_and_mut_integration() -> Void {
  info!("测试 RecordRef 与 RecordMut 对 MetaValue / SubKeyRef 的零拷贝联动");

  // 1. Meta 记录在 RecordRef 与 RecordMut 间联动
  let meta = MetaValue::new(10086, CollectionType::Hash, 1, 0);
  let meta_bytes = meta.to_bytes();
  let rec_bytes = try_encode_to_vec(0, b"hash_meta_key", &meta_bytes, false)?;

  // 只读零拷贝解析
  let rec_ref = RecordRef::from_slice(&rec_bytes)?;
  let decoded_meta = rec_ref.meta_value()?;
  assert_eq!(decoded_meta, meta);

  // 可变原位更新
  let mut mut_buf = rec_bytes.clone();
  let mut rec_mut = RecordMut::from_slice_mut(&mut mut_buf)?;
  assert_eq!(rec_mut.meta_value()?, meta);

  let mut updated_meta = meta;
  updated_meta.bump_version();
  updated_meta.inc_size(10);
  rec_mut.update_meta_value(&updated_meta)?;

  // 校验写回
  assert_eq!(rec_mut.meta_value()?, updated_meta);
  let final_ref = RecordRef::from_slice(&mut_buf)?;
  assert_eq!(final_ref.meta_value()?, updated_meta);

  // 2. SubKey 记录在 RecordRef 中的解析联动
  let subkey_bytes = SubKeyCodec::try_encode_to_vec(KeyTag::Hash, 10086, 1, b"field1")?;
  let subkey_rec_bytes = try_encode_to_vec(0, &subkey_bytes, b"value1", false)?;

  let subkey_rec_ref = RecordRef::from_slice(&subkey_rec_bytes)?;
  let sub_ref = subkey_rec_ref.sub_key_ref()?;
  assert_eq!(sub_ref.tag, KeyTag::Hash);
  assert_eq!(sub_ref.key_id, 10086);
  assert_eq!(sub_ref.version, 1);
  assert_eq!(sub_ref.payload, b"field1");

  OK
}

#[test]
fn test_compile_time_const_evaluation() {
  const META: MetaValue = MetaValue::new(42, CollectionType::Hash, 1, 10);
  const BYTES: [u8; 32] = META.to_bytes();
  const VERSION: Result<u64> = MetaValue::read_version(&BYTES);
  assert!(matches!(VERSION, Ok(1)));
  const SIZE: Result<u64> = MetaValue::read_size(&BYTES);
  assert!(matches!(SIZE, Ok(10)));
  const TYPE: Result<CollectionType> = MetaValue::read_collection_type(&BYTES);
  assert!(matches!(TYPE, Ok(CollectionType::Hash)));

  const HDR: [u8; 17] = SubKeyCodec::encode_header(KeyTag::Hash, 42, 1);
  const DEC_HDR: Result<(KeyTag, u64, u64)> = SubKeyCodec::decode_header(&HDR);
  assert!(matches!(DEC_HDR, Ok((KeyTag::Hash, 42, 1))));

  const fn extract_id_ver(hdr: &[u8]) -> Option<(u64, u64)> {
    match hdr {
      [_, rest @ ..] => SubKeyCodec::decode_id_version(rest),
      [] => None,
    }
  }
  const ID_VER: Option<(u64, u64)> = extract_id_ver(&HDR);
  assert_eq!(ID_VER, Some((42, 1)));

  // CompactMetaValue 编译期求值与快速探针验证
  const CMETA: CompactMetaValue = CompactMetaValue::new(
    CollectionType::ZSet,
    StorageEncoding::Flattened,
    888,
    1_700_000_000_000,
  );
  const C_BYTES: [u8; 16] = CMETA.to_bytes();
  const C_DEC: Result<CompactMetaValue> = CompactMetaValue::from_slice(&C_BYTES);
  assert!(matches!(C_DEC, Ok(cm) if cm.size == 888 && cm.expire_at_ms == 1_700_000_000_000));
  const C_TYPE: Option<CollectionType> = CompactMetaValue::read_collection_type(&C_BYTES);
  assert_eq!(C_TYPE, Some(CollectionType::ZSet));
  const C_ENC: Option<StorageEncoding> = CompactMetaValue::read_encoding(&C_BYTES);
  assert_eq!(C_ENC, Some(StorageEncoding::Flattened));
  const C_SIZE: Option<u32> = CompactMetaValue::read_size(&C_BYTES);
  assert_eq!(C_SIZE, Some(888));
  const C_EXP: Option<u64> = CompactMetaValue::read_expire_at_ms(&C_BYTES);
  assert_eq!(C_EXP, Some(1_700_000_000_000));
  const C_EXPIRED_NO: Option<bool> = CompactMetaValue::read_is_expired(&C_BYTES, 1_699_999_999_999);
  assert_eq!(C_EXPIRED_NO, Some(false));
  const C_EXPIRED_YES: Option<bool> =
    CompactMetaValue::read_is_expired(&C_BYTES, 1_700_000_000_000);
  assert_eq!(C_EXPIRED_YES, Some(true));

  // 紧凑容器 count const fn 契约验证（成功与截断错误分支全覆盖）
  const CNT_BYTES: [u8; 4] = [0x01, 0x2c, 0, 0]; // 0x012c = 300
  const SET_CNT: Result<usize> = wval::CompactSetCodec::count(&CNT_BYTES);
  assert!(matches!(SET_CNT, Ok(300)));
  const HASH_CNT: Result<usize> = wval::CompactHashCodec::count(&CNT_BYTES);
  assert!(matches!(HASH_CNT, Ok(300)));
  const ZSET_CNT: Result<usize> = wval::CompactZSetCodec::count(&CNT_BYTES);
  assert!(matches!(ZSET_CNT, Ok(300)));

  const SHORT_BYTES: [u8; 1] = [0];
  const SET_CNT_ERR: Result<usize> = wval::CompactSetCodec::count(&SHORT_BYTES);
  assert!(matches!(
    SET_CNT_ERR,
    Err(Error::BufferTooShort {
      expected: 2,
      actual: 1
    })
  ));
  const HASH_CNT_ERR: Result<usize> = wval::CompactHashCodec::count(&SHORT_BYTES);
  assert!(matches!(
    HASH_CNT_ERR,
    Err(Error::BufferTooShort {
      expected: 2,
      actual: 1
    })
  ));
  const ZSET_CNT_ERR: Result<usize> = wval::CompactZSetCodec::count(&SHORT_BYTES);
  assert!(matches!(
    ZSET_CNT_ERR,
    Err(Error::BufferTooShort {
      expected: 2,
      actual: 1
    })
  ));
}

#[test]
fn test_meta_value_bitcode_serialization() -> Void {
  info!("测试 MetaValue 与 StorageEncoding 的 bitcode 序列化往返");

  let meta =
    MetaValue::new(10086, CollectionType::ZSet, 3, 500).with_encoding(StorageEncoding::Flattened);
  let encoded = meta.encode_bitcode();
  assert!(!encoded.is_empty());

  let decoded = MetaValue::decode_bitcode(&encoded)?;
  assert_eq!(meta, decoded);
  assert_eq!(decoded.key_id, 10086);
  assert_eq!(decoded.collection_type, CollectionType::ZSet);
  assert_eq!(decoded.encoding(), StorageEncoding::Flattened);
  assert_eq!(decoded.version, 3);
  assert_eq!(decoded.size, 500);

  // 错误输入防御
  let bad_data = [0xFFu8; 2];
  assert!(MetaValue::decode_bitcode(&bad_data).is_err());

  OK
}

#[test]
fn test_compact_meta_value_16_bytes() -> Void {
  info!("测试 16 字节 CompactMetaValue 内存排布与功能");

  // 1. 严格断言定长 16 字节与 8 字节自然对齐
  assert_eq!(size_of::<CompactMetaValue>(), 16);
  assert_eq!(size_of::<CompactMetaValue>(), COMPACT_META_VALUE_SIZE);
  assert_eq!(align_of::<CompactMetaValue>(), 8);

  // 2. 构造与字段检查
  let mut cmeta = CompactMetaValue::new(
    CollectionType::Hash,
    StorageEncoding::Compact,
    100,
    1_700_000_000_000,
  );
  assert_eq!(cmeta.collection_type, CollectionType::Hash);
  assert_eq!(cmeta.encoding, StorageEncoding::Compact);
  assert_eq!(cmeta.size, 100);
  assert_eq!(cmeta.expire_at_ms, 1_700_000_000_000);

  // 3. 过期检查
  assert!(!cmeta.is_expired(1_699_999_999_999));
  assert!(cmeta.is_expired(1_700_000_000_000));
  assert!(cmeta.is_expired(1_700_000_000_001));

  // 永不过期 (expire_at_ms == 0)
  let no_exp = CompactMetaValue::new(CollectionType::Set, StorageEncoding::Flattened, 50, 0);
  assert!(!no_exp.is_expired(u64::MAX));

  // 4. 计数增减
  cmeta.inc_size(25);
  assert_eq!(cmeta.size, 125);
  cmeta.dec_size(50);
  assert_eq!(cmeta.size, 75);
  cmeta.dec_size(100); // 饱和减
  assert_eq!(cmeta.size, 0);

  // 5. 二进制大端往返
  let bytes = cmeta.to_bytes();
  assert_eq!(bytes.len(), 16);
  let restored = CompactMetaValue::from_bytes(bytes)?;
  assert_eq!(cmeta, restored);

  let from_slice = CompactMetaValue::from_slice(&bytes)?;
  assert_eq!(cmeta, from_slice);

  // 5.1 切片写入（零堆分配）
  let mut dst = [0u8; 16];
  cmeta.write_to_slice(&mut dst)?;
  assert_eq!(dst, bytes);
  let mut short_dst = [0u8; 15];
  assert_eq!(
    cmeta.write_to_slice(&mut short_dst),
    Err(Error::BufferTooShort {
      expected: 16,
      actual: 15,
    })
  );

  // 6. 切片长度不足错误拦截
  assert_eq!(
    CompactMetaValue::from_slice(&bytes[..15]),
    Err(Error::BufferTooShort {
      expected: 16,
      actual: 15,
    })
  );

  // 7. 快速字段探针验证
  assert_eq!(
    CompactMetaValue::read_collection_type(&bytes),
    Some(cmeta.collection_type)
  );
  assert_eq!(
    CompactMetaValue::read_encoding(&bytes),
    Some(cmeta.encoding)
  );
  assert_eq!(CompactMetaValue::read_size(&bytes), Some(cmeta.size));
  assert_eq!(
    CompactMetaValue::read_expire_at_ms(&bytes),
    Some(cmeta.expire_at_ms)
  );
  assert_eq!(
    CompactMetaValue::read_is_expired(&bytes, 1_699_999_999_999),
    Some(false)
  );
  assert_eq!(
    CompactMetaValue::read_is_expired(&bytes, 1_700_000_000_000),
    Some(true)
  );
  // 短切片探针安全返回 None
  assert_eq!(CompactMetaValue::read_collection_type(&bytes[..15]), None);
  assert_eq!(CompactMetaValue::read_size(&bytes[..15]), None);
  assert_eq!(CompactMetaValue::read_expire_at_ms(&bytes[..15]), None);
  assert_eq!(
    CompactMetaValue::read_is_expired(&bytes[..15], 1_700_000_000_000),
    None
  );

  // 8. bitcode 序列化往返
  let bc = cmeta.encode_bitcode();
  let from_bc = CompactMetaValue::decode_bitcode(&bc)?;
  assert_eq!(cmeta, from_bc);

  OK
}

#[test]
fn test_subkey_buf_stack_and_heap() -> Void {
  info!("测试 SubKeyBuf 优先栈分配与超长堆回退");

  // 1. 短 payload: <= 128 字节 (17 + 10 = 27 字节) -> 栈分配
  let short_payload = b"short_field";
  let sbuf = SubKeyBuf::encode(KeyTag::Hash, 1001, 1, short_payload)?;
  assert!(sbuf.is_stack());
  assert!(!sbuf.is_heap());
  assert_eq!(sbuf.len(), SUBKEY_HEADER_SIZE + short_payload.len());
  assert_eq!(&sbuf[SUBKEY_HEADER_SIZE..], short_payload);

  // 验证 Deref, AsRef, Borrow
  let slice: &[u8] = &sbuf;
  assert_eq!(slice, sbuf.as_slice());
  assert_eq!(sbuf, slice);

  // 验证 SubKeyRef 解析
  let sref = SubKeyRef::from_slice(&sbuf)?;
  assert_eq!(sref.tag, KeyTag::Hash);
  assert_eq!(sref.key_id, 1001);
  assert_eq!(sref.version, 1);
  assert_eq!(sref.payload, short_payload);

  // 2. 长 payload: > 128 字节 (17 + 120 = 137 字节) -> 堆分配
  let long_payload = vec![0xABu8; 120];
  let lbuf = SubKeyBuf::encode(KeyTag::Set, 2002, 5, &long_payload)?;
  assert!(!lbuf.is_stack());
  assert!(lbuf.is_heap());
  assert_eq!(lbuf.len(), SUBKEY_HEADER_SIZE + 120);
  assert_eq!(&lbuf[SUBKEY_HEADER_SIZE..], long_payload.as_slice());

  // 3. 转换 Vec
  let vec = sbuf.into_vec();
  assert_eq!(vec.len(), SUBKEY_HEADER_SIZE + short_payload.len());

  OK
}
