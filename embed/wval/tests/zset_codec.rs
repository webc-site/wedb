use core::cmp::Ordering;

use aok::{OK, Void};
use log::info;
use whasher::HashMap;
use wrecord::{RecordRef, try_encode_to_vec};
use wval::{
  BfTag, Error, KeyTag, MEMBER_KEY_HEADER_SIZE, RecordValueExt, SCORE_KEY_HEADER_SIZE,
  ZMemberKeyRef, ZSET_SUBKEY_STACK_CAP, ZScoreKeyRef, ZSetSubKeyBuf, ZSetSubKeyCodec,
  decode_order_preserving_f64, encode_order_preserving_f64,
};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

#[test]
fn test_key_tag_zset_and_subkeys() -> Void {
  info!("测试 KeyTag 针对 HASH_FIELD、SET_MEMBER、Z_MEMBER、Z_SCORE 的单字节映射与匹配");

  // 1. 验证单字节数值映射
  assert_eq!(KeyTag::Hash.as_u8(), 0x02);
  assert_eq!(u8::from(KeyTag::Hash), 0x02);
  assert_eq!(KeyTag::Hash as u8, 0x02);
  assert_eq!(KeyTag::from_u8(0x02), Some(KeyTag::Hash));
  assert_eq!(KeyTag::try_from(0x02)?, KeyTag::Hash);

  assert_eq!(KeyTag::Set.as_u8(), 0x03);
  assert_eq!(u8::from(KeyTag::Set), 0x03);
  assert_eq!(KeyTag::Set as u8, 0x03);
  assert_eq!(KeyTag::from_u8(0x03), Some(KeyTag::Set));
  assert_eq!(KeyTag::try_from(0x03)?, KeyTag::Set);

  assert_eq!(KeyTag::ZSetChunk.as_u8(), 0x04);
  assert_eq!(u8::from(KeyTag::ZSetChunk), 0x04);
  assert_eq!(KeyTag::ZSetChunk as u8, 0x04);
  assert_eq!(KeyTag::from_u8(0x04), Some(KeyTag::ZSetChunk));
  assert_eq!(KeyTag::try_from(0x04)?, KeyTag::ZSetChunk);

  assert_eq!(KeyTag::ZSetM2s.as_u8(), 0x05);
  assert_eq!(u8::from(KeyTag::ZSetM2s), 0x05);
  assert_eq!(KeyTag::ZSetM2s as u8, 0x05);
  assert_eq!(KeyTag::from_u8(0x05), Some(KeyTag::ZSetM2s));
  assert_eq!(KeyTag::try_from(0x05)?, KeyTag::ZSetM2s);

  // 2. 验证与原有标识的等价别名关系
  assert_eq!(KeyTag::Hash, KeyTag::Hash);
  assert_eq!(KeyTag::Set, KeyTag::Set);
  assert_eq!(KeyTag::ZSetChunk, KeyTag::ZSetChunk);
  assert_eq!(KeyTag::ZSetM2s, KeyTag::ZSetM2s);

  OK
}

#[test]
fn test_order_preserving_f64_codec() -> Void {
  info!("测试浮点数保序编码与无损往返还原");

  let test_floats = [
    f64::NEG_INFINITY,
    -1e300,
    -1e100,
    -100.5,
    -42.0,
    -1.0,
    -0.1,
    -1e-300,
    -0.0,
    0.0,
    1e-300,
    0.1,
    1.0,
    42.0,
    100.5,
    1e100,
    1e300,
    f64::INFINITY,
  ];

  // 1. 验证编码后的字节序严格单调递增
  for i in 0..test_floats.len() - 1 {
    let a = test_floats[i];
    let b = test_floats[i + 1];
    let enc_a = encode_order_preserving_f64(a);
    let enc_b = encode_order_preserving_f64(b);

    assert!(
      enc_a < enc_b,
      "保序失败: a={a} enc={enc_a:?} 应当小于 b={b} enc={enc_b:?}"
    );
  }

  // 2. 验证往返还原无损性（含 -0.0 与 +0.0 符号位区分）
  for &v in &test_floats {
    let enc = encode_order_preserving_f64(v);
    let dec = decode_order_preserving_f64(enc);

    assert_eq!(
      dec.to_bits(),
      v.to_bits(),
      "解码还原位不匹配: 原始 {v} (bits: {:#x}) -> 解码 {dec} (bits: {:#x})",
      v.to_bits(),
      dec.to_bits()
    );
  }

  // 3. 密集步进区间保序验证
  let mut current = -10.0;
  let mut prev_enc = encode_order_preserving_f64(current);
  for _ in 0..1000 {
    current += 0.02;
    let enc = encode_order_preserving_f64(current);
    assert!(prev_enc < enc);
    prev_enc = enc;
  }

  OK
}

#[test]
fn test_zset_member_key_codec() -> Void {
  info!("测试 ZSetSubKeyCodec 成员子键编解码 (0)");

  assert_eq!(MEMBER_KEY_HEADER_SIZE, 17);

  let key_id = 0x0123_4567_89ab_cdef_u64;
  let version = 0x0011_2233_4455_6677_u64;
  let member = b"user:profile:10086";

  // 1. 编码到 Vec
  let encoded = ZSetSubKeyCodec::encode_member_key(key_id, version, member)?;
  assert_eq!(encoded.len(), MEMBER_KEY_HEADER_SIZE + member.len());

  // 验证二进制布局: [0 | key_id: 8B be | version: 8B be | member]
  assert_eq!(encoded[0], BfTag::ZMember.as_u8());
  assert_eq!(encoded[0], 0);
  assert_eq!(&encoded[1..9], &key_id.to_be_bytes());
  assert_eq!(&encoded[9..17], &version.to_be_bytes());
  assert_eq!(&encoded[17..], member);

  // 2. 零拷贝解码（含 Ref::from_slice 包装）
  let key_ref = ZMemberKeyRef::from_slice(&encoded)?;
  assert_eq!(key_ref.key_id, key_id);
  let key_ref2 = ZSetSubKeyCodec::decode_member_key(&encoded)?;
  assert_eq!(key_ref2.key_id, key_id);
  assert_eq!(key_ref.version, version);
  assert_eq!(key_ref.member, member);
  assert_eq!(key_ref.encoded_len(), encoded.len());
  assert_eq!(
    key_ref.header(),
    ZSetSubKeyCodec::encode_member_header(key_id, version)
  );

  // 3. 编码到目标切片
  let mut dst = vec![0u8; encoded.len()];
  let written = ZSetSubKeyCodec::encode_member_key_to_slice(key_id, version, member, &mut dst)?;
  assert_eq!(written, encoded.len());
  assert_eq!(dst, encoded);

  OK
}

#[test]
fn test_zset_score_key_codec() -> Void {
  info!("测试 ZSetSubKeyCodec 分值子键编解码 (1)");

  assert_eq!(SCORE_KEY_HEADER_SIZE, 25);

  let key_id = 0x0123_4567_89ab_cdef_u64;
  let version = 0x0011_2233_4455_6677_u64;
  let score = -123.456_f64;
  let member = b"leaderboard:top_1";

  // 1. 编码到 Vec
  let encoded = ZSetSubKeyCodec::encode_score_key(key_id, version, score, member)?;
  assert_eq!(encoded.len(), SCORE_KEY_HEADER_SIZE + member.len());

  // 验证二进制布局: [1 | key_id: 8B be | version: 8B be | order_score: 8B | member]
  assert_eq!(encoded[0], BfTag::ZScore.as_u8());
  assert_eq!(encoded[0], 1);
  assert_eq!(&encoded[1..9], &key_id.to_be_bytes());
  assert_eq!(&encoded[9..17], &version.to_be_bytes());
  assert_eq!(&encoded[17..25], &encode_order_preserving_f64(score));
  assert_eq!(&encoded[25..], member);

  // 2. 零拷贝解码（含 Ref::from_slice 包装与定长头快捷解码）
  let score_ref = ZScoreKeyRef::from_slice(&encoded)?;
  let (h_id, h_ver, h_score) = ZSetSubKeyCodec::decode_score_header(&encoded)?;
  assert_eq!((h_id, h_ver), (key_id, version));
  assert_eq!(h_score.to_bits(), score.to_bits());
  assert_eq!(score_ref.key_id, key_id);
  assert_eq!(score_ref.version, version);
  assert_eq!(score_ref.score.to_bits(), score.to_bits());
  assert_eq!(score_ref.raw_score, encode_order_preserving_f64(score));
  assert_eq!(score_ref.member, member);
  assert_eq!(score_ref.encoded_len(), encoded.len());
  assert_eq!(
    score_ref.header(),
    ZSetSubKeyCodec::encode_score_header(key_id, version, score)
  );

  // 3. 验证在相同 key_id / version 下，不同分值键原生字节序完全保持数学有序性
  let score1 = -500.0;
  let score2 = 0.0;
  let score3 = 100.5;
  let k1 = ZSetSubKeyCodec::encode_score_key(key_id, version, score1, member)?;
  let k2 = ZSetSubKeyCodec::encode_score_key(key_id, version, score2, member)?;
  let k3 = ZSetSubKeyCodec::encode_score_key(key_id, version, score3, member)?;
  assert!(k1 < k2);
  assert!(k2 < k3);

  // 5. 验证相同分值不同成员按字典序保序
  let k_a = ZSetSubKeyCodec::encode_score_key(key_id, version, 100.0, b"member_a")?;
  let k_b = ZSetSubKeyCodec::encode_score_key(key_id, version, 100.0, b"member_b")?;
  assert!(k_a < k_b);

  OK
}

#[test]
fn test_zset_codec_boundary_and_defense() -> Void {
  info!("测试 ZSetSubKeyCodec 严格边界安全防御（空、短、巨型载荷不越界 panic）");

  let key_id = 42_u64;
  let version = 1_u64;

  // 1. 空成员合法，头部本身即为完整子键。
  let member_key = ZSetSubKeyCodec::encode_member_key(key_id, version, b"")?;
  let score_key = ZSetSubKeyCodec::encode_score_key(key_id, version, 1.0, b"")?;
  assert_eq!(member_key.len(), MEMBER_KEY_HEADER_SIZE);
  assert_eq!(score_key.len(), SCORE_KEY_HEADER_SIZE);
  assert_eq!(ZSetSubKeyCodec::decode_member_key(&member_key)?.member, b"");
  assert_eq!(ZSetSubKeyCodec::decode_score_key(&score_key)?.member, b"");
  let mut buf = [0u8; 32];
  assert_eq!(
    ZSetSubKeyCodec::encode_member_key_to_slice(key_id, version, b"", &mut buf)?,
    MEMBER_KEY_HEADER_SIZE
  );

  // 2. 非法短切片防御（长度不足头部长度）
  for len in 0..MEMBER_KEY_HEADER_SIZE {
    let short_slice = vec![0u8; len];
    assert!(matches!(
      ZSetSubKeyCodec::decode_member_key(&short_slice),
      Err(Error::BufferTooShort {
        expected: MEMBER_KEY_HEADER_SIZE,
        actual
      }) if actual == len
    ));
  }

  for len in 0..SCORE_KEY_HEADER_SIZE {
    let short_slice = vec![0u8; len];
    assert!(matches!(
      ZSetSubKeyCodec::decode_score_key(&short_slice),
      Err(Error::BufferTooShort {
        expected: SCORE_KEY_HEADER_SIZE,
        actual
      }) if actual == len
    ));
  }

  // 目标缓冲区容量不足防御
  let mut tiny_buf = [0u8; 10];
  assert!(matches!(
    ZSetSubKeyCodec::encode_member_key_to_slice(key_id, version, b"alice", &mut tiny_buf),
    Err(Error::BufferTooShort { .. })
  ));

  // 3. 非法 Tag 防御
  let mut corrupt_member_key = ZSetSubKeyCodec::encode_member_key(key_id, version, b"hello")?;
  corrupt_member_key[0] = 0x99;
  assert!(matches!(
    ZSetSubKeyCodec::decode_member_key(&corrupt_member_key),
    Err(Error::InvalidKeyTag(0x99))
  ));

  let mut corrupt_score_key = ZSetSubKeyCodec::encode_score_key(key_id, version, 1.0, b"hello")?;
  corrupt_score_key[0] = 0x88;
  assert!(matches!(
    ZSetSubKeyCodec::decode_score_key(&corrupt_score_key),
    Err(Error::InvalidKeyTag(0x88))
  ));

  // 4. 巨大 member 安全验证（1MB 载荷无崩溃、无栈溢出、零拷贝无损还原）
  let huge_member = vec![b'z'; 1024 * 1024];
  let huge_encoded = ZSetSubKeyCodec::encode_member_key(key_id, version, &huge_member)?;
  assert_eq!(
    huge_encoded.len(),
    MEMBER_KEY_HEADER_SIZE + huge_member.len()
  );

  let huge_ref = ZSetSubKeyCodec::decode_member_key(&huge_encoded)?;
  assert_eq!(huge_ref.member.len(), huge_member.len());
  assert_eq!(huge_ref.member, huge_member.as_slice());

  let huge_score_encoded =
    ZSetSubKeyCodec::encode_score_key(key_id, version, 999.99, &huge_member)?;
  let huge_score_ref = ZSetSubKeyCodec::decode_score_key(&huge_score_encoded)?;
  assert_eq!(huge_score_ref.member.len(), huge_member.len());
  assert_eq!(huge_score_ref.member, huge_member.as_slice());

  OK
}

#[test]
fn test_zset_sub_key_buf() -> Void {
  info!("测试 ZSetSubKeyBuf 栈分配与堆自动回退");

  assert_eq!(ZSET_SUBKEY_STACK_CAP, 128);

  let key_id = 8888_u64;
  let version = 2_u64;

  // 1. 短 member (应当走栈分配 Stack，消除堆分配开销)
  let short_member = b"user_1001";
  let member_buf = ZSetSubKeyCodec::encode_member_key_buf(key_id, version, short_member)?;
  assert!(member_buf.is_stack());
  assert!(!member_buf.is_heap());
  assert_eq!(
    member_buf.as_slice(),
    ZSetSubKeyCodec::encode_member_key(key_id, version, short_member)?.as_slice()
  );

  let score_buf = ZSetSubKeyCodec::encode_score_key_buf(key_id, version, 88.8, short_member)?;
  assert!(score_buf.is_stack());
  assert!(!score_buf.is_heap());
  assert_eq!(
    score_buf.as_slice(),
    ZSetSubKeyCodec::encode_score_key(key_id, version, 88.8, short_member)?.as_slice()
  );

  // 2. 刚好处于栈上限 128 字节边界
  // 对于 member key: 头部 17 字节，128 - 17 = 111 字节 member
  let exact_stack_member = vec![b'k'; 128 - MEMBER_KEY_HEADER_SIZE];
  let exact_buf = ZSetSubKeyCodec::encode_member_key_buf(key_id, version, &exact_stack_member)?;
  assert!(exact_buf.is_stack());
  assert_eq!(exact_buf.len(), 128);

  // 3. 超出 128 字节 (112 字节 member，总长 129 字节 -> 自动回退到堆 Heap)
  let overflow_member = vec![b'k'; 128 - MEMBER_KEY_HEADER_SIZE + 1];
  let heap_buf = ZSetSubKeyCodec::encode_member_key_buf(key_id, version, &overflow_member)?;
  assert!(heap_buf.is_heap());
  assert!(!heap_buf.is_stack());
  assert_eq!(heap_buf.len(), 129);
  assert_eq!(
    heap_buf.as_slice(),
    ZSetSubKeyCodec::encode_member_key(key_id, version, &overflow_member)?.as_slice()
  );

  // 4. 便捷构造器验证
  let buf1 = wval::ZSetSubKeyCodec::encode_member_key_buf(key_id, version, short_member)?;
  assert!(buf1.is_stack());
  assert_eq!(buf1.as_slice(), member_buf.as_slice());

  // 4.1 分值子键便捷构造改经 codec 栈/堆契约
  let buf2 = ZSetSubKeyCodec::encode_score_key_buf(key_id, version, 88.8, short_member)?;
  assert!(buf2.is_stack());
  assert_eq!(buf2.as_slice(), score_buf.as_slice());

  // 5. 空成员仍走栈编码。
  assert!(wval::ZSetSubKeyCodec::encode_member_key_buf(key_id, version, b"")?.is_stack());
  assert!(ZSetSubKeyCodec::encode_score_key_buf(key_id, version, 1.0, b"")?.is_stack());

  OK
}

#[test]
fn test_zset_sub_key_buf_borrow_and_ord_contract() -> Void {
  info!("测试 ZSetSubKeyBuf 严格遵循标准库 Borrow、Eq、Ord、Hash 契约与跨存储形式等价性");

  let key_id = 999_u64;
  let version = 1_u64;
  let member = b"user:session:token";

  let stack_buf = wval::ZSetSubKeyCodec::encode_member_key_buf(key_id, version, member)?;
  assert!(stack_buf.is_stack());

  // 构造相同二进制内容的 Heap 版本
  let heap_buf = ZSetSubKeyBuf::Heap(stack_buf.as_slice().to_vec());
  assert!(heap_buf.is_heap());

  // 1. 跨表现形式内容等价性验证 (Stack == Heap)
  assert_eq!(stack_buf, heap_buf);
  assert_eq!(heap_buf, stack_buf);
  assert_eq!(stack_buf.cmp(&heap_buf), Ordering::Equal);

  // 2. 切片直接比较支持
  assert_eq!(stack_buf, stack_buf.as_slice());
  assert_eq!(stack_buf.as_slice(), stack_buf.as_slice());

  // 3. Borrow<[u8]> 契约与 HashMap 查询
  let mut map: HashMap<ZSetSubKeyBuf, u32> = HashMap::default();
  map.insert(stack_buf.clone(), 12345);

  // 用 &[u8] 切片借用零拷贝查询
  let found = map.get(stack_buf.as_slice());
  assert_eq!(found, Some(&12345));

  // 用 Heap 实例查询 Stack 键
  let found_heap = map.get(&heap_buf);
  assert_eq!(found_heap, Some(&12345));

  // 4. From<&[u8]> 与 From<Vec<u8>>
  let from_slice = ZSetSubKeyBuf::from(stack_buf.as_slice());
  assert!(from_slice.is_stack());
  assert_eq!(from_slice, stack_buf);

  let huge = vec![b'a'; 200];
  let from_huge = ZSetSubKeyBuf::from(huge.as_slice());
  assert!(from_huge.is_heap());
  assert_eq!(from_huge.len(), 200);

  // 5. into_vec 零额外堆分配
  let moved_vec = heap_buf.into_vec();
  assert_eq!(moved_vec, stack_buf.as_slice());

  OK
}

#[test]
fn test_zscore_key_ref_lexicographical_ordering() -> Void {
  info!("测试 ZScoreKeyRef 的 Ord 全序与底层存储物理字节字典序 100% 同构");

  let key_id = 77_u64;
  let version = 3_u64;

  let test_cases = [
    (f64::NEG_INFINITY, b"a".as_slice()),
    (-1000.0, b"alpha".as_slice()),
    (-1000.0, b"beta".as_slice()),
    (-0.5, b"x".as_slice()),
    (-0.0, b"zero".as_slice()),
    (0.0, b"zero".as_slice()),
    (0.0, b"zero_2".as_slice()),
    (1e-100, b"tiny".as_slice()),
    (42.0, b"ans".as_slice()),
    (100.0, b"a".as_slice()),
    (100.0, b"b".as_slice()),
    (f64::INFINITY, b"inf".as_slice()),
  ];

  let mut key_refs: Vec<ZScoreKeyRef> = test_cases
    .iter()
    .map(|&(score, member)| ZScoreKeyRef::new(key_id, version, score, member))
    .collect();

  // 按照 ZScoreKeyRef::cmp 排序
  key_refs.sort();

  // 验证每个元素序列化后的字节数组严格单调递增
  let encoded_bytes: Vec<Vec<u8>> = key_refs.iter().map(|r| r.to_vec()).collect();
  for i in 0..encoded_bytes.len() - 1 {
    assert!(
      encoded_bytes[i] < encoded_bytes[i + 1],
      "排序同构性校验失败: [{i}] 应小于 [{}]",
      i + 1
    );
  }

  // 验证与 from_raw 的无损等价
  for r in &key_refs {
    let recreated = ZScoreKeyRef::from_raw(r.key_id, r.version, r.raw_score, r.member);
    assert_eq!(*r, recreated);
  }

  OK
}

#[test]
fn test_record_ref_zset_helpers() -> Void {
  info!("测试 RecordRef 对 ZMember 和 ZScore 子键的直接解析能力");

  let key_id = 1234_u64;
  let version = 5678_u64;
  let member = b"item_99";
  let score = 98.765_f64;

  // 1. ZMember 测试
  let member_key = ZSetSubKeyCodec::encode_member_key(key_id, version, member)?;
  let member_record = try_encode_to_vec(0, &member_key, b"value_payload", false)?;
  let rec_ref = RecordRef::from_slice(&member_record)?;

  assert_eq!(rec_ref.bftag(), Some(BfTag::ZMember));
  let parsed_member: ZMemberKeyRef = rec_ref.zmember_key_ref()?;
  assert_eq!(parsed_member.key_id, key_id);
  assert_eq!(parsed_member.version, version);
  assert_eq!(parsed_member.member, member);

  // 2. ZScore 测试
  let score_key = ZSetSubKeyCodec::encode_score_key(key_id, version, score, member)?;
  let score_record = try_encode_to_vec(0, &score_key, b"", false)?;
  let score_rec_ref = RecordRef::from_slice(&score_record)?;

  assert_eq!(score_rec_ref.bftag(), Some(BfTag::ZScore));
  let parsed_score: ZScoreKeyRef = score_rec_ref.zscore_key_ref()?;
  assert_eq!(parsed_score.key_id, key_id);
  assert_eq!(parsed_score.version, version);
  assert_eq!(parsed_score.score.to_bits(), score.to_bits());
  assert_eq!(parsed_score.member, member);

  // 3. 错误 Tag 调用应返回 InvalidKeyTag
  let mut invalid_key = member_key.clone();
  invalid_key[0] = 0x99;
  let invalid_record = try_encode_to_vec(0, &invalid_key, b"", false)?;
  let invalid_rec_ref = RecordRef::from_slice(&invalid_record)?;
  assert!(matches!(
    invalid_rec_ref.zmember_key_ref(),
    Err(Error::InvalidKeyTag(0x99))
  ));

  OK
}

#[test]
fn test_extreme_floats_and_denormals() -> Void {
  info!("测试极值浮点数、极小非正规数 (Subnormal/Denormal) 与 NaN 的保序与位还原");

  let extremes = [
    f64::NAN,
    f64::NEG_INFINITY,
    f64::MIN,
    -1.0e-323,
    -5.0e-324, // 最小绝对值负数 (subnormal)
    -0.0,
    0.0,
    5.0e-324, // 最小绝对值正数 (subnormal)
    1.0e-323,
    f64::MIN_POSITIVE, // 最小正正规数
    f64::MAX,
    f64::INFINITY,
  ];

  for &val in &extremes {
    let enc = encode_order_preserving_f64(val);
    let dec = decode_order_preserving_f64(enc);

    // 位级精确 100% 往返无损还原
    assert_eq!(
      dec.to_bits(),
      val.to_bits(),
      "极值还原位不匹配: {val:?} bits={:#x} -> {dec:?} bits={:#x}",
      val.to_bits(),
      dec.to_bits()
    );
  }

  // 非 NaN 区间保序性检验
  let non_nan_extremes = [
    f64::NEG_INFINITY,
    f64::MIN,
    -1.0e-323,
    -5.0e-324,
    -0.0,
    0.0,
    5.0e-324,
    1.0e-323,
    f64::MIN_POSITIVE,
    f64::MAX,
    f64::INFINITY,
  ];

  for i in 0..non_nan_extremes.len() - 1 {
    let a = non_nan_extremes[i];
    let b = non_nan_extremes[i + 1];
    let enc_a = encode_order_preserving_f64(a);
    let enc_b = encode_order_preserving_f64(b);
    assert!(enc_a < enc_b, "极端数值序校验失败: {a:?} 应小于 {b:?}");
  }

  OK
}
