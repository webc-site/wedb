use core::{
  cmp::Ordering,
  hash::{Hash, Hasher},
};

use wbase::{buf::put_header_payload, float};

use crate::{
  bftag::BfTag,
  buf::stack_heap_buf,
  error::{Error, Result},
  meta::kv_prefix,
};

/// 成员子键定长头部大小（1 字节 BfTag::ZMember + 8 字节 key_id + 8 字节 version = 17 字节）
pub const MEMBER_KEY_HEADER_SIZE: usize = 17;

/// 分值子键定长头部大小（1 字节 BfTag::ZScore + 8 字节 key_id + 8 字节 version + 8 字节 order_preserving_score = 25 字节）
pub const SCORE_KEY_HEADER_SIZE: usize = 25;

/// 子键栈分配最大容量（128 字节，对齐 2 条 64 字节缓存行，消除绝大多数短成员键的堆分配开销）
pub const ZSET_SUBKEY_STACK_CAP: usize = 128;

/// 校验集合成员长度合法性，并计算子键编码后的总字节数（const fn）
#[inline(always)]
const fn check_member_len(member_len: usize, header_size: usize) -> Result<usize> {
  if member_len > u32::MAX as usize - header_size {
    return Err(Error::KeyLengthOverflow(member_len));
  }
  match header_size.checked_add(member_len) {
    Some(len) => Ok(len),
    None => Err(Error::RecordSizeOverflow),
  }
}

/// 将 f64 浮点数转换为大端保序 8 字节数组（底层直接复用 wbase::float::encode_f64）
#[inline(always)]
pub const fn encode_order_preserving_f64(val: f64) -> [u8; 8] {
  float::encode_f64(val)
}

/// 从保序大端 8 字节数组还原 f64 浮点数（底层直接复用 wbase::float::decode_f64）
#[inline(always)]
pub const fn decode_order_preserving_f64(bytes: [u8; 8]) -> f64 {
  float::decode_f64(bytes)
}

/// 有序集合成员子键只读零拷贝切片视图
///
/// 物理二进制结构：
/// `[0x00 (BfTag::ZMember) | key_id: 8B be | version: 8B be | member]`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ZMemberKeyRef<'a> {
  /// 集合全局唯一 ID
  pub key_id: u64,
  /// 逻辑版本号
  pub version: u64,
  /// 成员二进制切片（零拷贝借用）
  pub member: &'a [u8],
}

impl<'a> ZMemberKeyRef<'a> {
  /// 创建成员键只读切片视图（const fn）
  #[inline(always)]
  pub const fn new(key_id: u64, version: u64, member: &'a [u8]) -> Self {
    Self {
      key_id,
      version,
      member,
    }
  }

  /// 从只读切片零拷贝解析成员键视图（const fn）
  #[inline(always)]
  pub const fn from_slice(slice: &'a [u8]) -> Result<Self> {
    ZSetSubKeyCodec::decode_member_key(slice)
  }

  /// 获取定长 17 字节前缀头（const fn）
  #[inline(always)]
  pub const fn header(&self) -> [u8; MEMBER_KEY_HEADER_SIZE] {
    ZSetSubKeyCodec::encode_member_header(self.key_id, self.version)
  }

  /// 获取序列化编码后的总字节数（const fn）
  #[inline(always)]
  pub const fn encoded_len(&self) -> usize {
    MEMBER_KEY_HEADER_SIZE + self.member.len()
  }

  /// 将成员键编码写入目标切片（零堆分配）
  #[inline]
  pub fn write_to_slice(&self, dst: &mut [u8]) -> Result<usize> {
    ZSetSubKeyCodec::encode_member_key_to_slice(self.key_id, self.version, self.member, dst)
  }

  /// 预分配精准容量并编码为全新 Vec<u8>（单次堆分配，长度溢出返回错误）
  #[inline]
  pub fn to_vec(&self) -> Vec<u8> {
    self.try_to_vec().unwrap_or_default()
  }

  /// 尝试编码为全新分配的 Vec<u8>，若长度溢出则返回错误
  #[inline]
  pub fn try_to_vec(&self) -> Result<Vec<u8>> {
    ZSetSubKeyCodec::encode_member_key(self.key_id, self.version, self.member)
  }
}

/// 有序集合分值索引子键只读零拷贝切片视图
///
/// 物理二进制结构：
/// `[0x01 (BfTag::ZScore) | key_id: 8B be | version: 8B be | order_preserving_score: 8B | member]`
#[derive(Debug, Clone, Copy)]
pub struct ZScoreKeyRef<'a> {
  /// 集合全局唯一 ID
  pub key_id: u64,
  /// 逻辑版本号
  pub version: u64,
  /// 还原后的原始分值
  pub score: f64,
  /// 保序编码后的原始 8 字节（大端保序映射）
  pub raw_score: [u8; 8],
  /// 成员二进制切片（零拷贝借用）
  pub member: &'a [u8],
}

impl<'a> ZScoreKeyRef<'a> {
  /// 创建分值键只读切片视图（const fn）
  #[inline(always)]
  pub const fn new(key_id: u64, version: u64, score: f64, member: &'a [u8]) -> Self {
    let raw_score = encode_order_preserving_f64(score);
    Self {
      key_id,
      version,
      score,
      raw_score,
      member,
    }
  }

  /// 从已知原始保序 8 字节创建分值键切片视图（const fn，零冗余位运算）
  #[inline(always)]
  pub const fn from_raw(key_id: u64, version: u64, raw_score: [u8; 8], member: &'a [u8]) -> Self {
    let score = decode_order_preserving_f64(raw_score);
    Self {
      key_id,
      version,
      score,
      raw_score,
      member,
    }
  }

  /// 从只读切片零拷贝解析分值键视图（const fn）
  #[inline(always)]
  pub const fn from_slice(slice: &'a [u8]) -> Result<Self> {
    ZSetSubKeyCodec::decode_score_key(slice)
  }

  /// 获取定长 25 字节前缀头（const fn，直接复用 raw_score，零重编码）
  #[inline(always)]
  pub const fn header(&self) -> [u8; SCORE_KEY_HEADER_SIZE] {
    ZSetSubKeyCodec::encode_score_header_from_raw(self.key_id, self.version, self.raw_score)
  }

  /// 获取序列化编码后的总字节数（const fn）
  #[inline(always)]
  pub const fn encoded_len(&self) -> usize {
    SCORE_KEY_HEADER_SIZE + self.member.len()
  }

  /// 将分值键编码写入目标切片（零堆分配，直接复用 raw_score 免重复浮点编码）
  #[inline]
  pub fn write_to_slice(&self, dst: &mut [u8]) -> Result<usize> {
    let total_len = check_member_len(self.member.len(), SCORE_KEY_HEADER_SIZE)?;
    if dst.len() < total_len {
      return Err(Error::BufferTooShort {
        expected: total_len,
        actual: dst.len(),
      });
    }
    dst[..SCORE_KEY_HEADER_SIZE].copy_from_slice(&self.header());
    dst[SCORE_KEY_HEADER_SIZE..total_len].copy_from_slice(self.member);
    Ok(total_len)
  }

  /// 预分配精准容量并编码为全新 Vec<u8>（单次堆分配，长度溢出返回错误）
  #[inline]
  pub fn to_vec(&self) -> Vec<u8> {
    self.try_to_vec().unwrap_or_default()
  }

  /// 尝试编码为全新分配的 Vec<u8>，若长度溢出则返回错误
  #[inline]
  pub fn try_to_vec(&self) -> Result<Vec<u8>> {
    ZSetSubKeyCodec::encode_score_key(self.key_id, self.version, self.score, self.member)
  }
}

impl<'a> PartialEq for ZScoreKeyRef<'a> {
  #[inline(always)]
  fn eq(&self, other: &Self) -> bool {
    self.key_id == other.key_id
      && self.version == other.version
      && self.raw_score == other.raw_score
      && self.member == other.member
  }
}

impl<'a> Eq for ZScoreKeyRef<'a> {}

impl<'a> PartialOrd for ZScoreKeyRef<'a> {
  #[inline(always)]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<'a> Ord for ZScoreKeyRef<'a> {
  #[inline(always)]
  fn cmp(&self, other: &Self) -> Ordering {
    (self.key_id, self.version, &self.raw_score, self.member).cmp(&(
      other.key_id,
      other.version,
      &other.raw_score,
      other.member,
    ))
  }
}

impl<'a> Hash for ZScoreKeyRef<'a> {
  #[inline(always)]
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.key_id.hash(state);
    self.version.hash(state);
    self.raw_score.hash(state);
    self.member.hash(state);
  }
}

// 有序集合子键高性能缓冲区：短 member 优先走 128 字节栈分配，超长自动回退到堆
// 栈容量 128 字节刚好对齐 2 条 64 字节 CPU 缓存行（Cache Line），消除高频短键堆分配
stack_heap_buf!(ZSetSubKeyBuf, ZSET_SUBKEY_STACK_CAP);

impl ZSetSubKeyBuf {
  /// 从成员参数直接构造优先栈分配的子键缓冲区
  #[inline]
  pub fn from_member(key_id: u64, version: u64, member: &[u8]) -> Result<Self> {
    ZSetSubKeyCodec::encode_member_key_buf(key_id, version, member)
  }

  /// 从分值参数直接构造优先栈分配的子键缓冲区
  #[inline]
  pub fn from_score(key_id: u64, version: u64, score: f64, member: &[u8]) -> Result<Self> {
    ZSetSubKeyCodec::encode_score_key_buf(key_id, version, score, member)
  }
}

/// 有序集合打平子键与保序分值无锁静态编解码器
pub struct ZSetSubKeyCodec;

impl ZSetSubKeyCodec {
  /// 编码 17 字节定长成员子键前缀头（const fn）
  #[inline(always)]
  pub const fn encode_member_header(key_id: u64, version: u64) -> [u8; MEMBER_KEY_HEADER_SIZE] {
    kv_prefix(BfTag::ZMember.as_u8(), key_id, version)
  }

  /// 编码 17 字节定长分值子键公共前缀头（const fn）
  #[inline(always)]
  pub const fn encode_score_prefix(key_id: u64, version: u64) -> [u8; MEMBER_KEY_HEADER_SIZE] {
    kv_prefix(BfTag::ZScore.as_u8(), key_id, version)
  }

  /// 从已知原始保序 8 字节编码 25 字节定长分值子键前缀头（const fn, 零循环展开）
  #[inline(always)]
  pub const fn encode_score_header_from_raw(
    key_id: u64,
    version: u64,
    raw_score: [u8; 8],
  ) -> [u8; SCORE_KEY_HEADER_SIZE] {
    let [
      p0,
      p1,
      p2,
      p3,
      p4,
      p5,
      p6,
      p7,
      p8,
      p9,
      p10,
      p11,
      p12,
      p13,
      p14,
      p15,
      p16,
    ] = Self::encode_score_prefix(key_id, version);
    let [s0, s1, s2, s3, s4, s5, s6, s7] = raw_score;
    [
      p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, s0, s1, s2, s3,
      s4, s5, s6, s7,
    ]
  }

  /// 编码 25 字节定长分值子键前缀头（const fn）
  #[inline(always)]
  pub const fn encode_score_header(
    key_id: u64,
    version: u64,
    score: f64,
  ) -> [u8; SCORE_KEY_HEADER_SIZE] {
    Self::encode_score_header_from_raw(key_id, version, encode_order_preserving_f64(score))
  }

  /// 从只读切片快速解码 17 字节成员键前缀头 (key_id, version)（const fn, 单次模式匹配零越界检查）
  #[inline]
  pub const fn decode_member_header(slice: &[u8]) -> Result<(u64, u64)> {
    match slice {
      [
        tag,
        k0,
        k1,
        k2,
        k3,
        k4,
        k5,
        k6,
        k7,
        v0,
        v1,
        v2,
        v3,
        v4,
        v5,
        v6,
        v7,
        ..,
      ] => {
        if *tag != BfTag::ZMember.as_u8() {
          return Err(Error::InvalidKeyTag(*tag));
        }
        let key_id = u64::from_be_bytes([*k0, *k1, *k2, *k3, *k4, *k5, *k6, *k7]);
        let version = u64::from_be_bytes([*v0, *v1, *v2, *v3, *v4, *v5, *v6, *v7]);
        Ok((key_id, version))
      }
      _ => Err(Error::BufferTooShort {
        expected: MEMBER_KEY_HEADER_SIZE,
        actual: slice.len(),
      }),
    }
  }

  /// 从只读切片快速解码 25 字节分值键前缀头原始数据 (key_id, version, raw_score)（const fn, 单次模式匹配零越界检查）
  #[inline]
  pub const fn decode_score_header_raw(slice: &[u8]) -> Result<(u64, u64, [u8; 8])> {
    match slice {
      [
        tag,
        k0,
        k1,
        k2,
        k3,
        k4,
        k5,
        k6,
        k7,
        v0,
        v1,
        v2,
        v3,
        v4,
        v5,
        v6,
        v7,
        s0,
        s1,
        s2,
        s3,
        s4,
        s5,
        s6,
        s7,
        ..,
      ] => {
        if *tag != BfTag::ZScore.as_u8() {
          return Err(Error::InvalidKeyTag(*tag));
        }
        let key_id = u64::from_be_bytes([*k0, *k1, *k2, *k3, *k4, *k5, *k6, *k7]);
        let version = u64::from_be_bytes([*v0, *v1, *v2, *v3, *v4, *v5, *v6, *v7]);
        let raw_score = [*s0, *s1, *s2, *s3, *s4, *s5, *s6, *s7];
        Ok((key_id, version, raw_score))
      }
      _ => Err(Error::BufferTooShort {
        expected: SCORE_KEY_HEADER_SIZE,
        actual: slice.len(),
      }),
    }
  }

  /// 从只读切片快速解码 25 字节分值键前缀头 (key_id, version, score)（const fn）
  #[inline]
  pub const fn decode_score_header(slice: &[u8]) -> Result<(u64, u64, f64)> {
    match Self::decode_score_header_raw(slice) {
      Ok((key_id, version, raw_score)) => {
        Ok((key_id, version, decode_order_preserving_f64(raw_score)))
      }
      Err(e) => Err(e),
    }
  }

  /// 编码有序集合成员子键至目标切片（零堆分配）
  #[inline]
  pub fn encode_member_key_to_slice(
    key_id: u64,
    version: u64,
    member: &[u8],
    dst: &mut [u8],
  ) -> Result<usize> {
    let total_len = check_member_len(member.len(), MEMBER_KEY_HEADER_SIZE)?;
    let header = Self::encode_member_header(key_id, version);
    put_header_payload(dst, &header, member).ok_or(Error::BufferTooShort {
      expected: total_len,
      actual: dst.len(),
    })
  }

  /// 编码有序集合成员子键为全新 Vec<u8>（单次堆分配）
  #[inline]
  pub fn encode_member_key(key_id: u64, version: u64, member: &[u8]) -> Result<Vec<u8>> {
    let total_len = check_member_len(member.len(), MEMBER_KEY_HEADER_SIZE)?;
    let mut vec = Vec::with_capacity(total_len);
    let header = Self::encode_member_header(key_id, version);
    vec.extend_from_slice(&header);
    vec.extend_from_slice(member);
    Ok(vec)
  }

  /// 编码有序集合成员子键为优先栈分配的缓冲区（消除短 member 堆分配）
  #[inline]
  pub fn encode_member_key_buf(key_id: u64, version: u64, member: &[u8]) -> Result<ZSetSubKeyBuf> {
    check_member_len(member.len(), MEMBER_KEY_HEADER_SIZE)?;
    Ok(ZSetSubKeyBuf::from_header_parts(
      &Self::encode_member_header(key_id, version),
      member,
    ))
  }

  /// 零拷贝解码有序集合成员子键（const fn）
  #[inline]
  pub const fn decode_member_key<'a>(slice: &'a [u8]) -> Result<ZMemberKeyRef<'a>> {
    let (key_id, version) = match Self::decode_member_header(slice) {
      Ok(v) => v,
      Err(e) => return Err(e),
    };
    let member = slice.split_at(MEMBER_KEY_HEADER_SIZE).1;
    if member.len() > u32::MAX as usize - MEMBER_KEY_HEADER_SIZE {
      return Err(Error::KeyLengthOverflow(member.len()));
    }
    Ok(ZMemberKeyRef {
      key_id,
      version,
      member,
    })
  }

  /// 编码有序集合分值子键至目标切片（零堆分配）
  #[inline]
  pub fn encode_score_key_to_slice(
    key_id: u64,
    version: u64,
    score: f64,
    member: &[u8],
    dst: &mut [u8],
  ) -> Result<usize> {
    let total_len = check_member_len(member.len(), SCORE_KEY_HEADER_SIZE)?;
    let header = Self::encode_score_header(key_id, version, score);
    put_header_payload(dst, &header, member).ok_or(Error::BufferTooShort {
      expected: total_len,
      actual: dst.len(),
    })
  }

  /// 编码有序集合分值子键为全新 Vec<u8>（单次堆分配）
  #[inline]
  pub fn encode_score_key(key_id: u64, version: u64, score: f64, member: &[u8]) -> Result<Vec<u8>> {
    let total_len = check_member_len(member.len(), SCORE_KEY_HEADER_SIZE)?;
    let mut vec = Vec::with_capacity(total_len);
    let header = Self::encode_score_header(key_id, version, score);
    vec.extend_from_slice(&header);
    vec.extend_from_slice(member);
    Ok(vec)
  }

  /// 编码有序集合分值子键为优先栈分配的缓冲区（消除短 member 堆分配）
  #[inline]
  pub fn encode_score_key_buf(
    key_id: u64,
    version: u64,
    score: f64,
    member: &[u8],
  ) -> Result<ZSetSubKeyBuf> {
    check_member_len(member.len(), SCORE_KEY_HEADER_SIZE)?;
    Ok(ZSetSubKeyBuf::from_header_parts(
      &Self::encode_score_header(key_id, version, score),
      member,
    ))
  }

  /// 零拷贝解码有序集合分值子键（const fn）
  #[inline]
  pub const fn decode_score_key<'a>(slice: &'a [u8]) -> Result<ZScoreKeyRef<'a>> {
    let (key_id, version, raw_score) = match Self::decode_score_header_raw(slice) {
      Ok(v) => v,
      Err(e) => return Err(e),
    };
    let score = decode_order_preserving_f64(raw_score);
    let member = slice.split_at(SCORE_KEY_HEADER_SIZE).1;
    if member.len() > u32::MAX as usize - SCORE_KEY_HEADER_SIZE {
      return Err(Error::KeyLengthOverflow(member.len()));
    }
    Ok(ZScoreKeyRef {
      key_id,
      version,
      score,
      raw_score,
      member,
    })
  }
}
