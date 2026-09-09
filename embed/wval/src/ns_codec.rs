use core::{
  borrow::Borrow,
  cmp::Ordering,
  hash::{Hash, Hasher},
  mem::{align_of, size_of},
  ops::Deref,
};

pub use wbase::varint::{
  MAX_VARINT_LEN, VARINT_1B_FIRST_BYTE_LIMIT, VARINT_1B_FIRST_BYTE_MAX, VARINT_1B_MAX,
  VARINT_2B_FIRST_BYTE_MAX, VARINT_2B_MARKER, VARINT_2B_MAX, VARINT_2B_PAYLOAD_MASK,
  VARINT_3B_FIRST_BYTE_MAX, VARINT_3B_MARKER, VARINT_3B_MAX, VARINT_3B_PAYLOAD_MASK,
  VARINT_4B_FIRST_BYTE_MAX, VARINT_4B_MARKER, VARINT_4B_MAX, VARINT_4B_PAYLOAD_MASK,
  VARINT_9B_MARKER, VARINT_LEN_LUT, VarintError, decode_u64, encode_u64_to_array, varint_len,
};
use wbase::{simd::fast_key_eq, stack_heap_buf};

use crate::{
  error::{Error, Result},
  meta::{SUBKEY_HEADER_SIZE, SubKeyCodec},
  tag::KeyTag,
};
/// 方案 A 最简会话前缀字节长度 (ns=1B + db=1B = 2B，覆盖 99.9% 业务场景)
pub const MIN_SESSION_PREFIX_LEN: usize = 2;

/// 方案 A 子键公共元数据头字节数 (key_id: 8B + version: 8B = 16B)
pub const SUBKEY_META_HEADER_LEN: usize = 16;
/// 方案 A 集合分块子键 chunk_id 长度 (4 字节)
pub const CHUNK_ID_LEN: usize = 4;
/// 单个 64 位大端整型字节数 (8 字节)
pub const U64_BYTE_LEN: usize = 8;
/// 方案 A 最简物理子键最小字节数 (ns: 1B + db: 1B + KeyTag: 1B + key_id: 8B + version: 8B = 19B)
pub const MIN_SUBKEY_LEN: usize = MIN_SESSION_PREFIX_LEN + KeyTag::TAG_LEN + SUBKEY_META_HEADER_LEN;
/// 方案 A 最简集合分块物理子键最小字节数 (19B + chunk_id: 4B = 23B)
pub const MIN_CHUNK_KEY_LEN: usize = MIN_SUBKEY_LEN + CHUNK_ID_LEN;

/// 会话前缀最大编码字节数 (NsVarint 9B + DbVarint 9B = 18 字节)
pub const MAX_SESSION_PREFIX_LEN: usize = 18;

/// 栈分配键容量上限 (62 字节，留出 1 字节长度与 1 字节枚举鉴别符，刚好满足 64 字节单缓存行)
pub const STACK_KEY_CAP: usize = 62;

/// 预计算的会话 (Namespace, DB) 变长前缀定长缓冲区
///
/// 结构紧凑（18 字节固定数组 + 1 字节长度 = 19 字节），支持 `Copy`。
/// 在客户端连接或执行 `SELECT <db>` 时单次计算常驻会话，后续热路径彻底免去变长编码开销。
#[derive(Clone, Copy, Debug)]
pub struct SessionPrefixBuf {
  buf: [u8; MAX_SESSION_PREFIX_LEN],
  len: u8,
}

impl SessionPrefixBuf {
  /// 编码命名空间与数据库编号，生成会话前缀 (const fn)
  #[inline]
  pub const fn new(ns: u64, db: u64) -> Self {
    let (buf, len) = NamespaceDbCodec::encode_session_prefix_to_array(ns, db);
    Self {
      buf,
      len: len as u8,
    }
  }

  /// 从已知切片安全解析并验证会话前缀 (const fn)
  #[inline]
  pub const fn from_slice(slice: &[u8]) -> Result<Self> {
    if let [b0, b1] = *slice
      && b0 < VARINT_1B_FIRST_BYTE_LIMIT
      && b1 < VARINT_1B_FIRST_BYTE_LIMIT
    {
      let mut buf = [0u8; MAX_SESSION_PREFIX_LEN];
      buf[0] = b0;
      buf[1] = b1;
      return Ok(Self { buf, len: 2 });
    }
    if slice.is_empty() || slice.len() > MAX_SESSION_PREFIX_LEN {
      return Err(Error::BufferTooShort {
        expected: 1,
        actual: slice.len(),
      });
    }
    let (_, ns_len) = match NamespaceDbCodec::decode_varint(slice) {
      Ok(v) => v,
      Err(e) => return Err(e),
    };
    let rest = match slice.split_at_checked(ns_len) {
      Some((_, r)) => r,
      None => {
        return Err(Error::BufferTooShort {
          expected: ns_len,
          actual: slice.len(),
        });
      }
    };
    let (_, db_len) = match NamespaceDbCodec::decode_varint(rest) {
      Ok(v) => v,
      Err(e) => return Err(e),
    };
    let total_len = ns_len + db_len;
    if slice.len() != total_len {
      return Err(Error::NonCanonicalEncoding);
    }
    let mut buf = [0u8; MAX_SESSION_PREFIX_LEN];
    let mut i = 0;
    while i < total_len {
      buf[i] = slice[i];
      i += 1;
    }
    Ok(Self {
      buf,
      len: total_len as u8,
    })
  }

  /// 解码会话前缀中的命名空间与数据库编号
  #[inline]
  pub fn decode(&self) -> Result<(u64, u64)> {
    if self.len == 2 {
      return Ok((self.buf[0] as u64, self.buf[1] as u64));
    }
    let slice = self.as_slice();
    let (ns, ns_len) = NamespaceDbCodec::decode_varint(slice)?;
    let (_, rest) = slice
      .split_at_checked(ns_len)
      .ok_or(Error::BufferTooShort {
        expected: ns_len,
        actual: slice.len(),
      })?;
    let (db, _) = NamespaceDbCodec::decode_varint(rest)?;
    Ok((ns, db))
  }

  /// 获取会话前缀只读切片 (const fn)
  #[inline(always)]
  pub const fn as_slice(&self) -> &[u8] {
    let len = if (self.len as usize) < MAX_SESSION_PREFIX_LEN {
      self.len as usize
    } else {
      MAX_SESSION_PREFIX_LEN
    };
    self.buf.split_at(len).0
  }

  /// 获取前缀字节长度 (const fn)
  #[inline(always)]
  pub const fn len(&self) -> usize {
    self.len as usize
  }

  /// 是否为空（理论上最少占用 2 字节，恒为 false）
  #[inline(always)]
  pub const fn is_empty(&self) -> bool {
    self.len == 0
  }
}

impl Default for SessionPrefixBuf {
  #[inline]
  fn default() -> Self {
    Self::new(0, 0)
  }
}

impl Deref for SessionPrefixBuf {
  type Target = [u8];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

impl AsRef<[u8]> for SessionPrefixBuf {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    self.as_slice()
  }
}

impl Borrow<[u8]> for SessionPrefixBuf {
  #[inline(always)]
  fn borrow(&self) -> &[u8] {
    self.as_slice()
  }
}

impl PartialEq for SessionPrefixBuf {
  #[inline(always)]
  fn eq(&self, other: &Self) -> bool {
    fast_key_eq(self.as_slice(), other.as_slice())
  }
}

impl Eq for SessionPrefixBuf {}

impl PartialOrd for SessionPrefixBuf {
  #[inline(always)]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for SessionPrefixBuf {
  #[inline(always)]
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_slice().cmp(other.as_slice())
  }
}

impl Hash for SessionPrefixBuf {
  #[inline(always)]
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_slice().hash(state);
  }
}

impl PartialEq<[u8]> for SessionPrefixBuf {
  #[inline(always)]
  fn eq(&self, other: &[u8]) -> bool {
    fast_key_eq(self.as_slice(), other)
  }
}

impl PartialEq<&[u8]> for SessionPrefixBuf {
  #[inline(always)]
  fn eq(&self, other: &&[u8]) -> bool {
    fast_key_eq(self.as_slice(), other)
  }
}

// 物理键内部存储排布表示
stack_heap_buf!(KeyBufRepr, STACK_KEY_CAP);

/// 64 字节单缓存行对齐物理键缓冲区
///
/// 严格对齐 CPU L1 缓存行（64 Bytes），彻底消除多核缓存颠簸与伪共享。
/// 方案 A 排布：`[NsVarint] + [DbVarint] + [KeyTag: 1B] + [Payload]`。
/// 在典型 Redis 场景下（ns < 128, db < 128, key <= 59B），键总长 <= 62B，全程零堆分配。
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct TaggedKeyBuf {
  /// 底层物理键存储排布（公开字段供底层引擎与测试直接访问）
  pub inner: KeyBufRepr,
}

// 编译期静态断言：TaggedKeyBuf 严格 64 字节尺寸与 64 字节 L1 缓存行对齐
const _: () = assert!(size_of::<TaggedKeyBuf>() == 64);
const _: () = assert!(align_of::<TaggedKeyBuf>() == 64);
const _: () = assert!(size_of::<KeyBufRepr>() <= 64);
const _: () = assert!(size_of::<SessionPrefixBuf>() == 19);

impl TaggedKeyBuf {
  /// 判断是否使用栈缓冲区存储
  #[inline(always)]
  pub const fn is_stack(&self) -> bool {
    matches!(self.inner, KeyBufRepr::Stack(..))
  }

  /// 判断是否已回退至堆内存存储
  #[inline(always)]
  pub const fn is_heap(&self) -> bool {
    matches!(self.inner, KeyBufRepr::Heap(..))
  }

  /// 构造空键缓冲区 (const fn, 默认栈存储)
  #[inline(always)]
  pub const fn new() -> Self {
    Self::from_stack([0u8; STACK_KEY_CAP], 0)
  }

  /// 从栈分配构造
  #[inline(always)]
  pub const fn from_stack(buf: [u8; STACK_KEY_CAP], len: u8) -> Self {
    Self {
      inner: KeyBufRepr::Stack(buf, len),
    }
  }

  /// 从堆分配构造
  #[inline(always)]
  pub const fn from_heap(vec: Vec<u8>) -> Self {
    Self {
      inner: KeyBufRepr::Heap(vec),
    }
  }

  /// 获取键的只读切片
  #[inline(always)]
  pub fn as_slice(&self) -> &[u8] {
    self.inner.as_slice()
  }

  /// 获取键的字节长度
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.inner.len()
  }

  /// 判断键是否为空
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.inner.is_empty()
  }

  /// 转换为持有所有权的 `Vec<u8>`
  #[inline]
  pub fn into_vec(self) -> Vec<u8> {
    match self.inner {
      KeyBufRepr::Stack(buf, len) => {
        let len = (len as usize).min(STACK_KEY_CAP);
        buf[..len].to_vec()
      }
      KeyBufRepr::Heap(vec) => vec,
    }
  }
}

impl Default for TaggedKeyBuf {
  #[inline(always)]
  fn default() -> Self {
    Self::new()
  }
}

impl Deref for TaggedKeyBuf {
  type Target = [u8];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

impl AsRef<[u8]> for TaggedKeyBuf {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    self.as_slice()
  }
}

impl Borrow<[u8]> for TaggedKeyBuf {
  #[inline(always)]
  fn borrow(&self) -> &[u8] {
    self.as_slice()
  }
}

impl PartialEq for TaggedKeyBuf {
  #[inline(always)]
  fn eq(&self, other: &Self) -> bool {
    fast_key_eq(self.as_slice(), other.as_slice())
  }
}

impl Eq for TaggedKeyBuf {}

impl PartialOrd for TaggedKeyBuf {
  #[inline(always)]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for TaggedKeyBuf {
  #[inline(always)]
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_slice().cmp(other.as_slice())
  }
}

impl Hash for TaggedKeyBuf {
  #[inline(always)]
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_slice().hash(state);
  }
}

impl PartialEq<[u8]> for TaggedKeyBuf {
  #[inline(always)]
  fn eq(&self, other: &[u8]) -> bool {
    fast_key_eq(self.as_slice(), other)
  }
}

impl PartialEq<&[u8]> for TaggedKeyBuf {
  #[inline(always)]
  fn eq(&self, other: &&[u8]) -> bool {
    fast_key_eq(self.as_slice(), other)
  }
}

impl From<Vec<u8>> for TaggedKeyBuf {
  #[inline]
  fn from(vec: Vec<u8>) -> Self {
    if vec.len() <= STACK_KEY_CAP {
      let mut buf = [0u8; STACK_KEY_CAP];
      buf[..vec.len()].copy_from_slice(&vec);
      Self::from_stack(buf, vec.len() as u8)
    } else {
      Self::from_heap(vec)
    }
  }
}

impl From<&[u8]> for TaggedKeyBuf {
  #[inline]
  fn from(slice: &[u8]) -> Self {
    if slice.len() <= STACK_KEY_CAP {
      let mut buf = [0u8; STACK_KEY_CAP];
      buf[..slice.len()].copy_from_slice(slice);
      Self::from_stack(buf, slice.len() as u8)
    } else {
      Self::from_heap(slice.to_vec())
    }
  }
}

impl From<TaggedKeyBuf> for Vec<u8> {
  #[inline]
  fn from(buf: TaggedKeyBuf) -> Self {
    buf.into_vec()
  }
}

/// 内部高效写入前缀、标签与有效载荷到目标切片
#[inline(always)]
fn write_key_parts(prefix: &[u8], tag: KeyTag, payload: &[u8], dst: &mut [u8]) {
  let prefix_len = prefix.len();
  dst[..prefix_len].copy_from_slice(prefix);
  dst[prefix_len] = tag as u8;
  dst[prefix_len + KeyTag::TAG_LEN..prefix_len + KeyTag::TAG_LEN + payload.len()]
    .copy_from_slice(payload);
}

/// 方案 A（命名空间先行，Namespace-First）统一物理键编解码器
///
/// 全库统一物理键结构：`[NsVarint] + [DbVarint] + [KeyTag: 1B] + [Payload]`
///
/// 特性：
/// 1. 严格大端字典序保序：OPPV 编码保证数值与二进制切片单调一致。
/// 2. 局部性极高：同会话的所有键具有相同前缀，热路径下只需单次 `strip_prefix(session_prefix)`。
/// 3. 全局强正交：String 与 Meta 拥有专属 `KeyTag`（0x00 与 0x01），彻底消除裸键与长度猜测。
pub struct NamespaceDbCodec;

impl NamespaceDbCodec {
  /// 租户命名空间合法上限（18446744073709550591）
  pub const MAX_TENANT_NAMESPACE: u64 = 18446744073709550591;

  /// 计算单个 u64 数值经 OPPV 编码后占用的字节数 (1..=9, const fn)
  #[inline(always)]
  pub const fn varint_len(val: u64) -> usize {
    varint_len(val)
  }

  /// 计算指定 (ns, db) 会话前缀所占用的总字节数 (2..=18, const fn)
  #[inline(always)]
  pub const fn session_prefix_len(ns: u64, db: u64) -> usize {
    Self::varint_len(ns) + Self::varint_len(db)
  }

  /// 计算完整物理键的总长度 (const fn)
  #[inline(always)]
  pub const fn key_len(ns: u64, db: u64, payload_len: usize) -> usize {
    Self::session_prefix_len(ns, db) + KeyTag::TAG_LEN + payload_len
  }

  /// 编码单一 u64 为 OPPV 变长字节数组 (const fn, 零堆分配)
  #[inline(always)]
  pub const fn encode_varint_to_array(val: u64) -> ([u8; MAX_VARINT_LEN], usize) {
    encode_u64_to_array(val)
  }

  /// 编码指定 (ns, db) 为定长数组与有效长度 (const fn)
  #[inline]
  pub const fn encode_session_prefix_to_array(
    ns: u64,
    db: u64,
  ) -> ([u8; MAX_SESSION_PREFIX_LEN], usize) {
    if ns < VARINT_1B_MAX && db < VARINT_1B_MAX {
      let mut buf = [0u8; MAX_SESSION_PREFIX_LEN];
      buf[0] = ns as u8;
      buf[1] = db as u8;
      return (buf, 2);
    }
    let (ns_buf, ns_len) = Self::encode_varint_to_array(ns);
    let (db_buf, db_len) = Self::encode_varint_to_array(db);
    let mut buf = [0u8; MAX_SESSION_PREFIX_LEN];
    let mut i = 0;
    while i < ns_len {
      buf[i] = ns_buf[i];
      i += 1;
    }
    let mut j = 0;
    while j < db_len {
      buf[ns_len + j] = db_buf[j];
      j += 1;
    }
    (buf, ns_len + db_len)
  }

  /// 编码单一 u64 为 OPPV 变长字节，返回实际写入字节数
  #[inline]
  pub fn encode_varint(val: u64, dst: &mut [u8]) -> usize {
    let (arr, len) = encode_u64_to_array(val);
    debug_assert!(dst.len() >= len, "目标缓冲区空间不足以写入变长整型");
    dst[..len].copy_from_slice(&arr[..len]);
    len
  }

  /// 从首字节判定单个 OPPV 变长整型的预期字节数 (const fn, 查表 0 分支, 零回溯)
  #[inline(always)]
  pub const fn varint_len_from_byte(first: u8) -> Option<usize> {
    let len = VARINT_LEN_LUT[first as usize] as usize;
    if len != 0 { Some(len) } else { None }
  }

  /// 从首字节快速提取变长整型长度（0 表示非法，常数时间 1 条指令）
  #[inline(always)]
  pub const fn varint_len_fast(first: u8) -> usize {
    VARINT_LEN_LUT[first as usize] as usize
  }

  /// 单步自定界解码单一 OPPV 数值 (首字节确定预期长度，零回溯且报错精准, const fn)
  #[inline]
  pub const fn decode_varint(slice: &[u8]) -> Result<(u64, usize)> {
    match decode_u64(slice) {
      Ok(v) => Ok(v),
      Err(VarintError::BufferTooShort { expected, actual }) => {
        Err(Error::BufferTooShort { expected, actual })
      }
      Err(VarintError::NonCanonical) => Err(Error::NonCanonicalEncoding),
    }
  }

  /// 编码指定 (ns, db) 会话前缀 (const fn)
  #[inline(always)]
  pub const fn encode_session_prefix(ns: u64, db: u64) -> SessionPrefixBuf {
    SessionPrefixBuf::new(ns, db)
  }

  /// 构造基于指定会话前缀切片的完整物理键缓冲区（复用前缀，零冗余）
  #[inline]
  pub fn encode_with_session_prefix(prefix: &[u8], tag: KeyTag, payload: &[u8]) -> TaggedKeyBuf {
    let total_len = prefix.len() + KeyTag::TAG_LEN + payload.len();
    if total_len <= STACK_KEY_CAP {
      let mut buf = [0u8; STACK_KEY_CAP];
      write_key_parts(prefix, tag, payload, &mut buf);
      TaggedKeyBuf::from_stack(buf, total_len as u8)
    } else {
      let mut vec = Vec::with_capacity(total_len);
      vec.extend_from_slice(prefix);
      vec.push(tag as u8);
      vec.extend_from_slice(payload);
      TaggedKeyBuf::from_heap(vec)
    }
  }

  /// 构造方案 A 物理键缓冲区: `[NsVarint] + [DbVarint] + [KeyTag: 1B] + [Payload]`
  ///
  /// 优先利用预计算的会话前缀 (SessionPrefixBuf)，消除重复分支判定与中间栈数组搬运 (1:1 对标 ns.md 规范)
  #[inline]
  pub fn encode_tagged_key(ns: u64, db: u64, tag: KeyTag, payload: &[u8]) -> TaggedKeyBuf {
    let prefix = SessionPrefixBuf::new(ns, db);
    Self::encode_with_session_prefix(prefix.as_slice(), tag, payload)
  }

  /// 便捷方法：编码普通字符串键 (KeyTag::String = 0x00)
  #[inline(always)]
  pub fn encode_string_key(ns: u64, db: u64, user_key: &[u8]) -> TaggedKeyBuf {
    Self::encode_tagged_key(ns, db, KeyTag::String, user_key)
  }

  /// 便捷方法：编码集合元数据键 (KeyTag::Meta = 0x01)
  #[inline(always)]
  pub fn encode_meta_key(ns: u64, db: u64, user_key: &[u8]) -> TaggedKeyBuf {
    Self::encode_tagged_key(ns, db, KeyTag::Meta, user_key)
  }

  /// 便捷方法：编码方案 A 集合打平子键
  /// 结构：`[NsVarint] + [DbVarint] + [KeyTag: 1B] + [key_id: 8B be] + [version: 8B be] + [field]`
  #[inline]
  pub fn encode_sub_key(
    ns: u64,
    db: u64,
    tag: KeyTag,
    key_id: u64,
    version: u64,
    field: &[u8],
  ) -> TaggedKeyBuf {
    let prefix = SessionPrefixBuf::new(ns, db);
    Self::encode_sub_key_with_prefix(prefix.as_slice(), tag, key_id, version, field)
  }

  /// 便捷方法：基于已知会话前缀编码方案 A 集合打平子键（栈优先零堆分配）
  #[inline]
  pub fn encode_sub_key_with_prefix(
    prefix: &[u8],
    tag: KeyTag,
    key_id: u64,
    version: u64,
    field: &[u8],
  ) -> TaggedKeyBuf {
    let payload_len = SUBKEY_META_HEADER_LEN + field.len();
    let total_len = prefix.len() + KeyTag::TAG_LEN + payload_len;
    let k = key_id.to_be_bytes();
    let v = version.to_be_bytes();

    if total_len <= STACK_KEY_CAP {
      let mut buf = [0u8; STACK_KEY_CAP];
      let p_len = prefix.len();
      buf[..p_len].copy_from_slice(prefix);
      buf[p_len] = tag as u8;
      let hdr_start = p_len + KeyTag::TAG_LEN;
      buf[hdr_start..hdr_start + U64_BYTE_LEN].copy_from_slice(&k);
      buf[hdr_start + U64_BYTE_LEN..hdr_start + SUBKEY_META_HEADER_LEN].copy_from_slice(&v);
      buf[hdr_start + SUBKEY_META_HEADER_LEN..total_len].copy_from_slice(field);
      TaggedKeyBuf::from_stack(buf, total_len as u8)
    } else {
      let mut vec = Vec::with_capacity(total_len);
      vec.extend_from_slice(prefix);
      vec.push(tag as u8);
      vec.extend_from_slice(&k);
      vec.extend_from_slice(&v);
      vec.extend_from_slice(field);
      TaggedKeyBuf::from_heap(vec)
    }
  }

  /// 便捷方法：编码方案 A 集合分块子键
  /// 结构：`[NsVarint] + [DbVarint] + [KeyTag: 1B] + [key_id: 8B be] + [version: 8B be] + [chunk_id: 4B be]`
  #[inline]
  pub fn encode_chunk_key(
    ns: u64,
    db: u64,
    tag: KeyTag,
    key_id: u64,
    version: u64,
    chunk_id: u32,
  ) -> TaggedKeyBuf {
    let prefix = SessionPrefixBuf::new(ns, db);
    Self::encode_chunk_key_with_prefix(prefix.as_slice(), tag, key_id, version, chunk_id)
  }

  /// 便捷方法：基于已知会话前缀编码方案 A 集合分块子键（栈优先零堆分配）
  #[inline]
  pub fn encode_chunk_key_with_prefix(
    prefix: &[u8],
    tag: KeyTag,
    key_id: u64,
    version: u64,
    chunk_id: u32,
  ) -> TaggedKeyBuf {
    Self::encode_sub_key_with_prefix(prefix, tag, key_id, version, &chunk_id.to_be_bytes())
  }

  /// 基于已知会话前缀在栈上构造物理键并执行闭包（短键全程零堆分配）
  #[inline]
  pub fn with_session_prefix<R>(
    prefix: &[u8],
    tag: KeyTag,
    payload: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> R {
    let buf = Self::encode_with_session_prefix(prefix, tag, payload);
    f(buf.as_slice())
  }

  /// 在栈缓冲区上零堆分配构造物理键并执行闭包（用户键 <= 59B 全程零分配）
  #[inline]
  pub fn with_tagged_key<R>(
    ns: u64,
    db: u64,
    tag: KeyTag,
    payload: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> R {
    let buf = Self::encode_tagged_key(ns, db, tag, payload);
    f(buf.as_slice())
  }

  /// 便捷闭包方法：普通字符串键
  #[inline(always)]
  pub fn with_string_key<R>(ns: u64, db: u64, user_key: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
    Self::with_tagged_key(ns, db, KeyTag::String, user_key, f)
  }

  /// 便捷闭包方法：集合元数据键
  #[inline(always)]
  pub fn with_meta_key<R>(ns: u64, db: u64, user_key: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
    Self::with_tagged_key(ns, db, KeyTag::Meta, user_key, f)
  }

  /// 将物理键原位直接写入目标预分配切片（零堆分配，零中间栈拷贝）
  #[inline]
  pub fn encode_to_slice(
    ns: u64,
    db: u64,
    tag: KeyTag,
    payload: &[u8],
    dst: &mut [u8],
  ) -> Result<usize> {
    let total_len = Self::key_len(ns, db, payload.len());
    if dst.len() < total_len {
      return Err(Error::BufferTooShort {
        expected: total_len,
        actual: dst.len(),
      });
    }
    let ns_len = Self::encode_varint(ns, dst);
    let db_len = Self::encode_varint(db, &mut dst[ns_len..]);
    let prefix_len = ns_len + db_len;
    dst[prefix_len] = tag as u8;
    dst[prefix_len + KeyTag::TAG_LEN..total_len].copy_from_slice(payload);
    Ok(total_len)
  }

  /// 将包含已知前缀的物理键写入目标切片
  #[inline]
  pub fn encode_with_session_prefix_to_slice(
    prefix: &[u8],
    tag: KeyTag,
    payload: &[u8],
    dst: &mut [u8],
  ) -> Result<usize> {
    let total_len = prefix.len() + KeyTag::TAG_LEN + payload.len();
    if dst.len() < total_len {
      return Err(Error::BufferTooShort {
        expected: total_len,
        actual: dst.len(),
      });
    }
    write_key_parts(prefix, tag, payload, dst);
    Ok(total_len)
  }

  /// 从完整物理键中解码出 `(ns, db, tag, payload)` (const fn)
  #[inline]
  pub const fn decode_tagged_key(key: &[u8]) -> Result<(u64, u64, KeyTag, &[u8])> {
    match key {
      [ns_b, db_b, tag_byte, payload @ ..]
        if *ns_b < VARINT_1B_FIRST_BYTE_LIMIT && *db_b < VARINT_1B_FIRST_BYTE_LIMIT =>
      {
        let tag = match KeyTag::from_u8(*tag_byte) {
          Some(t) => t,
          None => return Err(Error::InvalidKeyTag(*tag_byte)),
        };
        Ok((*ns_b as u64, *db_b as u64, tag, payload))
      }
      _ => {
        let (ns, ns_len) = match Self::decode_varint(key) {
          Ok(v) => v,
          Err(e) => return Err(e),
        };
        let rest = match key.split_at_checked(ns_len) {
          Some((_, r)) => r,
          None => {
            return Err(Error::BufferTooShort {
              expected: ns_len,
              actual: key.len(),
            });
          }
        };
        let (db, db_len) = match Self::decode_varint(rest) {
          Ok(v) => v,
          Err(e) => return Err(e),
        };
        let tag_slice = match rest.split_at_checked(db_len) {
          Some((_, r)) => r,
          None => {
            return Err(Error::BufferTooShort {
              expected: db_len,
              actual: rest.len(),
            });
          }
        };
        let (tag_byte, payload) = match tag_slice {
          [first, rest @ ..] => (*first, rest),
          [] => {
            return Err(Error::BufferTooShort {
              expected: 1,
              actual: 0,
            });
          }
        };
        let tag = match KeyTag::from_u8(tag_byte) {
          Some(t) => t,
          None => return Err(Error::InvalidKeyTag(tag_byte)),
        };
        Ok((ns, db, tag, payload))
      }
    }
  }

  /// 基于原物理键快速替换其 KeyTag（栈缓冲优先零堆分配）
  #[inline]
  pub fn replace_tag(key: &[u8], new_tag: KeyTag) -> Option<TaggedKeyBuf> {
    match key {
      [ns_b, db_b, tag_byte, _payload @ ..]
        if *ns_b < VARINT_1B_FIRST_BYTE_LIMIT
          && *db_b < VARINT_1B_FIRST_BYTE_LIMIT
          && KeyTag::from_u8(*tag_byte).is_some() =>
      {
        Some(Self::replace_tag_at(key, 2, new_tag))
      }
      _ => {
        let (_ns, _db, _tag, user_key) = Self::decode_tagged_key(key).ok()?;
        let tag_offset = key.len().checked_sub(user_key.len() + 1)?;
        Some(Self::replace_tag_at(key, tag_offset, new_tag))
      }
    }
  }

  /// 已知 tag_offset 时的高速替换方法（跳过 decode_tagged_key，零额外解析，栈优先零堆分配）
  #[inline]
  pub fn replace_tag_at(key: &[u8], tag_offset: usize, new_tag: KeyTag) -> TaggedKeyBuf {
    assert!(tag_offset < key.len(), "tag_offset 越界");
    if key.len() <= STACK_KEY_CAP {
      let mut buf = [0u8; STACK_KEY_CAP];
      buf[..key.len()].copy_from_slice(key);
      buf[tag_offset] = new_tag as u8;
      TaggedKeyBuf::from_stack(buf, key.len() as u8)
    } else {
      let mut vec = key.to_vec();
      vec[tag_offset] = new_tag as u8;
      TaggedKeyBuf::from_heap(vec)
    }
  }

  /// 从物理键中快速剥离当前会话前缀，提取 `(tag, payload)`
  ///
  /// 若键不属于当前会话或长度不足，返回 `None`。
  /// 热路径极速过滤：无需任何变长整型解码，底层为单次 SIMD 内存比对与单字节标签解析。
  #[inline(always)]
  pub fn strip_session_prefix<'a>(
    key: &'a [u8],
    session_prefix: &[u8],
  ) -> Option<(KeyTag, &'a [u8])> {
    let rest = key.strip_prefix(session_prefix)?;
    let (&tag_byte, payload) = rest.split_first()?;
    let tag = KeyTag::from_u8(tag_byte)?;
    Some((tag, payload))
  }

  /// 剥离指定会话前缀并校验特定标签，若标签或前缀不匹配则安全返回 `None`
  #[inline(always)]
  pub fn strip_session_prefix_with_tag<'a>(
    key: &'a [u8],
    session_prefix: &[u8],
    expected_tag: KeyTag,
  ) -> Option<&'a [u8]> {
    let rest = key.strip_prefix(session_prefix)?;
    let (&tag_byte, payload) = rest.split_first()?;
    if tag_byte == expected_tag as u8 {
      Some(payload)
    } else {
      None
    }
  }

  /// 便捷方法：剥离当前会话前缀并提取普通字符串键的用户键
  #[inline(always)]
  pub fn strip_string_key<'a>(key: &'a [u8], session_prefix: &[u8]) -> Option<&'a [u8]> {
    Self::strip_session_prefix_with_tag(key, session_prefix, KeyTag::String)
  }

  /// 便捷方法：剥离当前会话前缀并提取集合元数据键的用户键
  #[inline(always)]
  pub fn strip_meta_key<'a>(key: &'a [u8], session_prefix: &[u8]) -> Option<&'a [u8]> {
    Self::strip_session_prefix_with_tag(key, session_prefix, KeyTag::Meta)
  }

  /// 从物理键中提取属于当前会话的存活用户逻辑键（String 或 Meta）
  ///
  /// 若物理键不属于当前会话，或属于集合内部打平子键（Hash/Set/List等），安全返回 `None`。
  /// 彻底消除黑名单与长度探测 hack，供 `KEYS`、`SCAN`、`FLUSHDB`、`DBSIZE` 毫秒级流式过滤。
  #[inline(always)]
  pub fn extract_live_user_key<'a>(
    key: &'a [u8],
    session_prefix: &[u8],
  ) -> Option<(KeyTag, &'a [u8])> {
    match Self::strip_session_prefix(key, session_prefix) {
      Some((tag, user_key)) if tag.is_user_visible() => Some((tag, user_key)),
      _ => None,
    }
  }

  /// 从方案 A 物理键切片中快速解析出会话前缀 (ns + db) 的总字节数 (const fn, 零堆分配零回溯)
  /// 若键长度不足或首字节前缀非法则安全返回 None
  #[inline(always)]
  pub const fn session_prefix_len_from_slice(key: &[u8]) -> Option<usize> {
    match key {
      [ns_b, db_b, ..]
        if *ns_b < VARINT_1B_FIRST_BYTE_LIMIT && *db_b < VARINT_1B_FIRST_BYTE_LIMIT =>
      {
        Some(MIN_SESSION_PREFIX_LEN)
      }
      _ => {
        let first = match key {
          [first, ..] => *first,
          [] => return None,
        };
        let ns_len = match Self::varint_len_from_byte(first) {
          Some(l) => l,
          None => return None,
        };
        let rest = match key.split_at_checked(ns_len) {
          Some((_, r)) => r,
          None => return None,
        };
        let db_first = match rest {
          [first, ..] => *first,
          [] => return None,
        };
        let db_len = match Self::varint_len_from_byte(db_first) {
          Some(l) => l,
          None => return None,
        };
        let prefix_len = ns_len + db_len;
        if key.len() >= prefix_len {
          Some(prefix_len)
        } else {
          None
        }
      }
    }
  }

  /// 从方案 A 物理键中快速解析提取 KeyTag（const fn，零回溯零分配）
  #[inline(always)]
  pub const fn decode_tag(key: &[u8]) -> Option<KeyTag> {
    match key {
      [ns_b, db_b, tag_byte, ..]
        if *ns_b < VARINT_1B_FIRST_BYTE_LIMIT && *db_b < VARINT_1B_FIRST_BYTE_LIMIT =>
      {
        KeyTag::from_u8(*tag_byte)
      }
      _ => {
        let prefix_len = match Self::session_prefix_len_from_slice(key) {
          Some(l) => l,
          None => return None,
        };
        if let Some((_, rest)) = key.split_at_checked(prefix_len) {
          match rest {
            [tag_byte, ..] => KeyTag::from_u8(*tag_byte),
            [] => None,
          }
        } else {
          None
        }
      }
    }
  }

  /// 从方案 A 集合元数据物理键中快速提取逻辑用户键 (const fn，零回溯零分配)
  #[inline(always)]
  pub const fn decode_meta_user_key(key: &[u8]) -> Option<&[u8]> {
    match key {
      [ns_b, db_b, tag_byte, payload @ ..]
        if *ns_b < VARINT_1B_FIRST_BYTE_LIMIT && *db_b < VARINT_1B_FIRST_BYTE_LIMIT =>
      {
        if *tag_byte == KeyTag::Meta as u8 {
          Some(payload)
        } else {
          None
        }
      }
      _ => {
        let prefix_len = match Self::session_prefix_len_from_slice(key) {
          Some(l) => l,
          None => return None,
        };
        if let Some((_, rest)) = key.split_at_checked(prefix_len) {
          match rest {
            [tag_byte, payload @ ..] if *tag_byte == KeyTag::Meta as u8 => Some(payload),
            _ => None,
          }
        } else {
          None
        }
      }
    }
  }

  /// 从方案 A 物理子键中快速提取 (tag, key_id, version)（const fn，零分配，供紧缩器极速判定）
  #[inline(always)]
  pub const fn decode_subkey_id_version(key: &[u8]) -> Option<(KeyTag, u64, u64)> {
    let subkey = match key {
      [ns_b, db_b, rest @ ..]
        if *ns_b < VARINT_1B_FIRST_BYTE_LIMIT && *db_b < VARINT_1B_FIRST_BYTE_LIMIT =>
      {
        rest
      }
      _ => {
        let prefix_len = match Self::session_prefix_len_from_slice(key) {
          Some(l) => l,
          None => return None,
        };
        match key.split_at_checked(prefix_len) {
          Some((_, r)) => r,
          None => return None,
        }
      }
    };

    match SubKeyCodec::decode_header(subkey) {
      Ok((tag, key_id, version)) if tag.is_subkey() => Some((tag, key_id, version)),
      _ => None,
    }
  }
}

/// 解码物理子键后的各分量元组 `(ns, db, tag, key_id, version, payload)`
pub type DecodedSubKey<'a> = (u64, u64, KeyTag, u64, u64, &'a [u8]);

impl NamespaceDbCodec {
  /// 从方案 A 物理子键中一次性解码出 `(ns, db, tag, key_id, version, payload)` (const fn)
  #[inline]
  pub const fn decode_sub_key(key: &[u8]) -> Result<DecodedSubKey<'_>> {
    let (ns, db, subkey) = match key {
      [ns_b, db_b, rest @ ..]
        if *ns_b < VARINT_1B_FIRST_BYTE_LIMIT && *db_b < VARINT_1B_FIRST_BYTE_LIMIT =>
      {
        (*ns_b as u64, *db_b as u64, rest)
      }
      _ => {
        let (ns, ns_len) = match Self::decode_varint(key) {
          Ok(v) => v,
          Err(e) => return Err(e),
        };
        let rest = match key.split_at_checked(ns_len) {
          Some((_, r)) => r,
          None => {
            return Err(Error::BufferTooShort {
              expected: ns_len,
              actual: key.len(),
            });
          }
        };
        let (db, db_len) = match Self::decode_varint(rest) {
          Ok(v) => v,
          Err(e) => return Err(e),
        };
        let subkey = match rest.split_at_checked(db_len) {
          Some((_, r)) => r,
          None => {
            return Err(Error::BufferTooShort {
              expected: db_len,
              actual: rest.len(),
            });
          }
        };
        (ns, db, subkey)
      }
    };

    let (tag, key_id, version) = match SubKeyCodec::decode_header(subkey) {
      Ok((t, k, v)) => {
        if !t.is_subkey() {
          return Err(Error::InvalidKeyTag(t as u8));
        }
        (t, k, v)
      }
      Err(e) => return Err(e),
    };

    let payload = subkey.split_at(SUBKEY_HEADER_SIZE).1;
    Ok((ns, db, tag, key_id, version, payload))
  }

  /// 从方案 A 集合分块物理子键中一次性解码出 `(ns, db, tag, key_id, version, chunk_id)` (const fn)
  #[inline]
  pub const fn decode_chunk_key(key: &[u8]) -> Result<(u64, u64, KeyTag, u64, u64, u32)> {
    if key.len() < MIN_CHUNK_KEY_LEN {
      return Err(Error::BufferTooShort {
        expected: MIN_CHUNK_KEY_LEN,
        actual: key.len(),
      });
    }
    let (ns, db, tag, key_id, version, payload) = match Self::decode_sub_key(key) {
      Ok(v) => v,
      Err(e) => return Err(e),
    };
    let chunk_id = match payload {
      [c0, c1, c2, c3] => u32::from_be_bytes([*c0, *c1, *c2, *c3]),
      _ => {
        return Err(Error::BufferTooShort {
          expected: CHUNK_ID_LEN,
          actual: payload.len(),
        });
      }
    };
    Ok((ns, db, tag, key_id, version, chunk_id))
  }
}
