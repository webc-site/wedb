use bitcode::{Decode, Encode};
use wbase::buf::put_header_payload;

use crate::{
  buf::stack_heap_buf,
  error::{Error, Result},
  tag::{CollectionType, KeyTag},
};

/// 元数据大端布局中读取位于 `offset` 处的 u64（const fn，调用方保证长度充足）
#[inline(always)]
const fn read_be_u64_at(slice: &[u8], offset: usize) -> u64 {
  u64::from_be_bytes([
    slice[offset],
    slice[offset + 1],
    slice[offset + 2],
    slice[offset + 3],
    slice[offset + 4],
    slice[offset + 5],
    slice[offset + 6],
    slice[offset + 7],
  ])
}

/// 校验切片长度至少为 `need`，不足则返回 [`Error::BufferTooShort`]
#[inline(always)]
const fn ensure_len(slice: &[u8], need: usize) -> Result<()> {
  if slice.len() < need {
    return Err(Error::BufferTooShort {
      expected: need,
      actual: slice.len(),
    });
  }
  Ok(())
}

/// collection_type 字段在 32B 元数据大端布局中的字节偏移
const TYPE_OFFSET: usize = 8;
/// version 字段在 32B 元数据大端布局中的字节偏移
const VERSION_OFFSET: usize = 16;
/// size 字段在 32B 元数据大端布局中的字节偏移
pub const SIZE_OFFSET: usize = 24;
/// 单个 u64 的字节长度
const U64_LEN: usize = 8;

/// 16 字节紧凑型集合元数据定长大小（单缓存行容纳 4 条）
pub const COMPACT_META_VALUE_SIZE: usize = 16;

/// 元数据记录 Value 的定长字节大小（32 字节，2^5 对齐，单缓存行容纳 2 条）
pub const META_VALUE_SIZE: usize = 32;

/// 打平子键定长头部字节大小（1 字节 KeyTag + 8 字节 KeyId + 8 字节 Version = 17 字节）
pub const SUBKEY_HEADER_SIZE: usize = 17;

/// 通用打平子键栈分配最大容量（128 字节，对齐 2 条 64 字节缓存行，消除绝大多数短子键堆分配）
pub const SUBKEY_STACK_CAP: usize = 128;

/// 底层物理存储编码策略（小集合紧凑内联 vs 大集合打平子键/BfTree）
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Encode, Decode)]
pub enum StorageEncoding {
  /// 紧凑内联编码（单主 Key 连续内存内联存储，消除写放大与索引膨胀）
  #[default]
  Compact = 0,
  /// 打平子键/范围索引编码（打散为独立 SubKey 或外部 BfTree）
  Flattened = 1,
}

impl StorageEncoding {
  /// 从单字节快速转换为存储编码枚举（const fn）
  #[inline(always)]
  pub const fn from_u8(val: u8) -> Self {
    match val {
      1 => Self::Flattened,
      _ => Self::Compact,
    }
  }

  /// 转换为单字节表示（const fn）
  #[inline(always)]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }
}

/// 集合元数据紧凑记录结构（C 对齐，8 字节自然对齐，零空洞浪费）
///
/// 内存排布（32 字节，大端序保序持久化）：
/// - `[0..8)`: `key_id: u64` (集合全局唯一自增 ID)
/// - `[8..9)`: `collection_type: CollectionType` (集合逻辑数据结构类型)
/// - `[9..16)`: `reserved: [u8; 7]` (显式填充并预留扩展标志位，其中 reserved[0] 为 StorageEncoding)
/// - `[16..24)`: `version: u64` (逻辑删除版本号，支持 O(1) 秒删与事务乐观失效)
/// - `[24..32)`: `size: u64` (元素计数，保证 HLEN/SCARD/ZCARD 恒为 O(1))
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Encode, Decode)]
pub struct MetaValue {
  /// 集合全局唯一自增 ID
  pub key_id: u64,
  /// 集合逻辑数据结构类型（Hash / Set / ZSet / List）
  pub collection_type: CollectionType,
  /// 显式预留字节（reserved[0] 存储 StorageEncoding，避免未初始化内存 UB）
  pub reserved: [u8; 7],
  /// 逻辑删除版本号
  pub version: u64,
  /// 集合当前包含的元素总数
  pub size: u64,
}

impl MetaValue {
  /// 构造新的集合元数据记录（const fn）
  #[inline(always)]
  pub const fn new(key_id: u64, collection_type: CollectionType, version: u64, size: u64) -> Self {
    Self {
      key_id,
      collection_type,
      reserved: [0u8; 7],
      version,
      size,
    }
  }

  /// 获取当前集合的底层物理存储编码（const fn）
  #[inline(always)]
  pub const fn encoding(&self) -> StorageEncoding {
    StorageEncoding::from_u8(self.reserved[0])
  }

  /// 设置当前集合的底层物理存储编码（const fn，利用 reserved[0] 存储）
  #[inline(always)]
  pub const fn set_encoding(&mut self, enc: StorageEncoding) {
    self.reserved[0] = enc.as_u8();
  }

  /// 构造时链式设置存储编码（const fn）
  #[inline(always)]
  pub const fn with_encoding(mut self, enc: StorageEncoding) -> Self {
    self.reserved[0] = enc.as_u8();
    self
  }

  /// 单调递增版本号并返回更新后的版本号（const fn）
  #[inline(always)]
  pub const fn bump_version(&mut self) -> u64 {
    self.version = match self.version.checked_add(1) {
      Some(v) => v,
      None => 1,
    };
    self.version
  }

  /// 增加元素计数（饱和加，不溢出，const fn）
  #[inline(always)]
  pub const fn inc_size(&mut self, count: u64) {
    self.size = self.size.saturating_add(count);
  }

  /// 减少元素计数（饱和减，不溢出，const fn）
  #[inline(always)]
  pub const fn dec_size(&mut self, count: u64) {
    self.size = self.size.saturating_sub(count);
  }

  /// 编码为定长 32 字节数组（大端保序，const fn，4×64位字级展开零循环）
  #[inline(always)]
  pub const fn to_bytes(&self) -> [u8; META_VALUE_SIZE] {
    let k = self.key_id.to_be_bytes();
    let r = self.reserved;
    let v = self.version.to_be_bytes();
    let s = self.size.to_be_bytes();

    [
      k[0],
      k[1],
      k[2],
      k[3],
      k[4],
      k[5],
      k[6],
      k[7],
      self.collection_type.as_u8(),
      r[0],
      r[1],
      r[2],
      r[3],
      r[4],
      r[5],
      r[6],
      v[0],
      v[1],
      v[2],
      v[3],
      v[4],
      v[5],
      v[6],
      v[7],
      s[0],
      s[1],
      s[2],
      s[3],
      s[4],
      s[5],
      s[6],
      s[7],
    ]
  }

  /// 从只读切片快速读取集合版本号（零解析其他字段，const fn）
  #[inline(always)]
  pub const fn read_version(slice: &[u8]) -> Result<u64> {
    match ensure_len(slice, VERSION_OFFSET + U64_LEN) {
      Ok(()) => Ok(read_be_u64_at(slice, VERSION_OFFSET)),
      Err(e) => Err(e),
    }
  }

  /// 从只读切片快速读取元素总数（零解析其他字段，const fn）
  #[inline(always)]
  pub const fn read_size(slice: &[u8]) -> Result<u64> {
    match ensure_len(slice, SIZE_OFFSET + U64_LEN) {
      Ok(()) => Ok(read_be_u64_at(slice, SIZE_OFFSET)),
      Err(e) => Err(e),
    }
  }

  /// 从只读切片快速读取逻辑集合类型（const fn）
  #[inline(always)]
  pub const fn read_collection_type(slice: &[u8]) -> Result<CollectionType> {
    if let Err(e) = ensure_len(slice, TYPE_OFFSET + 1) {
      return Err(e);
    }
    match CollectionType::from_u8(slice[TYPE_OFFSET]) {
      Some(t) => Ok(t),
      None => Err(Error::InvalidCollectionType(slice[TYPE_OFFSET])),
    }
  }

  /// 从只读切片直接解码元数据记录（单次越界检查，const fn，零堆分配，4×64位并行展开提取）
  #[inline(always)]
  pub const fn from_slice(slice: &[u8]) -> Result<Self> {
    if let Err(e) = ensure_len(slice, META_VALUE_SIZE) {
      return Err(e);
    }

    let key_id = u64::from_be_bytes([
      slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]);
    let collection_type = match CollectionType::from_u8(slice[TYPE_OFFSET]) {
      Some(t) => t,
      None => return Err(Error::InvalidCollectionType(slice[TYPE_OFFSET])),
    };
    let reserved = [
      slice[9], slice[10], slice[11], slice[12], slice[13], slice[14], slice[15],
    ];

    Ok(Self {
      key_id,
      collection_type,
      reserved,
      version: read_be_u64_at(slice, VERSION_OFFSET),
      size: read_be_u64_at(slice, SIZE_OFFSET),
    })
  }

  /// 从定长 32 字节数组解码元数据记录（const fn）
  #[inline(always)]
  pub const fn from_bytes(bytes: [u8; META_VALUE_SIZE]) -> Result<Self> {
    Self::from_slice(&bytes)
  }

  /// 将元数据记录编码写入目标切片（零堆分配）
  #[inline]
  pub fn write_to_slice(&self, dst: &mut [u8]) -> Result<()> {
    if let Some(chunk) = dst.first_chunk_mut::<META_VALUE_SIZE>() {
      *chunk = self.to_bytes();
      Ok(())
    } else {
      Err(Error::BufferTooShort {
        expected: META_VALUE_SIZE,
        actual: dst.len(),
      })
    }
  }

  /// 使用 bitcode 编码为二进制字节向量
  #[inline]
  pub fn encode_bitcode(&self) -> Vec<u8> {
    bitcode::encode(self)
  }

  /// 从 bitcode 二进制切片解码 MetaValue
  #[inline]
  pub fn decode_bitcode(src: &[u8]) -> Result<Self> {
    bitcode::decode(src).map_err(Error::from)
  }
}

/// 16 字节定长紧凑集合元数据（单 64 字节缓存行容纳 4 条，大端序持久化）：
/// - `collection_type: CollectionType` (1 字节，[0..1))
/// - `encoding: StorageEncoding` (1 字节，[1..2))
/// - `reserved: [u8; 2]` (2 字节对齐填充预留，[2..4))
/// - `size: u32` (4 字节元素计数，[4..8))
/// - `expire_at_ms: u64` (8 字节绝对毫秒级过期时间戳，0 表示永不过期，[8..16))
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Encode, Decode)]
pub struct CompactMetaValue {
  /// 集合逻辑数据结构类型
  pub collection_type: CollectionType,
  /// 底层物理存储编码策略
  pub encoding: StorageEncoding,
  /// 显式预留对齐字节
  pub reserved: [u8; 2],
  /// 元素计数（容纳至 42 亿）
  pub size: u32,
  /// 绝对毫秒级过期时间戳（0 表示永不过期）
  pub expire_at_ms: u64,
}

impl CompactMetaValue {
  /// 构造新的 16 字节紧凑元数据（const fn）
  #[inline(always)]
  pub const fn new(
    collection_type: CollectionType,
    encoding: StorageEncoding,
    size: u32,
    expire_at_ms: u64,
  ) -> Self {
    Self {
      collection_type,
      encoding,
      reserved: [0u8; 2],
      size,
      expire_at_ms,
    }
  }

  /// 判断当前集合在给定时钟下是否已过期（const fn）
  #[inline(always)]
  pub const fn is_expired(&self, now_ms: u64) -> bool {
    self.expire_at_ms > 0 && self.expire_at_ms <= now_ms
  }

  /// 增加元素计数（饱和加，不溢出，const fn）
  #[inline(always)]
  pub const fn inc_size(&mut self, count: u32) {
    self.size = self.size.saturating_add(count);
  }

  /// 减少元素计数（饱和减，不溢出，const fn）
  #[inline(always)]
  pub const fn dec_size(&mut self, count: u32) {
    self.size = self.size.saturating_sub(count);
  }

  /// 编码为定长 16 字节数组（大端保序，双 64 位字融合编码，const fn）
  #[inline]
  pub const fn to_bytes(&self) -> [u8; COMPACT_META_VALUE_SIZE] {
    let w0 = ((self.collection_type.as_u8() as u64) << 56)
      | ((self.encoding.as_u8() as u64) << 48)
      | ((self.reserved[0] as u64) << 40)
      | ((self.reserved[1] as u64) << 32)
      | (self.size as u64);
    let b0 = w0.to_be_bytes();
    let b1 = self.expire_at_ms.to_be_bytes();
    [
      b0[0], b0[1], b0[2], b0[3], b0[4], b0[5], b0[6], b0[7], b1[0], b1[1], b1[2], b1[3], b1[4],
      b1[5], b1[6], b1[7],
    ]
  }

  /// 从定长 16 字节数组直接解码（const fn，复用 [Self::from_slice] 零重复）
  #[inline(always)]
  pub const fn from_bytes(bytes: [u8; COMPACT_META_VALUE_SIZE]) -> Result<Self> {
    Self::from_slice(&bytes)
  }

  /// 从只读切片解码 16 字节紧凑元数据（const fn，双 64 位字二进制解包，单次模式匹配零堆分配）
  #[inline]
  pub const fn from_slice(slice: &[u8]) -> Result<Self> {
    match slice {
      [
        b0,
        b1,
        b2,
        b3,
        b4,
        b5,
        b6,
        b7,
        b8,
        b9,
        b10,
        b11,
        b12,
        b13,
        b14,
        b15,
        ..,
      ] => {
        let w0 = u64::from_be_bytes([*b0, *b1, *b2, *b3, *b4, *b5, *b6, *b7]);
        let expire_at_ms = u64::from_be_bytes([*b8, *b9, *b10, *b11, *b12, *b13, *b14, *b15]);

        let type_byte = (w0 >> 56) as u8;
        let collection_type = match CollectionType::from_u8(type_byte) {
          Some(t) => t,
          None => return Err(Error::InvalidCollectionType(type_byte)),
        };
        let encoding = StorageEncoding::from_u8((w0 >> 48) as u8);
        let reserved = [(w0 >> 40) as u8, (w0 >> 32) as u8];
        let size = w0 as u32;

        Ok(Self {
          collection_type,
          encoding,
          reserved,
          size,
          expire_at_ms,
        })
      }
      _ => Err(Error::BufferTooShort {
        expected: COMPACT_META_VALUE_SIZE,
        actual: slice.len(),
      }),
    }
  }

  /// 零拷贝读取集合数据结构类型（const fn，1 条指令快速提取）
  #[inline]
  pub const fn read_collection_type(slice: &[u8]) -> Option<CollectionType> {
    if slice.len() < COMPACT_META_VALUE_SIZE {
      return None;
    }
    CollectionType::from_u8(slice[0])
  }

  /// 零拷贝读取物理存储编码（const fn，1 条指令快速提取）
  #[inline]
  pub const fn read_encoding(slice: &[u8]) -> Option<StorageEncoding> {
    if slice.len() < COMPACT_META_VALUE_SIZE {
      return None;
    }
    Some(StorageEncoding::from_u8(slice[1]))
  }

  /// 零拷贝读取元素大小（const fn，1 条指令快速提取）
  #[inline]
  pub const fn read_size(slice: &[u8]) -> Option<u32> {
    if slice.len() < COMPACT_META_VALUE_SIZE {
      return None;
    }
    Some(u32::from_be_bytes([slice[4], slice[5], slice[6], slice[7]]))
  }

  /// 零拷贝读取过期时间戳（const fn，1 条指令快速提取）
  #[inline]
  pub const fn read_expire_at_ms(slice: &[u8]) -> Option<u64> {
    if slice.len() < COMPACT_META_VALUE_SIZE {
      return None;
    }
    Some(u64::from_be_bytes([
      slice[8], slice[9], slice[10], slice[11], slice[12], slice[13], slice[14], slice[15],
    ]))
  }

  /// 零拷贝判定是否已过期（const fn）
  #[inline]
  pub const fn read_is_expired(slice: &[u8], now_ms: u64) -> Option<bool> {
    match Self::read_expire_at_ms(slice) {
      Some(exp) => Some(exp > 0 && exp <= now_ms),
      None => None,
    }
  }

  /// 写入目标切片（零堆分配）
  #[inline]
  pub fn write_to_slice(&self, dst: &mut [u8]) -> Result<()> {
    if let Some(chunk) = dst.first_chunk_mut::<COMPACT_META_VALUE_SIZE>() {
      *chunk = self.to_bytes();
      Ok(())
    } else {
      Err(Error::BufferTooShort {
        expected: COMPACT_META_VALUE_SIZE,
        actual: dst.len(),
      })
    }
  }

  /// 使用 bitcode 编码为二进制字节向量
  #[inline]
  pub fn encode_bitcode(&self) -> Vec<u8> {
    bitcode::encode(self)
  }

  /// 从 bitcode 二进制切片解码
  #[inline]
  pub fn decode_bitcode(src: &[u8]) -> Result<Self> {
    bitcode::decode(src).map_err(Error::from)
  }
}

/// 打平子键的只读零拷贝切片视图
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubKeyRef<'a> {
  /// 命名空间标签
  pub tag: KeyTag,
  /// 集合唯一 ID
  pub key_id: u64,
  /// 所属版本号
  pub version: u64,
  /// 子键载荷切片（字段名/成员名等）
  pub payload: &'a [u8],
}

impl<'a> SubKeyRef<'a> {
  /// 从只读切片直接零拷贝解析子键结构（const fn，无额外堆分配）
  #[inline]
  pub const fn from_slice(slice: &'a [u8]) -> Result<Self> {
    let (tag, key_id, version) = match SubKeyCodec::decode_header(slice) {
      Ok(h) => h,
      Err(e) => return Err(e),
    };
    let payload = slice.split_at(SUBKEY_HEADER_SIZE).1;

    Ok(Self {
      tag,
      key_id,
      version,
      payload,
    })
  }

  /// 提取 17 字节定长前缀头（const fn）
  #[inline(always)]
  pub const fn header(&self) -> [u8; SUBKEY_HEADER_SIZE] {
    SubKeyCodec::encode_header(self.tag, self.key_id, self.version)
  }

  /// 获取序列化编码后的总字节数（const fn）
  #[inline(always)]
  pub const fn encoded_len(&self) -> usize {
    SUBKEY_HEADER_SIZE + self.payload.len()
  }

  /// 将子键编码写入目标切片（零堆分配）
  #[inline]
  pub fn write_to_slice(&self, dst: &mut [u8]) -> Result<usize> {
    SubKeyCodec::encode_to_slice(self.tag, self.key_id, self.version, self.payload, dst)
  }

  /// 编码为优先栈分配的缓冲区（短 payload 零堆分配）
  #[inline]
  pub fn to_buf(&self) -> Result<SubKeyBuf> {
    SubKeyCodec::encode_to_buf(self.tag, self.key_id, self.version, self.payload)
  }

  /// 预分配精准容量并编码为 Vec<u8>（单次堆分配；tag 已由解析保证合法，失败仅剩 usize 溢出）
  #[inline]
  pub fn to_vec(&self) -> Vec<u8> {
    self.try_to_vec().unwrap_or_default()
  }

  /// 尝试编码为全新分配的 Vec<u8>，若长度溢出则返回错误
  #[inline]
  pub fn try_to_vec(&self) -> Result<Vec<u8>> {
    SubKeyCodec::try_encode_to_vec(self.tag, self.key_id, self.version, self.payload)
  }
}

// 打平子键高性能缓冲区：短 payload 优先走 128 字节栈分配，超长自动回退到堆
stack_heap_buf!(SubKeyBuf, SUBKEY_STACK_CAP);

impl SubKeyBuf {
  /// 从字段/成员参数直接构造优先栈分配的子键缓冲区
  #[inline]
  pub fn encode(tag: KeyTag, key_id: u64, version: u64, payload: &[u8]) -> Result<Self> {
    SubKeyCodec::encode_to_buf(tag, key_id, version, payload)
  }
}

/// 打平子键静态无分配编解码器
pub struct SubKeyCodec;

/// 通用 17 字节子键前缀头构建（tag: 1B + key_id: 8B + version: 8B，大端）
///
/// 供 [SubKeyCodec] 与 [crate::zset::ZSetSubKeyCodec] 复用，消除雷同数组字面量
#[inline(always)]
pub(crate) const fn kv_prefix(tag: u8, key_id: u64, version: u64) -> [u8; SUBKEY_HEADER_SIZE] {
  let k = key_id.to_be_bytes();
  let v = version.to_be_bytes();
  [
    tag, k[0], k[1], k[2], k[3], k[4], k[5], k[6], k[7], v[0], v[1], v[2], v[3], v[4], v[5], v[6],
    v[7],
  ]
}

impl SubKeyCodec {
  /// 编码 17 字节定长子键前缀头（const fn）
  #[inline(always)]
  pub const fn encode_header(tag: KeyTag, key_id: u64, version: u64) -> [u8; SUBKEY_HEADER_SIZE] {
    kv_prefix(tag.as_u8(), key_id, version)
  }

  /// 从切片快速解码 16 字节 (key_id, version) 元数据（const fn, 单次模式匹配零堆分配）
  #[inline(always)]
  pub const fn decode_id_version(slice: &[u8]) -> Option<(u64, u64)> {
    match slice {
      [
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
        let key_id = u64::from_be_bytes([*k0, *k1, *k2, *k3, *k4, *k5, *k6, *k7]);
        let version = u64::from_be_bytes([*v0, *v1, *v2, *v3, *v4, *v5, *v6, *v7]);
        Some((key_id, version))
      }
      _ => None,
    }
  }

  /// 从只读切片快速解码 17 字节前缀头信息 (tag, key_id, version)（const fn）
  ///
  /// 仅接受子键标签（Hash..=SetChunk）：String/Meta/Ttl 的载荷是用户键原文，
  /// 绝不允许被当作 (key_id, version) 头误解析（对标 C# RecordNamespace 的
  /// 命名空间封闭性约束，杜绝类型穿透）。
  #[inline(always)]
  pub const fn decode_header(slice: &[u8]) -> Result<(KeyTag, u64, u64)> {
    match slice {
      [tag_byte, rest @ ..] => {
        let tag = match KeyTag::from_u8(*tag_byte) {
          Some(t) if t.is_subkey() => t,
          _ => return Err(Error::InvalidKeyTag(*tag_byte)),
        };
        match Self::decode_id_version(rest) {
          Some((key_id, version)) => Ok((tag, key_id, version)),
          None => Err(Error::BufferTooShort {
            expected: SUBKEY_HEADER_SIZE,
            actual: slice.len(),
          }),
        }
      }
      [] => Err(Error::BufferTooShort {
        expected: SUBKEY_HEADER_SIZE,
        actual: 0,
      }),
    }
  }

  /// 校验标签属于子键族（Hash..=SetChunk），非子键标签拒绝编码
  #[inline(always)]
  const fn ensure_subkey(tag: KeyTag) -> Result<()> {
    if tag.is_subkey() {
      Ok(())
    } else {
      Err(Error::InvalidKeyTag(tag.as_u8()))
    }
  }

  /// 零拷贝解析子键切片（const fn）
  #[inline(always)]
  pub const fn decode(slice: &[u8]) -> Result<SubKeyRef<'_>> {
    SubKeyRef::from_slice(slice)
  }

  /// 将子键编码并写入目标缓冲区，返回写入的总字节数（零堆分配）
  ///
  /// 仅接受子键标签（Hash..=SetChunk），杜绝以子键布局编码非子键标签
  #[inline]
  pub fn encode_to_slice(
    tag: KeyTag,
    key_id: u64,
    version: u64,
    payload: &[u8],
    dst: &mut [u8],
  ) -> Result<usize> {
    Self::ensure_subkey(tag)?;
    let total_len = SUBKEY_HEADER_SIZE
      .checked_add(payload.len())
      .ok_or(Error::RecordSizeOverflow)?;
    let header = Self::encode_header(tag, key_id, version);
    put_header_payload(dst, &header, payload).ok_or(Error::BufferTooShort {
      expected: total_len,
      actual: dst.len(),
    })
  }

  /// 编码子键为优先栈分配的缓冲区（消除短 payload 堆分配）
  #[inline]
  pub fn encode_to_buf(
    tag: KeyTag,
    key_id: u64,
    version: u64,
    payload: &[u8],
  ) -> Result<SubKeyBuf> {
    Self::ensure_subkey(tag)?;
    SUBKEY_HEADER_SIZE
      .checked_add(payload.len())
      .ok_or(Error::RecordSizeOverflow)?;
    Ok(SubKeyBuf::from_header_parts(
      &Self::encode_header(tag, key_id, version),
      payload,
    ))
  }

  /// 尝试预分配容量并编码为 Vec<u8>，若溢出则返回错误
  #[inline]
  pub fn try_encode_to_vec(
    tag: KeyTag,
    key_id: u64,
    version: u64,
    payload: &[u8],
  ) -> Result<Vec<u8>> {
    Self::ensure_subkey(tag)?;
    let total_len = match SUBKEY_HEADER_SIZE.checked_add(payload.len()) {
      Some(l) => l,
      None => return Err(Error::RecordSizeOverflow),
    };
    let mut vec = Vec::with_capacity(total_len);
    let header = Self::encode_header(tag, key_id, version);
    vec.extend_from_slice(&header);
    vec.extend_from_slice(payload);
    Ok(vec)
  }
}
