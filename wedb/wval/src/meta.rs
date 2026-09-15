use crate::{
  error::{Error, Result},
  tag::GarnetObjectType,
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

/// 底层物理存储编码策略（Compact 内联 vs BfTree 树算子）
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageEncoding {
  /// 紧凑内联编码（单主 Key 连续内存内联存储，消除写放大与索引膨胀）
  #[default]
  Compact = 0,
  /// BfTree 树算子编码（独立持久化树文件，meta 记录为 MetaValue + RangeIndexStub 存根）
  FlattenedTree = 2,
}

impl StorageEncoding {
  /// 从单字节快速转换为存储编码枚举（const fn）
  ///
  /// 仅用于本构建写入的合法字节回读（`set_encoding`/`to_bytes` 的对称面）；
  /// 持久化/外部字节解码必须用 [`Self::try_from_u8`] 显式拒绝未知值，
  /// 杜绝未来版本新编码被静默折叠为 Compact 后按内联布局误解析
  #[inline(always)]
  pub const fn from_u8(val: u8) -> Self {
    match val {
      2 => Self::FlattenedTree,
      _ => Self::Compact,
    }
  }

  /// 严格解码：未知编码字节返回 None（持久化解码入口专用）
  #[inline(always)]
  pub const fn try_from_u8(val: u8) -> Option<Self> {
    match val {
      0 => Some(Self::Compact),
      2 => Some(Self::FlattenedTree),
      _ => None,
    }
  }

  /// 转换为单字节表示（const fn）
  #[inline(always)]
  pub const fn as_u8(self) -> u8 {
    self as u8
  }

  /// 是否为 BfTree 树算子编码（独立树文件生命周期口径）
  #[inline(always)]
  pub const fn is_flattened(self) -> bool {
    matches!(self, Self::FlattenedTree)
  }
}

/// 集合元数据紧凑记录结构（C 对齐，8 字节自然对齐，零空洞浪费）
///
/// 内存排布（32 字节，大端序保序持久化）：
/// - `[0..8)`: `key_id: u64` (集合全局唯一自增 ID)
/// - `[8..9)`: `collection_type: GarnetObjectType` (集合逻辑数据结构类型)
/// - `[9..16)`: `reserved: [u8; 7]` (显式填充并预留扩展标志位，其中 reserved 首字节为 StorageEncoding)
/// - `[16..24)`: `version: u64` (逻辑删除版本号，支持 O(1) 秒删与事务乐观失效)
/// - `[24..32)`: `size: u64` (元素计数，保证 HLEN/SCARD/ZCARD 恒为 O(1))
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MetaValue {
  /// 集合全局唯一自增 ID
  pub key_id: u64,
  /// 集合逻辑数据结构类型（Hash / Set / ZSet / List）
  pub collection_type: GarnetObjectType,
  /// 显式预留字节（reserved 首字节存储 StorageEncoding，避免未初始化内存 UB）
  pub reserved: [u8; 7],
  /// 逻辑删除版本号
  pub version: u64,
  /// 集合当前包含的元素总数
  pub size: u64,
}

impl MetaValue {
  /// 构造新的集合元数据记录（const fn）
  #[inline(always)]
  pub const fn new(
    key_id: u64,
    collection_type: GarnetObjectType,
    version: u64,
    size: u64,
  ) -> Self {
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

  /// 设置当前集合的底层物理存储编码（const fn，利用 reserved 首字节存储）
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
  pub const fn read_collection_type(slice: &[u8]) -> Result<GarnetObjectType> {
    if let Err(e) = ensure_len(slice, TYPE_OFFSET + 1) {
      return Err(e);
    }
    match GarnetObjectType::from_u8(slice[TYPE_OFFSET]) {
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
    let collection_type = match GarnetObjectType::from_u8(slice[TYPE_OFFSET]) {
      Some(t) => t,
      None => return Err(Error::InvalidCollectionType(slice[TYPE_OFFSET])),
    };
    // reserved 首字节承载 StorageEncoding：未知编码字节显式拒绝（前向兼容，
    // 杜绝未来新编码被折叠为 Compact 后按内联布局误解析）
    if StorageEncoding::try_from_u8(slice[9]).is_none() {
      return Err(Error::InvalidStorageEncoding(slice[9]));
    }
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
}

/// 16 字节定长紧凑集合元数据（单 64 字节缓存行容纳 4 条，大端序持久化）：
/// - `collection_type: GarnetObjectType` (1 字节，[0..1))
/// - `encoding: StorageEncoding` (1 字节，[1..2))
/// - `reserved: [u8; 2]` (2 字节对齐填充预留，[2..4))
/// - `size: u32` (4 字节元素计数，[4..8))
/// - `expire_at_ticks: i64` (8 字节绝对 .NET Ticks 过期时间戳，0 表示永不过期（100ns 单位，0001-01-01 纪元），[8..16))
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CompactMetaValue {
  /// 集合逻辑数据结构类型
  pub collection_type: GarnetObjectType,
  /// 底层物理存储编码策略
  pub encoding: StorageEncoding,
  /// 显式预留对齐字节
  pub reserved: [u8; 2],
  /// 元素计数（容纳至 42 亿）
  pub size: u32,
  /// 绝对过期 .NET Ticks（100ns 单位，0001-01-01 纪元；0 表示永不过期）
  pub expire_at_ticks: i64,
}

impl CompactMetaValue {
  /// 构造新的 16 字节紧凑元数据（const fn）
  #[inline(always)]
  pub const fn new(
    collection_type: GarnetObjectType,
    encoding: StorageEncoding,
    size: u32,
    expire_at_ticks: i64,
  ) -> Self {
    Self {
      collection_type,
      encoding,
      reserved: [0u8; 2],
      size,
      expire_at_ticks,
    }
  }

  /// 判断当前集合在给定时钟下是否已过期（const fn）
  ///
  /// 边界取严格小于（对标 C# LogRecordUtils.cs:20 `Expiration < UtcNow.Ticks`
  /// 的读路径惰性判断口径）：恰好等于 now 的记录视为未过期
  #[inline(always)]
  pub const fn is_expired(&self, now_ticks: i64) -> bool {
    self.expire_at_ticks > 0 && self.expire_at_ticks < now_ticks
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
    let b1 = self.expire_at_ticks.to_be_bytes();
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
        let expire_at_ticks = i64::from_be_bytes([*b8, *b9, *b10, *b11, *b12, *b13, *b14, *b15]);

        let type_byte = (w0 >> 56) as u8;
        let collection_type = match GarnetObjectType::from_u8(type_byte) {
          Some(t) => t,
          None => return Err(Error::InvalidCollectionType(type_byte)),
        };
        let encoding_byte = (w0 >> 48) as u8;
        let encoding = match StorageEncoding::try_from_u8(encoding_byte) {
          Some(e) => e,
          None => return Err(Error::InvalidStorageEncoding(encoding_byte)),
        };
        let reserved = [(w0 >> 40) as u8, (w0 >> 32) as u8];
        let size = w0 as u32;

        Ok(Self {
          collection_type,
          encoding,
          reserved,
          size,
          expire_at_ticks,
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
  pub const fn read_collection_type(slice: &[u8]) -> Option<GarnetObjectType> {
    if slice.len() < COMPACT_META_VALUE_SIZE {
      return None;
    }
    GarnetObjectType::from_u8(slice[0])
  }

  /// 零拷贝读取物理存储编码（const fn，1 条指令快速提取；未知编码字节 None）
  #[inline]
  pub const fn read_encoding(slice: &[u8]) -> Option<StorageEncoding> {
    if slice.len() < COMPACT_META_VALUE_SIZE {
      return None;
    }
    StorageEncoding::try_from_u8(slice[1])
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
  pub const fn read_expire_at_ticks(slice: &[u8]) -> Option<i64> {
    if slice.len() < COMPACT_META_VALUE_SIZE {
      return None;
    }
    Some(i64::from_be_bytes([
      slice[8], slice[9], slice[10], slice[11], slice[12], slice[13], slice[14], slice[15],
    ]))
  }

  /// 零拷贝判定是否已过期（const fn，边界口径与 [`Self::is_expired`] 一致：严格小于）
  #[inline]
  pub const fn read_is_expired(slice: &[u8], now_ticks: i64) -> Option<bool> {
    match Self::read_expire_at_ticks(slice) {
      Some(exp) => Some(exp > 0 && exp < now_ticks),
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
}
