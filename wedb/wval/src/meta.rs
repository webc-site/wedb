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

/// 元布局偏移常量默认私有（布局纪律：字段经 [`MetaValue`] 关联方法读取，偏移不外露），
/// 仅尺寸契约常量 [`META_VALUE_SIZE`] 对外
///
/// collection_type 字段在 32B 元数据大端布局中的字节偏移
const TYPE_OFFSET: usize = 8;
/// size 字段在 32B 元数据大端布局中的字节偏移
const SIZE_OFFSET: usize = 16;
/// next_expiry 最早到期刻度在 32B 元数据大端布局中的字节偏移
const NEXT_EXPIRY_OFFSET: usize = 24;
/// 单个 u64 的字节长度
const U64_LEN: usize = 8;

/// 元数据记录 Value 的定长字节大小（32 字节，8 字节自然对齐，4×64 位字容量）
pub const META_VALUE_SIZE: usize = 32;

/// 元数据 reserved 首字节的底层物理存储编码（BfTree 树算子，唯一编码）
///
/// 信封存储 + KeyTag::Ttl 旁路记录定案后无内联编码形态，持久化解码仅认此值
/// （[`Self::try_from_u8`] 显式拒绝未知字节，杜绝未知编码被静默放行）
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageEncoding {
  /// BfTree 树算子编码（独立持久化树文件，meta 记录为 MetaValue + RangeIndexStub 存根）
  FlattenedTree = 2,
}

impl StorageEncoding {
  /// 严格解码：未知编码字节返回 None（持久化解码入口专用）
  #[inline(always)]
  pub const fn try_from_u8(val: u8) -> Option<Self> {
    match val {
      2 => Some(Self::FlattenedTree),
      _ => None,
    }
  }
}

/// 集合元数据紧凑记录结构（C 对齐，8 字节自然对齐，零空洞浪费）
///
/// 内存排布（32 字节，大端序保序持久化）：
/// - `[0..8)`: `key_id: u64` (集合全局唯一自增 ID)
/// - `[8..9)`: `collection_type: GarnetObjectType` (集合逻辑数据结构类型)
/// - `[9..16)`: `reserved: [u8; 7]` (显式填充并预留扩展标志位，其中 reserved 首字节为 StorageEncoding)
/// - `[16..24)`: `size: u64` (元素计数，保证 HLEN/SCARD/ZCARD 恒为 O(1))
/// - `[24..32)`: `next_expiry: i64` (树内成员最早到期 .NET Ticks，字段级 TTL
///   计数抵扣水位；`i64::MAX` = 无成员挂 TTL——`now < next_expiry` 时树内
///   不存在已到期成员，计数直读 `size` 恒精确零树访问；水位命中即交由
///   分层写臂/收集执行体的到期收集内核校正，见 doc/zh/collection.md
///   大键 O(1) 计数规约第 3 条)
#[repr(C, align(8))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaValue {
  /// 集合全局唯一自增 ID
  pub key_id: u64,
  /// 集合逻辑数据结构类型（Hash / Set / ZSet / List）
  pub collection_type: GarnetObjectType,
  /// 显式预留字节（reserved 首字节存储 StorageEncoding，避免未初始化内存 UB）
  pub reserved: [u8; 7],
  /// 集合当前包含的元素总数
  pub size: u64,
  /// 树内成员最早到期刻度（.NET Ticks；`i64::MAX` = 无成员挂字段 TTL）
  pub next_expiry: i64,
}

impl MetaValue {
  /// 构造带到期时间的水位元数据记录（const fn）
  #[inline(always)]
  pub const fn new_with_expiry(
    key_id: u64,
    collection_type: GarnetObjectType,
    size: u64,
    next_expiry: i64,
  ) -> Self {
    Self {
      key_id,
      collection_type,
      reserved: [StorageEncoding::FlattenedTree as u8, 0, 0, 0, 0, 0, 0],
      size,
      next_expiry,
    }
  }

  /// 构造新的集合元数据记录（const fn，编码字节预置 FlattenedTree——唯一合法编码）
  #[inline(always)]
  pub const fn new(key_id: u64, collection_type: GarnetObjectType, size: u64) -> Self {
    Self::new_with_expiry(key_id, collection_type, size, i64::MAX)
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

  /// 水位推进：取更早到期刻度（const fn，树内写臂挂 TTL 时单点调用）
  #[inline(always)]
  pub const fn note_expiry(&mut self, ticks: i64) {
    if ticks < self.next_expiry {
      self.next_expiry = ticks;
    }
  }

  /// 判定元数据记录是否有效存活（对标 Tsavorite ReadMethods.cs Reader：RangeIndex 恒活 + size > 0）
  #[inline(always)]
  pub const fn is_live(&self) -> bool {
    self.is_range_index() || self.size > 0
  }

  /// 判定是否为范围索引集合类型
  #[inline(always)]
  pub const fn is_range_index(&self) -> bool {
    matches!(self.collection_type, GarnetObjectType::RangeIndex)
  }

  /// 编码为定长 32 字节数组（大端保序，const fn，4×64位字级展开零循环）
  #[inline(always)]
  pub const fn to_bytes(&self) -> [u8; META_VALUE_SIZE] {
    let k = self.key_id.to_be_bytes();
    let r = self.reserved;
    let s = self.size.to_be_bytes();
    let n = self.next_expiry.to_be_bytes();

    [
      k[0],
      k[1],
      k[2],
      k[3],
      k[4],
      k[5],
      k[6],
      k[7],                         //
      self.collection_type.as_u8(), //
      r[0],
      r[1],
      r[2],
      r[3],
      r[4],
      r[5],
      r[6], //
      s[0],
      s[1],
      s[2],
      s[3],
      s[4],
      s[5],
      s[6],
      s[7], //
      n[0],
      n[1],
      n[2],
      n[3],
      n[4],
      n[5],
      n[6],
      n[7],
    ]
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
      None => Err(Error::InvalidGarnetObjectType(slice[TYPE_OFFSET])),
    }
  }

  /// 从只读切片直接解码元数据记录（单次越界检查，const fn，零堆分配，4×64位并行展开提取）
  #[inline(always)]
  pub const fn from_slice(slice: &[u8]) -> Result<Self> {
    if let Err(e) = ensure_len(slice, META_VALUE_SIZE) {
      return Err(e);
    }

    let key_id = read_be_u64_at(slice, 0);
    let collection_type = match GarnetObjectType::from_u8(slice[TYPE_OFFSET]) {
      Some(t) => t,
      None => return Err(Error::InvalidGarnetObjectType(slice[TYPE_OFFSET])),
    };
    // reserved 首字节承载 StorageEncoding：未知编码字节显式拒绝（前向兼容，
    // 杜绝未来新编码被静默放行后按未知布局误解析）
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
      size: read_be_u64_at(slice, SIZE_OFFSET),
      next_expiry: read_be_u64_at(slice, NEXT_EXPIRY_OFFSET) as i64,
    })
  }

  /// 从定长 32 字节数组解码元数据记录（const fn）
  #[inline(always)]
  pub const fn from_bytes(bytes: [u8; META_VALUE_SIZE]) -> Result<Self> {
    Self::from_slice(&bytes)
  }
}
