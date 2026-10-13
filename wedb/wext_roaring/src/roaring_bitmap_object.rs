use std::{
  io::{Read, Write},
  result,
};

use roaring::RoaringBitmap;

use crate::error::Result;

/// 对象壳固定开销
///
/// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:ObjectOverhead
pub const OBJECT_OVERHEAD: i64 = 32;
/// 位图本体基座开销（对象 + 字段）
///
/// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmap.cs:ByteSize
pub const BITMAP_BASE: i64 = 32;
/// 每容器条目开销：2B key + 8B 容器引用（同上 ByteSize 的 per-chunk 项）
pub const PER_CONTAINER: i64 = 10;
/// 容器对象头
///
/// 在 garnet 中的相对路径:modules/RoaringBitmap/Containers/ArrayContainer.cs:ByteSize
/// 在 garnet 中的相对路径:modules/RoaringBitmap/Containers/BitmapContainer.cs:ByteSize
pub const CONTAINER_HEADER: i64 = 16;
/// 位图容器 body 字节数（1024 * u64，rust roaring `BitmapStore` 布局）
pub const BITMAP_BODY: usize = 8 * 1024;
/// 数组容器容量上限，超过即位图容器（rust roaring 不变量：Array len <= 4096）
pub const ARRAY_LIMIT: usize = 4096;
/// 旧格式 cookie（无 run 容器，count 在后续 u32）
const SERIAL_COOKIE_NO_RUN: u32 = 12346;
/// 新格式 cookie 低 16 位（容器数打进高 16 位，随附 run 容器位图）
const SERIAL_COOKIE: u32 = 12347;
/// 新格式 offset 表出现阈值（容器数达到才写 N * u32 偏移）
const NO_OFFSET_THRESHOLD: usize = 4;

/// 切片游标推进：取前 n 字节，剩余部分回写游标；不足则 None
#[inline]
fn take<'a>(cur: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
  let head = cur.get(..n)?;
  *cur = &cur[n..];
  Some(head)
}

/// 游标处读小端 u16
#[inline]
fn u16_le(cur: &mut &[u8]) -> Option<u16> {
  Some(u16::from_le_bytes(take(cur, 2)?.try_into().ok()?))
}

/// 游标处读小端 u32
#[inline]
fn u32_le(cur: &mut &[u8]) -> Option<u32> {
  Some(u32::from_le_bytes(take(cur, 4)?.try_into().ok()?))
}

/// 从 on-disk 序列化字节逐容器按类型估算堆内存（字节），格式损坏/截断回 None
///
/// 格式对标 rust roaring `serialize_into`（标准 Roaring on-disk 格式）：
/// 旧格式 `[cookie u32 = 12346][count u32]`，新格式 `[cookie = 12347 | ((count-1) << 16)]`
/// 随附 run 容器位图；此后 descriptive `count * (key u16 + card-1 u16)`，
/// offset 表旧格式恒带、新格式 count >= 4 才带；body 数组容器 card*2B、
/// 位图容器 8KB、run 容器 2B + runs*4B
fn heap_from_serialized(mut raw: &[u8]) -> Option<i64> {
  let cookie = u32_le(&mut raw)?;
  let (count, has_run) = if cookie == SERIAL_COOKIE_NO_RUN {
    (u32_le(&mut raw)? as usize, false)
  } else if cookie & 0xffff == SERIAL_COOKIE {
    (((cookie >> 16) as usize) + 1, true)
  } else {
    return None;
  };
  if count > u16::MAX as usize + 1 {
    return None;
  }
  // run 容器位图：每容器 1 位（仅新格式存在）
  let run_bitmap = if has_run {
    take(&mut raw, count.div_ceil(8))?
  } else {
    &[]
  };
  let desc = take(&mut raw, count * 4)?;
  if if has_run {
    count >= NO_OFFSET_THRESHOLD
  } else {
    true
  } {
    take(&mut raw, count * 4)?;
  }
  // 记账公式对标 C# HeapMemorySize：对象壳 32 + 位图基座 32 + 每容器 10B
  // + Σ(容器 16B 头 + body)；body 类型与 rust 反序列化布局同构
  let mut heap = OBJECT_OVERHEAD + BITMAP_BASE + PER_CONTAINER * count as i64;
  for (i, d) in desc.as_chunks::<4>().0.iter().enumerate() {
    let card = u16::from_le_bytes(d[2..].try_into().ok()?) as usize + 1;
    let is_run = has_run && run_bitmap[i / 8] & (1 << (i % 8)) != 0;
    if is_run {
      // run 容器：body 首部 u16 为段数，堆上为 Vec<[u16; 2]>
      let runs = u16_le(&mut raw)? as usize;
      heap += CONTAINER_HEADER + (runs * 4) as i64;
      take(&mut raw, runs * 4)?;
    } else if card <= ARRAY_LIMIT {
      heap += CONTAINER_HEADER + (card * 2) as i64;
      take(&mut raw, card * 2)?;
    } else {
      heap += CONTAINER_HEADER + BITMAP_BODY as i64;
      take(&mut raw, BITMAP_BODY)?;
    }
  }
  Some(heap)
}

/// 估算位图对象堆内存（字节），入参为信封内层序列化字节（不含 1B 类型标签）
///
/// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:HeapMemorySize
///
/// C# 构造/反序列化路径记账 `HeapMemorySize = ObjectOverhead + bitmap.ByteSize`，
/// ByteSize = 32 基座 + 每容器 10B + Σ(容器 16B 头 + body)；rust roaring 反序列化
/// 布局与之同构（Array = card*2B、Bitmap = 8192B、Run = runs*4B），故自序列化
/// 字节逐容器还原。格式损坏/截断按 0 计——信封记录物理尺寸已含序列化载荷
/// 本体，MEMORY USAGE 不重复计
pub fn heap_estimate(raw: &[u8]) -> i64 {
  heap_from_serialized(raw).unwrap_or(0)
}

/// COSCAN 成员扫描（位图成员扫描执行体，[`wcustom::CustomScanMembersFn`] 契约）
///
/// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:Scan
///
/// C# 上游实现刻意留空：恒空收集 + 游标 0，忽略 MATCH/COUNT/NOVALUES（稠密
/// 位图全枚举可达数十亿条目，官方注释建议 BITPOS 循环或未来 R.SCAN 承担迭代；
/// 其 doc 注释自称「输出置位十进制键」与实现矛盾，以实现为准）。故全参数臂
/// 输出帧恒 `*2\r\n$1\r\n0\r\n*0\r\n`
pub fn scan_members(
  _payload: &[u8],
  _start: i64,
  _count: i64,
  _pattern: &[u8],
  _is_no_value: bool,
) -> result::Result<(Vec<Vec<u8>>, i64), &'static [u8]> {
  Ok((Vec::new(), 0))
}

/// 表示 RoaringBitmap 对象相关的集合（单层：对象壳直包 roaring 位图）
///
/// 双锚：对象壳锚在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:RoaringBitmapObject，
/// 位图操作锚在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmap.cs:RoaringBitmap
///（C# `Enumerate()=>this` 透传臂无对位必要：升序枚举由上游 roaring crate 的
/// `iter()/range()` 直供，见 [`Self::bit_pos`] 等消费点）
pub struct RoaringBitmapObject {
  pub bitmap: RoaringBitmap,
}

impl RoaringBitmapObject {
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:Create
  pub fn create() -> Self {
    Self {
      bitmap: RoaringBitmap::new(),
    }
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:Deserialize
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmap.cs:Deserialize
  pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
    Ok(Self {
      bitmap: RoaringBitmap::deserialize_from(reader)?,
    })
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:SerializeObject
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmap.cs:Serialize
  ///
  /// I/O 与格式错误上抛，不以 unwrap panic 形式失败
  pub fn serialize_object<W: Write>(&self, writer: &mut W) -> Result<()> {
    Ok(self.bitmap.serialize_into(writer)?)
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:SetBit
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmap.cs:SetBit
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmap.cs:Remove
  ///
  /// insert/remove 的返回值即旧值语义（insert：true=原先不存在；remove：true=原先存在），
  /// 单次查找完成读取旧值 + 置位/清除，免除 get+set 两次树下降
  pub fn set_bit(&mut self, value: u32, set: bool) -> bool {
    if set {
      !self.bitmap.insert(value)
    } else {
      self.bitmap.remove(value)
    }
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:GetBit
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmap.cs:GetBit
  pub fn get_bit(&self, value: u32) -> bool {
    self.bitmap.contains(value)
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:BitCount
  pub fn bit_count(&self) -> i64 {
    self.bitmap.len() as i64
  }

  /// 位图是否为空（O(1) 判定是否有容器存活）
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.bitmap.is_empty()
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:BitPos
  pub fn bit_pos(&self, bit: bool, from: u32) -> i64 {
    if bit {
      // 首个 >= from 的置位位：range 经容器级 advance_to 定位，免去逐容器跳扫
      self
        .bitmap
        .range(from..)
        .next()
        .map_or(-1, |pos| pos as i64)
    } else {
      // 查找首个 >= from 的未置位位。
      //
      // C# 锚 modules/RoaringBitmap/RoaringBitmap.cs:BitPos（:139-187）bit=false 臂为容器级跳转：
      // 容器 gap 直接回段首（:164-169），容器内 NextUnsetBit 委托 word 级取反扫描
      // （Containers/BitmapContainer.cs:140-157）或排序数组线扫（Containers/ArrayContainer.cs:132-152）。
      // 原实现在此 range(from..) 逐元素迭代（O(区间置位位数)，稠密位图单命令可达 2^31 次
      // Iter::next，同步读臂秒级阻塞 worker），系转写时的复杂度分叉非既定决策（原码无自陈），
      // 改 rank 前缀和二分定洞位：base = rank(from-1) 即 [0, from) 占位数，则区间
      // [from, x] 占位数 = rank(x) - base，洞 <=> 占位数 < 区间长度；rank 单调不减保证
      // 谓词单调，二分最小 x 即首洞位。整体 O(log(2^32) × 单次 rank)
      // （rank 为容器二分 + 容器内 popcount 累加，见 roaring 0.11.5 inherent.rs:735）。
      let base = if from == 0 {
        0
      } else {
        self.bitmap.rank(from - 1)
      };
      // u32::MAX - from + 1 溢出 u32 算术，区间长度与占位数一律 u64 承载
      let from = u64::from(from);
      let span = u64::from(u32::MAX) - from + 1;
      // 无洞：[from, u32::MAX] 全满 <=> 占位数等于区间长度；
      // rank(u32::MAX) 即全量 len()，语义直判，不造哨兵
      if self.bitmap.rank(u32::MAX) - base == span {
        return -1;
      }
      // 二分最小 x ∈ [from, u32::MAX] 使 [from, x] 含洞（rank(x) - base < x - from + 1）
      let mut lo = from;
      let mut hi = u64::from(u32::MAX);
      while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if self.bitmap.rank(mid as u32) - base < mid - from + 1 {
          hi = mid;
        } else {
          lo = mid + 1;
        }
      }
      lo as i64
    }
  }
}
