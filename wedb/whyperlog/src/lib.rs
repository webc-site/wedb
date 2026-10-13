//! HyperLogLog（对标 libs/server/Resp/HyperLogLog/HyperLogLog.cs）
//!
//! 注意：本 crate 是基数估计概率数据结构，与混合日志分配器 whlog（对标 C#
//! Tsavorite AllocatorBase 日志体系）职责完全不同，二者仅命名相近，严禁混淆。
//!
//! 基于 "New cardinality estimation algorithms for HyperLogLog sketches"
//! <https://arxiv.org/abs/1702.01284> 的稀疏/稠密双编码实现：
//! - 稠密：16 字节头（HYLL 魔数 + 编码 + 卡数缓存）+ 16384 个 6 位寄存器；
//! - 稀疏：16 字节头 + 2 字节 RLE 长度 + RLE 操作码流（零段 1xxx xxxx / 非零 0vvv vvvv），
//!   超过 4KB 上限稠密化。
//!
//! 刻意差异（对照 C#）：C# 以裸指针就地改写存储页；Rust 统一操作字节切片
//! （`&mut [u8]`），由调用方负责切片与存储页的等价性。
//!
//! 在 garnet 中的相对路径: libs/server/Storage/Session/MainStore/HyperLogLogOps.cs + test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs（sparse/dense 结构面）

use wbase::hash::murmur_hash2_x64_a;

/// 寄存器位数
const REG_BITS: u32 = 6;
/// 6 位掩码
const REG_BITS_MSK: u8 = (1 << REG_BITS) - 1;
/// 头部字节数（HYLL | E | N/U | Cardin）
pub const HLL_HEADER_BYTES: usize = 16;
/// HYLL 魔数（小端 u32 对应 0x4859_4C4C）
pub const HYLL_MAGIC: u32 = 0x4859_4C4C;
/// 哈希位数
const HBIT: u32 = 64;
/// 0.5/ln(2) 修正常数
pub const ALPHA: f64 = 0.721_347_520_444_481_7;

/// 稀疏每次插入的最大增量字节
pub const SPARSE_MAX_BYTES_PER_INSERT: usize = 2;
/// 稀疏表示容量上限（超过即稠密化）
pub const SPARSE_SIZE_MAX_CAP: usize = 1 << 12;
/// 稀疏分配步长
pub const SPARSE_MEMORY_SECTOR_SIZE: usize = 1 << 7;
/// 稀疏头 = HLL_HEADER_BYTES + 2（RLE 长度），与 pbit 无关
pub const SPARSE_HEADER_SIZE: usize = HLL_HEADER_BYTES + 2;

/// 默认寄存器偏移位数（14 → 16384 寄存器，C# HyperLogLog() protected 默认构造）
pub const DEFAULT_PBIT: u8 = 14;
/// 默认 qbit 前编译期派生表：默认实例的全部派生量零运行时重算
pub const DEFAULT_QBIT: u8 = (HBIT - DEFAULT_PBIT as u32) as u8;
/// 默认寄存器数（16384）
pub const DEFAULT_MCNT: usize = 1_usize << DEFAULT_PBIT;
/// 默认稠密总字节（16 头 + 12288 寄存器 = 12304）
pub const DEFAULT_DENSE_BYTES: usize = HLL_HEADER_BYTES + ((REG_BITS as usize * DEFAULT_MCNT) >> 3);
/// 默认稀疏初始零段数（mcnt >> 7 = 128）
pub const DEFAULT_SPARSE_ZERO_RANGES: usize = DEFAULT_MCNT >> 7;
/// HLL 数据结构类型
///
/// libs/server/Resp/HyperLogLog/HyperLogLog.cs:HLL_DTYPE
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HllDtype {
  Sparse = 0,
  Dense = 1,
}

impl TryFrom<u8> for HllDtype {
  type Error = ();

  #[inline]
  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::Sparse),
      1 => Ok(Self::Dense),
      _ => Err(()),
    }
  }
}

/// Garnet HyperLogLog 实例参数（寄存器数、编码容量均由 pbit 决定）
///
/// libs/server/Resp/HyperLogLog/HyperLogLog.cs:HyperLogLog
pub struct HyperLogLog {
  /// 前导零计数位
  qbit: u8,
  /// 寄存器数
  mcnt: usize,
  /// 稠密总字节
  dense_bytes: usize,
  /// 稀疏初始零段数
  sparse_zero_ranges: usize,
  /// 稀疏头 = HLL_HEADER_BYTES + 2（RLE 长度）
  sparse_header_size: usize,
}

impl Default for HyperLogLog {
  fn default() -> Self {
    Self::with_pbit(DEFAULT_PBIT)
  }
}

mod dense;
mod estimate;
mod frame;
mod merge;
mod regs;
mod sparse;

pub use estimate::round_estimate;

impl HyperLogLog {
  /// 默认构造（pbit = 14）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:HyperLogLog() protected
  #[inline]
  pub const fn new() -> Self {
    Self {
      qbit: DEFAULT_QBIT,
      mcnt: DEFAULT_MCNT,
      dense_bytes: DEFAULT_DENSE_BYTES,
      sparse_zero_ranges: DEFAULT_SPARSE_ZERO_RANGES,
      sparse_header_size: SPARSE_HEADER_SIZE,
    }
  }

  /// 寄存器位构造 (HyperLogLog(byte pbit))：默认 pbit 直取编译期派生表，
  /// 自定义 pbit 才走派生计算
  pub const fn with_pbit(pbit: u8) -> Self {
    if pbit == DEFAULT_PBIT {
      return Self {
        qbit: DEFAULT_QBIT,
        mcnt: DEFAULT_MCNT,
        dense_bytes: DEFAULT_DENSE_BYTES,
        sparse_zero_ranges: DEFAULT_SPARSE_ZERO_RANGES,
        sparse_header_size: SPARSE_HEADER_SIZE,
      };
    }
    let qbit = (HBIT - pbit as u32) as u8;
    let mcnt = 1_usize << pbit;
    Self {
      qbit,
      mcnt,
      dense_bytes: HLL_HEADER_BYTES + ((REG_BITS as usize * mcnt) >> 3),
      sparse_zero_ranges: mcnt >> 7,
      sparse_header_size: SPARSE_HEADER_SIZE,
    }
  }

  /// 寄存器数
  #[inline]
  pub const fn mcnt(&self) -> usize {
    self.mcnt
  }

  /// 稠密表示总字节（16 头 + 12288 寄存器 = 12304）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DenseBytes
  #[inline]
  pub const fn dense_bytes(&self) -> usize {
    self.dense_bytes
  }

  /// 稀疏表示初始字节：头 + 默认零段 + 预留增长区
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseBytes
  #[inline]
  pub const fn sparse_bytes(&self) -> usize {
    self.sparse_header_size + self.sparse_zero_ranges + SPARSE_MEMORY_SECTOR_SIZE
  }

  /// 稀疏表示零段初始数
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseZeroRanges
  #[inline]
  pub const fn sparse_zero_ranges(&self) -> usize {
    self.sparse_zero_ranges
  }

  /// 前导零计数位上限（64 - pbit）
  #[inline]
  pub const fn qbit(&self) -> u8 {
    self.qbit
  }

  /// 按元素集初始化 HLL 载荷（长度决定稠密/稀疏），并更新寄存器
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:Init
  pub fn init(&self, elements: &[&[u8]], value: &mut [u8]) {
    let dense = value.len() == self.dense_bytes;

    if dense {
      self.init_dense(value);
    } else {
      self.init_sparse(value);
    }

    self.iterate_update(elements, value, dense);
  }

  /// 主多值更新：稠密直接更新；稀疏须确认可原位增长
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:Update
  /// 刻意差异：Rust 切片自带长度，C# 的 valueLen 形参恒等于 `value.len()`；
  /// `updated` 出参回传是否发生寄存器变更，返回 false 表示需申请新空间
  pub fn update(&self, elements: &[&[u8]], value: &mut [u8], updated: &mut bool) -> bool {
    if self.is_dense(value) {
      *updated = self.iterate_update(elements, value, true);
      return true;
    }

    if self.is_sparse(value) {
      if self.can_grow_in_place(value, value.len(), elements.len()) {
        *updated = self.iterate_update(elements, value, false);
        return true;
      }

      // 需申请更多空间
      return false;
    }
    debug_assert!(false, "Update HyperLogLog Error!");
    false
  }

  /// 逐元素更新（MurmurHash2x64A 哈希后按编码分派）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:IterateUpdate
  pub(crate) fn iterate_update(&self, elements: &[&[u8]], value: &mut [u8], dense: bool) -> bool {
    let mut updated = false;
    if dense {
      for element in elements {
        let hash_value = murmur_hash_2_x64_a(element);
        updated |= self.update_dense(value, hash_value);
      }
    } else {
      for element in elements {
        let hash_value = murmur_hash_2_x64_a(element);
        updated |= self.update_sparse(value, hash_value);
      }
    }
    updated
  }
}

/// MurmurHash2 64 位变体（PFADD 的元素哈希，委托 wbase::hash::murmur_hash2_x64_a）
#[inline]
pub fn murmur_hash_2_x64_a(b_string: &[u8]) -> u64 {
  murmur_hash2_x64_a(b_string, 0)
}
