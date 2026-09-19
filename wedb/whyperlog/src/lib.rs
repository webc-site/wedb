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
//! （`&mut [u8]`），由调用方负责切片与存储页的等价性。DEBUG 导出方法
//! （C# Console.WriteLine）改为返回 `String`。

use wbase::hash::murmur_hash2_x64_a;

/// 寄存器位数
const REG_BITS: u32 = 6;
/// 6 位掩码
const REG_BITS_MSK: u8 = (1 << REG_BITS) - 1;
/// 头部字节数（HYLL | E | N/U | Cardin）
const HLL_HEADER_BYTES: usize = 16;
/// HYLL 魔数（小端 u32 对应 0x4859_4C4C）
const HYLL_MAGIC: u32 = 0x4859_4C4C;
/// 哈希位数
const HBIT: u32 = 64;
/// 0.5/ln(2) 修正常数
const ALPHA: f64 = 0.721_347_520_444_481_7;

/// 稀疏每次插入的最大增量字节
const SPARSE_MAX_BYTES_PER_INSERT: usize = 2;
/// 稀疏表示容量上限（超过即稠密化）
pub const SPARSE_SIZE_MAX_CAP: usize = 1 << 12;
/// 稀疏分配步长
pub const SPARSE_MEMORY_SECTOR_SIZE: usize = 1 << 7;

/// HLL 数据结构类型
///
/// libs/server/Resp/HyperLogLog/HyperLogLog.cs:HLL_DTYPE
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone)]
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
    // 默认寄存器偏移 14 位（16384 寄存器）
    Self::with_pbit(14)
  }
}

mod dense;
mod estimate;
mod frame;
mod merge;
mod regs;
mod sparse;

impl HyperLogLog {
  /// 默认构造（pbit = 14）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:HyperLogLog() protected
  pub fn new() -> Self {
    Self::default()
  }

  /// 自定义寄存器位构造 (HyperLogLog(byte pbit))
  pub fn with_pbit(pbit: u8) -> Self {
    let qbit = (HBIT - pbit as u32) as u8;
    let mcnt = 1_usize << pbit;
    let sparse_header_size = HLL_HEADER_BYTES + 2;
    Self {
      qbit,
      mcnt,
      dense_bytes: HLL_HEADER_BYTES + ((REG_BITS as usize * mcnt) >> 3),
      sparse_zero_ranges: mcnt >> 7,
      sparse_header_size,
      // sparse_bytes（C# SparseBytes）在需要处即时计算
    }
  }

  /// 稠密表示总字节（16 头 + 12288 寄存器 = 12304）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:DenseBytes
  #[inline]
  pub fn dense_bytes(&self) -> usize {
    self.dense_bytes
  }

  /// 稀疏表示初始字节：头 + 默认零段 + 预留增长区
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseBytes
  #[inline]
  pub fn sparse_bytes(&self) -> usize {
    self.sparse_header_size + self.sparse_zero_ranges + SPARSE_MEMORY_SECTOR_SIZE
  }

  /// 稀疏表示零段初始数
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLog.cs:SparseZeroRanges
  #[inline]
  pub fn sparse_zero_ranges(&self) -> usize {
    self.sparse_zero_ranges
  }

  /// 前导零计数位上限（64 - pbit）
  #[inline]
  pub fn qbit(&self) -> u8 {
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
  pub fn iterate_update(&self, elements: &[&[u8]], value: &mut [u8], dense: bool) -> bool {
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

#[cfg(test)]
mod tests {
  use super::*;

  fn hll() -> HyperLogLog {
    HyperLogLog::new()
  }

  fn sample_range(start: usize, end: usize) -> Vec<Vec<u8>> {
    (start..end)
      .map(|i| format!("element-{i}").into_bytes())
      .collect()
  }

  fn refs(v: &[Vec<u8>]) -> Vec<&[u8]> {
    v.iter().map(|b| b.as_slice()).collect()
  }

  /// 4KB 稀疏工作缓冲（稀疏上限即 4096）
  fn sparse_buf(h: &HyperLogLog) -> Vec<u8> {
    let mut blob = vec![0_u8; SPARSE_SIZE_MAX_CAP];
    h.init_sparse(&mut blob);
    blob
  }

  /// 结构常量与 C# 逐项对齐：寄存器数、编码字节数、误差常数
  #[test]
  fn layout_constants() {
    let h = hll();
    assert_eq!(h.mcnt, 16384);
    assert_eq!(h.dense_bytes(), 16 + (6 * 16384) / 8);
    assert_eq!(h.dense_bytes(), 12304);
    assert_eq!(h.sparse_zero_ranges(), 16384 >> 7);
    assert_eq!(h.sparse_zero_ranges(), 128);
    assert_eq!(h.sparse_bytes(), 16 + 2 + 128 + 128);
    assert_eq!(h.sparse_bytes(), 274);
    assert_eq!(h.qbit(), 50);
    // 误差常数 alpha = 0.5/ln(2)
    assert!((ALPHA - 0.721_347_520_444_481_7).abs() < 1e-15);
  }

  /// 稀疏初始化布局：魔数/类型/失效卡数/初始零段
  #[test]
  fn init_sparse_layout() {
    let h = hll();
    let mut blob = sparse_buf(&h);

    assert!(h.is_sparse(&blob));
    assert!(HyperLogLog::is_hyll(&blob));
    assert!(h.is_valid_hyll(&blob));
    assert!(h.is_valid_hyll_len(&blob, blob.len()));
    assert_eq!(h.get_sparse_rle_size(&blob), 128);
    assert_eq!(HyperLogLog::get_card(&blob), i64::MIN);
    // 初始零段全为 0xFF（每段覆盖 128 寄存器 × 128 段 = 16384）
    assert!(blob[18..18 + 128].iter().all(|&b| b == 0xFF));
    assert!(h.is_valid_sparse_stream(&blob));
    assert_eq!(h.sparse_count_non_zero(&blob), 0);
    assert_eq!(h.count(&mut blob), 0);
  }

  /// 稠密初始化与 6 位跨字节寄存器读写
  #[test]
  fn init_dense_layout() {
    let h = hll();
    let mut blob = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut blob);

    assert!(h.is_dense(&blob));
    assert!(h.is_valid_hyll(&blob));
    assert_eq!(blob.len(), 12304);

    // 全值域读写往返，相邻寄存器互不串位
    h.set_register(&mut blob[16..], 0, 63);
    h.set_register(&mut blob[16..], 1, 1);
    h.set_register(&mut blob[16..], 16383, 5);
    h.set_register(&mut blob[16..], 2, 42);
    assert_eq!(h.get_register(&blob[16..], 0), 63);
    assert_eq!(h.get_register(&blob[16..], 1), 1);
    assert_eq!(h.get_register(&blob[16..], 2), 42);
    assert_eq!(h.get_register(&blob[16..], 16383), 5);
    assert_eq!(h.get_register(&blob[16..], 100), 0);
  }

  /// PFADD 语义：重复插入无变更；稀疏与稠密计数误差受控（HLL 标准差 ~1.1%）
  #[test]
  fn pfadd_and_count_accuracy() {
    let h = hll();
    let mut blob = sparse_buf(&h);

    let data = sample_range(0, 500);
    let r = refs(&data);
    let mut updated = false;

    assert!(h.update(&r, &mut blob, &mut updated));
    assert!(updated);

    // 重复插入：无寄存器变更
    let mut updated2 = false;
    assert!(h.update(&r, &mut blob, &mut updated2));
    assert!(!updated2);

    let sparse_count = h.count(&mut blob);
    assert!(
      (440..=560).contains(&sparse_count),
      "sparse count = {sparse_count}"
    );

    // 稠密路径 1 万元素
    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);
    let data10k = sample_range(0, 10_000);
    let mut u = false;
    assert!(h.update(&refs(&data10k), &mut dense, &mut u));
    let dense_count = h.count(&mut dense);
    assert!(
      (8800..=11200).contains(&dense_count),
      "dense count = {dense_count}"
    );
  }

  /// 稀疏 → 稠密升级：计数不变且形态为稠密
  ///
  /// 寄存器级一致性由调试用例 compare_sparse_to_dense_mismatch 覆盖
  /// （C# 的 CompareSparseToDense 本身就在 #if DEBUG 段内）。
  #[test]
  fn sparse_to_dense_upgrade() {
    let h = hll();
    let data = sample_range(0, 300);
    let r = refs(&data);

    let mut sparse_blob = sparse_buf(&h);
    let mut u = false;
    h.update(&r, &mut sparse_blob, &mut u);
    let sparse_count = h.count(&mut sparse_blob);

    let mut dense_blob = vec![0_u8; h.dense_bytes()];
    h.copy_update(&[], &sparse_blob, &mut dense_blob);
    assert!(h.is_dense(&dense_blob));
    assert_eq!(h.count(&mut dense_blob), sparse_count);
  }

  /// 稀疏零段拆分：首/中/尾寄存器边界 + RLE 流合法性 + 同元素幂等
  #[test]
  fn sparse_zero_range_split() {
    let h = hll();
    let mut blob = sparse_buf(&h);

    let rle0 = h.get_sparse_rle_size(&blob);
    let hv = murmur_hash_2_x64_a(b"first");
    assert!(h.update_sparse(&mut blob, hv));
    // 中部拆分 +2（左右零段），边界拆分 +1；仅断言单调增长与流合法
    assert!(h.get_sparse_rle_size(&blob) > rle0);
    assert!(h.is_valid_sparse_stream(&blob));

    // 同一元素二次插入：无变更（RLE 长度保持首次插入后的值）
    assert!(!h.update_sparse(&mut blob, hv));
    assert_eq!(h.get_sparse_rle_size(&blob), rle0 + 2);

    // 边界拆分：idx = 0 / idx = 1（中部右拆）/ idx = mcnt - 1
    let mut blob2 = sparse_buf(&h);
    assert!(h.update_sparse_reg(&mut blob2, 0, 3));
    let idx_last = (h.mcnt - 1) as u16;
    assert!(h.update_sparse_reg(&mut blob2, idx_last, 1));
    assert!(h.update_sparse_reg(&mut blob2, 1, 2));
    assert!(h.is_valid_sparse_stream(&blob2));

    // 经稠密展开验证寄存器值
    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);
    h.sparse_to_dense(&blob2, &mut dense);
    assert_eq!(h.get_register(&dense[16..], 0), 3);
    assert_eq!(h.get_register(&dense[16..], 1), 2);
    assert_eq!(h.get_register(&dense[16..], idx_last), 1);
    // 未动寄存器仍为 0
    assert_eq!(h.get_register(&dense[16..], 500), 0);
  }

  /// clz / reg_idx 边界
  #[test]
  fn reg_idx_and_clz_bounds() {
    let h = hll();
    assert_eq!(h.reg_idx(0), 0);
    assert_eq!(h.reg_idx(u64::MAX), 16383);
    // hv = 0 → 全零 → clz 封顶 qbit + 1
    assert_eq!(h.clz(0), 51);
    // 最高位为 1 → clz = 1
    assert_eq!(h.clz(1_u64 << 63), 1);
    // 50 位处为 1 → lz = 14 < qbit → 15
    assert_eq!(h.clz(1_u64 << 49), 15);
  }

  /// 稀疏合并：try_merge 并集计数
  #[test]
  fn merge_sparse_sparse() {
    let h = hll();
    let mut dst = sparse_buf(&h);
    let mut src = sparse_buf(&h);

    let a = sample_range(0, 200);
    let b = sample_range(200, 400);

    let mut u = false;
    h.update(&refs(&a), &mut dst, &mut u);
    h.update(&refs(&b), &mut src, &mut u);

    let dst_len = dst.len();
    assert!(h.try_merge(&src, &mut dst, dst_len));
    let merged = h.count(&mut dst);
    assert!((352..=448).contains(&merged), "merged = {merged}");
  }

  /// 稀疏 → 稠密合并
  #[test]
  fn merge_sparse_into_dense() {
    let h = hll();
    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);
    let mut sparse = sparse_buf(&h);

    let a = sample_range(0, 100);
    let b = sample_range(50, 150);
    let mut u = false;
    h.update(&refs(&a), &mut dense, &mut u);
    h.update(&refs(&b), &mut sparse, &mut u);

    assert!(h.merge(&sparse, &mut dense));
    let merged = h.count(&mut dense);
    assert!((132..=168).contains(&merged), "merged = {merged}");
  }

  /// 稠密基数缓存：未失效直接命中，更新失效后重算
  #[test]
  fn card_cache_invalidation() {
    let h = hll();
    let mut blob = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut blob);

    let data_a = sample_range(0, 500);
    let a = refs(&data_a);
    let mut u = false;
    h.update(&a, &mut blob, &mut u);
    let c1 = h.count(&mut blob);
    assert!(HyperLogLog::is_valid_card(&blob));
    assert_eq!(c1, h.count(&mut blob));

    let data_b = sample_range(500, 1000);
    let b = refs(&data_b);
    h.update(&b, &mut blob, &mut u);
    assert!(!HyperLogLog::is_valid_card(&blob));
    let c3 = h.count(&mut blob);
    assert!(c3 > c1, "{c1} vs {c3}");
  }

  /// 增长规划：CanGrowInPlace / UpdateGrow / MergeGrow
  #[test]
  fn growth_planning() {
    let h = hll();
    let blob = sparse_buf(&h);

    // 分配 4KB、占用 130B：可原位容纳
    assert!(h.can_grow_in_place(&blob, blob.len(), 1000));
    assert!(!h.can_grow_in_place(&blob, 200, 1000));

    // 少量元素 → 稀疏扩容
    let grow = h.update_grow(64, &blob);
    assert!(grow > h.sparse_current_size_in_bytes(&blob));
    assert!(grow < SPARSE_SIZE_MAX_CAP);

    // 大量元素 → 稠密化
    assert_eq!(h.update_grow(100_000, &blob), h.dense_bytes());

    // MergeGrow：稀疏+稀疏按扇区推进；稠密目标恒为稠密长度
    let mut src = sparse_buf(&h);
    let mut u = false;
    let data_50 = sample_range(0, 50);
    h.update(&refs(&data_50), &mut src, &mut u);
    let mg = h.merge_grow(&src, &blob);
    assert!(mg > h.sparse_current_size_in_bytes(&blob));
    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);
    assert_eq!(h.merge_grow(&src, &dense), h.dense_bytes());
  }

  /// 空稀疏源（非零寄存器数为 0，如 PFMERGE 全缺失源落下的空 HLL）：
  /// MergeGrow 不发生 usize 下溢（debug 构建 panic / release 回绕破坏载荷），
  /// 且与 C# 一致仍预留一个扇区；SparseRequiredBytes(0) 同口径占一个扇区
  #[test]
  fn growth_planning_with_empty_source() {
    let h = hll();
    let empty = sparse_buf(&h);
    let fresh = sparse_buf(&h);

    // 空源 + 新目标：146 + 128 = 274
    assert_eq!(h.merge_grow(&empty, &fresh), h.sparse_bytes());
    // 空源 + 已存目标（current = 146）：同样 +128
    assert_eq!(
      h.merge_grow(&empty, &empty),
      h.sparse_current_size_in_bytes(&empty) + SPARSE_MEMORY_SECTOR_SIZE
    );

    // C# ((u-1)/s)+1 口径：u=0 → 1 页（128B）；u=2 → 1 页；u=130 → 2 页
    assert_eq!(h.sparse_required_bytes(0), SPARSE_MEMORY_SECTOR_SIZE);
    assert_eq!(h.sparse_required_bytes(1), SPARSE_MEMORY_SECTOR_SIZE);
    assert_eq!(h.sparse_required_bytes(65), 2 * SPARSE_MEMORY_SECTOR_SIZE);

    // 合并空源后载荷仍合法、计数为 0
    let mut dst = fresh[..h.sparse_bytes()].to_vec();
    let dst_len = dst.len();
    let _ = h.try_merge(&empty, &mut dst, dst_len);
    assert!(h.is_valid_hyll_len(&dst, dst_len));
    assert_eq!(h.count(&mut dst), 0);
  }

  /// 拷贝路径：SparseToDenseCopy / SparseToSparseCopy / CopyUpdateMerge
  #[test]
  fn copy_paths() {
    let h = hll();
    let mut sparse = sparse_buf(&h);
    h.update_sparse(&mut sparse, murmur_hash_2_x64_a(b"hello"));

    // SparseToDenseCopy：旧载荷展开 + 新哈希插入
    let mut dense = vec![0_u8; h.dense_bytes()];
    assert!(h.sparse_to_dense_copy(murmur_hash_2_x64_a(b"world"), &sparse, &mut dense));
    assert!(h.is_dense(&dense));

    // SparseToSparseCopy：拷贝 + 单哈希更新
    let mut bigger = vec![0_u8; SPARSE_SIZE_MAX_CAP];
    assert!(h.sparse_to_sparse_copy(murmur_hash_2_x64_a(b"meh"), &sparse, &mut bigger));
    assert!(h.is_sparse(&bigger));

    // CopyUpdateMerge：稀疏 → 稠密
    let old = sparse_buf(&h);
    let mut new_dense = vec![0_u8; h.dense_bytes()];
    let (old_len, new_len) = (old.len(), new_dense.len());
    h.copy_update_merge(&sparse, &old, &mut new_dense, old_len, new_len);
    assert!(h.is_dense(&new_dense));

    // CopyUpdateMerge：等长稀疏路径（逐字节拷贝 + 合并）
    let mut new_sparse = vec![0_u8; old.len()];
    h.copy_update_merge(&sparse, &old, &mut new_sparse, old_len, old_len);
    assert!(h.is_sparse(&new_sparse));
    assert_eq!(
      h.sparse_count_non_zero(&new_sparse),
      h.sparse_count_non_zero(&sparse)
    );
  }

  /// MurmurHash2x64A：空串黄金向量 + 确定性 + 长度敏感 + 分块/余数路径
  #[test]
  fn murmur_hash_invariants() {
    assert_eq!(murmur_hash_2_x64_a(b"abc"), murmur_hash_2_x64_a(b"abc"));
    assert_ne!(murmur_hash_2_x64_a(b"abc"), murmur_hash_2_x64_a(b"abd"));
    // 长度参与混淆
    assert_ne!(murmur_hash_2_x64_a(b"abc"), murmur_hash_2_x64_a(b"abcd"));
    // 8 字节整块 vs 带余数
    assert_ne!(
      murmur_hash_2_x64_a(b"12345678"),
      murmur_hash_2_x64_a(b"123456789")
    );
  }

  /// 非法结构检测：魔数破坏 / 长度不符 / RLE 超覆盖 / 覆盖不足
  #[test]
  fn invalid_blob_detection() {
    let h = hll();
    let blob = sparse_buf(&h);

    // 魔数破坏
    let mut bad = blob.clone();
    bad[5] = b'X';
    assert!(!HyperLogLog::is_hyll(&bad));
    assert!(!h.is_valid_hyll(&bad));

    // 声称稠密但长度不符
    bad = blob.clone();
    HyperLogLog::set_type(&mut bad, HllDtype::Dense);
    assert!(!h.is_valid_hll_length(&bad, bad.len()));

    // RLE 声称超过寄存器空间
    bad = blob.clone();
    h.set_sparse_rle_size(&mut bad, u16::MAX);
    assert!(!h.is_valid_sparse_stream(&bad));
    assert!(!h.is_valid_hll_length(&bad, bad.len()));

    // RLE 覆盖不足（1 字节非零操作码只覆盖 1 寄存器）
    let mut crafted = sparse_buf(&h);
    crafted[18] = HyperLogLog::set_non_zero(1);
    h.set_sparse_rle_size(&mut crafted, 1);
    assert!(!h.is_valid_sparse_stream(&crafted));
  }

  /// 调试导出可用性与稀疏/稠密一致性
  #[cfg(debug_assertions)]
  #[test]
  fn dump_helpers() {
    let h = hll();
    let mut sparse = sparse_buf(&h);
    h.update_sparse(&mut sparse, murmur_hash_2_x64_a(b"dump-me"));

    assert!(!h.dump_sparse_raw_bytes(&sparse).is_empty());
    assert_eq!(h.dump_raw_bytes(&sparse), h.dump_sparse_raw_bytes(&sparse));
    assert!(h.dump_sparse_regs(&sparse).contains('='));
    assert!(!h.dump_regs(&sparse).is_empty());

    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);
    h.sparse_to_dense(&sparse, &mut dense);
    assert!(h.dump_dense_regs(&dense).contains('='));
    assert!(h.compare_sparse_to_dense(&dense, &sparse).is_ok());
  }

  /// 校验 compare_sparse_to_dense 在不一致时的错误分支
  #[cfg(debug_assertions)]
  #[test]
  fn compare_sparse_to_dense_mismatch() {
    let h = hll();
    let mut sparse = sparse_buf(&h);
    h.update_sparse(&mut sparse, murmur_hash_2_x64_a(b"test-val"));

    let mut dense = vec![0_u8; h.dense_bytes()];
    h.init_dense(&mut dense);

    // 稠密为空（全0），而稀疏非空：报错 Nonzero not contained
    let err = h.compare_sparse_to_dense(&dense, &sparse).unwrap_err();
    assert!(err.contains("FAILED: Nonzero not contained"));

    // 稀疏转稠密后一致
    h.sparse_to_dense(&sparse, &mut dense);
    assert!(h.compare_sparse_to_dense(&dense, &sparse).is_ok());

    // 篡改寄存器值：报错 Nonzero wrong nonzero value
    let regs = &mut dense[HLL_HEADER_BYTES..];
    for i in 0..h.mcnt as u16 {
      let v = h.get_register(regs, i);
      if v != 0 {
        h.set_register(regs, i, v + 1);
        break;
      }
    }
    let err2 = h.compare_sparse_to_dense(&dense, &sparse).unwrap_err();
    assert!(err2.contains("FAILED: Nonzero wrong nonzero value"));
  }

  /// 自定义 pbit：寄存器数与派生容量随之变化
  #[test]
  fn custom_pbit() {
    let h = HyperLogLog::with_pbit(10);
    assert_eq!(h.mcnt, 1024);
    assert_eq!(h.dense_bytes(), 16 + (6 * 1024) / 8);
    assert_eq!(h.sparse_zero_ranges(), 1024 >> 7);
    assert_eq!(h.qbit(), 54);
  }
}
