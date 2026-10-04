use whyperlog::{
  ALPHA, HLL_HEADER_BYTES, HllDtype, HyperLogLog, SPARSE_MEMORY_SECTOR_SIZE, SPARSE_SIZE_MAX_CAP,
  murmur_hash_2_x64_a, round_estimate,
};

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

/// x.5 中点落点：银行家舍入取偶（C# Math.Round 默认口径；
/// half-away-from-zero 会在 2.5/4.5 处给出 3/5）
#[test]
fn midpoint_ties_round_to_even() {
  assert_eq!(round_estimate(0.5), 0);
  assert_eq!(round_estimate(1.5), 2);
  assert_eq!(round_estimate(2.5), 2);
  assert_eq!(round_estimate(3.5), 4);
  assert_eq!(round_estimate(4.5), 4);
  assert_eq!(round_estimate(5.5), 6);
}

/// 非中点落点与 f64::round 行为一致（舍入口径仅在中点分叉）
#[test]
fn non_midpoint_unaffected() {
  assert_eq!(round_estimate(2.4), 2);
  assert_eq!(round_estimate(2.6), 3);
  assert_eq!(round_estimate(0.0), 0);
  assert_eq!(round_estimate(1024.5), 1024);
  assert_eq!(round_estimate(1025.5), 1026);
}

/// 越界档出口：非有限与达上界者一律得**负**哨兵（C# unchecked `(long)` 转换的
/// 可观测不变式——越界估计恒负 ⇒ 基数缓存恒判失效；rust 的饱和转换会得
/// 正极大使缓存转真，故此处必须与饱和形分道）。
/// 对位 C# HyperLogLog.cs 的 CountSparseNCEstimator 与 IsValidCard 不变式。
#[test]
fn out_of_range_estimate_is_negative_sentinel() {
  assert_eq!(round_estimate(f64::INFINITY), i64::MIN);
  assert_eq!(round_estimate(f64::NEG_INFINITY), i64::MIN);
  assert_eq!(round_estimate(f64::NAN), i64::MIN);
  // 上界 i64::MAX as f64（即 2^63）本身不可由 i64 表示 ⇒ 越界档
  assert_eq!(round_estimate(i64::MAX as f64), i64::MIN);
  // 界内最靠近上界的可表浮点数（2^63 在 f64 下相邻 ULP 为 2048.0）不得饱和、不得折成哨兵
  const PREV_F64: f64 = (i64::MAX as f64) - 2048.0;
  assert_eq!(round_estimate(PREV_F64), PREV_F64 as i64);
  // 下界 -2^63 恰可表示，属界内
  assert_eq!(round_estimate(i64::MIN as f64), i64::MIN);
}

/// 注入式稠密载荷构造：HYLL 魔数 + type = 0x01 + 长度恰为 dense_bytes，
/// 寄存器字节全 0xFF（16384 个 6 位寄存器恒为死区值 63）
///
/// 对位 C# HyperLogLog.cs 中 IsValidHLLLength 的稠密臂仅验
/// 长度（不核寄存器值域），故此载荷两侧皆过校验。基数缓存位取自 init_dense
/// 的失效哨兵（真实存储里的稠密载荷经 `::HyperLogLog::update_dense_register`
/// / `::dense_to_dense` 的 `set_card(i64::MIN)` 同样恒处失效态），否则字节 8..16
/// 全零会被 `::is_valid_card` 判为合法缓存、在到达估计器前短路
fn forged_dense_all_dead_zone(h: &HyperLogLog) -> Vec<u8> {
  let mut blob = vec![0_u8; h.dense_bytes()];
  h.init_dense(&mut blob);
  blob[HLL_HEADER_BYTES..].fill(0xFF);
  blob
}

/// 越界估计（`E = +inf` 形）绝不被当作合法基数缓存
///
/// 全 16384 寄存器恒 63 落在 `qbit + 2..=` 死区，对 z 零贡献：
/// `z = mcnt * cTau(1.0) = 0`（无寄存器取 qbit+1）、折半链加数全 0、
/// `mcnt * cSigma(0.0) = 0` ⇒ `z = 0.0` ⇒ `E = ALPHA * mcnt² / 0.0 = +inf`。
/// 该 E 经 round_estimate 收口为负哨兵，`set_card` 写入的缓存基数恒判失效，
/// 二次 count 仍走估计器（缓存短路臂的前置 `is_valid_card` 恒假）。
/// 对位 C# HyperLogLog.cs 的 CountDenseNCEstimator 与 Count 方法。
#[test]
fn infinite_estimate_is_never_cached_as_valid_card() {
  let h = hll();
  let mut blob = forged_dense_all_dead_zone(&h);

  // 前提钉死：畸形稠密载荷过校验（两侧等形，非分叉），病灶只在估计器出口
  assert!(h.is_valid_hyll(&blob));
  assert!(h.is_dense(&blob));
  assert!(
    !HyperLogLog::is_valid_card(&blob),
    "前置缓存须失效，估计器臂方可达"
  );
  assert_eq!(h.get_register(&blob[HLL_HEADER_BYTES..], 0), 63);
  assert_eq!(h.get_register(&blob[HLL_HEADER_BYTES..], 16383), 63);

  let c1 = h.count(&mut blob);
  assert!(c1 < 0, "越界估计须为负哨兵，实得 {c1}");
  assert!(HyperLogLog::get_card(&blob) < 0, "缓存基数不得转真");
  assert!(!HyperLogLog::is_valid_card(&blob));

  // 缓存恒失效 ⇒ 二次 count 必重算（短路臂无从触发），返回值同负
  let c2 = h.count(&mut blob);
  assert!(c2 < 0, "二次 count 不得回缓存垃圾基数，实得 {c2}");
  assert_eq!(c2, c1, "两轮重算结果须一致");
}

/// 越界估计（有限越界形）绝不被当作合法基数缓存
///
/// 仅 1 个寄存器落在 `1..=qbit`（置 qbit = 50）、其余 16383 个落死区：
/// z 只在折半链 j = 50 处得 1，续 halving 至 j = 1 ⇒ `z = 2^-50 ≈ 8.88e-16`，
/// `E = ALPHA * mcnt² / 2^-50 ≈ 2.18e23` —— 有限但越 i64 上界（2^63 ≈ 9.22e18），
/// 与 `+inf` 形同走越界档。
/// 对位 C# HyperLogLog.cs 的 CountDenseNCEstimator 方法。
#[test]
fn finite_out_of_range_estimate_is_never_cached_as_valid_card() {
  let h = hll();
  let mut blob = forged_dense_all_dead_zone(&h);
  let regs_last = (h.mcnt() - 1) as u16;
  h.set_register(&mut blob[HLL_HEADER_BYTES..], 0, h.qbit());

  // 构造自检：目标寄存器落在唯一存活档，其余仍为死区值 63
  let view = &blob[HLL_HEADER_BYTES..];
  assert_eq!(h.get_register(view, 0), h.qbit());
  assert_eq!(h.get_register(view, 1), 63);
  assert_eq!(h.get_register(view, regs_last), 63);

  let c1 = h.count(&mut blob);
  assert!(c1 < 0, "有限越界估计须为负哨兵，实得 {c1}");
  assert!(HyperLogLog::get_card(&blob) < 0, "缓存基数不得转真");
  assert!(!HyperLogLog::is_valid_card(&blob));

  let c2 = h.count(&mut blob);
  assert!(c2 < 0, "二次 count 不得回缓存垃圾基数，实得 {c2}");
  assert_eq!(c2, c1, "两轮重算结果须一致");
}

/// 结构常量与 C# 逐项对齐：寄存器数、编码字节数、误差常数
#[test]
fn layout_constants() {
  let h = hll();
  assert_eq!(h.mcnt(), 16384);
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
  let idx_last = (h.mcnt() - 1) as u16;
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

/// 拷贝路径：CopyUpdate（稠密化/扩容两臂）与 CopyUpdateMerge
#[test]
fn copy_paths() {
  let h = hll();
  let mut sparse = sparse_buf(&h);
  h.update_sparse(&mut sparse, murmur_hash_2_x64_a(b"hello"));

  // CopyUpdate 稠密化臂：旧载荷展开 + 新哈希插入
  let mut dense = vec![0_u8; h.dense_bytes()];
  assert!(h.copy_update(&[b"world".as_slice()], &sparse, &mut dense));
  assert!(h.is_dense(&dense));

  // CopyUpdate 扩容臂：拷贝 + 批量插入
  let mut bigger = vec![0_u8; SPARSE_SIZE_MAX_CAP];
  assert!(h.copy_update(&[b"meh".as_slice()], &sparse, &mut bigger));
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

/// MurmurHash2x64A：差分分布 + 长度敏感 + 分块/余数路径
#[test]
fn murmur_hash_invariants() {
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

/// 自定义 pbit：寄存器数与派生容量随之变化
#[test]
fn custom_pbit() {
  let h = HyperLogLog::with_pbit(10);
  assert_eq!(h.mcnt(), 1024);
  assert_eq!(h.dense_bytes(), 16 + (6 * 1024) / 8);
  assert_eq!(h.sparse_zero_ranges(), 1024 >> 7);
  assert_eq!(h.qbit(), 54);
}
