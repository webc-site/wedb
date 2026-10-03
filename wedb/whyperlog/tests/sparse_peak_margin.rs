//! 稀疏折叠峰值容纳界对抗回归（task zcode-r151c-pfconv 案一测试点·whyperlog 面）
//!
//! 钉 `HyperLogLog::sparse_fits` 唯一容纳谓词的代数本质：稀疏流批量更新的
//! 物理写峰 = 当前占用 + 2B/单元 + 1B 尾移峰值字节（UpdateSparseReg 后缀
//! 右移 `end + blen - 1`，blen ≤ 3），容纳充要条件即 `current + 2*count <
//! alloc`。C# 命令路径恒逐元素 count=1（HyperLogLogCommands.cs:34
//! Slice(i,1)），该裕度结构性恒成立；rust 批量折叠分配点（建键上探/扩容
//! 复检/合并复检）以此谓词为单判据。断言全为纯算形 + 真实落笔复验，
//! 不捕获 panic。

use whyperlog::{HyperLogLog, SPARSE_SIZE_MAX_CAP};

fn hll() -> HyperLogLog {
  HyperLogLog::new()
}

/// 逐枚隔离布点：寄存器下标 4i+1（彼此隔 ≥3 枚零寄存器且恒不落零段末格），
/// 每枚拆分零段恒 +2B（最坏 Δ 形）；返回即全部插入成功
fn insert_isolated(hll: &HyperLogLog, blob: &mut [u8], from: usize, count: usize) {
  for i in 0..count {
    let idx = (4 * (from + i) + 1) as u16;
    assert!(hll.update_sparse_reg(blob, idx, 1), "布点 {idx} 须发生变更");
  }
}

/// 谓词代数 = 1B 峰值裕度：N=65 最坏流峰 = 零段基座 146 + 2N + 1 = 277；
/// C# SparseInitialLength 折叠出形 274（2N 扇区取整不含基座与裕度）恒被拒；
/// 最小容纳形 277B 上 65 枚隔离布点真实落笔后占用恰 276、流与帧校验恒真
#[test]
fn fits_is_exact_peak_margin_and_worst_flow_lands() {
  let h = hll();
  let base = h.sparse_init_current();
  assert_eq!(base, 146, "init_sparse 基座 = 稀疏头 18 + 零段 128");

  // 界代数：容纳 ⟺ current + 2N < alloc ⟺ peak(current+2N+1) ≤ alloc
  assert!(!h.sparse_fits(base, 65, 276));
  assert!(h.sparse_fits(base, 65, 277));
  // 现树折叠初始形（= SparseInitialLength(65) = 18+roundup128(130) = 274）必拒
  let initial65 = h.sparse_initial_length(65);
  assert_eq!(initial65, 274, "C# 镜像式不变（兼 frame 下限职）");
  assert!(
    !h.sparse_fits(base, 65, initial65),
    "案一击穿形：初始分配形缺零段基座与 1B 裕度，折叠谓词须拒"
  );

  // 最小容纳形真实落笔：277B 上 65 枚隔位布点（每枚 Δ=2 最坏形），
  // 末枚尾移写峰恰触分配末格（索引 276 = len-1），流与帧校验恒真
  let mut blob = vec![0_u8; 277];
  h.init_sparse(&mut blob);
  insert_isolated(&h, &mut blob, 0, 65);
  assert_eq!(h.sparse_current_size_in_bytes(&blob), base + 2 * 65);
  assert!(h.is_valid_sparse_stream(&blob));
  assert!(h.is_valid_hyll_len(&blob, blob.len()));
}

/// 既有稀疏键 N=64 等界扩容形：update_grow 出形 current+roundup128(2N) 恰
/// 缺 1B 裕度，谓词必拒（消费点升稠密）；N=63 形裕度成立，谓词放行且 63 枚
/// 隔位布点在出形缓冲上真实落笔、流恒合法
#[test]
fn equal_boundary_grow_shape_rejected_by_predicate() {
  let h = hll();
  // 占用 246 的既有键：274B 分配上 50 枚隔位布点（全 Δ=2 最坏流）
  let mut blob = vec![0_u8; h.sparse_bytes()];
  h.init_sparse(&mut blob);
  insert_isolated(&h, &mut blob, 0, 50);
  let current = h.sparse_current_size_in_bytes(&blob);
  assert_eq!(current, 246);

  // N=64：2N 恰整扇区 → 出形 374 缺裕度，谓词拒（等号界即击穿点）
  let grow64 = h.update_grow(64, &blob);
  assert_eq!(grow64, current + SPARSE_SIZE_MAX_CAP / 32); // 246 + 128
  assert!(!h.sparse_fits(current, 64, grow64));

  // N=63：同出形 374 而 2N=126 留裕 → 放行；在出形缓冲上真实落笔复验
  let grow63 = h.update_grow(63, &blob);
  assert_eq!(grow63, grow64);
  assert!(h.sparse_fits(current, 63, grow63));
  let mut grown = vec![0_u8; grow63];
  h.init_sparse(&mut grown);
  grown[..current].copy_from_slice(&blob[..current]);
  insert_isolated(&h, &mut grown, 50, 63);
  assert_eq!(h.sparse_current_size_in_bytes(&grown), current + 2 * 63);
  assert!(h.is_valid_sparse_stream(&grown));
  assert!(h.is_valid_hyll(&grown));
}

/// crafted rle=payload 等号合法满容量稀疏源（64 枚隔位非零、274B 实占即
/// 分配）：帧校验含等号即收（对标 C# IsValidHLLLength 不查写裕度）；作
/// PFMERGE 源并入 274B 新种目标时 merge_grow 出形恰等原位长且缺裕度，
/// 唯一谓词与同源 try_merge 原位臂须同判拒（消费点升稠密，见案一 c）
#[test]
fn crafted_equality_source_rejected_by_inplace_merge() {
  let h = hll();
  let mut src = vec![0_u8; SPARSE_SIZE_MAX_CAP];
  h.init_sparse(&mut src);
  insert_isolated(&h, &mut src, 0, 64);
  src.truncate(h.sparse_current_size_in_bytes(&src));
  assert_eq!(src.len(), 274);
  assert!(
    h.is_valid_hyll(&src),
    "rle=payload 等号形合法（校验不含写裕度）"
  );
  assert_eq!(h.sparse_count_non_zero(&src), 64);

  // 274B 新种目标（current=146）：merge_grow 出形恰等原位分配
  let mut dst = vec![0_u8; h.sparse_bytes()];
  h.init_sparse(&mut dst);
  let mg = h.merge_grow(&src, &dst);
  assert_eq!(mg, dst.len());
  assert!(
    !h.sparse_fits(h.sparse_current_size_in_bytes(&dst), 64, mg),
    "64×Δ2 满 sector 等界形缺 1B 裕度，谓词须拒"
  );
  // try_merge 原位臂同源单判据：判拒且零字节落笔
  let before = dst.clone();
  let alloc = dst.len();
  assert!(!h.try_merge(&src, &mut dst, alloc));
  assert_eq!(dst, before, "容纳判拒即零覆写（C# InPlace 败形同构）");
}
