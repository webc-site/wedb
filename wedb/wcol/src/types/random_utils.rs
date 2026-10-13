//! 随机工具（对标 libs/common/RandomUtils.cs）
//!
//! 自研偏差锁: doc/zh/deviations.md SRANDMEMBER 采样域（fastrand 面）

use fastrand::Rng;
use wbase::map::{HashSet, HashSetExt};

/// HRANDFIELD/ZRANDMEMBER arg1 压缩字解包单源（信封态对象层与分层树内臂共用，
/// 对位 C# ObjectInput.arg1）：`(count << 1 | includedCount) << 1 | withFlag`
///
/// wnode 侧 `parse_random_member_args` 的打包口径（`RandomMemberArgs`）与之
/// 互为镜像；位段解包两份内联（hash_object_impl / sorted_set_object_impl）
/// 收敛于此
#[derive(Debug, Clone, Copy)]
pub(crate) struct RandomMemberOpts {
  /// 采样数（负数=可重复；缺省形态命令层传 1）
  pub count: i64,
  /// 附带值/分值（WITHVALUES/WITHSCORES，bit0）
  pub with_values: bool,
  /// 是否显式给了 count（bit1）
  pub included_count: bool,
}

impl RandomMemberOpts {
  /// 自 arg1 压缩字解包（>>2 取 count、&1 取 with、>>1&1 取 includedCount）
  #[inline]
  pub(crate) const fn from_arg1(arg1: i32) -> Self {
    Self {
      count: (arg1 >> 2) as i64,
      with_values: (arg1 & 1) == 1,
      included_count: ((arg1 >> 1) & 1) == 1,
    }
  }
}

/// 从 n 个元素中随机取 k 个下标逐个交 `sink`（HRANDFIELD/SRANDMEMBER/ZRANDMEMBER 共用）
///
/// libs/common/RandomUtils.cs:PickKRandomIndexes
///
/// 刻意差异（对照 C#）：.NET `Random(seed)` 的洗牌/迭代抽取序列与 fastrand 不同，
/// 仅保语义等价。分支结构 1:1 对齐：
/// - `distinct=false` 或 `k/n < K_OVER_N_THRESHOLD` 走迭代抽取（distinct 用
///   拒绝采样，O(k) 空间，C# PickKRandomIndexesIteratively）；
/// - 否则全量洗牌取前 k（C# PickKRandomDistinctIndexesWithShuffle）。
///
/// 下标流式 sink 产出、放回臂零存储：负 count 的 |k| 由客户端参数直控、与集合
/// 基数脱钩，C# 侧 `new int[countParameter]`（SetObjectImpl.cs:219 等）在该臂
/// 是连接级 OOM 面，rust 侧预分配更会放大为 GB 级单命令分配乃至分配失败 abort
/// 全进程——故禁止任何按 k 的预分配，消费点逐下标直写 RESP，空间 O(1)。
/// 空集/零取样不调 `sink`（C# `Random.Next(0)` 抛 ArgumentOutOfRangeException，
/// 按无结果处理）
pub fn pick_k_random_indexes(
  n: usize,
  k: usize,
  seed: i32,
  distinct: bool,
  mut sink: impl FnMut(usize),
) {
  /// k/n 低于该阈值走迭代抽取（C# RandomUtils.KOverNThreshold）
  const K_OVER_N_THRESHOLD: f64 = 0.1;

  let mut rng = Rng::with_seed(u64::from(seed as u32));
  if n == 0 || k == 0 {
    return;
  }

  if !distinct {
    // 放回臂：k 可为 |count| 极值，逐个产出零存储
    for _ in 0..k {
      sink(rng.usize(..n));
    }
    return;
  }

  if (k as f64) / (n as f64) < K_OVER_N_THRESHOLD {
    // 拒绝采样：k < n 才入此臂，k 受集合基数约束，O(k) 空间有界
    let mut picked = HashSet::with_capacity(k);
    let mut emitted = 0;
    while emitted < k {
      let idx = rng.usize(..n);
      if picked.insert(idx) {
        sink(idx);
        emitted += 1;
      }
    }
  } else {
    // 部分洗牌取前 k（k == n 时即全量洗牌）；置换域 n 即集合基数，
    // 分配与对象本体同阶
    let mut perm: Vec<usize> = (0..n).collect();
    for i in 0..k.min(n) {
      let j = rng.usize(i..perm.len());
      perm.swap(i, j);
    }
    perm.truncate(k);
    perm.into_iter().for_each(sink);
  }
}

/// 单下标随机取（HRANDFIELD/SRANDMEMBER 无 count 形态）
///
/// libs/common/RandomUtils.cs:PickRandomIndex（.NET rand 为非负随机数，% 取模）
#[inline]
pub fn pick_random_index(n: usize, rand: i32) -> usize {
  (rand as u32 as usize) % n
}
