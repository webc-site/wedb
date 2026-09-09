//! 统一高性能哈希算法与集合库（全量基于 AES 硬件向量加速的 `gxhash` 后端）
//!
//! 与 C# Garnet 的对应关系（哈希值刻意不逐位兼容，统一 gxhash 后端后跨语言无需一致）：
//! - `fast_hash` / `compute_checksum` ← `Garnet.common.HashUtils.MurmurHash2x64A`
//!   与 `Tsavorite.core.Utility.HashBytes`（字节序列单次哈希）
//! - `fast_hash_u64` ← `Tsavorite.core.Utility.GetHashCode(long)`（整数派生键）
//! - `fast_hash_with_seed` ← `MurmurHash2x64A(span, seed)` 带种子用法（迁移 Sketch 槽位等）
//! - `hash128` / `fast_hash128` ← `MurmurHash3x128` 与 `RangeIndexManager.KeyId`（XxHash128 → Guid）
//! - [`StreamHasher`] ← `System.IO.Hashing.XxHash64` 流式用法
//!   （Append / GetHashAndReset；此处为非破坏 `finish` + 显式 `reset`，语义等价且更灵活）
//! - `HashMap` / `HashSet` / `GxBuildHasher` ← C# Dictionary/HashSet 默认随机化哈希防御
//! - [`GxPapayaMap`] / [`new_papaya_map`] ← C# `ConcurrentDictionary`（papaya 无锁并发字典 + gxhash 构建器）
//!
//! # 可移植性约束
//! gxhash 在编译期要求目标具备 AES + NEON（aarch64）/ SSE2（x86_64）指令集
//! （工作区 `.cargo/config.toml` 已配 `target-feature=+aes`），
//! 否则触发 `compile_error!`（需 `RUSTFLAGS="-C target-cpu=native"` 或等效 target-feature）。
//! 哈希值与 gxhash 后端及版本绑定，跨架构比特稳定；更换后端会使既有派生值失效，
//! 落盘/传输的校验值与派生索引须与算法版本绑定。

use core::{
  hash::{Hash, Hasher},
  mem::{align_of, offset_of, size_of},
};

// 集合与构建器直接重导出 gxhash（gxhash::{HashMap, HashSet} 即 std 容器 + GxBuildHasher 别名，无重复定义）
pub use gxhash::{GxBuildHasher, HashMap, HashMapExt, HashSet, HashSetExt};
pub use papaya;

/// 流式条带宽度（64 字节，匹配 gxhash 大输入 ILP 路径的 4 倍向量宽度）
const STRIPE: usize = 64;

/// 并行折叠链数（同链条带串行、异链独立，AES 指令流水线满吞吐；必须为 2 的幂供 `fold` 位掩码轮转）
const LANES: usize = 4;
const _: () = assert!(LANES.is_power_of_two(), "LANES 必须为 2 的幂");
// write 的 4 链展开循环（quads）与 LANE_SALT/DEFAULT_LANES 均按 4 链硬编码，编译期钉死
const _: () = assert!(
  LANES == 4,
  "write 的展开折叠路径按 4 链实现，改动 LANES 须同步重写"
);

/// 64 位黄金分割比常数（2^64 / φ，斐波那契散列乘数）
pub const GOLDEN_RATIO_64: u64 = 0x9E37_79B9_7F4A_7C15;

/// Stafford Variant 13 终末雪崩双射变换（极速单射，无分支、无查表）
#[inline(always)]
pub const fn mix13(mut z: u64) -> u64 {
  z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
  z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
  z ^ (z >> 31)
}

/// SplitMix64 极速单射变换
#[inline(always)]
pub const fn splitmix64(z: u64) -> u64 {
  mix13(z.wrapping_add(GOLDEN_RATIO_64))
}

/// 将线程 ID 或顺序序号均匀打散为无偏条带槽位索引
#[inline(always)]
pub const fn mix_thread_id(tid: u64) -> usize {
  splitmix64(tid) as usize
}

/// 各折叠链初始种子的派生盐（互异常数经双射混合保证链初态互异）
const LANE_SALT: [u64; LANES] = [
  0,
  GOLDEN_RATIO_64,
  0xC2B2_AE3D_27D4_EB4F,
  0x1656_67C9_1973_60D5,
];

/// 默认种子 0 对应的编译期预计算 4 折叠链初态（mix13 双射，编译期把盐扩散为无关的链初态）
const DEFAULT_LANES: [i64; LANES] = [
  mix13(LANE_SALT[0]) as i64,
  mix13(LANE_SALT[1]) as i64,
  mix13(LANE_SALT[2]) as i64,
  mix13(LANE_SALT[3]) as i64,
];

/// 按种子派生 4 条折叠链初态 (种子 0 恒等 `0 ^ salt == salt`，走编译期常量路径)
#[inline]
const fn lanes_for(seed: u64) -> [i64; LANES] {
  if seed == 0 {
    DEFAULT_LANES
  } else {
    [
      mix13(seed ^ LANE_SALT[0]) as i64,
      mix13(seed ^ LANE_SALT[1]) as i64,
      mix13(seed ^ LANE_SALT[2]) as i64,
      mix13(seed ^ LANE_SALT[3]) as i64,
    ]
  }
}

/// 基于 gxhash 硬件向量加速的流式校验和状态机
///
/// 具备严格的分块一致性（Chunk-Invariance）：
/// 无论切片如何分块追加（如 1B、7B、1024B），多次 `write` 后的 `finish()` 与整块数据单次调用恒等。
/// 折叠方式：整条带以全局条带号 `i` 轮转落入 4 条独立折叠链（`lane = i & 3`），
/// 同链 `lane = gxhash64(stripe, lane)` 串行折叠、异链完全并行，AES 流水线无空泡；
/// `finish` 将 4 条链依序汇合为末端状态，再与尾部残留 + 总长度小端拼接做末端混合，
/// 长度参与雪崩，杜绝前后缀拼接歧义。
#[repr(C, align(64))]
#[derive(Clone, Debug)]
pub struct StreamHasher {
  /// 4 条并行折叠链（gxhash 种子按位解释为 i64）
  lanes: [i64; LANES],
  /// 初始种子（`reset` 回到该种子初态）
  seed: u64,
  /// 已折叠的全局条带数（决定下一条带的链归属）
  stripes: u64,
  /// 累计写入总字节数
  total: u64,
  /// 内部缓冲区有效字节数（恒 < STRIPE）
  buf_len: usize,
  /// 残留字节缓冲区（天然对齐到 64 字节缓存行边界，SIMD 向量读写无跨缓存行惩罚）
  buf: [u8; STRIPE],
}

const _: () = assert!(size_of::<StreamHasher>() == 128);
const _: () = assert!(align_of::<StreamHasher>() == 64);
const _: () = assert!(offset_of!(StreamHasher, buf) == 64);

impl Default for StreamHasher {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl StreamHasher {
  /// 创建默认种子 (0) 的流式校验器
  #[inline]
  pub const fn new() -> Self {
    Self {
      lanes: DEFAULT_LANES,
      seed: 0,
      stripes: 0,
      total: 0,
      buf_len: 0,
      buf: [0; STRIPE],
    }
  }

  /// 创建指定种子的流式校验器
  #[inline]
  pub const fn with_seed(seed: u64) -> Self {
    Self {
      lanes: lanes_for(seed),
      seed,
      stripes: 0,
      total: 0,
      buf_len: 0,
      buf: [0; STRIPE],
    }
  }

  /// 将整条带折叠进链 `stripes & 3`（条带号轮转均衡各链负载）
  #[inline(always)]
  fn fold(lanes: &mut [i64; LANES], stripes: &mut u64, stripe: &[u8; STRIPE]) {
    let lane = *stripes as usize & (LANES - 1);
    // SAFETY: lane 由 `stripes & (LANES - 1)` 计算，由于 LANES 为 4，lane 恒在 0..4 范围内
    let lane_ref = unsafe { lanes.get_unchecked_mut(lane) };
    *lane_ref = gxhash::gxhash64(stripe, *lane_ref) as i64;
    *stripes += 1;
  }

  /// 增量写入分块切片（任意分片边界均与单次整体计算恒等）
  #[inline]
  pub fn write(&mut self, mut bytes: &[u8]) {
    if bytes.is_empty() {
      return;
    }
    self.total = self.total.wrapping_add(bytes.len() as u64);

    // 1. 先补满缓冲区残留，凑齐整条带后折叠
    if self.buf_len > 0 {
      let needed = STRIPE - self.buf_len;
      if bytes.len() < needed {
        let next_len = self.buf_len + bytes.len();
        self.buf[self.buf_len..next_len].copy_from_slice(bytes);
        self.buf_len = next_len;
        return;
      }
      self.buf[self.buf_len..].copy_from_slice(&bytes[..needed]);
      Self::fold(&mut self.lanes, &mut self.stripes, &self.buf);
      self.buf_len = 0;
      bytes = &bytes[needed..];
    }

    // 2. 整条带直接从输入切片折叠（零拷贝）
    let (mut stripes, tail) = bytes.as_chunks::<STRIPE>();

    // 2.1 若当前条带未对齐到 4 链边界，先单条带折叠使其对齐
    while (self.stripes as usize & (LANES - 1)) != 0 && !stripes.is_empty() {
      Self::fold(&mut self.lanes, &mut self.stripes, &stripes[0]);
      stripes = &stripes[1..];
    }

    // 2.2 4 链并行折叠：展开 4 条独立折叠链，完全消除循环内寻址计算与数据依赖，AES 流水线满载
    let (quads, rem_stripes) = stripes.as_chunks::<LANES>();
    for quad in quads {
      self.lanes[0] = gxhash::gxhash64(&quad[0], self.lanes[0]) as i64;
      self.lanes[1] = gxhash::gxhash64(&quad[1], self.lanes[1]) as i64;
      self.lanes[2] = gxhash::gxhash64(&quad[2], self.lanes[2]) as i64;
      self.lanes[3] = gxhash::gxhash64(&quad[3], self.lanes[3]) as i64;
    }
    self.stripes += (quads.len() * LANES) as u64;

    // 2.3 折叠末尾不足 4 条带的残留条带（0..3）
    for stripe in rem_stripes {
      Self::fold(&mut self.lanes, &mut self.stripes, stripe);
    }

    // 3. 仅尾部不足一条带的数据拷贝进缓冲区
    self.buf[..tail.len()].copy_from_slice(tail);
    self.buf_len = tail.len();
  }

  /// 计算最终校验和（非破坏性：可重复调用，之后仍可继续 `write`）
  #[inline]
  pub fn finish(&self) -> u64 {
    debug_assert!(self.buf_len < STRIPE);
    // 4 条折叠链依序汇合为末端状态（展开 4 步无循环开销）
    let mut state = self.seed as i64;
    state = gxhash::gxhash64(&self.lanes[0].to_le_bytes(), state) as i64;
    state = gxhash::gxhash64(&self.lanes[1].to_le_bytes(), state) as i64;
    state = gxhash::gxhash64(&self.lanes[2].to_le_bytes(), state) as i64;
    state = gxhash::gxhash64(&self.lanes[3].to_le_bytes(), state) as i64;

    if self.buf_len == 0 {
      // 无残留快速路径：直接传入总长度，完全消除栈分配与内存拷贝
      gxhash::gxhash64(&self.total.to_le_bytes(), state)
    } else {
      // 尾部残留 + 64 位总长度小端拼接，长度参与末端混合杜绝长度扩展/前后缀歧义
      let mut tail = [0u8; STRIPE + 8];
      tail[..self.buf_len].copy_from_slice(&self.buf[..self.buf_len]);
      let end = self.buf_len + 8;
      tail[self.buf_len..end].copy_from_slice(&self.total.to_le_bytes());
      gxhash::gxhash64(&tail[..end], state)
    }
  }

  /// 复位到种子初始态（保留构造时种子，对标 `XxHash64.Reset`）
  ///
  /// 仅复位折叠链与计数器，保留内部缓冲区内存，避免冗余的 64 字节内存清零
  #[inline]
  pub fn reset(&mut self) {
    self.lanes = lanes_for(self.seed);
    self.stripes = 0;
    self.total = 0;
    self.buf_len = 0;
  }

}

impl Hasher for StreamHasher {
  #[inline]
  fn finish(&self) -> u64 {
    self.finish()
  }

  #[inline]
  fn write(&mut self, bytes: &[u8]) {
    self.write(bytes);
  }
}

/// 计算字节序列的 64 位校验和（基于 gxhash 硬件向量加速；
/// 对标 C# RangeIndexChunkedSerializer/Deserializer 中 XxHash64 单次整体计算）
///
/// 走 `new()` 的编译期预计算 `DEFAULT_LANES` 常量路径，种子 0 无任何运行时初始化开销
/// （与 `with_seed(0)` 数学恒等，由测试锁定）
#[inline]
pub fn compute_checksum(data: &[u8]) -> u64 {
  let mut hasher = StreamHasher::new();
  hasher.write(data);
  hasher.finish()
}

/// 带有自定义种子的 64 位校验和（基于 gxhash 硬件向量加速，对标 XxHash64(seed) 构造）
#[inline]
pub fn compute_checksum_with_seed(data: &[u8], seed: u64) -> u64 {
  let mut hasher = StreamHasher::with_seed(seed);
  hasher.write(data);
  hasher.finish()
}

/// 创建使用默认硬件向量加速构建器的空 HashMap
#[inline]
pub fn new_hash_map<K, V>() -> HashMap<K, V> {
  HashMap::with_hasher(GxBuildHasher::default())
}

/// 创建带初始容量的 HashSet
#[inline]
pub fn hash_set_with_capacity<T>(capacity: usize) -> HashSet<T> {
  HashSet::with_capacity_and_hasher(capacity, GxBuildHasher::default())
}

/// 基于硬件向量加速 gxhash 构建器的无锁高并发字典类型
///
/// papaya 并发哈希字典（无锁读、分段写）统一搭载 [`GxBuildHasher`]，
/// 全项目唯一出处：各 crate 一律 `use whasher::{GxPapayaMap, new_papaya_map}`，禁止本地重复定义
pub type GxPapayaMap<K, V> = papaya::HashMap<K, V, GxBuildHasher>;

/// 创建搭载硬件向量加速 gxhash 构建器的无锁并发字典
#[inline]
pub fn new_papaya_map<K, V>() -> GxPapayaMap<K, V> {
  papaya::HashMap::builder()
    .hasher(GxBuildHasher::default())
    .build()
}

/// 单次快速 64 位键哈希（采用 gxhash 硬件向量加速指令，单次耗时 ~0.31ns）
///
/// 对标 C# `HashUtils.MurmurHash2x64A` 与 `Utility.HashBytes`（HyperLogLog、迁移 Sketch、
/// 锁分段等键哈希场景统一由此承担）。固定种子 0，跨进程跨版本确定性；
/// 索引 tag、事务冲突检测等派生数据可安全落盘。
#[inline(always)]
pub fn fast_hash(bytes: &[u8]) -> u64 {
  gxhash::gxhash64(bytes, 0)
}

/// 单次快速 64 位整数哈希（对标 C# `Utility.GetHashCode(long)`，~0.31ns）
///
/// 固定种子 0，跨进程跨版本确定性；索引槽位、冲突检测等整数派生键可安全落盘。
/// i64 与 u64 按位同型，有符号调用方直接 `as u64` 位转换即可。
#[inline(always)]
pub fn fast_hash_u64(val: u64) -> u64 {
  fast_hash(&val.to_le_bytes())
}

/// 带自定义 64 位种子的单次快速键哈希（对标 C# `MurmurHash2x64A(span, seed)` 的种子用法）
#[inline(always)]
pub fn fast_hash_with_seed(bytes: &[u8], seed: u64) -> u64 {
  gxhash::gxhash64(bytes, seed as i64)
}

/// 默认种子 0 的单次快速 128 位哈希计算（对标 fast_hash 与 C# `MurmurHash3x64` 取首字路径）
#[inline(always)]
pub fn fast_hash128(bytes: &[u8]) -> u128 {
  gxhash::gxhash128(bytes, 0)
}

/// 双种子非线性合并为 gxhash 单种子（mix13 双射先打散再异或错位）
///
/// 直接 `a ^ rotl(b, 32)` 是 GF(2) 线性映射（128→64 位，核空间 64 维），
/// 结构化种子对会确定性碰撞（如 `(rotl(b,32), b)` 对任意 b 恒映射到 0）；
/// 先经 mix13 非线性双射再合并，代数结构碰撞被消除，只剩 128→64 固有的生日界随机碰撞
#[inline(always)]
const fn combine_seed(seed_a: u64, seed_b: u64) -> i64 {
  (mix13(seed_a) ^ mix13(seed_b).rotate_left(32)) as i64
}

/// 128 位强抗碰撞键 ID 计算（基于 gxhash::gxhash128 硬件向量加速，
/// 对标 C# `RangeIndexManager.KeyId` 的 XxHash128 → Guid 派生；C# 单种子，此处双种子域分离）
#[inline(always)]
pub fn hash128(bytes: &[u8], seed_a: u64, seed_b: u64) -> u128 {
  gxhash::gxhash128(bytes, combine_seed(seed_a, seed_b))
}

/// 为支持 Hash trait 的泛型对象快速计算确定性的 64 位哈希值
#[inline]
pub fn hash_value<T: Hash + ?Sized>(value: &T) -> u64 {
  let mut hasher = gxhash::GxHasher::with_seed(0);
  value.hash(&mut hasher);
  hasher.finish()
}

