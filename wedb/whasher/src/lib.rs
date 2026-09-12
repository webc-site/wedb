//! 统一高性能哈希算法与集合库（全量基于 AES 硬件向量加速的 `gxhash` 后端）
//!
//! # 与 `wbase::hash` 的职能分工
//! - `wbase::hash` 提供与 Redis / Garnet 严格逐位兼容的 MurmurHash2 实现，
//!   专供 Redis HyperLogLog 等协议比特级二进制兼容场景使用；
//! - `whasher` 基于硬件向量加速的 `gxhash`，提供通用极致性能哈希（索引 tag、锁分段、
//!   迁移 Sketch、校验和等通用场景，哈希值刻意不与 C# Murmur 逐位兼容）。
//!
//! 与 C# Garnet 的对应关系：
//! - [`fast_hash`] ← `libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs:HashBytes`
//!   （`SpanByteComparer` / `GarnetKeyComparer` 承担的通用字节序列键哈希）；
//!   ≥ 32 KiB 大输入路由到 [`StreamHasher`] 折叠族保留域种子（C# `HashBytes`
//!   全宽 `ulong len` 混入长度，gxhash 3.5.0 单次直算长度仅以 u32 混入且折叠
//!   可交换，路由补齐同等抗碰撞语义，见 [`fast_hash`] 注释）
//! - [`fast_hash_u64`] ← `libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs:GetHashCode`（整数派生键）
//! - [`fast_hash_with_seed`] ← `libs/common/HashUtils.cs:MurmurHash2x64A` 带种子用法
//!   （迁移 `libs/cluster/Server/Migration/Sketch.cs` 以命名空间为种子做槽位域分离）
//! - [`hash128`] ← `libs/server/Resp/RangeIndex/RangeIndexManager.cs:KeyId`
//!   （XxHash128 → Guid 派生；1:1 实现位于 `wbftree::RangeIndexManager::key_id`，
//!   C# 单种子，此处经 `combine_seed` 双种子域分离；跨语言不要求同值，
//!   前缀确定性仅由 rust 侧自身一致性承载，见 [`hash128`] 注释；
//!   ≥ 32 KiB 大输入路由到双独立流式校验和，规避 gxhash 单次直算的结构性碰撞族）
//! - [`StreamHasher`] / [`compute_checksum`] / [`compute_checksum_with_seed`]
//!   ← `libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs` 与
//!   `RangeIndexChunkedDeserializer.cs` 的 `System.IO.Hashing.XxHash64` 流式校验
//!   （Append / GetHashAndReset；此处为非破坏 `finish` + 显式 `reset`，语义等价且更灵活）
//! - [`HashMap`] / [`HashSet`] / [`GxBuildHasher`] ← C# Dictionary/HashSet 默认随机化哈希防御
//! - [`GxPapayaMap`] / [`new_papaya_map`] ← C# `ConcurrentDictionary`
//!   （papaya 无锁并发字典 + gxhash 构建器；单一来源 `wbase::map::ConcurrentMap`，此处为门面别名再导出）
//! - [`mix13`] / [`splitmix64`] / [`mix_thread_id`] / [`GOLDEN_RATIO_64`]：
//!   Rust 侧条带锁/分片基础设施原语，无 C# 函数一一对应
//!
//! # 可移植性约束
//! gxhash 在编译期要求目标具备 AES + NEON（aarch64）/ SSE2（x86_64）指令集
//! （工作区 `.cargo/config.toml` 已配 `target-feature=+aes`），
//! 否则触发 `compile_error!`（需 `RUSTFLAGS="-C target-cpu=native"` 或等效 target-feature）。
//! 哈希值与 gxhash 后端及版本绑定，跨架构比特稳定；更换后端会使既有派生值失效，
//! 落盘/传输的校验值与派生索引须与算法版本绑定。

use core::{
  hash::Hasher,
  mem::{align_of, offset_of, size_of},
};

// 集合与构建器直接重导出 gxhash（gxhash::{HashMap, HashSet} 即 std 容器 + GxBuildHasher 别名，无重复定义）；
// papaya 亦透传重导出——下游（如 wkv）经此引用 papaya::HashMap/Operation 等类型面，
// 免去各自再添 papaya 直接依赖，维持本 crate 作为哈希/并发集合唯一出口的门面地位
pub use gxhash::{GxBuildHasher, HashMap, HashMapExt, HashSet, HashSetExt};
pub use papaya;
use wbase::map::{ConcurrentMap, new_concurrent_map};

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
  /// 创建默认种子 (0) 的流式校验器（对标 `new XxHash64()`）
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

  /// 创建指定种子的流式校验器（对标 `new XxHash64(seed)`）
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

  /// 增量写入分块切片（任意分片边界均与单次整体计算恒等；对标 `XxHash64.Append`）
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
  ///
  /// 对标 `XxHash64.GetCurrentHash`（非破坏读取）；C# `GetHashAndReset` 的
  /// 「读取并复位」语义在此拆分为 [`finish`](Self::finish) + [`reset`](Self::reset) 两步。
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

/// 计算字节序列的 64 位校验和（gxhash 硬件向量加速）
///
/// 对标 `RangeIndexChunkedSerializer.cs` / `RangeIndexChunkedDeserializer.cs` 中
/// `XxHash64` 校验和字段的单次整体计算（与逐块流式 [`StreamHasher`] 恒等，由测试锁定）。
///
/// 走 `new()` 的编译期预计算 `DEFAULT_LANES` 常量路径，种子 0 无任何运行时初始化开销
/// （与 `with_seed(0)` 数学恒等，由测试锁定）
#[inline]
pub fn compute_checksum(data: &[u8]) -> u64 {
  let mut hasher = StreamHasher::new();
  hasher.write(data);
  hasher.finish()
}

/// 带自定义种子的 64 位校验和（gxhash 硬件向量加速，对标 `new XxHash64(seed)` + 单次整体计算）
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
/// papaya 并发哈希字典（无锁读、分段写）统一搭载 [`GxBuildHasher`]，对应 C#
/// `ConcurrentDictionary`。定义的单一来源是 `wbase::map::ConcurrentMap`
/// （转写规范：并发字典在 wedb/wbase/map.rs 中定义，本 crate 作为哈希/并发集合
/// 门面别名再导出，两处类型恒同——均为 `papaya::HashMap<K, V, gxhash::GxBuildHasher>`）。
pub type GxPapayaMap<K, V> = ConcurrentMap<K, V>;

/// 创建搭载硬件向量加速 gxhash 构建器的无锁并发字典（委托 `wbase::map` 单一实现）
#[inline]
pub fn new_papaya_map<K, V>() -> GxPapayaMap<K, V> {
  new_concurrent_map()
}

/// gxhash 单次直算的抗碰撞安全上限（32 KiB）
///
/// gxhash 3.5.0 的大输入折叠（compress_8）以纯 XOR 累加 lane 状态，位置区分仅靠
/// 按字节 mod 256 回绕的组计数器（周期 256 组 = 32 KiB），由此存在三类可构造的
/// 全态碰撞族（u128 级，测试锁定）：
/// 1. 同相位组交换：相距 32 KiB 整数倍的两个 128 字节组互换内容哈希不变（≥ 33280B 可构造）；
/// 2. 32 KiB 整块置换：`P‖U‖V` 与 `P‖V‖U` 同哈希（U、V 为 32 KiB 整数倍块）；
/// 3. 总长按 2^32 回绕：`q×(j+m·2^25)` 与 `q×j` 同哈希（长度仅以 `len as u32` 混入）。
///
/// 低于本上限时折叠组数 < 256，每个相位至多出现一次，上述结构均不存在，单次直算安全。
const ONE_SHOT_MAX: usize = 1 << 15;

/// fast_hash 族大输入路径的保留流式域种子（校验和调用方的种子域请避开该常量）
const FAST_HASH_DOMAIN: u64 = mix13(0xFA57_5EED_0000_0001);

/// 大输入路径：路由到 [`StreamHasher`](StreamHasher) 折叠族保留域种子
///
/// 折叠链以 gxhash 种子非线性串联（位置敏感），末端混入全宽 u64 总长，
/// 与单次直算的 XOR 可交换/相位回绕结构彻底隔离
#[cold]
fn large_one_shot(bytes: &[u8], seed: u64) -> u64 {
  compute_checksum_with_seed(bytes, seed ^ FAST_HASH_DOMAIN)
}

/// 单次快速 64 位键哈希（gxhash 硬件向量加速指令，小输入单次耗时 ~0.31ns）
///
/// 通用字节序列键哈希，对标 `libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs:HashBytes`
/// （C# 侧经 `SpanByteComparer` / `GarnetKeyComparer` 承担索引键哈希）；迁移 Sketch 槽位、
/// 锁分段等场景统一由此承担。注意：Redis 逐位兼容的 HyperLogLog 必须使用
/// `wbase::hash::murmur_hash2_x64_a`。固定种子 0，跨进程跨版本确定性；
/// 索引 tag、事务冲突检测等派生数据可安全落盘。
///
/// 输入域路由：长度 < 32 KiB 走 gxhash 单次直算（折叠相位唯一，抗碰撞安全）；
/// ≥ 32 KiB 路由到流式折叠族保留域种子 FAST_HASH_DOMAIN——C# `HashBytes`
/// 以全宽 `ulong len` 混入长度且按位置串链，无结构性碰撞，路由补齐同等抗碰撞语义。
#[inline(always)]
pub fn fast_hash(bytes: &[u8]) -> u64 {
  if bytes.len() < ONE_SHOT_MAX {
    gxhash::gxhash64(bytes, 0)
  } else {
    large_one_shot(bytes, 0)
  }
}

/// 单次快速 64 位整数哈希（对标 `libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs:GetHashCode`，~0.31ns）
///
/// 固定种子 0，跨进程跨版本确定性；索引槽位、冲突检测等整数派生键可安全落盘。
/// i64 与 u64 按位同型，有符号调用方直接 `as u64` 位转换即可。
#[inline(always)]
pub fn fast_hash_u64(val: u64) -> u64 {
  fast_hash(&val.to_le_bytes())
}

/// 带自定义 64 位种子的单次快速键哈希（种子域分离）
///
/// 对标 `HashUtils.cs` 的 `MurmurHash2x64A` 带种子用法（迁移 Sketch 以命名空间
/// 派生种子做槽位域分离）。与 [`compute_checksum_with_seed`] 的流式种子族、
/// [`fast_hash`] 固定种子域互为独立算法域，派生值不得跨域混用（测试锁定）。
/// 输入域路由同 [`fast_hash`]：≥ 32 KiB 走流式折叠族保留域种子 `seed ^ FAST_HASH_DOMAIN`。
#[inline(always)]
pub fn fast_hash_with_seed(bytes: &[u8], seed: u64) -> u64 {
  if bytes.len() < ONE_SHOT_MAX {
    gxhash::gxhash64(bytes, seed as i64)
  } else {
    large_one_shot(bytes, seed)
  }
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

/// 大输入路径：双独立流式校验和拼为 128 位
///
/// 两条非线性折叠链（种子互补）+ 全宽 u64 长度混合，联合碰撞概率 ~2^-128（生日界），
/// 无 gxhash 单次直算的结构性碰撞族（见 ONE_SHOT_MAX 注释）
#[cold]
fn large_hash128(bytes: &[u8], seed: i64) -> u128 {
  let s = seed as u64;
  let lo = compute_checksum_with_seed(bytes, s);
  let hi = compute_checksum_with_seed(bytes, !s);
  (hi as u128) << 64 | lo as u128
}

/// 128 位强抗碰撞键 ID 计算（gxhash::gxhash128 硬件向量加速）
///
/// 对标 `RangeIndexManager.cs` 的 `KeyId`（XxHash128 → Guid）派生原语，
/// 1:1 实现位于 `wbftree::RangeIndexManager::key_id`；C# 单种子，
/// 此处双种子经 `combine_seed` 非线性合并做域分离。
///
/// 输入域路由：长度 < 32 KiB 走 gxhash128 单次直算（折叠相位唯一，抗碰撞安全）；
/// ≥ 32 KiB 路由到双独立流式校验和（C# XxHash128 按位置串链且全宽混入长度，
/// 无结构性碰撞，路由补齐同等抗碰撞语义，两路径均跨进程跨版本确定）。
///
/// 跨语言不要求同值：C# 侧 `new Guid(XxHash128.Hash(bytes)).ToString("N")` 输出
/// 32 位小写 hex，其中 Guid 对 16 字节哈希做了小端混合端重排——算法换成 gxhash 后
/// 位序/字节序无从对齐，也无需对齐。文件名前缀语义（`HashKeyToPrefix`）只要求
/// 定长、确定性、均匀：同 key 恒同 128 位 ID（显式种子路径不受 gxhash 随机化影响，
/// 主版本内跨平台跨进程稳定），rust 侧自身一致即成立；具体前缀编码
/// （26 字符 base32，转写规范「文件名用 base32」）由 wbftree 层承担。
#[inline(always)]
pub fn hash128(bytes: &[u8], seed_a: u64, seed_b: u64) -> u128 {
  if bytes.len() < ONE_SHOT_MAX {
    gxhash::gxhash128(bytes, combine_seed(seed_a, seed_b))
  } else {
    large_hash128(bytes, combine_seed(seed_a, seed_b))
  }
}
