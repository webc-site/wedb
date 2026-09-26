//! whasher 原语锁定集：逐条对齐 `src/lib.rs` 四处「由测试锁定」doc 声明
//!
//! - `compute_checksum` 与逐块 [`StreamHasher`] 恒等（分块一致性，对标 C#
//!   `RangeIndexChunkedSerializer.cs:148` / `RangeIndexChunkedDeserializer.cs:262`
//!   的 `XxHash64.Append` 跨端拼接校验契约：写入端按任意块 Append、读出端按文件
//!   分片 Append，两面必须收敛到同一校验值）
//! - 种子 0 的编译期 `DEFAULT_LANES` 常量路径与运行时派生路径恒等
//! - `ONE_SHOT_MAX` 之下的 gxhash 单次直算结构性碰撞族真实存在，
//!   ≥ 32 KiB 输入经 `fast_hash` / `fast_hash_with_seed` / `hash128` 路由后碰撞消除
//! - 键哈希种子域与流式校验和种子域互为独立算法域（对标 C#
//!   `libs/cluster/Server/Migration/Sketch.cs:40/57` 的
//!   `MurmurHash2x64A` 默认种子槽位域与 ns 派生种子域分离）
//!
//! 自研依据: gxhash 显式种子域（transpile 契约：确定性哈希仅限落盘派生值）

use aok::{OK, Result};
use ctor::ctor;
use gxhash::HashSet;
use whasher::{
  StreamHasher, compute_checksum, compute_checksum_with_seed, fast_hash, fast_hash_i64,
  fast_hash_with_seed, hash128,
};

#[ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// xorshift64 测试内确定性伪随机源（无外部依赖，保证用例可复现）
struct Xs(u64);

impl Xs {
  fn next(&mut self) -> u64 {
    let mut x = self.0;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    self.0 = x;
    x
  }

  fn fill(&mut self, buf: &mut [u8]) {
    for chunk in buf.chunks_mut(8) {
      let v = self.next().to_le_bytes();
      chunk.copy_from_slice(&v[..chunk.len()]);
    }
  }
}

/// 锁定 `fast_hash` 注释声明的输入域路由边界（`ONE_SHOT_MAX` 为私有常量，
/// 数值以 doc 承诺的 32 KiB 为准，由下方两条断言从两侧钉死）
const ONE_SHOT_MAX: usize = 1 << 15;

/// gxhash 3.5.0 `compress_8` 每个 8×16 B = 128 B 折叠组推进一次组计数器
/// （逐字节 mod 256 回绕），同相位组间距即 256 组 = 32 KiB
const GX_GROUP: usize = 128;
const GX_PHASE: usize = 256;

/// lib.rs:292 锁定：种子 0 的编译期 `DEFAULT_LANES` 常量路径与运行时派生路径恒等。
/// 回归防护面：`LANE_SALT` / `mix13` / `lanes_for` 任何一侧漂移（如初态链序错位、
/// 常量路径与派生路径改用不同混合层）都会在此暴露，两种构造入口的 digest 必须处处一致。
#[test]
fn default_lanes_seed0_identity() -> Result<()> {
  assert_eq!(
    StreamHasher::new().finish(),
    StreamHasher::with_seed(0).finish()
  );
  assert_eq!(
    StreamHasher::default().finish(),
    StreamHasher::new().finish()
  );

  let mut rng = Xs(0x5EED_D00D_0000_0001);
  for len in [1usize, 63, 64, 65, 129, 258, 1024, 4096] {
    let mut data = vec![0u8; len];
    rng.fill(&mut data);
    // 同一数据分别经两条入口、两种分块节奏折叠，digest 必须恒等
    for stride in [1usize, 7, 64, 4096] {
      let mut via_const = StreamHasher::new();
      let mut via_derived = StreamHasher::with_seed(0);
      for chunk in data.chunks(stride) {
        via_const.write(chunk);
        via_derived.write(chunk);
      }
      assert_eq!(
        via_const.finish(),
        via_derived.finish(),
        "len={len} stride={stride}: new() 常量初态与 with_seed(0) 派生初态分裂"
      );
    }
  }
  OK
}

/// lib.rs:289 锁定：单次整体 `compute_checksum` 与任意分块的流式 [`StreamHasher`]
/// 恒等（分块一致性）。C# 一手依据：`RangeIndexChunkedSerializer.cs:148` 与
/// `RangeIndexChunkedDeserializer.cs:262` 两侧各按任意块边界 `XxHash64.Append`，
/// 落盘校验值仍须逐位互认——流式哈希的跨端契约正是分块一致性本身。
#[test]
fn stream_chunk_invariance() -> Result<()> {
  let mut rng = Xs(0xC0FF_EE12_3456_789A);
  // 覆盖条带边界（64/65）、4 链轮转边界（3/4/5 条带）、缓冲补满分支、空输入
  // 与 ONE_SHOT_MAX 分界两侧长度（32767/32768/33280，路由后仍须分块恒等）
  for len in [
    0usize, 1, 3, 7, 63, 64, 65, 127, 128, 129, 191, 192, 193, 256, 257, 1024, 4096, 32767, 32768,
    32769, 33280,
  ] {
    let mut data = vec![0u8; len];
    rng.fill(&mut data);
    let one_shot = compute_checksum(&data);
    let seeded = compute_checksum_with_seed(&data, 0xDEAD_BEEF);

    // 分块节奏覆盖：单字节、非整条带、单条带（逐条带 fold+链轮转）、整 4 链轮
    // （256 B = LANES×STRIPE，专打 quads 并行展开与 2.1 对齐分支）与大块直入
    for stride in [1usize, 3, 7, 17, 63, 64, 65, 256, 257, 1024, 4096] {
      let mut h = StreamHasher::default();
      let mut hs = StreamHasher::with_seed(0xDEAD_BEEF);
      for chunk in data.chunks(stride) {
        h.write(chunk);
        hs.write(chunk);
      }
      assert_eq!(h.finish(), one_shot, "len={len} stride={stride} 分块不一致");
      assert_eq!(
        hs.finish(),
        seeded,
        "seeded len={len} stride={stride} 分块不一致"
      );
    }

    // 零长写入穿插与整块写入等价（write 的空切片早退分支）
    let mut h = StreamHasher::default();
    for chunk in data.chunks(7) {
      h.write(&[]);
      h.write(chunk);
    }
    assert_eq!(h.finish(), one_shot, "len={len} 空写穿插破坏一致性");

    // finish 非破坏且幂等，收尾后继续 write 无缝衔接（对标 GetCurrentHash 后续 Append）
    let mut h = StreamHasher::default();
    h.write(&data);
    assert_eq!(h.finish(), one_shot);
    assert_eq!(h.finish(), one_shot);
    h.write(b"tail");
    let mut full = data.clone();
    full.extend_from_slice(b"tail");
    assert_eq!(
      h.finish(),
      compute_checksum(&full),
      "len={len} 收尾后续写断裂"
    );

    // reset 回到构造种子初态（对标 XxHash64.Reset）
    let mut h = StreamHasher::with_seed(0xDEAD_BEEF);
    h.write(b"junk");
    h.reset();
    h.write(&data);
    assert_eq!(h.finish(), seeded, "len={len} reset 未回种子初态");
  }
  OK
}

/// lib.rs:312 锁定：gxhash 3.5.0 单次直算在 ≥ 32 KiB 域真实存在两条结构性碰撞族
/// （一手依据 `gxhash-3.5.0/src/gxhash/mod.rs:compress_all` + `platform/arm.rs:compress_8`：
/// 输入前 128 B 被 head 向量与串行链定相消耗，其后才是 XOR 累加池——池按 128 B 组
/// 折叠、组计数器逐字节 mod 256 回绕，故同相位（池内组号差 256 的整数倍）互换、
/// 或整 256 组（32 KiB）块置换均使 XOR 多重集与长度混合逐位不变），
/// 且 `ONE_SHOT_MAX` 路由把它们从 `fast_hash` / `fast_hash_with_seed` / `hash128`
/// 三个键哈希面全部消除；同时从两侧钉死路由边界
/// （< 32 KiB 与 gxhash 直算逐位等值、≥ 32 KiB 改走流式域）。
/// 若后端升级消除该族，本测试首条 assert_eq 即红线，提示 ONE_SHOT_MAX 阈值须重审。
#[test]
fn one_shot_max_structural_collisions() -> Result<()> {
  // 族 1（同相位组交换，260 个 128 字节组 = 33280 B）：前 128 B（绝对组 0）由
  // head/串行链消耗必须原位固定，互换池内绝对组 1 与组 257（池内下标 0 与 256，
  // 相位同余 mod 256），XOR 累加逐位复原
  let mut rng = Xs(0xD1C0_DE50_0000_0001);
  let mut a = vec![0u8; (GX_PHASE + 4) * GX_GROUP];
  rng.fill(&mut a);
  let mut b = a.clone();
  let (lo, hi) = (GX_GROUP, (GX_PHASE + 1) * GX_GROUP);
  b[lo..lo + GX_GROUP].copy_from_slice(&a[hi..hi + GX_GROUP]);
  b[hi..hi + GX_GROUP].copy_from_slice(&a[lo..lo + GX_GROUP]);
  assert_ne!(a, b, "构造退化：两输入必须不同数据");
  // 直算碰撞真实存在（u64 与 u128 两面）
  assert_eq!(
    gxhash::gxhash64(&a, 0),
    gxhash::gxhash64(&b, 0),
    "gxhash64 直算族 1 形态已变，ONE_SHOT_MAX 阈值分析须重审"
  );
  assert_eq!(
    gxhash::gxhash128(&a, 0),
    gxhash::gxhash128(&b, 0),
    "gxhash128 直算族 1 形态已变，ONE_SHOT_MAX 阈值分析须重审"
  );
  // 路由消除：≥ 32 KiB 走位置敏感流式域后三个门面必须区分
  assert_ne!(
    fast_hash(&a),
    fast_hash(&b),
    "fast_hash 大输入路由未消除组交换碰撞"
  );
  assert_ne!(
    fast_hash_with_seed(&a, 42),
    fast_hash_with_seed(&b, 42),
    "fast_hash_with_seed 大输入路由未消除组交换碰撞"
  );
  assert_ne!(
    hash128(&a, 7, 9),
    hash128(&b, 7, 9),
    "hash128 大输入路由未消除组交换碰撞"
  );

  // 族 2（32 KiB 整块置换）：P‖U‖V 与 P‖V‖U，P 取整 256 B 覆盖 head/串行链并让
  // U、V 各落为池内整 256 组（恰走完一个计数器周期），置换后每组计数器同余相位、
  // XOR 累加不变（总长 65792 B 为 16 B 整倍，extra 分支不扰动池对齐）
  let mut p = vec![0u8; 2 * GX_GROUP];
  let mut u = vec![0u8; ONE_SHOT_MAX];
  let mut v = vec![0u8; ONE_SHOT_MAX];
  rng.fill(&mut p);
  rng.fill(&mut u);
  rng.fill(&mut v);
  let mut uv = p.clone();
  uv.extend_from_slice(&u);
  uv.extend_from_slice(&v);
  let mut vu = p.clone();
  vu.extend_from_slice(&v);
  vu.extend_from_slice(&u);
  assert_eq!(
    gxhash::gxhash64(&uv, 0),
    gxhash::gxhash64(&vu, 0),
    "gxhash 直算族 2（整块置换）形态已变，ONE_SHOT_MAX 阈值分析须重审"
  );
  assert_ne!(
    fast_hash(&uv),
    fast_hash(&vu),
    "fast_hash 大输入路由未消除整块置换碰撞"
  );

  // 路由边界两侧钉死：32 KiB - 1 仍走单次直算（与 gxhash 逐位等值），
  // 恰 32 KiB 与超出一位均改走流式域（与直算必异值）
  let mut edge = vec![0u8; ONE_SHOT_MAX];
  Xs(0xB007_E150_0000_0002).fill(&mut edge);
  assert_eq!(
    fast_hash(&edge[..ONE_SHOT_MAX - 1]),
    gxhash::gxhash64(&edge[..ONE_SHOT_MAX - 1], 0),
    "32 KiB 之下输入应走单次直算域"
  );
  assert_ne!(
    fast_hash(&edge),
    gxhash::gxhash64(&edge, 0),
    "恰 32 KiB 输入应路由到流式域（边界为 < 而非 <=）"
  );
  let over = [&edge[..], &[0xA5u8]].concat();
  assert_ne!(
    fast_hash(&over),
    gxhash::gxhash64(&over, 0),
    "32 KiB + 1 输入应路由到流式域"
  );
  // 分界处一次成型与流式路径结果一致：≥ 32 KiB 域 `fast_hash` 与
  // `fast_hash_with_seed(_, 0)` 两门面必须塌缩到同一保留流式域（FAST_HASH_DOMAIN
  // 偏置一致）；任一侧偏置公式漂移即红
  assert_eq!(
    fast_hash(&edge),
    fast_hash_with_seed(&edge, 0),
    "恰 32 KiB 两门面域分裂"
  );
  assert_eq!(
    fast_hash(&over),
    fast_hash_with_seed(&over, 0),
    "32 KiB + 1 两门面域分裂"
  );
  assert_ne!(
    fast_hash(&edge),
    fast_hash(&over),
    "大输入域长度未参与混合（尾字节不可见）"
  );
  OK
}

/// lib.rs:375 锁定：`fast_hash` 固定种子键哈希域、`fast_hash_with_seed` 带种子域、
/// `compute_checksum_with_seed` 流式校验和域互为独立算法域，同 (输入, 种子) 不得跨域同值。
/// C# 一手依据：`Sketch.cs:40` 默认种子与 `Sketch.cs:57` ns 派生种子在
/// `MurmurHash2x64A` 上以种子做槽位域分离；rust 侧域分离由「算法域 + 种子」共同承载，
/// 大输入路径经 `FAST_HASH_DOMAIN` 偏置流式种子，若该偏置被移除，
/// 带种子键哈希将塌缩进流式校验和域——下方大输入断言即红线。
#[test]
fn seed_domain_separation() -> Result<()> {
  let mut rng = Xs(0x5EED_D000_0000_0003);
  let small: Vec<u8> = {
    let mut d = vec![0u8; 24];
    rng.fill(&mut d);
    d
  };
  let large: Vec<u8> = {
    let mut d = vec![0u8; 48 * 1024];
    rng.fill(&mut d);
    d
  };

  // 固定种子键哈希域 vs 流式校验和种子 0 域（小输入：直算 vs 折叠）
  assert_ne!(
    fast_hash(&small),
    compute_checksum(&small),
    "小输入跨域同值"
  );
  assert_ne!(
    fast_hash(&large),
    compute_checksum(&large),
    "大输入跨域同值"
  );

  // 带种子键哈希域 vs 带种子流式域：同种子同输入不得跨域同值
  for seed in [0u64, 1, 42, u64::MAX] {
    assert_eq!(
      fast_hash_with_seed(&small, seed),
      fast_hash_with_seed(&small, seed),
      "带种子键哈希丢失确定性"
    );
    assert_ne!(
      fast_hash_with_seed(&small, seed),
      compute_checksum_with_seed(&small, seed),
      "seed={seed} 小输入跨域同值"
    );
    assert_ne!(
      fast_hash_with_seed(&large, seed),
      compute_checksum_with_seed(&large, seed),
      "seed={seed} 大输入跨域同值（FAST_HASH_DOMAIN 偏置失效？）"
    );
  }

  // 种子域内区分度（对标 Sketch 以 ns 为种子做槽位域分离）：不同种子必出不同派生值
  let seeds: Vec<u64> = (0..64u64).collect();
  let uniq: HashSet<u64> = seeds
    .iter()
    .map(|s| fast_hash_with_seed(&small, *s))
    .collect();
  assert_eq!(uniq.len(), seeds.len(), "键哈希种子域区分度不足");

  // i64 位面单点原语与 u64 域严格同值（全仓禁止另写 as i64 的机制锁）
  assert_eq!(fast_hash_i64(&small), fast_hash(&small) as i64);
  OK
}
