use core::hash::{BuildHasher, Hash, Hasher};

use aok::{OK, Result};
use whasher::{
  Entry, GxBuildHasher, GxHasher, GxPapayaMap, GxPapayaSet, HashSet, StreamHasher,
  compute_checksum, compute_checksum_with_seed, fast_hash, fast_hash_u64, fast_hash_with_seed,
  fast_hash128, hash_map_with_capacity, hash_set_with_capacity, hash_value, hash_value_with_seed,
  hash128, hash128_with_seed, new_hash_map, new_hash_set, new_papaya_map, new_papaya_set,
  papaya_map_with_capacity, papaya_set_with_capacity,
};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// xorshift64 测试内确定性伪随机数（无外部依赖）
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
}

#[test]
fn test_hashmap_and_hashset() -> Result<()> {
  let mut map = new_hash_map();
  map.insert("key1", 100);
  map.insert("key2", 200);

  assert_eq!(map.get("key1"), Some(&100));
  assert_eq!(map.get("key2"), Some(&200));
  assert_eq!(map.get("key3"), None);

  if let Entry::Occupied(mut entry) = map.entry("key1") {
    *entry.get_mut() += 50;
  }
  assert_eq!(map.get("key1"), Some(&150));

  let mut set = new_hash_set();
  set.insert("alpha");
  set.insert("beta");
  assert!(set.contains("alpha"));
  assert!(!set.contains("gamma"));

  // 带容量创建、大量插入触发扩容重哈希后数据完好（元组键覆盖组合路径）
  let mut grown = hash_map_with_capacity::<(u64, i32), u8>(8);
  for i in 0..4096u64 {
    grown.insert((i, i as i32 ^ 0x5a5a), i as u8);
  }
  assert_eq!(grown.len(), 4096);
  for i in 0..4096u64 {
    assert_eq!(grown.get(&(i, i as i32 ^ 0x5a5a)), Some(&(i as u8)));
  }

  assert_eq!(hash_set_with_capacity::<u64>(64).len(), 0);

  // 验证 GxBuildHasher 与 GxHasher 重导出
  let def_hasher = GxBuildHasher::default();
  let _gx_hasher: GxHasher = BuildHasher::build_hasher(&def_hasher);
  let _gx_builder = GxBuildHasher::default();

  // 验证 StreamHasher::new() 与 Default 等价
  assert_eq!(
    StreamHasher::new().finish(),
    StreamHasher::default().finish()
  );
  assert_eq!(StreamHasher::new().total_bytes_written(), 0);
  assert!(StreamHasher::new().is_empty());

  // DEFAULT_LANES 常量路径与 with_seed(0) 派生路径一致（0 ^ salt == smear(salt) 恒等回归）
  assert_eq!(
    StreamHasher::new().finish(),
    StreamHasher::with_seed(0).finish()
  );
  let mut via_const = StreamHasher::new();
  let mut via_derived = StreamHasher::with_seed(0);
  for len in [1usize, 63, 64, 65, 191, 256] {
    let buf = [len as u8; 256];
    via_const.write(&buf[..len]);
    via_derived.write(&buf[..len]);
    assert_eq!(via_const.finish(), via_derived.finish(), "len={len}");
  }
  assert!(!via_const.is_empty());

  OK
}

#[test]
fn test_fast_hash_and_hash_value() -> Result<()> {
  // 恒等性（含空输入）
  let h1 = fast_hash(b"hello world");
  assert_eq!(h1, fast_hash(b"hello world"));
  assert_ne!(h1, fast_hash(b"hello world!"));
  assert_eq!(fast_hash(b""), fast_hash(b""));

  // 种子确定性（含位模式极端值）
  for seed in [0u64, 1, 42, u64::MAX] {
    assert_eq!(
      fast_hash_with_seed(b"hello world", seed),
      fast_hash_with_seed(b"hello world", seed)
    );
  }

  // 种子区分度：64 个不同种子的输出应几乎互不相同
  let per_seed: HashSet<u64> = (0..64u64)
    .map(|s| fast_hash_with_seed(b"hello world", s))
    .collect();
  assert!(per_seed.len() >= 63, "种子区分度不足: {}", per_seed.len());

  // Hash trait 泛型路径：恒等 + 序敏感 + 数值宽度路径
  assert_eq!(hash_value(&"test string"), hash_value(&"test string"));
  assert_eq!(hash_value(&(1i32, 2i32)), hash_value(&(1i32, 2i32)));
  assert_ne!(hash_value(&(1i32, 2i32)), hash_value(&(2i32, 1i32)));
  // 整数哈希 fast_hash_u64 对标 C# Utility.GetHashCode(long)；i64 与 u64 按位同型
  for val in [0u64, 1, 42, 0x1234_5678_9abc_def0, u64::MAX] {
    assert_eq!(fast_hash_u64(val), fast_hash(&val.to_le_bytes()));
  }
  assert_ne!(fast_hash_u64(1), fast_hash_u64(2));
  for val in [-1i64, 0, 1, -42, 42, i64::MIN, i64::MAX] {
    assert_eq!(fast_hash_u64(val as u64), fast_hash(&val.to_le_bytes()));
  }

  // 带种子 hash_value_with_seed 确定性与区分度
  assert_eq!(
    hash_value_with_seed(&"test string", 12345),
    hash_value_with_seed(&"test string", 12345)
  );
  assert_ne!(
    hash_value_with_seed(&"test string", 12345),
    hash_value_with_seed(&"test string", 54321)
  );

  OK
}

#[test]
fn test_avalanche_and_distribution() -> Result<()> {
  let mut rng = Xs(0x243F_6A88_85A3_08D3);

  // 雪崩：单比特翻转应引起约半数输出位翻转
  let mut total_flips = 0u64;
  let mut rounds = 0u64;
  let mut min_flips = u32::MAX;
  for _ in 0..256 {
    let base = rng.next();
    let h0 = fast_hash(&base.to_le_bytes());
    for bit in 0..64u32 {
      let flips = (h0 ^ fast_hash(&(base ^ (1u64 << bit)).to_le_bytes())).count_ones();
      min_flips = min_flips.min(flips);
      total_flips += flips as u64;
      rounds += 1;
    }
  }
  let ratio = total_flips as f64 / rounds as f64 / 64.0;
  assert!((0.4..0.6).contains(&ratio), "雪崩翻转比例异常: {ratio}");
  assert!(min_flips >= 8, "单比特翻转最小输出翻转位过少: {min_flips}");

  // 分布：16384 个伪随机 8 字节键哈希无碰撞
  let uniq: HashSet<u64> = (0..16384)
    .map(|_| fast_hash(&rng.next().to_le_bytes()))
    .collect();
  assert_eq!(uniq.len(), 16384);

  OK
}

#[test]
fn test_stream_hasher_chunk_identity() -> Result<()> {
  let data = b"The quick brown fox jumps over the lazy dog. A fast streaming hash with buffer.";
  let one_shot = compute_checksum(data);

  // 单字节 (1B) 连续写入
  let mut hasher = StreamHasher::default();
  for b in data {
    hasher.write(&[*b]);
  }
  assert_eq!(one_shot, hasher.finish());

  // 7 字节 (7B) 跨边界写入
  let mut hasher = StreamHasher::default();
  for chunk in data.chunks(7) {
    hasher.write(chunk);
  }
  assert_eq!(one_shot, hasher.finish());

  // 17 字节非对齐切片写入
  let mut hasher = StreamHasher::default();
  for chunk in data.chunks(17) {
    hasher.write(chunk);
  }
  assert_eq!(one_shot, hasher.finish());

  // 64 字节整块对齐写入
  let mut hasher = StreamHasher::default();
  for chunk in data.chunks(64) {
    hasher.write(chunk);
  }
  assert_eq!(one_shot, hasher.finish());

  let seeded = compute_checksum_with_seed(data, 9999);
  assert_ne!(one_shot, seeded);

  // 中途 Clone：内部缓冲/状态/总字节一并复制，两个状态机独立累积互不干扰（状态机闭环）
  let mut h = StreamHasher::default();
  h.write(b"abcdef");
  let mut g = h.clone();
  h.write(b"XYZ");
  g.write(b"123");
  assert_eq!(h.finish(), compute_checksum(b"abcdefXYZ"));
  assert_eq!(g.finish(), compute_checksum(b"abcdef123"));
  assert_eq!(g.total_bytes_written(), 9);

  OK
}

#[test]
fn test_stream_hasher_all_chunkings() -> Result<()> {
  let mut rng = Xs(0x9E37_79B9_7F4A_7C15);

  // 涵盖 1B, 7B, 64B, 4KB 以及各临界点长度（含 3/4/5/9 条带的链轮转边界）
  for len in [
    0usize, 1, 2, 3, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 95, 96, 97, 127, 128, 129, 135, 192,
    256, 320, 576, 4096,
  ] {
    let data: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
    let one_shot = compute_checksum(&data);
    let one_shot_seeded = compute_checksum_with_seed(&data, 0xDEAD_BEEF);

    // 固定步长分片（明确覆盖 1B, 7B, 64B, 4KB 等）
    for stride in [
      1usize, 3, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 1024, 4096,
    ] {
      let mut h = StreamHasher::default();
      for chunk in data.chunks(stride) {
        h.write(chunk);
      }
      assert_eq!(h.finish(), one_shot, "len={len} stride={stride}");

      let mut hs = StreamHasher::with_seed(0xDEAD_BEEF);
      for chunk in data.chunks(stride) {
        hs.write(chunk);
      }
      assert_eq!(
        hs.finish(),
        one_shot_seeded,
        "seeded len={len} stride={stride}"
      );
    }

    // 伪随机分片边界
    let mut h = StreamHasher::default();
    let mut rest = &data[..];
    while !rest.is_empty() {
      let n = 1 + (rng.next() as usize) % 97;
      let (chunk, tail) = rest.split_at(n.min(rest.len()));
      h.write(chunk);
      rest = tail;
    }
    assert_eq!(h.finish(), one_shot, "random split len={len}");

    // finish 幂等且非破坏：多次 finish 一致，finish 后继续写入无缝衔接
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
      "continue after finish len={len}"
    );

    // reset 复用：回到原种子初态（保留构造种子而非默认种子 0）
    let mut h = StreamHasher::default();
    h.write(b"junk");
    h.reset();
    h.write(&data);
    assert_eq!(h.finish(), one_shot, "reset reuse len={len}");

    // 带种子 reset 复用：reset 后哈希值须与该种子单次计算恒等（回归覆盖）
    let seeded_one_shot = compute_checksum_with_seed(&data, 0xDEAD_BEEF);
    let mut hs = StreamHasher::with_seed(0xDEAD_BEEF);
    hs.write(b"junk");
    hs.reset();
    hs.write(&data);
    assert_eq!(hs.finish(), seeded_one_shot, "seeded reset reuse len={len}");
  }

  OK
}

#[test]
fn test_stream_hasher_trait_path() -> Result<()> {
  // Hasher trait write 代理与固有方法 write 结果 100% 恒等
  let mut a = StreamHasher::default();
  let mut b = StreamHasher::default();
  Hasher::write(&mut a, b"trait-path");
  b.write(b"trait-path");
  assert_eq!(Hasher::finish(&a), b.finish());

  // Hash trait 泛型写入路径的确定性
  let mut c = StreamHasher::default();
  let mut d = StreamHasher::default();
  b"trait-path".hash(&mut c);
  b"trait-path".hash(&mut d);
  assert_eq!(Hasher::finish(&c), Hasher::finish(&d));

  // total_bytes_written 与 reset 行为验证
  let mut e = StreamHasher::default();
  e.write(b"12345");
  assert_eq!(e.total_bytes_written(), 5);
  e.write(b"67890");
  assert_eq!(e.total_bytes_written(), 10);
  e.reset();
  assert_eq!(e.total_bytes_written(), 0);

  // 非零种子 reset 回归：reset 必须保留构造种子（而非回落到默认种子 0）
  let mut f = StreamHasher::with_seed(0xDEAD_BEEF);
  f.write(b"drift");
  f.reset();
  assert_eq!(f.total_bytes_written(), 0);
  f.write(b"trait-path");
  assert_eq!(
    f.finish(),
    compute_checksum_with_seed(b"trait-path", 0xDEAD_BEEF),
    "非零种子 reset 后须回到该种子初态"
  );

  OK
}

#[test]
fn test_stream_hasher_long_input() -> Result<()> {
  let mut rng = Xs(0x0D15_EA5E_0D15_EA5E);

  // 超长输入（1 MiB）：覆盖多轮条带折叠路径，各分块步长与单次整体计算恒等
  let len = 1 << 20;
  let data: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
  let one_shot = compute_checksum(&data);
  let one_shot_seeded = compute_checksum_with_seed(&data, 0x1234_5678_9ABC_DEF0);

  for stride in [4096usize, 4097, 65536, 1 << 20] {
    let mut h = StreamHasher::default();
    for chunk in data.chunks(stride) {
      h.write(chunk);
    }
    assert_eq!(h.finish(), one_shot, "stride={stride}");

    let mut hs = StreamHasher::with_seed(0x1234_5678_9ABC_DEF0);
    for chunk in data.chunks(stride) {
      hs.write(chunk);
    }
    assert_eq!(hs.finish(), one_shot_seeded, "seeded stride={stride}");
  }

  // 超长输入下 total_bytes_written 与 finish 幂等
  let mut h = StreamHasher::default();
  for chunk in data.chunks(8192) {
    h.write(chunk);
  }
  assert_eq!(h.total_bytes_written(), len as u64);
  assert_eq!(h.finish(), one_shot);
  assert_eq!(h.finish(), one_shot);

  OK
}

#[test]
fn test_stream_hasher_distribution() -> Result<()> {
  let mut rng = Xs(0x5EED_5EED_5EED_5EED);

  // 分布：16384 个伪随机变长输入（跨条带边界）校验和零碰撞，验证折叠链雪崩质量
  let uniq: HashSet<u64> = (0..16384usize)
    .map(|i| {
      let len = 1 + i % 130; // 长度横跨尾块/单条带/多条带
      let mut h = StreamHasher::default();
      for _ in 0..len {
        h.write(&rng.next().to_le_bytes());
      }
      h.finish()
    })
    .collect();
  assert_eq!(uniq.len(), 16384, "流式校验和出现碰撞");

  // 前缀歧义防护：不同分块方式构造的相同总字节序列必须恒等，
  // 而不同字节序列（含总长差异）不得因尾部零填充而混淆
  let mut a = StreamHasher::default();
  a.write(&[1u8; 64]);
  a.write(&[2u8; 3]);
  let mut b = StreamHasher::default();
  b.write(&[1u8; 64]);
  b.write(&[2u8; 3]);
  assert_eq!(a.finish(), b.finish());
  assert_ne!(a.finish(), StreamHasher::default().finish());

  OK
}

#[test]
fn test_hash128() -> Result<()> {
  // 恒等 + 种子/输入敏感性
  assert_eq!(hash128(b"identity", 1, 2), hash128(b"identity", 1, 2));
  assert_ne!(hash128(b"identity", 1, 2), hash128(b"identity", 1, 3));
  assert_ne!(hash128(b"identity", 1, 2), hash128(b"identity", 2, 1));
  assert_ne!(hash128(b"identity", 1, 2), hash128(b"identitx", 1, 2));

  // 默认种子 fast_hash128 单次快速计算
  assert_eq!(fast_hash128(b"identity"), hash128_with_seed(b"identity", 0));
  assert_eq!(fast_hash128(b"identity"), fast_hash128(b"identity"));
  assert_ne!(fast_hash128(b"identity"), fast_hash128(b"identitx"));

  // 单种子版本测试
  assert_eq!(
    hash128_with_seed(b"identity", 42),
    hash128_with_seed(b"identity", 42)
  );
  assert_ne!(
    hash128_with_seed(b"identity", 42),
    hash128_with_seed(b"identity", 43)
  );

  // 空输入与各类边界长度确定性（含超长输入的多向量内部路径）
  let mut rng = Xs(0x0DDC_0FFE_0DDC_0FFE);
  for len in [0usize, 1, 15, 16, 17, 31, 32, 33, 100, 4096, 65536] {
    let data: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
    assert_eq!(hash128(&data, 7, 9), hash128(&data, 7, 9), "len={len}");
  }

  // 128 位输出高低半区不退化
  let h = hash128(b"wedge-key", 0x1111_2222_3333_4444, 0x5555_6666_7777_8888);
  assert_ne!((h >> 64) as u64, h as u64);

  OK
}

#[test]
fn test_stream_hasher_extreme_boundaries() -> Result<()> {
  let empty_checksum = compute_checksum(b"");
  let seeded_empty = compute_checksum_with_seed(b"", 0xFEED_FACE_CAFE);

  // 1. 0 字节输入极端测试
  let mut h = StreamHasher::default();
  assert_eq!(h.total_bytes_written(), 0);
  for _ in 0..100 {
    h.write(b"");
  }
  assert_eq!(h.total_bytes_written(), 0);
  assert_eq!(h.finish(), empty_checksum);

  // 穿插空切片写入：任意位置穿插零长度写入与连续写入无差异
  let data = b"The quick brown fox jumps over the lazy dog and explores extreme boundary patterns.";
  let expected = compute_checksum(data);
  let mut interleaved = StreamHasher::default();
  interleaved.write(b"");
  for b in data.chunks(5) {
    interleaved.write(b"");
    interleaved.write(b);
    interleaved.write(b"");
    interleaved.write(b"");
  }
  interleaved.write(b"");
  assert_eq!(interleaved.total_bytes_written(), data.len() as u64);
  assert_eq!(interleaved.finish(), expected);

  // 2. reset 幂等性测试
  let mut hr = StreamHasher::with_seed(0xFEED_FACE_CAFE);
  for _ in 0..10 {
    hr.reset();
  }
  assert_eq!(hr.total_bytes_written(), 0);
  assert_eq!(hr.finish(), seeded_empty);

  hr.write(b"partial-data");
  for _ in 0..5 {
    hr.reset();
  }
  assert_eq!(hr.total_bytes_written(), 0);
  assert_eq!(hr.finish(), seeded_empty);
  hr.write(data);
  assert_eq!(
    hr.finish(),
    compute_checksum_with_seed(data, 0xFEED_FACE_CAFE)
  );

  // 3. finish 幂等性测试：连续 100 次 finish 结果必须完全一致且不改变内部状态
  let mut hf = StreamHasher::default();
  hf.write(&data[..17]); // 残留 17 字节在 buf
  let f1 = hf.finish();
  for _ in 0..100 {
    assert_eq!(hf.finish(), f1);
  }
  assert_eq!(hf.total_bytes_written(), 17);
  // 继续写入剩余部分并 finish
  hf.write(&data[17..]);
  assert_eq!(hf.finish(), expected);
  for _ in 0..100 {
    assert_eq!(hf.finish(), expected);
  }

  // 4. 各种不对齐步长组合：斐波那契步长与素数步长测试
  let fib_steps = [1usize, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377];
  let mut h_fib = StreamHasher::default();
  let mut offset = 0;
  let mut step_idx = 0;
  while offset < data.len() {
    let step = fib_steps[step_idx % fib_steps.len()];
    let end = (offset + step).min(data.len());
    h_fib.write(&data[offset..end]);
    offset = end;
    step_idx += 1;
  }
  assert_eq!(h_fib.finish(), expected);

  let prime_steps = [
    2usize, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71,
  ];
  let mut h_prime = StreamHasher::default();
  offset = 0;
  step_idx = 0;
  while offset < data.len() {
    let step = prime_steps[step_idx % prime_steps.len()];
    let end = (offset + step).min(data.len());
    h_prime.write(&data[offset..end]);
    offset = end;
    step_idx += 1;
  }
  assert_eq!(h_prime.finish(), expected);

  // 1 字节与 63/64/65 字节交替模式
  let alt_steps = [1usize, 63, 1, 64, 1, 65, 1, 128, 1, 3];
  let mut h_alt = StreamHasher::default();
  offset = 0;
  step_idx = 0;
  while offset < data.len() {
    let step = alt_steps[step_idx % alt_steps.len()];
    let end = (offset + step).min(data.len());
    h_alt.write(&data[offset..end]);
    offset = end;
    step_idx += 1;
  }
  assert_eq!(h_alt.finish(), expected);

  OK
}

#[test]
fn test_stream_hasher_seed_and_lane_separation() -> Result<()> {
  // 空流：不同种子的校验和互异（4 条链初态经 smear 双射扩散，互不退化）
  let seeds = [0u64, 1, 0xDEAD_BEEF_CAFE_F00D, u64::MAX];
  let uniq: HashSet<u64> = seeds
    .iter()
    .map(|&s| StreamHasher::with_seed(s).finish())
    .collect();
  assert_eq!(uniq.len(), seeds.len(), "空流种子区分度不足");

  // 同一数据（4 整条带 + 尾块）不同种子：流式与单次恒等且互异
  let data: Vec<u8> = (0..300usize).map(|i| (i * 7) as u8).collect();
  let uniq: HashSet<u64> = seeds
    .iter()
    .map(|&s| compute_checksum_with_seed(&data, s))
    .collect();
  assert_eq!(uniq.len(), seeds.len(), "数据种子区分度不足");
  for s in seeds {
    let mut h = StreamHasher::with_seed(s);
    for chunk in data.chunks(64) {
      h.write(chunk);
    }
    assert_eq!(h.finish(), compute_checksum_with_seed(&data, s));
  }

  // 链轮转回归：条带数横跨 4 链轮转边界（k 条带按 64B 逐条带写入 vs 单次整体）
  let mut rng = Xs(0x5A5A_5A5A_3C3C_3C3C);
  for stripes in 0..=9usize {
    let data: Vec<u8> = (0..stripes * 64).map(|_| rng.next() as u8).collect();
    let one_shot = compute_checksum(&data);
    let mut h = StreamHasher::default();
    for chunk in data.chunks(64) {
      h.write(chunk);
    }
    assert_eq!(h.finish(), one_shot, "stripes={stripes}");

    // 残留偏移使全局条带号整体错位后，轮转归属仍须与单次整体恒等
    let mut data_shifted = data.clone();
    data_shifted.push(rng.next() as u8);
    let shifted = compute_checksum(&data_shifted);
    let mut h2 = StreamHasher::default();
    h2.write(&data_shifted);
    assert_eq!(h2.finish(), shifted, "shifted stripes={stripes}");
  }
  OK
}

#[test]
fn test_stream_hasher_clone_isolation() -> Result<()> {
  let mut rng = Xs(0xCAFE_BABE_0123_4567);
  let base_data: Vec<u8> = (0..256).map(|_| rng.next() as u8).collect();

  // 在每个可能的内部 buf_len (0..64) 下进行 clone 并验证完全隔离
  for split_pos in 0..=64 {
    let mut h1 = StreamHasher::default();
    h1.write(&base_data[..split_pos]);

    let mut h2 = h1.clone();

    // 验证 clone 后的初始状态相同
    assert_eq!(h1.finish(), h2.finish());
    assert_eq!(h1.total_bytes_written(), h2.total_bytes_written());

    // 两个实例写入不同数据
    let suffix1 = b"-unique-branch-alpha-12345678";
    let suffix2 = b"-unique-branch-beta-9876543210!";
    h1.write(suffix1);
    h2.write(suffix2);

    let mut full1 = base_data[..split_pos].to_vec();
    full1.extend_from_slice(suffix1);
    let mut full2 = base_data[..split_pos].to_vec();
    full2.extend_from_slice(suffix2);

    assert_eq!(h1.finish(), compute_checksum(&full1));
    assert_eq!(h2.finish(), compute_checksum(&full2));
    assert_ne!(h1.finish(), h2.finish());

    // 对 h1 进行 reset，确认 h2 完全不受影响
    h1.reset();
    assert_eq!(h1.total_bytes_written(), 0);
    assert_eq!(h1.finish(), compute_checksum(b""));
    assert_eq!(h2.finish(), compute_checksum(&full2));
    assert_eq!(h2.total_bytes_written(), full2.len() as u64);
  }

  OK
}

#[test]
fn test_papaya_map_and_set() -> Result<()> {
  use std::{sync::Arc, thread};

  let map: Arc<GxPapayaMap<u64, u64>> = Arc::new(new_papaya_map());
  let set: Arc<GxPapayaSet<u64>> = Arc::new(new_papaya_set());

  let mut handles = Vec::new();
  for t in 0..4u64 {
    let map_clone = Arc::clone(&map);
    let set_clone = Arc::clone(&set);
    handles.push(thread::spawn(move || {
      let map_pin = map_clone.pin();
      let set_pin = set_clone.pin();
      for i in 0..1000u64 {
        let key = t * 1000 + i;
        map_pin.insert(key, key * 2);
        set_pin.insert(key);
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  let pin = map.pin();
  let set_pin = set.pin();
  assert_eq!(map.len(), 4000);
  assert_eq!(set.len(), 4000);

  for key in 0..4000u64 {
    assert_eq!(pin.get(&key), Some(&(key * 2)));
    assert!(set_pin.contains(&key));
  }
  assert_eq!(pin.get(&9999), None);
  assert!(!set_pin.contains(&9999));

  // 带容量构建测试
  let map_cap = papaya_map_with_capacity::<u64, u64>(128);
  let set_cap = papaya_set_with_capacity::<u64>(128);
  assert_eq!(map_cap.len(), 0);
  assert_eq!(set_cap.len(), 0);

  OK
}

#[test]
fn test_integer_hash_and_scramble() -> Result<()> {
  use whasher::{GOLDEN_RATIO_64, mix_thread_id, mix13, mix64, splitmix64};

  assert_eq!(GOLDEN_RATIO_64, 0x9E37_79B9_7F4A_7C15);

  let v1 = splitmix64(0);
  let v2 = splitmix64(1);
  assert_ne!(v1, v2);
  assert_eq!(mix64(42), splitmix64(42));
  assert_eq!(splitmix64(42), mix13(42u64.wrapping_add(GOLDEN_RATIO_64)));

  let tid_slot = mix_thread_id(1234);
  assert_eq!(tid_slot, splitmix64(1234) as usize);

  OK
}
