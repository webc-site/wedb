//! ZRANDMEMBER/HRANDFIELD 采样单源与零拷贝回归
//!
//! 对位 C# 的随机下标单源约束（garnet libs/common 下 RandomUtils 的
//! PickKRandomIndexes，调用方 HashObjectImpl 与 SortedSetObjectImpl 同口）。
//! rust 侧 zset 曾在函数体内自持第二套「部分洗牌取前 k」，本组用例把迁移前后的
//! 下标序列钉成断言（参照物即被删除的内联实现），并覆盖 element_at 改借用视图后
//! 的成员/分值配对与字段级过期下的「先算下标、再只读写出」段。
//!
//! RESP 层的种子是每命令现取（fastrand），故序列断言只能在对象层 operate 面上做。

use std::str::from_utf8;

use fastrand::Rng;
use wbase::{
  map::{GxBuildHasher, HashSet},
  time::now_ticks,
};
use wcol::{HashObject, HashOperation, ObjectOutput, SortedSetObject, SortedSetOperation};
use wresp::options::ExpireOption;

/// 远未来/过去刻度：造存活与已过期字段（免真实等待）
const LIVE_SPAN: i64 = 1_000_000_000;
const EXPIRED_SPAN: i64 = 1_000_000;

/// 迁移前 zset 函数体内自持的第二套采样（不放回部分洗牌 / 放回重复抽取），
/// 现由 hash_object::pick_k_random_indexes 单源承接；此处留作序列参照物
fn legacy_inline_sample(n: usize, k: usize, seed: i32, distinct: bool) -> Vec<usize> {
  let mut rng = Rng::with_seed(u64::from(seed as u32));
  let mut indexes = Vec::with_capacity(k);
  if distinct {
    let mut perm: Vec<usize> = (0..n).collect();
    for i in 0..k.min(n) {
      let j = rng.usize(i..perm.len());
      perm.swap(i, j);
      indexes.push(perm[i]);
    }
  } else if n > 0 {
    for _ in 0..k {
      indexes.push(rng.usize(..n));
    }
  }
  indexes
}

/// 共用单源的小比例臂（C# PickKRandomIndexesIteratively：不放回用拒绝采样、
/// 放回逐次抽取）：与内联洗牌的抽取次序不同，故用它可反证 zset 已改走单源
fn shared_iterative_sample(n: usize, k: usize, seed: i32, distinct: bool) -> Vec<usize> {
  let mut rng = Rng::with_seed(u64::from(seed as u32));
  let mut indexes = Vec::with_capacity(k);
  if !distinct {
    indexes.extend((0..k).map(|_| rng.usize(..n)));
    return indexes;
  }
  let mut picked = HashSet::with_capacity_and_hasher(k, GxBuildHasher::default());
  while indexes.len() < k {
    let index = rng.usize(..n);
    if picked.insert(index) {
      indexes.push(index);
    }
  }
  indexes
}

/// arg1 打包 `(count << 1 | includedCount) << 1 | withScores`（与 resp 层
/// parse_random_member_args 同式）
fn pack_arg1(count: i32, included_count: bool, with_scores: bool) -> i32 {
  ((count << 1) | i32::from(included_count)) << 1 | i32::from(with_scores)
}

/// 取一条 RESP bulk 头（$len\r\n）+ 正文 + \r\n，返回 (正文, 余下切片)
fn take_bulk(frame: &[u8]) -> (&[u8], &[u8]) {
  let len_end = frame.iter().position(|&b| b == b'\n').expect("缺 bulk 头");
  let len: usize = from_utf8(&frame[1..len_end - 1]).unwrap().parse().unwrap();
  let body_start = len_end + 1;
  (
    &frame[body_start..body_start + len],
    &frame[body_start + len + 2..],
  )
}

/// 解析 RESP2 bulk 数组应答 → 成员序列
fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
  let header_end = frame.iter().position(|&b| b == b'\n').expect("缺数组头");
  let length: usize = from_utf8(&frame[1..header_end - 1])
    .unwrap()
    .parse()
    .unwrap();
  let mut rest = &frame[header_end + 1..];
  let mut items = Vec::with_capacity(length);
  for _ in 0..length {
    let (item, tail) = take_bulk(rest);
    items.push(item.to_vec());
    rest = tail;
  }
  items
}

/// 把应答成员还原为采样下标序列（同一实例迭代序内成员唯一，还原无歧义）
fn indexes_of(reply: &[Vec<u8>], order: &[Vec<u8>]) -> Vec<usize> {
  reply
    .iter()
    .map(|member| {
      order
        .iter()
        .position(|slot| slot == member)
        .expect("应答成员不在集合内")
    })
    .collect()
}

/// ZRANDMEMBER 应答（resp2、不带分值）→ 成员序列
fn zrandmember(zset: &mut SortedSetObject, count: i32, seed: i32) -> Vec<Vec<u8>> {
  let mut sink = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut sink);
    zset.operate(
      SortedSetOperation::Zrandmember as u8,
      &[],
      pack_arg1(count, true, false),
      seed,
      &mut output,
      2,
    );
  }
  parse_bulk_array(&sink)
}

/// HRANDFIELD 应答（resp2、不带值）→ 字段序列
fn hrandfield(hash: &mut HashObject, count: i32, seed: i32) -> Vec<Vec<u8>> {
  let mut sink = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut sink);
    hash.operate(
      HashOperation::Hrandfield as u8,
      &[],
      pack_arg1(count, true, false),
      seed,
      &mut output,
      2,
    );
  }
  parse_bulk_array(&sink)
}

fn zset_with(members: &[&[u8]]) -> SortedSetObject {
  let mut zset = SortedSetObject::new();
  for (i, m) in members.iter().enumerate() {
    zset.add(m, (i + 1) as f64);
  }
  zset
}

fn hash_with(fields: &[&[u8]]) -> HashObject {
  let mut hash = HashObject::new();
  for f in fields {
    hash.hash.insert(f.to_vec(), b"v".to_vec());
  }
  hash
}

fn members_10() -> Vec<Vec<u8>> {
  (0..10).map(|i| format!("m{i}").into_bytes()).collect()
}

fn slices_of(members: &[Vec<u8>]) -> Vec<&[u8]> {
  members.iter().map(Vec::as_slice).collect()
}

/// count > 0 不放回：共用单源的洗牌臂与迁移前内联洗牌的次序同为
/// 「i 递增、在 [i, len) 取 j、swap」，同 seed 同 count 下标序列逐一相同
#[test]
fn zrandmember_distinct_sequence_matches_legacy_shuffle() {
  let members = members_10();
  let mut zset = zset_with(&slices_of(&members));
  let order: Vec<Vec<u8>> = zset.sorted_set_dict.keys().cloned().collect();

  for seed in [1_i32, 7, 42_000, -3] {
    // k/n = 0.5 ≥ K_OVER_N_THRESHOLD → 新旧同为洗牌臂
    let reply = zrandmember(&mut zset, 5, seed);
    assert_eq!(
      indexes_of(&reply, &order),
      legacy_inline_sample(10, 5, seed, true),
      "seed={seed} 不放回采样序列漂移"
    );

    let mut distinct = reply.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), 5, "seed={seed} 正 count 必须互异");
  }
}

/// 负 count 可重复：与迁移前内联放回抽取同序列
#[test]
fn zrandmember_repeat_sequence_matches_legacy_draw() {
  let members = members_10();
  let mut zset = zset_with(&slices_of(&members));
  let order: Vec<Vec<u8>> = zset.sorted_set_dict.keys().cloned().collect();

  for seed in [5_i32, 99, -12345] {
    let reply = zrandmember(&mut zset, -6, seed);
    assert_eq!(reply.len(), 6);
    assert_eq!(
      indexes_of(&reply, &order),
      legacy_inline_sample(10, 6, seed, false),
      "seed={seed} 放回采样序列漂移"
    );
  }
}

/// 一支采样器：同基数、同 count、同 seed 下 HRANDFIELD 与 ZRANDMEMBER 还原出的
/// 下标序列一致（zset 不再有第二套洗牌的行为证据）
#[test]
fn hrandfield_and_zrandmember_share_one_sampler() {
  let members = members_10();
  let slices = slices_of(&members);
  let mut zset = zset_with(&slices);
  let mut hash = hash_with(&slices);
  let zset_order: Vec<Vec<u8>> = zset.sorted_set_dict.keys().cloned().collect();
  let hash_order: Vec<Vec<u8>> = hash.hash.keys().cloned().collect();

  for seed in [3_i32, -77] {
    // 洗牌臂（k/n = 0.5）
    assert_eq!(
      indexes_of(&zrandmember(&mut zset, 5, seed), &zset_order),
      indexes_of(&hrandfield(&mut hash, 5, seed), &hash_order),
      "seed={seed} 不放回臂两命令下标不一致"
    );
    // 放回臂（负 count）
    assert_eq!(
      indexes_of(&zrandmember(&mut zset, -5, seed), &zset_order),
      indexes_of(&hrandfield(&mut hash, -5, seed), &hash_order),
      "seed={seed} 放回臂两命令下标不一致"
    );
  }
}

/// 千级成员抽 3 个：改走共用单源的 O(k) 迭代/拒绝采样臂（下标序列与小比例臂
/// 参照物逐元素相同，与迁移前的内联洗牌不同），应答为 3 个互异成员
#[test]
fn zrandmember_small_ratio_samples_k_distinct_members() {
  let members: Vec<Vec<u8>> = (0..1000).map(|i| format!("m{i}").into_bytes()).collect();
  let mut zset = zset_with(&slices_of(&members));
  let order: Vec<Vec<u8>> = zset.sorted_set_dict.keys().cloned().collect();

  for seed in [11_i32, -2_000, 65_535] {
    let picked = indexes_of(&zrandmember(&mut zset, 3, seed), &order);
    assert_eq!(
      picked,
      shared_iterative_sample(1000, 3, seed, true),
      "seed={seed} 未走共用采样器的阈值分派臂"
    );
    assert!(picked.iter().all(|&i| i < 1000));
    let mut distinct = picked;
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), 3, "seed={seed} 正 count 必须互异");
  }
}

/// WITHSCORES：成员借用与分值配对不串位（resp2 扁平 member/score 交替）
#[test]
fn zrandmember_withscores_pairs_member_and_score() {
  let members = members_10();
  let mut zset = zset_with(&slices_of(&members));

  let mut sink = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut sink);
    zset.operate(
      SortedSetOperation::Zrandmember as u8,
      &[],
      pack_arg1(4, true, true),
      2026,
      &mut output,
      2,
    );
  }

  let items = parse_bulk_array(&sink);
  assert_eq!(items.len(), 8, "resp2 WITHSCORES 应为 4×2 项");
  for pair in items.as_chunks::<2>().0 {
    // 分值即写入序下的 1 基序号（与 zset_with 打分一致）
    let score: f64 = from_utf8(&pair[1]).unwrap().parse().unwrap();
    assert_eq!(
      pair[0],
      members[score as usize - 1],
      "成员与分值配对串位: {:?} vs {score}",
      String::from_utf8_lossy(&pair[0])
    );
  }
}

/// 字段级过期：先算 count 与下标、再只读写出，过期字段不入应答
#[test]
fn hrandfield_skips_expired_fields_after_purge() {
  let mut hash = hash_with(&[b"live", b"dead"]);
  let now = now_ticks();
  hash.set_expiration(b"live", now + LIVE_SPAN, ExpireOption::NONE);
  // 过期字段经装载单点直挂过去刻度（set_expiration 对过去刻度直接删除条目）
  hash.insert_expiration(b"dead".to_vec(), now - EXPIRED_SPAN);

  assert_eq!(hrandfield(&mut hash, 2, 5), vec![b"live".to_vec()]);
}
