//! 分层集合降阶候选批次与防饿死洗牌集成测试
//! （对应 libs/server/Objects/TieredStorage/TieredObjectManager.cs）

use fastrand::Rng;
use wnode::resp::objects::tiered_demote::{
  DEMOTE_MAX_KEYS_PER_ROUND, DemoteCandidate, demote_batch,
};
use wval::GarnetObjectType;

/// 构造标记候选序：前 `dead` 个为死区键（过 meta.size 预筛、恒败物化后谓词、
/// 元记录不改写下轮仍入围），其余为冷键；用户键首字节作标记位
fn marked_keys(dead: usize, cold: usize) -> Vec<DemoteCandidate> {
  (0..dead + cold)
    .map(|i| {
      let marker = if i < dead { b'D' } else { b'C' };
      (0u64, 0u64, vec![marker, i as u8], GarnetObjectType::Hash)
    })
    .collect()
}

/// 限额内候选全保留（集合不变、仅次序随机）——候选不超限批时无饿死面
#[test]
fn batch_within_limit_keeps_all() {
  let cands = marked_keys(0, 8);
  let mut out = demote_batch(cands.clone(), &mut Rng::with_seed(42));
  out.sort_by(|a, b| a.2.cmp(&b.2));
  assert_eq!(out, cands);
}

/// 超限额恰取限额、无重复、不引入集合外候选
#[test]
fn batch_over_limit_truncates_exactly() {
  let cands = marked_keys(0, 64);
  let out = demote_batch(cands.clone(), &mut Rng::with_seed(42));
  assert_eq!(out.len(), DEMOTE_MAX_KEYS_PER_ROUND);
  assert!(out.iter().all(|c| cands.contains(c)), "出列候选须出自输入");
  let mut keys: Vec<&[u8]> = out.iter().map(|c| c.2.as_slice()).collect();
  keys.sort_unstable();
  keys.dedup();
  assert_eq!(keys.len(), DEMOTE_MAX_KEYS_PER_ROUND, "出列候选不得重复");
}

/// 死区键占满迭代序头部时冷键仍能出列：洗牌出列序与输入序独立，固定种子
/// 保证断言确定可复现。单种子冷键全灭概率 = 1/C(24,16) ≈ 1.4e-6，64 个
/// 种子下出列种子数下界 56 在 10σ 之外，零 flaky
#[test]
fn cold_keys_not_starved_when_deadzone_fills_head() {
  let cands = marked_keys(DEMOTE_MAX_KEYS_PER_ROUND, 8);
  let hits = (0..64u64)
    .filter(|s| {
      demote_batch(cands.clone(), &mut Rng::with_seed(*s))
        .iter()
        .any(|c| c.2[0] == b'C')
    })
    .count();
  assert!(hits >= 56, "死区占满头部时冷键出列种子数不足: {hits}/64");
}
