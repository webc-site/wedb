//! 紧缩扫描作用域内的方案 A 集合元数据水位回放与 key_id 死亡登记

use whasher::{HashMap, new_hash_map};
use wval::{KeyTag, META_VALUE_SIZE, MetaValue, NamespaceDbCodec};

use crate::host::CompactStore;

/// 紧缩扫描作用域内的方案 A 集合元数据归属表与 key_id 死亡登记表
///
/// 健全性论证（为何收尾时可以安全删除 key_id_versions 死条目）：
/// 1. 并发进行的集合删除（DEL）其墓碑/新 meta 记录一律追加在日志尾部
///    （地址 ≥ read_only_address ≥ until_address），绝不可能落入本次扫描区间
///    [begin_address, until_address)；因此扫描途中登记到的死亡记录必然是
///    "已完成"的删除：结合不变量「子键写入先于其 meta 落地」与「DEL 墓碑地址
///    大于该集合全部子键地址」，该 key_id 的全部子键地址 < 死亡地址 ≤ actual_until，
///    已全部在本次扫描中被 is_stale_subkey 判死回收，截断后日志中不再有该 key_id 的任何记录。
/// 2. 若之后（异常情况下）紧缩器再遇到该 key_id 的记录，get_key_id_meta 返回 None
///    ⇒ is_stale_subkey 保守保留该记录——错删方向是保守的（宁可泄漏一条元数据，绝不丢数据）。
/// 3. 两张表均为单次紧缩扫描的临时作用域状态（与 Scan 模式候选表同生命周期），不跨紧缩共享。
pub(super) struct MetaDeathScope {
  /// Meta 物理键（ns+MetaTag+用户键）-> key_id：每次遇到非墓碑 Meta 值记录覆盖写入，
  /// 保留最新归属（重建集合分配新 key_id，旧 key_id 的死亡登记不受影响）
  meta_owner: HashMap<Box<[u8]>, u64>,
  /// key_id -> 最早死亡记录地址（两种死亡形态：Fast Drop 幽灵 meta / Meta 键墓碑）
  death_addr: HashMap<u64, u64>,
}

impl MetaDeathScope {
  /// 创建空扫描作用域
  pub(super) fn new() -> Self {
    Self {
      meta_owner: new_hash_map(),
      death_addr: new_hash_map(),
    }
  }

  /// 登记死亡地址（扫描地址单调递增，首次登记即最早值）
  fn mark_death(&mut self, key_id: u64, addr: u64) {
    self.death_addr.entry(key_id).or_insert(addr);
  }

  /// 单条记录的元数据回放 + 死亡登记（与原 replay_key_meta 合并解析，避免重复解码 MetaValue）
  pub(super) fn observe<S: CompactStore>(
    &mut self,
    store: &S,
    is_tombstone: bool,
    key: &[u8],
    val: &[u8],
    addr: u64,
  ) {
    if !matches!(NamespaceDbCodec::decode_tag(key), Some(KeyTag::Meta)) {
      return;
    }
    if is_tombstone {
      // 死亡形态 b：Meta 物理键的墓碑记录（非 Flattened 删除路径留下），
      // 归属表中有该键 ⇒ 登记归属 key_id 的最早死亡地址
      if let Some(&owner) = self.meta_owner.get(key) {
        self.mark_death(owner, addr);
      }
      return;
    }
    if val.len() < META_VALUE_SIZE {
      return;
    }
    let Ok(meta) = MetaValue::from_slice(val) else {
      return;
    };
    // 归属表覆盖写入：同物理键以最新 meta 记录的归属为准
    // （归属未变化时零分配跳过，重复 meta 保存是常态路径）
    if self.meta_owner.get(key) != Some(&meta.key_id) {
      self.meta_owner.insert(Box::from(key), meta.key_id);
    }
    if meta.size == 0 {
      // 死亡形态 a：Fast Drop 幽灵 meta（version 递增 + size = 0 的非墓碑改写）
      self.mark_death(meta.key_id, addr);
    } else if let Some(&d) = self.death_addr.get(&meta.key_id) {
      // 不变量哨兵（「删除墓碑必在尾部」前提的检测点）：
      // DEL 墓碑/幽灵 meta 一律追加在日志尾部（地址 ≥ until_address），扫描区间内
      // 一旦登记死亡，同 key_id 绝无更晚的存活 meta 写入——集合复活必分配新 key_id
      // （「重建集合分配新 key_id」归属规则）。若死亡登记之后又出现同 key_id 的
      // 存活 meta，说明出现了向历史地址区写入的分配路径（日志地址复用），收尾
      // key_id_versions 死条目回收的前提即被破坏——立即暴露，绝不静默漏回收。
      // release 下 debug_assert 被剔除，error 日志保证破坏可见（评审 P2-4）
      if addr >= d {
        log::error!(
          "key_id {:x} 的存活 meta 写入 {addr:#x} 出现在死亡登记 {d:#x} 之后：非尾部分配路径破坏墓碑必在尾部不变量，死条目回收前提失效",
          meta.key_id
        );
      }
      debug_assert!(
        addr < d,
        "key_id {:x} 墓碑必在尾部不变量破坏（详见 error 日志）",
        meta.key_id
      );
    }
    // 单调回放版本水位：仅接受比当前已知更新的版本，保证紧缩途中的子键淘汰判定
    // 能看到时间线上已发生过的集合创建、删除与版本演进
    if store
      .get_key_id_meta(meta.key_id)
      .is_none_or(|(ver, _)| meta.version > ver)
    {
      store.update_key_id_meta(meta.key_id, meta.version, meta.size > 0);
    }
  }

  /// 紧缩收尾：回收死亡地址已完整落入本次紧缩区间的 key_id_versions 死条目
  ///
  /// 双重守卫防误删：da <= actual_until 保证死亡记录已随本次截断整体退役；
  /// 内存态仍判死（is_alive == false）保证绝不回收重建集合（新 key_id）
  /// 或已被并发复活/检查点重放恢复的条目。
  pub(super) fn collect_dead<S: CompactStore>(&self, store: &S, actual_until: u64) {
    for (&key_id, &da) in &self.death_addr {
      if da <= actual_until
        && store
          .get_key_id_meta(key_id)
          .is_some_and(|(_, is_alive)| !is_alive)
      {
        store.remove_key_id_meta(key_id);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn meta_death_scope_mark_death() {
    let mut scope = MetaDeathScope::new();
    scope.mark_death(100, 1024);
    scope.mark_death(100, 2048);
    assert_eq!(scope.death_addr.get(&100), Some(&1024));
  }
}
