//! FLUSH 域回收族（rust 换号隔离拓扑专属，无 C# partial 对位）：
//! [`RegistryReclaim`] 域值、登记表域回收漏斗与域内用户键枚举/计数，
//! 自 vector_manager.rs 核心拆出。

use wbase::map::HashSet;
use wvector::store::StoreCallbacks;

use super::{
  types::{CONTEXT_STEP, CONTEXTS_PER_METADATA},
  vector_manager::VectorManager,
  vector_manager_index::Index,
  vector_manager_locking::{RegistryDomain, registry_user_key, split_registry_key},
};

/// 登记表域回收范围（FLUSH 族三臂，域值与广播条目载荷同源）。
///
/// C# 每库独立 Tsavorite 日志，FLUSH 物理截断即索引记录随库整体消亡
/// （GarnetServer.cs:426-427 每库 store/AOF）；rust 共享单日志换号隔离，
/// 登记表回收必须显式联动，此为同等清库语义在 rust 拓扑下的唯一实现。
#[derive(Debug, Copy, Clone)]
pub enum RegistryReclaim {
  /// 单库：物理域 (vns, 换号前旧 vdb) 与可选逻辑槽位盖章——FLUSHDB 载荷域。
  Database {
    vns: u64,
    vdb: u64,
    slot: Option<u16>,
  },
  /// 整命名空间：换号前旧 vns——FLUSHNS 载荷域。
  Namespace { vns: u64 },
  /// 全域——FLUSHALL / reset。
  All,
}

impl RegistryReclaim {
  /// 域命中判定单点。
  #[inline]
  fn matches(self, domain: RegistryDomain) -> bool {
    match self {
      Self::Database { vns, vdb, .. } => domain.vns == vns && domain.vdb == vdb,
      Self::Namespace { vns } => domain.vns == vns,
      Self::All => true,
    }
  }
}

/// 域回收扫尾轮数（快照-逐键锁回收的竞速窗口补扫轮数；双轮即"存量 +
/// 第一轮锁间隙漏收"两段，残差见 [`VectorManager::reclaim_registry_domain`] 头注）。
const RECLAIM_SWEEP_ROUNDS: usize = 2;

impl<S: StoreCallbacks> VectorManager<S> {
  /// 登记表域回收单点漏斗（FLUSHDB/FLUSHNS/FLUSHALL/reset 主端执行段与
  /// AOF Flush 族重放臂共用）。
  ///
  /// 逐条目走 [`split_registry_key`] 比对域值，命中项复用既有
  /// [`Self::request_deletion`] + 摘表通道，不另造清理编排；换号语义下
  /// 旧域条目在新域不可达，回收即清库（重启经 AOF 带域条目按域重建）。
  ///
  /// 双轮扫尾：回收走「快照-逐键锁」，与 worker 线程在途 VADD/RENAME 竞速
  /// 时快照点之后完成落表的条目不在 victims（死域登记条目 + 原生索引 +
  /// context in_use 位运行期滞留）。单轮快照窗口以第二轮补扫收窄——第一轮
  /// 逐键锁间隙中完成落表的漏收条目由第二轮快照捕获；残差仅剩第二轮快照点
  /// 之后的在途落表（概率性运行期泄漏，AOF 全序保证重启重放收敛），不引入
  /// 全域登记锁（违背数据面零锁纪律）。
  pub async fn reclaim_registry_domain(&self, reclaim: RegistryReclaim) {
    let mut target_slots: HashSet<u16> = HashSet::default();
    if let RegistryReclaim::Database { slot: Some(s), .. } = reclaim {
      target_slots.insert(s);
    }

    for _ in 0..RECLAIM_SWEEP_ROUNDS {
      let victims: Vec<Vec<u8>> = self
        .key_index_registry
        .pin()
        .iter()
        .filter(|(rk, _)| reclaim.matches(split_registry_key(rk.as_slice()).0))
        .map(|(rk, _)| rk.as_slice().to_vec())
        .collect();
      if victims.is_empty() {
        break;
      }
      for rk in &victims {
        // 若外部未显式提供 slot，从被清退键的既有元数据中提取 slot 盖章
        if let Some(index_bytes) = self.stored_index_of(rk)
          && let Some(index) = Index::from_bytes(&index_bytes)
        {
          let (c_idx, c_val) = Self::decompose_context(index.context);
          if let Some(meta) = self.context_metadatas.lock().get(c_idx) {
            target_slots.insert(meta.slots[(c_val / (CONTEXT_STEP as u16)) as usize]);
          }
        }
        // 逐 victim 条带独占锁（对齐 C# RunRequestDropTaskAsync 逐键
        // AcquireExclusiveLock → DropIndex → Release 协议）；域级清库语义下
        // 键间锁间隙无碍——换号后旧域条目在新域不可达
        let _lock = self.vector_set_locks.acquire_exclusive(rk).await;
        self.delete_vector_set_of(rk).await;
      }
    }

    // 漏收兜底：按 context_metadatas 反查归属已清除 hash_slot 的残留上下文，补投清理通道，
    // 消除在途 VADD/RENAME 与 FLUSHDB 竞速导致的 context 位与原生索引内存滞留
    let live_contexts: HashSet<u64> = self
      .key_index_registry
      .pin()
      .iter()
      .filter_map(|(rk, bytes)| {
        let (domain, _) = split_registry_key(rk.as_slice());
        if !reclaim.matches(domain) {
          Index::from_bytes(bytes.as_slice()).map(|idx| idx.context)
        } else {
          None
        }
      })
      .collect();

    let all_domains = matches!(reclaim, RegistryReclaim::All);
    let mut orphan_contexts = Vec::new();
    let needs_meta_update = self.sweep_cleanable_contexts(
      |context, slot| {
        (all_domains || target_slots.contains(&slot)) && !live_contexts.contains(&context)
      },
      &mut orphan_contexts,
    );

    if needs_meta_update {
      self.update_context_metadata().await;
    }
    for context in orphan_contexts {
      self.service.drop_index(context);
      if !self.cleanup_task_channel.push(context) {
        log::warn!("Could not enqueue orphan Vector Set cleanup: {context}");
      }
    }
  }

  /// 登记表域内用户键枚举单点（DBSIZE/KEYS/SCAN 慢路径投影）。
  ///
  /// `f` 收剥域后的用户键切片（[`registry_user_key`] 单点）；域判定按复合键字节
  /// 前缀比对——OPPV 变长首字节查表定长，不同域的 `NsVarint`/`DbVarint` 段互不为
  /// 字节前缀，starts_with 即精确域命中（与 [`super::vector_manager_locking::registry_key`] 布局同源，零二次解码）。
  pub fn for_each_domain_user_key(&self, prefix: &[u8], mut f: impl FnMut(&[u8])) {
    self.key_index_registry.pin().iter().for_each(|(rk, _)| {
      if rk.starts_with(prefix) {
        f(registry_user_key(rk.as_slice()));
      }
    });
  }

  /// 登记表域内计数单点（DBSIZE 向量增量；字节前缀域比对同上）。
  pub fn registry_domain_count(&self, prefix: &[u8]) -> usize {
    self
      .key_index_registry
      .pin()
      .iter()
      .filter(|(rk, _)| rk.starts_with(prefix))
      .count()
  }

  // ======================== FLUSH 域回收 ========================

  /// FLUSH 域回收与恢复对账两臂共用的全表清扫骨架（唯一实现，禁第二逐位
  /// 扫描形态；内部自锁 context_metadatas，调用方不得持其守卫）：遍历各
  /// 元数据块内在用（context ≠ 0 非法哨兵）且未标清理的上下文，对
  /// `accept(context, 槽位盖章)` 放行者盖清理标记、收进 `out` 并标脏所在
  /// 块；返回本轮是否有标记。纯同步位图读改，不跨 await。
  pub(super) fn sweep_cleanable_contexts(
    &self,
    mut accept: impl FnMut(u64, u16) -> bool,
    out: &mut Vec<u64>,
  ) -> bool {
    let mut marked_any = false;
    let mut metas = self.context_metadatas.lock();
    for (i, meta) in metas.iter_mut().enumerate() {
      let (allow_zero, offset) = (i != 0, Self::offset_for_context_metadata(i));
      let mut marked = false;
      for j in 0..CONTEXTS_PER_METADATA {
        let context = offset + j * CONTEXT_STEP;
        let (_, context_value) = Self::decompose_context(context);
        if context != 0
          && meta.is_in_use(allow_zero, context_value)
          && !meta.is_cleaning_up(allow_zero, context_value)
          && accept(context, meta.slots[j as usize])
        {
          meta.mark_cleaning_up(allow_zero, context_value);
          out.push(context);
          marked = true;
        }
      }
      if marked {
        self.dirty_context_metadatas.lock().insert(i);
        marked_any = true;
      }
    }
    marked_any
  }
}
