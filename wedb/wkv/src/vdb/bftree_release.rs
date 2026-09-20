//! 换号清库 BfTree 树回收旁表（FLUSHDB / FLUSHNS 换号联动 RI 与升阶树延迟销毁）
//!
//! 树身份 = f(物理域, 用户键)（树身份键为物理 Meta 键，见
//! crate::range_index::tree_identity_key），但主存无键遍历 API 无法按域反查
//! 存根——换号清库若不联动回收，旧域树仍注册在 live_indexes 且数据文件在盘，
//! 同名重建索引被 create_bftree 的 contains_key 拦截报 IndexExists，索引卡死为
//! 不可用、不可删、不可重建（内存磁盘双泄漏）。
//!
//! 本旁表在 wkv 侧以 (vns, vdb) → 裸用户键集登记全部 BfTree 注册面（RI.CREATE、
//! 集合升阶、惰性激活、迁移发布/重命名、检查点恢复），换号时按域取走键集，
//! 逐键按域重建树身份键后 detach_tree 同步摘除注册表，再投递至待释放队列，
//! 物理释放（纪元屏障排空引擎 + 删除数据文件）由内置 GC 轮次经
//! [`WedbStore::drain_bftree_release`] 在后台线程承接，删除受与日志紧缩同口径
//! 的安全纪元期限门控（[`PendingTreeRelease`]，期限内只暂存）——树文件是集合
//! 内容唯一持久副本，删除先于换号批持久化落地即崩溃窗（doc/zh/db.md
//! 偏序 GC 屏障）；从库回放只换号与投递，不阻塞复制流水线；与
//! flush_all_databases 的 range_index.clear_all 收敛同一回收语义。
//!
//! 刻意差异（对标 garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase
//! 与 libs/server/Resp/RangeIndex/RangeIndexManager.cs:RegisterIndex）：C# 每库
//! 独立 Tsavorite 实例，FlushDatabase 即整段日志截断，RI 树随库消亡，无此问题面；
//! 其 RegisterIndex 撞活条目仅 LogError 后仍向客户端回 OK 并丢弃新树，rust 不
//! 照搬——IndexExists 按正确语义上抛，换号回收由本旁表显式编排。

use parking_lot::Mutex;
use wbase::{
  map::{HashMap, HashSet},
  time::now_ticks,
};
use wbftree::DetachedTree;
use wdev::Device;

use crate::{range_index::tree_identity_key, store::WedbStore, vdb::reclaim_expired_at};

/// 换号回收旁表：(vns, vdb) → 域内登记的 BfTree 裸用户键集
/// （gxhash 哈希 + parking_lot 互斥；登记/回收均为冷路径元数据操作）
pub(crate) type BftreeDomains = Mutex<HashMap<(u64, u64), HashSet<Box<[u8]>>>>;

/// 按域分组的登记键集批次（FLUSHNS 全命名空间取数的产出形态）
pub(crate) type DomainKeySets = Vec<(u64, u64, Vec<Box<[u8]>>)>;

/// 待物理释放条目：已摘注册的树批次 + 安全纪元删除期限
///
/// 树文件是集合内容唯一持久副本（hlog 仅 35B 存根），换号批的持久化镜像按
/// 提交策略落地（周期提交档 / 提交积压 / 刷盘失败），删除先于持久落地即打开
/// 「崩溃后映射回滚旧域、旧域存根复活而树已物理消失」的窗口——条目携带与
/// 日志紧缩死亡账本同口径的 `expired_at`（doc/zh/db.md 偏序 GC 屏障：树文件
/// 删除侧同受安全纪元约束），消费侧期限内只暂存绝不 unlink
pub(crate) struct PendingTreeRelease {
  /// 删除期限 ticks：入队时刻 + db_gc_reclaim_delay_secs（[`reclaim_expired_at`]）
  pub expired_at: i64,
  /// 已摘注册的待释放树批次
  pub tree: DetachedTree,
}

impl<D: Device> WedbStore<D> {
  /// 登记域内 BfTree 裸用户键（全部注册面统一落表内核；幂等）
  ///
  /// 两形态分工（会话侧一参入口 [`crate::session::StoreSession::register_bftree_key`]
  /// 与恢复侧三参直调共用本内核，本函数是唯一落表点）：
  /// - **会话态取当前域**：运行期注册面（RI.CREATE、集合升阶、惰性激活、
  ///   迁移发布/重命名、覆盖写清元记录）一律走会话一参入口，由会话方法一次性
  ///   load 本会话 active_vns/active_vdb 后转调本内核——域是会话当前上下文的事实，
  ///   调用方不传递任何派生量（对标 C# RangeIndexManager.cs:396/:457 只收 keyBytes、
  ///   :471 注销自算 keyId 的单点派生形态）；严禁在会话侧另开三参口手传域。
  /// - **恢复态取解出域**：检查点恢复登记（store::cpr_host）彼时 vdb 映射尚未
  ///   重建、会话不存在亦不可用活跃域原子，域必须自物理键前缀
  ///   （NamespaceDbCodec::decode_tagged_key）解出后三参直调本内核；后来者不得
  ///   把恢复站改接会话一参口而错取活跃域。
  ///
  /// 死亡域守卫：登记时域已换号退役（创建在途撞上换号的窄窗）则立即销毁刚建
  /// 树并拒绝登记——创建方后续元记录写路径同域死亡，语义无害；杜绝旁表残留
  /// 死域条目导致同名重建索引被 IndexExists 拦截。树销毁按域重建身份键
  /// （[`tree_identity_key`]），与死亡域在用树零交集
  pub(crate) fn register_bftree_key(&self, vns: u64, vdb: u64, key: &[u8]) {
    if self.vdb.is_dead_domain(vns, vdb) {
      let id_key = tree_identity_key(vns, vdb, key);
      if let Err(e) = self.range_index.delete_index(&id_key) {
        log::warn!("死亡域 BfTree 即时销毁失败，留待启动对账回收: vns={vns}, vdb={vdb}, err={e}");
      }
      return;
    }
    let mut domains = self.bftree_domains.lock();
    let set = domains.entry((vns, vdb)).or_default();
    if !set.contains(key) {
      set.insert(Box::from(key));
    }
  }

  /// 注销域内 BfTree 裸用户键（删除面统一出口的逆操作；未登记时零操作）
  pub(crate) fn unregister_bftree_key(&self, vns: u64, vdb: u64, key: &[u8]) {
    let mut domains = self.bftree_domains.lock();
    if let Some(keys) = domains.get_mut(&(vns, vdb)) {
      keys.remove(key);
      if keys.is_empty() {
        domains.remove(&(vns, vdb));
      }
    }
  }

  /// 取走指定域的全部登记键（FLUSHDB 换号回收编排取数；域表项随之移除）
  pub(crate) fn take_bftree_domain(&self, vns: u64, vdb: u64) -> Vec<Box<[u8]>> {
    self
      .bftree_domains
      .lock()
      .remove(&(vns, vdb))
      .map(|keys| keys.into_iter().collect())
      .unwrap_or_default()
  }

  /// 取走指定虚拟命名空间下全部域的登记键，按域分组成
  /// `(vns, vdb, 键集)`（FLUSHNS 换号回收编排取数；身份键按域重建依赖此分组）
  pub(crate) fn take_bftree_domains_of_vns(&self, vns: u64) -> DomainKeySets {
    let mut domains = self.bftree_domains.lock();
    let stale: Vec<(u64, u64)> = domains
      .keys()
      .filter(|&&(dvns, _)| dvns == vns)
      .copied()
      .collect();
    stale
      .into_iter()
      .filter_map(|domain| {
        domains
          .remove(&domain)
          .map(|keys| (domain.0, domain.1, keys.into_iter().collect()))
      })
      .collect()
  }

  /// 清空旁表（flush_all_databases 全域清空，与 range_index.clear_all 对齐）
  pub(crate) fn clear_bftree_domains(&self) {
    self.bftree_domains.lock().clear();
  }

  /// 按域取走键集投递异步物理释放（换号回收唯一投递入口）
  ///
  /// 逐键按域重建树身份键（[`tree_identity_key`]，物理 Meta 键形态）后
  /// detach_tree 同步摘除注册表（同名重建不被 IndexExists 拦截的语义保持：
  /// 本函数返回时旧域条目已从 live_indexes 摘净），取回的 DetachedTree 批次
  /// 压入待释放队列——纪元注册、引擎排空与数据文件删除一律移交
  /// [`Self::drain_bftree_release`]（内置 GC 轮次承接，doc/zh/db.md 主从异步
  /// 屏障：从库回放只换号投递、不阻塞复制流水线）。摘除与释放拆两段后仍是
  /// 一套编排：投递失败仅剩 detach 返回 None（条目本就不存在），无需再逐键
  /// 容错告警。
  pub(crate) fn reclaim_bftree_keys(&self, vns: u64, vdb: u64, keys: Vec<Box<[u8]>>) {
    let detached_batch: Vec<_> = keys
      .into_iter()
      .filter_map(|key| {
        let id_key = tree_identity_key(vns, vdb, &key);
        self.range_index.detach_tree(&id_key, true)
      })
      .collect();
    if !detached_batch.is_empty() {
      // 安全纪元删除期限入队即定（同批一次取样，逐键重复取样无增益），与
      // 死亡账本墓碑同一换算单点：期限内消费侧只暂存，宁滞留不提前
      let expired_at = reclaim_expired_at(now_ticks(), self.config.gc.db_gc_reclaim_delay_secs);
      self.bftree_release.lock().extend(
        detached_batch
          .into_iter()
          .map(|tree| PendingTreeRelease { expired_at, tree }),
      );
    }
  }

  /// 排空待释放队列：甄别已过安全纪元期限的条目逐条经 release_detached 挂入
  /// 纪元延迟清理（生产由 [`crate::gc::spawn_bftree_reclaimer`] 常驻后台任务
  /// 消费；无运行时形态的引擎与测试经本入口手动驱动，消费内核仅此一处）
  ///
  /// 期限内的条目原序留队待下轮（与日志紧缩 `pop_reclaimable` 的 expired_at
  /// 门控同口径——树文件删除侧不绕过安全纪元，期限内崩溃回滚旧域时旧域树
  /// 完好可恢复）；已到期条目至多消费 `cap` 条，剩余留待下轮（换号风暴下
  /// 物理释放有界推进）。收割线程因条带锁竞争让位而退订的批次同由本入口重投
  /// （RangeIndexManager::harvest_release_retries，同一条带锁内世代判据；重投
  /// 批次已过第一道期限门，不再复检）。返回本轮消费条数。
  pub fn drain_bftree_release(&self, cap: usize) -> usize {
    let now = now_ticks();
    let batch: Vec<DetachedTree> = {
      let mut pending = self.bftree_release.lock();
      // 单趟分区：已到期条目摘出消费（至多 cap），未到期原序暂存
      let mut due = Vec::new();
      let mut hold = Vec::new();
      for item in pending.drain(..) {
        let dst = if item.expired_at <= now && due.len() < cap {
          &mut due
        } else {
          &mut hold
        };
        dst.push(item);
      }
      *pending = hold;
      due.into_iter().map(|item| item.tree).collect()
    };
    let drained = batch.len();
    for detached in batch {
      self.range_index.release_detached(detached);
    }
    drained + self.range_index.harvest_release_retries(cap)
  }

  /// 启动对账：回收旁表中已换号死亡域的残留登记
  ///
  /// 检查点恢复期为全部 RI 存根登记旁表（含死亡域残留存根，恢复时 vdb 映射
  /// 尚未重建不可判死），rebuild_vdb 完成后据 is_dead_domain 逐键摘除恢复期
  /// 注册的 pending 条目并入待释放队列，同名重建索引不再被拦截；孤儿数据
  /// 文件删除随队列由后台 GC 轮次落地。投递即重新起算安全纪元期限——上次
  /// 运行期未竟的删除（期限内崩溃 / Drop 滞留）再多等一个周期，宁可滞留
  /// 不可提前
  pub(crate) fn reclaim_dead_domain_bftrees(&self) {
    let dead_domains: Vec<(u64, u64)> = {
      let domains = self.bftree_domains.lock();
      domains
        .keys()
        .filter(|&&(vns, vdb)| self.vdb.is_dead_domain(vns, vdb))
        .copied()
        .collect()
    };
    for (vns, vdb) in dead_domains {
      let keys = self.take_bftree_domain(vns, vdb);
      self.reclaim_bftree_keys(vns, vdb, keys);
    }
  }
}
