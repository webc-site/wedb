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
//! 逐键按域重建树身份键后 detach_tree 同步摘除注册表，引擎就地 dispose（摘
//! 注册点 release_cache 已归还配额，真实页环占用与记账同步归零，与冷树回收
//! dispose_tree_under_lock 同释放时长口径，绝不随删除期限滞留整树），数据文件
//! 删除由内置 GC 轮次经 [`WedbStore::drain_bftree_release`] 在后台线程承接，
//! 受与日志紧缩同口径的安全纪元期限门控（[`PendingTreeRelease`]，期限内只
//! 暂存）——树文件是集合内容唯一持久副本，删除先于换号批持久化落地即崩溃窗
//! （doc/zh/db.md 偏序 GC 屏障）；从库回放只换号与投递，不阻塞复制流水线；与
//! flush_all_databases 的 range_index.clear_all 收敛同一回收语义。
//!
//! 刻意差异（对标 garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase
//! 与 libs/server/Resp/RangeIndex/RangeIndexManager.cs:RegisterIndex）：C# 每库
//! 独立 Tsavorite 实例，FlushDatabase 即整段日志截断，RI 树随库消亡，无此问题面；
//! 其 RegisterIndex 撞活条目仅 LogError 后仍向客户端回 OK 并丢弃新树，rust 不
//! 照搬——IndexExists 按正确语义上抛，换号回收由本旁表显式编排。

use core::mem::take;
use std::sync::Arc;

use parking_lot::Mutex;
use wbase::{
  map::{HashMap, HashSet},
  time::now_ticks,
};
use wbftree::{DetachedTree, RangeIndexManager};
use wdev::Device;

use crate::{
  range_index::{RangeIndexError, range_index_blocking, tree_identity_key},
  store::WedbStore,
  vdb::reclaim_expired_at,
};

/// 换号回收旁表：(vns, vdb) → 域内登记的 BfTree 裸用户键集
/// （gxhash 哈希 + parking_lot 互斥；登记/回收均为冷路径元数据操作）
pub(crate) type BftreeDomains = Mutex<HashMap<(u64, u64), HashSet<Box<[u8]>>>>;

/// 按域分组的登记键集批次（FLUSHNS 全命名空间取数的产出形态）
pub(crate) type DomainKeySets = Vec<(u64, u64, Vec<Box<[u8]>>)>;

/// 待 unlink 条目：已摘注册且引擎已就地 dispose 的树批次 + 安全纪元删除期限
///
/// 树文件是集合内容唯一持久副本（hlog 仅 35B 存根），换号批的持久化镜像按
/// 提交策略落地（周期提交档 / 提交积压 / 刷盘失败），删除先于持久落地即打开
/// 「崩溃后映射回滚旧域、旧域存根复活而树已物理消失」的窗口——条目携带与
/// 日志紧缩死亡账本同口径的 `expired_at`（doc/zh/db.md 偏序 GC 屏障：树文件
/// 删除侧同受安全纪元约束），消费侧期限内只暂存绝不 unlink。
///
/// 换号臂投递的条目 `tree` 恒为 `None`（引擎已在入队前 dispose，unlink 的
/// 世代判据只依赖 key_id / key_hash / data_path，与 release_retries 重投条目
/// 同形态）；`Some` 分支保留给他臂（冷树懒恢复收口、启动对账）直达
/// release_detached 的既有形态，消费内核单一不分支
pub(crate) struct PendingTreeRelease {
  /// 删除期限 ticks：入队时刻 + db_gc_reclaim_delay_secs（[`reclaim_expired_at`]）
  pub expired_at: i64,
  /// 已摘注册的树批次（换号臂投递时引擎已 dispose，tree=None）
  pub tree: DetachedTree,
}

impl<D: Device> WedbStore<D> {
  /// 登记域内 BfTree 裸用户键（全部注册面统一落表内核；幂等）
  ///
  /// 消费形态分工（共用本内核，本函数是唯一落表点）：
  /// - **会话态取当前域**：逐点现解析且无换代窗的运行期注册面（惰性激活、迁移
  ///   发布/重命名、覆盖写清元记录）走会话一参入口
  ///   [`crate::session::StoreSession::register_bftree_key`]，由会话方法一次性
  ///   load 本会话 active_vns/active_vdb 后转调本内核——域是会话当前上下文的事实，
  ///   调用方不传递任何派生量（对标 C# RangeIndexManager.cs:396/:457 只收 keyBytes、
  ///   :471 注销自算 keyId 的单点派生形态）；
  /// - **域钉链传钉定域**：升阶 / RI.CREATE 多 await 发布链（promote /
  ///   range_index_create）链首单点钉定 (vns, vdb)，登记域必须 = 钉定域，链中
  ///   重解析即树身份域与旁表登记域跨代撕裂——两链直调本三参内核传钉定域
  ///   （禁另开新口，仍复用本唯一落表点），落盘前换代复核兜底显式失败；
  /// - **恢复态取解出域**：检查点恢复登记（store::cpr_host）彼时 vdb 映射尚未
  ///   重建、会话不存在亦不可用活跃域原子，域必须自物理键前缀
  ///   （NamespaceDbCodec::decode_tagged_key）解出后三参直调本内核；后来者不得
  ///   把恢复站改接会话一参口而错取活跃域。
  ///
  /// 死亡域守卫：登记时域已换号退役（创建在途撞上换号）则拒绝登记并返回
  /// `false`，树销毁由调用方经异步卸载单点 [`Self::destroy_dead_domain_tree`]
  /// 承接（detach_tree 内取树条带无界写停车档，同步 fn 内无法卸载，见票
  /// wkv-flush-replay-detach-tree-async-stripe-park-core-deadlock）——域钉链下
  /// 该形态由创建方落盘前换代复核显式失败回滚（[`Error::GenerationMoved`]，
  /// 命令不回 OK），杜绝旁表残留死域条目导致同名重建索引被 IndexExists 拦截；
  /// 树销毁按域重建身份键（[`tree_identity_key`]），与死亡域在用树零交集。
  /// 恢复期登记面（cpr_host）判定不可达（彼时 vdb 映射尚未重建恒不判死），
  /// 返回值可忽略
  pub(crate) fn register_bftree_key(&self, vns: u64, vdb: u64, key: &[u8]) -> bool {
    if self.vdb.is_dead_domain(vns, vdb) {
      return false;
    }
    let mut domains = self.bftree_domains.lock();
    let set = domains.entry((vns, vdb)).or_default();
    if !set.contains(key) {
      set.insert(Box::from(key));
    }
    true
  }

  /// 死亡域刚建树即时销毁单点（守卫拒绝登记后的异步卸载承接面）：整树注销
  /// （detach 摘注册 + 文件删除）经 [`crate::range_index::range_index_blocking`]
  /// 在阻塞线程执行——detach_tree 的条带无界写停车档绝不触本核 reactor；
  /// 失败仅告警（孤儿留待启动对账 [`Self::reclaim_dead_domain_bftrees`] 收敛），
  /// 与守卫臂原「尽力而为」语义同型
  pub(crate) async fn destroy_dead_domain_tree(self: &Arc<Self>, vns: u64, vdb: u64, key: &[u8]) {
    let id_key = tree_identity_key(vns, vdb, key);
    let mgr = Arc::clone(&self.range_index);
    // 双层错误同面告警：外层 JoinHandle 异常（卸载任务 panic）与内层
    // delete_index 失败（注销/文件删除 IO 硬错）同型兜底
    if let Err(e) = range_index_blocking(move || mgr.delete_index(&id_key))
      .await
      .and_then(|inner| inner.map_err(RangeIndexError::from))
    {
      log::warn!("死亡域 BfTree 即时销毁失败，留待启动对账回收: vns={vns}, vdb={vdb}, err={e:?}");
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

  /// 快照旁表全体登记域与键（只读，不取走；后台降阶候选发现取数入口）
  ///
  /// 与 [`Self::take_bftree_domain`] 系取走语义不同：本入口仅克隆当前登记全貌，
  /// 登记面零影响——调用方逐键点读元记录自行裁决存活与水位，候选发现开销
  /// 因此与旁表规模（已升阶树态键数）而非主存记录规模挂钩
  pub fn snapshot_bftree_domains(&self) -> DomainKeySets {
    self
      .bftree_domains
      .lock()
      .iter()
      .map(|(d, keys)| (d.0, d.1, keys.iter().cloned().collect()))
      .collect()
  }

  /// 取走指定虚拟命名空间下全部域的登记键，按域分组成
  /// `(vns, vdb, 键集)`（FLUSHNS 换号回收编排取数；身份键按域重建依赖此分组）
  pub(crate) fn take_bftree_domains_of_vns(&self, vns: u64) -> DomainKeySets {
    let mut domains = self.bftree_domains.lock();
    let mut result = Vec::new();
    domains.retain(|&(dvns, dvdb), keys| {
      if dvns == vns {
        result.push((dvns, dvdb, take(keys).into_iter().collect()));
        false
      } else {
        true
      }
    });
    result
  }

  /// 清空旁表（flush_all_databases 全域清空，与 range_index.clear_all 对齐）；
  /// 冷树观察账连带全清——全域摘除路径与投递入口单点同动作，条目随域绝迹
  pub(crate) fn clear_bftree_domains(&self) {
    self.bftree_domains.lock().clear();
    self.cold_bftree_observed.lock().clear();
  }

  /// 按域取走键集投递异步物理释放（换号回收唯一投递入口）
  ///
  /// 逐键按域重建树身份键（[`tree_identity_key`]，物理 Meta 键形态）后
  /// detach_tree 同步摘除注册表（同名重建不被 IndexExists 拦截的语义保持：
  /// 本函数返回时旧域条目已从 live_indexes 摘净）。引擎与文件两窗拆分：取回
  /// 的 DetachedTree 批次就地 dispose 引擎——摘注册点 release_cache 已归还
  /// 配额，真实页环占用与记账同步归零，与冷树回收 dispose_tree_under_lock 同
  /// 释放时长口径（C# DisposeTreeUnderLock → BumpCurrentEpoch 仅纪元量级，
  /// 绝不随删除期限滞留整树）；dispose 幂等（单句柄 swap(None)），已借出的
  /// 点读 Guard / 扫描 Arc 自行保活引擎至用毕。投递条目仅余 unlink 所需世代
  /// 判据与 data_path，数据文件删除移交 [`Self::drain_bftree_release`]（内置
  /// GC 轮次承接，doc/zh/db.md 主从异步屏障：从库回放只换号投递、不阻塞复制
  /// 流水线）。摘除与释放拆两段后仍是一套编排：投递失败仅剩 detach 返回
  /// None（条目本就不存在），无需再逐键容错告警。
  ///
  /// detach 摘注册整段在阻塞线程执行（[`range_index_blocking`] 卸载）：本函数
  /// 返回时旧域条目仍已从 live_indexes 摘净（await 收口语义），仅执行线程
  /// 离核——async 任务内直调 detach_tree 的条带无界停车档会与同核持条带写锁
  /// 跨 await 的分层写臂互候永挂（票 wkv-flush-replay-detach-tree-async-stripe-park-core-deadlock）。
  ///
  /// 观察账顺带销账（换号回收唯一投递入口单点）：摘域后该域永不进
  /// [`Self::snapshot_bftree_domains`]，冷树回收轮的全部销账点对该域条目不可达，
  /// 条目须在此随身份键重建逐键摘除（key_id 与扫描轮同 [`key_id_of`] 口径），
  /// 否则换号复用下观察账单调泄漏——单点覆盖 FLUSHDB/FLUSHNS/SWAP 全族摘域路径。
  ///
  /// [`key_id_of`]: wbftree::RangeIndexManager::key_id_of
  pub(crate) async fn reclaim_bftree_keys(
    self: &Arc<Self>,
    vns: u64,
    vdb: u64,
    keys: Vec<Box<[u8]>>,
  ) {
    if keys.is_empty() {
      return;
    }
    let store = Arc::clone(self);
    // detach 摘注册与引擎 dispose 整段卸载阻塞线程（[`range_index_blocking`]，
    // spawn_blocking 停车不触本核 reactor，与 drain.rs 分层排空臂既有正确形
    // 同款）：detach_tree 内直取树条带无界 `locks.write` 停车档（wbftree
    // lifecycle.rs），async 任务内直调与同核持条带写锁跨 await 的分层写臂互候
    // 永挂（票 wkv-flush-replay-detach-tree-async-stripe-park-core-deadlock）。
    // 卸载任务自身异常退出（进程级故障）仅告警：旧域树滞留由启动对账
    // [`Self::reclaim_dead_domain_bftrees`] 兜底，换号语义已在 commit 落地
    if let Err(e) = range_index_blocking(move || {
      store.reclaim_bftree_keys_blocking(vns, vdb, keys);
    })
    .await
    {
      log::warn!("换号树回收卸载任务异常退出，旧域树留待启动对账: vns={vns}, vdb={vdb}, err={e:?}");
    }
  }

  /// 换号回收同步内核（[`Self::reclaim_bftree_keys`] 的阻塞线程承接体）：逻辑
  /// 与原同步实现逐行同源，仅执行线程移至 spawn_blocking 池
  fn reclaim_bftree_keys_blocking(self: &Arc<Self>, vns: u64, vdb: u64, keys: Vec<Box<[u8]>>) {
    // 逐键重建树身份键单趟双用：观察账 key_id 取样与 detach 摘注册同源，杜绝
    // 二次重建漂移；观察账摘除独立成段单锁进出，绝不与条带锁嵌套持有（同 gc::cold_tree 纪律）
    let mut key_ids = Vec::with_capacity(keys.len());
    let mut detached_batch = Vec::with_capacity(keys.len());
    for key in &keys {
      let id_key = tree_identity_key(vns, vdb, key);
      key_ids.push(RangeIndexManager::key_id_of(&id_key));
      if let Some(detached) = self.range_index.detach_tree(&id_key, true) {
        detached_batch.push(detached);
      }
    }
    {
      let mut observed = self.cold_bftree_observed.lock();
      for key_id in key_ids {
        observed.remove(&key_id);
      }
    }
    if detached_batch.is_empty() {
      return;
    }
    // 安全纪元删除期限入队即定（同批一次取样，逐键重复取样无增益），与
    // 死亡账本墓碑同一换算单点：期限内消费侧只暂存，宁滞留不提前
    let expired_at = reclaim_expired_at(now_ticks(), self.config.gc.db_gc_reclaim_delay_secs);
    self
      .bftree_release
      .lock()
      .extend(detached_batch.into_iter().map(|mut detached| {
        // 引擎即时归还：队列只承载 unlink，页环绝不随期限滞留
        detached.dispose_engine();
        PendingTreeRelease {
          expired_at,
          tree: detached,
        }
      }));
  }

  /// 排空待释放队列：甄别已过安全纪元期限的条目逐条经 release_detached 挂入
  /// 纪元延迟清理（生产由 [`crate::gc::spawn_bftree_reclaimer`] 常驻后台任务
  /// 消费；无运行时形态的引擎与测试经本入口手动驱动，消费内核仅此一处）。
  /// 换号臂条目引擎已就地 dispose（tree=None），本排空只落 unlink；引擎在场
  /// 的条目（冷树臂直入 release_detached 的同型形态）由同一内核照常承接
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
  /// 注册的条目并同步物理销毁孤儿树数据文件，同名重建索引不再被拦截。
  /// detach 循环整段卸载阻塞线程（[`Self::reclaim_bftree_keys`] 同款离核形，
  /// 调用面 [`crate::store::cpr_host`] recover 为异步任务内直调），纪元延迟
  /// 释放挂入既有收割队列（生产由 GC 常驻轮次承接，绝不在本核 unlink）。
  /// 卸载闭包只捕获 manager 句柄（[`Arc`] 克隆）：recover 入口返回裸
  /// `Self`（尚未入 `Arc`），接收者取 `&self` 即可闭环，无需逃逸语义改造
  pub(crate) async fn reclaim_dead_domain_bftrees(&self) {
    let dead_domains = {
      let mut domains = self.bftree_domains.lock();
      let mut dead = Vec::new();
      domains.retain(|&(vns, vdb), keys| {
        if self.vdb.is_dead_domain(vns, vdb) {
          dead.push((vns, vdb, take(keys)));
          false
        } else {
          true
        }
      });
      dead
    };
    if dead_domains.is_empty() {
      return;
    }
    let mgr = Arc::clone(&self.range_index);
    if let Err(e) = range_index_blocking(move || {
      for (vns, vdb, keys) in dead_domains {
        for key in keys {
          let id_key = tree_identity_key(vns, vdb, &key);
          if let Some(detached) = mgr.detach_tree(&id_key, true) {
            mgr.release_detached(detached);
          }
        }
      }
    })
    .await
    {
      log::warn!("启动对账树销毁卸载任务异常退出: err={e:?}");
    }
  }
}
