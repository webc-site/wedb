//! 冷树缓存回收域（页驱逐联动释放冷树内存——C# OnEvict 对位的 rust 承接形态）
//!
//! 在 garnet 中的相对路径: libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnEvict
//! （RangeIndexRecordType 分支调 DisposeTreeUnderLock(key, valueSpan, deleteFiles: false)，
//! 见 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock）
//!
//! C# 主日志页关闭时逐记录同步触发 OnEvict，冷 RI 键的树页环随页驱逐即时释放、
//! 访问时 RestoreTree 懒重开；本仓页驱逐链（whlog 页池回绕 / 写路径背压）无记录级
//! 钩子面（既定改良，票 zcode-r20-review-evict 双侧亲验），故以常驻轮询驱动补位：
//! 宿主挂 wkv 唯一常驻回收轮 [`spawn_bftree_reclaimer`]（恒开、RELEASE_POLL_MS 节拍，
//! 换号树延迟释放队列的同轮消费者），每轮扫换号回收旁表登记键（全部 BfTree 注册面
//! 统一落表，登记即在册充要面，与后台降阶轮同源发现机制），判据
//! 「注册表在册 ∧ Meta 存根最新地址已越 head 入磁盘区 ∧ 冷态持续越迟滞窗口」即经
//! 唯一释放内核 [`RangeIndexManager::dispose_tree_under_lock`]（delete_file=false
//! 懒恢复收口臂：条带写锁内摘注册 + CPR 离线收口快照 + 纪元延迟 dispose，文件保留）
//! 释放常驻页环；冷键再读经既有 [`RangeIndexManager::get_or_open_tree`] 懒重开，
//! 零新机制。pending 条目（tree=None）同扫同摘（C# DisposeTreeUnderLock 同臂移除），
//! 下轮重访问重注册。
//!
//! 迟滞窗口（C# 无此参数——C# 逐页同步驱逐天然滞后于访问；本仓轮询驱动需显式
//! 补位）：存根页逐出后须保持冷态 [`COLD_TREE_HYSTERESIS_TICKS`] 方可回收，防热键
//! 在「写随元记录落尾 → 页回绕逐出 → 下轮即摘」节奏里反复释放-重开抖动；回收
//! 非正确性事件（数据在盘、懒重开无损），误抖只付重开 I/O 成本。
//!
//! [`spawn_bftree_reclaimer`]: crate::gc::spawn_bftree_reclaimer
//! [`RangeIndexManager::dispose_tree_under_lock`]: wbftree::RangeIndexManager::dispose_tree_under_lock
//! [`RangeIndexManager::get_or_open_tree`]: wbftree::RangeIndexManager::get_or_open_tree

use parking_lot::Mutex;
use wbase::{
  addr::is_read_cache,
  map::HashMap,
  time::{TICKS_PER_SECOND, now_ticks},
};
use wbftree::RangeIndexManager;
use wdev::Device;
use wval::TaggedKeyBuf;

use super::reclaim::RELEASE_BATCH;
use crate::{range_index::tree_identity_key, store::WedbStore};

/// 冷态迟滞窗口（.NET Ticks，100ns 域，与 [`now_ticks`] 同时钟）：Meta 存根记录
/// 越出内存窗后须持续冷满该时长方可回收树页环。取 1 秒 = 常驻轮 200ms 节拍下
/// 连续 5 轮冷观察，抖动抑制与自愈时效折中（预算耗尽显式报错的既定行为不变，
/// 拒绝臂另经同步回收当轮解闸，见 [`WedbStore::recycle_cold_bftrees`] 调用点）
const COLD_TREE_HYSTERESIS_TICKS: i64 = TICKS_PER_SECOND as i64;

/// 冷树回收观察账：key_id（树身份 128 位哈希，与 live_indexes 同标识域）→
/// 首轮冷观察 ticks。与旁表同形态的冷路径互斥容器；只在回收核内单锁使用，
/// 绝不与条带锁嵌套持有
pub(crate) type ColdBftreeObserved = Mutex<HashMap<u128, i64>>;

impl<D: Device> WedbStore<D> {
  /// 冷树回收单轮（常驻轮与 CacheBudgetExhausted 拒绝臂共用的唯一内核）：
  /// 扫换号回收旁表登记键，按「在册 ∧ 存根越 head ∧ 冷满迟滞窗」摘除冷树，
  /// 返回本轮实际摘除的在线树数；单轮摘除至多 [`RELEASE_BATCH`] 棵（同待释放
  /// 排空的限批纪律，detach 含 CPR 收口快照重 I/O，单次唤醒不长期占线程）。
  ///
  /// 全程同步零 await（compio thread-per-core 纪律）：常驻轮循环体与
  /// `range_index_blocking`（spawn_blocking 线程池）两宿主直接调用，不触异步
  /// 反应器；摘除内核自身持条带写锁串行化同键生命周期，与在途读写者（条带
  /// 读锁 + 写者纪元）互斥安全，纪元延迟 dispose 由同轮
  /// [`WedbStore::drain_bftree_release`] / 收割点承接。
  ///
  /// 判据探针与治愈快路径 [`heal_stub_in_place`] 同源（哈希索引单槽
  /// [`find_tag`] 探针）：RC 链头不可判、Tag 碰撞误探均为性能侧保守偏差
  /// （迟滞窗 + 懒重开兜底，回收非正确性事件）；索引扩容迁移窗探针结果按
  /// 陈旧轮次处理，误摘仅付重开成本，绝不误删数据（delete_file=false 文件保留）。
  ///
  /// [`heal_stub_in_place`]: crate::range_index::heal::heal_stub_in_place
  /// [`find_tag`]: windex::HashIndex::find_tag
  pub fn recycle_cold_bftrees(&self) -> usize {
    let now = now_ticks();
    // 段一：观察裁决（只在观察账锁内完成，摘除的重 I/O 绝不持该锁）
    let mut due: Vec<(u128, TaggedKeyBuf)> = Vec::new();
    let domains = self.snapshot_bftree_domains();
    {
      let mut observed = self.cold_bftree_observed.lock();
      'outer: for (vns, vdb, keys) in domains {
        // 死亡域守卫（旁表消费面同规：换号-回收间隙的残留登记绝不再动其树）
        if self.vdb.is_dead_domain(vns, vdb) {
          continue;
        }
        for key in &keys {
          let id_key = tree_identity_key(vns, vdb, key);
          let key_id = RangeIndexManager::key_id_of(&id_key);
          // 注册表无条目即不占树缓存面：观察账随之销账
          if !self.range_index.is_registered(&id_key) {
            observed.remove(&key_id);
            continue;
          }
          // Meta 存根最新地址：无记录（并发删除）销账；RC 链头不可判 →
          // 保持观察原样，下轮再看
          let Some(addr) = self.index.load().find_tag(&id_key) else {
            observed.remove(&key_id);
            continue;
          };
          if is_read_cache(addr) {
            continue;
          }
          // 存根仍在内存窗 = 热键，冷观察清零重新起表
          if !self.hlog.is_on_disk(addr) {
            observed.remove(&key_id);
            continue;
          }
          // 迟滞窗判定：首轮冷观察登记，冷满方可入围
          let first = *observed.entry(key_id).or_insert(now);
          if now.saturating_sub(first) < COLD_TREE_HYSTERESIS_TICKS {
            continue;
          }
          observed.remove(&key_id);
          due.push((key_id, id_key));
          if due.len() >= RELEASE_BATCH {
            break 'outer;
          }
        }
      }
    }
    // 段二：摘除（唯一释放内核，delete_file=false 懒恢复收口臂；Ok(false) =
    // 裁决与摘除间隙条目已消亡或 pending 条目被同臂移除，均零副作用）
    let mut detached = 0usize;
    for (_key_id, id_key) in &due {
      match self.range_index.dispose_tree_under_lock(id_key, false) {
        Ok(true) => detached += 1,
        Ok(false) => {}
        Err(e) => log::warn!("冷树回收摘除失败，留待下轮: err={e:?}"),
      }
    }
    if detached > 0 {
      log::debug!("冷树回收轮摘除 {detached} 棵，释放常驻页环预算");
    }
    detached
  }
}
