//! 会话级被监视键容器（对标 libs/server/Transaction/TxnWatchedKeysContainer.cs: WatchedKeysContainer）
//!
//! 持有被监视键副本，版本取自 [`super::watch_version_map::WatchVersionMap`]。

use std::sync::Arc;

use smallvec::SmallVec;

use super::{txn_key_entry_comparison::TxnKeyEntryComparison, watch_version_map::WatchVersionMap};

/// 单个被监视键的快照
#[derive(Debug, Clone)]
struct WatchedKeySlice {
  /// 键字节副本
  key: Box<[u8]>,
  /// 版本轨键哈希（登记期逻辑域种子派生，仅供版本核验；锁面另按现域现算）
  hash: u64,
  /// 监视时刻版本
  version: u64,
  /// 是否仍被监视
  is_watched: bool,
}

/// 每会话的被监视键容器
pub struct TxnWatchedKeysContainer {
  /// 被监视键数组（内联 4 槽位，覆盖绝大多数 WATCH 事务，消除堆分配）
  key_slices: SmallVec<[WatchedKeySlice; 4]>,
  /// 版本表
  version_map: Arc<WatchVersionMap>,
}

impl TxnWatchedKeysContainer {
  /// 构造容器
  pub fn new(version_map: Arc<WatchVersionMap>) -> Self {
    Self {
      key_slices: SmallVec::new(),
      version_map,
    }
  }

  /// 重置被监视键（EXEC / DISCARD / UNWATCH 收尾）
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnWatchedKeysContainer.cs:Reset
  pub fn reset(&mut self) {
    self.key_slices.clear();
  }

  /// 追加被监视键并记录当前版本（重复监视幂等去重）
  ///
  /// 版本表分槽经 [`TxnKeyEntryComparison::scoped_key_hash`] 单点构造，
  /// **版本轨=逻辑域种子**：`prefix` 为会话逻辑前缀（`[NsVarintLogic]
  /// [DbVarintLogic]`，wkv `StoreSession::session_logical_prefix` 投影，不含
  /// FLUSHDB/FLUSHNS/SWAPDB 换号虚拟代际）。C# 每库独持版本表
  /// （libs/server/GarnetDatabase.cs:156 实例终身持有）的「库身份由 map 实例
  /// 承载」语义在 rust 共享单表形态下由逻辑域种子承载：换号只换逻辑→物理
  /// 路由、不改逻辑身份，换号前后同逻辑键 bump 与核验恒落同槽，改后写必
  /// abort；跨租户/跨库正交性保持（逻辑 ns/db 仍入种子）。锁轨另置——
  /// EXEC 锁集并入按当前**物理**前缀现算（[`Self::save_lock_hashes`]），
  /// 两轨分置两单点，禁共口互染。
  ///
  /// 查重对标 Redis src/multi.c:watchForKey：遍历容器命中同 hash 且同键
  /// 内容的切片即早退，严禁刷新其 version（刷新会抹去首次监视至重复监视
  /// 期间的写推进，击穿乐观锁隔离），同时跳过 read_version 原子读与
  /// key.into() 堆分配；同键在容器内至多占一槽，常规 WATCH 事务保持
  /// 4 槽内联容量，杜绝重复 WATCH 打爆内联并放大 TxnKeyEntries 锁集。
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnWatchedKeysContainer.cs:AddWatch
  pub fn add_watch(&mut self, prefix: &[u8], key: &[u8]) {
    let hash = TxnKeyEntryComparison::scoped_key_hash(prefix, key) as u64;
    if self
      .key_slices
      .iter()
      .any(|slice| slice.hash == hash && slice.key.as_ref() == key)
    {
      return;
    }
    let version = self.version_map.read_version(hash);
    self.key_slices.push(WatchedKeySlice {
      key: key.into(),
      hash,
      version,
      is_watched: true,
    });
  }

  /// 校验全部被监视键版本未变（记录未被修改）
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnWatchedKeysContainer.cs:ValidateWatchVersion
  pub fn validate_watch_version(&self) -> bool {
    self
      .key_slices
      .iter()
      .filter(|slice| slice.is_watched)
      .all(|slice| self.version_map.read_version(slice.hash) == slice.version)
  }

  /// 仍被监视键的事务锁哈希现算序列（供管理器并入锁集）
  ///
  /// **锁轨=物理域种子**：`prefix` 为 EXEC 执行时刻的会话物理前缀
  /// （`StoreSession::session_prefix` 投影，含换号代际），逐键经
  /// [`TxnKeyEntryComparison::scoped_key_hash`] 对裸键体现算——对位 C#
  /// SaveKeysToLock→GetKeyHash 的运行期取值形态（TxnWatchedKeysContainer.cs:
  /// SaveKeysToLock 每键现算本库锁表哈希），替代消费登记期冻结 hash：
  /// 数据与桶闩恒在现域，WATCH 后换号、EXEC 并入的锁集随现域落位，杜绝
  /// 死锁旧代桶残面。版本轨核验另用登记期逻辑种子 hash（[`Self::add_watch`]），
  /// 两轨分置两单点、禁共口互染。
  ///
  /// 同根待裁面登记（不扩面）：邻缝票 task/ing/wtxn-wkv-keybucket-hash-scope-desync.md
  /// 裁「锁轨会合域」（wkv 窗/TTL 裸 fast_hash 升 scoped），与本票「版本轨种子
  /// 代际」危害面正交不并案；该票方案 1 落地「前缀真值源单点封装」时锁轨入参
  /// 必须显式钉死为物理前缀接驳本处与 `TxnKeyManager::save_key_entry_to_lock`，
  /// 禁与版本轨逻辑单点共口互染（否则锁轨随换代假斥复发）——消费次序甄别归
  /// 该票执行席，本票零扩面。
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnWatchedKeysContainer.cs:SaveKeysToLock
  pub fn save_lock_hashes<'a>(&'a self, prefix: &'a [u8]) -> impl Iterator<Item = i64> + 'a {
    self
      .key_slices
      .iter()
      .filter(|slice| slice.is_watched)
      .map(move |slice| TxnKeyEntryComparison::scoped_key_hash(prefix, slice.key.as_ref()))
  }

  /// 全部被监视键（含已移除监视位）的引用序列（供管理器登记进集群槽校验键列表）
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnWatchedKeysContainer.cs:SaveKeysToKeyList
  pub fn save_keys_to_key_list(&self) -> impl Iterator<Item = &[u8]> {
    self.key_slices.iter().map(|slice| slice.key.as_ref())
  }
}
