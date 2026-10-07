//! 成员过期账本（Hash / SortedSet 共用单点）：过期字典 + 最小堆 + 记账口径一处定义
//!
//! C# 对位自身即双份：libs/server/Objects/Hash/HashObject.cs 与
//! libs/server/Objects/SortedSet/SortedSetObject.cs 的 InitializeExpirationStructures /
//! UpdateExpirationSize / CleanupExpirationStructuresIfEmpty / DeleteExpiredItems(Worker) /
//! SetExpiration 过期段 / TryRemoveExpiration(Worker) 逐行同构，rust 收敛为本单件，
//! hash/zset 以 `floor`（主容器常驻基线）与成员剔除闭包参数化消费，无 dyn 分发。
//!
//! 记账口径沿用 [`wbase::heap`]：字典项两槽、堆项再两槽、结构整体基线
//! [`EXPIRY_STRUCT_BASE`]。`heap_memory_size` 本体留在宿主（主容器条目同账
//! 一本），本账本方法经 `heap: &mut i64` 直改；`floor` 形参为宿主主容器基线
//! （hash 单容器 `CONTAINER_BASE` / zset 双容器），透支 debug_assert 按宿主口径裁决。

use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

use wbase::{
  heap::{EXPIRY_STRUCT_BASE, SLOT},
  map::HashMap,
};
use wresp::options::ExpireOption;
use wval::expiry_elapsed;

use super::expiration_queue::{ExpirationQueue, ExpirationQueueEntry};
use crate::object_payload::NO_EXPIRY_WATERMARK;

/// 信封线格式存活挂期条目集（hash/zset wire 的 expirations 段同型）
pub(crate) type WireExpirations = Vec<(Vec<u8>, i64)>;

/// 字典项记账：times 条目两槽
const TIME_ENTRY_SLOTS: i64 = SLOT * 2;
/// 堆项记账：queue 条目两槽
const QUEUE_ENTRY_SLOTS: i64 = SLOT * 2;
/// 满额记账：字典项 + 堆项（一成员挂账两结构同开）
const FULL_ENTRY_SLOTS: i64 = TIME_ENTRY_SLOTS + QUEUE_ENTRY_SLOTS;

/// 成员过期账本：member → 过期 ticks 字典 + 到期最小堆，同生命周期惰性初始化
///
/// libs/server/Objects/Hash/HashObject.cs:expirationTimes/expirationQueue 与
/// libs/server/Objects/SortedSet/SortedSetObject.cs 同名字段共用本单点
///
/// 键为共享句柄 [`Arc`]（对位 C# expirationTimes/expirationQueue 与主容器共享同一
/// byte[] 引用）：times 与 queue 只增引用计数，不复制成员字节，字节驻留一份由
/// 宿主主容器 update_size 计账（对位 C# UpdateExpirationSize 只计槽位）
#[derive(Debug, Clone, Default)]
pub(crate) struct ExpiryLedger {
  /// member → 过期 ticks（惰性初始化）
  pub times: Option<HashMap<Arc<[u8]>, i64>>,
  /// 过期最小堆（与 times 同生命周期）
  pub queue: Option<ExpirationQueue>,
}

impl ExpiryLedger {
  /// 惰性创建过期字典 + 最小堆并计结构基线
  ///
  /// libs/server/Objects/Hash/HashObject.cs:InitializeExpirationStructures
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:InitializeExpirationStructures
  /// （同名方法同体，双份收敛于此）
  pub fn initialize(&mut self, heap: &mut i64) {
    if self.times.is_none() {
      self.times = Some(HashMap::default());
      self.queue = Some(BinaryHeap::new());
      *heap += EXPIRY_STRUCT_BASE;
    }
  }

  /// 挂成员过期条目：字典 + 最小堆 + 满额记账（字典两槽 + 堆两槽），结构未建先建
  ///
  /// Hash/SortedSet 两类 expirationTimes.Add + expirationQueue.Enqueue 的共享实现体
  /// （C# 锚由两侧宿主门面单点持有，本函数只承接实现，不重复挂锚）；
  /// 满额记账臂承接
  /// libs/server/Objects/Hash/HashObject.cs:UpdateExpirationSize
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:UpdateExpirationSize
  /// （add: true 语义内联；线格式装载与分层物化还原共用此单点）
  pub fn insert(&mut self, heap: &mut i64, key: Arc<[u8]>, expiration: i64) {
    self.initialize(heap);
    if let Some(times) = self.times.as_mut() {
      // 共享句柄：字典与堆只增引用计数，不复制成员字节
      times.insert(key.clone(), expiration);
    }
    if let Some(queue) = self.queue.as_mut() {
      queue.push(Reverse(ExpirationQueueEntry { expiration, key }));
    }
    *heap += FULL_ENTRY_SLOTS;
  }

  /// 摘字典项并退两槽（不含结构回收）
  ///
  /// 对应 C# Remove 过期段：PQ 无法定位移除，仅清字典项、退字典项两槽，
  /// 残余堆项交下一次堆序清扫（SortedSetObject.cs:TryRemoveExpirationWorker 前半同段）
  fn remove_time(&mut self, heap: &mut i64, floor: i64, key: &[u8]) -> bool {
    let removed = self.times.as_mut().is_some_and(|t| t.remove(key).is_some());
    if removed {
      *heap -= TIME_ENTRY_SLOTS;
      debug_assert!(*heap >= floor);
    }
    removed
  }

  /// 摘字典项 + 全空整体回收（TryRemoveExpirationWorker 完整语义）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:TryRemoveExpirationWorker
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:TryRemoveExpiration（薄壳直转 Worker）
  /// （HashObject.cs:Persist 的内联同段收敛于此）
  pub fn remove_expiration(&mut self, heap: &mut i64, floor: i64, key: &[u8]) -> bool {
    if !self.remove_time(heap, floor, key) {
      return false;
    }
    self.cleanup_if_empty(heap, floor);
    true
  }

  /// 过期结构全空则整体回收：退残余堆项两槽/项 + 结构基线，字典堆同拆
  ///
  /// libs/server/Objects/Hash/HashObject.cs:CleanupExpirationStructuresIfEmpty
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:CleanupExpirationStructuresIfEmpty
  /// （同名方法同体，双份收敛于此）
  pub fn cleanup_if_empty(&mut self, heap: &mut i64, floor: i64) {
    let Some(times) = self.times.as_ref() else {
      return;
    };
    if !times.is_empty() {
      return;
    }

    if let Some(queue) = self.queue.as_ref() {
      *heap -= QUEUE_ENTRY_SLOTS * queue.len() as i64;
    }
    *heap -= EXPIRY_STRUCT_BASE;
    // C#: CleanupExpirationStructuresIfEmpty 后 HeapMemorySize 回到主容器基线
    debug_assert!(*heap >= floor);
    self.times = None;
    self.queue = None;
  }

  /// 堆序摘到期条目（C# DeleteExpiredItems 与 DeleteExpiredItemsWorker 两口一体）：
  /// 堆顶先过期先出；字典堆失步（续期重复入堆）时以字典为真值、仅弹陈旧堆项。
  /// 命中臂退四槽后交宿主闭包剔除成员（`on_evict(key, heap)` 直改主容器与账本），
  /// 尾部全空整体回收
  ///
  /// Hash/SortedSet 两类 DeleteExpiredItemsWorker 的共享实现体（同名方法同体，
  /// 双份收敛于此；C# 锚由两侧宿主门面单点持有，本函数不重复挂锚）
  pub fn pop_expired(
    &mut self,
    heap: &mut i64,
    now: i64,
    floor: i64,
    mut on_evict: impl FnMut(&[u8], &mut i64),
  ) {
    // The PQ is ordered such that oldest items are dequeued first
    while let Some(queue) = self.queue.as_mut() {
      let Some(Reverse(head)) = queue.peek() else {
        break;
      };
      if head.expiration >= now {
        break;
      }

      let key = head.key.clone();
      let expiration = head.expiration;

      // expirationTimes 与 expirationQueue 失步（续期重复入堆）时以字典为准
      let in_times = self
        .times
        .as_ref()
        .and_then(|t| t.get(&key))
        .is_some_and(|&actual| actual == expiration);

      if in_times {
        self.times.as_mut().unwrap().remove(&key);
        queue.pop();
        *heap -= FULL_ENTRY_SLOTS;
        debug_assert!(*heap >= floor);
        on_evict(&key[..], heap);
      } else {
        // The key was not in expirationTimes. It may have been Remove()d.
        queue.pop();

        // Adjust memory size for the priority queue entry removal.
        *heap -= QUEUE_ENTRY_SLOTS;
      }
    }

    self.cleanup_if_empty(heap, floor);
  }

  /// 设置成员过期的过期段（条件闸门 + 槽位写入/新插 + 堆推 + 记账），false = 条件不满足
  ///
  /// Hash/SortedSet 两类 SetExpiration 的过期段共享实现体（同段同体，双份收敛于此；
  /// C# 锚由两侧宿主门面单点持有，本函数不重复挂锚）。成员存在性判定与
  /// KeyAlreadyExpired 移除臂为宿主语义：C# Hash 侧用过滤已过期的 ContainsKey、
  /// SortedSet 侧用裸 sortedSetDict.ContainsKey，差异 1:1 保留
  ///
  /// 与 C# 的刻意差异（宿主侧声明同源）：C# 以 GetValueRefOrAddDefault 插入
  /// 0 值幻影项、被拒字段自此在各方眼中失活且记账缺失，属 C# 缺陷；此处只读
  /// 探测现值（get_time），拒绝臂零副作用，禁止为「对齐 C#」复刻幻影项
  ///
  /// key 由宿主传主容器内的共享句柄：账本不另行分配，成员字节全容器单份
  /// （C# SetExpiration 将调用方 buffer 数组直接存账，字节同单份）
  pub fn set_expiration(
    &mut self,
    heap: &mut i64,
    key: Arc<[u8]>,
    expiration: i64,
    expire_option: ExpireOption,
  ) -> bool {
    self.initialize(heap);

    let current = self.get_time(&key);

    // 条件闸门：既有过期按 NX/GT/LT 判定；无过期按 XX/GT（C# 分支语义）
    let denied = match current {
      Some(current) => {
        expire_option.contains(ExpireOption::NX)
          || (expire_option.contains(ExpireOption::GT) && expiration <= current)
          || (expire_option.contains(ExpireOption::LT) && expiration >= current)
      }
      None => expire_option.contains(ExpireOption::XX) || expire_option.contains(ExpireOption::GT),
    };
    if denied {
      return false;
    }

    if current.is_some() {
      if let Some(slot) = self.times.as_mut().unwrap().get_mut(key.as_ref()) {
        *slot = expiration;
      }
      // 字典项槽位已计，仅补堆项两槽
      *heap += QUEUE_ENTRY_SLOTS;
      self
        .queue
        .as_mut()
        .unwrap()
        .push(Reverse(ExpirationQueueEntry { expiration, key }));
    } else {
      // 新键臂与 insert() 逐句同构（字典插 + 堆推 + 满额记账），转调单点
      self.insert(heap, key, expiration);
    }

    true
  }

  /// 查询成员过期 ticks（C#: expirationTimes.TryGetValue）
  #[inline]
  pub fn get_time(&self, key: &[u8]) -> Option<i64> {
    self.times.as_ref().and_then(|t| t.get(key)).copied()
  }

  /// 成员在给定时刻是否已过期
  ///
  /// Hash/SortedSet 两类 IsExpired 的共享实现体（同名方法同体，双份收敛于此；
  /// C# 锚由两侧宿主门面单点持有，本函数不重复挂锚）
  #[inline]
  pub fn is_expired_at(&self, key: &[u8], now: i64) -> bool {
    expiry_elapsed(self.get_time(key), now)
  }

  /// 是否已建过期结构
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:HasExpirableItems
  #[inline]
  pub fn has_items(&self) -> bool {
    self.times.is_some()
  }

  /// 信封序列化水位段单点（hash/zset serialize_wire 的「active 过期收集 +
  /// 水位取 min」段共用）：收集存活挂期条目（`exp >= now` 且宿主主容器仍持有
  /// 该成员）并取最早到期刻度；无存活挂期回 `(None, NO_EXPIRY_WATERMARK)`
  /// 永快道。写时已剔除到期成员，水位内不可能再有成员到期，读侧
  /// `now <= 水位` 期间头部计数恒精确
  pub fn collect_active(
    &self,
    now: i64,
    alive: impl Fn(&[u8]) -> bool,
  ) -> (Option<WireExpirations>, i64) {
    let mut watermark = NO_EXPIRY_WATERMARK;
    let Some(times) = &self.times else {
      return (None, watermark);
    };
    let mut active: Vec<(Vec<u8>, i64)> = Vec::with_capacity(times.len());
    for (member, exp) in times {
      if *exp >= now && alive(member) {
        active.push((member.to_vec(), *exp));
        watermark = watermark.min(*exp);
      }
    }
    ((!active.is_empty()).then_some(active), watermark)
  }

  /// 线格式装载存活挂账臂单点（hash/zset deserialize_from_slice 共用）：
  /// 账本无刻度才挂账（expirations 判重守卫——重复条目二次满额入账而字典项
  /// 仅覆盖，净虚标两槽且堆内滞留陈旧项，跳过保持首条刻度），回是否实际挂账
  pub fn insert_if_unrecorded(&mut self, heap: &mut i64, key: Arc<[u8]>, expiration: i64) -> bool {
    if self.get_time(key.as_ref()).is_some() {
      return false;
    }
    self.insert(heap, key, expiration);
    true
  }
}
