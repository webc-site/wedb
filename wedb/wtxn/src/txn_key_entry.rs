//! 事务键条目与加锁集合（对标 libs/server/Transaction/TxnKeyEntry.cs）
//!
//! 排序后按归并计划取锁——同主桶条目合并为最强锁型，持锁记录为桶下标序列，
//! 解锁为其逆序对偶；锁源为构造期注入的引擎实例锁表句柄，其桶闩即 windex 哈希桶
//! 内嵌闩（对标 C# `TxnKeyEntries(int, TransactionalContext)` 取会话所属 store 的锁表，
//! 持锁集合对标 C# ActiveLocks 持 HashBucketRef，rust 以钉定的 `Arc<HashIndex>` + 桶下标承接）。

use std::{sync::Arc, time::Duration};

use coarsetime::Instant;
use smallvec::SmallVec;
use wbase::backoff::Backoff;
use windex::HashIndex;

use super::{txn_key_entry_comparison::TxnKeyEntryComparison, txn_lock_table::TxnLockTable};

/// libs/server/Transaction/TxnKeyEntry.cs:LockType
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LockType {
  Exclusive = 1,
  Shared = 2,
}

/// Entry for a key to lock and unlock in transactions
///
/// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntry.cs:TxnKeyEntry
#[derive(Clone, Copy)]
pub struct TxnKeyEntry {
  pub key_hash: i64,
  pub routing_hash: i64,
  pub lock_type: LockType,
}

impl TxnKeyEntry {
  pub fn new(key_hash: i64, routing_hash: i64, lock_type: LockType) -> Self {
    Self {
      key_hash,
      routing_hash,
      lock_type,
    }
  }
}

/// 归并后的加锁计划项，同时是持锁期间的桶记录（纯数据，持锁状态由 windex 桶内嵌闩承载）
#[derive(Clone, Copy)]
struct LockPlanSlot {
  bucket: usize,
  exclusive: bool,
}

/// 增量重展开的单桶取闩动作（对标 C# `LockAllKeys` 一次成型形态在 rust 增量
/// 轨的分解：纯新增桶直取 / 旧代仅持 Shared 而新代要求 Exclusive 的桶先释后升）
#[derive(Clone, Copy)]
enum IncrementalStep {
  /// 纯新增桶：按归并后的强度直接 try 取闩
  Acquire { slot: LockPlanSlot },
  /// 升闩桶：先释本会话旧 Shared 闩，再取 Exclusive（严禁原地升排他——
  /// `HashBucket` 内嵌闩非重入，自家 Shared 未释时排他取闩必假）。
  /// `held_pos` 为步 2 二分时钉定的在座槽下标，成功臂据此原位落真，
  /// 绝不重二分——步骤 4 的 push 会令 `held` 暂时无序，重二分在垃圾尾上失准
  Upgrade { bucket: usize, held_pos: usize },
}

/// 事务键加锁集合（libs/server/Transaction/TxnKeyEntry.cs:TxnKeyEntries）
pub struct TxnKeyEntries {
  /// 所属引擎实例的锁表句柄（对标 C# 条目集持 store 事务上下文）
  lock_table: TxnLockTable,
  /// 待加锁键序列（内联 8 槽位，覆盖绝大多数常规事务，消除堆分配）
  keys: SmallVec<[TxnKeyEntry; 8]>,
  unified_store_key_locked: bool,
  /// 已持有桶记录（内联 4 槽位，覆盖绝大多数事务，消除堆分配）
  held: SmallVec<[LockPlanSlot; 4]>,
  /// 本笔事务钉定的索引版本（取锁与放锁共用同一 `HashIndex`，跨 resize 不串锁、不漏闩）
  latch: Option<Arc<HashIndex>>,
  /// 缓存归并计划（首轮构建后复用：异步臂每轮单次尝试不得重排/重建键集，
  /// 对标 C# `TransactionalContext.Lock` 外层 while 仅重试 `DoTransactionalLock`、
  /// 排序在循环外一次性完成）
  plan: Option<SmallVec<[LockPlanSlot; 4]>>,
}

impl TxnKeyEntries {
  /// 构造加锁集合（对标 C# `TxnKeyEntries(int initialCount, TransactionalContext)`：
  /// 锁表面由所属引擎实例的锁表句柄注入）
  pub fn new(initial_count: usize, lock_table: TxnLockTable) -> Self {
    Self {
      lock_table,
      // n 不超过内联容量时 SmallVec 本就不分配（with_capacity 内建该语义）
      keys: SmallVec::with_capacity(initial_count),
      unified_store_key_locked: false,
      held: SmallVec::new(),
      latch: None,
      plan: None,
    }
  }

  /// 所属引擎实例的锁表句柄
  #[inline]
  pub fn lock_table(&self) -> &TxnLockTable {
    &self.lock_table
  }

  /// 是否全为读锁（无排他条目）
  pub fn is_read_only(&self) -> bool {
    !self.keys.iter().any(|k| k.lock_type == LockType::Exclusive)
  }

  pub fn count(&self) -> usize {
    self.keys.len()
  }

  /// 待加锁键条目锁哈希迭代器
  ///
  /// 锁轨专用，用于锁表桶定位。
  #[inline]
  pub fn key_hashes(&self) -> impl Iterator<Item = i64> + '_ {
    self.keys.iter().map(|k| k.key_hash)
  }

  /// 待加锁键条目裸路由哈希迭代器
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntry.cs:GetKeyHash
  ///
  /// 对标 C# `keys[index].keyHash` 的 AOF 子日志路由语义，直读裸键哈希
  /// （等同于 GarnetLog.HASH），供 `compute_sublog_access_vector` 计算子日志访问向量。
  #[inline]
  pub fn routing_hashes(&self) -> impl Iterator<Item = i64> + '_ {
    self.keys.iter().map(|k| k.routing_hash)
  }

  /// 追加待锁键与路由哈希
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntry.cs:AddKey
  pub fn add_key(&mut self, key_hash: i64, routing_hash: i64, lock_type: LockType) {
    self.keys.push(TxnKeyEntry {
      key_hash,
      routing_hash,
      lock_type,
    });
  }

  /// 归并加锁计划：按主桶下标与键哈希全序升序排序，并在单次线性扫描中合并同桶锁位取最强锁型
  ///
  /// `index` 为本笔事务钉定的索引版本（与 [`Self::acquire_plan`] 取锁同源，桶下标即 `hash & size_mask`）。
  fn lock_plan(&mut self, index: &HashIndex) -> SmallVec<[LockPlanSlot; 4]> {
    if self.keys.is_empty() {
      return SmallVec::new();
    }
    self
      .keys
      .sort_unstable_by(|a, b| TxnKeyEntryComparison::compare(index, a, b));

    let mut plan: SmallVec<[LockPlanSlot; 4]> = SmallVec::with_capacity(self.keys.len());
    for entry in &self.keys {
      let bucket = index.bucket_index_for_hash(entry.key_hash as u64);
      let exclusive = entry.lock_type == LockType::Exclusive;
      if let Some(last) = plan.last_mut()
        && last.bucket == bucket
      {
        last.exclusive |= exclusive;
        continue;
      }
      plan.push(LockPlanSlot { bucket, exclusive });
    }
    plan
  }

  /// 逆序释放已登记的桶闩并清空持锁记录
  /// （对标 C# TransactionalContext.cs:DoTransactionalUnlock 的
  /// 「Unlock has to be done in the reverse order of locking」反向遍历）
  fn release_held(&mut self) {
    let Some(index) = self.latch.clone() else {
      self.held.clear();
      return;
    };
    for slot in self.held.drain(..).rev() {
      if slot.exclusive {
        index.get_bucket(slot.bucket).unlock_exclusive();
      } else {
        index.get_bucket(slot.bucket).unlock_shared();
      }
    }
  }

  /// 单轮取闩（对标 C# TransactionalContext.cs:DoTransactionalLock 的键集主体：
  /// 按升序计划逐桶 try 取闩，任一桶失败即逆序回滚本轮已取前缀并返回 false）
  fn acquire_plan(&mut self, plan: &[LockPlanSlot]) -> bool {
    self.held.clear();
    let index = self
      .latch
      .clone()
      .expect("取锁前必已钉定本笔事务的索引版本");
    for slot in plan {
      let bucket = index.get_bucket(slot.bucket);
      let taken = if slot.exclusive {
        bucket.try_lock_exclusive()
      } else {
        bucket.try_lock_shared()
      };
      if taken {
        self.held.push(*slot);
        continue;
      }
      // C# 失败分支：DoTransactionalUnlock(keys[..keyIdx]) 后整计划重试
      self.release_held();
      return false;
    }
    true
  }

  /// 首轮准备：钉定索引版本并构建、缓存归并计划（幂等，仅首调用构建一次）。
  ///
  /// 计划与钉定索引一经构建即缓存复用——异步臂每轮单次尝试共享同一键集/同一索引版本，
  /// 不得重排重算（对标 C# `Lock` 的排序在重试循环外一次完成）。
  fn ensure_plan(&mut self) {
    if self.plan.is_some() {
      return;
    }
    let index = self.lock_table.pin();
    let plan = self.lock_plan(&index);
    if !plan.is_empty() {
      self.latch = Some(index);
    }
    self.plan = Some(plan);
  }

  /// 单次取闩尝试（无自旋）：整计划失败已由 [`Self::acquire_plan`] 逆序回滚，
  /// 本方法不持任何闩、且保留键集与缓存计划与钉定索引。
  ///
  /// 唯一取闩内核，供三条驱动共用：线程态 [`Self::lock_all_keys`] /
  /// [`Self::try_lock_all_keys`] 的抢占式自旋臂，以及 compio 态事务侧
  /// `TransactionManager::run_exec` 的「单次尝试 + 执行器 yield」异步臂
  /// （每轮让步后复调本方法，直至成功）。成功置锁标记返回 `true`；
  /// 空键集视作已达成返回 `true`（对标 C# 空 keys 直接返回）。
  pub fn try_lock_all_keys_once(&mut self) -> bool {
    self.ensure_plan();
    // 取出缓存计划交取闩内核借用，轮末原样归还：零克隆、跨轮不重排不重建
    let plan = self.plan.take();
    let acquired = match &plan {
      Some(slots) if !slots.is_empty() => self.acquire_plan(slots),
      // 空键集视作已达成（对标 C# 空 keys 直接返回）
      _ => true,
    };
    self.plan = plan;
    if acquired && matches!(&self.plan, Some(slots) if !slots.is_empty()) {
      self.unified_store_key_locked = true;
    }
    // 争用时 acquire_plan 已回滚本轮前缀，键集/计划/钉定索引原样保留待重试。
    acquired
  }

  /// 阻塞加锁全部键（libs/server/Transaction/TxnKeyEntry.cs:LockAllKeys）
  ///
  /// 对标 C# TransactionalContext.cs:Lock 的 `while (!lockAcquired)` 外层——
  /// 抢占式线程池语义：单次尝试失败经 [`Backoff`] 三阶退避（自旋→让核→50µs 微睡）
  /// 等待重试。等待体必须是退避而非纯 `thread::yield_now`：套件级 CPU 超订阅下
  /// （nextest 多测试二进制并行），8 竞争者互相 `sched_yield` 踩踏成活锁风暴——
  /// 败者立刻重入重撞，胜者无窗收尾；微睡臂把竞争者移出就绪队列，还给持闩者
  /// 真实的解锁窗口（等待内核对标 C# AllocatorBase.cs:WaitToRetryNow）。
  /// ⚠️ 仅供真线程上下文（内部事务 / 单元测试）使用；compio 单核 worker 上的
  /// 外部 EXEC 走 `TransactionManager::run_exec` + 会话异步臂，绝不在此自旋。
  pub fn lock_all_keys(&mut self) {
    let mut backoff = Backoff::new();
    while !self.try_lock_all_keys_once() {
      backoff.snooze();
    }
  }

  /// 限时尝试加锁全部键（libs/server/Transaction/TxnKeyEntry.cs:TryLockAllKeys）
  ///
  /// 对标 C# TransactionalContext.cs:TryLock(keys, timeout)：单次尝试失败后在
  /// 超时预算内经 [`Backoff`] 三阶退避重试（理由同 [`Self::lock_all_keys`]）；
  /// `lock_timeout` 为零表单次尝试（快速失败路径）。
  /// 同为线程上下文原语，compio 态由 `run_exec` 异步臂承接，不在此自旋。
  pub fn try_lock_all_keys(&mut self, lock_timeout: Duration) -> bool {
    let start = Instant::now();
    let mut backoff = Backoff::new();
    loop {
      if self.try_lock_all_keys_once() {
        return true;
      }
      if lock_timeout.is_zero() || Duration::from(start.elapsed()) >= lock_timeout {
        // 超时放弃：清空钉定与缓存计划（本笔锁操作终结，不留半持态）
        self.latch = None;
        self.plan = None;
        self.unified_store_key_locked = false;
        return false;
      }
      backoff.snooze();
    }
  }

  /// 解锁全部键（libs/server/Transaction/TxnKeyEntry.cs:UnlockAllKeys）
  ///
  /// 释放已持桶闩（逆序）、放钉定索引并清空锁集；未持锁时为空操作（幂等，供 Drop 复用）。
  pub fn unlock_all_keys(&mut self) {
    if self.unified_store_key_locked {
      self.release_held();
    }
    self.held.clear();
    self.keys.clear();
    self.latch = None;
    self.plan = None;
    self.unified_store_key_locked = false;
  }

  /// 本命令键窗的主桶是否全落本事务已持闩桶域（EXEC 重放段脚本重入的锁器
  /// 模式选型判据，消费点在 wnode garnet_api::exec；rust 自研判据点，C# 内嵌
  /// processor 恒 Basic ephemeral 无让闩判据，登记 doc/zh/deviations.md §139）
  ///
  /// 键哈希经 [`TxnKeyEntryComparison::scoped_key_hash`] 全仓唯一构造口现算
  /// （与 [`TransactionManager::save_key_entry_to_lock`](crate::transaction_manager::TransactionManager::save_key_entry_to_lock)
  /// 的落键登记同源同种子域），桶下标按本笔事务钉定的索引版本
  /// （[`TxnLockTable::pin`] 同源 `latch`）经 `bucket_index_for_hash` 定位，
  /// 与 [`Self::lock_plan`] 的持锁桶定位同一算法；`held` 为升序去重桶序列，
  /// 二分即可判定。判据按桶 + 按强度双判：桶命中 **且** 该桶持槽强度 ≥
  /// 要求强度（`require_exclusive` 为真时持槽须为 Exclusive）才算覆盖——
  /// 与 [`Self::lock_plan`] / 增量臂归并步「同桶取最强」的强度维度口径对齐；
  /// 写命令落仅持 Shared 的桶时回 false，交调用方走 Basic ephemeral 自取
  /// 排他闩（doc/zh/deviations.md §139 已登记收口形态，不新造第三态）。
  ///
  /// 空键集恒真（无键命令不触桶闩）；未持闩（`latch` 未钉定）时对任何非空
  /// 键集回 false——无从证明覆盖，保守交调用方自取闩。
  pub fn covers_user_keys<'k, I: IntoIterator<Item = &'k [u8]>>(
    &self,
    prefix: &[u8],
    keys: I,
    require_exclusive: bool,
  ) -> bool {
    let mut keys = keys.into_iter().peekable();
    if keys.peek().is_none() {
      return true;
    }
    let Some(index) = self.latch.as_deref() else {
      return false;
    };
    keys.all(|key| {
      let bucket =
        index.bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(prefix, key) as u64);
      match self.held.binary_search_by(|slot| slot.bucket.cmp(&bucket)) {
        // 桶命中后核持槽强度：要求排他而该桶仅持共享即不覆盖
        Ok(pos) => !require_exclusive || self.held[pos].exclusive,
        Err(_) => false,
      }
    })
  }

  /// 查询指定桶在持锁记录中的强度（测试断言口：Some(true) = 排他，Some(false) = 共享，None = 未持）
  #[inline]
  pub fn held_bucket_exclusive(&self, bucket: usize) -> Option<bool> {
    self
      .held
      .binary_search_by(|slot| slot.bucket.cmp(&bucket))
      .ok()
      .map(|idx| self.held[idx].exclusive)
  }

  /// 对新增条目按「桶 + 锁型强度」双判增量 try 闩（旧代已持且强度足够的桶不
  /// 重复取；旧代仅持 Shared 而本代要求 Exclusive 的桶先释旧 Shared 再升取
  /// Exclusive），任一步争用即整批对称回滚（已升桶原样回补旧 Shared）并回 false。
  ///
  /// EXEC 重放窗换代单机制：新条目归并后，纯新增桶增量取闩、强度不足已持桶
  /// 升闩；成功时将新增/升闩槽并入 `held` 保持升序（升闩槽 `exclusive` 按
  /// 新强度落真），并将条目并入 `keys`。
  pub fn try_lock_incremental_entries(&mut self, new_entries: &[TxnKeyEntry]) -> bool {
    if new_entries.is_empty() {
      return true;
    }
    if self.latch.is_none() {
      self.latch = Some(self.lock_table.pin());
    }
    let index = self
      .latch
      .clone()
      .expect("取锁前必已钉定本笔事务的索引版本");

    // 1. 归并新条目的加锁计划（按 bucket 升序并合并同桶锁型）
    let mut plan: SmallVec<[LockPlanSlot; 4]> = SmallVec::with_capacity(new_entries.len());
    for entry in new_entries {
      let bucket = index.bucket_index_for_hash(entry.key_hash as u64);
      let exclusive = entry.lock_type == LockType::Exclusive;
      plan.push(LockPlanSlot { bucket, exclusive });
    }
    plan.sort_unstable_by_key(|slot| slot.bucket);

    let mut merged: SmallVec<[LockPlanSlot; 4]> = SmallVec::with_capacity(plan.len());
    for slot in plan {
      if let Some(last) = merged.last_mut()
        && last.bucket == slot.bucket
      {
        last.exclusive |= slot.exclusive;
        continue;
      }
      merged.push(slot);
    }

    // 2. 按桶 + 按强度双判筛选动作序列：不在 `held` 的桶进纯新增取闩动作；桶已持但
    //    本代要求排他而持槽仅共享的桶进升闩动作（先释本会话旧 Shared 再取
    //    Exclusive，严禁原地升排他）；已持排他或仅要求共享的命中桶跳过。
    //    因 `merged` 按桶严格升序，按序生成的 `steps` 自然保持桶升序
    let mut steps: SmallVec<[IncrementalStep; 4]> = SmallVec::with_capacity(merged.len());
    for slot in merged {
      match self.held.binary_search_by(|h| h.bucket.cmp(&slot.bucket)) {
        Ok(held_pos) => {
          if slot.exclusive && !self.held[held_pos].exclusive {
            steps.push(IncrementalStep::Upgrade {
              bucket: slot.bucket,
              held_pos,
            });
          }
        }
        Err(_) => steps.push(IncrementalStep::Acquire { slot }),
      }
    }

    if steps.is_empty() {
      self.keys.extend_from_slice(new_entries);
      self.plan = None;
      return true;
    }

    // 3. 逐桶增量 try 闩（非阻塞全程无等待，不构成死锁序；升序遍历与失败臂
    //    「逆序对称回滚」保持对偶同构，与全量臂 acquire_plan 的取放闩序口径一致）。
    //    任一桶失败即整批对称回滚并返回 false——本程已取新增桶逆序放闩、
    //    已升槽位降回并原样回补旧 Shared；回补亦失（该桶恰被他连接抢占）的桶
    //    从 `held` 如实除名，宁可让重驱慢臂重来，绝不留 `held` 记着有闩而桶上
    //    无闩（或反之）的半持态。
    let mut lost_buckets: SmallVec<[usize; 4]> = SmallVec::new();
    for (failed_idx, &step) in steps.iter().enumerate() {
      let taken = match step {
        IncrementalStep::Acquire { slot } => {
          let bucket = index.get_bucket(slot.bucket);
          if slot.exclusive {
            bucket.try_lock_exclusive()
          } else {
            bucket.try_lock_shared()
          }
        }
        IncrementalStep::Upgrade { bucket, .. } => {
          // 先释本会话旧 Shared（自家必成），再取 Exclusive
          index.get_bucket(bucket).unlock_shared();
          index.get_bucket(bucket).try_lock_exclusive()
        }
      };
      if taken {
        continue;
      }

      // 失败步为升闩时旧 Shared 已释出：先原样回补；补不回则该桶除名
      if let IncrementalStep::Upgrade { bucket, .. } = step
        && !index.get_bucket(bucket).try_lock_shared()
      {
        lost_buckets.push(bucket);
      }
      // 逆序对称回滚本程已完成动作（steps[..failed_idx] 即已成功的全部前驱动作）
      for s in steps[..failed_idx].iter().rev() {
        match *s {
          IncrementalStep::Acquire { slot } => {
            if slot.exclusive {
              index.get_bucket(slot.bucket).unlock_exclusive();
            } else {
              index.get_bucket(slot.bucket).unlock_shared();
            }
          }
          IncrementalStep::Upgrade { bucket, .. } => {
            // 已升槽位降回：释自家 Exclusive → 原样回补旧 Shared
            index.get_bucket(bucket).unlock_exclusive();
            if !index.get_bucket(bucket).try_lock_shared() {
              lost_buckets.push(bucket);
            }
          }
        }
      }
      for bucket in lost_buckets {
        if let Ok(pos) = self.held.binary_search_by(|h| h.bucket.cmp(&bucket)) {
          self.held.remove(pos);
        }
      }
      return false;
    }

    // 4. 全部动作成功获取：升闩槽按步 2 钉定的在座下标原位落真（exclusive
    //    置真）——此处 held 已被本程 Acquire push 暂时打乱，重二分会在垃圾
    //    尾上失准漏升（升闩漏记致 release 阶段 unlock_shared 断言/幽灵读者）；
    //    新增槽并入后整体按桶重排保持升序
    for step in &steps {
      match *step {
        IncrementalStep::Acquire { slot } => self.held.push(slot),
        IncrementalStep::Upgrade { held_pos, .. } => {
          self.held[held_pos].exclusive = true;
        }
      }
    }
    self.held.sort_unstable_by_key(|slot| slot.bucket);
    self.keys.extend_from_slice(new_entries);
    self.plan = None;
    self.unified_store_key_locked = true;
    true
  }
}

/// RAII 解绑：条目集析构即放闩（对标 C# UnlockAllKeys 的 finally 语义，
/// panic 展开亦不泄漏闩字）
impl Drop for TxnKeyEntries {
  #[inline]
  fn drop(&mut self) {
    self.unlock_all_keys();
  }
}
