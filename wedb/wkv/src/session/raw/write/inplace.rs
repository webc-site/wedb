//! 原位更新与 CAS 校验（对标 C# Garnet InPlaceUpdater / InPlaceDeleter / Record Elision 快慢路径）

use std::result::Result as StdResult;

use wbase::{addr::is_read_cache, backoff::Backoff, simd::fast_key_eq};
use wdev::Device;
use windex::Error as WindexError;
use wrecord::{ValSrc, record_size};

use super::{super::DEGRADE_ASYNC, CopyToTailOutcome};
use crate::{
  error::{Error, Result},
  read_cache::RcVisit,
  session::{INNER_LATCH_RETRY_BUDGET, StoreSession},
};

/// 重试预算门（upsert / insert / delete 三处主环共用，语义与文档见
/// [`INNER_LATCH_RETRY_BUDGET`]）：闩失败 / CAS 竞争 / RC 槽位恢复等待共用同一
/// `Backoff` 计数，`step_count` 比较零成本；耗尽即上抛可重试的 LockTimeout 交调用
/// 方退窗应答，杜绝外层持闩窗口（RmwWindow / ttl.rs KeyLatch）内的无界同步自旋
#[inline(always)]
fn retry_budget_gate(backoff: &Backoff) -> Result<()> {
  if backoff.step_count() >= INNER_LATCH_RETRY_BUDGET {
    return Err(Error::Index(WindexError::LockTimeout));
  }
  Ok(())
}

/// 优化5（严格对标 C# Helpers.FindOrCreate/FindTagAndTryEphemeralXLock +
/// HashEntryInfo.SetToCurrent）：复活配置开启时对槽位所在链首桶加 ephemeral 独占锁，
/// 防止复活/脱钩在回溯期间把记录从链上抽走（"Ephemeral must lock the bucket before
/// traceback"）；加锁失败按 C# RETRY_LATER 协议刷新纪元并让出一次预算，`$retry` 传入
/// 对应循环的 `continue` / `continue 'retry` 整链重试（预算内有界，耗尽即 LockTimeout
/// 上抛退窗）。复活池关闭时脱钩虽恒常态发生，但脱钩槽位即经 try_seal_record 密封且不
/// 被原地复用，无锁回溯至多 CAS 落败整链重试（安全面由 epoch 保护与 CAS 定序承接），
/// 维持现状免锁（判据详见 [`StoreSession::ephemeral_lock_enabled`] 注释）。
///
/// 锁成功后经 SetToCurrent 重读当前槽位，杜绝定位 Tag 与加锁之间被并发 CAS/脱钩改写
/// 的陈旧地址。RAII 守卫与 `hei` 借用次序、成功/失败分支的取放顺序均与逐处展开完全
/// 一致，`$hei` 恒为局部 `mut hei` 标识符、各方法依次收发于其上无二次求值。
macro_rules! ephemeral_x_latch {
  ($slf:expr, $hei:expr, $backoff:expr, $retry:expr) => {
    let _x_latch = if $slf.ephemeral_lock_enabled() {
      match $hei.lock_exclusive_guard() {
        Some(latch) => Some(latch),
        None => {
          $slf.participant.refresh();
          $backoff.snooze();
          $retry
        }
      }
    } else {
      None
    };
    $hei.set_to_current();
  };
}

/// CAS 失败分配的会话内暂存守卫（对标 C# BlockAllocate.SaveAllocationForRetry 的
/// operationState.retryNewLogicalAddress）：下一轮尝试先按 GetAllocationForRetry 口径
/// 复用判定，命中直接复用该槽位免一次尾部分配；任何提前退出路径 Drop 时自动落
/// 复活池（对标 GetAllocationForRetry Fail 分支的 OnDispose），杜绝槽位泄漏
struct RetryAlloc<'a, D: Device> {
  session: &'a StoreSession<D>,
  alloc: Option<(u64, u32)>,
}

impl<'a, D: Device> RetryAlloc<'a, D> {
  #[inline(always)]
  const fn new(session: &'a StoreSession<D>) -> Self {
    Self {
      session,
      alloc: None,
    }
  }

  /// 本次申请槽位下界（严格对标 C# BlockAllocate.cs:59-62 的 minRevivAddress 链首抬升）：
  /// - 链首脱钩且无前驱（整链清空 `elideSourceRecord && hei.Address == recSrc.LogicalAddress`）：
  ///   链上再无存活记录，回退全局复活水位，低水位槽位照常可复活；
  /// - 否则经 [`StoreSession::reviv_chain_floor`] 单点折算（链首 + 1 与全局水位取大），
  ///   严格保证新槽位地址 > 旧链首，prev_address 恒单调递减，杜绝哈希碰撞链逆向成环
  ///   （A -> B -> A 读路径死循环）
  #[inline(always)]
  fn chain_floor(&self, hei_addr: u64, chain_detached: bool) -> u64 {
    if chain_detached {
      self.session.store.min_revivifiable_address()
    } else {
      self.session.reviv_chain_floor(hei_addr)
    }
  }

  /// 复用判定失败的弃置（仅复活池开启时回收，否则遗弃——与既有 CAS 失败补偿口径一致）
  #[inline(always)]
  fn discard(&self, addr: u64, size: u32) {
    if self.session.store.config.enable_revivification {
      // 归池统一单点：先原子密封再入池（对标 C# SaveAllocationForRetry 落笔即
      // SetInvalid 使槽位恒处 Closed 态，Helpers.TryTransferToFreeList 前置断言
      // IsClosed）：弃置帧已写入完整记录，未密封则回溯中的无锁读者无法感知
      // Closed，槽位一经复活方原位覆写即读出新旧混合的撕裂内容
      self.session.store.transfer_to_reviv_pool(addr, size);
    }
  }

  /// 尝试复用暂存槽位或分配新记录（对标 C# BlockAllocate.GetAllocationForRetry）
  ///
  /// `min_eligible_addr` 为本次申请统一槽位下界（对标 C# 抬升后的 minRevivAddress，
  /// BlockAllocate.cs:59-62）：暂存复用与池取/尾追加共用同一下界，杜绝首轮与重试
  /// 两臂口径分裂致逆向链接。暂存槽低于下界仅弃置归池（可复活窗内他链仍可取用），
  /// 绝不就地清退。分配臂恒不发写监听（镜像点收口，票 zcode-r34-writekernel
  /// 条目三，对标 C# PostInitialWriter 提交后置 NeedAofLog 的单一「生效后镜像」
  /// 次序）：AOF 镜像由提交方在索引 CAS 成功后恰发一次，CAS 败帧的暂存复用/
  /// 弃置重试均不产生镜像条目
  ///
  /// 返回三元组的 ver 为复用落笔/分配成功点单读传导的 AOF 版本戳（读点下移，
  /// 见 [`whlog::HybridLog::append`] 方法文档），与记录头纪元位同源同点
  #[inline(always)]
  fn allocate_or_reuse<V: ValSrc + ?Sized>(
    &mut self,
    key: &[u8],
    val: &V,
    prev_addr: u64,
    is_tombstone: bool,
    min_head_addr: u64,
    min_eligible_addr: u64,
  ) -> Result<StdResult<(u64, u32, i64), u64>> {
    let rec_size = record_size(key.len(), val.val_len()) as u32;
    if let Some((addr, size)) = self.alloc.take() {
      if addr >= min_eligible_addr && addr >= min_head_addr && size >= rec_size {
        // 复用落笔点紧贴单读（读点下移）：纪元位与 AOF 版本戳同源同点
        let word = self.session.store.hlog.version_shift_word();
        // 复用判定命中即就地复活覆写；失败落下方弃置归池
        if let Ok(pad) = self
          .session
          .store
          .hlog
          .revivify_record_at(&whlog::RevivifyArgs {
            addr,
            slot_size: size as usize,
            key,
            val,
            prev_addr,
            is_tombstone,
            in_new_version: word & whlog::VERSION_SHIFT_OPEN_BIT != 0,
          })
        {
          // 切出的剩余块同形归池（对标 C# TryTransferToFreeList），
          // 杜绝复活槽位内的孤儿死内存
          if let Some((pad_addr, pad_size)) = pad {
            self.discard(pad_addr, pad_size);
          }
          return Ok(Ok((addr, size, (word & whlog::VERSION_MASK) as i64)));
        }
      }
      self.discard(addr, size);
    }
    self.session.try_allocate_or_append_record_sync(
      key,
      val,
      prev_addr,
      is_tombstone,
      min_eligible_addr,
    )
  }
}

impl<D: Device> Drop for RetryAlloc<'_, D> {
  fn drop(&mut self) {
    if let Some((addr, size)) = self.alloc.take() {
      self.discard(addr, size);
    }
  }
}

/// Upsert/Delete 内存探针分类结果（消除 5 元组装包解包与无谓的尺寸计算）
#[derive(Debug, Clone, Copy)]
enum MemoryRecordProbe {
  /// 密封在途记录
  Closed,
  /// 墓碑记录
  Tombstone { prev: u64, size: usize },
  /// 有效存活记录
  Active {
    prev: u64,
    size: usize,
    val_len: usize,
  },
  /// 键不匹配（Tag 碰撞），携前驱版本逻辑地址继续回溯
  Miss(u64),
}

impl MemoryRecordProbe {
  #[inline(always)]
  fn probe(rec: &wrecord::RecordRef<'_>, key: &[u8]) -> Self {
    if rec.matches_key(key) {
      if rec.is_closed() {
        Self::Closed
      } else if rec.is_tombstone() {
        Self::Tombstone {
          prev: rec.prev_address(),
          size: rec.physical_size(),
        }
      } else {
        Self::Active {
          prev: rec.prev_address(),
          size: rec.physical_size(),
          val_len: rec.header.val_len() as usize,
        }
      }
    } else {
      Self::Miss(rec.prev_address())
    }
  }
}

impl<D: Device> StoreSession<D> {
  /// 底层物理纯同步快速路径写入（Raw，对齐 Garnet InternalUpsert / NetworkSET 执行链路）
  #[inline(always)]
  pub fn try_upsert_raw_sync(&self, key: &[u8], val: &[u8]) -> Result<StdResult<u64, u64>> {
    let _guard = self.enter_gated();
    self.try_upsert_raw_sync_unprotected(key, val)
  }

  /// 底层物理纯同步快速路径写入内核（调用方须已处于纪元保护下，批处理上下文专用）
  ///
  /// - 纯内存执行：原位更新、链内原地复活、复活池分配、内存尾部原子追加；
  /// - 100% 避免异步状态机调度与协程让出；
  /// - 仅当遭遇环形缓冲区溢出需跨页落盘（PageNotReady）时返回 Ok(Err(page_id))，指示调用者降级至异步落盘；
  /// - 重试预算（[`INNER_LATCH_RETRY_BUDGET`]）耗尽时返回 Err(LockTimeout) 上抛
  ///   退窗（可重试错误，调用方不在持闩窗口内消化）。
  #[inline(always)]
  pub(in crate::session) fn try_upsert_raw_sync_unprotected(
    &self,
    key: &[u8],
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    self.try_upsert_raw_sync_unprotected_val(key, val)
  }

  /// [`Self::try_upsert_raw_sync_unprotected`] 的值源泛型内核（原位 / 链内复活 /
  /// 复活池 / 尾部追加四臂同源透传 [`ValSrc`]）：连续切片走既有路径，分段直写
  /// 源（对象信封 `[1B 对象标签][payload]` 单次成形）在记录槽位分配点一次落笔，
  /// 消除中间整值 `Vec` 暂存拷贝——对标 C# 序列化器直写记录 value span
  /// （garnet/libs/server/Objects/Types/GarnetObjectSerializer.cs:104）。分段源
  /// 无连续切片（`as_val_slice` 为 `None`），本内核不投 Write 镜像事件（信封
  /// 非墓碑写在 AOF sink 侧本就由专用 EnvelopeUpsert 单点承接，见
  /// service.rs Write 臂放行集），索引 CAS 成功后的镜像由调用方专用口恰发一次
  pub(in crate::session) fn try_upsert_raw_sync_unprotected_val<V: ValSrc + ?Sized>(
    &self,
    key: &[u8],
    val: &V,
  ) -> Result<StdResult<u64, u64>> {
    let hash = whasher::fast_hash(key);
    // 优化6：CAS 失败分配的会话内暂存（对标 C# BlockAllocate.SaveAllocationForRetry）；
    // 纪元位与 AOF 戳不再入口预采样——分配漏斗在分配成功点紧贴单读（读点下移，
    // 见 [`whlog::HybridLog::append`] 方法文档），原位臂通知在生效点后单读
    let mut retry_alloc = RetryAlloc::new(self);
    let mut backoff = Backoff::new();
    loop {
      // 重试预算门（闩失败 / CAS 竞争 / RC 槽位恢复等待共用同一 Backoff 计数，
      // step_count 比较零成本，预算语义见 [`INNER_LATCH_RETRY_BUDGET`]）：耗尽即
      // 上抛退窗交调用方按既有错误通道应答，杜绝外层 user_key 桶闩窗口
      // （RmwWindow / ttl.rs KeyLatch）持有期内的无界同步自旋与同核互阻
      retry_budget_gate(&backoff)?;
      self.ensure_split_by_hash(hash)?;
      let begin_addr = self.store.begin_address();
      let index = self.store.index.load();
      let mut hei = index.find_or_create_tag_by_hash_with_min_addr(hash, begin_addr)?;

      // 优化5：复活开启时回溯前对桶加 ephemeral 独占锁 + SetToCurrent（详见宏文档）
      ephemeral_x_latch!(self, hei, backoff, continue);

      // 优化3 elide 候选：命中记录即链首槽位时的 (源地址, 前驱地址, 物理尺寸)
      // （尺寸须在探针期随页读锁捕获，CAS 成功后源槽位随时可能被复活方改写）
      let mut elide_src = None;
      // 脱钩判定单点结果（对标 C# CreateNewRecordUpsert 的
      // allocOptions.elideSourceRecord，InternalUpsert.cs:315 一次判定）：仅在下方
      // 算前驱处按 CanElide 判据折出，前驱接管与 CAS 成功后的归池两处严格消费
      // 同一判定值，杜绝未真正脱钩的链上记录被误归复活池
      let mut elided: Option<(u64, u64, u32)> = None;
      let prev_addr = if !hei.is_found() {
        // 1. 全新 Key（或原截断槽位已被单趟探针 CAS 清退置零）：首项槽位为空，前驱逻辑地址为 0
        0
      } else {
        let addr = hei.address();

        if is_read_cache(addr) {
          // 2. ReadCache 槽位：只读内存缓存不可原位更新，顺链获取主日志真实地址后
          // 以其为前驱盲插；CAS 即原子脱钩 RC 前缀（对标 C# Helpers.CASRecordIntoChain），
          // 被取代的孤儿缓存记录交由 cleanse_page 在页关闭时回收，写侧不作废；
          // 断链（0）说明条目刚滑出窗口、驱逐方 cleanse 尚未恢复槽位，此时以 prev=0 盲插
          // 会截断碰撞键的主日志链（丢键），自旋重读等待恢复
          // None（走查触及滑窗/不可判读记录）折 0：并入既有自旋重读等待槽位恢复
          let real = self.store.read_cache.skip_read_cache(addr).unwrap_or(0);
          if real == 0 {
            backoff.snooze();
            continue;
          }
          real
        } else {
          let read_only_addr = self.store.hlog.read_only_address();

          // 3. 主日志内存可变区探针（严格对标 C# FindRecord.TryFindRecordForUpdate / TraceBackForKeyMatch，两符号均声明于 FindRecord.cs）：
          // 仅在 [read_only_addr, tail) 范围内以只读方式（零页读锁争用）沿 Tag 链回溯查找匹配键；
          // 仅当明确匹配到键时，才获取页写锁执行原位更新或原地复活；未匹配时沿链快速终止，绝不产生页写锁开销
          if addr >= read_only_addr {
            let mut cur = addr;
            while cur >= read_only_addr {
              let probe = self
                .store
                .hlog
                .with_memory_record(cur, |rec| Ok(MemoryRecordProbe::probe(&rec, key)));

              match probe {
                Ok(Some(MemoryRecordProbe::Active {
                  prev,
                  size,
                  val_len,
                })) => {
                  // 原位修改：新值不长于旧值时方可就地原位更新（等长覆写或短值填入松弛）；
                  // 若新值长于旧值（值增长）严格走 RCU 尾部追加（对标 C# InPlaceUpdater）
                  if val.val_len() <= val_len
                    && self.store.hlog.try_update_in_place(cur, key, val)?
                  {
                    if let Some(v) = val.as_val_slice() {
                      self.notify_write_listener(key, v, false)?;
                    }
                    return Ok(Ok(cur));
                  }
                  // 原位失败跳出尾部追加：命中记录为链首槽位时记录 elide 候选
                  // （严格对标 CreateNewRecordUpsert 的 elideSourceRecord 判定源）
                  if cur == addr {
                    elide_src = Some((cur, prev, size as u32));
                  }
                  break; // 长度变更或不可修改，跳出走尾部追加
                }
                Ok(Some(MemoryRecordProbe::Tombstone { prev, size })) => {
                  // 链内原地复活：墓碑记录命中即尝试就地复用该槽位（严格双检 expected_key）
                  // 双门与 C# 对位（InternalUpsert.cs:125 `RevivificationManager.IsEnabled
                  // && LogicalAddress >= GetMinRevivifiableAddress()`）：唯一启用谓词 +
                  // 复活窗口下限，缺一即落回尾部追加；下限公式经 store 单点推导，此处不重写。
                  // C# 该臂居于 CPR 闩之后（CheckCPRConsistencyUpsert 先行强制地板之下
                  // 目标 CreateNewRecord，InternalUpsert.cs:97-113），rust 对位门为第三重
                  // 且下沉在 whlog `try_revivify_in_chain` 内——版本推进窗口期开窗即拒
                  // （IsFrozen/IsInNewVersion 抑制臂同源谓词），本层两门不重复裁决
                  if self.store.reviv_pool.is_enabled()
                    && cur >= self.store.min_revivifiable_address()
                    && self.store.hlog.try_revivify_in_chain(cur, key, val)?
                  {
                    if let Some(v) = val.as_val_slice() {
                      self.notify_write_listener(key, v, false)?;
                    }
                    return Ok(Ok(cur));
                  }
                  if cur == addr {
                    elide_src = Some((cur, prev, size as u32));
                  }
                  break; // 复活失败，跳出走尾部追加
                }
                Ok(Some(MemoryRecordProbe::Closed)) => {
                  // 密封在途记录命中（严格对标 C# FindRecord.TryFindRecordForUpdate
                  // IsClosed → RETRY_LATER）：不可原位改写在途记录，跳出走尾部盲追加——
                  // 新版本盲插恒安全（并发胜者经索引 CAS 定序）；C# 此路径整操作重试
                  // 不建新记录，故不参与 elide
                  break;
                }
                Ok(Some(MemoryRecordProbe::Miss(prev))) => {
                  // 键不匹配（Tag 碰撞），继续沿 prev_address 反向回溯
                  cur = prev;
                }
                _ => break,
              }
            }
          }

          // 4. 零 I/O 盲插（Blind Tail Append）：
          // 若地址已进入只读区（addr < read_only_addr）或可变区未匹配/无法原位更新，
          // 彻底杜绝磁盘 I/O，前驱直接链接当前槽位地址；若并发推进导致 addr 低于截断边界则防御性置 0。
          // 优化3（对标 C# InternalUpsert.CreateNewRecordUpsert 的
          // allocOptions.elideSourceRecord = CanElide(...)，一次判定两处消费）：
          // 本处为全链唯一脱钩判定点（严格对标 C# Helpers.CanElide：命中记录即链首
          // 槽位 hei.Address == 源记录地址，且其前驱低于截断边界，链上仅此一条有效
          // 记录、无碰撞键可达性依赖），折出 elided 后前驱接管分支与 CAS 成功后的
          // seal+归池分支严格消费同一判定值——未折入判定的 elide_src 候选一律不得
          // 脱钩、密封或入池，否则仍挂在链上的旧记录被复活方覆写即致悬挂引用与丢键
          // 版本推进窗口期禁脱钩（对标 C# CanElide 复合 IsInNewVersion 抑制臂，
          // 与 whlog 原位复活同一窗口谓词）：脱钩会 seal+归池回滚锚点记录，
          // 恢复内核 undoNextVersion 便无处把槽位 CAS 回老版本。
          // 脱钩判定不含任何 reviv/elision 模式开关（严格对标 C# Helpers.CanElide
          // 三条件与 InternalUpsert.cs:315 `elideSourceRecord = HasMainLogSrc &&
          // CanElide(...)`——除 HasMainLogSrc 外无门；reviv 只影响回收侧
          // InternalUpsert.cs:371-372 的入池判定，即下方 CAS 成功分支的
          // config.enable_revivification 单点）：脱链与入池两事严格分离
          elided = if self.store.is_version_shift_open() {
            None
          } else {
            elide_src.filter(|(src, src_prev, _)| *src == addr && *src_prev < begin_addr)
          };
          if let Some((_, src_prev, _)) = elided {
            src_prev
          } else if addr < begin_addr {
            0
          } else {
            addr
          }
        }
      };

      // 5. 纯内存分配与原子 CAS 挂载
      // 链首地址下界（对标 C# BlockAllocate.cs:59-62）：非脱钩场景新槽位必须严格
      // 高于旧链首，杜绝逆向链接成环；整链清空（elided）时回退全局复活水位
      let min_eligible_addr = retry_alloc.chain_floor(hei.address(), elided.is_some());
      // 优化6（对标 C# BlockAllocate.GetAllocationForRetry）：优先复用上一轮 CAS
      // 失败保留的分配——地址不低于本次申请下界、未滑出内存且帧足印足够时，重写
      // 前驱指针（键值布局与上轮完全一致）即可整帧复用，免一次尾部分配
      let (new_addr, alloc_size, alloc_ver) = match retry_alloc.allocate_or_reuse(
        key,
        val,
        prev_addr,
        false,
        self.store.head_address(),
        min_eligible_addr,
      )? {
        Ok(ok) => ok,
        Err(page_id) => return Ok(Err(page_id)), // 环形缓冲区翻转，将精确 page_id 传回调用者
      };

      if hei.try_cas(new_addr) {
        // 索引提交成功后恰发一次 AOF 镜像（票 zcode-r34-writekernel 条目三，
        // 对标 C# PostInitialWriter 在记录提交后置 NeedAofLog 的单一「生效后
        // 镜像」次序）；CAS 败帧的暂存复用/弃置重试绝不重发。通知失败按全仓
        // AofEnqueue 契约以「已生效+镜像缺失」上抛拒绝。分段值源无连续切片，
        // 镜像由调用方专用通知口（信封 EnvelopeUpsert）承接，此处恰零次；戳取
        // 分配成功点传导值（读点下移，与记录头纪元位同源同点）
        if let Some(v) = val.as_val_slice() {
          self.notify_write_listener_with_version(key, v, false, alloc_ver)?;
        }
        // CAS 即原子脱钩被取代的 RC 前缀（若有），孤儿缓存记录交由 cleanse_page
        // 在页关闭时回收（对标 C# Helpers.CASRecordIntoChain），写侧不作废
        // 优化3（对标 CreateNewRecordUpsert CAS 成功后的 SealAndInvalidate +
        // TryTransferToFreeList，InternalUpsert.cs:366 `if (allocOptions.elideSourceRecord)`）：
        // 此处严格消费算前驱处折出的同一 elided 判定值——只有新记录确已接管其前驱、
        // 真正从链上脱钩的旧链首才被密封并转移至空闲池回收（低于复活下限则遗弃，
        // 对标 GetMinRevivifiableAddress 判定）；仍挂在链上（新记录前驱仍指向它）的
        // 旧记录一旦入池，复活方覆写即致其后继悬挂、碰撞键不可达（丢键）
        if let Some((src, _, src_size)) = elided {
          if self.store.config.enable_revivification {
            // CAS 成功后必先 SealAndInvalidate 旧链首再转移空闲池（对标 C#
            // InternalUpsert.cs:369 顺序：SealAndInvalidate → TryTransferToFreeList，
            // 后者前置断言 IsClosed）：在途无锁读者凭 SEALED 位触发 RETRY_LATER
            // 让步，杜绝槽位出池覆写后被读出撕裂内容
            self.store.transfer_to_reviv_pool(src, src_size);
          } else {
            // reviv 关闭时仍须 Seal 封闭已脱链记录（对标 C# InternalUpsert.cs:368
            // srcLogRecord.InfoRef.SealAndInvalidate()，在途读者凭 SEALED 触发重试）
            let _ = self.store.hlog.try_seal_record(src, true);
          }
        }
        return Ok(Ok(new_addr));
      }

      // 优化6（对标 C# BlockAllocate.SaveAllocationForRetry）：CAS 失败的分配连同
      // 真实帧足印暂存会话内，下一轮优先复用；替代原先直接落全局复活池（下一轮
      // 重新尾部分配）
      retry_alloc.alloc = Some((new_addr, alloc_size));
      backoff.snooze();
    }
  }

  /// 纯同步快速条件写入物理记录（NX 语义：仅当键不存在时原子写入，存在即原样保留）
  ///
  /// 返回：
  /// - `Ok(Ok(Some(addr)))`: 键不存在，新记录已成功 CAS 入链；
  /// - `Ok(Ok(None))`: 键已存在（内存探得 Active 记录），写入被拒；
  /// - `Ok(Err(page_id))`: 遭遇环形页翻转或磁盘候选，指示调用方降级慢路径；
  /// - `Err(e)`: 存储或内存分配失败；重试预算（[`INNER_LATCH_RETRY_BUDGET`]）
  ///   耗尽时为可重试的 LockTimeout，交调用方退窗应答
  pub(in crate::session) fn try_insert_raw_sync_unprotected(
    &self,
    key: &[u8],
    val: &[u8],
  ) -> Result<StdResult<Option<u64>, u64>> {
    let hash = whasher::fast_hash(key);
    // 同 upsert 臂：读点下移后无入口采样，分配漏斗紧贴单读、原位臂生效点读
    let mut retry_alloc = RetryAlloc::new(self);
    let mut backoff = Backoff::new();
    'retry: loop {
      // 重试预算门（同 upsert 主环，见 [`INNER_LATCH_RETRY_BUDGET`]）
      retry_budget_gate(&backoff)?;
      self.ensure_split_by_hash(hash)?;
      let begin_addr = self.store.begin_address();
      let index = self.store.index.load();
      let mut hei = index.find_or_create_tag_by_hash_with_min_addr(hash, begin_addr)?;

      // 优化5：同 upsert 主环的 ephemeral 独占锁 + SetToCurrent
      ephemeral_x_latch!(self, hei, backoff, continue);

      let prev_addr = if !hei.is_found() {
        0
      } else {
        let addr = hei.address();

        let (mut cur, prev_link) = if is_read_cache(addr) {
          let matched = matches!(
            self
              .store
              .read_cache
              .with_record(addr, |rec_key, _| { Some(fast_key_eq(rec_key, key)) }),
            RcVisit::Found(true)
          );
          if matched {
            return Ok(Ok(None));
          }
          let real = self.store.read_cache.skip_read_cache(addr).unwrap_or(0);
          if real == 0 {
            backoff.snooze();
            continue 'retry;
          }
          (real, if real < begin_addr { 0 } else { real })
        } else {
          (addr, if addr < begin_addr { 0 } else { addr })
        };

        if cur < begin_addr {
          0
        } else {
          let read_only_addr = self.store.hlog.read_only_address();
          if cur < read_only_addr {
            return Ok(Err(cur));
          }
          let mut found_active = false;
          let mut tombstone_slot = None;
          while cur >= read_only_addr {
            let probe = self
              .store
              .hlog
              .with_memory_record(cur, |rec| Ok(MemoryRecordProbe::probe(&rec, key)));

            match probe {
              Ok(Some(MemoryRecordProbe::Active { .. })) => {
                found_active = true;
                break;
              }
              Ok(Some(MemoryRecordProbe::Tombstone { prev, size })) => {
                if tombstone_slot.is_none() {
                  tombstone_slot = Some((cur, prev, size));
                }
                break;
              }
              Ok(Some(MemoryRecordProbe::Closed)) => {
                self.participant.refresh();
                backoff.snooze();
                continue 'retry;
              }
              Ok(Some(MemoryRecordProbe::Miss(prev))) => {
                cur = prev;
              }
              _ => break,
            }
          }
          if found_active {
            return Ok(Ok(None));
          }
          if let Some((cur_tomb, ..)) = tombstone_slot
            && self.store.reviv_pool.is_enabled()
            && cur_tomb >= self.store.min_revivifiable_address()
            && self.store.hlog.try_revivify_in_chain(cur_tomb, key, val)?
          {
            self.notify_write_listener(key, val, false)?;
            return Ok(Ok(Some(cur_tomb)));
          }
          if tombstone_slot.is_none() && cur >= begin_addr {
            return Ok(Err(cur));
          }
          prev_link
        }
      };

      let min_eligible_addr = retry_alloc.chain_floor(hei.address(), false);
      let (new_addr, alloc_size, alloc_ver) = match retry_alloc.allocate_or_reuse(
        key,
        val,
        prev_addr,
        false,
        self.store.head_address(),
        min_eligible_addr,
      )? {
        Ok(ok) => ok,
        Err(page_id) => return Ok(Err(page_id)),
      };

      if hei.try_cas(new_addr) {
        // 索引提交成功后恰发一次 AOF 镜像（与 upsert 臂同收口，条目三；戳取传导值）
        self.notify_write_listener_with_version(key, val, false, alloc_ver)?;
        return Ok(Ok(Some(new_addr)));
      }

      retry_alloc.alloc = Some((new_addr, alloc_size));
      backoff.snooze();
    }
  }

  /// 纯同步快速路径物理删除单个键（对齐 Garnet InternalDelete / NetworkDEL 执行链路）
  ///
  /// 语义与安全边界详见 [`Self::delete_or_take_raw_sync_unprotected`]
  #[inline(always)]
  pub fn try_delete_raw_sync(&self, key: &[u8]) -> Result<StdResult<bool, u64>> {
    let _guard = self.enter_gated();
    self.try_delete_raw_sync_unprotected(key)
  }

  /// 纯同步快速路径物理取删单个键（GETDEL 读删一体；[`Self::try_delete_raw_sync`]
  /// 的取值对位）
  ///
  /// 应答值 = 本次删除实际摘除记录的值：捕获与摘除同一临界区闭环（可变区页写锁内
  /// 同点捕获；不可变区记录捕获后值恒不变），杜绝「探针读旧值 → 并发盲写落新值 →
  /// 删除摘走新值」的答旧删新撕裂（C# 单记录 RMW 锁内 CopyRespTo + ExpireAndStop
  /// 的同原子性，RMWMethods.cs:764-768 + InternalRMW.cs:70）
  #[inline(always)]
  pub fn try_take_raw_sync(&self, key: &[u8]) -> Result<StdResult<Option<Vec<u8>>, u64>> {
    let _guard = self.enter_gated();
    self.try_take_raw_sync_unprotected(key)
  }

  #[inline(always)]
  pub fn try_take_raw_sync_with<R, F: FnOnce(&[u8]) -> R>(
    &self,
    key: &[u8],
    f: F,
  ) -> Result<StdResult<Option<R>, u64>> {
    let _guard = self.enter_gated();
    self.try_take_raw_sync_with_unprotected(key, f)
  }

  /// 纯同步快速路径物理删除内核（调用方须已处于纪元保护下，批处理上下文专用）
  #[inline]
  pub(in crate::session) fn try_delete_raw_sync_unprotected(
    &self,
    key: &[u8],
  ) -> Result<StdResult<bool, u64>> {
    self.delete_or_take_raw_sync_unprotected_with::<(), fn(&[u8])>(key, None)
  }

  /// 纯同步快速路径物理取删内核（[`Self::try_delete_raw_sync_unprotected`] 的
  /// 取值变体；`Ok(Ok(true))` 时携被摘记录值，`Ok(Ok(false))` 键缺席零摘除）
  #[inline]
  pub(in crate::session) fn try_take_raw_sync_unprotected(
    &self,
    key: &[u8],
  ) -> Result<StdResult<Option<Vec<u8>>, u64>> {
    self.try_take_raw_sync_with_unprotected(key, |v| v.to_vec())
  }

  /// 纯同步快速路径物理取删内核（闭包消费零分配变体）
  #[inline]
  pub(in crate::session) fn try_take_raw_sync_with_unprotected<R, F: FnOnce(&[u8]) -> R>(
    &self,
    key: &[u8],
    f: F,
  ) -> Result<StdResult<Option<R>, u64>> {
    let mut taken = None;
    match self.delete_or_take_raw_sync_unprotected_with(key, Some((&mut taken, Some(f))))? {
      Ok(true) => Ok(Ok(taken)),
      Ok(false) => Ok(Ok(None)),
      Err(page_id) => Ok(Err(page_id)),
    }
  }

  /// 删除/取删共同内核（`take` 为 `Some` 即取删模式：各摘除点同临界区捕获被摘
  /// 记录值；调用方须已处于纪元保护下，批处理上下文专用）
  ///
  /// - 纯内存单趟执行：若记录在可变区且无前驱，直接 CAS 脱钩（Record Elision）——
  ///   复活池开启时回收至 FreeRecordPool，关闭时仅清链 tidy（对标 C# HandleRecordElision
  ///   的 `!RevivificationManager.IsEnabled` 分支），零追加返回；
  /// - 若记录处于只读区、冷数据区或不可 elide，盲追加一条 0 字节墓碑记录（Blind Tombstone Append）并 CAS 挂载；
  /// - 100% 免除 Tokio 协程让出与 Future 构造；
  /// - 仅当追加墓碑遭遇环形缓冲区翻转（PageNotReady）时返回 Ok(Err(page_id))，交由外层异步驱逐；
  /// - 冷数据需磁盘确认时返回 Ok(Err(DEGRADE_ASYNC))，调用方降级全异步路径。
  /// - 重试预算（[`INNER_LATCH_RETRY_BUDGET`]）耗尽时返回 Err(LockTimeout) 上抛
  ///   退窗（可重试错误，调用方不在持闩窗口内消化）。
  ///
  /// 记录消解（脱钩 + 槽位回收/清退）协议对标
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:HandleRecordElision
  /// （上下两分支即其 IsEnabled 开关两臂，槽位去向单点承接；脱钩记录均经 try_seal_record 原子密封）。
  fn delete_or_take_raw_sync_unprotected_with<R, F: FnOnce(&[u8]) -> R>(
    &self,
    key: &[u8],
    mut take: Option<(&mut Option<R>, Option<F>)>,
  ) -> Result<StdResult<bool, u64>> {
    let hash = whasher::fast_hash(key);
    // 优化6：CAS 失败分配的会话内暂存（对标 C# BlockAllocate.SaveAllocationForRetry）；
    // 读点下移后无入口采样：盲墓碑追加臂的位/戳取自分配成功点单读，原位墓碑臂
    // 通知在生效点后单读
    let mut retry_alloc = RetryAlloc::new(self);
    let mut backoff = Backoff::new();
    'retry: loop {
      // 重试预算门（同 upsert 主环，见 [`INNER_LATCH_RETRY_BUDGET`]）
      retry_budget_gate(&backoff)?;
      self.ensure_split_by_hash(hash)?;
      let begin_addr = self.store.begin_address();
      let index = self.store.index.load();
      let Some(mut hei) = index.find_tag_entry_by_hash_with_min_addr(hash, begin_addr) else {
        return Ok(Ok(false));
      };

      // 优化5：同 upsert 主环的 ephemeral 独占锁 + SetToCurrent（锁失败 RETRY_LATER
      // 整链重试至 'retry 环首）
      ephemeral_x_latch!(self, hei, backoff, continue 'retry);

      let addr = hei.address();
      // ReadCache 链头分流（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalDelete.cs:InternalDelete.cs：TryFindRecordForUpdate 的
      // 键匹配链遍历 + HasReadCacheSrc → CreateNewRecord 盲插墓碑）
      let mut cur = addr;
      if is_read_cache(addr) {
        let matched = matches!(
          self
            .store
            .read_cache
            .with_record(addr, |rec_key, _rec_val| {
              if fast_key_eq(rec_key, key) {
                Some(true)
              } else {
                Some(false)
              }
            }),
          RcVisit::Found(true)
        );
        // None（走查触及滑窗/不可判读记录）折 0：并入既有自旋重读等待槽位恢复
        let real_prev = self.store.read_cache.skip_read_cache(addr).unwrap_or(0);
        if real_prev == 0 {
          // 链头条目刚滑出窗口（驱逐方 cleanse 尚未把槽位恢复为主日志地址）：
          // 槽位此刻承载的是 Tag 碰撞键的存活缓存条目，elide 会令碰撞键不可达（丢键），
          // 以 prev=0 盲插会截断其主日志链——自旋重读等待槽位恢复
          backoff.snooze();
          continue;
        }
        if matched {
          // 链头 ReadCache 缓存即本键存活记录：盲追加墓碑链接主日志前驱并 CAS 挂载
          // （前驱低于截断线无碍：读取墓碑即终止，绝不解引用前驱）
          // 优化6（对标 C# BlockAllocate.GetAllocationForRetry）：优先复用上一轮
          // CAS 失败保留的分配，免一次尾部分配；槽位下界同样以链首抬升（墓碑追加
          // 无脱钩臂，恒非整链清空），池内低档槽位仅跳过不误清退
          let min_eligible_addr = retry_alloc.chain_floor(hei.address(), false);
          let (new_addr, alloc_size, alloc_ver) = match retry_alloc.allocate_or_reuse(
            key,
            &[],
            real_prev,
            true,
            self.store.head_address(),
            min_eligible_addr,
          )? {
            Ok(ok) => ok,
            Err(page_id) => return Ok(Err(page_id)),
          };
          if hei.try_cas(new_addr) {
            // 索引提交成功后恰发一次墓碑 AOF 镜像（条目三收口，与 upsert 臂同序）
            self.notify_write_listener_with_version(key, &[], true, alloc_ver)?;
            // CAS 即原子脱钩链头 RC 缓存，孤儿记录交由 cleanse_page 页关闭回收
            if let Some((out, f_opt)) = take.as_mut() {
              self
                .store
                .read_cache
                .with_record(addr, |_rec_key, rec_val| {
                  let r = f_opt.take().expect("take closure called once")(rec_val);
                  **out = Some(r);
                  Some(true)
                });
            }
            return Ok(Ok(true));
          }
          // 优化6（对标 C# BlockAllocate.SaveAllocationForRetry）：CAS 失败的分配
          // 暂存会话内，下一轮优先复用
          retry_alloc.alloc = Some((new_addr, alloc_size));
          backoff.snooze();
          continue;
        }
        // 链头为 Tag 碰撞键的缓存记录：脱钩至首个主日志地址继续回溯；
        // 槽位是碰撞键的存活索引条目，绝不 elide
        cur = real_prev;
      }

      if cur < begin_addr {
        // 槽位指向的主日志记录已被截断物理丢弃：链上更早记录悉数死亡，本键确认不存在；
        // elide 清退死条目回收槽位（ReadCache 分支已提前分流，此处 addr 恒为主日志地址）
        let _ = hei.try_elide();
        return Ok(Ok(false));
      }

      let head_addr = self.store.head_address();
      let safe_ro_addr = self.store.safe_read_only_address();

      // 全内存区链条回溯探查（从链头主日志地址 cur 回溯至 head_addr）
      let mut key_status = None; // None: 记录可能在磁盘; Some(true): 存在有效记录; Some(false): 键不存在或已是墓碑

      while cur >= head_addr {
        let probe = if cur < safe_ro_addr {
          // SAFETY: 调用方纪元保护 + cur ∈ [head_addr, safe_ro_addr)，不可变已排空区纯指针直读（零页锁争用）
          Some(unsafe {
            self
              .store
              .hlog
              .with_immutable_record(cur, |rec| Ok(MemoryRecordProbe::probe(&rec, key)))?
          })
        } else {
          self
            .store
            .hlog
            .with_memory_record(cur, |rec| Ok(MemoryRecordProbe::probe(&rec, key)))?
        };

        match probe {
          Some(MemoryRecordProbe::Closed) => {
            // 密封在途记录命中（严格对标 C# FindRecord.TryFindRecordForUpdate
            // IsClosed → RETRY_LATER）：刷新纪元后整链重试（密封在途记录终将解封，
            // 此时按常规路径删除）
            self.participant.refresh();
            backoff.snooze();
            continue 'retry;
          }
          Some(MemoryRecordProbe::Tombstone { .. }) => {
            key_status = Some(false);
            break;
          }
          Some(MemoryRecordProbe::Active { prev, size, .. }) => {
            // 命中有效记录：检查能否 Record Elision
            // （cur == addr 判定链头：ReadCache 槽位分支已提前分流，此处 addr 恒为
            // 主日志地址；存活缓存条目绝不参与脱钩，避免令键不可达）
            // 可变区命中原位置墓碑（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalDelete.cs:InPlaceDeleter：
            // SetTombstone + SetModified 原位落笔，零追加、零尾部分配、零索引 CAS；
            // 旧版本经 prev 链保持可达，逻辑 AOF 经写监听端口发出与盲追加同构的
            // 墓碑条目）；只读区滑动/页未就绪/锁窗口竞态降级盲追加。
            // 注意：tests/compact 的墓碑计数断言（lazy_compaction /
            // more_log_compaction 多代连续更新）依赖本路径，移除必致测试红
            // 取值模式走原位取删内核（页写锁内同点捕获 + 翻墓碑，应答值即被摘
            // 记录值）；删除模式原样置墓碑
            let tombstone_set = if let Some((out, f_opt)) = take.as_mut() {
              let flipped = self
                .store
                .hlog
                .try_take_tombstone_in_place_with(cur, key, |v| {
                  f_opt.take().expect("take closure called once")(v)
                })?;
              let success = flipped.is_some();
              if let Some(taken_val) = flipped {
                **out = Some(taken_val);
              }
              success
            } else {
              self.store.hlog.try_set_tombstone_in_place(cur, key)?
            };
            if tombstone_set {
              // 优化4（严格对标 C# Helpers.HandleRecordElision 与 InternalDelete.cs:133-134）：
              // 原位打上墓碑后，若无有效旧版本，尝试从哈希链脱钩（"just tidying up the HashBucket"），
              // 开启复活池时转移至空闲池回收，关闭时 try_seal_record 密封
              if !self.store.is_version_shift_open()
                && cur == addr
                && (prev == 0 || prev < begin_addr)
                && hei.try_elide()
              {
                if self.store.config.enable_revivification {
                  self.store.transfer_to_reviv_pool(cur, size as u32);
                } else {
                  let _ = self.store.hlog.try_seal_record(cur, true);
                }
              }
              self.notify_write_listener(key, &[], true)?;
              return Ok(Ok(true));
            }

            // 原位失败复检（并发竞态兜底）：并发 DEL 已抢先原位置墓碑 → 幂等返回
            // false——对齐索引 CAS 失败重探的恰好一次删除语义（原位墓碑无 CAS
            // 线性化点，直接盲追加会产生双 notify 与重复 AOF 条目）；
            // 记录仍有效或已滑出内存 → 落回盲追加
            if self
              .store
              .hlog
              .with_memory_record(cur, |rec| Ok(rec.matches_key(key) && rec.is_tombstone()))?
              .unwrap_or(false)
            {
              return Ok(Ok(false));
            }

            key_status = Some(true);
            break;
          }
          Some(MemoryRecordProbe::Miss(prev)) => {
            cur = prev;
          }
          _ => break,
        }
      }

      if key_status == Some(false) {
        return Ok(Ok(false));
      }

      if cur == 0 || cur < begin_addr {
        // 链条已在内存区彻底终结，确认该键在系统中从未存在（Tag 碰撞）
        return Ok(Ok(false));
      }

      if key_status != Some(true) {
        // 内存区未探查到匹配记录且链条伸入磁盘区，需降级至异步冷数据确认
        return Ok(Err(DEGRADE_ASYNC));
      }

      // 取值模式摘除面守卫：命中记录驻留可变区而原位取删不可行（页未就绪/版本
      // 推进冻结）时，捕获与摘除无法同临界区闭环——交异步臂（驱逐后原位可即）
      // 重试，杜绝「捕获旧值 + 盲墓碑摘走新值」撕裂；不可变区记录捕获后值恒定，
      // 捕获 + 盲墓碑 CAS 摘除安全
      if take.is_some() && cur >= safe_ro_addr {
        return Ok(Err(DEGRADE_ASYNC));
      }

      // 确认为有效键：纯同步分配并追加盲墓碑（Blind Tombstone Append）
      // 优化6（对标 C# BlockAllocate.GetAllocationForRetry）：优先复用上一轮 CAS
      // 失败保留的分配，免一次尾部分配；槽位下界以链首抬升，严格保证墓碑地址 >
      // 被取代链首，碰撞链 prev_address 恒单调递减不成环
      let min_eligible_addr = retry_alloc.chain_floor(hei.address(), false);
      let prev_link = if addr < begin_addr { 0 } else { addr };
      let (new_addr, alloc_size, alloc_ver) = match retry_alloc.allocate_or_reuse(
        key,
        &[],
        prev_link,
        true,
        head_addr,
        min_eligible_addr,
      )? {
        Ok(ok) => ok,
        Err(page_id) => return Ok(Err(page_id)),
      };

      if hei.try_cas(new_addr) {
        // 索引提交成功后恰发一次墓碑 AOF 镜像（条目三收口，与 upsert 臂同序）
        self.notify_write_listener_with_version(key, &[], true, alloc_ver)?;
        // CAS 即原子脱钩链头（若为 RC 缓存或碰撞键缓存前缀），孤儿记录交由
        // cleanse_page 页关闭回收（对标 C# Helpers.CASRecordIntoChain），写侧不作废
        if let Some((out, f_opt)) = take.as_mut() {
          unsafe {
            self.store.hlog.with_immutable_record(cur, |rec| {
              let r = f_opt.take().expect("take closure called once")(rec.value());
              **out = Some(r);
              Ok(())
            })?;
          }
        }
        return Ok(Ok(true));
      }

      // 优化6（对标 C# BlockAllocate.SaveAllocationForRetry）：CAS 失败的分配连同
      // 真实帧足印暂存会话内，下一轮优先复用；替代原先直接落全局复活池（下一轮
      // 重新尾部分配）
      retry_alloc.alloc = Some((new_addr, alloc_size));
      backoff.snooze();
    }
  }

  /// 磁盘冷数据异步删除慢路径（候选链定位、追加、挂载与败帧收尾全部转调
  /// copy-to-tail 内核 [`Self::copy_record_to_tail`]，骨架与纪元纪律见该处）
  ///
  /// 沿记录 `prev_address` 前驱链回溯确保被 Tag 碰撞键掩埋的冷记录也能真实
  /// 删除；命中存活记录盲追加 0 字节墓碑（前驱 = 顺链解析的首个主日志地址），
  /// 命中墓碑或候选链无存活记录即确认不存在；CAS 落败（内核已将败帧回复活池）
  /// 返回 `None` 交调用方 `delete_raw` 刷新重试。
  pub(in crate::session) async fn delete_raw_disk_slow(&self, key: &[u8]) -> Result<Option<bool>> {
    match self
      .copy_record_to_tail(key, true, true, |record| {
        // 命中墓碑：键已删除，零写确认
        if record.is_tombstone().unwrap_or(false) {
          return None;
        }
        // 命中存活记录：空值墓碑帧
        Some(Vec::new())
      })
      .await?
    {
      CopyToTailOutcome::Miss | CopyToTailOutcome::Closed => Ok(Some(false)),
      CopyToTailOutcome::Appended { cas_ok: true, .. } => Ok(Some(true)),
      CopyToTailOutcome::Appended { .. } => Ok(None),
    }
  }

  /// 磁盘冷数据异步取删慢路径（[`Self::delete_raw_disk_slow`] 的取值对位）
  ///
  /// plan 构帧与取值同读一次源记录（冷数据不可变，捕获即摘除值），`Ok(None)`
  /// 为 CAS 落败交调用方刷新重试
  pub(in crate::session) async fn take_raw_disk_slow(
    &self,
    key: &[u8],
  ) -> Result<Option<Option<Vec<u8>>>> {
    let mut taken = None;
    match self
      .copy_record_to_tail(key, true, true, |record| {
        // 命中墓碑：键已删除，零写确认
        if record.is_tombstone().unwrap_or(false) {
          return None;
        }
        // 命中存活记录：捕获摘除值 + 空值墓碑帧
        taken = record.value().ok().map(Vec::from);
        Some(Vec::new())
      })
      .await?
    {
      CopyToTailOutcome::Miss | CopyToTailOutcome::Closed => Ok(Some(None)),
      CopyToTailOutcome::Appended { cas_ok: true, .. } => Ok(Some(taken)),
      CopyToTailOutcome::Appended { .. } => Ok(None),
    }
  }
}
