//! 原位更新与 CAS 校验（对标 C# Garnet InPlaceUpdater / InPlaceDeleter / Record Elision 快慢路径）

use std::result::Result as StdResult;

use wbase::{addr::is_read_cache, backoff::Backoff, simd::fast_key_eq};
use wdev::Device;
use wrecord::record_size;

use super::CopyToTailOutcome;
use crate::{error::Result, read_cache::RcVisit, session::StoreSession};

/// CAS 失败分配的会话内暂存守卫（对标 C# BlockAllocate.cs:SaveAllocationForRetry 的
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

  /// 复用判定失败的弃置（仅复活池开启时回收，否则遗弃——与既有 CAS 失败补偿口径一致）
  #[inline(always)]
  fn discard(&self, addr: u64, size: u32) {
    if self.session.store.config.enable_revivification {
      self
        .session
        .store
        .reviv_pool
        .put(addr, size, self.session.store.hlog.read_only_address());
    }
  }

  /// 尝试复用暂存槽位或分配新记录（对标 BlockAllocate.cs:GetAllocationForRetry）
  #[inline(always)]
  fn allocate_or_reuse(
    &mut self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
    min_head_addr: u64,
    hei_addr: u64,
  ) -> Result<StdResult<(u64, u32), u64>> {
    let rec_size = record_size(key.len(), val.len()) as u32;
    if let Some((addr, size)) = self.alloc.take() {
      if addr > hei_addr
        && addr >= min_head_addr
        && size >= rec_size
        && self
          .session
          .store
          .hlog
          .revivify_record_at(addr, size as usize, key, val, prev_addr, is_tombstone)
          .is_ok()
      {
        return Ok(Ok((addr, size)));
      }
      self.discard(addr, size);
    }
    self
      .session
      .try_allocate_or_append_record_sync(key, val, prev_addr, is_tombstone)
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
  Active { prev: u64, size: usize },
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
  /// - 仅当遭遇环形缓冲区溢出需跨页落盘（PageNotReady）时返回 Ok(Err(page_id))，指示调用者降级至异步落盘。
  pub(in crate::session) fn try_upsert_raw_sync_unprotected(
    &self,
    key: &[u8],
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    let hash = whasher::fast_hash(key);
    if self.store.is_growing() {
      self.store.split_buckets(hash)?;
    }
    // 优化6：CAS 失败分配的会话内暂存（对标 BlockAllocate.cs:SaveAllocationForRetry）
    let mut retry_alloc = RetryAlloc::new(self);
    let mut backoff = Backoff::new();
    loop {
      let begin_addr = self.store.begin_address();
      let index = self.store.index.load();
      let mut hei = index.find_or_create_tag_by_hash_with_min_addr(hash, begin_addr)?;

      // 优化5（严格对标 Helpers.cs:FindOrCreateTagAndTryEphemeralXLock）：复活配置
      // 开启时，traceback 前先对槽位所在桶加 ephemeral 独占锁，防止复活/脱钩在回溯
      // 期间把记录从链上抽走（"Ephemeral must lock the bucket before traceback"）；
      // 锁失败按 C# RETRY_LATER 协议刷新纪元后整链重试。复活功能全关时无抽链风险，
      // 维持现状免锁（语义等价裁剪）
      let _x_latch = if self.ephemeral_lock_enabled() {
        match hei.lock_exclusive_guard() {
          Some(latch) => Some(latch),
          None => {
            self.participant.refresh();
            backoff.snooze();
            continue;
          }
        }
      } else {
        None
      };
      // 优化5（对标 HashEntryInfo.SetToCurrent）：定位 Tag 与加锁之间槽位可能已被
      // 并发 CAS/脱钩改写，重读当前槽位杜绝陈旧地址
      hei.set_to_current();

      // 优化3 elide 候选：命中记录即链首槽位时的 (源地址, 前驱地址, 物理尺寸)
      // （尺寸须在探针期随页读锁捕获，CAS 成功后源槽位随时可能被复活方改写）
      let mut elide_src = None;
      let prev_addr = if !hei.is_found() {
        // 1. 全新 Key（或原截断槽位已被单趟探针 CAS 清退置零）：首项槽位为空，前驱逻辑地址为 0
        0
      } else {
        let addr = hei.address();

        if is_read_cache(addr) {
          // 2. ReadCache 槽位：只读内存缓存不可原位更新，顺链获取主日志真实地址后
          // 以其为前驱盲插；CAS 即原子脱钩 RC 前缀（对标 Helpers.cs:CASRecordIntoChain），
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

          // 3. 主日志内存可变区探针（严格对标 C# InternalUpsert.cs:TryFindRecordForUpdate / TraceBackForKeyMatch）：
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
                Ok(Some(MemoryRecordProbe::Active { prev, size })) => {
                  // 原位修改：尺寸匹配直接就地更新，零 I/O、零追加返回（严格双检 expected_key）
                  if self.store.hlog.try_update_in_place(cur, key, val)? {
                    self.notify_write_listener(key, val, false)?;
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
                  // 链内原地复活：若是墓碑记录，尝试就地复活复用该槽位（严格双检 expected_key）
                  if self.store.config.enable_revivification
                    && self.store.hlog.try_revivify_in_chain(cur, key, val)?
                  {
                    self.notify_write_listener(key, val, false)?;
                    return Ok(Ok(cur));
                  }
                  if cur == addr {
                    elide_src = Some((cur, prev, size as u32));
                  }
                  break; // 复活失败，跳出走尾部追加
                }
                Ok(Some(MemoryRecordProbe::Closed)) => {
                  // 密封在途记录命中（严格对标 FindRecord.cs:TryFindRecordForUpdate
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
          // 优化3（对标 InternalUpsert.cs:CreateNewRecordUpsert elideSourceRecord +
          // Helpers.cs:CanElide）：命中记录即链首槽位（hei.Address == 源记录地址）且其
          // 前驱低于截断边界（链上仅此一条有效记录，无碰撞键可达性依赖）时，新记录
          // 前驱直接接管旧记录前驱，将旧链首记录从链上摘除
          if let Some((src, src_prev, _)) = elide_src
            && src == addr
            && src_prev < begin_addr
          {
            src_prev
          } else if addr < begin_addr {
            0
          } else {
            addr
          }
        }
      };

      // 5. 纯内存分配与原子 CAS 挂载
      // 优化6（对标 BlockAllocate.cs:GetAllocationForRetry）：优先复用上一轮 CAS
      // 失败保留的分配——地址高于当前槽位条目、未滑出内存且帧足印足够时，重写
      // 前驱指针（键值布局与上轮完全一致）即可整帧复用，免一次尾部分配
      let (new_addr, alloc_size) = match retry_alloc.allocate_or_reuse(
        key,
        val,
        prev_addr,
        false,
        self.store.head_address(),
        hei.address(),
      )? {
        Ok(ok) => ok,
        Err(page_id) => return Ok(Err(page_id)), // 环形缓冲区翻转，将精确 page_id 传回调用者
      };

      if hei.try_cas(new_addr) {
        // CAS 即原子脱钩被取代的 RC 前缀（若有），孤儿缓存记录交由 cleanse_page
        // 在页关闭时回收（对标 Helpers.cs:CASRecordIntoChain），写侧不作废
        // 优化3（对标 CreateNewRecordUpsert CAS 成功后的 SealAndInvalidate +
        // TryTransferToFreeList）：旧链首记录已被新记录取代且新记录不再指向它，
        // 已从链上脱钩；复活池开启时转移至空闲池回收（低于复活下限则遗弃，
        // 对标 GetMinRevivifiableAddress 判定）
        if let Some((src, _, src_size)) = elide_src
          && self.store.config.enable_revivification
        {
          self
            .store
            .reviv_pool
            .put(src, src_size, self.store.hlog.read_only_address());
        }
        return Ok(Ok(new_addr));
      }

      // 优化6（对标 BlockAllocate.cs:SaveAllocationForRetry）：CAS 失败的分配连同
      // 真实帧足印暂存会话内，下一轮优先复用；替代原先直接落全局复活池（下一轮
      // 重新尾部分配）
      retry_alloc.alloc = Some((new_addr, alloc_size));
      backoff.snooze();
    }
  }

  /// 纯同步快速路径物理删除单个键（对齐 Garnet InternalDelete / NetworkDEL 执行链路）
  ///
  /// 语义与安全边界详见 `Self::try_delete_raw_sync_unprotected`
  #[inline(always)]
  pub fn try_delete_raw_sync(&self, key: &[u8]) -> Result<StdResult<bool, u64>> {
    let _guard = self.enter_gated();
    self.try_delete_raw_sync_unprotected(key)
  }

  /// 纯同步快速路径物理删除内核（调用方须已处于纪元保护下，批处理上下文专用）
  ///
  /// - 纯内存单趟执行：若记录在可变区且无前驱，直接 CAS 脱钩（Record Elision）——
  ///   复活池开启时回收至 FreeRecordPool，关闭时仅清链 tidy（对标 C# HandleRecordElision
  ///   的 `!RevivificationManager.IsEnabled` 分支），零追加返回；
  /// - 若记录处于只读区、冷数据区或不可 elide，盲追加一条 0 字节墓碑记录（Blind Tombstone Append）并 CAS 挂载；
  /// - 100% 免除 Tokio 协程让出与 Future 构造；
  /// - 仅当追加墓碑遭遇环形缓冲区翻转（PageNotReady）时返回 Ok(Err(page_id))，交由外层异步驱逐；
  /// - 冷数据需磁盘确认时返回 Ok(Err(u64::MAX))，调用方降级全异步路径。
  ///
  /// # 设计边界（对标 RecordInfo.TrySeal 的刻意裁剪）
  /// Record Elision 路径未移植 C# `InfoRef.TrySeal(invalidate: true)` 的记录头密封协议
  /// （裁决见 wreviv::FreeRecord 并发模型注释）：脱钩后槽位立即入池，可能在其他核心
  /// 的在途读者完成解析前被复活改写。安全性依赖 wreviv 声明的"单写者 + 页写锁 +
  /// epoch 保护"前提，并以会话级 `record_elision` 显式开启为门控（默认关闭）+
  /// ephemeral 桶锁互斥（优化5）；未来多核高争用场景需先在 whlog/wrecord 层落地原子
  /// TrySeal 语义，再放宽本前提。
  pub(in crate::session) fn try_delete_raw_sync_unprotected(
    &self,
    key: &[u8],
  ) -> Result<StdResult<bool, u64>> {
    let hash = whasher::fast_hash(key);
    if self.store.is_growing() {
      self.store.split_buckets(hash)?;
    }
    // 优化6：CAS 失败分配的会话内暂存（对标 BlockAllocate.cs:SaveAllocationForRetry）
    let mut retry_alloc = RetryAlloc::new(self);
    let mut backoff = Backoff::new();
    'retry: loop {
      let begin_addr = self.store.begin_address();
      let index = self.store.index.load();
      let Some(mut hei) = index.find_tag_entry_by_hash_with_min_addr(hash, begin_addr) else {
        return Ok(Ok(false));
      };

      // 优化5（严格对标 Helpers.cs:FindTagAndTryEphemeralXLock）：复活配置开启时，
      // traceback 前对槽位所在桶加 ephemeral 独占锁，防止复活/脱钩在回溯期间把记录
      // 从链上抽走；锁失败按 C# RETRY_LATER 协议刷新纪元后整链重试。复活功能全关时
      // 无抽链风险，维持现状免锁（语义等价裁剪）
      let _x_latch = if self.ephemeral_lock_enabled() {
        match hei.lock_exclusive_guard() {
          Some(latch) => Some(latch),
          None => {
            self.participant.refresh();
            backoff.snooze();
            continue 'retry;
          }
        }
      } else {
        None
      };
      // 优化5（对标 HashEntryInfo.SetToCurrent）：定位 Tag 与加锁之间槽位可能已被
      // 并发 CAS/脱钩改写，重读当前槽位杜绝陈旧地址
      hei.set_to_current();

      let addr = hei.address();
      // ReadCache 链头分流（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalDelete.cs:InternalDelete.cs：TryFindRecordForUpdate 的
      // 键匹配链遍历 + HasReadCacheSrc → CreateNewRecord 盲插墓碑）
      let mut cur = addr;
      if is_read_cache(addr) {
        let matched = matches!(
          self
            .store
            .read_cache
            .with_record(addr, |rec_key, _rec_val| Some(fast_key_eq(rec_key, key))),
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
          // 优化6（对标 BlockAllocate.cs:GetAllocationForRetry）：优先复用上一轮
          // CAS 失败保留的分配，免一次尾部分配
          let (new_addr, alloc_size) = match retry_alloc.allocate_or_reuse(
            key,
            &[],
            real_prev,
            true,
            self.store.head_address(),
            hei.address(),
          )? {
            Ok(ok) => ok,
            Err(page_id) => return Ok(Err(page_id)),
          };
          if hei.try_cas(new_addr) {
            // CAS 即原子脱钩链头 RC 缓存，孤儿记录交由 cleanse_page 页关闭回收
            return Ok(Ok(true));
          }
          // 优化6（对标 BlockAllocate.cs:SaveAllocationForRetry）：CAS 失败的分配
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
        // elide 清退死条目回收槽位（ReadCache 分支已提前返回，此处 addr 恒为主日志地址）
        let _ = hei.try_elide();
        return Ok(Ok(false));
      }

      let read_only_addr = self.store.hlog.read_only_address();
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
            // 密封在途记录命中（严格对标 FindRecord.cs:TryFindRecordForUpdate
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
          Some(MemoryRecordProbe::Active { prev, size }) => {
            // 命中有效记录：检查能否 Record Elision
            // （cur == addr 判定链头：ReadCache 槽位分支已提前分流，此处 addr 恒为
            // 主日志地址；存活缓存条目绝不参与脱钩，避免令键不可达）
            // 优化4（严格对标 Helpers.cs:HandleRecordElision）：脱钩清链不再被
            // enable_revivification 挡死——复活池关闭时同样 TryElide 清链
            // （"just tidying up the HashBucket"），开启时才转移至空闲池回收
            if self.record_elision()
              && cur == addr
              && cur >= read_only_addr
              && (prev == 0 || prev < begin_addr)
            {
              if hei.try_elide() {
                if self.store.config.enable_revivification {
                  self.store.reviv_pool.put(cur, size as u32, read_only_addr);
                }
                self.notify_write_listener(key, &[], true)?;
                return Ok(Ok(true));
              }
              backoff.snooze();
              continue;
            }

            // 可变区命中原位置墓碑（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalDelete.cs:InPlaceDeleter：
            // SetTombstone + SetModified 原位落笔，零追加、零尾部分配、零索引 CAS；
            // 旧版本经 prev 链保持可达，逻辑 AOF 经写监听端口发出与盲追加同构的
            // 墓碑条目）；只读区滑动/页未就绪/锁窗口竞态降级盲追加。
            // 注意：tests/compact 的墓碑计数断言（lazy_compaction /
            // more_log_compaction 多代连续更新）依赖本路径，移除必致测试红
            if self.store.hlog.try_set_tombstone_in_place(cur, key)? {
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
        return Ok(Err(u64::MAX));
      }

      // 确认为有效键：纯同步分配并追加盲墓碑（Blind Tombstone Append）
      // 优化6（对标 BlockAllocate.cs:GetAllocationForRetry）：优先复用上一轮 CAS
      // 失败保留的分配，免一次尾部分配
      let prev_link = if addr < begin_addr { 0 } else { addr };
      let (new_addr, alloc_size) =
        match retry_alloc.allocate_or_reuse(key, &[], prev_link, true, head_addr, hei.address())? {
          Ok(ok) => ok,
          Err(page_id) => return Ok(Err(page_id)),
        };

      if hei.try_cas(new_addr) {
        // CAS 即原子脱钩链头（若为 RC 缓存或碰撞键缓存前缀），孤儿记录交由
        // cleanse_page 页关闭回收（对标 Helpers.cs:CASRecordIntoChain），写侧不作废
        return Ok(Ok(true));
      }

      // 优化6（对标 BlockAllocate.cs:SaveAllocationForRetry）：CAS 失败的分配连同
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
        Some((Vec::new(), ()))
      })
      .await?
    {
      CopyToTailOutcome::Miss | CopyToTailOutcome::Closed => Ok(Some(false)),
      CopyToTailOutcome::Appended { cas_ok: true, .. } => Ok(Some(true)),
      CopyToTailOutcome::Appended { .. } => Ok(None),
    }
  }
}
