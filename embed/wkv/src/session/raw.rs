//! 纯引擎 KV 面（对标 C# Garnet ClientSession 的 Upsert/Read/Delete/RMW 快慢路径）
//!
//! 物理键操作：一切 `*_raw` 与 unprotected 变体、内存直读内核（FindTag 单点探针 +
//! TraceBackForKeyMatch 反向回溯 + 多候选扫描）、磁盘冷读回退、批量预取读、
//! Record Elision 删除与盲墓碑追加。用户键便捷层经 [`keys`](super::keys) 编码后
//! 落到同一套物理路径。

use std::{hint::spin_loop, result::Result as StdResult, thread::yield_now};

use futures_util::future::join_all;
use wdev::Device;
use windex::{CandidateAddresses, HashIndex, prefetch_read_l1};
use wrecord::{RecordHeader, fast_key_eq, record_size};
use wval::{KeyTag, NamespaceDbCodec, TaggedKeyBuf};

use crate::{
  error::{Error, Result},
  read_cache::is_read_cache_addr,
  session::StoreSession,
};

/// 批量读取流水线预取窗口大小（对齐 C# Garnet 12 项硬件级流水线预取）
pub const BATCH_READ_PREFETCH_SIZE: usize = 12;
/// 推进 SafeHeadAddress 时的自旋退避阈值
const SAFE_HEAD_SPIN_LIMIT: usize = 16;
/// 推进 SafeHeadAddress 时的最大等待轮次
const SAFE_HEAD_MAX_SPINS: usize = 256;

#[derive(Debug)]
enum ReadProbeResult<T> {
  /// Tag 碰撞未匹配：携带 `prev_address` 供磁盘候选链收集
  Miss(u64),
  Tombstone,
  Found(T),
}

#[derive(Debug)]
enum TraceBackResult<T> {
  Found(T),
  Tombstone,
  TraceBack(u64),
}

/// 内存直读内部结果（严格对照 C# Garnet InternalRead 单遍分类，附加磁盘候选透传）
enum MemRead<R> {
  /// 内存阶段已闭环：`Some` 为命中值，`None` 为确认不存在（含墓碑，对应 `NOTFOUND`）
  Done(Option<R>),
  /// 记录位于磁盘区：携带按新版本优先降序排列的磁盘候选地址（对应 `RECORD_ON_DISK`）
  OnDisk(CandidateAddresses),
}

impl<D: Device> StoreSession<D> {
  /// 尝试在内存可变区原位读-改-写记录（严格对标 C# Garnet InternalRMW.cs & InPlaceUpdaterWorker）
  ///
  /// 利用单趟探针定位槽位后，若记录处于内存可变区且键匹配非墓碑，直接持有页写锁在物理内存切片上执行闭包原地修改。
  /// 若原地修改成功返回 `Ok(Some(R))`，完全 0 堆分配、0 HLog 追加、0 哈希表 CAS！
  /// 若记录不在可变区、长度不符或闭包返回 `None`，安全返回 `Ok(None)` 供调用方降级到完整路径。
  pub fn try_modify_in_place<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&mut [u8]) -> Option<R>,
  ) -> Result<Option<R>> {
    let _guard = self.participant.enter();
    self.try_modify_in_place_unprotected(user_key, f)
  }

  /// 底层物理在已有纪元保护下尝试在内存可变区原位读-改-写记录（Raw）
  #[inline]
  pub fn try_modify_raw_in_place_unprotected<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&mut [u8]) -> Option<R>,
  ) -> Result<Option<R>> {
    if let Some(addr) = self.store.index.find_tag(key) {
      let begin_addr = self.store.begin_address();
      if !is_read_cache_addr(addr) && addr >= begin_addr && self.store.hlog.is_mutable(addr) {
        // 注入写监听时捕获修改后的值副本用于回调；未注入时捕获分支被常量折叠
        let capture = self.store.write_listener().is_some();
        let mut captured: Option<Vec<u8>> = None;
        let wrapped = |val: &mut [u8]| {
          let r = f(val);
          if capture && r.is_some() {
            captured = Some(val.to_vec());
          }
          r
        };
        let opt = self
          .store
          .hlog
          .try_modify_record_in_place(addr, key, wrapped)?;
        if opt.is_some()
          && let Some(new_val) = captured
        {
          self.notify_write_listener(key, &new_val, false);
        }
        return Ok(opt);
      }
    }
    Ok(None)
  }

  /// 在已有纪元保护下尝试在内存可变区原位读-改-写当前会话普通字符串记录（彻底绕过 enter() 原子开销）
  #[inline]
  pub fn try_modify_in_place_unprotected<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&mut [u8]) -> Option<R>,
  ) -> Result<Option<R>> {
    let str_k = self.session_string_key(user_key);
    self.try_modify_raw_in_place_unprotected(&str_k, f)
  }

  /// 在单次纪元保护下尝试利用动态松弛原位覆写当前会话字符串记录的值（严格对标 C# Garnet LogRecord.TrySetPinnedValueSpan）
  #[inline]
  pub fn try_modify_with_slack(&self, user_key: &[u8], new_val: &[u8]) -> Result<bool> {
    let _guard = self.participant.enter();
    self.try_modify_with_slack_unprotected(user_key, new_val)
  }

  /// 底层物理在已有纪元保护下尝试在内存可变区基于动态松弛原位覆写记录的值（Raw）
  #[inline]
  pub fn try_modify_raw_with_slack_unprotected(&self, key: &[u8], new_val: &[u8]) -> Result<bool> {
    if let Some(addr) = self.store.index.find_tag(key) {
      let begin_addr = self.store.begin_address();
      if !is_read_cache_addr(addr) && addr >= begin_addr && self.store.hlog.is_mutable(addr) {
        let ok = self
          .store
          .hlog
          .try_modify_record_with_slack(addr, key, new_val)?;
        if ok {
          self.notify_write_listener(key, new_val, false);
        }
        return Ok(ok);
      }
    }
    Ok(false)
  }

  /// 在已有纪元保护下尝试在内存可变区基于动态松弛原位覆写当前会话普通字符串记录的值（严格对标 C# Garnet LogRecord.TrySetPinnedValueSpan）
  #[inline]
  pub fn try_modify_with_slack_unprotected(&self, user_key: &[u8], new_val: &[u8]) -> Result<bool> {
    let str_k = self.session_string_key(user_key);
    self.try_modify_raw_with_slack_unprotected(&str_k, new_val)
  }

  /// 追加记录到混合日志尾部，并在环形缓冲区耗尽触发 PageNotReady 时自动将旧页刷盘并驱逐至磁盘
  pub async fn append_record(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
  ) -> Result<u64> {
    self
      .append_record_inner(key, val, prev_addr, is_tombstone, true)
      .await
  }

  /// 紧缩搬迁专用追加：旁路写监听
  ///
  /// AOF 只记原始用户写效果；搬迁帧入 AOF 会在并发写下造成恢复回退
  /// （搬迁旧值帧晚于并发新值帧入队，重放序错乱），故物理布局优化不入 AOF
  pub async fn append_record_compacted(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
  ) -> Result<u64> {
    self
      .append_record_inner(key, val, prev_addr, is_tombstone, false)
      .await
  }

  /// 追加公共体：PageNotReady 时驱逐旧页重试；`notify` 控制写监听回调
  async fn append_record_inner(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
    notify: bool,
  ) -> Result<u64> {
    loop {
      match self.store.hlog.append(key, val, prev_addr, is_tombstone) {
        Ok(addr) => {
          if notify {
            self.notify_write_listener(key, val, is_tombstone);
          }
          return Ok(addr);
        }
        Err(whlog::Error::PageNotReady(page_id)) => {
          self.evict_pages_for(page_id).await?;
        }
        Err(e) => return Err(e.into()),
      }
    }
  }

  /// 纯同步尝试分配空闲槽位或追加 Tail（严格对标 C# Garnet BlockAllocate.cs & InternalUpsert.cs）
  /// - Ok(Ok(addr)): 纯内存就地分配或追加成功
  /// - Ok(Err(page_id)): 环形缓冲区翻转（PageNotReady），需调用者执行异步落盘与驱逐
  fn try_allocate_or_append_record_sync(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
  ) -> Result<StdResult<u64, u64>> {
    if self.store.config.enable_revivification {
      let rec_size = record_size(key.len(), val.len());
      let min_reviv_addr = self.store.hlog.read_only_address();

      // 优先从 FreeRecordPool 提取最适配的空闲槽位并就地复活写入
      if let Some((free_addr, slot_size)) =
        self.store.reviv_pool.take(rec_size as u32, min_reviv_addr)
      {
        if self
          .store
          .hlog
          .revivify_record_at(
            free_addr,
            slot_size as usize,
            key,
            val,
            prev_addr,
            is_tombstone,
          )
          .is_ok()
        {
          self.notify_write_listener(key, val, is_tombstone);
          return Ok(Ok(free_addr));
        } else if self.store.hlog.is_mutable(free_addr) {
          // 若临时写入失败且槽位仍在可变区，归还至复活池防槽位丢失
          self
            .store
            .reviv_pool
            .put(free_addr, slot_size, min_reviv_addr);
        }
      }
    }

    match self.store.hlog.append(key, val, prev_addr, is_tombstone) {
      Ok(addr) => {
        self.notify_write_listener(key, val, is_tombstone);
        Ok(Ok(addr))
      }
      Err(whlog::Error::PageNotReady(page_id)) => Ok(Err(page_id)),
      Err(e) => Err(e.into()),
    }
  }

  /// 触发写监听端口（未注入则零开销跳过）
  #[inline]
  fn notify_write_listener(&self, key: &[u8], val: &[u8], tombstone: bool) {
    if let Some(listener) = self.store.write_listener() {
      listener(key, val, tombstone);
    }
  }

  /// 当环形缓冲区发生翻转并遇到槽位尚未驱逐时，异步刷盘并推进 HeadAddress
  async fn evict_pages_for(&self, page_id: u64) -> Result<()> {
    let num_pages = self.store.hlog.config.num_pages as u64;
    if page_id < num_pages {
      return Ok(());
    }
    let old_page = page_id - num_pages;
    let curr_tail = self.store.hlog.tail_address();
    let curr_tail_page = self.store.hlog.config.page_id(curr_tail);

    // 批量推进驱逐窗口，平摊磁盘 I/O 成本并避免逐页频繁颠簸
    let batch_pages = (num_pages / 8).clamp(1, 64);
    let target_evict_page = (old_page + batch_pages)
      .min(curr_tail_page.saturating_sub(1))
      .max(old_page);

    let flushed_until = self.store.hlog.flushed_until_address();
    let start_page = self.store.hlog.config.page_id(flushed_until);

    if start_page <= target_evict_page {
      for p in start_page..=target_evict_page {
        self.store.hlog.flush_page(p).await?;
      }
      self.store.device.sync().await.map_err(Error::from)?;
    }

    let min_evicted_addr = self
      .store
      .hlog
      .config
      .page_start_address(target_evict_page + 1);
    self.store.shift_read_only_address(min_evicted_addr);
    self.store.shift_head_address(min_evicted_addr);

    // 关键：临时退出当前会话的纪元保护区，确保 SafeHeadAddress 能够顺利推进越过目标旧页
    let reentrant = self.participant.reentrant_count();
    for _ in 0..reentrant {
      self.participant.exit();
    }
    let mut spins = 0usize;
    while self.store.hlog.safe_head_address() < min_evicted_addr {
      self.store.epoch.bump_epoch();
      spins = spins.saturating_add(1);
      if spins <= SAFE_HEAD_SPIN_LIMIT {
        spin_loop();
      } else {
        yield_now();
      }
      if spins > SAFE_HEAD_MAX_SPINS {
        break;
      }
    }
    for _ in 0..reentrant {
      let _ = self.participant.enter();
    }

    Ok(())
  }

  /// 底层物理纯同步快速路径写入（Raw，严格对标 C# Garnet NetworkSET & InternalUpsert）
  pub fn try_upsert_raw_sync(&self, key: &[u8], val: &[u8]) -> Result<StdResult<u64, u64>> {
    let _guard = self.participant.enter();
    self.try_upsert_raw_sync_unprotected(key, val)
  }

  /// 底层物理纯同步快速路径写入内核（调用方须已处于纪元保护下，批处理上下文专用）
  ///
  /// - 纯内存执行：原位更新、链内原地复活、复活池分配、内存尾部原子追加；
  /// - 100% 避免异步状态机调度与协程让出；
  /// - 仅当遭遇环形缓冲区溢出需跨页落盘（PageNotReady）时返回 Ok(Err(page_id))，指示调用者降级至异步落盘。
  fn try_upsert_raw_sync_unprotected(&self, key: &[u8], val: &[u8]) -> Result<StdResult<u64, u64>> {
    loop {
      let begin_addr = self.store.begin_address();
      let mut hei = self
        .store
        .index
        .find_or_create_tag_with_min_addr(key, begin_addr)?;

      let prev_addr = if !hei.is_found() {
        // 1. 全新 Key（或原截断槽位已被单趟探针 CAS 清退置零）：首项槽位为空，前驱逻辑地址为 0
        0
      } else {
        let addr = hei.address();

        if is_read_cache_addr(addr) {
          // 2. ReadCache 槽位：只读内存缓存不可原位更新，顺链获取主日志真实地址后直接脱钩盲插；
          // 断链（0）说明条目刚滑出窗口、驱逐方 cleanse 尚未恢复槽位，此时以 prev=0 盲插
          // 会截断碰撞键的主日志链（丢键），自旋重读等待恢复
          let real = self.store.read_cache.skip_read_cache(addr);
          if real == 0 {
            spin_loop();
            continue;
          }
          real
        } else {
          let read_only_addr = self.store.hlog.read_only_address();

          // 3. 主日志内存可变区探针：严格剪枝，只在 [read_only_addr, tail) 范围内检索
          // （注：根据 HLog 不变量 read_only >= begin，addr >= read_only 蕴含 addr >= begin）
          if addr >= read_only_addr {
            let mut cur = addr;
            while cur >= read_only_addr {
              let probe = self.store.hlog.with_memory_record(cur, |rec| {
                if rec.matches_key(key) {
                  Ok((true, rec.is_tombstone(), 0))
                } else {
                  Ok((false, false, rec.prev_address()))
                }
              });

              match probe {
                Ok(Some((true, false, _))) => {
                  // 原位修改：尺寸匹配直接就地更新，零 I/O、零追加返回（严格双检 expected_key）
                  if self.store.hlog.try_update_in_place(cur, key, val)? {
                    self.notify_write_listener(key, val, false);
                    return Ok(Ok(cur));
                  }
                  break; // 长度变更或不可修改，跳出走尾部追加
                }
                Ok(Some((true, true, _))) => {
                  // 链内原地复活：若是墓碑记录，尝试就地复活复用该槽位（严格双检 expected_key）
                  if self.store.config.enable_revivification
                    && self.store.hlog.try_revivify_in_chain(cur, key, val)?
                  {
                    self.notify_write_listener(key, val, false);
                    return Ok(Ok(cur));
                  }
                  break; // 复活失败，跳出走尾部追加
                }
                Ok(Some((false, _, prev))) => {
                  // 键不匹配（Tag 碰撞），继续沿 prev_address 反向回溯
                  cur = prev;
                }
                _ => break,
              }
            }
          }

          // 4. 零 I/O 盲插（Blind Tail Append）：
          // 若地址已进入只读区（addr < read_only_addr）或可变区未匹配/无法原位更新，
          // 彻底杜绝磁盘 I/O，前驱直接链接当前槽位地址；若并发推进导致 addr 低于截断边界则防御性置 0
          if addr < begin_addr { 0 } else { addr }
        }
      };

      // 5. 纯内存分配与原子 CAS 挂载
      let new_addr = match self.try_allocate_or_append_record_sync(key, val, prev_addr, false)? {
        Ok(addr) => addr,
        Err(page_id) => return Ok(Err(page_id)), // 环形缓冲区翻转，将精确 page_id 传回调用者
      };

      if hei.try_cas(new_addr) {
        return Ok(Ok(new_addr));
      }

      // CAS 冲突补偿：将未挂载成功的新分配槽位回收至复活池，自旋重试
      if self.store.config.enable_revivification {
        let rec_size = record_size(key.len(), val.len()) as u32;
        self
          .store
          .reviv_pool
          .put(new_addr, rec_size, self.store.hlog.read_only_address());
      }
      spin_loop();
    }
  }

  /// 纯同步快速路径写入当前会话普通字符串键（严格对标 C# Garnet NetworkSET & InternalUpsert）
  ///
  /// SET 语义同步清除既有 key 级 TTL 记录：TTL 记录驻留可变区时墓碑同步闭环；
  /// 需异步驱逐（PageNotReady）或冷数据确认时返回 Ok(Err(u64::MAX))，
  /// 交由调用方降级异步 upsert 路径闭环清除（杜绝残留 TTL 使新值被误判过期）
  #[inline(always)]
  pub fn try_upsert_sync(&self, user_key: &[u8], val: &[u8]) -> Result<StdResult<u64, u64>> {
    let ttl_k = self.ttl_key(user_key);
    if self.has_ttl_key(&ttl_k)? && self.try_delete_raw_sync(&ttl_k)?.is_err() {
      return Ok(Err(u64::MAX));
    }
    let str_k = self.session_string_key(user_key);
    self.try_upsert_raw_sync(&str_k, val)
  }

  /// 纯同步快速路径写入内核（调用方须已处于纪元保护下，批处理上下文专用）
  ///
  /// 语义与 [`Self::try_upsert_sync`] 完全一致：
  /// SET 语义同步清除既有 key 级 TTL 记录，TTL 清除需异步驱逐或冷数据确认时返回
  /// Ok(Err(u64::MAX))，环形缓冲区翻转时返回精确 page_id，均由调用方退出批处理
  /// 纪元后降级全异步路径闭环（纪元守卫绝不跨越任何可能磁盘 I/O 的 await）
  #[inline(always)]
  pub fn try_upsert_sync_unprotected(
    &self,
    user_key: &[u8],
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    let ttl_k = self.ttl_key(user_key);
    if self.has_ttl_key_unprotected(&ttl_k)?
      && self.try_delete_raw_sync_unprotected(&ttl_k)?.is_err()
    {
      return Ok(Err(u64::MAX));
    }
    let str_k = self.session_string_key(user_key);
    self.try_upsert_raw_sync_unprotected(&str_k, val)
  }

  /// 底层物理写入单个键值对（Upsert Raw）
  pub async fn upsert_raw(&self, key: &[u8], val: &[u8]) -> Result<u64> {
    loop {
      match self.try_upsert_raw_sync(key, val)? {
        Ok(addr) => return Ok(addr),
        Err(page_id) => {
          self.evict_pages_for(page_id).await?;
        }
      }
    }
  }

  /// 写入或更新当前会话普通字符串键（Upsert）
  ///
  /// SET 语义：同步清除既有 key 级 TTL 记录（Redis 字符串写入命令一律移除 TTL）；
  /// 内部元数据/分块写入必须走 upsert_raw，避免误清用户键 TTL
  #[inline(always)]
  pub async fn upsert(&self, user_key: &[u8], val: &[u8]) -> Result<u64> {
    self.del_ttl(user_key).await?;
    let str_k = self.session_string_key(user_key);
    self.upsert_raw(&str_k, val).await
  }

  /// 基于预先获得的首地址探针执行底层物理同步内存直读快路径（Raw，严格对照 C# Garnet InternalRead 与 FindTag）
  ///
  /// # 注意
  /// 调用方须确保当前线程处于 LightEpoch 纪元保护下。
  ///
  /// - **首项快速探针（FindTag）**：绝大多数情况下（99.9%）哈希索引首个槽位即命中，
  ///   直接进行单次内存记录解析并零拷贝执行闭包 `f` 返回，
  ///   彻底消除全桶 7 槽位原子加载（load(Acquire)）与候选数组构建开销。
  /// - **反向链表回溯（TraceBackForKeyMatch）**：遇到 15 位 Tag 碰撞时，沿着记录的
  ///   `prev_address` 反向链表回溯检查前驱版本；若未命中则回退到完整候选扫描路径保证 100% 正确性。
  /// - 返回值语义（三态，对齐 Garnet OperationStatus）：
  ///   - `Ok(Some(Some(val)))`：在内存页中精准命中并零拷贝执行闭包 `f` 返回（对应 Garnet `OperationStatus.SUCCESS`）。
  ///   - `Ok(Some(None))`：在内存中明确确认不存在（无候选 Tag，或最新记录为墓碑 Tombstone，对应 Garnet `OperationStatus.NOTFOUND`）。
  ///   - `Ok(None)`：内存中未找到匹配记录，但存在磁盘候选地址（`addr < head_address && addr >= begin_address`），需要调用方转入异步磁盘 I/O（对应 Garnet `OperationStatus.RECORD_ON_DISK`）。
  #[inline]
  pub fn try_read_raw_in_memory_with_addr<R>(
    &self,
    key: &[u8],
    first_addr: Option<u64>,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    let mut f = Some(f);
    match self.try_read_mem(key, first_addr, &mut f)? {
      MemRead::Done(res) => Ok(Some(res)),
      MemRead::OnDisk(_) => Ok(None),
    }
  }

  /// 底层物理同步内存直读快路径（Raw）
  #[inline]
  pub fn try_read_raw_in_memory<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    let _guard = self.participant.enter();
    let first_addr = self.store.index.find_tag(key);
    self.try_read_raw_in_memory_with_addr(key, first_addr, f)
  }

  /// 基于预先获得的首地址探针执行当前会话普通字符串同步内存直读快路径
  #[inline]
  pub fn try_read_in_memory_with_addr<R>(
    &self,
    user_key: &[u8],
    first_addr: Option<u64>,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    let str_k = self.session_string_key(user_key);
    self.try_read_raw_in_memory_with_addr(&str_k, first_addr, f)
  }

  /// 在已有纪元保护下执行当前会话普通字符串同步内存直读快路径（完全绕过 enter() 原子开销）
  #[inline]
  pub fn try_read_in_memory_unprotected<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    let str_k = self.session_string_key(user_key);
    let first_addr = self.store.index.find_tag(&str_k);
    self.try_read_raw_in_memory_with_addr(&str_k, first_addr, f)
  }

  /// 当前会话普通字符串同步内存直读快路径（严格对照 C# Garnet InternalRead 与 FindTag 实现）
  #[inline]
  pub fn try_read_in_memory<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    let _guard = self.participant.enter();
    self.try_read_in_memory_unprotected(user_key, f)
  }

  /// 内存直读核心路径（调用方须处于纪元保护下，严格对照 C# Garnet InternalRead 单遍分类）
  ///
  /// - **首项快速探针（FindTag）**：绝大多数情况下（99.9%）哈希索引首个槽位即命中，
  ///   直接进行单次内存记录解析并零拷贝执行闭包 `f` 返回。
  /// - **反向链表回溯（TraceBackForKeyMatch）**：遇到 15 位 Tag 碰撞时，沿着记录的
  ///   `prev_address` 反向链表回溯检查前驱版本；若未命中则回退到完整候选扫描路径保证 100% 正确性。
  /// - 磁盘候选地址单遍收集后经 `MemRead::OnDisk` 原样透传，冷读回退路径零重复索引遍历。
  #[inline]
  fn try_read_mem<R>(
    &self,
    key: &[u8],
    first_addr: Option<u64>,
    f: &mut Option<impl FnOnce(&[u8]) -> R>,
  ) -> Result<MemRead<R>> {
    let Some(first_addr) = first_addr else {
      // 哈希表中连对应 Tag 都完全不存在，100% 确认无此键，极速返回
      return Ok(MemRead::Done(None));
    };

    let mut curr_addr = first_addr;
    let head_addr = self.store.head_address();
    let begin_addr = self.store.begin_address();

    // 1. ReadCache 内存直读快路径（严格对标 Garnet ReadCache: DRAM 纳秒级纯内存直读）
    while is_read_cache_addr(curr_addr) {
      let mut next_addr = 0;
      let matched = self
        .store
        .read_cache
        .with_record(curr_addr, |rec_key, rec_val, prev| {
          next_addr = prev;
          if fast_key_eq(rec_key, key) {
            // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
            let func = unsafe { f.take().unwrap_unchecked() };
            Some(func(rec_val))
          } else {
            None
          }
        });
      if let Some(Some(val)) = matched {
        return Ok(MemRead::Done(Some(val)));
      }
      if next_addr == 0 {
        break;
      }
      curr_addr = next_addr;
    }

    // 2. 内存常态快路径（99%+ 场景）：处于 HLog 内存驻留区，直接单点探针与回溯
    if !is_read_cache_addr(curr_addr) && curr_addr >= head_addr {
      // 严格对照 C# Garnet TraceBackForKeyMatch 实现反向链表回溯
      while curr_addr >= head_addr {
        let probed = self.store.hlog.with_memory_record(curr_addr, |rec| {
          if rec.matches_key(key) {
            if rec.is_tombstone() {
              Ok(TraceBackResult::Tombstone)
            } else {
              // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
              let func = unsafe { f.take().unwrap_unchecked() };
              Ok(TraceBackResult::Found(func(rec.value())))
            }
          } else {
            // 发生 15 位 Tag 碰撞，沿反向链表回溯前驱版本（prev_address）
            Ok(TraceBackResult::TraceBack(rec.prev_address()))
          }
        })?;

        match probed {
          Some(TraceBackResult::Found(val)) => {
            // 不可变区命中：对齐 C# InternalRead.CopyFromImmutable 预提升挂入 ReadCache，
            // 后续读取直接命中纯 DRAM 缓存，免重复回溯主日志
            if !self.store.hlog.is_mutable(curr_addr) {
              self.promote_immutable_to_read_cache(curr_addr, key);
            }
            return Ok(MemRead::Done(Some(val)));
          }
          Some(TraceBackResult::Tombstone) => return Ok(MemRead::Done(None)),
          Some(TraceBackResult::TraceBack(next_addr)) if next_addr != 0 => {
            curr_addr = next_addr;
            continue;
          }
          _ => break,
        }
      }
    }

    // 3. 首项未命中、处于磁盘区或发生跨槽位 Tag 碰撞，回退到多候选扫描与落盘判定路径；
    //    curr_addr 为内存链回溯穷尽后的下一地址——链已伸入磁盘区时作为附加磁盘候选
    //    传递（严格对照 C# pending read 沿 prev 链从内存伸入磁盘的读取语义）
    self.try_read_mem_fallback(key, f, begin_addr, head_addr, curr_addr)
  }

  /// 多候选扫描与冷数据落盘判定路径（标记为 cold 避免内联膨胀热点指令缓存）
  ///
  /// 单次 `lookup_candidates` 遍历同时闭环内存 Tag 碰撞消解与磁盘候选收集，
  /// 磁盘候选按新版本优先降序经 `MemRead::OnDisk` 透传，供冷读回退路径直接消费。
  /// `chain_disk_addr` 为内存链回溯穷尽后的下一地址（< head 且 >= begin 时有效）。
  #[cold]
  fn try_read_mem_fallback<R>(
    &self,
    key: &[u8],
    f: &mut Option<impl FnOnce(&[u8]) -> R>,
    begin_addr: u64,
    head_addr: u64,
    chain_disk_addr: u64,
  ) -> Result<MemRead<R>> {
    let mut addrs = self.store.index.lookup_candidates(key);
    if addrs.is_empty() {
      // 无槽位候选（并发清退等瞬态）且内存链已伸入磁盘区：直读链出地址兜底；
      // 链仍在内存区或已穷尽为 0 则确认不存在
      if chain_disk_addr != 0 && chain_disk_addr >= begin_addr && chain_disk_addr < head_addr {
        let mut disk = CandidateAddresses::new();
        disk.push(chain_disk_addr);
        return Ok(MemRead::OnDisk(disk));
      }
      return Ok(MemRead::Done(None));
    }
    addrs.sort_descending();

    let mut disk_cands = CandidateAddresses::new();

    for &addr in addrs.iter() {
      let mut cur_addr = addr;
      if is_read_cache_addr(cur_addr) {
        let matched = self
          .store
          .read_cache
          .with_record(cur_addr, |rec_key, rec_val, _prev| {
            if fast_key_eq(rec_key, key) {
              // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
              let func = unsafe { f.take().unwrap_unchecked() };
              Some(func(rec_val))
            } else {
              None
            }
          });
        if let Some(Some(val)) = matched {
          return Ok(MemRead::Done(Some(val)));
        }
        cur_addr = self.store.read_cache.skip_read_cache(cur_addr);
        if cur_addr == 0 {
          continue;
        }
      }

      if cur_addr < begin_addr {
        continue;
      }

      if cur_addr < head_addr {
        disk_cands.push(cur_addr);
        continue;
      }

      if let Some(probe) = self.store.hlog.with_memory_record(cur_addr, |rec| {
        if rec.matches_key(key) {
          if rec.is_tombstone() {
            Ok(ReadProbeResult::Tombstone)
          } else {
            // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
            let func = unsafe { f.take().unwrap_unchecked() };
            Ok(ReadProbeResult::Found(func(rec.value())))
          }
        } else {
          Ok(ReadProbeResult::Miss(rec.prev_address()))
        }
      })? {
        match probe {
          ReadProbeResult::Found(val) => {
            // 不可变区命中：同样预提升挂入 ReadCache（与主路径口径一致）
            if !self.store.hlog.is_mutable(cur_addr) {
              self.promote_immutable_to_read_cache(cur_addr, key);
            }
            return Ok(MemRead::Done(Some(val)));
          }
          ReadProbeResult::Tombstone => return Ok(MemRead::Done(None)),
          // Tag 碰撞：前驱已滑入磁盘区时收集为冷读候选（同 tag 多链头的兜底）
          ReadProbeResult::Miss(prev) => {
            if prev != 0 && prev < head_addr && prev >= begin_addr {
              disk_cands.push(prev);
            }
            continue;
          }
        }
      } else {
        disk_cands.push(cur_addr);
      }
    }

    if disk_cands.is_empty() {
      Ok(MemRead::Done(None))
    } else {
      Ok(MemRead::OnDisk(disk_cands))
    }
  }

  /// 不可变区命中预提升：将内存驻留的只读记录挂入 ReadCache（严格对照 C# Garnet InternalRead.CopyFromImmutable）
  ///
  /// - ReadCache 为非脏 DRAM 日志，追加零持久化成本、零写放大；
  /// - 与磁盘读回填路径天然去重：提升后索引地址已 CAS 指向 RC，后续读取直接命中 RC，不再触达磁盘回填；
  /// - append 失败（未启用/单记录超页容量/CAS 冲突）静默降级，不影响正确性，后续读取仍走主日志路径；
  /// - 仅不可变区命中时调用：可变区记录本就是热数据，避免无谓的重复缓存占用。
  #[cold]
  fn promote_immutable_to_read_cache(&self, addr: u64, key: &[u8]) {
    if !self.store.read_cache.is_enabled {
      return;
    }
    // 独立第二次单遍记录访问：不可变区在纪元保护下无锁纯指针直读，无页锁嵌套；
    // 若期间已被驱逐出内存则返回 None，静默放弃本次提升
    let _ = self.store.hlog.with_memory_record(addr, |rec| {
      if !rec.is_tombstone()
        && rec.matches_key(key)
        && let Some(rc_addr) =
          self
            .store
            .read_cache
            .append(key, rec.value(), addr, &self.store.index)
      {
        let _ = self.store.index.update_address(key, addr, rc_addr);
      }
      Ok(())
    });
  }

  /// 同步内存直读快捷方法
  #[inline]
  pub fn read_in_memory(&self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
    self.try_read_in_memory(key, |v| v.to_vec())
  }

  /// 异步磁盘回退路径（仅当内存阶段确认存在磁盘候选地址时调用）
  ///
  /// - 候选列表由 `try_read_mem_fallback` 单遍索引扫描收集并降序透传，此处零重复索引遍历；
  /// - 磁盘 I/O 期间不持有纪元守卫（对标 C# IO 期间 `UnsafeSuspendThread`），
  ///   彻底消除单次冷读阻塞纪元推进与页回收的尾延迟隐患；
  /// - 读取走 `read_disk_record` 纯设备路径（不触碰内存页缓冲），无守卫读取安全性成立；
  /// - 磁盘链回溯：Tag 碰撞键不匹配时沿记录 `prev_address` 前驱链继续冷读
  ///   （严格对照 C# AllocatorBase.AsyncGetFromDiskCallback "skips colliding keys by
  ///   following the .PreviousAddress chain"；链地址在磁盘区内严格单调下降，
  ///   直至链尽 0 或低于截断线 begin_address）。
  #[cold]
  async fn read_from_disk<R>(
    &self,
    key: &[u8],
    addrs: CandidateAddresses,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    let mut f = Some(f);
    for &addr in addrs.iter() {
      let mut cur = addr;
      while cur >= self.store.begin_address() {
        let record = match self.store.hlog.read_disk_record(cur).await {
          Ok(r) => r,
          Err(e) => {
            if cur < self.store.begin_address() {
              break;
            }
            return Err(e.into());
          }
        };

        if !record.key().is_ok_and(|rec_key| fast_key_eq(rec_key, key)) {
          // Tag 碰撞：解析记录头提取前驱版本地址，磁盘链回溯
          cur = RecordHeader::from_slice(record.as_slice())
            .map(|h| h.address())
            .unwrap_or(0);
          continue;
        }
        if record.is_tombstone().unwrap_or(false) {
          return Ok(None);
        }
        let val_slice = record.value()?;
        // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
        let func = unsafe { f.take().unwrap_unchecked() };
        let result = func(val_slice);

        // 回填阶段重新进入纪元保护（ReadCache 挂链与索引地址更新均为共享内存结构变更）；
        // 严格对照 C# Garnet TryCopyToReadCache 与 TryCopyToTail：
        // 1. 若启用了 ReadCache，优先将冷数据挂入纯 DRAM 只读非脏页内存日志（零持久化开销、零写放大）；
        // 2. 否则若开启 copy_reads_to_tail，则回退到追加 Tail 内存活跃区晋升。
        // （匹配记录非链头时索引更新自然失配为 no-op，RC 挂链由环形覆盖自然回收）
        let _guard = self.participant.enter();
        if self.store.read_cache.is_enabled {
          if let Some(rc_addr) =
            self
              .store
              .read_cache
              .append(key, val_slice, cur, &self.store.index)
          {
            let _ = self.store.index.update_address(key, cur, rc_addr);
          }
        } else if self.copy_reads_to_tail()
          && let Ok(new_addr) = self.append_record(key, val_slice, cur, false).await
          && !self.store.index.update_address(key, cur, new_addr)
          && self.store.config.enable_revivification
        {
          let rec_size = record_size(key.len(), val_slice.len()) as u32;
          self
            .store
            .reviv_pool
            .put(new_addr, rec_size, self.store.hlog.read_only_address());
        }

        return Ok(Some(result));
      }
    }

    Ok(None)
  }

  /// 底层无包装零拷贝读取物理键的值（Read Raw With Closure）
  ///
  /// 无 TTL 守卫的裸读内核：load_meta / ttl_of / contains_key_ignore_ttl 等已在上层
  /// 完成 TTL 裁决的调用链统一走此处，绝不嵌套二次裁决（读路径 TTL 探测收敛不变式：
  /// 同一同步调用链内同一用户键的 TTL 裁决只在唯一入口做一次）。
  ///
  /// - **同步内存直读快路径**：首先尝试纯同步内存零拷贝直读（`try_read_raw_in_memory`），
  ///   若在内存中精准命中或确认不存在（墓碑/无候选），纳秒级同步返回，彻底规避异步 Future 状态机开销。
  /// - **异步磁盘扫描回退**：仅当数据位于磁盘区时，才进入 `read_from_disk` 异步 I/O 等待。
  #[inline]
  pub async fn read_raw_with<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    let mut f = Some(f);
    let cands = {
      let _guard = self.participant.enter();
      let first_addr = self.store.index.find_tag(key);
      match self.try_read_mem(key, first_addr, &mut f)? {
        MemRead::Done(res) => return Ok(res),
        MemRead::OnDisk(cands) => cands,
      }
    };
    // SAFETY: try_read_mem 返回 OnDisk 时闭包 f 未被消费，必为 Some
    let func = unsafe { f.take().unwrap_unchecked() };
    self.read_from_disk(key, cands, func).await
  }

  /// 零拷贝读取当前会话普通字符串键的值（Read With Closure）
  ///
  /// 命中后惰性探测 key 级 TTL 记录；已过期则经统一 DEL 路径物理清除并视同不存在
  /// （check_expired 内部仅走 raw 路径，无递归）。read/read_string/type_of/load_object
  /// 等入口均经此处获得惰性过期语义。
  /// f 可能带副作用（如直写响应缓冲），TTL 裁决必须前移到闭包执行前，
  /// 杜绝过期键"先执行读闭包后回 None"的双写；has_ttl_tag 为单次哈希探针，
  /// 无 TTL 记录时保持快路径零额外 I/O。本守卫是本键在整条同步调用链内的
  /// 唯一 TTL 裁决点，内部裸读（read_raw_with）不再重复探测
  #[inline]
  pub async fn read_with<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    if self.has_ttl_tag(user_key)? && self.check_expired(user_key).await? {
      return Ok(None);
    }
    let str_k = self.session_string_key(user_key);
    self.read_raw_with(&str_k, f).await
  }

  /// 底层无包装物理读取（原始 Key-Value 读取）
  #[inline]
  pub async fn read_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    self.read_raw_with(key, |v| v.to_vec()).await
  }

  /// 读取当前会话普通字符串键的值（Read）
  ///
  /// - 优先内存驻留零拷贝探针：直接在页上比对键，命中时直接提取值切片，彻底消除整记录拷贝
  /// - 命中后经 read_with 惰性探测 TTL 记录，已过期物理清除并视同不存在
  #[inline]
  pub async fn read(&self, user_key: &[u8]) -> Result<Option<Vec<u8>>> {
    self.read_with(user_key, |v| v.to_vec()).await
  }

  /// 批量分块回调索引平移适配：把底层相对本块切片的 idx 平移 chunk 基址后转发，
  /// 维持"idx 相对调用方全量键列表"的外部契约（返回 impl FnMut 保持 HRTB 借用直通）
  fn offset_batch_idx<'f>(
    cb: &'f mut (impl FnMut(usize, Option<&[u8]>) + ?Sized),
    base: usize,
  ) -> impl FnMut(usize, Option<&[u8]>) + 'f {
    move |i, v| cb(base + i, v)
  }

  /// 批量读取当前会话普通字符串记录（严格对照 C# Garnet ContextReadWithPrefetch 实现 12 项硬件流水线预取）
  ///
  /// 按预取窗口常量分块，`TaggedKeyBuf`（Copy）在单个栈上数组内编码后逐块调用底层批量读，
  /// 任意批量规模全程零堆物化（仅磁盘候选收割冷路径按需分配）。
  pub async fn read_batch_with<K, F>(&self, keys: &[K], mut on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    let prefix = self.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut stack_keys = [const { TaggedKeyBuf::new() }; BATCH_READ_PREFETCH_SIZE];
    // 契约：on_item 的 idx 相对调用方全量键列表（底层按本次调用键切片编号），
    // 分块调用必须回填 chunk 基址，否则多块时 idx 重复从 0 起算
    for (chunk_ix, chunk) in keys.chunks(BATCH_READ_PREFETCH_SIZE).enumerate() {
      let chunk_base = chunk_ix * BATCH_READ_PREFETCH_SIZE;
      for (stack_k, k) in stack_keys.iter_mut().zip(chunk.iter()) {
        *stack_k =
          NamespaceDbCodec::encode_with_session_prefix(prefix_slice, KeyTag::String, k.as_ref());
      }
      let mut f = Self::offset_batch_idx(&mut on_item, chunk_base);
      self
        .read_batch_raw_with(&stack_keys[..chunk.len()], &mut f)
        .await?;
    }
    Ok(())
  }

  /// 底层物理批量读取记录（Raw）
  ///
  /// # 回调次序契约
  /// `on_item` 严格按 idx 升序对每个键恰好各回调一次；任一磁盘候选收割失败时以
  /// `Err` 中止交付（首个磁盘候选之前的内存项已先行交付、无法回滚，调用方须整体
  /// 丢弃本批部分结果）。`read_batch_with` 分块串行推进，跨块亦保持全局升序。
  /// MGET 线上协议（`mget_each` 无 idx 参数、按回调序对位写响应）依赖此契约。
  pub async fn read_batch_raw_with<K, F>(&self, keys: &[K], mut on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    let count = keys.len();
    if count == 0 {
      return Ok(());
    }

    let mask = self.store.index.mask;
    let mut next_batch_ix = 0;
    // 批间复用同一缓冲区：仅首次命中磁盘候选时分配一次，多批次 MGET 场景消除反复分配
    let mut pending: Vec<(usize, CandidateAddresses)> = Vec::new();

    while next_batch_ix < count {
      let batch_len = (count - next_batch_ix).min(BATCH_READ_PREFETCH_SIZE);
      let mut hashes = [0u64; BATCH_READ_PREFETCH_SIZE];
      let mut first_addrs = [None; BATCH_READ_PREFETCH_SIZE];

      let guard = Some(self.participant.enter());

      // 1. 第一级硬件预取：计算 64 位哈希值并将对应哈希桶（64 字节）拉入 CPU L1 数据缓存
      for (i, hash_slot) in hashes[..batch_len].iter_mut().enumerate() {
        let key = unsafe { keys.get_unchecked(next_batch_ix + i) }.as_ref();
        let hash = HashIndex::hash_key(key);
        *hash_slot = hash;
        let bucket_idx = (hash as usize) & mask;
        let bucket_ptr = unsafe { self.store.index.buckets.as_ptr().add(bucket_idx) };
        prefetch_read_l1(bucket_ptr);
      }

      // 2. 第二级硬件预取：探测 FindTag 对应地址，若驻留在内存有效区间，预取记录物理内存
      let head_addr = self.store.head_address();
      let tail_addr = self.store.hlog.tail_address();
      for (i, addr_slot) in first_addrs[..batch_len].iter_mut().enumerate() {
        let hash = hashes[i];
        let addr_opt = self.store.index.find_tag_by_hash(hash);
        *addr_slot = addr_opt;
        if let Some(addr) = addr_opt
          && addr >= head_addr
          && addr < tail_addr
        {
          let phys_ptr = unsafe { self.store.hlog.get_physical_address(addr) };
          prefetch_read_l1(phys_ptr);
        }
      }

      // 3. 内存快路径同步直读；磁盘候选统一收集后并发提交批量收割
      //    （对标 C# CompletePending：compio 完成制模型下首轮 poll 提交、单线程重叠多路磁盘 I/O）
      pending.clear();
      // 混批保序暂存：本批一旦出现磁盘候选，其后命中的内存结果必须拷贝暂存
      //（纪元守卫释放后页内借引用即刻失效），待磁盘收割完成后合流按 idx 升序交付；
      // Vec::new 零分配，纯内存批（绝大多数）全程零拷贝直通
      let mut buffered: Vec<(usize, Option<Vec<u8>>)> = Vec::new();

      for (i, &first_addr) in first_addrs[..batch_len].iter().enumerate() {
        let item_idx = next_batch_ix + i;
        let key = unsafe { keys.get_unchecked(item_idx) }.as_ref();

        let disk_mixed = !pending.is_empty();
        let mut buffered_val = None;
        let mut item_f = Some(|v: &[u8]| {
          if disk_mixed {
            // 混批：值切片仅在纪元守卫存活期有效，收割等待期前必须拷贝为所有权值
            buffered_val = Some(v.to_vec());
          } else {
            on_item(item_idx, Some(v));
          }
        });
        match self.try_read_mem(key, first_addr, &mut item_f)? {
          MemRead::Done(Some(())) => {}
          MemRead::Done(None) if disk_mixed => buffered.push((item_idx, None)),
          MemRead::Done(None) => on_item(item_idx, None),
          MemRead::OnDisk(cands) => pending.push((item_idx, cands)),
        }
        if let Some(v) = buffered_val {
          buffered.push((item_idx, Some(v)));
        }
      }

      // 磁盘读提交前一次性释放纪元守卫（收集候选为纯本地操作无需让出），
      // 防止长时间 I/O 阻塞纪元推进
      drop(guard);

      if !pending.is_empty() {
        // 并发收割全部磁盘读（冷路径物化 Vec，规避跨并发闭包共享 FnMut）
        let futs = pending.drain(..).map(|(item_idx, cands)| async move {
          let key = unsafe { keys.get_unchecked(item_idx) }.as_ref();
          let val = self.read_from_disk(key, cands, |v| v.to_vec()).await?;
          Ok::<_, Error>((item_idx, val))
        });
        // 任一磁盘读失败即刻上抛中止交付（首个磁盘候选之前的内存项已先行交付，
        // 调用方须整体丢弃本批部分结果，契约详见方法注释）
        let mut disk_vals: Vec<(usize, Option<Vec<u8>>)> = Vec::with_capacity(batch_len);
        for res in join_all(futs).await {
          disk_vals.push(res?);
        }

        // 归并交付：磁盘候选之后的内存命中暂存与磁盘收割结果合流，
        // 按 idx 升序统一回调（严格对照 Redis MGET：结果顺序恒等于请求顺序、
        // 缺失键为 nil；MGET 线上协议按回调序对位写响应，故必须严格升序），
        // 合流规模上界 2×预取窗口，排序成本在磁盘 I/O 冷路径下可忽略
        disk_vals.extend(buffered);
        disk_vals.sort_unstable_by_key(|&(idx, _)| idx);
        for (item_idx, val) in disk_vals {
          on_item(item_idx, val.as_deref());
        }
      }

      next_batch_ix += batch_len;
    }

    Ok(())
  }

  /// 同步纯内存批量直读当前会话普通字符串记录（严格对照 C# Garnet ContextReadWithPrefetch 12 项流水线预取）
  ///
  /// 与异步版一致按预取窗口常量分块栈上编码，任意批量规模全程零堆分配。
  pub fn try_read_batch_in_memory<K, F>(&self, keys: &[K], mut on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    let prefix = self.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut stack_keys = [const { TaggedKeyBuf::new() }; BATCH_READ_PREFETCH_SIZE];
    // 契约同 read_batch_with：idx 相对调用方全量键列表，分块调用回填 chunk 基址
    for (chunk_ix, chunk) in keys.chunks(BATCH_READ_PREFETCH_SIZE).enumerate() {
      let chunk_base = chunk_ix * BATCH_READ_PREFETCH_SIZE;
      for (stack_k, k) in stack_keys.iter_mut().zip(chunk.iter()) {
        *stack_k =
          NamespaceDbCodec::encode_with_session_prefix(prefix_slice, KeyTag::String, k.as_ref());
      }
      let mut f = Self::offset_batch_idx(&mut on_item, chunk_base);
      self.try_read_batch_raw_in_memory(&stack_keys[..chunk.len()], &mut f)?;
    }
    Ok(())
  }

  /// 底层物理同步纯内存批量直读（Raw，严格对照 C# Garnet ContextReadWithPrefetch 12 项流水线预取）
  ///
  /// - 适用于纯内存驻留读场景或快速内存筛选；
  /// - 若记录处于内存中且存在，调用 `on_item(idx, Some(val))`；
  /// - 若记录不存在、为墓碑或已落盘驱逐，调用 `on_item(idx, None)`；
  /// - 全程纯同步调用栈执行，无任何堆分配与异步开销。
  pub fn try_read_batch_raw_in_memory<K, F>(&self, keys: &[K], mut on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    let count = keys.len();
    if count == 0 {
      return Ok(());
    }

    let _guard = self.participant.enter();
    let mask = self.store.index.mask;
    let head_addr = self.store.head_address();
    let tail_addr = self.store.hlog.tail_address();

    let mut next_batch_ix = 0;
    while next_batch_ix < count {
      let batch_len = (count - next_batch_ix).min(BATCH_READ_PREFETCH_SIZE);
      let mut hashes = [0u64; BATCH_READ_PREFETCH_SIZE];
      let mut first_addrs = [None; BATCH_READ_PREFETCH_SIZE];

      // 1. 第一级预取：哈希桶
      for (i, hash_slot) in hashes[..batch_len].iter_mut().enumerate() {
        let key = unsafe { keys.get_unchecked(next_batch_ix + i) }.as_ref();
        let hash = HashIndex::hash_key(key);
        *hash_slot = hash;
        let bucket_idx = (hash as usize) & mask;
        let bucket_ptr = unsafe { self.store.index.buckets.as_ptr().add(bucket_idx) };
        prefetch_read_l1(bucket_ptr);
      }

      // 2. 第二级预取：记录物理地址
      for (i, addr_slot) in first_addrs[..batch_len].iter_mut().enumerate() {
        let hash = hashes[i];
        let addr_opt = self.store.index.find_tag_by_hash(hash);
        *addr_slot = addr_opt;
        if let Some(addr) = addr_opt
          && addr >= head_addr
          && addr < tail_addr
        {
          let phys_ptr = unsafe { self.store.hlog.get_physical_address(addr) };
          prefetch_read_l1(phys_ptr);
        }
      }

      // 3. 执行底层物理同步内存读取
      for (i, &first_addr) in first_addrs[..batch_len].iter().enumerate() {
        let item_idx = next_batch_ix + i;
        let key = unsafe { keys.get_unchecked(item_idx) }.as_ref();

        match self
          .try_read_raw_in_memory_with_addr(key, first_addr, |v| on_item(item_idx, Some(v)))?
        {
          Some(Some(())) => {}
          _ => on_item(item_idx, None),
        }
      }

      next_batch_ix += batch_len;
    }

    Ok(())
  }

  /// 纯同步快速路径物理删除单个键（严格对标 C# Garnet NetworkDEL & InternalDelete）
  ///
  /// 语义与安全边界详见 [`Self::try_delete_raw_sync_unprotected`]
  pub fn try_delete_raw_sync(&self, key: &[u8]) -> Result<StdResult<bool, u64>> {
    let _guard = self.participant.enter();
    self.try_delete_raw_sync_unprotected(key)
  }

  /// 纯同步快速路径物理删除内核（调用方须已处于纪元保护下，批处理上下文专用）
  ///
  /// - 纯内存单趟执行：若记录在可变区且无前驱，直接 CAS 脱钩并回收至 FreeRecordPool（Record Elision），零追加返回；
  /// - 若记录在可变区有前驱，直接就地标记墓碑（In-Place Tombstone），零追加返回；
  /// - 若记录处于只读区或冷数据区，盲追加一条 0 字节墓碑记录（Blind Tombstone Append）并 CAS 挂载；
  /// - 100% 免除 Tokio 协程让出与 Future 构造；
  /// - 仅当追加墓碑遭遇环形缓冲区翻转（PageNotReady）时返回 Ok(Err(page_id))，交由外层异步驱逐；
  /// - 冷数据需磁盘确认时返回 Ok(Err(u64::MAX))，调用方降级全异步路径。
  ///
  /// # 设计边界（对标 C# TrySeal 的刻意裁剪）
  /// Record Elision 路径未移植 C# `InfoRef.TrySeal(invalidate: true)` 的记录头密封协议
  /// （裁决见 wreviv::FreeRecord 并发模型注释）：脱钩后槽位立即入池，可能在其他核心
  /// 的在途读者完成解析前被复活改写。安全性依赖 wreviv 声明的"单写者 + 页写锁 +
  /// epoch 保护"前提，且需 `enable_revivification` 与会话级 `record_elision` 双重显式
  /// 开启（均默认关闭）；未来多核高争用场景需先在 whlog/wrecord 层落地原子
  /// TrySeal 语义，再放宽本前提。
  fn try_delete_raw_sync_unprotected(&self, key: &[u8]) -> Result<StdResult<bool, u64>> {
    loop {
      let begin_addr = self.store.begin_address();
      let mut hei = self
        .store
        .index
        .find_or_create_tag_with_min_addr(key, begin_addr)?;

      if !hei.is_found() {
        return Ok(Ok(false));
      }

      let addr = hei.address();
      // ReadCache 链头分流（严格对标 C# InternalDelete.cs：TryFindRecordForUpdate 的
      // 键匹配链遍历 + HasReadCacheSrc → CreateNewRecord 盲插墓碑）
      let mut cur = addr;
      if is_read_cache_addr(addr) {
        let matched = self
          .store
          .read_cache
          .with_record(addr, |rec_key, _rec_val, _prev| fast_key_eq(rec_key, key));
        let real_prev = self.store.read_cache.skip_read_cache(addr);
        if real_prev == 0 {
          // 链头条目刚滑出窗口（驱逐方 cleanse 尚未把槽位恢复为主日志地址）：
          // 槽位此刻承载的是 Tag 碰撞键的存活缓存条目，elide 会令碰撞键不可达（丢键），
          // 以 prev=0 盲插会截断其主日志链——自旋重读等待槽位恢复
          spin_loop();
          continue;
        }
        if matched == Some(true) {
          // 链头 ReadCache 缓存即本键存活记录：盲追加墓碑链接主日志前驱并 CAS 挂载
          // （前驱低于截断线无碍：读取墓碑即终止，绝不解引用前驱）
          let new_addr = match self.try_allocate_or_append_record_sync(key, &[], real_prev, true)? {
            Ok(a) => a,
            Err(page_id) => return Ok(Err(page_id)),
          };
          if hei.try_cas(new_addr) {
            return Ok(Ok(true));
          }
          if self.store.config.enable_revivification {
            let rec_size = record_size(key.len(), 0) as u32;
            self
              .store
              .reviv_pool
              .put(new_addr, rec_size, self.store.hlog.read_only_address());
          }
          spin_loop();
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

      // 全内存区链条回溯探查（从链头主日志地址 cur 回溯至 head_addr）
      let mut key_status = None; // None: 记录可能在磁盘; Some(true): 存在有效记录; Some(false): 键不存在或已是墓碑

      while cur >= head_addr {
        let probe = self.store.hlog.with_memory_record(cur, |rec| {
          if rec.matches_key(key) {
            Ok((
              true,
              rec.is_tombstone(),
              rec.prev_address(),
              rec.total_size(),
            ))
          } else {
            Ok((false, false, rec.prev_address(), 0))
          }
        });

        match probe {
          Ok(Some((true, is_tomb, prev, size))) => {
            if is_tomb {
              key_status = Some(false);
            } else {
              // 命中有效记录：检查能否 Record Elision
              // （cur == addr 判定链头：ReadCache 槽位分支已提前分流，此处 addr 恒为
              // 主日志地址；存活缓存条目绝不参与脱钩，避免令键不可达）
              if self.store.config.enable_revivification
                && self.record_elision()
                && cur == addr
                && cur >= read_only_addr
                && (prev == 0 || prev < begin_addr)
              {
                if hei.try_elide() {
                  self.store.reviv_pool.put(cur, size as u32, read_only_addr);
                  return Ok(Ok(true));
                }
                spin_loop();
                continue;
              }
              key_status = Some(true);
            }
            break;
          }
          Ok(Some((false, _, prev, _))) => {
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
      let prev_link = if addr < begin_addr { 0 } else { addr };
      let new_addr = match self.try_allocate_or_append_record_sync(key, &[], prev_link, true)? {
        Ok(a) => a,
        Err(page_id) => return Ok(Err(page_id)),
      };

      if hei.try_cas(new_addr) {
        return Ok(Ok(true));
      }

      // CAS 冲突补偿：回收至复活池，自旋重试
      if self.store.config.enable_revivification {
        let rec_size = record_size(key.len(), 0) as u32;
        self
          .store
          .reviv_pool
          .put(new_addr, rec_size, read_only_addr);
      }
      spin_loop();
    }
  }

  /// 纯同步快速删除键（支持普通键快速路径，严格对标 Garnet NetworkDEL）
  ///
  /// - 若为普通键且内存命中：纯同步直接返回 Ok(Ok(deleted))；
  /// - 若遭遇环形页翻转：返回 Ok(Err(page_id))；
  /// - 若属于复合对象元数据：返回 Ok(Err(u64::MAX)) 指示调用方降级走完整异步路由。
  #[inline]
  pub fn try_delete_sync(&self, user_key: &[u8]) -> Result<StdResult<bool, u64>> {
    match self.check_object_meta_fast(user_key)? {
      Some(false) => {
        let str_k = self.session_string_key(user_key);
        self.try_delete_raw_sync(&str_k)
      }
      _ => Ok(Err(u64::MAX)),
    }
  }

  /// 墓碑标记底层物理删除单个键（Delete Raw）
  pub async fn delete_raw(&self, key: &[u8]) -> Result<bool> {
    loop {
      match self.try_delete_raw_sync(key)? {
        Ok(deleted) => return Ok(deleted),
        Err(page_id) if page_id == u64::MAX => {
          return self.delete_raw_disk_slow(key).await;
        }
        Err(page_id) => {
          self.evict_pages_for(page_id).await?;
        }
      }
    }
  }

  /// 磁盘冷数据异步删除慢路径
  ///
  /// ReadCache 条目顺链解析为首个主日志地址（键归属由 fast_key_eq 校验兜底）后，
  /// 沿记录 `prev_address` 前驱链回溯（严格对照 C# AllocatorBase.AsyncGetFromDiskCallback
  /// 沿链跳过碰撞键语义），确保被 Tag 碰撞键掩埋的冷记录也能真实删除。
  /// 盲墓碑以「链头（槽位地址）」为前驱追加并 CAS 槽位：哈希链 prev 语义即
  /// 「插入时刻的槽位地址」，与快路径 prev_link 口径一致；碰撞键经墓碑前驱仍可达。
  async fn delete_raw_disk_slow(&self, key: &[u8]) -> Result<bool> {
    let _guard = self.participant.enter();
    let begin_addr = self.store.begin_address();
    let addrs = self.store.index.lookup_candidates(key);
    for cand in addrs {
      // cand 为槽位原始地址（可能为 ReadCache 虚拟地址）：墓碑 CAS 挂载必须以它为
      // old_address；prev 链接顺链解析后的首个主日志地址（跳过易失 RC 环节）
      let main_head = self.store.read_cache.skip_read_cache(cand);
      if main_head == 0 {
        continue;
      }
      let mut cur = main_head;
      while cur >= begin_addr {
        let record = match self.store.hlog.read_record(cur).await {
          Ok(r) => r,
          Err(_) => break,
        };
        if !record.key().is_ok_and(|rec_key| fast_key_eq(rec_key, key)) {
          cur = RecordHeader::from_slice(record.as_slice())
            .map(|h| h.address())
            .unwrap_or(0);
          continue;
        }
        if record.is_tombstone().unwrap_or(false) {
          return Ok(false);
        }
        let new_addr = self.append_record(key, &[], main_head, true).await?;
        if self.store.index.update_address(key, cand, new_addr) {
          return Ok(true);
        }
        break;
      }
    }
    Ok(false)
  }

  /// 底层检查指定物理键是否存在且未被墓碑删除（Contains Key Raw）
  /// 对标 C# Garnet InternalContainsKeyInMemory 与完整读路径：
  /// 基于 zero-copy 闭包读取，0 堆分配，严格沿 prev_address 反向链回溯处理 Tag 碰撞
  #[inline]
  pub async fn contains_key_raw(&self, key: &[u8]) -> Result<bool> {
    Ok(self.read_raw_with(key, |_| ()).await?.is_some())
  }

  /// 在当前会话纪元保护下按逻辑地址直接读取记录（对标 Tsavorite ReadAtAddress）
  pub async fn read_record(&self, addr: u64) -> Result<whlog::RecordOutput> {
    let _guard = self.participant.enter();
    self.store.hlog.read_record(addr).await.map_err(Into::into)
  }
}
