//! 物理键单点读取路径（对标 C# Garnet ClientSession 的 Read 快慢路径）

use wdev::Device;
use windex::{CandidateAddresses, HashBucketEntry};
use wrecord::{RecordHeader, fast_key_eq, record_size};

use super::{MemRead, ReadProbeResult, TraceBackResult};
use crate::{error::Result, read_cache::is_read_cache_addr, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 基于预先获得的首地址探针执行底层物理同步内存直读快路径（Raw，严格对照 Tsavorite InternalRead 与 FindTag 探针）
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
    let mut first_addr = first_addr;
    loop {
      match self.try_read_mem(key, first_addr, &mut f)? {
        MemRead::Done(res) => return Ok(Some(res)),
        MemRead::OnDisk(_) => return Ok(None),
        MemRead::Retry => {
          // C# RETRY_LATER（InternalRead.cs:105-106）：刷新纪元（ProtectAndDrain 语义，
          // 推进密封在途记录的写者完成 CAS 解封）后整链重试——对齐 C# 会话层
          // HandleOperationStatus 的「Refresh the epoch and retry」协议
          self.participant.refresh();
          first_addr = self.store.index.find_tag(key);
        }
      }
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

  /// 当前会话普通字符串同步内存直读快路径（严格对照 Tsavorite InternalRead 与 FindTag 快路径实现）
  #[inline]
  pub fn try_read_in_memory<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    let _guard = self.participant.enter();
    self.try_read_in_memory_unprotected(user_key, f)
  }

  /// 在已有纪元保护下同步读取当前会话普通字符串键（TTL 快门控 + 内存直读，彻底绕过 enter() 原子开销）
  ///
  /// 返回三态（严格对标 Garnet 同步读状态）：
  /// - `Ok(Some(Some(r)))`：内存命中，闭包零拷贝消费（对应 OperationStatus.SUCCESS）；
  /// - `Ok(Some(None))`：内存中明确不存在（无候选 Tag 或最新记录为墓碑，对应 OperationStatus.NOTFOUND）；
  /// - `Ok(None)`：须降级全异步 `read_with().await`（存在磁盘候选，或命中 TTL 标签需异步过期裁决/物理清除，对应 OperationStatus.RECORD_ON_DISK / RETRY）。
  #[inline]
  pub fn try_read_sync_unprotected<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    if self.has_ttl_tag_unprotected(user_key)? {
      // 命中 TTL 标签：同步内存路径无法执行异步过期物理删除（purge_expired），降级异步读裁决
      return Ok(None);
    }
    self.try_read_in_memory_unprotected(user_key, f)
  }

  /// 同步读取当前会话普通字符串键（TTL 快门控 + 内存直读，严格对标 Garnet 同步读取路径）
  #[inline]
  pub fn try_read_sync<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    let _guard = self.participant.enter();
    self.try_read_sync_unprotected(user_key, f)
  }

  /// 内存直读核心路径（调用方须处于纪元保护下，严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:InternalRead 单遍分类）
  ///
  /// - **首项快速探针（FindTag）**：绝大多数情况下（99.9%）哈希索引首个槽位即命中，
  ///   直接进行单次内存记录解析并零拷贝执行闭包 `f` 返回。
  /// - **反向链表回溯（TraceBackForKeyMatch）**：遇到 15 位 Tag 碰撞时，沿着记录的
  ///   `prev_address` 反向链表回溯检查前驱版本；每跳按 C# FindRecord.cs:101-103
  ///   IsValidTracebackRecord 口径消费 is_valid/is_closed——密封在途记录参与键比对，
  ///   命中后按 IsClosedOrTombstoned（InternalRead.cs:118-131）降级 RETRY_LATER，
  ///   由调用方刷新纪元后整链重试；
  ///   若未命中则回退到完整候选扫描路径保证 100% 正确性。
  /// - 磁盘候选地址单遍收集后经 `MemRead::OnDisk` 原样透传，冷读回退路径零重复索引遍历。
  #[inline]
  pub(super) fn try_read_mem<R>(
    &self,
    key: &[u8],
    first_addr: Option<u64>,
    f: &mut Option<impl FnOnce(&[u8]) -> R>,
  ) -> Result<MemRead<R>> {
    let Some(mut curr_addr) = first_addr else {
      // 哈希表中连对应 Tag 都完全不存在，100% 确认无此键，极速返回
      return Ok(MemRead::Done(None));
    };

    // 优化5（严格对标 Helpers.cs:FindTagAndTryEphemeralSLock）：复活相关配置开启时，
    // traceback 前以共享 latch 锁住链首桶，防止复活/脱钩在回溯期间把记录从链上抽走
    // （"Ephemeral must lock the bucket before traceback"）；锁成功后重读当前链首槽位
    // （对标 HashEntryInfo.SetToCurrent），锁失败按 C# RETRY_LATER 协议交由调用方
    // 刷新纪元后整链重试。复活功能全关时无抽链风险，维持现状免锁（语义等价裁剪）
    let _s_latch = if self.ephemeral_lock_enabled() {
      let hash = whasher::fast_hash(key);
      let bucket = self
        .store
        .index
        .bucket(self.store.index.bucket_index_for_hash(hash));
      let Some(latch) = bucket.lock_shared_guard() else {
        return Ok(MemRead::Retry);
      };
      // SetToCurrent：定位 Tag 与加锁之间槽位可能已被并发 CAS/脱钩改写，
      // 链首槽位不再持有原地址时重读当前条目
      let tag = HashBucketEntry::tag_from_hash(hash);
      if bucket.find_entry_by_address(tag, curr_addr).is_none()
        && let Some(hei) = self.store.index.find_tag_entry(key)
      {
        curr_addr = hei.address();
      }
      Some(latch)
    } else {
      None
    };
    let head_addr = self.store.head_address();
    let begin_addr = self.store.begin_address();
    // 免锁直读门槛快照：对标 InternalRead.cs:114-124 可变区甚至模糊区（>= SafeReadOnlyAddress）
    // 的无锁 CreateLogRecord 直读——门槛 read_only 覆盖模糊区 [safe_read_only, read_only)，
    // 撕裂安全由 wrecord 头 RDH 单 8 字节原子字发布协议保证（whlog::probe_resident 文档
    // 第 2/3 条）；真可变区 [read_only, tail) 走保守页读锁与原位更新互斥。
    // 偏旧快照只会把可变区记录误判入页锁路径（方向安全）；偏新不可能（Acquire 载入单调原子）。
    let ro_addr = self.store.hlog.read_only_address();
    // 不可变区命中提升门槛快照：严格对标 C# CopyFromImmutable 仅作用于不可变区
    // [HeadAddress, SafeReadOnlyAddress)（InternalRead.cs:126-139 immutable 分支），
    // 模糊区瞬态窗口内不提升（保守方向正确性无损）
    let safe_ro_addr = self.store.safe_read_only_address();

    // 1. ReadCache 内存直读快路径（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:DRAM 纳秒级纯内存直读）
    while is_read_cache_addr(curr_addr) {
      // 读侧驱逐等待协议（严格对标 ReadCache.cs:ReadCacheNeedToWaitForEviction）：
      // 地址滑出 RC 环形窗口（abs < head）时短自旋等待驱逐方 cleanse 完成并发布
      // ClosedUntilAddress，再按 UpdateRecordSourceToCurrentHashEntry 语义回链头重探
      // （RestartChain）——否则本次读会被误判为 NOTFOUND（瞬态一致性缺口）
      if self
        .store
        .read_cache
        .need_to_wait_for_eviction(curr_addr, || self.participant.refresh())
      {
        return Ok(MemRead::Retry);
      }
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
      curr_addr = next_addr;
      if curr_addr == 0 {
        break;
      }
    }

    // 2. 内存常态快路径（99%+ 场景）：处于 HLog 内存驻留区，单次快照三分区判定后直读与回溯
    if !is_read_cache_addr(curr_addr) && curr_addr >= head_addr {
      // 严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindRecord.cs:TraceBackForKeyMatch 实现反向链表回溯：
      // 每跳 IsValidTracebackRecord 口径消费 is_closed（密封在途记录参与键比对，
      // 命中降级 RETRY_LATER）；read_only 之前的记录走纯指针直读（CreateLogRecord +
      // GetPhysicalAddress 口径），真可变区保留页锁探针
      while curr_addr >= head_addr {
        // 探针闭包以字面量直接内联传入两分支（不用 &mut 提取：间接层会阻断
        // with_*_record 与闭包的一体化内联，热点工况实测退化 ~12%）
        let probed = if curr_addr < ro_addr {
          // SAFETY: 调用方纪元保护 + curr_addr ∈ [head_addr, ro_addr) 均为进入前
          // 快照，双门槛契约见 with_immutable_record 文档；该分区驻留由快照门槛保证，
          // 直接产出探针结果（包装 Some 与页锁分支的 Option 口径对齐）
          Some(unsafe {
            self.store.hlog.with_immutable_record(curr_addr, |rec| {
              if rec.matches_key(key) {
                // C# InternalRead.cs:118 IsClosedOrTombstoned：closed → RETRY_LATER，
                // tombstone → NOTFOUND（closed 优先于 tombstone 判定）
                if rec.is_closed() {
                  Ok(TraceBackResult::Retry)
                } else if rec.is_tombstone() {
                  Ok(TraceBackResult::Tombstone)
                } else {
                  // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
                  let func = f.take().unwrap_unchecked();
                  Ok(TraceBackResult::Found(func(rec.value())))
                }
              } else {
                // 发生 15 位 Tag 碰撞，沿反向链表回溯前驱版本（prev_address）
                Ok(TraceBackResult::TraceBack(rec.prev_address()))
              }
            })
          }?)
        } else {
          self.store.hlog.with_memory_record(curr_addr, |rec| {
            if rec.matches_key(key) {
              if rec.is_closed() {
                Ok(TraceBackResult::Retry)
              } else if rec.is_tombstone() {
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
          })?
        };

        match probed {
          Some(TraceBackResult::Found(val)) => {
            // 不可变区命中：对齐 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:CopyFromImmutable 预提升挂入 ReadCache，
            // 后续读取直接命中纯 DRAM 缓存，免重复回溯主日志；
            // 提升门槛维持 safe_read_only 快照（严格对标 C# CopyFromImmutable 仅作用于
            // 不可变区，模糊区瞬态窗口内不提升，保守方向无损正确性）
            if curr_addr < safe_ro_addr {
              self.promote_immutable_to_read_cache(curr_addr, key);
            }
            return Ok(MemRead::Done(Some(val)));
          }
          Some(TraceBackResult::Tombstone) => return Ok(MemRead::Done(None)),
          Some(TraceBackResult::Retry) => return Ok(MemRead::Retry),
          Some(TraceBackResult::TraceBack(next_addr)) if next_addr != 0 => {
            curr_addr = next_addr;
            continue;
          }
          _ => break,
        }
      }
    }

    // 3. 链条已在内存区终结（curr_addr == 0 或已低于 begin_addr 截断边界）：
    //    严格对标 Tsavorite InternalRead.cs:142-157：
    //    若链条未伸入有效磁盘区（curr_addr < begin_addr），确认该键在整个存储中不存在，
    //    直接返回 MemRead::Done(None)，彻底消除无效的多候选扫描与二次哈希遍历！
    if curr_addr == 0 || (!is_read_cache_addr(curr_addr) && curr_addr < begin_addr) {
      return Ok(MemRead::Done(None));
    }

    // 4. 首项未命中、处于磁盘区或发生跨槽位 Tag 碰撞，回退到多候选扫描与落盘判定路径；
    //    curr_addr 为内存链回溯穷尽后的下一地址——链已伸入磁盘区时作为附加磁盘候选
    //    传递（严格对照 C# pending read 沿 prev 链从内存伸入磁盘的读取语义）
    self.try_read_mem_fallback(key, f, begin_addr, head_addr, safe_ro_addr, curr_addr)
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
    safe_ro_addr: u64,
    chain_disk_addr: u64,
  ) -> Result<MemRead<R>> {
    let mut addrs = self.store.index.lookup_candidates(key);
    if addrs.is_empty() {
      // 无槽位候选（并发清退等瞬态）且内存链已伸入磁盘区：直读链出地址兜底；
      // 链仍在内存区或已穷尽为 0 则确认不存在
      if chain_disk_addr != 0 && chain_disk_addr >= begin_addr {
        let mut disk = CandidateAddresses::new();
        disk.push(chain_disk_addr);
        return Ok(MemRead::OnDisk(disk));
      }
      return Ok(MemRead::Done(None));
    }
    addrs.sort_descending();

    // 免锁直读门槛快照：与 try_read_mem_once 主路径口径一致（read_only 覆盖模糊区）
    let ro_addr = self.store.hlog.read_only_address();
    let mut disk_cands = CandidateAddresses::new();

    for &addr in addrs.iter() {
      let mut cur_addr = addr;
      if is_read_cache_addr(cur_addr) {
        // 读侧驱逐等待协议（严格对标 ReadCache.cs:ReadCacheNeedToWaitForEviction，
        // 与主路径 try_read_mem 口径一致）：滑出窗口时自旋等待清洗完成后回链头重探，
        // 杜绝驱逐窗口内候选被静默丢弃
        if self
          .store
          .read_cache
          .need_to_wait_for_eviction(cur_addr, || self.participant.refresh())
        {
          return Ok(MemRead::Retry);
        }
        let matched = self
          .store
          .read_cache
          .with_record(cur_addr, |rec_key, rec_val, _meta| {
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

      while cur_addr >= head_addr {
        let probe = if cur_addr < ro_addr {
          // SAFETY: 调用方纪元保护 + cur_addr ∈ [head_addr, ro_addr)，走无锁纯指针直读
          Some(unsafe {
            self.store.hlog.with_immutable_record(cur_addr, |rec| {
              if rec.matches_key(key) {
                // C# InternalRead.cs:118 IsClosedOrTombstoned：closed → RETRY_LATER，
                // tombstone → NOTFOUND（closed 优先于 tombstone 判定）
                if rec.is_closed() {
                  Ok(ReadProbeResult::Retry)
                } else if rec.is_tombstone() {
                  Ok(ReadProbeResult::Tombstone)
                } else {
                  // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
                  let func = f.take().unwrap_unchecked();
                  Ok(ReadProbeResult::Found(func(rec.value())))
                }
              } else {
                Ok(ReadProbeResult::Miss(rec.prev_address()))
              }
            })?
          })
        } else {
          self.store.hlog.with_memory_record(cur_addr, |rec| {
            if rec.matches_key(key) {
              if rec.is_closed() {
                Ok(ReadProbeResult::Retry)
              } else if rec.is_tombstone() {
                Ok(ReadProbeResult::Tombstone)
              } else {
                // SAFETY: 闭包 f 仅在初次命中时消费一次，且此时必然为 Some
                let func = unsafe { f.take().unwrap_unchecked() };
                Ok(ReadProbeResult::Found(func(rec.value())))
              }
            } else {
              Ok(ReadProbeResult::Miss(rec.prev_address()))
            }
          })?
        };

        if let Some(probe) = probe {
          match probe {
            ReadProbeResult::Found(val) => {
              // 不可变区命中：同样预提升挂入 ReadCache（与主路径 safe_read_only 快照
              // 口径一致：严格对标 C# CopyFromImmutable 仅作用于不可变区，模糊区
              // 瞬态窗口内不提升，保守方向，无损正确性）
              if cur_addr < safe_ro_addr {
                self.promote_immutable_to_read_cache(cur_addr, key);
              }
              return Ok(MemRead::Done(Some(val)));
            }
            ReadProbeResult::Tombstone => return Ok(MemRead::Done(None)),
            ReadProbeResult::Retry => return Ok(MemRead::Retry),
            ReadProbeResult::Miss(prev) => {
              cur_addr = prev;
            }
          }
        } else {
          break;
        }
      }

      if cur_addr != 0 && cur_addr >= begin_addr {
        disk_cands.push(cur_addr);
      }
    }

    if disk_cands.is_empty() {
      Ok(MemRead::Done(None))
    } else {
      Ok(MemRead::OnDisk(disk_cands))
    }
  }

  /// 不可变区命中预提升：将内存驻留的只读记录挂入 ReadCache（严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:CopyFromImmutable）
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
    let _ = unsafe {
      self.store.hlog.with_immutable_record(addr, |rec| {
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
      })
    };
  }

  /// 异步磁盘回退路径（仅当内存阶段确认存在磁盘候选地址时调用）
  ///
  /// - 候选列表由 `try_read_mem_fallback` 单遍索引扫描收集并降序透传，此处零重复索引遍历；
  /// - 磁盘 I/O 期间不持有纪元守卫（对标 C# IO 期间 `UnsafeSuspendThread`），
  ///   彻底消除单次冷读阻塞纪元推进与页回收的尾延迟隐患；
  /// - 读取走 `read_disk_record` 纯设备路径（不触碰内存页缓冲），无守卫读取安全性成立；
  /// - 磁盘链回溯：Tag 碰撞键不匹配时沿记录 `prev_address` 前驱链继续冷读
  ///   （严格对照 libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:AsyncGetFromDiskCallback "skips colliding keys by
  ///   following the .PreviousAddress chain"；链地址在磁盘区内严格单调下降，
  ///   直至链尽 0 或低于截断线 begin_address）。
  #[cold]
  pub(super) async fn read_from_disk<R>(
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
        // 严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToReadCache.cs:TryCopyToReadCache 与 TryCopyToTail：
        // 1. 若启用了 ReadCache，优先将冷数据挂入纯 DRAM 只读非脏页内存日志（零持久化开销、零写放大）；
        // 2. 否则若开启 copy_reads_to_tail，则回退到追加 Tail 内存活跃区晋升。
        // （匹配记录非链头时索引更新自然失配为 no-op，RC 挂链由环形覆盖自然回收）
        // 晋升帧走紧缩搬迁同款旁路写监听（append_record_compacted）：帧内容为已存在的
        // 旧值，属物理布局优化而非用户写效果，镜像入 AOF 会在并发写下造成恢复回退
        // （旧值帧晚于并发新值帧入队，重放序错乱，见 append_record_compacted 注释）
        if self.store.read_cache.is_enabled {
          let _guard = self.participant.enter();
          if let Some(rc_addr) =
            self
              .store
              .read_cache
              .append(key, val_slice, cur, &self.store.index)
          {
            let _ = self.store.index.update_address(key, cur, rc_addr);
          }
        } else if self.copy_reads_to_tail()
          && let Ok(new_addr) = self
            .append_record_compacted(key, val_slice, cur, false)
            .await
        {
          let _guard = self.participant.enter();
          if !self.store.index.update_address(key, cur, new_addr)
            && self.store.config.enable_revivification
          {
            let rec_size = record_size(key.len(), val_slice.len()) as u32;
            self
              .store
              .reviv_pool
              .put(new_addr, rec_size, self.store.hlog.read_only_address());
          }
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
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:InternalReadAtAddress
  #[inline]
  pub async fn read_raw_with<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<R>> {
    let mut f = Some(f);
    let cands = {
      let _guard = self.participant.enter();
      let mut first_addr = self.store.index.find_tag(key);
      loop {
        match self.try_read_mem(key, first_addr, &mut f)? {
          MemRead::Done(res) => return Ok(res),
          MemRead::OnDisk(cands) => break cands,
          // RETRY_LATER：刷新纪元后整链重试（守卫存活期内，密封在途记录终将解封）
          MemRead::Retry => {
            self.participant.refresh();
            first_addr = self.store.index.find_tag(key);
          }
        }
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
}
