//! 紧缩执行流：扫描游标、两种紧缩模式、死亡判定与条件迁移
//!
//! - Lookup 逐记录判活迁移 ← `libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs:CompactLookup`
//! - Scan 三阶段候选去重迁移 ← 同文件 `:CompactScan`
//! - 死亡判定 ← C# `!iter1.Info.Tombstone && !cf.IsDeleted(in iter1)` 两通道短路（同文件 48、95 行内联）
//! - 条件迁移 ← `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ConditionalCopyToTail.cs:CompactionConditionalCopyToTail`

use std::result;

use log::warn;
use wbase::{map::HashMap, time::now_ticks};
use wdev::Device;
use whlog::{HybridLog, ScanIterator};
use wrecord::RecordRef;

use super::{CompactRunResult, CopyOutcome, LogCompactor};
use crate::{
  error::Result,
  host::{CompactSession, CompactStore, CompactionFunctions},
};

// ===================== 紧缩扫描游标 =====================

/// 紧缩扫描游标（hlog 页缓冲预取迭代器 + 单次分配可复用记录缓冲）
///
/// 拉取返回的记录视图借用自内部缓冲，下一次拉取前保持有效；
/// 冷区扫描由迭代器按页预取，整轮紧缩每页至多一次设备 I/O。
struct ScanCursor<'a, D: Device> {
  iter: ScanIterator<'a, D>,
  buf: Vec<u8>,
}

impl<'a, D: Device> ScanCursor<'a, D> {
  /// 创建 [from, until) 区间的紧缩扫描游标（缓冲按整页容量预分配，扫描期零增长）
  fn new(hlog: &'a HybridLog<D>, from: u64, until: u64) -> Self {
    Self {
      iter: hlog.scan_iter(from, until),
      buf: Vec::with_capacity(hlog.config.page_size),
    }
  }

  /// 拉取下一条记录视图（逻辑地址 + 零拷贝解析视图）
  async fn pull(&mut self) -> Result<Option<(u64, RecordRef<'_>)>> {
    match self.iter.next_into(&mut self.buf).await? {
      Some((addr, raw)) => Ok(Some((addr, RecordRef::from_slice(raw)?))),
      None => Ok(None),
    }
  }

  /// 当前游标地址（扫描耗尽时停在记录边界或页首对齐处）
  const fn cursor(&self) -> u64 {
    self.iter.current_address()
  }
}

/// 扫描候选记录元数据（用于 Scan 紧缩模式，不缓存值体，空间 O(唯一键)）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CandidateRecord {
  addr: u64,
  /// 死亡判定（墓碑、业务谓词判死，不止墓碑一种形态）
  is_dead: bool,
  /// 墓碑形态（判死处置分流：墓碑纯条件摘除，业务判死另接宿主键级清退）
  is_tombstone: bool,
}

/// 单次紧缩执行的折叠量：五个计数 + 截断点上界的收敛与结果组装
///
/// Lookup 与 Scan 两模式共用同一份字段清单与同一处钳制次序，消除「同族字段靠人记」
/// 的双写分叉（C# 无对位：`TsavoriteCompaction.cs:CompactLookup`/`:CompactScan` 两模式
/// 各自循环、只返回一个 long，本仓自加的观测量在此单点定义）。
struct CompactRunTally {
  scanned_records: usize,
  live_copied: usize,
  superseded: usize,
  dead_dropped: usize,
  retained: usize,
  /// 截断点：始终预推进到最后一条已校验记录的结束边界
  actual_until: u64,
  /// 保守保留回退下界：截断点绝不越过仍为最新存活版本的记录起始边界
  retain_floor: Option<u64>,
}

impl CompactRunTally {
  /// 以紧缩区间起点建仓（五个计数归零、尚无保守保留回退）
  const fn new(begin_addr: u64) -> Self {
    Self {
      scanned_records: 0,
      live_copied: 0,
      superseded: 0,
      dead_dropped: 0,
      retained: 0,
      actual_until: begin_addr,
      retain_floor: None,
    }
  }

  /// 游标尾对齐 + 安全只读区钳制（对标 C# 循环尾的 `untilAddress = iter1.NextAddress`
  /// 对齐，本仓在其上叠加上界钳制）
  ///
  /// 游标推进至页末 PadRecord 时自动对齐至下一页首（段文件物理截断与页末跳过必要
  /// 保障），绝不越过安全只读区快照。保守保留回退钳制见 [`Self::finish`]。
  fn align_cursor_and_clamp(&mut self, cursor_addr: u64, safe_ro_addr: u64) {
    if cursor_addr <= safe_ro_addr {
      self.actual_until = cursor_addr;
    }
    self.actual_until = self.actual_until.min(safe_ro_addr);
  }

  /// 保守保留单条记录：计入保留数并钳制截断点下界（与 CopyOutcome::Retain 通路一致）
  fn retain_record(&mut self, addr: u64) {
    self.retained += 1;
    self.retain_floor = Some(match self.retain_floor {
      Some(floor) => floor.min(addr),
      None => addr,
    });
  }

  /// 收尾组装：施加保守保留回退钳制（紧缩区间止步于最早仍存活记录的起始边界之前）
  /// 后搬运为单次紧缩执行结果
  fn finish(mut self) -> CompactRunResult {
    if let Some(floor) = self.retain_floor {
      self.actual_until = self.actual_until.min(floor);
    }
    CompactRunResult {
      scanned_records: self.scanned_records,
      live_copied: self.live_copied,
      superseded: self.superseded,
      dead_dropped: self.dead_dropped,
      retained: self.retained,
      actual_until: self.actual_until,
    }
  }
}

impl<S: CompactStore> LogCompactor<S> {
  /// Lookup 模式紧缩实现（对标 Tsavorite `CompactLookup`）
  ///
  /// 与 C# 的口径对齐：C# 逐记录独立判定（`ConditionalCopyToTail` 对单记录无界重试，
  /// 绝不因单记录竞争放弃整个紧缩区间）；本实现对单记录采用有界重试 + 记录级保守
  /// 保留——CAS 预算耗尽且记录仍存活时仅将该记录标记为保留（截断点下界回退至其
  /// 起始边界），扫描继续推进，后续记录照常判定/迁移。最终截断点取全部保留记录的
  /// 最小起始边界：保留记录原位存活（区间永不被截断到其之上），其后的已迁移记录
  /// 以 RCU 副本形式存在于尾部、原位置留作垃圾由下一轮紧缩回收——与 Scan 模式的
  /// 保守保留回退语义一致。
  pub(super) async fn compact_lookup<C>(
    &self,
    session: &S::Session,
    begin_addr: u64,
    until_address: u64,
    safe_ro_addr: u64,
    cf: &C,
  ) -> Result<CompactRunResult>
  where
    C: CompactionFunctions<S>,
  {
    let mut cursor = ScanCursor::new(self.store.hlog(), begin_addr, until_address);
    let mut tally = CompactRunTally::new(begin_addr);
    let now = now_ticks();

    while let Some((curr_addr, rec)) = cursor.pull().await? {
      // 物理终点含松弛填充，与扫描游标推进口径一致（对标 C# iter1.NextAddress）
      tally.actual_until = curr_addr + rec.physical_size() as u64;
      if tally.actual_until > safe_ro_addr {
        // 记录跨出安全只读区快照：尾部残段不可紧缩，截断点回退至该记录起始边界
        tally.actual_until = curr_addr;
        break;
      }

      tally.scanned_records += 1;
      let is_tombstone = rec.is_tombstone();
      let key = rec.key;
      let val = if is_tombstone { &[] } else { rec.value };

      // 墓碑短路在前，业务谓词仅对非墓碑记录生效（对标 C# `!Tombstone && !IsDeleted`）
      if self
        .judge_dead(session, is_tombstone, key, val, now, cf)
        .await
      {
        match self
          .drop_dead(session, cf, key, curr_addr, is_tombstone)
          .await
        {
          Ok(()) => tally.dead_dropped += 1,
          Err(err) => {
            warn!("紧缩清退失败，跳过摘槽并回退截断: {err}");
            tally.retain_record(curr_addr);
          }
        }
      } else {
        let latest = self
          .find_latest_address(session, key, Some((curr_addr, false)))
          .await?;
        if let Some(latest) = latest
          && latest.main_addr == curr_addr
          && !latest.is_tombstone
        {
          match self
            .conditional_copy_to_tail(session, key, val, curr_addr, latest.index_addr)
            .await?
          {
            CopyOutcome::Copied => tally.live_copied += 1,
            // 复核确认已被并发覆盖/删除：安全放弃迁移
            CopyOutcome::Superseded => tally.superseded += 1,
            // 重试耗尽仍为最新存活版本：记录级保守保留，截断点下界回退至其起始
            // 边界，扫描继续推进（论证见本方法文档）
            CopyOutcome::Retain => {
              tally.retain_record(curr_addr);
            }
          }
        } else {
          // 非最新版本：探针已过分裂协同门（find_latest_address 入口铁律），空候选/
          // 地址不符即真并发新版本或墓碑已生效，安全弃迁
          tally.superseded += 1;
        }
      }
    }

    // 游标尾对齐 + 安全只读区钳制，收尾施加保守保留回退钳制（次序两模式同一，见 tally）
    tally.align_cursor_and_clamp(cursor.cursor(), safe_ro_addr);
    Ok(tally.finish())
  }

  /// Scan 模式紧缩实现（对标 Tsavorite `CompactScan`）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs:CompactScan
  ///
  /// C# 的临时 KV（tempKv）由本函数的候选表取代（阶段 1 收集 = tempKv Upsert/Delete，
  /// 阶段 2 = 定稿尾区间剔除，阶段 3 = 存活候选回迁），阶段 2 即 C#
  /// libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs:ScanImmutableTailToRemoveFromTempKv
  /// 的等价物：扫过 `[until, SafeReadOnlyAddress)` 定稿尾区间，把在该区间发生过
  /// 更新/删除的候选从临时集中剔除（C# 逐条 Delete，rust 直接 `candidates.remove`）。
  pub(super) async fn compact_scan<C>(
    &self,
    session: &S::Session,
    begin_addr: u64,
    until_address: u64,
    safe_ro_addr: u64,
    cf: &C,
  ) -> Result<CompactRunResult>
  where
    C: CompactionFunctions<S>,
  {
    let mut cursor = ScanCursor::new(self.store.hlog(), begin_addr, until_address);
    let mut tally = CompactRunTally::new(begin_addr);
    // 候选表：键 -> 区间内最新版本元数据（值体不缓存，阶段 3 确认存活后按需单次回读）
    let mut candidates: HashMap<Box<[u8]>, CandidateRecord> = HashMap::default();
    let now = now_ticks();

    // 阶段 1：扫描紧缩区间，收集每键最新版本元数据（对标 C# 临时 KV 的 Upsert/Delete）
    while let Some((curr_addr, rec)) = cursor.pull().await? {
      // 物理终点含松弛填充，与扫描游标推进口径一致（对标 C# iter1.NextAddress）
      tally.actual_until = curr_addr + rec.physical_size() as u64;
      if tally.actual_until > safe_ro_addr {
        tally.actual_until = curr_addr;
        break;
      }

      tally.scanned_records += 1;
      let is_tombstone = rec.is_tombstone();
      let key = rec.key;
      let raw_val = if is_tombstone { &[] } else { rec.value };
      let is_dead = self
        .judge_dead(session, is_tombstone, key, raw_val, now, cf)
        .await;

      match candidates.get_mut(key) {
        Some(cand) => {
          // 同键更新版本在区间内出现：旧候选记录被新版本替换，计为弃迁
          // （精确守恒：每键 n 次出现 = n-1 次弃迁 + 末版候选的阶段 3 处置）
          tally.superseded += 1;
          cand.addr = curr_addr;
          cand.is_dead = is_dead;
          cand.is_tombstone = is_tombstone;
        }
        None => {
          candidates.insert(
            Box::from(key),
            CandidateRecord {
              addr: curr_addr,
              is_dead,
              is_tombstone,
            },
          );
        }
      }
    }

    // 游标尾对齐 + 安全只读区钳制，得到阶段 2 的起点（此刻保守保留回退尚未产生，
    // 其钳制统一在 finish 施加，与 Lookup 模式同一处次序）
    tally.align_cursor_and_clamp(cursor.cursor(), safe_ro_addr);

    // 阶段 2：扫过剩余定稿区（上界即 safe_ro，对标 C# CompactScan:108
    // `scanUntil = hlogBase.SafeReadOnlyAddress`），单趟剔除在该区间发生过更新/删除的键
    // （候选集清空即提前终止）
    cursor = ScanCursor::new(self.store.hlog(), tally.actual_until, safe_ro_addr);
    while !candidates.is_empty()
      && let Some((curr_addr, rec)) = cursor.pull().await?
    {
      if curr_addr + rec.physical_size() as u64 > safe_ro_addr {
        break;
      }
      if candidates.remove(rec.key).is_some() {
        // 阶段 2 剔除：紧缩区候选已被定稿尾部区间的新版本/墓碑取代，计为弃迁
        tally.superseded += 1;
      }
    }

    // 阶段 3：校验并迁移幸存候选（索引现值校验 + 条件复制，绝不覆盖并发新版本）。
    // 复查以阶段 3 当下时刻为准：长时间紧缩期间 TTL 可能刚到期，CAS 迁移前必须以最新状态重估
    let now = now_ticks();
    for (key, cand) in candidates {
      // 快速清理通道：阶段 1 已判死——先于 find_latest/read 的无记录 I/O 快路径
      if cand.is_dead {
        match self
          .drop_dead(session, cf, &key, cand.addr, cand.is_tombstone)
          .await
        {
          Ok(()) => tally.dead_dropped += 1,
          Err(err) => {
            warn!("紧缩清退失败，跳过摘槽并回退截断: {err}");
            tally.retain_record(cand.addr);
          }
        }
        continue;
      }

      let latest = self
        .find_latest_address(session, &key, Some((cand.addr, false)))
        .await?;
      let Some(latest) = latest.filter(|l| l.main_addr == cand.addr && !l.is_tombstone) else {
        // 非最新版本：探针已过分裂协同门，空候选/地址不符即真并发新版本已生效，安全弃迁
        tally.superseded += 1;
        continue;
      };

      // 按需单次回读值体（阶段 1 不缓存大值，空间 O(唯一键) 元数据），冷读分派收口内核单点
      let record = session.read_record_at(cand.addr).await?;
      let val = record.value()?;
      // CAS 前同口径复查：阶段 1 之后 TTL 可能
      // 刚到期，以最新 TTL 状态与当下时间统一重判，杜绝长时间紧缩迁移刚过期记录
      if self.judge_dead(session, false, &key, val, now, cf).await {
        match self.drop_dead(session, cf, &key, cand.addr, false).await {
          Ok(()) => tally.dead_dropped += 1,
          Err(err) => {
            warn!("紧缩清退失败，跳过摘槽并回退截断: {err}");
            tally.retain_record(cand.addr);
          }
        }
        continue;
      }

      match self
        .conditional_copy_to_tail(session, &key, val, cand.addr, latest.index_addr)
        .await?
      {
        CopyOutcome::Copied => tally.live_copied += 1,
        // 真并发覆盖：新版本已在紧缩区间之外生效，旧版本安全随截断退役
        CopyOutcome::Superseded => tally.superseded += 1,
        // 重试耗尽仍为最新存活版本：截断点回退至其起始边界，绝不误删
        CopyOutcome::Retain => {
          tally.retain_record(cand.addr);
        }
      }
    }

    // 收尾：施加保守保留回退钳制并组装结果（与 Lookup 模式同一处次序）
    Ok(tally.finish())
  }

  // ===================== 死亡判定 =====================

  /// 判死记录处置单点（两段式：键级清退先行 + 条件摘除收尾，.Lookup 与 Scan
  /// 两模式及阶段 3 复查共用同一口径）
  ///
  /// 1. 键级清退：非墓碑时交宿主 [`CompactionFunctions::on_dropped`] 整键退册
  ///    （树实例注销/向量删除链退册/随键旁路级联，判死语义等价一次过期 DEL），
  ///    清退闭环后该记录的物理空间方随截断回收。清退必须先于摘除（清退链靠
  ///    索引/读链定位各物理域，先摘主键槽位即定位失败跳过树注销），并发安全垫
  ///    由宿主清退入口的过期重读裁决承担（并发续期未过期零副作用）。墓碑不接
  ///    清退：墓碑即删除标记本身，键级退册已由正轨 DEL 链承接（C# 紧缩丢墓碑
  ///    同样无 OnDispose 挂点）。若清退返回 Err，必须跳过摘槽并回传失败，由调用
  ///    方进行截断回退钳制（防悬挂槽）。
  /// 2. 条件摘除：CAS 置零索引槽位，仅当槽位仍指向本记录才生效——清退链已将
  ///    槽位移交墓碑/摘除时 CAS 自然落败零操作，并发新版本已接管时同样零操作，
  ///    本记录物理面随截断安全退役（RCU 旧版本语义）。
  async fn drop_dead<C>(
    &self,
    session: &S::Session,
    cf: &C,
    key: &[u8],
    addr: u64,
    is_tombstone: bool,
  ) -> result::Result<(), C::Error>
  where
    C: CompactionFunctions<S>,
  {
    if !is_tombstone {
      cf.on_dropped(session, key).await?;
    }
    self.store.index().delete(key, addr);
    Ok(())
  }

  /// 单条记录死亡判定统一入口：墓碑 → 业务过滤谓词，两通道短路判定
  /// （对标 C# `!iter1.Info.Tombstone && !cf.IsDeleted(...)`）
  ///
  /// Lookup 逐记录、Scan 阶段 1 与 Scan 阶段 3 CAS 前复查共用同一口径，消除各阶段
  /// 判定逻辑漂移：墓碑为记录级确定性判定，业务谓词（宿主注入的 TTL 过期/孤儿
  /// 等判定）为「读最新状态」的时敏判定——复查时以调用时刻的 `now`（.NET Ticks，
  /// 与 TTL 记录存储值同域）与最新索引/日志状态重估
  async fn judge_dead<C>(
    &self,
    session: &S::Session,
    is_tombstone: bool,
    key: &[u8],
    val: &[u8],
    now: i64,
    cf: &C,
  ) -> bool
  where
    C: CompactionFunctions<S>,
  {
    is_tombstone || cf.is_deleted(session, key, val, now).await
  }

  // ===================== 条件迁移 =====================

  /// 条件性复制存活记录至尾部（对标 Tsavorite `CompactionConditionalCopyToTail`
  /// 及其内部 `ConditionalCopyToTail` 的重试环）
  ///
  /// 仅当该记录依然是哈希索引中的最新版本且未被并发覆盖时，将其追加至尾部并原子 CAS 替换索引地址。
  /// CAS 失败须区分两类竞争：真并发写（同键新版本/墓碑已生效，返回 [`CopyOutcome::Superseded`]
  /// 弃迁）与良性改写（并发读触发 ReadCache 挂链提升或驱逐回写索引槽位，记录仍存活且仍是
  /// 最新版本，必须复核后换新期望槽位重试，否则存活记录会随截断被误弃）。孤儿副本一律归还
  /// 复活池复用，绝不覆写并发新版本数据。全程持有纪元保护：尾部追加、索引 CAS 与复活池归还
  /// 均为共享内存结构变更（与 session 写路径及 ReadCache 回填口径一致）。
  /// 分配复用 `session.allocate_record` 的复活池取臂 + 有界重试（对标 C#
  /// CompactionConditionalCopyToTail 经 ConditionalCopyToTail → TryCopyToTail 的
  /// `AllocateOptions{recycle:true}` 统一 TryAllocateRecord 契约）：环形缓冲翻转
  /// （PageNotReady）时先异步刷盘并驱逐被覆盖的旧页再重试，循环直至成功或非
  /// PageNotReady 错误上抛，保证长区间紧缩不会因页耗尽而整轮失败（与 session
  /// 写路径口径一致）。
  /// 与 C# `while (true)` 无界重试的刻意差异：竞争重试耗尽且复核确认记录仍为最新存活版本时，
  /// 返回 [`CopyOutcome::Retain`] 而非弃迁——调用方回退截断点保住该记录，绝不误删存活数据。
  async fn conditional_copy_to_tail(
    &self,
    session: &S::Session,
    key: &[u8],
    val: &[u8],
    expected_main_addr: u64,
    expected_index_addr: u64,
  ) -> Result<CopyOutcome> {
    let _guard = session.enter_epoch();
    let mut index_addr = expected_index_addr;
    for _ in 0..self.cas_retries {
      // 1. 复活池取臂优先 + 尾部追加，版本链指针指向 expected_main_addr；下界由宿主
      //    按候选链首 index_addr 抬升（PageNotReady 自动驱逐旧页后重试）
      let (new_addr, frame) = session
        .allocate_record(key, val, expected_main_addr, false, index_addr)
        .await?;

      // 2. 原子 CAS 替换索引中的地址：index_addr -> new_addr
      if self.store.index().update_address(key, index_addr, new_addr) {
        // 搬迁成功：源存根所有权转出（C# PostCopyToTail 源侧 ClearTreeHandle +
        // SetTransferredFlag 对位，与 RIPROMOTE 同一宿主编排），置位后组提交
        // 刷盘的 is_transferred 防护对本滞留源生效，绝不对已被本新帧取代的旧源
        // 做全树 CPR 快照、不产出无消费方 flush 件
        session.transfer_out_source(key, val, expected_main_addr);
        return Ok(CopyOutcome::Copied);
      }

      // 3. 竞争失败：孤儿副本归还复活池（足印为分配内核回传的本帧实际尺寸），
      //    复核该记录是否仍是最新存活版本
      //    （入池门槛由 reviv_put 内部按复活下限单点推导——对标 C#
      //    Helpers.cs:107 GetMinRevivifiableAddress(tail, ReadOnlyAddress)，
      //    紧缩侧不再外传水位，杜绝 read_only 与 min_address 两套口径混用）
      if self.store.enable_revivification() {
        self.store.reviv_put(new_addr, frame);
      }
      match self
        .find_latest_address(session, key, Some((expected_main_addr, false)))
        .await?
      {
        // 良性竞争（索引槽位被 ReadCache 挂链/驱逐回写改写）：换新期望槽位重试
        Some(latest) if latest.main_addr == expected_main_addr && !latest.is_tombstone => {
          index_addr = latest.index_addr;
        }
        // 真并发覆盖/删除：新数据已生效，安全放弃迁移（复核同样走协同门探针，
        // grow 迁移窗未迁分块键先协同迁移再采样，杜绝陈旧空候选假判 superseded）
        _ => return Ok(CopyOutcome::Superseded),
      }
    }
    // 重试耗尽且记录仍为最新存活版本：保守保留，绝不随截断误删存活数据
    Ok(CopyOutcome::Retain)
  }
}
