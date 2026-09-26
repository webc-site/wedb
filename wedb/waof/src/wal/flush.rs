//! WAL 提交刷盘面（对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/
//! TsavoriteLog.cs:CommitAsync 与 TryEnqueueCommitRecord；Garnet Group Commit
//! 流水线合并模式：Leader 级联写盘 + fdatasync + commit 元数据帧随批）。
//!
//! 提交见证契约：commit 帧入队遇环形满不跳帧——腾窗自旋重试至入队成功
//!（对标 CommitInternal 内 `while (!TryEnqueueCommitRecord) Thread.Yield()`），
//! committed_until_address 恒 ≤ 持久 commit 帧尾，已确认写必有磁盘提交见证，
//! 恢复侧绝不丢弃。

use std::{convert::Infallible, sync::atomic::Ordering};

use wbase::{
  future::yield_now,
  group_commit::{Enter, GroupCommitStep},
};
use wdev::{Device, FlushError};

use super::{
  commit,
  config::FsyncPolicy,
  log::{WalLog, WalLogInner},
};
use crate::error::{Error, Result};

/// 帧入队腾窗自旋上限（对标 C# 重试契约的有界化收敛界）
const COMMIT_FRAME_SPIN_LIMIT: u32 = 1024;

impl<D: Device> WalLogInner<D> {
  /// 刷盘内核：把环形缓冲 [flushed, upto) 区间写入底层分段设备并按同步策略
  /// fdatasync，只推进 flushed_until_address 水位
  ///
  /// 刻意不推进 committed_until_address：腾窗刷新（提交流水线腾出环形窗口）与
  /// 提交推进解耦的单点——腾窗只释放环形空间、不产生任何提交见证，committed
  /// 的推进策略由调用方按语义单点承担（提交批次 = 帧尾，副本镜像 = 刷盘尾）
  pub(crate) async fn flush_window(&self, flushed: u64, upto: u64) -> Result<()> {
    if upto <= flushed {
      return Ok(());
    }

    // 刷盘写原语单源下沉（对标 AllocatorBase.cs:WriteInlinePageAsync）：扇区界
    // 圆整、设备池借还、尾补零与短写校验全部由 Device::flush_range_aligned 内核
    // 承担，本函数仅提供环形缓冲→连续字节的填充闭包（闭包收到圆整起点，有效区
    // 含 [flushed 所在扇区起点, upto) 的已落盘前缀重叠字节，整扇区幂等重写）。
    // 填充失败类型以 Infallible 编码：环形读为纯内存拷贝，绝无失败可能
    self
      .device
      .flush_range_aligned(flushed, upto, |start_aligned, buf| {
        self.ring_buffer.read_bytes(start_aligned, buf);
        Ok::<_, Infallible>(())
      })
      .await
      .map_err(|e| match e {
        FlushError::Fill(e) => match e {},
        FlushError::ShortWrite { expected, written } => Error::ShortWrite { expected, written },
        FlushError::Device(e) => e.into(),
      })?;

    // 同步策略位（对标 TsavoriteLogSettings.cs:AutoCommit 的持久性显式配置面）：
    // Always 批次等待设备落盘，Deferred 批次仅写设备页缓存即推进刷盘水位
    if self.config.fsync == FsyncPolicy::Always
      && let Err(e) = self.device.sync_data().await
    {
      return Err(e.into());
    }

    self.flushed_until_address.store(upto, Ordering::Release);
    Ok(())
  }

  /// 执行物理段写入与 fdatasync 并推进提交水位（副本镜像 / 提交元数据对齐语义：
  /// committed_until_address = 刷盘尾）
  pub(crate) async fn flush_and_sync_range(&self, flushed: u64, safe_tail: u64) -> Result<u64> {
    if safe_tail <= flushed {
      return Ok(self.committed_until_address.load(Ordering::Acquire));
    }
    self.flush_window(flushed, safe_tail).await?;
    self
      .committed_until_address
      .store(safe_tail, Ordering::Release);
    Ok(safe_tail)
  }
}

impl<D: Device> WalLog<D> {
  /// 异步将已入队的记录刷到底层分段设备，更新 flushed_until_address 和
  /// committed_until_address
  ///
  /// 批次合并唯一由 [`Self::commit_to`] 的提交流水线 Leader/Follower 协商面承担
  /// （对标 TsavoriteLog.cs:CommitAsync：C# 准入拒绝即不生成提交记录、不发起
  /// 新提交，仅 await 既有 CommitTask 链至已提交水位覆盖调用方自取的 tail
  /// 下界；rust 流水线 Follower 挂起等待在途 Leader 批次覆盖下界正是该等待的
  /// 实现本身——有 Leader 在位即合批零重复物理 I/O，流水线空闲则升级 Leader
  /// 在本调用内完成提交，返回即代表 target 已持久）。
  /// 无新增数据时直接返回当前水位（对标 C# CommitInternal 无元数据变更且无新
  /// 条目即返回 false，CommitAsync 的下界等待立即退出）
  ///
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:Commit
  pub async fn commit(&self) -> Result<u64> {
    let target = self.safe_tail_address();
    let committed = self.committed_until_address.load(Ordering::Acquire);
    if target <= committed {
      if self.should_commit_metadata() {
        return self.commit_to(committed + 1).await;
      }
      return Ok(committed);
    }
    self.commit_to(target).await
  }

  /// 纯刷盘提交（不写 commit 元数据帧）：副本角色的落盘入口
  ///
  /// 副本 AOF 须为主端流的严格镜像——本地追加的 commit 帧不在主端流内，副本
  /// 重启后以此位点协商增量续传时，该位点落在主端帧边界之外，主端扫描在负载
  /// 中段读帧必然判 Invalid 终止，增量流死锁。C# 对偶约束：副本 Dispose 链无
  /// CommitAsync（TrueDispose 仅释放资源），副本日志永不出现本地 record。
  /// 本入口只把已入队记录刷设备并推进提交水位，跳过 [`WalCommitStep`] 的帧
  /// 写入；已提交面语义与 [`Self::commit`] 一致
  ///
  /// 与提交流水线的并发：[`Self::commit_to`] Leader 持同一提交锁，本入口获得
  /// 锁即与其串行；锁等待期入队的 Follower 由阻塞中的 Leader 恢复后首轮级联
  /// 唤醒——本入口只推进水位，不承担唤醒
  pub async fn commit_flush_only(&self) -> Result<u64> {
    let committed = self.committed_until_address.load(Ordering::Acquire);
    if self.tail_address.load(Ordering::Acquire) <= committed
      || self.safe_tail_address() <= committed
    {
      return Ok(committed);
    }
    let _commit_guard = self.commit_lock.lock().await;
    // 持锁重取安全尾：锁等待期新入队数据一并覆盖（target ≤ flushed 时
    // [`Self::flush_and_sync_range`] 内置早退，语义同直接返回水位）
    let target = self.safe_tail_address();
    let flushed = self.flushed_until_address.load(Ordering::Acquire);
    self.flush_and_sync_range(flushed, target).await
  }

  /// 设备级数据同步（fdatasync）：[`FsyncPolicy::Deferred`] 档的强持久化显式
  /// 入口——把已提交但未 sync 的批次推至设备；Always 档无需调用（每次提交
  /// 批次已随批 sync）。设备级全量同步语义见 `Device::sync_data`（覆盖全部
  /// 线程在调用发起前完成的写入），位点原子量不动
  pub async fn sync(&self) -> Result<()> {
    Ok(self.device.sync_data().await?)
  }

  /// 提交并持久化至指定逻辑地址（严格对标 Garnet Group Commit 流水线合并模式）
  /// libs/storage/Tsavorite/cs/src/core/Allocator/TsavoriteLogAllocatorImpl.cs:WriteAsync
  ///
  /// C# 日志消费/提交管理接口的统一落点（Leader 级联批量消费环形缓冲条目落盘，
  /// 提交元数据帧随批尾写入即 commit 点推进）：
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/ILogEntryConsumer.cs:Consume
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.Chunked.cs:Consume
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/ILogCommitManager.cs:Commit
  pub async fn commit_to(&self, mut target: u64) -> Result<u64> {
    // 1. 快速短路（0 I/O）：目标水位已被硬件 sync 持久化覆盖且无元数据需提交
    let committed = self.committed_until_address.load(Ordering::Acquire);
    if target <= committed {
      if self.should_commit_metadata() {
        target = committed + 1;
      } else {
        return Ok(committed);
      }
    }

    // 2. 状态机协商：判定成为 Leader 还是 Follower
    match self.commit_pipeline.enter(target, || {
      self.committed_until_address.load(Ordering::Acquire)
    }) {
      Enter::Done(committed) => return Ok(committed),
      Enter::Follow(rx) => {
        // 3. Follower 分支：挂起等待 Leader 批量唤醒（0 重复物理 I/O）
        return self
          .commit_pipeline
          .wait(rx, target, || {
            self.committed_until_address.load(Ordering::Acquire)
          })
          .await
          .map_err(Error::from);
      }
      // 升级为 Leader，接管物理刷盘管道
      Enter::Lead => {}
    }

    // 4. Leader 级联写盘主循环（Cascade Loop）：持提交锁与 reset/truncate/recover 串行
    let _commit_guard = self.commit_lock.lock().await;
    self
      .commit_pipeline
      .run_leader(WalCommitStep { wal: self })
      .await
  }

  /// 高速提交栅栏（Fast Commit Barrier）：等待指定逻辑地址提交落盘（0 表示等待当前尾地址）
  ///
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:WaitForCommit
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:EnqueueAndWaitForCommitAsync
  ///（C# 入队+等待组合 API；rust 调用方经 pipeline enqueue + 本方法两步组合，等待语义单点在此）
  pub async fn wait_for_commit(&self, target_addr: u64) -> Result<u64> {
    let target = if target_addr == 0 {
      self.tail_address.load(Ordering::Acquire)
    } else {
      target_addr
    };
    self.commit_to(target).await
  }
}

/// WAL 提交步进器：批次目标在 step 内单点钳制至实时重读的 safe_tail_address
/// （在途写入下界），水位取已提交位点，物理持久化复用环形缓冲刷盘 + fdatasync
/// 单点实现
///
/// 单点钳制同时封堵三条越界来源——Follower waiter target、Leader 自身登记
/// target、step 内 commit 元数据帧 goal.max(frame_end) 抬升——保证刷盘永不越过
/// safe_tail、未达标差额由 [`wbase::group_commit::GroupCommitPipeline::run_leader`]
/// 级联循环 yield_now 让步承接；禁止在 group_commit.rs 与 flush.rs 两处各钳一次
/// 形成双份钳制（全链路一套机制）
///
/// 钳制回落帧尾之下时由同点的持久帧尾见证闸承接（见 [`GroupCommitStep::step`]）：
/// committed 恒不越过磁盘最后持久 commit 帧尾，已确认写必有磁盘提交见证
///
/// 与 wkv `FlushStep` 共用同一内核 `wbase::GroupCommitPipeline`（leader 级联循环、
/// 退避与批次编排都在内核里），本 Step 仅注入批次目标与水位语义；二者同构不同参，
/// 属 Step 注入形态而非重复实现，严禁合并或拆出第三套流水线。
struct WalCommitStep<'a, D: Device> {
  wal: &'a WalLog<D>,
}

impl<D: Device> GroupCommitStep for WalCommitStep<'_, D> {
  type Error = Error;

  #[inline]
  fn tail(&self) -> u64 {
    self.wal.safe_tail_address()
  }

  #[inline]
  fn watermark(&self) -> u64 {
    self.wal.committed_until_address.load(Ordering::Acquire)
  }

  async fn step(&self, target: u64) -> Result<u64> {
    // 见证界：step 入口提交水位。归纳不变式（见下方见证闸）保证 committed 恒
    // ≤ 磁盘最后持久 commit 帧尾，入口水位即上一持久帧尾的现成可观测量，
    // 无需新增原子量
    let committed_in = self.wal.committed_until_address.load(Ordering::Acquire);
    // commit 元数据帧随批尾写入（对标 TsavoriteLog.cs:TryEnqueueCommitRecord）：
    // 帧覆盖至自身末尾并与数据记录同一刷盘批次原子持久；帧游标防级联轮重复写帧
    let mut goal = target;
    let last_frame = self.wal.last_commit_frame.load(Ordering::Acquire);
    let should_meta = self.wal.should_commit_metadata();
    if goal > last_frame || should_meta {
      let cookie = self.wal.pending_cookie.load(Ordering::Acquire);
      let begin = self.wal.begin_address.load(Ordering::Acquire);
      let payload = commit::encode_payload(commit::CommitMeta { begin, cookie });
      // 腾窗自旋重试（对标 TsavoriteLog.cs:CommitInternal 的
      // while (!TryEnqueueCommitRecord(ref info)) Thread.Yield() 重试契约：
      // fastCommitMode 下提交记录是不可跳过的前置条件，分配失败只让核重试，
      // 绝不跳过——跳帧推进提交即令已确认写失去磁盘提交见证，恢复侧以最后
      // 帧尾收敛并把帧后记录物理擦除）。rust 对偶：环形满时先腾窗刷新
      //（[`WalLogInner::flush_window`] 只推进 flushed 水位，committed 恒 ≤
      // 持久帧尾不变式不因腾窗破缺），再让核重试帧入队；入队全程同步无挂起
      // 点、在途槽位必然释放，腾窗后帧级预留必然可得，自旋有界收敛，界尽即
      // 以原错误上抛（响亮失败，绝不静默丢见证）
      let mut spins = 0u32;
      let frame_addr = loop {
        match self.wal.enqueue(&payload) {
          Ok(addr) => break addr,
          Err(e @ Error::BufferFull { .. }) => {
            if spins >= COMMIT_FRAME_SPIN_LIMIT {
              return Err(e);
            }
            spins += 1;
            let flushed = self.wal.flushed_until_address.load(Ordering::Acquire);
            self
              .wal
              .flush_window(flushed, self.wal.safe_tail_address())
              .await?;
            yield_now().await;
          }
          Err(e) => return Err(e),
        }
      };
      let frame_end = frame_addr + commit::COMMIT_FRAME_TOTAL_LEN;
      self
        .wal
        .last_commit_frame
        .store(frame_end, Ordering::Release);
      self.wal.last_commit_cookie.store(cookie, Ordering::Release);
      self.wal.last_commit_begin.store(begin, Ordering::Release);
      goal = goal.max(frame_end);
    }

    // 单点钳制：批次目标恒不越过实时重读的 safe_tail_address（在途写入者压低的
    // 已定稿下界）。刷盘与 committed 推进永不越过该下界，未达标差额由
    // wbase::GroupCommitPipeline::run_leader 级联循环的 yield_now 让步承接——
    // 在途写入者释放槽位后 safe_tail 回升，下一轮 step 补齐。此单点同时封堵
    // 三条越界来源：Follower waiter target（enter 传入）、step 内 commit 元数据
    // 帧 goal.max(frame_end) 抬升、Leader 自身登记的 target（enter Lead 分支入
    // 首等待者），杜绝在 group_commit.rs 与本文件双份钳制形成冗余。
    //
    // 对标 C# libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs
    // :SafeTailAddress（104 行，the largest address below which every byte has
    // been fully written）与 CommitInternal→AllocatorBase.cs:ShiftReadOnlyToTail
    // （1627-1639 行）的 epoch.BumpCurrentEpoch(OnPagesMarkedReadOnly) 纪元动作：
    // C# 刷盘动作在全部写入者退出当前纪元后才运行，落盘面恒 ≤ SafeTailAddress；
    // Rust 无 LightEpoch 纪元屏障，改以 step 内 `goal.min(safe_tail_address())`
    // 的实时钳制复现同等不变式。
    let goal = goal.min(self.wal.safe_tail_address());

    // 持久帧尾见证闸（对标 C# libs/storage/Tsavorite/cs/src/core/TsavoriteLog/
    // SerialCommitCallbackWorker:2799-2800 的 addr > commitInfo.UntilAddress 即
    // break——唤醒资格绑定本批 flush 完成范围覆盖帧尾，唤醒即见证是硬闸）：
    // 钳制回落可把 goal 折到本轮新入队帧尾之下（帧字节仍滞留环形缓冲未落盘），
    // 若仍按 goal 直刷直推，committed 即越过磁盘最后持久帧尾——(帧尾, committed]
    // 已确认区间无任何磁盘提交见证，崩溃恢复以最后持久帧尾收敛并
    // erase_tail_after 物理擦除，客户端已收 OK 的写静默丢失。
    //
    // 判据：committed 推进越过入口水位的唯一凭证是游标帧尾（本轮写帧即新帧尾，
    // 写帧轮 goal 钳后恒 ≤ 帧尾——goal = goal.max(frame_end) 而钳上界
    // safe_tail ≤ 帧入队后的 tail = frame_end；游标 ≤ 入口水位时无新见证候选）
    // 随本轮刷盘完整落盘且落在 (committed_in, goal] 内。帧被钳悬空
    //（goal < 游标帧尾）时提交目标折回见证界 min(goal, committed_in)：committed
    // 本轮恒不越上一持久帧尾，达标差额由 run_leader_cascade 零推进 yield_now
    // 让步臂承接，safe_tail 回升后同帧随下一轮 step 补齐、见证到位即唤醒。
    //
    // 见证界之下的已定稿字节仍照常腾刷（下方 [`WalLogInner::flush_window`]，
    // 只推 flushed 不推 committed）：腾窗与提交推进解耦的既有语义保证环满阻塞
    // 不因本闸放大；刷出的无见证字节崩溃恢复即被擦除，持久性语义不受损。
    //
    // 与上方单点钳制同点布置（本闸是钳制的见证半边，非第二钳制点位），禁止在
    // group_commit.rs 另置一份
    let frame_cursor = self.wal.last_commit_frame.load(Ordering::Acquire);
    let commit_to = if goal >= frame_cursor && frame_cursor > committed_in {
      goal
    } else {
      committed_in.min(goal)
    };

    let flushed = self.wal.flushed_until_address.load(Ordering::Acquire);
    if goal > flushed {
      self.wal.flush_and_sync_range(flushed, commit_to).await?;
      if commit_to < goal {
        self.wal.flush_window(commit_to.max(flushed), goal).await?;
      }
    }
    Ok(self.wal.committed_until_address.load(Ordering::Acquire))
  }
}
