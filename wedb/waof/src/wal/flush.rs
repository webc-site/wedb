//! WAL 提交刷盘面（对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/
//! TsavoriteLog.cs:CommitAsync 与 TryEnqueueCommitRecord；Garnet Group Commit
//! 流水线合并模式：Leader 级联写盘 + fdatasync + commit 元数据帧随批）。

use std::{convert::Infallible, sync::atomic::Ordering};

use wbase::group_commit::{Enter, GroupCommitStep};
use wdev::{Device, FlushError};

use super::{
  commit,
  config::FsyncPolicy,
  log::{WalLog, WalLogInner},
};
use crate::error::{Error, Result};

impl<D: Device> WalLogInner<D> {
  /// 执行物理段写入与 fdatasync
  pub(crate) async fn flush_and_sync_range(&self, flushed: u64, safe_tail: u64) -> Result<u64> {
    if safe_tail <= flushed {
      return Ok(self.committed_until_address.load(Ordering::Acquire));
    }

    // 刷盘写原语单源下沉（对标 AllocatorBase.cs:WriteInlinePageAsync）：扇区界
    // 圆整、设备池借还、尾补零与短写校验全部由 Device::flush_range_aligned 内核
    // 承担，本函数仅提供环形缓冲→连续字节的填充闭包（闭包收到圆整起点，有效区
    // 含 [flushed 所在扇区起点, safe_tail) 的已落盘前缀重叠字节，整扇区幂等重写）。
    // 填充失败类型以 Infallible 编码：环形读为纯内存拷贝，绝无失败可能
    self
      .device
      .flush_range_aligned(flushed, safe_tail, |start_aligned, buf| {
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
    // Always 批次等待设备落盘，Deferred 批次仅写设备页缓存即推进提交水位
    if self.config.fsync == FsyncPolicy::Always
      && let Err(e) = self.device.sync_data().await
    {
      return Err(e.into());
    }

    self
      .flushed_until_address
      .store(safe_tail, Ordering::Release);
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
      return Ok(committed);
    }
    self.commit_to(target).await
  }

  /// 设备级数据同步（fdatasync）：[`FsyncPolicy::Deferred`] 档的强持久化显式
  /// 入口——把已提交但未 sync 的批次推至设备；Always 档无需调用（每次提交
  /// 批次已随批 sync）。设备级全量同步语义见 `Device::sync_data`（覆盖全部
  /// 线程在调用发起前完成的写入），位点原子量不动
  pub async fn sync(&self) -> Result<()> {
    Ok(self.device.sync_data().await?)
  }

  /// 提交并持久化至指定逻辑地址（严格对标 Garnet Group Commit 流水线合并模式）
  ///
  /// C# 日志消费/提交管理接口的统一落点（Leader 级联批量消费环形缓冲条目落盘，
  /// 提交元数据帧随批尾写入即 commit 点推进）：
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/ILogEntryConsumer.cs:Consume
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.Chunked.cs:Consume
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/ILogCommitManager.cs:Commit
  pub async fn commit_to(&self, target: u64) -> Result<u64> {
    // 1. 快速短路（0 I/O）：目标水位已被硬件 sync 持久化覆盖
    let committed = self.committed_until_address.load(Ordering::Acquire);
    if target <= committed {
      return Ok(committed);
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

/// WAL 提交步进器：批次目标取安全尾地址（在途写入下界），水位取已提交位点，
/// 物理持久化复用环形缓冲刷盘 + fdatasync 单点实现
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
    // commit 元数据帧随批尾写入（对标 TsavoriteLog.cs:TryEnqueueCommitRecord）：
    // 帧覆盖至自身末尾并与数据记录同一刷盘批次原子持久；帧游标防级联轮重复写帧
    let mut goal = target;
    let last_frame = self.wal.last_commit_frame.load(Ordering::Acquire);
    if goal > last_frame {
      match self
        .wal
        .enqueue(&commit::encode_payload(commit::CommitMeta {
          begin: self.wal.begin_address.load(Ordering::Acquire),
          cookie: self.wal.pending_cookie.load(Ordering::Acquire),
        })) {
        Ok(frame_addr) => {
          let frame_end = frame_addr + commit::COMMIT_FRAME_TOTAL_LEN;
          self
            .wal
            .last_commit_frame
            .store(frame_end, Ordering::Release);
          goal = goal.max(frame_end);
        }
        // 环形满时跳帧降级：本批数据照常刷盘腾窗，帧待下轮 commit 补写
        //（恢复侧回退上一帧收敛，提交边界不虚高）
        Err(Error::BufferFull { .. }) => {}
        Err(e) => return Err(e),
      }
    }

    let flushed = self.wal.flushed_until_address.load(Ordering::Acquire);
    if goal > flushed {
      self.wal.flush_and_sync_range(flushed, goal).await
    } else {
      Ok(self.wal.committed_until_address.load(Ordering::Acquire))
    }
  }
}
