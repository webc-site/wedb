use std::sync::atomic::Ordering;

use wbase::group_commit::{Enter, GroupCommitStep};
use wbftree::Error as WbftreeError;
use wdev::Device;
use wval::NamespaceDbCodec;

use super::WedbStore;
use crate::{
  error::{Error, Result},
  range_index::patch_stub_record,
};

impl<D: Device> WedbStore<D> {
  /// OnFlush 记录语义内核：对 `[from_addr, until_addr)` 内的驻留记录原位触发刷盘快照
  /// (1:1 对标 C# libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnFlush，
  /// 底层页走查内核对标 Tsavorite ObjectAllocatorImpl.cs:FlushRecordsInRange)
  ///
  /// 区间界一律由 [`Self::flush_until`] 经 whlog [`whlog::HybridLog::flush_write_range`]
  /// 单点求值后传入（与同一轮的设备写同界）；驻留判定、页写锁、初始页起步偏移、页首
  /// 逻辑地址换算等页簿记归 whlog [`whlog::HybridLog::flush_records_in_addr_range`]，
  /// 本函数只提供记录语义闭包，不钻取 buffer/config。
  ///
  /// 分发闭环说明：C# `GarnetRecordTriggers.CallOnFlush => rangeIndexManager != null`，
  /// OnFlush 的唯一触发类别即 RangeIndex 存根快照（VectorManager 仅挂 OnEvict/
  /// OnDiskRead，无 OnFlush 钩子），本实现只识别 RangeIndex 即与 C# 注册面完全等价，
  /// 无遗漏的通用分发臂。
  ///
  /// core 触发器契约同挂此处（宿主回调与 core 接口在 rust 折叠为同一挂点）：
  /// libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/IRecordTriggers.cs:OnFlush
  fn on_flush_records(&self, from_addr: u64, until_addr: u64) -> Result<()> {
    // OnFlush 触发器门控（对标 C# GarnetRecordTriggers.cs:53
    // `CallOnFlush => rangeIndexManager != null`：管理器未装配则 Flush 零走查）。
    // rust 侧管理器恒装配（[Self::range_index] 非 Option），语义等价物 =
    // 「无任何活树」：meta 键记录只由已注册树存在期间的 range/vector 域写入，
    // 无树时走查闭包对每条记录都在墓碑/非 meta 判据处零功返回，剩余成本纯是
    // 页走查步进与逐条解码尝试（探针实测 1M 条全表 16.6ms，bulk 段 6.7%），
    // 短路不省略任何置位义务。树注册/删除与走查判定的竞态窗口内若有 meta
    // 记录存根未置位，由既有「惰性恢复承接」路径治愈（与快照未落拒绝置位
    // 同路，见下方闭包注释），不构成第二套治愈机制。
    if self.range_index.live_index_count() == 0 {
      return Ok(());
    }
    let mut flush_err: Option<WbftreeError> = None;
    self
      .hlog
      .flush_records_in_addr_range(from_addr, until_addr, |header, addr, key, val| {
        if header.is_tombstone() {
          return true;
        }
        if NamespaceDbCodec::decode_meta_user_key(key).is_none() {
          return true;
        }
        // OnFlush 存根置位一律转调 wkv 唯一治愈内核（对标 C#
        // GarnetRecordTriggers.cs:OnFlush → RangeIndexManager.cs:SnapshotTreeForFlush
        // 成功尾段的 SetFlushedFlag(valueSpan)）：内核识别存活 RangeIndex 元记录
        // 并就地改 35B 存根窗口，页写锁内零复制零分配、值体长度无上限，
        // 杜绝旧实现在此的第四口径（解码后无条件补写 Flushed 位）。
        // 快照未落（所有权已转移 / 工作文件缺失的不变量破坏 → on_flush_address
        // 按 C# LogOnFlushInvariantViolation 明令「must NOT set IsFlushed」拒绝置位）
        // 时内核返回 false 零写，记录留在未刷盘态交由惰性恢复承接。
        // 树身份 = 物理 Meta 键（decode_meta_user_key 仅为 Meta 域过滤，
        // 整物理键即树身份键直传，零解码换键——身份含域与树注册同源）
        patch_stub_record(val, |stub| {
          if stub.is_flushed() {
            return false;
          }
          match self.range_index.on_flush_address(key, stub, addr) {
            Ok(()) => stub.is_flushed(),
            Err(e) => {
              flush_err = Some(e);
              false
            }
          }
        });
        flush_err.is_none()
      });
    if let Some(e) = flush_err {
      return Err(e.into());
    }
    Ok(())
  }

  /// 测试装配专用握手口：不触碰设备，对当前日志的全部驻留记录触发一次 OnFlush
  /// （外部唯一消费为 range_index / tiered_stub_heal 两处集成测试）。
  ///
  /// 走查 `[0, tail)` 全域（含已承诺前缀；已置 Flushed 的存根按 `patch_stub_record`
  /// 的幂等判据自然跳过），与生产轮的区间求值无关，故不构成第二套刷盘通道。
  #[doc(hidden)]
  pub fn on_flush_walk(&self) -> Result<()> {
    self.on_flush_records(0, self.tail_address())
  }

  /// 把持久化前缀到 `until_addr` 之间的新增字节落盘，并对同一区间原位触发 OnFlush
  ///
  /// 走查界与写界同源于 whlog [`whlog::HybridLog::flush_write_range`]（对标 C#
  /// `OnPagesMarkedReadOnlyWorker` 先 `FlushRecordsInRange(flushStart, flushEnd)` 再
  /// `AsyncFlushPagesForReadOnly(flushStart, flushEnd)` 的同区间串联）：多走查是幂等冗余，
  /// 少走查即令存根带着未置位上盘（虽由惰性恢复承接，但白付一次恢复代价），故二者必为
  /// 一次求值、一个区间。前缀已覆盖请求终点时零 I/O 返回。
  pub(crate) async fn flush_until(&self, until_addr: u64) -> Result<()> {
    let range = self.hlog.flush_write_range(until_addr);
    if range.is_empty() {
      return Ok(());
    }
    self.on_flush_records(range.from_address, range.until_address)?;
    self
      .hlog
      .flush_addr_range(range.from_address, range.until_address)
      .await
      .map_err(Error::from)
  }

  /// 将内存中所有驻留脏页异步刷盘并同步设备（严格对标 Garnet Group Commit 流水线）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:Flush
  /// （C# Flush(wait) = ShiftReadOnlyAddress(tail, wait) 封印并等传播；rust 侧
  /// 「等持久化」语义由 GroupCommit 管线以 synced_until 水位承接，封印面
  /// 收敛在 [`Self::flush_and_evict_all`] 与检查点链）
  pub async fn flush_all(&self) -> Result<()> {
    let target = self.tail_address();

    // 1. 快速短路（0 I/O）：目标水位已被硬件 sync 持久化覆盖
    if target <= self.synced_until() {
      return Ok(());
    }

    // 2. 状态机协商：判定成为 Leader 还是 Follower
    let guard = match self.flush_pipeline.enter(target, || self.synced_until()) {
      Enter::Done(_) => return Ok(()),
      Enter::Follow(rx) => {
        // 3. Follower 分支：挂起等待 Leader 批量唤醒，绝不重复发起 I/O
        return self
          .flush_pipeline
          .wait(rx, target, || self.synced_until())
          .await
          .map(|_| ())
          .map_err(Error::from);
      }
      // 升级为 Leader，接管物理刷盘管道
      Enter::Lead(guard) => guard,
    };

    // 4. Leader 级联执行循环（Cascade Loop）
    self
      .flush_pipeline
      .run_leader(guard, FlushStep { store: self })
      .await
      .map(|_| ())
  }

  /// 将内存所有页面刷盘并全部驱逐至磁盘区（对标 Tsavorite FlushAndEvict）
  ///
  /// 次序严格对标 C# `LogAccessor.FlushAndEvict`：`ShiftReadOnlyToTail`（封印并等
  /// 纪元排空 → 由排空动作发起刷盘）→ 等 FlushedUntil 达标 → 推进 head 驱逐。
  /// rust 侧的排空+刷盘折叠在同一个刷盘内核里（whlog `flush_addr_range` 入口先封后刷），
  /// 故此处显式封印的意义是把 wkv 侧联动（复活池清扫）落在驱逐之前，且使
  /// 「先封后刷」在读侧上界单源之上再有一处意图声明；重复封印为 fetch_max 空转，零成本。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:FlushAndEvict
  ///
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftReadOnlyToTail
  ///（本函数第一臂 shift_read_only_address(tail) 即其 tail 特化单点）
  pub async fn flush_and_evict_all(&self) -> Result<()> {
    let tail = self.tail_address();
    self.shift_read_only_address(tail);
    self.flush_all().await?;
    self.shift_head_address(tail);
    Ok(())
  }

  /// 读取硬件已完成 sync 持久化的最高连续逻辑地址水位
  #[inline]
  pub(crate) fn synced_until(&self) -> u64 {
    self.synced_until.load(Ordering::Acquire)
  }
}

/// KV 页刷盘步进器：批次目标取混合日志尾地址，水位取硬件 sync 持久化位点，
/// 物理持久化为增量逻辑区间刷盘 + 硬件 fsync。step 只给出批次上界，
/// 区间求值、OnFlush 走查与落盘一律经 store::flush_until 单点分发
/// （对标 C# AllocatorBase.AsyncFlushPagesForSnapshot 的单一刷盘内核形态）
///
/// 读侧上界口径：step 不自备 safe_read_only 门槛，也不另立钳制——刷盘内核
/// （whlog `flush_addr_range`）在写入之前把待刷上界封印为 SafeReadOnlyAddress 并等
/// 纪元排空，故无论宿主是自带封印的检查点链（wcpr create.rs）还是 FLUSHLOG 链
/// （[WedbStore::flush_and_evict_all]），落盘的字节恒已定稿，`flushed_until` 恒不越过
/// 安全只读线（对标 C# 仅由 OnPagesMarkedReadOnly 纪元动作驱动刷盘的单一形态）
///
/// 与 waof `WalCommitStep` 共用同一内核 `wbase::GroupCommitPipeline`（leader 级联
/// 循环、退避与批次编排都在内核里），本 Step 仅注入批次目标与水位语义；二者同构
/// 不同参，属 Step 注入形态而非重复实现，严禁合并或拆出第三套流水线。
struct FlushStep<'a, D: Device> {
  store: &'a WedbStore<D>,
}

impl<D: Device> GroupCommitStep for FlushStep<'_, D> {
  type Error = Error;

  #[inline]
  fn tail(&self) -> u64 {
    self.store.tail_address()
  }

  #[inline]
  fn watermark(&self) -> u64 {
    self.store.synced_until()
  }

  async fn step(&self, target: u64) -> Result<u64> {
    // (1) 增量落盘：仅把持久化前缀到 target 之间的新增字节写向设备。下界圆整、上界
    // 钳制与 OnFlush 走查全在 store::flush_until → whlog 单点，step 不另算页区间，
    // 同页重叠写的页序守卫由刷盘内核 flush_gate 异步闸承接，杜绝在此另加直连 hlog
    // 的快路径
    self.store.flush_until(target).await?;

    // (2) 物理介质持久化（硬件 fsync）：全系统单协程串行执行，彻底消除抖动争抢
    self.store.device.sync().await.map_err(Error::from)?;

    // (3) 推进持久化水位（取 min 兜住并发刷盘已先行推进前缀的情形）
    let new_synced = target.min(self.store.hlog.flushed_until_address());
    self
      .store
      .synced_until
      .fetch_max(new_synced, Ordering::AcqRel);
    Ok(new_synced)
  }
}
