//! 冷数据 copy-to-tail 内核：候选链定位 → 尾部追加 → CAS 挂载 → 统一收尾
//!
//! 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToTail.cs:TryCopyToTail
//! （rust 对位：磁盘/只读区记录的 CopyUpdater 慢路径单源内核，
//! `session/raw/write/inplace.rs` 的删除慢路径与 `range_index/stub.rs` 的
//! RIPROMOTE/RIRESTORE 治愈路径在此转调，杜绝两份手写骨架与收尾口径分叉；
//! 挂载收尾 [`StoreSession::cas_mount_copied_frame`] 另为 `raw/read.rs` 的两条
//! 冷读晋升臂（磁盘回填、内存不可变区命中）共用，全库仅此一处分派）

use wbase::simd::fast_key_eq;
use wdev::Device;
use whlog::RecordOutput;
use wrecord::RecordHeader;

use crate::{error::Result, session::StoreSession};

/// copy-to-tail 内核结果（对标 C# TryCopyToTail 的 SUCCESS / NOTFOUND / 零写
/// 分类；调用方各自映射到自己的错误码/重试语义）
pub(crate) enum CopyToTailOutcome {
  /// 候选链上无该键存活记录（已截断 / 从未存在）
  Miss,
  /// 命中但 plan 判定零写（墓碑 / 非目标记录 / 已治愈）
  Closed,
  /// 已追加补丁帧并执行索引 CAS 挂载
  Appended {
    /// 命中的源记录地址
    src_addr: u64,
    /// 索引 CAS 是否成功
    cas_ok: bool,
  },
}

impl<D: Device> StoreSession<D> {
  /// 尾部晋升帧的索引挂载与败帧回收（copy-to-tail 收尾单点，对标 C#
  /// TryCopyToTail 的 `hei.TryCAS` + 败帧 `OnDispose` 回收两步）
  ///
  /// 三个晋升来源共用本单点，杜绝「追加 + 挂链 + 败帧回收」样板复抄：
  /// - [`Self::copy_record_to_tail`] 内核（删除慢路径 / RIPROMOTE 治愈）；
  /// - 磁盘冷读回填（`raw/read.rs::read_from_disk`）；
  /// - 内存不可变区命中同步晋升（`raw/read.rs::promote_immutable_read_hit`）。
  ///
  /// `old_addr` 为挂载前索引应指的源地址（候选槽位原始地址，RC 虚拟地址亦可），
  /// 失配即并发写已推进索引，本次晋升作废；命中记录非链头时 CAS 自然失配为
  /// no-op（对标 C# 同一 TryCAS 判据）。败帧仅在复活池开启时回收，关闭时帧已
  /// 脱链不可达、交由截断回收，与快路径 RetryAlloc::discard 的补偿口径一致。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToTail.cs:TryCopyToTail
  #[cold]
  pub fn cas_mount_copied_frame(
    &self,
    key: &[u8],
    old_addr: u64,
    new_addr: u64,
    frame: u32,
  ) -> bool {
    let _guard = self.enter_gated();
    let cas_ok = self
      .store
      .index
      .load()
      .update_address(key, old_addr, new_addr);
    if !cas_ok && self.store.config.enable_revivification {
      // 败帧回收（CAS 已落败，新帧不可达；对齐 GetAllocationForRetry 的
      // OnDispose(InitialWriterCASFailed) 口径，杜绝「既不置失效也不回收」泄漏）
      // 归池单点含落笔前原子密封（对标 C# TryTransferToFreeList 的 IsClosed
      // 前置断言，Helpers.cs:128），防无锁读者对将复用帧读出撕裂内容
      self.store.transfer_to_reviv_pool(new_addr, frame);
    }
    cas_ok
  }

  /// 冷数据 copy-to-tail 慢路径内核
  ///
  /// 骨架（严格对标 C# TryCopyToTail 所在异步冷数据执行链路）：begin_address →
  /// lookup_candidates → skip_read_cache_with_wait（逐位置锚定的带等待走查单口：链上
  /// 任一 RC 记录滑出窗口即以该地址就地等待驱逐方完成清洗并发布 ClosedUntilAddress，
  /// 再重读本键哈希项回链头重探，绝不静默丢弃存活候选）→ while
  /// cur >= begin 沿记录 prev 回溯（is_on_disk 分派磁盘纯设备读 / 内存守卫读，
  /// fast_key_eq 跳过 Tag 碰撞键）→ plan 判定与补丁帧构造 → 尾部追加 →
  /// update_address CAS 挂载（对标 Helpers.cs:CASRecordIntoChain 的哈希槽位
  /// 单点 CAS 即原子脱钩 ReadCache 前缀；挂载以 cand 槽位地址二次哈希定位，
  /// 与 C# HashEntryInfo.TryCAS 零二次哈希口径的差异属 windex API 形态，另列
  /// 收敛条，本内核不吞）。
  ///
  /// 收尾统一（C# 同一内核绝不留未挂载的存活帧：SetNewRecordInvalid +
  /// OnDispose + SaveAllocationForRetry 三步在 rust 的单点承接）：
  /// - CAS 挂载与败帧回收两步已收成 [`Self::cas_mount_copied_frame`] 单点，与读路径
  ///   两条冷读晋升臂（磁盘回填、内存不可变区命中）同享一份口径；
  /// - CAS 败帧：复活池开启时必回复活池（对标 SaveAllocationForRetry /
  ///   OnDispose(InitialWriterCASFailed) 的 FreeRecordPool 回收口径），杜绝
  ///   「既不置失效也不回收」的槽位泄漏；复活池关闭时帧已脱链不可达，交由
  ///   截断回收——与快路径 RetryAlloc::discard 的既有 CAS 失败补偿口径一致；
  /// - 成功侧 ReadCache 前缀经 CAS 原子脱钩后即为孤儿记录，统一交由
  ///   `read_cache/cleanse.rs` 的 cleanse_page 在页关闭时恢复/跳过回收（对标
  ///   C# CASRecordIntoChain 注释「Dropped read-cache records are orphaned
  ///   and reclaimed by ReadCacheEvict when their page is closed」），写侧
  ///   不再各自作废，杜绝「四有一无」的处置并存。
  ///
  /// 纪元守卫纪律（对齐 read_from_disk 冷读协议）：索引探测与挂载持短守卫
  /// 分段执行；磁盘区记录读取走免纪元纯设备路径，绝不持守卫跨越磁盘 I/O
  /// await——否则冷数据链回溯全程钉住本线程纪元，阻塞其他会话的 safe_head
  /// 推进与页回收。
  ///
  /// 参数：
  /// - `is_tombstone` 决定新帧墓碑标记；`notify` 选择写监听口径（用户写效果在
  ///   索引 CAS 挂载成功后发通知镜像 AOF，内部治愈帧旁路）；追加统一经
  ///   [`StoreSession::allocate_record`] 的复活池取臂（对标 C# TryCopyToTail 的
  ///   AllocateOptions{recycle=true}），池取失败自然回落尾部追加；
  /// - `plan` 对命中记录产出补丁帧；返回 `None` 即零写 `Closed`（调用方在 plan
  ///   内完成墓碑/类型/幂等判定）。
  ///
  /// 镜像点收口（票 zcode-r34-writekernel 条目三，对标 C# TryCopyToTail 在 CAS
  /// 挂载成功后才进入 AOF 回调链的单一「生效后镜像」次序）：分配臂恒不通知，
  /// 通知与 [`Self::cas_mount_copied_frame`] 结果联动——CAS 败帧（`cas_ok=false`
  /// 交调用方 `delete_raw`/`take_raw` 刷新重试）不再产生 AOF 条目，杜绝「CAS 败
  /// + 重试遇硬错误终止」窗内主存键存活而 AOF 已有墓碑的持久双发散发散窗
  pub(crate) async fn copy_record_to_tail(
    &self,
    key: &[u8],
    notify: bool,
    is_tombstone: bool,
    mut plan: impl FnMut(&RecordOutput) -> Option<Vec<u8>>,
  ) -> Result<CopyToTailOutcome> {
    // 候选收集前的分裂协同（对标 C# TryCopyToTail 所在冷数据执行链的入口相位
    // 铁律，InternalUpsert.cs:64-66 / InternalDelete.cs:57-59 同一协议）：
    // 扩容期先迁移目标分块，杜绝在未迁移新表查得空候选集假 Miss
    // （冷数据删除被吞 / RangeIndex 存根自愈失效）
    self.ensure_split(key)?;
    let begin_addr = self.store.begin_address();
    let addrs = {
      let _guard = self.enter_gated();
      self.store.index.load().lookup_candidates(key)
    };
    for cand in addrs {
      // cand 为槽位原始地址（可能为 ReadCache 虚拟地址）：新帧 CAS 挂载必须以它
      // 为 old_address；prev 链接顺链解析后的首个主日志地址（跳过易失 RC 环节）。
      // 解析走带等待单口：触到滑窗驱逐过渡态（含链中段滑出）时以该滑出地址就地
      // 等待清洗落定、再重读本键哈希项回链头重探（对标 C# SkipReadCache 的
      // RestartChain），绝不静默丢弃存活候选；等待为纯自旋，故本调用恒在
      // enter_gated 短守卫之外发起（持守卫自旋会钉住本线程纪元，阻塞驱逐方的
      // 纪元延迟清洗，等待永不落定）
      let main_head = self.store.read_cache.skip_read_cache_with_wait(
        || self.rc_hash_entry_head(key, cand),
        || self.participant.refresh(),
      );
      if main_head == 0 {
        continue;
      }
      let mut cur = main_head;
      while cur >= begin_addr {
        // 磁盘区（cur < head）免纪元纯设备读；内存驻留（含过渡区罕见回退）守卫内
        // 读取，read_record 内存命中路径纯同步完成、无实际让出；读失败断链止走查
        let Ok(record) = self.read_record(cur).await else {
          break;
        };
        if !record.key().is_ok_and(|rec_key| fast_key_eq(rec_key, key)) {
          // Tag 碰撞：解析记录头提取前驱地址，沿链回溯
          cur = RecordHeader::read_address(record.as_slice()).unwrap_or(0);
          continue;
        }
        // 命中源记录：plan 判定零写即 Closed，否则携补丁帧入池取/尾部追加
        // 池取收编（对标 C# TryCopyToTail.cs:33 以 AllocateOptions{recycle=true}
        // 统一经 TryAllocateRecord 的 TryTakeFreeRecord 臂，BlockAllocate.cs:57-82）：
        // 下界按候选链首 cand 抬升（reviv_chain_floor 单点，严格保证复活槽地址高于
        // 旧链首防 prev 逆向成环）；池取失败自然回落尾部追加，行为严格超集；
        // 分配恒不通知（镜像点收口），通知在下方 CAS 挂载成功后联动发出
        let Some(payload) = plan(&record) else {
          return Ok(CopyToTailOutcome::Closed);
        };
        let src_addr = cur;
        let min_eligible = self.reviv_chain_floor(cand);
        let (new_addr, frame, ver) = self
          .allocate_record(key, &payload, main_head, is_tombstone, min_eligible)
          .await?;
        let cas_ok = self.cas_mount_copied_frame(key, cand, new_addr, frame);
        // 生效后镜像（条目三收口）：仅索引 CAS 挂载成功的帧入 AOF，败帧由调用
        // 方刷新重试后重新走本内核（重试帧重新判定、重新分配、重新挂载）；
        // 戳取分配成功点传导值（读点下移，与记录头纪元位同源同点）
        if cas_ok && notify {
          self.notify_write_listener_with_version(key, &payload, is_tombstone, ver)?;
        }
        return Ok(CopyToTailOutcome::Appended { src_addr, cas_ok });
      }
    }
    Ok(CopyToTailOutcome::Miss)
  }
}
