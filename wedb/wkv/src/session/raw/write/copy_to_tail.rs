//! 冷数据 copy-to-tail 内核：候选链定位 → 尾部追加 → CAS 挂载 → 统一收尾
//!
//! 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToTail.cs:TryCopyToTail
//! （rust 对位：磁盘/只读区记录的 CopyUpdater 慢路径单源内核，
//! `session/raw/write/inplace.rs` 的删除慢路径与 `range_index/stub.rs` 的
//! RIPROMOTE/RIRESTORE 治愈路径在此转调，杜绝两份手写骨架与收尾口径分叉）

use wbase::simd::fast_key_eq;
use wdev::Device;
use whlog::RecordOutput;
use wrecord::{RecordHeader, record_size};

use crate::{error::Result, session::StoreSession};

/// copy-to-tail 内核结果（对标 C# TryCopyToTail 的 SUCCESS / NOTFOUND / 零写
/// 分类；调用方各自映射到自己的错误码/重试语义）
pub(crate) enum CopyToTailOutcome<T> {
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
    /// plan 闭包产出的调用方上下文
    ctx: T,
  },
}

impl<D: Device> StoreSession<D> {
  /// 冷数据 copy-to-tail 慢路径内核
  ///
  /// 骨架（严格对标 C# TryCopyToTail 所在异步冷数据执行链路）：begin_address →
  /// lookup_candidates → skip_read_cache（链头 RC 滑出窗口时等待驱逐方完成清洗
  /// 并发布 ClosedUntilAddress 后回链头重探，绝不静默丢弃存活候选）→ while
  /// cur >= begin 沿记录 prev 回溯（is_on_disk 分派磁盘纯设备读 / 内存守卫读，
  /// fast_key_eq 跳过 Tag 碰撞键）→ plan 判定与补丁帧构造 → 尾部追加 →
  /// update_address CAS 挂载（对标 Helpers.cs:CASRecordIntoChain 的哈希槽位
  /// 单点 CAS 即原子脱钩 ReadCache 前缀；挂载以 cand 槽位地址二次哈希定位，
  /// 与 C# HashEntryInfo.TryCAS 零二次哈希口径的差异属 windex API 形态，另列
  /// 收敛条，本内核不吞）。
  ///
  /// 收尾统一（C# 同一内核绝不留未挂载的存活帧：SetNewRecordInvalid +
  /// OnDispose + SaveAllocationForRetry 三步在 rust 的单点承接）：
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
  /// - `is_tombstone` 决定新帧墓碑标记；`notify` 选择写监听口径（用户写效果
  ///   入 AOF 走 `append_record`，内部治愈帧旁路走 `append_record_compacted`）；
  /// - `plan` 对命中记录产出 `(补丁帧, 调用方上下文)`；返回 `None` 即零写
  ///   `Closed`（调用方在 plan 内完成墓碑/类型/幂等判定）。
  pub(crate) async fn copy_record_to_tail<T>(
    &self,
    key: &[u8],
    notify: bool,
    is_tombstone: bool,
    mut plan: impl FnMut(&RecordOutput) -> Option<(Vec<u8>, T)>,
  ) -> Result<CopyToTailOutcome<T>> {
    loop {
      let begin_addr = self.store.begin_address();
      let addrs = {
        let _guard = self.enter_gated();
        self.store.index.load().lookup_candidates(key)
      };
      let mut restart = false;
      for cand in addrs {
        // cand 为槽位原始地址（可能为 ReadCache 虚拟地址）：新帧 CAS 挂载必须以它
        // 为 old_address；prev 链接顺链解析后的首个主日志地址（跳过易失 RC 环节）
        let main_head = {
          let _guard = self.enter_gated();
          self.store.read_cache.skip_read_cache(cand)
        };
        let main_head = match main_head {
          Some(m) => m,
          // 走查触及滑出窗口的 RC 记录不可判读：等待驱逐方完成清洗后刷新纪元
          // 回链头重探
          None => {
            self
              .store
              .read_cache
              .need_to_wait_for_eviction(cand, || self.participant.refresh());
            self.participant.refresh();
            restart = true;
            break;
          }
        };
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
          // 命中源记录：plan 判定零写即 Closed，否则携补丁帧入尾部追加
          let Some((payload, ctx)) = plan(&record) else {
            return Ok(CopyToTailOutcome::Closed);
          };
          let src_addr = cur;
          let new_addr = if notify {
            self
              .append_record(key, &payload, main_head, is_tombstone)
              .await?
          } else {
            self
              .append_record_compacted(key, &payload, main_head, is_tombstone)
              .await?
          };
          let cas_ok = {
            let _guard = self.enter_gated();
            self.store.index.load().update_address(key, cand, new_addr)
          };
          if !cas_ok && self.store.config.enable_revivification {
            // 败帧回收（CAS 已落败，新帧不可达；对齐快路径 RetryAlloc::discard
            // 的补偿口径，杜绝治愈路径此前「落败即遗弃」的槽位泄漏）
            let rec_size = record_size(key.len(), payload.len()) as u32;
            self
              .store
              .reviv_pool
              .put(new_addr, rec_size, self.store.hlog.read_only_address());
          }
          return Ok(CopyToTailOutcome::Appended {
            src_addr,
            cas_ok,
            ctx,
          });
        }
      }
      if !restart {
        return Ok(CopyToTailOutcome::Miss);
      }
    }
  }
}
