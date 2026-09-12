//! 条件迁移：存活记录原子复制至日志尾部（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ConditionalCopyToTail.cs:CompactionConditionalCopyToTail）

use wrecord::record_size;

use super::{CopyOutcome, LogCompactor};
use crate::{
  error::Result,
  host::{CompactSession, CompactStore},
};

impl<S: CompactStore> LogCompactor<S> {
  /// 条件性复制存活记录至尾部（对标 Tsavorite `CompactionConditionalCopyToTail`
  /// 及其内部 `ConditionalCopyToTail` 的重试环）
  ///
  /// 仅当该记录依然是哈希索引中的最新版本且未被并发覆盖时，将其追加至尾部并原子 CAS 替换索引地址。
  /// CAS 失败须区分两类竞争：真并发写（同键新版本/墓碑已生效，返回 [`CopyOutcome::Superseded`]
  /// 弃迁）与良性改写（并发读触发 ReadCache 挂链提升或驱逐回写索引槽位，记录仍存活且仍是
  /// 最新版本，必须复核后换新期望槽位重试，否则存活记录会随截断被误弃）。孤儿副本一律归还
  /// 复活池复用，绝不覆写并发新版本数据。全程持有纪元保护：尾部追加、索引 CAS 与复活池归还
  /// 均为共享内存结构变更（与 session 写路径及 ReadCache 回填口径一致）。
  /// 追加复用 `session.append_record` 的有界重试：环形缓冲翻转（PageNotReady）时先异步
  /// 刷盘并驱逐被覆盖的旧页再重试，循环直至成功或非 PageNotReady 错误上抛，
  /// 保证长区间紧缩不会因页耗尽而整轮失败（与 session 写路径口径一致）。
  /// 与 C# `while (true)` 无界重试的刻意差异：竞争重试耗尽且复核确认记录仍为最新存活版本时，
  /// 返回 [`CopyOutcome::Retain`] 而非弃迁——调用方回退截断点保住该记录，绝不误删存活数据。
  pub(super) async fn conditional_copy_to_tail(
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
      // 1. 追加至尾部，版本链指针指向 expected_main_addr（PageNotReady 自动驱逐旧页后重试）
      let new_addr = session
        .append_record(key, val, expected_main_addr, false)
        .await?;

      // 2. 原子 CAS 替换索引中的地址：index_addr -> new_addr
      if self.store.index().update_address(key, index_addr, new_addr) {
        return Ok(CopyOutcome::Copied);
      }

      // 3. 竞争失败：孤儿副本归还复活池，复核该记录是否仍是最新存活版本
      if self.store.enable_revivification() {
        let rec_size = record_size(key.len(), val.len()) as u32;
        self
          .store
          .reviv_put(new_addr, rec_size, self.store.read_only_address());
      }
      match self
        .find_latest_address(session, key, Some((expected_main_addr, false)))
        .await?
      {
        // 良性竞争（索引槽位被 ReadCache 挂链/驱逐回写改写）：换新期望槽位重试
        Some(latest) if latest.main_addr == expected_main_addr && !latest.is_tombstone => {
          index_addr = latest.index_addr;
        }
        // 真并发覆盖/删除：新数据已生效，安全放弃迁移
        _ => return Ok(CopyOutcome::Superseded),
      }
    }
    // 重试耗尽且记录仍为最新存活版本：保守保留，绝不随截断误删存活数据
    Ok(CopyOutcome::Retain)
  }
}
