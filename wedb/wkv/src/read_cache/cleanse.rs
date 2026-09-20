//! 页关闭清洗：环形回绕驱逐前恢复哈希链
//!
//! 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:ReadCacheEvict
//! （CleanseHashChain）
//!
//! 对标口径：C# CASRecordIntoChain 的锁免设计下，写侧 CAS 即原子脱钩 ReadCache
//! 前缀，被取代的孤儿缓存记录不在写侧作废，统一由页关闭清洗回收（仍指向槽位的
//! 记录经 update_address 恢复主日志地址，已脱钩槽位 CAS 自然落败跳过）；本文件
//! 承接同一单点回收机制。挂载失败帧的即时作废由 `read_cache/append.rs` 的
//! `set_invalid_atomic` 承担（对标 C# BlockAllocate.cs:TryAllocateRecordReadCache
//! 的败帧 ReadCacheAbandonRecord）。

use wbase::addr::with_read_cache;
use whlog::for_each_record_in_page;
use windex::HashIndex;

use super::ReadCache;

impl ReadCache {
  /// 覆写旧页前扫描其中的 ReadCache 记录，将仍指向这些记录的哈希索引槽位原子恢复至主日志地址（严格对标 Garnet CleanseHashChain）
  ///
  /// 页内记录链走查复用 whlog 单点内核 [`for_each_record_in_page`]（零头/pad/
  /// 不可解码/越页界终止与按物理尺寸步进不再本地复刻），本函数只提供
  /// 哈希链恢复语义闭包。
  pub(super) fn cleanse_page(&self, page_id: u64, index: &HashIndex) {
    let page_start_addr = self.buffer.page_start_address(page_id);
    let guard = self.buffer.read_page(page_id);
    for_each_record_in_page(&guard, 0, |header, offset, key, _val| {
      // 已作废记录（append 挂载失败即时作废）：对标 C# ReadCacheEvict 的
      // kTempInvalidAddress 跳过语义——未挂载记录无需哈希链恢复，推进下一条
      // 而非截断扫描
      if header.is_closed() {
        return true;
      }

      let rc_addr = with_read_cache(page_start_addr + offset as u64);
      let prev_addr = header.address();
      if prev_addr == 0 {
        index.delete(key, rc_addr);
      } else {
        index.update_address(key, rc_addr, prev_addr);
      }
      true
    });
  }
}
