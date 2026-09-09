//! 哈希索引最新记录探针（Lookup 判活与 CAS 复核共用的地址解析路径）

use windex::CandidateAddresses;

use super::LogCompactor;
use crate::{
  error::Result,
  host::{CompactSession, CompactStore},
};

/// 最新记录探查结果
pub(super) struct LatestRecord {
  /// 主日志中的有效逻辑地址
  pub(super) main_addr: u64,
  /// 索引槽位中的地址（若为 ReadCache 驻留则携带虚拟标记位）
  pub(super) index_addr: u64,
  /// 是否为墓碑记录
  pub(super) is_tombstone: bool,
}

impl<S: CompactStore> LogCompactor<S> {
  /// 探查指定键在 store 中的最新记录地址及墓碑状态
  ///
  /// 1. 通过无堆分配栈列表 `lookup_candidates` 获取 Tag 匹配的所有候选槽位并降序排列；
  /// 2. 剥离 ReadCache 虚拟标记位（若已落入 ReadCache，顺链跳跃获取底层主日志真实逻辑地址）；
  /// 3. 若候选地址匹配调用方已知当前记录（`known_curr`），直接复用其键匹配与墓碑状态，零冗余 I/O；
  /// 4. 清理已截断或失效的陈旧索引槽位引用；
  /// 5. 返回首个匹配指定 Key 的最新记录信息（包含主日志真实地址与索引原槽位地址）。
  pub(super) async fn find_latest_address(
    &self,
    session: &S::Session,
    key: &[u8],
    known_curr: Option<(u64, bool)>,
  ) -> Result<Option<LatestRecord>> {
    let _guard = session.enter_epoch();
    let mut addrs = self.store.index().lookup_candidates(key);
    addrs.sort_descending();
    let begin_addr = self.store.begin_address();

    let mut found = None;
    let mut duplicates = CandidateAddresses::new();

    for &addr in addrs.iter() {
      // 剥离 ReadCache 虚拟标记位
      let main_addr = if self.store.is_read_cache_addr(addr) {
        self.store.skip_read_cache(addr)
      } else {
        addr
      };

      // 地址为 0（已滑出 ReadCache 窗口）或已落入截断区，标记为失效陈旧槽位
      if main_addr == 0 || main_addr < begin_addr {
        duplicates.push(addr);
        continue;
      }

      // 若当前候选地址与调用方已知记录地址一致，直接复用已校验结果，避免冗余 I/O
      if let Some((known_addr, known_tombstone)) = known_curr
        && main_addr == known_addr
      {
        if found.is_none() {
          found = Some(LatestRecord {
            main_addr,
            index_addr: addr,
            is_tombstone: known_tombstone,
          });
        } else {
          duplicates.push(addr);
        }
        continue;
      }

      match self.store.hlog().read_record(main_addr).await {
        Ok(record) => {
          match record.key() {
            Ok(rec_key) if rec_key == key => {
              if found.is_none() {
                let is_tombstone = record.is_tombstone().unwrap_or(false);
                found = Some(LatestRecord {
                  main_addr,
                  index_addr: addr,
                  is_tombstone,
                });
              } else {
                duplicates.push(addr);
              }
            }
            // 键解码失败：介质损坏或半帧，本轮候选弃迁必须显式暴露，
            // 静默跳过会让存活记录在紧缩后凭空消失且无迹可查
            Err(e) => {
              log::warn!(
                "紧缩探针：候选 {main_addr:#x} 键解码失败（{e}），本轮弃迁，存活记录可能丢失可见性"
              );
            }
            Ok(_) => {}
          }
        }
        Err(e) => {
          // 读取间隙起始地址被并发紧缩推进，按陈旧槽位处理；其余错误快速上抛
          if main_addr < self.store.begin_address() {
            duplicates.push(addr);
            continue;
          }
          return Err(e.into());
        }
      }
    }

    for &stale_addr in duplicates.iter() {
      self.store.index().delete(key, stale_addr);
    }

    Ok(found)
  }
}
