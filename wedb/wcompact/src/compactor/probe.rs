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
  /// 0. 探针前先过会话侧分裂协同门 [`CompactSession::ensure_split`]（与读面/扫描面
  ///    同构单机制），杜绝 grow 迁移窗内对未迁分块采得陈旧空候选被上判 superseded
  ///    弃迁活键（C# 紧缩面同等保障为 NOTFOUND 保守补拷臂，本仓收口于协同门）；
  /// 1. 通过无堆分配栈列表 `lookup_candidates` 获取 Tag 匹配的所有候选槽位并降序排列；
  /// 2. 剥离 ReadCache 虚拟标记位（若已落入 ReadCache，顺链跳跃获取底层主日志真实逻辑
  ///    地址）：滑窗换页驱逐过渡态的等待与回链头重探全部收口于宿主走查单口
  ///    [`CompactStore::skip_read_cache_with_wait`]（对标 C# SkipReadCache 的 RestartChain
  ///    环，
  ///    libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:SkipReadCache），
  ///    本探针不再有第二套重启环，与 wkv `raw/write/copy_to_tail.rs` 同一端口；
  /// 3. 若候选地址匹配调用方已知记录（`known_curr`），直接复用已校验结果，零冗余 I/O；
  /// 4. 清理已截断或失效的陈旧索引槽位引用——仅限主日志物理槽位；
  ///    携带 ReadCache 虚拟标记位的槽位一律不得摘除，其脱钩与回收生命周期
  ///    收口于宿主 cleanse_page 与写侧 CAS 替换；
  /// 5. 返回首个匹配指定 Key 的最新记录信息（包含主日志真实地址与索引原槽位地址）。
  pub(super) async fn find_latest_address(
    &self,
    session: &S::Session,
    key: &[u8],
    known_curr: Option<(u64, bool)>,
  ) -> Result<Option<LatestRecord>> {
    // 索引探测持短守卫分段执行（共享内存结构访问口径对齐 wkv delete_raw_disk_slow）
    let mut addrs = {
      let _guard = session.enter_epoch();
      // 分裂协同门（探针前必过，与读面 read_probe / 扫描面 find_tag_cooperative 同
      // 机制同构，对标 C# 会话面入口铁律与紧缩面 NOTFOUND 保守补拷臂的等价承接）：
      // grow 迁移窗内未迁分块键在新表探得空候选，直判 superseded 随 begin 截断即
      // 成批静默丢键；先行迁移本键分块后采样，写侧 CAS 的期望槽位恒来自协同后
      // 探查，天然收口。迁移内核错误显式上抛，严禁折成空候选假弃迁
      session.ensure_split(key)?;
      self.store.index().lookup_candidates(key)
    };
    addrs.sort_descending();
    // 确定性扩容窗注入钩（wkv 读面 test_read_gap_hook 同族形态，生产恒不触发）：
    // growing 期协同门后仍采得空候选即门被破坏的注入点，钩内确定性完成全量迁移
    // 并翻回 Rest，复现「陈旧空候选直判 superseded」的跨阶段 TOCTOU；修复在位时
    // 钩永不消费，回归以「钩仍挂载 + 活键零弃迁」双断言锁定
    if addrs.is_empty()
      && self.store.is_growing()
      && let Some(hook) = self.probe_gap_hook.lock().take()
    {
      hook();
    }
    let begin_addr = self.store.begin_address();

    let mut found = None;
    let mut duplicates = CandidateAddresses::new();

    for &addr in addrs.iter() {
      // 剥离 ReadCache 虚拟标记位；主日志物理槽位原样即用。RC 槽位绝不按断链降级：
      // 解析端口内部等待清洗落定后重读本键哈希项，恒交付确定地址（0 = 链尽/槽位已摘除），
      // 两种形态均严禁入 duplicates
      let is_rc = self.store.is_read_cache_addr(addr);
      let main_addr = if is_rc {
        self.store.skip_read_cache_with_wait(key, addr, session)
      } else {
        addr
      };

      // 地址为 0（ReadCache 专属记录链尽，无主日志对应）或已落入截断区：
      // 仅主日志物理槽位标为失效陈旧摘除，ReadCache 虚拟槽位不触索引
      if main_addr == 0 || main_addr < begin_addr {
        if !is_rc {
          duplicates.push(addr);
        }
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
        } else if !is_rc {
          // found 已确立后同键更旧主日志槽位方可摘除
          duplicates.push(addr);
        }
        continue;
      }

      // 冷读分派收口内核单点（契约见内核 read_record），此处按结果判活
      let record = session.read_record_at(main_addr).await;
      match record {
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
              } else if !is_rc {
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
          // 读取间隙起始地址被并发紧缩推进，按陈旧槽位处理（ReadCache 虚拟
          // 槽位除外，绝不触索引）；其余错误快速上抛
          if main_addr < self.store.begin_address() {
            if !is_rc {
              duplicates.push(addr);
            }
            continue;
          }
          return Err(e);
        }
      }
    }

    for &stale_addr in duplicates.iter() {
      self.store.index().delete(key, stale_addr);
    }

    Ok(found)
  }
}
