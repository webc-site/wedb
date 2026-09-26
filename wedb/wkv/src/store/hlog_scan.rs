//! 混合日志内存分布扫描（INFO HLOGSCAN 段存储域数据源）
//!
//! 在 garnet 中的相对路径: libs/server/Databases/DatabaseManagerBase.cs:CollectHybridLogStats
//!
//! C# 对主存储与对象存储各扫一遍（`[HeadAddress, TailAddress]` 逐记录按
//! 区域 × 状态 × 字节数聚合）；wedb 单物理日志 + wcol 信封统一值域，
//! 只保留 main store 形态，扫描产出 [`HybridLogScanMetrics`]。
//!
//! 状态枚举收敛（C# 五桶 → rust 三桶）：判定顺序对齐 C#
//! （`IsSealed` → `Invalid` → `Tombstone` → 索引回溯 → `Live`），
//! 以 wrecord/wkv 实际状态机为准——
//! - rust 无持久 `RCUdSealed` 形态：SEALED 位是纯易失标记（复活池槽位锁定，
//!   任何持久化路径都不置位，见 `whlog` SEALED 位约定），正常 RCU 更新不
//!   seal 旧记录，扫描撞上的 sealed 仅复活改写瞬态，死槽位语义与被取代
//!   旧版同质，并入 `RCUdUnsealed`；
//! - C# `ElidedFromHashIndex` 的判据是记录头 Invalid 位（elide 物理标记）；
//!   rust 的记录脱钩只清索引槽位不改记录头，
//!   脱钩记录与被取代旧版在记录头维度不可分，按「索引不可达」归并进
//!   `RCUdUnsealed`（绝不保留恒 0 假桶）；
//! - `Live`：该键哈希索引链回溯命中且最新版即本记录（对齐 C#
//!   `ContainsKeyInMemory(...).Found && tempKeyAddress == CurrentAddress`）；
//! - `Tombstoned`：wrecord 墓碑位（DEL 盲追加的 0 字节墓碑同此形态）。
//!
//! 在 garnet 中的相对路径: libs/server Garnet CollectHybridLogStats（INFO HLOGSCAN 段语义）

use std::sync::Arc;

use itoa::Buffer;
use wdev::Device;

use super::WedbStore;
use crate::error::Result;

/// 扫描统计区域枚举（对标 Garnet Log.ReadOnlyAddress 二分边界）
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub(crate) enum ScanRegion {
  /// 不可变只读冷区（C# `"Immutable"`：地址 < ReadOnlyAddress）
  Immutable = 0,
  /// 可变热区（C# `"Mutable"`：地址 ≥ ReadOnlyAddress）
  Mutable = 1,
}

impl ScanRegion {
  /// 所有区域常量迭代列表，保持地址序（Immutable < Mutable）
  pub const ALL: [Self; 2] = [Self::Immutable, Self::Mutable];

  /// 转为 C# 对齐的区域名称字符串
  #[inline]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Immutable => "Immutable",
      Self::Mutable => "Mutable",
    }
  }
}

/// 扫描统计状态枚举（按 wedb 状态机收敛为 3 态）
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub(crate) enum ScanState {
  /// 活动版本（C# `"Live"`）
  Live = 0,
  /// 被取代版本（C# `"RCUdUnsealed"`）
  RCUdUnsealed = 1,
  /// 墓碑（C# `"Tombstoned"`）
  Tombstoned = 2,
}

impl ScanState {
  /// 所有状态常量迭代列表
  pub const ALL: [Self; 3] = [Self::Live, Self::RCUdUnsealed, Self::Tombstoned];

  /// 转为 C# 对齐的状态名称字符串
  #[inline]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Live => "Live",
      Self::RCUdUnsealed => "RCUdUnsealed",
      Self::Tombstoned => "Tombstoned",
    }
  }
}

/// 单项条目聚合（记录条数与物理字节数）
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetricEntry {
  pub count: i64,
  pub size: i64,
}

/// 混合日志扫描的区域/状态分布统计
///（对标 libs/server/Metrics/HybridLogScanMetrics.cs:HybridLogScanMetrics）。
///
/// 相比 C# 动态嵌套 Dictionary 与动态查找，本结构采用固定 2×3 矩阵
/// （ScanRegion × ScanState），完全消除堆分配与哈希查表，统计累加为纯寄存器/栈上 O(1) 索引操作。
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct HybridLogScanMetrics {
  metrics: [[MetricEntry; 3]; 2],
}

impl HybridLogScanMetrics {
  /// libs/server/Metrics/HybridLogScanMetrics.cs:AddScanMetric
  ///
  /// 记录一次扫描命中：区域 `region` 下状态 `state` 的条数加一、字节数累加。
  /// 纯数组下标直接累加，零堆分配、零哈希查表。
  #[inline]
  pub(crate) fn add_scan_metric(&mut self, region: ScanRegion, state: ScanState, size: i64) {
    let entry = &mut self.metrics[region as usize][state as usize];
    entry.count += 1;
    entry.size += size;
  }

  /// libs/server/Metrics/HybridLogScanMetrics.cs:DumpScanMetricsInfo
  ///
  /// 转储为 INFO 多行文本；空统计返回空串（对齐 C# 仅输出起始换行前的空内容）。
  pub fn dump_scan_metrics_info(&self) -> String {
    let has_any = self
      .metrics
      .iter()
      .any(|region| region.iter().any(|e| e.count > 0));
    if !has_any {
      return String::new();
    }
    let mut out = String::with_capacity(128);
    out.push('\n');
    let mut num_buf = Buffer::new();
    for (r_idx, region) in ScanRegion::ALL.iter().enumerate() {
      let region_has_records = self.metrics[r_idx].iter().any(|e| e.count > 0);
      if !region_has_records {
        continue;
      }
      out.push_str("# Region: ");
      out.push_str(region.as_str());
      out.push('\n');
      for (s_idx, state) in ScanState::ALL.iter().enumerate() {
        let entry = &self.metrics[r_idx][s_idx];
        if entry.count > 0 {
          out.push_str("  State: ");
          out.push_str(state.as_str());
          out.push_str(", Count: ");
          out.push_str(num_buf.format(entry.count));
          out.push_str(", Size: ");
          out.push_str(num_buf.format(entry.size));
          out.push('\n');
        }
      }
    }
    out
  }
}

/// 单条记录的扫描裁决：已入桶或待锁外回溯二分
enum ScanVerdict {
  /// 已按 sealed/tombstone/窗口内直判（索引命中本记录 / 索引无槽位）入桶
  Classified,
  /// Live 候选：索引链头不指向本记录（tag 碰撞/被取代），键拷贝出页锁
  /// 窗口，待锁外完整链回溯二分 Live/被取代
  Pending {
    key: Vec<u8>,
    addr: u64,
    region: ScanRegion,
    size: i64,
  },
}

impl<D: Device> WedbStore<D> {
  /// 全日志内存分布扫描：遍历已提交区间 `[head, tail)`，按区域（可写热区/
  /// 只读冷区）× 状态（活动/被取代/墓碑）聚合记录数与物理字节数
  ///
  /// 在 garnet 中的相对路径: libs/server/Databases/DatabaseManagerBase.cs:CollectHybridLogStats
  ///
  /// - 扫描区间对齐 C# `[HeadAddress, TailAddress]`（内存驻留 + 磁盘冷区
  ///   混合连续扫描，[`whlog::HybridLog::scan_iter`] 自动读盘）；
  /// - 区域判定对齐 C# `CurrentAddress >= ReadOnlyAddress`，边界取扫描起点
  ///   快照（诊断面容差：扫描期间只读边界推进不回溯重判）；
  /// - 字节口径对齐 C# `NextAddress - CurrentAddress` = 记录物理条宽
  ///   （含隐式对齐填充与显式松弛填充）；
  /// - Live/被取代二分对齐 C# `ContainsKeyInMemory` + `TraceBackForKeyMatch`：
  ///   哈希索引取链头，跳过 ReadCache 前缀后沿 `prev_address` 链在内存区内
  ///   回溯键匹配，命中且非墓碑的最新版地址即本记录 → Live；
  /// - 流式单遍、内存 O(1)：聚合仅落 [`HybridLogScanMetrics`] 桶。索引判定
  ///   前移进扫描页锁窗口：`find_tag` 只读索引表原子（不触 hlog 页锁），
  ///   链头即本记录（Live 常态热路径）与索引无槽位（elide 脱钩孤儿）均
  ///   零分配直判；唯一越界分配是「链头不指向本记录」记录的键拷贝（tag
  ///   碰撞/被取代场景才产生，供锁外完整链回溯——回溯内嵌
  ///   [`whlog::HybridLog::with_memory_record`] 的二次页读锁，页读锁窗口内
  ///   不得嵌套，规避 parking_lot 写者优先下同页读锁重入的死锁窗口）；
  /// - 最终一致尽力语义：同 [`whlog::ScanIterator`] 在途零头自旋契约，
  ///   与热写并发的扫描轮次自愈（诊断面，非 checkpoint 级强一致快照）。
  pub async fn hlog_scan_metrics(self: &Arc<Self>) -> Result<HybridLogScanMetrics> {
    let head = self.hlog.head_address();
    let tail = self.hlog.tail_address();
    let read_only = self.hlog.read_only_address();
    let mut metrics = HybridLogScanMetrics::default();
    let mut it = self.hlog.scan_iter(head, tail);

    while let Some(pending) = it
      .next_ref(|item| {
        // 字节口径 = 物理条宽（含对齐/松弛填充，对齐 C# NextAddress-CurrentAddress）
        let size = item.bytes.len() as i64;
        let region = if item.addr >= read_only {
          ScanRegion::Mutable
        } else {
          ScanRegion::Immutable
        };
        // 判定顺序对齐 C#：sealed → tombstone → 索引回溯
        let rec = item.rec;
        if rec.is_tombstone() {
          metrics.add_scan_metric(region, ScanState::Tombstoned, size);
          Ok(ScanVerdict::Classified)
        } else if rec.is_sealed() {
          metrics.add_scan_metric(region, ScanState::RCUdUnsealed, size);
          Ok(ScanVerdict::Classified)
        } else {
          // 键切片借用自页缓冲，仅窗口内可用：只读索引表原子判定（find_tag
          // 不触 hlog 页锁，页读锁窗口内安全），三态零拷贝直判——
          // 链头（剥 ReadCache 前缀后）即本记录 → Live 热路径零分配；
          // 索引无此键槽位 → elide 脱钩/清退孤儿 → 非活动零分配；
          // 链头另有其人 → 拷贝键出窗口，锁外完整链回溯二分（见循环体）
          match self
            .index
            .load()
            .find_tag(rec.key)
            // None（滑窗/不可判读）折 0：按非链头处理，转入锁外回溯复核
            .map(|first| self.read_cache.skip_read_cache(first).unwrap_or(0))
          {
            Some(curr) if curr == item.addr => {
              metrics.add_scan_metric(region, ScanState::Live, size);
              Ok(ScanVerdict::Classified)
            }
            None => {
              metrics.add_scan_metric(region, ScanState::RCUdUnsealed, size);
              Ok(ScanVerdict::Classified)
            }
            Some(_) => Ok(ScanVerdict::Pending {
              key: rec.key.to_vec(),
              addr: item.addr,
              region,
              size,
            }),
          }
        }
      })
      .await?
    {
      let ScanVerdict::Pending {
        key,
        addr,
        region,
        size,
      } = pending
      else {
        continue;
      };
      let state = if self.is_latest_hlog_version(&key, addr, head) {
        ScanState::Live
      } else {
        ScanState::RCUdUnsealed
      };
      metrics.add_scan_metric(region, state, size);
    }

    Ok(metrics)
  }

  /// 索引链回溯判定记录是否为其键的当前活动版本
  ///
  /// 判定内核与 C# `ContainsKeyInMemory(key, out addr, fromAddress)` 同构
  /// （严格映射单点在 [`crate::session::StoreSession::contains_key_raw`]，
  /// 本方法为其扫描面复用形态）：
  /// `FindTag` 取链头（键哈希 tag 槽位）→ `SkipReadCache` 剥离 ReadCache
  /// 前缀 → `TraceBackForKeyMatch` 在内存区 `[head, tail)` 内沿
  /// `prev_address` 链回溯键匹配：命中墓碑 → 非活动（C# NotFound 分支）；
  /// 命中键匹配的非墓碑记录，其地址即 C# `tempKeyAddress`，与本记录地址
  /// 相等方为 Live。链尽（prev = 0）、滑出内存区（< head）或链头记录不
  /// 再驻留均按非活动兜底（诊断面保守方向：宁可计入被取代桶，不计虚活）。
  ///
  /// 调用契约：仅「窗口内预判链头不指向本记录」的候选进入本方法（Live
  /// 直判与索引无槽位已在扫描页读锁窗口内零分配直判，见
  /// [`Self::hlog_scan_metrics`]）；且必须在页读锁窗口之外调用（本方法
  /// 内嵌 [`whlog::HybridLog::with_memory_record`] 的可变区页读锁）。
  ///
  /// 非重复说明：本方法专用于诊断扫描面的 [head, tail) 全域最新版本判活；
  /// 与 StoreSession::trace_live_mutable_addr 专精于可变区 [read_only, tail) 的热路径原位读改写判据各司其职。
  fn is_latest_hlog_version(&self, key: &[u8], addr: u64, head: u64) -> bool {
    let Some(first) = self.index.load().find_tag(key) else {
      // 索引无此键槽位：elide 脱钩/索引清退后的孤儿记录 → 非活动
      return false;
    };
    // None（滑窗/不可判读）折 0：按非最新处理，降级回溯复核
    let mut curr = self.read_cache.skip_read_cache(first).unwrap_or(0);
    // 热路径短路：链头即本记录（绝大多数活动版本直命中，免回溯）
    if curr == addr {
      return true;
    }
    while curr >= head {
      // 三元组平铺传出（RecordRef 借用不可跨闭包逃逸）
      let (matched, tombstone, prev) = match self.hlog.with_memory_record(curr, |rec| {
        Ok((rec.matches_key(key), rec.is_tombstone(), rec.prev_address()))
      }) {
        Ok(Some(v)) => v,
        _ => return false,
      };
      if matched {
        return !tombstone && curr == addr;
      }
      curr = prev;
    }
    false
  }
}

#[cfg(test)]
mod tests {
  use super::{HybridLogScanMetrics, ScanRegion, ScanState};

  #[test]
  fn aggregate_and_dump() {
    let mut m = HybridLogScanMetrics::default();
    m.add_scan_metric(ScanRegion::Mutable, ScanState::Live, 64);
    m.add_scan_metric(ScanRegion::Mutable, ScanState::Live, 32);
    m.add_scan_metric(ScanRegion::Mutable, ScanState::RCUdUnsealed, 128);
    m.add_scan_metric(ScanRegion::Immutable, ScanState::Live, 16);

    let dump = m.dump_scan_metrics_info();
    assert!(dump.contains("# Region: Immutable\n"));
    assert!(dump.contains("  State: Live, Count: 1, Size: 16\n"));
    assert!(dump.contains("# Region: Mutable\n"));
    assert!(dump.contains("  State: Live, Count: 2, Size: 96\n"));
    assert!(dump.contains("  State: RCUdUnsealed, Count: 1, Size: 128\n"));
    assert!(dump.find("Immutable").unwrap() < dump.find("Mutable").unwrap());

    // 空状态转储为空串（dump_scan_metrics_info 的提前返回分支）。
    assert_eq!(HybridLogScanMetrics::default().dump_scan_metrics_info(), "");
  }
}
