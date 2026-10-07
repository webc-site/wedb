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
#[doc(hidden)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ScanRegion {
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
#[doc(hidden)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ScanState {
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
#[derive(Default, Clone)]
pub(crate) struct MetricEntry {
  pub count: i64,
  pub size: i64,
}

/// 混合日志扫描的区域/状态分布统计
///（对标 libs/server/Metrics/HybridLogScanMetrics.cs:HybridLogScanMetrics）。
///
/// 相比 C# 动态嵌套 Dictionary 与动态查找，本结构采用固定 2×3 矩阵
/// （ScanRegion × ScanState），完全消除堆分配与哈希查表，统计累加为纯寄存器/栈上 O(1) 索引操作。
#[derive(Default, Clone)]
pub struct HybridLogScanMetrics {
  metrics: [[MetricEntry; 3]; 2],
}

impl HybridLogScanMetrics {
  /// libs/server/Metrics/HybridLogScanMetrics.cs:AddScanMetric
  ///
  /// 记录一次扫描命中：区域 `region` 下状态 `state` 的条数加一、字节数累加。
  /// 纯数组下标直接累加，零堆分配、零哈希查表。
  #[doc(hidden)]
  #[inline]
  pub fn add_scan_metric(&mut self, region: ScanRegion, state: ScanState, size: i64) {
    let entry = &mut self.metrics[region as usize][state as usize];
    entry.count += 1;
    entry.size += size;
  }

  /// 读取单个「区域 × 状态」桶的条目计数（[`Self::add_scan_metric`] 的数值读口）
  ///
  /// Rust 侧新增：C# 的 `HybridLogScanMetrics` 只有 `AddScanMetric` 与
  /// `DumpScanMetricsInfo`，任何数值消费方只能从转储文本回解；本仓直接把
  /// 矩阵单元以数值读出，杜绝「先 dump 再 parse」的文本往返与第二套计数口径。
  #[inline]
  pub fn scan_metric_count(&self, region: ScanRegion, state: ScanState) -> i64 {
    self.metrics[region as usize][state as usize].count
  }

  /// 读取单个「区域 × 状态」桶的物理字节数（与 [`Self::scan_metric_count`] 同点）
  #[inline]
  pub fn scan_metric_size(&self, region: ScanRegion, state: ScanState) -> i64 {
    self.metrics[region as usize][state as usize].size
  }

  /// 跨区域求和：某状态的条目计数总和（如全日志存活记录数 = `Live` 总和）
  ///
  /// 求和口径即 [`Self::scan_metric_count`] 沿 [`ScanRegion::ALL`] 逐项累加，
  /// 与 [`Self::dump_scan_metrics_info`] 输出的同状态各区域 `Count` 之和恒等。
  #[inline]
  pub fn state_count(&self, state: ScanState) -> i64 {
    ScanRegion::ALL
      .iter()
      .map(|region| self.scan_metric_count(*region, state))
      .sum()
  }

  /// libs/server/Metrics/HybridLogScanMetrics.cs:DumpScanMetricsInfo
  ///
  /// 转储为 INFO 多行文本；空统计返回空串（对齐 C# 仅输出起始换行前的空内容）。
  /// 纯人读诊断面（`INFO HLOGSCAN` 慢路径）；数值消费方走
  /// [`Self::scan_metric_count`] / [`Self::state_count`]，不得回解本输出。
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

/// 预取流水深度：桶 cacheline 预取与实际查桶 [`HashIndex::find_tag_by_hash`]
/// 之间隔开的记录数。深度须覆盖随机桶访问的访存停顿（大表 DRAM 往返 ~100ns）
/// 所需的顺序做功窗（每记录 ~45ns 走查做功 × 深度 ≈ 360ns ≥ 往返延迟）。
/// 深度扫描实测（R21，1.0 档 5.15M 键索引 128MiB 全脱 LLC）：4 的窗对 ~100ns
/// 停顿余量不足（len ~239ms），8 藏净（~233ms，同窗配对全正 +2.3%），12 无
/// 进一步收益（停顿已尽）；0.2 档索引 32MiB 贴 LLC（停顿 ~15ns）深度 4 早已
/// 藏净，8 深同窗三对全打平零回归——全局单值取 8。
const LEN_PF_DEPTH: usize = 8;

/// 环内键内联上限（绝大多数键 ≤ 32B 内联零分配；超长键转堆，罕见路径）
const LEN_KEY_INLINE: usize = 32;

/// 预取流水环槽：键哈希一次算定随槽贯穿（同 C# ContextReadWithPrefetch 的
/// hashes[] 形态，windex [`windex::PrefetchProbe`] 同源），分类所需键字节内联
/// 携带（链头不指向本记录时供锁外完整链回溯，替代逐条 `to_vec`）
struct LenPend {
  hash: u64,
  addr: u64,
  region: ScanRegion,
  size: i64,
  key_len: usize,
  key_inline: [u8; LEN_KEY_INLINE],
  key_heap: Option<Box<[u8]>>,
}

impl LenPend {
  /// 键 ≤ 内联上限零堆分配装箱；超长键一次性转堆
  fn new(hash: u64, addr: u64, region: ScanRegion, size: i64, key: &[u8]) -> Self {
    if key.len() <= LEN_KEY_INLINE {
      let mut key_inline = [0u8; LEN_KEY_INLINE];
      key_inline[..key.len()].copy_from_slice(key);
      Self {
        hash,
        addr,
        region,
        size,
        key_len: key.len(),
        key_inline,
        key_heap: None,
      }
    } else {
      Self {
        hash,
        addr,
        region,
        size,
        key_len: key.len(),
        key_inline: [0u8; LEN_KEY_INLINE],
        key_heap: Some(key.to_vec().into_boxed_slice()),
      }
    }
  }

  /// 分类用键切片（内联优先，堆侧兜底）
  fn key(&self) -> &[u8] {
    match &self.key_heap {
      Some(h) => h,
      None => &self.key_inline[..self.key_len],
    }
  }
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
  /// - 流式单遍、内存 O(1)：聚合仅落 [`HybridLogScanMetrics`] 桶。驱动用
  ///   [`whlog::ScanIterator::for_each_ref`] 单守卫整走查形态（逐记录的纪元
  ///   开关税与异步推进税随单趟守卫消除，对标 C# TryBulkConsumeNext 的整段
  ///   Resume/Suspend 块）；索引判定与走查解耦为两级流水：阶段一逐记录算定
  ///   键哈希并预取桶 cacheline 入 [`LenPend`] 环（哈希随槽贯穿，同 C#
  ///   ContextReadWithPrefetch 的 hashes[] 形态），阶段二在 [`LEN_PF_DEPTH`]
  ///   条记录之后查桶分类——随机桶访问（大索引表DRAM 往返）被顺序走查做功
  ///   藏匿，Live 直判与索引无槽位仍零分配；唯一堆分配是「链头不指向本记录」
  ///   且键超内联上限的记录（供锁外完整链回溯——回溯内嵌
  ///   [`whlog::HybridLog::with_memory_record`] 的二次页读锁，页读锁窗口内
  ///   不得嵌套，规避 parking_lot 写者优先下同页读锁重入的死锁窗口）；
  /// - 最终一致尽力语义：同 [`whlog::ScanIterator`] 在途零头自旋契约，
  ///   与热写并发的扫描轮次自愈（诊断面，非 checkpoint 级强一致快照）。
  ///   预取流水使链头读取时刻较记录交付后移约 [`LEN_PF_DEPTH`] 条记录的
  ///   走查窗，与既有「扫描期间只读边界推进不回溯重判」同属诊断面容差，
  ///   空闲场景（len 口径常态）零差。
  pub async fn hlog_scan_metrics(self: &Arc<Self>) -> Result<HybridLogScanMetrics> {
    use windex::{HashIndex, prefetch_read_l1};

    let head = self.hlog.head_address();
    let tail = self.hlog.tail_address();
    let read_only = self.hlog.read_only_address();
    let mut metrics = HybridLogScanMetrics::default();
    let mut it = self.hlog.scan_iter(head, tail);

    // 预取流水环（FIFO 排水保持日志序；深度即预取与查桶的流水距离）
    let mut pend_ring: [Option<LenPend>; LEN_PF_DEPTH] = Default::default();
    let mut cursor = 0usize;

    it.for_each_ref(|item| {
      // 字节口径 = 物理条宽（含对齐/松弛填充，对齐 C# NextAddress-CurrentAddress）
      let size = item.bytes.len() as i64;
      let region = if item.addr >= read_only {
        ScanRegion::Mutable
      } else {
        ScanRegion::Immutable
      };
      // 判定顺序对齐 C#：sealed → tombstone → 索引回溯（前两态不触索引，即时入桶）
      let rec = item.rec;
      if rec.is_tombstone() {
        metrics.add_scan_metric(region, ScanState::Tombstoned, size);
      } else if rec.is_sealed() {
        metrics.add_scan_metric(region, ScanState::RCUdUnsealed, size);
      } else {
        // 阶段一：键哈希一次算定 + 桶 cacheline 预取入环（哈希随槽贯穿零重算）
        let idx = self.index.load();
        let hash = HashIndex::hash_key(rec.key);
        prefetch_read_l1(idx.get_bucket((hash as usize) & idx.mask));
        let entry = LenPend::new(hash, item.addr, region, size, rec.key);
        // 阶段二：环满槽位排水查桶——桶线已经过 [`LEN_PF_DEPTH`] 条记录的
        // 顺序走查做功窗，随机访存停顿被藏匿
        if let Some(old) = pend_ring[cursor].replace(entry) {
          self.classify_len_pending(old, &mut metrics, head);
        }
        cursor = (cursor + 1) % LEN_PF_DEPTH;
      }
      Ok(true)
    })
    .await?;
    // 排水残余（保持日志序：自游标起按槽位序即入环序）
    for i in 0..LEN_PF_DEPTH {
      let slot = (cursor + i) % LEN_PF_DEPTH;
      if let Some(old) = pend_ring[slot].take() {
        self.classify_len_pending(old, &mut metrics, head);
      }
    }

    Ok(metrics)
  }

  /// 预取流水环槽的分类尾：查桶（剥 ReadCache 前缀）三态直判，链头另有其人
  /// 时以内联键走锁外完整链回溯二分（原 Pending 形态的环内承接，键零重复拷贝）
  fn classify_len_pending(&self, e: LenPend, metrics: &mut HybridLogScanMetrics, head: u64) {
    // None（滑窗/不可判读）折 0：按非链头处理，转入锁外回溯复核
    let probed = self
      .index
      .load()
      .find_tag_by_hash(e.hash)
      .map(|first| self.read_cache.skip_read_cache(first).unwrap_or(0));
    let state = match probed {
      // 链头即本记录 → Live 热路径零分配直判
      Some(curr) if curr == e.addr => ScanState::Live,
      // 索引无此键槽位 → elide 脱钩/清退孤儿 → 非活动零分配
      None => ScanState::RCUdUnsealed,
      // 链头另有其人（tag 碰撞/被取代）→ 锁外完整链回溯二分
      Some(_) => {
        if self.is_latest_hlog_version(e.key(), e.addr, head) {
          ScanState::Live
        } else {
          ScanState::RCUdUnsealed
        }
      }
    };
    metrics.add_scan_metric(e.region, state, e.size);
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
