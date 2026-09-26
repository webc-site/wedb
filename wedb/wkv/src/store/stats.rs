//! INFO 存储域快照与诊断转储（STORE / PERSISTENCE 段的存储侧聚合 +
//! STOREHASHTABLE / STOREREVIV 段的转储文本）
//!
//! 在 garnet 中的相对路径:
//! - libs/server/StoreWrapper.cs:GetDatabasesSnapshot（快照聚合入口的存储侧数据面）
//! - libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:DumpDistributionInternal
//! - libs/storage/Tsavorite/cs/src/core/ClientSession/ManageClientSessions.cs:DumpRevivificationStats
//!
//! 快照聚合只做纯原子读与静态直读（无 epoch、无扫描、无 await），任意线程
//! 可安全调用；哈希分布转储是 O(桶数) 的纯内存诊断扫描，仅供显式
//! `INFO STOREHASHTABLE` 的慢路径执行域触发，不进同步命令面。

use std::{mem::size_of, sync::atomic::Ordering};

use itoa::Buffer;
use wbase::{
  addr::{is_read_cache, to_absolute},
  map::{GxBuildHasher, HashMap},
};
use wdev::Device;
use windex::{HashBucket, HashBucketEntry};

use super::WedbStore;
use crate::read_cache::ReadCache;

/// 直方图容器（键 = 计数，值 = 桶数）
type Hist = HashMap<u32, u64>;

/// 读缓存运行时统计（常驻整环静态量 + 窗口地址水位）
#[derive(Debug, Default, Clone, Copy)]
pub struct ReadCacheStats {
  /// 单页容量（字节）。
  pub page_size_bytes: u64,
  /// 环形页槽位总数（常驻整环分配，已分配 == 上限）。
  pub num_pages: u64,
  /// 常驻内存字节数（num_pages × page_size）。
  pub memory_size_bytes: u64,
  /// 有效起始地址（滑窗下界）。
  pub head_address: u64,
  /// 活跃尾部分配地址。
  pub tail_address: u64,
}

/// 存储域统计快照（非泛型纯数据；STORE / PERSISTENCE / MEMORY store_* 段的
/// 存储侧数据源，由 [`WedbStore::store_snapshot`] 单点聚合）
#[derive(Debug, Default, Clone, Copy)]
pub struct StoreSnapshot {
  /// 当前存储版本（仅由 checkpoint 拍摄/恢复推进；0 = 无 checkpoint 历史）。
  pub current_version: i64,
  /// 上一次成功检查点的版本号（0 = 无成功检查点历史）。
  pub last_checkpointed_version: u64,
  /// 哈希索引主桶数（2 的幂）。
  pub index_bucket_count: u64,
  /// 单桶字节数（64B 缓存行对齐）。
  pub index_bucket_size_bytes: u64,
  /// 溢出桶累计分配数（含空闲栈残留，与内存占用同口径）。
  pub index_overflow_bucket_count: u64,
  /// 溢出桶空闲栈残留数（静止态守恒校验用：分配数 = 在用 + 本值）。
  pub index_overflow_free_bucket_count: u64,
  /// 升阶树页缓存已预留字节数（在线活跃树环 + 在途 scratch 环容量和）。
  pub tree_cache_reserved_bytes: u64,
  /// 升阶树页缓存总预算定额字节（0 = 不设限）。
  pub tree_cache_budget_bytes: u64,
  /// 混合日志单页容量（字节）。
  pub log_page_size_bytes: u64,
  /// 混合日志环形页槽位总数（常驻整环分配，已分配 == 上限）。
  pub log_num_pages: u64,
  /// 日志有效起始地址。
  pub log_begin_address: u64,
  /// 日志内存头地址。
  pub log_head_address: u64,
  /// 日志只读安全地址。
  pub log_safe_readonly_address: u64,
  /// 日志已刷盘地址。
  pub log_flushed_until_address: u64,
  /// 日志尾部分配地址。
  pub log_tail_address: u64,
  /// 读缓存统计（未启用为 None）。
  pub read_cache: Option<ReadCacheStats>,
}

/// 有效条目判定（主存地址 ≥ begin 或读缓存滑窗内 / 折算回主存 ≥ begin；
/// tentative 臂与 [`WedbStore::entry_count`] 不同、与 C# DumpDistributionInternal
/// 同向：不剔除 tentative 条目，由其地址判定归入 valid 或 below-begin）
#[inline]
fn entry_valid(rc: &ReadCache, raw: u64, begin_addr: u64) -> bool {
  let entry = HashBucketEntry::from_raw(raw);
  let addr = entry.address();
  if is_read_cache(addr) {
    let abs = to_absolute(addr);
    (abs >= rc.head_address() && abs < rc.tail_address())
      || rc.skip_read_cache(addr).unwrap_or(0) >= begin_addr
  } else {
    addr >= begin_addr
  }
}

/// 直方图按键升序追加为缩进行（对齐 C# OrderBy 输出）
fn hist_lines(m: &Hist, out: &mut String, buf: &mut Buffer) {
  let mut rows: Vec<_> = m.iter().collect();
  rows.sort_unstable_by_key(|(k, _)| **k);
  for (k, v) in rows {
    out.push_str("  ");
    out.push_str(buf.format(*k));
    out.push_str(" : ");
    out.push_str(buf.format(*v));
    out.push('\n');
  }
}

impl<D: Device> WedbStore<D> {
  /// 聚合存储域统计快照（单点；纯原子读与静态直读）
  ///
  /// whlog 为常驻整页分配模型（环形缓冲构造期一次性分配整环），页数与
  /// 内存为静态量直接换算，无运行时计数点；地址水位取 AddressManager
  /// 原子快照，读侧无锁。
  ///
  /// 在 garnet 中的相对路径:libs/server/StoreWrapper.cs:GetDatabasesSnapshot
  pub fn store_snapshot(&self) -> StoreSnapshot {
    let index = self.active_index();
    let rc = &self.read_cache;
    StoreSnapshot {
      current_version: self.current_version(),
      last_checkpointed_version: self.last_checkpointed_version(),
      index_bucket_count: index.size as u64,
      index_bucket_size_bytes: size_of::<HashBucket>() as u64,
      index_overflow_bucket_count: index.overflow_pool.allocated_count(),
      index_overflow_free_bucket_count: index.overflow_pool.free_count(),
      tree_cache_reserved_bytes: self.range_index.cache_reserved() as u64,
      tree_cache_budget_bytes: self.range_index.cache_budget() as u64,
      log_page_size_bytes: self.hlog.config.page_size as u64,
      log_num_pages: self.hlog.config.num_pages as u64,
      log_begin_address: self.hlog.begin_address(),
      log_head_address: self.hlog.head_address(),
      log_safe_readonly_address: self.hlog.safe_read_only_address(),
      log_flushed_until_address: self.hlog.flushed_until_address(),
      log_tail_address: self.hlog.tail_address(),
      read_cache: rc.is_enabled.then(|| ReadCacheStats {
        page_size_bytes: rc.page_size as u64,
        num_pages: rc.num_pages as u64,
        memory_size_bytes: (rc.num_pages * rc.page_size) as u64,
        head_address: rc.head_address(),
        tail_address: rc.tail_address(),
      }),
    }
  }

  /// 哈希索引分布转储（桶占用直方图 + 溢出链分布，纯内存诊断扫描）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:DumpDistributionInternal
  ///
  /// 输出骨架对齐 C#（桶数 / 溢出桶数 / 桶大小 / 表大小 / 总条目 / 均条 /
  /// 零槽汇总 / 四个直方图），计数口径按 windex 真实结构收敛：有效条目判定与
  /// C# DumpDistributionInternal 同向（tentative 计入 valid 或 below-begin），
  /// 非零且判无效计入 below-begin 计数，全零槽计入空闲槽。O(桶数) 诊断扫描，仅供显式
  /// INFO STOREHASHTABLE 的慢路径执行域触发。
  pub fn hash_distribution_dump(&self) -> String {
    let index = self.active_index();
    let table_size = index.size as u64;
    let begin_addr = self.hlog.begin_address();
    let rc = &self.read_cache;

    let mut total_records: u64 = 0;
    let mut total_below_begin: u64 = 0;
    let mut total_ofb_entries: u64 = 0;
    let mut total_ofb_below_begin: u64 = 0;
    let mut total_tentative: u64 = 0;
    let mut histogram = Hist::with_hasher(GxBuildHasher::default());
    let mut chain_histogram = Hist::with_hasher(GxBuildHasher::default());
    let mut unused_main = Hist::with_hasher(GxBuildHasher::default());
    let mut unused_ofb = Hist::with_hasher(GxBuildHasher::default());

    for bucket in index.buckets.iter() {
      let mut valid_in_bucket: u32 = 0;
      let mut ofb_entries: u32 = 0;
      let mut curr = bucket;
      let mut in_ofb = false;
      loop {
        let mut unused_slots: u32 = 0;
        for slot in curr.entries.iter().take(HashBucket::DATA_ENTRIES) {
          let raw = slot.load(Ordering::Acquire);
          if raw == 0 {
            unused_slots += 1;
            continue;
          }
          if HashBucketEntry::from_raw(raw).is_tentative() {
            total_tentative += 1;
          }
          if entry_valid(rc, raw, begin_addr) {
            valid_in_bucket += 1;
            if in_ofb {
              ofb_entries += 1;
            }
          } else {
            total_below_begin += 1;
            if in_ofb {
              total_ofb_below_begin += 1;
            }
          }
        }
        let unused_hist = if in_ofb {
          &mut unused_ofb
        } else {
          &mut unused_main
        };
        *unused_hist.entry(unused_slots).or_default() += 1;

        let next = curr.overflow_index();
        if next == 0 {
          break;
        }
        match index.overflow_pool.get(next) {
          Some(nb) => curr = nb,
          None => break,
        }
        in_ofb = true;
      }
      total_records += valid_in_bucket as u64;
      total_ofb_entries += ofb_entries as u64;
      *histogram.entry(valid_in_bucket).or_default() += 1;
      *chain_histogram.entry(ofb_entries).or_default() += 1;
    }

    let bucket_size = size_of::<HashBucket>() as u64;
    let total_zeroed: u64 = unused_main
      .iter()
      .chain(unused_ofb.iter())
      .map(|(&k, &v)| k as u64 * v)
      .sum();
    let mut buf = Buffer::new();
    let mut out = String::with_capacity(256);
    out.push_str("Number of hash buckets: ");
    out.push_str(buf.format(table_size));
    out.push_str("\nNumber of overflow buckets: ");
    out.push_str(buf.format(index.overflow_pool.allocated_count()));
    out.push_str("\nSize of each bucket: ");
    out.push_str(buf.format(bucket_size));
    out.push_str(" bytes\nHash-table size: ");
    out.push_str(buf.format(bucket_size * table_size));
    out.push_str(" bytes\nTotal distinct hash-table entry count: ");
    out.push_str(buf.format(total_records));
    out.push_str("\nAverage #entries per hash bucket: ");
    out.push_str(&fmt_entries_per_bucket(total_records, table_size));
    out.push_str("\nTotal zeroed out slots: ");
    out.push_str(buf.format(total_zeroed));
    out.push_str("\nTotal entries below begin addr: ");
    out.push_str(buf.format(total_below_begin));
    out.push_str("\nTotal entries in overflow buckets: ");
    out.push_str(buf.format(total_ofb_entries));
    out.push_str("\nTotal entries in overflow buckets below begin addr: ");
    out.push_str(buf.format(total_ofb_below_begin));
    out.push_str("\nTotal entries with tentative bit set: ");
    out.push_str(buf.format(total_tentative));
    out.push_str("\nHistogram of #entries per bucket:\n");
    hist_lines(&histogram, &mut out, &mut buf);
    out.push_str("Histogram of #buckets per OFB chain and their frequencies: \n");
    hist_lines(&chain_histogram, &mut out, &mut buf);
    out.push_str("Histogram of #unused slots per bucket in main hash index:\n");
    hist_lines(&unused_main, &mut out, &mut buf);
    out.push_str("Histogram of #unused slots per bucket in overflow buckets:\n");
    hist_lines(&unused_ofb, &mut out, &mut buf);
    out
  }

  /// 复活回收池统计转储（四计数直读，O(1)）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationStats.cs:Dump
  ///
  /// C# 九计数（adds/takes 成败分拆 + 空桶/地址/尺寸三类失败 + 链内复活
  /// 成败）在 wreviv 收敛为 put/take/hit/drop 四计数（wreviv/src/lib.rs
  /// 模块注释的配置面收缩决策），转储按真实计数面给等价可解读行式，
  /// 不虚报分拆。
  pub fn revivification_dump(&self) -> String {
    let pool = &self.reviv_pool;
    let (puts, takes) = (
      pool.put_count.load(Ordering::Relaxed),
      pool.take_count.load(Ordering::Relaxed),
    );
    let hits = pool.hit_count.load(Ordering::Relaxed);
    let drops = pool.drop_count.load(Ordering::Relaxed);

    let mut buf = Buffer::new();
    let mut out = String::with_capacity(128);
    out.push_str("Puts: ");
    out.push_str(buf.format(puts));
    out.push_str("\nTakes: ");
    out.push_str(buf.format(takes));
    out.push_str("\n\t Take hits: ");
    out.push_str(buf.format(hits));
    out.push_str("\n\t Take misses (bin empty / size / address): ");
    out.push_str(buf.format(takes.saturating_sub(hits)));
    out.push_str("\nDropped or invalidated: ");
    out.push_str(buf.format(drops));
    out.push('\n');
    out
  }

  /// 复位复活化统计账目（复活池四计数归零，可复活槽位与池启用态不动）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/ClientSession/ManageClientSessions.cs:ResetRevivificationStats
  ///
  /// C# 该口在清零全局 `revivificationStats` 前先合并全部活跃会话的复活统计；
  /// rust 复活统计唯一记账源即 [`wreviv::FreeRecordPool`] 的四个全局原子计数
  /// （会话侧无第二份账），故合并步骤无对应物，直下 `reset_stats` 即等价。
  pub fn reset_revivification_stats(&self) {
    self.reviv_pool.reset_stats();
  }
}

/// 均条目数两位小数（C# `{count / (double)table:0.00}`；表空为 0.00）
fn fmt_entries_per_bucket(total: u64, table_size: u64) -> String {
  let per = if table_size == 0 {
    0.0
  } else {
    total as f64 / table_size as f64
  };
  format!("{per:.2}")
}
