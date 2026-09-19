//! Microsoft Garnet 官方架构对标的独立只读非脏页内存日志（ReadCache）
//!
//! 生产开关：`StoreConfig::enable_read_cache`（默认关闭，对标 C# GarnetServerOptions
//! EnableReadCache 默认 false；生产命令行经 wconf `hlog.read_cache` 暴露）。
//! 地址位原语（READ_CACHE_BIT 判定/清除/打标）单点在 [`wbase::addr`]，
//! 本模块直连消费，不再设包装层。
//!
//! 模块拓扑严格对照 C# TsavoriteKV 的 partial 分文件形态：
//! - [`append`]：CAS 预留 + 编码 + 哈希索引挂载（对标
//!   libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToReadCache.cs:TryCopyToReadCache）
//! - [`cleanse`]：页关闭清洗与脱钩作废（对标 ReadCache.cs:ReadCacheEvict /
//!   ReadCacheEvictChain / ReadCacheAbandonRecord）
//! - [`window`]：地址窗口水位与读侧链路（对标 ReadCache.cs:FindInReadCache /
//!   SkipReadCache / ReadCacheNeedToWaitForEviction 与 AllocatorBase.cs
//!   地址窗口水位域 ClosedUntilAddress）

mod append;
mod cleanse;
mod window;

use std::sync::atomic::AtomicU64;

use itoa::Buffer;
use parking_lot::Mutex;
use wbase::align::{CachePadded, DEFAULT_SECTOR_SIZE};
use whlog::{CircularPageBuffer, HybridLogConfig};

use crate::error::{Error, Result};

/// 页槽位关闭位（两阶段关闭协议：置位后新编码注册必被拒绝，直至换页完成重置）
const INFLIGHT_CLOSED: u64 = 1 << 63;

/// 读缓存链走查三态结果（严格对标
/// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:FindInReadCache
/// 的「非 Invalid 才比对键、无条件沿 PreviousAddress 续链」与
/// ReadCacheNeedToWaitForEviction → RestartChain 的重启协议）
///
/// 三态的核心价值：把「记录不可判读」与「记录可读但未命中」严格分离——
/// 前者绝不可折叠成链终止产出确定性 NOTFOUND（活键误报不存在的根因），
/// 必须回链头重探
#[derive(Debug)]
pub enum RcVisit<R> {
  /// 记录可读且闭包命中（键比对成功）
  Found(R),
  /// 记录可读但未命中：携带前驱地址续链。closed 作废记录对标 C# 跳过 Invalid
  /// 沿 prev 继续；0 表示链尾（pad/零头/链尽）
  Next(u64),
  /// 记录滑出环形窗口或在途换装竞态下不可判读：调用方须按
  /// UpdateRecordSourceToCurrentHashEntry 语义回链头重探（RestartChain），
  /// 绝不可按不存在降级
  Gone,
}

/// 页槽位在途编码计数掩码
const INFLIGHT_COUNT_MASK: u64 = !INFLIGHT_CLOSED;

/// Microsoft Garnet 官方架构对标的独立只读非脏页内存日志（ReadCache）
///
/// 严格对齐 Garnet `ReadCache.cs` + `TryCopyToReadCache.cs`：
/// 1. 纯 DRAM 环形日志分配器，无任何物理磁盘持久化开销，零写放大；
/// 2. 磁盘冷数据命中回填后，挂载为哈希链首部前缀，加速后续高频读请求纳秒级命中；
/// 3. 主日志执行写操作（Upsert / RMW / Delete）时通过单次 CAS 原子脱钩整条 ReadCache 链；
/// 4. 环形覆盖自然淘汰旧页，在复用前执行 CleanseHashChain 原子解构恢复主日志链接，杜绝悬垂指针与数据丢失。
pub struct ReadCache {
  /// 环形页内存池
  buffer: CircularPageBuffer,
  /// 页面大小（字节，2 的幂）
  pub page_size: usize,
  /// 缓冲页总数（2 的幂）
  pub num_pages: usize,
  /// 页面位移量（用于取代 64 位整数除法）
  page_shift: u32,
  /// 页面偏移掩码（用于取代 64 位整数取模）
  page_mask: u64,
  /// 环形缓冲区总容量字节数
  capacity: u64,
  /// 活跃尾部分配地址（写热点，独占 64 字节缓存行）
  tail_address: CachePadded<AtomicU64>,
  /// 有效起始地址（滑动窗口下界，读热点，独占 64 字节缓存行）
  head_address: CachePadded<AtomicU64>,
  /// 驱逐清洗完成高水位地址（严格对标 AllocatorBase.cs:ClosedUntilAddress）
  ///
  /// 换页路径在 cleanse_page 恢复哈希链后按 MonotonicUpdate 口径单调推进；
  /// 读侧 SpinWaitUntilRecordIsClosed 以 `abs < closed_until_address` 为关闭判据
  /// （等于该值表示记录尚未关闭）。低频标记位，不加缓存行填充
  closed_until_address: AtomicU64,
  /// 各页槽位在途编码状态字（两阶段关闭协议：位63 [`INFLIGHT_CLOSED`] + 低 63 位在途编码计数）
  ///
  /// 闭环回写窗撕裂：CAS 预留切片与编码之间存在无锁窗口，滞留编码者（单线程
  /// 停滞横跨整环回绕）唤醒后会落入已被换装复用的页槽。本状态字令换页侧能够
  /// 等待被复用槽上全部在途编码完成（计数清零）并拒绝新注册（CLOSED 位），
  /// 对标 C# Tsavorite 页清除/驱逐的 epoch-gated 排空语义
  /// （AllocatorBase.cs:OnPagesClosed "wait for reader drain"）：C# 以全局纪元
  /// 排空实现同一不变式，此处粒度收敛到单槽，规避 turn_lock 等待者持旧纪元
  /// 与锁内全局排空互待的死锁
  page_inflight: Box<[AtomicU64]>,
  /// 换页保护互斥锁
  turn_lock: Mutex<()>,
  /// 是否启用 ReadCache
  pub is_enabled: bool,
}

impl ReadCache {
  /// 创建新的 ReadCache 实例
  pub fn new(page_size: usize, num_pages: usize, is_enabled: bool) -> Result<Self> {
    if !page_size.is_power_of_two() || page_size == 0 {
      let mut msg = String::from("ReadCache page_size 必须为非零且为 2 的幂，当前为 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(page_size));
      return Err(Error::InvalidConfig(msg));
    }
    if !num_pages.is_power_of_two() || num_pages == 0 {
      let mut msg = String::from("ReadCache num_pages 必须为非零且为 2 的幂，当前为 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(num_pages));
      return Err(Error::InvalidConfig(msg));
    }

    let dummy_config = HybridLogConfig {
      page_size,
      num_pages,
      mutable_fraction: 1.0,
      ro_lag_num: whlog::ro_lag_num_from_fraction(1.0),
      initial_address: 0,
    };
    // 禁用态零预算占位（对标 C# 仅 EnableReadCache 时才构造读缓存分配器）：
    // 全部读 写路径首行短路 `is_enabled`，占位环（单扇区页）永不被寻址；
    // 否则大页配置（16MB）下禁用态也将常驻整环内存
    let buffer = if is_enabled {
      CircularPageBuffer::new(&dummy_config)?
    } else {
      CircularPageBuffer::new(&HybridLogConfig {
        page_size: DEFAULT_SECTOR_SIZE,
        num_pages: 1,
        mutable_fraction: 1.0,
        ro_lag_num: whlog::ro_lag_num_from_fraction(1.0),
        initial_address: 0,
      })?
    };
    buffer.clear_page(0);

    let page_shift = page_size.trailing_zeros();
    let page_mask = (page_size - 1) as u64;
    let capacity = (num_pages * page_size) as u64;

    Ok(Self {
      buffer,
      page_size,
      num_pages,
      page_shift,
      page_mask,
      capacity,
      tail_address: CachePadded(AtomicU64::new(0)),
      head_address: CachePadded(AtomicU64::new(0)),
      closed_until_address: AtomicU64::new(0),
      page_inflight: (0..num_pages).map(|_| AtomicU64::new(0)).collect(),
      turn_lock: Mutex::new(()),
      is_enabled,
    })
  }
}

#[cfg(test)]
mod tests {
  use std::{
    sync::atomic::{
      AtomicU32,
      Ordering::{Relaxed, Release},
    },
    thread::{scope, sleep},
    time::{Duration, Instant},
  };

  use wbase::addr::{is_read_cache, to_absolute, with_read_cache};
  use windex::{HashBucketEntry, HashIndex};
  use wrecord::{HEADER_SIZE, RecordHeader};

  use super::*;

  /// 已从 windex 生产导出面收敛掉的 key 版 `insert`：测试灌数按唯一免查重追加
  /// 写入口 [`HashIndex::insert_to_bucket`] 等价复现（桶下标与 Tag 在此显式换算）
  trait HashIndexTestOps {
    fn insert(&self, key: &[u8], address: u64) -> windex::Result<()>;
  }

  impl HashIndexTestOps for HashIndex {
    #[inline]
    fn insert(&self, key: &[u8], address: u64) -> windex::Result<()> {
      let hash = HashIndex::hash_key(key);
      let tag = HashBucketEntry::tag_from_hash(hash);
      self.insert_to_bucket(self.bucket_index_for_hash(hash), tag, address)
    }
  }

  /// 门控：非 RC 地址 / 窗口内 / 未启用一律不等待（对标 ReadCacheNeedToWaitForEviction 快路径）
  #[test]
  fn gate_is_noop_for_non_rc_in_window_and_disabled() -> Result<()> {
    let rc = ReadCache::new(4096, 4, true)?;
    let index = HashIndex::new(16)?;
    // append 内部 CAS 挂载（对标 hei.TryCAS）：先挂主日志地址条目再追加
    index.insert(b"k", 12345)?;
    let rc_addr = rc.append(b"k", b"v", 12345, &index).expect("append 应成功");

    assert!(!rc.need_to_wait_for_eviction(123, || ()));
    assert!(!rc.need_to_wait_for_eviction(rc_addr, || ()));

    let off = ReadCache::new(4096, 4, false)?;
    assert!(!off.need_to_wait_for_eviction(with_read_cache(0), || ()));
    Ok(())
  }

  /// 等待协议：ClosedUntilAddress 未发布时至少续转一轮（C# 首轮 ProtectAndDrain
  /// 强制语义），驱逐方发布高水位后立即退出
  #[test]
  fn wait_blocks_until_closed_until_published() -> Result<()> {
    let rc = ReadCache::new(4096, 4, true)?;
    let index = HashIndex::new(16)?;
    // append 内部 CAS 挂载（对标 hei.TryCAS）：先挂主日志地址条目再追加
    index.insert(b"k", 12345)?;
    let rc_addr = rc.append(b"k", b"v", 12345, &index).expect("append 应成功");
    let abs_addr = to_absolute(rc_addr);

    // 模拟驱逐方完成 head 推进、cleanse 尚未发布 ClosedUntilAddress 的窗口
    rc.head_address.fetch_max(abs_addr + 1, Release);

    let rounds = AtomicU32::new(0);
    let waited = rc.need_to_wait_for_eviction(rc_addr, || {
      let n = rounds.fetch_add(1, Relaxed) + 1;
      if n == 2 {
        // 第二轮模拟驱逐方完成清洗：发布越过记录地址的高水位
        rc.closed_until_address.fetch_max(abs_addr + 1, Release);
      }
    });

    assert!(waited);
    assert_eq!(rounds.load(Relaxed), 2);
    Ok(())
  }

  /// 真实环形回绕驱逐：cleanse_page 完成后 ClosedUntilAddress 单调发布到被驱逐页末尾，
  /// 被驱逐页旧地址门控触发且仅一轮 refresh 即返回（回链头重探）
  #[test]
  fn page_turn_publishes_closed_until() -> Result<()> {
    let rc = ReadCache::new(4096, 2, true)?;
    let index = HashIndex::new(16)?;

    let mut first_rc_addr = None;
    for i in 0..4096u32 {
      let key = format!("k{i}");
      index.insert(key.as_bytes(), 12345)?;
      if let Some(addr) = rc.append(key.as_bytes(), b"v", 12345, &index)
        && first_rc_addr.is_none()
      {
        first_rc_addr = Some(addr);
      }
      if rc.closed_until_address() > 0 {
        break;
      }
    }
    assert!(
      rc.closed_until_address() > 0,
      "环形回绕应发布 ClosedUntilAddress"
    );

    let evicted = first_rc_addr.expect("首条 RC 记录应存在");
    let rounds = AtomicU32::new(0);
    assert!(rc.need_to_wait_for_eviction(evicted, || {
      rounds.fetch_add(1, Relaxed);
    }));
    assert_eq!(rounds.load(Relaxed), 1);
    Ok(())
  }

  /// 撕裂窗闭环（滞留门）：被复用槽存在在途注册（模拟停滞横跨整环的滞留编码者）
  /// 时，换页侧必须阻塞至其退订后才清洗 + 换装 + 推进 tail
  #[test]
  fn turn_waits_for_inflight_registration() -> Result<()> {
    use std::sync::atomic::Ordering;

    let rc = ReadCache::new(512, 2, true)?;
    let index = HashIndex::new(16)?;

    // 填满页 0（512B 页，单记录 24B，页界分支将翻向页 1）
    let mut i = 0u32;
    while rc.tail_address() < 512 {
      let key = format!("a{i}");
      index.insert(key.as_bytes(), 12345)?;
      assert!(rc.append(key.as_bytes(), b"v", 12345, &index).is_some());
      i += 1;
    }

    // 模拟滞留编码者：已注册在途（页 2 回绕复用页 0 的槽 0）但尚未完成编码
    rc.page_inflight[0].store(1, Release);

    scope(|s| {
      s.spawn(|| {
        // 持续 append 填满页 1 并触发翻页 2（回绕驱逐页 0）；滞留注册存在时换页阻塞
        let mut j = 0u32;
        while rc.closed_until_address() == 0 {
          let key = format!("b{j}");
          let _ = index.insert(key.as_bytes(), 12345);
          let _ = rc.append(key.as_bytes(), b"v", 12345, &index);
          j += 1;
        }
      });

      // 滞留注册未退订：换页不得推进（ClosedUntilAddress 未发布、tail 不越过页 1）
      sleep(Duration::from_millis(50));
      assert_eq!(rc.closed_until_address(), 0, "在途注册存在时换页必须阻塞");
      assert!(rc.tail_address() < 1024, "tail 不得越过未清洗的页边界");

      // 滞留编码者完成（退订）：换页放行，清洗 + 换装 + ClosedUntilAddress 发布
      rc.page_inflight[0].fetch_sub(1, Ordering::Release);
    });
    assert!(rc.closed_until_address() >= 1024);
    Ok(())
  }

  /// 撕裂窗闭环（关闭拒绝）：槽位被置 CLOSED 后，旧快照注册必被拒绝退订重试，
  /// 状态字重置后恢复注册并成功编码
  #[test]
  fn closed_slot_rejects_stale_registration() -> Result<()> {
    use std::sync::atomic::Ordering;

    let rc = ReadCache::new(512, 2, true)?;
    let index = HashIndex::new(16)?;

    // 填满页 0 并翻向页 1，使 tail 落在页 1 开头
    let mut i = 0u32;
    while rc.tail_address() < 512 {
      let key = format!("c{i}");
      index.insert(key.as_bytes(), 12345)?;
      assert!(rc.append(key.as_bytes(), b"v", 12345, &index).is_some());
      i += 1;
    }
    index.insert(b"first-on-page1", 12345)?;
    assert!(rc.append(b"first-on-page1", b"v", 12345, &index).is_some());

    // 模拟换页侧两阶段关闭的第一阶段：关闭页 1 槽位
    rc.page_inflight[1].fetch_or(INFLIGHT_CLOSED, Ordering::AcqRel);

    scope(|s| {
      // 40ms 后重置（模拟换页完成）
      s.spawn(|| {
        sleep(Duration::from_millis(40));
        rc.page_inflight[1].store(0, Ordering::Release);
      });

      // 期间 append 不断重试，重置后成功且地址落在页 1
      let deadline = Instant::now() + Duration::from_secs(5);
      let _ = index.insert(b"after-close", 12345);
      let mut addr = None;
      while addr.is_none() {
        assert!(Instant::now() < deadline, "CLOSED 重置后 append 应成功");
        addr = rc.append(b"after-close", b"v", 12345, &index);
      }
      let abs = to_absolute(addr.unwrap());
      assert!((512..1024).contains(&abs), "重试成功后应落在页 1");
    });
    Ok(())
  }

  /// 撕裂窗闭环（清洗完整性）：删除页尾 pad 头后，回绕换页的被驱逐页内容恒为
  /// 完整记录序列 + 零松弛区（无 pad 头），cleanse 全量恢复索引指向主日志，
  /// 无悬垂 RC 指向、无断链
  #[test]
  fn cleansed_page_restores_index_without_pad() -> Result<()> {
    let rc = ReadCache::new(512, 2, true)?;
    let index = HashIndex::new(16)?;

    let mut mounted = Vec::new();
    for i in 0..512u32 {
      let key = format!("d{i:03}");
      // 先挂主日志地址 42，append 内部 CAS 挂载 RC 地址（对标 hei.TryCAS）；
      // 清洗后应恢复主日志地址 42
      index.insert(key.as_bytes(), 42)?;
      if let Some(rc_addr) = rc.append(key.as_bytes(), b"v", 42, &index) {
        mounted.push((key.into_bytes(), rc_addr));
      }
      if rc.closed_until_address() > 0 {
        break;
      }
    }
    assert!(rc.closed_until_address() > 0, "回绕应发生");

    // 被驱逐页（页 0）物理内容：无 pad 头，记录完整可解析且前驱为主日志地址
    let page0 = rc.buffer.read_page(0);
    let mut offset = 0;
    while offset + HEADER_SIZE <= rc.page_size {
      let Some(header) = RecordHeader::decode_opt(&page0[offset..]) else {
        break;
      };
      assert!(
        !header.is_pad(),
        "页尾只允许完整记录或零松弛区，不得有 pad 头"
      );
      if header.is_null() {
        break; // 零松弛区起点，其后必全零
      }
      let Some(rec_size) = header.checked_physical_size() else {
        break;
      };
      assert_eq!(header.address(), 42, "记录前驱必须是主日志地址");
      let key = &page0[offset + HEADER_SIZE..offset + HEADER_SIZE + header.key_len() as usize];
      assert!(key.starts_with(b"d"), "键必须完整无撕裂");
      offset += rec_size;
    }

    // 全量挂载键：索引要么仍指向窗口内完整 RC 记录，要么已被清洗恢复主日志地址
    for (key, rc_addr) in &mounted {
      let slot = index.find_tag(key).expect("已挂载键必须可寻址");
      if is_read_cache(slot) {
        assert_eq!(slot, *rc_addr, "未清洗键的 RC 指向不得漂移");
        let parsed = match rc.with_record(slot, |k, _| Some(k.to_vec())) {
          RcVisit::Found(v) => Some(v),
          _ => None,
        };
        assert_eq!(
          parsed.as_deref(),
          Some(key.as_slice()),
          "窗口内记录必须完整可解析"
        );
      } else {
        assert_eq!(slot, 42, "清洗必须将索引恢复至主日志地址，不得断链");
      }
    }
    Ok(())
  }
}
