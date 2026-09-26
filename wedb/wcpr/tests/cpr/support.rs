//! wcpr 集成测试公共 fixture：以 whlog + windex 搭建最小宿主存储引擎
//!
//! 刻意不依赖 wkv：在 crate 层直接实现 [`CprStore`]/[`CprRecover`]，
//! 校验检查点状态机与恢复语义在最小依赖面下自洽。

use std::{
  collections::{HashMap, HashSet},
  fs::OpenOptions,
  io::{Seek, SeekFrom, Write},
  path::Path,
  sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use aok::Result;
use wbase::addr::clean_address;
use wcpr::{CprRecover, CprStore, Error as WcprError, RecoveredCheckpoint, StoreMeta};
use wdev::SegmentedDevice;
use wepoch::{LightEpoch, Participant};
use whlog::{Error, HybridLog, HybridLogConfig};
use windex::{HashBucket, HashBucketEntry, HashIndex};

/// 已从 windex 生产导出面收敛掉的 key 版 `insert`：fixture 灌数按唯一免查重追加
/// 写入口 [`HashIndex::insert_to_bucket`] 等价复现（桶下标与 Tag 在此显式换算）
pub(crate) trait HashIndexTestOps {
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

/// 主哈希桶数（刻意取 64：迫使部分键落入溢出桶链，让溢出池参与快照往返）
pub(crate) const INDEX_BUCKETS: usize = 64;
/// 单页容量 16KB
pub(crate) const PAGE_SIZE: usize = 16 * 1024;
/// 环形缓冲页数
pub(crate) const NUM_PAGES: usize = 8;
/// 可变区比例
pub(crate) const MUTABLE_FRACTION: f64 = 0.5;
/// 纪元参与者上限（并发测试多 writer 各持一个参与者）
pub(crate) const MAX_SESSIONS: usize = 64;
/// ReadCache 虚拟地址基址：虚拟地址 = 主日志地址 + 基址（bit47 指示位由槽位改写方置位）
pub(crate) const RC_VIRTUAL_BASE: u64 = 1 << 40;
/// 多代 RC 晋升地址带的起点（刻意远高于任何单代 `main + RC_VIRTUAL_BASE` 形态，
/// 两套地址空间天然不相交，杜绝 fixture 内误撞）
pub(crate) const RC_GEN_BASE: u64 = RC_VIRTUAL_BASE + (1 << 30);
/// 相邻代际地址步长（仅保证发号单调、不承载布局语义）
pub(crate) const RC_GEN_STRIDE: u64 = 64;

/// fixture 版 RC 走查单步结论（口径对齐 wkv `ReadCache::walk_step`，链尽即前驱为 0）
enum RcStep {
  /// 记录仍在窗内可判读，携前驱地址续链
  Next(u64),
  /// 记录已滑出环形窗口（驱逐过渡态，清洗未落定），须等待后回链头重探
  Evicted,
}

/// 无 ReadCache 宿主的恒等解析端口（对标 `write_index_checkpoint` 文档给出的恒等闭包
/// `&|slot| clean_address(slot.load(Acquire))`：槽位不含 ReadCache 位时写侧根本不会调入）
pub(crate) fn no_rc_resolve(slot: &AtomicU64) -> u64 {
  clean_address(slot.load(Ordering::Acquire))
}

/// 元数据采样竞态注入档位（票 wcpr-hlog-meta-late-sampling-race）：关闭态。
pub(crate) const RACE_NONE: u8 = 0;
/// 驱逐形态：检查点第 6 步刷盘窗口内以真实 `shift_head_address` 把 head 顶至已刷盘前缀
/// （环形回绕驱逐 `ensure_page_ready` 的同款钳制结果），推过第 3 步捕获 tail。
pub(crate) const RACE_HEAD_PAST_TAIL: u8 = 1;
/// 紧缩形态：同窗口内以真实 `shift_begin_address` 把逻辑 begin 顶至当前活跃 tail，
/// 物理删段受 `delete_floor`（未发布检查点为 0）全钳制、不删任何数据。
pub(crate) const RACE_BEGIN_PAST_TAIL: u8 = 2;
/// 竞态注入窗口写入的固定值体（4KB，循环真实 append 直至活跃 tail 越过捕获截断点两整页）
const RACE_WINDOW_VALUE: &[u8] = &[0x77; 4096];

/// 索引快照悬挂竞态注入的会合超时（毫秒）：超时即断言判负，绝不死等拖垮门禁
const SNAPSHOT_SHIFT_TIMEOUT_MS: u64 = 10_000;

/// 挑选首个 15 位指纹（tag）非零的键（指纹取自键哈希高位，几乎必然首个即命中）；
/// `mark` 为该用例的键名前缀，保证跨用例键空间互不相交
pub(crate) fn pick_nonzero_tag_key(mark: &str) -> Vec<u8> {
  (0u32..1024)
    .map(|i| format!("{mark}{i}").into_bytes())
    .find(|k| HashBucketEntry::tag_from_hash(HashIndex::hash_key(k)) != 0)
    .expect("1024 个候选键必有非零指纹")
}

/// 最小宿主存储引擎（fixture）
pub(crate) struct MiniStore {
  pub device: Arc<SegmentedDevice>,
  pub hlog: Arc<HybridLog<SegmentedDevice>>,
  pub index: Arc<HashIndex>,
  pub epoch: Arc<LightEpoch>,
  /// 恢复路径携带的检查点元数据（新建实例为空壳占位）
  pub meta: Option<wcpr::CheckpointMeta>,
  /// 最近一次恢复内核 [`wcpr::run_recovery_kernel`] 的 undoNextVersion 回滚计数
  /// （跨轮冻结判据回归用，防静默路径漂移）
  pub recovery_undone: AtomicU64,
  /// 最近一次恢复内核模糊窗口内索引重插计数（同上）
  pub recovery_replayed: AtomicU64,
  /// 最近一次恢复内核 on_record 主机回调总次数（双承幂等回归：扫描恰一趟，
  /// 每条未被回滚的记录恰回调一次）
  pub recovery_on_record: AtomicU64,
  /// 有效条目计数（新增 +1，删除 -1）
  entries: AtomicUsize,
  /// 在线扩容相位（1:1 模拟 wkv `IndexResizeState.phase` 的 IN_PROGRESS_GROW 发布
  /// 态：置位即扩容中，检查点入口必须拒绝发起）
  growing_phase: AtomicBool,
  /// 检查点临界区独占槽位（1:1 模拟 wkv WedbStore 的 Checkpoint 相位 CAS 单槽）
  ckpt_slot: AtomicBool,
  /// ReadCache 链代次表（fixture 版 RC 环形内存的 prev 链）：RC 虚拟地址 → 前驱地址
  /// （前驱可再为 RC 打标地址（同一键多代晋升）或主日志地址；未登记者按单代
  /// [`Self::resolve_main`] 一对一映射，断链形态即映射为 0 链尽）
  rc_chain: Mutex<HashMap<u64, u64>>,
  /// 滑出环形窗口的 RC 地址集：走查命中即不可判读（「head 已过、cleanse 未及」的
  /// 驱逐过渡态，时序对齐 wkv `read_cache/append.rs` 的 head 推进 → cleanse_page →
  /// ClosedUntil 发布三段），须等待清洗落定后回链头重探
  rc_gone: Mutex<HashSet<u64>>,
  /// 走查端口累计进入驱逐等待的次数（等待重探环收敛有界的判据）
  rc_evict_waits: AtomicUsize,
  /// 多代 RC 晋升的地址发号器（见 [`Self::next_rc_addr`]）
  rc_gen_seq: AtomicU64,
  /// 元数据采样竞态注入档位（见 [`RACE_NONE`] 族）：非关闭档时 [`Self::flush_all`]
  /// 于检查点第 6 步入口消费一次——真实 append 把活跃 tail 推过第 3 步捕获截断点，
  /// 真实 flush 令持久化前缀覆盖其上，再以真实驱逐/紧缩原语把 head/begin 推过截断点，
  /// 复现旧「第 9 步现场采样」的时序撕裂态（修复后取值冻结于第 3 步同点快照）
  race_window: AtomicU8,
  /// 索引快照悬挂竞态注入（票 wcpr-index-snapshot-entry-below-compacted-begin）：
  /// 布防值 = 受害键 ReadCache 槽位原始地址（0 = 关闭）。索引快照扫描经
  /// [`Self::skip_read_cache_with_wait`] 端口触达该槽位（扫描已读入、即将固化该
  /// 条目）即置 [`Self::snapshot_shift_parked`] 会合挂起，等后台紧缩线程以真实
  /// `shift_begin_address` 把 begin 推过受害条目地址后置 [`Self::snapshot_shift_done`]
  /// 再放行——复现「快照条目已固化、其后 begin 采样被并发紧缩越位」的危害形态
  pub snapshot_shift_arm: AtomicU64,
  /// 快照扫描已读入受害槽位、挂起等待紧缩移位（见 [`Self::snapshot_shift_arm`]）
  pub snapshot_shift_parked: AtomicBool,
  /// 后台紧缩线程已完成 `shift_begin_address`（见 [`Self::snapshot_shift_arm`]）
  pub snapshot_shift_done: AtomicBool,
}

impl MiniStore {
  /// 打开（创建）fixture 存储
  pub fn open(db_path: impl AsRef<Path>) -> Result<Arc<Self>> {
    let db_path = db_path.as_ref().to_path_buf();
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let config = HybridLogConfig::new(PAGE_SIZE, NUM_PAGES, MUTABLE_FRACTION)?;
    let epoch = Arc::new(LightEpoch::new(MAX_SESSIONS));
    let hlog = Arc::new(HybridLog::new(
      config,
      Arc::clone(&device),
      Arc::clone(&epoch),
    )?);
    let index = Arc::new(HashIndex::new(INDEX_BUCKETS)?);
    Ok(Arc::new(Self {
      device,
      hlog,
      index,
      epoch,
      meta: None,
      recovery_undone: AtomicU64::new(0),
      recovery_replayed: AtomicU64::new(0),
      recovery_on_record: AtomicU64::new(0),
      entries: AtomicUsize::new(0),
      growing_phase: AtomicBool::new(false),
      ckpt_slot: AtomicBool::new(false),
      rc_chain: Mutex::new(HashMap::new()),
      rc_gone: Mutex::new(HashSet::new()),
      rc_evict_waits: AtomicUsize::new(0),
      rc_gen_seq: AtomicU64::new(0),
      race_window: AtomicU8::new(RACE_NONE),
      snapshot_shift_arm: AtomicU64::new(0),
      snapshot_shift_parked: AtomicBool::new(false),
      snapshot_shift_done: AtomicBool::new(false),
    }))
  }

  /// 发布扩容相位（对标 wkv grow_index 的相位发布手法——wkv/tests/store/resize.rs
  /// 同款：先置 IN_PROGRESS_GROW 再切表；置位期间检查点入口必须拒绝发起）
  pub fn publish_growing_phase(&self) {
    self.growing_phase.store(true, Ordering::Release);
  }

  /// 扩容完成，回到 REST 相位
  pub fn clear_growing_phase(&self) {
    self.growing_phase.store(false, Ordering::Release);
  }

  /// 注册纪元参与者（等同上层 new_session）
  pub fn session(&self) -> Result<Participant> {
    Ok(self.epoch.register()?)
  }

  /// 当前有效条目数
  pub fn entry_count(&self) -> usize {
    self.entries.load(Ordering::Acquire)
  }

  /// 追加一条记录（PageNotReady 时补刷驱逐旧页后重试；prev 为 0 表示无版本链前驱）
  pub async fn append(
    &self,
    p: &Participant,
    key: &[u8],
    val: &[u8],
    prev: u64,
    tombstone: bool,
  ) -> Result<u64> {
    loop {
      let attempt = {
        let _guard = p.enter();
        self.hlog.append(key, val, prev, tombstone)
      };
      match attempt {
        Ok((addr, _)) => return Ok(addr),
        Err(Error::PageNotReady(page_id)) => self.evict_page(page_id).await?,
        Err(e) => return Err(e.into()),
      }
    }
  }

  /// 补刷并驱逐环形槽位上的旧页（简化版宿主 evict 路径）
  async fn evict_page(&self, page_id: u64) -> Result<()> {
    let num_pages = self.hlog.config.num_pages as u64;
    if page_id < num_pages {
      return Ok(());
    }
    let old_page = page_id - num_pages;
    self.hlog.flush_page(old_page).await?;
    self.device.sync().await?;
    let min_evicted = self.hlog.config.page_start_address(old_page + 1);
    self.hlog.shift_read_only_address(min_evicted);
    self.hlog.shift_head_address(min_evicted);
    self.epoch.bump_current_epoch();
    Ok(())
  }

  /// 解析索引槽位地址为主日志真实逻辑地址（剥离 ReadCache 指示位并顺链映射）
  pub fn resolve_main(&self, slot: u64) -> u64 {
    let abs = slot & HashBucketEntry::ADDRESS_MASK;
    if abs & HashBucketEntry::READ_CACHE_BIT != 0 {
      (abs & !HashBucketEntry::READ_CACHE_BIT) - RC_VIRTUAL_BASE
    } else {
      abs
    }
  }

  /// 写入键值（新键插入索引，覆盖键 CAS 更新槽位；返回新记录地址）
  pub async fn put(&self, p: &Participant, key: &[u8], val: &[u8]) -> Result<u64> {
    let old = {
      let _guard = p.enter();
      self.index.find_tag(key)
    };
    let prev = old.map_or(0, |slot| self.resolve_main(slot));
    let addr = self.append(p, key, val, prev, false).await?;
    {
      let _guard = p.enter();
      match old {
        Some(slot) => {
          if !self.index.update_address(key, slot, addr) {
            aok::bail!("fixture put: 索引 CAS 更新失败（测试串行场景不应发生）: key={key:?}");
          }
        }
        None => self.index.insert(key, addr)?,
      }
    }
    self.entries.fetch_add(1, Ordering::AcqRel);
    Ok(addr)
  }

  /// 追加墓碑删除键（索引槽位指向墓碑记录）
  pub async fn del(&self, p: &Participant, key: &[u8]) -> Result<u64> {
    let old = {
      let _guard = p.enter();
      self.index.find_tag(key)
    };
    if old.is_none() {
      aok::bail!("fixture del: 键不存在: {key:?}");
    }
    let prev = old.map_or(0, |slot| self.resolve_main(slot));
    let addr = self.append(p, key, &[], prev, true).await?;
    {
      let _guard = p.enter();
      let slot = old.expect("old 已判非空");
      if !self.index.update_address(key, slot, addr) {
        aok::bail!("fixture del: 索引 CAS 更新失败: key={key:?}");
      }
    }
    self.entries.fetch_sub(1, Ordering::AcqRel);
    Ok(addr)
  }

  /// 读取键值（None = 不存在或已墓碑）
  pub async fn get(&self, p: &Participant, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let slot = {
      let _guard = p.enter();
      self.index.find_tag(key)
    };
    let Some(slot) = slot else {
      return Ok(None);
    };
    let addr = self.resolve_main(slot);
    if addr == 0 {
      return Ok(None);
    }
    let rec = self.hlog.read_record(addr).await?;
    if !rec.key().is_ok_and(|k| k == key) || rec.is_tombstone().unwrap_or(true) {
      return Ok(None);
    }
    Ok(Some(rec.value()?.to_vec()))
  }

  /// 将指定键的索引槽位改写为 ReadCache 虚拟形态（模拟读缓存提升回写索引槽位，
  /// 单代：该 RC 记录的前驱即原主日志地址）；返回改写后的 RC 槽位地址
  pub fn install_read_cache_entry(&self, p: &Participant, key: &[u8]) -> Result<u64> {
    let _guard = p.enter();
    let main = self
      .index
      .find_tag(key)
      .ok_or_else(|| aok::anyhow!("install_read_cache_entry: 键不存在: {key:?}"))?
      & HashBucketEntry::ADDRESS_MASK;
    let virt = (main + RC_VIRTUAL_BASE) | HashBucketEntry::READ_CACHE_BIT;
    self.rc_chain_mut().insert(virt, main);
    if self.cas_key_slot(key, main, virt) {
      Ok(virt)
    } else {
      aok::bail!("install_read_cache_entry: 未定位到匹配槽位: key={key:?}")
    }
  }

  /// 追加一代 ReadCache 晋升：把该键槽位当前值串到新代 RC 记录之下（槽位改指新代、
  /// 新代 prev 指旧值），返回新代 RC 地址——对标同一键反复命中读缓存逐代晋升出的
  /// 多代 RC 链（链中段滑出形态的构造入口）
  pub fn push_read_cache_generation(&self, p: &Participant, key: &[u8]) -> Result<u64> {
    let _guard = p.enter();
    let cur = self
      .index
      .find_tag(key)
      .ok_or_else(|| aok::anyhow!("push_read_cache_generation: 键不存在: {key:?}"))?
      & HashBucketEntry::ADDRESS_MASK;
    let next = self.next_rc_addr();
    self.rc_chain_mut().insert(next, cur);
    if self.cas_key_slot(key, cur, next) {
      Ok(next)
    } else {
      aok::bail!("push_read_cache_generation: 槽位 CAS 失败: key={key:?}")
    }
  }

  /// 新一代 RC 记录地址（RC 环形内存内单调递增的虚拟偏移，落在 [`RC_GEN_BASE`]
  /// 高位带，与单代 `main + RC_VIRTUAL_BASE` 形态天然不相交）
  fn next_rc_addr(&self) -> u64 {
    let n = self.rc_gen_seq.fetch_add(1, Ordering::AcqRel);
    (RC_GEN_BASE + (n + 1) * RC_GEN_STRIDE) | HashBucketEntry::READ_CACHE_BIT
  }

  /// 在指定键的槽位上把地址字段从 `old` 换指 `new`（高 16 位指纹位原样保留，对标
  /// C# HashBucketEntry.cs:49 Address setter 的掩码换写语义）
  fn cas_key_slot(&self, key: &[u8], old: u64, new: u64) -> bool {
    let bucket = self.index.bucket(self.index.bucket_index_for_key(key));
    bucket.entries[..HashBucket::DATA_ENTRIES]
      .iter()
      .any(|slot| {
        let cur = slot.load(Ordering::Acquire);
        cur & HashBucketEntry::ADDRESS_MASK == old
          && slot
            .compare_exchange(
              cur,
              (cur & !HashBucketEntry::ADDRESS_MASK) | new,
              Ordering::AcqRel,
              Ordering::Acquire,
            )
            .is_ok()
      })
  }

  fn rc_chain_mut(&self) -> MutexGuard<'_, HashMap<u64, u64>> {
    self.rc_chain.lock().unwrap_or_else(|e| e.into_inner())
  }

  /// 布防 ReadCache 换页驱逐过渡态（链头形态）：把该键槽位当前 RC 地址登记为「已
  /// 滑出窗口」，此后该地址走查恒不可判读，直到快照写侧进入驱逐等待端口（端口内
  /// 1:1 复现 `cleanse_page` 落定），与真实换页临界区的落定顺序严格同构
  pub fn arm_read_cache_eviction(&self, key: &[u8]) -> Result<()> {
    let slot = self
      .index
      .find_tag(key)
      .ok_or_else(|| aok::anyhow!("arm_read_cache_eviction: 键不存在: {key:?}"))?;
    if slot & HashBucketEntry::READ_CACHE_BIT == 0 {
      aok::bail!("arm_read_cache_eviction: 槽位非 ReadCache 形态: {slot:#x}");
    }
    self.arm_read_cache_eviction_at(slot & HashBucketEntry::ADDRESS_MASK);
    Ok(())
  }

  /// 布防指定 RC 地址为「已滑出窗口」：链上任一代皆可（链中段形态即本口存在的全部
  /// 理由——槽头仍在窗内，锚链头判定的等待口永不触发）
  pub fn arm_read_cache_eviction_at(&self, rc_addr: u64) {
    self
      .rc_gone
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .insert(rc_addr);
  }

  /// 驱逐等待端口累计调入次数（等待重探环的收敛上界判据）
  pub fn rc_evict_waits(&self) -> usize {
    self.rc_evict_waits.load(Ordering::Acquire)
  }

  /// fixture 版走查单步（三态口径同 wkv `ReadCache::walk_step`）：已滑出窗口的 RC
  /// 地址报不可判读，其余携前驱续链
  fn rc_step(&self, rc_addr: u64) -> RcStep {
    if self
      .rc_gone
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .contains(&rc_addr)
    {
      return RcStep::Evicted;
    }
    RcStep::Next(self.rc_prev_of(rc_addr))
  }

  /// 取该 RC 地址登记的前驱（未登记者按单代一对一映射，断链形态即映射为 0 链尽）；
  /// 滑出记录自身的前驱仍可读，对标真实环形内存中被密封记录的头部前驱字段
  fn rc_prev_of(&self, rc_addr: u64) -> u64 {
    self
      .rc_chain
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .get(&rc_addr)
      .copied()
      .unwrap_or_else(|| self.resolve_main(rc_addr))
  }

  /// 1:1 复现 `cleanse_page` 的落定动作（对标 wkv `read_cache/cleanse.rs`）：
  /// 1. 取滑出记录的前驱 `prev`（清洗后本槽应换指的地址，对标 `hei.try_cas(rec_prev)`）；
  /// 2. 指向本记录的链项改指 `prev`（对标 `patch_prev_atomic` 缝链，走查自此跳过它）；
  /// 3. 仍指向本记录的索引槽位 CAS 换指 `prev`（指纹位原样保留）；
  /// 4. 解除本地址的滑出登记（对标 ClosedUntilAddress 发布）
  fn cleanse_read_cache(&self, gone: u64) {
    let prev = self.rc_prev_of(gone);
    {
      let mut chain = self.rc_chain_mut();
      for link in chain.values_mut() {
        if *link == gone {
          *link = prev;
        }
      }
    }
    for bucket in self.index.buckets.iter() {
      for slot in bucket.entries[..HashBucket::DATA_ENTRIES].iter() {
        let cur = slot.load(Ordering::Acquire);
        if cur & HashBucketEntry::ADDRESS_MASK != gone {
          continue;
        }
        let _ = slot.compare_exchange(
          cur,
          (cur & !HashBucketEntry::ADDRESS_MASK) | prev,
          Ordering::AcqRel,
          Ordering::Acquire,
        );
      }
    }
    self
      .rc_gone
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .remove(&gone);
  }

  /// 布防元数据采样竞态注入（见 [`RACE_HEAD_PAST_TAIL`]/[`RACE_BEGIN_PAST_TAIL`]）：
  /// 由检查点第 6 步 [`Self::flush_all`] 入口消费一次，在 tail 捕获（第 3 步）之后、
  /// 元数据采样（第 9 步）之前的真实并发窗口内推高 head/begin 越过捕获截断点
  pub fn arm_sampling_race(&self, mode: u8) {
    self.race_window.store(mode, Ordering::Release);
  }

  /// 布防索引快照悬挂竞态注入（见 [`Self::snapshot_shift_arm`]）：`rc_slot_addr`
  /// 为置 ReadCache 位的受害槽位原始地址（快照扫描解析端口按其匹配触发会合）
  pub fn arm_index_snapshot_shift_race(&self, rc_slot_addr: u64) {
    self.snapshot_shift_parked.store(false, Ordering::Release);
    self.snapshot_shift_done.store(false, Ordering::Release);
    self
      .snapshot_shift_arm
      .store(rc_slot_addr, Ordering::Release);
  }

  /// 竞态注入窗口内并发前台写入：真实 `append` 把活跃 tail 推高至少两个整页，确保随后
  /// 的 `flush_all` 将持久化前缀（`flushed_until`）推过第 3 步捕获的截断点——head/begin
  /// 受 `flushed_until`/`tail` 钳制，唯此方可越过截断点复现旧现采撕裂态（非假 mock）
  async fn grow_tail_beyond_captured(&self) -> wcpr::Result<()> {
    let base = self.hlog.tail_address();
    let target = base + 2 * PAGE_SIZE as u64;
    let participant = self.session().map_err(|e| WcprError::Host(e.to_string()))?;
    let mut i = 0u64;
    while self.hlog.tail_address() < target {
      let key = format!("race:window:{i}");
      self
        .append(&participant, key.as_bytes(), RACE_WINDOW_VALUE, 0, false)
        .await
        .map_err(|e| WcprError::Host(e.to_string()))?;
      i += 1;
    }
    Ok(())
  }

  /// 统计 [from, until) 区间内的记录条数（含墓碑）
  pub async fn scan_count(&self, p: &Participant, from: u64, until: u64) -> Result<usize> {
    let _guard = p.enter();
    let mut iter = self.hlog.scan_iter(from, until);
    let mut buf = Vec::with_capacity(PAGE_SIZE);
    let mut n = 0usize;
    while iter.next_into(&mut buf).await?.is_some() {
      n += 1;
    }
    Ok(n)
  }

  /// 模拟崩溃残留：绕过日志协议直接向设备文件 tail 起始处覆写垃圾字节
  pub fn scorch_device_beyond_tail(&self, len: usize) -> Result<u64> {
    let tail = self.hlog.tail_address();
    let seg_size = self.device.segment_size();
    let seg_id = (tail / seg_size) as u32;
    let seg_offset = tail % seg_size;
    let actual_path = self.device.segment_path(seg_id);

    let mut f = OpenOptions::new().write(true).open(&actual_path)?;
    f.seek(SeekFrom::Start(seg_offset))?;

    const CHUNK: [u8; 1024] = [0xA5; 1024];
    let mut remain = len;
    while remain > 0 {
      let n = remain.min(CHUNK.len());
      f.write_all(&CHUNK[..n])?;
      remain -= n;
    }
    f.sync_all()?;
    Ok(tail)
  }
}

impl CprStore for MiniStore {
  type Device = SegmentedDevice;

  fn hlog(&self) -> &HybridLog<Self::Device> {
    &self.hlog
  }

  fn index(&self) -> Arc<HashIndex> {
    Arc::clone(&self.index)
  }

  fn is_growing(&self) -> bool {
    self.growing_phase.load(Ordering::Acquire)
  }

  /// 进入检查点临界区（对标 wkv WedbStore 实现：单槽 CAS，被占即 Host 拒绝）
  fn enter_checkpoint(&self) -> wcpr::Result<()> {
    self
      .ckpt_slot
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .map(|_| ())
      .map_err(|_| WcprError::Host("检查点临界区槽位被占".into()))
  }

  /// 退出检查点临界区（幂等复位）
  fn exit_checkpoint(&self) {
    self.ckpt_slot.store(false, Ordering::Release);
  }

  fn epoch(&self) -> &LightEpoch {
    &self.epoch
  }

  fn tail_address(&self) -> u64 {
    self.hlog.tail_address()
  }

  fn begin_address(&self) -> u64 {
    self.hlog.begin_address()
  }

  fn head_address(&self) -> u64 {
    self.hlog.head_address()
  }

  fn shift_read_only_address(&self, target: u64) {
    self.hlog.shift_read_only_address(target);
  }

  async fn flush_all(&self) -> wcpr::Result<()> {
    // 元数据采样竞态注入（默认关闭，一次消费）：本入口即检查点第 6 步，运行于第 3 步
    // tail 捕获之后、第 9 步 meta 采样之前的真实并发窗口。先以真实 append 把活跃 tail
    // 推过捕获截断点、令随后 flush 的持久化前缀覆盖其上，再在刷盘后用真实驱逐/紧缩原语
    // 把 head/begin 推过截断点——旧「第 9 步现场采样」据此读到越界现值产出撕裂元数据，
    // 修复后取值冻结于第 3 步同点快照（head/begin 恒 <= 捕获 tail）。
    let race = self.race_window.swap(RACE_NONE, Ordering::AcqRel);
    if race != RACE_NONE {
      self.grow_tail_beyond_captured().await?;
    }
    self.hlog.flush_all().await?;
    self.device.sync().await?;
    match race {
      RACE_HEAD_PAST_TAIL => {
        // 环形回绕驱逐结果：head 顶至已刷盘前缀（`ensure_page_ready` 同款钳制）
        let flushed = self.hlog.flushed_until_address();
        self.hlog.shift_head_address(flushed);
      }
      RACE_BEGIN_PAST_TAIL => {
        // 紧缩链推进逻辑 begin（物理删段受 delete_floor 全钳制，不删任何数据）
        let tail = self.hlog.tail_address();
        self.hlog.shift_begin_address(tail).await?;
      }
      _ => {}
    }
    Ok(())
  }

  fn entry_count(&self) -> usize {
    self.entries.load(Ordering::Acquire)
  }

  fn skip_read_cache_with_wait(&self, slot: &AtomicU64) -> u64 {
    // 索引快照悬挂竞态会合（默认关闭，一次消费）：本槽位命中布防值即快照扫描
    // 已读入受害条目——在此挂起，等后台紧缩线程以真实 shift_begin_address 把
    // begin 推过该条目地址后再放行（旧形态其后第 3 步 cp_begin 采样必采到越界
    // 值；修复后 begin 已冻结于入口 PREPARE 段采样、恒 <= 受害条目地址）。
    // 挂起为同步线程让渡（1ms sleep），紧缩线程携独立 Runtime 在另一核推进，
    // 不复用本线程 reactor，绝无 compio block_on 重入
    let armed = self.snapshot_shift_arm.load(Ordering::Acquire);
    if armed != 0 && slot.load(Ordering::Acquire) & HashBucketEntry::ADDRESS_MASK == armed {
      self.snapshot_shift_arm.store(0, Ordering::Release);
      self.snapshot_shift_parked.store(true, Ordering::Release);
      let started = Instant::now();
      while !self.snapshot_shift_done.load(Ordering::Acquire) {
        assert!(
          started.elapsed() < Duration::from_millis(SNAPSHOT_SHIFT_TIMEOUT_MS),
          "后台紧缩线程未在超时内完成移位：注入门失效"
        );
        thread::sleep(Duration::from_millis(1));
      }
    }
    // 与 wkv::ReadCache::skip_read_cache_with_wait 同契约：恒返回解析后的裸主日志地址
    // （0 = 链尽），链头以**重读槽位**取得，滑出形态进入等待（复现 cleanse 落定）后
    // 重读槽位回链头重探。槽位高 16 位 tag 的保留是快照写侧 resolve_read_cache 的职责，
    // fixture 不得代劳（此前在返回值拼回 tag 恰好掩盖了写侧清零 tag 的缺陷）
    loop {
      let mut addr = slot.load(Ordering::Acquire) & HashBucketEntry::ADDRESS_MASK;
      let mut restart = false;
      while addr & HashBucketEntry::READ_CACHE_BIT != 0 {
        match self.rc_step(addr) {
          RcStep::Next(prev) => addr = prev,
          // 逐位置锚定的驱逐等待：以滑出地址本身落定，绝不锚链头
          RcStep::Evicted => {
            self.rc_evict_waits.fetch_add(1, Ordering::AcqRel);
            self.cleanse_read_cache(addr);
            restart = true;
            break;
          }
        }
      }
      if !restart {
        return addr;
      }
    }
  }

  fn take_range_index_checkpoints(&self, _dir: &Path, _token: u128) -> wcpr::Result<usize> {
    Ok(0)
  }

  fn set_range_index_checkpoint_barrier(&self) {}

  fn clear_range_index_checkpoint_barrier(&self) {}

  fn checkpoint_store_meta(&self) -> StoreMeta {
    StoreMeta {
      index_size: INDEX_BUCKETS,
      page_size: PAGE_SIZE,
      num_pages: NUM_PAGES,
      mutable_fraction: MUTABLE_FRACTION,
      max_sessions: MAX_SESSIONS,
      enable_revivification: false,
      enable_read_cache: true,
      read_cache_num_pages: 8,
      range_index_dir: None,
      next_key_id: 0,
    }
  }
}

/// 最小宿主的恢复回调：计数形态（双承幂等回归断言 on_record 恰一收集；
/// 无 RangeIndex/DbMeta 等宿主状态可收集，计数即全部素材面）
struct CountingVisitor {
  on_record: u64,
}

impl wcpr::RecoveryVisitor for CountingVisitor {
  fn on_record(&mut self, _addr: u64, _key: &[u8], _value: &[u8], _is_tombstone: bool) {
    self.on_record += 1;
  }
}

impl CprRecover for MiniStore {
  async fn from_recovered(
    recovered: RecoveredCheckpoint<Self::Device>,
    _checkpoint_dir: &Path,
    device: Arc<Self::Device>,
  ) -> wcpr::Result<Self> {
    let entry_count = recovered.meta.index_meta.entry_count;
    // 契约：恢复期唯一一次有序扫描内核恰好调用一次（模糊区重插）。恢复期
    // 写者已冻结，仍按扫描内核的纪元契约进入只读临界区兜底。
    let participant = recovered.epoch.register()?;
    let index_start = recovered.meta.index_start_logical_address;
    let begin = recovered.hlog.begin_address();
    let tail = recovered.hlog.tail_address();
    let (stats, on_record) = {
      let _guard = participant.enter();
      let mut visitor = CountingVisitor { on_record: 0 };
      let stats = wcpr::run_recovery_kernel(
        &recovered.hlog,
        &recovered.index,
        begin,
        index_start,
        tail,
        // 宿主默认启用模糊区回滚（undoNextVersion），与 wkv 正式恢复栈同参
        true,
        &mut visitor,
      )
      .await?;
      (stats, visitor.on_record)
    };
    Ok(Self {
      device: Arc::clone(&device),
      meta: Some(recovered.meta),
      hlog: recovered.hlog,
      index: recovered.index,
      epoch: recovered.epoch,
      recovery_undone: AtomicU64::new(stats.undone),
      recovery_replayed: AtomicU64::new(stats.replayed),
      recovery_on_record: AtomicU64::new(on_record),
      entries: AtomicUsize::new(entry_count),
      growing_phase: AtomicBool::new(false),
      ckpt_slot: AtomicBool::new(false),
      rc_chain: Mutex::new(HashMap::new()),
      rc_gone: Mutex::new(HashSet::new()),
      rc_evict_waits: AtomicUsize::new(0),
      rc_gen_seq: AtomicU64::new(0),
      race_window: AtomicU8::new(RACE_NONE),
      snapshot_shift_arm: AtomicU64::new(0),
      snapshot_shift_parked: AtomicBool::new(false),
      snapshot_shift_done: AtomicBool::new(false),
    })
  }
}

/// 并发测试看门狗：超出预算后周期性输出诊断，防测试挂死无从定位
pub(crate) struct Watchdog {
  done: Arc<AtomicBool>,
}

impl Watchdog {
  pub fn start(name: &'static str, budget: Duration) -> Self {
    let done = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&done);
    let start = Instant::now();
    let spawned = thread::Builder::new()
      .name(format!("watchdog-{name}"))
      .spawn(move || {
        let mut last_report = 0u64;
        while !flag.load(Ordering::Relaxed) {
          thread::sleep(Duration::from_secs(1));
          let elapsed = start.elapsed().as_secs();
          if elapsed >= budget.as_secs() && elapsed > last_report {
            last_report = elapsed;
            eprintln!(
              "[watchdog] 测试 {name} 已运行 {elapsed}s 超预算 {}s 仍未结束，疑似挂死",
              budget.as_secs()
            );
          }
        }
      });
    if spawned.is_err() {
      eprintln!("[watchdog] 看门狗线程创建失败，仅损失诊断能力: {name}");
    }
    Self { done }
  }
}

impl Drop for Watchdog {
  fn drop(&mut self) {
    self.done.store(true, Ordering::Relaxed);
  }
}
