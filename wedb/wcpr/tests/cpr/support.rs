//! wcpr 集成测试公共 fixture：以 whlog + windex 搭建最小宿主存储引擎
//!
//! 刻意不依赖 wkv：在 crate 层直接实现 [`CprStore`]/[`CprRecover`]，
//! 校验检查点状态机与恢复语义在最小依赖面下自洽。

use std::{
  fs::OpenOptions,
  io::{Seek, SeekFrom, Write},
  path::{Path, PathBuf},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use aok::Result;
use wcpr::{CprRecover, CprStore, RecoveredCheckpoint, StoreMeta};
use wdev::SegmentedDevice;
use wepoch::{LightEpoch, Participant};
use whlog::{HybridLog, HybridLogConfig};
use windex::{HashBucket, HashBucketEntry, HashIndex};

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

/// 最小宿主存储引擎（fixture）
pub(crate) struct MiniStore {
  pub device: Arc<SegmentedDevice>,
  pub hlog: Arc<HybridLog<SegmentedDevice>>,
  pub index: Arc<HashIndex>,
  pub epoch: Arc<LightEpoch>,
  /// 设备文件路径（模拟崩溃残留直接写文件用）
  pub db_path: PathBuf,
  /// 恢复路径携带的检查点元数据（新建实例为空壳占位）
  pub meta: Option<wcpr::CheckpointMeta>,
  /// 有效条目计数（新增 +1，删除 -1）
  entries: AtomicUsize,
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
      db_path,
      meta: None,
      entries: AtomicUsize::new(0),
    }))
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
  async fn append(
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
        Ok(addr) => return Ok(addr),
        Err(whlog::Error::PageNotReady(page_id)) => self.evict_page(page_id).await?,
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
    self.epoch.bump_epoch();
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

  /// 将指定键的索引槽位改写为 ReadCache 虚拟形态（模拟读缓存提升回写索引槽位）
  pub fn install_read_cache_entry(&self, p: &Participant, key: &[u8]) -> Result<()> {
    let _guard = p.enter();
    let main = self
      .index
      .find_tag(key)
      .ok_or_else(|| aok::anyhow!("install_read_cache_entry: 键不存在: {key:?}"))?;
    let bucket = self.index.bucket_for_key(key);
    for slot in &bucket.entries[..HashBucket::DATA_ENTRIES] {
      let cur = slot.load(Ordering::Acquire);
      if cur & HashBucketEntry::ADDRESS_MASK == main {
        let virt = (main + RC_VIRTUAL_BASE) | HashBucketEntry::READ_CACHE_BIT;
        let new_raw = (cur & !HashBucketEntry::ADDRESS_MASK) | virt;
        if slot
          .compare_exchange(cur, new_raw, Ordering::AcqRel, Ordering::Acquire)
          .is_ok()
        {
          return Ok(());
        }
      }
    }
    aok::bail!("install_read_cache_entry: 未定位到匹配槽位: key={key:?}");
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
    let mut f = OpenOptions::new().write(true).open(&self.db_path)?;
    f.seek(SeekFrom::Start(tail))?;
    f.write_all(&vec![0xA5u8; len])?;
    f.sync_all()?;
    Ok(tail)
  }
}

impl CprStore for MiniStore {
  type Device = SegmentedDevice;

  fn hlog(&self) -> &HybridLog<Self::Device> {
    &self.hlog
  }

  fn index(&self) -> &HashIndex {
    &self.index
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
    self.hlog.flush_all().await?;
    self.device.sync().await?;
    Ok(())
  }

  fn entry_count(&self) -> usize {
    self.entries.load(Ordering::Acquire)
  }

  fn skip_read_cache(&self, addr: u64) -> u64 {
    // 与 wkv::ReadCache::skip_read_cache 同契约：返回裸主日志地址（tag 全零），
    // 断链返回 0。槽位高 16 位 tag 的保留是快照写侧 resolve_read_cache 的职责，
    // fixture 不得代劳（此前在返回值拼回 tag 恰好掩盖了写侧清零 tag 的缺陷）
    self.resolve_main(addr)
  }

  fn take_range_index_checkpoints(&self, _dir: &Path, _token: u128) -> wcpr::Result<usize> {
    Ok(0)
  }

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

impl CprRecover for MiniStore {
  async fn from_recovered(
    recovered: RecoveredCheckpoint<Self::Device>,
    _checkpoint_dir: &Path,
    device: Arc<Self::Device>,
  ) -> wcpr::Result<Self> {
    let db_path = device.segment_path(device.start_segment());
    let entry_count = recovered.meta.index_meta.entry_count;
    Ok(Self {
      db_path,
      device: Arc::clone(&device),
      meta: Some(recovered.meta),
      hlog: recovered.hlog,
      index: recovered.index,
      epoch: recovered.epoch,
      entries: AtomicUsize::new(entry_count),
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
