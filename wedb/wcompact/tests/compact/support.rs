//! wcompact 集成测试公共 fixture：以 whlog + windex 搭建最小宿主存储引擎，
//! 并提供确定性竞争注入点（在紧缩迁移的追加路径上单次触发，模拟宿主并发槽位改写）

use std::{
  mem,
  path::Path,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use aok::Result as AokResult;
use wcompact::{CompactSession, CompactStore, Result as CompactResult};
use wdev::SegmentedDevice;
use wepoch::{EpochGuard, LightEpoch, Participant};
use whasher::{HashMap, new_hash_map};
use whlog::{HybridLog, HybridLogConfig};
use windex::{HashBucket, HashBucketEntry, HashIndex};

/// 主哈希桶数
pub(crate) const INDEX_BUCKETS: usize = 256;
/// 单页容量 64KB
pub(crate) const PAGE_SIZE: usize = 64 * 1024;
/// 环形缓冲页数
pub(crate) const NUM_PAGES: usize = 8;
/// 可变区比例
pub(crate) const MUTABLE_FRACTION: f64 = 0.5;
/// 纪元参与者上限
pub(crate) const MAX_SESSIONS: usize = 64;

/// 确定性竞争注入（在 session 追加路径上单次触发后自动解除）
pub(crate) enum Inject {
  /// 无注入
  None,
  /// 良性竞争：指定键的索引槽位原位置上 ReadCache 指示位（主地址不变，
  /// 模拟读缓存提升/驱逐回写对索引槽位的改写，记录仍为最新存活版本）
  BenignReadCache { key: Vec<u8> },
  /// 真覆盖竞争：指定键的索引槽位 CAS 至 to 地址（模拟并发新版本已生效）
  StealSlot { key: Vec<u8>, to: u64 },
}

/// 最小宿主存储引擎（fixture）
pub(crate) struct FixtureStore {
  pub device: Arc<SegmentedDevice>,
  pub hlog: Arc<HybridLog<SegmentedDevice>>,
  pub index: Arc<HashIndex>,
  pub epoch: Arc<LightEpoch>,
  /// 孤儿副本归还记录 (addr, size)，用于复活池计数断言
  pub reviv_puts: Mutex<Vec<(u64, u32)>>,
  /// key_id -> (current_version, is_alive)
  pub key_meta: Mutex<HashMap<u64, (u64, bool)>>,
  inject: Mutex<Inject>,
}

/// 紧缩会话（fixture）
pub(crate) struct FixtureSession {
  pub store: Arc<FixtureStore>,
  participant: Participant,
}

impl FixtureStore {
  /// 打开（创建）fixture 存储
  pub fn open(db_path: impl AsRef<Path>) -> AokResult<Arc<Self>> {
    let device = Arc::new(SegmentedDevice::single_file(db_path.as_ref())?);
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
      reviv_puts: Mutex::new(Vec::new()),
      key_meta: Mutex::new(new_hash_map()),
      inject: Mutex::new(Inject::None),
    }))
  }

  /// 创建紧缩会话
  pub fn session(self: &Arc<Self>) -> AokResult<FixtureSession> {
    Ok(FixtureSession {
      store: Arc::clone(self),
      participant: self.epoch.register()?,
    })
  }

  /// 孤儿副本归还记录快照（供复活池计数断言）
  pub fn reviv_puts_snapshot(&self) -> Vec<(u64, u32)> {
    self
      .reviv_puts
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .clone()
  }

  /// 预埋一次性竞争注入（下一次 session 追加路径触发）
  pub fn arm_inject(&self, inject: Inject) {
    *self.inject.lock().unwrap_or_else(|e| e.into_inner()) = inject;
  }

  /// 触发并解除注入（仅 session 追加路径调用）
  fn fire_inject(&self) {
    let inject = mem::replace(
      &mut *self.inject.lock().unwrap_or_else(|e| e.into_inner()),
      Inject::None,
    );
    match inject {
      Inject::None => {}
      Inject::BenignReadCache { key } => {
        let Some(main) = self.index.find_tag(&key) else {
          return;
        };
        // 单桶内定位地址匹配的数据槽位，原位置上 ReadCache 指示位（主地址不变）
        let bucket = self.index.bucket_for_key(&key);
        for slot in &bucket.entries[..HashBucket::DATA_ENTRIES] {
          let cur = slot.load(Ordering::Acquire);
          if cur & HashBucketEntry::ADDRESS_MASK == main {
            let _ = slot.compare_exchange(
              cur,
              cur | HashBucketEntry::READ_CACHE_BIT,
              Ordering::AcqRel,
              Ordering::Acquire,
            );
            break;
          }
        }
      }
      Inject::StealSlot { key, to } => {
        let Some(cur) = self.index.find_tag(&key) else {
          return;
        };
        assert!(
          self.index.update_address(&key, cur, to),
          "注入失败: 槽位抢占未生效 key={key:?}"
        );
      }
    }
  }

  /// 解析槽位地址为主日志真实逻辑地址（剥离 ReadCache 指示位）
  pub fn resolve_main(&self, slot: u64) -> u64 {
    slot & HashBucketEntry::ADDRESS_MASK & !HashBucketEntry::READ_CACHE_BIT
  }

  /// 封印只读边界至 until
  pub fn seal_read_only(&self, until: u64) {
    self.hlog.shift_read_only_address(until);
    self.epoch.bump_epoch();
  }

  /// 写入 String 物理键（新键插入索引，覆盖键 CAS 挂载，CAS 失败取新槽位重试）
  pub async fn put(&self, s: &FixtureSession, key: &[u8], val: &[u8]) -> CompactResult<u64> {
    loop {
      let old = {
        let _guard = s.participant.enter();
        self.index.find_tag(key)
      };
      let prev = old.map_or(0, |slot| self.resolve_main(slot));
      let addr = s.append_record(key, val, prev, false).await?;
      let mounted = {
        let _guard = s.participant.enter();
        match old {
          Some(slot) => self.index.update_address(key, slot, addr),
          None => {
            self.index.insert(key, addr)?;
            true
          }
        }
      };
      if mounted {
        return Ok(addr);
      }
    }
  }

  /// 追加墓碑删除 String 物理键
  pub async fn del(&self, s: &FixtureSession, key: &[u8]) -> CompactResult<()> {
    let old = {
      let _guard = s.participant.enter();
      self.index.find_tag(key)
    };
    let prev = old.map_or(0, |slot| self.resolve_main(slot));
    let addr = s.append_record(key, &[], prev, true).await?;
    {
      let _guard = s.participant.enter();
      match old {
        Some(slot) => {
          assert!(
            self.index.update_address(key, slot, addr),
            "fixture del: 索引 CAS 更新失败: key={key:?}"
          );
        }
        None => {
          self.index.insert(key, addr)?;
        }
      }
    }
    Ok(())
  }

  /// 绕过索引直写尾部（模拟并发写者已追加新版本但尚未发布索引）
  pub async fn append_raw(&self, s: &FixtureSession, key: &[u8], val: &[u8]) -> CompactResult<u64> {
    let _guard = s.participant.enter();
    Ok(self.hlog.append(key, val, 0, false)?)
  }

  /// 读取 String 物理键（None = 不存在或已墓碑）
  pub async fn get(&self, s: &FixtureSession, key: &[u8]) -> CompactResult<Option<Vec<u8>>> {
    let slot = {
      let _guard = s.participant.enter();
      self.index.find_tag(key)
    };
    let Some(slot) = slot else {
      return Ok(None);
    };
    let rec = self.hlog.read_record(self.resolve_main(slot)).await?;
    if !rec.key().is_ok_and(|k| k == key) || rec.is_tombstone().unwrap_or(true) {
      return Ok(None);
    }
    Ok(Some(rec.value()?.to_vec()))
  }

  /// 写入 TTL 物理键记录（8 字节大端 .NET Ticks 到期戳，与宿主 TTL 记录值同域）
  pub async fn put_ttl(
    &self,
    s: &FixtureSession,
    user_key: &[u8],
    expiry_ticks: i64,
  ) -> CompactResult<()> {
    self
      .put(s, &ttl_key(user_key), &expiry_ticks.to_be_bytes())
      .await?;
    Ok(())
  }
}

/// 用户键的 String 物理键
pub(crate) fn str_key(user_key: &[u8]) -> Vec<u8> {
  let mut key = Vec::with_capacity(4 + user_key.len());
  key.extend_from_slice(b"str:");
  key.extend_from_slice(user_key);
  key
}

/// 用户键的 TTL 物理键
pub(crate) fn ttl_key(user_key: &[u8]) -> Vec<u8> {
  let mut key = Vec::with_capacity(4 + user_key.len());
  key.extend_from_slice(b"ttl:");
  key.extend_from_slice(user_key);
  key
}

impl CompactSession<SegmentedDevice> for FixtureSession {
  type EpochGuard<'a>
    = EpochGuard<'a>
  where
    Self: 'a;

  fn enter_epoch(&self) -> Self::EpochGuard<'_> {
    self.participant.enter()
  }

  async fn append_record(
    &self,
    key: &[u8],
    val: &[u8],
    expected_main_addr: u64,
    is_tombstone: bool,
  ) -> CompactResult<u64> {
    // 确定性竞争注入点：紧缩迁移追加前模拟宿主并发槽位改写
    self.store.fire_inject();
    loop {
      let attempt = {
        let _guard = self.participant.enter();
        self
          .store
          .hlog
          .append(key, val, expected_main_addr, is_tombstone)
      };
      match attempt {
        Ok(addr) => return Ok(addr),
        Err(whlog::Error::PageNotReady(page_id)) => {
          self.store.evict_page(page_id).await?;
        }
        Err(e) => return Err(e.into()),
      }
    }
  }

  async fn read_ttl_expiry(&self, ttl_key: &[u8]) -> CompactResult<Option<i64>> {
    let slot = {
      let _guard = self.participant.enter();
      self.store.index.find_tag(ttl_key)
    };
    let Some(slot) = slot else {
      return Ok(None);
    };
    let rec = self
      .store
      .hlog
      .read_record(self.store.resolve_main(slot))
      .await?;
    if !rec.key().is_ok_and(|k| k == ttl_key) || rec.is_tombstone().unwrap_or(true) {
      return Ok(None);
    }
    Ok(
      <[u8; 8]>::try_from(rec.value()?)
        .ok()
        .map(i64::from_be_bytes),
    )
  }
}

impl FixtureStore {
  /// 补刷并驱逐环形槽位上的旧页（简化版宿主 evict 路径）
  async fn evict_page(&self, page_id: u64) -> CompactResult<()> {
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
}

impl CompactStore for FixtureStore {
  type Device = SegmentedDevice;
  type Session = FixtureSession;

  fn new_session(self: &Arc<Self>) -> CompactResult<Self::Session> {
    Ok(FixtureSession {
      store: Arc::clone(self),
      participant: self.epoch.register()?,
    })
  }

  fn hlog(&self) -> &whlog::HybridLog<Self::Device> {
    &self.hlog
  }

  fn index(&self) -> &HashIndex {
    &self.index
  }

  fn read_only_address(&self) -> u64 {
    self.hlog.read_only_address()
  }

  fn begin_address(&self) -> u64 {
    self.hlog.begin_address()
  }

  async fn shift_begin_address(&self, until: u64) -> CompactResult<()> {
    Ok(self.hlog.shift_begin_address(until).await?)
  }

  fn is_read_cache_addr(&self, addr: u64) -> bool {
    addr & HashBucketEntry::READ_CACHE_BIT != 0
  }

  fn skip_read_cache(&self, addr: u64) -> u64 {
    addr & !HashBucketEntry::READ_CACHE_BIT
  }

  fn enable_revivification(&self) -> bool {
    true
  }

  fn reviv_put(&self, addr: u64, size: u32, _read_only_addr: u64) {
    self
      .reviv_puts
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .push((addr, size));
  }

  fn get_key_id_meta(&self, key_id: u64) -> Option<(u64, bool)> {
    self
      .key_meta
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .get(&key_id)
      .copied()
  }

  fn update_key_id_meta(&self, key_id: u64, version: u64, is_alive: bool) {
    self
      .key_meta
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .insert(key_id, (version, is_alive));
  }

  fn remove_key_id_meta(&self, key_id: u64) {
    self
      .key_meta
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .remove(&key_id);
  }

  fn is_meta_key(&self, _key: &[u8]) -> bool {
    false
  }

  fn parse_meta_value(&self, _val: &[u8]) -> Option<wcompact::CompactMetaInfo> {
    None
  }

  fn is_stale_subkey(&self, _key: &[u8]) -> bool {
    false
  }

  async fn is_expired_or_orphan_record(
    &self,
    session: &Self::Session,
    key: &[u8],
    val: &[u8],
    now: i64,
  ) -> bool {
    if let Some(user_key) = key.strip_prefix(b"ttl:") {
      // 1. 若 TTL 自身已到期：直接判死
      if let Ok(be) = <[u8; 8]>::try_from(val) {
        let exp = i64::from_be_bytes(be);
        if exp <= now {
          return true;
        }
      }
      // 2. 检查宿主 String 键是否存在
      let _guard = session.enter_epoch();
      let str_k = str_key(user_key);
      self.index.find_tag(&str_k).is_none()
    } else if let Some(user_key) = key.strip_prefix(b"str:") {
      let ttl_k = ttl_key(user_key);
      if let Ok(Some(exp)) = session.read_ttl_expiry(&ttl_k).await {
        exp <= now
      } else {
        false
      }
    } else {
      false
    }
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
