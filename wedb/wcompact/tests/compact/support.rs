//! wcompact 集成测试公共 fixture：以 whlog + windex 搭建最小宿主存储引擎，
//! 并提供确定性竞争注入点（在紧缩迁移的追加路径上单次触发，模拟宿主并发槽位改写）

use std::{
  convert::Infallible,
  mem,
  path::Path,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use aok::Result as AokResult;
use wcompact::{CompactSession, CompactStore, CompactionFunctions, Result as CompactResult};
use wdev::SegmentedDevice;
use wepoch::{EpochGuard, EpochSuspendGuard, LightEpoch, Participant};
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

/// grow 回迁追加（收窗与会话协同共用，走 fixture 唯一免查重口）
fn stash_insert(index: &HashIndex, key: &[u8], addr: u64) {
  index.insert(key, addr).expect("grow 回迁追加失败");
}

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
  /// 搬迁源转出通知 (key, src_addr)：CAS 成功臂的宿主钩子调用记录，
  /// 供「Copied 必通知 / Superseded 零通知」断言（生产对位见 wkv
  /// `CompactSession::transfer_out_source`）
  pub transfer_outs: Mutex<Vec<(Vec<u8>, u64)>>,
  inject: Mutex<Inject>,
  /// 模拟正被换页驱逐的 ReadCache 槽位地址（0 = 无驱逐）：置位期间
  /// `skip_read_cache_with_wait` 走查触到该地址即滑窗不可判读、就地等待，
  /// 自旋至清洗方收口
  evicting_slot: AtomicU64,
  /// 驱逐等待协议命中次数（供测试断言探针确曾等待）
  evict_waits: AtomicU32,
  /// 扩容迁移窗装配标记（真 IN_PROGRESS_GROW 的 fixture 对位形态：置位期间
  /// 被摘存键在活跃表探得空候选，须会话 `ensure_split` 协同迁移后方可再探）
  growing: AtomicBool,
  /// 未迁分块条目暂存表（对位旧表独占条目：`ensure_split` 按键协同回迁，
  /// 收窗一次性全量回迁）
  grow_stash: Mutex<Vec<(Vec<u8>, Vec<u64>)>>,
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
      transfer_outs: Mutex::new(Vec::new()),
      inject: Mutex::new(Inject::None),
      evicting_slot: AtomicU64::new(0),
      evict_waits: AtomicU32::new(0),
      growing: AtomicBool::new(false),
      grow_stash: Mutex::new(Vec::new()),
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

  /// 搬迁源转出通知快照（供 Copied/Superseded 通知断言）
  pub fn transfer_outs_snapshot(&self) -> Vec<(Vec<u8>, u64)> {
    self
      .transfer_outs
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
        let bucket = self.index.bucket(self.index.bucket_index_for_key(&key));
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

  /// 模拟指定键当前索引槽位进入 ReadCache 换页驱逐过渡态（对标驱逐方推进
  /// head 后、cleanse_page 尚未收口的窗口）：槽位原地址携 ReadCache 指示位
  /// 且登记为滑窗不可判读——探针 `skip_read_cache_with_wait` 触到该地址即
  /// 就地自旋，直至 [`Self::cleanse_read_cache_eviction`] 发布清洗完成。
  /// 返回打标后的槽位地址。
  pub fn arm_read_cache_eviction(&self, key: &[u8]) -> Option<u64> {
    let main = self.index.find_tag(key)?;
    let bucket = self.index.bucket(self.index.bucket_index_for_key(key));
    for slot in &bucket.entries[..HashBucket::DATA_ENTRIES] {
      let cur = slot.load(Ordering::Acquire);
      if cur & HashBucketEntry::ADDRESS_MASK == main {
        // 槽位字面量原位置 ReadCache 位（Tag 高位不动，RC 位属低 48 位地址域）
        assert!(
          slot
            .compare_exchange(
              cur,
              cur | HashBucketEntry::READ_CACHE_BIT,
              Ordering::AcqRel,
              Ordering::Acquire,
            )
            .is_ok(),
          "注入失败: ReadCache 打标槽位 CAS 未生效 key={key:?}"
        );
        let rc = (cur | HashBucketEntry::READ_CACHE_BIT) & HashBucketEntry::ADDRESS_MASK;
        self.evicting_slot.store(rc, Ordering::Release);
        return Some(rc);
      }
    }
    None
  }

  /// 驱逐方页清洗收口（对标 wkv `read_cache/cleanse.rs:cleanse_page` 的哈希链
  /// 恢复）：把槽位自 ReadCache 虚拟地址原子换指主日志物理地址，发布清洗完成
  /// 解除等待端口；槽位已被摘除（探针误删的故障形态）时 CAS 落败，返回 false
  pub fn cleanse_read_cache_eviction(&self, key: &[u8], rc_addr: u64) -> bool {
    let restored = self
      .index
      .update_address(key, rc_addr, self.resolve_main(rc_addr));
    self.evicting_slot.store(0, Ordering::Release);
    restored
  }

  /// 驱逐等待协议命中次数（供测试断言探针确曾进入等待而非直接判死）
  pub fn evict_waits(&self) -> u32 {
    self.evict_waits.load(Ordering::Relaxed)
  }

  // ===================== 扩容迁移窗装配（grow 协同回归专用） =====================

  /// 装配 grow 迁移窗：把指定键的索引条目摘入未迁暂存（活跃表此后对这些键探得
  /// 空候选），外部形态与真 IN_PROGRESS_GROW 未迁分块键一致——探针未过协同门
  /// 即空候选直判 superseded 弃迁活键（本票危害链的 fixture 复现面）
  pub fn stage_grow_window(&self, keys: &[&[u8]]) {
    let mut stash = self.grow_stash.lock().unwrap_or_else(|e| e.into_inner());
    for key in keys {
      let addrs = self.index.lookup_candidates(key).to_vec();
      for addr in &addrs {
        assert!(
          self.index.delete(key, *addr),
          "扩容窗装配失败: 无法摘除 {key:?} 的条目 {addr:#x}"
        );
      }
      stash.push(((*key).to_vec(), addrs));
    }
    self.growing.store(true, Ordering::Release);
  }

  /// 会话侧协同迁移（对位 split_buckets 在迁移窗内对本键所在分块的迁移效果，
  /// fixture 分块粒度收口到单键）：仅扩容期生效
  pub(crate) fn grow_migrate(&self, key: &[u8]) {
    if !self.growing.load(Ordering::Acquire) {
      return;
    }
    let mut stash = self.grow_stash.lock().unwrap_or_else(|e| e.into_inner());
    let Some(pos) = stash.iter().position(|(k, _)| k == key) else {
      return;
    };
    let (_, addrs) = stash.remove(pos);
    for addr in addrs {
      stash_insert(&self.index, key, addr);
    }
  }

  /// 收窗：一次性全量回迁幸存暂存条目并翻回 Rest（对位 finish_resize_window
  /// 的确定性收口，注入钩体内调用）
  pub fn finish_grow_window(&self) {
    self.growing.store(false, Ordering::Release);
    let stash: Vec<(Vec<u8>, Vec<u64>)> = {
      let mut guard = self.grow_stash.lock().unwrap_or_else(|e| e.into_inner());
      mem::take(&mut *guard)
    };
    for (key, addrs) in stash {
      for addr in addrs {
        stash_insert(&self.index, &key, addr);
      }
    }
  }

  /// 封印只读边界至 until（同步排空：safe_ro 达 until 才返回，对标 C#
  /// BumpCurrentEpoch 动作排空后 SafeReadOnlyAddress 生效的语义；单线程无保护
  /// 窗口下动作在注册线程内联收割，循环通常零迭代）
  pub fn seal_read_only(&self, until: u64) {
    self.hlog.shift_read_only_address(until);
    while self.hlog.safe_read_only_address() < until {
      self.epoch.bump_current_epoch();
      self.epoch.drain();
      thread::yield_now();
    }
  }

  /// 写入 String 物理键（新键插入索引，覆盖键 CAS 挂载，CAS 失败取新槽位重试）
  pub async fn put(&self, s: &FixtureSession, key: &[u8], val: &[u8]) -> CompactResult<u64> {
    loop {
      let old = {
        let _guard = s.participant.enter();
        self.index.find_tag(key)
      };
      let prev = old.map_or(0, |slot| self.resolve_main(slot));
      let addr = s.append_pure(key, val, prev, false).await?;
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
    let addr = s.append_pure(key, &[], prev, true).await?;
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
    Ok(self.hlog.append(key, val, 0, false)?.0)
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

impl FixtureSession {
  /// 预置数据纯追加（不走 trait 分配契约：mock 无复活池，PageNotReady 驱逐后重试）
  pub(crate) async fn append_pure(
    &self,
    key: &[u8],
    val: &[u8],
    prev: u64,
    is_tombstone: bool,
  ) -> CompactResult<u64> {
    loop {
      let attempt = {
        let _guard = self.participant.enter();
        self.store.hlog.append(key, val, prev, is_tombstone)
      };
      match attempt {
        Ok((addr, _)) => return Ok(addr),
        Err(Error::PageNotReady(page_id)) => self.store.evict_page(self, page_id).await?,
        Err(e) => return Err(e.into()),
      }
    }
  }
}

impl CompactSession<SegmentedDevice> for FixtureSession {
  type EpochGuard<'a>
    = EpochGuard<'a>
  where
    Self: 'a;

  fn enter_epoch(&self) -> Self::EpochGuard<'_> {
    self.participant.enter()
  }

  /// 探针前分裂协同门：扩容窗内对本键暂存条目执行协同回迁（生产对位见 wkv
  /// `CompactSession::ensure_split` → `StoreSession::ensure_split_by_hash` 单点）
  fn ensure_split(&self, key: &[u8]) -> CompactResult<()> {
    self.store.grow_migrate(key);
    Ok(())
  }

  async fn allocate_record(
    &self,
    key: &[u8],
    val: &[u8],
    expected_main_addr: u64,
    is_tombstone: bool,
    _chain_head: u64,
  ) -> CompactResult<(u64, u32)> {
    // 确定性竞争注入点：紧缩迁移追加前模拟宿主并发槽位改写
    // （测试 mock 无复活池，恒纯尾部追加，足印为对齐逻辑尺寸）
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
        Ok((addr, _)) => {
          return Ok((addr, wrecord::record_size(key.len(), val.len()) as u32));
        }
        Err(Error::PageNotReady(page_id)) => {
          self.store.evict_page(self, page_id).await?;
        }
        Err(e) => return Err(e.into()),
      }
    }
  }

  async fn read_record_at(&self, addr: u64) -> CompactResult<whlog::RecordOutput> {
    if self.store.hlog.is_on_disk(addr) {
      self
        .store
        .hlog
        .read_disk_record(addr)
        .await
        .map_err(Into::into)
    } else {
      let _guard = self.participant.enter();
      self.store.hlog.read_record(addr).await.map_err(Into::into)
    }
  }

  /// mock 无分层存根语义，源存根所有权转出零处置，仅记录通知供断言
  /// （生产对位见 wkv `CompactSession::transfer_out_source`）
  fn transfer_out_source(&self, key: &[u8], _val: &[u8], src_addr: u64) {
    self
      .store
      .transfer_outs
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .push((key.to_vec(), src_addr));
  }
}

impl FixtureStore {
  /// 补刷并驱逐环形槽位上的旧页（简化版宿主 evict 路径，对齐 wkv
  /// `StoreSession::evict_pages_for` 的已落地纪元纪律）：刷盘与等待 safe_head
  /// 排空期间经 `EpochSuspendGuard` 按重入深度整体挂起本会话保护区——紧缩迁移
  /// `conditional_copy_to_tail` 的外层会话守卫横跨追加重试 await，若不挂起，
  /// 本线程自钉旧纪元将令排空永不可达（PageNotReady 自旋活锁）
  async fn evict_page(&self, session: &FixtureSession, page_id: u64) -> CompactResult<()> {
    let num_pages = self.hlog.config.num_pages as u64;
    if page_id < num_pages {
      return Ok(());
    }
    let old_page = page_id - num_pages;
    let _suspend = EpochSuspendGuard::new(&session.participant);
    self.hlog.flush_page(old_page).await?;
    self.device.sync().await?;
    let min_evicted = self.hlog.config.page_start_address(old_page + 1);
    self.hlog.shift_read_only_address(min_evicted);
    self.hlog.shift_head_address(min_evicted);
    let target = min_evicted.min(self.hlog.head_address());
    if target > self.hlog.safe_head_address() {
      self.hlog.wait_safe_head_drained(target).await;
    }
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

  fn index(&self) -> Arc<HashIndex> {
    Arc::clone(&self.index)
  }

  /// 扩容迁移窗是否装配中（探针注入钩守卫消费，见 grow_window.rs 回归）
  fn is_growing(&self) -> bool {
    self.growing.load(Ordering::Acquire)
  }

  fn safe_read_only_address(&self) -> u64 {
    self.hlog.safe_read_only_address()
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

  fn skip_read_cache_with_wait(&self, key: &[u8], head: u64, session: &FixtureSession) -> u64 {
    // 与 wkv::ReadCache::skip_read_cache_with_wait 同契约的 fixture 复现：每步先判
    // 当前位置是否滑窗（登记在案的驱逐槽位不可判读），命中即以该地址自旋等待清洗
    // 落定，再回链头**重读本键哈希项**重探（对标 C# UpdateRecordSourceToCurrentHashEntry），
    // 循环直至解析落定；其余打标槽位视作窗口内可走查，直剥指示位回主日志地址
    let mut addr = head;
    loop {
      let mut restart = false;
      while self.is_read_cache_addr(addr) {
        if addr != 0 && self.evicting_slot.load(Ordering::Acquire) == addr {
          self.evict_waits.fetch_add(1, Ordering::Relaxed);
          while self.evicting_slot.load(Ordering::Acquire) != 0 {
            session.participant.refresh();
            thread::yield_now();
          }
          restart = true;
          break;
        }
        addr &= !HashBucketEntry::READ_CACHE_BIT;
      }
      if !restart {
        return addr;
      }
      // 重读本键哈希项取清洗后的新链头（fixture 单代链：槽位即哈希项当前值）
      addr = self.index.find_tag(key).unwrap_or(0) & HashBucketEntry::ADDRESS_MASK;
    }
  }

  fn enable_revivification(&self) -> bool {
    true
  }

  fn reviv_put(&self, addr: u64, size: u32) {
    self
      .reviv_puts
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .push((addr, size));
  }
}

/// fixture 紧缩业务过滤谓词（ttl:/str: 前缀键的业务判定，模拟宿主注入位，
/// 对标 wkv WedbCompactionFunctions 在生产路径的角色）
pub(crate) struct FixtureCompactionFunctions;

/// 读取 TTL 物理键的到期戳（None = 不存在或已墓碑）
async fn read_ttl_expiry(session: &FixtureSession, ttl_k: &[u8]) -> Option<i64> {
  let slot = {
    let _guard = session.enter_epoch();
    session.store.index.find_tag(ttl_k)
  }?;
  let rec = session
    .store
    .hlog
    .read_record(session.store.resolve_main(slot))
    .await
    .ok()?;
  if !rec.key().is_ok_and(|k| k == ttl_k) || rec.is_tombstone().unwrap_or(true) {
    return None;
  }
  <[u8; 8]>::try_from(rec.value().ok()?)
    .ok()
    .map(i64::from_be_bytes)
}

impl CompactionFunctions<FixtureStore> for FixtureCompactionFunctions {
  type Error = Infallible;

  async fn is_deleted(&self, session: &FixtureSession, key: &[u8], val: &[u8], now: i64) -> bool {
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
      session.store.index.find_tag(&str_k).is_none()
    } else if let Some(user_key) = key.strip_prefix(b"str:") {
      // 数据记录：单次 TTL 记录读取裁决附带 TTL 是否已过期
      let ttl_k = ttl_key(user_key);
      read_ttl_expiry(session, &ttl_k)
        .await
        .is_some_and(|exp| exp <= now)
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
