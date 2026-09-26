//! store 测试二进制公共支撑：临时目录环境、配置快捷构造与补零工具

use std::{
  iter::repeat_n,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  thread::yield_now,
};

use aok::Result;
use itoa::Buffer;
use tempfile::{TempDir, tempdir};
use wbase::addr::is_read_cache;
use wdev::SegmentedDevice;
use windex::{HashBucketEntry, HashIndex, SPLIT_UNSTARTED, chunk_count, chunk_offset_for_hash};
use wkv::{RcVisit, StoreConfig, WedbStore, store::ResizePhase};
use wval::{KeyTag, NamespaceDbCodec};

/// 已从 windex 生产导出面收敛掉的索引便捷口，测试支撑层按生产入口等价复现：
/// 写入一律经唯一免查重追加口 [`HashIndex::insert_to_bucket`]，候选读出仍为
/// 零拷贝 [`HashIndex::lookup_candidates`]（`lookup_vec` 仅供断言做集合比较）
pub trait HashIndexTestOps {
  /// 免查重追加键对应逻辑地址（等价历史 `HashIndex::insert`）
  fn insert(&self, key: &[u8], address: u64) -> windex::Result<()>;

  /// 免查重追加指定哈希的逻辑地址（等价历史 `HashIndex::insert_by_hash`）
  fn insert_by_hash(&self, hash: u64, address: u64) -> windex::Result<()>;

  /// 候选地址 Vec 化读出（等价历史 `HashIndex::lookup`，仅测试断言用）
  fn lookup_vec(&self, key: &[u8]) -> Vec<u64>;
}

impl HashIndexTestOps for HashIndex {
  #[inline]
  fn insert_by_hash(&self, hash: u64, address: u64) -> windex::Result<()> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    self.insert_to_bucket(self.bucket_index_for_hash(hash), tag, address)
  }

  #[inline]
  fn insert(&self, key: &[u8], address: u64) -> windex::Result<()> {
    self.insert_by_hash(HashIndex::hash_key(key), address)
  }

  #[inline]
  fn lookup_vec(&self, key: &[u8]) -> Vec<u64> {
    self.lookup_candidates(key).to_vec()
  }
}

/// 临时目录保活与存储实例绑定环境
pub struct TestEnv {
  /// 保活临时目录（Drop 时自动清理数据文件）
  pub _dir: TempDir,
  /// 存储引擎实例
  pub store: Arc<WedbStore<SegmentedDevice>>,
}

/// 快捷构造默认可变占比 0.5 的测试配置
pub fn config(index_size: usize, page_size: usize, num_pages: usize) -> Result<StoreConfig> {
  Ok(StoreConfig::new(index_size, page_size, num_pages, 0.5)?)
}

/// 在指定临时目录中打开存储实例（目录与数据文件路径由调用方持有，便于
/// 检查点目录挂载与崩溃后重开同一路径）
pub fn open_store_in(
  dir: &TempDir,
  name: &str,
  config: StoreConfig,
) -> Result<Arc<WedbStore<SegmentedDevice>>> {
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  Ok(Arc::new(WedbStore::open(config, device)?))
}

/// 在独立临时目录中打开存储实例（目录随 [`TestEnv`] 存活，Drop 自动清理）
pub fn open_store(name: &str, config: StoreConfig) -> Result<TestEnv> {
  let dir = tempdir()?;
  let store = open_store_in(&dir, name, config)?;
  Ok(TestEnv { _dir: dir, store })
}

/// 指定地址当前是否作为空闲槽位存在于复活池分桶（复活/脱钩归池回归共用足印内省口）
pub fn slot_in_pool(store: &WedbStore<SegmentedDevice>, addr: u64) -> bool {
  store
    .reviv_pool
    .bins
    .iter()
    .flat_map(|bin| bin.slots.iter())
    .any(|slot| !slot.is_empty() && slot.address() == addr)
}

/// 十进制补零到 width 位
pub fn pad(v: impl itoa::Integer, width: usize) -> String {
  let mut buf = Buffer::new();
  let digits = buf.format(v);
  let mut s = String::with_capacity(width.max(digits.len()));
  s.extend(repeat_n('0', width.saturating_sub(digits.len())));
  s.push_str(digits);
  s
}

/// 树身份键构造（测试域 `(vns, vdb)` 的物理 Meta 键形态，与 wkv
/// `range_index::tree_identity_key` / 会话 `session_meta_key` 同一编码内核）：
/// 树注册、claim 表、数据文件名全部按该键派生，跨库同名键按物理域隔离
pub fn tree_id_key(vns: u64, vdb: u64, user_key: &[u8]) -> wval::TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(vns, vdb, KeyTag::Meta, user_key)
}

/// 自哈希桶入口项顺 ReadCache 前缀走链比对键（严格对标 C#
/// libs/storage/Tsavorite/cs/test/test.session/ReadCacheChainTests.cs:183
/// FindRecordInReadCache：`while (isReadCache)` 命中键即真、脱离 RC 区即假）
///
/// 走查触及滑窗/不可判读记录返回 None（C# 无该竞态窗，本端不为降级留口径），
/// 调用方以 `Some(..)` 精确匹配拒绝假 NOTFOUND 折叠。
pub fn read_cache_holds_key(store: &WedbStore<SegmentedDevice>, phys_key: &[u8]) -> Option<bool> {
  let mut addr = store.index.load().find_tag(phys_key)?;
  loop {
    if !is_read_cache(addr) {
      return Some(false);
    }
    match store.read_cache.with_record(addr, |k, _| Some(k.to_vec())) {
      RcVisit::Found(k) => {
        if k == phys_key {
          return Some(true);
        }
      }
      // 已作废（closed）记录不比对键、携 prev 续链（对标 C# 走查跳过 Invalid）
      RcVisit::Next(prev) => {
        addr = prev;
        continue;
      }
      RcVisit::Gone => return None,
    }
    addr = store.read_cache.prev_address_of(addr)?;
  }
}

/// 装配在线扩容迁移窗口（确定性，不依赖时序竞争）：发布分块迁移状态位图与
/// 迁移源旧表；`publish` 决定是否先行切上 2 倍容量新表（false 装配
/// 「相位已发布而表未切」过渡窗，true 装配「新表在场、分块未迁移」迁移窗）
pub fn stage_resize(store: &WedbStore<SegmentedDevice>, publish: bool, phase: ResizePhase) {
  let old_index = store.active_index();
  let count = chunk_count(old_index.size);
  store.resize.split_status.store(Arc::new(
    (0..count)
      .map(|_| AtomicI64::new(SPLIT_UNSTARTED))
      .collect(),
  ));
  store
    .resize
    .num_pending_chunks
    .store(count, Ordering::Release);
  store.resize.old_index.store(Some(Arc::clone(&old_index)));
  if publish {
    store
      .index
      .store(Arc::new(HashIndex::new(old_index.size * 2).unwrap()));
  }
  store.resize.phase.store(phase as u8, Ordering::Release);
}

/// 确定性收尾迁移窗口（严格复刻 grow_index 步 4/5 的合法完成序）：全量分块
/// 迁移至 pending 归零，再注销迁移源并翻回 Rest 相位
pub fn finish_resize_window(store: &WedbStore<SegmentedDevice>) {
  let old_index = store
    .resize
    .old_index
    .load_full()
    .expect("装配窗口必有迁移源");
  let count = chunk_count(old_index.size);
  for i in 0..count {
    store.split_single_chunk(i, count, &old_index).unwrap();
  }
  while store.resize.num_pending_chunks.load(Ordering::Acquire) > 0 {
    yield_now();
  }
  store.resize.old_index.store(None);
  store.resize.split_status.store(Arc::new(Vec::new()));
  store
    .resize
    .phase
    .store(ResizePhase::Rest as u8, Ordering::Release);
}

/// 指定键当前所落旧表分块的迁移状态（SPLIT_UNSTARTED / IN_PROGRESS / COMPLETED）
///（物理记录键域用：裸键 `HashIndex::hash_key` 口径）
pub fn split_status_of(store: &WedbStore<SegmentedDevice>, key: &[u8]) -> i64 {
  split_status_of_hash(store, HashIndex::hash_key(key))
}

/// [`split_status_of`] 的哈希入参对位：rmw 窗用户键 scoped 口径寻桶
///（`whasher::scoped_hash` 前缀种子），协同分块与闩桶必须同 hash 单源
pub fn split_status_of_hash(store: &WedbStore<SegmentedDevice>, hash: u64) -> i64 {
  let old_index = store
    .resize
    .old_index
    .load_full()
    .expect("装配窗口必有迁移源");
  let count = chunk_count(old_index.size);
  let chunk = chunk_offset_for_hash(hash, old_index.mask) & (count - 1);
  store.resize.split_status.load()[chunk].load(Ordering::Acquire)
}
