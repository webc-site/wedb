//! 混合日志在线紧缩器集成（对标 C# Tsavorite Compaction API）

use std::sync::Arc;

use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexStub};
use wcompact::{
  CompactMetaInfo, CompactSession, CompactStore, CompactionStats, CompactionType, LogCompactor,
};
use wdev::Device;
use whlog::HybridLog;
use windex::HashIndex;
use wval::{
  CollectionType, KeyTag, META_VALUE_SIZE, MetaValue, NamespaceDbCodec, StorageEncoding, TtlCodec,
};

use crate::{
  error::{Error, Result},
  read_cache::is_read_cache_addr,
  session::StoreSession,
  store::WedbStore,
  ttl::TTL_VALUE_LEN,
};

/// 宿主引擎错误 → 紧缩域错误的类型化重映射（穷尽匹配，无字符串化降级）
///
/// 紧缩路径真实可达的底层故障（纪元注册、混合日志、记录编解码）按类型透明转发；
/// 其余宿主变体在紧缩路径不可达，统一落入 [`wcompact::Error::Host`] 哨兵承接——
/// 触发即宿主错误面与紧缩契约失配，需同步扩展此映射
impl From<Error> for wcompact::Error {
  fn from(e: Error) -> Self {
    match e {
      Error::Epoch(x) => Self::Epoch(x),
      Error::HLog(x) => Self::Hlog(x),
      Error::Record(x) => Self::Record(x),
      Error::Index(x) => Self::Index(x),
      Error::Device(x) => Self::Device(x),
      other => Self::Host(other.to_string()),
    }
  }
}

impl<D: Device> CompactSession<D> for StoreSession<D> {
  type EpochGuard<'a>
    = wepoch::EpochGuard<'a>
  where
    Self: 'a;

  #[inline]
  fn enter_epoch(&self) -> Self::EpochGuard<'_> {
    self.participant.enter()
  }

  /// libs/server/Storage/Functions/GarnetRecordTriggers.cs:PostCopyToTail
  #[inline]
  async fn append_record(
    &self,
    key: &[u8],
    val: &[u8],
    expected_main_addr: u64,
    is_tombstone: bool,
  ) -> wcompact::Result<u64> {
    // 紧缩搬迁存根自愈 (1:1 对标 GarnetRecordTriggers.cs:PostCopyToTail)：
    // 若搬迁记录为 RangeIndex 存根元记录且带 is_flushed=true，
    // 搬迁至可变尾区的新记录必须清除 Flushed 标志，避免后续读取误判为冷数据触发多余晋升循环；
    // 且当冷存根源在磁盘上时，预置 data.bftree (对标 C# PreStageAndRegisterPending)
    let mut healed_buf = [0u8; 128];
    let actual_val = if !is_tombstone
      && val.len() >= META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE
      && let Ok(meta) = MetaValue::from_slice(val)
      && (meta.collection_type == CollectionType::RangeIndex
        || (meta.encoding() == StorageEncoding::FlattenedTree && meta.size > 0))
      && let Ok(mut stub) =
        RangeIndexStub::decode(&val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])
      && stub.is_flushed()
    {
      if let Some(user_key) = NamespaceDbCodec::decode_meta_user_key(key)
        && expected_main_addr != 0
        && let Err(err) = self
          .store
          .range_index
          .pre_stage_and_register_pending(user_key, expected_main_addr)
      {
        log::warn!("压缩迁移预存 RangeIndex 失败: {err}");
      }
      stub.set_flushed(false);
      let len = val.len().min(128);
      healed_buf[..len].copy_from_slice(&val[..len]);
      healed_buf[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE]
        .copy_from_slice(&stub.encode());
      &healed_buf[..len]
    } else {
      val
    };

    // 紧缩搬迁旁路写监听：AOF 只记原始写效果，物理搬迁帧入 AOF 会导致恢复回退
    self
      .append_record_compacted(key, actual_val, expected_main_addr, is_tombstone)
      .await
      .map_err(wcompact::Error::from)
  }

  #[inline]
  async fn read_ttl_expiry(&self, ttl_key: &[u8]) -> wcompact::Result<Option<i64>> {
    let mem_exp = {
      let _guard = self.participant.enter();
      let Some(first_addr) = self.store.index.find_tag(ttl_key) else {
        return Ok(None);
      };
      self
        .try_read_raw_in_memory_with_addr(ttl_key, Some(first_addr), |v| {
          <[u8; TTL_VALUE_LEN]>::try_from(v)
            .ok()
            .map(i64::from_be_bytes)
        })
        .map_err(wcompact::Error::from)?
    };
    match mem_exp {
      Some(Some(Some(exp))) => Ok(Some(exp)),
      Some(None) | Some(Some(None)) => Ok(None),
      None => {
        if let Some(raw_ttl) = self
          .read_raw(ttl_key)
          .await
          .map_err(wcompact::Error::from)?
          && let Ok(be) = <[u8; TTL_VALUE_LEN]>::try_from(raw_ttl.as_slice())
        {
          return Ok(Some(i64::from_be_bytes(be)));
        }
        Ok(None)
      }
    }
  }
}

impl<D: Device> CompactStore for WedbStore<D> {
  type Device = D;
  type Session = StoreSession<D>;

  #[inline]
  fn new_session(self: &Arc<Self>) -> wcompact::Result<Self::Session> {
    self.new_session().map_err(wcompact::Error::from)
  }

  #[inline]
  fn hlog(&self) -> &HybridLog<D> {
    &self.hlog
  }

  #[inline]
  fn index(&self) -> &HashIndex {
    &self.index
  }

  #[inline]
  fn read_only_address(&self) -> u64 {
    self.hlog.read_only_address()
  }

  #[inline]
  fn begin_address(&self) -> u64 {
    self.hlog.begin_address()
  }

  #[inline]
  async fn shift_begin_address(&self, until: u64) -> wcompact::Result<()> {
    self
      .shift_begin_address(until)
      .await
      .map_err(wcompact::Error::from)
  }

  #[inline]
  fn is_read_cache_addr(&self, addr: u64) -> bool {
    is_read_cache_addr(addr)
  }

  #[inline]
  fn skip_read_cache(&self, addr: u64) -> u64 {
    if self.read_cache.is_enabled {
      self.read_cache.skip_read_cache(addr)
    } else {
      0
    }
  }

  #[inline]
  fn enable_revivification(&self) -> bool {
    self.config.enable_revivification
  }

  #[inline]
  fn reviv_put(&self, addr: u64, size: u32, read_only_addr: u64) {
    self.reviv_pool.put(addr, size, read_only_addr);
  }

  #[inline]
  fn get_key_id_meta(&self, key_id: u64) -> Option<(u64, bool)> {
    self.get_key_id_meta(key_id)
  }

  #[inline]
  fn update_key_id_meta(&self, key_id: u64, version: u64, is_alive: bool) {
    self.update_key_id_meta(key_id, version, is_alive);
  }

  #[inline]
  fn remove_key_id_meta(&self, key_id: u64) {
    self.remove_key_id_meta(key_id);
  }

  #[inline]
  fn is_meta_key(&self, key: &[u8]) -> bool {
    matches!(NamespaceDbCodec::decode_tag(key), Some(KeyTag::Meta))
  }

  #[inline]
  fn parse_meta_value(&self, val: &[u8]) -> Option<CompactMetaInfo> {
    if val.len() < META_VALUE_SIZE {
      return None;
    }
    let meta = MetaValue::from_slice(val).ok()?;
    Some(CompactMetaInfo {
      key_id: meta.key_id,
      version: meta.version,
      size: meta.size,
    })
  }

  #[inline]
  fn is_stale_subkey(&self, key: &[u8]) -> bool {
    if let Some((_, key_id, sub_version)) = NamespaceDbCodec::decode_subkey_id_version(key) {
      match self.get_key_id_meta(key_id) {
        None => false,
        Some((current_version, is_alive)) => !is_alive || current_version > sub_version,
      }
    } else {
      false
    }
  }

  async fn is_expired_or_orphan_record(
    &self,
    session: &Self::Session,
    key: &[u8],
    val: &[u8],
    now: i64,
  ) -> bool {
    let Ok((_, _, tag, user_key)) = NamespaceDbCodec::decode_tagged_key(key) else {
      return false;
    };
    let Some(tag_offset) = key.len().checked_sub(user_key.len() + 1) else {
      return false;
    };

    match tag {
      KeyTag::Ttl => {
        // 1. 若 TTL 自身已到期：直接判死（严格小于，读路径口径）
        if let Some(exp) = TtlCodec::decode(val)
          && exp < now
        {
          return true;
        }
        // 2. 若 TTL 尚未到期，短路检查其宿主主键在哈希索引中是否存在（优先探查绝大多数场景存在的 String 键）
        let _guard = session.enter_epoch();
        let str_k = NamespaceDbCodec::replace_tag_at(key, tag_offset, KeyTag::String);
        if self.index.find_tag(&str_k).is_some() {
          return false;
        }
        let meta_k = NamespaceDbCodec::replace_tag_at(key, tag_offset, KeyTag::Meta);
        // 宿主既无 String 也无 Meta：属于已被删除的主键留下的孤儿 TTL 记录，直接判死丢弃！
        self.index.find_tag(&meta_k).is_none()
      }
      KeyTag::String | KeyTag::Meta => {
        // 检查该数据记录是否附带 TTL 且已过期
        let ttl_k = NamespaceDbCodec::replace_tag_at(key, tag_offset, KeyTag::Ttl);
        if let Ok(Some(exp)) = session.read_ttl_expiry(&ttl_k).await {
          exp < now
        } else {
          false
        }
      }
      _ => false,
    }
  }
}

impl<D: Device> WedbStore<D> {
  /// 创建绑定的日志在线紧缩器实例
  #[inline]
  pub fn compactor(self: &Arc<Self>) -> LogCompactor<Self> {
    LogCompactor::new(Arc::clone(self))
  }

  /// 执行混合日志在线紧缩（对标 C# Tsavorite `TsavoriteKV.Compact`）
  #[inline]
  pub async fn compact(
    self: &Arc<Self>,
    until_address: u64,
    comp_type: CompactionType,
  ) -> Result<CompactionStats> {
    self
      .compactor()
      .compact(until_address, comp_type)
      .await
      .map_err(Error::from)
  }

  /// 执行带自定义判定谓词的在线紧缩（对标 C# Tsavorite `Compact` 带 `ICompactionFunctions`）
  #[inline]
  pub async fn compact_with_filter<F>(
    self: &Arc<Self>,
    until_address: u64,
    comp_type: CompactionType,
    is_deleted: F,
  ) -> Result<CompactionStats>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    self
      .compactor()
      .compact_with_filter(until_address, comp_type, is_deleted)
      .await
      .map_err(Error::from)
  }

  /// 执行周期惰性紧缩（对标 Garnet 周期紧缩任务语义）
  #[inline]
  pub async fn compact_lazy(self: &Arc<Self>, max_seek_bytes: u64) -> Result<CompactionStats> {
    self
      .compactor()
      .compact_lazy(max_seek_bytes)
      .await
      .map_err(Error::from)
  }
}
