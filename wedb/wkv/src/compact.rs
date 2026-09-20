//! 混合日志在线紧缩器集成（对标 C# Tsavorite Compaction API）

use std::sync::Arc;

use wbase::addr::is_read_cache;
use wcompact::{
  self, CompactSession, CompactStore, CompactionStats, CompactionType, Error as WcompactError,
  LogCompactor,
};
use wdev::Device;
use whlog::HybridLog;
use windex::HashIndex;
use wval::{KeyTag, NamespaceDbCodec};

use crate::{
  error::{Error, Result},
  range_index::{clear_flushed_patch, patch_stub_record, range_index_stub_of},
  session::StoreSession,
  store::WedbStore,
  ttl::is_expired,
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
    self.enter_gated()
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
    // 紧缩搬迁存根自愈 (1:1 对标 GarnetRecordTriggers.cs:PostCopyToTail 的
    // ClearFlushedFlag(dstSpan))：搬迁记录为 RangeIndex 存根元记录且带
    // is_flushed=true 时，尾部新记录必须清除 Flushed 位，避免后续读取误判为冷数据
    // 触发多余晋升循环；冷存根源在磁盘上时预置 data.bftree
    // (对标 C# PreStageAndRegisterPending)。
    // 治愈一律转调 wkv 唯一内核 crate::range_index::patch_stub_record：只改 35B 存根
    // 窗口、值体长度无上限（对标 C# 就地改 Span + TryCopyToTail 分配新尾记录），
    // 杜绝旧实现在此的 [u8;128] 私有副本与 min(128) 静默截断丢尾部扩展字段。
    // 探针先行：非 Flushed 记录（紧缩热路径绝大多数）零分配直过。
    // 预置登记的树身份 = 物理 Meta 键（decode_meta_user_key 仅为 Meta 域过滤，
    // 整物理键直传，零解码换键——身份含域与树注册同源）
    let healed: Option<Vec<u8>> =
      if is_tombstone || !range_index_stub_of(val).is_some_and(|stub| stub.is_flushed()) {
        None
      } else {
        if NamespaceDbCodec::decode_meta_user_key(key).is_some()
          && expected_main_addr != 0
          && let Err(err) = self
            .store
            .range_index
            .pre_stage_and_register_pending(key, expected_main_addr)
        {
          log::warn!("压缩迁移预存 RangeIndex 失败: {err}");
        }
        let mut frame = val.to_vec();
        patch_stub_record(&mut frame, clear_flushed_patch);
        Some(frame)
      };

    // 紧缩搬迁旁路写监听：AOF 只记原始写效果，物理搬迁帧入 AOF 会导致恢复回退
    self
      .append_record_compacted(
        key,
        healed.as_deref().unwrap_or(val),
        expected_main_addr,
        is_tombstone,
      )
      .await
      .map_err(WcompactError::from)
  }

  /// 端口体单点：直接转调内核 [`StoreSession::read_record`]，冷读分派与纪元纪律
  /// 只在内核一处维持，紧缩面不复述
  #[inline]
  async fn read_record_at(&self, addr: u64) -> wcompact::Result<whlog::RecordOutput> {
    self.read_record(addr).await.map_err(WcompactError::from)
  }
}

impl<D: Device> CompactStore for WedbStore<D> {
  type Device = D;
  type Session = StoreSession<D>;

  #[inline]
  fn new_session(self: &Arc<Self>) -> wcompact::Result<Self::Session> {
    self.new_session().map_err(WcompactError::from)
  }

  #[inline]
  fn hlog(&self) -> &HybridLog<D> {
    &self.hlog
  }

  #[inline]
  fn index(&self) -> Arc<HashIndex> {
    self.active_index()
  }

  #[inline]
  fn read_only_address(&self) -> u64 {
    self.hlog.read_only_address()
  }

  /// 端口体单点：转调内核 [`WedbStore::safe_read_only_address`]（addr.rs 单一地址源），
  /// 紧缩上界唯一口径，见 trait 文档
  #[inline]
  fn safe_read_only_address(&self) -> u64 {
    self.hlog.safe_read_only_address()
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
      .map_err(WcompactError::from)
  }

  #[inline]
  fn is_read_cache_addr(&self, addr: u64) -> bool {
    is_read_cache(addr)
  }

  #[inline]
  fn skip_read_cache(&self, addr: u64) -> u64 {
    // 端口体单点：见 crate::read_cache 的 skip_read_cache_addr（None 折断开链 →
    // 紧缩面标失效陈旧槽位，另有记录级复核兜底）
    self.read_cache.skip_read_cache_addr(addr)
  }

  #[inline]
  fn enable_revivification(&self) -> bool {
    self.config.enable_revivification
  }

  #[inline]
  fn reviv_put(&self, addr: u64, size: u32, read_only_addr: u64) {
    let _ = self.hlog.try_seal_record(addr, true);
    self.reviv_pool.put(addr, size, read_only_addr);
  }
}

/// wedb 业务紧缩过滤谓词（业务过滤注入位，对标 garnet 相对路径
/// libs/server/Storage/Functions/GarnetRecordTriggers.cs:IsDeleted 与
/// libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs）
///
/// C# TTL 内嵌记录物理头、压缩引擎天然可见；wedb TTL 为独立标签物理键
/// （KeyTag::Ttl 旁路记录，见 crate::ttl 模块文档），过期/孤儿判定属业务语义，
/// 须在紧缩时经 [`wcompact::CompactionFunctions`] 注入，绝不进入
/// [`CompactStore`] 纯存储契约
#[derive(Debug, Default, Clone, Copy)]
pub struct WedbCompactionFunctions;

impl<D: Device> wcompact::CompactionFunctions<WedbStore<D>> for WedbCompactionFunctions {
  /// 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs:IsDeleted
  ///
  /// 单次解析键标签后按记录形态裁决：
  /// - DbMeta 换号元数据：豁免不判死——记录经专属墓碑退出（GC 回收后
  ///   delete_dbmeta 物理注销、rebuild 删除臂承接），紧缩谓词若判死将在根库
  ///   退役窗口把 (0, *)/(*, 0) 活域记录连同全部换号映射整批误删（数据丢失洞）；
  /// - ACL 用户规则：豁免不判死——物理前缀为逻辑 ns 直编码 + 恒驻 vdb 0，
  ///   根本不属虚号换号域，死亡账本比对必误伤（详见 is_deleted 内注释）；
  /// - TTL 旁路记录：自身到期直接判死；未到期经单缓冲轮换标签字节依次探查
  ///   String/ObjectEnvelope/Meta 三种宿主形态（首探 String 命中即短路，覆盖
  ///   绝大多数场景），三者均不存在 = 已删主键遗留的孤儿 TTL 记录，判死丢弃；
  /// - 数据记录（用户可见域，单点谓词 KeyTag::is_user_visible）：单次 TTL 读裁决附带 TTL 是否已过期
  async fn is_deleted(&self, session: &StoreSession<D>, key: &[u8], val: &[u8], now: i64) -> bool {
    let Ok((rec_vns, rec_vdb, tag, user_key)) = NamespaceDbCodec::decode_tagged_key(key) else {
      return false;
    };
    // 换号元数据固定驻留根域前缀 (0,0)，其生死由自身 0x03/0x04 墓碑与 GC 注销
    // 决定，绝不进业务死域判定（判定误伤即全量映射丢失）。
    // ACL 用户规则同理豁免：两域键位语义不同——DbMeta 虽驻 (0,0) 但其生死
    // 自治，而 AclStore 以调用方逻辑 ns 直编码物理前缀、恒驻 vdb 0（不经
    // vdb 虚号映射，逻辑号永不换号退役），死亡账本里的退役虚号与之不同域：
    // 根库 FLUSHDB 退役 vdb 0 的窗口（gc_dead 键 0 库级角色）将经第一比对臂
    // 直击全部 ACL 记录，逻辑 ns 与某退役 vns 撞号（虚号自 1 自增、逻辑 ns
    // 任意 u64）亦经第二臂误杀整租户用户，且无任何日志告警面
    if matches!(tag, KeyTag::DbMeta | KeyTag::Acl) {
      return false;
    }
    if session
      .store
      .vdb
      .is_virtual_id_dead_and_expired(rec_vns, rec_vdb, now)
    {
      return true;
    }
    // 标签字节位于用户键前 1 字节处（[NsVarint] + [DbVarint] + [KeyTag: 1B] + [Payload]）
    let Some(tag_offset) = key.len().checked_sub(user_key.len() + 1) else {
      return false;
    };

    match tag {
      KeyTag::Ttl => {
        // 1. 若 TTL 自身已到期：直接判死（统一收敛至 is_expired，严格小于读路径口径）
        if is_expired(val, now) {
          return true;
        }
        // 2. TTL 未到期：单次拷贝构造探查缓冲后原位轮换标签字节，短路检查宿主
        //    主键在哈希索引中是否存在。探查宿主集合与 KeyTag::is_user_visible
        //    同域（String/ObjectEnvelope/Meta），二者必须保持一致；此处保留显式
        //    顺序令首探 String 短路（覆盖绝大多数场景）
        let _guard = session.enter_epoch();
        let index = session.store.index.load();
        let mut probe = NamespaceDbCodec::replace_tag_at(key, tag_offset, KeyTag::String);
        for host_tag in [KeyTag::String, KeyTag::ObjectEnvelope, KeyTag::Meta] {
          probe.set_tag_at(tag_offset, host_tag);
          if index.find_tag(&probe).is_some() {
            return false;
          }
        }
        // 宿主既无 String 也无对象信封也无 Meta：属于已被删除的主键留下的
        // 孤儿 TTL 记录，直接判死丢弃！
        true
      }
      tag if tag.is_user_visible() => {
        // 检查该数据记录是否附带 TTL 且已过期（单次解析 + 单次 TTL 记录读取；
        // 转发 wkv I64 旁路读单点 read_i64_sidecar，与 ttl_of 同一 read_raw_with 内核）
        let ttl_k = NamespaceDbCodec::replace_tag_at(key, tag_offset, KeyTag::Ttl);
        session
          .read_i64_sidecar(&ttl_k)
          .await
          .is_ok_and(|exp| is_expired(exp, now))
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

  /// 执行混合日志在线紧缩（对标 C# Tsavorite `TsavoriteKV.Compact`；生产入口
  /// 显式注入 wedb 业务过滤，对标 libs/server/Databases/DatabaseManagerBase.cs:449
  /// 注入 GarnetRecordTriggers 的形态）
  #[inline]
  pub async fn compact(
    self: &Arc<Self>,
    until_address: u64,
    comp_type: CompactionType,
  ) -> Result<CompactionStats> {
    self
      .compactor()
      .compact_with_filter(until_address, comp_type, &WedbCompactionFunctions)
      .await
      .map_err(Error::from)
  }

  /// 执行带自定义判定谓词的在线紧缩（对标 C# Tsavorite `Compact` 带 `ICompactionFunctions`
  /// 泛型注入；自定义谓词整体替换注入位，需要 TTL 判死语义时可委托 [`WedbCompactionFunctions`]）
  #[inline]
  pub async fn compact_with_filter<C>(
    self: &Arc<Self>,
    until_address: u64,
    comp_type: CompactionType,
    cf: &C,
  ) -> Result<CompactionStats>
  where
    C: wcompact::CompactionFunctions<Self>,
  {
    self
      .compactor()
      .compact_with_filter(until_address, comp_type, cf)
      .await
      .map_err(Error::from)
  }
}
