//! 混合日志在线紧缩器集成（对标 C# Tsavorite Compaction API）

use std::{io, result, sync::Arc};

use wbase::{addr::is_read_cache, map::HashSet, time::now_ticks};
use wcompact::{
  self, CompactSession, CompactStore, CompactionStats, CompactionType, Error as WcompactError,
  LogCompactor,
};
use wdev::Device;
use whlog::HybridLog;
use windex::HashIndex;
use wval::{KeyTag, NamespaceDbCodec, TaggedKeyBuf, VectorRegistrySubTag};

use crate::{
  error::{Error, Result},
  range_index::{
    clear_flushed_patch, patch_stub_record, range_index_blocking, range_index_stub_of,
  },
  session::StoreSession,
  store::{StoreEvent, WedbStore},
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

  /// 紧缩探针分裂协同门：转调会话侧唯一入口 [`Self::ensure_split_by_hash`] 单点
  /// （与读面 `read_probe` / 扫描面 `find_tag_cooperative` 同一协同机制，非扩容期
  /// 仅一次 old_index 判空原子 load），grow 迁移窗内先行迁移本键分块，杜绝紧缩面
  /// 对未迁新桶采得空候选判 superseded 弃迁活键；迁移内核错误经类型化映射显式
  /// 上抛，严禁折成空候选假弃迁
  #[inline]
  fn ensure_split(&self, key: &[u8]) -> wcompact::Result<()> {
    self
      .ensure_split_by_hash(whasher::fast_hash(key))
      .map_err(WcompactError::from)
  }

  /// libs/server/Storage/Functions/GarnetRecordTriggers.cs:PostCopyToTail
  ///
  /// core 触发器契约同挂此处（宿主回调与 core 接口在 rust 折叠为同一挂点）：
  /// libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/IRecordTriggers.cs:PostCopyToTail
  ///
  /// dst 侧承接 ClearFlushedFlag：紧缩搬迁存根自愈——搬迁记录为 RangeIndex 存根
  /// 元记录时，尾部新帧清除 Flushed 位，避免后续读取误判为冷数据触发多余晋升
  /// 循环（C# 无条件调 ClearFlushedFlag(dstSpan)，幂等位变更器对非 Flushed 帧
  /// 零写，本条件与 C# 语义等价）。源侧三步（ClearTreeHandle +
  /// SetTransferredFlag + 冷源 PreStageAndRegisterPending）收口于
  /// [`Self::transfer_out_source`]，在搬迁 CAS 成功后执行——与 C# 的「写 dst 时
  /// 即转出」差异见 trait 文档（rust Retain 保守保留语义所迫）。
  /// 治愈一律转调 wkv 唯一内核 crate::range_index::patch_stub_record：只改 35B 存根
  /// 窗口、值体长度无上限（对标 C# 就地改 Span + TryCopyToTail 分配新尾记录）。
  // 探针先行：非分层存根记录（紧缩热路径绝大多数）零分配直过。
  async fn allocate_record(
    &self,
    key: &[u8],
    val: &[u8],
    expected_main_addr: u64,
    is_tombstone: bool,
    chain_head: u64,
  ) -> wcompact::Result<(u64, u32)> {
    let healed: Option<Vec<u8>> = if is_tombstone || range_index_stub_of(val).is_none() {
      None
    } else {
      let mut frame = val.to_vec();
      patch_stub_record(&mut frame, clear_flushed_patch);
      Some(frame)
    };

    // 紧缩搬迁旁路写监听：AOF 只记原始写效果，物理搬迁帧入 AOF 会导致恢复回退；
    // 下界按候选链首 chain_head 经 reviv_chain_floor 单点折算（6 参调用解析到
    // StoreSession 固有 allocate_record，非本 trait 方法）
    let min_eligible = self.reviv_chain_floor(chain_head);
    StoreSession::<D>::allocate_record(
      self,
      key,
      healed.as_deref().unwrap_or(val),
      expected_main_addr,
      is_tombstone,
      min_eligible,
    )
    .await
    .map(|(addr, frame, _ver)| (addr, frame))
    .map_err(WcompactError::from)
  }

  /// 搬迁 CAS 成功后的源存根所有权转出（C# PostCopyToTail 源侧
  /// ClearTreeHandle + SetTransferredFlag + 冷源 PreStageAndRegisterPending）
  ///
  /// 仅分层 Meta 存根记录承接（C# `srcLogRecord.RecordType != RangeIndexRecordType`
  /// 短路对位）；树身份键 = 记录物理键（Tag 即 Meta，零解码换键），冷源预置与
  /// 句柄清零/置位编排全部转调 RIPROMOTE 单点
  /// [`Self::transfer_out_source_stub`]，与晋升口径并轨，绝不另立第二套判定
  fn transfer_out_source(&self, key: &[u8], val: &[u8], src_addr: u64) {
    if range_index_stub_of(val).is_none() {
      return;
    }
    self.transfer_out_source_stub(key, src_addr);
  }

  /// 端口体单点：直接转调内核 [`StoreSession::read_record`]，冷读分派与纪元纪律
  /// 只在内核一处维持，紧缩面不复述
  #[inline]
  async fn read_record_at(&self, addr: u64) -> wcompact::Result<whlog::RecordOutput> {
    self.read_record(addr).await.map_err(WcompactError::from)
  }
}

impl<D: Device> StoreSession<D> {
  /// 回链头重读单点（对标 C# ReadCache.cs:111 `UpdateRecordSourceToCurrentHashEntry`
  /// 重读哈希项）：驱逐清洗方以槽位 CAS 把易失 ReadCache 前缀换指主日志地址后，重走
  /// 旧链头地址会命中已清零页误判链尽，故等待落定必须重读本槽当前值
  ///
  /// 紧缩面与 copy-to-tail 面上手上都没有槽位句柄（索引探针只交付候选地址值），故以
  /// 该键的索引候选链等价复现「重读本槽」：值未变即原槽续链；清洗后本槽值为更旧地址
  /// （prev 严格递减），取不高于原链头的最新候选；全数候选已被并发写者整体换指更高
  /// 地址时按最高候选重走；候选清空即本槽摘除，按链尽 0 收口
  pub(crate) fn rc_hash_entry_head(&self, key: &[u8], head: u64) -> u64 {
    let addrs = self.store.index.load().lookup_candidates(key);
    if addrs.contains(head) {
      return head;
    }
    let mut older = 0;
    let mut newest = 0;
    for &addr in addrs.iter() {
      newest = newest.max(addr);
      if addr <= head {
        older = older.max(addr);
      }
    }
    if older != 0 { older } else { newest }
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

  /// 扩容相位纯观测口（零成本原子读，无门控语义）：紧缩探针确定性扩容窗注入钩
  /// 守卫消费（wcompact `probe.rs`，与读面 `test_read_gap_hook` 留钩同族同守卫）
  #[inline]
  fn is_growing(&self) -> bool {
    self.resize.is_growing()
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

  /// 端口体单点：转调带等待的走查内核（与快照面 `store/cpr_host.rs`、写侧
  /// `raw/write/copy_to_tail.rs` 同一内核，杜绝锚点分叉）——滑窗驱逐过渡态以**当前
  /// 走查位置**就地等待清洗落定，随后重读本键哈希项回链头重探（对标 C#
  /// SkipReadCache 的 RestartChain），绝不折 0 判槽位陈旧
  #[inline]
  fn skip_read_cache_with_wait(&self, key: &[u8], head: u64, session: &Self::Session) -> u64 {
    self.read_cache.skip_read_cache_with_wait(
      || session.rc_hash_entry_head(key, head),
      || session.participant.refresh(),
    )
  }

  #[inline]
  fn enable_revivification(&self) -> bool {
    self.config.enable_revivification
  }

  #[inline]
  fn reviv_put(&self, addr: u64, size: u32) {
    // 密封 + 下限门槛入池单点在 [`WedbStore::transfer_to_reviv_pool`]
    self.transfer_to_reviv_pool(addr, size);
  }
}

/// TTL/ETag 旁路记录三形态宿主存在性协同探查单点（TTL 与 Etag 两臂共用，杜绝
/// 双写分叉）：轮换标签字节短路探查 String/ObjectEnvelope/Meta 三宿主形态，
/// 探查一律经 [`StoreSession::find_tag_cooperative`] 先协同迁移键所在分块再采样
/// （与读面 `read_probe` / 扫描面 / 紧缩探针同一协同单机制，对位 C# 紧缩面
/// NOTFOUND 保守补拷臂 TsavoriteCompaction.cs:51 → FindRecord.cs:40-43 →
/// ConditionalCopyToTail.cs:111-113 的「索引未命中绝不判缺失」语义）。
///
/// 探查集合与 [`KeyTag::is_user_visible`] 宿主域同域（String/ObjectEnvelope/Meta），
/// 二者必须保持一致；首探 String 短路覆盖绝大多数场景。返回 `Err` 为迁移内核
/// 错误（溢出桶耗尽 / 环形 Cycle 等），调用方须按「存活」保守处置本轮，严禁
/// 折成孤儿判死丢弃（grow 迁移窗内宿主条目未迁即新表查空，直判孤儿即 TTL 静默
/// 消失、ETag 对偶校验记录丢失的成批数据丢失洞）
fn host_exists_cooperative<D: Device>(
  session: &StoreSession<D>,
  key: &[u8],
  tag_offset: usize,
) -> Result<bool> {
  let mut probe = NamespaceDbCodec::replace_tag_at(key, tag_offset, KeyTag::String);
  for host_tag in [KeyTag::String, KeyTag::ObjectEnvelope, KeyTag::Meta] {
    probe.set_tag_at(tag_offset, host_tag);
    if session.find_tag_cooperative(&probe)?.is_some() {
      return Ok(true);
    }
  }
  Ok(false)
}

/// wedb 业务紧缩过滤谓词（业务过滤注入位，对标 garnet 相对路径
/// libs/server/Storage/Functions/GarnetRecordTriggers.cs:IsDeleted 与
/// libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs）
///
/// C# TTL 内嵌记录物理头、压缩引擎天然可见；wedb TTL 为独立标签物理键
/// （KeyTag::Ttl 旁路记录，见 crate::ttl 模块文档），过期/孤儿判定属业务语义，
/// 须在紧缩时经 [`wcompact::CompactionFunctions`] 注入，绝不进入
/// [`CompactStore`] 纯存储契约
#[derive(Debug, Default, Clone)]
pub struct WedbCompactionFunctions {
  failed_trees: Arc<parking_lot::Mutex<HashSet<TaggedKeyBuf>>>,
}

impl<D: Device> wcompact::CompactionFunctions<WedbStore<D>> for WedbCompactionFunctions {
  type Error = crate::Error;

  /// 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs:IsDeleted
  ///
  /// 单次解析键标签后按记录形态裁决：
  /// - DbMeta 换号元数据：豁免不判死——记录经专属墓碑退出（GC 回收后
  ///   delete_dbmeta 物理注销、rebuild 删除臂承接），紧缩谓词若判死将在根库
  ///   退役窗口把 (0, *)/(*, 0) 活域记录连同全部换号映射整批误删（数据丢失洞）；
  /// - ACL 用户规则：豁免不判死——物理前缀为逻辑 ns 直编码 + 恒驻 vdb 0，
  ///   根本不属虚号换号域，死亡账本比对必误伤（详见 is_deleted 内注释）；
  /// - VectorRegistry·Metadata 旁路记录：豁免不判死——上下文元数据全局单例
  ///   恒驻根域前缀 (0,0)，与 DbMeta/Acl 同格恒根域住户三形（见下臂注释），
  ///   生死由登记表回收通道自治；Index 子标签不豁免，照常进死域判定；
  /// - TTL 旁路记录：自身到期直接判死；未到期经 [`host_exists_cooperative`] 协同
  ///   探查（先迁移键所在分块再采样，与读面/扫描面/紧缩探针同一单机制）单缓冲
  ///   轮换标签字节依次探查 String/ObjectEnvelope/Meta 三种宿主形态（首探 String
  ///   命中即短路，覆盖绝大多数场景），三者均不存在 = 已删主键遗留的孤儿 TTL
  ///   记录，判死丢弃；
  /// - ETag 旁路记录：与 TTL 形成对偶生命周期校验，分宿主探查与过期复核两步
  ///   （详见 is_deleted 内 Etag 臂注释）；
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
    // 任意 u64）亦经第二臂误杀整租户用户，且无任何日志告警面。
    // 向量上下文元数据旁路记录（VectorRegistry·Metadata）为同族恒根域住户
    // 第三形（zcode-r149c-flushsnap 案一）：全局单例恒落 (0,0) 前缀、换号不
    // 迁位，根库 FLUSHDB 退役 vdb 0 满回收期限后经第一比对臂同样恒命中——
    // 住户自驻根域坐标而退役域恰为该坐标，角色精确比对不覆「域内住户非受
    // 清对象」形，豁免臂即唯一对症通道；误伤即全库向量上下文 in_use/slots
    // 唯一持久源丢失、重启回建趟静默失据。其生死由登记表回收通道
    // （RegistryReclaim 命中域 + All/reset）自治，与 DbMeta/Acl 同格豁免。
    // 子标签粒度即精确边界：Index (0x01) 旁路记录前缀随所属会话域走，(0,0)
    // 坐标的 Index 记录恰系根库自身向量集的预期退役对象，照常进死域判定。
    if matches!(tag, KeyTag::DbMeta | KeyTag::Acl)
      || (tag == KeyTag::VectorRegistry
        && user_key.first() == Some(&VectorRegistrySubTag::Metadata.as_u8()))
    {
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
        // 2. TTL 未到期：协同探查宿主主键存在性（三形态宿主域轮换，单点见
        //    [`host_exists_cooperative`]——grow 迁移窗内宿主条目未迁分块时先协同
        //    迁移再采样，杜绝新表查空误判孤儿致 TTL 静默消失）
        let _guard = session.enter_epoch();
        match host_exists_cooperative(session, key, tag_offset) {
          // 宿主存在：TTL 存活
          Ok(true) => false,
          // 宿主 String/对象信封/Meta 三形态均不存在：属于已被删除的主键留下的
          // 孤儿 TTL 记录，直接判死丢弃！
          Ok(false) => true,
          // 迁移内核错误：保守判存活延至下轮复审，严禁折成孤儿误杀
          Err(_) => false,
        }
      }
      KeyTag::Etag => {
        // ETag 与 TTL 同为脱离主记录的旁路物理键，须成对回收，否则落入下方
        // 通配兜底恒判存活、经 conditional_copy_to_tail 无限回拷永生，霸占
        // 索引槽位与日志空间且污染未来同名键的条件写语义。与 KeyTag::Ttl
        // 形成对偶生命周期校验，分两步：
        let _guard = session.enter_epoch();
        // 1. 宿主存在性协同探查（单点 [`host_exists_cooperative`]，与 KeyTag::Ttl
        //    臂共用）：三宿主形态轮换且 grow 迁移窗内先协同后采样，三者均不在
        //    索引中 = 主键已被删除或已被紧缩丢弃，本 ETag 记录确属无主孤儿；
        //    迁移内核错误保守判存活延至下轮复审，严禁折成孤儿误杀
        let host_found = match host_exists_cooperative(session, key, tag_offset) {
          Ok(found) => found,
          Err(_) => {
            return false;
          }
        };
        drop(_guard);
        if !host_found {
          return true;
        }
        // 2. 宿主过期复核：宿主虽仍在索引中，但其附带 TTL 若已到期，宿主已死
        //    且正被/即将被丢弃，本 ETag 记录随之判死（与 is_user_visible 臂同一
        //    read_i64_sidecar + is_expired 口径）。此步是 ETag 相对 TTL 的对偶
        //    补强——紧缩 Scan 阶段 1 宿主尚未从索引摘除，仅靠存在性探查会漏判
        //    「宿主 TTL 刚过期」的 ETag，须复核宿主 TTL 方能杜绝孤儿回拷
        let ttl_k = NamespaceDbCodec::replace_tag_at(key, tag_offset, KeyTag::Ttl);
        session
          .read_i64_sidecar(&ttl_k)
          .await
          .is_ok_and(|exp| is_expired(exp, now))
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

  /// 判死键级清退（对标 C# GarnetRecordTriggers.cs:OnDispose 的
  /// Deleted/Expired 臂 DisposeTreeUnderLock(deleteFiles: true) +
  /// vectorManager.RequestDeletion；判死语义等价一次过期 DEL，见 trait 文档）
  ///
  /// 仅执行整键退册（树实例注销、磁盘文件清理、换号回收退册与向量删除链钩子）：
  /// 绝不向混合日志追加新墓碑——紧缩器随后会通过 CAS 置零直接摘除槽位，并在物理
  /// 截断时回收空间；若向日志写墓碑不仅造成额外垃圾，更会破坏槽位 CAS 并在注销
  /// 失败时留下半程墓碑导致下轮重判失效。
  /// 并发安全垫由当下 TTL 重判承担：并发续期未过期即零副作用放行。
  async fn on_dropped(
    &self,
    session: &StoreSession<D>,
    key: &[u8],
  ) -> result::Result<(), Self::Error> {
    let Ok((vns, vdb, _tag, user_key)) = NamespaceDbCodec::decode_tagged_key(key) else {
      return Ok(());
    };
    // 直设落域显式携版本轨入账逻辑域（死域孤域代位，见 version_domain_of 单点）
    let (lns, ldb) = session.store.vdb.version_domain_of(vns, vdb);
    session.set_virtual_context(vns, vdb, lns, ldb);

    // 并发安全垫：若在判死与清退之间键被并发续期，未过期零副作用
    if let Some(exp) = session.ttl_of(user_key).await?
      && !is_expired(exp, now_ticks())
    {
      return Ok(());
    }

    let tree_key = NamespaceDbCodec::encode_tagged_key(vns, vdb, KeyTag::Meta, user_key);
    let mgr = Arc::clone(&session.store.range_index);
    if self.failed_trees.lock().contains(&tree_key) {
      return Err(Error::Io(io::Error::other(
        "分层宿主清退失败，同键关联记录保守保留至下轮",
      )));
    }

    // 树实例整键退册（对标 C# OnDispose 的 DisposeTreeUnderLock(deleteFiles: true)）
    //
    // 日志先行（WAL / error.rs AofEnqueue 契约）：RangeIndexDrop 先入账 AOF
    // 再注销物理树——入队失败即树分毫未动、回 Err 跳过摘槽，紧缩位点不前移、
    // 记录保守保留，下轮紧缩重试入账必达；若先注销后入账，入队失败时事件已永
    // 无重发通道（下轮 get_tree 落空直接跳过入账臂），副本永久遗漏清理事件、
    // 孤儿树资源泄漏
    if mgr.get_tree(&tree_key).is_some() {
      session.store.emit_event(
        session.aof_session_id,
        StoreEvent::RangeIndexDrop {
          ns: vns,
          db: vdb,
          key: user_key,
        },
      )?;
      let del_key = tree_key.clone();
      let res = range_index_blocking(move || {
        mgr
          .delete_index(&del_key)
          .map_err(|e| Error::swapped(e.into()))
      })
      .await;

      match res {
        Ok(Ok(_)) => {
          session.unregister_bftree_key(user_key);
        }
        _ => {
          self.failed_trees.lock().insert(tree_key);
          return Err(Error::Io(io::Error::other(
            "分层树注销失败，记录保留至下轮",
          )));
        }
      }
    }

    // 向量删除钩子（对标 C# OnDispose 的 RequestDeletion）
    if let Some(hook) = session.store.delete_miss_hook.get() {
      hook
        .call(session.session_prefix().as_slice(), user_key)
        .await;
    }

    Ok(())
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
  ///
  /// 会话层紧缩入口同挂此处：
  /// libs/storage/Tsavorite/cs/src/core/ClientSession/ClientSession.cs:Compact
  /// （C# ClientSession.Compact 转调 tsavorite.Compact，rust 由宿主直接持 store 调用）
  #[inline]
  pub async fn compact(
    self: &Arc<Self>,
    until_address: u64,
    comp_type: CompactionType,
  ) -> Result<CompactionStats> {
    self
      .compactor()
      .compact_with_filter(
        until_address,
        comp_type,
        &WedbCompactionFunctions::default(),
      )
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
