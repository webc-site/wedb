use std::{
  convert::Infallible,
  error::Error,
  future::{Future, ready},
  result,
  sync::Arc,
};

use wdev::Device;
use whlog::HybridLog;
use windex::HashIndex;

use crate::error::Result;

/// 紧缩器对会话读写操作的抽象契约接口（彻底解耦对具体 StoreSession 的反向依赖）
pub trait CompactSession<D: Device> {
  /// 进入纪元保护区守卫
  type EpochGuard<'a>: Drop
  where
    Self: 'a;

  /// 获取会话级纪元保护守卫
  fn enter_epoch(&self) -> Self::EpochGuard<'_>;

  /// 索引探针前的分裂协同门（会话读写面入口铁律在紧缩面的同构承接，对标 C#
  /// InternalRead.cs:71、InternalRMW.cs:68、InternalUpsert.cs:65、InternalDelete.cs:58
  /// 一律「先协同、后探针」；C# 紧缩面的同等保障由 NOTFOUND 保守补拷臂承担——
  /// TsavoriteCompaction.cs:45-55 逐活记录 CompactionCopyToTail → FindRecord.cs:40-43
  /// FindTag 未命中落 NOTFOUND → ConditionalCopyToTail.cs:111-113 无条件补拷插回，
  /// 绝不因探针未命中弃迁记录）
  ///
  /// 宿主在此先行迁移键所在分块至 SPLIT_COMPLETED，杜绝 grow 迁移窗内对未迁新桶
  /// 采得空候选被判 superseded 的成批静默丢键；非扩容期仅一次原子判空 load。
  /// 与读面 `read_probe` / 扫描面 `find_tag_cooperative` 同一协同单机制，严禁另立
  /// 第二套补拷语义或相位互斥。迁移内核错误显式上抛，调用方严禁折成空候选假弃迁
  fn ensure_split(&self, key: &[u8]) -> Result<()>;

  /// 分配记录至尾部（复活池取臂 + 尾部追加统一编排，对标 C# TryCopyToTail.cs:33
  /// 以 `AllocateOptions{recycle=true}` 统一经 TryAllocateRecord 的慢路径分配契约，
  /// BlockAllocate.cs:57-82；PageNotReady 时宿主驱逐旧页后重试至成功）
  ///
  /// `chain_head` 为候选链首地址（索引期望槽位）：宿主负责按「链首 + 1 与全局复活
  /// 水位取大」折算槽位下界，严格保证复活槽位地址高于旧链首，杜绝哈希碰撞链
  /// prev 逆向成环。返回 `(新地址, 本帧实际足印)`，足印供败帧归池精确登记。
  /// 旁路写监听：紧缩搬迁/晋升帧属物理布局优化而非用户写效果，不入 AOF
  fn allocate_record(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
    chain_head: u64,
  ) -> impl Future<Output = Result<(u64, u32)>>;

  /// 冷读单点口：按逻辑地址读取记录，磁盘区/内存驻留区分派与纪元纪律全收口于宿主
  /// 内核（wkv `StoreSession::read_record`），调用方不得自持纪元
  fn read_record_at(&self, addr: u64) -> impl Future<Output = Result<whlog::RecordOutput>>;

  /// 搬迁 CAS 成功后的源存根所有权转出（对标 C#
  /// libs/server/Storage/Functions/GarnetRecordTriggers.cs:PostCopyToTail 源侧三步
  /// ClearTreeHandle + SetTransferredFlag + 冷源 PreStageAndRegisterPending）
  ///
  /// 仅宿主关心的记录形态承接（C# `srcLogRecord.RecordType != RangeIndexRecordType`
  /// 短路对位，判据归宿主单点），句柄清零/置位/预置登记与 RIPROMOTE
  /// PostCopyUpdater 同一编排，严禁宿主另立第二套判定。置位后组提交刷盘的
  /// OnFlush 防护（is_transferred）对该滞留源生效，绝不对已被尾部新帧取代的
  /// 旧源做全树 CPR 快照、不产出无消费方 flush 件。
  ///
  /// 时点刻意差异（rust Retain 语义所迫，无 C# 对位）：C# 在 PostCopyToTail
  /// （写 dst 时、CAS 前）就转出源——C# 无界重试无保守保留态；rust 复试耗尽
  /// 返回 Retain 时源记录原位存活继续服务，CAS 前置位会令权威版本永不被刷盘
  /// 快照，故转出收口在 CAS 成功之后（Superseded 源与普通 RCU 旧版本等价，
  /// 既有语义覆盖）
  fn transfer_out_source(&self, key: &[u8], val: &[u8], src_addr: u64);
}

/// 紧缩器对宿主存储引擎的抽象契约接口（彻底解耦对顶层具体 WedbStore 的反向依赖）
///
/// 纯存储访问契约：仅暴露日志、索引、地址边界与复活池等底层原语。业务过滤
/// （TTL 过期、孤儿判定等）一律经 [`CompactionFunctions`] 注入，对标 C# Tsavorite
/// 通用底层组件形态（libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs）
pub trait CompactStore {
  /// 底层存储设备类型
  type Device: Device;

  /// 会话类型
  type Session: CompactSession<Self::Device>;

  /// 创建会话
  fn new_session(self: &Arc<Self>) -> Result<Self::Session>;

  /// 混合日志分配器引用
  fn hlog(&self) -> &HybridLog<Self::Device>;

  /// 当前活跃无锁哈希索引共享句柄
  fn index(&self) -> Arc<HashIndex>;

  /// 宿主哈希索引是否正处于在线扩容期（PrepareGrow 构建窗与 IN_PROGRESS_GROW
  /// 迁移窗均算，与 wcpr `CheckpointStore::is_growing` 口径同形）
  ///
  /// 零成本原子相位读，纯观测口：不承载任何门控/互斥语义（grow 可穿插紧缩，
  /// 入口相位门不足收口，紧缩面防线是探针协同门 [`CompactSession::ensure_split`]），
  /// 唯一消费方为探针的确定性扩容窗注入钩守卫（见 `compactor/probe.rs`）
  fn is_growing(&self) -> bool;

  /// 安全线只读区边界逻辑地址（SafeReadOnlyAddress，纪元排空后的定稿区上沿）
  ///
  /// 紧缩上界的唯一口径：[begin, safe_ro) 内不存在任何在途原位写（原位写者持纪元
  /// 保护，safe_ro 推进以排空为前提），模糊区 [safe_ro, read_only) 按可变区处理、
  /// 紧缩不得触达（取界对标 TsavoriteCompaction.cs:35,72 的内核硬校验）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:SafeReadOnlyAddress
  fn safe_read_only_address(&self) -> u64;

  /// 有效起始逻辑地址
  fn begin_address(&self) -> u64;

  /// 推进起始逻辑地址并回收底层设备段
  fn shift_begin_address(&self, until: u64) -> impl Future<Output = Result<()>>;

  /// 判断地址是否属于 ReadCache 独立只读内存日志
  fn is_read_cache_addr(&self, addr: u64) -> bool;

  /// 顺链跳过 ReadCache 取得底层真实主日志地址（带驱逐等待的走查单口，对标 C#
  /// SkipReadCache 每步判定当前位置 + RestartChain 回链头重读哈希项，
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:SkipReadCache）
  ///
  /// 本面手上没有槽位句柄（索引探针只交付候选地址值），故链头重读以**该键哈希项的
  /// 当前值**形态给出（对标 C# `UpdateRecordSourceToCurrentHashEntry` 重读哈希项）：
  /// 走查触到正被换页驱逐的 ReadCache 记录（含链中段滑出这一驱逐进行中的常态形态）
  /// 时，以该滑出地址就地自旋等待驱逐方清洗落定并发布 ClosedUntilAddress，随后重读
  /// `head` 所在键的哈希项重探，循环直至解析落定。
  ///
  /// 返回值恒为解析后的主日志地址，绝无「不可判读」三态：`0` 为链尽合法形态
  /// （ReadCache 专属记录无主日志对应，或本槽已被摘除）。**绝不得把存活槽位折成
  /// 0**，紧缩据此判陈旧摘除即永久丢键
  fn skip_read_cache_with_wait(&self, key: &[u8], head: u64, session: &Self::Session) -> u64;

  /// 是否启用了复活池
  fn enable_revivification(&self) -> bool;

  /// 归还孤儿槽位至复活池
  ///
  /// 入池门槛由宿主内部按复活下限 min_address 单点推导（对标 C#
  /// `FreeRecordPool.TryAdd` 自持 `GetMinRevivifiableAddress()`，调用方绝不传入水位），
  /// 与出池侧 `take` 同口径；低于下限的缓冲窗死槽一律拒入，杜绝容量污染
  fn reviv_put(&self, addr: u64, size: u32);
}

/// 紧缩业务过滤谓词契约（在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs:IsDeleted）
///
/// C# 压缩引擎为通用底层组件：死亡判定仅「墓碑 → 业务谓词」两通道短路，业务
/// 语义完全由泛型注入。宿主实现方（wkv）在此承载 TTL 过期/孤儿等业务判定，
/// 对标 libs/server/Storage/Functions/GarnetRecordTriggers.cs:IsDeleted 的业务注入位
pub trait CompactionFunctions<S: CompactStore> {
  /// 宿主关联错误类型（经宿主错误透明上浮，全程无字符串化降级）
  type Error: Error + Send + Sync + 'static;

  /// 判定日志记录是否属于业务侧已死亡（TTL 过期、孤儿等）
  ///
  /// 墓碑判定由紧缩器先行短路，本谓词仅对非墓碑记录生效（与 C# 口径一致）；
  /// `now` 为 .NET Ticks 过期判定基准（与 TTL 记录存储值同域）
  fn is_deleted(
    &self,
    session: &S::Session,
    key: &[u8],
    val: &[u8],
    now: i64,
  ) -> impl Future<Output = bool>;

  /// 判死记录的键级清退钩子（宿主清退先行，紧缩器随后条件摘除槽位）
  ///
  /// 宿主在此承接整键退册：树实例注销、向量删除链退册与随键 TTL/ETag 旁路
  /// 级联（对标 C# GarnetRecordTriggers.cs:OnDispose 的 Deleted/Expired 臂
  /// `DisposeTreeUnderLock(deleteFiles: true)` + `vectorManager.RequestDeletion`；
  /// C# IsDeleted 恒 false 无业务判死，本钩子属 rust 自研 TTL 判死的对偶配套，
  /// 判死语义须等价一次过期 DEL）。清退必须先于紧缩器的索引摘除：清退链靠
  /// 索引/读链定位各物理域（Meta 路由域、TTL/ETag 旁域），若紧缩器先摘主键
  /// 槽位，清退链即定位失败跳过树注销，树实例/缓存预算/数据文件依旧成永久
  /// 孤儿。并发安全垫由宿主的过期重读裁决承担：判死裁决与清退之间键可能被
  /// 并发 SET 续期，清退入口以当下 TTL 重判，未过期零副作用。
  ///
  /// 默认零处置（C# 无对位：[`DefaultCompactionFunctions`] 等纯墓碑紧缩宿主
  /// 无键级退册语义）
  fn on_dropped(
    &self,
    _session: &S::Session,
    _key: &[u8],
  ) -> impl Future<Output = result::Result<(), Self::Error>> {
    ready(Ok(()))
  }
}

/// 默认紧缩过滤：恒不判死（在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs:DefaultCompactionFunctions）
pub struct DefaultCompactionFunctions;

impl<S: CompactStore> CompactionFunctions<S> for DefaultCompactionFunctions {
  type Error = Infallible;

  #[inline]
  async fn is_deleted(&self, _session: &S::Session, _key: &[u8], _val: &[u8], _now: i64) -> bool {
    false
  }
}
