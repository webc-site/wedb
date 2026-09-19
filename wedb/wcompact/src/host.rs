use std::{future::Future, sync::Arc};

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

  /// 写入追加新记录至尾部
  fn append_record(
    &self,
    key: &[u8],
    val: &[u8],
    expected_main_addr: u64,
    is_tombstone: bool,
  ) -> impl Future<Output = Result<u64>>;

  /// 冷读单点口：按逻辑地址读取记录，磁盘区/内存驻留区分派与纪元纪律全收口于宿主
  /// 内核（wkv `StoreSession::read_record`），调用方不得自持纪元
  fn read_record_at(&self, addr: u64) -> impl Future<Output = Result<whlog::RecordOutput>>;
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

  /// 只读区边界逻辑地址（Unsafe ReadOnlyAddress，模糊区上沿）
  ///
  /// 紧缩链上的余留职责仅为度量类判据（复活资格 min_address，见 run.rs
  /// conditional_copy_to_tail），绝不作紧缩上界——上界一律走
  /// [`Self::safe_read_only_address`]
  fn read_only_address(&self) -> u64;

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

  /// 顺链跳过 ReadCache 取得底层真实主日志地址
  fn skip_read_cache(&self, addr: u64) -> u64;

  /// 是否启用了复活池
  fn enable_revivification(&self) -> bool;

  /// 归还孤儿槽位至复活池
  fn reviv_put(&self, addr: u64, size: u32, read_only_addr: u64);
}

/// 紧缩业务过滤谓词契约（在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs:IsDeleted）
///
/// C# 压缩引擎为通用底层组件：死亡判定仅「墓碑 → 业务谓词」两通道短路，业务
/// 语义完全由泛型注入。宿主实现方（wkv）在此承载 TTL 过期/孤儿等业务判定，
/// 对标 libs/server/Storage/Functions/GarnetRecordTriggers.cs:IsDeleted 的业务注入位
pub trait CompactionFunctions<S: CompactStore> {
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
}

/// 默认紧缩过滤：恒不判死（在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs:DefaultCompactionFunctions）
pub struct DefaultCompactionFunctions;

impl<S: CompactStore> CompactionFunctions<S> for DefaultCompactionFunctions {
  #[inline]
  async fn is_deleted(&self, _session: &S::Session, _key: &[u8], _val: &[u8], _now: i64) -> bool {
    false
  }
}
