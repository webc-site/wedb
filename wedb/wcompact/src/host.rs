use std::{future::Future, sync::Arc};

use wdev::Device;
use whlog::HybridLog;
use windex::HashIndex;

use crate::error::Result;

/// 紧缩器扫描期间关心的集合元数据摘要
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactMetaInfo {
  /// 集合唯一标识 ID
  pub key_id: u64,
  /// 集合当前版本号
  pub version: u64,
  /// 集合元素数量（为 0 表示幽灵/空集合，触发死亡标记）
  pub size: u64,
}

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

  /// 读取 TTL 键对应的绝对过期 .NET Ticks（若存在且未失效；与 TTL 记录存储值同域）
  fn read_ttl_expiry(&self, ttl_key: &[u8]) -> impl Future<Output = Result<Option<i64>>>;
}

/// 紧缩器对宿主存储引擎的抽象契约接口（彻底解耦对顶层具体 WedbStore 的反向依赖）
pub trait CompactStore {
  /// 底层存储设备类型
  type Device: Device;

  /// 会话类型
  type Session: CompactSession<Self::Device>;

  /// 创建会话
  fn new_session(self: &Arc<Self>) -> Result<Self::Session>;

  /// 混合日志分配器引用
  fn hlog(&self) -> &HybridLog<Self::Device>;

  /// 无锁哈希索引引用
  fn index(&self) -> &HashIndex;

  /// 只读区边界逻辑地址
  fn read_only_address(&self) -> u64;

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

  /// 读取 key_id 的集合元数据 (current_version, is_alive)
  fn get_key_id_meta(&self, key_id: u64) -> Option<(u64, bool)>;

  /// 更新 key_id 的集合元数据水位
  fn update_key_id_meta(&self, key_id: u64, version: u64, is_alive: bool);

  /// 移除已死 key_id 的元数据条目
  fn remove_key_id_meta(&self, key_id: u64);

  /// 判断键是否为集合元数据物理键
  fn is_meta_key(&self, key: &[u8]) -> bool;

  /// 解析集合元数据载荷
  fn parse_meta_value(&self, val: &[u8]) -> Option<CompactMetaInfo>;

  /// 判定物理键是否为已废弃的集合历史子键（集合已删除或当前版本大于子键版本）
  fn is_stale_subkey(&self, key: &[u8]) -> bool;

  /// 判定物理键与载荷是否属于已过期的 TTL 记录、无主孤儿 TTL 记录、或已到期的数据记录
  ///
  /// `now` 为 .NET Ticks 过期判定基准（与 TTL 记录存储值同域）
  fn is_expired_or_orphan_record(
    &self,
    session: &Self::Session,
    key: &[u8],
    val: &[u8],
    now: i64,
  ) -> impl Future<Output = bool>;
}
