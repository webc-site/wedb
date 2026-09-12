//! BfTree 核心类型定义与状态枚举 (1:1 对标 Microsoft bf-tree 与 Garnet BfTreeService)

use std::{
  ops::{Deref, DerefMut},
  path::Path,
};

pub use bf_tree::{ScanReturnField, StorageBackend};

use crate::stub::RangeIndexStub;

/// 高级 BfTree 配置封装器 (零 Debug 字符串解析，显式跟踪存储后端与数据文件路径)
#[derive(Clone, Debug, Default)]
pub struct BfTreeConfig {
  pub(crate) inner: bf_tree::Config,
  pub(crate) file_path: Option<String>,
  pub(crate) storage_backend: StorageBackendType,
}

impl Deref for BfTreeConfig {
  type Target = bf_tree::Config;
  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl DerefMut for BfTreeConfig {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.inner
  }
}

impl From<BfTreeConfig> for bf_tree::Config {
  #[inline]
  fn from(cfg: BfTreeConfig) -> Self {
    cfg.inner
  }
}

impl BfTreeConfig {
  /// 设置数据文件路径（自动配置 Std 磁盘持久化后端）
  pub fn file_path(&mut self, path: impl AsRef<Path>) -> &mut Self {
    let p = path.as_ref();
    self.file_path = Some(p.to_string_lossy().into_owned());
    self.storage_backend = StorageBackendType::Disk;
    self.inner.storage_backend(StorageBackend::Std);
    self.inner.file_path(p);
    self
  }

  /// 设置存储后端
  pub fn storage_backend(&mut self, backend: StorageBackend) -> &mut Self {
    self.storage_backend = match backend {
      StorageBackend::Memory => StorageBackendType::Memory,
      _ => StorageBackendType::Disk,
    };
    self.inner.storage_backend(backend);
    self
  }

  /// 设置是否仅使用内存缓存
  pub fn cache_only(&mut self, cache_only: bool) -> &mut Self {
    if cache_only {
      self.storage_backend = StorageBackendType::Memory;
    }
    self.inner.cache_only(cache_only);
    self
  }
}

/// Garnet 风格的存储后端类型 (1:1 对标 Garnet StorageBackendType)
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum StorageBackendType {
  /// 磁盘持久化后端 (文件系统 + 页面缓存)
  #[default]
  Disk = 0,
  /// 纯内存环形缓冲区后端 (无文件持久化)
  Memory = 1,
}

impl StorageBackendType {
  /// 转换为静态字符串切片 (const fn，供日志与错误消息使用)
  #[inline]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Disk => "Disk",
      Self::Memory => "Memory",
    }
  }

  /// 从 u8 安全转换
  #[inline]
  pub const fn from_u8(val: u8) -> Self {
    match val {
      1 => Self::Memory,
      _ => Self::Disk,
    }
  }

  /// 转换为 u8
  #[inline]
  pub const fn to_u8(self) -> u8 {
    self as u8
  }
}

impl From<StorageBackendType> for StorageBackend {
  #[inline]
  fn from(t: StorageBackendType) -> Self {
    match t {
      StorageBackendType::Disk => StorageBackend::Std,
      StorageBackendType::Memory => StorageBackend::Memory,
    }
  }
}

impl From<StorageBackend> for StorageBackendType {
  #[inline]
  fn from(b: StorageBackend) -> Self {
    match b {
      StorageBackend::Memory => StorageBackendType::Memory,
      _ => StorageBackendType::Disk,
    }
  }
}

/// BfTree 创建调优参数（0 值表示使用默认配置）
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct TreeTuning {
  /// 环形缓冲区（页缓存）大小（字节）
  pub cache_size: usize,
  /// 最小记录大小
  pub min_record_size: usize,
  /// 最大记录大小
  pub max_record_size: usize,
  /// 最大键长度
  pub max_key_len: usize,
  /// 叶子页面大小（0 = 按 max_record_size 自动推导）
  pub leaf_page_size: usize,
}

impl From<&RangeIndexStub> for TreeTuning {
  fn from(s: &RangeIndexStub) -> Self {
    Self {
      cache_size: s.cache_size as usize,
      min_record_size: s.min_record_size as usize,
      max_record_size: s.max_record_size as usize,
      max_key_len: s.max_key_len as usize,
      leaf_page_size: s.leaf_page_size as usize,
    }
  }
}

/// 读取操作返回状态码 (1:1 对标 Garnet BfTreeReadResult)
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum BfTreeReadResult {
  /// 命中有效值
  Found = 0,
  /// 键不存在
  NotFound = -1,
  /// 键已被删除 (墓碑)
  Deleted = -2,
  /// 键非法 (超出配置最大长度等)
  InvalidKey = -3,
  /// 参数非法
  InvalidArguments = -4,
}

/// 写入操作返回状态码 (1:1 对标 Garnet BfTreeInsertResult)
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum BfTreeInsertResult {
  /// 写入成功
  Success = 0,
  /// 键值违反大小限制
  InvalidKV = 1,
  /// 参数非法
  InvalidArguments = -1,
}

/// 删除操作返回状态码 (1:1 对标 Garnet BfTreeDeleteResult)
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum BfTreeDeleteResult {
  /// 删除成功
  Success = 0,
  /// 参数非法
  InvalidArguments = -1,
}

/// 扫描单条记录输出 (1:1 对标 Garnet ScanRecord)
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ScanRecord {
  /// 键切片 (若返回字段不包含 Key 则为空)
  pub key: Vec<u8>,
  /// 值切片 (若返回字段不包含 Value 则为空)
  pub value: Vec<u8>,
}

impl ScanRecord {
  /// 流式扫描的记录收集闭包工厂 (列表式扫描的单一实现)
  ///
  /// 回调逐条按实际长度 `to_vec` 入表并持续扫描；本 crate 与 wkv 会话层的
  /// 列表式扫描共用此闭包，杜绝逐调用点重复的收集逻辑
  #[inline]
  pub fn sink(records: &mut Vec<Self>) -> impl FnMut(&[u8], &[u8]) -> bool + '_ {
    |k, v| {
      records.push(Self {
        key: k.to_vec(),
        value: v.to_vec(),
      });
      true
    }
  }
}

/// 范围索引操作结果码
///
/// 在 garnet 中的相对路径:libs/server/Resp/RangeIndex/RangeIndexResult.cs:RangeIndexResult
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum RangeIndexResult {
  /// 操作成功 (RangeIndexResult.OK)
  #[default]
  Ok = 0,
  /// 键或字段不存在 (RangeIndexResult.NotFound)
  NotFound = 1,
  /// 键或字段已被逻辑删除 (RangeIndexResult.Deleted)
  Deleted = 2,
  /// 非法键（超出配置最大长度等） (RangeIndexResult.InvalidKey)
  InvalidKey = 3,
  /// 操作错误 (RangeIndexResult.Error)
  Error = 4,
  /// 内存模式不支持该操作 (RangeIndexResult.MemoryModeNotSupported)
  MemoryModeNotSupported = 5,
}

impl From<BfTreeReadResult> for RangeIndexResult {
  #[inline]
  fn from(r: BfTreeReadResult) -> Self {
    match r {
      BfTreeReadResult::Found => Self::Ok,
      BfTreeReadResult::NotFound => Self::NotFound,
      BfTreeReadResult::Deleted => Self::Deleted,
      BfTreeReadResult::InvalidKey => Self::InvalidKey,
      BfTreeReadResult::InvalidArguments => Self::Error,
    }
  }
}

impl From<BfTreeInsertResult> for RangeIndexResult {
  #[inline]
  fn from(r: BfTreeInsertResult) -> Self {
    match r {
      BfTreeInsertResult::Success => Self::Ok,
      BfTreeInsertResult::InvalidKV => Self::InvalidKey,
      BfTreeInsertResult::InvalidArguments => Self::Error,
    }
  }
}

impl From<BfTreeDeleteResult> for RangeIndexResult {
  #[inline]
  fn from(r: BfTreeDeleteResult) -> Self {
    match r {
      BfTreeDeleteResult::Success => Self::Ok,
      BfTreeDeleteResult::InvalidArguments => Self::Error,
    }
  }
}
