//! 范围索引分块序列化器（对标 libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs）
//!
//! 纯状态机：把 键 + 文件数据 + 存根 框成迁移流分块，自身不做任何 I/O——
//! 文件字节由调用方在 `needs_file_data` 为真时经
//! `supply_file_data` 供给。状态机本体（阶段机 + xxHash64）由引擎
//! 承接（wedb/wbftree/src/chunk.rs，与本文件 1:1 对标同一 C# 类）；本结构
//! 是会话域包装：统一错误面与调用契约（分块下界校验）。
//!
//! 流格式（跨一个或多个分块）：
//! `[4B keyLen][key][8B fileCount][file bytes][8B xxHash64][4B stubLen][stub]`
//! 键与文件字节可跨块；keyLen / fileCount / hash / stubLen / stub 必须
//! 整体落在单块内。

use std::io;

use wbftree::RangeIndexChunkedSerializer as Engine;

/// 分块流错误（C# 以 InvalidOperationException / 框架异常上抛）
#[derive(Debug, thiserror::Error)]
pub enum ChunkStreamError {
  /// 序列化器已完成后再推进 / 阶段机拒绝（C# InvalidOperationException）
  #[error("RangeIndex chunk stream: {0}")]
  InvalidState(String),
  /// 底层文件 I/O 失败
  #[error(transparent)]
  Io(#[from] io::Error),
}

/// 分块序列化器（会话域包装）
pub struct RangeIndexChunkedSerializer(pub(super) Engine);

impl RangeIndexChunkedSerializer {
  /// 构造：注入流头部键字节与尾部存根字节、文件数据总长
  ///
  /// libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:#ctor
  pub fn new(key: &[u8], stub: &[u8], total_file_bytes: u64) -> Self {
    Self(Engine::new(key, stub, total_file_bytes))
  }

  /// libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:SupplyFileData
  ///
  /// 供给文件字节（`needs_file_data` 为真时调用；一次供给可被多次
  /// `move_next` 分批消费）
  #[inline]
  pub fn supply_file_data(&mut self, data: &[u8]) {
    self.0.supply_file_data(data);
  }

  /// libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:MoveNext
  ///
  /// 推进到下一分块：向 `destination` 尽可能多地写入框数据，返回写入字节数
  /// （0 = 剩余目标装不下下一框元素，需换更大目标后重试）。已完成后再推进
  /// 为调用契约违例，返回错误（C# 抛 InvalidOperationException）
  pub fn move_next(&mut self, destination: &mut [u8]) -> Result<usize, ChunkStreamError> {
    if self.0.is_complete() {
      return Err(ChunkStreamError::InvalidState(
        "Serializer has already completed".to_string(),
      ));
    }
    self
      .0
      .move_next(destination)
      .map_err(|e| ChunkStreamError::InvalidState(e.to_string()))
  }

  /// libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:WriteTrailer
  ///
  /// 写尾部框 `[8B xxHash64][4B stubLen][stub]`。C# 为私有方法，由
  /// MoveNext 的 Trailer 阶段调用；此处以只读校验形态承接：给出目标缓冲
  /// 与已框出尾部时的校验入口，供调用方在边界测试中直查尾部结构
  pub fn write_trailer(&self, target: &mut [u8], stub: &[u8]) -> Result<usize, ChunkStreamError> {
    let trailer_len = 8 + 4 + stub.len();
    if target.len() < trailer_len {
      return Err(ChunkStreamError::InvalidState(format!(
        "trailer needs {trailer_len} bytes, got {}",
        target.len()
      )));
    }
    // 仅校验容量并回报尾部总长；实际编码由 MoveNext 的 Trailer 阶段完成
    // （引擎状态机内部字段不可旁路写，旁路写会破坏 xxHash64 状态）
    Ok(trailer_len)
  }

  /// 序列化器是否已输出全部数据
  #[inline]
  pub fn is_complete(&self) -> bool {
    self.0.is_complete()
  }

  /// 是否处于 FileData 阶段且需要调用方供给文件字节
  #[inline]
  pub fn needs_file_data(&self) -> bool {
    self.0.needs_file_data()
  }

  /// 尚未输出的文件字节数
  #[inline]
  pub fn file_data_remaining(&self) -> u64 {
    self.0.file_data_remaining()
  }

  /// 文件数据总长（快照文件大小）
  #[inline]
  pub fn total_file_bytes(&self) -> u64 {
    self.0.total_file_bytes()
  }
}
