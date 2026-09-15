//! 范围索引分块反序列化器（对标 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs）
//!
//! 把入站迁移流块重组成 键 + 临时快照文件 + 存根：文件字节边收边写临时
//! 文件并滚动 xxHash64，尾部框校验通过后进入 Complete。状态机与校验本体
//! 由引擎承接（wedb/wbftree/src/chunk.rs，1:1 对标同一 C# 类）；本结构是
//! 会话域包装：统一错误面并承接 C# 的三个私有步骤——ParseTrailer（尾部框
//! 解析 + 校验和裁决，引擎 Trailer 阶段）、WriteFileBytes（文件字节落盘 +
//! 滚动哈希，引擎 FileData 阶段）、CloseStream（文件句柄落盘关闭，引擎在
//! FileData→WaitingForTrailer 迁移中内联执行）。
//!
//! 流格式与约束同 RangeIndexChunkedSerializer（键/文件可跨块；keyLen /
//! fileCount / 尾部框必须整体落在单块内）。

use std::{
  fs,
  path::{Path, PathBuf},
};

use wbftree::RangeIndexChunkedDeserializer as Engine;

use super::range_index_chunked_serializer::ChunkStreamError;

/// 分块反序列化器（会话域包装）
///
/// Drop 等价 C# Dispose：释放文件句柄并尽力删除临时文件
pub struct RangeIndexChunkedDeserializer(pub(super) Engine);

impl RangeIndexChunkedDeserializer {
  /// 构造：指定文件数据写入的临时目标路径
  ///
  /// libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:#ctor
  pub fn new(temp_path: impl Into<PathBuf>) -> Result<Self, ChunkStreamError> {
    Ok(Self(Engine::new(temp_path).map_err(|e| {
      ChunkStreamError::InvalidState(e.to_string())
    })?))
  }

  /// 处理一个入站流块（转发至底层 wbftree 引擎实现）。`Ok(false)` = 协议损坏 / 校验失败（此后可经
  /// [`Self::take_error`] 取走具体原因）；已完成 / 出错 / 已释放后再喂块
  /// 亦返回 `Ok(false)`
  pub fn process_chunk(&mut self, data: &[u8]) -> Result<bool, ChunkStreamError> {
    self
      .0
      .process_chunk(data)
      .map_err(|e| ChunkStreamError::InvalidState(e.to_string()))
  }

  /// libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:ParseTrailer
  ///
  /// 尾部框解析与校验和裁决（C# 私有步骤；引擎在 Trailer 阶段内联执行）。
  /// 此处以查询形态承接：流完成即代表尾部框已解析且 xxHash64 校验通过，
  /// 返回存根字节长度供调用方核对（C# stubLen != IndexSizeBytes 即判损坏）
  pub fn parse_trailer(&self) -> Result<usize, ChunkStreamError> {
    if !self.0.is_complete() {
      return Err(ChunkStreamError::InvalidState(
        "trailer not parsed: stream is not complete".to_string(),
      ));
    }
    Ok(self.0.stub().len())
  }

  /// libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:WriteFileBytes
  ///
  /// 文件字节落盘 + 滚动哈希（C# 私有步骤；引擎 FileData 阶段内联执行）。
  /// 观测形态：返回 WriteFileBytes 已写入临时文件的字节数（std File 无
  /// 用户态缓冲，元数据长度即已落盘量；文件段未开启时为 0）
  pub fn write_file_bytes(&self) -> u64 {
    fs::metadata(self.temp_path()).map(|m| m.len()).unwrap_or(0)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:CloseStream
  ///
  /// 文件句柄落盘关闭（C# 私有步骤：先置空字段再 Flush/Dispose，保证
  /// Flush 抛错也不泄漏句柄；引擎在文件段收满时内联关流）。观测形态：
  /// 返回临时文件是否已创建（文件段开启即创建，收满即关流）
  pub fn close_stream(&self) -> bool {
    self.temp_path().exists()
  }

  /// 流是否完整且校验通过
  #[inline]
  pub fn is_complete(&self) -> bool {
    self.0.is_complete()
  }

  /// 是否遭遇不可恢复错误
  #[inline]
  pub fn has_error(&self) -> bool {
    self.0.has_error()
  }

  /// 取走协议损坏的具体原因（仅 `has_error` 后有值）
  #[inline]
  pub fn take_error(&mut self) -> Option<String> {
    self.0.take_error().map(|e| e.to_string())
  }

  /// 已重组的键字节（仅 `is_complete` 后有效）
  #[inline]
  pub fn key(&self) -> &[u8] {
    self.0.key()
  }

  /// 已重组的存根字节（仅 `is_complete` 后有效）
  #[inline]
  pub fn stub(&self) -> &[u8] {
    self.0.stub()
  }

  /// 临时文件路径
  #[inline]
  pub fn temp_path(&self) -> &Path {
    self.0.temp_path()
  }

  /// libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:Dispose
  ///
  /// 释放句柄并尽力删除临时文件；幂等（Drop 自动兜底）
  pub fn dispose(&mut self) {
    self.0.dispose();
  }
}
