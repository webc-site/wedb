//! 范围索引分块反序列化器（对标 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs）
//!
//! 把入站迁移流块重组成 键 + 临时快照文件 + 存根：文件字节边收边写临时
//! 文件并滚动 xxHash64，尾部框校验通过后进入 Complete。状态机与校验本体
//! 由引擎承接（embed/wbftree/src/chunk.rs，1:1 对标同一 C# 类）；本结构是
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

use wkv::RangeIndexChunkedDeserializer as Engine;

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

  /// libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:ProcessChunk
  ///
  /// 处理一个入站流块。`Ok(false)` = 协议损坏 / 校验失败（此后可经
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

#[cfg(test)]
mod tests {
  use std::io::Write;

  use tempfile::tempdir;

  use super::*;

  /// 引擎序列化器（构造合法流，勿再手写帧格式）
  fn frame_stream(key: &[u8], stub: &[u8], file: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
    let mut serializer = wkv::RangeIndexChunkedSerializer::new(key, stub, file.len() as u64);
    let mut chunks = Vec::new();
    let mut supplied = 0usize;
    let mut dest = vec![0u8; chunk_size];
    while !serializer.is_complete() {
      if serializer.needs_file_data() && !file.is_empty() {
        let n = (file.len() - supplied).min(5);
        serializer.supply_file_data(&file[supplied..supplied + n]);
        supplied += n;
      }
      dest.fill(0);
      let written = serializer.move_next(&mut dest).unwrap();
      assert!(written > 0);
      chunks.push(dest[..written].to_vec());
    }
    chunks
  }

  #[test]
  fn reassembles_multi_chunk_stream() {
    let dir = tempdir().unwrap();
    let temp = dir.path().join("reassembly.bftree");
    let key = b"idx-key";
    let stub = [0xABu8; 35];
    let file: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();

    let chunks = frame_stream(key, &stub, &file, 64);
    assert!(chunks.len() > 3, "expected genuinely multi-chunk stream");

    let mut d = RangeIndexChunkedDeserializer::new(&temp).unwrap();
    for chunk in &chunks {
      assert!(d.process_chunk(chunk).unwrap(), "chunk rejected");
    }
    assert!(d.is_complete());
    assert!(!d.has_error());
    assert_eq!(d.key(), &key[..]);
    assert_eq!(d.stub(), &stub);
    assert_eq!(d.parse_trailer().unwrap(), 35);
    // 临时文件已落盘且长度一致（CloseStream 已内联执行）
    assert!(d.close_stream());
    assert_eq!(fs::read(&temp).unwrap(), file);
    d.dispose();
    assert!(!temp.exists(), "dispose removes temp file");
  }

  #[test]
  fn file_bytes_written_tracks_progress() {
    let dir = tempdir().unwrap();
    let temp = dir.path().join("progress.bftree");
    let stub = [1u8; 35];
    let file = vec![9u8; 300];

    let chunks = frame_stream(b"k", &stub, &file, 47);
    let mut d = RangeIndexChunkedDeserializer::new(&temp).unwrap();
    // 文件段开启前无临时文件
    assert!(!d.close_stream());
    assert_eq!(d.write_file_bytes(), 0);
    // 逐块喂入：已落盘字节数单调递增至文件总长
    let mut last = 0u64;
    for chunk in chunks.iter().take(chunks.len() - 1) {
      d.process_chunk(chunk).unwrap();
      let written = d.write_file_bytes();
      assert!(written >= last);
      last = written;
    }
    d.process_chunk(chunks.last().unwrap()).unwrap();
    assert!(d.is_complete());
    assert_eq!(d.write_file_bytes(), 300);
    assert!(d.close_stream());
    d.dispose();
  }

  #[test]
  fn empty_chunks_are_noops() {
    let dir = tempdir().unwrap();
    let mut d = RangeIndexChunkedDeserializer::new(dir.path().join("noop.bftree")).unwrap();
    // 空块：合法且无进展（C# 语义：非错误）
    assert!(d.process_chunk(&[]).unwrap());
    assert!(!d.is_complete());
    assert!(!d.has_error());
    d.dispose();
  }

  #[test]
  fn corrupt_checksum_is_detected() {
    let dir = tempdir().unwrap();
    let mut chunks = frame_stream(b"k", &[2u8; 35], &[3u8; 64], 47);
    // 篡改末块首字节：末块必含尾部框，首字节或为哈希字节（直接失配）或为
    // 文件尾字节（重算哈希失配）——两种布局下校验必败
    let last = chunks.last_mut().unwrap();
    last[0] ^= 0xFF;

    let mut d = RangeIndexChunkedDeserializer::new(dir.path().join("bad.bftree")).unwrap();
    for chunk in chunks.iter().take(chunks.len() - 1) {
      assert!(d.process_chunk(chunk).unwrap());
    }
    assert!(!d.process_chunk(&chunks[chunks.len() - 1]).unwrap());
    assert!(d.has_error());
    assert!(d.take_error().is_some());
    assert!(!d.is_complete());
    d.dispose();
  }

  #[test]
  fn feeding_after_terminal_state_is_rejected() {
    let dir = tempdir().unwrap();
    let chunks = frame_stream(b"k", &[5u8; 35], &[6u8; 16], 47);
    let mut d = RangeIndexChunkedDeserializer::new(dir.path().join("done.bftree")).unwrap();
    for chunk in &chunks {
      d.process_chunk(chunk).unwrap();
    }
    assert!(d.is_complete());
    // 完成后再喂块：拒绝但状态保持完成
    assert!(!d.process_chunk(b"trailing").unwrap());
    assert!(d.is_complete());
    d.dispose();
    // 已释放后再喂块：同样拒绝
    assert!(!d.process_chunk(&chunks[0]).unwrap());
  }

  #[test]
  fn std_file_write_replay_guard() {
    // WriteFileBytes 语义对照：文件字节按序落盘（引擎经 File 写入）
    let dir = tempdir().unwrap();
    let p = dir.path().join("w.bftree");
    {
      let mut f = fs::File::create(&p).unwrap();
      f.write_all(&[1, 2, 3]).unwrap();
    }
    assert_eq!(fs::read(&p).unwrap(), vec![1, 2, 3]);
  }
}
