//! 迁移分块记录重组器
//!
//! 在 garnet 中的相对路径: libs/cluster/Session/ChunkedRecordReassembler.cs
//!
//! 单条超过发送缓冲内容上限的迁移记录按上限切块发送（kind=CHUNKED 帧，续块
//! 高位标志置位直至末块），各 chunk 载荷顺序拼接即完整单条记录编码
//! `[u8 kind][body]`；重组完成后由接收端按既有单记录解码路径处理。一个实例
//! 服务一条迁移流（同源节点的 MIGRATE/SYNC 帧停等串行，chunk 可跨多个命令载荷
//! 分布，与 C# per-connection 重组状态同构）。
//!
//! C# 侧按记录数据头把字节流路由到 inline/overflow 双槽位的状态机（Phase 枚举与
//! FillInline / FillOverflow / TryReadLengthPrefix 等游标方法）不转写：rust 迁移链
//! 只把 chunk 拼成完整单条记录编码，随后统一交给既有单记录解码路径，无组件级
//! 落位需求，故本器仅保留单段 `Vec<u8>` 累加模型（缺口登记于
//! js/check/ignore/garnet/libs/cluster/Session/ChunkedRecordReassembler.yml）。

use std::mem::take;

/// 迁移分块记录重组器（单流累积：chunk 载荷顺序拼接，末块即完整记录）
#[derive(Default, Debug, Clone)]
pub struct ChunkReassembler {
  /// 累积中的记录流（末块到达即产出并清空）
  stream: Vec<u8>,
}

impl ChunkReassembler {
  /// libs/cluster/Session/ChunkedRecordReassembler.cs:ChunkedRecordReassembler
  pub fn new() -> Self {
    Self::default()
  }

  /// 追加一个 chunk 载荷；`more` 为 true 表示后续还有本记录的 chunk，
  /// 末块（more = false）到达时返回完整拼接记录并复位，等待下一条记录
  ///
  /// libs/cluster/Session/ChunkedRecordReassembler.cs:Append
  #[inline]
  pub fn append(&mut self, chunk: &[u8], more: bool) -> Option<Vec<u8>> {
    self.process(chunk);
    (!more).then(|| take(&mut self.stream))
  }

  /// 驱动流数据载荷吸纳
  ///
  /// libs/cluster/Session/ChunkedRecordReassembler.cs:Process
  #[inline]
  pub fn process(&mut self, data: &[u8]) {
    self.stream.extend_from_slice(data);
  }

  /// 丢弃半途状态（错误恢复：远端已拒批，流残段不得污染后续记录）
  ///
  /// libs/cluster/Session/ChunkedRecordReassembler.cs:Reset
  #[inline]
  pub fn reset(&mut self) {
    self.stream.clear();
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn chunk_reassembler_appends_until_final_chunk() {
    let mut r = ChunkReassembler::new();
    assert!(r.append(b"ab", true).is_none());
    assert!(r.append(b"cd", true).is_none());
    assert_eq!(r.append(b"ef", false), Some(b"abcdef".to_vec()));
    // 末块后复位：下一条记录从空流开始
    assert!(r.append(b"z", false).is_some());
  }

  #[test]
  fn chunk_reassembler_reset_drops_partial_stream() {
    let mut r = ChunkReassembler::new();
    assert!(r.append(b"half", true).is_none());
    r.reset();
    assert_eq!(r.append(b"ok", false), Some(b"ok".to_vec()));
  }
}
