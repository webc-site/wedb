//! 范围索引树文件分块流：帧编解码与接收端重组
//!
//! 对标 Garnet `RangeIndexFileDataSource`/`RangeIndexFileDataSink`：
//! `.bftree` 文件序列化为定界 chunk 帧流经传输层发货，接收端按
//! [`TreeFileSink`] 重组为完整文件字节。本层只管帧协议与重组，
//! 文件枚举/落盘由上层结合 `wnode` 暴露的 RangeIndex 接口完成。

use std::result;

/// 树文件 chunk 帧布局（全小端）：
/// `[key_len u32][key][seq u32][last u8][chunk_len u32][chunk]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeChunkFrame<'a> {
  /// 索引键（树属主）
  pub key: &'a [u8],
  /// chunk 序号，从 0 连续递增
  pub seq: u32,
  /// 是否末块
  pub last: bool,
  /// chunk 字节
  pub chunk: &'a [u8],
}

/// 固定帧头字节数：key_len 4 + seq 4 + last 1 + chunk_len 4
pub const TREE_CHUNK_HEADER: usize = 13;

#[derive(Debug, thiserror::Error)]
pub enum Error {
  /// 帧字节数不足或长度前缀越界（半截帧）
  #[error("tree chunk frame truncated: need {need}, got {got}")]
  Truncated { need: usize, got: usize },
  /// chunk 序号不连续（乱序或丢帧）
  #[error("tree chunk out of order: expect {expect}, got {got}")]
  OutOfOrder { expect: u32, got: u32 },
  /// 声称末块后又收到后续帧
  #[error("tree chunk after last frame")]
  AfterLast,
  /// 重组期间收到其他索引键的帧
  #[error("tree chunk key mismatch: expect {expect:?}, got {got:?}")]
  KeyMismatch { expect: Vec<u8>, got: Vec<u8> },
}

pub type Result<T> = result::Result<T, Error>;

impl<'a> TreeChunkFrame<'a> {
  /// 编码一帧；预估容量一次分配
  pub fn encode(&self) -> Vec<u8> {
    let total = TREE_CHUNK_HEADER + self.key.len() + self.chunk.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
    buf.extend_from_slice(self.key);
    buf.extend_from_slice(&self.seq.to_le_bytes());
    buf.push(u8::from(self.last));
    buf.extend_from_slice(&(self.chunk.len() as u32).to_le_bytes());
    buf.extend_from_slice(self.chunk);
    buf
  }

  /// 借用解码一帧（零拷贝）
  pub fn decode(buf: &'a [u8]) -> Result<Self> {
    if buf.len() < TREE_CHUNK_HEADER {
      return Err(Error::Truncated {
        need: TREE_CHUNK_HEADER,
        got: buf.len(),
      });
    }
    let key_len =
      u32::from_le_bytes(unsafe { buf.get_unchecked(0..4).try_into().unwrap_unchecked() }) as usize;
    let key_end = 4 + key_len;
    let fixed_end = key_end + 9; // seq 4 + last 1 + chunk_len 4
    if buf.len() < fixed_end {
      return Err(Error::Truncated {
        need: fixed_end,
        got: buf.len(),
      });
    }
    let key = &buf[4..key_end];
    let seq = u32::from_le_bytes(unsafe {
      buf
        .get_unchecked(key_end..key_end + 4)
        .try_into()
        .unwrap_unchecked()
    });
    let last = buf[key_end + 4] != 0;
    let chunk_len = u32::from_le_bytes(unsafe {
      buf
        .get_unchecked(key_end + 5..key_end + 9)
        .try_into()
        .unwrap_unchecked()
    }) as usize;
    let chunk_end = fixed_end + chunk_len;
    if buf.len() < chunk_end {
      return Err(Error::Truncated {
        need: chunk_end,
        got: buf.len(),
      });
    }
    Ok(Self {
      key,
      seq,
      last,
      chunk: &buf[fixed_end..chunk_end],
    })
  }
}

/// 单键树文件重组器：按序接收 chunk，拼装完整文件字节
///
/// 校验三事：键一致、序号连续、末块之后无帧
#[derive(Debug, Default)]
pub struct TreeFileSink {
  key: Vec<u8>,
  expect_seq: u32,
  finished: bool,
  data: Vec<u8>,
}

impl TreeFileSink {
  /// 接收一帧；非法序列即报错
  pub fn push(&mut self, frame: TreeChunkFrame<'_>) -> Result<()> {
    if self.finished {
      return Err(Error::AfterLast);
    }
    // 先验序号连续性，再学习/比对键——首帧即乱序时报 OutOfOrder 而非
    // 以未学习键误报 KeyMismatch
    if frame.seq != self.expect_seq {
      return Err(Error::OutOfOrder {
        expect: self.expect_seq,
        got: frame.seq,
      });
    }
    if self.expect_seq == 0 {
      self.key.clear();
      self.key.extend_from_slice(frame.key);
    } else if self.key != frame.key {
      return Err(Error::KeyMismatch {
        expect: self.key.clone(),
        got: frame.key.to_vec(),
      });
    }
    self.data.extend_from_slice(frame.chunk);
    self.expect_seq += 1;
    self.finished = frame.last;
    Ok(())
  }

  /// 重组是否完成（已收到末块）
  #[inline]
  pub fn is_finished(&self) -> bool {
    self.finished
  }

  /// 取重组后的完整文件字节（ finish 前调用返回当前半成品）
  pub fn into_data(self) -> Vec<u8> {
    self.data
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn frame_round_trip() {
    let f = TreeChunkFrame {
      key: b"idx",
      seq: 3,
      last: true,
      chunk: b"payload",
    };
    let buf = f.encode();
    let got = TreeChunkFrame::decode(&buf).unwrap();
    assert_eq!(got, f);
  }

  #[test]
  fn sink_reassembles_in_order() {
    let mut sink = TreeFileSink::default();
    let parts = [
      (0u32, &b"AA"[..], false),
      (1, &b"BB"[..], false),
      (2, &b"CC"[..], true),
    ];
    for (seq, chunk, last) in parts {
      let buf = TreeChunkFrame {
        key: b"k",
        seq,
        last,
        chunk,
      }
      .encode();
      sink.push(TreeChunkFrame::decode(&buf).unwrap()).unwrap();
    }
    assert!(sink.is_finished());
    assert_eq!(sink.into_data(), b"AABBCC");
  }

  #[test]
  fn sink_rejects_gap_and_after_last() {
    let mut sink = TreeFileSink::default();
    let f0 = TreeChunkFrame {
      key: b"k",
      seq: 0,
      last: false,
      chunk: b"x",
    }
    .encode();
    let f2 = TreeChunkFrame {
      key: b"k",
      seq: 2,
      last: false,
      chunk: b"y",
    }
    .encode();
    let f1 = TreeChunkFrame {
      key: b"k",
      seq: 1,
      last: true,
      chunk: b"z",
    }
    .encode();

    sink.push(TreeChunkFrame::decode(&f0).unwrap()).unwrap();
    assert!(matches!(
      sink.push(TreeChunkFrame::decode(&f2).unwrap()),
      Err(Error::OutOfOrder { expect: 1, got: 2 })
    ));
    sink.push(TreeChunkFrame::decode(&f1).unwrap()).unwrap();
    assert!(sink.is_finished());
    assert!(matches!(
      sink.push(TreeChunkFrame::decode(&f2).unwrap()),
      Err(Error::AfterLast)
    ));
  }
}
