//! 跨段寻址与扇区对齐读写主体（快慢路径）
//!
//! 对位 C# 设备基类的内联换段算术与本地设备读写主体：单偏移寻址
//! （[`SegmentedDevice::get_segment_and_offset`]）与区间切片迭代器
//! （`crate::chunk::SegmentChunks`）不是重复件，前者定位单点、
//! 后者把逻辑区间切成逐段落地的分片；对齐校验单点仍复用 `chunk::validate_aligned_io`。

use std::io::{Error as IoError, ErrorKind};

use compio::{
  buf::{BufResult, IntoInner, IoBuf},
  io::{AsyncReadAt, AsyncWriteAt},
};
use wbase::pool::AlignedBuf;

use super::SegmentedDevice;
use crate::{
  chunk::{SegmentChunks, segment_mask, segment_shift, validate_aligned_io},
  error::{Error, Result},
};

impl SegmentedDevice {
  /// 根据逻辑 offset 计算段编号及段内偏移
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:WriteAsync /
  /// StorageDeviceBase.cs:ReadAsync 的 `address >> segmentSizeBits` / `& segmentSizeMask`
  /// 单点换段算术（C# 内联于 IO 入口，Rust 抽为独立查询方法）
  #[inline]
  pub fn get_segment_and_offset(&self, offset: u64) -> Result<(u32, u64)> {
    match self.segment_size {
      Some(seg_size) => {
        let seg_id_u64 = offset >> segment_shift(seg_size);
        let seg_id = u32::try_from(seg_id_u64).map_err(|_| Error::SegmentExceeded(seg_id_u64))?;
        Ok((seg_id, offset & segment_mask(seg_size)))
      }
      None => Ok((0, offset)),
    }
  }

  /// 判断 [offset, offset+len) 是否完全落在单个段内（单段零切片快速路径判定）
  #[inline]
  fn within_single_segment(&self, offset: u64, len: usize) -> bool {
    match self.segment_size {
      None => true,
      Some(seg_size) => {
        let off_in_seg = offset & segment_mask(seg_size);
        off_in_seg
          .checked_add(len as u64)
          .is_some_and(|end| end <= seg_size)
      }
    }
  }

  /// 读取统一内核：`aligned` 为 true 时执行扇区对齐校验（Direct I/O 专用），
  /// 为 false 时按任意逻辑范围直读（缓冲 I/O 专用）；单段/跨段切片与收割逻辑共享
  pub(super) async fn read_impl(
    &self,
    offset: u64,
    mut buf: AlignedBuf,
    aligned: bool,
  ) -> (Result<usize>, AlignedBuf) {
    let sector_size = self.sector_size;
    // 读长度按租借时的请求需求封顶（非池化缓冲区即容量），杜绝 class 圆整导致的读放大
    let target_len = buf.required_len().min(buf.capacity());
    if aligned {
      if let Err(e) = validate_aligned_io(offset, target_len, &buf, sector_size) {
        return (Err(e), buf);
      }
    } else if offset.checked_add(target_len as u64).is_none() {
      return (
        Err(Error::OutOfBounds {
          offset,
          len: target_len,
        }),
        buf,
      );
    }
    if target_len == 0 {
      return (Ok(0), buf);
    }

    // 单段快速路径：整块直接异步读取，避免逐段 slice 开销；
    // 按 required_len 精确封顶，杜绝池 class 圆整导致的读放大与末段幽灵段创建
    if self.within_single_segment(offset, target_len) {
      let (seg_id, start_off) = match self.get_segment_and_offset(offset) {
        Ok(v) => v,
        Err(e) => return (Err(e), buf),
      };
      let file = match self.get_or_open_file(seg_id, true).await {
        Ok(f) => f,
        Err(e) => return (Err(e), buf),
      };
      let slice = buf.slice(0..target_len);
      let BufResult(res, slice) = file.read_at(slice, start_off).await;
      buf = slice.into_inner();
      let bytes_read = match res {
        Ok(n) => n,
        Err(e) => {
          unsafe { buf.set_len_unchecked(0) };
          return (Err(Error::from(e)), buf);
        }
      };
      // 成功路径 compio 已借 SetLen 回写长度，此处显式收口保证口径一致
      unsafe { buf.set_len_unchecked(bytes_read) };
      return (Ok(bytes_read), buf);
    }

    // 跨段读取慢路径：一次性暴露请求长度以支持逐段 slice，收尾时统一收缩到实际读到的长度
    unsafe { buf.set_len_unchecked(target_len) };

    let mut total_read = 0;
    let mut first_err = None;
    for chunk in SegmentChunks::new(offset, target_len, self.segment_size) {
      let chunk = match chunk {
        Ok(c) => c,
        Err(e) => {
          first_err = Some(e);
          break;
        }
      };

      let file = match self.get_or_open_file(chunk.seg_id, true).await {
        Ok(f) => f,
        Err(e) => {
          first_err = Some(e);
          break;
        }
      };

      // slice/into_inner 不改变父缓冲区长度，循环内无需重复 set_len
      let slice = buf.slice(chunk.buf_pos..chunk.buf_pos + chunk.len);
      let BufResult(res, slice) = file.read_at(slice, chunk.off_in_seg).await;
      buf = slice.into_inner();

      match res {
        Ok(n) => {
          total_read += n;
          if n < chunk.len {
            // 已读至文件末尾 (EOF)
            break;
          }
        }
        Err(e) => {
          first_err = Some(Error::Io(e));
          break;
        }
      }
    }

    unsafe { buf.set_len_unchecked(total_read) };
    match first_err {
      Some(e) => (Err(e), buf),
      None => (Ok(total_read), buf),
    }
  }

  /// 写入统一内核：扇区对齐校验 + 单段快路径 + `SegmentChunks` 跨段慢路径
  ///
  /// 对标 C# 本地设备的句柄使用与短写补写；由 `Device::write_aligned` 门面转发，
  /// 无第二实现点。
  pub(super) async fn write_impl(
    &self,
    offset: u64,
    mut buf: AlignedBuf,
  ) -> (Result<usize>, AlignedBuf) {
    let sector_size = self.sector_size;
    let total_len = buf.len();
    if self.read_only {
      return (
        Err(Error::ReadOnly {
          offset,
          len: total_len,
        }),
        buf,
      );
    }
    // 单文件模式容量为硬上界：offset + len 越界即拒绝。与 C# 的差异论证：C#
    // StorageDeviceBase.HandleCapacity 在 segmentSize = -1（单文件）时
    // `Capacity >> segmentSizeBits`（bits=64）位移按 C# 语义归约为 >> 0，
    // newStartSegment 为巨负数使单调推进失效，容量形同虚设、写入无限增长；
    // Rust 补齐该语义空洞，以显式越界拒绝背书"容量即物理上限"承诺。
    // 分段模式容量不走此分支：经 handle_capacity 逐出最老段腾挪（对标 HandleCapacity）
    if let Some(cap) = self.capacity
      && self.segment_size.is_none()
      && offset.saturating_add(total_len as u64) > cap
    {
      return (
        Err(Error::OutOfBounds {
          offset,
          len: total_len,
        }),
        buf,
      );
    }
    if let Err(e) = validate_aligned_io(offset, total_len, &buf, sector_size) {
      return (Err(e), buf);
    }
    if total_len == 0 {
      return (Ok(0), buf);
    }

    // 单段快速路径：整块直接异步写入，避免 slice 开销
    if self.within_single_segment(offset, total_len) {
      let (seg_id, start_off) = match self.get_segment_and_offset(offset) {
        Ok(v) => v,
        Err(e) => return (Err(e), buf),
      };
      if let Err(e) = self.handle_capacity(seg_id).await {
        return (Err(e), buf);
      }
      let file = match self.get_or_open_file(seg_id, true).await {
        Ok(f) => f,
        Err(e) => return (Err(e), buf),
      };
      let mut file_ref = &*file;
      let BufResult(res, mut cur_buf) = file_ref.write_at(buf, start_off).await;
      match res {
        Ok(n) if n == total_len => {
          #[cfg(debug_assertions)]
          self.debug_mark_dirty(seg_id);
          return (Ok(n), cur_buf);
        }
        Ok(0) => {
          return (
            Err(Error::Io(IoError::new(ErrorKind::WriteZero, "零字节写入"))),
            cur_buf,
          );
        }
        Ok(mut written) => {
          // 短写补写循环：切片递进直至写满或报错
          while written < total_len {
            let slice = cur_buf.slice(written..total_len);
            let mut file_ref = &*file;
            let BufResult(res, slice) = file_ref.write_at(slice, start_off + written as u64).await;
            cur_buf = slice.into_inner();
            match res {
              Ok(0) => {
                return (
                  Err(Error::Io(IoError::new(ErrorKind::WriteZero, "零字节写入"))),
                  cur_buf,
                );
              }
              Ok(n) => written += n,
              Err(e) => return (Err(Error::Io(e)), cur_buf),
            }
          }
          #[cfg(debug_assertions)]
          self.debug_mark_dirty(seg_id);
          return (Ok(written), cur_buf);
        }
        Err(e) => return (Err(Error::from(e)), cur_buf),
      }
    }

    // 跨段写入慢路径：基于 SegmentChunks 进行流式分片写入
    let mut total_written = 0;
    for chunk in SegmentChunks::new(offset, total_len, self.segment_size) {
      let chunk = match chunk {
        Ok(c) => c,
        Err(e) => return (Err(e), buf),
      };
      if let Err(e) = self.handle_capacity(chunk.seg_id).await {
        return (Err(e), buf);
      }
      let file = match self.get_or_open_file(chunk.seg_id, true).await {
        Ok(f) => f,
        Err(e) => return (Err(e), buf),
      };

      let mut chunk_written = 0;
      while chunk_written < chunk.len {
        let slice = buf.slice(chunk.buf_pos + chunk_written..chunk.buf_pos + chunk.len);
        let mut file_ref = &*file;
        let BufResult(res, slice) = file_ref
          .write_at(slice, chunk.off_in_seg + chunk_written as u64)
          .await;
        buf = slice.into_inner();

        match res {
          Ok(0) => {
            return (
              Err(Error::Io(IoError::new(ErrorKind::WriteZero, "零字节写入"))),
              buf,
            );
          }
          Ok(n) => {
            chunk_written += n;
            total_written += n;
          }
          Err(e) => return (Err(Error::Io(e)), buf),
        }
      }
      #[cfg(debug_assertions)]
      self.debug_mark_dirty(chunk.seg_id);
    }

    (Ok(total_written), buf)
  }
}
