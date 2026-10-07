//! 跨段寻址与扇区对齐读写主体（快慢路径）
//!
//! 对位 C# 设备基类的内联换段算术与本地设备读写主体：单偏移寻址
//! （[`SegmentedDevice::get_segment_and_offset`]）与区间切片迭代器
//! （`crate::chunk::SegmentChunks`）不是重复件，前者定位单点、
//! 后者把逻辑区间切成逐段落地的分片；对齐校验单点仍复用 `chunk::validate_aligned_io`。

use core::slice::from_raw_parts_mut;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};

use compio::fs::File;
use wbase::pool::AlignedBuf;

use super::SegmentedDevice;
use crate::{
  chunk::{SegmentChunks, segment_mask, segment_shift, validate_aligned_io},
  error::{Error, Result},
};

/// 零字节写入错误文案（写路径短写循环三处逐字共用，单点收敛）
const ZERO_BYTE_WRITE_MSG: &str = "零字节写入";

/// 设备写内核唯一系统调用点：调用线程直调 `pwrite(2)` 单笔下发
///
/// 执行位置取**调用线程同步直调**而非 compio 异步原语（与 sync 模块
/// [`super::sync_one`] 的 R1 轮裁定同构）：macOS（无 io_uring）poll driver
/// 对文件级 `WriteAt` op 判 `Decision::Blocking`——compio-driver 0.12.5 的
/// `sys/op/general/poll.rs:54` pre_submit 走 `decide_write`，而
/// `sys/pal/poll/aio.rs:90` 的 aio cfg 仅 freebsd/solarish 启用
/// （build.rs:6 `aio: { any(freebsd, solarish) }`），macOS 落 `_` 臂返回
/// `Decision::Blocking`，随后 `sys/driver/poll/mod.rs:319` 派发 AsyncifyPool
/// worker 执行、完成后经 channel + waker 两次跨线程唤醒收割——探针实测该
/// 环回税约 7.3μs/笔（individual 稳态窗，占每笔提交 27.5μs 的 26%）。
///
/// 串行化无损论证（与 sync 的「按 inode 全量」论据不同，写按区间生效，
/// 须单独论证）：生产写调用面仅两处——waof `flush_window` 与 whlog
/// `flush_range_aligned`（`Device::write_aligned` 门面的全部消费者），
/// 均为同设备**顺序单笔 await 下发**（WAL 单段尾追加；whlog 有 flush_gate
/// 写序闸），全仓无 join_all 并发写调用面；compio 形态的池并行（thread
/// 上限 256）在顺序单笔下经 rendezvous channel（bounded(0)）逐笔串行
/// handoff，并行度无从兑现。设备侧：macOS 段文件为缓冲 I/O
/// （`direct_io` 仅 Linux 探测启用），pwrite 落页缓存即返、不进设备队列，
/// 跨段串行无 NVMe 队列深度损失。故直调只删环回、不减并行。
///
/// 语义对齐 compio poll driver 的同型 op（`sys/pal/unix/mod.rs poll_io`）：
/// `EINTR` 重试（POSIX pwrite 被中断且未写任何字节时返回 -1，同 offset
/// 同缓冲重试安全），其余错误原样上抛；普通文件 pwrite 不返回
/// `WOULDBLOCK`。短写返回已写字节数、由调用方补写循环推进 offset，与
/// compio `write_at` 单笔短写返回同口径。写序契约：调用线程直调使
/// 「写完成 → 后续 sync」的先后由程序序直接保证，强于原形态经池完成
/// 回调 + waker 的跨线程 happens-before；脏段登记（写前）与在途销记
/// （守卫 Drop）不受执行位置影响。
#[cfg(unix)]
#[inline]
fn pwrite_one(file: &File, buf: &[u8], offset: u64) -> IoResult<usize> {
  use std::os::fd::AsRawFd;
  let fd = file.as_raw_fd();
  loop {
    // off_t 与 compio 同口径 as 转（段号上界经 SegmentExceeded 拦截，不超 i64）
    let r = unsafe { libc::pwrite(fd, buf.as_ptr().cast(), buf.len(), offset as libc::off_t) };
    if r >= 0 {
      return Ok(r as usize);
    }
    let err = IoError::last_os_error();
    if err.raw_os_error() == Some(libc::EINTR) {
      continue;
    }
    return Err(err);
  }
}

/// 设备读内核唯一系统调用点：调用线程直调 `pread(2)` 单笔下发
///
/// 与 [`pwrite_one`] 对偶（R6 探针坐实同款环回税：macOS poll driver 对
/// `ReadAt` 判 `Decision::Blocking` 派发 AsyncifyPool，读环回 ~9-10μs/次，
/// `read_range` 门面全程 11.4-12.0μs，直调形态 1.3μs）。读侧并发面与写侧
/// 不同（wkv 磁盘候选 join_all 扇出），直调把重叠让渡为调用线程串行：
/// macOS 缓冲 I/O 读落页缓存（命中 ~0.4μs、未命中一次介质往返），生产冷读
/// 调用面（恢复预热、扫描冷臂、缓存驱逐回源）吞吐瓶颈在介质本身而非
/// 发起端并行度，单核心串行下发不放大时延；thread-per-core 下各核独立
/// 串行，核间并行度不受影响。短读语义：普通文件 pread 仅在文件尾返回
/// 不足额（EOF 0 返回与 compio 同口径），调用方跨段循环按已读数推进。
/// `EINTR` 重试与其余错误上抛同 [`pwrite_one`] 口径。
#[cfg(unix)]
#[inline]
fn pread_one(file: &File, buf: &mut [u8], offset: u64) -> IoResult<usize> {
  use std::os::fd::AsRawFd;
  let fd = file.as_raw_fd();
  loop {
    // off_t 与 compio 同口径 as 转（段号上界经 SegmentExceeded 拦截，不超 i64）
    let r = unsafe {
      libc::pread(
        fd,
        buf.as_mut_ptr().cast(),
        buf.len(),
        offset as libc::off_t,
      )
    };
    if r >= 0 {
      return Ok(r as usize);
    }
    let err = IoError::last_os_error();
    if err.raw_os_error() == Some(libc::EINTR) {
      continue;
    }
    return Err(err);
  }
}

impl SegmentedDevice {
  /// 根据逻辑 offset 计算段编号及段内偏移
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:WriteAsync /
  /// libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:ReadAsync 的
  /// `address >> segmentSizeBits` / `& segmentSizeMask`
  /// 单点换段算术（C# 内联于 IO 入口，Rust 抽为独立查询方法）
  #[inline]
  pub(crate) fn get_segment_and_offset(&self, offset: u64) -> Result<(u32, u64)> {
    let seg_size = self.segment_size;
    let seg_id_u64 = offset >> segment_shift(seg_size);
    let seg_id = u32::try_from(seg_id_u64).map_err(|_| Error::SegmentExceeded(seg_id_u64))?;
    Ok((seg_id, offset & segment_mask(seg_size)))
  }

  /// 判断 [offset, offset+len) 是否完全落在单个段内（单段零切片快速路径判定）
  #[inline]
  fn within_single_segment(&self, offset: u64, len: usize) -> bool {
    let seg_size = self.segment_size;
    let off_in_seg = offset & segment_mask(seg_size);
    off_in_seg
      .checked_add(len as u64)
      .is_some_and(|end| end <= seg_size)
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
      // 读侧长度不强制对齐（pread 短读语义天然安全：EOF/尾零头截断由调用方
      // 处理；写侧仍按 direct_io 严格——R27 CI linux 实证恢复读逻辑尾 512
      // 被 4096 扇区校验误拒）
      if let Err(e) = validate_aligned_io(offset, target_len, &buf, sector_size, false) {
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

    // 单段快速路径：整块单次下发，避免逐段 slice 开销；
    // 按 required_len 精确封顶，杜绝池 class 圆整导致的读放大与末段幽灵段创建
    if self.within_single_segment(offset, target_len) {
      let (seg_id, start_off) = match self.get_segment_and_offset(offset) {
        Ok(v) => v,
        Err(e) => return (Err(e), buf),
      };
      // 读路径以 create=false 打开：缺失段直接 SegmentNotFound，绝不在磁盘幽灵新建
      // 段文件（对标 C# CreateReadHandle 不预分配、C++ FileSystemSegmentedFile.ReadAsync
      // 仅开已存在段；对齐 sync 的 create=false 与 recover 的幽灵段防御）
      let file = match self.get_or_open_file(seg_id, false).await {
        Ok(f) => f,
        Err(e) => return (Err(e), buf),
      };
      // SAFETY: ptr 非空且 [0, target_len) ⊆ [0, capacity) 界内（target_len 经
      // required_len 封顶），目标区间无其他持有者
      let dst = unsafe { from_raw_parts_mut(buf.as_mut_buf_ptr(), target_len) };
      // unix：pread(2) 直调（R11，环回税消尽）；windows：compio IOCP 异步读
      // （poll driver 池环回形态为 unix 特有，windows 原生完成端口无此税）
      #[cfg(unix)]
      let bytes_read = match pread_one(&file, dst, start_off) {
        Ok(n) => n,
        Err(e) => {
          unsafe { buf.set_len_unchecked(0) };
          return (Err(Error::from(e)), buf);
        }
      };
      #[cfg(windows)]
      let bytes_read = {
        // windows（IOCP）：IoBuf 需 owned 缓冲（借用跨 async 状态机不可行），
        // 临时缓冲中转一次拷贝；windows 非对基性能口径，可接受
        use compio::io::AsyncReadAt;
        // owned Vec 移入 AsyncReadAt（IoBufMut for &mut 局部借用要求
        // 'static 不可满足，唯一合法形态是值移动；BufResult.1 取回后拷出）
        let br = (&*file).read_at(vec![0u8; dst.len()], start_off).await;
        let n = match br.0 {
          Ok(n) => n,
          Err(e) => {
            unsafe { buf.set_len_unchecked(0) };
            return (Err(Error::from(e)), buf);
          }
        };
        let io_buf = br.1;
        dst[..n].copy_from_slice(&io_buf[..n]);
        n
      };
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

      // 跨段分片同样 create=false：任一缺失分片段即 SegmentNotFound，不建幽灵段。
      // 分片目的区界内构造（SegmentChunks 分片恒落 [0, target_len) ⊆
      // [0, capacity)），直调读内核（同单段臂裁定，见 pread_one 文档）
      let file = match self.get_or_open_file(chunk.seg_id, false).await {
        Ok(f) => f,
        Err(e) => {
          first_err = Some(e);
          break;
        }
      };
      // SAFETY: ptr 非空且 [buf_pos, buf_pos+len) ⊆ [0, target_len) ⊆ [0, capacity) 界内
      let dst = unsafe { from_raw_parts_mut(buf.as_mut_buf_ptr().add(chunk.buf_pos), chunk.len) };
      #[cfg(unix)]
      match pread_one(&file, dst, chunk.off_in_seg) {
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
      #[cfg(windows)]
      {
        // 同单段臂：owned 临时缓冲中转（windows IOCP 形态）
        use compio::io::AsyncReadAt;
        // owned Vec 移入（'static 约束同单段臂）
        let br = (&*file)
          .read_at(vec![0u8; chunk.len], chunk.off_in_seg)
          .await;
        let n = match br.0 {
          Ok(n) => n,
          Err(e) => {
            first_err = Some(Error::Io(e));
            break;
          }
        };
        let io_buf = br.1;
        dst[..n].copy_from_slice(&io_buf[..n]);
        total_read += n;
        if n < chunk.len {
          // 已读至文件末尾 (EOF)
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
    buf: AlignedBuf,
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
    // 长度维对齐校验按 I/O 形态分级（Direct 恒要求，缓冲放行——尾零头精确写
    // 契约见 [`Device::flush_range_aligned`]）；offset 与缓冲地址两维恒校验
    if let Err(e) = validate_aligned_io(offset, total_len, &buf, sector_size, self.direct_io()) {
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
      // 写前登记脏段（见 sync 模块）：守卫持有至本函数返回，短写补写全程在途计数
      let _dirty = self.mark_write(seg_id);
      // unix：pwrite(2) 直调（R5，环回税消尽），短写补写同线程推进；
      // windows：compio IOCP 异步写（poll driver 池环回形态为 unix 特有）
      #[cfg(unix)]
      {
        let mut written = 0usize;
        loop {
          match pwrite_one(&file, &buf[written..total_len], start_off + written as u64) {
            Ok(0) => {
              return (
                Err(Error::Io(IoError::new(
                  ErrorKind::WriteZero,
                  ZERO_BYTE_WRITE_MSG,
                ))),
                buf,
              );
            }
            Ok(n) => written += n,
            Err(e) => return (Err(Error::from(e)), buf),
          }
          if written == total_len {
            break;
          }
        }
        return (Ok(written), buf);
      }
      #[cfg(windows)]
      {
        // windows（IOCP）：owned 分片提交（借用不跨 async 状态机）
        use compio::io::AsyncWriteAt;
        let mut written = 0usize;
        while written < total_len {
          let tmp = buf[written..total_len].to_vec();
          let n = match (&*file).write_at(tmp, start_off + written as u64).await.0 {
            Ok(n) => n,
            Err(e) => return (Err(Error::from(e)), buf),
          };
          if n == 0 {
            return (
              Err(Error::Io(IoError::new(
                ErrorKind::WriteZero,
                ZERO_BYTE_WRITE_MSG,
              ))),
              buf,
            );
          }
          written += n;
        }
        return (Ok(written), buf);
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
      // 每个落地段各自写前登记；守卫随本轮分片写入结束而销记在途计数
      let _dirty = self.mark_write(chunk.seg_id);

      // unix：pread/pwrite 直调（同单段臂裁定）；windows：compio IOCP 异步写
      #[cfg(unix)]
      {
        let mut chunk_written = 0;
        while chunk_written < chunk.len {
          match pwrite_one(
            &file,
            &buf[chunk.buf_pos + chunk_written..chunk.buf_pos + chunk.len],
            chunk.off_in_seg + chunk_written as u64,
          ) {
            Ok(0) => {
              return (
                Err(Error::Io(IoError::new(
                  ErrorKind::WriteZero,
                  ZERO_BYTE_WRITE_MSG,
                ))),
                buf,
              );
            }
            Ok(n) => {
              chunk_written += n;
              total_written += n;
            }
            Err(e) => return (Err(Error::from(e)), buf),
          }
        }
      }
      #[cfg(windows)]
      {
        // windows（IOCP）：owned 分片移入 AsyncWriteAt（值语义零借用，
        // 提交后缓冲内容不再需要、随 BufResult 丢弃）
        use compio::io::AsyncWriteAt;
        let mut chunk_written = 0;
        while chunk_written < chunk.len {
          let slice = &buf[chunk.buf_pos + chunk_written..chunk.buf_pos + chunk.len];
          let br = (&*file)
            .write_at(slice.to_vec(), chunk.off_in_seg + chunk_written as u64)
            .await;
          let n = match br.0 {
            Ok(n) => n,
            Err(e) => return (Err(Error::from(e)), buf),
          };
          if n == 0 {
            return (
              Err(Error::Io(IoError::new(
                ErrorKind::WriteZero,
                ZERO_BYTE_WRITE_MSG,
              ))),
              buf,
            );
          }
          chunk_written += n;
          total_written += n;
        }
      }
    }

    (Ok(total_written), buf)
  }
}
