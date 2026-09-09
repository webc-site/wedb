//! 主从 AOF 同步驱动
//!
//! 对标 Garnet `AofSyncDriver`：副本接入后从指定地址起顺序发货已提交
//! WAL 记录。每条记录的 AOF 条目帧原样透传（含 [`wnode::AofEntryRef`]
//! 头），副本端经 [`wnode::Replay`] 语义回放。传输介质抽象为
//! [`AofTransport`]——TCP/QUIC/共享内存各自实现，本层只关心顺序与位点。

use std::{io, result, sync::Arc};

use thiserror::Error as ThisError;
use wnode::{Device, WalError, WalLog};

#[derive(Debug, ThisError)]
pub enum Error {
  /// WAL 扫描错误
  #[error(transparent)]
  Wal(#[from] WalError),
  /// 传输层 IO 错误
  #[error(transparent)]
  Io(#[from] io::Error),
  /// 发货期间未提交数据被环形覆写，扫描提前终止（位点不推进，需全量重同步）
  #[error("wal overwritten while shipping: {skipped} record(s) lost")]
  Overwritten { skipped: u64 },
}

pub type Result<T> = result::Result<T, Error>;

/// AOF 帧传输介质抽象
///
/// 实现方保证帧按调用顺序到达副本端；帧内容为一整条 WAL 记录的
/// AOF 条目字节（调用方不再切片）
pub trait AofTransport {
  /// 发送一帧到副本端
  fn send_frame(&self, frame: &[u8]) -> impl Future<Output = io::Result<()>> + Send;
}

/// 共享句柄透传：`Arc<T>` 自动获得传输能力
impl<T: AofTransport + ?Sized> AofTransport for Arc<T> {
  fn send_frame(&self, frame: &[u8]) -> impl Future<Output = io::Result<()>> + Send {
    (**self).send_frame(frame)
  }
}

/// 主端同步驱动：从 `from` 地址起把已提交 WAL 流发货到传输介质
///
/// 返回发货完成后的下一地址，供断线重连时续传（对标
/// ReplicationPrimaryAofSync 的位点推进语义）
pub struct AofSyncDriver<D: Device, T: AofTransport> {
  wal: Arc<WalLog<D>>,
  transport: T,
}

impl<D: Device, T: AofTransport> AofSyncDriver<D, T> {
  /// 组装驱动（WAL 句柄与传输介质由调用方装配）
  pub fn new(wal: Arc<WalLog<D>>, transport: T) -> Self {
    Self { wal, transport }
  }

  /// 发货 `[from, committed_until)` 区间的已提交记录
  ///
  /// 区间为空时返回 `from` 本身；未提交的尾部数据不发货。
  /// 环形缓冲覆写导致扫描提前终止时显式报错，位点不推进——
  /// 缺数据必须显式暴露，绝不容静默缺帧
  pub async fn ship_since(&self, from: u64) -> Result<u64> {
    let committed = self.wal.committed_until_address();
    if from >= committed {
      return Ok(from);
    }

    let mut iter = self.wal.scan(from, committed);
    while let Some(record) = iter.next().await? {
      self.transport.send_frame(&record.payload).await?;
    }
    let overwritten = iter.overwritten_skips();
    if overwritten != 0 {
      return Err(Error::Overwritten {
        skipped: overwritten,
      });
    }
    Ok(iter.current_address())
  }
}

#[cfg(test)]
mod tests {
  use std::io;

  use compio::runtime::Runtime;
  use parking_lot::Mutex;
  use tempfile::tempdir;
  use wdev::SegmentedDevice;
  use wnode::WalConfig;

  use super::*;

  #[derive(Default)]
  struct MemoryTransport(Mutex<Vec<Vec<u8>>>);

  impl AofTransport for MemoryTransport {
    async fn send_frame(&self, frame: &[u8]) -> io::Result<()> {
      self.0.lock().push(frame.to_vec());
      Ok(())
    }
  }

  #[test]
  fn ship_committed_range_in_order() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let dir = tempdir()?;
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("sync.wal"))?);
      let wal = Arc::new(WalLog::new(device, WalConfig::default())?);

      let a = wnode::encode_entry(wnode::AofOp::RiCreate, 1, b"k1", b"blob-a");
      let b = wnode::encode_entry(wnode::AofOp::RiSet, 2, b"k1", b"blob-b");
      let addr_a = wal.enqueue(&a)?;
      wal.enqueue(&b)?;
      wal.commit().await?;

      let transport = Arc::new(MemoryTransport::default());
      let driver = AofSyncDriver::new(Arc::clone(&wal), Arc::clone(&transport));
      let next = driver.ship_since(addr_a).await?;

      {
        let sent = transport.0.lock();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0], a);
        assert_eq!(sent[1], b);
      }
      // 位点推进到已提交尾部，重复发货为空
      assert_eq!(next, wal.committed_until_address());
      assert_eq!(driver.ship_since(next).await?, next);
      aok::OK
    })
  }
}
