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
  /// 请求的起始位点已被环形截断物理回收：扫描器会把起点钳制到 begin 静默
  /// 丢掉 `[from, begin)`，副本却按从 `from` 起续传记账，产生不可恢复的
  /// 位点错位——显式拒绝并要求全量重同步（对标 C# 主端对过期 AOF 位点
  /// 回全量同步指示而非部分发货的语义）
  #[error("aof range expired: from {from:#x} already truncated (begin {begin:#x}), full resync required")]
  Expired { from: u64, begin: u64 },
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
  /// `from` 已被环形截断回收时报 [`Error::Expired`]，绝不静默缺帧；
  /// 环形缓冲覆写导致扫描提前终止时同样显式报错，位点不推进
  pub async fn ship_since(&self, from: u64) -> Result<u64> {
    let committed = self.wal.committed_until_address();
    let begin = self.wal.begin_address();
    if from < begin {
      return Err(Error::Expired { from, begin });
    }
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

  /// 未提交的尾部数据不发货
  #[test]
  fn skips_uncommitted_tail() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let dir = tempdir()?;
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("sync.wal"))?);
      let wal = Arc::new(WalLog::new(device, WalConfig::default())?);

      let a = wnode::encode_entry(wnode::AofOp::RiCreate, 1, b"k1", b"blob-a");
      let addr_a = wal.enqueue(&a)?;
      wal.commit().await?;
      // 已提交后追加第二条，但不提交：处于未提交尾部
      let b = wnode::encode_entry(wnode::AofOp::RiSet, 2, b"k1", b"blob-b");
      wal.enqueue(&b)?;

      let transport = Arc::new(MemoryTransport::default());
      let driver = AofSyncDriver::new(Arc::clone(&wal), Arc::clone(&transport));
      let next = driver.ship_since(addr_a).await?;
      let sent = transport.0.lock();
      assert_eq!(sent.len(), 1, "未提交尾部不得越界发货");
      assert_eq!(sent[0], a);
      assert_eq!(next, wal.committed_until_address(), "位点止于已提交边界");
      aok::OK
    })
  }

  /// 起始位点已被环形截断回收：显式报 Expired 拒绝发货，绝不静默缺帧
  #[test]
  fn rejects_truncated_range() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let dir = tempdir()?;
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("sync.wal"))?);
      let wal = Arc::new(WalLog::new(device, WalConfig::default())?);

      let a = wnode::encode_entry(wnode::AofOp::RiCreate, 1, b"k1", b"blob-a");
      let addr_a = wal.enqueue(&a)?;
      let b = wnode::encode_entry(wnode::AofOp::RiSet, 2, b"k1", b"blob-b");
      let addr_b = wal.enqueue(&b)?;
      wal.commit().await?;

      // 物理截断越过 addr_a，[addr_a, addr_b) 区间已不可得
      wal.truncate(addr_b).await?;
      assert!(wal.begin_address() >= addr_b);

      let transport = Arc::new(MemoryTransport::default());
      let driver = AofSyncDriver::new(Arc::clone(&wal), Arc::clone(&transport));
      let err = driver.ship_since(addr_a).await.unwrap_err();
      assert!(matches!(err, Error::Expired { .. }));
      assert!(transport.0.lock().is_empty(), "过期区间不得发出任何帧");
      // 合法区间（>= begin）仍可正常续传
      assert_eq!(driver.ship_since(addr_b).await?, wal.committed_until_address());
      aok::OK
    })
  }
}
