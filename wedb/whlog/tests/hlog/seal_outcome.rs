//! try_seal_record 密封三态结果与不可密封注入形（票 wkv-reviv-pool-transfer-seal-discard-unsealed-pooling）
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs
//! （C# SealAndInvalidate 纯内存单原子字、结构上无失败面，TrySeal 仅 bool「已密封回
//! false」单失败态；rust 页驻留引入真实失败面后折为三态：Sealed / AlreadySealed /
//! NotSealable。本文件锁三态可分辨与页未就绪、头部越界、解码失败三类注入形，供
//! wkv 归池单点门控 [`WedbStore::transfer_to_reviv_pool`] 消费）

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{HybridLog, HybridLogConfig, RevivifyArgs, SealOutcome};
use wrecord::{HEADER_SIZE, RecordHeader};

const PAGE_SIZE: usize = 64 * 1024;

/// 测试用混合日志装配（单文件设备 + 16 页环形缓冲）
fn new_hlog(dir: &tempfile::TempDir, name: &str) -> whlog::Result<HybridLog<SegmentedDevice>> {
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  HybridLog::new(
    HybridLogConfig::new(PAGE_SIZE, 16, 0.5)?,
    device,
    Arc::new(LightEpoch::new(16)),
  )
}

/// 页内地址的记录头内省（断言 SEALED 位落笔用）
fn header_at(hlog: &HybridLog<SegmentedDevice>, addr: u64) -> Option<RecordHeader> {
  let page_id = hlog.config.page_id(addr);
  if !hlog.buffer.is_page_loaded(page_id) {
    return None;
  }
  let guard = hlog.buffer.read_page(page_id);
  RecordHeader::decode_opt(&guard[hlog.config.page_offset(addr)..])
}

/// 三态可分辨：未密封首封回 Sealed，已密封再封幂等回 AlreadySealed（两态均确认
/// Closed），与不可密封注入形（NotSealable）互斥可辨
#[test]
fn test_seal_outcome_sealed_and_idempotent() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let hlog = new_hlog(&dir, "seal_states.db")?;

    let key = b"seal";
    let (addr, _) = hlog.append(key, b"payload", 0, false)?;

    assert!(
      !header_at(&hlog, addr).unwrap().is_closed(),
      "前置条件：新追加记录必须未密封"
    );
    assert_eq!(hlog.try_seal_record(addr), SealOutcome::Sealed);
    assert!(
      header_at(&hlog, addr).unwrap().is_closed(),
      "SEALED 位必须已原子落笔"
    );

    // 幂等：已密封槽位再封不破坏旧标记，且与「不可密封」可分辨（归池门控判据）
    assert_eq!(hlog.try_seal_record(addr), SealOutcome::AlreadySealed);
    assert!(hlog.try_seal_record(addr).is_closed());

    info!("密封三态可分辨测试通过");
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 不可密封三注入形：页未就绪 / 头部越出页界 / 头部解码失败——恒回 NotSealable，
/// 绝不折入「已密封」（否则未闭合槽位将被误判可入池）
#[test]
fn test_seal_outcome_not_sealable() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let hlog = new_hlog(&dir, "seal_not_sealable.db")?;

    let (addr, _) = hlog.append(b"ns", b"payload", 0, false)?;

    // 1. 页未就绪：环形页缓冲仅装载尾页，下一页基址（页滑窗驱逐对位形态）未装载
    let next_page = PAGE_SIZE as u64;
    assert!(!hlog.buffer.is_page_loaded(hlog.config.page_id(next_page)));
    assert_eq!(hlog.try_seal_record(next_page), SealOutcome::NotSealable);

    // 2. 头部越出页界：页尾仅余半头，容不下 16 字节完整头
    let overrun = PAGE_SIZE - 8;
    assert!(overrun + HEADER_SIZE > PAGE_SIZE);
    assert_eq!(
      hlog.try_seal_record(overrun as u64),
      SealOutcome::NotSealable
    );

    // 3. 头部解码失败：槽位头声明键 8 字节 + 值 u32::MAX，物理尺寸恒越出页内剩余
    let garbage_addr = hlog.tail_address();
    let garbage = RecordHeader::new(0, 8, u32::MAX, false)?.to_bytes();
    {
      let page_id = hlog.config.page_id(garbage_addr);
      let offset = hlog.config.page_offset(garbage_addr);
      let mut guard = hlog.buffer.write_page(page_id);
      guard[offset..offset + HEADER_SIZE].copy_from_slice(&garbage);
    }
    assert_eq!(hlog.try_seal_record(garbage_addr), SealOutcome::NotSealable);

    // 对照：合法槽位三态判定不受污染
    assert_eq!(hlog.try_seal_record(addr), SealOutcome::Sealed);

    info!("不可密封注入形测试通过");
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// Pad 切分块特化臂：分裂切出块首封回 Sealed、再封幂等回 AlreadySealed——「归池
/// 槽位恒处 Closed 态」不变量覆盖复活池合法驻留的 Pad 形态
#[test]
fn test_seal_outcome_pad_split_block() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let hlog = new_hlog(&dir, "seal_pad.db")?;

    // 4096 字节整槽复活写入 24 字节记录：富余 4072 超填充表示上限，保留 512 字节
    // 松弛填充后切出 3560 字节 Pad 块（坐标与 inplace_lifecycle 同形断言一致）
    let big_val = vec![b'G'; 4077];
    let (addr, _) = hlog.append(b"g", &big_val, 0, false)?;
    let (pad_addr, pad_size) = hlog
      .revivify_record_at(&RevivifyArgs {
        addr,
        slot_size: 4096,
        key: b"new",
        val: b"value",
        prev_addr: 0,
        is_tombstone: false,
        in_new_version: false,
      })?
      .expect("富余超上限必须分裂并报出切出块");
    assert_eq!((pad_addr, pad_size), (addr + 536, 3560));

    assert_eq!(hlog.try_seal_record(pad_addr), SealOutcome::Sealed);
    assert_eq!(hlog.try_seal_record(pad_addr), SealOutcome::AlreadySealed);

    info!("Pad 切分块密封特化臂测试通过");
    aok::Result::<()>::Ok(())
  })?;
  OK
}
