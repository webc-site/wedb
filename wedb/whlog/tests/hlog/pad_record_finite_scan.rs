//! 验证畸形头在有限步内跳页返回，绝无死循环
//!
//! 验证：在内存页 offset 处写入 (info = 0, rdh = 8) 畸形头
//! （filler_words = 1, key_len = 0, val_len = 0），扫描器在有限步内跳页返回，绝不挂死
//!
//! 自研回归锁: PadRecord 填充记录有限扫描（C# 对应 LogRecord Pad 面本仓缺陷防御）

use std::{
  ptr::{write_bytes, write_unaligned},
  sync::Arc,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{DEFAULT_INITIAL_ADDRESS, HybridLog, HybridLogConfig};

#[test]
fn test_abnormal_filler_header_finite_scan_skips_page() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("finite_scan.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let page_size = 64 * 1024;
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let k1 = b"test:key1";
    let v1 = b"value1";
    let (addr1, _) = hlog.append(k1, v1, 0, false)?;
    assert_eq!(addr1, DEFAULT_INITIAL_ADDRESS);

    // 获取当前尾部逻辑地址及页内偏移
    let tail = hlog.tail_address();
    let offset = (tail % page_size as u64) as usize;
    let page_id = tail / page_size as u64;

    // 直接在内存页尾部写入畸形头：info = 0, rdh = 8 (u64 小端)
    // 对应 filler_words = 1, key_len = 0, val_len = 0
    unsafe {
      let slot = hlog.buffer.page_idx(page_id);
      let page_ptr = hlog.buffer.raw_page_ptr_mut(slot);
      let header_ptr = page_ptr.add(offset);
      // 前 8 字节 info = 0
      write_bytes(header_ptr, 0, 8);
      // 后 8 字节 rdh = 8 (8u64 小端)
      let rdh_ptr = header_ptr.add(8) as *mut u64;
      write_unaligned(rdh_ptr, 8u64.to_le());
    }

    // 设置扫描目标跨越该页到下一页
    let scan_end = (page_id + 1) * page_size as u64 + 1024;
    let mut scan = hlog.scan_iter(addr1, scan_end);

    // 第一条记录能正常读出
    let first = scan.next().await?;
    assert!(first.is_some());
    let (addr, rec) = first.unwrap();
    assert_eq!(addr, addr1);
    assert_eq!(rec.key()?, k1);

    // 第二次调用 next() 会遇到 (info = 0, rdh = 8)
    // 扫描器在至多一轮复核后跳页，并在有限步内正常返回 None，绝不卡死
    let second = scan.next().await?;
    assert!(second.is_none());

    aok::Result::<()>::Ok(())
  })?;

  OK
}
