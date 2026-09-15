use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::{Device, SegmentedDevice};
use wepoch::LightEpoch;
use whlog::{HybridLog, HybridLogConfig, RecordOutput, SECTOR_ALIGNMENT};
use wrecord::HEADER_SIZE;

use super::support::CountingDevice;

/// 测试 26: 点读冷路径整页磁盘读缓存——同页多条记录零重复 I/O 与跨页边界正确性
#[test]
fn test_disk_read_page_cache() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_page_cache.db");
    let device = Arc::new(CountingDevice::new(SegmentedDevice::single_file(&db_path)?));
    let epoch = Arc::new(LightEpoch::new(16));

    // 4KB 最小页：精细排布记录边界，覆盖 ~1KB 小记录、紧贴页尾记录与跨页边界
    let page_size = SECTOR_ALIGNMENT;
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config, device.clone(), epoch)?;

    // 第 0 页：7 条 512 字节记录（16 头 + 8 键 + 488 值）+ 1 条恰好抵达页尾的记录
    //（64 起始 + 7*512 + 448 = 4096，紧贴页尾零空隙，不产生换页 Pad）
    let mut recs: Vec<(u64, Vec<u8>, Vec<u8>)> = Vec::new();
    for i in 0..7u8 {
      let key = format!("k:{i:06}").into_bytes();
      let val = vec![b'a' + i; 488];
      let addr = hlog.append(&key, &val, 0, false)?;
      recs.push((addr, key, val));
    }
    let tail_key = b"tail:000".to_vec();
    let tail_val = vec![b'T'; 424]; // 16 + 8 + 424 = 448，恰好填满 [3648, 4096)
    let tail_addr = hlog.append(&tail_key, &tail_val, 0, false)?;
    assert_eq!(tail_addr + 448, page_size as u64, "记录必须恰好抵达页尾");
    recs.push((tail_addr, tail_key.clone(), tail_val.clone()));

    // 第 1 页第一条（跨页边界：上页页尾最后一条 + 下页第一条）+ 恰好填满该页的收尾记录
    let page1_end = 2 * page_size as u64;
    let c_key = b"c:first1".to_vec();
    let c_val = vec![b'c'; 100];
    let c_addr = hlog.append(&c_key, &c_val, 0, false)?;
    assert_eq!(c_addr, page_size as u64, "页尾满后新记录必须落在下一页开头");
    // 收尾记录（16 + 8 + 3944 = 3968 对齐逻辑尺寸），与首条（对齐 128）合计 4096，
    // 恰好填满 [4224, 8192)
    let d_val = vec![b'd'; page_size - 152];
    let d_addr = hlog.append(b"d:fill00", &d_val, 0, false)?;
    assert_eq!(d_addr + 3968, page1_end, "第 1 页必须被记录精确填满");

    // 第 2 页（槽位与第 0 页直接映射冲突，验证驱逐与重装载正确性）
    let page2_end = 3 * page_size as u64;
    let e_key = b"e:first2".to_vec();
    let e_val = vec![b'e'; 100];
    let e_addr = hlog.append(&e_key, &e_val, 0, false)?;
    assert_eq!(e_addr, page2_end - page_size as u64);
    let f_val = vec![b'f'; page_size - 152];
    let f_addr = hlog.append(b"f:fill00", &f_val, 0, false)?;
    assert_eq!(f_addr + 3968, page2_end);

    // 三页全部整页落盘（整页刷满才满足缓存装载门槛），并驱逐至磁盘区
    for p in 0..3u64 {
      hlog.flush_page(p).await?;
    }
    hlog.shift_read_only_address(page2_end);
    hlog.shift_head_address(page2_end);
    assert!(hlog.is_on_disk(tail_addr));

    /// 单条冷读回验：Disk 形态 + 键值逐字节一致
    async fn assert_disk_read<D: Device>(
      hlog: &HybridLog<D>,
      addr: u64,
      key: &[u8],
      val: &[u8],
    ) -> Void {
      let out = hlog.read_disk_record(addr).await?;
      assert!(matches!(out, RecordOutput::Disk(_)));
      assert_eq!(out.key()?, key);
      assert_eq!(out.value()?, val);
      OK
    }

    // 第 2 页先读：首条仅 probe 不装载（消费启发式初值），次条同页未命中触发整页装载
    assert_disk_read(&hlog, e_addr, &e_key, &e_val).await?;
    assert_disk_read(&hlog, f_addr, b"f:fill00", &f_val).await?;

    // 第 0 页逐条冷读（连续性装载门槛）：首条仅 probe 不装载，第二条同页未命中触发整页
    // 装载（驱逐直接映射槽位上的第 2 页），其余 7 条（含 ~1KB 小记录与页尾紧贴记录）
    // 全部命中零 I/O
    for (addr, key, val) in &recs {
      assert_disk_read(&hlog, *addr, key, val).await?;
    }
    // 跨页边界：页 1 第一条 probe（页号切换，连续性不满足）+ 同页收尾条整页装载
    assert_disk_read(&hlog, c_addr, &c_key, &c_val).await?;
    assert_disk_read(&hlog, d_addr, b"d:fill00", &d_val).await?;
    let reads_filled = device.reads();
    assert_eq!(
      reads_filled, 6,
      "三页 12 条点读应恰好产生 6 次设备 I/O（每页 1 次 probe + 1 次整页装载）"
    );

    // 跨页边界二次回验：页 0 页尾最后一条与页 1 第一条连续重读，全部命中缓存
    assert_disk_read(&hlog, tail_addr, &tail_key, &tail_val).await?;
    assert_disk_read(&hlog, c_addr, &c_key, &c_val).await?;
    assert_eq!(device.reads(), reads_filled, "二次回验必须全部命中缓存");

    // 直接映射驱逐：重读第 2 页（槽位 0 已被页 0 占用）——首条仅 probe 不装载，
    // 同页收尾条触发整页重装载并驱逐页 0，随后第 2 页首条命中
    assert_disk_read(&hlog, e_addr, &e_key, &e_val).await?;
    assert_disk_read(&hlog, f_addr, b"f:fill00", &f_val).await?;
    assert_disk_read(&hlog, e_addr, &e_key, &e_val).await?;
    assert_eq!(
      device.reads(),
      reads_filled + 2,
      "页 2 重读应恰好产生 1 次 probe + 1 次整页重装载"
    );

    // 页 0 已被第 2 页驱逐：重读页尾紧贴记录仅 probe（页号切换不满足连续性），
    // 同页首条第二次未命中才整页重装载，内容完好
    assert_disk_read(&hlog, tail_addr, &tail_key, &tail_val).await?;
    assert_disk_read(&hlog, recs[0].0, &recs[0].1, &recs[0].2).await?;
    assert_eq!(
      device.reads(),
      reads_filled + 4,
      "槽位冲突驱逐后同页重读应恰好产生 1 次 probe + 1 次整页重装载"
    );

    info!("点读冷路径整页磁盘读缓存测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 27: 磁盘读缓存连续性装载门槛——顺序/热点负载整页装载，均匀随机负载恒 4KB probe
///
/// 行为矩阵（每次点读的设备 I/O 字节数，页 64KB）：
/// - 顺序读：首条 probe 4KB、第二条触发整页装载 64KB（页首整页读恰好一次）、其后全命中零 I/O；
/// - 均匀随机：访问互不相邻的不同页（零命中率场景），每次仅 probe 级小读，无整页读
///   ——避免迭代2"未命中即整页读"在随机负载下的 16 倍单次字节放大。
#[test]
fn test_disk_read_adaptive_install() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_adaptive_install.db");
    let device = Arc::new(CountingDevice::new(SegmentedDevice::single_file(&db_path)?));
    let epoch = Arc::new(LightEpoch::new(16));

    // 64KB 页：每页 16 条 4092 字节物理记录恰好精确填满（64 + 16×4092 = 65536，无换页 Pad，
    // < 4KB probe 单次覆盖），页内偏移 64 + i×4092
    let page_size = 64 * 1024;
    const RECS_PER_PAGE: u64 = 16;
    let val_len = 4092 - HEADER_SIZE - 8;
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config, device.clone(), epoch)?;

    // 页 0..=4 各 16 条记录（页 3 留作顺序读目标页），页 5 一条收尾使页 0..=4 均为完整非尾页
    let mut recs = Vec::new();
    for page in 0..5u64 {
      for i in 0..RECS_PER_PAGE {
        let key = format!("k:{page:02}:{i:02}").into_bytes();
        let val = vec![b'a' + (i as u8 % 26); val_len];
        let addr = hlog.append(&key, &val, 0, false)?;
        recs.push((addr, key, val));
      }
    }
    let tail_key = b"tail:0000".to_vec();
    let tail_addr = hlog.append(&tail_key, &vec![b'T'; val_len], 0, false)?;
    assert_eq!(tail_addr / page_size as u64, 5, "收尾记录必须落在第 5 页");

    // 全部落盘并驱逐至磁盘区（页 0..=4 完整落盘，满足缓存装载门槛）
    for p in 0..=5u64 {
      hlog.flush_page(p).await?;
    }
    let tail = hlog.tail_address();
    hlog.shift_read_only_address(tail);
    hlog.shift_head_address(tail);

    let rec = |page: u64, i: u64| &recs[(page * RECS_PER_PAGE + i) as usize];

    /// 单条冷读回验：Disk 形态 + 键值逐字节一致
    async fn assert_disk_read<D: Device>(
      hlog: &HybridLog<D>,
      addr: u64,
      key: &[u8],
      val: &[u8],
    ) -> Void {
      let out = hlog.read_disk_record(addr).await?;
      assert!(matches!(out, RecordOutput::Disk(_)));
      assert_eq!(out.key()?, key);
      assert_eq!(out.value()?, val);
      OK
    }

    // ---- 顺序读形态（页 3）：首条 probe 不装载，第二条未命中触发页首整页装载，第三条命中 ----
    let reads_before = device.reads();
    let bytes_before = device.read_bytes();
    for i in 0..3u64 {
      let (addr, key, val) = rec(3, i);
      assert_disk_read(&hlog, *addr, key, val).await?;
    }
    assert_eq!(
      device.reads() - reads_before,
      2,
      "同页 3 条点读应恰好产生 2 次 I/O：1 次 probe + 1 次整页装载，第三条命中零 I/O"
    );
    let seq_bytes = device.read_bytes() - bytes_before;
    assert!(
      seq_bytes >= (page_size + 4096) as u64 && seq_bytes < (page_size + 8192) as u64,
      "顺序读应恰好产生 1 次整页装载（{page_size} 字节）+ 1 次 probe（4KB 级），实际 {seq_bytes} 字节"
    );

    // 顺序页保持驻留：重读第二条记录零新增 I/O
    let (addr1, key1, val1) = rec(3, 1);
    assert_disk_read(&hlog, *addr1, key1, val1).await?;
    assert_eq!(
      device.reads() - reads_before,
      2,
      "已装载页重读必须全部命中缓存"
    );

    // ---- 均匀随机形态：交替访问互不相邻的页 0/2/4（无连续同页访问，零命中率场景）----
    // 每次未命中页号均与上次不同 ⇒ 恒 probe 级小读，绝不触发整页装载
    let reads_before = device.reads();
    let bytes_before = device.read_bytes();
    for page in [0u64, 2, 4, 0, 2, 4, 0] {
      let (addr, key, val) = rec(page, 0);
      assert_disk_read(&hlog, *addr, key, val).await?;
    }
    assert_eq!(
      device.reads() - reads_before,
      7,
      "均匀随机读每次未命中应恰好一次 probe 级设备 I/O"
    );
    let rand_bytes = device.read_bytes() - bytes_before;
    assert!(
      rand_bytes < 7 * 8192,
      "均匀随机读总 I/O 必须为 probe 级小读之和，不得出现任何整页装载（单次整页即 {page_size} 字节）: {rand_bytes}"
    );

    // 随机页绝不装载：与顺序页交替回验——顺序页仍命中，随机页仍仅 probe
    assert_disk_read(&hlog, *addr1, key1, val1).await?;
    assert_eq!(
      device.reads() - reads_before,
      7,
      "顺序装载页不得被随机 probe 驱逐"
    );
    let (addr2, key2, val2) = rec(2, 0);
    assert_disk_read(&hlog, *addr2, key2, val2).await?;
    assert_eq!(
      device.reads() - reads_before,
      8,
      "随机页重读仍应仅 probe，不得整页装载"
    );

    info!("磁盘读缓存连续性装载门槛测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
