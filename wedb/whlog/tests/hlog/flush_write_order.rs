//! 刷盘写序闸回归（票：whlog-flush-overlapping-page-writes-unordered-stale-copy-overwrites-committed-prefix）
//!
//! 契约对标 C# AllocatorBase.cs:AsyncFlushPagesForReadOnly :2210-2214：部分页片段
//! 必须等待前一相邻刷盘完成，否则前片段尾扇区（未完成）会覆盖后片段同扇区
//! （已完成）——C# 以 PendingFlush 入队 + AsyncFlushPageCallback 回调链实现同页
//! 设备写恒按地址次序串行、后写者恒为新内容；rust 侧收敛为刷盘内核内的
//! `flush_gate` 异步闸等价承接（不变式见 `flush_sealed_page_range` 文档
//! 「刷盘写序」一节）。
//!
//! 本测试以「写前挂起握手」（[`FaultDevice::arm_stall`]）把交错钉死：
//! 1. 任务 A 封印上界 t1（页 P 中段）拷贝旧页后扣停在设备写前——旧拷贝
//!    [t1, 页尾) 为零字节；
//! 2. 继续追加使 tail = s_B（仍在页 P，[t1, s_B) 定稿），任务 B 对同一页发起刷盘；
//! 3. 放行 A，join 两任务。
//! - 修复后：B 阻塞在闸上等 A 的写在途完成，B 后拷贝（[t1, s_B) 已定稿）后落盘，
//!   设备页字节与内存页逐字节相等，`flushed_until` 推进到 s_B；
//! - 摘除闸复跑：B 先完成记账（flushed_until = s_B），A 旧拷贝后落盘把 [t1, s_B)
//!   回写为零字节——设备页与内存页在 [t1, s_B) 必然相异（此断言必红）。

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use log::info;
use tempfile::tempdir;
use wdev::{Device, SegmentedDevice};
use wepoch::LightEpoch;
use whlog::{HybridLog, HybridLogConfig, SECTOR_ALIGNMENT};

use super::support::FaultDevice;

/// 挂起窗口就位轮询间隔（单线程 runtime 下让渡一拍即可，给足余量防抖动）
const STALL_POLL: Duration = Duration::from_millis(1);

/// 挂起窗口就位等待预算（超时即测试环境异常，防挂死）
const STALL_BUDGET: Duration = Duration::from_secs(10);

/// 同页重叠刷盘写序：旧拷贝不得在 flushed_until 推进后落盘覆盖已提交前缀
#[test]
fn test_flush_write_order_gate_serializes_overlapping_page_writes() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_flush_order.db");
    let device = Arc::new(FaultDevice::new(SegmentedDevice::single_file(&db_path)?));
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 4, 0.5)?;
    let hlog = Arc::new(HybridLog::new(config, device.clone(), epoch)?);

    // 1. 追加 r1 使 tail = t1 落页 0 中段（16 头 + 1 键 + 1000 值 ≈ 1017 字节）
    hlog.append(b"a", &vec![b'a'; 1000], 0, false)?;
    let t1 = hlog.tail_address();
    assert!(
      t1 > 0 && t1 < SECTOR_ALIGNMENT as u64,
      "t1 必须位于页 0 中段: {t1}"
    );

    // 2. 布防写前挂起，任务 A flush_page(0)：封印上界 t1，旧页拷贝成形后扣停在写前
    device.arm_stall();
    let hlog_a = hlog.clone();
    let task_a = spawn(async move { hlog_a.flush_page(0).await });
    let mut waited = Duration::ZERO;
    while !device.is_stalled() {
      assert!(waited < STALL_BUDGET, "任务 A 未在预算内扣停在写前窗口");
      sleep(STALL_POLL).await;
      waited += STALL_POLL;
    }

    // 3. 追加 r2 使 tail = s_B 仍在页 0（[t1, s_B) 此刻定稿）
    hlog.append(b"b", &vec![b'b'; 1000], 0, false)?;
    let s_b = hlog.tail_address();
    assert!(
      s_b > t1 && s_b <= SECTOR_ALIGNMENT as u64,
      "s_B 必须仍在页 0: {s_b}"
    );

    // 4. 任务 B 对同一页发起刷盘（修复后阻塞在 flush_gate 上等 A 的写在途完成）
    let hlog_b = hlog.clone();
    let task_b = spawn(async move { hlog_b.flush_page(0).await });

    // 5. 放行 A → 先 join A 再 join B（B 依赖 A 出闸，不可先单独 await B）
    device.release_stall();
    task_a.await.expect("任务 A panic")?;
    task_b.await.expect("任务 B panic")?;

    // 6. 持久化前缀推进到 s_B，设备页与内存页逐字节相等（含 [t1, s_B) 定稿区）；
    //    摘除闸复跑时 A 旧拷贝后落盘，[t1, s_B) 回写为零，字节断言必红。
    //    设备字节尾 = 逻辑前缀 s_B（尾零头 pad 消解），读长钳到前缀防越尾短读
    assert!(
      hlog.flushed_until_address() >= s_b,
      "flushed_until 必须覆盖 s_B: {} < {s_b}",
      hlog.flushed_until_address()
    );
    let disk = device.read_range(0, s_b as usize).await?;
    let mem = hlog.buffer.read_page(0);
    assert_eq!(
      &disk[..s_b as usize],
      &mem[..s_b as usize],
      "设备页必须与内存页逐字节一致（旧拷贝覆盖了已提交前缀）"
    );

    info!("同页重叠刷盘写序闸测试通过（t1={t1}, s_B={s_b}）");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
