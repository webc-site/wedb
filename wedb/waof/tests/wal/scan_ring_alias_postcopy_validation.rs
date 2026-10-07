//! 环形别名覆写 × 扫描读取的拷后复核闭环（对标 C# 纪元保护语义的验证）
//!
//! C# TsavoriteLogScanIterator.GetNext 于 epoch.Resume 内拷贝，OnPagesClosed
//! 等读者退纪元才回收页帧，内存臂被读字节绝不被覆写。本实现的环形缓冲以
//! 物理别名复用内存：读者快照判定在窗内后，写者仍可经 reserve_address 把
//! tail 推至 align_down(flushed) + cap，其写入与读者被读字节在环上物理重合
//!（逻辑差恰为容量整数倍）——错位帧（addr + cap 处新帧顶替 addr 处记录，
//! 定长负载下整帧自洽 CRC 必过）会被当作本址记录返回，游标从此偏离真实
//! 帧边界。修复收口为「预占先行 + 拷后复核」（seqlock）后，撕裂或错位拷贝
//! 被 ring_copy_intact 复核拦截并回退设备权威数据。
//!
//! 两个用例：
//! - 门闩用例（确定性检出）：利用 scan_memory_records 入口一次性快照 +
//!   回调执行窗口，在首帧回调内门闩放行写者完成别名覆写，随后主循环解码
//!   的帧物理上必然已被完整新帧顶替——修复前回调收到错位帧（断言失败），
//!   修复后复核拦截、错位帧绝不进入回调；
//! - 并发冒烟用例（回归护栏）：多写者持续入队 + 周期 commit、读者限速
//!   贴窗扫描（next_frame 与 scan_memory_records 两路），读出帧与写入
//!   登记逐条吻合。

use std::{
  collections::HashMap,
  hint::spin_loop,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  thread::{sleep, spawn},
  time::Duration,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use waof::{COMMIT_FRAME_TOTAL_LEN, Error, is_commit_frame};

use super::support::WalFixture;

/// 数据帧负载长：8B 序号 + 48B 填充，帧总长 8 + 56 = 64（2 的幂）
const PAYLOAD_LEN: usize = 56;
const FRAME_LEN: u64 = 64;
/// 垫帧负载长（24B）：帧总长 32，与 commit 帧等长，用于凑齐 64B 相位对齐
const PAD_PAYLOAD_LEN: usize = 24;

fn seq_payload(seq: u64) -> Vec<u8> {
  let mut payload = vec![0x5A; PAYLOAD_LEN];
  payload[..8].copy_from_slice(&seq.to_le_bytes());
  payload
}

fn pad_payload() -> Vec<u8> {
  vec![0xA5; PAD_PAYLOAD_LEN]
}

/// 门闩用例：scan_memory_records 内存臂无拷后复核时，快照后的别名覆写
/// 会把 addr + cap 处的新帧当作 addr 处记录送进回调（确定性时序构造）
#[test]
fn test_scan_memory_records_alias_overwrite_gate() -> Void {
  let rt = Runtime::new()?;
  // 1 扇区小环：一圈 64 帧，写者别名覆写只需一次窗口预算
  let fixture = WalFixture::single_file("scan_alias_gate.log", 4096)?;
  let wal = fixture.wal;
  let begin = wal.begin_address();
  // 布局：frame0 @+0、frame1 @+64（64B 数据帧）……共 63 帧 + commit(32B)
  // + 垫帧(32B) + commit(32B，环形满时经腾窗自旋落位) + 垫帧(32B)，
  // 写者起点恰落 begin+4160 ≡ 0 (mod 64)。扫描从 frame1（+64）起，
  // 恰处入口快照窗界等号（start == mem_base - cap）；门闩放行后写者的
  // 第二帧 @+4224 物理 128 与 frame2 @+128 完全重合（整帧自洽顶替）
  let writer_base = begin + 4160;

  // 登记表（addr -> seq）：断言读出帧与写入登记逐条吻合的唯一权威
  let mut registry = HashMap::<u64, u64>::new();
  rt.block_on(async {
    for seq in 0..63u64 {
      let addr = wal.enqueue(&seq_payload(seq))?;
      registry.insert(addr, seq);
    }
    wal.commit().await?;
    wal.enqueue(&pad_payload())?;
    wal.commit().await?;
    wal.enqueue(&pad_payload())?;
    aok::Result::<()>::Ok(())
  })?;
  assert_eq!(wal.tail_address(), writer_base, "前置尾位须精确落位");

  // 门闩：frame1 回调内放行写者，等其完成两帧别名覆写后再放主循环继续解码
  let go = Arc::new(AtomicBool::new(false));
  let done = Arc::new(AtomicBool::new(false));
  let writer = {
    let wal = Arc::clone(&wal);
    let go = Arc::clone(&go);
    let done = Arc::clone(&done);
    spawn(move || {
      while !go.load(Ordering::Acquire) {
        spin_loop();
      }
      // 两帧均在 reserve 窗口内（required_end ≤ align_down(flushed) + cap）：
      // 首帧 @4160 顶替 frame1 旧物理位（已读完），次帧 @4224 物理 128
      // 恰与未读的 frame2 @128 完全重合
      let addr = wal.enqueue(&seq_payload(999)).expect("窗口内预占必成");
      assert_eq!(addr, writer_base);
      let addr = wal.enqueue(&seq_payload(1000)).expect("窗口内预占必成");
      assert_eq!(addr, writer_base + 64);
      done.store(true, Ordering::Release);
    })
  };

  // 读者（同步扫描，无 async）：frame1 断言通过后门闩放行写者，
  // 其后主循环解到的 frame2 物理上必然已被完整新帧顶替
  let mut fired = false;
  let covered = wal.scan_memory_records(begin + 64, writer_base, |rec| {
    if !is_commit_frame(&rec.payload) && rec.payload.len() == PAYLOAD_LEN {
      let seq = u64::from_le_bytes(rec.payload[..8].try_into().unwrap());
      assert_eq!(
        seq, registry[&rec.address],
        "地址 {:#x} 读出序号 {seq} 与登记不符（环形别名错位帧）",
        rec.address
      );
    }
    if !fired {
      fired = true;
      go.store(true, Ordering::Release);
      while !done.load(Ordering::Acquire) {
        spin_loop();
      }
    }
    true
  });
  writer.join().expect("写者线程正常结束");

  // 修复后语义：frame2 的物理位已被别名覆写，复核必然拦截（covered=false），
  // 回调收到的每一帧都与登记吻合（上面断言）；修复前在 frame2 处收到
  // seq=1000 的错位帧，断言已失败暴露
  assert!(
    !covered,
    "别名覆写后同步扫描须被复核拦截（covered=false），不得续扫错位帧"
  );

  OK
}

/// 并发冒烟用例（回归护栏）：多写者持续入队 + 周期 commit、读者限速贴窗
/// 扫描（next_frame 与 scan_memory_records 两路），读出帧与登记逐条吻合
#[test]
fn test_scan_ring_alias_postcopy_concurrent() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    // 4 扇区环：一圈 256 帧
    let fixture = WalFixture::single_file("scan_ring_alias.log", 4 * 4096)?;
    let wal = fixture.wal;
    let begin = wal.begin_address();

    let stop = Arc::new(AtomicBool::new(false));
    let next_seq = Arc::new(AtomicU64::new(0));
    let registry = Arc::new(Mutex::new(HashMap::<u64, u64>::new()));

    // 4 写者：持续 enqueue + 周期 commit（BufferFull 自旋等待 flushed 推进）
    let mut writers = Vec::new();
    for _ in 0..4 {
      let wal = Arc::clone(&wal);
      let registry = Arc::clone(&registry);
      let next_seq = Arc::clone(&next_seq);
      writers.push(spawn(move || -> aok::Result<()> {
        let writer_rt = Runtime::new()?;
        writer_rt.block_on(async move {
          for round in 0..150u64 {
            let seq = next_seq.fetch_add(1, Ordering::Relaxed);
            let payload = seq_payload(seq);
            let addr = loop {
              match wal.enqueue(&payload) {
                Ok(addr) => break addr,
                Err(Error::BufferFull { .. }) => sleep(Duration::from_micros(100)),
                Err(e) => return Err(e.into()),
              }
            };
            registry.lock().unwrap().insert(addr, seq);
            if round % 16 == 15 {
              wal.commit().await?;
            }
          }
          wal.commit().await?;
          aok::Result::<()>::Ok(())
        })?;
        Ok(())
      }));
    }

    // 读者：持久游标（帧边界由 next_addr 链保持）限速循环扫描，
    // 滞后围绕环容量震荡、反复穿越窗底别名窄带
    let reader = {
      let wal = Arc::clone(&wal);
      let stop = Arc::clone(&stop);
      spawn(move || -> aok::Result<Vec<(u64, u64)>> {
        let reader_rt = Runtime::new()?;
        reader_rt.block_on(async move {
          let mut cur = begin;
          let mut observed = Vec::new();
          while !stop.load(Ordering::Acquire) {
            let committed = wal.committed_until_address();
            if cur >= committed {
              sleep(Duration::from_micros(100));
              continue;
            }
            // 路一：迭代器帧直组（复制推流泵消费面），小预算限速贴窗
            let round_end = (cur + 32 * FRAME_LEN).min(committed);
            let mut iter = wal.scan(cur, round_end);
            while let Some(frame) = iter.next_frame().await? {
              if !is_commit_frame(&frame.frame[8..]) {
                assert_eq!(
                  frame.frame.len(),
                  8 + PAYLOAD_LEN,
                  "数据帧定长，地址 {:#x}",
                  frame.address
                );
                let seq = u64::from_le_bytes(frame.frame[8..16].try_into().unwrap());
                assert_eq!(
                  frame.next_address,
                  frame.address + FRAME_LEN,
                  "数据帧链推进，地址 {:#x}",
                  frame.address
                );
                observed.push((frame.address, seq));
              } else {
                assert_eq!(
                  frame.next_address,
                  frame.address + COMMIT_FRAME_TOTAL_LEN,
                  "commit 帧链推进，地址 {:#x}",
                  frame.address
                );
              }
              cur = frame.next_address;
            }
            // 路二：内存窗口同步扫描（快照恢复消费面）；复核拦截（false）
            // 或回调 break 均保持游标，下轮续扫
            let step = cur;
            if step < round_end {
              wal.scan_memory_records(step, round_end, |rec| {
                if !is_commit_frame(&rec.payload) {
                  assert_eq!(rec.payload.len(), PAYLOAD_LEN);
                  let seq = u64::from_le_bytes(rec.payload[..8].try_into().unwrap());
                  observed.push((rec.address, seq));
                }
                cur = rec.next_address;
                true
              });
            }
            sleep(Duration::from_micros(200));
          }
          Ok(observed)
        })
      })
    };

    for writer in writers {
      writer.join().expect("写者线程正常结束")?;
    }
    stop.store(true, Ordering::Release);
    let observed = reader.join().expect("读者线程正常结束")?;

    // 闭环：读者全部 (addr, seq) 逐条命中写者登记
    let registry = registry.lock().unwrap();
    assert!(!observed.is_empty(), "读者须实际扫到数据帧");
    for (addr, seq) in &observed {
      assert_eq!(
        registry.get(addr),
        Some(seq),
        "地址 {addr:#x} 处读出序号 {seq} 与写入登记不符（环形别名错位帧）"
      );
    }

    aok::Result::<()>::Ok(())
  })?;

  OK
}
