//! 截断边界 commit 帧的「跳越读者不透传」回归（wal/iterator.rs:
//! skip_truncation_boundary_commit 三条件复合分支：jumped_over × 游标落点
//! == 截断点 × 帧为 commit 元数据帧）。
//!
//! 触发链（确定性构造，无需真并发）：
//! 1. 两批写入各自 commit 后，迭代器自早期地址起扫，游标停在低位；
//! 2. truncate 至第二批提交尾（= committed）：begin 推进至截断点 T，
//!    last_commit_frame 同点钳制；
//! 3. 其后空转 commit 因 `begin > last_commit_begin` 走元数据补帧臂，
//!    commit 帧恰好补写于截断点 T（enqueue 落点 = 旧尾 = T）；
//! 4. 持有中的迭代器续读：跳至 T 命中该帧 → 三条件全真 → 不透传，
//!    续读 T 之后的新写入至耗尽。
//!
//! 对照臂：truncate 之后新起的迭代器（非跳越读者）物理扫描面须透传该帧
//!（消费层自滤契约）——证明跳过是跳越读者专属行为而非帧丢失。

use std::sync::atomic::Ordering;

use aok::{OK, Void};
use log::info;
use waof::{COMMIT_FRAME_TOTAL_LEN, is_commit_frame};

use super::support::{self, WalFixture, make_pattern_payload};

/// 环形窗口容量（扇区 4096 整数倍）
const BUF: usize = 64 * 1024;
/// 段大小：容纳两批写入，令 truncate 落在段内（逻辑推进 + 物理删旧段）
const SEG: u64 = 32 * 1024;

/// 持迭代器被 truncate 越过后，截断点补写的 commit 帧不透传、续读至耗尽
#[compio::test]
async fn test_iterator_skips_boundary_commit_frame_after_truncate() -> Void {
  let fixture = WalFixture::segmented("trunc_boundary_commit.log", SEG, BUF)?;
  let wal = &fixture.wal;

  // 阶段 1：A 批 30 条 + B 批 20 条，各自 commit（B 批提交尾即截断点 T）
  let mut addrs = Vec::new();
  for i in 0..30 {
    addrs.push(wal.enqueue(&make_pattern_payload(i, 120))?);
  }
  wal.commit().await?;
  for i in 30..50 {
    addrs.push(wal.enqueue(&make_pattern_payload(i, 120))?);
  }
  let tail_b = wal.commit().await?;
  assert_eq!(wal.committed_until_address(), tail_b);

  // 阶段 2：迭代器自 A 批第 4 条起扫，读 3 条后游标停在低位（< T）
  let mut iter = wal.scan(addrs[3], u64::MAX);
  for i in 3..6 {
    let rec = iter.next().await?.expect("truncate 前记录须可读");
    assert_eq!(rec.payload, make_pattern_payload(i, 120));
  }
  assert!(iter.current_address() < tail_b, "游标须停在截断点之下");

  // 阶段 3：truncate 至 B 批提交尾——begin 推进至 T，迭代器被越过
  wal.truncate(tail_b).await?;
  assert_eq!(wal.begin_address(), tail_b);

  // 阶段 4：空转 commit 走元数据补帧臂（begin > last_commit_begin），
  // commit 帧恰好补写于截断点 T（enqueue 落点 = 旧尾）
  let committed = wal.commit().await?;
  assert_eq!(
    committed,
    tail_b + COMMIT_FRAME_TOTAL_LEN,
    "补帧提交上界 = 帧尾"
  );
  assert_eq!(wal.last_commit_frame.load(Ordering::Acquire), committed);

  // 阶段 5：截断点后新写一条数据 + 随批帧（迭代器续读的透传面）
  let append_addr = wal.enqueue(&make_pattern_payload(50, 120))?;
  assert_eq!(append_addr, committed, "新写入须自补帧尾接续");
  let final_committed = wal.commit().await?;

  // 阶段 6：持有中的迭代器续读——边界帧不透传，新数据透传，读到耗尽
  let mut all = Vec::new();
  while let Some(rec) = iter.next().await? {
    all.push(rec);
  }
  assert_eq!(iter.overwritten_skips(), 0, "无覆写跳过");
  assert!(
    !all.iter().any(|r| r.address == tail_b),
    "截断点补写的 commit 帧不得透传给跳越读者"
  );
  let data: Vec<_> = all
    .iter()
    .filter(|r| !is_commit_frame(&r.payload))
    .collect();
  assert_eq!(data.len(), 1, "截断点后新数据须唯一透传");
  assert_eq!(data[0].address, append_addr);
  assert_eq!(data[0].payload, make_pattern_payload(50, 120));

  // 对照臂：非跳越读者（truncate 后新起的迭代器）物理面须透传该边界帧
  let fresh = support::collect_iter(wal.scan(tail_b, final_committed)).await?;
  assert!(
    fresh
      .iter()
      .any(|r| r.address == tail_b && is_commit_frame(&r.payload)),
    "非跳越读者的物理扫描面须透传截断边界帧（消费层自滤契约）"
  );

  info!("截断边界 commit 帧：跳越读者不透传且续读到耗尽测试通过");
  OK
}
