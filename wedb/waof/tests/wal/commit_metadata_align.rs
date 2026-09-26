//! WAL 提交元数据对齐（[`waof::WalLog::unsafe_commit_metadata_only`]）单元测试
//!
//! 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/
//! TsavoriteLog.cs:UnsafeCommitMetadataOnly（:758）与 UpdateCommittedState
//! （:2732）——副本本地不写 commit 帧，本地提交上界唯一推进源是回放链把主端
//! 帧位对齐进来（ReplicaReplayDriver.cs:ConsumeDirect 的 payloadLength < 0
//! 分支）。
//!
//! 危害回归（修复前红相）：副本 committed_until_address 恒滞留构造初值，
//! [`waof::WalLog::truncate`] 的 safe_until = until.min(committed) 恒 0——
//! 物理删段永久失效、磁盘段无限积压，scan_committed 恒空区间，
//! ReplicationManager::get_committed_replication_offset 观测恒零值。

use std::sync::atomic::Ordering;

use aok::{OK, Void};
use log::info;
use waof::{CommitMeta, NO_COOKIE};

use super::support::{self, WalFixture, make_payload};

/// 对齐主端帧位后 truncate 物理删段与已提交扫描恢复生效（危害链回归主臂；
/// 物理删段断言面对标 C# LogTests.cs 的 TruncateUntilTest 的副本提交边界前提）
#[compio::test]
async fn commit_metadata_align_enables_truncate_and_committed_scan() -> Void {
  let seg_size = 16 * 1024;
  let fixture = WalFixture::segmented("commit_meta_align.log", seg_size, 64 * 1024)?;
  let wal = fixture.wal;

  // 副本形态写入：只入队不驱动本地 commit（本地永不写帧），50 条跨段积压
  let mut addrs = Vec::new();
  for i in 0..50 {
    let addr = wal.enqueue(&make_payload(1000, (i % 256) as u8))?;
    addrs.push(addr);
  }
  assert_eq!(
    wal.committed_until_address(),
    0,
    "缺陷锚定：未对齐时提交边界滞留构造初值"
  );

  // 回放链对齐主端帧位：until = 本批帧尾，meta = 主端帧负载（begin 快照 + cookie）
  let until = wal.tail_address();
  wal
    .unsafe_commit_metadata_only(
      CommitMeta {
        begin: 0,
        cookie: 7,
      },
      until,
    )
    .await?;

  assert_eq!(wal.committed_until_address(), until, "提交上界推进至帧尾");
  assert_eq!(
    wal.flushed_until_address(),
    until,
    "对齐含刷盘（提交 = 已持久，wait_for_commit 承诺不虚）"
  );
  assert_eq!(wal.recovered_cookie(), 7, "cookie 单调回填");
  assert_eq!(wal.recovered_committed_begin(), 0, "begin 快照直赋");
  assert_eq!(
    wal.last_commit_frame.load(Ordering::Acquire),
    until,
    "帧游标对齐镜像帧位"
  );

  // truncate 生效：safe_until 不再被 0 钳死，段 0/1 物理删除（修复前此处
  // safe_until = until.min(0) = 0，段文件永存）
  let target = addrs[35];
  assert!(target >= 2 * seg_size);
  wal.truncate(target).await?;
  assert_eq!(wal.begin_address(), target);
  assert!(!wal.device().segment_path(0).exists());
  assert!(!wal.device().segment_path(1).exists());
  assert!(wal.device().segment_path(2).exists());

  // scan_committed 由恒空修复为真实已提交区间
  let committed = support::collect_data(wal.scan_committed()).await?;
  assert_eq!(committed.len(), 50 - 35);
  for rec in committed {
    assert!(rec.address >= target);
  }

  info!("提交元数据对齐：truncate 物理删段与已提交扫描恢复生效");
  OK
}

/// 幂等与单调：同帧位重复对齐无漂移，回退帧位/无 cookie 帧不回退任何水位
///（对标 C# UpdateCommittedState 的 `MonotonicUpdate(persistedCommitNum)`
/// 单调承诺与 CommittedBeginAddress 直赋语义）
#[compio::test]
async fn commit_metadata_align_is_idempotent_and_monotonic() -> Void {
  let fixture = WalFixture::single_file("commit_meta_align_mono.log", 64 * 1024)?;
  let wal = fixture.wal;

  let _a = wal.enqueue(&make_payload(64, 1))?;
  let _b = wal.enqueue(&make_payload(64, 2))?;
  let tail = wal.tail_address();

  // 首次对齐：cookie = 5
  wal
    .unsafe_commit_metadata_only(
      CommitMeta {
        begin: 0,
        cookie: 5,
      },
      tail,
    )
    .await?;
  assert_eq!(wal.committed_until_address(), tail);
  assert_eq!(
    support::collect_data(wal.scan_committed()).await?.len(),
    2,
    "已提交区间恢复可扫"
  );

  // 幂等：同帧位重复对齐（无 cookie 帧）无任何漂移
  wal
    .unsafe_commit_metadata_only(
      CommitMeta {
        begin: 0,
        cookie: NO_COOKIE,
      },
      tail,
    )
    .await?;
  assert_eq!(wal.committed_until_address(), tail);
  assert_eq!(
    wal.recovered_cookie(),
    5,
    "NO_COOKIE 哨兵恒不回退真实序列号（fetch_max 单调）"
  );

  // 新帧位 + 新 begin 快照：水位 / 帧游标 / begin 栅栏单调推进
  let c = wal.enqueue(&make_payload(64, 3))?;
  let tail2 = wal.tail_address();
  assert!(tail2 > tail);
  wal
    .unsafe_commit_metadata_only(
      CommitMeta {
        begin: c,
        cookie: 9,
      },
      tail2,
    )
    .await?;
  assert_eq!(wal.committed_until_address(), tail2);
  assert_eq!(wal.recovered_cookie(), 9);
  assert_eq!(wal.recovered_committed_begin(), c);
  assert_eq!(wal.begin_address(), c, "begin 逻辑栅栏推进至主端快照");

  // 回退帧位：一切单调面不回退（乱序/陈旧帧防御）
  wal
    .unsafe_commit_metadata_only(
      CommitMeta {
        begin: 0,
        cookie: 1,
      },
      _a,
    )
    .await?;
  assert_eq!(wal.committed_until_address(), tail2, "提交上界不回退");
  assert_eq!(wal.begin_address(), c, "begin 不回退");
  assert_eq!(wal.recovered_cookie(), 9, "cookie 不回退");
  assert_eq!(
    wal.recovered_committed_begin(),
    c,
    "begin 快照取对齐序列最大者"
  );

  info!("提交元数据对齐：幂等与单调语义测试通过");
  OK
}
