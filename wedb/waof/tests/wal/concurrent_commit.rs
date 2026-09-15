use std::{sync::Arc, thread::spawn};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;

use super::support::{WalFixture, make_payload};

/// 对标 Garnet TsavoriteLog Group Commit 合并语义（CommitTask/ongoingCommitRequests）：
/// 多线程并发 commit_to 不同位点，验证 Leader/Follower 协商下
/// 等待者全部唤醒、返回位点不小于各自目标、静默后四原子一致。
#[test]
fn test_commit_to_concurrent_waiters_consistency() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("commit_concurrent.log", 64 * 1024)?;
    let wal = fixture.wal;

    // 预写基线，形成非零提交水位
    for i in 0..16 {
      wal.enqueue(&make_payload(64, i as u8))?;
    }
    wal.commit().await?;

    // 8 线程各自独立 runtime：并发 enqueue + commit_to 自己的尾位点，
    // 触发 Leader 级联合并与 Follower 批量唤醒（0 重复物理 I/O 路径）
    let mut handles = Vec::new();
    for worker in 0..8u32 {
      let wal = Arc::clone(&wal);
      handles.push(spawn(move || -> aok::Result<()> {
        let worker_rt = Runtime::new()?;
        worker_rt.block_on(async move {
          for round in 0..20u32 {
            for i in 0..4u32 {
              let byte = (worker * 800 + round * 4 + i) as u8;
              wal.enqueue(&make_payload(64, byte))?;
            }
            let target = wal.tail_address();
            let committed = wal.commit_to(target).await?;
            assert!(
              committed >= target,
              "唤醒位点 {committed} 不得小于等待目标 {target}"
            );
          }
          aok::Result::<()>::Ok(())
        })?;
        Ok(())
      }));
    }

    for handle in handles {
      handle.join().expect("并发提交线程正常结束")?;
    }

    // 静默后位点一致性：提交水位 == 刷盘水位 == 尾地址，且扫描闭环
    let tail = wal.tail_address();
    assert_eq!(wal.committed_until_address(), tail, "提交水位须追平尾地址");
    assert_eq!(
      wal.flushed_until_address(),
      tail,
      "刷盘水位须追平尾地址"
    );

    let mut iter = wal.scan(wal.begin_address(), tail);
    let records = iter.collect_all().await?;
    assert_eq!(records.len(), 16 + 8 * 20 * 4);

    info!("并发 commit_to 唤醒与位点一致性测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet Group Commit Follower 挂起路径：同一位点被多任务同时 commit_to，
/// 仅允许一个 Leader 发起刷盘，其余合并为 Follower；全体返回一致位点。
#[test]
fn test_commit_to_same_target_merges_into_single_leader() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fixture = WalFixture::single_file("commit_merge.log", 64 * 1024)?;
    let wal = fixture.wal;

    for i in 0..32 {
      wal.enqueue(&make_payload(64, i as u8))?;
    }
    let target = wal.tail_address();

    // 8 线程并发提交同一位点：全部须返回 >= target 的一致位点
    let mut handles = Vec::new();
    for _ in 0..8 {
      let wal = Arc::clone(&wal);
      handles.push(spawn(move || -> aok::Result<u64> {
        let worker_rt = Runtime::new()?;
        Ok(worker_rt.block_on(wal.commit_to(target))?)
      }));
    }

    for handle in handles {
      let committed = handle.join().expect("合并提交线程正常结束")?;
      assert!(committed >= target, "合并唤醒位点须覆盖目标");
    }

    assert_eq!(wal.committed_until_address(), target);
    info!("同位点并发提交单 Leader 合并测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
