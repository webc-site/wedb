//! 并发写入者与检查点并行：有界时长 + 看门狗，校验快照一致性前缀语义与
//! 并发检查点的 token 单调、数据零丢失

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use tempfile::tempdir;
use wcpr::{CheckpointManager, CheckpointType};
use wdev::SegmentedDevice;

use super::support::{MiniStore, Watchdog};

/// 写者数量与各阶段操作数（有界并发，测试总时长可控）
const WRITERS: usize = 4;
const PHASE1_OPS: usize = 25;
const PHASE2_OPS: usize = 25;
/// 看门狗预算：正常应在秒级完成
const WATCHDOG_BUDGET: Duration = Duration::from_secs(60);

/// 生成第 w 个写者第 j 个操作的键
fn key_of(w: usize, j: usize) -> String {
  format!("w{w}:key:{j:04}")
}

/// 生成第 w 个写者第 j 个操作的值
fn val_of(w: usize, j: usize) -> String {
  format!("w{w}:val:{j:04}")
}

#[test]
fn writers_and_checkpoints_run_concurrently() -> Void {
  let _watchdog = Watchdog::start("writers_and_checkpoints", WATCHDOG_BUDGET);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("concurrent.db");
    let store = MiniStore::open(&db_path)?;
    let mgr = CheckpointManager::<SegmentedDevice>::new();

    // 阶段一：全部写者先行落盘（后续所有检查点的必然包含前缀）
    for w in 0..WRITERS {
      let p = store.session()?;
      for j in 0..PHASE1_OPS {
        store
          .put(&p, key_of(w, j).as_bytes(), val_of(w, j).as_bytes())
          .await?;
      }
    }
    let first = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;

    // 阶段二：写者与周期检查点并行推进
    let mut handles = Vec::new();
    for w in 0..WRITERS {
      let store = Arc::clone(&store);
      handles.push(rt.spawn(async move {
        let p = store.session()?;
        for j in 0..PHASE2_OPS {
          // 全新键直写尾部
          let fresh_j = 100 + j;
          store
            .put(
              &p,
              key_of(w, fresh_j).as_bytes(),
              val_of(w, fresh_j).as_bytes(),
            )
            .await?;
          // 末笔覆盖历史键（若被最后一次检查点捕获则恢复为覆盖值）
          if j + 1 == PHASE2_OPS {
            store
              .put(
                &p,
                key_of(w, 0).as_bytes(),
                format!("w{w}:val:0:p2over").as_bytes(),
              )
              .await?;
          }
          // 让出执行权：制造与检查点状态机交错的真实并发窗口
          sleep(Duration::from_micros(100)).await;
        }
        aok::Result::<()>::Ok(())
      }));
    }
    // 检查点驱动任务：串行闸门内连续发布，token 必须严格递增
    let ckpt_store = Arc::clone(&store);
    let ckpt_task_dir = ckpt_dir.clone();
    handles.push(rt.spawn(async move {
      let mut tokens = vec![first.token];
      let mut last_tail = first.hlog_meta.tail_address;
      for _ in 0..3 {
        sleep(Duration::from_millis(1)).await;
        let meta = mgr
          .create_checkpoint(&ckpt_store, &ckpt_task_dir, CheckpointType::FoldOver)
          .await?;
        assert!(
          meta.token > *tokens.last().expect("tokens 非空"),
          "并发检查点 token 必须严格递增"
        );
        assert!(
          meta.hlog_meta.tail_address >= last_tail,
          "并发检查点截断点必须单调不减"
        );
        last_tail = meta.hlog_meta.tail_address;
        tokens.push(meta.token);
      }
      aok::Result::<()>::Ok(())
    }));
    for h in handles {
      h.await
        .map_err(|e| aok::anyhow!("并行任务异常结束: {e}"))??;
    }

    // 活实例终态校验：全部键值精确一致（覆盖键为末笔覆盖值）
    let p_live = store.session()?;
    for w in 0..WRITERS {
      for j in 0..PHASE1_OPS {
        let expect = if j == 0 {
          format!("w{w}:val:0:p2over")
        } else {
          val_of(w, j)
        };
        let actual = store.get(&p_live, key_of(w, j).as_bytes()).await?;
        assert_eq!(
          actual.as_deref(),
          Some(expect.as_bytes()),
          "活实例历史键必须一致: {}",
          key_of(w, j)
        );
      }
      for j in 0..PHASE2_OPS {
        let actual = store.get(&p_live, key_of(w, 100 + j).as_bytes()).await?;
        assert_eq!(
          actual.as_deref(),
          Some(val_of(w, 100 + j).as_bytes()),
          "活实例新键必须一致: {}",
          key_of(w, 100 + j)
        );
      }
    }

    // 崩溃恢复：recover_latest 选取最后一次检查点，
    // 阶段一前缀必须完整；阶段二键按一致性截断语义「存在即正确」
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let restored = CheckpointManager::recover_latest::<MiniStore>(&ckpt_dir, device).await?;
    let p_restored = restored.session()?;
    for w in 0..WRITERS {
      for j in 1..PHASE1_OPS {
        let actual = restored.get(&p_restored, key_of(w, j).as_bytes()).await?;
        assert_eq!(
          actual.as_deref(),
          Some(val_of(w, j).as_bytes()),
          "恢复实例必须包含全部阶段一前缀: {}",
          key_of(w, j)
        );
      }
      for j in 0..PHASE2_OPS {
        let actual = restored
          .get(&p_restored, key_of(w, 100 + j).as_bytes())
          .await?;
        assert!(
          actual.is_none() || actual.as_deref() == Some(val_of(w, 100 + j).as_bytes()),
          "截断点之后的键绝不允许出现撕裂错值: {} -> {actual:?}",
          key_of(w, 100 + j)
        );
      }
      let j0 = restored.get(&p_restored, key_of(w, 0).as_bytes()).await?;
      let p1 = val_of(w, 0);
      let p2 = format!("w{w}:val:0:p2over");
      assert!(
        j0.as_deref() == Some(p1.as_bytes()) || j0.as_deref() == Some(p2.as_bytes()),
        "覆盖键恢复值必须为完整前缀值或完整覆盖值: {j0:?}"
      );
    }

    aok::Result::<()>::Ok(())
  })?;
  OK
}
