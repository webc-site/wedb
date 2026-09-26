//! 同进程双实例检查点闸门实例粒度：各实例持独立 `CkptGateState`（宿主所有权
//! 字段，对标 C# GarnetDatabase.CheckpointingLock 逐实例锁）与各独立 checkpoint
//! 目录并发 SAVE，全部成功且互不假死，目录间检查点文件集互不串扰
//! （task/ing/wcpr-checkpoint-gate-global-registry.md：原目录级进程闸语义随
//! 全局表删除消亡，改写为实例级语义）
//!
//! 自研依据: 检查点闸门实例粒度（C# 检查点串行由 per-storeWrapper 状态机内部
//! 驱动，无进程级闸门形态；rust 闸门粒度随存储引擎实例归属对齐 per-instance 语义）

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{CheckpointType, CkptGateState};
use wdev::SegmentedDevice;

use super::support::{MiniStore, Watchdog};

/// 看门狗预算：双实例并发 SAVE 应秒级完成；跨实例互斥回归或死锁即超时
const WATCHDOG_BUDGET: Duration = Duration::from_secs(60);

/// 各实例 SAVE 轮次
const ROUNDS: usize = 3;

#[test]
fn dual_instance_saves_are_independent_per_gate() -> Void {
  let _watchdog = Watchdog::start("dual_instance_saves", WATCHDOG_BUDGET);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir_a = dir.path().join("checkpoints_a");
    let ckpt_dir_b = dir.path().join("checkpoints_b");
    let store_a = MiniStore::open(dir.path().join("dual_a.db"))?;
    let store_b = MiniStore::open(dir.path().join("dual_b.db"))?;
    // 各实例独立闸门状态（宿主所有权形态）：存储引擎状态机本互不相干，
    // 不同实例的 SAVE 并发发起绝不同闸排队
    let gate_a = Arc::new(CkptGateState::default());
    let gate_b = Arc::new(CkptGateState::default());

    // 各实例独立预写数据（互不相同的键域）
    for (store, tag) in [(&store_a, "a"), (&store_b, "b")] {
      let p = store.session()?;
      store
        .put(
          &p,
          format!("{tag}:k").as_bytes(),
          format!("{tag}:v").as_bytes(),
        )
        .await?;
    }

    // 双实例并发 SAVE：跨实例互斥回归（如误共闸）时此处退化串行甚至死锁，
    // 看门狗预算 + src 侧 ckpt_gate_is_per_instance 单测共同钉死粒度回归
    let mut handles = Vec::new();
    for (store, gate, ckpt_dir) in [
      (
        Arc::clone(&store_a),
        Arc::clone(&gate_a),
        ckpt_dir_a.clone(),
      ),
      (store_b, gate_b, ckpt_dir_b.clone()),
    ] {
      handles.push(rt.spawn(async move {
        let mut last_token = 0u128;
        for r in 0..ROUNDS {
          let meta =
            wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
          assert!(
            meta.token > last_token,
            "实例内 SAVE token 必须严格递增: 第 {r} 轮"
          );
          last_token = meta.token;
        }
        aok::Result::<()>::Ok(())
      }));
    }
    for h in handles {
      h.await
        .map_err(|e| aok::anyhow!("双实例 SAVE 任务异常结束: {e}"))??;
    }

    // 实例闸随实例生命周期：store_a 的闸门状态与他实例无涉，目录恢复视图
    // 只含本实例键域，互不串扰
    for (ckpt_dir, tag) in [(&ckpt_dir_a, "a"), (&ckpt_dir_b, "b")] {
      let device = Arc::new(SegmentedDevice::single_file(
        dir.path().join(format!("dual_{tag}.db")),
      )?);
      let restored = wcpr::recover_latest::<_, MiniStore>(ckpt_dir, device).await?;
      let p = restored.session()?;
      assert_eq!(
        restored
          .get(&p, format!("{tag}:k").as_bytes())
          .await?
          .as_deref(),
        Some(format!("{tag}:v").as_bytes()),
        "实例 {tag} 恢复视图必须含本实例键"
      );
      let other = if tag == "a" { "b" } else { "a" };
      assert!(
        restored
          .get(&p, format!("{other}:k").as_bytes())
          .await?
          .is_none(),
        "实例 {tag} 恢复视图不得串入实例 {other} 的键"
      );
    }

    OK
  })?;

  OK
}
