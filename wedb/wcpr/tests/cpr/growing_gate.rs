//! 检查点发起缺 is_growing 门的回归：索引在线扩容迁移中发起检查点必须即时
//! 拒绝且零副作用（对标 C# StateMachineDriver.cs:164-166 单槽 CAS 互斥——
//! `GrowIndexAsync` 经同一驱动器注册，grow 在跑时检查点状态机注册返回 false；
//! Tsavorite.cs:343-350 文档明言 "initiation may fail if we are already taking
//! a checkpoint or performing some other operation such as growing the index"）

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{CheckpointType, CkptGateState, CprStore, Error};

use super::support::MiniStore;

/// 扩容中发起必回绝、REST 态正常路径行为逐字节不变
#[test]
fn checkpoint_initiation_is_refused_while_index_growing() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("growing_gate.db"))?;
    let gate = CkptGateState::default();

    let p = store.session()?;
    store.put(&p, b"seed_key", b"seed_val").await?;

    // 构造扩容中态：复用 wkv/tests/store/resize.rs 的相位发布手法（grow_index
    // 先发布 IN_PROGRESS_GROW 后切表，fixture 将相位投影为原子标志），发布即扩容态
    store.publish_growing_phase();
    assert!(store.is_growing(), "相位已发布即处于扩容态");

    // 自动签发入口：发起即失败，回绝走既有错误档（不新造第二套错误变体）
    let err = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver)
      .await
      .expect_err("扩容中发起 Checkpoint 必须被拒绝");
    assert!(
      matches!(err, Error::Host(_)),
      "必须返回既有 Host 错误变体: {err}"
    );
    // 零副作用：拒绝先于目录创建与 Token 签发，连检查点目录本身都不应存在
    assert!(
      !ckpt_dir.exists(),
      "回绝的检查点不得留下半截目录/文件: {err}"
    );

    // 指定 Token 入口同样回绝且零副作用
    let token = wcpr::next_token_above(0);
    let index_start = store.tail_address();
    let err = wcpr::create_checkpoint_with_token(
      &store,
      &gate,
      &ckpt_dir,
      CheckpointType::FoldOver,
      token,
      index_start,
    )
    .await
    .expect_err("扩容中发起 Checkpoint（指定 Token）必须被拒绝");
    assert!(
      matches!(err, Error::Host(_)),
      "必须返回既有 Host 错误变体: {err}"
    );
    assert!(
      !ckpt_dir.exists(),
      "回绝的检查点（指定 Token）不得留下任何残留"
    );

    // 扩容完成回到 REST：正常检查点路径恢复且行为逐字节不变
    store.clear_growing_phase();
    assert!(!store.is_growing(), "相位清除后回到 REST 态");
    let meta = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
    assert_eq!(
      meta.index_meta.entry_count, 1,
      "REST 态快照必须完整收录种子条目"
    );
    let tokens = wcpr::list_checkpoints(&ckpt_dir)?;
    assert_eq!(
      tokens,
      vec![meta.token],
      "REST 态目录内只有本次发布的检查点"
    );

    OK
  })?;

  OK
}
