//! 检查点临界区单槽互斥契约：宿主槽位被占时 create_checkpoint 必须在闸门内即时
//! 拒绝且零副作用，退出复位后恢复（对标 C# StateMachineDriver.cs:167-190 单槽
//! `Interlocked.CompareExchange(ref stateMachine, sm, null)` 注册——检查点状态机
//! 重复注册同一槽位即失败，与扩容状态机双向互斥）

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{CheckpointType, CkptGateState, CprStore, Error};

use super::support::MiniStore;

/// 槽位被占：发起即拒绝零副作用；复位后正常创建（覆盖 create_gated 闸门内
/// enter_checkpoint 拒绝路径、trait 端口经 `Arc<MiniStore>` 透明转发）
#[test]
fn checkpoint_slot_is_enforced_by_create_path() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("ckpt_slot.db"))?;
    let gate = CkptGateState::default();

    let p = store.session()?;
    store.put(&p, b"slot_seed", b"seed_val").await?;

    // 占槽：重复进入必须失败（单槽语义）
    store.enter_checkpoint()?;
    assert!(
      store.enter_checkpoint().is_err(),
      "检查点槽位被占时二次进入必须失败"
    );

    // 发起被拒：闸门前 is_growing 放行（REST 态），闸门内 enter CAS 失败拒绝
    let err = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver)
      .await
      .expect_err("槽位被占时发起 Checkpoint 必须被拒绝");
    assert!(matches!(err, Error::Host(_)), "必须返回 Host 错误: {err}");
    assert!(!ckpt_dir.exists(), "拒绝路径零副作用，不得创建检查点目录");

    // 复位后正常创建
    store.exit_checkpoint();
    let meta = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
    assert_eq!(
      meta.index_meta.entry_count, 1,
      "复位后快照必须完整收录种子条目"
    );

    OK
  })?;

  OK
}
