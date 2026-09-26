//! 检查点网络接收处理器单测（r316 自 src/server/replication/receive_checkpoint_handler.rs
//! 的 `#[cfg(test)] mod tests` 迁出，纯搬运零语义；仅两处 `handler.hlog_dirty
//! .load(Ordering::Acquire)` 直读私有字段改走既有同形公共开口
//! `is_hlog_dirty()`（其实现即同一 load，零语义差），其余逐字保留）

use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::{Device, SegmentedDevice};
use wedb::server::replication::{
  checkpoint_entry::CheckpointFileType,
  receive_checkpoint_handler::{CheckpointImportCtx, ReceiveCheckpointHandler},
  replication_manager::ReplicationManager,
};

/// 测试装配（四测试同形的 dir/device/rm/ctx 构建收口）：临时目录 +
/// 分段设备 + 复制管理器 + 导入上下文束；TempDir 由调用方持有，
/// 提前丢弃即删目录
fn fixture() -> (
  tempfile::TempDir,
  Arc<ReplicationManager>,
  CheckpointImportCtx,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::new(dir.path().join("store.log"), 1 << 20, 4096).unwrap());
  let ctx = CheckpointImportCtx {
    store_device: device,
    checkpoint_dir: dir.path().join("ckpts"),
    vector_manager: None,
  };
  (dir, Arc::new(ReplicationManager::new()), ctx)
}

#[test]
fn test_active_sink_token_and_type_matching() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, rm, ctx) = fixture();

    let handler = ReceiveCheckpointHandler::new();
    let token = 100u128;

    // 首段 StoreIndex 开槽写入
    let data = [1u8; 16];
    let res = handler
      .process_snapshot_data(&rm, &ctx, token, CheckpointFileType::StoreIndex, 0, &data)
      .await;
    assert!(res.is_ok(), "首段写入应该成功");

    // 同 token 但 file_type 跨类型变为 StoreHlog，应该返回协议错误
    let hlog_data = [2u8; 4096];
    let res2 = handler
      .process_snapshot_data(
        &rm,
        &ctx,
        token,
        CheckpointFileType::StoreHlog,
        0,
        &hlog_data,
      )
      .await;
    assert!(
      res2
        .as_ref()
        .is_err_and(|e| e.contains("checkpoint token or file_type changed")),
      "同 token 跨 file_type 必须报错拦截: {res2:?}"
    );

    // 异 token 也应该报错拦截
    let res3 = handler
      .process_snapshot_data(
        &rm,
        &ctx,
        token + 1,
        CheckpointFileType::StoreIndex,
        16,
        &data,
      )
      .await;
    assert!(
      res3
        .as_ref()
        .is_err_and(|e| e.contains("checkpoint token or file_type changed")),
      "异 token 必须报错拦截: {res3:?}"
    );

    Ok(())
  })
}

/// 本轮会话闸门：StoreHlog 段写失败即关闭本轮接收（后续快照段、文件段、
/// 元数据一律拒收），杜绝把半截覆盖伪装成完整快照喂给导入面
#[test]
fn test_store_hlog_write_failure_gates_current_session() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, rm, ctx) = fixture();

    let handler = ReceiveCheckpointHandler::new();
    assert!(!handler.is_device_contaminated());

    let token = 200u128;
    let res = handler
      .process_snapshot_data(
        &rm,
        &ctx,
        token,
        CheckpointFileType::StoreHlog,
        0,
        &[7u8; 4096],
      )
      .await;
    assert!(res.is_ok(), "StoreHlog 首块写入应该成功");
    assert!(handler.is_hlog_dirty());
    assert!(!handler.is_device_contaminated());

    // 非扇区对齐的段地址：设备写开口即判失败，本轮 hlog 半写不可信
    let res_bad = handler
      .process_snapshot_data(
        &rm,
        &ctx,
        token,
        CheckpointFileType::StoreHlog,
        1,
        &[8u8; 4096],
      )
      .await;
    assert!(res_bad.is_err(), "非对齐 hlog 段必须失败: {res_bad:?}");
    assert!(
      handler.is_device_contaminated(),
      "hlog 写失败必须关闭本轮接收闸门"
    );

    // 闸门关闭期间，三张接收入口全部拒收
    let res_snap = handler
      .process_snapshot_data(
        &rm,
        &ctx,
        token,
        CheckpointFileType::StoreIndex,
        0,
        &[1u8; 16],
      )
      .await;
    assert!(
      res_snap
        .as_ref()
        .is_err_and(|e| e.contains("refusing remaining checkpoint frames")),
      "本轮闸门关闭后快照段应被拒绝: {res_snap:?}"
    );

    let res_seg = handler
      .process_file_segment(
        &rm,
        &ctx,
        token,
        CheckpointFileType::StoreIndex,
        0,
        &[1u8; 16],
      )
      .await;
    assert!(
      res_seg
        .as_ref()
        .is_err_and(|e| e.contains("refusing remaining checkpoint frames")),
      "本轮闸门关闭后文件段应被拒绝: {res_seg:?}"
    );

    let res_meta = handler
      .process_metadata(
        &rm,
        &ctx,
        token,
        CheckpointFileType::StoreSnapshot,
        &[0u8; 32],
      )
      .await;
    assert!(
      res_meta
        .as_ref()
        .is_err_and(|e| e.contains("refusing remaining checkpoint frames")),
      "本轮闸门关闭后元数据应被拒绝: {res_meta:?}"
    );

    Ok(())
  })
}

/// 会话复位（C# 每轮 attach `recvCheckpointHandler = new(...)` 对位）：
/// 新一轮全量必须被放行并自起始地址覆盖上轮半写，同时把未恢复的半写
/// 事实回报给调用面升级到管理面屏障
#[test]
fn test_reset_opens_next_session_and_reports_half_write() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, rm, ctx) = fixture();
    let reader = Arc::clone(&ctx.store_device);

    let handler = ReceiveCheckpointHandler::new();
    let token = 400u128;
    handler
      .process_snapshot_data(
        &rm,
        &ctx,
        token,
        CheckpointFileType::StoreHlog,
        0,
        &[7u8; 4096],
      )
      .await
      .unwrap();
    handler.mark_device_contaminated();
    assert!(handler.is_device_contaminated());

    // 重连复位：本轮半写须上报（调用点据此升级管理面屏障）
    assert!(
      handler.reset(),
      "未恢复的 hlog 半写在复位时必须上报: 见 ClusterProvider::reset_recv_checkpoint_handler"
    );
    // 复位即净态：本轮闸门与脏标记不得跨会话闭锁后续一切快照
    assert!(
      !handler.is_device_contaminated(),
      "复位后接收闸门必须净态，否则重连的全量重试被自己永久拒收"
    );
    assert!(!handler.is_hlog_dirty());

    // 新一轮从头分块重推：必须收下并顺序覆盖旧半写
    let res = handler
      .process_snapshot_data(
        &rm,
        &ctx,
        token,
        CheckpointFileType::StoreHlog,
        0,
        &[9u8; 4096],
      )
      .await;
    assert!(res.is_ok(), "新一轮全量首块必须被收下: {res:?}");
    let written = reader.read_range(0, 4096).await.unwrap();
    assert!(
      written.iter().all(|b| *b == 9),
      "新一轮必须从头覆盖旧半写数据"
    );

    Ok(())
  })
}

/// 一次成功的全量恢复收口 = 污染两面标记的闭环清除点
#[test]
fn test_recovery_success_cleans_contamination_gate() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, rm, ctx) = fixture();

    let handler = ReceiveCheckpointHandler::new();
    let token = 300u128;
    let data = vec![0u8; 4096];
    handler
      .process_snapshot_data(&rm, &ctx, token, CheckpointFileType::StoreHlog, 0, &data)
      .await
      .unwrap();
    // 导入面失败即置位（try_replica_diskbased_recovery 的
    // mark_contaminated_if_dirty 形态）
    handler.mark_device_contaminated();
    assert!(handler.is_device_contaminated());

    // 模拟恢复成功收尾：两面标记一并闭环清除，否则一次导入失败永久
    // 闭锁本节点的后续全量
    handler.on_recovery_success();
    assert!(!handler.is_hlog_dirty());
    assert!(!handler.is_device_contaminated());

    // 净态后的 reset 既不上报半写也不置位
    assert!(!handler.reset());
    assert!(!handler.is_device_contaminated());

    // 收口后仍可继续接收新一轮快照
    handler
      .process_snapshot_data(
        &rm,
        &ctx,
        token + 1,
        CheckpointFileType::StoreIndex,
        0,
        &[1u8; 16],
      )
      .await
      .expect("成功恢复后的新一轮快照必须被收下");

    Ok(())
  })
}
