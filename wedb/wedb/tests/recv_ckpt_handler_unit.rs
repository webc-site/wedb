#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 检查点网络接收处理器单测（r316 自 src/server/replication/receive_checkpoint_handler.rs
//! 的 `#[cfg(test)] mod tests` 迁出，纯搬运零语义；仅两处 `handler.hlog_dirty
//! .load(Ordering::Acquire)` 直读私有字段改走既有同形公共开口
//! `is_hlog_dirty()`（其实现即同一 load，零语义差），其余逐字保留）

use std::{pin::pin, sync::Arc};

use compio::runtime::Runtime;
use wdev::{Device, SegmentedDevice};
use wedb::server::replication::{
  checkpoint_entry::CheckpointFileType,
  error::ReplicationError,
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
        .is_err_and(
          |e| matches!(e, ReplicationError::Protocol(m) if m.contains("checkpoint token or file_type changed")),
        ),
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
      res3.as_ref().is_err_and(|e| matches!(
        e,
        ReplicationError::Protocol(m) if m.contains("checkpoint token or file_type changed")
      )),
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
        .is_err_and(
          |e| matches!(e, ReplicationError::Protocol(m) if m.contains("refusing remaining checkpoint frames")),
        ),
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
      res_seg.as_ref().is_err_and(|e| matches!(
        e,
        ReplicationError::Protocol(m) if m.contains("refusing remaining checkpoint frames")
      )),
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
      res_meta.as_ref().is_err_and(|e| matches!(
        e,
        ReplicationError::Protocol(m) if m.contains("refusing remaining checkpoint frames")
      )),
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

/// 取消安全（写驱动提交后的会话断连窗）：STORE_HLOG 段写取消落在
/// `device.write_aligned` 挂起点（写 op 已提交 compio 驱动、可能已落地覆盖
/// 在线引擎设备）时，活跃槽随未来体弃置、失败臂未走、成功臂置脏不可达——
/// 脏标记必须在入 await 前置位，[`ReceiveCheckpointHandler::reset`] 才能在
/// 「写已驱动但会话死」场景上报半写，重连臂（assembly.rs:recover_replication
/// → ClusterProvider::reset_recv_checkpoint_handler）据此升级管理面屏障，
/// 窗口内本地 take_checkpoint/flush 不得在被污染设备上放行
///
/// 构造方式（无网络 mock）：手动 poll `process_snapshot_data` 未来体一次——
/// 设备写 op 提交内核即挂起——随后弃置未来体即会话取消；若后端首 poll 内联
/// 收尾（写已完成），各断言在修复前后同样成立，测试跨后端稳定
#[test]
fn test_cancelled_mid_device_write_still_reports_half_write_on_reset() -> aok::Void {
  use std::task::{Context, Waker};

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, rm, ctx) = fixture();

    let handler = ReceiveCheckpointHandler::new();
    let token = 500u128;

    // 单次 poll 驱动至设备写挂起点，块尾弃置未来体 = 会话断连取消
    let mut cx = Context::from_waker(Waker::noop());
    {
      let fut = handler.process_snapshot_data(
        &rm,
        &ctx,
        token,
        CheckpointFileType::StoreHlog,
        0,
        &[0x5au8; 4096],
      );
      let mut fut = pin!(fut);
      let _ = fut.as_mut().poll(&mut cx);
    }

    // 取消窗核心判据：写已驱动即脏——槽已空、失败臂未走，脏标记是 reset
    // 上报半写的唯一判据
    assert!(
      handler.is_hlog_dirty(),
      "段写进入 await 即须置脏：取消落在挂起点时槽已弃置，漏置即漏升级管理面屏障"
    );
    assert!(
      !handler.is_device_contaminated(),
      "取消不是写失败：本轮接收闸门不得闭锁，重连重推必须放行"
    );
    assert!(
      handler.reset(),
      "写已驱动但会话死：reset 必须上报半写供重连臂升级管理面屏障"
    );
    assert!(
      !handler.is_hlog_dirty() && !handler.is_device_contaminated(),
      "复位即净态，新一轮全量不得被取消残留闭锁"
    );

    // 复位后新一轮自起始地址重推照常收下
    let res = handler
      .process_snapshot_data(
        &rm,
        &ctx,
        token,
        CheckpointFileType::StoreHlog,
        0,
        &[0xa5u8; 4096],
      )
      .await;
    assert!(res.is_ok(), "取消复位后新一轮全量首块必须被收下: {res:?}");

    Ok(())
  })
}
