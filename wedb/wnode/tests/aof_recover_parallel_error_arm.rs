#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 并行恢复中间批错误臂有界收敛回归（对标 C# RecoverReplayTask.cs:45-74
//! 错误臂 CancelAsync 后 leader 经 cts.Token 解除闸栏、RecoverLogDriver.cs
//! RunAsync:214 OCE 收敛——恢复过程有界结束，错误沿装配链上抛拒启）
//!
//! 回归点（task/ing/zcode-r27-boottimeline 发现一）：Worker 回放错误臂置
//! error_slot 与 is_cancelled 后其余 Worker 在循环头退出；leader 侧 consume
//! 借取消位截停扫描后，run 的尾批冲刷臂若不检查 is_cancelled 会对已退出的
//! Worker 再发一次批次，ready 闸（participant = replay_task_count + 1）永无
//! 全员会合，启动装配段冻结、error_slot 内错误因 join 不可达而永不落账——
//! 中段坏记录（错误后日志中尚存至少一批记录且扫描缓冲残留非空）即点亮。
//!
//! 契约合一断言：单任务路径对同一损坏输入同样快速上抛（process 版本域门
//! 拒绝），并行修复后行为与单任务分叉收敛。

use std::{sync::Arc, time::Duration};

use compio::time::timeout;
use waof::WalConfig;
use wnode::aof::{
  aof_processor::AofProcessor, garnet_append_only_file::GarnetAppendOnlyFile,
  recover::aof_recover::AofRecover,
};
use wnode_test::enqueue_set_at;
use wtest_base::open_test_store_with_budget;

/// 错误臂收敛有界时限：正常毫秒级完成，并发跑批调度争用放大下设 30s，
/// 修复前（冲刷臂缺取消检查）在此确定性判失败而非无限死等
const ERROR_ARM_TIMEOUT: Duration = Duration::from_secs(30);

/// 损坏记录字节：flags 全 1 → 头型位段（低 3 位）= 0b111 落在
/// AofHeaderType 全集之外，并行 can_replay 显式上抛 UnsupportedReplayHeaderType；
/// 版本域亦非当前构建代际，单任务 process 版本门同样拒绝
const CORRUPT_ENTRY: [u8; 32] = [0xFF; 32];

/// 错误前注入记录数：须跨过一批（扫描批次 256 条）使错误落于已冲刷批次，
/// 且错误截停扫描时缓冲残留非空
const PRE_CORRUPT_RECORDS: usize = 300;

/// 错误后续写记录数：错误后日志尾段仍有记录（冻结链的点亮条件）
const POST_CORRUPT_RECORDS: usize = 300;

/// 装配含损坏中段记录的单物理日志 AOF：前段正常 → 损坏记录 → 后段正常，
/// 物理刷盘落定恢复扫描面（`replay_task_count` 决定恢复走并行/单任务路径）
async fn build_aof_with_mid_corruption(
  case: &str,
  replay_task_count: i32,
) -> waof::Result<Arc<GarnetAppendOnlyFile>> {
  let aof = wnode_test::parallel_aof(case, replay_task_count, WalConfig::default())?;
  let log = aof.log();

  for i in 0..PRE_CORRUPT_RECORDS {
    enqueue_set_at(log, 5, 7, 0, format!("pre-{i}").as_bytes(), b"v")?;
  }
  // 裸字节损坏条目（绕过形状编码，恢复扫描按原样读回 payload）
  log.get_sub_log(0).enqueue(&CORRUPT_ENTRY)?;
  for i in 0..POST_CORRUPT_RECORDS {
    enqueue_set_at(log, 5, 7, 0, format!("post-{i}").as_bytes(), b"v")?;
  }

  log.commit_async().await;
  Ok(aof)
}

/// 中段坏记录 + 并行回放（replay_task_count>1）：恢复须在有界时间返回 Err
/// 并携带损坏条目身份，而非冻结于双闸栏（修复前冲刷臂对已退出 Worker 续
/// 发布批次，ready 闸永挂、error_slot 永不落账）
#[compio::test]
async fn mid_batch_corruption_parallel_recover_bounded_error() -> aok::Result<()> {
  let aof = build_aof_with_mid_corruption("aof_para_err_mid", 2).await?;

  let (_dir, store) = open_test_store_with_budget("aof-para-err-mid-dst.db", 64 << 20)?;
  wnode_test::replay_target!(store, 5, vec![], storage, target);
  let processor = AofProcessor::new(Arc::clone(&aof));

  let recover = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target);
  let inner = timeout(ERROR_ARM_TIMEOUT, recover)
    .await
    .expect("恢复不得冻结于双闸栏（有界时限外仍无返回）");
  let err = inner.expect_err("中段损坏记录必须使恢复以 Err 收敛");
  let msg = err.to_string();
  assert!(
    msg.contains("不支持的回放头类型") || msg.contains("Unsupported"),
    "错误须携带损坏条目身份透出，实得: {msg}"
  );
  Ok(())
}

/// 同一损坏输入的单任务对照臂（replay_task_count=1）：无闸栏路径直接上抛，
/// 并行修复后两路径对同一带病 AOF 的快速失败契约收敛（修复前并行臂冻结、
/// 单任务臂报错退出的行为分叉即本票危害面）
#[compio::test]
async fn mid_batch_corruption_single_task_recover_fails_fast() -> aok::Result<()> {
  let aof = build_aof_with_mid_corruption("aof_para_err_single", 1).await?;

  let (_dir, store) = open_test_store_with_budget("aof-para-err-single-dst.db", 64 << 20)?;
  wnode_test::replay_target!(store, 5, vec![], storage, target);
  let processor = AofProcessor::new(Arc::clone(&aof));

  let inner = timeout(
    ERROR_ARM_TIMEOUT,
    AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target),
  )
  .await
  .expect("单任务恢复亦须有界返回");
  let err = inner.expect_err("单任务路径对同一损坏输入必须快速失败");
  let msg = err.to_string();
  assert!(
    msg.contains("Unsupported AOF format version") || msg.contains("不支持的回放头类型"),
    "单任务臂错误须为版本域/头型拒绝，实得: {msg}"
  );
  Ok(())
}
