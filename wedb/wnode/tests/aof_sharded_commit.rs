//! 分片拓扑 AOF 提交扇出集成测试
//! （对标 libs/server/AOF/GarnetLog.cs:CommitAsync 与 :WaitForCommitAsync 的
//! `Task.WhenAll` 形态：全部物理子日志并发发起刷盘、全体落盘后才返回；
//! 单个子日志刷盘失败按聚合语义处理，不取消、不短路兄弟子日志）。

use std::{sync::Arc, time::Duration};

use compio::{runtime::Runtime, time::sleep};
use futures_util::future::join;
use tempfile::{TempDir, tempdir};
use waof::{SequenceNumberGenerator, WalConfig, WalLog};
use wbase::align::DEFAULT_SECTOR_SIZE;
use wconf::RuntimeServerOptions;
use wdev::{DeviceParams, SegmentedDevice};
use wnode::aof::{
  garnet_log::GarnetLog,
  waof_sublog::{AofSublog, WaofSublog},
};

/// 分片拓扑子日志数（aof_physical_sublog_count > 1 才进入扇出分支）
const SUBLOGS: usize = 4;

/// 注入刷盘失败的子日志索引
const FAILING: usize = 1;

/// 每子日志一个提交批的体量：32 × 256KB = 8MB（默认 16MB 环形窗口的一半）。
/// 单批越大，一次刷盘窗口越长，在途采样越稳
const FILL_ROUNDS: usize = 32;
const FILL_CHUNK: usize = 256 * 1024;

/// 在途采样间隔与预算（预算 ≈ 0.8s，覆盖串行形态的整轮耗时）
const SAMPLE_INTERVAL: Duration = Duration::from_micros(200);
const SAMPLE_BUDGET: usize = 4_000;

/// 真实段设备子日志后端（构造期注入 read_only 使设备面 `write_aligned` 直返 ReadOnly，
/// 用于注入刷盘失败；入队侧纯内存不受影响）
fn make_sublog(tag: &str, read_only: bool) -> (TempDir, Arc<AofSublog>) {
  let dir = tempdir().expect("临时目录");
  let device = SegmentedDevice::with_params(
    dir.path().join(format!("{tag}.wal")),
    None,
    DEFAULT_SECTOR_SIZE,
    DeviceParams {
      read_only,
      ..DeviceParams::default()
    },
  )
  .expect("段设备装配");
  let wal = Arc::new(WalLog::new(Arc::new(device), WalConfig::default()).expect("WalLog 装配"));
  (dir, Arc::new(WaofSublog::new(wal)))
}

/// 分片拓扑 GarnetLog（单一序列号生成器 = C# cookieGeneratorCallback 单点取号）
fn make_sharded_log(backends: Vec<Arc<AofSublog>>) -> GarnetLog {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: backends.len() as i32,
    aof_replay_task_count: 1,
    ..RuntimeServerOptions::default()
  };
  GarnetLog::new(
    &options,
    backends,
    Some(Arc::new(SequenceNumberGenerator::new(0))),
  )
  .expect("分片拓扑构造")
}

/// 逐子日志写满一个提交批，返回各自提交目标（扇出前的尾地址）
fn fill_batch(log: &GarnetLog) -> Vec<i64> {
  let payload = vec![b'x'; FILL_CHUNK];
  (0..log.size())
    .map(|i| {
      let sublog = log.get_sub_log(i);
      for _ in 0..FILL_ROUNDS {
        sublog.enqueue(&payload).expect("记录入环形缓冲");
      }
      sublog.tail_address()
    })
    .collect()
}

/// 峰值在途子日志数采样：同处「已发起刷盘且提交水位未达目标」的子日志个数。
///
/// 发起标记取子日志 cookie——`WaofSublog::commit_flush_async` 在 await 设备刷盘
/// 之前先落 cookie。串行 for-await 下前一子日志未达水位时后一个尚未发起，
/// 峰值恒为 1；WhenAll 形态下全部子日志在同一轮 poll 内发起，峰值 = 子日志数。
async fn in_flight_peak(log: &GarnetLog, targets: &[i64]) -> usize {
  let mut peak = 0;
  for _ in 0..SAMPLE_BUDGET {
    let in_flight = targets
      .iter()
      .enumerate()
      .filter(|(i, target)| {
        let sublog = log.get_sub_log(*i);
        sublog.recovered_cookie().is_some() && sublog.committed_until_address() < **target
      })
      .count();
    peak = peak.max(in_flight);
    if peak == targets.len() {
      break;
    }
    sleep(SAMPLE_INTERVAL).await;
  }
  peak
}

/// 断言目标子日志均已落盘（`except` 索引允许未落盘，用于失败注入）
fn assert_flushed(log: &GarnetLog, targets: &[i64], except: Option<usize>, ctx: &str) {
  for (i, target) in targets.iter().enumerate() {
    if Some(i) == except {
      continue;
    }
    assert!(
      log.get_sub_log(i).committed_until_address() >= *target,
      "{ctx}：子日志 {i} 提交水位未达目标"
    );
  }
}

/// 分片扇出为并发等待（本条主证）：提交返回前全部子日志均已落盘，
/// 且同一次扇出共用单点取号的 cookie（对标 GarnetLog.cs:524-530）
#[test]
fn sharded_commit_awaits_all_sublogs_concurrently() {
  let (_dirs, backends): (Vec<TempDir>, Vec<_>) = (0..SUBLOGS)
    .map(|i| make_sublog(&format!("aof_whenall_{i}"), false))
    .unzip();
  let log = make_sharded_log(backends);
  let targets = fill_batch(&log);
  for (i, target) in targets.iter().enumerate() {
    let sublog = log.get_sub_log(i);
    assert_eq!(sublog.recovered_cookie(), None, "扇出前不应有提交 cookie");
    assert!(
      sublog.committed_until_address() < *target,
      "前置条件：子日志 {i} 待刷盘"
    );
  }

  let peak = Runtime::new().expect("compio 运行时").block_on(async {
    let ((), peak) = join(log.commit_async(), in_flight_peak(&log, &targets)).await;
    peak
  });

  assert!(
    peak >= 2,
    "分片提交扇出峰值在途子日志数 {peak}：N 次刷盘被串成求和，\
     与 C# GarnetLog.cs:530 Task.WhenAll（耗时上界）不一致"
  );
  assert_flushed(&log, &targets, None, "commit_async 返回后");

  let cookie = log
    .get_sub_log(0)
    .recovered_cookie()
    .expect("分片提交须带序列号 cookie");
  for i in 1..SUBLOGS {
    assert_eq!(
      log.get_sub_log(i).recovered_cookie(),
      Some(cookie),
      "子日志 {i} 的 cookie 与本次扇出取号不同（应为单点取号广播）"
    );
  }
}

/// 等待面语义与失败传播：一个子日志设备只读（刷盘必失败）时，
/// `commit_async` 与 `wait_for_commit_all_async` 仍全体返回，
/// 兄弟子日志全部落盘（WhenAll 聚合，非首个错误即取消其余）
#[test]
fn sharded_commit_aggregates_sublog_failure_without_cancelling_siblings() {
  let (_dirs, backends): (Vec<TempDir>, Vec<_>) = (0..SUBLOGS)
    .map(|i| make_sublog(&format!("aof_whenall_fail_{i}"), i == FAILING))
    .unzip();
  let log = make_sharded_log(backends);
  let targets = fill_batch(&log);
  let rt = Runtime::new().expect("compio 运行时");

  rt.block_on(log.commit_async());
  assert_flushed(&log, &targets, Some(FAILING), "兄弟子日志被取消/短路");
  assert!(
    log.get_sub_log(FAILING).committed_until_address() < targets[FAILING],
    "只读设备子日志不应前移提交水位"
  );

  // 等待面：失败子日志的等待内部告警后返回，既不挂死也不阻断其余子日志
  rt.block_on(log.wait_for_commit_all_async(0));
  for (i, target) in targets.iter().enumerate() {
    let sublog = log.get_sub_log(i);
    if i == FAILING {
      assert!(
        sublog.committed_until_address() < *target,
        "等待面不应替失败子日志伪造落盘"
      );
    } else {
      assert!(
        sublog.committed_until_address() >= *target,
        "wait_for_commit_all_async 返回后子日志 {i} 未落盘"
      );
    }
  }
}
