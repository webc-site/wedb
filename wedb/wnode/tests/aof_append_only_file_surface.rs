//! Garnet 追加日志文件表面集成测（自 src/aof/garnet_append_only_file.rs 内联
//! 测模块迁入：tempdir + SegmentedDevice 真段设备形态，断言与覆盖原样保留）
//!
//! 覆盖：总尺寸/虚拟子日志换算、关停对偶（dispose 落盘 + 提交帧角色分派 +
//! 背压闸放行）、序列号推进、一致性管理器代际与漂移参数转发、多子日志尾地址
//! 探针。子日志装配（src/aof/test_support.rs 的集成测试对应物）按其文档指引
//! 在本文件内联重写——依赖面全部为既有 pub API（waof/WaofSublog/GarnetLog），
//! 零 crate 私有项。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::TempDir;
use waof::{AofEntryType, SequenceNumberGenerator, WalConfig, WalLog};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, PrimaryTasks,
  aof::waof_sublog::{AofSublog, WaofSublog},
};

/// 轻量真实段设备子日志：tempfile + `SegmentedDevice` 单文件 + `WalLog`
/// 默认配置（测试统一走真实设备，杜绝 mock 抽象；与 src/aof/test_support.rs
/// 同一装配口径）
fn test_sublog(tag: &str) -> (TempDir, Arc<AofSublog>) {
  let dir = tempfile::tempdir().expect("tempdir");
  let device = Arc::new(
    SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).expect("SegmentedDevice"),
  );
  let wal = WalLog::new(device, WalConfig::default()).expect("WalLog");
  (dir, Arc::new(WaofSublog::new(Arc::new(wal))))
}

fn aof_with(sublogs: usize, replay_tasks: i32) -> (Vec<TempDir>, Arc<GarnetAppendOnlyFile>) {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: sublogs as i32,
    aof_replay_task_count: replay_tasks,
    commit_frequency_ms: 50,
    ..RuntimeServerOptions::default()
  };
  let dirs = (0..sublogs.max(1))
    .map(|i| {
      let (dir, backend) = test_sublog(&format!("aof_{i}"));
      (dir, backend)
    })
    .collect::<Vec<_>>();
  let backends: Vec<Arc<AofSublog>> = dirs.iter().map(|(_, b)| Arc::clone(b)).collect();
  let seq_num_gen = (sublogs > 1).then(|| Arc::new(SequenceNumberGenerator::new(0)));
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, seq_num_gen.clone()).expect("构造 GarnetLog")),
    &options,
    seq_num_gen,
  ));
  (dirs.into_iter().map(|(d, _)| d).collect(), aof)
}

#[test]
fn total_size_and_virtual_index() {
  let (_dirs, aof) = aof_with(1, 1);
  assert_eq!(aof.total_size(), 0);
  let _ = aof.enqueue_raw(AofEntryType::StoreUpsert, (1, 1), b"k", b"vv", &[]);
  assert!(aof.total_size() > 0);
  assert_eq!(aof.get_virtual_sublog_idx(0, 0), 0);
  assert_eq!(aof.virtual_sublog_count(), 1);
}

/// 关停对偶：dispose 收口未提交帧并放行闸门（C# GarnetAppendOnlyFile.
/// Dispose 的 backpressure?.Dispose + Log.Dispose 对位）
#[test]
fn dispose_async_flushes_frames_and_releases_gate() {
  let (_dirs, aof) = aof_with(1, 1);
  let _ = aof.enqueue_raw(AofEntryType::StoreUpsert, (1, 1), b"k", b"vv", &[]);
  let tail = aof.log().tail_address().max();
  let sublog = aof.log().get_sub_log(0);
  assert!(
    sublog.flushed_until_address() < tail,
    "预置：dispose 前未刷盘"
  );

  Runtime::new().unwrap().block_on(aof.dispose_async());

  assert!(
    sublog.flushed_until_address() >= tail,
    "dispose 后未提交帧已落设备"
  );
  // 主端分派写 commit 元数据帧：尾位点随帧推进（与副本分派用例的
  // 「尾位点不动」断言对偶，锁死双向角色分派）
  assert!(
    aof.log().tail_address().max() > tail,
    "主端 dispose 随 commit 帧推进尾位点"
  );
  // 闸门放行：dispose 置位后任何滞后快照均判释放
  assert!(aof.backpressure().unwrap().is_released(0, i64::MAX));
}

/// 停机分派：副本角色纯刷盘不写本地 commit 元数据帧（已收记录落设备、
/// 尾位点不动——副本 AOF 为主端流严格镜像，本地帧会令重启增量协商位点
/// 漂出主端帧边界）；主端分派对偶断言（尾位点随 commit 帧推进）见上例。
/// 角色装配先于入队（自动提交面在角色闸内）；停机收口前先经异步提交入口
/// （commit_aof/检查点/周期任务同源生产面）驱动一次副本落盘——r57 红即
/// 该面常驻协程 commit_to 写帧的交错竞态，此驱动把竞窗折叠为确定路径
#[test]
fn dispose_async_replica_flushes_without_commit_frame() {
  let (_dirs, aof) = aof_with(1, 1);
  let tasks = Arc::new(PrimaryTasks::default());
  tasks.suspend();
  aof.attach_primary_tasks(tasks);
  let _ = aof.enqueue_raw(AofEntryType::StoreUpsert, (1, 1), b"k", b"vv", &[]);
  let tail = aof.log().tail_address().max();
  let sublog = aof.log().get_sub_log(0);

  let rt = Runtime::new().unwrap();
  rt.block_on(aof.log().commit_async());
  rt.block_on(aof.dispose_async());

  assert!(
    sublog.flushed_until_address() >= tail,
    "副本 dispose 后已收记录须落设备"
  );
  assert_eq!(
    aof.log().tail_address().max(),
    tail,
    "副本纯刷盘不写本地 commit 帧，尾位点不动"
  );
}

#[test]
fn sequence_numbers_strictly_increase_past_tail() {
  let (_dirs, aof) = aof_with(2, 1);
  let first = aof.get_sequence_number();
  let larger = aof.get_larger_than_maximum_sequence_number();
  assert!(larger > first);
  assert!(aof.get_larger_than_maximum_sequence_number() >= larger);
}

#[test]
fn consistency_manager_generation_bump() {
  let (_dirs, aof) = aof_with(2, 1);
  let v1 = aof.read_consistency_manager().unwrap();
  assert_eq!(v1.current_version(), 1);
  aof.create_or_update_key_sequence_manager();
  let v2 = aof.read_consistency_manager().unwrap();
  assert_eq!(v2.current_version(), 2, "代际 = 前代 + 1");
}

#[test]
fn reset_sequence_generator_after_replay() {
  let (_dirs, aof) = aof_with(2, 1);
  let manager = aof.read_consistency_manager().unwrap();
  manager.update_physical_sublog_max_sequence_number(0, 77);
  manager.update_physical_sublog_max_sequence_number(1, 42);
  aof.reset_sequence_number_generator();
  // 抬升后取号不小于已回放最大序列号（同毫秒重取同值，时钟前进才严格增大）
  assert!(aof.get_sequence_number() >= 77, "恢复后时间前进");
}

#[test]
fn invalid_address_shape() {
  let (_dirs, aof) = aof_with(2, 1);
  let invalid = aof.invalid_aof_address();
  assert_eq!(invalid.length(), 2);
  assert_eq!(invalid.get(0), Some(-1));
}

#[test]
fn multi_sublog_probe_methods() {
  // TempDir 绑定持目录存活至测试结束，段文件随 Drop 清理
  let (_dirs, aof) = aof_with(2, 1);
  let sub0 = Arc::clone(aof.log().get_sub_log(0));
  let sub1 = Arc::clone(aof.log().get_sub_log(1));

  // 初始状态：双子日志空日志（真实段设备首地址 0：begin=tail=flushed=committed=0）
  assert_eq!(aof.log().tail_address().max(), 0);

  // 仅向 sublog 1 写入数据，0 号保持空闲
  let addr = sub1.enqueue(b"sublog_1_record").unwrap();
  assert_eq!(addr, 0, "首条记录落设备首地址");
  assert!(sub1.tail_address() > 0);
  assert_eq!(sub0.tail_address(), 0);

  // 尾地址取所有子日志的最大尾地址
  assert_eq!(aof.log().tail_address().max(), sub1.tail_address());

  // 提交 sub1
  let sub1_tail = sub1.tail_address();
  sub1.commit(sub1_tail, 0);

  // 向 sub0 写入更大长度数据并提交
  let _ = sub0.enqueue(b"sublog_0_longer_payload_data");
  let sub0_tail = sub0.tail_address();
  sub0.commit(sub0_tail, 0);

  // 尾地址应取各子日志尾地址的最大值
  let expected_max_tail = sub0.tail_address().max(sub1.tail_address());
  assert_eq!(aof.log().tail_address().max(), expected_max_tail);
}

#[test]
fn drift_options_forwarded_to_consistency_manager() {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 2,
    aof_replay_task_count: 1,
    replay_drift_threshold: 10,
    replay_drift_check_freq: 2,
    ..RuntimeServerOptions::default()
  };
  let (dir0, b0) = test_sublog("aof_drift_0");
  let (dir1, b1) = test_sublog("aof_drift_1");
  let seq_num_gen = Some(Arc::new(SequenceNumberGenerator::new(0)));
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        vec![Arc::clone(&b0), Arc::clone(&b1)],
        seq_num_gen.clone(),
      )
      .expect("构造 GarnetLog"),
    ),
    &options,
    seq_num_gen,
  ));
  let rcm = aof.read_consistency_manager().expect("存在一致性管理器");
  assert_eq!(rcm.current_version(), 1);
  assert_eq!(rcm.virtual_sublog_count(), 2);
  assert_eq!(rcm.vsr(0).next_drift_check_window_lower_bound(), 0);
  assert_eq!(rcm.vsr(1).next_drift_check_window_lower_bound(), 20);
  drop(dir0);
  drop(dir1);
}
