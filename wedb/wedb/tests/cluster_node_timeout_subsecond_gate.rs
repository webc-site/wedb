//! cluster-node-timeout 亚秒拒启、上限拒启与秒槽播种一致性（真闭环回归）
//!
//! 对标 garnet C# 契约：--cluster-timeout 为秒整型（Options.cs:298-300
//! IntRangeValidation(0, int.MaxValue)），0 经 GetTimeSpan 归
//! Timeout.InfiniteTimeSpan（RuntimeServerConfig.cs:314-329），槽值与生效值
//! 恒一致。rust CLI 毫秒粒度自引入回显面漏洞，以亚秒拒启（0 无限哨兵豁免）
//! 收口低边，保持同一不变量：CONFIG GET 回显（秒槽 get_time_span 读面）与
//! provider 生效槽（cluster_node_timeout）恒同向，绝不出现「回显无限、
//! 实际亚秒超时」语义反转。高边同册承接：C# 契约带上界 i32::MAX 秒原样
//! 搬到毫秒域（i32::MAX*1000），越界系契约外笔误，boot 闸名单拒启。

use itoa::Buffer;
use wconf::{
  ConfigFileArgs, RuntimeServerConfig, ServerConfigType,
  runtime_server_options::DEFAULT_CLUSTER_NODE_TIMEOUT_MS,
};
use wedb::{
  ClusterArgs, Error as WedbError, run_cluster_server,
  server::{
    boot::{
      cluster_node_timeout_seed_secs, is_oversized_cluster_node_timeout,
      is_subsecond_cluster_node_timeout,
    },
    cluster_provider::ClusterProvider,
  },
};
use wnode::Error as WnodeError;

/// 亚秒毫秒值（500ms 为票面案值，1/499/999 覆盖拒启域）在 run_cluster_server
/// 顶层即被拒——对齐 gossip 抽样越界拒启先例（InvalidArgument），未及任何
/// bootstrap 装配副作用
#[test]
fn subsecond_ms_rejected_before_boot() {
  for ms in ["1", "499", "500", "999"] {
    let args = ClusterArgs::from_args_iter(["wedb", "--cluster-node-timeout-ms", ms]).unwrap();
    match run_cluster_server(args, None) {
      Err(WedbError::Node(WnodeError::InvalidArgument(msg))) => {
        assert!(
          msg.contains("cluster-node-timeout-ms"),
          "错误信息应指明 cluster-node-timeout-ms: {msg}"
        );
      }
      other => panic!("{ms}ms 应拒启（InvalidArgument），实际: {other:?}"),
    }
  }
}

/// 亚秒门域边界：0 无限哨兵豁免（args.rs 明写 0 = 无限超时，字面 < 1000
/// 会误杀）、1..=999 拒、>= 1000 放行
#[test]
fn subsecond_gate_boundary() {
  assert!(
    !is_subsecond_cluster_node_timeout(0),
    "0 = 无限哨兵必须豁免"
  );
  for ms in 1..1000u64 {
    assert!(is_subsecond_cluster_node_timeout(ms), "{ms}ms 应判亚秒");
  }
  for ms in [1000u64, 1001, 60_000, u64::MAX] {
    assert!(!is_subsecond_cluster_node_timeout(ms), "{ms}ms 应放行");
  }
}

/// 播种一致性真闭环：boot 播种式（seed_secs 经 itoa 进 try_set 落秒槽）后，
/// CONFIG GET 读面（get_time_span）与 provider 生效槽（cluster_node_timeout）
/// 恒同向——有限 ⟺ 有限，回显永不把有限生效值显成无限；整除截断仅损秒内
/// 粒度（1500ms 显 1s，C# 秒槽原生粒度），不反转语义
#[test]
fn seed_secs_keeps_config_echo_aligned_with_provider_slot() {
  let cp = ClusterProvider::new();
  let config = RuntimeServerConfig::with_defaults();
  let mut buf = Buffer::new();
  for ms in [0u64, 1000, 1500, 30_000, 60_000, u64::MAX] {
    assert!(!is_subsecond_cluster_node_timeout(ms), "{ms}ms 须在合法域");
    cp.set_cluster_node_timeout_ms(ms);
    let secs = cluster_node_timeout_seed_secs(ms);
    assert_eq!(secs > 0, ms > 0, "{ms}ms 播种值有限性与生效槽须同向");
    config
      .try_set(ServerConfigType::ClusterNodeTimeout, buf.format(secs))
      .unwrap();
    let echo = config.get_time_span(ServerConfigType::ClusterNodeTimeout);
    let effective = cp.cluster_node_timeout();
    assert_eq!(
      echo.is_none(),
      effective.is_none(),
      "{ms}ms 回显与生效槽无限性必须一致（不得语义反转）"
    );
  }
  // 环绕防线：u64::MAX 整除后饱和 i32::MAX，不落负值触发「非正即无限」反转
  //（与 cluster_node_timeout.rs 的 C# 换算回归映射互为两章程，有意双章）
  assert_eq!(cluster_node_timeout_seed_secs(u64::MAX), i32::MAX as i64);
}

/// 超大毫秒档拒启（契约带上界拒收断言，非溢出 panic 断言）：clap 裸 u64
/// 解析（args.rs:72-73 无 value_parser）令 1e16 与 u64::MAX 字面哨兵档
/// 照常进槽（值源可达实证即下方 from_args_iter 不 Err），run_cluster_server
/// 顶层闸名单在任何 bootstrap 装配副作用之前拒启——对齐 C# 入口
/// IntRangeValidation(0, int.MaxValue) 秒域上界（毫秒域 i32::MAX*1000）
#[test]
fn oversized_cluster_node_timeout_rejected_before_boot() {
  for ms in ["10000000000000000", "18446744073709551615"] {
    let args = ClusterArgs::from_args_iter(["wedb", "--cluster-node-timeout-ms", ms]).unwrap();
    match run_cluster_server(args, None) {
      Err(WedbError::Node(WnodeError::InvalidArgument(msg))) => {
        assert!(
          msg.contains("cluster-node-timeout-ms"),
          "错误信息应指明 cluster-node-timeout-ms: {msg}"
        );
      }
      other => panic!("{ms}ms 应契约带上界拒启（InvalidArgument），实际: {other:?}"),
    }
  }
}

/// 上限门域边界（纯判据面，与亚秒门互补不重叠）：0 = 无限哨兵豁免臂维持
/// （与亚秒门同豁免）、缺省 60000 与契约上界 i32::MAX*1000 本身放行、
/// i32::MAX*1000+1 与 u64::MAX 拒
#[test]
fn oversized_cluster_node_timeout_gate_boundary() {
  assert!(
    !is_oversized_cluster_node_timeout(0),
    "0 = 无限哨兵必须豁免（两闸共用豁免臂）"
  );
  assert!(
    !is_oversized_cluster_node_timeout(DEFAULT_CLUSTER_NODE_TIMEOUT_MS),
    "缺省 60000ms 回归不破"
  );
  for ms in [1000u64, 60_000, (i32::MAX as u64) * 1000] {
    assert!(
      !is_oversized_cluster_node_timeout(ms),
      "契约带内 {ms}ms 应放行"
    );
  }
  for ms in [(i32::MAX as u64) * 1000 + 1, u64::MAX] {
    assert!(
      is_oversized_cluster_node_timeout(ms),
      "契约带外 {ms}ms 应拒（带上界拒收）"
    );
  }
}
