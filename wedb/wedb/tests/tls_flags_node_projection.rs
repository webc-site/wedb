#![recursion_limit = "256"]
//! TLS 旗标 → NodeArgs 落值解析锁测（票 boot-tls-gate-non-tls-form §2a）
//!
//! 本册刻意不受 `tls` 特性门：`./test.sh`（`cargo nextest run --all-features`）门禁形态
//! 下恒可执行。`error_sink_tests.rs` 整册 `#![cfg(not(feature = "tls"))]` 在门禁形态
//! 编译出局（0 tests），其端到端拒启面（`guard_no_tls` 仅在 wnode 非 tls 形态存在）
//! 在门禁内不可见；本册以纯同步断言把「旗标解析/向 NodeArgs 投影」与「门禁执行」
//! 两面切开——四枚 `--tls-*` 旗标在任何特性组合下都必须经
//! `ClusterArgs::from_args_iter` 落进 `NodeArgs` 对应字段（`has_tls()` 为真），
//! 门禁执行面（拒启）据此输入面在位方可信赖。
//! 对标 C# `garnet/libs/host/Configuration/Options.cs` TLS 校验面：证书类旗标已给
//! 即计入 TLS 配置事实，`GarnetServer` 构造期校验先于起服。

use wconf::{ConfigFileArgs, ServerArgs};
use wedb::ClusterArgs;

/// 四枚 `--tls-*` 旗标逐一经 ClusterArgs 解析落进 NodeArgs 并点亮 has_tls()
#[test]
fn tls_flags_land_in_node_args() {
  // (旗标, 期望对位字段名)
  for (flag, probe) in [
    ("--tls-cert", "tls_cert"),
    ("--tls-key", "tls_key"),
    ("--tls-issuer-cert", "tls_issuer_cert"),
    ("--tls-client-target-host", "tls_client_target_host"),
  ] {
    let args = ClusterArgs::from_args_iter(["wedb", flag, "test_val"])
      .unwrap_or_else(|e| panic!("{flag} 解析失败: {e}"));
    let node = args.node_args();
    assert!(
      node.has_tls(),
      "{flag} 解析后 has_tls() 必须为真（门禁判据输入面）"
    );
    let landed: Option<String> = match probe {
      "tls_cert" => node
        .tls_cert
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned()),
      "tls_key" => node
        .tls_key
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned()),
      "tls_issuer_cert" => node
        .tls_issuer_cert
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned()),
      "tls_client_target_host" => node.tls_client_target_host.clone(),
      _ => unreachable!("probe 与旗标表同源"),
    };
    assert_eq!(
      landed.as_deref(),
      Some("test_val"),
      "{flag} 解析值必须落进 NodeArgs 字段 {probe}"
    );
  }
}

/// 未给任何 `--tls-*` 旗标时 has_tls() 为假（门禁放行面不误伤明文形态）
#[test]
fn no_tls_flags_leaves_has_tls_false() {
  let args = ClusterArgs::from_args_iter(["wedb"]).unwrap();
  assert!(!args.node_args().has_tls());
}
