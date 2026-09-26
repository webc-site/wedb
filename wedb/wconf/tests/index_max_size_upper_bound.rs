//! index-max-size 上界启动期定界测试（对标 C# ServerOptions.cs:208
//! IndexSizeCachelines 的 `adjustedSize < 64 || adjustedSize > (1L << 37)`
//! 双界同抛，调用点 GarnetServerOptions.cs:808 GetSettings）
//!
//! 拒启单点挂在 [`NodeArgs::validate`]（from_layered_matches 漏斗末端，
//! CLI 与 toml 文件两路同受约束）；折桶口
//! [`NodeArgs::index_max_size_buckets`] 同常量口径兜底。
//!
//! 自研依据: MaxIndexSize 上界校验（C# 对应 libs/host/ServerSettingsManager.cs 校验面）

use std::{env::temp_dir, fs, path::PathBuf};

use wconf::{
  ConfigFileArgs, NodeArgs, NodeOptionsError,
  node_options::{INDEX_MAX_SIZE_MAX_BYTES, INDEX_MAX_SIZE_MIN_BYTES},
};

/// 写临时 toml 配置文件（进程内唯一名，测试结束自清理）
fn temp_config(name: &str, content: &str) -> PathBuf {
  let path = temp_dir().join(format!("wedb-wconf-indexmax-{name}.toml"));
  fs::write(&path, content).unwrap();
  path
}

/// 上界越界（下取 2 的幂后 > 1<<37）CLI 与文件两路皆拒启
#[test]
fn oversized_index_max_size_rejected_at_startup() {
  // "64tb" = 2^46：折桶达 2^40 桶，扩容闸无顶即每轮翻倍至 OOM
  let err = NodeArgs::from_args_iter(["wedb", "--index-max-size", "64tb"]).unwrap_err();
  assert!(
    matches!(
      err,
      NodeOptionsError::SizeOutOfRange("index-max-size", INDEX_MAX_SIZE_MIN_BYTES, size)
        if size == 64 * 1024 * 1024 * 1024 * 1024
    ),
    "64tb 应启动期拒启: {err:?}"
  );
  // 恰越界的下一档 2 的幂（2^38 > 1<<37）同样拒启
  assert!(NodeArgs::from_args_iter(["wedb", "--index-max-size", "256gb"]).is_err());
  // 文件面同受约束（校验挂在合并后的漏斗末端，非 CLI 专属）
  let file = temp_config("oversized", "index_max_size = \"64tb\"\n");
  assert!(
    NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).is_err(),
    "文件配 64tb 同样拒启"
  );
  fs::remove_file(&file).ok();
}

/// 上界端点合法：`>` 严格越界（C# 同判），128GiB 恰在界内
#[test]
fn upper_bound_endpoint_is_legal() {
  let args = NodeArgs::from_args_iter(["wedb", "--index-max-size", "128gb"])
    .expect("1<<37 恰为上界端点，应合法");
  // adjustedSize / 64 折桶口径与 C# 一致：2^37 / 64 = 2^31 桶
  assert_eq!(
    args.index_max_size_buckets(),
    Some((INDEX_MAX_SIZE_MAX_BYTES / INDEX_MAX_SIZE_MIN_BYTES) as usize)
  );
  assert_eq!(args.index_max_size_buckets(), Some(1 << 31));
  // 129gb 非 2 的幂，下取至 128gb 后仍在界内（证伪 >= 判定）
  let args = NodeArgs::from_args_iter(["wedb", "--index-max-size", "129gb"]).expect("折损后应合法");
  assert_eq!(args.index_max_size_buckets(), Some(1 << 31));
}

/// 双界两侧的越界值在投影侧同样不出桶数（validate 之外的调用链不吃敞口）
#[test]
fn out_of_range_yields_no_buckets() {
  let oversized = NodeArgs {
    index_max_size: Some("64tb".into()),
    ..Default::default()
  };
  assert_eq!(oversized.index_max_size_buckets(), None);
  let undersized = NodeArgs {
    index_max_size: Some("32".into()),
    ..Default::default()
  };
  assert_eq!(undersized.index_max_size_buckets(), None);
  // 界内值折算不变（下界端点 64B = 1 桶）
  let legal = NodeArgs {
    index_max_size: Some("64".into()),
    ..Default::default()
  };
  assert_eq!(legal.index_max_size_buckets(), Some(1));
}
