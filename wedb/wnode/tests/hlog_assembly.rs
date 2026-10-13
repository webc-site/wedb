#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wconf::{HlogOptions, HlogProjection, NodeArgs};
use wkv::StoreConfig as WkvStoreConfig;
use wnode::{
  Error,
  service::{apply_hlog_overrides, store_config, store_config_from_node},
};

/// 构造 hlog 覆盖投影（ReadCache 与 reviv 三旋钮默认关闭透传）
fn proj(
  page_size: Option<usize>,
  memory_size: Option<usize>,
  mutable_fraction: Option<f64>,
) -> HlogProjection {
  HlogProjection {
    page_size,
    memory_size,
    mutable_fraction,
    read_cache: false,
    read_cache_memory_size: None,
    tree_cache_budget: None,
    reviv: false,
    reviv_fraction: None,
    copy_reads_to_tail: false,
  }
}

/// hlog 配置段投影：显式项覆盖自适应基线（对标 C# GetSettings → KVSettings）
#[test]
fn hlog_overrides_apply_to_store_config() {
  let mut config = store_config();
  apply_hlog_overrides(
    &mut config,
    proj(Some(8 * 1024 * 1024), Some(256 * 1024 * 1024), Some(0.6)),
  )
  .expect("合法覆盖项");
  assert_eq!(config.page_size, 8 * 1024 * 1024);
  // 256MB / 8MB = 32 页（向下取 2 的幂）
  assert_eq!(config.num_pages, 32);
  assert!((config.mutable_fraction - 0.6).abs() < f64::EPSILON);
  // GC 默认禁用态不受 hlog 覆盖影响（装配不越权开后台任务，对标 C#
  // ExpiredKeyDeletionScanFrequencySecs = -1）
  assert!(!config.gc.enabled);

  // 全 None：保持自适应基线
  let mut config = store_config();
  let baseline_page = config.page_size;
  let baseline_pages = config.num_pages;
  let baseline_fraction = config.mutable_fraction;
  apply_hlog_overrides(&mut config, proj(None, None, None)).expect("空覆盖项");
  assert_eq!(config.page_size, baseline_page);
  assert_eq!(config.num_pages, baseline_pages);
  assert!((config.mutable_fraction - baseline_fraction).abs() < f64::EPSILON);
}

/// 非法覆盖项必须被拦截（页容量非 2 的幂）
#[test]
fn hlog_overrides_reject_invalid_page_size() {
  let mut config = store_config();
  let err = apply_hlog_overrides(&mut config, proj(Some(4095), None, None));
  assert!(err.is_err(), "非 2 的幂页容量必须被 validate 拦截");
}

/// memory_size 预算配不足最小页数时装配期显式拒绝（不静默钳制超配内存：
/// 100MB 预算 + 16MB 页 = 6 页 < 16 页下限，旧逻辑钳 16 页实配 256MB）
#[test]
fn hlog_overrides_reject_memory_size_below_min_pages() {
  let mut config = store_config();
  let err = apply_hlog_overrides(
    &mut config,
    proj(Some(16 * 1024 * 1024), Some(100 * 1024 * 1024), None),
  )
  .expect_err("预算不足最小页数必须拒绝");
  assert!(
    matches!(&err, Error::InvalidArgument(msg)
      if msg.contains("104857600") && msg.contains("16777216")),
    "报错须给出预算与页大小: {err}"
  );
  // 恰好 16 页为合法下界（prev_power_of2(16) = 16）
  let mut config = store_config();
  apply_hlog_overrides(
    &mut config,
    proj(Some(16 * 1024 * 1024), Some(256 * 1024 * 1024), None),
  )
  .expect("恰好最小页数为合法下界");
  assert_eq!(config.num_pages, 16);
}

/// NodeArgs 全链投影：hlog 配置段 → 生产 StoreConfig（大值通道默认打通）
#[test]
fn node_args_hlog_section_reaches_store_config() {
  let node = NodeArgs {
    hlog: wconf::HlogOptions {
      page_size: Some(16 * 1024 * 1024),
      memory_size: Some(512 * 1024 * 1024),
      mutable_percent: Some(50),
      read_cache: true,
      read_cache_memory_size: Some(256 * 1024 * 1024),
      tree_cache_budget: Some(1024 * 1024 * 1024),
      ..HlogOptions::default()
    },
    ..NodeArgs::default()
  };
  let config = store_config_from_node(&node).expect("合法 hlog 配置段");
  assert_eq!(config.page_size, 16 * 1024 * 1024);
  assert_eq!(config.num_pages, 32);
  assert!((config.mutable_fraction - 0.5).abs() < f64::EPSILON);
  // ReadCache 开关 + 页数推导（256MB / 16MB = 16 页）
  assert!(config.enable_read_cache);
  assert_eq!(config.read_cache_num_pages, 16);
  // 树页缓存总闸旋钮贯通：显式项直达 StoreConfig（注入源，INFO STORE
  // TreeCache.BudgetBytes 同源）；缺省态保持 DEFAULT_TREE_CACHE_BUDGET_BYTES
  assert_eq!(config.tree_cache_budget_bytes, 1024 * 1024 * 1024);
  let default_config = store_config_from_node(&NodeArgs::default()).expect("默认配置");
  assert_eq!(
    default_config.tree_cache_budget_bytes,
    WkvStoreConfig::default().tree_cache_budget_bytes
  );
}

/// ReadCache 开关关闭时页数保持引擎默认（不参与推导）
#[test]
fn node_args_read_cache_disabled_keeps_default_pages() {
  let node = NodeArgs {
    hlog: wconf::HlogOptions {
      read_cache: false,
      read_cache_memory_size: Some(256 * 1024 * 1024),
      ..HlogOptions::default()
    },
    ..NodeArgs::default()
  };
  let config = store_config_from_node(&node).expect("合法 hlog 配置段");
  assert!(!config.enable_read_cache);
  assert_eq!(
    config.read_cache_num_pages,
    WkvStoreConfig::default().read_cache_num_pages
  );
}

/// 小预算装配收敛 ≥2 页硬下界：读缓存预算与页容量同量级时
/// `prev_power_of2(预算/页容量) = 1`，须经 wkv 唯一收敛口
/// `with_read_cache_pages` 钳至 2（对标 C# AllocatorBase.cs:650-651 读缓存
/// 预算 ≥2× 页容量；期望值由页界式推导：1 页环首轮换页即
/// closed == (e-1+1)×page_size == 新 tail，head/tail 无余页分立，2 页为
/// 环形换页最小几何）。页容量取主日志扇区对齐门径下最小合法值 4096
/// （512B 页在本仓 wkv `validate` 的 4096 扇区对齐门下不可达）
#[test]
fn read_cache_small_budget_converges_to_min_pages() {
  let mut config = store_config();
  let mut overrides = proj(Some(4096), None, None);
  overrides.read_cache = true;
  overrides.read_cache_memory_size = Some(4096);
  apply_hlog_overrides(&mut config, overrides).expect("小预算装配应经收敛口钳位而非拒绝");
  assert_eq!(
    config.read_cache_num_pages, 2,
    "1 页推导值必须收敛至 ≥2 页下界"
  );
}
