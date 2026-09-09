//! 容量防线测试：index_size 校验报错升级、恢复组件一致性预检与容量规划公式

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::HybridLog;
use windex::HashIndex;
use wkv::{
  BfTreeService, Error, INDEX_BUCKET_BYTES, INDEX_BUCKET_DATA_SLOTS, MAX_INDEX_SIZE,
  MIN_INDEX_SIZE, StoreConfig, WedbStore,
};

/// 构造 64KB 页 16 页的标准小容量测试配置
fn test_config(index_size: usize) -> aok::Result<StoreConfig> {
  Ok(StoreConfig::new(index_size, 64 * 1024, 16, 0.5)?)
}

/// 取出装配预检的拒绝错误（WedbStore 无 Debug，不能 expect_err）
fn unwrap_err<T>(res: wkv::Result<T>) -> Error {
  match res {
    Ok(_) => panic!("容量防线必须拒绝非法装配"),
    Err(e) => e,
  }
}

/// 装配预检测试的公共组件（hlog/epoch 与 config 容量解耦）
fn build_components(
  config: &StoreConfig,
  device: &Arc<SegmentedDevice>,
) -> aok::Result<(Arc<HybridLog<SegmentedDevice>>, Arc<LightEpoch>)> {
  let epoch = Arc::new(LightEpoch::new(config.max_sessions));
  let hlog = Arc::new(HybridLog::new(
    config.to_hlog_config()?,
    Arc::clone(device),
    Arc::clone(&epoch),
  )?);
  Ok((hlog, epoch))
}

/// 防线 b：index_size 校验报错含当前值、定容与溢出风险说明及就近建议值
#[test]
fn config_index_size_error_carries_guidance() -> Void {
  let err = StoreConfig::new(1000, 64 * 1024, 16, 0.5).expect_err("非 2 的幂必须被拒绝");
  let msg = err.to_string();
  assert!(msg.contains("1000"), "报错须含当前配置值: {msg}");
  assert!(msg.contains("2 的幂"), "报错须含约束说明: {msg}");
  assert!(msg.contains("64B"), "报错须含每桶内存成本: {msg}");
  assert!(
    msg.contains("OverflowPoolExhausted"),
    "报错须含运行期超限行为说明: {msg}"
  );
  assert!(msg.contains("1024"), "报错须含就近建议值: {msg}");

  // 0 桶：就近建议回退最小桶数基线
  let err = StoreConfig::new(0, 64 * 1024, 16, 0.5).expect_err("0 桶必须被拒绝");
  let msg = err.to_string();
  assert!(msg.contains("当前为 0"), "报错须含当前配置值: {msg}");
  assert!(
    msg.contains(&MIN_INDEX_SIZE.to_string()),
    "0 桶建议值应为最小桶数基线: {msg}"
  );
  OK
}

/// 容量规划公式与布局常量契约（每桶 64B、7 数据槽）
#[test]
fn recommended_index_size_formula() -> Void {
  assert_eq!(INDEX_BUCKET_BYTES, 64);
  assert_eq!(INDEX_BUCKET_DATA_SLOTS, 7);

  // 350_000 键 × 2 / 7 = 100_000 → next_power_of_two = 131_072
  assert_eq!(StoreConfig::recommended_index_size(350_000), 131_072);
  // 微量键数钳制最小桶数基线；超量键数饱和最大桶数
  assert_eq!(StoreConfig::recommended_index_size(7), MIN_INDEX_SIZE);
  assert_eq!(
    StoreConfig::recommended_index_size(u64::from(u32::MAX)),
    MAX_INDEX_SIZE
  );
  OK
}

/// 防线 b：绕过 builder 手搓非法 index_size（字段公开可直改），open 在资源分配前拦截
#[test]
fn open_rejects_hand_mutated_index_size() -> Void {
  let dir = tempdir()?;
  let mut config = test_config(1024)?;
  config.index_size = 1000;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("cap_open.db"),
  )?);

  let err = unwrap_err(WedbStore::open(config, device));
  assert!(
    matches!(err, Error::InvalidConfig(msg) if msg.contains("1000")),
    "open 入口预检必须拦截非法 index_size"
  );
  OK
}

/// 防线 a：小配置装配大索引（恢复组件路径）被结构化拦截，绝不静默缩表
#[test]
fn from_components_rejects_shrunk_capacity() -> Void {
  let dir = tempdir()?;
  let config = test_config(1024)?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("cap_shrink.db"),
  )?);
  let (hlog, epoch) = build_components(&config, &device)?;

  // 模拟恢复出的索引快照实际容量（2048）大于本次配置（1024）
  let index = Arc::new(HashIndex::new(2048)?);
  let err = unwrap_err(WedbStore::from_components(
    config, index, hlog, epoch, device,
  ));

  assert!(
    matches!(
      err,
      Error::IndexSizeMismatch {
        config: 1024,
        actual: 2048
      }
    ),
    "须报结构化 IndexSizeMismatch 并携带两个容量值"
  );
  OK
}

/// 防线 a：大配置装配小索引（反向漂移）同样被拦截
#[test]
fn from_components_rejects_inflated_capacity() -> Void {
  let dir = tempdir()?;
  let config = test_config(1024)?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("cap_inflate.db"),
  )?);
  let (hlog, epoch) = build_components(&config, &device)?;

  let index = Arc::new(HashIndex::new(512)?);
  let err = unwrap_err(WedbStore::from_components(
    config, index, hlog, epoch, device,
  ));

  assert!(
    matches!(
      err,
      Error::IndexSizeMismatch {
        config: 1024,
        actual: 512
      }
    ),
    "反向容量漂移必须被结构化拦截"
  );
  OK
}

/// 防线 a：with_bftree 装配变体同样执行一致性预检，报错含建议动作
#[test]
fn from_components_with_bftree_rejects_capacity_mismatch() -> Void {
  let dir = tempdir()?;
  let config = test_config(1024)?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("cap_bf.db"))?);
  let (hlog, epoch) = build_components(&config, &device)?;
  let bftree = Arc::new(BfTreeService::open_disk(dir.path().join("cap.bftree"), 4)?);

  let err = unwrap_err(WedbStore::from_components_with_bftree(
    config,
    Arc::new(HashIndex::new(2048)?),
    hlog,
    epoch,
    device,
    bftree,
  ));

  let msg = err.to_string();
  assert!(
    matches!(err, Error::IndexSizeMismatch { .. }),
    "须报结构化 IndexSizeMismatch"
  );
  assert!(
    msg.contains("请将 index_size 设为 2048"),
    "报错须含建议动作（对齐实际容量）: {msg}"
  );
  OK
}

/// 防线 a：边界相等容量放行，装配后的引擎立即可用
#[test]
fn from_components_accepts_equal_capacity() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let config = test_config(2048)?;
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("cap_eq.db"))?);
    let (hlog, epoch) = build_components(&config, &device)?;

    let index = Arc::new(HashIndex::new(2048)?);
    let store = Arc::new(WedbStore::from_components(
      config, index, hlog, epoch, device,
    )?);
    assert_eq!(store.config.index_size, store.index.size);

    // 装配结果立即可用：会话可开
    let _session = store.new_session()?;
    aok::Result::<()>::Ok(())
  })?;
  OK
}
