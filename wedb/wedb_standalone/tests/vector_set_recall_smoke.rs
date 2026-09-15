//! 向量集合磁盘持久化与召回烟雾测试（对标 C# VectorSetRecallSmokeTests.cs）
//!
//! 验证物理鲁棒性：原生图销毁（模拟重启）后，通过 WedbVectorStoreCallbacks 自 wkv
//! 存储恢复起点与空闲空间，磁盘检索结果正确且距离排序一致。

use std::{path::Path, sync::Arc};

use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, StoreSession, WedbStore};
use wnode::resp::vector::vector_store_callbacks::WedbVectorStoreCallbacks;
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, SearchParams,
  VectorDistanceMetricType, VectorQuantType,
};

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn config(dims: u32) -> IndexConfig {
  IndexConfig {
    dims,
    reduce_dims: 0,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    build_exploration_factor: 64,
    num_links: 8,
  }
}

fn search_params() -> SearchParams {
  SearchParams {
    count: 10,
    search_exploration_factor: 32,
    filter_len: 0,
    max_filtering_effort: 0,
  }
}

fn open_session(dir: &Path, name: &str) -> Arc<StoreSession<SegmentedDevice>> {
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(name)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  Arc::new(store.new_session().unwrap())
}

/// 磁盘持久化与重启恢复（recall 路径）：
/// 向量/邻接表/映射/FSM 块经存储回调以统一物理键落盘到 wkv；
/// 丢弃内存索引（原生图销毁）后同 context 重建，provider 自存储恢复
/// 起点与空闲空间映射，检索自盘读取向量与邻接表。
#[test]
fn wkv_persistence_and_recall_after_recreate() {
  let dir = tempdir().unwrap();
  let session = open_session(dir.path(), "vector.db");
  let service = DiskANNService::default();
  let context = 8;
  let query = f32_bytes(&[0.1, 0.1]);

  // 建索引 + 写入元素（对齐 C# DiskANNService.CreateIndex + Insert）
  service
    .create_index(
      context,
      config(2),
      Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(session.clone()))),
    )
    .unwrap();
  assert_eq!(
    service.insert(context, b"a", &f32_bytes(&[0.0, 0.0]), b""),
    DiskAnnInsertResult::True
  );
  assert_eq!(
    service.insert(context, b"b", &f32_bytes(&[1.0, 1.0]), b""),
    DiskAnnInsertResult::True
  );
  assert_eq!(service.card(context), 2);

  let out = service
    .search_vector(context, &query, search_params())
    .unwrap();
  assert_eq!(out.found, 2);

  // 模拟进程死亡：丢弃内存索引（原生图销毁），存储数据保留
  service.drop_index(context);

  // 重启恢复：同 context 重建索引（对齐 C# RecreateIndex），recall 路径自盘读取
  service
    .create_index(
      context,
      config(2),
      Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(session))),
    )
    .unwrap();
  assert_eq!(service.card(context), 2);
  assert!(service.check_external_id_valid(context, b"a"));
  assert!(service.check_external_id_valid(context, b"b"));

  let out = service
    .search_vector(context, &query, search_params())
    .unwrap();
  assert_eq!(out.found, 2);
  // L2(a=(0,0), q=(0.1,0.1)) = 0.02 < L2(b=(1,1), q) = 1.62，结果按距离升序
  assert!(
    out.distances[0] < out.distances[1] && out.distances[0] < 0.1,
    "a 距离最近且排序正确: {:?}",
    out.distances
  );
}
