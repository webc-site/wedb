//! 向量集合磁盘持久化与召回烟雾测试（对标 test/standalone/Garnet.test.vectorset/VectorSetRecallSmokeTests.cs）
//!
//! 验证物理鲁棒性：原生图销毁（模拟重启）后，通过 WedbVectorStoreCallbacks 自 wkv
//! 存储恢复起点与空闲空间，磁盘检索结果正确且距离排序一致。
//!
//! 两例均在 compio 运行时线程上执行：向量存储回调的冷读收割（对标 C#
//! `VectorReadBatch.CompletePending` 的 `CompletePending(wait: true)`）只认本线程的
//! I/O driver，无运行时的线程不可调入。

use std::{path::Path, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::addr::is_read_cache;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::vector::vector_store_callbacks::WedbVectorStoreCallbacks;
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, SearchParams,
  VectorDistanceMetricType, VectorQuantType,
  store::{LengthPrefixedIter, term},
};

/// 冷读用例元素数（一次批量读的键数远超引擎批量预取窗口，覆盖跨窗口折叠）
const ELEMENTS: u32 = 40;

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// 定长 2 字节外部 id（与检索输出的 4B 长度前缀串接格式对位）
fn eid(i: u32) -> [u8; 2] {
  [b'a' + (i / 10) as u8, b'0' + (i % 10) as u8]
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

/// 单文件设备存储
fn open_store(dir: &Path, name: &str, config: StoreConfig) -> Arc<WedbStore<SegmentedDevice>> {
  let device = Arc::new(SegmentedDevice::single_file(dir.join(name)).unwrap());
  Arc::new(WedbStore::open(config, device).unwrap())
}

/// 磁盘持久化与重启恢复（recall 路径）：
/// 向量/邻接表/映射/FSM 块经存储回调以统一物理键落盘到 wkv；
/// 丢弃内存索引（原生图销毁）后同 context 重建，provider 自存储恢复
/// 起点与空闲空间映射，检索自盘读取向量与邻接表。
#[test]
fn wkv_persistence_and_recall_after_recreate() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let store_cfg = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
    let store = open_store(dir.path(), "vector.db", store_cfg);
    let session = Arc::new(store.new_session().unwrap());
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
  });
}

/// 落盘驱逐后召回自磁盘批量读路径存活（对标
/// test/standalone/Garnet.test.vectorset/VectorSetRecallSmokeTests.cs:RecallSurvivesFlushAndEvict
/// 的 readcache 档：建图于内存 → DEBUG FLUSHANDEVICT → 重查比结果）
///
/// 与 recreate 用例分工：本例把整段内存日志驱逐至磁盘区，使检索的邻接表与全向量读
/// 全部落冷路径，且单次批读键数远超引擎批量预取窗口，覆盖 read_multi 折叠为单次批量
/// 收割后的跨窗口下标对位；并验证冷读回填 ReadCache（承接 C#
/// VectorReadBatch.ReadCopyOptions 的 StubReadCopyTo 语义）后二次检索自内存命中、
/// 结果与冷读一轮完全一致。
#[test]
fn cold_batch_read_recall_after_flush_and_evict() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    // 小容量日志（16KB × 8 页、可变区 2 页）：建图写入量远超可变区，配合
    // flush_and_evict_all 使读全部落磁盘冷路径；启用 ReadCache 以验回填
    let store_cfg = StoreConfig::new(1024, 16 * 1024, 8, 0.25)
      .unwrap()
      .with_read_cache(true);
    let store = open_store(dir.path(), "cold.db", store_cfg);
    let session = Arc::new(store.new_session().unwrap());
    let service = DiskANNService::default();
    let context = 8;
    service
      .create_index(
        context,
        config(2),
        Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(session.clone()))),
      )
      .unwrap();
    for i in 0..ELEMENTS {
      assert_eq!(
        service.insert(
          context,
          &eid(i),
          &f32_bytes(&[i as f32, (ELEMENTS - i) as f32]),
          b""
        ),
        DiskAnnInsertResult::True
      );
    }
    assert_eq!(service.card(context), ELEMENTS as u64);

    // 整段内存日志落盘并驱逐（对标 DEBUG FLUSHANDEVICT）：其后读全为磁盘冷读
    store.flush_and_evict_all().await.unwrap();

    let query = f32_bytes(&[3.0, (ELEMENTS - 3) as f32]);
    let out = service
      .search_vector(context, &query, search_params())
      .unwrap();
    assert_eq!(out.found, 10);
    assert!(
      out.distances.windows(2).all(|w| w[0] <= w[1]),
      "冷读批量结果按距离升序: {:?}",
      out.distances
    );
    let ids: Vec<&[u8]> = LengthPrefixedIter::new(&out.ids).collect();
    assert_eq!(ids.len(), 10, "结果 id 长度前缀串接完整");
    assert_eq!(ids[0], &eid(3)[..], "查询点自身格点应为最近邻");

    // 冷读回填：邻接表记录槽位地址落在 ReadCache 地址域
    let adjacency_cached = (0..ELEMENTS).any(|iid| {
      let key = session.vector_key(context | term::NEIGHBOR_LIST, &iid.to_le_bytes());
      store
        .index
        .load()
        .find_tag(key.as_slice())
        .is_some_and(is_read_cache)
    });
    assert!(adjacency_cached, "冷读的邻接表记录应回填 ReadCache");

    // 二次检索自回填内存命中，结果与冷读一轮完全一致（批量下标对位无误）
    let again = service
      .search_vector(context, &query, search_params())
      .unwrap();
    assert_eq!(again.distances, out.distances);
    assert_eq!(again.ids, out.ids);
  });
}
