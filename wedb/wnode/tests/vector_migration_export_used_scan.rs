//! 迁移导出元素枚举走 fsm 占用位精确扫描回归（票：zcode-r27-vectordiskann 发现五）
//!
//! 缺陷形态：export_migration_elements 原以随机取样通道枚举全集
//! （sample(context, card+1)），高删除碎片集合上首批 batch 即已铸 id 全空间
//! （sample_inplace 按全长分配 u32 向量并洗牌，已铸 4e9 时约 16GB 直接打爆
//! 进程），且每个死 id（已删未复用）各触发一次 ExtMap 缺失存储读，导出
//! 时间与内存随碎片线性膨胀；同一 crate 的 fsm.visit_used 位图精确枚举
//! 原语（O(块) 扫描仅访占用位）在位未用。
//!
//! 修复契约：导出改走 fsm.visit_used（起点 0 排除），消除抽样分配与死 id
//! 读放大；VRANDMEMBER 随机语义臂保持采样不动。
//!
//! 注入面：内存桩在 ExtMap 域 read 未命中处计数——死 id 的唯一读信号。

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::{
  vector_manager::{VectorAddArgs, VectorManager, VectorManagerOptions},
  vector_manager_index::Index,
  vector_store_callbacks::OwnedActiveVectorSession,
};
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, IndexConfig, StoreCallbacks, VectorDistanceMetricType, VectorQuantType,
  VectorSetFlags, VectorValueType, store::Term,
};
use wvector_test::MemStore;

const CTX: u64 = 0;
fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}

/// 内存存储桩（基座 [`MemStore`] 转发 + ExtMap 域缺失读计数钩子臂）。
struct CountingStore {
  base: MemStore,
  extmap_miss_reads: AtomicU64,
}

impl CountingStore {
  fn new() -> Self {
    Self {
      base: MemStore::new(),
      extmap_miss_reads: AtomicU64::new(0),
    }
  }

  fn extmap_misses(&self) -> u64 {
    self.extmap_miss_reads.load(Ordering::Acquire)
  }
}

impl StoreCallbacks for CountingStore {
  /// 计数钩子臂：ExtMap 域未命中即死 id 读信号，命中路径原样转基座。
  async fn read<F>(&self, context: u64, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    let hit = self.base.data.lock().contains_key(&(context, key.to_vec()));
    if !hit && context & 0b111 == Term::ExtMap as u64 {
      self.extmap_miss_reads.fetch_add(1, Ordering::AcqRel);
      return false;
    }
    self.base.read(context, key, f).await
  }

  async fn read_multi<F>(&self, context: u64, keys: &[u8], length_hint: usize, f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    self.base.read_multi(context, keys, length_hint, f).await
  }

  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    self.base.write(context, key, value).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.base.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    self.base.rmw(context, key, write_len, f).await
  }

  async fn filter(&self, context: u64, internal_id: u32) -> bool {
    self.base.filter(context, internal_id).await
  }

  async fn purge_context(&self, context: u64) -> bool {
    self.base.purge_context(context).await
  }

  fn log(&self, context: u64, msg: &str) {
    self.base.log(context, msg);
  }
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// 主靶：含删除碎片的集合导出元素集与存活集精确一致，且无死 id 读放大。
#[compio::test]
async fn export_uses_exact_used_scan_without_dead_id_reads() {
  // 执行域绑定（try_get_raw_embedding 的会话守卫 debug 断言要求；
  // 形态同 resp_vector_set::bound_domain）
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("bind.db")).unwrap());
  let kv = Arc::new(WedbStore::open(wtest_base::test_store_config(), device).unwrap());
  let _bound = OwnedActiveVectorSession::new(kv.new_session().unwrap());

  let store = Arc::new(CountingStore::new());
  let manager = VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::clone(&store)),
  );
  let service = &manager.service;
  assert_eq!(
    service
      .create_index(
        CTX,
        IndexConfig::new(
          2,
          0,
          VectorQuantType::NoQuant,
          VectorDistanceMetricType::L2,
          64,
          8
        ),
        Callbacks::new(Arc::clone(&store)),
      )
      .await,
    Ok(false),
    "集合创建失败"
  );
  let index = Index {
    context: CTX,
    index_ptr: 1,
    dimensions: 2,
    reduce_dims: 0,
    num_links: 8,
    build_exploration_factor: 64,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    flags: VectorSetFlags::NONE,
  };
  let stored = index.to_bytes();

  for (i, name) in ["e1", "e2", "e3", "e4", "e5"].iter().enumerate() {
    manager
      .try_add(
        root().as_slice(),
        b"k",
        &stored,
        &VectorAddArgs::new(
          name.as_bytes(),
          VectorValueType::FP32,
          &f32_bytes(&[i as f32, 1.0]),
          &format!("{{\"n\":\"{name}\"}}").into_bytes(),
        ),
      )
      .await
      .unwrap();
  }

  // 铸造删除碎片：e1/e2 死 id（fsm 空闲、ExtMap 已清）
  assert!(service.remove(CTX, b"e1").await);
  assert!(service.remove(CTX, b"e2").await);
  assert_eq!(service.card(CTX), 3);
  store.extmap_miss_reads.store(0, Ordering::Release);

  // 导出：元素集与存活集精确一致，死 id（含起点 0）不出现
  let exported = manager.export_migration_elements(&stored).await;
  let mut ids: Vec<Vec<u8>> = exported.iter().map(|e| e.element.clone()).collect();
  ids.sort();
  assert_eq!(
    ids,
    [b"e3".to_vec(), b"e4".to_vec(), b"e5".to_vec()],
    "导出集必须与存活集一致，死 id 不得出现"
  );
  for e in &exported {
    assert_eq!(e.values.len(), 8, "NoQuant 导出载荷为全精度宽 dim*4");
  }
  let mut attrs: Vec<Vec<u8>> = exported.iter().map(|e| e.attributes.clone()).collect();
  attrs.sort();
  assert_eq!(
    attrs,
    [
      b"{\"n\":\"e3\"}".to_vec(),
      b"{\"n\":\"e4\"}".to_vec(),
      b"{\"n\":\"e5\"}".to_vec()
    ],
    "属性随元素一并导出"
  );

  // 读放大归零：导出全程无死 id / 起点的 ExtMap 缺失读
  assert_eq!(
    store.extmap_misses(),
    0,
    "导出不得对死 id 或起点发起 ExtMap 缺失读"
  );
}
