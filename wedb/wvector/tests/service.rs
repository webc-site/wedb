use std::sync::Arc;

use parking_lot::Mutex;
use wbase::map::HashMap;
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, SearchParams, StoreCallbacks,
  VectorDistanceMetricType, VectorQuantType,
};

type MemStoreMap = HashMap<(u64, Vec<u8>), Vec<u8>>;

/// 内存桥接存储（(context, key) → 值）。
struct MemStore {
  data: Mutex<MemStoreMap>,
}

impl MemStore {
  fn new() -> Self {
    Self {
      data: Mutex::new(HashMap::default()),
    }
  }
}

impl StoreCallbacks for MemStore {
  fn read_multi<F>(&self, context: u64, keys: &[u8], _length_hint: usize, mut f: F) -> bool
  where
    F: FnMut(u32, &[u8]),
  {
    let mut index = 0u32;
    let mut rest = keys;
    while rest.len() >= 4 {
      let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
      let total = 4 + len;
      if rest.len() < total {
        break;
      }
      let key = &rest[4..total];
      if let Some(value) = self.data.lock().get(&(context, key.to_vec())) {
        f(index, value);
      }
      index += 1;
      rest = &rest[total..];
    }
    true
  }

  fn read<F>(&self, context: u64, key: &[u8], mut f: F) -> bool
  where
    F: FnMut(&[u8]),
  {
    match self.data.lock().get(&(context, key.to_vec())) {
      Some(value) => {
        f(value);
        true
      }
      None => false,
    }
  }

  fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    self
      .data
      .lock()
      .insert((context, key.to_vec()), value.to_vec());
    true
  }

  fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.data.lock().remove(&(context, key.to_vec())).is_some()
  }

  fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, mut f: F) -> bool
  where
    F: FnMut(&mut [u8]),
  {
    let mut map = self.data.lock();
    let entry = map.entry((context, key.to_vec())).or_default();
    if entry.len() < write_len {
      entry.resize(write_len, 0);
    }
    f(&mut entry[..write_len]);
    true
  }

  fn filter(&self, _context: u64, _internal_id: u32) -> bool {
    true
  }

  fn log(&self, _context: u64, _msg: &str) {}
}

fn callbacks() -> Callbacks<MemStore> {
  Callbacks::new(Arc::new(MemStore::new()))
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn config(dims: u32, quant: VectorQuantType, metric: VectorDistanceMetricType) -> IndexConfig {
  IndexConfig {
    dims,
    reduce_dims: 0,
    quant_type: quant,
    distance_metric: metric,
    build_exploration_factor: 64,
    num_links: 8,
  }
}

#[test]
fn create_insert_search_roundtrip() {
  let service = DiskANNService::default();
  assert_eq!(
    service.create_index(
      8,
      config(2, VectorQuantType::NoQuant, VectorDistanceMetricType::L2),
      callbacks()
    ),
    Ok(false)
  );

  // 首个插入同时设起点
  let res = service.insert(8, b"a", &f32_bytes(&[0.0, 0.0]), b"{\"k\":1}");
  assert_eq!(res, DiskAnnInsertResult::True);
  let res = service.insert(8, b"b", &f32_bytes(&[1.0, 1.0]), b"");
  assert_eq!(res, DiskAnnInsertResult::True);
  // 重复插入 → False
  let res = service.insert(8, b"a", &f32_bytes(&[5.0, 5.0]), b"");
  assert_eq!(res, DiskAnnInsertResult::False);
  // 维度不匹配 → False
  let res = service.insert(8, b"c", &f32_bytes(&[1.0]), b"");
  assert_eq!(res, DiskAnnInsertResult::False);

  assert_eq!(service.card(8), 2);
  assert!(service.check_external_id_valid(8, b"a"));
  assert!(!service.check_external_id_valid(8, b"zz"));

  let out = service
    .search_vector(
      8,
      &f32_bytes(&[0.1, 0.1]),
      SearchParams {
        count: 10,
        search_exploration_factor: 32,
        filter_len: 0,
        max_filtering_effort: 0,
      },
    )
    .unwrap();
  assert_eq!(out.found, 2);

  // 属性读取
  assert_eq!(service.get_attribute(8, b"a").unwrap(), b"{\"k\":1}");
  assert!(service.set_attribute(8, b"a", b"{}"));
  assert_eq!(service.get_attribute(8, b"a").unwrap(), b"{}");
  assert!(!service.set_attribute(8, b"nope", b"{}"));

  // 删除
  assert!(service.remove(8, b"a"));
  assert!(!service.remove(8, b"a"));
  assert_eq!(service.card(8), 1);
  assert!(service.get_full_vector(8, b"a").is_none());

  service.drop_index(8);
  assert_eq!(service.card(8), 0);
  assert!(
    service
      .search_vector(
        8,
        &f32_bytes(&[0.0, 0.0]),
        SearchParams {
          count: 10,
          search_exploration_factor: 8,
          filter_len: 0,
          max_filtering_effort: 0,
        },
      )
      .is_err()
  );
}

#[test]
fn search_element_and_embedding() {
  let service = DiskANNService::default();
  service
    .create_index(
      24,
      config(2, VectorQuantType::NoQuant, VectorDistanceMetricType::L2),
      callbacks(),
    )
    .unwrap();
  service.insert(24, b"x", &f32_bytes(&[1.0, 0.0]), b"");
  service.insert(24, b"y", &f32_bytes(&[0.0, 1.0]), b"");

  let params = SearchParams {
    count: 10,
    search_exploration_factor: 32,
    filter_len: 0,
    max_filtering_effort: 0,
  };
  let out = service.search_element(24, b"x", params).unwrap();
  assert!(out.found >= 1);

  let emb = service.embedding_of(24, b"x").unwrap();
  assert_eq!(emb, vec![1.0, 0.0]);

  let raw = service.get_full_vector(24, b"x").unwrap();
  assert_eq!(raw.len(), 8);

  let links = service.links_of(24, b"x").unwrap();
  assert!(!links.is_empty());

  let sample = service.sample(24, 5);
  assert_eq!(sample.len(), 2);

  assert_eq!(service.internal_id_of(24, b"x"), Some(1));
  assert_eq!(service.internal_id_of(24, b"none"), None);
  assert_eq!(service.quant_of(24), Some(VectorQuantType::NoQuant));
}
