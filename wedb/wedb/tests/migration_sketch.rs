#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wedb::server::migration::{sketch::Sketch, sketch_status::SketchStatus};

#[test]
fn sketch_probe_follows_hash_and_store_and_status() {
  let sketch = Sketch::with_key_count(1024);

  // 未收录：probe 不命中，状态回落 Initializing
  let (exists, status) = sketch.probe(b"user:1001");
  assert!(!exists);
  assert_eq!(status, SketchStatus::Initializing);

  // 收录后置位命中
  sketch.hash_and_store(b"user:1001");
  let (exists, status) = sketch.probe(b"user:1001");
  assert!(exists);
  assert_eq!(status, SketchStatus::Initializing);

  // 状态推进经 probe 透出
  sketch.set_status(SketchStatus::Transmitting);
  let (exists, status) = sketch.probe(b"user:1001");
  assert!(exists);
  assert_eq!(status, SketchStatus::Transmitting);

  sketch.hash_and_store(b"user:1002");
  let (exists, status) = sketch.probe(b"user:1002");
  assert!(exists);
  assert_eq!(status, SketchStatus::Transmitting);

  // clear 复位 bitmap 与状态
  sketch.clear();
  let (exists, status) = sketch.probe(b"user:1001");
  assert!(!exists);
  assert_eq!(status, SketchStatus::Initializing);
  assert!(!sketch.probe(b"user:1002").0);
}
