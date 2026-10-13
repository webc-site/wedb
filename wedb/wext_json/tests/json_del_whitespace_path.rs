#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wext_json::{GarnetJsonObject, SetResult};
use wresp::options::ExistOptions;

#[test]
fn del_whitespace_path_as_root() {
  let mut obj = GarnetJsonObject::create();
  // 1. Set initial object
  let res = obj.set(b"$", br#"{"a":1}"#, ExistOptions::None).unwrap();
  assert_eq!(res, SetResult::Success);

  // 2. DEL with " " (whitespace) should be treated as root, return 1, and delete the key
  let del_count = obj.del(Some(b" "));
  assert_eq!(del_count, 1, "DEL on whitespace path should return 1");

  // 3. GET should return empty since root is deleted
  let mut out = Vec::new();
  obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap();
  assert!(out.is_empty() || obj.is_empty(), "Root should be deleted");
  assert!(obj.is_empty(), "Object should be empty after root deletion");

  // 4. Test "$" regression
  let mut obj2 = GarnetJsonObject::create();
  obj2.set(b"$", br#"{"a":1}"#, ExistOptions::None).unwrap();
  let del_count2 = obj2.del(Some(b"$"));
  assert_eq!(del_count2, 1);
  assert!(obj2.is_empty());

  // 5. Test "" regression
  let mut obj3 = GarnetJsonObject::create();
  obj3.set(b"$", br#"{"a":1}"#, ExistOptions::None).unwrap();
  let del_count3 = obj3.del(Some(b""));
  assert_eq!(del_count3, 1);
  assert!(obj3.is_empty());
}
