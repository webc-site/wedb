use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use wedb_cluster::redis::RespValue;
use wedb_cluster::redis::json_util::{
  json_arr_append, json_arr_pop, json_arr_trim, json_merge_patch, json_num_op, json_path_query,
  json_path_set, json_to_resp, json_toggle,
};

#[test]
fn test_json_path_query_and_set() {
  let mut doc = sonic_rs::json!({
      "a": 1,
      "b": {
          "c": "hello",
          "d": [10, 20, 30]
      }
  });

  let q1 = json_path_query(&doc, "$.b.c");
  assert_eq!(q1.len(), 1);
  assert_eq!(q1[0].as_str(), Some("hello"));

  let q2 = json_path_query(&doc, "$.b.d[1]");
  assert_eq!(q2.len(), 1);
  assert_eq!(q2[0].as_i64(), Some(20));

  let q_neg = json_path_query(&doc, "$.b.d[-1]");
  assert_eq!(q_neg.len(), 1);
  assert_eq!(q_neg[0].as_i64(), Some(30));

  let ok = json_path_set(&mut doc, "$.b.c", sonic_rs::json!("world"), false, false);
  assert!(ok);
  assert_eq!(doc["b"]["c"].as_str(), Some("world"));

  // NX option
  let ok_nx = json_path_set(&mut doc, "$.b.c", sonic_rs::json!("test"), true, false);
  assert!(!ok_nx);
  assert_eq!(doc["b"]["c"].as_str(), Some("world"));
}

#[test]
fn test_json_num_and_array_ops() {
  let mut doc = sonic_rs::json!({
      "num": 10,
      "arr": [1, 2, 3],
      "flag": true
  });

  json_num_op(&mut doc, "$.num", 5.0, true).unwrap();
  assert_eq!(doc["num"].as_i64(), Some(15));

  json_num_op(&mut doc, "$.num", 2.0, false).unwrap();
  assert_eq!(doc["num"].as_i64(), Some(30));

  json_toggle(&mut doc, "$.flag");
  assert_eq!(doc["flag"].as_bool(), Some(false));

  json_arr_append(
    &mut doc,
    "$.arr",
    vec![sonic_rs::json!(4), sonic_rs::json!(5)],
  );
  assert_eq!(doc["arr"].as_array().unwrap().len(), 5);

  let popped = json_arr_pop(&mut doc, "$.arr", -1);
  assert_eq!(popped, vec![Some(sonic_rs::json!(5))]);
  assert_eq!(doc["arr"].as_array().unwrap().len(), 4);

  json_arr_trim(&mut doc, "$.arr", 1, 2);
  assert_eq!(doc["arr"].as_array().unwrap().len(), 2);
  assert_eq!(doc["arr"][0].as_i64(), Some(2));
  assert_eq!(doc["arr"][1].as_i64(), Some(3));
}

#[test]
fn test_json_merge_patch() {
  let mut target = sonic_rs::json!({
      "title": "Goodbye!",
      "author": {
          "givenName": "John",
          "familyName": "Doe"
      },
      "tags": ["example", "sample"],
      "content": "This will be unchanged"
  });

  let patch = sonic_rs::json!({
      "title": "Hello!",
      "phoneNumber": "+01-123-456-7890",
      "author": {
          "familyName": null
      },
      "tags": ["example"]
  });

  json_merge_patch(&mut target, &patch);

  assert_eq!(target["title"].as_str(), Some("Hello!"));
  assert_eq!(target["phoneNumber"].as_str(), Some("+01-123-456-7890"));
  assert_eq!(target["author"]["givenName"].as_str(), Some("John"));
  assert!(target["author"].get("familyName").is_none());
  assert_eq!(target["tags"].as_array().unwrap().len(), 1);
  assert_eq!(target["content"].as_str(), Some("This will be unchanged"));
}

#[test]
fn test_json_to_resp() {
  let val = sonic_rs::json!({
      "name": "wedb",
      "active": true,
      "count": 42
  });
  let resp = json_to_resp(&val);
  if let RespValue::Arr(elements) = resp {
    assert_eq!(elements[0], RespValue::Simple("{".to_string()));
  } else {
    unreachable!("Expected Arr from json_to_resp");
  }
}
