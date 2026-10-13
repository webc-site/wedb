#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wacl::{AclError, AclPassword};

/// garnet/test/standalone/Garnet.test.acl/Resp/ACL/AclTest.cs:DummyPasswordHash
const DUMMY_PASSWORD_HASH: &str =
  "8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9";

#[test]
fn from_string_sha256_hex() {
  let p = AclPassword::from_string("passw0rd");
  assert_eq!(p.to_string(), DUMMY_PASSWORD_HASH);
  assert_eq!(p, AclPassword::from_bytes(b"passw0rd"));
}

#[test]
fn from_hash_roundtrip() {
  let p = AclPassword::from_hash(DUMMY_PASSWORD_HASH).unwrap();
  assert_eq!(p, AclPassword::from_string("passw0rd"));

  // 大写十六进制同样接受
  let upper = AclPassword::from_hash(&DUMMY_PASSWORD_HASH.to_uppercase()).unwrap();
  assert_eq!(upper, p);
}

#[test]
fn from_hash_rejects_bad_input() {
  assert!(matches!(
    AclPassword::from_hash("abcd"),
    Err(AclError::Password(_))
  ));
  assert!(matches!(
    AclPassword::from_hash(&"z".repeat(64)),
    Err(AclError::Password(_))
  ));
}

#[test]
fn password_equality_and_copy() {
  let p1 = AclPassword::from_string("secret1");
  let p2 = AclPassword::from_string("secret1");
  let p3 = AclPassword::from_string("secret2");

  // Copy 语义
  let p1_copy = p1;
  assert_eq!(p1, p1_copy);
  assert_eq!(p1, p2);
  assert_ne!(p1, p3);
}
