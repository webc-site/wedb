//! ACL #/! 口令哈希携空白 C# byte.Parse HexNumber 收受、rust 精确拒收锁测
//!（deviations §134，零行为改动纯增量锁）
//!
//! 在 garnet 中的相对路径: libs/server/ACL/ACLPassword.cs:ACLPasswordFromHash
//!
//! C# HexNumber=515（AllowLeadingWhite|AllowTrailingWhite|AllowHexSpecifier）
//! 使二字切片「 0」形可解析，32 组「<空白><hex>」64 字符串 C# 收 +OK 落账；
//! rust wbase::hex 精确查表空白必拒。本锁锁 rust 现状拒收面，严禁补容忍。

use wacl::{AclError, AclParser, AclPassword, User};

/// 32 组「<空白><hex>」64 字符哈希串（C# byte.Parse HexNumber 可收受形）
fn whitespace_hash() -> String {
  " 0".repeat(32)
}

/// from_hash 拒收臂：空白形 Err(Password) 文案与 C# 逐字同（严禁补容忍）
#[test]
fn from_hash_rejects_whitespace_pairs_with_exact_message() {
  match AclPassword::from_hash(&whitespace_hash()) {
    Err(AclError::Password(msg)) => assert_eq!(
      msg,
      "Unable to parse input password hash. The input is not of the correct format."
    ),
    other => panic!("应落 Err(Password)，实得 {other:?}"),
  }
  // 制表位同形必拒
  let tabbed: String = "\t0".repeat(32);
  assert!(matches!(
    AclPassword::from_hash(&tabbed),
    Err(AclError::Password(_))
  ));
}

/// apply 链拒收臂：`#<空白哈希>` Err(Parsing) 且口令集零变更；正对照纯
/// hex 大写形照常收受去重
#[test]
fn apply_hash_op_whitespace_rejected_passwords_unchanged() {
  let mut user = User::new("u".to_string());
  match AclParser::apply_acl_op_to_user(&mut user, &format!("#{}", whitespace_hash())) {
    Err(AclError::Parsing { message, .. }) => assert_eq!(
      message,
      "Unable to parse input password hash. The input is not of the correct format."
    ),
    other => panic!("应落 Err(Parsing)，实得 {other:?}"),
  }
  // 口令集零变更：干净用户无任何哈希落账
  assert!(!user.describe_user().contains(" #"));
  assert!(!user.validate_password(&AclPassword::from_string("x")));

  // 正对照：纯 hex 大写形照常收受，且集合语义去重
  let upper = "ABCD0123".repeat(8);
  AclParser::apply_acl_op_to_user(&mut user, &format!("#{upper}")).unwrap();
  AclParser::apply_acl_op_to_user(&mut user, &format!("#{upper}")).unwrap();
  let described = user.describe_user();
  assert_eq!(
    described.matches(" #").count(),
    1,
    "重复施加须去重: {described}"
  );
  assert!(user.validate_password(&AclPassword::from_hash(&upper).unwrap()));
}

/// `!` 删臂同形：空白哈希 Err(Parsing) 零删改；正对照既有哈希照常移除
#[test]
fn apply_remove_hash_op_whitespace_rejected_and_clean_remove_works() {
  let mut user = User::new("u".to_string());
  let hex = "ab".repeat(32);
  AclParser::apply_acl_op_to_user(&mut user, &format!("#{hex}")).unwrap();
  match AclParser::apply_acl_op_to_user(&mut user, &format!("!{}", whitespace_hash())) {
    Err(AclError::Parsing { .. }) => {}
    other => panic!("应落 Err(Parsing)，实得 {other:?}"),
  }
  // 空白形删除臂不被动既有哈希
  assert!(user.validate_password(&AclPassword::from_hash(&hex).unwrap()));
  // 正对照：合法哈希删除臂照常移除
  AclParser::apply_acl_op_to_user(&mut user, &format!("!{hex}")).unwrap();
  assert!(!user.validate_password(&AclPassword::from_hash(&hex).unwrap()));
}
