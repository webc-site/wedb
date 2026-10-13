#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 用户数据双域 / 物理标签域读状态枚举契约（user_read.rs 内联测试迁出）：
//! 存储层权威态（wkv StoreResult）→ 会话层折叠态转换单源 from_result 的
//! 逐臂映射；变体存在性即由逐臂 assert_eq 钉住，不设恒真式变体自匹配测试

use wkv::StoreResult;
use wnode::storage::session::common::user_read::{TagRead, UserRead};

#[test]
fn user_read_from_result_mappings() {
  assert_eq!(
    UserRead::from_result::<()>(Ok(StoreResult::Success(Ok(42)))),
    Ok(UserRead::Hit(42))
  );
  assert_eq!(
    UserRead::<i32>::from_result::<()>(Ok(StoreResult::Success(Err(())))),
    Ok(UserRead::WrongType)
  );
  assert_eq!(
    UserRead::<i32>::from_result::<()>(Ok(StoreResult::NotFound)),
    Ok(UserRead::Missing)
  );
  assert_eq!(
    UserRead::<i32>::from_result::<()>(Ok(StoreResult::RecordOnDisk)),
    Ok(UserRead::Deferred)
  );
  assert_eq!(
    UserRead::<i32>::from_result::<&str>(Err("disk error")),
    Err("disk error")
  );
}

#[test]
fn tag_read_from_result_mappings() {
  assert_eq!(
    TagRead::from_result::<()>(Ok(StoreResult::Success(42))),
    Ok(TagRead::Hit(42))
  );
  assert_eq!(
    TagRead::<i32>::from_result::<()>(Ok(StoreResult::NotFound)),
    Ok(TagRead::Missing)
  );
  assert_eq!(
    TagRead::<i32>::from_result::<()>(Ok(StoreResult::RecordOnDisk)),
    Ok(TagRead::Deferred)
  );
  assert_eq!(
    TagRead::<i32>::from_result::<&str>(Err("disk error")),
    Err("disk error")
  );
}
