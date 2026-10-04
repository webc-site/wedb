use aok::{OK, Void};
use tempfile::tempdir;
use wconf::NodeArgs;
use wnode::service::StorageSessionProvider;

#[compio::test]
async fn aof_size_limit_empty_string_boot_guard_sentinel() -> Void {
  let dir = tempdir()?;
  let data_path = dir.path();

  let node = NodeArgs {
    aof: false,
    aof_size_limit: Some("".to_string()),
    ..Default::default()
  };

  let res = StorageSessionProvider::open_from_args(&node, data_path, |_, _| None).await;
  assert!(res.is_ok(), "空串且关态启动应通过: {:?}", res.err());

  let node2 = NodeArgs {
    aof: false,
    aof_size_limit: Some("10MB".to_string()),
    ..Default::default()
  };

  let res2 = StorageSessionProvider::open_from_args(&node2, data_path, |_, _| None).await;
  assert!(res2.is_err(), "非空值携关态必须拒启");
  if let Err(e) = res2 {
    let msg = e.to_string();
    assert!(
      msg.contains("cannot be enforced with disabled AOF"),
      "错误信息不匹配: {msg}"
    );
  }

  OK
}
