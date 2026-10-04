//! 截断开放索引器回归测试
//!
//! 对标 C# garnet/modules/GarnetJSON/JSONPath/JsonPath.cs:ParseIndexer (EnsureLength 守卫)
//! 验证 `$[*, $.a[*, $..[*` 等截断开放索引器返回 `Error::InvalidPath` 错误帧而非越界 panic。

use wext_json::{Error, JsonPath};

#[test]
fn test_truncated_open_indexer_returns_invalid_path() {
  let truncated_paths = [
    "$[*", "$.a[*", "$..[*", "$[ *", "$[* ", "$.a[* ", "$..[*   ", "[*", "[* ",
  ];

  for path in truncated_paths {
    let res = JsonPath::parse(path);
    assert!(
      res.is_err(),
      "截断路径 {path} 应当解析失败返回 Err，但得到了 Ok"
    );
    match res {
      Err(Error::InvalidPath(msg)) => {
        assert_eq!(
          msg, "Path ended with open indexer.",
          "路径 {path} 应当返回 EnsureLength 错误文案，实得: {msg}"
        );
      }
      other => panic!("路径 {path} 应当返回 InvalidPath，实得: {other:?}"),
    }
  }
}

#[test]
fn test_valid_wildcard_indexer_regression() {
  let valid_paths = ["$[*]", "$.a[*]", "$..[*]", "[*]", "$[ * ]", "$[  *  ]"];

  for path in valid_paths {
    assert!(
      JsonPath::parse(path).is_ok(),
      "合法路径 {path} 应当解析成功，但返回了 Err"
    );
  }
}

#[test]
fn test_unexpected_char_in_indexer_regression() {
  let invalid_paths = ["$[*abc]", "$[*123]"];

  for path in invalid_paths {
    let res = JsonPath::parse(path);
    assert!(res.is_err(), "非法路径 {path} 应当解析失败");
    match res {
      Err(Error::InvalidPath(msg)) => {
        assert!(
          msg.starts_with("Unexpected character:"),
          "非法路径 {path} 应当返回意外字符错误，实得: {msg}"
        );
      }
      other => panic!("非法路径 {path} 应当返回 InvalidPath，实得: {other:?}"),
    }
  }
}
