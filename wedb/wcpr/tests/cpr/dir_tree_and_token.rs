//! 目录树刷写与 Token 解析/目录列举集成测试（自 src/manager/mod.rs 内联测迁入：
//! tokio 式真文件系统形态——tempdir 真目录 + compio 运行时，断言与覆盖原样保留）
//!
//! 依赖面：`list_checkpoints`/`purge_checkpoint`/`meta_filename`/`token_to_base32`
//! 均为既有 pub 面；`sync_dir_tree`/`parse_token` 经 #[doc(hidden)] 测试专用口
//! 访问（见 lib.rs 隐藏面注）。

use std::fs::{create_dir_all, read, write};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{
  list_checkpoints, meta_filename, parse_token, purge_checkpoint, sync_dir_tree, token_to_base32,
};

/// sync_dir_tree 必须容忍任意深度的嵌套目录树、空目录与空文件，且幂等可重入
///
/// fsync 的持久化效果在用户态不可观测（落盘与否内核说了算），可观测面为
/// 「刷写遍历零扰动」：两轮 sync 后文件字节原样可回读、目录结构原样在册
#[test]
fn sync_dir_tree_handles_nested_tree_and_is_idempotent() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let deep = dir.path().join("token/rangeindex/prefix");
    create_dir_all(&deep).unwrap();
    write(deep.join("data.bftree"), b"payload").unwrap();
    write(deep.join("empty.bftree"), b"").unwrap();
    create_dir_all(dir.path().join("token/empty_dir")).unwrap();

    sync_dir_tree(dir.path()).await.unwrap();
    sync_dir_tree(dir.path()).await.unwrap();

    // 幂等重入后内容零扰动：文件回读逐字节一致、空文件仍空、空目录仍在
    assert_eq!(
      read(deep.join("data.bftree")).unwrap(),
      b"payload",
      "两轮 sync 后数据文件必须逐字节无损"
    );
    assert!(
      read(deep.join("empty.bftree")).unwrap().is_empty(),
      "空文件必须保持为空（不得被刷写遍历写入杂音）"
    );
    assert!(
      dir.path().join("token/empty_dir").is_dir(),
      "空目录必须原样保留"
    );
  });
}

/// 目标目录不存在必须报错暴露（由调用方的 exists() 守卫先行过滤）
#[test]
fn sync_dir_tree_fails_on_missing_dir() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    assert!(sync_dir_tree(&dir.path().join("missing")).await.is_err());
  });
}

/// 快照 Token 规范 Base32 编码解析与目录列举验证
#[test]
fn test_token_parsing_and_listing() {
  let token = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210_u128;
  let b32 = token_to_base32(token);

  // 1. Base32 格式正确解析
  assert_eq!(parse_token(b32.as_str()), Some(token));
  // 非规范格式（十进制、无效长度等）拒绝
  assert_eq!(parse_token(&token.to_string()), None);
  assert_eq!(parse_token("invalid-guid-string"), None);

  // 2. 目录中 Base32 文件名列举与排序
  let dir = tempdir().unwrap();
  let f1 = dir.path().join(meta_filename(token));
  write(f1, b"").unwrap();

  let list = list_checkpoints(dir.path()).unwrap();
  assert_eq!(list.len(), 1);
  assert_eq!(list[0], token);

  // 3. 清理
  purge_checkpoint(dir.path(), token);
  let list_after = list_checkpoints(dir.path()).unwrap();
  assert!(list_after.is_empty());
}
