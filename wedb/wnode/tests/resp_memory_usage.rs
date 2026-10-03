//! MEMORY USAGE 命令测试（对标 libs/server/Storage/Functions/UnifiedStore/
//! ReadMethods.cs:HandleMemoryUsage 的 RESP 面 BasicCommands.cs:NetworkMemoryUsage）

use core::str;

use wnode_test::{err_frame, with_batch};
use wresp::cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR;

fn parse_resp_int(out: &[u8]) -> i64 {
  let text = str::from_utf8(&out[1..out.len() - 2]).unwrap();
  text.parse().unwrap()
}

/// 字符串键回值 > 0 且随值长度增长（AllocatedSize 口径：记录头 + 键 + 值 + 对齐）
#[test]
fn memory_usage_string_grows_with_value() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"mk", b"abc"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_memory_usage(&[b"mk"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out[0], b':');
    let small = parse_resp_int(&out);
    assert!(small > 0, "string key usage must be positive: {small}");

    // 值增长后尺寸单调增长（记录物理布局随值长扩展）
    let long_val = vec![b'x'; 4096];
    let mut out = Vec::new();
    s.network_set(&[b"mk", &long_val], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_memory_usage(&[b"mk"], batch, None, &mut out)
      .unwrap();
    let large = parse_resp_int(&out);
    assert!(
      large > small,
      "usage must grow with value size: {small} -> {large}"
    );
  });
}

/// hash / zset 对象键回值 > 0（信封记录物理尺寸 + 内层对象堆内存记账）
#[test]
fn memory_usage_collection_objects_positive() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"hk", b"field1", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_memory_usage(&[b"hk"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out[0], b':');
    let hash_usage = parse_resp_int(&out);
    assert!(hash_usage > 0, "hash key usage must be positive");

    let mut out = Vec::new();
    s.sorted_set_add(&[b"zk", b"1", b"member1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    let mut out = Vec::new();
    s.network_memory_usage(&[b"zk"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out[0], b':');
    let zset_usage = parse_resp_int(&out);
    assert!(zset_usage > 0, "zset key usage must be positive");
  });
}

/// 缺键回 nil（C# status != OK → WriteNull，RESP2 为 $-1）
#[test]
fn memory_usage_missing_key_returns_null() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_memory_usage(&[b"missing"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// SAMPLES 选项接受（嵌套类型采样对 Garnet 无效，语法兼容校验；负数拒绝）
#[test]
fn memory_usage_samples_option_accepted() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"sk", b"v"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    s.network_memory_usage(&[b"sk", b"SAMPLES", b"5"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out[0], b':');
    assert!(parse_resp_int(&out) > 0);

    // 语法错误：负采样数与未知选项
    let mut out = Vec::new();
    s.network_memory_usage(&[b"sk", b"SAMPLES", b"-1"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    let mut out = Vec::new();
    s.network_memory_usage(&[b"sk", b"BOGUS", b"5"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));
  });
}
