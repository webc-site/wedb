//! 有序集合成员级 TTL 回归测试（对标 libs/server/Resp/Objects/SortedSetCommands.cs）
//!
//! 验证：
//! 1. ZEXPIRE / ZTTL / ZPERSIST 正确解析 MEMBERS nummembers member；
//! 2. 缺少 MEMBERS / nummembers 非正整数 / 参数数量不匹配时的报错文案必须与 Garnet 完全一致；
//! 3. 键缺失或成员不存在时返回 -2 数组。

use std::sync::Arc;

use tempfile::tempdir;
use wdev::SegmentedDevice;
use wedb_test::test_store_config;
use wkv::{BatchStoreSession, WedbStore};
use wnode::resp::resp_server_session::RespServerSession;

type TestBatch<'a> = BatchStoreSession<'a, SegmentedDevice>;

fn with_test_env(f: impl FnOnce(&mut RespServerSession, &TestBatch)) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持 TTL 惰性过期语义
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let mut resp = RespServerSession::default();
  f(&mut resp, &batch);
}

#[test]
fn test_sorted_set_ttl_members_header_and_expiry() {
  with_test_env(|session, batch| {
    let mut out = Vec::new();

    // 初始化有序集合 myzset：包含 member1 (1.0), member2 (2.0)
    let ok = session
      .sorted_set_add(
        &[b"myzset", b"1.0", b"member1", b"2.0", b"member2"],
        batch,
        &mut out,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b":2\r\n");

    // 1. ZEXPIRE myzset 100 MEMBERS 2 member1 member2
    out.clear();
    let ok = session
      .sorted_set_expire(
        &[b"myzset", b"100", b"MEMBERS", b"2", b"member1", b"member2"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*2\r\n:1\r\n:1\r\n");

    // 带 NX 条件：已有过期，NX 拒绝更新（返回 0）
    out.clear();
    let ok = session
      .sorted_set_expire(
        &[b"myzset", b"200", b"NX", b"MEMBERS", b"1", b"member1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*1\r\n:0\r\n");

    // 带 XX 条件：已有过期，XX 允许更新（返回 1）
    out.clear();
    let ok = session
      .sorted_set_expire(
        &[b"myzset", b"300", b"XX", b"MEMBERS", b"1", b"member1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*1\r\n:1\r\n");

    // 2. ZTTL myzset MEMBERS 2 member1 member2
    out.clear();
    let ok = session
      .sorted_set_time_to_live(
        &[b"myzset", b"MEMBERS", b"2", b"member1", b"member2"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert!(ok);
    // member1 TTL 应该在 (0, 300]，member2 TTL 应该在 (0, 100]
    assert!(out.starts_with(b"*2\r\n:"));

    // 3. ZPERSIST myzset MEMBERS 2 member1 member2
    out.clear();
    let ok = session
      .sorted_set_persist(
        &[b"myzset", b"MEMBERS", b"2", b"member1", b"member2"],
        batch,
        &mut out,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*2\r\n:1\r\n:1\r\n");

    // 再次 ZPERSIST：无关联过期，返回 -1
    out.clear();
    let ok = session
      .sorted_set_persist(
        &[b"myzset", b"MEMBERS", b"2", b"member1", b"member2"],
        batch,
        &mut out,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*2\r\n:-1\r\n:-1\r\n");

    // 再次 ZTTL：已无关联过期，返回 -1
    out.clear();
    let ok = session
      .sorted_set_time_to_live(
        &[b"myzset", b"MEMBERS", b"2", b"member1", b"member2"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*2\r\n:-1\r\n:-1\r\n");
  });
}

#[test]
fn test_sorted_set_ttl_error_messages_match_garnet() {
  with_test_env(|session, batch| {
    let mut out = Vec::new();

    // ---- 参数数量不足 ----
    // ZEXPIRE 需至少 5 个参数
    out.clear();
    session
      .sorted_set_expire(
        &[b"k", b"10", b"MEMBERS", b"1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ZEXPIRE' command\r\n"
    );

    // ZTTL 需至少 4 个参数
    out.clear();
    session
      .sorted_set_time_to_live(&[b"k", b"MEMBERS", b"1"], batch, &mut out, false, false)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ZTTL' command\r\n"
    );

    // ZPERSIST 需至少 4 个参数
    out.clear();
    session
      .sorted_set_persist(&[b"k", b"MEMBERS", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ZPERSIST' command\r\n"
    );

    // ---- 过期时间非法 ----
    // 非整数
    out.clear();
    session
      .sorted_set_expire(
        &[b"k", b"invalid_int", b"MEMBERS", b"1", b"m1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    // 负数
    out.clear();
    session
      .sorted_set_expire(
        &[b"k", b"-10", b"MEMBERS", b"1", b"m1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(out, b"-ERR invalid expire time, must be >= 0\r\n");

    // ---- 缺少 MEMBERS 关键字 ----
    // ZEXPIRE 无选项缺少 MEMBERS
    out.clear();
    session
      .sorted_set_expire(
        &[b"k", b"10", b"WRONG", b"1", b"m1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(
      out,
      b"-Mandatory argument MEMBERS is missing or not at the right position\r\n"
    );

    // ZEXPIRE 带选项缺少 MEMBERS
    out.clear();
    session
      .sorted_set_expire(
        &[b"k", b"10", b"NX", b"WRONG", b"1", b"m1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(
      out,
      b"-Mandatory argument MEMBERS is missing or not at the right position\r\n"
    );

    // ZTTL 缺少 MEMBERS
    out.clear();
    session
      .sorted_set_time_to_live(
        &[b"k", b"WRONG", b"1", b"m1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(
      out,
      b"-Mandatory argument MEMBERS is missing or not at the right position\r\n"
    );

    // ZPERSIST 缺少 MEMBERS
    out.clear();
    session
      .sorted_set_persist(&[b"k", b"WRONG", b"1", b"m1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-Mandatory argument MEMBERS is missing or not at the right position\r\n"
    );

    // ---- numMembers 非正整数 ----
    out.clear();
    session
      .sorted_set_expire(
        &[b"k", b"10", b"MEMBERS", b"0", b"m1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Parameter `numMembers` should be greater than 0\r\n"
    );

    out.clear();
    session
      .sorted_set_time_to_live(
        &[b"k", b"MEMBERS", b"-1", b"m1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Parameter `numMembers` should be greater than 0\r\n"
    );

    out.clear();
    session
      .sorted_set_persist(&[b"k", b"MEMBERS", b"abc", b"m1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Parameter `numMembers` should be greater than 0\r\n"
    );

    // ---- numMembers 与参数数量不匹配 ----
    out.clear();
    session
      .sorted_set_expire(
        &[b"k", b"10", b"MEMBERS", b"2", b"m1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(
      out,
      b"-The `numMembers` parameter must match the number of arguments\r\n"
    );

    out.clear();
    session
      .sorted_set_time_to_live(
        &[b"k", b"MEMBERS", b"1", b"m1", b"m2"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(
      out,
      b"-The `numMembers` parameter must match the number of arguments\r\n"
    );

    out.clear();
    session
      .sorted_set_persist(&[b"k", b"MEMBERS", b"3", b"m1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-The `numMembers` parameter must match the number of arguments\r\n"
    );
  });
}

#[test]
fn test_sorted_set_ttl_missing_key_or_member() {
  with_test_env(|session, batch| {
    let mut out = Vec::new();

    // 1. 键缺失（不存在的键）：返回全 -2 数组
    out.clear();
    let ok = session
      .sorted_set_expire(
        &[b"nonexistent", b"10", b"MEMBERS", b"2", b"m1", b"m2"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*2\r\n:-2\r\n:-2\r\n");

    out.clear();
    let ok = session
      .sorted_set_time_to_live(
        &[b"nonexistent", b"MEMBERS", b"2", b"m1", b"m2"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*2\r\n:-2\r\n:-2\r\n");

    out.clear();
    let ok = session
      .sorted_set_persist(
        &[b"nonexistent", b"MEMBERS", b"2", b"m1", b"m2"],
        batch,
        &mut out,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*2\r\n:-2\r\n:-2\r\n");

    // 2. 键存在，但部分成员不存在
    session
      .sorted_set_add(&[b"myzset2", b"1.0", b"m1"], batch, &mut out)
      .unwrap();

    out.clear();
    let ok = session
      .sorted_set_expire(
        &[b"myzset2", b"10", b"MEMBERS", b"2", b"m1", b"ghost"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*2\r\n:1\r\n:-2\r\n");

    out.clear();
    let ok = session
      .sorted_set_time_to_live(
        &[b"myzset2", b"MEMBERS", b"2", b"m1", b"ghost"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert!(ok);
    assert!(out.starts_with(b"*2\r\n:"));
    assert!(out.ends_with(b":-2\r\n"));

    out.clear();
    let ok = session
      .sorted_set_persist(
        &[b"myzset2", b"MEMBERS", b"2", b"m1", b"ghost"],
        batch,
        &mut out,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*2\r\n:1\r\n:-2\r\n");
  });
}
