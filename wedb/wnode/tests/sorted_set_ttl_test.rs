//! 有序集合成员级 TTL 回归测试（对标 libs/server/Resp/Objects/SortedSetCommands.cs）
//!
//! 验证：
//! 1. ZEXPIRE / ZTTL / ZPERSIST 正确解析 MEMBERS nummembers member；
//! 2. 缺少 MEMBERS / nummembers 非整数 / 计数不吻合（含 0 与负数）时的报错文案必须
//!    与 Garnet 完全一致（C# 无值域门，0/负数一律落 must-match 臂）；
//! 3. 键缺失或成员不存在时返回 -2 数组。

use std::{sync::Arc, thread::sleep, time::Duration};

use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{BatchStoreSession, WedbStore};
use wnode::resp::resp_server_session::RespServerSession;
use wtest_base::test_store_config;

type TestBatch<'a> = BatchStoreSession<'a, SegmentedDevice>;

fn with_test_env(f: impl FnOnce(&mut RespServerSession, &TestBatch)) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持 TTL 惰性过期语义
  let config = test_store_config();
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
        "ZEXPIRE",
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
        "ZEXPIRE",
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
        "ZEXPIRE",
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
        "ZTTL",
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
        "ZTTL",
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
    for cmd in ["ZEXPIRE", "ZPEXPIRE", "ZEXPIREAT", "ZPEXPIREAT"] {
      out.clear();
      session
        .sorted_set_expire(
          cmd,
          &[b"k", b"10", b"MEMBERS", b"1"],
          batch,
          &mut out,
          false,
          false,
        )
        .unwrap();
      assert_eq!(
        out,
        format!("-ERR wrong number of arguments for '{cmd}' command\r\n").as_bytes()
      );
    }

    for cmd in ["ZTTL", "ZPTTL", "ZEXPIRETIME", "ZPEXPIRETIME"] {
      out.clear();
      session
        .sorted_set_time_to_live(
          cmd,
          &[b"k", b"MEMBERS", b"1"],
          batch,
          &mut out,
          false,
          false,
        )
        .unwrap();
      assert_eq!(
        out,
        format!("-ERR wrong number of arguments for '{cmd}' command\r\n").as_bytes()
      );
    }

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
        "ZEXPIRE",
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
        "ZEXPIRE",
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
        "ZEXPIRE",
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
        "ZEXPIRE",
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
        "ZTTL",
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

    // ---- numMembers 计数不吻合（0/负数对位 C# 落 Count 比对臂）与非整数 ----
    out.clear();
    session
      .sorted_set_expire(
        "ZEXPIRE",
        &[b"k", b"10", b"MEMBERS", b"0", b"m1"],
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
        "ZTTL",
        &[b"k", b"MEMBERS", b"-1", b"m1"],
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
        "ZEXPIRE",
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
        "ZTTL",
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
        "ZEXPIRE",
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
        "ZTTL",
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
        "ZEXPIRE",
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
        "ZTTL",
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

/// MEMBERS 0 零成员合法形态对位（C# SortedSetCommands.cs:SortedSetExpire 门序无
/// num 值域门：NX 形态 Count==currIdx+0 放行、零成员执行回 *0；多余实参落
/// must-match 臂；RESP2/RESP3 帧头一致）
#[test]
fn test_zexpire_members_zero_parity() {
  with_test_env(|session, batch| {
    let mut out = Vec::new();
    session
      .sorted_set_add(&[b"myzset", b"1.0", b"m1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // ZEXPIRE myzset 100 NX MEMBERS 0 → *0（计数吻合，零成员执行）
    out.clear();
    let ok = session
      .sorted_set_expire(
        "ZEXPIRE",
        &[b"myzset", b"100", b"NX", b"MEMBERS", b"0"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*0\r\n");

    // 多余实参 → must match（不得抢答 greater-than-0）
    out.clear();
    session
      .sorted_set_expire(
        "ZEXPIRE",
        &[b"myzset", b"100", b"NX", b"MEMBERS", b"0", b"x"],
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

    // RESP3 会话同帧
    session.resp_protocol_version = 3;
    out.clear();
    session
      .sorted_set_expire(
        "ZPEXPIRE",
        &[b"myzset", b"100", b"XX", b"MEMBERS", b"0"],
        batch,
        &mut out,
        true,
        false,
      )
      .unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}

/// 成员级 TTL 窄窗回归（zcode-r34-memberttl2）：
/// 极短 TTL 到期后：
/// 1. ZADD NX 视同缺席新增，回 1 且 ZSCORE 可见新分值，重插不带旧 TTL（ZTTL 回 -1）；
/// 2. ZINCRBY 视同缺席新增，以 0 为基底落分值，且写回落盘不因旧刻度被滤除；
/// 3. ZADD GT / LT 视同缺席新增，不被旧分值比较拒绝。
#[test]
fn test_zset_expired_member_zadd_nx_and_zincrby_treated_as_missing() {
  with_test_env(|session, batch| {
    let mut out = Vec::new();

    // 1. ZADD NX 测试：初始存活成员挂 50ms 极短 TTL
    session
      .sorted_set_add(&[b"zk:nx", b"10.0", b"m1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();

    session
      .sorted_set_expire(
        "ZPEXPIRE",
        &[b"zk:nx", b"50", b"MEMBERS", b"1", b"m1"],
        batch,
        &mut out,
        true,
        false,
      )
      .unwrap();
    assert_eq!(out, b"*1\r\n:1\r\n");
    out.clear();

    sleep(Duration::from_millis(80));

    // ZADD NX：恰到期成员视同缺席，走新增臂回 1 并写入新分值（旧实现被 NX 拦截回 0）
    session
      .sorted_set_add(&[b"zk:nx", b"NX", b"20.0", b"m1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n", "到期成员 ZADD NX 应视同缺席新增回 1");
    out.clear();

    session
      .sorted_set_score(&[b"zk:nx", b"m1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$2\r\n20\r\n", "重插新分值应可见");
    out.clear();

    // 守卫先摘后插：旧过期账不残留（ZTTL 归 -1，新成员不被后续误剔）
    session
      .sorted_set_time_to_live(
        "ZTTL",
        &[b"zk:nx", b"MEMBERS", b"1", b"m1"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(out, b"*1\r\n:-1\r\n", "重插成员不得携带旧 TTL");
    out.clear();

    // 2. ZINCRBY 测试：恰到期成员先摘后按新增臂落 incr_value，杜绝残留旧刻度落盘被滤除
    session
      .sorted_set_add(&[b"zk:incr", b"100.0", b"m2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();

    session
      .sorted_set_expire(
        "ZPEXPIRE",
        &[b"zk:incr", b"50", b"MEMBERS", b"1", b"m2"],
        batch,
        &mut out,
        true,
        false,
      )
      .unwrap();
    assert_eq!(out, b"*1\r\n:1\r\n");
    out.clear();

    sleep(Duration::from_millis(80));

    // ZINCRBY：到期成员按缺席处理（0 + 5.5 = 5.5，而不是 100 + 5.5）
    session
      .sorted_set_increment(&[b"zk:incr", b"5.5", b"m2"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out, b"$3\r\n5.5\r\n",
      "到期成员 ZINCRBY 应以 0 为基底落分值"
    );
    out.clear();

    session
      .sorted_set_score(&[b"zk:incr", b"m2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$3\r\n5.5\r\n", "ZINCRBY 后分值可读回");
    out.clear();

    // 成员存活且不带旧 TTL
    session
      .sorted_set_time_to_live(
        "ZTTL",
        &[b"zk:incr", b"MEMBERS", b"1", b"m2"],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
    assert_eq!(out, b"*1\r\n:-1\r\n", "ZINCRBY 重插成员不得携带旧 TTL");
    out.clear();

    // 3. ZADD GT / LT 测试：到期成员视同缺席，按新增语义不受 GT 拦截
    session
      .sorted_set_add(&[b"zk:gt", b"100.0", b"m3"], batch, &mut out)
      .unwrap();
    out.clear();

    session
      .sorted_set_expire(
        "ZPEXPIRE",
        &[b"zk:gt", b"50", b"MEMBERS", b"1", b"m3"],
        batch,
        &mut out,
        true,
        false,
      )
      .unwrap();
    out.clear();

    sleep(Duration::from_millis(80));

    // 旧分值为 100.0，新分值为 50.0；若未到期，GT 比较 100.0 > 50.0 会拒绝更新（回 0）；
    // 到期视同缺席走新增臂，成功新增回 1
    session
      .sorted_set_add(&[b"zk:gt", b"GT", b"50.0", b"m3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n", "到期成员 ZADD GT 应按缺席语义新增回 1");
    out.clear();

    session
      .sorted_set_score(&[b"zk:gt", b"m3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$2\r\n50\r\n", "新分值应可见");
  });
}
