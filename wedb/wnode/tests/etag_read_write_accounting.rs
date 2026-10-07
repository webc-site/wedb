#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! ETag 命令族（GETWITHETAG / GETIFNOTMATCH / SETIFMATCH / SETIFGREATER /
//! SETWITHETAG / DELIFGREATER）session found/notfound 会话指标记账回归
//! （票 task/ing/wnode-etag-family-session-found-notfound-accounting-fast-slow-asymmetry.md）
//!
//! 对标 C# MainStoreOps / Tsavorite 会话计数表：
//! - 读族（GETWITHETAG / GETIFNOTMATCH）：Hit → found + 1；Missing / Expired → notfound + 1；WrongType → 0
//! - SET 条件写族（SETIFMATCH / SETIFGREATER / SETWITHETAG）：
//!   * 键存活（Hit 命中写入 / Hit 失配零写）→ found + 1
//!   * 键缺席 / 过期初写 → notfound + 1
//!   * 对象键 WRONGTYPE promote 删写后初写 → notfound + 1
//!   * 快臂前置读保持 None 静默，慢臂走 read_value_and_etag_quiet_async 静默，
//!     命令出帧处单点收口折叠；Pending 降级尾参回放单帧按 found 补账
//! - DELIFGREATER：
//!   * 条件满足执行删除 → found + 1
//!   * 键缺席 / 过期 → notfound + 1
//!   * 对象键（WrongType）不删 → notfound + 1
//!   * 条件失配零删除 → 0 计（两臂均不增）

use std::{thread::sleep, time::Duration};

use compio::runtime::Runtime;
use wkv::SessionLocking;
use wnode::resp::{EtagResume, TtlLeg, garnet_api::GarnetApi, slow_path::SlowWait};
use wnode_test::{counts, metrics_env as env, roundtrip};
use wresp::command::RespCommand;

const RESP_V2: u8 = 2;

/// 慢臂直驱（与降级快照投递同径）：SETIFMATCH/SETIFGREATER/SETWITHETAG 三命
/// 令快照尾参恒携本族续跑标记（exec 契约）
fn slow_direct(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let mut snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  if matches!(
    cmd,
    RespCommand::Setifmatch | RespCommand::Setifgreater | RespCommand::Setwithetag
  ) {
    snapshot.push(EtagResume::Full.tail_bytes());
  }
  rt.block_on(SlowWait::for_command(api, cmd, snapshot, RESP_V2, SessionLocking::Basic).resolve())
}

fn slow_direct_resume(
  rt: &Runtime,
  api: &GarnetApi,
  cmd: RespCommand,
  args: &[&[u8]],
  resume: EtagResume,
) -> Vec<u8> {
  let mut snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  snapshot.push(resume.tail_bytes());
  rt.block_on(SlowWait::for_command(api, cmd, snapshot, RESP_V2, SessionLocking::Basic).resolve())
}

#[test]
fn getwithetag_accounting_fast_and_slow() {
  let (rt, mut c, api, handle, _dir, _store) = env("etag-acc-getwithetag.db");

  // 1. Hit
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"k1", b"v1"]), b"+OK\r\n");
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"GETWITHETAG", b"k1"]);
  assert!(resp.starts_with(b"*2\r\n:0\r\n"));
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "GETWITHETAG 快臂 Hit 应计 found+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Getwithetag, &[b"k1"]);
  assert!(resp.starts_with(b"*2\r\n:0\r\n"));
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "GETWITHETAG 慢臂 Hit 应计 found+1"
  );

  // 2. Missing
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"GETWITHETAG", b"absent1"]);
  assert_eq!(resp, b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "GETWITHETAG 快臂 Missing 应计 notfound+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Getwithetag, &[b"absent2"]);
  assert_eq!(resp, b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "GETWITHETAG 慢臂 Missing 应计 notfound+1"
  );

  // 3. WrongType
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"h1", b"f", b"v"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"GETWITHETAG", b"h1"]);
  assert!(resp.starts_with(b"-WRONGTYPE"));
  assert_eq!(
    counts(&handle),
    (f, n),
    "GETWITHETAG 快臂 WrongType 应 0 计"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Getwithetag, &[b"h1"]);
  assert!(resp.starts_with(b"-WRONGTYPE"));
  assert_eq!(
    counts(&handle),
    (f, n),
    "GETWITHETAG 慢臂 WrongType 应 0 计"
  );

  // 4. Expired
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp1", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"GETWITHETAG", b"exp1"]);
  assert_eq!(resp, b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "GETWITHETAG 快臂 Expired 应计 notfound+1"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp2", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Getwithetag, &[b"exp2"]);
  assert_eq!(resp, b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "GETWITHETAG 慢臂 Expired 应计 notfound+1"
  );
}

#[test]
fn getifnotmatch_accounting_fast_and_slow() {
  let (rt, mut c, api, handle, _dir, _store) = env("etag-acc-getifnotmatch.db");

  // 1. Hit match (given == current -> nil val)
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"k1", b"v1"]), b"+OK\r\n");
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"GETIFNOTMATCH", b"k1", b"0"]);
  assert_eq!(resp, b"*2\r\n:0\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "GETIFNOTMATCH 快臂 Hit match 应计 found+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Getifnotmatch, &[b"k1", b"0"]);
  assert_eq!(resp, b"*2\r\n:0\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "GETIFNOTMATCH 慢臂 Hit match 应计 found+1"
  );

  // 2. Hit mismatch (given != current -> return val)
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"GETIFNOTMATCH", b"k1", b"1"]);
  assert_eq!(resp, b"*2\r\n:0\r\n$2\r\nv1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "GETIFNOTMATCH 快臂 Hit mismatch 应计 found+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Getifnotmatch, &[b"k1", b"1"]);
  assert_eq!(resp, b"*2\r\n:0\r\n$2\r\nv1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "GETIFNOTMATCH 慢臂 Hit mismatch 应计 found+1"
  );

  // 3. Missing
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"GETIFNOTMATCH", b"absent1", b"0"]);
  assert_eq!(resp, b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "GETIFNOTMATCH 快臂 Missing 应计 notfound+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Getifnotmatch, &[b"absent2", b"0"]);
  assert_eq!(resp, b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "GETIFNOTMATCH 慢臂 Missing 应计 notfound+1"
  );

  // 4. WrongType
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"h1", b"f", b"v"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"GETIFNOTMATCH", b"h1", b"0"]);
  assert!(resp.starts_with(b"-WRONGTYPE"));
  assert_eq!(
    counts(&handle),
    (f, n),
    "GETIFNOTMATCH 快臂 WrongType 应 0 计"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Getifnotmatch, &[b"h1", b"0"]);
  assert!(resp.starts_with(b"-WRONGTYPE"));
  assert_eq!(
    counts(&handle),
    (f, n),
    "GETIFNOTMATCH 慢臂 WrongType 应 0 计"
  );

  // 5. Expired
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp1", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"GETIFNOTMATCH", b"exp1", b"0"]);
  assert_eq!(resp, b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "GETIFNOTMATCH 快臂 Expired 应计 notfound+1"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp2", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Getifnotmatch, &[b"exp2", b"0"]);
  assert_eq!(resp, b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "GETIFNOTMATCH 慢臂 Expired 应计 notfound+1"
  );
}

#[test]
fn setifmatch_accounting_fast_and_slow() {
  let (rt, mut c, api, handle, _dir, _store) = env("etag-acc-setifmatch.db");

  // 1. Missing 初写 → notfound + 1
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETIFMATCH", b"k1", b"v1", b"0"]);
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFMATCH 快臂 Missing 初写应计 notfound+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Setifmatch, &[b"k2", b"v2", b"0"]);
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFMATCH 慢臂 Missing 初写应计 notfound+1"
  );

  // 2. Hit match 命中写 → found + 1
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETIFMATCH", b"k1", b"v1_new", b"1"]);
  assert_eq!(resp, b"*2\r\n:2\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETIFMATCH 快臂 Hit match 应计 found+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(
    &rt,
    &api,
    RespCommand::Setifmatch,
    &[b"k2", b"v2_new", b"1"],
  );
  assert_eq!(resp, b"*2\r\n:2\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETIFMATCH 慢臂 Hit match 应计 found+1"
  );

  // 3. Hit mismatch 条件失配零写 → found + 1 (返回旧值或 NOGET nil)
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETIFMATCH", b"k1", b"v1_x", b"99"]);
  assert_eq!(resp, b"*2\r\n:2\r\n$6\r\nv1_new\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETIFMATCH 快臂 Hit mismatch 应计 found+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Setifmatch, &[b"k2", b"v2_x", b"99"]);
  assert_eq!(resp, b"*2\r\n:2\r\n$6\r\nv2_new\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETIFMATCH 慢臂 Hit mismatch 应计 found+1"
  );

  // 4. WrongType 对象键 promote 删写后初写 → notfound + 1
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"h1", b"f", b"v"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETIFMATCH", b"h1", b"val", b"0"]);
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFMATCH 快臂 WrongType promote 初写应计 notfound+1"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"h2", b"f", b"v"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Setifmatch, &[b"h2", b"val", b"0"]);
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFMATCH 慢臂 WrongType promote 初写应计 notfound+1"
  );

  // 5. Expired 初写 → notfound + 1
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp1", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETIFMATCH", b"exp1", b"v_new", b"0"]);
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFMATCH 快臂 Expired 初写应计 notfound+1"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp2", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = slow_direct(
    &rt,
    &api,
    RespCommand::Setifmatch,
    &[b"exp2", b"v_new", b"0"],
  );
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFMATCH 慢臂 Expired 初写应计 notfound+1"
  );

  // 6. 慢臂 Pending 续跑补投（found: true 与 found: false）
  let (f, n) = counts(&handle);
  let resume = EtagResume::Pending {
    ttl: TtlLeg {
      ticks: 0,
      domain: (0, 0),
    },
    new_etag: 5,
    found: true,
  };
  let resp = slow_direct_resume(
    &rt,
    &api,
    RespCommand::Setifmatch,
    &[b"k1", b"v", b"4"],
    resume,
  );
  assert_eq!(resp, b"*2\r\n:5\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETIFMATCH 慢臂 Pending found:true 续跑应计 found+1"
  );

  let (f, n) = counts(&handle);
  let resume = EtagResume::Pending {
    ttl: TtlLeg {
      ticks: 0,
      domain: (0, 0),
    },
    new_etag: 6,
    found: false,
  };
  let resp = slow_direct_resume(
    &rt,
    &api,
    RespCommand::Setifmatch,
    &[b"k1", b"v", b"5"],
    resume,
  );
  assert_eq!(resp, b"*2\r\n:6\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFMATCH 慢臂 Pending found:false 续跑应计 notfound+1"
  );
}

#[test]
fn setifgreater_accounting_fast_and_slow() {
  let (rt, mut c, api, handle, _dir, _store) = env("etag-acc-setifgreater.db");

  // 1. Missing 初写 → notfound + 1
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETIFGREATER", b"k1", b"v1", b"1"]);
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFGREATER 快臂 Missing 初写应计 notfound+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Setifgreater, &[b"k2", b"v2", b"1"]);
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFGREATER 慢臂 Missing 初写应计 notfound+1"
  );

  // 2. Hit match (given > existing) → found + 1
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETIFGREATER", b"k1", b"v1_new", b"5"]);
  assert_eq!(resp, b"*2\r\n:5\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETIFGREATER 快臂 Hit match 应计 found+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(
    &rt,
    &api,
    RespCommand::Setifgreater,
    &[b"k2", b"v2_new", b"5"],
  );
  assert_eq!(resp, b"*2\r\n:5\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETIFGREATER 慢臂 Hit match 应计 found+1"
  );

  // 3. Hit mismatch (given <= existing) → found + 1
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETIFGREATER", b"k1", b"v1_x", b"3"]);
  assert_eq!(resp, b"*2\r\n:5\r\n$6\r\nv1_new\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETIFGREATER 快臂 Hit mismatch 应计 found+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(
    &rt,
    &api,
    RespCommand::Setifgreater,
    &[b"k2", b"v2_x", b"3"],
  );
  assert_eq!(resp, b"*2\r\n:5\r\n$6\r\nv2_new\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETIFGREATER 慢臂 Hit mismatch 应计 found+1"
  );

  // 4. WrongType promote 初写 → notfound + 1
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"h1", b"f", b"v"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETIFGREATER", b"h1", b"val", b"1"]);
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFGREATER 快臂 WrongType promote 初写应计 notfound+1"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"h2", b"f", b"v"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Setifgreater, &[b"h2", b"val", b"1"]);
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFGREATER 慢臂 WrongType promote 初写应计 notfound+1"
  );

  // 5. Expired 初写 → notfound + 1
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp1", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETIFGREATER", b"exp1", b"v_new", b"1"]);
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFGREATER 快臂 Expired 初写应计 notfound+1"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp2", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = slow_direct(
    &rt,
    &api,
    RespCommand::Setifgreater,
    &[b"exp2", b"v_new", b"1"],
  );
  assert_eq!(resp, b"*2\r\n:1\r\n$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETIFGREATER 慢臂 Expired 初写应计 notfound+1"
  );
}

#[test]
fn setwithetag_accounting_fast_and_slow() {
  let (rt, mut c, api, handle, _dir, _store) = env("etag-acc-setwithetag.db");

  // 1. Missing 初写 → notfound + 1
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETWITHETAG", b"k1", b"v1"]);
  assert_eq!(resp, b":1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETWITHETAG 快臂 Missing 初写应计 notfound+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Setwithetag, &[b"k2", b"v2"]);
  assert_eq!(resp, b":1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETWITHETAG 慢臂 Missing 初写应计 notfound+1"
  );

  // 2. Hit 覆写 → found + 1
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETWITHETAG", b"k1", b"v1_new"]);
  assert_eq!(resp, b":2\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETWITHETAG 快臂 Hit 覆写应计 found+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Setwithetag, &[b"k2", b"v2_new"]);
  assert_eq!(resp, b":2\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETWITHETAG 慢臂 Hit 覆写应计 found+1"
  );

  // 3. WrongType promote 初写 → notfound + 1
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"h1", b"f", b"v"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETWITHETAG", b"h1", b"val"]);
  assert_eq!(resp, b":1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETWITHETAG 快臂 WrongType promote 初写应计 notfound+1"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"h2", b"f", b"v"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Setwithetag, &[b"h2", b"val"]);
  assert_eq!(resp, b":1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETWITHETAG 慢臂 WrongType promote 初写应计 notfound+1"
  );

  // 4. Expired 初写 → notfound + 1
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp1", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"SETWITHETAG", b"exp1", b"v_new"]);
  assert_eq!(resp, b":1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETWITHETAG 快臂 Expired 初写应计 notfound+1"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp2", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Setwithetag, &[b"exp2", b"v_new"]);
  assert_eq!(resp, b":1\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETWITHETAG 慢臂 Expired 初写应计 notfound+1"
  );

  // 5. 慢臂 Pending 续跑补投（found: true 与 found: false）
  let (f, n) = counts(&handle);
  let resume = EtagResume::Pending {
    ttl: TtlLeg {
      ticks: 0,
      domain: (0, 0),
    },
    new_etag: 10,
    found: true,
  };
  let resp = slow_direct_resume(&rt, &api, RespCommand::Setwithetag, &[b"k1", b"v"], resume);
  assert_eq!(resp, b":10\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "SETWITHETAG 慢臂 Pending found:true 续跑应计 found+1"
  );

  let (f, n) = counts(&handle);
  let resume = EtagResume::Pending {
    ttl: TtlLeg {
      ticks: 0,
      domain: (0, 0),
    },
    new_etag: 11,
    found: false,
  };
  let resp = slow_direct_resume(&rt, &api, RespCommand::Setwithetag, &[b"k1", b"v"], resume);
  assert_eq!(resp, b":11\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "SETWITHETAG 慢臂 Pending found:false 续跑应计 notfound+1"
  );
}

#[test]
fn delifgreater_accounting_fast_and_slow() {
  let (rt, mut c, api, handle, _dir, _store) = env("etag-acc-delifgreater.db");

  // 1. Missing → notfound + 1
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"DELIFGREATER", b"absent1", b"0"]);
  assert_eq!(resp, b":0\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "DELIFGREATER 快臂 Missing 应计 notfound+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Delifgreater, &[b"absent2", b"0"]);
  assert_eq!(resp, b":0\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "DELIFGREATER 慢臂 Missing 应计 notfound+1"
  );

  // 2. WrongType → notfound + 1 (C# Tsavorite NotFound)
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"h1", b"f", b"v"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"DELIFGREATER", b"h1", b"0"]);
  assert_eq!(resp, b":0\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "DELIFGREATER 快臂 WrongType 应计 notfound+1"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Delifgreater, &[b"h1", b"0"]);
  assert_eq!(resp, b":0\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "DELIFGREATER 慢臂 WrongType 应计 notfound+1"
  );

  // 3. Hit mismatch (given <= existing) → 0 计（两臂均不增）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SETWITHETAG", b"k1", b"v1"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"DELIFGREATER", b"k1", b"1"]);
  assert_eq!(resp, b":0\r\n");
  assert_eq!(
    counts(&handle),
    (f, n),
    "DELIFGREATER 快臂 Hit mismatch 应 0 计"
  );

  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Delifgreater, &[b"k1", b"1"]);
  assert_eq!(resp, b":0\r\n");
  assert_eq!(
    counts(&handle),
    (f, n),
    "DELIFGREATER 慢臂 Hit mismatch 应 0 计"
  );

  // 4. Hit match (given > existing) → deleted=1, found + 1
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"DELIFGREATER", b"k1", b"2"]);
  assert_eq!(resp, b":1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "DELIFGREATER 快臂 Hit match 应计 found+1"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SETWITHETAG", b"k2", b"v2"]),
    b":1\r\n"
  );
  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Delifgreater, &[b"k2", b"2"]);
  assert_eq!(resp, b":1\r\n");
  assert_eq!(
    counts(&handle),
    (f + 1, n),
    "DELIFGREATER 慢臂 Hit match 应计 found+1"
  );

  // 5. Expired → notfound + 1
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp1", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = roundtrip(&rt, &mut c, &[b"DELIFGREATER", b"exp1", b"5"]);
  assert_eq!(resp, b":0\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "DELIFGREATER 快臂 Expired 应计 notfound+1"
  );

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"exp2", b"v", b"PX", b"20"]),
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(50));
  let (f, n) = counts(&handle);
  let resp = slow_direct(&rt, &api, RespCommand::Delifgreater, &[b"exp2", b"5"]);
  assert_eq!(resp, b":0\r\n");
  assert_eq!(
    counts(&handle),
    (f, n + 1),
    "DELIFGREATER 慢臂 Expired 应计 notfound+1"
  );
}
