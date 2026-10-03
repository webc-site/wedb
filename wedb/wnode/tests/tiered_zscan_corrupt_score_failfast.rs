//! 分层态 ZSCAN 损坏分值载荷 fail-fast 撤帧回归
//! （票 task/ing/wnode-tiered-zscan-corrupt-score-silently-zero-vs-materialize-failfast.md）
//!
//! 覆盖：树内 zset 分值载荷非 8B 大端 f64（编码损坏 / codec 缺陷）时，扫描臂
//! `exec_tiered_scan` 不再 `unwrap_or(0.0)` 静默伪装成合法分值出帧，而是援物化臂
//! `tiered_materialize_blob` / 全扫选择臂 `zset_scan_select` 同形的 corrupt 标志
//! 停扫，经既有 `truncate(base) + Err(())` 出口连同预留头撤回臂进入点，上游闭环成
//! RESP 错误帧。断言三面：
//!   1. 同键分层 ZSCAN 得错误帧且无前导半帧（损坏成员之前的合法成员出帧字节被
//!      truncate 一并撤回，不残留 `*2…` 前缀）；
//!   2. 同类全扫臂（ZRANGE 走 zset_scan_select）对同一损坏事实同归 Err；
//!   3. 点读臂（ZSCORE）对同一损坏成员按在册姿态答 null（非错误帧，严禁误断错误）。
//!
//! 既有正常 8B / inf / -inf / zset 忽略 NOVALUES / start 越界守卫臂的逐字节回归
//! 由 `scan_family_dualstate_frames.rs` 在册承载，本文件不重复也不弱化，另附健康
//! 键邻命令应答无损回归。
//!
//! 自研依据：本仓自定不变量一致性（同一份树内 8B f64 分值载荷两读路同一失败口径）；
//! C# 分层态无对应扫描臂，判据非 C# 行为（详见票面一.1）

use wcol::types::member_ttl::encode_member;
use wnode_test::{
  TestEnv, auto_exec_env as auto_exec, bulk_bytes as bulk, promote_env as promote, scan_frame,
  session_with, tiered_env,
};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

/// 慢路径存储错误统一应答帧（Err(()) 经 exec_slow `err_frame!` 单源落帧，
/// 见 wnode/src/resp/garnet_api/slow/ RESP_ERR_SLOW_PATH_STORAGE）
const STORAGE_ERR_FRAME: &[u8] = b"-ERR slow path storage error\r\n";

/// 判据恒为「载荷非 8B 大端 f64」：此处构造 16B 非分值载荷，score_of_payload
/// 必返 None；成员名 / 载荷长度均远超 min_record_size 硬下限 2，稳过契约闸
fn corrupt_score_record() -> Vec<u8> {
  encode_member(b"not-an-8b-f64-score!", None)
}

/// 分层损坏分值键 `cz`：字典序 `a`(合法 1.5) < `z`(损坏) < `~`(哨兵 SET 值非 zset，
/// 此处不放)。`a` 先于损坏成员被出帧，故旧 unwrap_or(0.0) 缺陷形会回
/// `[a,1.5,z,0]` 合法帧；修复后须整体撤回、无前导半帧。
/// 健康对照键 `hz`：全合法载荷 + inf/-inf 文本项（回归既有正常臂不被本次改动波及）。
fn fixtures(env: &TestEnv) {
  promote(
    env,
    b"cz",
    GarnetObjectType::SortedSet,
    vec![
      (b"a".to_vec(), encode_member(&1.5f64.to_be_bytes(), None)),
      (b"z".to_vec(), corrupt_score_record()),
    ],
    i64::MAX,
  );
  promote(
    env,
    b"hz",
    GarnetObjectType::SortedSet,
    vec![
      (b"a".to_vec(), encode_member(&1.5f64.to_be_bytes(), None)),
      (b"b".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
      (
        b"i".to_vec(),
        encode_member(&f64::INFINITY.to_be_bytes(), None),
      ),
      (
        b"x".to_vec(),
        encode_member(&(-f64::INFINITY).to_be_bytes(), None),
      ),
    ],
    i64::MAX,
  );
}

#[test]
fn tiered_zscan_corrupt_score_failfast() {
  let env = tiered_env("zscan-corrupt-score.db");
  let mut s = session_with(&env);
  fixtures(&env);

  // 1. 损坏键分层 ZSCAN 得错误帧，且应答恰等于错误帧本体（无前导半帧：
  //    `a` 成员的合法出帧字节连同预留帧头被 truncate(base) 一并撤回臂进入点，
  //    绝不残留 `*2…$1…a…` 前缀，亦绝不以假分值 0 出损坏成员）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscan, &[b"cz", b"0"]),
    STORAGE_ERR_FRAME
  );

  // 2. 同类全扫臂（ZRANGE 走 zset_scan_select）对同一损坏事实同归 Err → 同错误帧
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zrange, &[b"cz", b"0", b"-1"]),
    STORAGE_ERR_FRAME
  );

  // 3. 点读臂（ZSCORE）对同一损坏成员在册姿态答 null（严禁断成错误帧）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscore, &[b"cz", b"z"]),
    b"$-1\r\n"
  );

  // 4. 撤帧只撤本命令应答：健康邻键 ZSCAN 紧随其后仍逐字节全量正确
  //    （正常 8B + inf/-inf 文本项，证明扫描臂 corrupt 标志与会话输出缓冲无损邻命令）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscan, &[b"hz", b"0"]),
    scan_frame(
      0,
      &[
        bulk(b"a"),
        bulk(b"1.5"),
        bulk(b"b"),
        bulk(b"2"),
        bulk(b"i"),
        bulk(b"inf"),
        bulk(b"x"),
        bulk(b"-inf")
      ]
    )
  );
}

/// 起始游标越界守卫臂不得因本次改动弱化：损坏键 start 越过 size 时早退出
/// [0, 空]（不触碰树、不进 corrupt 臂），与 scan_family_dualstate_frames.rs
/// 同族回归口径一致。
#[test]
fn tiered_zscan_corrupt_key_start_beyond_guard() {
  let env = tiered_env("zscan-corrupt-start.db");
  let mut s = session_with(&env);
  fixtures(&env);

  // cz size=2，start=99 越界：早退守卫命中，恒不触树、不出错误帧
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscan, &[b"cz", b"99"]),
    scan_frame(0, &[])
  );
  // 健康键同臂回归：zset 忽略 NOVALUES（分值照常出），正常 8B 帧逐字节不变
  assert_eq!(
    auto_exec(
      &env,
      &mut s,
      RespCommand::Zscan,
      &[b"hz", b"0", b"MATCH", b"a", b"NOVALUES"]
    ),
    scan_frame(0, &[bulk(b"a"), bulk(b"1.5")])
  );
}
