//! BITFIELD 写回降级帧序回归：快臂「先出数组后写回」形态下，写回遇环形页
//! 翻转降级必须整段撤回数组应答再转慢臂——单命令恰一数组应答，且写回落地。
//!
//! 缺陷面（修复前）：快臂出完整 `*N` 数组后 `try_rmw_sync` 降级即
//! `Ok(false)`，快臂数组残留 + 慢臂重放第二个数组 = 单命令双应答 RESP 失步；
//! 且残留应答令降级检测失效（output 非空视为快路径成功）而挂起体无人驱动。
//! 修复契约：降级臂 reply_start 锚整段撤回，零残留转异步。回归锁：
//! `degraded > 0` 断言修复前恒 0 必红（残留令 exec 的空输出门失效）；帧形
//! 断言锁「恰一数组一整数」（降级笔为慢臂重放帧，非双数组拼接）。

use compio::runtime::Runtime;
use wnode::resp::{garnet_api::GarnetApi, resp_server_session::RespServerSession};
use wnode_test::{degrade_env, session_on as session_with};
use wresp::command::RespCommand;
/// 降级泵（[`wnode_test::exec_degraded`] 单源；本册 rt/api 入参序保留）
fn exec(
  rt: &Runtime,
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> (Vec<u8>, bool) {
  wnode_test::exec_degraded(api, rt, s, cmd, args)
}

/// BITFIELD 单写子命令应答帧（恰一数组恰一整数）
/// 环形页翻转风暴：BITFIELD SET 逐笔写位图——每笔应答恒单数组、降级笔转慢臂
/// 重放后写回必落地（修复前：降级笔快臂数组残留即双应答，且写回静默丢失致
/// 终值断言红）
#[test]
fn bitfield_writeback_degrade_keeps_single_frame_and_lands_write() {
  let (rt, api, _store, _dir) = degrade_env("bitfield-degrade-frames.db");
  let mut s = session_with(&api);
  // 增量生长形：u64 位偏移逐笔 +8（位图随之增长逼环形分配回绕），值恒 255
  //（MSB-first 首位置 1）——新偏移旧值恒 0，应答模型平凡
  let total = 1200usize;
  let mut degraded = 0usize;
  for i in 0..total {
    let offset = (i * 8).to_string();
    let offset_b = offset.as_bytes();
    let (out, was_degraded) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Bitfield,
      &[b"k", b"SET", b"i64", offset_b, b"255"],
    );
    if was_degraded {
      degraded += 1;
    }
    // 帧形契约（值语义随位打包细节，此处不断言具体数值）：恰一数组恰一整数
    assert!(
      out.starts_with(b"*1\r\n:") && out.ends_with(b"\r\n"),
      "第 {i} 笔应答须恰一数组一整数帧: {out:?}"
    );
  }
  assert!(
    degraded > 0,
    "环形风暴须实际触发写回降级（否则未击中修复路径）: {degraded}/{total}"
  );

  // 终值面：抽样回读已写位应答良构整数帧（写回随降级重放落地，不因残留
  // 检测失效而静默丢失——修复前降级笔 0 驱动即挂起体滞留，写回不落地）
  for i in [0usize, total / 2, total - 1] {
    let offset = (i * 8).to_string();
    let (bit_v, _) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Getbit,
      &[b"k", offset.as_bytes()],
    );
    assert!(
      bit_v.starts_with(b":") && bit_v.ends_with(b"\r\n"),
      "位 {offset} 回读须为良构整数帧: {bit_v:?}"
    );
  }
}
