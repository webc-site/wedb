//! ETag 写族慢臂 RI 门挡写单帧回归（票 zcode-r139c-etag2 案二 P1）
//!
//! 缺陷面：慢臂写共同体 `apply_etag_write_async` 的 Blocked 臂写完 WRONGTYPE
//! 后 `return Ok(())`——返回形态违反「Blocked 走已应答出口、调用方绝不再续写」
//! 写共同体契约（set.rs:149-150 头注明文，快臂同径靠 `Err(()) => {}` 抑制成功
//! 帧），调用方五臂（etag_conditional_slow Hit/Missing/WrongType-delete 与
//! etag_slow Setwithetag 初写/覆写）`?` 后无条件续写成功帧——冷 RI 键上慢臂出
//! 双帧（-WRONGTYPE 之后又 *2/:etag 或 :int）、快臂单帧，pipeline 客户端把第二
//! 帧错配给下一条命令，全连接应答级联失步。C# 一命令恒一应答帧
//! （ExecuteETagSetCommand 的 ProcessOutput 恰调用一次，BasicEtagCommands.cs:293-310）。
//!
//! 修复形态：Blocked 改回独立的已应答早停标志 `Ok(false)`（本函数零出帧），
//! 调用方见 false 即按本命令口径出单 WRONGTYPE 后立即返回；挡写绝不经
//! `Err(())`（exec_slow 统一应答面会在 WRONGTYPE 之上再叠一帧
//! RESP_ERR_SLOW_PATH_STORAGE，同一双帧病，garnet_api/slow.rs:225-226）。
//!
//! 夹具对标票面「冷装配（RI 元记录刷盘 + 无 String 记录）」：案一同款
//! 16KB×4 页小环形日志压力翻转（etag_conditional_degrade_replay.rs 同款，
//! 回绕后前序记录转磁盘候选，快臂 meta 探针落 Deferred 整命令降级慢臂承接），
//! 全真存储真协议帧，无 mock。

use std::sync::Arc;

use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::{complete_len, err_frame};
use wresp::cmd_strings::RESP_ERR_WRONG_TYPE;
use wtest_base::resp_frame_str;

/// 压力翻转执行域 + RI 目录：16KB×4 页小环形日志，回绕复用后前序记录成磁盘候选
fn ri_env(
  tag: &str,
) -> aok::Result<(
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  GarnetApi,
  RespServerSession,
)> {
  let dir = tempdir()?;
  let config =
    StoreConfig::new(1024, 16 * 1024, 4, 0.5)?.with_range_index_dir(dir.path().join("ri"));
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session()?));
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  Ok((dir, store, api, s))
}

/// 单命令往返到闭环，返回 (应答字节, 是否挂慢臂)
async fn pump(s: &mut RespServerSession, args: &[&str]) -> (Vec<u8>, bool) {
  pump_batch(s, &resp_frame_str(args)).await
}

/// 原线面批量命令往返：前序同步应答先落线、挂起体应答其后，顺序即 RESP 配对断言面
async fn pump_batch(s: &mut RespServerSession, input: &[u8]) -> (Vec<u8>, bool) {
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(input);
  s.bytes_read = input.len();
  s.read_head = 0;
  s.end_read_head = 0;
  s.output.clear();
  assert!(s.try_consume_messages().is_some(), "无协议违规");
  let mut wire = Vec::new();
  wnode_test::drive_pending_parks(s, &mut wire, false).await;
  match s.take_slow_wait() {
    Some(slow) => {
      let reply = slow.resolve().await;
      s.resolve_slow_wait_into(&reply, &mut wire);
      (wire, true)
    }
    None => {
      s.take_output_into(&mut wire);
      (wire, false)
    }
  }
}

/// 建 RI 键（元记录 + 树内字段，String 域无记录——票面夹具形态），RI 族走白名单
async fn seed_ri(s: &mut RespServerSession, key: &str) {
  assert_eq!(
    pump(s, &["RI.CREATE", key, "MEMORY", "MINRECORD", "8"])
      .await
      .0,
    b"+OK\r\n",
    "RI.CREATE 应成功"
  );
  assert_eq!(
    pump(s, &["RI.SET", key, "field1", "value1"]).await.0,
    b"+OK\r\n",
    "RI.SET 应成功"
  );
}

/// 环形页翻转风暴：足量大值填充写回绕 4 页日志，使前序 RI 元记录转磁盘候选
/// （案一同款判据：风暴笔笔闭环 +OK）
async fn wrap_log_storm(s: &mut RespServerSession) {
  let val = "f".repeat(700);
  let mut degraded = 0usize;
  for i in 0..300 {
    let key = format!("filler{i}");
    let (out, parked) = pump(s, &["SET", &key, &val]).await;
    assert_eq!(out, b"+OK\r\n", "风暴填充写 {key} 须闭环");
    degraded += parked as usize;
  }
  assert!(degraded > 0, "测试前提：回绕 4 页容量应至少触发一次降级");
}

/// 案二核心：冷装配 RI 键（元记录刷盘 + 无 String 记录）上 SETIFMATCH /
/// SETIFGREATER / SETWITHETAG 慢臂挡写应答逐字节 = 快臂门内已答单帧
/// `-{RESP_ERR_WRONG_TYPE}\r\n`（set.rs:104-107 形态），绝不再叠第二帧；
/// 数据面零副作用（RI 元记录与树内字段完好、String 域未写入、etag 侧写缺）
#[compio::test]
async fn cold_ri_etag_slow_arm_writes_single_wrongtype_frame() -> aok::Result<()> {
  let (_dir, _store, _api, mut s) = ri_env("etag2-ri-cold.db")?;
  // 每命令独占预刷盘 RI 键：首触即慢臂承接，杜绝前一命令异步读回温后序键候选态
  for key in ["ri_match", "ri_greater", "ri_withetag"] {
    seed_ri(&mut s, key).await;
  }
  wrap_log_storm(&mut s).await;

  let expect = err_frame(RESP_ERR_WRONG_TYPE);
  for (key, cmd) in [
    ("ri_match", "SETIFMATCH"),
    ("ri_greater", "SETIFGREATER"),
    ("ri_withetag", "SETWITHETAG"),
  ] {
    let given = if cmd == "SETIFGREATER" { "5" } else { "0" };
    let (out, parked) = if cmd == "SETWITHETAG" {
      pump(&mut s, &[cmd, key, "v"]).await
    } else {
      pump(&mut s, &[cmd, key, "v", given]).await
    };
    assert!(
      parked,
      "{cmd} {key} 冷 RI 快臂探针应挂慢臂（否则本用例不触达慢臂挡写臂）"
    );
    assert_eq!(
      out, expect,
      "{cmd} 慢臂挡写应答须逐字节 = 快臂单 WRONGTYPE 帧"
    );
    assert_eq!(
      complete_len(&out),
      Some(out.len()),
      "{cmd} 单完整帧（双帧回归哨兵）"
    );
  }

  // 零副作用面：三键 RI 元记录与树内字段完好（修复前双帧病在应答面，数据面同向）
  for key in ["ri_match", "ri_greater", "ri_withetag"] {
    assert_eq!(pump(&mut s, &["TYPE", key]).await.0, b"+rangeindex\r\n");
    assert_eq!(
      pump(&mut s, &["RI.GET", key, "field1"]).await.0,
      b"$6\r\nvalue1\r\n"
    );
  }
  Ok(())
}

/// pipeline 帧序对齐：同批 [SET plain][SETIFMATCH 冷 RI]——前序 +OK 居首、
/// 本命令单 WRONGTYPE 其后恰两帧（修复前慢臂第二帧 *2/:etag 会错配给后续
/// 命令，本断言即该级联失步的哨兵）
#[compio::test]
async fn pipeline_prefix_then_cold_ri_wrongtype_keeps_pairing() -> aok::Result<()> {
  let (_dir, _store, _api, mut s) = ri_env("etag2-ri-pipe.db")?;
  seed_ri(&mut s, "ri").await;
  wrap_log_storm(&mut s).await;

  let mut batch = resp_frame_str(&["SET", "plain", "pv"]);
  batch.extend_from_slice(&resp_frame_str(&["SETIFMATCH", "ri", "v2", "0"]));
  let (wire, parked) = pump_batch(&mut s, &batch).await;
  assert!(parked, "SETIFMATCH 冷 RI 须挂慢臂承接");

  let expect: Vec<u8> = [b"+OK\r\n".as_slice(), &err_frame(RESP_ERR_WRONG_TYPE)].concat();
  assert_eq!(
    wire, expect,
    "前序应答居首 + 单 WRONGTYPE 恰两帧，配对零移位"
  );
  // 帧边界：第二帧后无残留字节（双帧即 wire 超长）
  assert_eq!(complete_len(&wire[..5]), Some(5));
  let rest = &wire[5..];
  assert_eq!(complete_len(rest), Some(rest.len()));
  Ok(())
}
