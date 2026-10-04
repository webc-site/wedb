//! RMW 族过期重建臂 ETag 旁路对偶清退回归（票 zcode-r157c-srethead 案一 P2）
//!
//! 缺陷面：rust ETag 为 KeyTag::Etag 独立旁路记录（wkv/src/etag.rs:5-14），
//! 级联纪律成文「键删除（DEL/GETDEL/过期 purge/FLUSHDB）级联删除 etag」，
//! 删除内核 TTL+ETag 成对清退（write/mod.rs:550-561/:645-656）；唯独同步
//! RMW 写回单点 `RmwWindow::try_rmw_sync` Due 臂只清残留 TTL 不配 etag
//! 对偶，SETRANGE/APPEND/INCR 等无条件写族命中过期未回收键重建后，旁路旧
//! etag 被存活宿主 String 记录掩护永久泄漏（紧缩回收 Etag 臂两步校验判
//! 宿主存活恒放行）——GETWITHETAG 成帧 [旧, 新值]、持过期前基线者
//! SETIFMATCH 对语义全新键「恰一者命中」误写可达；且 TTL 内存 Due（快臂
//! 漏清）与磁盘候选/降级（异步臂 purge_expired→delete 级联清）对同键给
//! 不同 etag 基线，双臂分叉。
//!
//! 修复形态（zcode-r157c-srethead 初裁 → 票 wkv-set-family-etag-bypass-residue-
//! cas-baseline-fork 定裁 (a) 收口升级）：et 旁路清退单源现落 String 域覆写
//! 内核 `StoreSession::try_upsert_tag_sync_unprotected_with_prefix`——TTL 腿删除
//! 同点成对清退 etag（单次哈希探针初筛，命中即 try_delete_raw_sync_unprotected，
//! 遭环形页翻转零副作用早退降级，异步 upsert_tag 幂等重做）。`try_rmw_sync` Due
//! 臂不再自持对偶探针，改道内核承接（对齐 C# MainStore/RMWMethods.cs 过期臂
//! RemoveETag :441-446/:1043-1049 ExpireAndResume 转无 etag 初记录）；Pass/原位/
//! 异步臂按 C# GetRMWModifiedFieldInfo 恒 HasETag=false 同判据清退，健在键覆写
//! 亦消亡 etag，杜绝同旁路两套覆写语义。wkv 单点一族全治，RESP 层四臂零改动，
//! 重放臂同口自动收口。
//!
//! 全真存储真协议帧无 mock：热态过期重建主夹具逐字节钉 [0, 新值] 与
//! SETIFMATCH 失配；同口族抽 INCR/APPEND 钉 wkv 单点而非命令层补丁；
//! 存活形改钉随覆写清退 etag（旧「恒零前进」系上游 InPlaceUpdater 残留偶然）；
//! 纯缺席零填建形探针零写入；异步降级两态——
//! 环形页翻转风暴（慢臂承接对拍）与 DEBUG FLUSHANDEVICT 物理驱逐注入
//! 「TTL 内存 Due × etag 磁盘候选」首发降级形，断言异步闭环后 etag 终局
//! 判 0 且 TTL 级联完备。

use std::{sync::Arc, time::Duration};

use compio::time::sleep;
use tempfile::{TempDir, tempdir};
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{
      ConnectionProtectionOption, RespServerSession, RespServerSessionOptions,
    },
  },
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wnode_test::pump_session_park;
use wtest_base::resp_frame_str;

/// 压力翻转执行域（etag2/nx_conditional 同款）：16KB×4 页小环形日志；
/// DEBUG 档放行（FLUSHANDEVICT 受 enable-debug-command 保护）
fn env(
  tag: &str,
) -> aok::Result<(
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  GarnetApi,
  RespServerSession,
)> {
  let dir = tempdir()?;
  let config = StoreConfig::new(1024, 16 * 1024, 4, 0.5)?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session()?)).into();
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      enable_debug_command: ConnectionProtectionOption::Yes,
      ..RespServerSessionOptions::default()
    },
  );
  s.set_garnet_api(api.clone());
  Ok((dir, store, api, s))
}

/// `[etag, value]` 期望帧（RESP2，与 write_etag_val_array 同形）
fn etag_pair(etag: i64, val: &[u8]) -> Vec<u8> {
  let mut out = format!("*2\r\n:{etag}\r\n${}\r\n", val.len()).into_bytes();
  out.extend_from_slice(val);
  out.extend_from_slice(b"\r\n");
  out
}

/// SETIFMATCH 命中写成功帧 `[new_etag, nil]`
fn cond_ok(etag: i64) -> Vec<u8> {
  format!("*2\r\n:{etag}\r\n$-1\r\n").into_bytes()
}

/// 环风暴填充写回绕 4 页日志，使前序记录转磁盘候选
async fn wrap_log_storm(s: &mut RespServerSession) {
  let val = "f".repeat(700);
  let mut degraded = 0usize;
  for i in 0..300 {
    let key = format!("filler{i}");
    let (out, parked) = pump_session_park(s, &resp_frame_str(&["SET", &key, &val])).await;
    assert_eq!(out, b"+OK\r\n", "风暴填充写 {key} 须闭环");
    degraded += parked as usize;
  }
  assert!(
    degraded > 0,
    "测试前提：回绕 4 页容量应至少触发一次降级（否则对拍不触达慢臂）"
  );
}

/// 验证点 a（主夹具，热态快臂直证）：SETIFMATCH v 0 PX 1 建键（etag 1、
/// TTL 在场）→ 过期未回收 → SETRANGE 回 :1 成功重建 → GETWITHETAG 逐字节
/// [0, x]（修复前旁路残值成帧 [1, x]）→ 持过期前基线 1 者 SETIFMATCH y 1
/// 必失配（修复前「恰一者命中」误写语义全新键）→ SETWITHETAG 回初写 1 →
/// 新值无 TTL（PTTL -1，残留 TTL 清退完备）
#[compio::test]
async fn expired_setrange_rebuild_resets_etag_hot() -> aok::Result<()> {
  let (_dir, _store, _api, mut s) = env("rmw-etag-setrange-hot.db")?;
  assert_eq!(
    pump_session_park(
      &mut s,
      &resp_frame_str(&["SETIFMATCH", "kh", "v", "0", "PX", "1"])
    )
    .await
    .0,
    cond_ok(1)
  );
  sleep(Duration::from_millis(60)).await;
  let (out, parked) =
    pump_session_park(&mut s, &resp_frame_str(&["SETRANGE", "kh", "0", "x"])).await;
  assert!(!parked, "热态夹具应快臂闭环（否则不触 Due 臂）");
  assert_eq!(out, b":1\r\n", "SETRANGE 过期重建应答新长度");
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["GETWITHETAG", "kh"])).await;
  assert_eq!(
    out,
    etag_pair(0, b"x"),
    "过期重建后 etag 域必从头计（修复前泄漏旁路旧值成帧 [1, x]）"
  );
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["PTTL", "kh"])).await;
  assert_eq!(out, b":-1\r\n", "重建新值无 TTL（残留 TTL 已清退）");
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["SETIFMATCH", "kh", "y", "1"])).await;
  assert_eq!(
    out,
    etag_pair(0, b"x"),
    "持过期前基线 1 者必失配且零覆写（修复前对语义全新键误写 y、etag 抬 2）"
  );
  assert_eq!(
    pump_session_park(&mut s, &resp_frame_str(&["GET", "kh"]))
      .await
      .0,
    b"$1\r\nx\r\n",
    "失配写不得触值"
  );
  assert_eq!(
    pump_session_park(&mut s, &resp_frame_str(&["SETWITHETAG", "kh", "z"]))
      .await
      .0,
    b":1\r\n",
    "SETWITHETAG 初写口径回 1（修复前续号 stale+1=2）"
  );
  Ok(())
}

/// 验证点 b：APPEND 同式一形——过期键 APPEND 转重建，回新长度且 etag 归 0
#[compio::test]
async fn expired_append_rebuild_resets_etag() -> aok::Result<()> {
  let (_dir, _store, _api, mut s) = env("rmw-etag-append.db")?;
  assert_eq!(
    pump_session_park(
      &mut s,
      &resp_frame_str(&["SETWITHETAG", "ah", "v", "PX", "1"])
    )
    .await
    .0,
    b":1\r\n"
  );
  sleep(Duration::from_millis(60)).await;
  let (out, parked) = pump_session_park(&mut s, &resp_frame_str(&["APPEND", "ah", "y"])).await;
  assert!(!parked, "热态夹具应快臂闭环");
  assert_eq!(out, b":1\r\n", "过期值判缺，APPEND 即新键首写");
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["GETWITHETAG", "ah"])).await;
  assert_eq!(out, etag_pair(0, b"y"), "etag 域从头计（修复前 [1, y]）");
  Ok(())
}

/// 验证点 c：同口族抽 INCR 一形钉 wkv 单点而非命令层补丁——过期键 INCR
/// 后经 GETWITHETAG 回 0
#[compio::test]
async fn expired_incr_rebuild_resets_etag() -> aok::Result<()> {
  let (_dir, _store, _api, mut s) = env("rmw-etag-incr.db")?;
  assert_eq!(
    pump_session_park(
      &mut s,
      &resp_frame_str(&["SETWITHETAG", "ih", "5", "PX", "1"])
    )
    .await
    .0,
    b":1\r\n"
  );
  sleep(Duration::from_millis(60)).await;
  let (out, parked) = pump_session_park(&mut s, &resp_frame_str(&["INCR", "ih"])).await;
  assert!(!parked, "热态夹具应快臂闭环");
  assert_eq!(out, b":1\r\n", "过期旧值判缺，INCR 即新键首写 1");
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["GETWITHETAG", "ih"])).await;
  assert_eq!(out, etag_pair(0, b"1"), "etag 归零（修复前旁路 [1, 1]）");
  Ok(())
}

/// 验证点 d（定裁 (a) 改钉）：健在带 etag 键经 SETRANGE/APPEND RMW 覆写同样
/// 清退 etag——C# GetRMWModifiedFieldInfo 恒 HasETag=false，String 域覆写（含
/// 原位/尾部写回）一律使 etag 消亡，与 SET 覆写内核同判据，杜绝同旁路两套
/// 覆写语义。旧 r157c「存活键 etag 恒零前进」钉测系该上游 InPlaceUpdater 残留
/// 偶然，随裁撤销为覆写清退
#[compio::test]
async fn alive_rmw_overwrite_clears_etag() -> aok::Result<()> {
  let (_dir, _store, _api, mut s) = env("rmw-etag-alive.db")?;
  assert_eq!(
    pump_session_park(&mut s, &resp_frame_str(&["SETWITHETAG", "dh", "v"]))
      .await
      .0,
    b":1\r\n"
  );
  assert_eq!(
    pump_session_park(&mut s, &resp_frame_str(&["SETRANGE", "dh", "1", "zz"]))
      .await
      .0,
    b":3\r\n"
  );
  assert_eq!(
    pump_session_park(&mut s, &resp_frame_str(&["APPEND", "dh", "Q"]))
      .await
      .0,
    b":4\r\n"
  );
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["GETWITHETAG", "dh"])).await;
  assert_eq!(
    out,
    etag_pair(0, b"vzzQ"),
    "健在键 RMW 覆写清退 etag（基线归 0，对位 C# FieldInfo HasETag=false）"
  );
  // 持覆写前陈旧基线 1 者 SETIFMATCH 必失配且零覆写（CAS 基线随覆写消解）
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["SETIFMATCH", "dh", "r", "1"])).await;
  assert_eq!(out, etag_pair(0, b"vzzQ"), "陈旧 etag 1 失配回 [0, 旧值]");
  assert_eq!(
    pump_session_park(&mut s, &resp_frame_str(&["GET", "dh"]))
      .await
      .0,
    b"$4\r\nvzzQ\r\n",
    "失配写不得触值"
  );
  // 基线 0 命中 → 新 etag 1（条件写自归零基线重新推进）
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["SETIFMATCH", "dh", "r", "0"])).await;
  assert_eq!(out, cond_ok(1), "基线 0 恰一者命中");
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["GETWITHETAG", "dh"])).await;
  assert_eq!(out, etag_pair(1, b"r"), "条件写自基线 0 抬号 1");
  Ok(())
}

/// 验证点 e（纯缺席零填建形）：从未有 etag 的键 SETRANGE 零填建形与
/// APPEND 缺键首写——探针初筛恒空零额外写入，etag 基线 0 对位 C#
/// InitialUpdater 无 ETag 不变式
#[compio::test]
async fn absent_first_write_etag_zero_baseline() -> aok::Result<()> {
  let (_dir, _store, _api, mut s) = env("rmw-etag-absent.db")?;
  assert_eq!(
    pump_session_park(&mut s, &resp_frame_str(&["SETRANGE", "ef", "2", "ab"]))
      .await
      .0,
    b":4\r\n"
  );
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["GETWITHETAG", "ef"])).await;
  assert_eq!(out, etag_pair(0, b"\0\0ab"), "零填建形 etag 基线 0");
  assert_eq!(
    pump_session_park(&mut s, &resp_frame_str(&["APPEND", "eg", "xy"]))
      .await
      .0,
    b":2\r\n"
  );
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["GETWITHETAG", "eg"])).await;
  assert_eq!(out, etag_pair(0, b"xy"), "缺键首写 etag 基线 0");
  assert_eq!(
    pump_session_park(
      &mut s,
      &resp_frame_str(&["SETIFMATCH", "ef", "z", "0", "NOGET"])
    )
    .await
    .0,
    cond_ok(1),
    "缺席建形键 CAS 基线 0 恰一者命中（旁路零污染）"
  );
  Ok(())
}

/// 验证点 f 之一（快慢双臂对拍，环形页翻转风暴态）：同形过期重建键转
/// 磁盘候选后经 SETRANGE 重建——首发挂慢臂承接（upsert_rmw 经
/// purge_expired→delete 全域级联），终局应答与热臂逐字节全等
#[compio::test]
async fn expired_setrange_rebuild_cold_arm_parity() -> aok::Result<()> {
  let (_dir, _store, _api, mut s) = env("rmw-etag-cold-parity.db")?;
  // 热臂基准形
  assert_eq!(
    pump_session_park(
      &mut s,
      &resp_frame_str(&["SETWITHETAG", "ph", "v", "PX", "1"])
    )
    .await
    .0,
    b":1\r\n"
  );
  sleep(Duration::from_millis(60)).await;
  let (hot, parked) =
    pump_session_park(&mut s, &resp_frame_str(&["SETRANGE", "ph", "0", "x"])).await;
  assert!(!parked);
  assert_eq!(hot, b":1\r\n");
  let (hot_get, _) = pump_session_park(&mut s, &resp_frame_str(&["GETWITHETAG", "ph"])).await;
  assert_eq!(hot_get, etag_pair(0, b"x"));
  // 冷臂同形：风暴使 TTL/etag 旁路转磁盘候选
  assert_eq!(
    pump_session_park(
      &mut s,
      &resp_frame_str(&["SETWITHETAG", "pc", "v", "PX", "1"])
    )
    .await
    .0,
    b":1\r\n"
  );
  sleep(Duration::from_millis(60)).await;
  wrap_log_storm(&mut s).await;
  let (cold, parked) =
    pump_session_park(&mut s, &resp_frame_str(&["SETRANGE", "pc", "0", "x"])).await;
  assert!(parked, "风暴后 pc 须挂慢臂承接（否则对拍不触达异步闭环）");
  assert_eq!(cold, hot, "SETRANGE 重建应答快慢双臂逐字节全等");
  let (cold_get, _) = pump_session_park(&mut s, &resp_frame_str(&["GETWITHETAG", "pc"])).await;
  assert_eq!(cold_get, hot_get, "GETWITHETAG 终局 etag 快慢双臂收敛 0");
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["PTTL", "pc"])).await;
  assert_eq!(out, b":-1\r\n", "冷臂重建后残留 TTL 级联完备");
  Ok(())
}

/// 验证点 f 之二（Due 臂 × etag 磁盘候选注入）：SETWITHETAG 建键后 DEBUG
/// FLUSHANDEVICT 物理驱逐 etag/字符串旁路记录（应答即驱逐收口，记录地址
/// 恒低于新 head，磁盘候选按地址算术单调成立），再仿 msetnx_atomic.rs
/// :expire_residual 以 EXPIRE 同款同步内核 TTL 腿裸写过去刻度——TTL 记录
/// 确定性驻内存已死且无任何冷读交互——Due 臂 etag 探针命中、首发删除遭
/// 磁盘候选 Ok(Err) 零副作用早退降级（TTL 腿未触，异步臂 has_ttl_tag 恒真
/// 承接），异步闭环 purge→delete 级联幂等重做，终局 etag 判 0、TTL 完备、
/// 应答与热臂全等（修复前此形 GETWITHETAG 冷读残值成帧 [1, x]）。
/// 前代 PEXPIRE+sleep 注入形在门禁并发负载下呈 Gate=Pass 竞态红：EXPIRE 慢臂
/// 与驱逐收口交错，TTL 记录可落进迟到的 head 推进区而被内存门判无 TTL，
/// Due 臂不触发；改 TTL 裸写置于 FLUSHANDEVICT 应答之后，程序序即因果序，
/// 竞态窗归零（RESP 面无法自然构造「过期未清退且旁路在场」，先例同注）
#[compio::test]
async fn due_arm_etag_disk_candidate_degrades_and_closes() -> aok::Result<()> {
  let (_dir, store, _api, mut s) = env("rmw-etag-evict-degrade.db")?;
  assert_eq!(
    pump_session_park(&mut s, &resp_frame_str(&["SETWITHETAG", "fh", "v"]))
      .await
      .0,
    b":1\r\n",
    "etag 旁路记录落内存"
  );
  let (out, parked) = pump_session_park(&mut s, &resp_frame_str(&["DEBUG", "FLUSHANDEVICT"])).await;
  assert!(parked, "FLUSHANDEVICT 恒挂存储执行域慢路径");
  assert!(out.starts_with(b"+OK head="), "驱逐应答 +OK 文案: {out:?}");
  // TTL 记录以同步内核裸写过去刻度：无 EXPIRE 冷读交互、应答前必已完成
  // 索引挂账，SETRANGE 的 TTL 内存门恒判 Due（案二前提）
  {
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    put_ttl_sync(&batch, b"fh", now_ticks() - TICKS_PER_SECOND).unwrap();
  }
  let (out, parked) =
    pump_session_park(&mut s, &resp_frame_str(&["SETRANGE", "fh", "0", "x"])).await;
  assert!(
    parked,
    "Due 覆写遭 etag/字符串磁盘候选须首发降级挂慢臂（内核旁路腿承接清退）"
  );
  assert_eq!(out, b":1\r\n", "异步闭环后应答与快臂同帧");
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["GETWITHETAG", "fh"])).await;
  assert_eq!(
    out,
    etag_pair(0, b"x"),
    "闭环终局 etag 判 0（修复前旁路残值冷读成帧 [1, x]）"
  );
  let (out, _) = pump_session_park(&mut s, &resp_frame_str(&["PTTL", "fh"])).await;
  assert_eq!(out, b":-1\r\n", "TTL 级联完备");
  Ok(())
}
