//! HELLO 冷库挂起臂段形态移交的流水线帧序回归（zcode-r126c-hello1 案一 P2）
//!
//! 票面命题：`process_hello_command` 挂起移交臂旧写 `mem::take(output)`
//! 整包卷走会话输出缓冲，装载失败时挂起体 Err 臂弃整段 reply，同批前序
//! 流水线应答连坐丢失——RESP 请求应答配对永久错拍（远超单命令报错的局部
//! 失效）；且挂起闭环（`resolve_slow_wait_into`）以应答首字节 `-` 判装载
//! 失败，前序错误帧开头时连坐误跳标量物化。修复为 `output.split_off(start_len)`
//! 最小移交（与事务守卫臂 truncate 同锚，先例见 AUTH 挂起臂 auth.rs 与
//! SELECT 挂起臂 array_commands.rs）。
//!
//! C# 契约对位：libs/server/Resp/BasicCommands.cs:ProcessHelloCommand
//!（:1774-1849）应答与同批前序命令回帧共用同一累积缓冲、同步内联出帧，
//! 任何拒绝/失败臂只追加本命令错误帧，绝不回收已在缓冲内的前序命令应答。
//!
//! 故障注入为真设备真故障（非 mock）：冷库 DbMeta 点查所在段文件截断为空，
//! `resolve_context` 磁盘读以 IO 错误上抛（同仓
//! `wnode/tests/cold_context_park_no_scalar_tear.rs` 同款物理介质故障口径）。

use std::{fs::OpenOptions, sync::Arc, time::Duration};

use compio::time::sleep;
use tempfile::{TempDir, tempdir};
use wacl::{AccessControlList, AclPassword, GarnetAclAuthenticator, user::User};
use wdev::SegmentedDevice;
use wkv::{GcConfig, GcManager, StoreConfig, WedbStore};
use wnode::resp::{
  acl_store::AclStore, garnet_api::StoreGarnetApi, resp_server_session::RespServerSession,
};
use wnode_test::{complete_len, err_frame};
use wresp::{
  catalog::RespAclCategories,
  cmd_strings::{RESP_ERR_SLOW_PATH_STORAGE, RESP_ERR_WRONG_TYPE},
};
use wtest_base::{resp_frame, test_store_config};

/// 测试库配置：路由空闲析构期限 0 秒即期（GC 手动驱动），冷态可达
fn config() -> aok::Result<StoreConfig> {
  let mut cfg = test_store_config();
  cfg.gc = GcConfig {
    route_idle_evict_secs: 0,
    ..GcConfig::default()
  };
  Ok(cfg)
}

/// 构造租户 5 的启用用户 bob（口令 bobpw、+@all），编码为存储真源记录字节
fn bob_record() -> Vec<u8> {
  let mut user = User::new("bob".to_string());
  user.set_enabled(true);
  user.add_password_hash(AclPassword::from_string("bobpw"));
  user.add_category(RespAclCategories::ALL).unwrap();
  user.to_bytes()
}

/// 装配冷态环境（同 cold_context_park_no_scalar_tear.rs 口径）：租户 5 建域、
/// 落 ACL 用户、建 db 3 映射与数据后全量析构；严格会话视角下租户 5 全冷，
/// 挂起面语义可达
async fn cold_env() -> aok::Result<(
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<StoreGarnetApi<SegmentedDevice>>,
  RespServerSession,
)> {
  let dir = tempdir()?;
  let store = Arc::new(WedbStore::open(
    config()?,
    Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db"))?),
  )?);

  // 阶段 A：非严格会话建域即持久化 DbMeta（磁盘为映射权威）
  {
    let session = store.new_session()?;
    session.set_context(5, 0);
    AclStore::new(&session)
      .write(5, b"bob", &bob_record())
      .await?;
    session.set_context(5, 3);
    {
      let batch = session.enter_batch();
      batch.try_upsert_sync(b"k3", b"v3").unwrap().unwrap();
    }
  } // 会话 Drop：租户 5 路由引用归零
  store.flush_and_evict_all().await?;
  sleep(Duration::from_millis(5)).await;
  GcManager::new(&store).run_once().await?;

  // 阶段 B：路由快照已摘除（冷），严格会话挂起面生效
  assert!(
    store.vdb.is_cold_db(5, 0),
    "空闲析构后租户 5 无库路由常驻（db 0 冷），否则本模块不触达挂起面"
  );

  let session = store.new_session()?;
  session.set_strict_context(true);
  let mut resp = RespServerSession::default();
  resp.max_databases = 16;
  resp.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::new(
    AccessControlList::new("")?,
  )))));
  let api = Arc::new(StoreGarnetApi::new(session));
  resp.set_garnet_api(api.clone());
  Ok((dir, store, api, resp))
}

/// 驱动一轮消费到闭环并回线面应答字节，返回 `(应答字节, 是否挂慢臂)`
///
/// 停车臂与慢路径应答一律经产线单点（drive_pending_parks 的
/// `take_output_into` 冲出与 `resolve_slow_wait_into` 并入），与真实网络泵
/// 同一写出面；前序应答先落线、挂起体应答其后，顺序即 RESP 配对断言面
async fn pump(session: &mut RespServerSession, input: &[u8]) -> (Vec<u8>, bool) {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(input);
  session.bytes_read = input.len();
  session.read_head = 0;
  session.end_read_head = 0;
  session.output.clear();
  assert!(session.try_consume_messages().is_some(), "无协议违规");
  // wire 首段 = 停车臂闭环已冲出的前序应答（drive_slow=false 保留慢臂自驱）
  let mut wire = Vec::new();
  wnode_test::drive_pending_parks(session, &mut wire, false).await;
  match session.take_slow_wait() {
    Some(slow) => {
      let reply = slow.resolve().await;
      session.resolve_slow_wait_into(&reply, &mut wire);
      (wire, true)
    }
    None => {
      session.take_output_into(&mut wire);
      (wire, false)
    }
  }
}

/// 会话绑定的底层 StoreSession 物理域（外层镜像的对齐参照物）
fn physical_domain(api: &StoreGarnetApi<SegmentedDevice>) -> (u64, u64) {
  (api.session.namespace(), api.session.active_db())
}

/// 装载成功臂：同批 [GET][HELLO 3 AUTH 冷租户用户] 全帧保序逐字节测——
/// 前序 GET 应答原样居首，HELLO map 其后且与独立 HELLO 3 应答逐字节等形，
/// 配对零错位
#[compio::test]
async fn hello_cold_park_success_keeps_pipeline_prefix_in_order() -> aok::Result<()> {
  let (_dir, _store, api, mut s) = cold_env().await?;

  // ns0 热区基线数据（前序命令应答的取材）
  let (wire, slow) = pump(&mut s, &resp_frame(&[b"SET", b"foo", b"bar"])).await;
  assert!(!slow);
  assert_eq!(wire, b"+OK\r\n");

  let mut batch = resp_frame(&[b"GET", b"foo"]);
  batch.extend_from_slice(&resp_frame(&[
    b"HELLO",
    b"3",
    b"AUTH",
    b"5#bob",
    b"bobpw",
    b"SETNAME",
    b"cold-conn",
  ]));
  let (wire, slow) = pump(&mut s, &batch).await;
  assert!(slow, "HELLO 冷租户认证须挂起磁盘点查装载");

  // 前序应答保全居首（修复点：旧整包 take 后装载失败连坐丢帧，成功路径
  // 亦被 - 连坐判据威胁）
  let prefix_len = complete_len(&wire).expect("首帧应完整");
  assert_eq!(
    &wire[..prefix_len],
    b"$3\r\nbar\r\n",
    "前序 GET 应答原样保序"
  );
  assert_eq!(
    wire[prefix_len..].first(),
    Some(&b'%'),
    "HELLO 应答段应为 RESP3 map 帧"
  );
  // 逐字节：挂起体产出的 HELLO map 与独立 HELLO 3 SETNAME 等形（配对无移位）
  let (ref_wire, ref_slow) = pump(
    &mut s,
    &resp_frame(&[b"HELLO", b"3", b"SETNAME", b"cold-conn"]),
  )
  .await;
  assert!(!ref_slow);
  assert_eq!(&wire[prefix_len..], &ref_wire[..], "HELLO 段逐字节等形");
  assert_eq!(
    complete_len(&wire[prefix_len..]),
    Some(wire.len() - prefix_len)
  );

  // 物化对齐：装载成功标量一次落位
  assert_eq!((s.namespace, s.active_db_id), (5, 0));
  assert_eq!(s.resp_protocol_version, 3);
  assert_eq!(s.client_name.as_deref(), Some("cold-conn"));
  let (who, _) = pump(&mut s, &resp_frame(&[b"ACL", b"WHOAMI"])).await;
  assert_eq!(who, b"$3\r\nbob\r\n", "装载成功后认证载荷真实可用");
  assert_eq!(physical_domain(&api), (5, 0), "物理域与外层镜像对齐");
  Ok(())
}

/// 装载失败臂：同批 [GET][HELLO 3 AUTH 冷租户用户] 磁盘故障下前序 GET 应答
/// 保全、错误帧居后直断——修复点核心：旧实现整包 take 使前序应答连坐丢弃，
/// 该连接此后每条应答错配到错误命令
#[compio::test]
async fn hello_cold_park_load_failure_preserves_prefix_and_appends_error() -> aok::Result<()> {
  let (_dir, store, _api, mut s) = cold_env().await?;

  let (wire, slow) = pump(&mut s, &resp_frame(&[b"SET", b"foo", b"bar"])).await;
  assert!(!slow);
  assert_eq!(wire, b"+OK\r\n");
  // 挂起前认证身份基线（免认证引导档恒 default，失败后须原样保持）
  let (who0, _) = pump(&mut s, &resp_frame(&[b"ACL", b"WHOAMI"])).await;

  let mut batch = resp_frame(&[b"GET", b"foo"]);
  batch.extend_from_slice(&resp_frame(&[
    b"HELLO",
    b"3",
    b"AUTH",
    b"5#bob",
    b"bobpw",
    b"SETNAME",
    b"cold-conn",
  ]));

  // 两段驱动：停车臂（认证点查成功、HELLO 段挂起移交）完成后再注入磁盘
  // 故障，慢体 resolve_context 装载必败——认证点查本身仍在故障前走通
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(&batch);
  s.bytes_read = batch.len();
  s.read_head = 0;
  s.end_read_head = 0;
  s.output.clear();
  assert!(s.try_consume_messages().is_some(), "无协议违规");
  let mut wire = Vec::new();
  wnode_test::drive_pending_parks(&mut s, &mut wire, false).await;
  OpenOptions::new()
    .write(true)
    .open(store.device.segment_path(0))
    .expect("段文件应已随 store 打开创建")
    .set_len(0)
    .expect("截断段文件注入磁盘故障");
  let slow = s
    .take_slow_wait()
    .expect("HELLO 冷租户须挂起装载体，否则本用例不触达失败臂");
  let reply = slow.resolve().await;
  s.resolve_slow_wait_into(&reply, &mut wire);

  // 前序 GET 应答保全 + 本命令错误帧居后（逐字节两帧拼接）
  let expect = [
    b"$3\r\nbar\r\n".as_slice(),
    &err_frame(RESP_ERR_SLOW_PATH_STORAGE),
  ]
  .concat();
  assert_eq!(wire, expect, "装载失败只弃本命令段，前序应答零连坐");

  // 零撕裂：认证态/协议版本/客户端名严格保持旧值（失败臂与认证失败同口径）
  assert_eq!(s.namespace, 0);
  assert_eq!(s.active_db_id, 0);
  assert_eq!(s.resp_protocol_version, 2);
  assert_eq!(s.client_name, None);
  // bob 句柄仅暂存挂起、失败即弃：认证身份经协议面回读须与挂起前基线等形
  let (who1, _) = pump(&mut s, &resp_frame(&[b"ACL", b"WHOAMI"])).await;
  assert_eq!(who1, who0, "装载失败后认证态严格保持旧身份");
  assert!(store.vdb.is_cold_db(5, 0), "装载失败不产生任何映射装载");
  Ok(())
}

/// 连坐判据测试位（pump.rs resolve_slow_wait_into 的 `-` 开头判据）：同批
/// 前序应答以 `-` 开头（WRONGTYPE 错误帧）+ HELLO 冷挂起装载成功——旧整包
/// take 使 reply 首字节为前序错误帧的 `-`，装载成功亦被误跳物化；段形态
/// 移交后判据只看本命令应答段，成功臂物化必达
#[compio::test]
async fn hello_cold_park_after_error_prefix_still_materializes() -> aok::Result<()> {
  let (_dir, _store, api, mut s) = cold_env().await?;

  let (wire, slow) = pump(&mut s, &resp_frame(&[b"SET", b"str", b"v"])).await;
  assert!(!slow);
  assert_eq!(wire, b"+OK\r\n");

  // 前序 LPUSH 打在 string 键上 → -WRONGTYPE 错误帧以 `-` 开头
  let mut batch = resp_frame(&[b"LPUSH", b"str", b"x"]);
  batch.extend_from_slice(&resp_frame(&[
    b"HELLO",
    b"3",
    b"AUTH",
    b"5#bob",
    b"bobpw",
    b"SETNAME",
    b"cold-conn",
  ]));
  let (wire, slow) = pump(&mut s, &batch).await;
  assert!(slow, "HELLO 冷租户认证须挂起磁盘点查装载");

  let prefix_len = complete_len(&wire).expect("首帧应完整");
  assert_eq!(
    &wire[..prefix_len],
    err_frame(RESP_ERR_WRONG_TYPE),
    "前序错误应答保全居首"
  );

  // 连坐封死：装载成功必须物化（旧实现此处 namespace 恒 0）
  assert_eq!(s.namespace, 5, "前序 - 开头应答不得连坐误跳标量物化");
  assert_eq!(s.resp_protocol_version, 3);
  assert_eq!(s.client_name.as_deref(), Some("cold-conn"));
  let (who, _) = pump(&mut s, &resp_frame(&[b"ACL", b"WHOAMI"])).await;
  assert_eq!(who, b"$3\r\nbob\r\n", "装载成功认证载荷随物化真实落位");
  assert_eq!((s.active_db_id, physical_domain(&api)), (0, (5, 0)));

  // HELLO 段与独立等形参照逐字节对齐（RESP 配对续用不移位）
  let (ref_wire, ref_slow) = pump(
    &mut s,
    &resp_frame(&[b"HELLO", b"3", b"SETNAME", b"cold-conn"]),
  )
  .await;
  assert!(!ref_slow);
  assert_eq!(&wire[prefix_len..], &ref_wire[..], "HELLO 段逐字节等形");
  Ok(())
}
