//! 冷上下文挂起失败后会话标量零撕裂回归
//!
//! 票面命题：严格会话切库 / 认证切租命中冷库冷租户时，会话标量
//!（`active_db_id` / `namespace` / `acl_user_handle`）与 HELLO 元数据严禁在
//! 底层 `set_context` 确认就绪前提前覆写（对标 C# RespServerSession.cs:
//! TrySwitchActiveDatabaseSession 的 success 门——`TryGetOrSetDatabaseSession`
//! 失败即原库保持、绝不 `SwitchActiveDatabaseSession`）。异步点查装载
//!（`resolve_context`）失败时若标量已被篡改，外层会话汇报新域、底层读写
//! 穿透旧域，即产生跨库错写与多租户隔离击穿的永久撕裂。
//!
//! 故障注入为真设备真故障（非 mock）：冷库 DbMeta 点查所在段文件被截断为空，
//! `resolve_context` 磁盘读以 IO 错误上抛（同仓
//! `wnode/tests/mget_slow_path_storage_error.rs` 同款物理介质故障口径）。
//!
//! 对标 C#：`ArrayCommands.cs:NetworkSELECT` → `TrySwitchActiveDatabaseSession`
//! 原库保持语义；`BasicCommands.cs:NetworkAUTH/NetworkHELLO` 认证失败即时终止、
//! 绝不提前变更会话上下文与协议版本。

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
use wresp::{catalog::RespAclCategories, cmd_strings::RESP_ERR_SLOW_PATH_STORAGE};
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

/// 装配冷态环境（运行期空闲析构口径，无重启）：
///
/// 阶段 A：非严格会话建权威态——租户 5 建域、落 ACL 用户、建 db 3 映射与
/// 数据；`flush_and_evict_all` 后全部记录（含 DbMeta）滑出内存热区成为磁盘
/// 候选，会话解绑后 GC 轮次摘除租户 5 路由快照（期限 0 秒即期）；
///
/// 阶段 B：严格会话（RESP 连接同形态）视角下租户 5 库级路由全冷
///（`is_cold_db` 为真），挂起面语义可达；返回的会话已挂 ACL 认证器与存储
/// 执行域，`Arc<StoreGarnetApi>` 为物理域断言句柄
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
    store.vdb.is_cold_db(5, 3),
    "空闲析构后 db 3 必须冷（库映射未装载），否则本模块不触达挂起面"
  );
  assert!(
    store.vdb.is_cold_db(5, 0),
    "空闲析构后 db 0 亦须冷（租户 5 无库路由常驻）"
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

/// 驱动一轮消费并回线面应答字节，返回 `(应答字节, 是否走慢路径)`
///
/// 慢路径应答经产线冲出口 `RespServerSession::resolve_slow_wait_into` 并入，
/// 与真实网络泵同一写出面（该口同时承担冷上下文暂存的物化 / 弃置闭环）
async fn pump(session: &mut RespServerSession, input: &[u8]) -> (Vec<u8>, bool) {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(input);
  session.bytes_read = input.len();
  session.read_head = 0;
  session.end_read_head = 0;
  session.output.clear();
  let consumed = session.try_consume_messages();
  assert!(consumed.is_some(), "无协议违规");
  // 停车臂驱动（ACL 挂载刷新重驱 + AUTH/HELLO/ACL 族产应答闭环），与泵同构；
  // 慢路径挂起面保留由本 harness 显式驱动（断言挂起发生与物化/弃置方向）。
  // 驱动内部的重入消费已保证整段消费完毕（未停车时首轮即 Some(0)），此处
  // 绝不补跑消费——批进入的 output.clear() 会抹掉未停车命令的应答
  let mut resp_buf = Vec::new();
  wnode_test::drive_pending_parks(session, &mut resp_buf, false).await;

  let mut wire = Vec::new();
  match session.take_slow_wait() {
    Some(slow) => {
      let reply = slow.resolve().await;
      session.resolve_slow_wait_into(&reply, &mut wire);
      (wire, true)
    }
    None => {
      session.take_output_into(&mut wire);
      wire.extend_from_slice(&resp_buf);
      (wire, false)
    }
  }
}

/// 会话绑定的底层 StoreSession 物理域（外层镜像的对齐参照物）
fn physical_domain(api: &StoreGarnetApi<SegmentedDevice>) -> (u64, u64) {
  (api.session.namespace(), api.session.active_db())
}

/// SELECT 冷库装载失败：会话标量与底层物理域严格保持旧库，后续命令安全
/// 操作于旧域——跨库错写的撕裂形态被本用例封死
///
/// 前置 AUTH 落租户 5（ns0 根域恒权威，冷挂起只发生于冷租户域）：AUTH 挂起
/// 装载 (5, 0) 后租户 5 路由非权威、db 3 仍冷，SELECT 3 挂起面可达
#[compio::test]
async fn select_cold_db_load_failure_keeps_scalar_on_old_db() -> aok::Result<()> {
  let (_dir, store, api, mut s) = cold_env().await?;

  // 前置：AUTH 挂起装载租户 5 的 db 0（ns=5，db 仍 0）
  let (auth_wire, auth_slow) = pump(&mut s, &resp_frame(&[b"AUTH", b"5#bob", b"bobpw"])).await;
  assert!(auth_slow, "AUTH 冷租户须挂起");
  assert_eq!(auth_wire, b"+OK\r\n");
  assert_eq!((s.namespace, s.active_db_id), (5, 0));

  // 截断段文件：冷库 db 3 的 DbMeta 点查磁盘读必败（resolve_context → Err）
  OpenOptions::new()
    .write(true)
    .open(store.device.segment_path(0))
    .expect("段文件应已随 store 打开创建")
    .set_len(0)
    .expect("截断段文件注入磁盘故障");

  let (wire, went_slow) = pump(&mut s, &resp_frame(&[b"SELECT", b"3"])).await;
  assert!(
    went_slow,
    "冷库 SELECT 须挂起磁盘点查装载，否则本用例不触达挂起面"
  );
  assert_eq!(wire, err_frame(RESP_ERR_SLOW_PATH_STORAGE));
  assert_eq!(complete_len(&wire), Some(wire.len()));

  // 标量未撕裂：装载失败严禁覆写 active_db_id（修复点：挂起前旧值保持）
  assert_eq!(s.active_db_id, 0, "装载失败后会话活跃库必须保持旧库 0");
  assert_eq!(s.namespace, 5);
  // 底层 StoreSession 物理域同步未动：外层镜像与物理域零分裂
  assert_eq!(physical_domain(&api), (5, 0), "物理域必须保持旧域");
  assert!(store.vdb.is_cold_db(5, 3), "装载失败不产生任何映射装载");

  // 连接续用：后续命令在旧域安全读写（外层视角 db=0 与物理写入域一致）
  let (ok, _) = pump(&mut s, &resp_frame(&[b"SET", b"foo", b"bar"])).await;
  assert_eq!(ok, b"+OK\r\n");
  let (got, _) = pump(&mut s, &resp_frame(&[b"GET", b"foo"])).await;
  assert_eq!(got, b"$3\r\nbar\r\n", "旧域写入必须旧域可读（零跨库错写）");
  Ok(())
}

/// SELECT 冷库装载成功：暂存标量随应答回写物化，物理域与外层镜像对齐——
/// 防过度回滚对照（成功路径的物化能力不因修复而丢失）
#[compio::test]
async fn select_cold_db_load_success_materializes_scalar() -> aok::Result<()> {
  let (_dir, _store, api, mut s) = cold_env().await?;

  // 前置：AUTH 挂起装载租户 5 的 db 0（租户 5 路由非权威，db 3 仍冷）
  let (auth_wire, auth_slow) = pump(&mut s, &resp_frame(&[b"AUTH", b"5#bob", b"bobpw"])).await;
  assert!(auth_slow, "AUTH 冷租户须挂起");
  assert_eq!(auth_wire, b"+OK\r\n");

  let (wire, went_slow) = pump(&mut s, &resp_frame(&[b"SELECT", b"3"])).await;
  assert!(went_slow, "冷库 SELECT 须挂起磁盘点查装载");
  assert_eq!(wire, b"+OK\r\n");

  // 标量物化：挂起前严禁提前覆写，装载确认后必须跟上
  assert_eq!(s.active_db_id, 3, "装载成功后活跃库物化为 3");
  assert_eq!(s.namespace, 5);
  assert_eq!(physical_domain(&api), (5, 3), "物理域与外层镜像对齐");

  // 物理域对齐：装载域既有数据可读、新写可读回（零撕裂）
  let (got, _) = pump(&mut s, &resp_frame(&[b"GET", b"k3"])).await;
  assert_eq!(got, b"$2\r\nv3\r\n", "装载域磁盘既有数据可读");
  let (ok, _) = pump(&mut s, &resp_frame(&[b"SET", b"k3", b"v3x"])).await;
  assert_eq!(ok, b"+OK\r\n");
  let (got, _) = pump(&mut s, &resp_frame(&[b"GET", b"k3"])).await;
  assert_eq!(got, b"$3\r\nv3x\r\n");
  Ok(())
}

/// AUTH 冷租户挂起：认证落位（namespace / 用户句柄）与 HELLO 元数据在装载
/// 确认前严禁提前落位，成功回写时一次物化——多租户隔离防线不因冷挂起失守
#[compio::test]
async fn auth_cold_tenant_park_defers_binding_until_resolve() -> aok::Result<()> {
  let (_dir, _store, api, mut s) = cold_env().await?;

  // HELLO 3 AUTH 5#bob bobpw：租户 5 全冷（db 0 亦冷）→ 认证点查命中后
  // set_context(5, 0) 报告未装载 → 挂起（此臂协议版本与客户端名严禁提前落）
  let (wire, went_slow) = pump(
    &mut s,
    &resp_frame(&[
      b"HELLO",
      b"3",
      b"AUTH",
      b"5#bob",
      b"bobpw",
      b"SETNAME",
      b"cold-conn",
    ]),
  )
  .await;
  assert!(
    went_slow,
    "冷租户 AUTH 须挂起磁盘点查装载，否则本用例不触达认证挂起面"
  );
  // 应答为 HELLO map（认证成功语义，应答恒按目标协议版本 RESP3 组帧）
  assert_eq!(wire.first(), Some(&b'%'), "HELLO 应答须为 RESP3 map 帧");
  assert_eq!(complete_len(&wire), Some(wire.len()));

  // 物化对齐：namespace / 句柄 / 协议版本 / 客户端名随成功应答一并落位
  assert_eq!(s.namespace, 5, "装载成功后命名空间物化为租户 5");
  assert!(s.acl_user_handle.is_some(), "装载成功后用户句柄挂载");
  assert_eq!(s.resp_protocol_version, 3, "协议版本随物化落位");
  assert_eq!(s.client_name.as_deref(), Some("cold-conn"));
  assert_eq!(physical_domain(&api), (5, 0), "物理域与外层镜像对齐");

  // WHOAMI 物化后生效（认证载荷真实可用，非标量镜像单边落位）
  let (who, _) = pump(&mut s, &resp_frame(&[b"ACL", b"WHOAMI"])).await;
  assert_eq!(who, b"$3\r\nbob\r\n");
  Ok(())
}
