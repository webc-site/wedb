//! RESTORE 快臂对「物理在位、TTL 已过期未清退」残留键假 BUSYKEY 回归
//! （票 zcode-r131c-dumprest 案一，P2）
//!
//! 缺陷面：rust 快臂 network_restore 把 C# 的单次 SET_Conditional(SETEXNX)
//! 条件写拆成「存活探针 → 纯物理 NX 插入」两步异源判据。探针 probe_alive
//! 含 TTL 裁决（已过期键视同不存在回 Some(false)，ttl_sync.rs，物理清除留
//! 写路径惰性清退与后台 GC），插入步 try_insert_sync 却纯物理 NX——链上探得
//! Active 残留记录即假回 -BUSYKEY（持键闩内判死后再无并发竞态可能，唯过期
//! 残留一态）；慢臂同输入经判死 + upsert_string 清退重建出 +OK。同一键
//! SET k v EX 1 自然过期后至后台扫到之前（GC 间隔常态窗口），EXISTS :0、
//! GET nil，RESTORE 却恒 BUSYKEY 且重试不自愈，与 C# 契约分叉：
//! KeyAdminCommands.cs:112-124 存在判定与写入同一次条件写、
//! RMWMethods.cs:974-980 SETEXNX 臂 CheckExpiry 判死转 ExpireAndResume
//! 「复设新值」恒 +OK。
//!
//! 修复形态（票面方案 1）：快臂 try_insert_sync Ok(Ok(false)) 出口不再直写
//! BUSYKEY，改回 Ok(false) 沿既有降级通道交慢臂同窗重探单源裁决——真存活
//! 并发键照常 BUSYKEY（在档锁 key_admin_latch_concurrency.rs 本形零漂移），
//! 过期残留经慢臂 upsert 清退重建 +OK。
//!
//! 测试全真存储真协议帧，无 mock。过期残留态经 put_ttl_sync 裸写过去刻度
//! 确定性构造（先例 probe_alive_single_point.rs /
//! nx_conditional_ttl_degrade_replay.rs，RESP 面无法自然构造「过期未清」态）。

use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wbase::{convert::TICKS_PER_SECOND, crc64::hash as crc64_hash, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  resp::{
    TtlResume,
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
    slow_path::SlowWait,
  },
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wnode_test::roundtrip;
use wresp::{command::RespCommand, length::try_write_length};
use wtest_base::open_test_store;

/// 既有 BUSYKEY 错误帧（cmd_strings::RESP_ERR_BUSSYKEY 成帧，逐字节锁）
const REPLY_BUSYKEY: &[u8] = b"-BUSYKEY Target key name already exists.\r\n";
const REPLY_OK: &[u8] = b"+OK\r\n";
/// RESTORE 带 TTL 秒数（Garnet 口径为秒），PTTL 上界含粗化余量宽窗
const EXPIRE_SECONDS: &[u8] = b"100";
const PTTL_UPPER_MS: i64 = 130_000;

fn consumer_on(store: &Arc<WedbStore<SegmentedDevice>>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// `:N\r\n` 整数回执解析（非整数回 None）
fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

/// `$N\r\n…\r\n` bulk 回执解析（nil / 非 bulk 回 None）
fn reply_bulk(resp: &[u8]) -> Option<Vec<u8>> {
  let body = resp.strip_prefix(b"$")?;
  let nl = body.iter().position(|b| *b == b'\r')?;
  let len: usize = from_utf8(&body[..nl]).ok()?.parse().ok()?;
  let val = body.get(nl + 2..nl + 2 + len)?;
  if body.get(nl + 2 + len..nl + 4 + len)? != b"\r\n" {
    return None;
  }
  Some(val.to_vec())
}

/// 合法 RESTORE 载荷（类型 0x00 + 长度前缀 + 值 + rdb 版本 11 + crc64；crc
/// 含类型字节起算的 rust 口径，deviations §21，与 types.rs 推导单源同构）
fn restore_payload(val: &[u8]) -> Vec<u8> {
  let mut encoded_len = [0u8; 5];
  let written = try_write_length(val.len() as u32, &mut encoded_len).unwrap();
  let mut payload = Vec::with_capacity(1 + written + val.len() + 2 + 8);
  payload.push(0x00);
  payload.extend_from_slice(&encoded_len[..written]);
  payload.extend_from_slice(val);
  payload.extend_from_slice(&11u16.to_le_bytes());
  let crc = crc64_hash(&payload);
  payload.extend_from_slice(&crc);
  payload
}

/// 裸写过去刻度构造「物理在位、TTL 已过期未清退」残留态（RESP 面无法自然
/// 构造过期未清：SET k v EX 1 的惰性清退点位不可控，put_ttl_sync 内核直落
/// 过去 ticks 即确定性残留，先例 probe_alive_single_point.rs:expire_in_past）
fn expire_residual(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  put_ttl_sync(&batch, key, now_ticks() - TICKS_PER_SECOND).unwrap();
}

/// 慢臂直驱（与降级快照投递同径，Full 尾参 = 整命令全量重放，
/// 先例 restore_10byte_payload_slice.rs:slow_restore）
fn slow_restore(rt: &Runtime, api: &GarnetApi, args: &[&[u8]]) -> Vec<u8> {
  let mut snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  snapshot.push(TtlResume::Full.tail_bytes());
  rt.block_on(async {
    SlowWait::for_command(
      api,
      RespCommand::Restore,
      snapshot,
      wconf::DEFAULT_RESP_VERSION,
    )
    .resolve()
    .await
  })
}

/// 案一主锁：过期残留键 RESTORE 快臂恒 +OK（修复前假回 BUSYKEY），
/// 零 TTL 与带 TTL 两形俱收编，终态值与 TTL 正确；未过期已存在键
/// BUSYKEY 形零漂移
#[test]
fn restore_expired_residual_heals_and_alive_busykey_no_drift() {
  let (_dir, store) = open_test_store("restore-expired-residual.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(&store);

  // 前置形态确证：SET 后裸写过去刻度 → 逻辑已死（EXISTS :0、GET nil）而
  // 物理记录 Active 残留（GC 关闭下恒在，见 open_test_store gc off）
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"r1", b"old"]), REPLY_OK);
  expire_residual(&store, b"r1");
  assert_eq!(
    reply_int(&roundtrip(&rt, &mut c, &[b"EXISTS", b"r1"])),
    Some(0),
    "残留键 EXISTS 必先判死（案一前提）"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"r1"]), b"$-1\r\n");

  // 零 TTL 形：修复后 +OK（修复前 -BUSYKEY 假失败且重试恒失败直至 GC 触及）
  let payload = restore_payload(b"new1");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RESTORE", b"r1", b"0", &payload]),
    REPLY_OK,
    "RESTORE 过期残留键应经慢臂单源裁决收编（修复前快臂假回 BUSYKEY）"
  );
  assert_eq!(
    reply_bulk(&roundtrip(&rt, &mut c, &[b"GET", b"r1"])).as_deref(),
    Some(b"new1".as_slice()),
    "RESTORE 后载荷值可读回"
  );
  assert_eq!(
    reply_int(&roundtrip(&rt, &mut c, &[b"PTTL", b"r1"])),
    Some(-1),
    "零 TTL 形：残留旧 TTL 必随重建清退（upsert SET 语义），PTTL -1"
  );

  // 带 TTL 形：+OK 且新 TTL 落库（修复前拒时 TTL 与值俱未落）
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"r2", b"old"]), REPLY_OK);
  expire_residual(&store, b"r2");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RESTORE", b"r2", EXPIRE_SECONDS, &payload]),
    REPLY_OK,
    "RESTORE 带 TTL 对过期残留键亦恒 +OK"
  );
  assert_eq!(
    reply_bulk(&roundtrip(&rt, &mut c, &[b"GET", b"r2"])).as_deref(),
    Some(b"new1".as_slice())
  );
  let pttl = reply_int(&roundtrip(&rt, &mut c, &[b"PTTL", b"r2"])).expect("存活");
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl),
    "RESTORE 带 TTL 终态新 TTL 在场: {pttl}"
  );

  // 未过期已存在键 BUSYKEY 形零漂移（快臂探针判存活直出，不经本出口；
  // 与在档锁 key_admin_latch_concurrency.rs 同款逐字节帧）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RESTORE", b"r1", b"0", &payload]),
    REPLY_BUSYKEY,
    "存活键 RESTORE 恒 BUSYKEY（修复不误伤既有契约）"
  );
  assert_eq!(
    reply_bulk(&roundtrip(&rt, &mut c, &[b"GET", b"r1"])).as_deref(),
    Some(b"new1".as_slice()),
    "BUSYKEY 拒时终态不动"
  );
}

/// 双臂同构锁：慢臂直驱对同一过期残留输入与快臂同终态（+OK 收编），
/// 对存活键维持 BUSYKEY（全量重放形零漂移）
#[test]
fn restore_slow_arm_same_input_same_outcome() {
  let (_dir, store) = open_test_store("restore-expired-residual-slow.db").unwrap();
  let rt = Runtime::new().unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut c = consumer_on(&store);
  let payload = restore_payload(b"slowv");

  // 慢臂对过期残留键：判死 → upsert 清退重建 → +OK（快臂修后同此终态）
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"s1", b"old"]), REPLY_OK);
  expire_residual(&store, b"s1");
  assert_eq!(
    slow_restore(&rt, &api, &[b"s1", EXPIRE_SECONDS, &payload]),
    REPLY_OK,
    "慢臂对过期残留键恒 +OK（C# ExpireAndResume 契约）"
  );
  assert_eq!(
    reply_bulk(&roundtrip(&rt, &mut c, &[b"GET", b"s1"])).as_deref(),
    Some(b"slowv".as_slice())
  );
  let pttl = reply_int(&roundtrip(&rt, &mut c, &[b"PTTL", b"s1"])).expect("存活");
  assert!((1..=PTTL_UPPER_MS).contains(&pttl), "慢臂 TTL 落库: {pttl}");

  // 双臂同输入同终态：残留键 s2 走快臂（降级承接）、s1 已走慢臂，同出 +OK；
  // 存活键全量重放形维持 BUSYKEY
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"s2", b"old"]), REPLY_OK);
  expire_residual(&store, b"s2");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"RESTORE", b"s2", EXPIRE_SECONDS, &payload]),
    REPLY_OK,
    "快臂降级承接后与慢臂直驱同终态（双臂同构）"
  );
  assert_eq!(
    slow_restore(&rt, &api, &[b"s2", b"0", &payload]),
    REPLY_BUSYKEY,
    "慢臂对存活键 BUSYKEY 形零漂移"
  );
}
