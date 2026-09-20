//! 键存活判定单点回归（fixloop「清理重复逻辑、确保只有一套机制」）
//!
//! 键存活判定单点回归（fixloop「清理重复逻辑、确保只有一套机制」）
//!
//! 同一键态在四个判定面上必须同态，用以证伪「同步档 / 异步档两态语义漂移」
//! 与「内部 API 口另起第二套判定」：
//! 1. EXISTS 快路径（`resp::key_admin_commands::types` 的 `network_exists` →
//!    `ttl_sync::probe_alive_with_registry`，批处理纪元内内存直读三域 + 登记表
//!    第四态）；
//! 2. EXISTS 慢路径（`resp::key_admin_commands::slow` 的 `C::Exists` 臂 →
//!    `ttl_sync::probe_alive_with_registry_async`，异步读口闭环三域 + 同一第四态
//!    判据折叠）；
//! 3. TTL 同步侧（`ttl_sync::probe_alive` 三域单点，TTL / EXPIRE 族与 SET 条件写
//!    共用同一取数面）；
//! 4. 内部 API 口（`StorageSession::exists`，簇侧键可操作判定
//!    `wedb::server::cluster_manager_slot_gate` 的唯一存活面）：本面必须是探针
//!    折叠的薄壳（同步档先行、降级态转异步档收尾），不得自持判定体——旧形态在
//!    此手写三域 `read_tag_with` 串，且 Meta 域用裸 `|_| ()` 判据（死元记录 /
//!    畸形记录误判存活），与 1/2 的 `meta_collection_type_of` 单源口径分叉。
//!
//! 覆盖面：登记表有 / 无、墓碑（DEL 秒删）、已过期键（TTL 到期视同缺失）、
//! 对象信封域键、跨 db 同名键（物理前缀刚性隔离）。
//!
//! 层次差异（本票裁决，判据共用的唯一形态）：1、2、4 只差三域取数通道，
//! 折叠式与第四态判据同源（`ttl_sync::registry_alive` 单点），向量键在三面
//! 一律判存活；3 刻意不接向量登记表第四态（ttl_sync::probe_alive_with_registry
//! 头注），故向量键在 3 判缺失、在 1/2/4 判存活是既定口径，不是漂移。
//!
//! 对标 C#：存活判定唯 UnifiedStore ReadMethods.cs:19-46 `Reader`（叠
//! LogRecordUtils.cs:18-20 `CheckExpiry`）一处；EXISTS
//! （libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:86-96）、
//! TTL/EXPIRETIME（libs/server/API/GarnetApiUnifiedCommands.cs:53-66）与簇侧
//! 判定（libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:17）皆转调
//! 该单点，同步/异步之别只在 Read_UnifiedStore
//! （libs/server/Storage/Session/UnifiedStore/AdvancedOps.cs:12-21）的
//! `IsPending → CompletePending` 收尾。

use std::{path::PathBuf, sync::Arc};

use compio::runtime::Runtime;
use itoa::Buffer;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcol::types::member_ttl::encode_member;
use wconf::DEFAULT_RESP_VERSION;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer, StorageSession,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
    slow_path::SlowWait,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  },
  storage::session::common::ttl_sync::{data_alive_sync_with_prefix, probe_alive, put_ttl_sync},
  types::GarnetStatus,
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wval::{GarnetObjectType, KeyTag, MetaValue, SessionPrefixBuf};
use wvector::Callbacks;

/// 装配带向量登记表的会话消费者（每用例独立临时目录，GC 关闭以保持
/// 墓碑 / 惰性过期形态）
fn env() -> (
  Runtime,
  RespSessionConsumer,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<VectorManager>,
) {
  let dir: PathBuf = tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("probe.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();

  let v_session = Arc::new(store.new_session().unwrap());
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(v_session)));
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    callbacks,
  ));

  let api: GarnetApi = Arc::new(StoreGarnetApi::new(session).with_vector_manager(Arc::clone(&vm)));
  let consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::clone(&api));
  (Runtime::new().unwrap(), consumer, api, store, vm)
}

fn encode_frame(parts: &[&[u8]]) -> Vec<u8> {
  let est = parts.iter().map(|s| s.len() + 16).sum::<usize>() + 16;
  let mut out = Vec::with_capacity(est);
  let mut ibuf = Buffer::new();
  out.push(b'*');
  out.extend_from_slice(ibuf.format(parts.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  for p in parts {
    out.push(b'$');
    out.extend_from_slice(ibuf.format(p.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(p);
    out.extend_from_slice(b"\r\n");
  }
  out
}

fn pump(consumer: &mut RespSessionConsumer, frame: &[&[u8]]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(&encode_frame(frame));
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应被完整消费: {frame:?}");
  resp
}

/// 同步快路径 EXISTS 应答；`None` = 同步段遇磁盘候选整体降级（应答由慢路径出）
fn fast_exists(c: &mut RespSessionConsumer, keys: &[&[u8]]) -> Option<Vec<u8>> {
  let mut parts: Vec<&[u8]> = Vec::with_capacity(keys.len() + 1);
  parts.push(b"EXISTS");
  parts.extend_from_slice(keys);
  let out = pump(c, &parts);
  let degraded = c.take_slow_wait().is_some();
  if degraded {
    assert!(out.is_empty(), "降级臂同步段不得产出应答: {out:?}");
    None
  } else {
    assert!(!out.is_empty(), "快路径必须出应答");
    Some(out)
  }
}

/// EXISTS 慢路径直答（`exec_slow`，与降级快照投递面同径）
fn slow_exists(rt: &Runtime, api: &GarnetApi, keys: &[&[u8]]) -> Vec<u8> {
  rt.block_on(
    SlowWait::for_command(
      api,
      RespCommand::Exists,
      keys.iter().map(|k| k.to_vec()).collect(),
      DEFAULT_RESP_VERSION,
    )
    .resolve(),
  )
}

/// TTL 同步侧三域探针终态（`None` = 磁盘候选须降级）
fn sync_probe(store: &Arc<WedbStore<SegmentedDevice>>, db: u64, key: &[u8]) -> Option<bool> {
  let session = store.new_session().unwrap();
  session.set_active_db(db);
  let batch = session.enter_batch();
  probe_alive(&batch, key).unwrap()
}

/// 内部 API 口 EXISTS 终态（`StorageSession::exists`，簇侧键可操作判定的唯一
/// 存活面）：本面须为探针折叠薄壳，判定结果必须与 EXISTS 两态同字节
fn api_exists(
  rt: &Runtime,
  store: &Arc<WedbStore<SegmentedDevice>>,
  db: u64,
  key: &[u8],
  vector: Option<&VectorManager>,
) -> bool {
  let session = store.new_session().unwrap();
  session.set_active_db(db);
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  rt.block_on(storage.exists(key, vector)).unwrap() == GarnetStatus::Ok
}

/// TTL 同步侧裸写内核：落一个已过期的 TTL（RESP 面无法自然构造「过期未清」态）
fn expire_in_past(store: &Arc<WedbStore<SegmentedDevice>>, db: u64, key: &[u8]) {
  let session = store.new_session().unwrap();
  session.set_active_db(db);
  let batch = session.enter_batch();
  put_ttl_sync(&batch, key, now_ticks() - TICKS_PER_SECOND).unwrap();
}

/// 建向量集键（登记表第四态在场，wkv 三域无记录）
fn vadd(c: &mut RespSessionConsumer, key: &[u8]) {
  let vec3: Vec<u8> = [1.0f32, 2.0, 3.0]
    .iter()
    .flat_map(|v| v.to_le_bytes())
    .collect();
  assert_eq!(
    pump(c, &[b"VADD", key, b"FP32", &vec3, b"elem1"]),
    b":1\r\n",
    "VADD 创建向量集失败"
  );
}

/// 三判定面取齐：快路径（可降级为 None）/ 慢路径 / TTL 同步侧探针
fn verdicts(
  rt: &Runtime,
  c: &mut RespSessionConsumer,
  api: &GarnetApi,
  store: &Arc<WedbStore<SegmentedDevice>>,
  db: u64,
  key: &[u8],
) -> (Option<Vec<u8>>, Vec<u8>, Option<bool>) {
  let fast = fast_exists(c, &[key]);
  let slow = slow_exists(rt, api, &[key]);
  let sync = sync_probe(store, db, key);
  (fast, slow, sync)
}

/// 快慢两态必须逐字节同答；`expect` 为用户可见 EXISTS 应答
fn assert_fast_slow(
  rt: &Runtime,
  c: &mut RespSessionConsumer,
  api: &GarnetApi,
  keys: &[&[u8]],
  expect: &[u8],
) {
  let slow = slow_exists(rt, api, keys);
  assert_eq!(slow, expect, "慢路径 EXISTS 应答漂移: {keys:?}");
  if let Some(fast) = fast_exists(c, keys) {
    assert_eq!(fast, expect, "快路径 EXISTS 与慢路径不同态: {keys:?}");
  }
}

/// 普通键各存活态在四个判定面上的同态回归：命中 / 缺失 / 墓碑 / 已过期 / 信封域
#[test]
fn plain_key_states_agree_across_four_surfaces() {
  let (rt, mut c, api, store, vm) = env();
  let vm_ref = Some(&*vm);
  assert_eq!(pump(&mut c, &[b"SELECT", b"0"]), b"+OK\r\n");

  // 1 命中：EXISTS 1（两态）、探针 Some(true)、TTL 无记录 -1
  assert_eq!(pump(&mut c, &[b"SET", b"p:live", b"v"]), b"+OK\r\n");
  assert_fast_slow(&rt, &mut c, &api, &[b"p:live"], b":1\r\n");
  assert_eq!(sync_probe(&store, 0, b"p:live"), Some(true));
  assert!(api_exists(&rt, &store, 0, b"p:live", vm_ref), "API 口判活");
  assert_eq!(pump(&mut c, &[b"TTL", b"p:live"]), b":-1\r\n");

  // 2 缺失：EXISTS 0（两态）、探针 Some(false)、TTL -2
  assert_fast_slow(&rt, &mut c, &api, &[b"p:missing"], b":0\r\n");
  assert_eq!(sync_probe(&store, 0, b"p:missing"), Some(false));
  assert!(
    !api_exists(&rt, &store, 0, b"p:missing", vm_ref),
    "API 口判缺失"
  );
  assert_eq!(pump(&mut c, &[b"TTL", b"p:missing"]), b":-2\r\n");

  // 3 墓碑（DEL 秒删）：两态判 0、探针 Some(false)
  assert_eq!(pump(&mut c, &[b"SET", b"p:dead", b"v"]), b"+OK\r\n");
  assert_eq!(pump(&mut c, &[b"DEL", b"p:dead"]), b":1\r\n");
  assert_fast_slow(&rt, &mut c, &api, &[b"p:dead"], b":0\r\n");
  assert_eq!(sync_probe(&store, 0, b"p:dead"), Some(false));
  assert!(
    !api_exists(&rt, &store, 0, b"p:dead", vm_ref),
    "API 口不得见墓碑"
  );

  // 4 已过期（数据在场、TTL 已到期）：两态判 0、探针 Some(false)、TTL -2
  //（登记表不参与，判据完全由三域 + TTL 门裁决）
  assert_eq!(pump(&mut c, &[b"SET", b"p:expired", b"v"]), b"+OK\r\n");
  expire_in_past(&store, 0, b"p:expired");
  assert_fast_slow(&rt, &mut c, &api, &[b"p:expired"], b":0\r\n");
  assert_eq!(sync_probe(&store, 0, b"p:expired"), Some(false));
  assert!(
    !api_exists(&rt, &store, 0, b"p:expired", vm_ref),
    "API 口到期须视同缺失"
  );
  assert_eq!(pump(&mut c, &[b"TTL", b"p:expired"]), b":-2\r\n");

  // 5 信封域键（集合对象键，String 域缺失）：两态判 1、探针 Some(true)
  assert_eq!(
    pump(&mut c, &[b"HSET", b"p:obj", b"f", b"v"]),
    b":1\r\n",
    "HSET 建信封域键失败"
  );
  assert_fast_slow(&rt, &mut c, &api, &[b"p:obj"], b":1\r\n");
  assert_eq!(sync_probe(&store, 0, b"p:obj"), Some(true));
  assert!(
    api_exists(&rt, &store, 0, b"p:obj", vm_ref),
    "API 口须见信封域键"
  );
  assert_eq!(pump(&mut c, &[b"TTL", b"p:obj"]), b":-1\r\n");

  // 多键混合帧：逐键累加后单次应答，两态同字节
  assert_fast_slow(
    &rt,
    &mut c,
    &api,
    &[b"p:live", b"p:missing", b"p:expired", b"p:obj", b"p:live"],
    b":3\r\n",
  );
}

/// 向量登记表第四态在三态 EXISTS 的同态 + 跨 db 隔离；TTL 同步侧刻意不接
/// 第四态（既定口径，非漂移）
#[test]
fn registry_fourth_state_agrees_across_sites_and_dbs() {
  let (rt, mut c, api, store, vm) = env();
  let db0 = SessionPrefixBuf::new(0, 0);
  let db1 = SessionPrefixBuf::new(0, 1);
  let vm_ref = Some(&*vm);

  // db0 向量键：登记表有、三域无 —— 三态 EXISTS 均判 1（第四态折叠同源）
  assert_eq!(pump(&mut c, &[b"SELECT", b"0"]), b"+OK\r\n");
  vadd(&mut c, b"r:vs");
  assert!(
    vm.read_stored_index(db0.as_slice(), b"r:vs").is_some(),
    "前置自检：db0 登记表应在场"
  );
  let (fast, slow, sync) = verdicts(&rt, &mut c, &api, &store, 0, b"r:vs");
  assert_eq!(slow, b":1\r\n", "慢路径对向量键必须判活（第四态）");
  assert_eq!(
    fast,
    Some(b":1\r\n".to_vec()),
    "快路径对向量键必须判活（第四态）"
  );
  assert_eq!(
    sync,
    Some(false),
    "TTL 同步侧刻意不接登记表第四态（向量键 TTL -2 与 EXPIRE :0 同源）"
  );
  assert!(
    api_exists(&rt, &store, 0, b"r:vs", vm_ref),
    "API 口（簇侧门评面）对向量键必须判活：C# ClusterSlotVerify 转调的 Exists
     看得见主存向量记录，漏判即迁移门误发 ASK"
  );
  assert_eq!(pump(&mut c, &[b"TTL", b"r:vs"]), b":-2\r\n");

  // 跨 db：同名向量键在 db1 尚未登记 → 各态均判 0（前缀刚性隔离，第四态
  // 判据取同一外提前缀，不得越库命中）
  assert_eq!(pump(&mut c, &[b"SELECT", b"1"]), b"+OK\r\n");
  assert_fast_slow(&rt, &mut c, &api, &[b"r:vs"], b":0\r\n");
  assert_eq!(sync_probe(&store, 1, b"r:vs"), Some(false));
  assert!(
    !api_exists(&rt, &store, 1, b"r:vs", vm_ref),
    "API 口不得越库命中"
  );

  // db1 建同名向量键后各态各自判 1，db0 不受影响
  vadd(&mut c, b"r:vs");
  assert_fast_slow(&rt, &mut c, &api, &[b"r:vs"], b":1\r\n");
  assert!(
    vm.read_stored_index(db1.as_slice(), b"r:vs").is_some()
      && vm.read_stored_index(db0.as_slice(), b"r:vs").is_some(),
    "两域各自登记"
  );
  assert_eq!(pump(&mut c, &[b"SELECT", b"0"]), b"+OK\r\n");
  assert_fast_slow(&rt, &mut c, &api, &[b"r:vs"], b":1\r\n");
  assert!(
    api_exists(&rt, &store, 0, b"r:vs", vm_ref),
    "API 口 db0 同名向量键判活"
  );

  // 混合帧：普通键 + 向量键 + 缺失键，两态同字节
  assert_eq!(pump(&mut c, &[b"SET", b"r:str", b"v"]), b"+OK\r\n");
  assert_fast_slow(
    &rt,
    &mut c,
    &api,
    &[b"r:str", b"r:vs", b"r:none", b"r:vs"],
    b":3\r\n",
  );

  // DEL 向量键：登记项随键清退，各态同判 0（不得留幽灵第四态）
  assert_eq!(pump(&mut c, &[b"DEL", b"r:vs"]), b":1\r\n");
  assert!(
    vm.read_stored_index(db0.as_slice(), b"r:vs").is_none(),
    "DEL 后 db0 登记须清退"
  );
  assert_eq!(
    vm.registry_domain_count(db0.as_slice()),
    0,
    "db0 登记域须整域清空"
  );
  assert_fast_slow(&rt, &mut c, &api, &[b"r:vs", b"r:str"], b":1\r\n");
  assert!(
    !api_exists(&rt, &store, 0, b"r:vs", vm_ref),
    "DEL 后 API 口不得留幽灵态"
  );
}

/// 冷化落盘（快路径整体降级）后，慢路径与内部 API 口裁决必须与降级前快路径
/// 逐键一致——直接证伪「两条取数通道判出两种终态」
#[test]
fn cold_evicted_keys_keep_same_verdict_through_slow_arm() {
  let (rt, mut c, api, store, vm) = env();
  assert_eq!(pump(&mut c, &[b"SELECT", b"0"]), b"+OK\r\n");

  let keys: &[&[u8]] = &[b"c:live", b"c:missing", b"c:tomb", b"c:expired", b"c:vs"];
  assert_eq!(pump(&mut c, &[b"SET", b"c:live", b"v"]), b"+OK\r\n");
  assert_eq!(pump(&mut c, &[b"SET", b"c:tomb", b"v"]), b"+OK\r\n");
  assert_eq!(pump(&mut c, &[b"DEL", b"c:tomb"]), b":1\r\n");
  assert_eq!(pump(&mut c, &[b"SET", b"c:expired", b"v"]), b"+OK\r\n");
  expire_in_past(&store, 0, b"c:expired");
  vadd(&mut c, b"c:vs");

  // 冷化前：全部由同步快路径应答（无降级）
  let mut before = Vec::new();
  for k in keys {
    let out = fast_exists(&mut c, &[*k]).unwrap_or_else(|| panic!("{:?} 冷化前必须快路径应答", *k));
    before.push(out);
  }
  assert_eq!(
    before,
    vec![
      b":1\r\n".to_vec(),
      b":0\r\n".to_vec(),
      b":0\r\n".to_vec(),
      b":0\r\n".to_vec(),
      b":1\r\n".to_vec()
    ],
    "冷化前快路径逐键基线"
  );

  // 冷化：内存记录落盘为磁盘候选
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 冷化后先取证「同步档确实判不了」：内存直读遇磁盘候选整体降级（异步收尾
  // 臂真在承责），TTL 同步侧同判降级态；本步必须在慢路径读之前——异步读口对
  // 过期键惰性清退后键即物理消失，届时快路径也能自行出应答，取证失效
  assert!(
    fast_exists(&mut c, &[b"c:live"]).is_none(),
    "冷化后 c:live 快路径须降级异步，不得内存盲判"
  );
  assert!(
    fast_exists(&mut c, &[b"c:expired"]).is_none(),
    "冷化后 c:expired 快路径须降级异步"
  );
  assert_eq!(
    sync_probe(&store, 0, b"c:live"),
    None,
    "TTL 同步侧对磁盘候选须回降级态"
  );

  // 冷化后：慢路径逐键裁决与基线逐字节一致（多键帧整体比对）
  for (k, expect) in keys.iter().zip(&before) {
    let slow = slow_exists(&rt, &api, &[*k]);
    assert_eq!(&slow, expect, "冷化后慢路径对 {:?} 判出不同终态", *k);
  }
  let mixed_slow = slow_exists(&rt, &api, keys);
  assert_eq!(mixed_slow, b":2\r\n", "冷化后多键慢路径计数须等同比对基线");

  // 内部 API 口（StorageSession::exists，簇侧门评唯一存活面）逐键同终态：
  // 本面降级后走异步档收尾，取数通道换、判据不换
  for (k, expect) in keys.iter().zip(&before) {
    let alive = api_exists(&rt, &store, 0, k, Some(&*vm));
    assert_eq!(
      alive,
      *expect == b":1\r\n",
      "冷化后 API 口对 {:?} 判出不同终态（期望 {:?}）",
      *k,
      *expect
    );
  }

  // 快路径若自行出应答（登记表态、无磁盘候选态），必须与慢路径基线同字节
  for (k, expect) in keys.iter().zip(&before) {
    if let Some(fast) = fast_exists(&mut c, &[*k]) {
      assert_eq!(&fast, expect, "快路径降级后与慢路径判定漂移: {:?}", *k);
    }
  }
}

/// 慢路径直驱命令（分层态写臂与 TTL 族降级态经同一异步收尾臂出应答）
fn exec(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  rt.block_on(
    SlowWait::for_command(
      api,
      cmd,
      args.iter().map(|k| k.to_vec()).collect(),
      DEFAULT_RESP_VERSION,
    )
    .resolve(),
  )
}

/// Meta 域裸在位探针（旧 `StorageSession::exists` 降级臂的 `|_| ()` 口径）
///
/// 本测试仅用它取证「记录确实驻留主存」，不作任何判定——生产存活判据恒走
/// [`probe_alive_domain_with_prefix`] 单点（Meta 域叠 `meta_collection_type_of`
/// 的 `is_live` 门），本口径在产线零消费者
fn meta_present_in_memory(
  store: &Arc<WedbStore<SegmentedDevice>>,
  db: u64,
  key: &[u8],
) -> Option<bool> {
  let session = store.new_session().unwrap();
  session.set_active_db(db);
  let batch = session.enter_batch();
  let prefix = batch.session_prefix();
  data_alive_sync_with_prefix(&batch, prefix.as_slice(), key, KeyTag::Meta).unwrap()
}

/// 法一回归：size == 0 且非 RangeIndex 的死元记录在所有判定面一律判缺失
///
/// 该态可直编构造（`MetaValue::to_bytes`）且取证为「确实驻留主存」（裸在位判据
/// 回 `Some(true)`），正是旧 `exists` 降级臂 Meta 域用 `|_| ()` 时误判存活的输入
/// ——删臂后本面对称：EXISTS 快慢两态 / 内部 API 口 / TTL 同步侧探针同判缺失，
/// TYPE 回 none、TTL 回 -2（与 `meta_collection_type_of` 的 is_live 门同答复）。
/// 冷化（同步档回磁盘候选、整体降级异步档收尾）后同一批判据必须仍同答复——
/// 异步档收尾臂正是被删降级臂的位置，此处钉死它不再自持判定体。
/// 阳性对照：同法写一条 size > 0 元记录，各面须判活且 TYPE 回集合类型名，
/// 证明上述「判缺失」来自 is_live 门而非探针盲视
#[test]
fn dead_meta_record_answers_missing_across_all_surfaces() {
  let (rt, mut c, api, store, vm) = env();
  let vm_ref = Some(&*vm);
  assert_eq!(pump(&mut c, &[b"SELECT", b"0"]), b"+OK\r\n");

  let session = store.new_session().unwrap();
  session.set_active_db(0);

  // 1 死元记录（Hash 型、size=0）直写 KeyTag::Meta
  let dead = MetaValue::new(7, GarnetObjectType::Hash, 0).to_bytes();
  rt.block_on(session.upsert_tag(b"d:dead", KeyTag::Meta, &dead))
    .unwrap();
  assert_eq!(
    meta_present_in_memory(&store, 0, b"d:dead"),
    Some(true),
    "前置取证：死元记录须驻留主存（否则本用例构不出旧降级臂的输入态）"
  );
  // 2 各判定面同判缺失
  assert_eq!(
    sync_probe(&store, 0, b"d:dead"),
    Some(false),
    "三域单点探针须判缺失（is_live 门生效）"
  );
  assert_fast_slow(&rt, &mut c, &api, &[b"d:dead"], b":0\r\n");
  assert!(
    !api_exists(&rt, &store, 0, b"d:dead", vm_ref),
    "内部 API 口（簇侧门评面）对死元记录不得判活"
  );
  assert_eq!(pump(&mut c, &[b"TYPE", b"d:dead"]), b"+none\r\n");
  assert_eq!(pump(&mut c, &[b"TTL", b"d:dead"]), b":-2\r\n");

  // 3 阳性对照：size > 0 元记录判活
  let live = MetaValue::new(7, GarnetObjectType::Hash, 3).to_bytes();
  rt.block_on(session.upsert_tag(b"d:live", KeyTag::Meta, &live))
    .unwrap();
  assert_eq!(sync_probe(&store, 0, b"d:live"), Some(true));
  assert_fast_slow(&rt, &mut c, &api, &[b"d:live"], b":1\r\n");
  assert!(api_exists(&rt, &store, 0, b"d:live", vm_ref));
  // PTTL 走 wkv `pttl_ms` → `contains_key`（同一 is_live 单点）；本态刻意取
  // PTTL 而非 TTL：TTL 慢路径的秒换算把 -2 哨兵折成 -1（div_euclid 病灶，
  // 另票登记），不影响本面判据取证
  assert_eq!(
    exec(&rt, &api, RespCommand::Pttl, &[b"d:dead"]),
    b":-2\r\n",
    "冷化后 PTTL 与 EXISTS 同判缺失"
  );
  assert_eq!(
    exec(&rt, &api, RespCommand::Pttl, &[b"d:live"]),
    b":-1\r\n",
    "冷化后 PTTL 对存活元记录判无过期"
  );
  assert_eq!(pump(&mut c, &[b"TYPE", b"d:live"]), b"+hash\r\n");
  assert_eq!(pump(&mut c, &[b"TTL", b"d:live"]), b":-1\r\n");

  // 4 冷化：同步档整体降级（磁盘候选），异步档收尾臂必须同判终态
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    meta_present_in_memory(&store, 0, b"d:dead"),
    None,
    "冷化后同步档对死元记录须回磁盘候选（异步收尾臂真在承责）"
  );
  assert!(
    fast_exists(&mut c, &[b"d:dead"]).is_none(),
    "冷化后 EXISTS 快路径须整体降级，不得内存盲判"
  );
  assert_eq!(
    slow_exists(&rt, &api, &[b"d:dead", b"d:live"]),
    b":1\r\n",
    "冷化后 EXISTS 慢路径计数须与常驻态同比对（死元记录仍判缺失）"
  );
  assert!(
    !api_exists(&rt, &store, 0, b"d:dead", vm_ref),
    "冷化后内部 API 口经异步档收尾仍不得判活"
  );
  assert!(api_exists(&rt, &store, 0, b"d:live", vm_ref));
}

/// 法二实测：严格空删不留 size == 0 元记录（信封态与分层态两条业务链）
///
/// 结论（原始输出见 task 方案文档）：新建集合 → 删空最后一个成员后，
/// String / ObjectEnvelope / Meta 三域裸在位判据全部确认缺失，无「元记录驻留」
/// 窗口，各判定面同判缺失——正常命令序列构造不出法一的死元记录，故本票不主张
/// 现网行为缺陷。本用例锁死该事实：若日后空删改为「先写 size=0 再清退」留下
/// 可观测窗口，三域探针与 EXISTS / TYPE / TTL 也必须继续同答复
#[test]
fn strict_empty_delete_leaves_no_resident_meta_record() {
  let (rt, mut c, api, store, vm) = env();
  assert_eq!(pump(&mut c, &[b"SELECT", b"0"]), b"+OK\r\n");

  // (a) 信封态小集合：HSET 一成员 → HDEL 删空
  assert_eq!(
    exec(&rt, &api, RespCommand::Hset, &[b"s:env", b"f", b"v"]),
    b":1\r\n"
  );
  assert_eq!(
    exec(&rt, &api, RespCommand::Hdel, &[b"s:env", b"f"]),
    b":1\r\n"
  );
  assert_eq!(
    meta_present_in_memory(&store, 0, b"s:env"),
    Some(false),
    "信封态空删后 Meta 域不得留驻记录"
  );
  assert_eq!(sync_probe(&store, 0, b"s:env"), Some(false));
  assert_fast_slow(&rt, &mut c, &api, &[b"s:env"], b":0\r\n");
  assert!(
    !api_exists(&rt, &store, 0, b"s:env", Some(&*vm)),
    "内部 API 口不得见空删残留"
  );
  assert_eq!(pump(&mut c, &[b"TYPE", b"s:env"]), b"+none\r\n");
  assert_eq!(pump(&mut c, &[b"TTL", b"s:env"]), b":-2\r\n");

  // (b) 分层态集合：手工升阶一成员（Meta 域在册、size=1 判活）→ HDEL 删空
  let sess = store.new_session().unwrap();
  sess.set_active_db(0);
  rt.block_on(sess.promote_collection_to_bftree(
    b"s:tier",
    GarnetObjectType::Hash,
    vec![(b"f".to_vec(), encode_member(b"v", None))],
    i64::MAX,
    false,
  ))
  .unwrap();
  assert_eq!(
    meta_present_in_memory(&store, 0, b"s:tier"),
    Some(true),
    "前置取证：升阶元记录驻留主存"
  );
  assert_fast_slow(&rt, &mut c, &api, &[b"s:tier"], b":1\r\n");
  assert_eq!(pump(&mut c, &[b"TYPE", b"s:tier"]), b"+hash\r\n");

  assert_eq!(
    exec(&rt, &api, RespCommand::Hdel, &[b"s:tier", b"f"]),
    b":1\r\n",
    "分层态 HDEL 删空最后一个成员"
  );
  assert_eq!(
    meta_present_in_memory(&store, 0, b"s:tier"),
    Some(false),
    "分层态严格空删后不得留下 size==0 驻留元记录（自愈清退契约）"
  );
  assert_eq!(sync_probe(&store, 0, b"s:tier"), Some(false));
  assert_fast_slow(&rt, &mut c, &api, &[b"s:tier"], b":0\r\n");
  assert!(
    !api_exists(&rt, &store, 0, b"s:tier", Some(&*vm)),
    "内部 API 口不得见分层空删残留"
  );
  assert_eq!(pump(&mut c, &[b"TYPE", b"s:tier"]), b"+none\r\n");
  assert_eq!(pump(&mut c, &[b"TTL", b"s:tier"]), b":-2\r\n");
  assert_eq!(exec(&rt, &api, RespCommand::Hlen, &[b"s:tier"]), b":0\r\n");
}
