//! CLUSTER 槽位枚举 RESP 命令族分配失败的单会话错误帧回归（票 zcode-r125c-clucount1）
//!
//! 对位 C# 契约：槽迭代逐键堆物化（ClusterKeyIterationFunctions.cs:67
//! `keys.Add(key.ToArray())`）分配失败抛 `OutOfMemoryException`，被会话主循环
//! `catch (Exception)`（RespServerSession.cs:566）兜住，仅处置当前会话，
//! 进程与其余连接存活。rust 侧裸 `to_vec`+`push` 触顶即 `handle_alloc_error`
//! abort 全进程——本锁验证族内物化点补齐同文件 [`reserve_fail`] 单机制后，
//! RESP 线面收单条 `RESP_ERR_SLOW_PATH_STORAGE` 完整错误帧、本会话续用无
//! 错位、其余连接存活。内核面（StorageSession API 直调）逐臂精确锁见
//! wnode/tests/slot_keys_alloc_fail_smooth.rs。
//!
//! 故障注入与 wnode 侧同法：「大额分配失败」定向分配器（仅本测试二进制），
//! 开关开启后 >= 512KB 的分配返空（严格高于 256KB 日志页读分配，证因唯一点
//! 落物化容器面）。触发面构造对位真实物化臂：
//! DELKEYSINSLOT 以 20000 热键使删除枚举 `Vec<Vec<u8>>` 倍增扩容越阈踩注入
//! 回错误帧；COUNTKEYSINSLOT（Live 臂零分配）与 GETKEYSINSLOT
//! 小额页（逐键小额物化）在注入开启态照常应答——三臂合锁族内不分叉、
//! 平滑机制零误伤。

use std::{
  alloc::{GlobalAlloc, Layout, System},
  ptr,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use compio::runtime::Runtime;
use wbase::hash_slot::{CLUSTER_SLOT_COUNT, slot_of};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::err_frame;
use wresp::cmd_strings::RESP_ERR_SLOW_PATH_STORAGE;
use wtest_base::{resp_frame_str, test_store_config};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 大额分配阈值（与 wnode 侧槽族内核锁同量级）：严格高于 16MB 测试预算
/// 推导的 256KB 日志页容量，冷区页读与小额记录处置照常放行
const THRESHOLD: usize = 512 * 1024;

/// 注入开关（关闭时分配器全通；store 装配与灌键必须在关闭态完成）
static INJECT: AtomicBool = AtomicBool::new(false);

/// 大额失败定向分配器：开关开启后 >= THRESHOLD 的 alloc 返空，其余原样转调
struct FailLarge;

// SAFETY: 开关关闭时逐参转调 System；开启后仅对 >= THRESHOLD 的分配返回空
// 指针（std 容器对该返回的处置即本票平滑收口的触发面），dealloc 逐参转调
unsafe impl GlobalAlloc for FailLarge {
  unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
    if INJECT.load(Ordering::Relaxed) && layout.size() >= THRESHOLD {
      return ptr::null_mut();
    }
    unsafe { System.alloc(layout) }
  }

  unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
    unsafe { System.dealloc(ptr, layout) }
  }
}

#[global_allocator]
static FAIL_LARGE: FailLarge = FailLarge;

use parking_lot::Mutex;

static SERIAL_LOCK: Mutex<()> = Mutex::new(());

/// 注入开关 RAII 兜底：同进程串行执行（非 nextest）时防护残留开关污染后续用例
struct InjectGuard;

impl Drop for InjectGuard {
  fn drop(&mut self) {
    INJECT.store(false, Ordering::Relaxed);
  }
}

/// 默认会话 (0,0) 库槽位（库级定槽：键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);

/// 灌键数：删除枚举容器按每键 24B 记账，16384 满容后下一次倍增扩容请求
/// 32768×24B=786KB 越过 512KB 注入阈值，保证 delete_slot_keys 物化臂必踩注入
const KEY_COUNT: usize = 20000;

/// 全槽本地属主的双主拓扑提供者（槽枚举臂全部走本地慢路径真扫描）
fn local_slots_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap_or_else(|| {
    let m = Arc::new(ClusterManager::new(Arc::clone(&cp)));
    *cp.cluster_manager.write() = Some(Arc::clone(&m));
    m
  });
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    for slot in 0..CLUSTER_SLOT_COUNT {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
  }
  cp
}

/// 挂共享存储的集群会话消费者 + 存储句柄（供关闭注入态灌键）
fn cluster_store_consumer(
  cp: &ClusterProvider,
) -> (RespSessionConsumer, Arc<WedbStore<SegmentedDevice>>) {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("slot_oom.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  (consumer, store)
}

/// 第二连接（其余连接存活断言面）
fn second_consumer(
  cp: &ClusterProvider,
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  RespSessionConsumer::with_cluster(
    2,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 注入关闭态灌 `n` 个热区字符串键（默认 (0,0) 库 → SLOT0）
fn seed_keys(store: &Arc<WedbStore<SegmentedDevice>>, n: usize) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  for i in 0..n {
    let key = format!("k:{i:07}");
    batch
      .try_upsert_sync(key.as_bytes(), b"v")
      .unwrap()
      .unwrap();
  }
}

/// 单命令泵（整帧消费 → 应答取出；慢路径挂起交由 slow_roundtrip 驱动）
fn pump(c: &mut RespSessionConsumer, frame_bytes: &[u8]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(frame_bytes);
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let consumed = c.try_consume_messages_into(&mut out);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame_bytes:?}");
  out
}

/// 慢命令往返（同步段挂起 → block_on 驱动慢路径应答）
fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame_bytes: &[u8]) -> Vec<u8> {
  let mut out = pump(c, frame_bytes);
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// CLUSTER DELKEYSINSLOT：删除枚举物化臂注入下回单条完整存储错误帧，
/// 键集零误删，本会话续用、其余连接存活（C# 单会话 Dispose 对位）
#[test]
fn cluster_delkeysinslot_alloc_fail_degrades_to_error_frame() {
  let _serial = SERIAL_LOCK.lock();
  let rt = Runtime::new().unwrap();
  let cp = local_slots_provider();
  let (mut consumer, store) = cluster_store_consumer(&cp);
  let mut peer = second_consumer(&cp, &store);
  seed_keys(&store, KEY_COUNT);

  let _guard = InjectGuard;
  INJECT.store(true, Ordering::Relaxed);
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT", &SLOT0.to_string()]),
  );
  // 断言前先关注入：失败态 panic 格式化不得再踩注入器（防嵌套分配挂死）
  INJECT.store(false, Ordering::Relaxed);
  assert_eq!(out, err_frame(RESP_ERR_SLOW_PATH_STORAGE));

  // 平滑收口错误帧后本会话续用无错位，其余连接存活
  INJECT.store(false, Ordering::Relaxed);
  assert_eq!(
    pump(&mut consumer, &resp_frame_str(&["PING"])),
    b"+PONG\r\n"
  );
  assert_eq!(pump(&mut peer, &resp_frame_str(&["PING"])), b"+PONG\r\n");

  // 注入面仅失败于枚举物化，未触及删除执行：关注入照常整批删除且计数吻合
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT", &SLOT0.to_string()]),
  );
  assert_eq!(out, b"+OK\r\n");
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", &SLOT0.to_string()]),
  );
  assert_eq!(out, b":0\r\n");
}

/// CLUSTER GETKEYSINSLOT 流式臂注入零误伤锁：逐候选小额物化在注入开启态
/// 照常流式回满额数组帧（完整 bulk 帧形零错位），关注入后面包络一致——
/// 流式单点补齐 try_key_vec 后与全族同轨（≥ 阈值单巨键面因 whlog 记录重组
/// 非平滑轨先触顶，注入器形态下不可安全触达，逐候选失败路径由同函数
/// try_key_vec 的 collect 内核锁承接，见 wnode 侧文件头注记）
#[test]
fn cluster_getkeysinslot_streaming_unaffected_by_inject() {
  let _serial = SERIAL_LOCK.lock();
  let rt = Runtime::new().unwrap();
  let cp = local_slots_provider();
  let (mut consumer, store) = cluster_store_consumer(&cp);
  let mut peer = second_consumer(&cp, &store);
  seed_keys(&store, KEY_COUNT);

  let _guard = InjectGuard;
  INJECT.store(true, Ordering::Relaxed);
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "GETKEYSINSLOT", &SLOT0.to_string(), "3"]),
  );
  // 断言前先关注入：失败态 panic 格式化不得再踩注入器（防嵌套分配挂死）
  INJECT.store(false, Ordering::Relaxed);

  let head = b"*3\r\n$9\r\n";
  assert!(
    out.starts_with(head),
    "小额页须回 3 键数组帧，实测前 16 字节 {:?}",
    &out[..16.min(out.len())]
  );
  // 帧长对账：`*3` 数组头 4B + 3×（bulk 头 4B + 9B 键名 + 尾 2B）= 49B，
  // 逐字节帧形完整零错位
  assert_eq!(
    out.len(),
    4 + 3 * (4 + 9 + 2),
    "三键 bulk 帧长须整 49 字节: {out:?}"
  );
  assert_eq!(
    pump(&mut consumer, &resp_frame_str(&["PING"])),
    b"+PONG\r\n"
  );
  assert_eq!(pump(&mut peer, &resp_frame_str(&["PING"])), b"+PONG\r\n");
}

/// CLUSTER COUNTKEYSINSLOT 零误伤面：热区活键（Live 臂零分配）在注入开启
/// 态照常回 :N——计数面未被无谓预留拖垮，且同会话 GETKEYSINSLOT 小额页
/// 照常应答（族内三臂在注入窗口下同进程共存，杜绝族内双轨）
#[test]
fn cluster_countkeysinslot_hot_keys_unaffected_by_inject() {
  let _serial = SERIAL_LOCK.lock();
  let rt = Runtime::new().unwrap();
  let cp = local_slots_provider();
  let (mut consumer, store) = cluster_store_consumer(&cp);
  let mut peer = second_consumer(&cp, &store);
  seed_keys(&store, KEY_COUNT);

  let _guard = InjectGuard;
  INJECT.store(true, Ordering::Relaxed);
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", &SLOT0.to_string()]),
  );
  assert_eq!(out, format!(":{KEY_COUNT}\r\n").into_bytes());
  assert_eq!(
    pump(&mut consumer, &resp_frame_str(&["PING"])),
    b"+PONG\r\n"
  );
  assert_eq!(pump(&mut peer, &resp_frame_str(&["PING"])), b"+PONG\r\n");
  INJECT.store(false, Ordering::Relaxed);
}

/// 对照组：注入关闭态同一规模 DELKEYSINSLOT 正常 +OK 清槽（证明错误帧根因
/// 即大额分配失败注入，而非枚举链本身缺陷）
#[test]
fn cluster_delkeysinslot_succeeds_without_injection() {
  let _serial = SERIAL_LOCK.lock();
  let rt = Runtime::new().unwrap();
  let cp = local_slots_provider();
  let (mut consumer, store) = cluster_store_consumer(&cp);
  seed_keys(&store, KEY_COUNT);

  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT", &SLOT0.to_string()]),
  );
  assert_eq!(out, b"+OK\r\n");
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", &SLOT0.to_string()]),
  );
  assert_eq!(out, b":0\r\n");
}
