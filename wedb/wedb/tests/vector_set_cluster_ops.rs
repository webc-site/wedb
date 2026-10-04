//! 向量集集群深臂集成测试
//!
//! 对标 C# test/cluster/Garnet.test.cluster.vectorsets/VectorSets/
//! ClusterVectorSetTests.cs 四法（副本对账面已由
//! migrate_source_vector_set_replica_converge.rs / diskless_sync_ri_vector.rs
//! 承担，本册聚焦迁移四臂）：
//! 1. RepeatedCreateDeleteAsync → repeated_create_delete_keeps_set_coherent：
//!    反复 DEL + 双 VADD + VSIM 循环，集一致、删空自愈（迭代数 100→30 收窄
//!    控时长，断言面同型）；
//! 2. MigrateVectorSetWhileModifyingAsync → migrate_vector_set_while_modifying：
//!    MIGRATING 窗内 VADD 按 WaitForSlotToStabalize 真实机制挂起（等待体登记、
//!    游标回退零消费），迁移收口后槽位稳定、挂起写恢复执行、目标端续写——
//!    修改横跨迁移边界确定性等价（C# 并发写循环的 rust 无网络线程阻塞形态）；
//! 3. MigrateVectorSetBackAsync → migrate_vector_set_back_no_data_loss：
//!    A→B 迁移续写、B→A 迁回再写，最终属主三段元素齐（对标 VEMB 逐元素
//!    无损断言的登记表 + 基数等价面）；
//! 4. VectorSetMigrationPreservesExpirationAsync →
//!    migrate_vector_set_ttl_family_invariant：迁移前后 TTL 族应答不变式。
//!    C# 修复（cdf966b06 #2179）源于其索引记录驻主存储、键可携 TTL，迁移
//!    丢 expiration 需经迁移载荷携带；rust 向量索引驻登记表第四态（wkv 值
//!    域外，doc/zh/deviations.md §75/§79），向量键 TTL 族写侧 :0 / 读侧 -2
//!    刻意收敛（vector_key_domain_ops.rs 单点锁定）——键本无 expiration，
//!    「迁移丢失 expiration」结构性不存在，本用例锁「迁移前后 TTL 族应答
//!    零变化 + 元素无损」的对拍不变式。
//!
//! 装配沿用 cluster_migration.rs::migrate_vector_set_keys_e2e 既有夹具形态：
//! 双主 provider 拓扑 + 真命令臂消费驱动 + 真 TCP 桥（RESERVE/MIGRATE 交
//! 目标端真消费，握手直答 +OK）。

use std::{str::from_utf8, sync::Arc};

use aok::Void;
use compio::{
  net::TcpListener,
  runtime::{Runtime, spawn},
};
use tempfile::TempDir;
use wbase::hash_slot::{CLUSTER_SLOT_COUNT, slot_of};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::{
  de11_node_id::DE11_NODE_ID, de12_node_id::DE12_NODE_ID, fake_frame_pump::pump_frames,
  resp_drive_scratch::drive, two_primary_provider::two_primary_provider,
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_manager_index::Index,
      vector_store_callbacks::{ActiveVectorSessionGuard, WedbVectorStoreCallbacks},
    },
  },
  storage::session::storage_session::vector_registry_delete_hook,
};
use wtest_base::resp_frame;
use wtxn::{TxnLockTable, WatchVersionMap};
use wval::SessionPrefixBuf;
use wvector::Callbacks;

/// 默认会话 (0,0) 库槽位（库级定槽 doc/zh/db.md 4.1：键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);
/// 远端节点承载的槽位（与 SLOT0 异槽）
const REMOTE_SLOT: u16 = SLOT0 ^ 1;
/// 向量集键（根域用户键）
const VS_KEY: &[u8] = b"vs:cluster-ops";
/// 反复增删轮数（C# 100 轮收窄，断言面同型）
const REPEAT_ROUNDS: u32 = 30;

/// 打开迁移测试专用存储（每用例独立目录，GC 关闭；TempDir 守卫随绑定存活，
/// 用例尾随 store 之后 Drop 清理，不泄漏临时目录）
fn migrate_store(tag: &str) -> (TempDir, Arc<WedbStore<SegmentedDevice>>) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(wtest_base::test_store_config(), device).unwrap());
  (dir, store)
}

/// 构造启用的向量集合管理器（回调绑定独立存储会话，落盘直达给定存储）
fn vector_manager_for(store: &Arc<WedbStore<SegmentedDevice>>) -> Arc<VectorManager> {
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new()));
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..VectorManagerOptions::default()
    },
    callbacks,
  ));
  let _ = store.set_delete_miss_hook(vector_registry_delete_hook(Arc::clone(&vm)));
  vm
}

/// 挂共享存储的集群会话消费者（provider.set_store 与执行域同源，真命令臂）
fn vector_consumer(
  cp: &Arc<ClusterProvider>,
  store: &Arc<WedbStore<SegmentedDevice>>,
  vm: &Arc<VectorManager>,
) -> RespSessionConsumer {
  let cluster_session = cp.create_cluster_session();
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap()).with_vector_manager(Arc::clone(vm))),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}

/// 装配目标端 provider（DE12 视角：本地持全槽、迁移槽置 IMPORTING、
/// DE11@port 地址簿在册——RESERVE/MIGRATE 承接与迁回目标解析同源）
fn importing_target_provider(de11_port: i32) -> Arc<ClusterProvider> {
  let target_cp = ClusterProvider::new();
  let m = target_cp.cluster_manager().unwrap();
  let mut config = m.current_config.write();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: DE12_NODE_ID,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 2,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  config.workers.push(Worker {
    nodeid: Some(DE11_NODE_ID),
    address: "127.0.0.1".into(),
    port: de11_port,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
  for (i, sm) in config.slot_map.iter_mut().enumerate() {
    *sm = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state: if i as u16 == SLOT0 {
        SlotState::Importing
      } else {
        SlotState::Stable
      },
    };
  }
  target_cp
}

/// 把 provider 中指定节点的端口改写为桥端口（MIGRATE 命令入口按集群配置
/// 解析 target_node_id，须与之对齐）
fn retarget_worker_port(cp: &ClusterProvider, node_id: u128, port: i32) {
  let m = cp.cluster_manager().unwrap();
  let mut config = m.current_config.write();
  if let Some(w) = config
    .workers
    .iter_mut()
    .find(|w| w.nodeid == Some(node_id))
  {
    w.port = port;
  }
}

/// 迁移收尾稳态（KEYS 链收口的槽位复位：NODE 交接后槽位脱 IMPORTING/MIGRATING
/// 回 STABLE，向量集写的稳定等待门随之放行）
fn reset_slot_to_stable(cp: &ClusterProvider, slot: u16) {
  cp.cluster_manager()
    .unwrap()
    .try_reset_slot_state(slot as usize);
  assert_eq!(slot_state(cp, slot), SlotState::Stable);
}

/// 直查槽位图状态（可读断言辅助）
fn slot_state(cp: &ClusterProvider, slot: u16) -> SlotState {
  cp.cluster_manager()
    .unwrap()
    .current_config
    .read()
    .get_state(slot)
}

/// 目标端会话驱动：泵消费 + 慢路径收敛（迁移处理为慢路径挂起形态）
async fn drive_target(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = wnode_test::pump(c, frame);
  assert_eq!(consumed, Some(0), "目标端帧应完整消费");
  if let Some(slow) = c.take_slow_wait() {
    out.extend_from_slice(&slow.resolve().await);
  }
  out
}

/// VADD 真命令臂往返（FP32 + NOQUANT，规避量化建表时序）
fn vadd(rt: &Runtime, c: &mut RespSessionConsumer, elem: &[u8], vec4: [f32; 4]) {
  let vec_bytes: Vec<u8> = vec4.iter().flat_map(|f| f.to_le_bytes()).collect();
  let reply = drive(
    rt,
    c,
    &resp_frame(&[b"VADD", VS_KEY, b"FP32", &vec_bytes, elem, b"NOQUANT"]),
  );
  assert_eq!(reply, b":1\r\n", "VADD {elem:?} 应新增成功");
}

/// VSIM 真命令臂往返，解析扁平 bulk 数组应答为元素字节集合
fn vsim(rt: &Runtime, c: &mut RespSessionConsumer, query4: [f32; 4]) -> Vec<Vec<u8>> {
  let query_bytes: Vec<u8> = query4.iter().flat_map(|f| f.to_le_bytes()).collect();
  let reply = drive(
    rt,
    c,
    &resp_frame(&[b"VSIM", VS_KEY, b"FP32", &query_bytes]),
  );
  parse_bulk_array(&reply)
}

/// DEL 真命令臂往返，返回删除计数应答
fn del(rt: &Runtime, c: &mut RespSessionConsumer) -> Vec<u8> {
  drive(rt, c, &resp_frame(&[b"DEL", VS_KEY]))
}

/// 手工 KEYS 迁移真命令臂往返（MIGRATING 前置由调用方排布）
fn migrate_keys(rt: &Runtime, c: &mut RespSessionConsumer, target_port: i32) -> Vec<u8> {
  let port_text = target_port.to_string();
  drive(
    rt,
    c,
    &resp_frame(&[
      b"MIGRATE",
      b"127.0.0.1",
      port_text.as_bytes(),
      b"",
      b"0",
      b"5000",
      b"KEYS",
      VS_KEY,
    ]),
  )
}

/// 解析扁平 bulk 数组应答（`*N\r\n` + N × `$len\r\n<payload>\r\n`）
fn parse_bulk_array(reply: &[u8]) -> Vec<Vec<u8>> {
  assert_eq!(
    reply.first(),
    Some(&b'*'),
    "应答应为数组形: {}",
    String::from_utf8_lossy(reply)
  );
  let header_end = reply
    .iter()
    .position(|b| *b == b'\n')
    .expect("数组头应有行尾")
    + 1;
  let count: usize = from_utf8(&reply[1..header_end - 2])
    .expect("数组头应为数字")
    .parse()
    .expect("数组头应可解析");
  let mut items = Vec::with_capacity(count);
  let mut pos = header_end;
  for _ in 0..count {
    assert_eq!(
      reply.get(pos),
      Some(&b'$'),
      "元素应为 bulk 形，实际应答字节: {reply:?}"
    );
    let len_end = reply[pos + 1..]
      .iter()
      .position(|b| *b == b'\n')
      .expect("bulk 头应有行尾")
      + pos
      + 2;
    let len: usize = from_utf8(&reply[pos + 1..len_end - 2])
      .expect("bulk 头应为数字")
      .parse()
      .expect("bulk 头应可解析");
    items.push(reply[len_end..len_end + len].to_vec());
    pos = len_end + len + 2;
  }
  items
}

/// 桥任务装配：真 TCP 单连接 → RESERVE/MIGRATE 交 `target_consumer` 真消费，
/// 余帧直答 +OK（对标 cluster_migration.rs::migrate_vector_set_keys_e2e 桥形）
async fn spawn_migration_bridge(listener: TcpListener, mut target_consumer: RespSessionConsumer) {
  spawn(async move {
    let (mut stream, _) = listener.accept().await.unwrap();
    pump_frames(&mut stream, 1 << 16, async |_: &[u8], args: &[&[u8]]| {
      let is_cluster = args.len() >= 2 && args[0].eq_ignore_ascii_case(b"CLUSTER");
      let reply: Vec<u8> = if is_cluster
        && (args[1].eq_ignore_ascii_case(b"RESERVE") || args[1].eq_ignore_ascii_case(b"MIGRATE"))
      {
        drive_target(&mut target_consumer, &resp_frame(args)).await
      } else {
        b"+OK\r\n".to_vec()
      };
      Some(reply)
    })
    .await;
  })
  .detach();
}

/// 绑定桥监听口：绑定 + 源端地址簿端口重定向，返回桥端口
async fn bind_bridge(cp: &ClusterProvider, remote_node_id: u128) -> (TcpListener, i32) {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let port = addr.rsplit(':').next().unwrap().parse::<i32>().unwrap();
  retarget_worker_port(cp, remote_node_id, port);
  (listener, port)
}

/// 法一 RepeatedCreateDeleteAsync：反复 DEL + 双 VADD + VSIM 循环，集一致、
/// 删空自愈（登记表随删空摘除、重建幂等）
#[test]
fn repeated_create_delete_keeps_set_coherent() -> Void {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider(
      Some(100),
      CLUSTER_SLOT_COUNT,
      CLUSTER_SLOT_COUNT,
      &[REMOTE_SLOT],
      None,
    );
    let (_dir, store) = migrate_store("vsc_repeat.db");
    let vm = vector_manager_for(&store);
    cp.set_store(Arc::clone(&store));
    cp.set_vector_manager(Arc::clone(&vm));
    let mut consumer = vector_consumer(&cp, &store, &vm);

    // 查询向量与两插入向量近距（VSIM 全量召回两元素）
    let v1 = [1.0, 0.5, 0.25, 0.125];
    let v2 = [0.9, 0.5, 0.25, 0.125];
    let query = [1.0, 0.5, 0.25, 0.2];

    for i in 0..REPEAT_ROUNDS {
      // DEL：首轮无键判 0，此后每轮重建集判 1（C# ClassicAssert 同构）
      let expected_del: &[u8] = if i == 0 { b":0\r\n" } else { b":1\r\n" };
      assert_eq!(
        del(&rt, &mut consumer),
        expected_del,
        "第 {i} 轮 DEL 应答计数不符（删空自愈面）"
      );

      // 元素 id 随轮递增（C# key0/key1 Incr 形态，4 字节小端）
      let key0 = (i + 1).to_le_bytes();
      let key1 = (i + 2).to_le_bytes();
      vadd(&rt, &mut consumer, &key0, v1);
      vadd(&rt, &mut consumer, &key1, v2);

      // VSIM 恰召回两元素且均在册（新集一致面；C# queryPrimary.Length == 2 同构）
      let sim = vsim(&rt, &mut consumer, query);
      assert_eq!(sim.len(), 2, "第 {i} 轮 VSIM 应回召两元素");
      for item in &sim {
        assert!(
          item.as_slice() == key0.as_slice() || item.as_slice() == key1.as_slice(),
          "第 {i} 轮召回元素 {:?} 不属本轮元素集 {{key0, key1}}",
          item
        );
      }
    }

    // 收尾删空自愈：DEL 后登记表摘除（删空键零残迹，重建幂等由循环内证毕）
    assert_eq!(del(&rt, &mut consumer), b":1\r\n", "收尾 DEL 应答删 1");
    assert!(
      vm.read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), VS_KEY)
        .is_none(),
      "删空后向量登记表必须摘除（无幽灵集，failover 不复活）"
    );
    aok::OK
  })
}

/// 法二 MigrateVectorSetWhileModifyingAsync：MIGRATING 窗内向量写按
/// WaitForSlotToStabalize 挂起（零消费 + 等待体），迁移收口后槽位稳定、
/// 挂起写恢复执行，目标端接收后续写
#[test]
fn migrate_vector_set_while_modifying() -> Void {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ── 源端装配（node_1 本地持全槽）──
    let cp = two_primary_provider(
      Some(100),
      CLUSTER_SLOT_COUNT,
      CLUSTER_SLOT_COUNT,
      &[REMOTE_SLOT],
      None,
    );
    let (_dir_a, store_a) = migrate_store("vsc_while_src.db");
    let vm_a = vector_manager_for(&store_a);
    cp.set_store(Arc::clone(&store_a));
    cp.set_vector_manager(Arc::clone(&vm_a));
    let mut consumer_a = vector_consumer(&cp, &store_a, &vm_a);

    // 基线元素入集（槽位 Stable，写直接执行）
    vadd(&rt, &mut consumer_a, b"el_a", [1.0, 0.5, 0.25, 0.125]);

    // ── 目标端装配（DE12 视角：迁移槽 IMPORTING；DE11 地址簿占位待重定向）──
    let target_cp = importing_target_provider(7000);
    let (_dir_b, store_b) = migrate_store("vsc_while_dst.db");
    let vm_b = vector_manager_for(&store_b);
    target_cp.set_store(Arc::clone(&store_b));
    target_cp.set_vector_manager(Arc::clone(&vm_b));
    let consumer_b = vector_consumer(&target_cp, &store_b, &vm_b);

    // ── 桥任务：真 TCP → 目标端真消费 ──
    let (listener, port) = bind_bridge(&cp, DE12_NODE_ID).await;
    spawn_migration_bridge(listener, consumer_b).await;

    // ── MIGRATING 窗内置位 ──
    cp.cluster_manager()
      .unwrap()
      .try_prepare_slot_for_migration(SLOT0 as usize, DE12_NODE_ID)
      .expect("源端迁移前置应成功");
    assert_eq!(slot_state(&cp, SLOT0), SlotState::Migrating);

    // ── 窗内向量写挂起（WaitForSlotToStabalize 对位：等待体登记、游标回退
    //    零消费、无应答；对标 cluster_slot_verify_wait.rs 用例 6 泵形。
    //    独立消费口承载：挂起帧驻留本口缓冲，不干扰迁移命令臂）──
    let mut consumer_hold = vector_consumer(&cp, &store_a, &vm_a);
    let vec_b: Vec<u8> = [0.9f32, 0.5, 0.25, 0.125]
      .iter()
      .flat_map(|f| f.to_le_bytes())
      .collect();
    let vadd_b_frame = resp_frame(&[b"VADD", VS_KEY, b"FP32", &vec_b, b"el_b", b"NOQUANT"]);
    let (consumed, out) = wnode_test::pump(&mut consumer_hold, &vadd_b_frame);
    assert_eq!(
      consumed,
      Some(vadd_b_frame.len()),
      "迁移窗内向量写应挂起回退（字节驻留缓冲零消费）"
    );
    assert!(out.is_empty(), "挂起的向量写不应有应答");
    let slow_vadd = consumer_hold.take_slow_wait().expect("稳定等待体应已登记");

    // ── 挂起等待体在途时真链路迁移（RESERVE + 索引/元素帧带外通道）──
    assert_eq!(
      migrate_keys(&rt, &mut consumer_a, port),
      b"+OK\r\n",
      "迁移应 +OK"
    );
    assert!(
      vm_a
        .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), VS_KEY)
        .is_none(),
      "迁移后源端向量集键应消失"
    );
    // 目标端：基线元素随迁移快照落位
    let index_value = vm_b
      .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), VS_KEY)
      .expect("目标端应有迁移索引");
    let index = Index::from_bytes(&index_value).unwrap();
    assert_eq!(vm_b.service.card(index.context), 1, "目标端应收基线元素");

    // ── 迁移收尾：槽位回稳 → 挂起写恢复执行（源端重放；语义等价 C# 客户端
    //    ASK 重定向后转目标端重试的收场。先例同形：等待体不产出应答字节，
    //    应答由驻留字节重评放行后续消费）──
    reset_slot_to_stable(&cp, SLOT0);
    // 门控等待体收敛（只等待不产出应答，先例同形）
    let _ = slow_vadd.resolve().await;
    // 重评放行 → 驻留帧续解析 → VADD 进向量慢路径挂起 → 收敛执行应答
    let mut out = Vec::new();
    let consumed = consumer_hold.try_consume_messages_into(&mut out);
    assert_eq!(consumed, Some(0), "重评后驻留帧应完整消费");
    let slow_exec = consumer_hold
      .take_slow_wait()
      .expect("重评放行后的向量写应进入慢路径执行体");
    let reply = slow_exec.resolve().await;
    assert_eq!(reply, b":1\r\n", "槽位稳定后挂起的窗内向量写应恢复执行");

    // ── 目标端迁移收口续写（目标端槽位同步回稳）──
    reset_slot_to_stable(&target_cp, SLOT0);
    {
      let mut direct_b = vector_consumer(&target_cp, &store_b, &vm_b);
      vadd(&rt, &mut direct_b, b"el_c", [0.8, 0.5, 0.25, 0.125]);
    }
    assert_eq!(
      vm_b.service.card(index.context),
      2,
      "目标端续写后应基线与续写两元素在册"
    );
    // 直调服务面探针：同步段自持绑定会话（与生产 RESP 臂同形，见
    // diskless_sync_ri_vector.rs 装配口径）
    {
      let bind_sess = store_b.new_session().unwrap();
      let _domain = ActiveVectorSessionGuard::bind(&bind_sess);
      assert!(
        vm_b
          .service
          .check_external_id_valid(index.context, b"el_c")
          .await
          .unwrap(),
        "目标端应含迁移后续写元素 el_c"
      );
    }
    aok::OK
  })
}

/// 法三 MigrateVectorSetBackAsync：A→B 迁移续写、B→A 迁回再写，最终属主
/// 三段元素齐、无数据丢失
#[test]
fn migrate_vector_set_back_no_data_loss() -> Void {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ── 双主装配：node_1 源 / node_2 目标 ──
    let cp_a = two_primary_provider(
      Some(100),
      CLUSTER_SLOT_COUNT,
      CLUSTER_SLOT_COUNT,
      &[REMOTE_SLOT],
      None,
    );
    let (_dir_a, store_a) = migrate_store("vsc_back_a.db");
    let vm_a = vector_manager_for(&store_a);
    cp_a.set_store(Arc::clone(&store_a));
    cp_a.set_vector_manager(Arc::clone(&vm_a));
    let mut consumer_a = vector_consumer(&cp_a, &store_a, &vm_a);

    let cp_b = importing_target_provider(7000);
    let (_dir_b, store_b) = migrate_store("vsc_back_b.db");
    let vm_b = vector_manager_for(&store_b);
    cp_b.set_store(Arc::clone(&store_b));
    cp_b.set_vector_manager(Arc::clone(&vm_b));
    // 桥 1 专用目标消费口（hop-2 迁回时 consumer_b 转任迁移源，桥口须独立）
    let consumer_b_bridge = vector_consumer(&cp_b, &store_b, &vm_b);
    // hop-2 迁回的迁移源消费口（命令臂与桥口各持会话）
    let mut consumer_b = vector_consumer(&cp_b, &store_b, &vm_b);

    // 首段元素入 A
    vadd(&rt, &mut consumer_a, b"el_a", [1.0, 0.5, 0.25, 0.125]);

    // ── 迁移 0：A → B（桥 1 → 目标端 B 真消费）──
    let (listener1, port1) = bind_bridge(&cp_a, DE12_NODE_ID).await;
    spawn_migration_bridge(listener1, consumer_b_bridge).await;
    cp_a
      .cluster_manager()
      .unwrap()
      .try_prepare_slot_for_migration(SLOT0 as usize, DE12_NODE_ID)
      .expect("A→B 迁移前置应成功");
    assert_eq!(
      migrate_keys(&rt, &mut consumer_a, port1),
      b"+OK\r\n",
      "A→B 迁移应 +OK"
    );
    assert!(
      vm_a
        .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), VS_KEY)
        .is_none(),
      "A→B 迁移后源端应摘除"
    );

    // ── 属主 B 侧迁移收口 + 续写（目标端槽位回稳后写放行）──
    reset_slot_to_stable(&cp_b, SLOT0);
    let index_b = vm_b
      .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), VS_KEY)
      .expect("B 端应有迁移索引");
    let index_b = Index::from_bytes(&index_b).unwrap();
    {
      let mut direct_b = vector_consumer(&cp_b, &store_b, &vm_b);
      vadd(&rt, &mut direct_b, b"el_b", [0.9, 0.5, 0.25, 0.125]);
    }
    assert_eq!(vm_b.service.card(index_b.context), 2, "B 端续写后应两元素");

    // ── 迁移 1：B → A 迁回（槽位图互换属主/导入态；桥 2 → 目标端 A 真消费）──
    cp_a
      .cluster_manager()
      .unwrap()
      .current_config
      .write()
      .slot_map[SLOT0 as usize] = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state: SlotState::Importing,
    };
    let (listener2, port2) = bind_bridge(&cp_b, DE11_NODE_ID).await;
    spawn_migration_bridge(listener2, consumer_a).await;
    cp_b
      .cluster_manager()
      .unwrap()
      .try_prepare_slot_for_migration(SLOT0 as usize, DE11_NODE_ID)
      .expect("B→A 迁移前置应成功");
    assert_eq!(slot_state(&cp_b, SLOT0), SlotState::Migrating);
    assert_eq!(
      migrate_keys(&rt, &mut consumer_b, port2),
      b"+OK\r\n",
      "B→A 迁回应 +OK"
    );

    // ── 迁回收口：A 端槽位回稳，两段元素齐，B 端摘除 ──
    reset_slot_to_stable(&cp_a, SLOT0);
    assert!(
      vm_b
        .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), VS_KEY)
        .is_none(),
      "迁回后 B 端应摘除"
    );
    let index_a = vm_a
      .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), VS_KEY)
      .expect("迁回后 A 端应有索引");
    let index_a = Index::from_bytes(&index_a).unwrap();
    assert_eq!(
      vm_a.service.card(index_a.context),
      2,
      "迁回后 A 端应齐收两段元素"
    );
    // 直调服务面探针：同步段自持绑定会话（与生产 RESP 臂同形）
    {
      let bind_sess = store_a.new_session().unwrap();
      let _domain = ActiveVectorSessionGuard::bind(&bind_sess);
      for elem in [b"el_a".as_slice(), b"el_b".as_slice()] {
        assert!(
          vm_a
            .service
            .check_external_id_valid(index_a.context, elem)
            .await
            .unwrap(),
          "迁回后 A 端应含 {elem:?}（无数据丢失）"
        );
      }
    }

    // ── 迁回后属主 A 侧再写（C# 第三段 VADD 对标：所有权易手两轮后仍可写）──
    {
      let mut direct_a = vector_consumer(&cp_a, &store_a, &vm_a);
      vadd(&rt, &mut direct_a, b"el_c", [0.8, 0.5, 0.25, 0.125]);
    }
    assert_eq!(vm_a.service.card(index_a.context), 3, "迁回后再写应三元素");
    aok::OK
  })
}

/// 法四 VectorSetMigrationPreservesExpirationAsync（cdf966b06 #2179 对拍）：
/// 迁移前后 TTL 族应答不变式 + 元素无损
///
/// C# 修复的缺陷根因是其索引记录驻主存储（键可携 TTL），迁移索引键不携带
/// expiration 致目标端丢 TTL；rust 向量索引驻登记表第四态（wkv 值域外，
/// doc/zh/deviations.md §75/§79），向量键 TTL 族写侧 :0 / 读侧 -2 刻意收敛
/// （vector_key_domain_ops.rs::vector_set_ttl_family_converged_with_write_side
/// 单点锁定）——键本无 expiration，「迁移丢失 expiration」结构性不存在：
/// 源端 EXPIRE :0（无 TTL 可挂），迁移后目标端 TTL -2 / EXPIRE :0 同源，
/// TTL 状态零携带零丢失，元素面迁移无损。
#[test]
fn migrate_vector_set_ttl_family_invariant() -> Void {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ── 双主装配：node_1 源 / node_2 目标 ──
    let cp_a = two_primary_provider(
      Some(100),
      CLUSTER_SLOT_COUNT,
      CLUSTER_SLOT_COUNT,
      &[REMOTE_SLOT],
      None,
    );
    let (_dir_a, store_a) = migrate_store("vsc_ttl_a.db");
    let vm_a = vector_manager_for(&store_a);
    cp_a.set_store(Arc::clone(&store_a));
    cp_a.set_vector_manager(Arc::clone(&vm_a));
    let mut consumer_a = vector_consumer(&cp_a, &store_a, &vm_a);

    let target_cp = importing_target_provider(7000);
    let (_dir_b, store_b) = migrate_store("vsc_ttl_b.db");
    let vm_b = vector_manager_for(&store_b);
    target_cp.set_store(Arc::clone(&store_b));
    target_cp.set_vector_manager(Arc::clone(&vm_b));
    let consumer_b = vector_consumer(&target_cp, &store_b, &vm_b);

    // 基线元素入集
    vadd(&rt, &mut consumer_a, b"el_a", [1.0, 0.5, 0.25, 0.125]);

    // TTL 族断言助手（读侧 -2 / 写侧 :0，登记表第四态收敛口径）
    let ttl_family = |rt: &Runtime, c: &mut RespSessionConsumer, tag: &str| {
      for (cmd, expect) in [
        (vec![b"TTL".as_slice(), VS_KEY], &b":-2\r\n"[..]),
        (vec![b"PTTL", VS_KEY], b":-2\r\n"),
        (vec![b"EXPIRE", VS_KEY, b"60"], b":0\r\n"),
        (vec![b"PEXPIRE", VS_KEY, b"60000"], b":0\r\n"),
        (vec![b"PERSIST", VS_KEY], b":0\r\n"),
      ] {
        assert_eq!(
          drive(rt, c, &resp_frame(&cmd)),
          expect,
          "{tag} TTL 族 {cmd:?} 应答不符（登记表第四态收敛口径）"
        );
      }
    };

    // 迁移前：源端 TTL 族收敛（EXPIRE :0 ⇒ 键无 expiration，后续迁移无从丢失）
    ttl_family(&rt, &mut consumer_a, "迁移前源端");

    // ── A → B 迁移（桥 → 目标端真消费）──
    let (listener, port) = bind_bridge(&cp_a, DE12_NODE_ID).await;
    spawn_migration_bridge(listener, consumer_b).await;
    cp_a
      .cluster_manager()
      .unwrap()
      .try_prepare_slot_for_migration(SLOT0 as usize, DE12_NODE_ID)
      .expect("A→B 迁移前置应成功");
    assert_eq!(
      migrate_keys(&rt, &mut consumer_a, port),
      b"+OK\r\n",
      "A→B 迁移应 +OK"
    );
    assert!(
      vm_a
        .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), VS_KEY)
        .is_none(),
      "迁移后源端向量集键应消失"
    );

    // ── 目标端收口：元素无损 + TTL 族应答与源端迁移前零变化 ──
    reset_slot_to_stable(&target_cp, SLOT0);
    let index_b = vm_b
      .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), VS_KEY)
      .expect("目标端应有迁移索引");
    let index_b = Index::from_bytes(&index_b).unwrap();
    assert_eq!(vm_b.service.card(index_b.context), 1, "目标端应收基线元素");
    {
      let bind_sess = store_b.new_session().unwrap();
      let _domain = ActiveVectorSessionGuard::bind(&bind_sess);
      assert!(
        vm_b
          .service
          .check_external_id_valid(index_b.context, b"el_a")
          .await
          .unwrap(),
        "目标端应含迁移元素 el_a（无数据丢失）"
      );
    }
    let mut direct_b = vector_consumer(&target_cp, &store_b, &vm_b);
    ttl_family(&rt, &mut direct_b, "迁移后目标端");

    aok::OK
  })
}
