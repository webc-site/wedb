//! 迁移源端向量集删除入 AOF 复制链回归（P1：failover 复活幽灵集、主从永久
//! 发散；票 wedb-migrate-source-vector-set-delete-outside-aof-replica-diverge）
//!
//! C# 契约（libs/cluster/Server/Migration/MigrateOperation.cs:DeleteVectorSet
//! 经 BasicGarnetApi.DELETE 落主日志随复制链传播，副本重放经
//! libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnDispose
//! 的 Deleted 臂 → VectorManager.RequestDeletion 同形收敛）：
//! 迁移源端向量集删除必须入 AOF 复制流。修复前 rust 两臂直调登记表摘除旁路
//!（零 replicate、wkv 双域缺席零追加），删除在日志面无痕——源端副本的登记
//! 表与 HNSW 图长期持有已迁走向量集，failover 即复活幽灵集。
//!
//! 判别三点（全真链路：真宿主装配 + 真命令臂 + 真删除通道 + 真日志帧推流 +
//! 真背景重放，无替身、无手工构造条目）：
//! 1. 源端经迁移统一删除通道（storage.delete_string，与 keys.rs /
//!    migrate_session_vector_set.rs 两臂删点同形）删除向量集键：登记表摘除 +
//!    删除以 StoreDelete 墓碑镜像入 AOF（缺席键墓碑镜像单点，wkv collection
//!    delete 的缺席观测钩子臂）；
//! 2. 源端 AOF 全量条目（VADD 合成写 + 删除墓碑）按日志序推流副本，背景
//!    重放先重建登记（store_rmw 向量分支）再经 store_delete String 臂缺席
//!    观测钩子摘除登记（C# OnDispose Deleted 臂对位）；
//! 3. 副本登记表探针判无 + VCARD 判零——源端副本无幽灵集，failover 不复活。

#[path = "common/replica_topology.rs"]
mod replica_topology_core;
use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::runtime::Runtime;
use replica_topology_core::replica_topology_with;
use waof::{AofEntryType, AofHeader, WalFrameHeader};
use wbase::hash_slot::slot_of;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    cluster_replication_session::{AppendLogOutcome, ClusterReplicationSession},
    replica_replay_task::ReplayAssets,
  },
};
use wnode::{
  primary_tasks::PrimaryTasks,
  resp::vector::{
    resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
    vector_manager::VectorManager,
    vector_store_callbacks::ActiveVectorSessionGuard,
  },
  service::{SharedStore, StorageSessionProvider},
  storage::session::storage_session::{StorageSession, vector_registry_delete_hook},
};
use wnode_test::session_factory;
use wtest_base::{test_store_config, wait_for};
use wval::SessionPrefixBuf;

/// 测试节点身份（副本重放握手按主端 id 注册驱动）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A1;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00B2;

/// 默认域槽位（库级定槽，与源端 VADD / 副本探针同槽）
const SLOT0: u16 = slot_of(0, 0);

/// 迁移向量集键（根域用户键）
const VS_KEY: &[u8] = b"vs:migrate-ghost";

/// FP32 向量字节（行主小端，4 维）
const VEC4: &[u8] = &[0u8, 0, 128, 63, 0, 0, 0, 64, 0, 0, 128, 63, 0, 0, 64, 64];

/// 登记缺席观测钩子装配（生产构造单点 engine_swap_hook_bundle 同形；OnceLock
/// 保首重复注入幂等——嵌入式 open 链不挂引擎级钩子，此处显式在位）
fn attach_delete_miss_hook(store: &SharedStore<SegmentedDevice>, vm: &Arc<VectorManager>) {
  let _ = store.set_delete_miss_hook(vector_registry_delete_hook(Arc::clone(vm)));
  assert!(
    store.engine_hook_slots().delete_miss_hook,
    "删除缺席观测钩子必须在位"
  );
}

/// VADD 直调臂建集（真命令臂；绑定守卫同步段持活，与生产 RESP 臂同形）
async fn vadd(store: &SharedStore<SegmentedDevice>, vm: &Arc<VectorManager>) {
  let vsess = RespServerSessionVectors::new(Arc::clone(vm));
  let bind_sess = store.new_session().expect("vadd 绑定会话");
  let _domain = ActiveVectorSessionGuard::bind(&bind_sess);
  let reply = vsess
    .network_vadd(
      SessionPrefixBuf::ROOT.as_slice(),
      &[VS_KEY, b"FP32", VEC4, b"el-live", b"NOQUANT"],
      SLOT0,
      false,
    )
    .await;
  assert!(
    !matches!(reply, VectorReply::Error(_)),
    "VADD 失败: {reply:?}"
  );
}

/// 登记表探针（源端删除与副本收敛共用的判集单点）
fn registry_holds(vm: &Arc<VectorManager>) -> bool {
  vm.read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), VS_KEY)
    .is_some()
}

/// 完整记录帧（8B wal 记录头 + AOF 条目，主端推流帧口径）
fn record_frame(entry: &[u8]) -> Vec<u8> {
  let mut frame = WalFrameHeader::for_payload_parts(&[entry])
    .to_bytes()
    .to_vec();
  frame.extend_from_slice(entry);
  frame
}

#[test]
fn migrated_vector_set_delete_reaches_replica_and_prunes_registry() -> Void {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 源端真宿主：VADD 建集（真命令臂）→ 迁移统一删除通道删键
    let dir_p = tempfile::tempdir().expect("tempdir");
    let host_p = StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir_p.path().join("node").join("src.db"),
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open source host")
    .with_vector_set_preview(true);
    let store_p = host_p.store();
    let vm_p = Arc::clone(&host_p.vector_manager);
    attach_delete_miss_hook(&store_p, &vm_p);
    vadd(&store_p, &vm_p).await;
    assert!(registry_holds(&vm_p), "前置条件：源端向量集已入登记表");

    // 迁移统一删除通道（keys.rs / migrate_session_vector_set.rs 两臂删点同形：
    // storage.delete_string(用户键)，DELETING 门控在上游驱动臂）
    let deleted = {
      let session = store_p.new_session().expect("删除会话");
      let batch = session.enter_batch();
      let storage = StorageSession::new(batch);
      storage.delete_string(VS_KEY).await.expect("源端删除")
    };
    assert!(
      deleted,
      "登记态键删除应答删（缺席观测钩子命中视同删除成功）"
    );
    assert!(!registry_holds(&vm_p), "源端登记表应摘除");

    // 源端 AOF 含删除墓碑镜像（StoreDelete 条目携本键；VADD 段零删除条目）
    let aof_p = host_p.aof().expect("源端 AOF").clone();
    let log_p = aof_p.log().clone();
    let mut entries: Vec<Vec<u8>> = Vec::new();
    log_p.scan_single_with(
      0,
      log_p.get_begin_address(0),
      log_p.get_tail_address(0),
      |r| {
        entries.push(r.payload.clone());
        true
      },
    );
    assert!(
      entries.iter().any(|e| {
        AofHeader::parse(e).is_some_and(|h| h.op_type == AofEntryType::StoreDelete as u8)
      }),
      "删除须以 StoreDelete 墓碑镜像入 AOF 复制流"
    );
    assert!(
      entries.iter().any(|e| {
        AofHeader::parse(e).is_some_and(|h| h.op_type == AofEntryType::StoreDelete as u8)
          && e[AofHeader::TOTAL_SIZE..]
            .windows(VS_KEY.len())
            .any(|w| w == VS_KEY)
      }),
      "StoreDelete 条目应携被删用户键"
    );

    // ===== 副本真宿主：真帧推流（VADD 合成写 + 删除墓碑按日志序）→ 背景重放
    let dir_r = tempfile::tempdir().expect("tempdir");
    let host_r = StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir_r.path().join("node").join("replica.db"),
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open replica host")
    .with_vector_set_preview(true);
    let store_r = host_r.store();
    let vm_r = Arc::clone(&host_r.vector_manager);
    attach_delete_miss_hook(&store_r, &vm_r);

    // 拓扑预置（副本角色 + 主端地址簿；process_append_log 按主端 id 注册驱动）
    let (cp_r, rm_r) =
      replica_topology_with(ClusterProvider::new(), REPLICA_ID, PRIMARY_ID, Some(""));
    cp_r.set_primary_tasks(Arc::new(PrimaryTasks::default()));
    cp_r.set_aof_replay_max_lag_bytes(-1);
    rm_r.set_replay_assets(Some(Arc::new(ReplayAssets::new(
      host_r.aof().expect("副本 AOF").clone(),
      Arc::clone(&store_r),
      None,
      None,
    ))));
    let session_r =
      ClusterReplicationSession::new(cp_r.clone(), host_r.wal().expect("副本 wal").clone(), None);

    // 握手注册重放驱动 + 按日志序推流全部真实条目（VADD 合成写先行重建登记，
    // 删除墓碑殿后摘除——顺序即复制全序）
    session_r
      .process_append_log(PRIMARY_ID, 0, -1, -1, -1, &[])
      .expect("init handshake");
    let wal_r = host_r.wal().expect("副本 wal").clone();
    let mut next = 0i64;
    for entry in &entries {
      let frame = record_frame(entry);
      let current = wal_r.tail_address() as i64;
      next = current + frame.len() as i64;
      let outcome = session_r
        .process_append_log(PRIMARY_ID, 0, current, current, next, &frame)
        .expect("record push");
      assert_eq!(outcome, AppendLogOutcome::Record);
    }
    assert!(
      wait_for(
        || rm_r.get_replication_offset(0) >= next,
        Duration::from_secs(5),
      )
      .await,
      "副本重放位点应追平帧尾（全部条目已应用）"
    );

    // 收敛断言：副本登记重建后被删除墓碑摘除——无幽灵集，failover 不复活
    assert!(
      !registry_holds(&vm_r),
      "副本登记表应随删除墓碑摘除（无幽灵集）"
    );
    let vcard = RespServerSessionVectors::new(Arc::clone(&vm_r))
      .network_vcard(SessionPrefixBuf::ROOT.as_slice(), &[VS_KEY])
      .await;
    assert_eq!(vcard, VectorReply::Integer(0), "副本 VCARD 应判集已清");

    OK
  })
}
