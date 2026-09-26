//! 副本磁盘基全量同步换引擎后向量登记回建 + 残影镜像清退回归
//!（工单 zcode-r137c-snaplock2 宗二锁）
//!
//! C# 契约（libs/server/Databases/SingleDatabaseManager.cs:406 RecoverVectorSets
//! 于初始化与 --recover 两臂点亮；副本 disk-based 原位恢复
//! ReplicaDiskbasedSync.cs 无回建臂系 N.A.——登记表记录存主存，整表随
//! store 重置与引擎天然连续）：rust 引擎实例置换形态下，副本全量收口换入
//! 新引擎后，内存向量登记表既无新引擎登记条目（主端向量集的旁路记录已随
//! 导入文件集入新引擎日志）也无旧集清退通道——修复前双缺口：
//! 1. 换引擎后不调 recover_vector_sets → 主端向量集在副本内存面永不显形
//!    （VADD 复用即与主端 context 编号冲突写透回环、VINFO/VCARD 答不存在）；
//! 2. 副本同步前本地旧集的内存镜像残留 → 指向已弃置物理实例的虚体登记
//!    （DiskANN 原生句柄悬挂）。
//!
//! 判别四点（全真链路：真帧接收臂 SNAPSHOT_DATA / BEGIN_REPLICA_RECOVER 直投
//! 会话 + 真设备落盘 + 宿主真装配，无替身、无 sleep、无环境门）：
//! 1. 全量收口成功（BEGIN_REPLICA_RECOVER 授予位点应答；回建失败即本轮
//!    全量收口失败回 -ERR）且引擎置换；
//! 2. 主端带元素向量集经 recover_vector_sets 入副本内存登记表，VCARD 非零
//!    答（元素随导入日志可达，对标 vector_registry_recovery.rs 启动面
//!    回建后 VSIM 命中同型判据）；
//! 3. 副本同步前旧集内存镜像经 rebuild 残影清退臂清零（同步执行于回建
//!    收口内，收口返回即确定性可见，无清理竞态窗口）；
//! 4. 旧引擎本地残留键在新引擎视图不可见（引擎面换新）。

#[path = "common/cluster_consumer.rs"]
mod cluster_cc;

use std::{
  fs,
  path::{Path, PathBuf},
  sync::Arc,
};

use cluster_cc::cluster_consumer;
use compio::runtime::Runtime;
use waof::{AofAddress, WalLog};
use wbase::hash_slot::slot_of;
use wconf::RuntimeServerOptions;
use wcpr::{CheckpointMeta, CheckpointType};
use wdev::{Device, SegmentedDevice};
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::{
    checkpoint_entry::{CheckpointEntry, CheckpointFileType, CheckpointMetadata},
    recovery_status::RecoveryStatus,
  },
  worker::{LocalWorkerSpec, NodeRole},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  database::checkpoint_version,
  resp::vector::{
    resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
    vector_manager::VectorManager,
    vector_store_callbacks::ActiveVectorSessionGuard,
  },
  service::StorageSessionProvider,
  storage::session::storage_session::StorageSession,
};
use wnode_test::session_factory;
use wtest_base::{resp_frame, test_store_config};
use wval::SessionPrefixBuf;

/// 测试节点身份
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A1;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00B2;

/// 快照分块尺寸（扇区整数倍，与主端发送面切分同构）
const CHUNK: usize = 1 << 17;

/// VADD 直调臂槽位门基线（与 diskless 同步夹具同形）
const SLOT0: u16 = slot_of(0, 0);

/// 单槽位点 span（C# AofAddress.Span：8B LE）
fn aof_span(address: i64) -> Vec<u8> {
  address.to_le_bytes().to_vec()
}

/// 写 string 键
async fn put_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8], value: &[u8]) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage.upsert_string(key, value).await.unwrap();
}

/// 读 string 键
async fn read_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage.read_string(key).await.unwrap()
}

/// 向量集直调臂建集（多元素；绑定守卫同步段持活，写面与登记写透均落
/// 本 store——与生产 RESP 臂 ActiveVectorSessionGuard 绑定同形）
async fn vadd(
  store: &Arc<WedbStore<SegmentedDevice>>,
  vm: &Arc<VectorManager>,
  key: &[u8],
  elements: &[&[u8]],
) {
  let vsess = RespServerSessionVectors::new(Arc::clone(vm));
  let bind_sess = store.new_session().expect("vadd 绑定会话");
  let _domain = ActiveVectorSessionGuard::bind(&bind_sess);
  for element in elements {
    let reply = vsess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          key, b"VALUES", b"4", b"1.5", b"-2.5", b"0.25", b"4.0", element,
        ],
        SLOT0,
        false,
      )
      .await;
    assert!(!matches!(reply, VectorReply::Error(_)), "VADD 失败");
  }
}

/// 带角色 provider（对齐宿主装配：rm/store/wal/checkpoint_dir 全接线）
fn wired_provider(
  store: &Arc<WedbStore<SegmentedDevice>>,
  checkpoint_dir: PathBuf,
  wal: &Arc<WalLog<SegmentedDevice>>,
  node_id: u128,
  port: i32,
  role: NodeRole,
) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port,
    config_epoch: 1,
    role,
    replica_of_node_id: if role == NodeRole::Replica {
      Some(PRIMARY_ID)
    } else {
      None
    },
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);
  provider.set_store(Arc::clone(store));
  provider.set_checkpoint_dir(checkpoint_dir);
  provider.set_wal(Arc::clone(wal));
  provider
}

/// 慢命令往返：同步段消费，挂起慢路径时 block_on 驱动应答
fn drive(rt: &Runtime, c: &mut RespSessionConsumer, frame_bytes: &[u8]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(frame_bytes);
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let remaining = c.try_consume_messages_into(&mut out);
  assert_eq!(remaining, Some(0), "帧应被完整消费");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}

/// SNAPSHOT_DATA 单帧发射
fn snapshot_data(
  token: u128,
  file_type: CheckpointFileType,
  start_address: i64,
  data: &[u8],
) -> Vec<u8> {
  let type_str = (file_type as i64).to_string();
  let start_str = start_address.to_string();
  resp_frame(&[
    b"CLUSTER",
    b"SNAPSHOT_DATA",
    &token.to_le_bytes(),
    type_str.as_bytes(),
    start_str.as_bytes(),
    data,
  ])
}

/// 段流帧发射：按段发 SNAPSHOT_DATA + 空载荷收尾，断言逐帧 +OK
fn emit_file_stream(
  rt: &Runtime,
  consumer: &mut RespSessionConsumer,
  token: u128,
  file_type: CheckpointFileType,
  source: &[u8],
  start_address: u64,
) {
  let mut offset = start_address;
  let end = start_address + source.len() as u64;
  while offset < end {
    let len = CHUNK.min((end - offset) as usize);
    let chunk = &source[(offset - start_address) as usize..][..len];
    let resp = drive(
      rt,
      consumer,
      &snapshot_data(token, file_type, offset as i64, chunk),
    );
    assert_eq!(resp, b"+OK\r\n", "文件段应答");
    offset += len as u64;
  }
  let resp = drive(
    rt,
    consumer,
    &snapshot_data(token, file_type, offset as i64, &[]),
  );
  assert_eq!(resp, b"+OK\r\n", "EOF 哨兵应答");
}

/// 主端拍检查点并登记复制域条目（发送面同源）
async fn take_primary_checkpoint(
  store: &Arc<WedbStore<SegmentedDevice>>,
  checkpoint_dir: &Path,
  provider: &Arc<ClusterProvider>,
) -> (u128, CheckpointEntry, i64) {
  let meta = wcpr::create_checkpoint(
    store.as_ref(),
    &store.ckpt_gate,
    checkpoint_dir,
    CheckpointType::FoldOver,
  )
  .await
  .unwrap();
  let token = meta.token;
  let covered = 0;
  let mut metadata = CheckpointMetadata::new(1);
  metadata.store_version = checkpoint_version(token);
  metadata.store_hlog_token = token;
  metadata.store_index_token = token;
  metadata.store_checkpoint_covered_aof_address = AofAddress::create(1, covered);
  metadata.store_primary_repl_id = Some(provider.replication_manager().unwrap().primary_repl_id());
  let entry = CheckpointEntry::new(metadata);
  provider
    .replication_manager()
    .unwrap()
    .add_checkpoint_entry(entry.clone(), true);
  (token, entry, covered)
}

/// 主端检查点文件集快照（发送面同源读取口径）
struct PrimaryCheckpointFiles {
  hlog: Vec<u8>,
  hlog_start: u64,
  index: Vec<u8>,
  meta: Vec<u8>,
}

async fn read_primary_checkpoint_files(
  store: &Arc<WedbStore<SegmentedDevice>>,
  checkpoint_dir: &Path,
  token: u128,
) -> PrimaryCheckpointFiles {
  let meta_bytes = fs::read(checkpoint_dir.join(wcpr::meta_filename(token))).unwrap();
  let meta = CheckpointMeta::decode(&meta_bytes).unwrap();
  let sector = store.device.sector_size() as u64;
  let start = meta.hlog_meta.begin_address / sector * sector;
  let file_len = store.device.get_file_size(0).unwrap();
  let raw_end = file_len
    .max(meta.hlog_meta.flushed_until_address)
    .max(meta.hlog_meta.tail_address);
  let end = raw_end / sector * sector;
  let hlog = store
    .device
    .read_range(start, (end - start) as usize)
    .await
    .unwrap();
  let index = fs::read(checkpoint_dir.join(wcpr::index_filename(token))).unwrap();
  PrimaryCheckpointFiles {
    hlog: hlog[..].to_vec(),
    hlog_start: start,
    index,
    meta: meta_bytes,
  }
}

/// 判别主用例：主端带元素向量集全量同步到非空副本 → 收口后新引擎登记
/// 回建、元素可达、副本旧集镜像残影清零
#[test]
fn diskbased_full_sync_rebuilds_vector_registry_and_prunes_stale_mirror() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 主端真装配（AOF 点亮：登记写透与宿主钩子束均依赖 aof 装配臂）
    let dir_p = tempfile::tempdir().unwrap();
    let host_p = StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir_p.path().join("node").join("host.db"),
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open primary host")
    .with_vector_set_preview(true);
    put_str(&host_p.store(), b"repl:str", b"from-primary").await;
    vadd(
      &host_p.store(),
      &host_p.vector_manager,
      b"vs:primary",
      &[b"e1", b"e2"],
    )
    .await;
    let provider_p = wired_provider(
      &host_p.store(),
      host_p.checkpoint_dir.clone(),
      host_p.wal().unwrap(),
      PRIMARY_ID,
      7000,
      NodeRole::Primary,
    );
    let (token, entry, covered) =
      take_primary_checkpoint(&host_p.store(), &host_p.checkpoint_dir, &provider_p).await;
    let files = read_primary_checkpoint_files(&host_p.store(), &host_p.checkpoint_dir, token).await;
    assert!(!files.hlog.is_empty(), "主端 hlog 段源非空");

    // ===== 副本非空库：本地旧集 + 旧键（全量同步前存量形态）
    let dir_r = tempfile::tempdir().unwrap();
    let host_r = StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir_r.path().join("node").join("host.db"),
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open replica host")
    .with_vector_set_preview(true);
    put_str(&host_r.store(), b"local:stale", b"pre-sync").await;
    vadd(&host_r.store(), &host_r.vector_manager, b"vs:old", &[b"o1"]).await;
    assert!(
      host_r
        .vector_manager
        .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), b"vs:old")
        .is_some(),
      "前置条件：副本旧集已入内存登记表"
    );

    // ===== 副本 provider 全接线（对齐 boot 装配束：宿主槽/管理面/向量/钩束/
    // 注册表同源，swap_online_store 置换漏斗全臂点亮）
    let provider_r = wired_provider(
      &host_r.store(),
      host_r.checkpoint_dir.clone(),
      host_r.wal().unwrap(),
      REPLICA_ID,
      7001,
      NodeRole::Replica,
    );
    provider_r.set_store_swap_slot(host_r.store_swap_slot());
    provider_r.set_database_manager(Arc::clone(&host_r.database_manager));
    provider_r.set_vector_manager(Arc::clone(&host_r.vector_manager));
    provider_r.set_engine_swap_hooks(host_r.engine_swap_hook_bundle());
    provider_r.set_consumer_registry(Arc::clone(&host_r.registry));
    let rm = provider_r.replication_manager().unwrap();
    let mut consumer = cluster_consumer(&provider_r);
    let old_store = Arc::clone(&host_r.store());

    // ===== 段流接收：hlog → index → meta（元数据最后落盘 = 提交标记）
    emit_file_stream(
      &rt,
      &mut consumer,
      token,
      CheckpointFileType::StoreHlog,
      &files.hlog,
      files.hlog_start,
    );
    emit_file_stream(
      &rt,
      &mut consumer,
      token,
      CheckpointFileType::StoreIndex,
      &files.index,
      0,
    );
    let resp = drive(
      &rt,
      &mut consumer,
      &snapshot_data(token, CheckpointFileType::StoreSnapshot, -1, &files.meta),
    );
    assert_eq!(resp, b"+OK\r\n", "元数据整包必须收下");

    // ===== 导入收口：向量回建失败即本轮全量失败（-ERR），故 +OK 应答
    // 本身即「recover_vector_sets 收口成功」判别
    assert!(rm.begin_recovery(RecoveryStatus::ClusterReplicate, false));
    let recover_frame = resp_frame(&[
      b"CLUSTER",
      b"BEGIN_REPLICA_RECOVER",
      b"1",
      b"0",
      b"primary-repl-id-1",
      &entry.to_byte_array(),
      &aof_span(covered),
      &aof_span(covered),
    ]);
    let resp = drive(&rt, &mut consumer, &recover_frame);
    let expected = format!("${}\r\n{covered}\r\n", covered.to_string().len());
    assert_eq!(resp, expected.as_bytes(), "恢复应答为授予位点 bulk string");

    // ===== 判别 1：引擎置换 + 主端键在新引擎可读
    let new_store = provider_r.try_store().unwrap();
    assert!(!Arc::ptr_eq(&new_store, &old_store), "导入必须置换在线引擎");
    assert_eq!(
      read_str(&new_store, b"repl:str").await.unwrap(),
      b"from-primary"
    );
    // 旧引擎本地残留键在新引擎视图不可见（引擎面换新）
    assert!(
      read_str(&new_store, b"local:stale").await.is_none(),
      "副本同步前旧键不得泄漏进新引擎视图"
    );

    // ===== 判别 2：主端向量集经 recover_vector_sets 入内存登记表且元素可达
    let vm_r = &host_r.vector_manager;
    assert!(
      vm_r
        .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), b"vs:primary")
        .is_some(),
      "换引擎后主端向量集登记条目必须经回建入内存镜像"
    );
    {
      let bind_sess = new_store.new_session().expect("vcard 绑定会话");
      let _domain = ActiveVectorSessionGuard::bind(&bind_sess);
      let vsess = RespServerSessionVectors::new(Arc::clone(vm_r));
      assert!(
        matches!(
          vsess
            .network_vcard(SessionPrefixBuf::ROOT.as_slice(), &[b"vs:primary"])
            .await,
          VectorReply::Integer(2)
        ),
        "主端向量集两元素必须随导入日志在副本回建后可达（VCARD==2）"
      );
    }

    // ===== 判别 3：副本同步前旧集内存镜像经残影清退臂清零（回建收口内
    // 同步执行，收口返回即确定性可见，无清理竞态窗口）
    assert!(
      vm_r
        .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), b"vs:old")
        .is_none(),
      "指向已弃置旧引擎物理实例的旧集镜像必须随回建残影清退摘除，不留虚体"
    );
  });
}
