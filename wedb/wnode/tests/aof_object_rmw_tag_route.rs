//! ObjectStoreRMW 重放条目物理键标签路由测试（镜像 / 信封两域一处判定）
//!
//! 验证审查意见【中高】的修复：分层稳态写镜像条目物理键恒 Meta 域、信封
//! RMW 条目恒 ObjectEnvelope 域，AOF 内本可零成本区分——重放端
//! object_store_rmw 先验物理键 tag 再路由，禁仅凭 load_collection_stub 探测：
//! - tag=Meta 且 stub 缺失（盘同步快照留痕跳过键的锚后镜像形态）：留痕跳过，
//!   绝不落信封通道——信封空对象重建只会物化出只含锚后字段的半截幻影信封，
//!   与源端升阶键整体缺失的 diskless 留痕契约相违；
//! - tag 非 Meta / ObjectEnvelope（AOF 流损坏 / 演化失配）：留痕跳过；
//! - tag=ObjectEnvelope：既有信封通道重建不受影响（回归守卫）。

use std::sync::Arc;

use waof::AofEntryType;
use wcol::{RespInputFlags, hash::hash_object::HashOperation};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  AofProcessor, GarnetAppendOnlyFile, GarnetLog, ReplayInput,
  aof::{
    aof_processor::ReplayTarget, garnet_log::RecordShape, recover::aof_recover::AofRecover,
    replay_input::ReplayInputSlice,
  },
  storage::session::storage_session::StorageSession,
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec};

/// 内存 AOF（无盘拓扑；重放面与磁盘拓扑同路径）
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = wnode_test::test_sublogs("aof_rmw_tag", 1);
          backends
        },
        None,
      )
      .expect("构造 GarnetLog"),
    ),
    &options,
    None,
  ))
}

/// 直写入队一条 ObjectStoreRMW 条目（物理键标签 / 对象类型 / 操作码 / 参数
/// 由调用方指定；条目负载形态与 service.rs 两镜像单点同一 ReplayInput 布局）
fn enqueue_object_rmw(
  aof: &GarnetAppendOnlyFile,
  tag: KeyTag,
  key: &[u8],
  obj_type: GarnetObjectType,
  op_code: u8,
  args: &[&[u8]],
) {
  let input = ReplayInputSlice::new(RespCommand::None, args)
    .with_flags(RespInputFlags::DETERMINISTIC.bits())
    .with_sub_id(op_code)
    .with_obj_type(obj_type);
  let phys = NamespaceDbCodec::encode_tagged_key(0, 0, tag, key);
  ReplayInput::with_encoded_slices(&input, |serialized| {
    let _ = aof.log().enqueue(&RecordShape {
      op_type: AofEntryType::ObjectStoreRMW,
      version: 0,
      session_id: 0,
      key: phys.as_slice(),
      value: &[],
      input: serialized,
      database_id: 0,
    });
  });
}

/// 组装重放面（空库 + 内存 AOF）并单日志恢复至完成
async fn recover_all(store: &Arc<WedbStore<SegmentedDevice>>, aof: &Arc<GarnetAppendOnlyFile>) {
  let replay_session = store.new_session().unwrap();
  let batch = replay_session.enter_batch();
  let storage = StorageSession::new(batch);
  let processor = AofProcessor::new(Arc::clone(aof));
  let target = ReplayTarget::new(&storage, store);
  AofRecover::single_log_recover(&processor, aof, 0, 0, -1, &target)
    .await
    .expect("标签路由面条目重放应闭环（执行或留痕跳过），不得失败");
}

/// 断言用户键的信封物理域无残留（误落信封通道即红）
async fn assert_no_envelope(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) {
  let sess = store.new_session().unwrap();
  let env_k = sess.session_tag_key(KeyTag::ObjectEnvelope, key);
  assert!(
    sess.read_raw(&env_k).await.unwrap().is_none(),
    "键 '{key:?}' 信封域出现幻影残留——镜像条目误走信封通道"
  );
}

/// tag=Meta 且 stub 缺失（盘同步快照留痕跳过键的锚后镜像形态）：留痕跳过，
/// 信封域零幻影、树态零物化——升阶键在副本整体缺失是留痕契约已知面，
/// 不得造半截幻影信封对象
#[compio::test]
async fn object_store_rmw_meta_tag_missing_stub_skips_not_envelope() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("meta.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let aof = memory_aof();

  enqueue_object_rmw(
    &aof,
    KeyTag::Meta,
    b"skipped",
    GarnetObjectType::Hash,
    u8::from(HashOperation::Hset),
    &[b"f1", b"v1"],
  );
  aof.log().commit();

  recover_all(&store, &aof).await;
  assert_no_envelope(&store, b"skipped").await;

  let sess = store.new_session().unwrap();
  assert!(
    sess
      .load_collection_stub(b"skipped")
      .await
      .unwrap()
      .is_none(),
    "留痕跳过不得在副本物化任何树态存根"
  );
}

/// tag 非 Meta / ObjectEnvelope（String 域承载 ObjectStoreRMW 条目 = 流损坏 /
/// 演化失配）：留痕跳过，绝不落信封通道
#[compio::test]
async fn object_store_rmw_unexpected_tag_skips_not_envelope() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("str.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let aof = memory_aof();

  enqueue_object_rmw(
    &aof,
    KeyTag::String,
    b"bogus",
    GarnetObjectType::Hash,
    u8::from(HashOperation::Hset),
    &[b"f1", b"v1"],
  );
  aof.log().commit();

  recover_all(&store, &aof).await;
  assert_no_envelope(&store, b"bogus").await;
}

/// tag=ObjectEnvelope（信封 RMW 条目既有形态）：信封通道照常重建，键缺失按
/// 空对象承接后回写信封（回归守卫——标签路由不得伤及历史条目）
#[compio::test]
async fn object_store_rmw_envelope_tag_keeps_envelope_channel() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("env.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let aof = memory_aof();

  enqueue_object_rmw(
    &aof,
    KeyTag::ObjectEnvelope,
    b"k",
    GarnetObjectType::Hash,
    u8::from(HashOperation::Hset),
    &[b"f1", b"v1"],
  );
  aof.log().commit();

  recover_all(&store, &aof).await;

  let sess = store.new_session().unwrap();
  let env_k = sess.session_tag_key(KeyTag::ObjectEnvelope, b"k");
  assert!(
    sess.read_raw(&env_k).await.unwrap().is_some(),
    "信封 RMW 条目须维持信封通道重建"
  );
}
