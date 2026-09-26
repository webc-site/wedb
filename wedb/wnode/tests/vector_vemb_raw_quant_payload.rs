//! VEMB RAW 载荷契约回归（票：zcode-r27-vectordiskann 发现三）
//!
//! 缺陷形态：try_get_raw_embedding 原无条件 get_full_vector，函数注释宣称
//! 「量化系先读量化向量、未完成回退完整向量」与实现相反——量化集合上 RAW
//! 应答载荷宽度与语义同 C# 逐字节分叉（Q8 集合 dim*4 vs dim+20），按
//! quantType 解析载荷的客户端必然错读；迁移导出面复用同函数，量化集合
//! 迁移帧膨胀 4 至 32 倍。
//!
//! 修复契约（对齐 C# VectorManager.cs:TryGetRawEmbedding 读序）：NoQuant 系
//! 直读完整向量；量化系读量化记录（QuantizedVector 项），记录缺失（回填未
//! 完成）回退完整向量——RAW 载荷宽度即量化记录规范宽。

use std::sync::Arc;

use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::{
  resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
  vector_manager::{VectorAddArgs, VectorManager, VectorManagerOptions},
  vector_manager_index::Index,
  vector_store_callbacks::OwnedActiveVectorSession,
};
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, IndexConfig, VectorDistanceMetricType, VectorQuantType, VectorSetFlags,
  VectorValueType,
};
use wvector_test::MemStore;

fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}

/// 执行域绑定装配（try_get_raw_embedding 的会话守卫 debug 断言要求；
/// 形态同 resp_vector_set::bound_domain）。
fn bound_domain() -> (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  OwnedActiveVectorSession<SegmentedDevice>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("bind.db")).unwrap());
  let store = Arc::new(WedbStore::open(wtest_base::test_store_config(), device).unwrap());
  let bound = OwnedActiveVectorSession::new(store.new_session().unwrap());
  (dir, store, bound)
}

fn session() -> RespServerSessionVectors<MemStore> {
  let manager = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(MemStore::new())),
  ));
  RespServerSessionVectors::new(manager)
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// 铸造指定量化类型的集合索引并写入登记表，返回索引记录字节。
async fn seed_index_quant(
  sess: &RespServerSessionVectors<MemStore>,
  key: &[u8],
  dims: u32,
  quant: VectorQuantType,
) -> [u8; 56] {
  let context = sess.manager.next_vector_set_context(0).await.unwrap();
  let _ = sess
    .manager
    .service
    .create_index(
      context,
      IndexConfig::new(dims, 0, quant, VectorDistanceMetricType::L2, 64, 8),
      sess.manager.callbacks.clone(),
    )
    .await;
  let index = Index {
    context,
    index_ptr: 1,
    dimensions: dims,
    reduce_dims: 0,
    num_links: 8,
    build_exploration_factor: 64,
    quant_type: quant,
    distance_metric: VectorDistanceMetricType::L2,
    flags: VectorSetFlags::NONE,
  };
  let bytes = index.to_bytes();
  sess
    .manager
    .write_stored_index(root().as_slice(), key, &bytes)
    .await;
  bytes
}

/// Q8 集合：RAW 载荷为量化记录规范宽（dim + 20），与量化记录直读逐字节一致。
#[compio::test]
async fn vemb_raw_returns_quantized_record_for_q8() {
  let (_dir, _kv, _bound) = bound_domain();
  let sess = session();
  let stored = seed_index_quant(&sess, b"q", 2, VectorQuantType::Q8).await;
  sess
    .manager
    .try_add(
      root().as_slice(),
      b"q",
      &stored,
      &VectorAddArgs {
        quant_type: VectorQuantType::Q8,
        ..VectorAddArgs::new(b"e1", VectorValueType::FP32, &f32_bytes(&[5.0, 6.0]), b"")
      },
    )
    .await
    .unwrap();

  // Q8 就地量化：量化记录应已存在且宽度为规范宽（Q8 = dim + 20）
  let quant_bytes = sess
    .manager
    .service
    .get_quant_vector(stored_context(&stored), b"e1")
    .await
    .expect("Q8 集合应落量化记录");
  assert_eq!(quant_bytes.len(), 2 + 20, "Q8 量化记录规范宽 dim+20");
  assert_ne!(quant_bytes.len(), 8, "不得恒回全精度宽度 dim*4");

  let raw = sess
    .network_vemb(root().as_slice(), &[b"q", b"e1", b"RAW"])
    .await;
  let VectorReply::Array(items) = raw else {
    panic!("RAW 应为数组: {raw:?}");
  };
  assert_eq!(items[0], VectorReply::Simple(b"q8"));
  assert_eq!(
    items[1],
    VectorReply::Bulk(Some(quant_bytes.clone().into())),
    "RAW 载荷必须等于量化记录原始字节"
  );
  // Q8 追加量化范围
  assert_eq!(items.len(), 4);
  assert_eq!(items[3], VectorReply::Double(1.0));
}

/// Bin 集合（未训练回填，量化记录缺失）：RAW 回退完整向量，宽度与全精度
/// 一致——回退路径显式锁定。
#[compio::test]
async fn vemb_raw_falls_back_to_full_vector_for_unbackfilled_bin() {
  let (_dir, _kv, _bound) = bound_domain();
  let sess = session();
  let stored = seed_index_quant(&sess, b"b", 2, VectorQuantType::Bin).await;
  sess
    .manager
    .try_add(
      root().as_slice(),
      b"b",
      &stored,
      &VectorAddArgs {
        quant_type: VectorQuantType::Bin,
        ..VectorAddArgs::new(b"e1", VectorValueType::FP32, &f32_bytes(&[5.0, 6.0]), b"")
      },
    )
    .await
    .unwrap();

  // 未训练回填：无量化记录（除收尾外），RAW 应回退完整向量
  assert!(
    sess
      .manager
      .service
      .get_quant_vector(stored_context(&stored), b"e1")
      .await
      .is_none(),
    "前置：Bin 未回填应无量化记录"
  );

  let raw = sess
    .network_vemb(root().as_slice(), &[b"b", b"e1", b"RAW"])
    .await;
  let VectorReply::Array(items) = raw else {
    panic!("RAW 应为数组: {raw:?}");
  };
  assert_eq!(items[0], VectorReply::Simple(b"bin"));
  assert_eq!(
    items[1],
    VectorReply::Bulk(Some(f32_bytes(&[5.0, 6.0]).into())),
    "量化记录缺失时 RAW 应回退完整向量"
  );
  assert_eq!(items.len(), 3, "Bin 不追加量化范围");
}

/// NoQuant 集合：稳态直读完整向量（现有行为回归锁定）。
#[compio::test]
async fn vemb_raw_noquant_reads_full_vector() {
  let (_dir, _kv, _bound) = bound_domain();
  let sess = session();
  let stored = seed_index_quant(&sess, b"n", 2, VectorQuantType::NoQuant).await;
  sess
    .manager
    .try_add(
      root().as_slice(),
      b"n",
      &stored,
      &VectorAddArgs::new(b"e1", VectorValueType::FP32, &f32_bytes(&[5.0, 6.0]), b""),
    )
    .await
    .unwrap();

  let raw = sess
    .network_vemb(root().as_slice(), &[b"n", b"e1", b"RAW"])
    .await;
  let VectorReply::Array(items) = raw else {
    panic!("RAW 应为数组: {raw:?}");
  };
  assert_eq!(items[0], VectorReply::Simple(b"fp32"));
  assert_eq!(
    items[1],
    VectorReply::Bulk(Some(f32_bytes(&[5.0, 6.0]).into()))
  );
}

/// 从索引记录字节解出 context（断言辅助）。
fn stored_context(bytes: &[u8]) -> u64 {
  Index::from_bytes(bytes).unwrap().context
}
