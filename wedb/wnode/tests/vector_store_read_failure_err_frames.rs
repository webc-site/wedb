//! 存储读失败折叠面锁测（票：wvector-store-read-failure-folded-to-empty-missing）
//!
//! 缺陷形态（修复前红）：fsm 占用位读失败被折叠——
//! ① `vector_id_exists/vector_iid_exists` 的 `unwrap_or(true)` 折「空闲→不存在」，
//!   VREM 故障窗假成功应答 `:0`、VISMEMBER 假阴性 `:0`（与存储分叉）；
//! ② `enumerate_elements` err 臂折空集，迁移导出静默丢整集；
//!   `get_full_vector` 的 `unwrap_or_default` 把向量读失败物化为空载荷照常导出。
//!
//! 修复契约（本文件锁死）：
//! - VREM：故障窗存活元素回 ERR 错误帧；正常缺元素仍 `:0`、 disarm 后删除仍 `:1`
//!   （应答契约对标 C# RespServerSessionVectors.cs NetworkVREM 非 OK→0，缺席语义不变）；
//! - VISMEMBER：故障窗存活元素 ERR、正常缺元素仍 false；
//! - 迁移导出：fsm 占用位读失败 → Err 中止（禁空集成功导出）；向量项读失败 →
//!   Err 中止（禁空载荷照常导出）。
//!
//! 注入面：`FaultStore` 基座 [`MemStore`] 转发，按臂开关在 `read` 落盘口对
//! `_fsm` 块键（Term::Metadata 宽 id 键，is_free/visit_used 唯一通道）与
//! Term::Vector 单读返回 false——对齐生产 read 契约 false=读失败/缺失同形，
//! 触发 fsm 侧 Err 臂（账面块键缺失/冷读失败即此形）。

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use compio::runtime::Runtime;
use wbase::hash_slot::slot_of;
use wnode::resp::vector::{
  resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
  vector_manager::{VectorAddArgs, VectorManager, VectorManagerOptions},
  vector_manager_index::Index,
};
use wnode_test::{bound_domain, index_config_dims2};
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, StoreCallbacks, VectorDistanceMetricType, VectorQuantType, VectorSetFlags,
  VectorValueType,
  store::{TERM_BITMASK, Term},
};
use wvector_test::MemStore;

const CTX: u64 = 0;
const SLOT0: u16 = slot_of(0, 0);

/// FSM 块键前缀（与 wvector fsm `block_key` 同构：宽 id 高 32 位）。
const FSM_PREFIX: u32 = u32::from_be_bytes(*b"_fsm");

/// 读故障注桩：`fail_fsm_reads` 武装时 `_fsm` 块键单读返 false（is_free /
/// visit_used 即 Err(Store::Read)）；`fail_vector_reads` 武装时 Term::Vector
/// 单读返 false（provider.get_full_vector 即 Err）。其余臂原样转基座。
struct FaultStore {
  base: MemStore,
  fail_fsm_reads: AtomicBool,
  fail_vector_reads: AtomicBool,
}

impl FaultStore {
  fn new() -> Self {
    Self {
      base: MemStore::new(),
      fail_fsm_reads: AtomicBool::new(false),
      fail_vector_reads: AtomicBool::new(false),
    }
  }

  fn arm_fsm(&self) {
    self.fail_fsm_reads.store(true, Ordering::Release);
  }

  fn disarm_fsm(&self) {
    self.fail_fsm_reads.store(false, Ordering::Release);
  }

  fn arm_vector(&self) {
    self.fail_vector_reads.store(true, Ordering::Release);
  }

  fn disarm_vector(&self) {
    self.fail_vector_reads.store(false, Ordering::Release);
  }

  /// `_fsm` 块宽 id 键判定（fsm::block_key：块号左移 32 位 | `_fsm` 前缀，
  /// 前缀在低 32 位）。
  fn is_fsm_block_key(key: &[u8]) -> bool {
    key.len() == 8
      && u64::from_le_bytes(key.try_into().unwrap()) & u32::MAX as u64 == FSM_PREFIX as u64
  }
}

impl StoreCallbacks for FaultStore {
  async fn read<F>(&self, context: u64, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    let term = context & TERM_BITMASK;
    if term == Term::Metadata as u64
      && Self::is_fsm_block_key(key)
      && self.fail_fsm_reads.load(Ordering::Acquire)
    {
      return false;
    }
    if term == Term::Vector as u64 && self.fail_vector_reads.load(Ordering::Acquire) {
      return false;
    }
    self.base.read(context, key, f).await
  }

  async fn read_multi<F>(&self, context: u64, keys: &[u8], length_hint: usize, f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    self.base.read_multi(context, keys, length_hint, f).await
  }

  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    self.base.write(context, key, value).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.base.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    self.base.rmw(context, key, write_len, f).await
  }

  async fn filter(&self, context: u64, internal_id: u32) -> bool {
    self.base.filter(context, internal_id).await
  }

  async fn purge_context(&self, context: u64) -> bool {
    self.base.purge_context(context).await
  }

  fn log(&self, context: u64, msg: &str) {
    self.base.log(context, msg);
  }
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}

/// VREM/VISMEMBER 存储故障窗错误帧锁测（票方案 3a/3c 契约双向）。
#[test]
fn vrem_vis_member_storage_failure_err_not_folded() {
  Runtime::new().unwrap().block_on(async {
    let (_dir, _store, _bound) = bound_domain();
    let fault = Arc::new(FaultStore::new());
    let manager = Arc::new(VectorManager::new(
      VectorManagerOptions {
        is_enabled: true,
        ..Default::default()
      },
      Callbacks::new(Arc::clone(&fault)),
    ));
    let sess = RespServerSessionVectors::new(Arc::clone(&manager));

    // 存活元素 e1（VADD 走正常路径落位）
    assert_eq!(
      sess
        .network_vadd(
          root().as_slice(),
          &[b"vk", b"VALUES", b"2", b"1.0", b"2.0", b"e1"],
          SLOT0,
          false,
        )
        .await,
      VectorReply::Integer(1)
    );

    // ── 故障窗前契约（对标 C# 缺元素→0 不变）──
    // VISMEMBER 正常缺元素 → false(0)
    assert_eq!(
      sess
        .network_vismember(root().as_slice(), &[b"vk", b"zz"], false)
        .await,
      VectorReply::Integer(0)
    );
    // VREM 正常缺元素 → :0（非错误）
    assert_eq!(
      sess
        .network_vrem(root().as_slice(), &[b"vk", b"zz"], false)
        .await,
      VectorReply::Integer(0)
    );

    // ── 故障窗（fsm 占用位读失败）──
    fault.arm_fsm();
    // VREM 存活元素：禁折叠假成功 :0，须 ERR 错误帧、不写 AOF
    match sess
      .network_vrem(root().as_slice(), &[b"vk", b"e1"], false)
      .await
    {
      VectorReply::Error(msg) => assert!(msg.starts_with(b"ERR"), "错误帧文案: {msg:?}"),
      other => panic!("故障窗 VREM 存活元素应回 ERR 错误帧，实际 {other:?}"),
    }
    // VISMEMBER 存活元素：禁假阴性 false，须 ERR 错误帧
    match sess
      .network_vismember(root().as_slice(), &[b"vk", b"e1"], false)
      .await
    {
      VectorReply::Error(msg) => assert!(msg.starts_with(b"ERR"), "错误帧文案: {msg:?}"),
      other => panic!("故障窗 VISMEMBER 应回 ERR 错误帧，实际 {other:?}"),
    }
    // VISMEMBER 正常缺元素（to_internal_id 缺席臂不触 fsm）：缺席语义不变
    assert_eq!(
      sess
        .network_vismember(root().as_slice(), &[b"vk", b"zz"], false)
        .await,
      VectorReply::Integer(0)
    );

    // ── 撤障回归：正常删除/存在性全绿 ──
    fault.disarm_fsm();
    assert_eq!(
      sess
        .network_vismember(root().as_slice(), &[b"vk", b"e1"], false)
        .await,
      VectorReply::Integer(1)
    );
    assert_eq!(
      sess
        .network_vrem(root().as_slice(), &[b"vk", b"e1"], false)
        .await,
      VectorReply::Integer(1)
    );
    assert_eq!(
      sess
        .network_vrem(root().as_slice(), &[b"vk", b"e1"], false)
        .await,
      VectorReply::Integer(0)
    );
  })
}

/// 建集并注入存活元素，返回 (manager, 注桩, index_value)。
async fn seeded_export_set(
  elements: &[&[u8]],
) -> (Arc<VectorManager<FaultStore>>, Arc<FaultStore>, Index) {
  let fault = Arc::new(FaultStore::new());
  let manager = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::clone(&fault)),
  ));
  assert_eq!(
    manager
      .service
      .create_index(
        CTX,
        index_config_dims2(),
        Callbacks::new(Arc::clone(&fault)),
      )
      .await,
    Ok(false),
    "集合创建失败"
  );
  let index = Index {
    context: CTX,
    index_ptr: 1,
    dimensions: 2,
    reduce_dims: 0,
    num_links: 8,
    build_exploration_factor: 64,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    flags: VectorSetFlags::NONE,
  };
  for (i, name) in elements.iter().enumerate() {
    manager
      .try_add(
        root().as_slice(),
        b"vk",
        &index.to_bytes(),
        &VectorAddArgs::new(
          name,
          VectorValueType::FP32,
          &f32_bytes(&[i as f32, 1.0]),
          b"",
        ),
      )
      .await
      .unwrap();
  }
  (manager, fault, index)
}

/// 迁移导出故障窗中止锁测（票方案 3b 双向）：fsm 占用位读失败禁空集成功、
/// 向量项读失败禁空载荷照常导出；撤障后导出与存活集一致回归绿。
#[test]
fn migration_export_storage_failure_aborts_not_empty() {
  Runtime::new().unwrap().block_on(async {
    let (_dir, _store, _bound) = bound_domain();
    let (manager, fault, index) = seeded_export_set(&[b"e1", b"e2"]).await;
    let stored = index.to_bytes();

    // 正常路径：导出恰为存活 2 元素（非空集）
    let exported = manager.export_migration_elements(&stored).await.unwrap();
    assert_eq!(exported.len(), 2, "撤障态导出集必须完整");

    // fsm 占用位读失败：禁折叠空集「静默丢整集」，必须 Err 中止
    fault.arm_fsm();
    assert!(
      manager.export_migration_elements(&stored).await.is_err(),
      "fsm 读失败导出必须 Err 中止，禁空集成功应答"
    );
    fault.disarm_fsm();

    // 向量项读失败：禁 unwrap_or_default 物化空载荷照常导出，必须 Err 中止
    fault.arm_vector();
    assert!(
      manager.export_migration_elements(&stored).await.is_err(),
      "向量读失败导出必须 Err 中止，禁空载荷导出"
    );
    fault.disarm_vector();

    // 撤障回归：导出载荷完整非空
    let exported = manager.export_migration_elements(&stored).await.unwrap();
    assert!(
      exported.iter().all(|e| e.values.len() == 8),
      "撤障态导出向量载荷须为全精度 dim*4"
    );
  })
}
