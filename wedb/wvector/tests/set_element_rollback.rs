//! set_element 分步写失败回滚回归（票：wvector-set-element-partial-term-write-residue-on-failure）
//!
//! 缺陷形态：provider `set_element` 四步序贯写（Vector → Quantized → ExtMap →
//! IntMap）任一失败仅 `mark_free` 归还槽位，已写项一概不清；(d) 档（IntMap 失败）
//! ExtMap 残留被 `random_members`（VRANDMEMBER 后端，对 `[0, max_internal_id]`
//! 均匀采样不过滤 `is_free`）投影成幽灵外部 id。
//! 修复契约（对标 C# `garnet/libs/server/Resp/Vector/DiskANNService.cs:Insert`
//! 单次原生调用失败即 managed 无残留）：失败出口按已写档位单次逆序回滚，
//! Vector/Quantized/ExtMap 按 internal_id 与 IntMap 按 eid 全部不可见，
//! fsm 计数据实归位，id 复用覆写与跨 load_state 重建后残留均不回灌。
//!
//! 注入面：共享内存桥 `wvector_test::MemStore` 注入槽按 armed 项类型位域
//! （`context & TERM_BITMASK` 命中）在写落盘口返回 false（生产
//! `WedbVectorStoreCallbacks::write` IO 失败同形返回）。

use std::sync::Arc;

use wvector::{Callbacks, DiskANNService, DiskAnnInsertResult, VectorQuantType, store::Term};
use wvector_test::{FaultArm, MemStore, f32_bytes, test_config};

/// 集合上下文基址（低 3 位保留给项类型）。
const CTX: u64 = 8;
/// 起点内部 id。
const IID_START: u32 = 0;
/// keeper 元素内部 id（起点占 0 后首个新铸）。
const IID_KEEP: u32 = 1;
/// 失败元素内部 id（LIFO 序下一枚新铸）。
const IID_GHOST: u32 = 2;

/// 装配：keeper `k1` 正常插入（占起点 0 + 元素 1）→ 注入档位故障 → ghost `g1`
/// 插入失败（ghost 铸 id 2）。返回存储与服务句柄供逐档断言。
async fn scenario(fail: Term) -> (Arc<MemStore>, DiskANNService<MemStore>) {
  let store = Arc::new(MemStore::new());
  let service = DiskANNService::default();
  assert_eq!(
    service
      .create_index(
        CTX,
        test_config(VectorQuantType::NoQuant),
        Callbacks::new(Arc::clone(&store))
      )
      .await,
    Ok(false)
  );
  assert_eq!(
    service
      .insert(CTX, b"k1", &f32_bytes(&[1.0, 0.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  store.arm(FaultArm::Write, fail);
  // provider set_element 写失败 → diskann insert Err → service.rs:897 折叠 False
  assert_eq!(
    service
      .insert(CTX, b"g1", &f32_bytes(&[0.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::False
  );
  (store, service)
}

/// 三档写失败的统一不可见断言：Vector/Quantized/ExtMap 按 internal_id、
/// IntMap 按 eid 全部 miss，fsm 计数据实归位，采样面无幽灵。
async fn assert_all_invisible(store: &MemStore, service: &DiskANNService<MemStore>) {
  assert!(
    store
      .peek(CTX, Term::Vector, &IID_GHOST.to_le_bytes())
      .is_none(),
    "向量本体残留"
  );
  assert!(
    store
      .peek(CTX, Term::Quantized, &IID_GHOST.to_le_bytes())
      .is_none(),
    "量化向量残留"
  );
  assert!(
    store
      .peek(CTX, Term::ExtMap, &IID_GHOST.to_le_bytes())
      .is_none(),
    "ExtMap 残留"
  );
  assert!(
    store.peek(CTX, Term::IntMap, b"g1").is_none(),
    "IntMap 残留"
  );
  // fsm：槽位空闲且计数不含失败元素（起点 + keeper = 2）
  assert!(
    !service
      .check_internal_id_valid(CTX, IID_GHOST)
      .await
      .unwrap(),
    "失败槽位应空闲"
  );
  assert!(
    !service.check_external_id_valid(CTX, b"g1").await.unwrap(),
    "失败元素不应按 eid 存在"
  );
  assert_eq!(service.card(CTX), 1, "计数不得含失败元素");
  let mut sample = service.sample(CTX, 5).await;
  sample.sort();
  assert_eq!(sample, vec![b"k1".to_vec()], "采样面不得出现幽灵 eid");
}

#[compio::test]
async fn full_vector_write_failure_rolls_back() {
  let (store, service) = scenario(Term::Vector).await;
  assert_all_invisible(&store, &service).await;
  // 起点与 keeper 不受波及
  assert!(
    store
      .peek(CTX, Term::Vector, &IID_START.to_le_bytes())
      .is_some()
  );
  assert!(
    store
      .peek(CTX, Term::Vector, &IID_KEEP.to_le_bytes())
      .is_some()
  );
}

#[compio::test]
async fn ext_map_write_failure_rolls_back() {
  let (store, service) = scenario(Term::ExtMap).await;
  assert_all_invisible(&store, &service).await;
}

/// (d) 档主靶：IntMap 失败 ⇒ ExtMap 残留即 VRANDMEMBER 幽灵 eid。
/// 修复后残留尽清；解除故障重试经 LIFO 复用同槽覆写，旧 eid 全面 miss。
#[compio::test]
async fn int_map_write_failure_rolls_back_and_id_reuse_overwrites() {
  let (store, service) = scenario(Term::IntMap).await;
  assert_all_invisible(&store, &service).await;

  store.disarm();
  assert_eq!(
    service
      .insert(CTX, b"g2", &f32_bytes(&[1.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  // LIFO 复用：mark_free 归还的 IID_GHOST 由 next_id 优先弹出
  assert_eq!(
    service.internal_id_of(CTX, b"g2").await.unwrap(),
    Some(IID_GHOST),
    "重试应复用失败槽位"
  );
  assert_eq!(
    store
      .peek(CTX, Term::ExtMap, &IID_GHOST.to_le_bytes())
      .as_deref(),
    Some(&b"g2"[..])
  );
  assert!(
    store.peek(CTX, Term::IntMap, b"g1").is_none(),
    "旧 eid 映射不得复活"
  );
  let mut sample = service.sample(CTX, 5).await;
  sample.sort();
  assert_eq!(sample, vec![b"g2".to_vec(), b"k1".to_vec()]);
}

/// 跨重启回灌：同存储新建服务（`WedbProvider::new` → fsm `load_state`
/// 只从位图重建，不清 ExtMap），残留必须已在失败当场被逆序回滚清道、
/// 不得复活；复用槽位覆写后读侧两面收敛一致。
#[compio::test]
async fn int_map_write_failure_residue_absent_after_state_reload() {
  let (store, _service) = scenario(Term::IntMap).await;
  store.disarm();

  let reloaded = DiskANNService::default();
  assert_eq!(
    reloaded
      .create_index(
        CTX,
        test_config(VectorQuantType::NoQuant),
        Callbacks::new(Arc::clone(&store))
      )
      .await,
    Ok(false),
    "load_state 重建应成功"
  );
  assert!(
    store
      .peek(CTX, Term::ExtMap, &IID_GHOST.to_le_bytes())
      .is_none(),
    "重启后 ExtMap 残留回灌"
  );
  assert!(
    store
      .peek(CTX, Term::Vector, &IID_GHOST.to_le_bytes())
      .is_none(),
    "重启后向量残留回灌"
  );
  assert_eq!(reloaded.card(CTX), 1);
  assert_eq!(reloaded.internal_id_of(CTX, b"g1").await.unwrap(), None);
  let mut sample = reloaded.sample(CTX, 5).await;
  sample.sort();
  assert_eq!(sample, vec![b"k1".to_vec()], "重启后采样面不得出现幽灵 eid");

  // 重启后新插入复用同槽覆写，全部读面对旧 eid miss
  assert_eq!(
    reloaded
      .insert(CTX, b"g2", &f32_bytes(&[1.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  assert_eq!(
    reloaded.internal_id_of(CTX, b"g2").await.unwrap(),
    Some(IID_GHOST)
  );
  assert!(!reloaded.check_external_id_valid(CTX, b"g1").await.unwrap());
  let mut sample = reloaded.sample(CTX, 5).await;
  sample.sort();
  assert_eq!(sample, vec![b"g2".to_vec(), b"k1".to_vec()]);
}
