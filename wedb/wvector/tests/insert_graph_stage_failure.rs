//! VADD 图阶段失败回滚与错误信号回归（票：zcode-r27-vectordiskann 发现四）
//!
//! 缺陷形态：上游 diskann insert 先 set_element（id 分配 + Vector/Quantized/
//! ExtMap/IntMap 四记录落盘 + fsm 占用）后执行图搜索/剪枝/邻接写，其后任一
//! 存储错误使包装层返回 Err；service.insert 原把该 Err 一律折为
//! DiskAnnInsertResult::False——元素四记录与 fsm 占用位已持久，VADD 应答
//! 「已存在」，重试恒 Duplicate（external_id_exists 命中 IntMap）、存储故障
//! 信号被吞，客户端按重复键语义重试永不收敛。
//!
//! 修复契约（与属性写失败臂同机制）：图阶段失败（记录已持久，exists 为真）
//! 先尽力 remove 回滚摘除收敛回「未插入」再报
//! [`DiskAnnInsertResult::StoreError`]；set_element 自身失败已由 provider
//! 失败出口逆序清道（exists 为假，存储无残留），维持插入期拒绝语义（False，
//! 由 set_element_rollback.rs 锁定）。
//!
//! 注入面：共享内存桥 `wvector_test::MemStore` 注入槽按 armed 项类型对
//! **rmw** 落盘口返回 false——图阶段的邻接写（set_neighbors/append_vector）
//! 走 rmw，而 set_element 四步全走 direct write，故 Neighbors 域 rmw 故障
//! 恰好只斩断图阶段。

use std::sync::Arc;

use wvector::{Callbacks, DiskANNService, DiskAnnInsertResult, VectorQuantType, store::Term};
use wvector_test::{FaultArm, MemStore, f32_bytes, test_config};

const CTX: u64 = 8;
/// keeper 元素内部 id（起点 0 后首铸）。
const IID_KEEP: u32 = 1;
/// ghost 元素内部 id（LIFO 序下一枚新铸）。
const IID_GHOST: u32 = 2;

/// 图阶段失败主靶：Neighbors 域 rmw 故障斩断图邻接写，插入应答存储错误
/// （非 False/Duplicate），回滚摘除后存储收敛「未插入」，重试可成功。
#[compio::test]
async fn graph_stage_failure_rolls_back_and_reports_store_error() {
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
  // keeper 正常插入（起点 0 + keeper 1，含图邻接 rmw）
  assert_eq!(
    service
      .insert(CTX, b"k1", &f32_bytes(&[1.0, 0.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );

  // 注入 Neighbors 域 rmw 故障：set_element 四步（direct write）不受影响，
  // 图阶段邻接写（rmw）失败 → diskann insert Err
  store.arm(FaultArm::Rmw, Term::Neighbors);
  assert_eq!(
    service
      .insert(CTX, b"g1", &f32_bytes(&[0.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::StoreError,
    "图阶段失败严禁折 False 误报 Duplicate"
  );

  // 回滚摘除后存储收敛「未插入」：四记录与 fsm 占用位均无残留
  let ghost = IID_GHOST.to_le_bytes();
  assert!(store.peek(CTX, Term::Vector, &ghost).is_none(), "向量残留");
  assert!(
    store.peek(CTX, Term::ExtMap, &ghost).is_none(),
    "ExtMap 残留"
  );
  assert!(
    store.peek(CTX, Term::IntMap, b"g1").is_none(),
    "IntMap 残留"
  );
  assert!(
    !service
      .check_internal_id_valid(CTX, IID_GHOST)
      .await
      .unwrap(),
    "失败槽位应已归还"
  );
  assert!(
    !service.check_external_id_valid(CTX, b"g1").await.unwrap(),
    "回滚后不得按 eid 存在（否则重试恒 Duplicate）"
  );
  assert_eq!(service.card(CTX), 1, "计数不得含失败元素");
  // keeper 不受波及
  assert!(
    store
      .peek(CTX, Term::Vector, &IID_KEEP.to_le_bytes())
      .is_some()
  );

  // 解除故障重试：LIFO 复用失败槽位，插入成功
  store.disarm();
  assert_eq!(
    service
      .insert(CTX, b"g2", &f32_bytes(&[1.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  assert_eq!(
    service.internal_id_of(CTX, b"g2").await.unwrap(),
    Some(IID_GHOST),
    "重试应复用回滚归还的槽位"
  );
  assert!(!service.check_external_id_valid(CTX, b"g1").await.unwrap());
}
