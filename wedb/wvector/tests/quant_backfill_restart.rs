//! 量化状态记录完整性与崩溃窗屏障补齐回归（票：zcode-r27-vectordiskann 发现一/二）
//!
//! 发现一（P0）：Bin 系量化完成标志原以 `rmw_iid(write_len=1)` 翻位，生产
//! rmw 内核以 write_len 为目标尺寸重建记录（旧值仅拷前缀）——状态记录
//! （标志字节 + 序列化量化器）被永久截断成单字节，重启后 Bin 臂 len<=1 判
//! InvalidQuantizer，此后集合全部命令重试重建恒失败：数据完好在盘却永久
//! 不可达。修复契约：回填收尾读全量状态置位标志后整值写回（翻标志不缩
//! 记录），重启恢复闭环。
//!
//! 发现二（P1）：崩溃窗（训练已写 _qnt、标志未置）重启后量化器反序列化即
//! is_trained，train_quantizer 原早退直接返回——enable_quantization 缺席，
//! 回填上界恒 u32::MAX，backfill 首行守卫恒拒绝，分片恒空转，enable_reuse
//! 与 all_quantized 永不置位，id 复用永久禁用。修复契约：早退臂同走屏障
//! 补齐（幂等，上界已收口时免重发布）。
//!
//! 注入面：内存桥接存储（rmw 与生产内核同口径：write_len 即目标记录尺寸，
//! 旧值截短/补零后闭包改写、整值写回）。

use std::sync::Arc;

use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, VectorDistanceMetricType,
  VectorQuantType, store::Term,
};
use wvector_test::MemStore;

const CTX: u64 = 8;

/// 量化状态键（`_qnt`；与 dynamic_quant 的 QUANT_STATE_KEY 同值，pub(crate)
/// 不出 crate，测试侧同值重铸）。
const QUANT_STATE_KEY: u32 = u32::from_be_bytes(*b"_qnt");

/// 训练样本门槛（Spherical1Bit::required_vectors 恒 1000；插入 1001 条用户
/// 向量使 max_internal_id=1001 > 1000 触发建表调度判据）。
const TRAIN_ROWS: usize = 1000;
const ELEMENTS: usize = TRAIN_ROWS + 1;

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn config() -> IndexConfig {
  IndexConfig {
    dims: 2,
    reduce_dims: 0,
    quant_type: VectorQuantType::Bin,
    distance_metric: VectorDistanceMetricType::L2,
    build_exploration_factor: 64,
    num_links: 8,
  }
}

/// 铸造Bin 集合：CTX 上插满 ELEMENTS 条用户向量（id 1..=ELEMENTS）。
async fn seed(store: &Arc<MemStore>) -> DiskANNService<MemStore> {
  let service = DiskANNService::default();
  assert_eq!(
    service
      .create_index(CTX, config(), Callbacks::new(Arc::clone(store)))
      .await,
    Ok(false)
  );
  for i in 0..ELEMENTS {
    let x = (i % 32) as f32 * 0.5;
    let y = (i / 32) as f32 * 0.5;
    let id = format!("e{i:0>6}");
    let res = service
      .insert(CTX, id.as_bytes(), &f32_bytes(&[x, y]), b"")
      .await;
    assert!(
      matches!(
        res,
        DiskAnnInsertResult::True | DiskAnnInsertResult::QuantizationRequested
      ),
      "第 {i} 条插入失败: {res:?}"
    );
  }
  service
}

/// 发现一主靶：回填收尾翻标志不得缩记录，重启后集合完整恢复。
#[compio::test]
async fn backfill_flag_flip_preserves_quant_state_record() {
  let store = Arc::new(MemStore::new());
  let service = seed(&store).await;

  // 建表：_qnt 落盘（标志 0 + 序列化量化器）
  assert!(service.build_quantization_table(CTX).await);
  let state = store
    .peek(CTX, Term::Metadata, &QUANT_STATE_KEY.to_le_bytes())
    .expect("_qnt 状态记录缺失");
  assert!(state.len() > 1, "训练态记录应含序列化量化器载荷");
  assert_eq!(state[0], 0, "建表后完成标志应为 0");

  // 单分片回填跑完触发收尾
  service.backfill_quantized_vectors(CTX, 0, 1).await;

  // 翻标志不缩记录：长度不变、标志置 1
  let flipped = store
    .peek(CTX, Term::Metadata, &QUANT_STATE_KEY.to_le_bytes())
    .expect("_qnt 状态记录缺失");
  assert_eq!(flipped.len(), state.len(), "收尾翻标志不得缩记录");
  assert_eq!(flipped[0], 1, "回填收尾应置完成标志");

  // 重启（同存储重建服务）：Bin 臂读 _qnt 恢复量化器，不得判
  // InvalidQuantizer；全量化态恢复后 id 复用随之启用
  let reloaded = DiskANNService::default();
  assert_eq!(
    reloaded
      .create_index(CTX, config(), Callbacks::new(Arc::clone(&store)))
      .await,
    Ok(false),
    "重启恢复失败：量化状态记录被截断"
  );
  assert_eq!(reloaded.card(CTX) as usize, ELEMENTS);

  // 检索可用（量化通道恢复）
  let out = reloaded
    .search_vector(
      CTX,
      &f32_bytes(&[0.0, 0.0]),
      wvector::SearchParams {
        count: 10,
        search_exploration_factor: 32,
        filter_len: 0,
        max_filtering_effort: 0,
      },
    )
    .await
    .expect("重启后检索失败");
  assert!(out.found >= 1);

  // id 复用：删后重插复用同槽（all_quantized 恢复 ⇒ reuse_enabled 恢复）
  let victim_iid = ELEMENTS as u32 / 2 + 1;
  let victim = format!("e{:0>6}", victim_iid - 1);
  assert!(reloaded.remove(CTX, victim.as_bytes()).await);
  assert_eq!(
    reloaded
      .insert(CTX, b"reborn", &f32_bytes(&[7.0, 7.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  assert_eq!(
    reloaded.internal_id_of(CTX, b"reborn").await,
    Some(victim_iid),
    "重启后 id 复用应恢复"
  );
}

/// 发现二主靶：崩溃窗（训练已落盘、标志未置）重启后，早退臂补齐启用屏障，
/// 回填分片实写量化记录，enable_reuse 生效。
#[compio::test]
async fn crash_window_retrain_enables_backfill_barrier() {
  let store = Arc::new(MemStore::new());
  let service = seed(&store).await;
  assert!(service.build_quantization_table(CTX).await);
  // 崩溃窗快照：_qnt 已写、标志 0
  assert_eq!(
    store
      .peek(CTX, Term::Metadata, &QUANT_STATE_KEY.to_le_bytes())
      .expect("_qnt 缺失")[0],
    0
  );
  drop(service);

  // 崩溃窗重启：量化器经反序列化即 is_trained，all_quantized=false——
  // 重建即判定需调度建表（Ok(true)），正是本缺陷窗口的可观察特征
  let reloaded = DiskANNService::default();
  assert_eq!(
    reloaded
      .create_index(CTX, config(), Callbacks::new(Arc::clone(&store)))
      .await,
    Ok(true)
  );

  // worker Build 臂：早退返回 true（修复前：不补屏障，下方回填恒空转）
  assert!(reloaded.build_quantization_table(CTX).await);

  // 回填分片实际执行：全量用户 id 量化记录落盘（修复前：上界恒
  // u32::MAX 守卫直接 return false，Quantized 域除收尾外恒空）
  reloaded.backfill_quantized_vectors(CTX, 0, 1).await;
  let sample_iid = 1u32;
  assert!(
    store
      .peek(CTX, Term::Quantized, &sample_iid.to_le_bytes())
      .is_some(),
    "回填未实写量化记录：启用屏障缺失，分片空转"
  );

  // 收尾翻转 + enable_reuse：删后重插复用同槽
  assert_eq!(
    store
      .peek(CTX, Term::Metadata, &QUANT_STATE_KEY.to_le_bytes())
      .expect("_qnt 缺失")[0],
    1
  );
  let victim_iid = ELEMENTS as u32 / 3 + 1;
  let victim = format!("e{:0>6}", victim_iid - 1);
  assert!(reloaded.remove(CTX, victim.as_bytes()).await);
  assert_eq!(
    reloaded
      .insert(CTX, b"reborn", &f32_bytes(&[3.0, 9.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  assert_eq!(
    reloaded.internal_id_of(CTX, b"reborn").await,
    Some(victim_iid),
    "崩溃窗恢复后 id 复用应生效"
  );
}
