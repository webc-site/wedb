//! enable_quantization 回填上界与并发铸造竞态回归（票：zcode-r123c-vechain1 案一 P2）
//!
//! 缺陷：fsm.rs enable_quantization 旧序「快照→置位→排空」使「登记早于
//! 置位、铸造晚于快照」的插入铸出回填上界之外的未量化 id——set_element
//! 仅认铸造时观测的 should_quantize 快照不写 Quantized 域，回填分片区间
//! [0..=上界] 亦不覆盖；all_quantized 翻转后标志=1 使重启恢复直接全量化、
//! 永不重回填，该 id 在量化轨（VSIM/图遍历）永久不可见，而
//! VRANDMEMBER/VGETATTR/VEMB 照常存活——同键双态应答数据视图分叉。
//!
//! 修复契约（对账原生 diskann-garnet fsm.rs:enable_quantization 写屏障内
//! 快照的同等不变量）：rust 计数屏障改「置位→排空→快照→再排空」定点收敛
//! 发布——置位后新登记者经 next_id 复读臂必走启用臂就地量化；排空置位前
//! 登记的在途插入（铸造先于计数注销）后取快照，上界恒覆盖全部未就地量化
//! 的已铸 id。
//!
//! 注入面：内存桥接存储（quant_backfill_restart.rs 同型 MemStore 桥）+
//! 真实双线程跨核并发（compio thread-per-core 生产形态：单索引串行动线
//! 的插入线程与并行的建表线程交叠，复现案一跨线程竞态窗）。
//!
//! 锁面：插入线程与 train+enable 线程全程交叠，跑完回填后断言全部存活
//! iid 在 Quantized 域有点读记录、VSIM 对每条命中可见。新码下该不变量由
//! 收敛序构造性保证，断言恒成立。

use std::{
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  thread::{scope, yield_now},
};

use compio::runtime::Runtime;
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, VectorDistanceMetricType,
  VectorQuantType, store::Term,
};
use wvector_test::MemStore;

const CTX: u64 = 8;

/// 量化状态键（`_qnt`，与 dynamic_quant 的 QUANT_STATE_KEY 同值）。
const QUANT_STATE_KEY: u32 = u32::from_be_bytes(*b"_qnt");

/// 训练样本门槛（Spherical1Bit::required_vectors 恒 1000）。
const TRAIN_ROWS: usize = 1000;

/// 插入线程总插入量（生产形态下单索引插入串行动线；训练/启用在独立核上
/// 并发进行，正是案一竞态窗的暴露形态）。
const TOTAL: usize = 1600;

/// 建表线程起跑水位：≥ 训练门槛 + 余量，保证 train 取样可成，且 enable
/// 置位时仍有大量插入在途与待铸。
const BUILD_TRIGGER: usize = TRAIN_ROWS + 150;

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

fn ext_id(i: usize) -> Vec<u8> {
  format!("e{i:0>6}").into_bytes()
}

/// 案一主靶：train+enable 与并发插入真实交叠后，回填上界恒覆盖全部未就地
/// 量化的已铸 id——Quantized 域点读全量在案、VSIM 全量可见。
#[compio::test]
async fn enable_quantization_snapshot_covers_concurrent_mints() {
  let store = Arc::new(MemStore::new());
  let service = DiskANNService::default();
  assert_eq!(
    service
      .create_index(CTX, config(), Callbacks::new(Arc::clone(&store)))
      .await,
    Ok(false)
  );
  let service = Arc::new(service);
  let progress = Arc::new(AtomicUsize::new(0));

  scope(|s| {
    // 插入线程：专属 compio 运行时连续铸造（生产单索引串行动线），与建表
    // 线程真实跨核并发——「登记早于置位、铸造晚于快照」窗口的暴露形态
    {
      let service = Arc::clone(&service);
      let progress = Arc::clone(&progress);
      s.spawn(move || {
        Runtime::new().unwrap().block_on(async {
          for i in 0..TOTAL {
            let x = (i % 32) as f32 * 0.5;
            let y = (i / 32) as f32 * 0.5;
            let id = ext_id(i);
            let res = service.insert(CTX, &id, &f32_bytes(&[x, y]), b"").await;
            assert!(
              matches!(
                res,
                DiskAnnInsertResult::True | DiskAnnInsertResult::QuantizationRequested
              ),
              "第 {i} 条插入失败: {res:?}"
            );
            progress.fetch_add(1, Ordering::Release);
          }
        });
      });
    }

    // 建表线程：到达水位即 train+enable，与在途/后续铸造全程交叠——
    // 正是旧序「快照先于置位」的竞态窗敞口最大的形态
    let service = Arc::clone(&service);
    let progress = Arc::clone(&progress);
    s.spawn(move || {
      Runtime::new().unwrap().block_on(async {
        while progress.load(Ordering::Acquire) < BUILD_TRIGGER {
          yield_now();
        }
        while !service.build_quantization_table(CTX).await {
          // 取样期存活量未达门槛等瞬态按既有语义重试
          yield_now();
        }
      });
    });
  });

  // 单分片回填收敛（收尾翻标志 + enable_reuse）
  service.backfill_quantized_vectors(CTX, 0, 1).await;

  let state = store
    .peek(CTX, Term::Metadata, &QUANT_STATE_KEY.to_le_bytes())
    .expect("_qnt 状态记录缺失");
  assert_eq!(state[0], 1, "回填收尾应置全量化完成标志");

  // 锁面核心：全部存活 iid 在 Quantized 域有点读记录——任何「登记早于置位、
  // 铸造晚于快照」的漏网 id（旧序产物）都会在此暴露为缺记录
  for i in 0..TOTAL {
    let id = ext_id(i);
    let iid = service
      .internal_id_of(CTX, &id)
      .await
      .unwrap_or_else(|| panic!("{id:?} 存活映射缺失"));
    assert!(
      store
        .peek(CTX, Term::Quantized, &iid.to_le_bytes())
        .is_some(),
      "iid {iid}（{id:?}）Quantized 域缺点读记录：晚铸键未入回填区间且未就地量化，量化轨永久不可见"
    );
  }

  // VSIM 对每条命中可见（量化轨全量交付）
  let out = service
    .search_vector(
      CTX,
      &f32_bytes(&[0.0, 0.0]),
      wvector::SearchParams {
        count: TOTAL,
        search_exploration_factor: TOTAL,
        filter_len: 0,
        max_filtering_effort: 0,
      },
    )
    .await
    .expect("量化轨检索失败");
  assert_eq!(out.found, TOTAL, "VSIM 全量可见性破口：found 缺项");
  let hit: Vec<Vec<u8>> = out.iter().map(|(id, _)| id.to_vec()).collect();
  for i in 0..TOTAL {
    assert!(hit.contains(&ext_id(i)), "e{i:0>6} 在 VSIM 中永久不可见");
  }
}
