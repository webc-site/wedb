#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::sync::Arc;

use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, SearchParams, SearchResults,
  VectorDistanceMetricType, VectorQuantType, store,
};
use wvector_test::{MemStore, f32_bytes};

/// 索引配置（维度 / 量化 / 度量参数化；检索臂回归面）
fn config(dims: u32, quant: VectorQuantType, metric: VectorDistanceMetricType) -> IndexConfig {
  IndexConfig {
    dims,
    reduce_dims: 0,
    quant_type: quant,
    distance_metric: metric,
    build_exploration_factor: 64,
    num_links: 8,
  }
}

fn callbacks() -> Callbacks<MemStore> {
  Callbacks::new(Arc::new(MemStore::new()))
}

#[compio::test]
async fn create_insert_search_roundtrip() {
  let service = DiskANNService::default();
  assert_eq!(
    service
      .create_index(
        8,
        config(2, VectorQuantType::NoQuant, VectorDistanceMetricType::L2),
        callbacks()
      )
      .await,
    Ok(false)
  );

  // 首个插入同时设起点
  let res = service
    .insert(8, b"a", &f32_bytes(&[0.0, 0.0]), b"{\"k\":1}")
    .await;
  assert_eq!(res, DiskAnnInsertResult::True);
  let res = service.insert(8, b"b", &f32_bytes(&[1.0, 1.0]), b"").await;
  assert_eq!(res, DiskAnnInsertResult::True);
  // 重复插入 → False
  let res = service.insert(8, b"a", &f32_bytes(&[5.0, 5.0]), b"").await;
  assert_eq!(res, DiskAnnInsertResult::False);
  // 维度不匹配 → False
  let res = service.insert(8, b"c", &f32_bytes(&[1.0]), b"").await;
  assert_eq!(res, DiskAnnInsertResult::False);

  assert_eq!(service.card(8), 2);
  assert!(service.check_external_id_valid(8, b"a").await.unwrap());
  assert!(!service.check_external_id_valid(8, b"zz").await.unwrap());

  let out = service
    .search_vector(
      8,
      &f32_bytes(&[0.1, 0.1]),
      SearchParams {
        count: 10,
        search_exploration_factor: 32,
        filter_len: 0,
        max_filtering_effort: 0,
      },
    )
    .await
    .unwrap();
  assert_eq!(out.found, 2);

  // 属性读取
  assert_eq!(
    service.get_attribute(8, b"a").await.unwrap().unwrap(),
    b"{\"k\":1}"
  );
  assert!(service.set_attribute(8, b"a", b"{}").await.unwrap());
  assert_eq!(
    service.get_attribute(8, b"a").await.unwrap().unwrap(),
    b"{}"
  );
  assert!(!service.set_attribute(8, b"nope", b"{}").await.unwrap());

  // 删除
  assert!(service.remove(8, b"a").await.unwrap());
  assert!(!service.remove(8, b"a").await.unwrap());
  assert_eq!(service.card(8), 1);
  assert!(service.get_full_vector(8, b"a").await.is_err());

  service.drop_index(8);
  assert_eq!(service.card(8), 0);
  assert!(
    service
      .search_vector(
        8,
        &f32_bytes(&[0.0, 0.0]),
        SearchParams {
          count: 10,
          search_exploration_factor: 8,
          filter_len: 0,
          max_filtering_effort: 0,
        },
      )
      .await
      .is_err()
  );
}

#[compio::test]
async fn search_element_and_embedding() {
  let service = DiskANNService::default();
  service
    .create_index(
      24,
      config(2, VectorQuantType::NoQuant, VectorDistanceMetricType::L2),
      callbacks(),
    )
    .await
    .unwrap();
  service.insert(24, b"x", &f32_bytes(&[1.0, 0.0]), b"").await;
  service.insert(24, b"y", &f32_bytes(&[0.0, 1.0]), b"").await;

  let params = SearchParams {
    count: 10,
    search_exploration_factor: 32,
    filter_len: 0,
    max_filtering_effort: 0,
  };
  let out = service.search_element(24, b"x", params).await.unwrap();
  assert!(out.found >= 1);

  let emb = service.embedding_of(24, b"x").await.unwrap();
  assert_eq!(emb, vec![1.0, 0.0]);

  let raw = service.get_full_vector(24, b"x").await.unwrap();
  assert_eq!(raw.len(), 8);

  let links = service.links_of(24, b"x").await.unwrap();
  assert!(!links.is_empty());

  let sample = service.sample(24, 5).await;
  assert_eq!(sample.len(), 2);

  assert_eq!(service.internal_id_of(24, b"x").await.unwrap(), Some(1));
  assert_eq!(service.internal_id_of(24, b"none").await.unwrap(), None);
  assert_eq!(service.quant_of(24), Some(VectorQuantType::NoQuant));
}

/// 内联定容：单元素 `4B LE 长度 + 8B id`（对标 C#
/// VectorManager.cs:67 `MinimumSpacePerId = sizeof(int) + 8`）。
const MIN_SPACE_PER_ID: usize = 4 + 8;

/// 生成 `width` 字节定宽数字串 id（如 width=32 → "000...000" + i）。
fn numeric_id(i: usize, width: usize) -> Vec<u8> {
  format!("{i:0>width$}").into_bytes()
}

/// 直接驱动 [`SearchResults`] 输出缓冲：以 `push_id` 逐条写入 k 条
/// 定宽外部 id，触发 overflow 分支后 `into_search_output` 物化。
///
/// 本函数复刻 service.rs run_search 的定容逻辑（ids = k×MIN_SPACE_PER_ID，
/// dists = k），与检索命令共用同一 [`SearchResults`] 写出协议。
fn collect(k: usize, width: usize) -> wvector::SearchOutput {
  let mut ids = vec![0u8; k * MIN_SPACE_PER_ID];
  let mut dists = vec![0f32; k];
  let mut output = SearchResults::new(k, &mut ids, &mut dists);
  for i in 0..k {
    let _ = output.push_id(store::VectorSetId::from(numeric_id(i, width)));
  }
  output.into_search_output()
}

/// 回归核心：溢出项遗漏 4 字节长度前缀导致 [`SearchOutput::iter`] 截断 / 错位。
///
/// 对标 C# `VectorIdFormat.I32LengthPrefixed`：会话层
/// RespServerSessionVectors.cs:WriteRESP3Result（及 WriteRESP2Result）以
/// `BinaryPrimitives.ReadInt32LittleEndian` 逐项解包 id 流，要求内联与溢出
/// 两条写出路径共用同一 `[4B LE 长度][载荷]` 协议（diskann-garnet SearchResults
/// 契约；current_len 须计入溢出项以与 is_full/size_hint 闭环）。
///
/// 32B id 单元素占 4+32=36B，内联定容 k×12B 仅容 ⌊12k/36⌋ 条，其余全部
/// 落入 overflow 缓冲——正是缺陷分支。修复前 overflow 只追加裸字节，迭代器
/// 把首条溢出 id 的前 4 字节误读为长度头，命中项被静默丢弃或整体错位。
#[test]
fn search_results_overflow_keeps_length_prefix() {
  const K: usize = 6;
  const WIDTH: usize = 32;

  // 定容 6×12=72B：前 2 条内联（各 36B，恰填满），第 3..6 条溢出
  let out = collect(K, WIDTH);
  assert_eq!(out.found, K, "found 漏计溢出项");

  let hits = out.hits();
  assert_eq!(hits.len(), K, "溢出项被迭代器截断/错位");
  for (i, hit) in hits.iter().enumerate() {
    assert_eq!(
      hit.external_id,
      numeric_id(i, WIDTH),
      "第 {i} 条 id 载荷损坏（长度前缀缺失致偏移错位）"
    );
  }

  // 原始 id 流严格为 k 组 [4B LE 长度=32][32B 载荷]，无裸字节泄漏
  let mut expect = Vec::with_capacity(K * (4 + WIDTH));
  for i in 0..K {
    expect.extend_from_slice(&(WIDTH as u32).to_le_bytes());
    expect.extend_from_slice(&numeric_id(i, WIDTH));
  }
  assert_eq!(out.ids, expect, "溢出缓冲写出协议与内联不一致");
}

/// 内联容量边界两侧：8B id（4+8=12）恰满单格全内联；9B id（4+9=13）
/// 使末条跨出容量进入 overflow。两侧解包结果必须同样完整精确。
#[test]
fn search_results_inline_capacity_boundary() {
  // 边界内：4 条 8B id，定容 4×12=48B，逐条 12B 全部内联
  {
    const K: usize = 4;
    let out = collect(K, 8);
    assert_eq!(out.found, K);
    let hits = out.hits();
    assert_eq!(hits.len(), K, "全内联路径不应受损");
    for (i, hit) in hits.iter().enumerate() {
      assert_eq!(hit.external_id, numeric_id(i, 8));
    }
  }

  // 刚越界：4 条 9B id，单条 13B，定容 4×12=48B；
  // 前 3 条内联(39B)，第 4 条 39+13=52>48 落入 overflow
  {
    const K: usize = 4;
    let out = collect(K, 9);
    assert_eq!(out.found, K, "跨边界第 4 条溢出项漏计");
    let hits = out.hits();
    assert_eq!(hits.len(), K, "跨内联容量的溢出项损坏");
    for (i, hit) in hits.iter().enumerate() {
      assert_eq!(hit.external_id, numeric_id(i, 9));
    }
  }
}

/// fsm 重填臂复用回归（票：wvector-fsm-refill-reuse-arm-drops-marked-id）：
/// churn 超出快速队列容量（FAST_SIZE=1024）令每轮删除溢出、每轮插入命中
/// 扫块重填臂，断言 card（total_used 记账）与内部 id 水位（next_id 铸造）
/// 零漂移——重填命中必交付已删 id，不得吞丢转铸致 id 空间耗散。
#[compio::test]
async fn fsm_refill_reuse_churn_zero_drift() {
  const ROUNDS: usize = 3;
  const N: usize = 1150;

  let service = DiskANNService::default();
  assert_eq!(
    service
      .create_index(
        8,
        config(2, VectorQuantType::NoQuant, VectorDistanceMetricType::L2),
        callbacks()
      )
      .await,
    Ok(false)
  );
  assert_eq!(
    service.insert(8, b"s", &f32_bytes(&[0.0, 0.0]), b"").await,
    DiskAnnInsertResult::True
  );

  let mut watermark = 0u32;
  for round in 0..ROUNDS {
    // 插入 1150 枚（> 队列容量，首铸后每轮必有溢出删除→重填命中）
    let mut eids: Vec<Vec<u8>> = Vec::with_capacity(N);
    for i in 0..N {
      let eid = format!("r{round}i{i}").into_bytes();
      assert_eq!(
        service
          .insert(8, &eid, &f32_bytes(&[i as f32, round as f32]), b"")
          .await,
        DiskAnnInsertResult::True
      );
      eids.push(eid);
    }
    // card = total_used 记账（排除起点）：泄漏幽灵 id 在此虚增暴露
    assert_eq!(service.card(8), 1 + N as u64, "第 {round} 轮 card 虚增");
    // id 水位：首铸轮定格后，复用轮全部交付自已删集，水位零推进
    let max_id = {
      let mut m = 0u32;
      for eid in &eids {
        m = m.max(service.internal_id_of(8, eid).await.unwrap().unwrap());
      }
      m
    };
    if round == 0 {
      watermark = max_id;
    } else {
      assert_eq!(
        max_id, watermark,
        "第 {round} 轮 id 水位漂移（重填吞丢转铸）"
      );
    }
    for eid in &eids {
      assert!(service.remove(8, eid).await.unwrap());
    }
    assert_eq!(service.card(8), 1, "第 {round} 轮删除后记账未归位");
  }
}
