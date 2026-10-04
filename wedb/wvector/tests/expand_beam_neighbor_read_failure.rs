//! beam 展开臂邻接读失败折叠回归（票：wvector-expand-beam-neighbor-read-failure-folded-empty-adjacency）
//!
//! 缺陷形态：DynamicAccessor 三条 beam 展开臂（expand_beam / expand_beam_filtered /
//! expand_beam_accept_only）对 `provider.get_neighbors(...).await` 的 bool 返回值裸
//! 丢弃；下游 cache.rs get_neighbors 读失败臂 guard.finish(0) 置空邻接——邻接冷读
//! 失败被折叠成「空邻域」，beam 静默缺边、召回退化零信号，与同仓剪枝臂
//! DelegateNeighborAccessor::get_neighbors（false→Err(ANNError) 透明上抛）及
//! neighbors 命令臂（cache.rs:238 false→Err(StoreError::Read)）构成双轨。
//!
//! 修复契约：三臂比照剪枝臂既有机制把 false 单轨映射 Err 上抛（检索失败帧），
//! 零新机制、不动 cache.rs 读语义（宿主读三态化系 §181 另案，本票禁走）。
//!
//! 注入面：共享内存桥 `wvector_test::MemStore` 注入槽在 **read** 臂按 armed
//! 项类型对 Neighbors 域返回 false——检索期向量收割走 Vector 域 read_multi、
//! 映射与占用位走各自项类型 read，故 Neighbors 域读故障恰好只斩邻接冷读
//! （形态同 insert_graph_stage_failure.rs / vlinks_dangling_neighbor.rs 族的
//! rmw 注入面，臂位由 rmw 换 read）。`filter_pass` 置真放行，保内联过滤臂
//! 正常推进到二次展开。
//!
//! 臂位覆盖说明：expand_beam 经无过滤 KNN 检索驱动、expand_beam_filtered 经
//! 内联过滤检索（InlineFilterSearch）驱动；expand_beam_accept_only 臂折叠点
//! 同型同修（同文件三处同一映射），但其唯一库侧调用方为 MultihopFilterSearch
//! （webc-diskann search/multihop_filter_search.rs:221），本仓公开检索面
//! （DiskANNService search_vector/search_element 及其过滤形）不派发该型参数，
//! 集成册无法经真实 DynamicAccessor 驱动该臂——如实登记为本册覆盖边界，
//! 不为凑臂新造公开入口（零新机制）。

use std::sync::{Arc, atomic::Ordering};

use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, SearchParams, VectorQuantType, store::Term,
};
use wvector_test::{FaultArm, MemStore, f32_bytes, test_config};

const CTX: u64 = 8;

fn params(count: usize) -> SearchParams {
  SearchParams {
    count,
    search_exploration_factor: 200,
    filter_len: 0,
    max_filtering_effort: 0,
  }
}

fn params_filtered(count: usize) -> SearchParams {
  SearchParams {
    count,
    search_exploration_factor: 200,
    filter_len: 1,
    max_filtering_effort: 100,
  }
}

/// 铸造集合：插 16 条用户向量建图（起点与各元素邻接记录落盘），返回服务。
async fn seed(store: &Arc<MemStore>) -> DiskANNService<MemStore> {
  let service = DiskANNService::default();
  assert_eq!(
    service
      .create_index(
        CTX,
        test_config(VectorQuantType::NoQuant),
        Callbacks::new(Arc::clone(store))
      )
      .await,
    Ok(false)
  );
  for i in 0..16 {
    let x = (i % 4) as f32 * 0.5;
    let y = (i / 4) as f32 * 0.5;
    let eid = format!("e{i:0>3}");
    assert_eq!(
      service
        .insert(CTX, eid.as_bytes(), &f32_bytes(&[x, y]), b"")
        .await,
      DiskAnnInsertResult::True,
      "第 {i} 条插入失败"
    );
  }
  service
}

/// expand_beam 臂：Neighbors 域读故障下无过滤检索必须报错帧；解除后同查询
/// 正常出结果——证明错误帧恰源于邻接冷读失败，而非他因。
#[compio::test]
async fn expand_beam_surfaces_adjacency_read_failure() {
  let store = Arc::new(MemStore::new());
  // filter 恒真放行：保内联过滤臂推进到二次展开（注障靶位）
  store.filter_pass.store(true, Ordering::Release);
  let service = seed(&store).await;
  let query = f32_bytes(&[0.0, 0.0]);

  // 基线健全性：无故障检索出结果
  let out = service
    .search_vector(CTX, &query, params(10))
    .await
    .expect("无故障检索应成功");
  assert!(out.found >= 1, "基线检索应有命中");

  // 注入 Neighbors 域读故障：起点臂走邻接缓存仍真，二次展开元素邻接冷读
  // 失败 ⇒ expand_beam 必须上抛错误帧（修复前折叠空邻域静默半截，返回 Ok）
  store.arm(FaultArm::Read, Term::Neighbors);
  assert!(
    service
      .search_vector(CTX, &query, params(10))
      .await
      .is_err(),
    "邻接读失败须上抛检索错误帧，禁折叠空邻域静默半截"
  );

  // 解除后同查询恢复成功：错误帧确由注入窗致因
  store.disarm();
  let out = service
    .search_vector(CTX, &query, params(10))
    .await
    .expect("解除注入后检索应恢复成功");
  assert!(out.found >= 1, "解除注入后同查询应正常出结果");
}

/// expand_beam_filtered 臂：内联过滤检索（InlineFilterSearch）在 Neighbors 域
/// 读故障下同样必须报错帧，与剪枝臂单轨同形。
#[compio::test]
async fn expand_beam_filtered_surfaces_adjacency_read_failure() {
  let store = Arc::new(MemStore::new());
  store.filter_pass.store(true, Ordering::Release);
  let service = seed(&store).await;
  let query = f32_bytes(&[0.0, 0.0]);

  // 基线健全性：无故障过滤检索出结果（filter 恒真全命中）
  let out = service
    .search_vector(CTX, &query, params_filtered(10))
    .await
    .expect("无故障过滤检索应成功");
  assert!(out.found >= 1, "基线过滤检索应有命中");

  store.arm(FaultArm::Read, Term::Neighbors);
  assert!(
    service
      .search_vector(CTX, &query, params_filtered(10))
      .await
      .is_err(),
    "过滤臂邻接读失败须上抛检索错误帧，禁折叠空邻域静默半截"
  );

  store.disarm();
  let out = service
    .search_vector(CTX, &query, params_filtered(10))
    .await
    .expect("解除注入后过滤检索应恢复成功");
  assert!(out.found >= 1, "解除注入后同过滤查询应正常出结果");
}
