#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! VLINKS 悬垂邻接毒化回归（票：wvector-vlinks-dangling-adjacency-poisons-echo）
//!
//! 缺陷形态：删除体先摘数据并释放 fsm 槽位（delete_element 的 mark_free
//! 在前），图边回收（inplace_delete 的 add_edge_and_prune / drop_adj_list）
//! 在其后；`service.remove` 以 is_ok 吞错，半途失败使他人邻接表残留悬空 id
//! 成稳态（全仓无图完整性巡检臂，不自愈）。neighbors 遍历臂对悬空邻居
//! get_full_vector/to_external_id 以 `?` 整链上抛、links_of `.ok()?` 折
//! None、network_vlinks 与「元素缺席」同形写 null——单个已删邻居毒化整包，
//! 存活成员 VISMEMBER 真、VLINKS null 命令面自相矛盾。
//!
//! 修复契约（null 单源保留给键/元素真缺席）：neighbors 迭代臂对每个邻居
//! 先 vector_iid_exists（fsm 占用位，与 status_by_internal_id 同单源）前置
//! 过滤，向量/映射读取残余失败跳过留痕，不再 `?` 整链上抛。
//!
//! 注入面：共享内存桥 `wvector_test::MemStore` 注入槽按 armed 项类型对
//! **rmw** 落盘口返回 false——删除体五记录删除走 direct delete，图阶段边
//! 回收（set_neighbors）走 rmw，故 Neighbors 域 rmw 故障恰好只斩断边回收，
//! 构成「数据已删 + 悬空边残留」稳态（形态同 insert_graph_stage_failure.rs 族）。

use std::sync::Arc;

use wvector::{Callbacks, DiskANNService, DiskAnnInsertResult, VectorQuantType, store::Term};
use wvector_test::{FaultArm, MemStore, f32_bytes, test_config};

const CTX: u64 = 8;

/// 悬空邻接不毒化整包：VREM 半途失败（数据已删、边回收失败）后，存活
/// 成员 links_of 恒 Some 且跳过已删邻居；null 单源仍留给真缺席。
#[compio::test]
async fn dangling_neighbor_does_not_poison_links() {
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
  assert_eq!(
    service
      .insert(CTX, b"n1", &f32_bytes(&[0.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );

  // 图连边基线：k1 的层 0 邻接含 n1（回显面健全性前提）
  let links = service.links_of(CTX, b"k1").await.unwrap();
  assert!(
    links.iter().any(|(id, _)| id.as_slice() == b"n1"),
    "k1 应邻接 n1: {links:?}"
  );

  // 注入 Neighbors 域 rmw 故障：删除体（direct delete + mark_free）完成后
  // 图阶段边回收（set_neighbors 走 rmw）失败 → VREM 半途失败稳态；
  // 写失败透明上抛存储错误（本票契约：故障不得折叠成删除失败假阴性 false）
  store.arm(FaultArm::Rmw, Term::Neighbors);
  assert!(
    service.remove(CTX, b"n1").await.is_err(),
    "边回收写失败须上抛存储错误，禁折 false"
  );
  store.disarm();

  // 删除体已生效：n1 映射摘除、槽位归还
  assert!(!service.check_external_id_valid(CTX, b"n1").await.unwrap());
  assert_eq!(service.card(CTX), 1);

  // 核心：悬空邻接不得毒化整包（修复前 links_of 折 None），且已删邻居跳过
  let links = service.links_of(CTX, b"k1").await.unwrap();
  assert!(
    !links.iter().any(|(id, _)| id.as_slice() == b"n1"),
    "悬空邻居必须跳过回显: {links:?}"
  );

  // null 单源：键/元素真缺席恒 None
  assert!(service.links_of(CTX, b"n1").await.is_none());
  assert!(service.links_of(CTX + 1, b"k1").await.is_none());

  // 槽位复用（LIFO 归还 iid）后 k1 回显面仍健康不毒化
  assert_eq!(
    service
      .insert(CTX, b"n2", &f32_bytes(&[1.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  assert!(service.links_of(CTX, b"k1").await.is_some());
}
