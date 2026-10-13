#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! SWAPDB 跨租户槽位碰撞回归（换库存户零向量集时租户过滤器不得退化全局扫）
//!
//! 缺陷面：swap_database_slots 的换章循环曾有 `!target_contexts.is_empty() &&`
//! 前置——换库存户（vns）名下零向量集时 target_contexts 为空集，租户过滤器
//! 退化为全局扫：16384 槽空间多租户 (ns,db) 槽位碰撞是稳态，他租户在用
//! context 凡盖章槽位与本次换章槽对碰撞即被静默换章并持久化，其向量集在
//! 发现面（get_namespaces_for_hash_slots / get_vector_set_keys_for_slots_with）
//! 脱离真槽、转挂他租户槽位（迁移面跨租户错发）。
//!
//! 修复契约：过滤器恒生效——换库存户零向量集即空转返回（零向量集租户本无
//! 自己 context 可换；回放侧流序保证有向量集时登记项必先于 DbSwap 条目在册）。
//!
//! 场景：租户 ns=0 在 db0 VADD（context 盖章 slot_of(0,0)）；租户 ns=1 零向量集，
//! 搜寻 slot_of(1,dbA)==slot_of(0,0) 的碰撞对后 SWAPDB 1 dbA dbB——ns0 发现面
//! 必须前后逐值不变（修复前该断言必红：碰撞换章把 slot0 改挂）。

use std::{collections::BTreeSet, sync::Arc};

use aok::Void;
use wbase::hash_slot::slot_of;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::{AofNode, bind_vector_domain, open_aof_node, vector_manager_of, wire_vector_aof};
use wtest_base::resp_frame as frame;

#[compio::test]
async fn swapdb_on_vectorless_tenant_preserves_colliding_tenant_slots() -> Void {
  let AofNode {
    _dir,
    device,
    store,
    aof,
    cp_dir,
    ..
  } = open_aof_node("swapdb_cross_tenant")?;

  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    Arc::clone(&device),
    cp_dir.clone(),
    Some(Arc::clone(&aof)),
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(cp_dir.clone(), db));
  let vm = vector_manager_of(&store);
  let _vector_domain = bind_vector_domain(&vm);
  wire_vector_aof(&store, &aof, &vm);
  mgr.attach_vector_manager(Arc::clone(&vm));

  let api = StoreGarnetApi::new(store.new_session()?)
    .with_database_manager(Arc::clone(&mgr))
    .with_vector_manager(Arc::clone(&vm));
  let mut consumer =
    RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api));

  // 1. 租户 ns=0 在 db0 VADD：context 盖章 slot_a = slot_of(0, 0)
  let ns0 = 0u64;
  let db0 = 0u64;
  let slot_a = slot_of(ns0, db0);
  let vec_data = [0u8, 0, 128, 63, 0, 0, 0, 64]; // [1.0, 2.0]
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(&frame(&[
    b"VADD",
    b"vs_collision",
    b"FP32",
    &vec_data,
    b"e1",
    b"NOQUANT",
  ]));
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应完整消费");
  if let Some(slow) = consumer.take_slow_wait() {
    let reply = slow.resolve().await;
    consumer.resolve_slow_wait_into(&reply, &mut resp);
  }
  assert_eq!(resp, b":1\r\n", "VADD 须成功");

  let keys_before =
    vm.get_vector_set_keys_for_slots_with(&BTreeSet::from([i32::from(slot_a)]), |vns, vdb| {
      store
        .vdb
        .logic_domain_of(vns, vdb)
        .map(|(lns, ldb)| slot_of(lns, ldb))
    });
  assert_eq!(keys_before.len(), 1, "换章前 slot_a 须发现 vs_collision");
  // 发现面快照（值域为在用 context id 集非租户集，前后相等即换章零波及）
  let ctxs_before = vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(slot_a)]));

  // 2. 租户 ns=1 零向量集：搜寻 slot_of(1, dbA) == slot_a 的碰撞对
  let ns1 = 1u64;
  let mut db_a = None;
  for candidate in 1u64..(1 << 20) {
    if slot_of(ns1, candidate) == slot_a {
      db_a = Some(candidate);
      break;
    }
  }
  let db_a = db_a.expect("ns=1 域内必存在与 slot_a 碰撞的 db");
  // db_a 自 1 起搜恒 ≥ 1，db_b 取 0 即异库（swap 的 db1==db2 早退护栏不触发）
  let db_b = 0u64;
  assert_eq!(slot_of(ns1, db_a), slot_a, "碰撞对须成立");

  // 3. 零向量集租户 ns=1 执行 SWAPDB：不得波及 ns0 的在用 context
  vm.swap_database_slots(ns1, ns1, db_a, db_b).await;

  // 发现面键集前后逐值不变（现算口径：他租户域映射未被零向量集租户换库波及；
  // 盖章槽位面由下方在用 context 集前后比对承接）
  let keys_after =
    vm.get_vector_set_keys_for_slots_with(&BTreeSet::from([i32::from(slot_a)]), |vns, vdb| {
      store
        .vdb
        .logic_domain_of(vns, vdb)
        .map(|(lns, ldb)| slot_of(lns, ldb))
    });
  assert_eq!(
    keys_before, keys_after,
    "零向量集租户换库不得改写他租户在用 context 的盖章槽位"
  );
  let ctxs_after = vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(slot_a)]));
  assert_eq!(
    ctxs_before, ctxs_after,
    "换库后 slot_a 的在用 context 集须前后逐值不变: {ctxs_before:?} → {ctxs_after:?}"
  );
  Ok(())
}
