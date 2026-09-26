//! 装配面专用会话工厂收口锁测（工单 wnode-vector-registry-put-slow-arm-session-unbound）
//!
//! 契约（集成门禁红用例 cluster_reserve_contexts 归责回归的装配面锁，仿
//! wedb/tests/engine_swap_hook_bundle.rs 逐件全举断言先例，防装配件再漏）：
//! 1. `open_node_with_config` 三件套形态须自带钉死自身 store 的默认专用会话
//!    工厂——`bind_dedicated_session` 产出非 None 且产出即绑定当前执行域、
//!    守卫析构即解绑；否则 CLUSTER RESERVE 慢臂「兜底自持专用会话」前置断裂，
//!    写透 registry put 命中缺绑会话的失败口径；
//! 2. 工厂注入槽为 last-wins 覆写语义：装配面之后显式 attach 即刻覆写默认
//!    工厂且旧工厂即刻失效不回退（夹具手工 attach 面 vector_set_drop_cleanup
//!    等仍生效）。
//!
//! Node::open_with_config 覆写终态（from_parts 尾段置换槽工厂）由既有换引擎
//! 面锁测 engine_swap_hook_bundle.rs / diskless_sync_ri_vector.rs /
//! replica_diskbased_vector_rebuild.rs 保持绿承接，钉宗二不回退，本测不重复。

use std::sync::{
  Arc,
  atomic::{AtomicUsize, Ordering},
};

use compio::runtime::Runtime;
use wnode::{
  resp::vector::vector_store_callbacks::{
    ActiveDedicatedVectorSession, OwnedActiveVectorSession, active_vector_session_bound,
  },
  service::open_node_with_config,
};
use wtest_base::test_store_config;

#[test]
fn triplet_default_factory_and_last_wins_attach() {
  let rt = Runtime::new().expect("compio runtime");
  rt.block_on(async {
    let dir = tempfile::tempdir().expect("tempdir");
    // 真夹具：嵌入式三件套唯一装配入口，无 mock
    let (store, _broker, vm) =
      open_node_with_config(test_store_config(), dir.path().join("assembly.db"))
        .expect("三件套装配");

    // 判别 1：默认工厂在场——产出非 None、产出即绑定、析构即解绑
    assert!(
      !active_vector_session_bound(),
      "前置：本执行域初始未绑定向量会话"
    );
    let guard = vm
      .bind_dedicated_session()
      .expect("三件套形态必须钉自身 store 的默认专用会话工厂");
    assert!(
      active_vector_session_bound(),
      "工厂产出即已绑定当前执行域（兜底自持会话前置成立）"
    );
    drop(guard);
    assert!(
      !active_vector_session_bound(),
      "专用会话守卫析构即解绑并销毁会话"
    );

    // 判别 2：last-wins 覆写——装配面后手工 attach 即刻生效
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    let factory_store = Arc::clone(&store);
    vm.attach_dedicated_session_factory(Arc::new(move || {
      counter.fetch_add(1, Ordering::Relaxed);
      factory_store
        .new_session()
        .ok()
        .map(OwnedActiveVectorSession::new)
        .map(ActiveDedicatedVectorSession::from_bound)
    }));
    let over = vm
      .bind_dedicated_session()
      .expect("覆写后的手工工厂须产出会话");
    assert!(active_vector_session_bound(), "覆写工厂产出同样即绑执行域");
    drop(over);
    assert_eq!(
      hits.load(Ordering::Relaxed),
      1,
      "attach 即覆写：bind 必须走手工工厂而非装配期默认工厂"
    );

    // 覆写单向失效：后覆 None 工厂即刻接管，旧工厂绝不回退兜底
    vm.attach_dedicated_session_factory(Arc::new(|| None));
    assert!(
      vm.bind_dedicated_session().is_none(),
      "终次覆写工厂即刻接管，缺厂口径按 None 失败回报"
    );
    assert_eq!(
      hits.load(Ordering::Relaxed),
      1,
      "覆写 None 工厂后旧手工工厂不得再被调用"
    );
  });
}
