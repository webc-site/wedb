//! 引擎在线置换钩子束三件全举重挂回归（工单 zcode-r137c-snaplock2 宗一）
//!
//! 契约（wedb/src/server/cluster_provider/checkpoint.rs:swap_online_store
//! 「投槽前挂载保证引擎可见即钩子在场，不存在无钩子写入窗口」）：rust 引擎
//! 实例置换形态下，wkv 引擎实例级三件 OnceLock 钩（watch_hook、event_sink、
//! delete_miss_hook，全集枚举见 wkv::session::EngineHookSlots）必须全部由宿主
//! 钩子束 engine_swap_hook_bundle 在投槽前对换入引擎重挂。修复前束仅重挂前两件，
//! 第三件 delete_miss_hook 唯一注入口是逐连接 get_session 装饰链——换引擎后
//! 回放会话/紧缩先行臂脱钩，对新引擎的缺席删除观测分裂至首个客户端会话装饰
//! 才收敛（嵌入式宿主永久）。
//!
//! 判别两面：
//! 1. 全枚举换面断言——swap_online_store 直驱后换入引擎 engine_hook_slots()
//!    三字段恒在位（wkv 新增引擎级钩时本构造字面量即编译红，防第四钩再漏）；
//! 2. 行为面——换槽后经新引擎会话对不存在键 delete，缺席观测钩在场即摘除
//!    向量登记表镜像（修复前维持缺席删除原口径：返回 false、幽灵镜像驻留）。
//!
//! 对标夹具：replica_swap_store_desync.rs 直驱 swap_online_store 同形态，
//! 宿主侧取真 StorageSessionProvider 装配（AOF 点亮，事件汇臂在位）。

use std::sync::Arc;

use compio::runtime::Runtime;
use wbase::hash_slot::slot_of;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::cluster_provider::ClusterProvider;
use wkv::{EngineHookSlots, WedbStore};
use wnode::{
  resp::vector::{
    resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
    vector_store_callbacks::ActiveVectorSessionGuard,
  },
  service::StorageSessionProvider,
};
use wnode_test::session_factory;
use wtest_base::test_store_config;
use wval::SessionPrefixBuf;

const SLOT0: u16 = slot_of(0, 0);

#[test]
fn swap_in_engine_gets_full_hook_bundle_and_delete_miss_observed() {
  let rt = Runtime::new().expect("compio runtime");
  rt.block_on(async {
    let dir = tempfile::tempdir().expect("tempdir");

    // ===== 宿主真装配（AOF 点亮：钩子束三臂齐挂的前提）
    let host = StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir.path().join("node").join("host.db"),
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open host with aof")
    .with_vector_set_preview(true);
    let cluster = ClusterProvider::new();
    cluster.set_store(host.store());
    cluster.set_store_swap_slot(host.store_swap_slot());
    cluster.set_database_manager(Arc::clone(&host.database_manager));
    cluster.set_engine_swap_hooks(host.engine_swap_hook_bundle());

    // ===== 旧引擎上建向量集（VADD 直驱臂与 diskless 同步夹具同形）
    let vm = Arc::clone(&host.vector_manager);
    {
      let bind_sess = host.store().new_session().expect("bind session");
      let _domain = ActiveVectorSessionGuard::bind(&bind_sess);
      let vsess = RespServerSessionVectors::new(Arc::clone(&vm));
      let reply = vsess
        .network_vadd(
          SessionPrefixBuf::ROOT.as_slice(),
          &[
            b"vs:set", b"VALUES", b"4", b"1.5", b"-2.5", b"0.25", b"4.0", b"e1",
          ],
          SLOT0,
          false,
        )
        .await;
      assert!(
        !matches!(reply, VectorReply::Error(_)),
        "旧引擎 VADD 须成功"
      );
    }
    assert!(
      vm.read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), b"vs:set")
        .is_some(),
      "前置条件：向量集已入内存登记表"
    );

    // ===== 直驱置换漏斗：换入全新引擎（副本检查点导入产物形态）
    let swapped_device = Arc::new(
      SegmentedDevice::single_file(dir.path().join("node").join("swapped.db"))
        .expect("swapped device"),
    );
    let new_store =
      Arc::new(WedbStore::open(test_store_config(), swapped_device).expect("swapped store"));
    cluster.swap_online_store(Arc::clone(&new_store));

    // ===== 判别 1：全枚举换面断言——三件 OnceLock 先于投槽全部在位
    assert_eq!(
      new_store.engine_hook_slots(),
      EngineHookSlots {
        watch_hook: true,
        event_sink: true,
        delete_miss_hook: true,
      },
      "换入引擎必须逐件承接 EngineHookSlots 全举三钩"
    );

    // ===== 判别 2：换槽后对不存在键 DEL 即触登记表摘除
    let s_new = new_store.new_session().expect("new session");
    assert!(
      s_new.delete(b"vs:set").await.expect("delete"),
      "钩子在场的缺席删除命中登记即视同删除成功"
    );
    assert!(
      vm.read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), b"vs:set")
        .is_none(),
      "换入引擎缺席删除必须经 delete_miss_hook 摘除登记表镜像，不留幽灵"
    );
  });
}
