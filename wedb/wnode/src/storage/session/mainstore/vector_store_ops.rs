//! 向量写钩操作面（对标 libs/server/Storage/Session/MainStore/VectorStoreOps.cs，
//! C# 为 StorageSession partial）
//!
//! 装配期注入引擎的钩子族：向量登记表写面 WATCH 版本推进（物理域换算逻辑
//! 域后与主存储写面落同版本轨槽）、向量集登记表缺席删除观测（真异步闭环）。
//! 装配侧 use 路径经 `storage_session` 模块 re-export 保持稳定。

use std::sync::Arc;

use wkv::{DeleteMissHook, WatchHook};
use wtxn::{TxnKeyEntryComparison, WatchVersionMap};
use wval::SessionPrefixBuf;

use crate::{
  resp::vector::vector_manager::VectorManager,
  service::{SharedStore, StoreSwapSlot},
};

/// 向量登记表写面版本轨推进上下文（装配期初引擎 + 在线置换槽 + 版本表）
struct VectorVersionBump {
  store: SharedStore<wdev::SegmentedDevice>,
  store_swap: StoreSwapSlot,
  map: Arc<WatchVersionMap>,
}

/// 构造向量登记表写面的 WATCH 版本推进钩子（装配期经
/// `VectorManager::set_watch_bump` 一次性注入）
///
/// 向量写漏斗（try_add/try_remove/try_set_attribute/rename 新旧键与 AOF
/// 重放臂）以**物理**会话前缀寻址登记表（`registry_key` 复合键口径，随
/// 换号域迁移属既定架构），而版本轨=逻辑域：本装配钩子在构槽单点前将
/// 入参物理域经 `VirtualDbManager::version_domain_of` 换算为逻辑域再交
/// [`TxnKeyEntryComparison::scoped_key_hash`]，与主存储写面
/// [`crate::storage::session::storage_session::version_map_watch_hook`] 落同版本轨槽（换号后向量改写对在途 WATCH
/// 同样必 abort，对位 C# 向量写经本库 VersionMap 单表推进）。当前引擎经
/// 置换槽现取、回落装配期初值（与 `crate::service::build_txn_lock_table`
/// 锁源同形态——向量钩子 OnceLock 一次注入跨置换存活，禁钉死旧引擎映射）
pub fn vector_version_watch_hook(
  store: SharedStore<wdev::SegmentedDevice>,
  store_swap: StoreSwapSlot,
  map: Arc<WatchVersionMap>,
) -> wkv::WatchHook {
  fn on_vector_write(ctx: &VectorVersionBump, prefix: &[u8], key: &[u8]) {
    // 入参前缀理论上恒为登记表寻址的 [NsVarint][DbVarint] 物理域；解码
    // 异常回根域 (0,0)——与未绑定会话口径一致，至多数值重合碰撞面、只多
    // abort 不少 abort，属安全侧
    let (pns, pdb) = SessionPrefixBuf::from_slice(prefix)
      .and_then(|buf| buf.decode())
      .unwrap_or((0, 0));
    let store = ctx
      .store_swap
      .get()
      .unwrap_or_else(|| Arc::clone(&ctx.store));
    let (lns, ldb) = store.vdb.version_domain_of(pns, pdb);
    ctx
      .map
      .increment_version(TxnKeyEntryComparison::scoped_key_hash(
        SessionPrefixBuf::new(lns, ldb).as_slice(),
        key,
      ) as u64);
  }

  WatchHook::new(
    Arc::new(VectorVersionBump {
      store,
      store_swap,
      map,
    }),
    on_vector_write,
  )
}

/// 构造向量集登记表缺席删除观测钩子（对标 C# MainStore RemoveKey 回调 →
/// VectorManager.RequestDeletion，GarnetRecordTriggers.OnDispose 的 Deleted 臂。
/// 观测臂为真异步：条带独占锁 + 登记写透 `.await` 闭环，无内联收割——
/// 同步快删路径遇钩子在场降级完整异步路由后归此收口，见 wkv DeleteMissHook）
pub fn vector_registry_delete_hook(vm: Arc<VectorManager>) -> wkv::DeleteMissHook {
  DeleteMissHook::new(move |prefix, key| {
    let vm = Arc::clone(&vm);
    Box::pin(async move { vm.delete_vector_set(prefix, key).await })
  })
}
