//! 向量集跨节点集群迁移发送驱动
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionSlots.cs:ReserveDestinationVectorSetsAsync
//! 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionSlots.cs:CreateAndRunMigrateTasksAsync（向量集收尾段）
//! 在 garnet 中的相对路径: libs/server/Resp/Vector/VectorManager.Migration.cs（帧序列化语义）
//!
//! 编排（rust 存储模型的差异投影，见 vector_manager_migration.rs 模块注释）：
//! 1. `CLUSTER RESERVE VECTOR_SET_CONTEXTS` 在目标端预留上下文并建立
//!    源→目标重映射（对标 ReserveDestinationVectorSetsAsync 的 namespaceMap）
//! 2. 逐键发送索引帧（kind=5，上下文已重映射 + index_ptr=0）——目标端先建
//!    内存索引
//! 3. 逐键枚举元素按批发送元素帧（kind=6，VADD 语义载荷）——目标端经
//!    既有执行域真实插入（内存图 + 磁盘记录 + AOF 合成写）
//! 4. DELETING：非 copy 逐键登记清理并摘除登记表项（对标 DeleteVectorSet，
//!    键随之从源端消失）
//!
//! SLOTS 链全程 sketch 门控（INITIALIZING → TRANSMITTING → DELETING，相位
//! 切换后纪元静止等待），停等帧经 [`send_payload_and_wait`] 限时并联动会话
//! 取消令牌，与 RangeIndex 通道同构；KEYS 链走门控外置路径（外层 sketch
//! 覆盖全部请求键，帧传输核 [`transmit_vector_set_frames`]（已归位
//! sync_transport）不触碰
//! sketch，删除并入外层统一 DELETING 收口，见 migrate_driver/keys.rs）。

use std::time::Duration;

use wnode::resp::vector::{
  vector_manager::{INDEX_SIZE_BYTES, VectorManager},
  vector_manager_locking::registry_user_key,
};

use crate::{
  client::GarnetClient,
  error::Result,
  server::{
    migration::{
      migrate_driver::send_payload_and_wait,
      migrate_session::{MigrateSession, MigrateTaskSpec},
      sketch_status::SketchStatus,
    },
    sync_transport::transmit_vector_set_frames,
  },
};

/// 迁移向量集键全集（SLOTS 链专属带 sketch 重置形态，与 RangeIndex 通道
/// 同构门控；KEYS 链走门控外置路径：直调 [`transmit_vector_set_frames`]
/// 以及外层统一 DELETING 收口，见 migrate_driver/keys.rs）。
///
/// 返回 false 表示远端拒绝/装配缺失（判败走 recover）；Err 为停等超时/取消
pub async fn migrate_vector_set_keys_async(
  client: &GarnetClient,
  session: &MigrateSession,
  vm: Option<&VectorManager>,
  spec: &MigrateTaskSpec,
  vector_set_keys: &[(Vec<u8>, [u8; INDEX_SIZE_BYTES])],
  max_chunk: usize,
  timeout: Duration,
) -> Result<bool> {
  if vector_set_keys.is_empty() {
    return Ok(true);
  }
  let Some(vm) = vm else {
    log::error!("向量集迁移需向量集合管理器装配，本次迁移拒绝");
    return Ok(false);
  };

  log::warn!(
    "MigrateVectorSetKeysAsync: migrating {} Vector Set keys",
    vector_set_keys.len()
  );

  // sketch 门控复位 + 收录（对标 MigrateRangeIndexKeysAsync 的门控形态；
  // 键门按剥域用户键收录，与命令面键域一致）
  session.sketch.clear();
  session.sketch.set_status(SketchStatus::Initializing);
  for (rk, _) in vector_set_keys {
    let key = registry_user_key(rk);
    session.sketch.hash_and_store(key);
  }

  // TRANSMITTING：快照与传输期间阻止写操作
  session.sketch.set_status(SketchStatus::Transmitting);
  // 纪元静止等待：等 TRANSMITTING 键门对全会话生效后再传输（与
  // MigrateRangeIndexKeysAsync 门控同构，对位
  // MigrateSession.RangeIndex.cs:114；返值忽略：C# 无限自旋，rust 有界放行）
  let _ = session
    .cluster_provider
    .bump_and_wait_for_epoch_transition_async()
    .await;

  let res = transmit_vector_set_frames(
    client,
    vm,
    vector_set_keys,
    max_chunk,
    || session.is_cancelled(),
    async |payload| send_payload_and_wait(client, session, timeout, spec, payload).await,
  )
  .await;

  match res {
    Ok(true) => {}
    Ok(false) => {
      session.sketch.clear();
      return Ok(false);
    }
    Err(e) => {
      session.sketch.clear();
      return Err(e);
    }
  }

  // 4. 非 copy：源端删除（登记清理 + 摘除登记表项，键消失）
  if !spec.copy_option {
    session.sketch.set_status(SketchStatus::Deleting);
    // 纪元静止等待：等 DELETING 键门对全会话生效后再落删除（对位
    // MigrateSession.RangeIndex.cs:135；返值忽略口径同上）
    let _ = session
      .cluster_provider
      .bump_and_wait_for_epoch_transition_async()
      .await;
    // 源端删除按复合键直删（含源端会话域，与枚举产物同域）
    for (rk, src_index) in vector_set_keys {
      vm.delete_migrated_vector_set_of(rk, src_index);
    }
  }

  session.sketch.clear();
  Ok(true)
}
