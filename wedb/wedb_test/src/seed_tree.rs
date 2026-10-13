//! 升阶分层集合造键单源（wbftree 升阶页存储 Hash 样本）
//!
//! 收口 cluster_migration / diskless_sync_ri_vector /
//! migrate_deleting_unheld_claim / migrate_epoch_drain_failclose 四册逐字
//! 同形的 `promote_collection_to_bftree` 造键（Hash 双字段 f1/f2 样本）：
//! SLOTS 发现面 load_collection_stub 收录、带外分块流通道迁移的判据形态。
//! 消费面经 `wedb_test::seed_tree` 引用（原 common/ 直挂面已收口进本 crate）。
//!

use std::sync::Arc;

use wdev::SegmentedDevice;
use wkv::WedbStore;
use wval::GarnetObjectType;

/// 造 wbftree 升阶页存储集合键（Hash，f1=v1 / f2=v2 双字段样本）
pub async fn seed_tree(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) {
  let sess = store.new_session().unwrap();
  sess
    .promote_collection_to_bftree(
      key,
      GarnetObjectType::Hash,
      vec![
        (b"f1".to_vec(), b"v1".to_vec()),
        (b"f2".to_vec(), b"v2".to_vec()),
      ],
      i64::MAX,
      false,
    )
    .await
    .unwrap();
}
