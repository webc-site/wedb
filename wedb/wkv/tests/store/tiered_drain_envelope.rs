//! 分层排空信封域幂等墓碑集成测试（task/ing/tiered-drain-envelope-tombstone.md）
//!
//! 缺陷口径：升阶收尾是两条独立物理写（落元记录 → 删信封），崩溃 / AOF 截断
//! 部分回放 / 信封删除 IO 失败留下「信封旧快照 + Meta 存根 + 树」双态残留；
//! 旧排空回收单点 handle_bftree_drain_and_delete 删键臂只清 Meta + 树 + TTL，
//! 删空后命令回落信封域双域探测，已删空集合以升阶时刻完整旧数据幽灵复活。
//!
//! 修复判据（一处收口，与 DEL 复合臂 delete() 双域墓碑同口径共用 delete_raw
//! 单原语）：keep_ttl=false 删键臂信封墓碑先于元记录落笔（回落域先死）；
//! keep_ttl=true 迁移臂绝不触碰信封域（重灌臂随后 promote 内删信封、降阶臂
//! 随后 obj_save 写回新信封）。C# 集合恒驻对象域单一物理域，删记录即连尾随
//! 字段同亡（libs/server/Storage/Functions/ObjectStore/RMWMethods.cs
//! InPlaceUpdaterWorker 的 HasRemoveKey → ExpireAndStop），双域清理属本仓
//! 分层自定义架构的消亡不变式，C# 测试集无逐例对位。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::TreeTuning;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wval::{GarnetObjectType, KeyTag};

use crate::support::open_store_in;

fn open_store(dir: &tempfile::TempDir, name: &str) -> aok::Result<Arc<WedbStore<SegmentedDevice>>> {
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  open_store_in(dir, name, config)
}

/// 构造双态残留键：升阶后手工塞回信封旧快照副本（等价信封删除臂失败/崩溃窗口）
async fn make_dual_state(
  s: &wkv::StoreSession<SegmentedDevice>,
  key: &[u8],
) -> aok::Result<Vec<u8>> {
  let stale = b"\x03stale-envelope-snapshot".to_vec();
  s.upsert_tag(key, KeyTag::ObjectEnvelope, &stale).await?;
  s.promote_collection_to_bftree(
    key,
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), b"v1".to_vec()),
      (b"f2".to_vec(), b"v2".to_vec()),
    ],
    i64::MAX,
  )
  .await?;
  let env_k = s.session_tag_key(KeyTag::ObjectEnvelope, key);
  assert!(
    !s.contains_key_raw(&env_k).await?,
    "promote 收尾必删信封（且删除失败上抛，不再静默 Ok）"
  );
  s.upsert_tag(key, KeyTag::ObjectEnvelope, &stale).await?;
  Ok(stale)
}

/// 删键臂双域收口：双态残留键经排空回收后信封/Meta/TTL 三记录齐消亡，
/// 无任何域可供命令回落复活；二次排空幂等零错
#[test]
fn test_drain_dual_state_key_tombstones_envelope() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "drain_env.db")?;
    let s = store.new_session()?;
    make_dual_state(&s, b"h").await?;
    s.put_ttl(b"h", i64::MAX / 2).await?;

    s.handle_bftree_drain_and_delete(b"h", false).await?;

    let env_k = s.session_tag_key(KeyTag::ObjectEnvelope, b"h");
    assert!(
      !s.contains_key_raw(&env_k).await?,
      "删键臂后信封域必须消亡，旧快照不得残留"
    );
    assert!(
      s.load_collection_stub(b"h").await?.is_none(),
      "删键臂后元记录必须消亡"
    );
    assert!(
      !s.contains_key(b"h").await?,
      "EXISTS 判据三域齐落空：幽灵复活即此处变红"
    );
    assert!(
      s.ttl_of(b"h").await?.is_none(),
      "删键臂随键 TTL 必须一并清除，杜绝孤儿 TTL"
    );
    assert!(
      store.range_index().get_tree(b"h").is_none(),
      "树实例必须注销"
    );

    // 幂等：两域皆已消亡的二次排空零写零错（信封墓碑臂探针落空）
    s.handle_bftree_drain_and_delete(b"h", false).await?;
    OK
  })
}

/// 迁移臂护栏（keep_ttl=true）：键全程存活，信封域与 TTL 旁路一律不触碰，
/// 只清退 Meta + 树——杜绝重灌/降阶迁移臂「先墓碑后写回」自相残杀
#[test]
fn test_drain_migration_arm_preserves_envelope_and_ttl() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "drain_keep.db")?;
    let s = store.new_session()?;
    let stale = make_dual_state(&s, b"h").await?;
    s.put_ttl(b"h", i64::MAX / 2).await?;

    s.handle_bftree_drain_and_delete(b"h", true).await?;

    let env_k = s.session_tag_key(KeyTag::ObjectEnvelope, b"h");
    assert_eq!(
      s.read_raw(&env_k).await?.as_deref(),
      Some(stale.as_slice()),
      "迁移臂信封域必须原样存活（降阶写回臂的旧信封、重灌臂的残留都不得被误杀）"
    );
    assert!(
      s.load_collection_stub(b"h").await?.is_none(),
      "迁移臂照常墓碑元记录"
    );
    assert_eq!(
      s.ttl_of(b"h").await?,
      Some(i64::MAX / 2),
      "迁移臂键级 TTL 逐 tick 原样保留"
    );
    OK
  })
}

/// DEL 复合臂行为不变：双态残留键经 delete() 恰一次排空即两域齐清、应答 true
#[test]
fn test_delete_dual_state_key_unchanged() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "drain_del.db")?;
    let s = store.new_session()?;
    make_dual_state(&s, b"h").await?;

    assert!(
      s.delete(b"h").await?,
      "DEL 复合臂对双态残留键应答不变（meta 域命中即 true）"
    );
    assert!(!s.contains_key(b"h").await?, "DEL 后无任何域可复活");
    OK
  })
}

/// 纯 RangeIndex 键（设计上绝无信封记录）删空自愈：信封墓碑臂探针落空零写，
/// 行为与改前一致
#[test]
fn test_drain_pure_range_index_zero_envelope_write() -> Void {
  use wbftree::StorageBackendType;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "drain_ri.db")?;
    let s = store.new_session()?;
    let tune = TreeTuning::DEFAULT_RI_COLLECTION;
    s.range_index_create(b"idx", StorageBackendType::Disk, tune)
      .await?;
    s.range_index_set(b"idx", b"f", b"v").await?;

    s.handle_bftree_drain_and_delete(b"idx", false).await?;

    assert!(
      s.load_collection_stub(b"idx").await?.is_none(),
      "RI 键排空后元记录消亡"
    );
    assert!(!s.contains_key(b"idx").await?, "RI 键排空后 EXISTS 落空");
    OK
  })
}
