//! 大 Hash 打平存储（Flattened SubKey）与升降级路由端到端集成测试
//!
//! 验证点：
//! 1. 打平存储基础 CRUD 及 O(1) HLEN 直读；
//! 2. 从 Compact 紧凑编码自动升级到 Flattened 打平存储；
//! 3. DEL key 后的 O(1) 版本号栅栏秒删机制；
//! 4. 删空自愈（最后字段删完后 key 彻底消失与 TTL 清理）；
//! 5. 降级闭环：删减到迟滞门限以下自动降级回 Compact，迟滞区间 [16385, 32768]
//!    内双向均不切换（不震荡）。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wbase::time::now_ticks;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, TtlOpt, WedbStore};
use wval::{CollectionType, MetaValue, StorageEncoding};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// 构造独立测试库
async fn open_store(tag: &str) -> aok::Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("hash_flattened_{tag}.db")),
  )?);
  let mut config = StoreConfig::new(1024, 2 * 1024 * 1024, 16, 0.5)?;
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

#[test]
fn test_flattened_hash_basic_crud_and_hlen() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("crud").await?;
    let session = store.new_session()?;
    let key = b"flat_hash_test";

    // 1. 初始为空
    assert_eq!(session.flattened_hlen(key).await?, 0);
    assert_eq!(session.flattened_hget(key, b"f1").await?, None);
    assert!(!session.flattened_hexists(key, b"f1").await?);

    // 2. 插入字段
    assert!(session.flattened_hset(key, b"f1", b"v1").await?);
    assert!(session.flattened_hset(key, b"f2", b"v2").await?);
    // 重复插入相同字段应更新值并返回 false
    assert!(!session.flattened_hset(key, b"f1", b"v1_updated").await?);

    // 3. 验证 O(1) HLEN
    assert_eq!(session.flattened_hlen(key).await?, 2);

    // 4. 点查与判断存在
    assert_eq!(
      session.flattened_hget(key, b"f1").await?,
      Some(b"v1_updated".to_vec())
    );
    assert_eq!(
      session.flattened_hget(key, b"f2").await?,
      Some(b"v2".to_vec())
    );
    assert!(session.flattened_hexists(key, b"f1").await?);
    assert!(session.flattened_hexists(key, b"f2").await?);
    assert!(!session.flattened_hexists(key, b"f3").await?);

    // 5. 批量读取 HMGET
    let mvals = session.flattened_hmget(key, &[b"f1", b"f2", b"f3"]).await?;
    assert_eq!(mvals.len(), 3);
    assert_eq!(mvals[0], Some(b"v1_updated".to_vec()));
    assert_eq!(mvals[1], Some(b"v2".to_vec()));
    assert_eq!(mvals[2], None);

    // 6. 验证元数据存储编码为 Flattened
    let meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta.collection_type, CollectionType::Hash);
    assert_eq!(meta.encoding(), StorageEncoding::Flattened);
    assert_eq!(meta.size, 2);

    // 7. 删除单字段：2 项小 hash 满足迟滞降级双门限，自动降级回 Compact；
    //    flattened_* 直连算子此后视同不存在（返回默认值），数据经透明门面读取
    assert!(session.flattened_hdel(key, b"f1").await?);
    assert!(!session.flattened_hdel(key, b"f1").await?);
    assert_eq!(session.flattened_hlen(key).await?, 0);
    assert_eq!(session.flattened_hget(key, b"f2").await?, None);
    let meta_downgraded = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta_downgraded.encoding(), StorageEncoding::Compact);
    assert_eq!(session.hlen(key).await?, 1);
    assert_eq!(session.hget(key, b"f1").await?, None);
    assert_eq!(session.hget(key, b"f2").await?, Some(b"v2".to_vec()));

    OK
  })
}

#[test]
fn test_hash_auto_upgrade_from_compact_to_flattened() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("upgrade").await?;
    let session = store.new_session()?;
    let key = b"auto_upgrade_hash";

    // 1. 小数据量初始写入，默认为 Compact
    assert!(session.hset(key, b"k1", b"v1").await?);
    assert!(session.hset(key, b"k2", b"v2").await?);
    let meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta.encoding(), StorageEncoding::Compact);
    assert_eq!(session.hlen(key).await?, 2);

    // 2. 写入超出紧凑编码上限(>65535B)的大值，触发自动升级
    let big_value = vec![b'x'; 70000];
    assert!(session.hset(key, b"big_field", &big_value).await?);

    // 3. 验证自动升级为 Flattened
    let meta_after = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta_after.encoding(), StorageEncoding::Flattened);
    assert_eq!(session.hlen(key).await?, 3);

    // 4. 验证原本字段与超大字段全部完整可读
    assert_eq!(session.hget(key, b"k1").await?, Some(b"v1".to_vec()));
    assert_eq!(session.hget(key, b"k2").await?, Some(b"v2".to_vec()));
    assert_eq!(session.hget(key, b"big_field").await?, Some(big_value));

    // 5. 再次写入新字段走打平存储
    assert!(session.hset(key, b"k3", b"v3").await?);
    assert_eq!(session.hlen(key).await?, 4);
    assert_eq!(session.hget(key, b"k3").await?, Some(b"v3".to_vec()));

    OK
  })
}

#[test]
fn test_flattened_hash_version_fence_instant_delete() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("version_fence").await?;
    let session = store.new_session()?;
    let key = b"fence_hash";

    // 1. 建立打平存储并写入数据
    assert!(session.flattened_hset(key, b"f1", b"v1").await?);
    assert!(session.flattened_hset(key, b"f2", b"v2").await?);
    let meta = session.load_meta(key).await?.expect("meta exists");
    let old_key_id = meta.key_id;
    let old_version = meta.version;
    assert!(old_version > 0);

    // 2. 执行统一 DEL 语义
    assert!(session.delete(key).await?);

    // 3. 校验逻辑秒删：元记录与集合判定立即失效
    assert_eq!(session.load_meta(key).await?, None);
    assert!(!session.contains_key(key).await?);
    assert_eq!(session.hget(key, b"f1").await?, None);
    assert_eq!(session.hget(key, b"f2").await?, None);
    assert_eq!(session.hlen(key).await?, 0);

    // 4. 重建同名键：分配新 key_id，旧子键因版本号栅栏彻底隔离
    assert!(session.hset(key, b"f1", b"v1_new").await?);
    let new_meta = session.load_meta(key).await?.expect("meta exists");
    assert_ne!(new_meta.key_id, old_key_id);
    assert_eq!(session.hlen(key).await?, 1);
    assert_eq!(session.hget(key, b"f1").await?, Some(b"v1_new".to_vec()));
    // 旧字段 f2 绝不可见
    assert_eq!(session.hget(key, b"f2").await?, None);

    OK
  })
}

#[test]
fn test_flattened_hash_drain_self_healing() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("drain").await?;
    let session = store.new_session()?;
    let key = b"drain_hash";

    // 1. 创建打平哈希并插入 2 个字段
    assert!(session.flattened_hset(key, b"f1", b"v1").await?);
    assert!(session.flattened_hset(key, b"f2", b"v2").await?);
    assert_eq!(session.hlen(key).await?, 2);
    assert!(session.contains_key(key).await?);

    // 2. 删除第 1 个字段
    assert!(session.hdel(key, b"f1").await?);
    assert_eq!(session.hlen(key).await?, 1);
    assert!(session.contains_key(key).await?);

    // 3. 删除第 2 个字段（最后一个存活字段）
    assert!(session.hdel(key, b"f2").await?);

    // 4. 校验严格删空自愈：集合完全消亡，元数据被清理
    assert_eq!(session.hlen(key).await?, 0);
    assert!(!session.contains_key(key).await?);
    assert_eq!(session.load_meta(key).await?, None);
    assert_eq!(session.hget(key, b"f2").await?, None);

    OK
  })
}

#[test]
fn test_hash_hysteresis_thresholds() {
  assert!(!wkv::should_upgrade_hash(32768, 1024 * 1024));
  assert!(wkv::should_upgrade_hash(32769, 100));
  assert!(wkv::should_upgrade_hash(100, 1024 * 1024 + 1));

  assert!(wkv::should_downgrade_hash(16384, 512 * 1024));
  assert!(!wkv::should_downgrade_hash(16385, 100));
  assert!(!wkv::should_downgrade_hash(100, 512 * 1024 + 1));
}

#[test]
fn test_zero_copy_hget_with_and_flattened_hget_with() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("zero_copy").await?;
    let session = store.new_session()?;
    let key = b"zc_hash";

    // 1. Compact 模式下的 hget_with 零拷贝切片借用
    assert!(session.hset(key, b"f1", b"val_1").await?);
    let matched = session
      .hget_with(key, b"f1", |slice| slice == b"val_1")
      .await?;
    assert_eq!(matched, Some(true));

    // 2. 升级为 Flattened 打平存储
    let big_val = vec![b'y'; 70000];
    assert!(session.hset(key, b"big_f", &big_val).await?);

    // 3. 打平存储下的 hget_with 与 flattened_hget_with 零拷贝切片借用
    let len = session
      .hget_with(key, b"big_f", |slice| slice.len())
      .await?;
    assert_eq!(len, Some(70000));

    let f1_matches = session
      .flattened_hget_with(key, b"f1", |slice| slice == b"val_1")
      .await?;
    assert_eq!(f1_matches, Some(true));

    let non_existent = session.flattened_hget_with(key, b"none", |_| true).await?;
    assert_eq!(non_existent, None);

    OK
  })
}

#[test]
fn test_flattened_hset_migrates_existing_compact() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("migrate_compact").await?;
    let session = store.new_session()?;
    let key = b"compat_hash";

    // 1. 初始为 Compact 哈希
    assert!(session.hset(key, b"orig_1", b"val_1").await?);
    assert!(session.hset(key, b"orig_2", b"val_2").await?);
    let meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta.encoding(), StorageEncoding::Compact);

    // 2. 直接对 Compact 哈希调用 flattened_hset
    assert!(session.flattened_hset(key, b"flat_3", b"val_3").await?);

    // 3. 验证原字段与新字段全部完好，且元数据已升级为 Flattened
    let meta_after = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta_after.encoding(), StorageEncoding::Flattened);
    assert_eq!(session.hlen(key).await?, 3);

    assert_eq!(session.hget(key, b"orig_1").await?, Some(b"val_1".to_vec()));
    assert_eq!(session.hget(key, b"orig_2").await?, Some(b"val_2".to_vec()));
    assert_eq!(session.hget(key, b"flat_3").await?, Some(b"val_3".to_vec()));

    OK
  })
}

#[test]
fn test_flattened_to_compact_smooth_downgrade() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("smooth_downgrade").await?;
    let session = store.new_session()?;
    let key = b"downgrade_hash";

    // 1. 创建打平哈希
    assert!(session.flattened_hset(key, b"f1", b"v1").await?);
    assert!(session.flattened_hset(key, b"f2", b"v2").await?);
    assert!(session.flattened_hset(key, b"f3", b"v3").await?);
    let mut meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta.encoding(), StorageEncoding::Flattened);
    let old_version = meta.version;

    // 2. 验证满足 50% 迟滞降级条件 (3 <= 16384 且体积 << 512KB)
    let entries = [
      (b"f1".as_slice(), b"v1".as_slice(), None),
      (b"f2".as_slice(), b"v2".as_slice(), None),
      (b"f3".as_slice(), b"v3".as_slice(), None),
    ];
    let total_bytes: usize = entries.iter().map(|(f, v, _)| f.len() + v.len()).sum();
    assert!(wkv::should_downgrade_hash(entries.len(), total_bytes));

    // 3. 执行平滑降级迁移
    session
      .migrate_flattened_to_compact_hash(key, &mut meta, &entries)
      .await?;

    // 4. 验证元数据已降级为 Compact，版本号已推进
    let meta_after = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta_after.encoding(), StorageEncoding::Compact);
    assert_eq!(meta_after.version, old_version + 1);
    assert_eq!(meta_after.size, 3);

    // 5. 校验全部字段可通过通用 hget / hget_with 准确读取
    assert_eq!(session.hget(key, b"f1").await?, Some(b"v1".to_vec()));
    assert_eq!(session.hget(key, b"f2").await?, Some(b"v2".to_vec()));
    assert_eq!(session.hget(key, b"f3").await?, Some(b"v3".to_vec()));
    assert_eq!(session.hget(key, b"nonexist").await?, None);

    OK
  })
}

#[test]
fn test_flattened_to_compact_downgrade_with_expirations() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("downgrade_exp").await?;
    let session = store.new_session()?;
    let key = b"downgrade_exp_hash";

    assert!(session.flattened_hset(key, b"alive", b"val_alive").await?);
    assert!(
      session
        .flattened_hset(key, b"expired", b"val_expired")
        .await?
    );
    let mut meta = session.load_meta(key).await?.expect("meta exists");

    let now = now_ticks();
    let entries = [
      (b"alive".as_slice(), b"val_alive".as_slice(), None),
      (
        b"expired".as_slice(),
        b"val_expired".as_slice(),
        Some(now - 1000), // 已过期
      ),
    ];

    // 执行带过期过滤的平滑降级
    session
      .migrate_flattened_to_compact_hash(key, &mut meta, &entries)
      .await?;

    // 验证仅存活字段被迁移，meta.size 精准同步为 1
    let meta_after = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta_after.encoding(), StorageEncoding::Compact);
    assert_eq!(meta_after.size, 1);
    assert_eq!(
      session.hget(key, b"alive").await?,
      Some(b"val_alive".to_vec())
    );
    assert_eq!(session.hget(key, b"expired").await?, None);

    // 全部过期情况：验证触发删空自愈
    let all_expired = [(
      b"alive".as_slice(),
      b"val_alive".as_slice(),
      Some(now - 1000),
    )];
    session
      .migrate_flattened_to_compact_hash(key, &mut meta, &all_expired)
      .await?;
    assert_eq!(session.load_meta(key).await?, None);
    assert_eq!(session.hlen(key).await?, 0);

    OK
  })
}

#[test]
fn test_compact_hdel_drain_and_delete_self_healing() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("compact_hdel_drain").await?;
    let session = store.new_session()?;
    let key = b"compact_drain_hash";

    // 1. 创建单字段 Compact 哈希并设置 TTL
    assert!(session.hset(key, b"f_only", b"v_only").await?);
    let future_ticks = now_ticks() + 3600 * 10_000_000;
    session
      .expire_at(key, future_ticks, TtlOpt::default())
      .await?;
    assert_eq!(session.hlen(key).await?, 1);
    assert!(session.contains_key(key).await?);
    assert!(session.has_ttl_tag(key)?);

    // 2. 删除唯一的字段（触发删空自愈）
    assert!(session.hdel(key, b"f_only").await?);

    // 3. 校验严格删空自愈：元记录被清除，随键 TTL 彻底清除，无孤儿元记录
    assert_eq!(session.hlen(key).await?, 0);
    assert!(!session.contains_key(key).await?);
    assert_eq!(session.load_meta(key).await?, None);
    assert_eq!(session.ttl_of(key).await?, None);
    assert!(!session.contains_key_raw(&session.ttl_key(key)).await?);

    OK
  })
}

#[test]
fn test_ghost_meta_record_self_healing() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("ghost_meta").await?;
    let session = store.new_session()?;
    let key = b"ghost_hash_key";

    // 手动构造并写入一条 size == 0 的幽灵元记录
    let meta_k = session.session_meta_key(key);
    let mut ghost_meta = MetaValue::new(999, CollectionType::Hash, 1, 0);
    ghost_meta.set_encoding(StorageEncoding::Flattened);
    session.upsert_raw(&meta_k, &ghost_meta.to_bytes()).await?;

    // load_meta 读取探测到 size == 0 幽灵元记录：触发静默自愈并返回 None
    assert_eq!(session.load_meta(key).await?, None);

    // 再次探查：底层记录已被自愈物理清除
    assert!(!session.contains_key_raw(&meta_k).await?);

    OK
  })
}

/// 生成定长字段名（f_ + 10 位数字，字典序无关，仅保证唯一）
#[inline]
fn field_name(i: usize) -> [u8; 12] {
  let mut name = *b"f_0000000000";
  let mut n = i;
  for pos in (2..12).rev() {
    name[pos] = b'0' + (n % 10) as u8;
    n /= 10;
  }
  name
}

#[test]
fn test_flattened_hash_auto_downgrade_hysteresis_no_oscillation() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("downgrade_hysteresis").await?;
    let session = store.new_session()?;
    let key = b"hysteresis_hash";

    // 1. 直建 Flattened 打平哈希：16386 个小字段（值 8B，远小于门限）
    const N: usize = wkv::HASH_DOWNGRADE_ITEM_THRESHOLD + 2;
    for i in 0..N {
      assert!(
        session
          .flattened_hset(key, &field_name(i), b"val_0000")
          .await?
      );
    }
    let meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta.encoding(), StorageEncoding::Flattened);
    assert_eq!(meta.size as usize, N);

    // 2. 降到 16385 项：处于迟滞区间上沿之外（> 16384），不得降级
    assert!(session.hdel(key, &field_name(0)).await?);
    let meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta.encoding(), StorageEncoding::Flattened);
    assert_eq!(meta.size as usize, wkv::HASH_DOWNGRADE_ITEM_THRESHOLD + 1);
    assert_eq!(
      session.hlen(key).await?,
      wkv::HASH_DOWNGRADE_ITEM_THRESHOLD + 1
    );

    // 3. 再降到 16384 项：满足项数与字节数双门限，自动降级回 Compact
    assert!(session.hdel(key, &field_name(1)).await?);
    let meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta.encoding(), StorageEncoding::Compact);
    assert_eq!(meta.size as usize, wkv::HASH_DOWNGRADE_ITEM_THRESHOLD);
    assert_eq!(session.hlen(key).await?, wkv::HASH_DOWNGRADE_ITEM_THRESHOLD);

    // 4. 迟滞区间内不震荡：降级后再 hset 1 个字段（16385 项，< 32768 且 < 1MB），
    //    不得触发升级，保持 Compact
    assert!(session.hset(key, &field_name(0), b"val_0000").await?);
    let meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta.encoding(), StorageEncoding::Compact);
    assert_eq!(
      session.hlen(key).await?,
      wkv::HASH_DOWNGRADE_ITEM_THRESHOLD + 1
    );

    // 5. 数据完整性抽查：降级迁移后字段全量可读
    assert_eq!(
      session.hget(key, &field_name(2)).await?,
      Some(b"val_0000".to_vec())
    );
    assert_eq!(
      session.hget(key, &field_name(N - 1)).await?,
      Some(b"val_0000".to_vec())
    );
    assert_eq!(session.hget(key, &field_name(1)).await?, None);

    OK
  })
}

#[test]
fn test_flattened_hash_downgrade_oversize_value_guard() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("downgrade_oversize").await?;
    let session = store.new_session()?;
    let key = b"oversize_hash";

    // 1. 直建 Flattened：1 个超 u16 上限大值字段 + 4 个小字段
    let big_value = vec![b'z'; 70_000];
    assert!(session.flattened_hset(key, b"big", &big_value).await?);
    for i in 0..4u8 {
      assert!(session.flattened_hset(key, &[b's', i], b"small").await?);
    }

    // 2. hdel 减项触发降级判定：大值字段超 Compact u16 编码上限，守卫放弃降级
    assert!(session.hdel(key, &[b's', 0]).await?);
    let meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta.encoding(), StorageEncoding::Flattened);
    assert_eq!(meta.size, 4);

    // 3. 删除大值字段后再 hdel：无超限字段，降级成功
    assert!(session.hdel(key, b"big").await?);
    let meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(meta.encoding(), StorageEncoding::Compact);
    assert_eq!(meta.size, 3);
    assert_eq!(
      session.hget(key, &[b's', 1]).await?,
      Some(b"small".to_vec())
    );
    assert_eq!(session.hget(key, b"big").await?, None);

    OK
  })
}

/// 测试: 字节超门限受阻后纯删除跨过 512KB 门限自动降级（负缓存字节快照记账解锁）
///
/// 场景：600 项 × (5B 字段名 + 900B 值) = 543KB > 512KB 降级门限且无超 u16 字段——
/// 首次 hdel 聚合判定受阻（字节超门限）置位负缓存并记录字节快照；此后每次 hdel
/// 以「快照 - (field.len + 被删值长度)」递减估算（零全库扫描），估算跨过门限即
/// 解锁并就地重试聚合，自动降级回 Compact
#[test]
fn test_flattened_hash_byte_blocked_downgrade_retries_on_pure_deletes() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("byte_blocked").await?;
    let session = store.new_session()?;
    let key = b"byte_blocked_hash";
    const N: usize = 600;
    let value = vec![b'v'; 900];

    // 1. 直建 Flattened：600 项 × 905B ≈ 543KB（> 512KB），全部字段在 u16 限内
    for i in 0..N {
      let f = format!("f{i:04}");
      assert!(session.flattened_hset(key, f.as_bytes(), &value).await?);
    }

    // 2. 首次 hdel：触发降级聚合判定——字节超门限受阻，保持 Flattened
    assert!(session.hdel(key, b"f0000").await?);
    let meta = session.load_meta(key).await?.expect("meta exists");
    assert_eq!(
      meta.encoding(),
      StorageEncoding::Flattened,
      "字节超门限必须受阻保持 Flattened"
    );

    // 3. 纯删除跨过门限：循环删除直至降级发生（快照递减解锁 + 就地重试聚合）
    let mut downgraded = false;
    for i in 1..N {
      let f = format!("f{i:04}");
      assert!(session.hdel(key, f.as_bytes()).await?);
      let meta = session.load_meta(key).await?.expect("meta exists");
      if meta.encoding() == StorageEncoding::Compact {
        // 降级后剩余字段全部可见且计数精确
        assert!(meta.size > 0, "降级时刻必须仍有存活字段");
        let survivors = (i + 1..N).map(|j| format!("f{j:04}")).collect::<Vec<_>>();
        for f in &survivors {
          assert_eq!(session.hget(key, f.as_bytes()).await?, Some(value.clone()));
        }
        assert_eq!(meta.size as usize, survivors.len());
        downgraded = true;
        break;
      }
    }
    assert!(
      downgraded,
      "纯删除跨过 512KB 门限后必须解锁并自动降级回 Compact"
    );

    OK
  })
}
