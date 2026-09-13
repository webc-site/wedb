//! Hash 双物理后端互操作集成测试（FlattenedTree encoding 分裂修复的回归门禁）
//!
//! 背景：whlog 打平 hash（meta 记录 32B）与 BfTree hash（meta 记录 67B 含存根）
//! 此前共用 StorageEncoding::Flattened，混用两套 API 时门面按 whlog 假设写子键
//! 会覆盖带存根的树元记录，破坏 BfTree hash 完整性。修复后 BfTree 后端独立
//! 使用 FlattenedTree，门面/whlog 算子按 encoding 分流，本测试验证：
//! 1. bftree_hset 写入 → hget/hget_with/hexists/hmget/hlen 门面读取互不互毁；
//! 2. hset/hdel 门面对树后端键透明转发（含 meta 存根物理完整性断言）；
//! 3. flattened_hset/flattened_hdel 对树后端键转发防互毁；
//! 4. whlog 打平 hash 与 BfTree hash 并存不串扰（同库两键独立后端）；
//! 5. delete 门面对树后端键的完整清理（元记录 + 树文件）。

use std::{fs, sync::Arc};

use aok::{OK, Result, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, StoreSession, WedbStore};
use wval::{CollectionType, META_VALUE_SIZE, StorageEncoding};

/// 构造独立临时目录中的全新引擎与会话
fn open_store(dir: &tempfile::TempDir, name: &str) -> Result<Arc<WedbStore<SegmentedDevice>>> {
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  Ok(Arc::new(WedbStore::open(config, device)?))
}

/// 断言键的元记录为存活 BfTree 树后端（encoding = FlattenedTree 且记录含 35B 存根）
async fn assert_tree_meta(session: &StoreSession<SegmentedDevice>, key: &[u8]) -> Result<()> {
  let meta_k = session.session_meta_key(key);
  let rec = session.read_raw(&meta_k).await?.expect("树元记录必须存在");
  assert!(
    rec.len() >= META_VALUE_SIZE + 35,
    "树元记录必须保留存根（>= 67B），实际 {}B",
    rec.len()
  );
  let meta = wval::MetaValue::from_slice(&rec)?;
  assert_eq!(meta.encoding(), StorageEncoding::FlattenedTree);
  assert_eq!(meta.collection_type, CollectionType::Hash);
  OK
}

/// 测试 1: bftree_hset 写入 → 门面读取/写入/删除全链互操作，数据完整不互毁
#[test]
fn test_bftree_hash_facade_interop() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "interop.db")?;
    let session = store.new_session()?;
    let key = b"tree_hash";

    // 树算子写入
    assert!(session.bftree_hset(key, b"f1", b"v1").await?);
    assert!(session.bftree_hset(key, b"f2", b"v2").await?);

    // 门面读取（透明分流 FlattenedTree）
    assert_eq!(session.hget(key, b"f1").await?, Some(b"v1".to_vec()));
    assert_eq!(
      session.hget_with(key, b"f2", |v| v.len()).await?,
      Some(2),
      "hget_with 零拷贝闭包必须透传树后端数据"
    );
    assert!(session.hexists(key, b"f1").await?);
    assert!(!session.hexists(key, b"missing").await?);
    assert_eq!(session.hlen(key).await?, 2);

    let hm = session.hmget(key, &[b"f1", b"missing", b"f2"]).await?;
    assert_eq!(
      hm,
      vec![Some(b"v1".to_vec()), None, Some(b"v2".to_vec())],
      "hmget 门面必须透传树后端数据"
    );

    // 门面写入（hset 透明转发树算子，不覆盖 67B 存根元记录）
    assert!(session.hset(key, b"f3", b"v3").await?);
    assert!(
      !session.hset(key, b"f3", b"v3_upd").await?,
      "覆盖已有字段返回 false"
    );
    assert_eq!(
      session.bftree_hget(key, b"f3").await?,
      Some(b"v3_upd".to_vec())
    );
    assert_tree_meta(&session, key).await?;
    assert_eq!(session.hlen(key).await?, 3);

    // 门面删除（hdel 透明转发树算子）
    assert!(session.hdel(key, b"f1").await?);
    assert_eq!(session.bftree_hget(key, b"f1").await?, None);
    assert_tree_meta(&session, key).await?;

    // 删空：元记录与树文件彻底释放
    assert!(session.hdel(key, b"f2").await?);
    assert!(session.hdel(key, b"f3").await?);
    assert!(session.load_meta(key).await?.is_none());
    let ri_dir = dir.path().join("range_indexes");
    let mut files = fs::read_dir(&ri_dir)?.flatten().collect::<Vec<_>>();
    files.retain(|e| e.path().extension().is_some_and(|x| x != "ckpt"));
    assert!(
      files.is_empty(),
      "删空后树数据文件必须被 unlink，实际残留 {:?}",
      files.iter().map(|e| e.path()).collect::<Vec<_>>()
    );

    OK
  })?;
  OK
}

/// 测试 2: flattened_hset/flattened_hdel 对树后端键转发（防 whlog 子键覆写存根）
#[test]
fn test_flattened_ops_forward_tree_backend() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "fwd.db")?;
    let session = store.new_session()?;
    let key = b"tree_hash";

    assert!(session.bftree_hset(key, b"a", b"1").await?);

    // flattened_hset 不得按 whlog 假设写子键（否则 save_meta 会丢弃 35B 存根）
    assert!(session.flattened_hset(key, b"b", b"2").await?);
    assert_eq!(session.bftree_hget(key, b"b").await?, Some(b"2".to_vec()));
    assert_eq!(session.hlen(key).await?, 2);
    assert_tree_meta(&session, key).await?;

    // flattened_hget/flattened_hexists 是 whlog 显式算子：树后端键返回默认值（不误报）
    assert_eq!(session.flattened_hget(key, b"a").await?, None);
    assert!(!session.flattened_hexists(key, b"a").await?);

    // flattened_hdel 转发树算子
    assert!(session.flattened_hdel(key, b"a").await?);
    assert_eq!(session.bftree_hget(key, b"a").await?, None);
    assert_eq!(session.hlen(key).await?, 1);
    assert_tree_meta(&session, key).await?;

    OK
  })?;
  OK
}

/// 测试 3: whlog 打平 hash 与 BfTree hash 同库并存，两套读写互不串扰
#[test]
fn test_dual_backend_coexistence() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "dual.db")?;
    let session = store.new_session()?;
    let flat_key = b"whlog_hash";
    let tree_key = b"tree_hash";

    // whlog 打平后端：flattened_hset 直建（32B meta 记录）
    assert!(session.flattened_hset(flat_key, b"x", b"10").await?);
    // 树后端
    assert!(session.bftree_hset(tree_key, b"y", b"20").await?);

    // 编码互异断言
    let flat_meta = session.load_meta(flat_key).await?.unwrap();
    assert_eq!(flat_meta.encoding(), StorageEncoding::Flattened);
    let tree_meta = session.load_meta(tree_key).await?.unwrap();
    assert_eq!(tree_meta.encoding(), StorageEncoding::FlattenedTree);

    // 门面对两键均透明正确
    assert_eq!(session.hget(flat_key, b"x").await?, Some(b"10".to_vec()));
    assert_eq!(session.hget(tree_key, b"y").await?, Some(b"20".to_vec()));
    // 交叉字段不可见（物理隔离）
    assert_eq!(session.hget(flat_key, b"y").await?, None);
    assert_eq!(session.hget(tree_key, b"x").await?, None);

    // delete 门面分别正确清理两种后端
    assert!(session.delete(flat_key).await?);
    assert!(session.delete(tree_key).await?);
    assert!(session.load_meta(flat_key).await?.is_none());
    assert!(session.load_meta(tree_key).await?.is_none());

    OK
  })?;
  OK
}
