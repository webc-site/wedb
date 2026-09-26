//! 首升阶元记录落盘失败回滚回归（票：wkv-bftree-promote-meta-save-failure-orphans-tree-blocks-retry）
//!
//! 缺陷形态：promote_collection_to_bftree 换入后 save_bftree_meta_stub 失败臂
//! 仅发副本 RangeIndexDrop 补偿、不回滚本地——首升阶（replace=false）残留
//! 「孤儿新树常驻 live_indexes + 内存态信封」，该键后续重试升阶在换入锁内被
//! publish_tree_from_snapshot_locked 的 IndexExists 防重门永久拦截，冻结为
//! 不可写不可升阶。
//!
//! 修复契约（1:1 对标 C# 存根落盘失败即销毁刚建 BfTree，
//! garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexCreate
//! 的 `RMW failed → bfTree.Dispose()`，rust 侧与 range_index_create 同一
//! unregister + delete_index 回滚机制，全库只一套）：
//!   * 首升阶存根落盘失败 ⇒ 注销换号旁表 + 摘除孤儿树并清理数据文件 ⇒ 键回到
//!     纯信封态；重试首升阶顺利完成建树换入，不再 IndexExists；
//!   * 重灌（replace=true）存根落盘失败 ⇒ 保持既有「新树 + 旧 meta」滞后态行为
//!     不变——新树已原子顶替数据文件，误删即丢数据。
//!
//! 故障注入为真实失败路径：生产写监听回调口（StoreEventSink 的 Write 臂）对
//! 本测试键 Meta 域物理写恒败——与 save_bftree_meta_stub 落盘 I/O 失败同形
//! 上抛（对照 wnode/tests/vadd_attribute_write_failure.rs 的回调注错形态，
//! 注入面在 wkv 本域）。
//!
//! 自研依据: doc/zh/collection.md「转换流程」升阶元记录保存失败回滚

use std::{
  io,
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use aok::{OK, Void};
use compio::time::sleep;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{Error as WkvError, StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec};

/// 测试键：会话默认域 (0,0) 下的分层集合
const KEY: &[u8] = b"prom:retry";

/// 本测试键的树身份键 = 物理 Meta 键（与会话 session_meta_key 同一编码内核）
fn coll_meta_key() -> wval::TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Meta, KEY)
}

/// Meta 记录存根落盘写恒败注错口：armed 期间对本键 Meta 域物理写镜像失败
/// （写监听口沿 upsert_raw 上抛 = 落盘硬错同形），其余事件直通
fn meta_save_fault(
  armed: &AtomicBool,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  if let StoreEvent::Write { key, .. } = event
    && armed.load(Ordering::Relaxed)
    && key == coll_meta_key().as_slice()
  {
    return Err(WkvError::Io(io::Error::other(
      "injected meta stub save failure",
    )));
  }
  Ok(())
}

/// 装配：真盘 wkv + RangeIndex 目录 + Meta 落盘注错 sink（sink 注入先于会话创建）
fn harness(
  name: &str,
) -> aok::Result<(
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<AtomicBool>,
)> {
  let dir = tempdir()?;
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let armed = Arc::new(AtomicBool::new(false));
  assert!(
    store.set_event_sink(StoreEventSink::new(Arc::clone(&armed), meta_save_fault)),
    "注错 sink 注入应成功"
  );
  Ok((dir, store, armed))
}

/// 驱动释放消费并轮询等待纪元延迟删除收敛（同 store/flush_database wait_file_gone
/// 判据：换号队列 drain + 参与者进出推进条带锁让位批次的重投收割）
async fn wait_file_gone(path: &Path, store: &Arc<WedbStore<SegmentedDevice>>) -> Void {
  for _ in 0..2500 {
    store.drain_bftree_release(usize::MAX);
    if !path.exists() {
      return OK;
    }
    drop(store.new_session()?);
    sleep(Duration::from_millis(2)).await;
  }
  panic!("回滚删除未收敛，磁盘孤儿数据文件残留: {}", path.display());
}

/// 旁表在册判据（snapshot_bftree_domains 只读快照，登记面零影响）
fn side_table_holds(store: &WedbStore<SegmentedDevice>) -> bool {
  store
    .snapshot_bftree_domains()
    .into_iter()
    .any(|(_, _, keys)| keys.iter().any(|k| k.as_ref() == KEY))
}

/// 首升阶存根落盘失败 ⇒ 逆序回滚至纯信封态，重试升阶不再 IndexExists；
/// 重灌态存根落盘失败 ⇒ 已换入新树不误删、旁表登记保持，行为不变
#[compio::test]
async fn promote_first_round_meta_save_failure_rolls_back_for_retry() -> Void {
  let (_dir, store, armed) = harness("promote_rollback.db")?;
  let s = store.new_session()?;
  let id_key = s.session_meta_key(KEY);
  let data_path = store.range_index().data_file_path_for_key(&id_key);
  let env_k = s.session_tag_key(KeyTag::ObjectEnvelope, KEY);

  // 纯内存信封态预置（首升阶前置形态）
  s.upsert_tag(KEY, KeyTag::ObjectEnvelope, b"\x03envelope-snapshot")
    .await?;

  // ── 注入：首升阶换入后元记录落盘恒败 ──
  armed.store(true, Ordering::Relaxed);
  let err = s
    .promote_collection_to_bftree(
      KEY,
      GarnetObjectType::Hash,
      vec![(b"f1".to_vec(), b"v1".to_vec())],
      i64::MAX,
      false,
    )
    .await
    .expect_err("存根落盘失败必须上抛令升阶命令报错");
  assert!(
    err.to_string().contains("injected meta stub"),
    "错误面应为注入的落盘失败（他臂失败即措辞不符），实际 {err}"
  );
  assert!(
    matches!(err, WkvError::Swapped(_)),
    "换入已生效后的失败保持 Swapped 分级，实际 {err:?}"
  );
  armed.store(false, Ordering::Relaxed);

  // ── 断言①：注册表无残留条目、磁盘无孤儿数据文件、换号旁表无残留 ──
  assert!(
    store.range_index().get_tree(&id_key).is_none(),
    "首升阶失败后孤儿树不得常驻注册表——残留即重试被 IndexExists 永久拦截"
  );
  wait_file_gone(&data_path, &store).await?;
  assert!(
    !side_table_holds(&store),
    "回滚须注销换号旁表登记，残留即 FLUSHDB 取走他键、本键树漏回收"
  );

  // ── 断言②：键回到纯内存信封态（无 meta 记录、信封完整保留）──
  assert!(
    s.load_meta(KEY).await?.is_none(),
    "首升阶回滚后主存不得有元记录（落盘被注入失败，槽位未挂载）"
  );
  assert!(
    s.contains_key_raw(&env_k).await?,
    "回滚后键须为纯信封态，信封记录完整保留"
  );

  // ── 断言③：重试首升阶（replace=false）顺利完成建树换入，不再 IndexExists ──
  s.promote_collection_to_bftree(
    KEY,
    GarnetObjectType::Hash,
    vec![(b"f1".to_vec(), b"v1".to_vec())],
    i64::MAX,
    false,
  )
  .await
  .expect("回滚后重试首升阶必须成功（旧缺陷下此处撞 IndexExists 永久拦截）");
  assert!(
    store.range_index().get_tree(&id_key).is_some(),
    "重试升阶的新树须换入在册"
  );
  assert_eq!(
    s.load_meta(KEY).await?.map(|m| m.size),
    Some(1),
    "重试收尾元记录落盘、计数为灌入批"
  );
  assert!(
    data_path.exists(),
    "重试换入的数据文件须在正式路径（旧世代迟滞 unlink 有世代判据，不误删新文件）"
  );
  assert!(!s.contains_key_raw(&env_k).await?, "重试成功收尾删除信封");
  assert!(side_table_holds(&store), "重试升阶成功后须重新登记换号旁表");

  // ── 断言④：重灌态（replace=true）存根落盘失败行为不变——新树不误删 ──
  armed.store(true, Ordering::Relaxed);
  let err2 = s
    .promote_collection_to_bftree(
      KEY,
      GarnetObjectType::Hash,
      vec![
        (b"g1".to_vec(), b"w1".to_vec()),
        (b"g2".to_vec(), b"w2".to_vec()),
      ],
      i64::MAX,
      true,
    )
    .await
    .expect_err("重灌存根落盘失败同样上抛报错");
  assert!(
    err2.to_string().contains("injected meta stub"),
    "重灌臂错误面应为注入的落盘失败，实际 {err2}"
  );
  armed.store(false, Ordering::Relaxed);
  assert!(
    store.range_index().get_tree(&id_key).is_some(),
    "重灌失败臂严禁误删已换入新树（删树 = 灌入数据丢失）"
  );
  assert!(data_path.exists(), "重灌失败臂数据文件须随新树保留");
  assert!(
    side_table_holds(&store),
    "重灌失败臂旁表登记保持（换树不换键，全程在册）"
  );

  // 重灌重试收敛：换入窗内旧树顶替、计数更新
  s.promote_collection_to_bftree(
    KEY,
    GarnetObjectType::Hash,
    vec![
      (b"g1".to_vec(), b"w1".to_vec()),
      (b"g2".to_vec(), b"w2".to_vec()),
    ],
    i64::MAX,
    true,
  )
  .await
  .expect("重灌重试必须成功");
  assert_eq!(
    s.load_meta(KEY).await?.map(|m| m.size),
    Some(2),
    "重灌重试收尾元记录计数为再灌批"
  );
  OK
}
