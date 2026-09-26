//! zcode-r22-wcompact 回归：紧缩判死两段式（键级清退 + 条件摘除）与搬迁源存根
//! 所有权转出
//!
//! 发现一（TTL 判死旁路树清退）：紧缩谓词对附 TTL 且过期的分层 Meta 记录判死后
//! 只摘索引，`handle_bftree_drain_and_delete` 树清退与向量删除链完全旁路——树
//! 实例/缓存预算/数据文件成永久孤儿，expire sweep 失去该键数据源后永不复扫。
//! 修复后判死语义等价一次过期 DEL：宿主清退先行（`on_dropped` → check_expired
//! 正轨链），紧缩器随后按地址条件摘除残留槽位。C# IsDeleted 恒 false 无判死
//! 对位，处置语义对标 libs/server/Storage/Functions/GarnetRecordTriggers.cs
//! :OnDispose 的 Deleted/Expired 臂 DisposeTreeUnderLock + RequestDeletion。
//!
//! 发现二（搬迁漏做源存根转出）：C# PostCopyToTail 源侧 ClearTreeHandle +
//! SetTransferredFlag + 冷源 PreStageAndRegisterPending，组提交刷盘 OnFlush 的
//! is_transferred 防护据此跳过滞留源；rust 紧缩搬迁纯 append+CAS 无源转出，
//! 未刷盘滞留源被误做全树 CPR 快照并产出无消费方 flush 件。修复后 CAS 成功即
//! 经 RIPROMOTE 单点 `transfer_out_source_stub` 并轨转出。
//!
//! 自研依据: 分层态到期纪元出账（doc/zh/collection.md 第 6 条水位内 O(1) 计数与出账补则）

use std::sync::{Arc, atomic::Ordering};

use aok::{OK, Void};
use tempfile::TempDir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wbftree::{DELETE_INDEX_FAIL_INJECT, RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub};
use wcompact::CompactionType;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wval::{GarnetObjectType, KeyTag, META_VALUE_SIZE, NamespaceDbCodec, TaggedKeyBuf};

const PAGE: usize = 1024 * 1024;

fn open_store(dir: &TempDir, name: &str) -> aok::Result<Arc<WedbStore<SegmentedDevice>>> {
  let mut config =
    StoreConfig::new(2048, PAGE, 16, 0.5)?.with_range_index_dir(dir.path().join("range_indexes"));
  config.gc.enabled = false;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  Ok(Arc::new(WedbStore::open(config, device)?))
}

/// 树身份键 = 物理 Meta 键（默认会话域 (0, 0)）
fn id_key(user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Meta, user_key)
}

/// 读指定地址记录值体中的存根（界内性由 Meta 复合记录编码保证）
async fn stub_at(
  store: &Arc<WedbStore<SegmentedDevice>>,
  addr: u64,
) -> aok::Result<RangeIndexStub> {
  let session = store.new_session()?;
  let rec = session.read_record(addr).await?;
  let val = rec.value()?;
  Ok(RangeIndexStub::decode(
    &val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE],
  )?)
}

/// 发现一：EXPIRE 过期的分层 hash 键触发紧缩后，树注册表/缓存预算/数据文件
/// 须随判死清退整键回收，活键对照不受波及
#[compio::test]
async fn compact_expired_tiered_key_reclaims_tree_and_files() -> Void {
  let dir = TempDir::new()?;
  let store = open_store(&dir, "compact_tiered.db")?;
  let session = store.new_session()?;

  let key = b"h:expired";
  session
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
    .await?;
  let tree_key = id_key(key);
  let mgr = store.range_index();
  assert!(
    mgr.get_tree(&tree_key).is_some(),
    "升阶后树实例必须在注册表在册"
  );
  let data_path = mgr.data_file_path_for_key(&tree_key);
  assert!(data_path.exists(), "升阶即落 data 工作文件");
  assert!(mgr.cache_reserved() > 0, "树页缓存预算必须在账");

  // 活键对照：紧缩不得波及无 TTL 存活数据
  session.upsert(b"h:alive", b"v").await?;

  // 免 sleep 直接写入已过期 TTL（前台未触发惰性 purge）
  session.put_ttl(key, now_ticks() - TICKS_PER_SECOND).await?;

  let tail = store.tail_address();
  store.shift_read_only_address(tail);
  let stats = store.compact(tail, CompactionType::Scan).await?;

  assert!(stats.dead_dropped >= 2, "Meta + Ttl 两条死记录须全部判死");
  assert!(
    mgr.get_tree(&tree_key).is_none(),
    "判死清退须注销树实例，杜绝注册表孤儿"
  );
  assert_eq!(mgr.cache_reserved(), 0, "树注销须归还页缓存预算");
  assert!(
    !data_path.exists(),
    "判死清退须物理删除树数据文件，杜绝磁盘孤儿"
  );
  // 命令面语义断言（索引槽位多已移交墓碑，物理 find_tag 不再是存活判据）
  assert!(
    session.load_meta(key).await?.is_none(),
    "分层路由域须消亡，命令面不得回落复活"
  );
  assert!(
    !session.contains_key(key).await?,
    "过期键紧缩清退后对命令面必须不存在"
  );
  // 活键对照存活
  assert_eq!(
    session.read(b"h:alive").await?.as_deref(),
    Some(b"v".as_slice())
  );
  OK
}

/// 票据回归：清退臂注销/IO 故障时，紧缩不摘槽、截断点回退至记录边界防悬挂槽；
/// 故障恢复后下轮紧缩闭环清退，杜绝孤儿树
#[compio::test]
async fn compact_expired_tiered_key_drop_failure_retains_slot_and_recovers_next_round() -> Void {
  let dir = TempDir::new()?;
  let store = open_store(&dir, "compact_drop_fail.db")?;
  let session = store.new_session()?;

  let key = b"h:expired_fail";
  session
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
    .await?;
  let tree_key = id_key(key);
  let mgr = store.range_index();
  assert!(mgr.get_tree(&tree_key).is_some());
  let initial_cache_reserved = mgr.cache_reserved();
  assert!(initial_cache_reserved > 0);

  // 写入已过期 TTL
  session.put_ttl(key, now_ticks() - TICKS_PER_SECOND).await?;

  let tail = store.tail_address();
  store.shift_read_only_address(tail);

  // 注入清退故障（wbftree 测试钩子：模拟树注销/文件删除 IO 失败）
  DELETE_INDEX_FAIL_INJECT.store(true, Ordering::SeqCst);

  let stats = store.compact(tail, CompactionType::Scan).await?;

  // 断言：清退失败臂跳过摘槽并回退截断，记录保留至下轮
  assert!(
    stats.retained >= 1,
    "清退失败记录必须计入 retained 保守保留: {stats:?}"
  );
  assert!(
    mgr.get_tree(&tree_key).is_some(),
    "清退失败时树实例不可被注销成半程孤儿"
  );
  assert_eq!(
    mgr.cache_reserved(),
    initial_cache_reserved,
    "树页缓存预算必须保持在账"
  );
  assert!(
    store.index.load().find_tag(&tree_key).is_some(),
    "索引槽位必须保留，严禁误摘成悬挂槽"
  );

  // 故障解除，下一轮紧缩完成注销闭环
  DELETE_INDEX_FAIL_INJECT.store(false, Ordering::SeqCst);
  let stats2 = store.compact(tail, CompactionType::Scan).await?;
  assert!(
    stats2.dead_dropped >= 1,
    "下轮重判清退成功必须计入 dead_dropped: {stats2:?}"
  );
  assert!(
    mgr.get_tree(&tree_key).is_none(),
    "故障解除后紧缩必须闭环注销树实例"
  );
  assert_eq!(mgr.cache_reserved(), 0, "树注销归还全部页缓存预算");
  assert!(
    !mgr.data_file_path_for_key(&tree_key).exists(),
    "数据文件必须被删除"
  );
  OK
}

/// 发现二：活树存根搬迁轮次后组提交刷盘，滞留源不得被误快照产出无消费方
/// flush.bftree 文件（is_transferred 防护生效），新址帧照常快照
#[compio::test]
async fn compact_transfer_out_suppresses_stale_flush_snapshot() -> Void {
  let dir = TempDir::new()?;
  let store = open_store(&dir, "compact_transfer.db")?;
  let session = store.new_session()?;

  let key = b"h:migrate";
  session
    .promote_collection_to_bftree(
      key,
      GarnetObjectType::Hash,
      vec![(b"f1".to_vec(), b"v1".to_vec())],
      i64::MAX,
      false,
    )
    .await?;
  let tree_key = id_key(key);
  assert!(store.range_index().get_tree(&tree_key).is_some());
  let src_addr = store
    .index
    .load()
    .find_tag(&tree_key)
    .expect("升阶 Meta 存根必须挂载索引");
  assert!(
    !stub_at(&store, src_addr).await?.is_transferred(),
    "前置：源存根初始未转出"
  );

  // 封印（不刷盘）：存根进只读区且保持未刷盘态，恰为票面危害形态
  let tail = store.tail_address();
  store.shift_read_only_address(tail);
  store.compact(tail, CompactionType::Scan).await?;

  let dst_addr = store
    .index
    .load()
    .find_tag(&tree_key)
    .expect("搬迁后索引必须挂载尾部新帧");
  assert_ne!(dst_addr, src_addr, "活树存根必须经紧缩搬迁至尾部");

  // 组提交刷盘：OnFlush 物理走查仍触达截断前沿的滞留源页与新址帧——
  // is_transferred 防护失效即对源误做全树 CPR 快照并产出无消费方 flush 件
  store.flush_all().await?;

  let prefix = RangeIndexManager::base32_prefix_of(&tree_key);
  let mgr = store.range_index();
  let src_flush = mgr.log_flush_path(&prefix, src_addr);
  assert!(
    !src_flush.exists(),
    "滞留源被 is_transferred 防护跳过，绝不得产出无消费方 flush 件"
  );
  assert!(
    mgr.log_flush_path(&prefix, dst_addr).exists(),
    "新址帧照常快照，排除防护误杀全部刷盘的假绿"
  );
  OK
}

/// 回归护栏：无 TTL 的活存根紧缩搬迁后命令面读值不丢（判死清退不误伤存活分层键）
#[compio::test]
async fn compact_keeps_live_tiered_key_readable() -> Void {
  let dir = TempDir::new()?;
  let store = open_store(&dir, "compact_live.db")?;
  let session = store.new_session()?;

  let key = b"h:live";
  session
    .promote_collection_to_bftree(
      key,
      GarnetObjectType::Hash,
      vec![(b"f1".to_vec(), b"v1".to_vec())],
      i64::MAX,
      false,
    )
    .await?;
  let tree_key = id_key(key);
  let tail = store.tail_address();
  store.shift_read_only_address(tail);
  let stats = store.compact(tail, CompactionType::Scan).await?;
  assert_eq!(stats.dead_dropped, 0, "无 TTL 活键不得判死");

  assert!(store.range_index().get_tree(&tree_key).is_some());
  let meta = session.load_meta(key).await?.expect("路由域存活");
  assert_eq!(meta.size, 1);
  OK
}
