//! 票1 回归：紧缩过滤谓词 WedbCompactionFunctions::is_deleted 对 ETag 旁路记录的漏判
//!
//! 对标 C# Tsavorite LogRecord 记录尾可选 ETag 字段随主记录一体消亡
//! （libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs:ETagSize；
//! wedb 仿 KeyTag::Ttl 以 KeyTag::Etag 独立旁路记录建模，业务判死注入位
//! 见 libs/server/Storage/Functions/GarnetRecordTriggers.cs:IsDeleted）。
//!
//! 修复前 ETag 落入 is_deleted 通配兜底 `_ => false`，宿主 TTL 过期后主数据
//! 与 TTL 记录双双判死丢弃，唯独 ETag 被误判存活经 conditional_copy_to_tail
//! 无限回拷永生，索引槽位与日志空间永久泄漏。探针一律以物理键查询（逻辑键假绿）。
//!
//! 自研依据: 紧缩孤儿 ETag 旁路清退（「严格删空生命周期与原子墓碑」契约，物理垃圾由紧缩异步回收）

use aok::{OK, Void};
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcompact::CompactionType;
use wtest_base::open_test_store;

/// 宿主 TTL 已过期的 ETag 旁路记录：紧缩须判死丢弃，杜绝孤儿回拷
#[compio::test]
async fn compact_drops_etag_of_expired_host() -> Void {
  let (_dir, store) = open_test_store("etag_orphan")?;
  let session = store.new_session()?;

  let key = b"k:etag:expired";
  session.upsert(key, b"v").await?;
  session.put_etag(key, 42).await?;
  // 免 sleep 直接写入已过期 TTL：宿主键已死，前台未触发惰性 purge
  session.put_ttl(key, now_ticks() - TICKS_PER_SECOND).await?;

  store.flush_all().await?;
  let tail = store.tail_address();
  store.shift_read_only_address(tail);

  let stats = store.compact(tail, CompactionType::Scan).await?;

  // 判别断言：修复前 ETag 落入通配恒判存活 → 经 conditional_copy_to_tail
  // 无限回拷永生。修复后宿主 TTL 过期判死等价一次过期 DEL：String 判死经
  // on_dropped 正轨清退链级联 del_etag（槽位移交墓碑），ETag 与宿主同亡，
  // 命令面读侧不得再见任何存活记录
  assert!(
    session.etag_of(key).await?.is_none(),
    "宿主 TTL 已过期的 ETag 旁路记录须随宿主判死清退，杜绝无主孤儿回拷"
  );
  // 宿主数据与 TTL 旁路记录同被清退，三条死记录一并退役（槽位多已移交
  // 墓碑，随下轮紧缩退役）
  assert!(
    session.read(key).await?.is_none(),
    "过期宿主键紧缩清退后命令面必须不存在"
  );
  assert!(
    session.ttl_of(key).await?.is_none(),
    "TTL 旁路记录须随过期清退级联消亡"
  );
  assert_eq!(
    stats.dead_dropped, 3,
    "String + Ttl + Etag 三条旁路死记录须全部判死丢弃"
  );
  OK
}

/// 正向对照：宿主存活且无 TTL 的 ETag 旁路记录不得被误删，须随宿主回拷保留
#[compio::test]
async fn compact_keeps_etag_of_live_host() -> Void {
  let (_dir, store) = open_test_store("etag_live")?;
  let session = store.new_session()?;

  let key = b"k:etag:live";
  session.upsert(key, b"v").await?;
  session.put_etag(key, 7).await?;

  store.flush_all().await?;
  let tail = store.tail_address();
  store.shift_read_only_address(tail);

  store.compact(tail, CompactionType::Scan).await?;

  // 宿主存活：ETag 记录须仍在索引中、值不丢，杜绝过度判死误删
  let index = store.index.load();
  assert!(
    index.find_tag(session.etag_key(key).as_slice()).is_some(),
    "宿主存活未过期的 ETag 旁路记录必须保留回拷"
  );
  assert_eq!(session.etag_of(key).await?, Some(7));
  assert_eq!(session.read(key).await?.as_deref(), Some(b"v".as_slice()));
  OK
}
