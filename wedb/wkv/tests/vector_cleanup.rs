//! 向量域 drop 清扫集成测试（对标 libs/server/Resp/Vector/VectorManager.Cleanup.cs
//! 的 PostDropCleanupFunctions + IterateLookupSnapshot 全日志扫描删除段；测试语义
//! 对标 garnet/test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:DeleteVectorSet
//! ——删除集合后关联元素记录随之消亡，且不波及异上下文与异域数据）：
//!
//! 1. 清扫后目标上下文全部项类型子域（低 3 位）记录不可扫描（无链首存活版）
//!    且不可点查；
//! 2. 异上下文记录与 String/Registry 异域记录不受波及；
//! 3. 同键多版本只产生一次墓碑（去重收集）；已墓碑键不复活、幂等重扫零删除；
//! 4. 跨物理前缀（虚库换代旧域）的记录同样被清扫覆盖。
//!
//! 在 garnet 中的相对路径: libs/server/Storage/Functions/MainStore/DeleteMethods.cs:InitialDeleter → VectorManager.RequestDeletion（值域外登记随删除清退）
use std::sync::Arc;

use aok::Void;
use wdev::SegmentedDevice;
use wkv::{StoreSession, WedbStore};
use wtest_base::open_test_store;
use wval::{KeyTag, NamespaceDbCodec};

/// 上下文基址掩码（与 wnode CONTEXT_STEP = 8 对齐的项类型子域位）
const TERM_MASK: u64 = 0b111;

/// 生产同型物理键编码：`[prefix][KeyTag::Vector][context|term 8B BE][key]`
fn vector_key(prefix: &[u8], ctx: u64, key: &[u8]) -> Vec<u8> {
  let default_prefix = [0u8, 0u8];
  let prefix = if prefix.is_empty() {
    &default_prefix[..]
  } else {
    prefix
  };
  NamespaceDbCodec::encode_vector_key_with_prefix(prefix, ctx, key).into_vec()
}

/// 物理键是否为目标上下文基址的向量域记录（测试侧独立实现，交叉验证内核口径）
fn matches_ctx(key: &[u8], ctx: u64) -> bool {
  matches!(NamespaceDbCodec::decode_tagged_key(key), Ok((_, _, tag, payload))
        if tag == KeyTag::Vector
            && payload.len() >= 8
            && u64::from_be_bytes(payload[..8].try_into().unwrap()) & !TERM_MASK == ctx)
}

/// 全日志扫描统计目标上下文基址的链首存活记录数（不可扫描判据：
/// 墓碑/被取代旧版不计，仅索引链首即本记录的版本算存活）
async fn count_live(store: &Arc<WedbStore<SegmentedDevice>>, ctx: u64) -> aok::Result<usize> {
  let index = store.index.load();
  let mut live = 0usize;
  store
    .hlog()
    .scan(store.begin_address(), store.tail_address(), |addr, rec| {
      if !rec.is_tombstone()
        && matches_ctx(rec.key(), ctx)
        && index.find_tag(rec.key()) == Some(addr)
      {
        live += 1;
      }
      Ok(true)
    })
    .await?;
  Ok(live)
}

/// 点查物理键（冷热统一全读口径）
async fn point_read(
  session: &StoreSession<SegmentedDevice>,
  key: &[u8],
) -> aok::Result<Option<Vec<u8>>> {
  session.read_raw(key).await.map_err(Into::into)
}

/// 测试 1 + 3 + 4：全项类型子域清扫、异上下文/异域不波及、同键多版本去重、
/// 跨物理前缀覆盖、幂等重扫零删除
#[compio::test]
async fn purge_removes_context_records_and_spares_others() -> Void {
  let (_dir, store) = open_test_store("vector_cleanup")?;
  let session = store.new_session()?;

  // 目标上下文 8：8 个项类型子域各一记录，其中子域 0 键写两个版本（去重判据）
  let ctx = 8u64;
  for term in 0..8u64 {
    session
      .upsert_raw(&vector_key(&[], ctx | term, b"victim"), &[term as u8; 4])
      .await?;
  }
  session
    .upsert_raw(&vector_key(&[], ctx, b"victim"), &[9u8; 4])
    .await?;
  // 跨物理前缀（虚库换代旧域形态）的同上下文记录
  session
    .upsert_raw(&vector_key(&[0x05, 0x07], ctx, b"victim"), &[7u8; 4])
    .await?;

  // 幸存上下文 16（子域 0/3 各一）与异域记录（String 用户键）
  let other = 16u64;
  session
    .upsert_raw(&vector_key(&[], other, b"keep0"), &[0u8; 4])
    .await?;
  session
    .upsert_raw(&vector_key(&[], other | 3, b"keep3"), &[3u8; 4])
    .await?;
  session.upsert(b"user_key", b"plain").await?;

  assert!(
    count_live(&store, ctx).await? == 9,
    "前置：9 个唯一物理键（子域 0 双版本算一键）应全部链首存活"
  );
  assert!(count_live(&store, other).await? == 2);
  assert!(
    point_read(&session, &vector_key(&[], ctx, b"victim"))
      .await?
      .is_some()
  );

  // 清扫：9 个唯一物理键（8 子域键 + 跨前缀键），子域 0 双版本去重为一次墓碑
  let purged = session.purge_vector_context(ctx).await?;
  assert_eq!(purged, 9, "去重后应恰 9 次物理墓碑");

  // 不可扫描：目标上下文零链首存活版
  assert_eq!(count_live(&store, ctx).await?, 0, "清扫后不得残留存活记录");
  // 不可点查：全部项类型子域点读皆空
  for term in 0..8u64 {
    assert!(
      point_read(&session, &vector_key(&[], ctx | term, b"victim"))
        .await?
        .is_none(),
      "term {term} 点查应为空"
    );
  }
  assert!(
    point_read(&session, &vector_key(&[0x05, 0x07], ctx, b"victim"))
      .await?
      .is_none()
  );

  // 幸存者不受波及
  assert_eq!(count_live(&store, other).await?, 2);
  assert!(
    point_read(&session, &vector_key(&[], other, b"keep0"))
      .await?
      .is_some()
  );
  assert_eq!(
    session.read(b"user_key").await?.as_deref(),
    Some(b"plain".as_slice())
  );

  // 幂等重扫零删除
  assert_eq!(session.purge_vector_context(ctx).await?, 0, "重扫应零删除");
  Ok(())
}

/// 测试 2：墓碑键不复活也不重复计数（扫描段不过滤最新态会收集到其存活旧版，
/// 但删除段 delete_raw 探测链首已墓碑即零追加返回假——实际墓碑键数不含已删键）
#[compio::test]
async fn purge_skips_tombstoned_keys() -> Void {
  let (_dir, store) = open_test_store("vector_cleanup_tombstone")?;
  let session = store.new_session()?;
  let ctx = 8u64;

  session
    .upsert_raw(&vector_key(&[], ctx, b"gone"), &[1u8; 4])
    .await?;
  session
    .upsert_raw(&vector_key(&[], ctx | 1, b"alive"), &[2u8; 4])
    .await?;
  // 预先墓碑一个键（生产对位：元素级 VREM 已删的记录）
  session.delete_raw(&vector_key(&[], ctx, b"gone")).await?;
  assert!(
    point_read(&session, &vector_key(&[], ctx, b"gone"))
      .await?
      .is_none()
  );

  // 已墓碑键不计入实际墓碑数（链首墓碑探测短路），仅 alive 一次
  let purged = session.purge_vector_context(ctx).await?;
  assert_eq!(purged, 1, "已墓碑键不得计入清扫");

  assert_eq!(count_live(&store, ctx).await?, 0);
  assert!(
    point_read(&session, &vector_key(&[], ctx | 1, b"alive"))
      .await?
      .is_none()
  );
  Ok(())
}

/// 测试 3：不存在的上下文清扫为空操作（零扫描命中、零删除、零错误）
#[compio::test]
async fn purge_unknown_context_is_noop() -> Void {
  let (_dir, store) = open_test_store("vector_cleanup_noop")?;
  let session = store.new_session()?;
  assert_eq!(session.purge_vector_context(1 << 20).await?, 0);
  assert_eq!(session.purge_vector_context(0).await?, 0);
  Ok(())
}
