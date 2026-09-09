//! hash 字段级 TTL 全链路集成测试（对标 Redis 7.4 HEXPIRE 系 / Garnet HashObject
//! 字段级过期与 ObjectCollectTask 后台收集）
//!
//! 覆盖：hexpire_at 四返回码（-2/-1/0/1）与 NX/XX/GT/LT 四条件、hpersist 四态、
//! 读路径字段级惰性 purge（到期后读不可见且物理回写压缩载荷）、has_expire 粘性
//! 标志置位与未置位时读路径零额外开销（记录地址不变断言零回写）、GC run_once
//! 驱动后台字段收集闭环（物理清除 + 统计口径）、过去时间戳立即删字段与删空即
//! 删键、以及 WRONGTYPE 传播。

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use tempfile::{TempDir, tempdir};
use wbase::time::now_ms;
use wdev::SegmentedDevice;
use wkv::{GcConfig, GcManager, StoreConfig, StoreSession, TtlOpt, WedbStore};
use wval::{CollectionType, CompactHashCodec, META_VALUE_SIZE, MetaValue, StorageEncoding};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// 构造独立临时库（page 4KB / 16 页，GC 关闭避免后台物理清除干扰断言）
async fn open_store(tag: &str) -> aok::Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("hash_ttl_{tag}.db")),
  )?);
  let mut config = StoreConfig::new(1024, 4096, 16, 0.5)?;
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// 写入一个紧凑 Hash 集合（key_id 自增保证唯一），返回写入的字段数
async fn put_hash(
  session: &StoreSession<SegmentedDevice>,
  key: &[u8],
  key_id: u64,
  fields: &[(&[u8], &[u8])],
) -> aok::Result<()> {
  let mut hash = wval::CompactHash::new();
  for (f, v) in fields {
    hash.set_field(f, v, None)?;
  }
  let meta = MetaValue::new(key_id, CollectionType::Hash, 0, fields.len() as u64);
  session
    .save_compact_meta(key, &meta, hash.as_slice())
    .await?;
  Ok(())
}

/// 读取集合元记录物理字节（32B MetaValue + 紧凑载荷）
async fn read_meta_record(
  session: &StoreSession<SegmentedDevice>,
  key: &[u8],
) -> aok::Result<Vec<u8>> {
  let meta_k = session.session_meta_key(key);
  Ok(session.read_raw(&meta_k).await?.expect("元记录必须存在"))
}

/// 测试 1: hexpire_at 四返回码 + NX/XX/GT/LT 四条件
#[test]
fn test_hexpire_at_return_codes_and_conditions() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("codes").await?;
    let session = store.new_session()?;
    let future = now_ms() + 60_000;
    let opt = |nx: bool, xx: bool, gt: bool, lt: bool| TtlOpt { nx, xx, gt, lt };

    // -2：集合不存在（各选项一律 -2 且零副作用）
    for o in [
      opt(false, false, false, false),
      opt(true, false, false, false),
      opt(false, true, false, false),
      opt(false, false, true, false),
      opt(false, false, false, true),
    ] {
      assert_eq!(
        session.hexpire_at(b"ht:nokey", b"f", future, o).await?,
        -2,
        "集合不存在必须返回 -2"
      );
    }

    // -1：集合存在但 field 不存在
    put_hash(&session, b"ht:k", 1, &[(b"a", b"v1"), (b"b", b"v2")]).await?;
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"missing", future, TtlOpt::NONE)
        .await?,
      -1
    );

    // WRONGTYPE：字符串键上传入 hexpire_at 显式报错（对齐 Redis WRONGTYPE）
    session.upsert(b"ht:str", b"v").await?;
    assert!(
      session
        .hexpire_at(b"ht:str", b"f", future, TtlOpt::NONE)
        .await
        .is_err()
    );

    // 1：无条件设置成功，绝对过期毫秒精确落载
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"a", future, TtlOpt::NONE)
        .await?,
      1
    );
    let meta_rec = read_meta_record(&session, b"ht:k").await?;
    assert!(
      session
        .load_meta(b"ht:k")
        .await?
        .is_some_and(|m| session_may_have_expire(&m)),
      "写路径必须置位 has_expire 粘性标志"
    );
    let payload = &meta_rec[META_VALUE_SIZE..];
    assert_eq!(
      CompactHashCodec::find(payload, b"a").unwrap().expire_at_ms,
      Some(future),
      "字段 TTL 必须精确落载"
    );
    assert_eq!(
      CompactHashCodec::find(payload, b"b").unwrap().expire_at_ms,
      None,
      "未设置 TTL 的字段不受影响"
    );

    // 0：NX——已有 TTL 的字段拒绝且原 TTL 原样保留
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"a", future, opt(true, false, false, false))
        .await?,
      0
    );
    let meta_rec = read_meta_record(&session, b"ht:k").await?;
    assert_eq!(
      CompactHashCodec::find(&meta_rec[META_VALUE_SIZE..], b"a")
        .unwrap()
        .expire_at_ms,
      Some(future)
    );

    // 0：XX——从未设 TTL 的字段拒绝
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"b", future, opt(false, true, false, false))
        .await?,
      0
    );
    // 1：XX——已有 TTL 的字段放行
    let later = future + 10_000;
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"a", later, opt(false, true, false, false))
        .await?,
      1
    );

    // GT：更小/相等拒绝，更大放行；从未设 TTL 的字段 GT 一律拒绝
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"a", future, opt(false, false, true, false))
        .await?,
      0,
      "GT 对更小的新值必须拒绝"
    );
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"a", later, opt(false, false, true, false))
        .await?,
      0,
      "GT 对相等的新值必须拒绝"
    );
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"a", later + 1, opt(false, false, true, false))
        .await?,
      1
    );
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"b", later + 1, opt(false, false, true, false))
        .await?,
      0,
      "GT 对无 TTL 字段无可比值必须拒绝"
    );

    // LT：更大/相等拒绝，更小放行；从未设 TTL 的字段按既有口径放行
    let shorter = now_ms() + 30_000;
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"a", later + 2, opt(false, false, false, true))
        .await?,
      0,
      "LT 对更大的新值必须拒绝"
    );
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"a", shorter, opt(false, false, false, true))
        .await?,
      1
    );
    assert_eq!(
      session
        .hexpire_at(b"ht:k", b"b", shorter, opt(false, false, false, true))
        .await?,
      1,
      "LT 对无 TTL 字段按无当前值口径放行"
    );

    // 条件拒绝后字段值原样保留（绝不误删）
    let meta_rec = read_meta_record(&session, b"ht:k").await?;
    assert_eq!(
      CompactHashCodec::find(&meta_rec[META_VALUE_SIZE..], b"a")
        .unwrap()
        .value,
      b"v1"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// load_meta 结果的 has_expire 粘性标志断言辅助（保留 reserved 位读取出口）
fn session_may_have_expire(meta: &MetaValue) -> bool {
  meta.reserved[1] & 0x80 != 0
}

/// 测试 2: hpersist 四态（-2/-1/0/1）与 NX 联动
#[test]
fn test_hpersist_codes() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("persist").await?;
    let session = store.new_session()?;
    let future = now_ms() + 60_000;

    // -2：集合不存在
    assert_eq!(session.hpersist(b"hp:nokey", b"f").await?, -2);

    put_hash(&session, b"hp:k", 1, &[(b"a", b"v1"), (b"b", b"v2")]).await?;
    // -1：field 不存在
    assert_eq!(session.hpersist(b"hp:k", b"missing").await?, -1);
    // 0：field 存在但未设置字段级 TTL
    assert_eq!(session.hpersist(b"hp:k", b"a").await?, 0);
    // 1：移除成功，随后字段 TTL 消失且 NX 重新可设
    assert_eq!(
      session
        .hexpire_at(b"hp:k", b"a", future, TtlOpt::NONE)
        .await?,
      1
    );
    assert_eq!(session.hpersist(b"hp:k", b"a").await?, 1);
    let meta_rec = read_meta_record(&session, b"hp:k").await?;
    let payload = &meta_rec[META_VALUE_SIZE..];
    assert_eq!(
      CompactHashCodec::find(payload, b"a").unwrap().expire_at_ms,
      None,
      "persist 后字段 TTL 必须移除"
    );
    assert_eq!(
      CompactHashCodec::count(payload).ok(),
      Some(2),
      "persist 不得删字段"
    );
    assert_eq!(
      session
        .hexpire_at(
          b"hp:k",
          b"a",
          future,
          TtlOpt {
            nx: true,
            ..TtlOpt::NONE
          }
        )
        .await?,
      1,
      "persist 后 NX 必须重新放行"
    );
    // 值原样保留
    let meta_rec = read_meta_record(&session, b"hp:k").await?;
    assert_eq!(
      CompactHashCodec::find(&meta_rec[META_VALUE_SIZE..], b"a")
        .unwrap()
        .value,
      b"v1"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 3: 读路径字段级惰性 purge——到期字段对读取不可见且载荷被物理回写压缩
#[test]
fn test_lazy_purge_on_read_compresses_payload() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("lazy").await?;
    let session = store.new_session()?;
    put_hash(
      &session,
      b"ht:lazy",
      1,
      &[(b"live", b"v1"), (b"dead", b"v2")],
    )
    .await?;
    assert_eq!(
      session
        .hexpire_at(b"ht:lazy", b"dead", now_ms() + 50, TtlOpt::NONE)
        .await?,
      1
    );
    // 标志置位物理验证：元记录 reserved[1] 最高位为 1
    let meta_rec = read_meta_record(&session, b"ht:lazy").await?;
    assert!(
      meta_rec[10] & 0x80 != 0,
      "hexpire_at 写路径必须置位元记录 has_expire 标志"
    );
    sleep(Duration::from_millis(120)).await;
    // 读取前的物理记录仍含过期条目（惰性未清除）
    let rec_before_read = read_meta_record(&session, b"ht:lazy").await?;
    assert_eq!(MetaValue::read_size(&rec_before_read), Ok(2));
    assert_eq!(
      CompactHashCodec::count(&rec_before_read[META_VALUE_SIZE..]).ok(),
      Some(2)
    );

    // 到期字段对读取不可见：meta.size 同步缩减，载荷中 dead 消失
    let rc = session
      .load_collection_raw_read(b"ht:lazy", CollectionType::Hash)
      .await?
      .expect("存活集合必须可见");
    assert_eq!(rc.meta.size, 1, "purge 后元素计数必须同步缩减");
    assert_eq!(rc.meta.encoding(), StorageEncoding::Compact);
    let payload = rc.compact_payload().unwrap();
    assert!(CompactHashCodec::find(payload, b"live").is_some());
    assert!(
      CompactHashCodec::find(payload, b"dead").is_none(),
      "过期字段必须对读取不可见"
    );

    // 回写物理验证：可变区内经动态松弛原位收缩或 RCU 追加（字节级对比不依赖布局细节）
    let rec_after_read = read_meta_record(&session, b"ht:lazy").await?;
    assert_ne!(
      rec_after_read, rec_before_read,
      "存在过期字段时读取必须触发物理回写"
    );
    assert_eq!(
      MetaValue::read_size(&rec_after_read),
      Ok(1),
      "元记录 size 必须回写"
    );
    assert_eq!(
      CompactHashCodec::count(&rec_after_read[META_VALUE_SIZE..]).ok(),
      Some(1)
    );

    // 稳态：再次读取零回写（无过期字段可清），内容稳定
    let addr_stable = store.index.find_tag(&session.session_meta_key(b"ht:lazy"));
    let rc2 = session
      .load_collection_raw_read(b"ht:lazy", CollectionType::Hash)
      .await?
      .unwrap();
    assert_eq!(rc2.meta.size, 1);
    assert_eq!(
      store.index.find_tag(&session.session_meta_key(b"ht:lazy")),
      addr_stable,
      "无过期字段时读取不得再次回写"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 4: 标志未置位的 hash 读取零额外开销——记录地址与物理字节完全不变
///（单探针门控：无字段 TTL 的绝大多数 hash 零回写、零扫描副作用）
#[test]
fn test_unflagged_hash_read_zero_overhead() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("noopt").await?;
    let session = store.new_session()?;
    put_hash(&session, b"ht:plain", 1, &[(b"f1", b"v1"), (b"f2", b"v2")]).await?;

    let meta_k = session.session_meta_key(b"ht:plain");
    let addr_before = store.index.find_tag(&meta_k);
    let rec_before = read_meta_record(&session, b"ht:plain").await?;
    assert!(
      rec_before[10] & 0x80 == 0,
      "前置：从未写过字段 TTL 标志必须未置位"
    );
    sleep(Duration::from_millis(30)).await;

    for _ in 0..3 {
      let rc = session
        .load_collection_raw_read(b"ht:plain", CollectionType::Hash)
        .await?
        .expect("集合必须可见");
      assert_eq!(rc.meta.size, 2);
      assert!(rc.compact_payload().is_some());
    }
    assert_eq!(
      store.index.find_tag(&meta_k),
      addr_before,
      "标志未置位的 hash 读取必须零回写（地址不变）"
    );
    assert_eq!(
      read_meta_record(&session, b"ht:plain").await?,
      rec_before,
      "标志未置位的 hash 读取必须零字节改动"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 5: GC run_once 驱动后台字段收集闭环——到期字段物理清除 + 统计口径
///（对标 Garnet ObjectCollectTask 后台收集可观测性）
#[test]
fn test_gc_run_once_collects_expired_fields() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("hash_ttl_gc.db"),
    )?);
    let mut config = StoreConfig::new(1024, 4096, 16, 0.5)?;
    config.gc = GcConfig {
      compaction_max_segments: 0,
      ..GcConfig::default()
    };
    config.gc.enabled = false;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);

    put_hash(&session, b"ht:gc", 1, &[(b"live", b"v1"), (b"dead", b"v2")]).await?;
    assert_eq!(
      session
        .hexpire_at(b"ht:gc", b"dead", now_ms() + 50, TtlOpt::NONE)
        .await?,
      1
    );
    sleep(Duration::from_millis(120)).await;

    mgr.run_once().await?;
    let st = mgr.stats();
    assert_eq!(
      st.last_scan_fields_deleted, 1,
      "后台收集必须物理清除到期字段"
    );
    assert_eq!(st.expired_fields_deleted, 1);
    assert_eq!(st.expired_deleted, 0, "无 key 级 TTL 时不得误删键");
    assert_eq!(st.last_scan_deleted, 0);

    // 物理闭环验证：载荷压缩、size 回写、live 字段完好
    let meta_rec = read_meta_record(&session, b"ht:gc").await?;
    assert_eq!(MetaValue::read_size(&meta_rec), Ok(1));
    let payload = &meta_rec[META_VALUE_SIZE..];
    assert_eq!(CompactHashCodec::count(payload).ok(), Some(1));
    assert!(CompactHashCodec::find(payload, b"live").is_some());
    assert!(CompactHashCodec::find(payload, b"dead").is_none());

    // 再跑一轮：标志粘性但无过期字段，幂等零清除
    mgr.run_once().await?;
    let st2 = mgr.stats();
    assert_eq!(st2.last_scan_fields_deleted, 0, "幂等双检后必须零清除");
    assert_eq!(st2.expired_fields_deleted, 1, "累计数必须保持");

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 6: 过去时间戳立即物理删除字段（返回 1）；NX 条件先行拒绝时绝不误删；
/// 删除最后一个存活字段后集合随之消亡（删空即删键，含 key 级 TTL 清除）
#[test]
fn test_past_timestamp_and_empty_hash_removal() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("past").await?;
    let session = store.new_session()?;
    let past = now_ms() - 1_000;
    let future = now_ms() + 60_000;

    put_hash(&session, b"ht:past", 1, &[(b"a", b"v1"), (b"b", b"v2")]).await?;
    // NX + 过去时间戳：条件先行拒绝返回 0，字段绝不误删
    assert_eq!(
      session
        .hexpire_at(
          b"ht:past",
          b"a",
          future,
          TtlOpt {
            nx: true,
            ..TtlOpt::NONE
          }
        )
        .await?,
      1
    );
    assert_eq!(
      session
        .hexpire_at(
          b"ht:past",
          b"a",
          past,
          TtlOpt {
            nx: true,
            ..TtlOpt::NONE
          }
        )
        .await?,
      0,
      "NX 条件不满足必须先于过去时间戳删除"
    );
    let meta_rec = read_meta_record(&session, b"ht:past").await?;
    assert!(CompactHashCodec::find(&meta_rec[META_VALUE_SIZE..], b"a").is_some());

    // 无条件 + 过去时间戳：立即物理删除字段返回 1
    assert_eq!(
      session
        .hexpire_at(b"ht:past", b"a", past, TtlOpt::NONE)
        .await?,
      1
    );
    let meta_rec = read_meta_record(&session, b"ht:past").await?;
    assert!(CompactHashCodec::find(&meta_rec[META_VALUE_SIZE..], b"a").is_none());
    assert_eq!(MetaValue::read_size(&meta_rec), Ok(1));

    // 删除最后一个存活字段：集合消亡（元记录与 key 级 TTL 一并清除）
    assert_eq!(
      session
        .hexpire_at(b"ht:past", b"b", past, TtlOpt::NONE)
        .await?,
      1
    );
    assert!(session.load_meta(b"ht:past").await?.is_none());
    assert!(!session.contains_key(b"ht:past").await?);
    assert_eq!(
      session
        .read_raw(&session.session_meta_key(b"ht:past"))
        .await?,
      None,
      "删空后元记录必须物理删除"
    );
    // 后续 hexpire_at 视同集合不存在
    assert_eq!(
      session
        .hexpire_at(b"ht:past", b"b", future, TtlOpt::NONE)
        .await?,
      -2
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 7: 过期字段在 hexpire_at/hpersist 下视同不存在（-1）且装载即被惰性 purge
#[test]
fn test_expired_field_treated_as_missing() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("expired_f").await?;
    let session = store.new_session()?;
    put_hash(&session, b"ht:ef", 1, &[(b"live", b"v1"), (b"dead", b"v2")]).await?;
    assert_eq!(
      session
        .hexpire_at(b"ht:ef", b"dead", now_ms() + 50, TtlOpt::NONE)
        .await?,
      1
    );
    sleep(Duration::from_millis(120)).await;

    // hexpire_at：过期字段视同不存在返回 -1（过期即不存在，NX 也无资格放行），
    // 且装载即完成惰性 purge（物理回写压缩）
    assert_eq!(
      session
        .hexpire_at(b"ht:ef", b"dead", now_ms() + 60_000, TtlOpt::NONE)
        .await?,
      -1
    );
    assert_eq!(session.hpersist(b"ht:ef", b"dead").await?, -1);
    let meta_rec = read_meta_record(&session, b"ht:ef").await?;
    assert_eq!(MetaValue::read_size(&meta_rec), Ok(1));
    assert!(CompactHashCodec::find(&meta_rec[META_VALUE_SIZE..], b"dead").is_none());

    aok::Result::<()>::Ok(())
  })?;
  OK
}
