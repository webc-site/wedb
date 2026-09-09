//! TTL 写路径集成测试（EXPIREAT / PERSIST 返回码语义）
//!
//! 覆盖：expire_at 融合读改写路径的返回码口径（-2 / 0 / 1 / 2）、NX/XX/GT/LT
//! 四条件组合（含条件不满足绝不误删、孤儿 TTL 记录绝不误判存活）、过去时间戳
//! 立即物理删除、persist 融合路径四态，以及集合元数据读入口的惰性过期不可见语义
//! （读路径 TTL 探测收敛的回归锁）。

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use tempfile::{TempDir, tempdir};
use wbase::time::now_ms;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, TtlOpt, WedbStore};
use wval::{CollectionType, MetaValue, TtlCodec};

/// 构造独立临时库（4KB 页 / 16 页，GC 关闭避免后台物理删除干扰断言）
async fn open_store(tag: &str) -> aok::Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("ttl_{tag}.db")),
  )?);
  let mut config = StoreConfig::new(1024, 4096, 16, 0.5)?;
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// 断言键已被物理删除：数据读空、contains_key 为假、raw 层 TTL 记录消失
async fn assert_purged(
  session: &wkv::StoreSession<SegmentedDevice>,
  key: &[u8],
) -> aok::Result<()> {
  assert_eq!(session.read(key).await?, None, "数据记录必须物理读空");
  assert!(
    !session.contains_key(key).await?,
    "contains_key 必须视同不存在"
  );
  assert_eq!(
    session.read_raw(&session.ttl_key(key)).await?,
    None,
    "raw 层 TTL 记录必须墓碑化消失"
  );
  Ok(())
}

/// 测试 1: expire_at 对已过期键返回 -2 并惰性物理清除（数据 + raw 层 TTL 记录）；
/// 从未存在的键同样返回 -2 且无任何副作用
#[test]
fn test_expire_at_expired_key_returns_minus_two() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("expired").await?;
    let session = store.new_session()?;

    // 已过期键（TTL 记录仍在，惰性未清除）：expire_at 视同不存在返回 -2
    let dead = b"ttl:dead";
    session.upsert(dead, b"v").await?;
    assert_eq!(
      session.expire_at(dead, now_ms() + 50, TtlOpt::NONE).await?,
      1
    );
    sleep(Duration::from_millis(120)).await;
    assert_eq!(
      session
        .expire_at(dead, now_ms() + 60_000, TtlOpt::NONE)
        .await?,
      -2,
      "已过期键必须视同不存在返回 -2"
    );
    assert_purged(&session, dead).await?;

    // 从未存在的键：各选项一律 -2 且零副作用
    for opt in [
      TtlOpt::NONE,
      TtlOpt {
        nx: true,
        ..TtlOpt::NONE
      },
      TtlOpt {
        xx: true,
        ..TtlOpt::NONE
      },
      TtlOpt {
        gt: true,
        ..TtlOpt::NONE
      },
      TtlOpt {
        lt: true,
        ..TtlOpt::NONE
      },
    ] {
      assert_eq!(
        session
          .expire_at(b"ttl:never", now_ms() + 60_000, opt)
          .await?,
        -2
      );
    }
    assert_purged(&session, b"ttl:never").await?;

    // 孤儿 TTL 记录（数据从未写入，仅手写 TTL 记录）：存活判定以数据记录为准，
    // 绝不因 TTL 记录存在而误判存活——这是融合路径以裸数据判定在前的语义锁
    let orphan = b"ttl:orphan";
    let future = now_ms() + 60_000;
    session
      .upsert_raw(&session.ttl_key(orphan), &TtlCodec::encode(future))
      .await?;
    assert_eq!(
      session.expire_at(orphan, future, TtlOpt::NONE).await?,
      -2,
      "孤儿 TTL 记录不得让不存在的键误判存活"
    );
    assert_eq!(session.pttl_ms(orphan).await?, -2);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 2: NX/XX/GT/LT 四条件组合——设置成功返回 1、条件不满足返回 0 且
/// 原 TTL 与键存活状态原样保留（条件校验先于任何删除，绝不误删）
#[test]
fn test_expire_at_nx_xx_gt_lt_conditions() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("conditions").await?;
    let session = store.new_session()?;
    let future = now_ms() + 60_000;
    let nx = TtlOpt {
      nx: true,
      ..TtlOpt::NONE
    };
    let xx = TtlOpt {
      xx: true,
      ..TtlOpt::NONE
    };
    let gt = TtlOpt {
      gt: true,
      ..TtlOpt::NONE
    };
    let lt = TtlOpt {
      lt: true,
      ..TtlOpt::NONE
    };

    // NX：无 TTL 键成功；已有 TTL 键拒绝且 TTL 不变
    session.upsert(b"k:nx", b"v").await?;
    assert_eq!(session.expire_at(b"k:nx", future, nx).await?, 1);
    let with_ttl = session.pttl_ms(b"k:nx").await?;
    assert!(with_ttl > 0);
    assert_eq!(session.expire_at(b"k:nx", future, nx).await?, 0);
    assert!(
      (session.pttl_ms(b"k:nx").await? - with_ttl).abs() <= 5,
      "NX 拒绝后原 TTL 必须原样保留"
    );

    // XX：无 TTL 键拒绝；已有 TTL 键成功
    session.upsert(b"k:xx", b"v").await?;
    assert_eq!(session.expire_at(b"k:xx", future, xx).await?, 0);
    assert_eq!(session.pttl_ms(b"k:xx").await?, -1, "XX 拒绝后不得残留 TTL");
    assert_eq!(session.expire_at(b"k:xx", future, xx).await?, 1);
    assert!(session.pttl_ms(b"k:xx").await? > 0);

    // GT：无 TTL 键拒绝；已有 TTL 键仅当新值更大才成功（相等亦拒绝）
    session.upsert(b"k:gt", b"v").await?;
    assert_eq!(session.expire_at(b"k:gt", future, gt).await?, 0);
    assert_eq!(session.expire_at(b"k:gt", future, xx).await?, 1);
    assert_eq!(
      session.expire_at(b"k:gt", future, gt).await?,
      0,
      "相等不满足 GT"
    );
    assert_eq!(session.expire_at(b"k:gt", future + 10_000, gt).await?, 1);

    // LT：已有 TTL 键仅当新值更小才成功（相等亦拒绝）；无 TTL 键按既有口径放行
    assert_eq!(
      session.expire_at(b"k:gt", now_ms() + 30_000, lt).await?,
      1,
      "更小的新过期时间满足 LT"
    );
    assert_eq!(
      session.expire_at(b"k:gt", now_ms() + 30_000, lt).await?,
      0,
      "相等不满足 LT"
    );
    session.upsert(b"k:lt", b"v").await?;
    assert_eq!(session.expire_at(b"k:lt", future, lt).await?, 1);

    // 条件不满足绝不误删：GT 拒绝后键仍存活可读
    assert_eq!(session.read(b"k:gt").await?, Some(b"v".to_vec()));

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 3: 过去时间戳在条件全部通过后立即物理删除（返回 2）；
/// 条件不满足时过去时间戳绝不触发删除（判序：先选项校验、后过去时间戳删除）
#[test]
fn test_expire_at_past_timestamp_deletes() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("past").await?;
    let session = store.new_session()?;
    let past = now_ms() - 1_000;
    let future = now_ms() + 60_000;
    let nx = TtlOpt {
      nx: true,
      ..TtlOpt::NONE
    };

    // 无 TTL 键 + 过去时间戳：条件通过，立即物理删除返回 2
    session.upsert(b"k:past", b"v").await?;
    assert_eq!(
      session.expire_at(b"k:past", past, TtlOpt::NONE).await?,
      2,
      "过去时间戳必须立即物理删除并返回 2"
    );
    assert_purged(&session, b"k:past").await?;
    assert_eq!(session.pttl_ms(b"k:past").await?, -2);

    // 已有 TTL 键 + NX + 过去时间戳：条件先行拒绝返回 0，键绝不误删
    session.upsert(b"k:keep", b"v").await?;
    assert_eq!(session.expire_at(b"k:keep", future, nx).await?, 1);
    assert_eq!(
      session.expire_at(b"k:keep", past, nx).await?,
      0,
      "NX 条件不满足必须先于过去时间戳删除返回 0"
    );
    assert_eq!(session.read(b"k:keep").await?, Some(b"v".to_vec()));
    assert!(
      session.pttl_ms(b"k:keep").await? > 0,
      "误删防线：原 TTL 保留"
    );

    // 已过期键 + 过去时间戳：已过期分支先行，返回 -2 而非 2
    let dead = b"k:past:dead";
    session.upsert(dead, b"v").await?;
    assert_eq!(session.expire_at(dead, now_ms() + 40, nx).await?, 1);
    sleep(Duration::from_millis(100)).await;
    assert_eq!(session.expire_at(dead, past, TtlOpt::NONE).await?, -2);
    assert_purged(&session, dead).await?;

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 4: persist 融合路径四态——有 TTL 移除返回 1、无 TTL/不存在返回 0、
/// 已过期键惰性物理清除后返回 0
#[test]
fn test_persist_fusion() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("persist").await?;
    let session = store.new_session()?;

    // 有 TTL：移除成功返回 1，随后 pttl -1 且数据完好
    session.upsert(b"k:persist", b"v").await?;
    assert_eq!(
      session
        .expire_at(b"k:persist", now_ms() + 60_000, TtlOpt::NONE)
        .await?,
      1
    );
    assert_eq!(session.persist(b"k:persist").await?, 1);
    assert_eq!(session.pttl_ms(b"k:persist").await?, -1);
    assert_eq!(session.read(b"k:persist").await?, Some(b"v".to_vec()));

    // 无 TTL：返回 0
    assert_eq!(session.persist(b"k:persist").await?, 0);

    // 已过期未清除：惰性物理清除后返回 0
    session.upsert(b"k:expired", b"v").await?;
    assert_eq!(
      session
        .expire_at(b"k:expired", now_ms() + 40, TtlOpt::NONE)
        .await?,
      1
    );
    sleep(Duration::from_millis(100)).await;
    assert_eq!(session.persist(b"k:expired").await?, 0);
    assert_purged(&session, b"k:expired").await?;

    // 不存在：返回 0
    assert_eq!(session.persist(b"k:never").await?, 0);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 5: 读路径 TTL 探测收敛回归锁——过期集合在元数据读取（load_meta）与
/// contains_key 下不可见且物理清除，清除后重复读取稳定为 None（守卫无递归）
#[test]
fn test_expired_collection_invisible_on_meta_read() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("coll_meta").await?;
    let session = store.new_session()?;

    // 存活集合（Hash size=2）设 TTL，过期后未 GC
    let meta = MetaValue::new(1, CollectionType::Hash, 0, 2);
    session.save_meta(b"coll", &meta).await?;
    assert_eq!(
      session
        .expire_at(b"coll", now_ms() + 40, TtlOpt::NONE)
        .await?,
      1
    );
    sleep(Duration::from_millis(100)).await;

    // 元数据读取：惰性过期守卫清除并视同不存在
    assert!(
      session.load_meta(b"coll").await?.is_none(),
      "过期集合在元数据读取下必须不可见"
    );
    assert_purged(&session, b"coll").await?;

    // 清除后重复读取稳定为 None：TTL 守卫仅裁决一次、无递归二次清除
    assert!(session.load_meta(b"coll").await?.is_none());
    assert!(!session.contains_key(b"coll").await?);

    // 未过期集合照常可见
    let live = MetaValue::new(2, CollectionType::Hash, 0, 3);
    session.save_meta(b"coll:live", &live).await?;
    assert_eq!(
      session
        .expire_at(b"coll:live", now_ms() + 60_000, TtlOpt::NONE)
        .await?,
      1
    );
    assert!(session.load_meta(b"coll:live").await?.is_some());
    assert!(session.contains_key(b"coll:live").await?);

    aok::Result::<()>::Ok(())
  })?;
  OK
}
