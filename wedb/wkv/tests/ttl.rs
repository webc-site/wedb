//! TTL 写路径集成测试（EXPIREAT / PERSIST 返回码语义）
//!
//! 覆盖：expire_at 融合读改写路径的返回码口径（-2 / 0 / 1 / 2）、NX/XX/GT/LT
//! 四条件组合（含条件不满足绝不误删、孤儿 TTL 记录绝不误判存活）、过去时间戳
//! 立即物理删除、persist 融合路径四态，以及集合元数据读入口的惰性过期不可见语义
//! （读路径 TTL 探测收敛的回归锁）。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/ExpirationTests.cs + test/standalone/Garnet.test/RespTests.cs（EXPIRE/EXPIREAT/PERSIST/TTL 语义）

use std::time::Duration;

use aok::{OK, Void};
use compio::time::sleep;
use wbase::{
  convert::{TICKS_PER_MILLISECOND, TICKS_PER_SECOND, unix_time_in_milliseconds_from_ticks},
  time::now_ticks,
};
use wdev::SegmentedDevice;
use wkv::{StoreResult, TtlOpt};
use wtest_base::open_test_store;
use wval::{GarnetObjectType, I64Codec, KeyTag, MetaValue};

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

/// 确定性过期屏障：轮询裸 TTL 记录（ttl_of 纯读，不触发裁决、不物理清除）
/// 取落盘粗化过期点，墙钟真越过该点才返回——替代固定 sleep，消除
/// 「单调 sleep 对墙钟步进」的时序假设（now_ticks 走 SystemTime 墙钟而
/// sleep 走单调钟，NTP 回拨只会推迟屏障通过，绝不让「已过期」前提
/// 不成立时进入裁决断言）
async fn wait_expired(session: &wkv::StoreSession<SegmentedDevice>, key: &[u8]) -> aok::Result<()> {
  let exp = session
    .ttl_of(key)
    .await?
    .expect("前置：被测键必须已设 TTL 记录");
  while now_ticks() <= exp {
    sleep(Duration::from_millis(1)).await;
  }
  Ok(())
}

/// 测试 1: expire_at 对已过期键返回 -2 并惰性物理清除（数据 + raw 层 TTL 记录）；
/// 从未存在的键同样返回 -2 且无任何副作用
#[compio::test]
async fn test_expire_at_expired_key_returns_minus_two() -> Void {
  let (_dir, store) = open_test_store("expired")?;
  let session = store.new_session()?;

  // 已过期键（TTL 记录仍在，惰性未清除）：expire_at 视同不存在返回 -2
  let dead = b"ttl:dead";
  session.upsert(dead, b"v").await?;
  assert_eq!(
    session
      .expire_at(dead, now_ticks() + TICKS_PER_MILLISECOND * 50, TtlOpt::NONE)
      .await?,
    1
  );
  wait_expired(&session, dead).await?;
  assert_eq!(
    session
      .expire_at(dead, now_ticks() + TICKS_PER_SECOND * 60, TtlOpt::NONE)
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
        .expire_at(b"ttl:never", now_ticks() + TICKS_PER_SECOND * 60, opt)
        .await?,
      -2
    );
  }
  assert_purged(&session, b"ttl:never").await?;

  // 孤儿 TTL 记录（数据从未写入，仅手写 TTL 记录）：存活判定以数据记录为准，
  // 绝不因 TTL 记录存在而误判存活——这是融合路径以裸数据判定在前的语义锁
  let orphan = b"ttl:orphan";
  let future = now_ticks() + TICKS_PER_SECOND * 60;
  session
    .upsert_raw(&session.ttl_key(orphan), &I64Codec::encode(future))
    .await?;
  assert_eq!(
    session.expire_at(orphan, future, TtlOpt::NONE).await?,
    -2,
    "孤儿 TTL 记录不得让不存在的键误判存活"
  );
  assert_eq!(session.pttl_ms(orphan).await?, -2);
  OK
}

/// 测试 2: NX/XX/GT/LT 四条件组合——设置成功返回 1、条件不满足返回 0 且
/// 原 TTL 与键存活状态原样保留（条件校验先于任何删除，绝不误删）
#[compio::test]
async fn test_expire_at_nx_xx_gt_lt_conditions() -> Void {
  let (_dir, store) = open_test_store("conditions")?;
  let session = store.new_session()?;
  let future = now_ticks() + TICKS_PER_SECOND * 60;
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
  assert_eq!(session.expiretime_ms(b"k:xx").await?, -1);
  assert_eq!(session.expire_at(b"k:xx", future, TtlOpt::NONE).await?, 1);
  assert_eq!(
    session.expiretime_ms(b"k:xx").await?,
    unix_time_in_milliseconds_from_ticks(future)
  );
  let updated_future = future + TICKS_PER_SECOND * 10;
  assert_eq!(session.expire_at(b"k:xx", updated_future, xx).await?, 1);
  assert_eq!(
    session.expiretime_ms(b"k:xx").await?,
    unix_time_in_milliseconds_from_ticks(updated_future)
  );
  assert!(session.pttl_ms(b"k:xx").await? > 0);

  // GT：无 TTL 键拒绝；已有 TTL 键仅当新值更大才成功（相等亦拒绝）
  session.upsert(b"k:gt", b"v").await?;
  assert_eq!(session.expire_at(b"k:gt", future, gt).await?, 0);
  assert_eq!(session.pttl_ms(b"k:gt").await?, -1);
  assert_eq!(session.expire_at(b"k:gt", future, TtlOpt::NONE).await?, 1);
  assert_eq!(
    session.expire_at(b"k:gt", future, gt).await?,
    0,
    "相等不满足 GT"
  );
  assert_eq!(
    session
      .expire_at(b"k:gt", future + TICKS_PER_SECOND * 10, gt)
      .await?,
    1
  );

  // LT：已有 TTL 键仅当新值更小才成功（相等亦拒绝）；无 TTL 键按既有口径放行
  assert_eq!(
    session
      .expire_at(b"k:gt", now_ticks() + TICKS_PER_SECOND * 30, lt)
      .await?,
    1,
    "更小的新过期时间满足 LT"
  );
  assert_eq!(
    session
      .expire_at(b"k:gt", now_ticks() + TICKS_PER_SECOND * 30, lt)
      .await?,
    0,
    "相等不满足 LT"
  );
  session.upsert(b"k:lt", b"v").await?;
  assert_eq!(session.expire_at(b"k:lt", future, lt).await?, 1);

  // 条件不满足绝不误删：GT 拒绝后键仍存活可读
  assert_eq!(session.read(b"k:gt").await?, Some(b"v".to_vec()));
  OK
}

/// 测试 3: 过去时间戳在条件全部通过后立即物理删除（返回 2）；
/// 条件不满足时过去时间戳绝不触发删除（判序：先选项校验、后过去时间戳删除）
#[compio::test]
async fn test_expire_at_past_timestamp_deletes() -> Void {
  let (_dir, store) = open_test_store("past")?;
  let session = store.new_session()?;
  let past = now_ticks() - TICKS_PER_SECOND;
  let future = now_ticks() + TICKS_PER_SECOND * 60;
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
  assert_eq!(
    session
      .expire_at(dead, now_ticks() + TICKS_PER_MILLISECOND * 40, nx)
      .await?,
    1
  );
  wait_expired(&session, dead).await?;
  assert_eq!(session.expire_at(dead, past, TtlOpt::NONE).await?, -2);
  assert_purged(&session, dead).await?;
  OK
}

/// 测试 4: persist 融合路径四态——有 TTL 移除返回 1、无 TTL/不存在返回 0、
/// 已过期键惰性物理清除后返回 0
#[compio::test]
async fn test_persist_fusion() -> Void {
  let (_dir, store) = open_test_store("persist")?;
  let session = store.new_session()?;

  // 有 TTL：移除成功返回 1，随后 pttl -1 且数据完好
  session.upsert(b"k:persist", b"v").await?;
  assert_eq!(
    session
      .expire_at(
        b"k:persist",
        now_ticks() + TICKS_PER_SECOND * 60,
        TtlOpt::NONE
      )
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
      .expire_at(
        b"k:expired",
        now_ticks() + TICKS_PER_MILLISECOND * 40,
        TtlOpt::NONE
      )
      .await?,
    1
  );
  wait_expired(&session, b"k:expired").await?;
  assert_eq!(session.persist(b"k:expired").await?, 0);
  assert_purged(&session, b"k:expired").await?;

  // 不存在：返回 0
  assert_eq!(session.persist(b"k:never").await?, 0);
  OK
}

/// 测试 5: 读路径 TTL 探测收敛回归锁——过期索引在元数据读取（load_meta）与
/// contains_key 下不可见且物理清除，清除后重复读取稳定为 None（守卫无递归）
#[compio::test]
async fn test_expired_collection_invisible_on_meta_read() -> Void {
  let (_dir, store) = open_test_store("coll_meta")?;
  let session = store.new_session()?;

  // 存活 RI 索引元记录（size=2）设 TTL，过期后未 GC
  // （KeyTag::Meta 仅供 RangeIndex，直写元记录构造最小场景）
  let meta = MetaValue::new(1, GarnetObjectType::RangeIndex, 2);
  let meta_k = session.session_meta_key(b"coll");
  session.upsert_raw(&meta_k, &meta.to_bytes()).await?;
  assert_eq!(
    session
      .expire_at(
        b"coll",
        now_ticks() + TICKS_PER_MILLISECOND * 40,
        TtlOpt::NONE
      )
      .await?,
    1
  );
  wait_expired(&session, b"coll").await?;

  // 元数据读取：惰性过期守卫清除并视同不存在
  assert!(
    session.load_meta(b"coll").await?.is_none(),
    "过期集合在元数据读取下必须不可见"
  );
  assert_purged(&session, b"coll").await?;

  // 清除后重复读取稳定为 None：TTL 守卫仅裁决一次、无递归二次清除
  assert!(session.load_meta(b"coll").await?.is_none());
  assert!(!session.contains_key(b"coll").await?);

  // 未过期索引照常可见
  let live = MetaValue::new(2, GarnetObjectType::RangeIndex, 3);
  let live_k = session.session_meta_key(b"coll:live");
  session.upsert_raw(&live_k, &live.to_bytes()).await?;
  assert_eq!(
    session
      .expire_at(
        b"coll:live",
        now_ticks() + TICKS_PER_SECOND * 60,
        TtlOpt::NONE
      )
      .await?,
    1
  );
  assert!(session.load_meta(b"coll:live").await?.is_some());
  assert!(session.contains_key(b"coll:live").await?);
  OK
}

/// 键级 TTL 恒等裸写单源回归（deviations.md §143 收口形）：4-bit coarse
/// 粗化唯 wnode `network_expire` 命令边界一处施加（对标 C# 二参打包构造器
/// ExpirationWithOption.cs:20-24 唯 EXPIRE 族调用），异步会话入口 expire_at
/// 与 TTL 写内核 put_ttl 一律恒等直通——低 4 位非零的 ticks 落盘与输入逐位
/// 相等（对标 C# 存储侧 word 形恒等装载 UnifiedStore/RMWMethods.cs:216、
/// :228，SETEX/GETEX 族裸 ticks 经重放臂不再被会话入口摧毁）
#[compio::test]
async fn test_coarse_single_source_expire_at_put_ttl() -> Void {
  let (_dir, store) = open_test_store("coarse_ttl")?;
  let session = store.new_session()?;

  session.upsert(b"ck", b"v").await?;

  // 异步入口 expire_at：恒等直通，低 4 位非零的输入逐位落盘
  //（先 16 对齐再加 0b1011，低 4 位形态对任意时刻确定性成立）
  let expire = ((now_ticks() >> 4) << 4) + TICKS_PER_SECOND * 60 + 0b1011;
  assert_eq!(
    session.expire_at(b"ck", expire, TtlOpt::NONE).await?,
    1,
    "无附加条件对存活键设置成功"
  );
  let stored = session.ttl_of(b"ck").await?.expect("TTL 记录在场");
  assert_eq!(
    stored, expire,
    "expire_at 会话入口必须恒等直通（粗化唯命令边界，§143）"
  );
  assert_ne!(stored & 0xF, 0, "低 4 位原样保留（重放精度锁）");

  // 直通不改存活判定：落盘值仍 > now，读路径不触发清除
  assert!(session.contains_key(b"ck").await?);
  assert!(session.ttl_of(b"ck").await?.is_some());

  // 全精度比较口径：expire_at 对既有落盘值做 GT 判定，等值按严格大于口径
  // 拒绝（比较与落盘同源皆裸值）
  assert_eq!(
    session
      .expire_at(
        b"ck",
        stored,
        TtlOpt {
          gt: true,
          ..TtlOpt::NONE
        }
      )
      .await?,
    0,
    "等值重设按 GT 严格大于口径拒绝"
  );

  // 内核裸写对照臂：put_ttl 不粗化不判域，落盘与输入逐位相等
  //（值域裁决唯上游命令边界单点）
  let raw = now_ticks() + TICKS_PER_SECOND * 120 + 0b1011;
  session.upsert(b"ck-raw", b"v").await?;
  session.put_ttl(b"ck-raw", raw).await?;
  assert_eq!(
    session.ttl_of(b"ck-raw").await?.expect("TTL 记录在场"),
    raw,
    "put_ttl 裸写内核必须逐位相等，不得二次粗化"
  );
  OK
}

// 测试 7: 同步读快路径 TTL 同栈裁决（对标 C# ReadMethods.cs:Reader 内
// LogRecordUtils.CheckExpiry：CheckExpiry = HasExpiration && Expiration < UtcNow.Ticks）
/// ——未过期 TTL 键放行内存直读零拷贝命中（不再全量降级异步）；已过期键快路径
/// 直接 NOTFOUND（StoreResult::NotFound，物理清理留写路径惰性清退与后台 GC）；过期键
/// 经 SET 覆盖写入后同步清退旧 TTL，新值存活（语义自愈闭环）
#[compio::test]
async fn test_sync_read_fastpath_ttl_gate() -> Void {
  let (_dir, store) = open_test_store("sync_gate")?;
  let session = store.new_session()?;

  // 未过期 TTL 键：快路径放行直读命中（修复前 has_ttl 即降级 Ok(None)）
  session.upsert(b"sk:live", b"v1").await?;
  session
    .expire_at(
      b"sk:live",
      now_ticks() + TICKS_PER_SECOND * 60,
      TtlOpt::NONE,
    )
    .await?;
  {
    let batch = session.enter_batch();
    assert_eq!(
      batch.try_read_sync(b"sk:live", |v| v.to_vec())?,
      StoreResult::Success(b"v1".to_vec()),
      "未过期 TTL 键必须走快路径直读命中"
    );
  }

  // 已过期键（惰性未清除）：快路径直接 NOTFOUND，闭包不执行，不做物理清除
  session.upsert(b"sk:dead", b"v2").await?;
  session
    .expire_at(
      b"sk:dead",
      now_ticks() + TICKS_PER_MILLISECOND * 40,
      TtlOpt::NONE,
    )
    .await?;
  wait_expired(&session, b"sk:dead").await?;
  {
    let batch = session.enter_batch();
    let mut called = false;
    assert_eq!(
      batch.try_read_sync(b"sk:dead", |v| {
        called = true;
        v.to_vec()
      })?,
      StoreResult::NotFound,
      "已过期键快路径必须直接 NOTFOUND"
    );
    assert!(!called, "NOTFOUND 语义下读闭包不得执行");
    assert_eq!(
      batch.try_read_tag_sync_with_size(b"sk:dead", KeyTag::String, |v, _| v.to_vec())?,
      StoreResult::NotFound,
      "带尺寸读同口径 NOTFOUND"
    );
    // 物理记录仍驻留（清理留写路径/GC）：绕过 TTL 门控的裸读可见
    assert_eq!(
      batch.try_read_tag_in_memory_unprotected(b"sk:dead", KeyTag::String, |v| v.to_vec())?,
      StoreResult::Success(b"v2".to_vec()),
      "快路径 NOTFOUND 是逻辑过期，物理记录不得被同步路径删除"
    );
  }

  // 过期键 SET 覆盖：同步写面清退旧 TTL，新值存活可读（自愈闭环）
  {
    let batch = session.enter_batch();
    assert!(
      batch.try_upsert_sync(b"sk:dead", b"v3")?.is_ok(),
      "过期键覆盖写入不得因 TTL 清除受阻降级"
    );
    assert_eq!(
      batch.try_read_sync(b"sk:dead", |v| v.to_vec())?,
      StoreResult::Success(b"v3".to_vec()),
      "覆盖写入后新值必须存活"
    );
  }
  assert_eq!(
    session.pttl_ms(b"sk:dead").await?,
    -1,
    "旧 TTL 必须随 SET 覆盖同步清退"
  );

  // 无 TTL 键照常命中；不存在键快路径 NOTFOUND 口径不变
  session.upsert(b"sk:plain", b"v4").await?;
  {
    let batch = session.enter_batch();
    assert_eq!(
      batch.try_read_sync(b"sk:plain", |v| v.to_vec())?,
      StoreResult::Success(b"v4".to_vec())
    );
    assert_eq!(
      batch.try_read_sync(b"sk:missing", |v| v.to_vec())?,
      StoreResult::NotFound
    );
  }
  OK
}

/// 测试 8: check_expired 键独占闩与过期物理清理
///
/// 覆盖：
/// 1. 键已过期但外部持有键独占闩时：check_expired 获取独占闩失败放弃本轮，返回 Ok(false)，
///    且键数据与 TTL 记录均不被物理清理；
/// 2. 外部放闩后：check_expired 成功获取独占闩并复查到期，物理清除数据与 TTL 记录，返回 Ok(true)；
/// 3. 复查未过期防线：未到期键调用 check_expired 返回 Ok(false) 且不触发清理。
#[compio::test]
async fn test_check_expired_exclusive_latch() -> Void {
  let (_dir, store) = open_test_store("check_expired_latch")?;
  let session = store.new_session()?;

  let key = b"k:latch_test";
  session.upsert(key, b"val").await?;
  // 设置已过期时间戳
  let past = now_ticks() - TICKS_PER_SECOND;
  session.put_ttl(key, past).await?;

  // 1. 模拟并发操作（如 EXPIRE/PERSIST/事务）持有该键的独占闩
  {
    let index = store.index.load();
    // check_expired 键闩 scoped 口径（会话物理前缀种子，与窗口/事务键锁同桶）
    let latch = index
      .try_lock_key_hash_exclusive(whasher::scoped_hash(
        session.session_prefix().as_slice(),
        key,
      ))
      .expect("首度获取独占闩必须成功");

    // 独占闩被外部持有：check_expired 无法获取闩，放弃本轮返回 Ok(false)
    assert!(
      !session.check_expired(key).await?,
      "独占闩争用时 check_expired 必须放弃本轮返回 Ok(false)"
    );

    // 数据与 TTL 记录原样保留，未被误删
    assert!(
      session.read_raw(&session.ttl_key(key)).await?.is_some(),
      "持闩争用放弃时 TTL 记录不得被删除"
    );

    drop(latch);
  }

  // 2. 外部放闩后：check_expired 正常获取独占闩，完成复查并物理删除，返回 Ok(true)
  assert!(
    session.check_expired(key).await?,
    "无闩争用时 check_expired 必须成功获取闩并完成清理返回 Ok(true)"
  );
  assert_purged(&session, key).await?;

  // 3. 对未过期键：check_expired 初检未到期直接返回 Ok(false)，零副作用
  let live_key = b"k:live_test";
  session.upsert(live_key, b"live_val").await?;
  let future = now_ticks() + TICKS_PER_SECOND * 60;
  session.put_ttl(live_key, future).await?;

  assert!(
    !session.check_expired(live_key).await?,
    "未过期键 check_expired 必须返回 Ok(false)"
  );
  assert_eq!(
    session.read(live_key).await?,
    Some(b"live_val".to_vec()),
    "未到期键不得被误删"
  );
  OK
}
