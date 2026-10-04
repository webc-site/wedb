//! 迁移/快照 TTL 逐位保真与读间隙到期防护回归测试（工单 wedb-migrate-live-value-ttl-ms-roundtrip-truncation）
//!
//! 验证点：
//! 1. 构造非 16 对齐裸 ticks TTL 键迁移往返与全量同步快照往返，断言接收端落盘 TTL ticks 与源端逐位一致（杜绝 Unix 毫秒往返截断与亚毫秒丢失）；
//! 2. 构造值读后 TTL 读前到期的窗口用例（TEST_LIVE_VALUE_READ_HOOK 注入），断言 read_live_value 落入 LiveValue::Gone 臂，不产生无 TTL 永生键装帧且源端不删。

use std::sync::Arc;

use async_lock::Mutex;
use parking_lot::Mutex as PlMutex;
use wbase::time::now_ticks;
use wconn::record::{
  BatchItem, MigrationFrame, MigrationRecord, encode_migration_payload, parse_migration_payload,
};
use wedb::server::{
  cluster_provider::ClusterProvider,
  migration::{
    chunk_reassembler::ChunkReassembler,
    frame_import::{FrameImport, import_migration_frames},
    migrate_driver::{LiveValue, TEST_LIVE_VALUE_READ_HOOK, read_live_value},
  },
};
use wedb_test::store_node::{StoreNode, open_store};
use wnode::{
  StorageSession, range_index::TreeStreamMeta, storage::session::common::ttl_sync::put_ttl_sync,
};
use wval::{GarnetObjectType, KeyTag};

/// 留钩静态跨用例串行门（钩槽系进程级单例，用例并行会互相抢占武装）
static CASE_GUARD: Mutex<()> = Mutex::new(());

const KEY_STR_UNALIGNED: &[u8] = b"key:unaligned:str";
const VAL_STR: &[u8] = b"value-for-unaligned-str";
const KEY_ENV_UNALIGNED: &[u8] = b"key:unaligned:env";
const VAL_ENV: &[u8] = b"\x03hash-unaligned-payload";
const KEY_GAP_EXPIRE: &[u8] = b"key:gap:expiring";
const VAL_GAP: &[u8] = b"gap-expiring-val";

/// 验证点 1：非 16 对齐裸 ticks TTL 键迁移往返与带外树流往返，接收端与源端逐位一致
#[compio::test]
async fn raw_ticks_migration_and_snapshot_roundtrip_preserves_submillisecond() {
  let _guard = CASE_GUARD.lock().await;

  let StoreNode {
    _dir,
    store: src_store,
  } = open_store("ttl_unaligned_src.db");
  let StoreNode {
    _dir,
    store: dst_store,
  } = open_store("ttl_unaligned_dst.db");
  let dst_provider = ClusterProvider::new();
  dst_provider.set_store(Arc::clone(&dst_store));

  // 构造非 16 对齐裸 ticks TTL：余数 7 或 9 ticks（亚毫秒位），保证 16 对齐与毫秒格均失真
  let future_ticks = {
    let base = now_ticks() + 120_000 * 10_000;
    if (base + 7) % 16 != 0 {
      base + 7
    } else {
      base + 9
    }
  };
  assert_ne!(future_ticks % 16, 0, "断言为非 16 对齐 ticks");
  assert_ne!(future_ticks % 10_000, 0, "断言含非零亚毫秒位 ticks");

  // 1) 源端造键：带裸 ticks 的 String 键与 Hash 信封键
  {
    let sess = src_store.new_session().unwrap();
    let batch = sess.enter_batch();
    let storage = StorageSession::new(batch);
    storage
      .upsert_string(KEY_STR_UNALIGNED, VAL_STR)
      .await
      .unwrap();
    storage
      .expire_at_ticks(KEY_STR_UNALIGNED, future_ticks)
      .await
      .unwrap();

    storage
      .upsert_tag(KEY_ENV_UNALIGNED, KeyTag::ObjectEnvelope, VAL_ENV)
      .await
      .unwrap();
    storage
      .expire_at_ticks(KEY_ENV_UNALIGNED, future_ticks)
      .await
      .unwrap();
  }

  // 2) 发送端 read_live_value 读值：断言提取到的 ticks 为精确原值，绝无毫秒截断
  let (str_val, str_ticks) = {
    let sess = src_store.new_session().unwrap();
    let batch = sess.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    match read_live_value(&storage, None, KEY_STR_UNALIGNED)
      .await
      .unwrap()
    {
      LiveValue::Migratable(val, ticks) => (val, ticks),
      _ => panic!("KEY_STR_UNALIGNED 应为可迁移活值"),
    }
  };
  assert_eq!(str_ticks, future_ticks, "String 键提取到精确裸 ticks");

  let (env_val, env_ticks) = {
    let sess = src_store.new_session().unwrap();
    let batch = sess.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    match read_live_value(&storage, None, KEY_ENV_UNALIGNED)
      .await
      .unwrap()
    {
      LiveValue::Migratable(val, ticks) => (val, ticks),
      _ => panic!("KEY_ENV_UNALIGNED 应为可迁移活值"),
    }
  };
  assert_eq!(env_ticks, future_ticks, "Envelope 键提取到精确裸 ticks");

  // 3) 装帧与编码解码往返：断言帧格式线格式直接承载 .NET Ticks
  let items = vec![
    BatchItem {
      key: KEY_STR_UNALIGNED,
      val: str_val,
      expire_ticks: str_ticks,
    },
    BatchItem {
      key: KEY_ENV_UNALIGNED,
      val: env_val,
      expire_ticks: env_ticks,
    },
  ];
  let payload = encode_migration_payload(&items);
  let (count, frames) = parse_migration_payload(&payload).unwrap();
  assert_eq!(count, 2);
  match &frames[0] {
    MigrationFrame::Record(MigrationRecord::Str { expire_ticks, .. }) => {
      assert_eq!(
        *expire_ticks, future_ticks,
        "线格式 String 帧 TTL 为精确裸 ticks"
      );
    }
    _ => panic!("首帧应为 String 记录"),
  }
  match &frames[1] {
    MigrationFrame::Record(MigrationRecord::Env { expire_ticks, .. }) => {
      assert_eq!(
        *expire_ticks, future_ticks,
        "线格式 Env 帧 TTL 为精确裸 ticks"
      );
    }
    _ => panic!("次帧应为 Env 记录"),
  }

  // 4) 接收端导入
  {
    let dst_sess = dst_store.new_session().unwrap();
    let dst_batch = dst_sess.enter_batch();
    let dst_storage = StorageSession::new(dst_batch);
    let chunks = PlMutex::new(ChunkReassembler::new());
    let ri = None;
    let import = FrameImport {
      provider: &dst_provider,
      session: &dst_sess,
      storage: &dst_storage,
      chunks: &chunks,
      ri: &ri,
      replace: true,
      vector_slot: 0,
      accept_domain_frames: false,
    };
    import_migration_frames(frames, &import)
      .await
      .expect("迁移帧导入必须成功");
  }

  // 5) 接收端断言：底层落盘 TTL ticks 与源端逐位全等（绝无 ≤1ms 向下偏取误差）
  {
    let sess = dst_store.new_session().unwrap();
    let batch = sess.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    assert_eq!(
      storage.batch.ttl_of(KEY_STR_UNALIGNED).await.unwrap(),
      Some(future_ticks),
      "接收端 String 键 TTL 与源端逐位一致"
    );
    assert_eq!(
      storage.batch.ttl_of(KEY_ENV_UNALIGNED).await.unwrap(),
      Some(future_ticks),
      "接收端 Env 键 TTL 与源端逐位一致"
    );
  }

  // 6) 带外树流流元 (kind=4 RangeIndexStream) 裸 ticks 直落接收端 put_ttl
  {
    let meta = TreeStreamMeta {
      obj_type: GarnetObjectType::Hash,
      next_expiry: i64::MAX,
      expire_ticks: future_ticks,
    };
    assert_eq!(meta.expire_ticks, future_ticks);
    let dst_sess = dst_store.new_session().unwrap();
    dst_sess
      .put_ttl(b"tree_key", meta.expire_ticks)
      .await
      .unwrap();
    assert_eq!(
      dst_sess.ttl_of(b"tree_key").await.unwrap(),
      Some(future_ticks),
      "TreeStreamMeta expire_ticks 直落接收端 put_ttl 保持精确 ticks"
    );
  }
}

/// 验证点 2：字符串键值读与 TTL 读两独立 await 间隙内键到期，复核存活失败归 Gone 臂，杜绝无 TTL 永生键装帧
#[compio::test]
async fn live_value_read_gap_expiration_falls_to_gone_and_never_emits_eternal_key() {
  let _guard = CASE_GUARD.lock().await;

  let StoreNode { _dir, store } = open_store("ttl_gap_expire.db");

  // 1) 源端写入存活键（带有有效 TTL）
  let initial_ticks = now_ticks() + 100_000 * 10_000;
  {
    let sess = store.new_session().unwrap();
    let batch = sess.enter_batch();
    let storage = StorageSession::new(batch);
    storage
      .upsert_string(KEY_GAP_EXPIRE, VAL_GAP)
      .await
      .unwrap();
    storage
      .expire_at_ticks(KEY_GAP_EXPIRE, initial_ticks)
      .await
      .unwrap();
  }

  // 2) 注入留钩：在值读完成之后、TTL 读之前，将键 TTL 篡改为过去的时间戳（模拟间隙内超时过期）
  let past_ticks = now_ticks() - 1000;
  let store_clone = Arc::clone(&store);
  *TEST_LIVE_VALUE_READ_HOOK.lock() = Some(Box::new(move || {
    let sess = store_clone.new_session().unwrap();
    let batch = sess.enter_batch();
    // 篡改为已过期时间戳（过去的 1000 ticks），通过 put_ttl_sync 原位覆写 TTL sidecar 记录
    put_ttl_sync(&batch, KEY_GAP_EXPIRE, past_ticks).unwrap();
  }));

  // 3) 执行 read_live_value：值读命中 VAL_GAP，随后留钩触发改为已过期；TTL 读复核发现 exp <= now_ticks
  let res = {
    let sess = store.new_session().unwrap();
    let batch = sess.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    read_live_value(&storage, None, KEY_GAP_EXPIRE)
      .await
      .unwrap()
  };

  // 4) 核心断言：必须落入 LiveValue::Gone 臂！绝不落入 LiveValue::Migratable(_, 0)！
  assert!(
    matches!(res, LiveValue::Gone),
    "读间隙到期键必须归入 LiveValue::Gone 臂，严禁落入 Migratable(val, 0) 无 TTL 永生键！"
  );

  // 5) 断言留钩已被消费
  assert!(
    TEST_LIVE_VALUE_READ_HOOK.lock().is_none(),
    "读窗留钩必须已被消费"
  );

  // 6) 断言源端不删：底层数据物理记录与 TTL 记录完好存在，read_live_value 读间隙不物理误删源端数据
  {
    let sess = store.new_session().unwrap();
    let batch = sess.enter_batch();
    let rec_k = batch.session_tag_key(KeyTag::String, KEY_GAP_EXPIRE);
    assert!(
      batch.read_raw(&rec_k).await.unwrap().is_some(),
      "源端底层数据物理记录完好，read_live_value 绝不误删源端数据"
    );
    assert_eq!(
      batch.ttl_of(KEY_GAP_EXPIRE).await.unwrap(),
      Some(past_ticks),
      "源端底层 TTL 记录完好"
    );
  }
}

/// 验证点 3：信封键值读与 TTL 读间隙内键到期，复核存活失败同样归 Gone 臂且源端不删
#[compio::test]
async fn live_value_env_read_gap_expiration_falls_to_gone() {
  let _guard = CASE_GUARD.lock().await;

  let StoreNode { _dir, store } = open_store("ttl_env_gap_expire.db");
  let key = b"key:gap:expiring:env";
  let val = b"\x03env-gap-val";

  let initial_ticks = now_ticks() + 100_000 * 10_000;
  {
    let sess = store.new_session().unwrap();
    let batch = sess.enter_batch();
    let storage = StorageSession::new(batch);
    storage
      .upsert_tag(key, KeyTag::ObjectEnvelope, val)
      .await
      .unwrap();
    storage.expire_at_ticks(key, initial_ticks).await.unwrap();
  }

  let past_ticks = now_ticks() - 1000;
  let store_clone = Arc::clone(&store);
  *TEST_LIVE_VALUE_READ_HOOK.lock() = Some(Box::new(move || {
    let sess = store_clone.new_session().unwrap();
    let batch = sess.enter_batch();
    put_ttl_sync(&batch, key, past_ticks).unwrap();
  }));

  let res = {
    let sess = store.new_session().unwrap();
    let batch = sess.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    read_live_value(&storage, None, key).await.unwrap()
  };

  assert!(
    matches!(res, LiveValue::Gone),
    "Envelope 读间隙到期键必须归入 LiveValue::Gone 臂"
  );
  assert!(
    TEST_LIVE_VALUE_READ_HOOK.lock().is_none(),
    "读窗留钩必须已被消费"
  );

  // 断言源端不删
  {
    let sess = store.new_session().unwrap();
    let batch = sess.enter_batch();
    let rec_k = batch.session_tag_key(KeyTag::ObjectEnvelope, key);
    assert!(
      batch.read_raw(&rec_k).await.unwrap().is_some(),
      "源端信封物理记录完好，read_live_value 读间隙不物理误删源端数据"
    );
    assert_eq!(
      batch.ttl_of(key).await.unwrap(),
      Some(past_ticks),
      "源端底层 TTL 记录完好"
    );
  }
}
