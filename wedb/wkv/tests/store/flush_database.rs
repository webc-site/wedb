//! 按库清空（flush_database / flush_all_databases）隔离语义测试。
//!
//! flush_database / flush_all_databases 语义测试（实现与 C# 映射见 wkv/src/store.rs）
//!
//! 共享单日志多库模型：清库 0 不得波及库 1 与其它命名空间的同号库，
//! 随键 TTL 旁路记录与对象信封域记录一并清除。

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use wval::KeyTag;

use crate::support::{config, open_store};

/// 多库写入 → flush_database(0, 0) → 库 0 全清（String/ObjectEnvelope 域与
/// TTL 旁路记录），库 1 与命名空间 1 的同号库数据完好
#[test]
fn test_flush_database_isolation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("flush_database.db", config(2048, 64 * 1024, 16)?)?;
    let store = env.store;

    // 库 0：两个 String 键（其一带 TTL 侧车记录）+ 一个对象信封域记录
    let db0 = store.new_session()?;
    db0.set_context(0, 0);
    db0.upsert(b"k0:plain", b"v0").await?;
    db0.upsert(b"k0:ttl", b"v0").await?;
    db0.put_ttl(b"k0:ttl", i64::MAX).await?;
    db0
      .upsert_tag(b"k0:obj", KeyTag::ObjectEnvelope, b"\x01payload")
      .await?;

    // 库 1：数据 + TTL 侧车记录，清库 0 后必须完好
    let db1 = store.new_session()?;
    db1.set_context(0, 1);
    db1.upsert(b"k1", b"v1").await?;
    db1.put_ttl(b"k1", i64::MAX).await?;

    // 命名空间 1 的同号库 0：不得被 flush_database(0, 0) 波及
    let ns1 = store.new_session()?;
    ns1.set_context(1, 0);
    ns1.upsert(b"kns", b"vns").await?;

    let deleted = store.flush_database(0, 0).await?;
    assert_eq!(
      deleted, 3,
      "库 0 应删除 3 个用户键（String x2 + ObjectEnvelope x1）"
    );

    // 库 0 全清：String 域、对象信封域与 TTL 旁路记录一并消失
    db0.set_context(0, 0);
    assert_eq!(db0.read(b"k0:plain").await?, None);
    assert_eq!(db0.read(b"k0:ttl").await?, None);
    assert_eq!(db0.ttl_of(b"k0:ttl").await?, None, "TTL 侧车记录须随键清除");
    let env_key = db0.session_tag_key(KeyTag::ObjectEnvelope, b"k0:obj");
    let obj_hit = db0.read_raw_with(&env_key, |_| ()).await?;
    assert!(obj_hit.is_none(), "对象信封域记录须一并清除");

    // 库 1 完好：数据与 TTL 侧车记录不受影响
    db1.set_context(0, 1);
    assert_eq!(db1.read(b"k1").await?, Some(b"v1".to_vec()));
    // put_ttl 落盘前 4-bit coarse 粗化（ExpirationWithOption.cs:22-23），
    // i64::MAX 读回为低 4 位清零值
    assert_eq!(
      db1.ttl_of(b"k1").await?,
      Some((i64::MAX >> 4) << 4)
    );

    // 命名空间 1 的库 0 完好
    ns1.set_context(1, 0);
    assert_eq!(ns1.read(b"kns").await?, Some(b"vns".to_vec()));

    info!("flush_database 单库隔离测试通过");
    OK
  })
}

///
/// 多库写入 → flush_all_databases → 全部域无存活用户键（FLUSHALL 全清语义）
#[test]
fn test_flush_all_databases() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("flush_all_databases.db", config(2048, 64 * 1024, 16)?)?;
    let store = env.store;

    for (ns, db, key) in [(0, 0, b"a"), (0, 1, b"b"), (1, 0, b"c")] {
      let session = store.new_session()?;
      session.set_context(ns, db);
      session.upsert(key, b"v").await?;
    }

    let deleted = store.flush_all_databases().await?;
    assert_eq!(deleted, 3, "全清应删除全部 3 个用户键");

    for (ns, db, key) in [(0u64, 0u64, &b"a"[..]), (0, 1, b"b"), (1, 0, b"c")] {
      let session = store.new_session()?;
      session.set_context(ns, db);
      assert_eq!(session.read(key).await?, None, "{ns}/{db} 域应已清空");
    }

    info!("flush_all_databases 全清测试通过");
    OK
  })
}
