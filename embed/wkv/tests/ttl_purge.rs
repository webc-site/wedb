//! TTL 过期物理清除的单条化挂点测试（purge 端口 + 物理写镜像抑制）
//!
//! 覆盖：
//! 1. purge 端口未注册：purge 链保持现状两条物理墓碑镜像（TTL 记录 + 数据），
//!    嵌入式无 AOF 场景行为不变；
//! 2. purge 端口注册：链内物理 notify 被会话级精确抑制，端口恰好触发一次且
//!    参数 (ns, db, 用户键, expire_at_ms) 正确，purge 后普通写镜像不受影响
//!    （抑制标志零残留）；
//! 3. expire_at 过去时间戳与 persist 过期分支同样经端口单条化，携带精确到期值。

use std::sync::{Arc, Mutex};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wbase::time::now_ms;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, TtlOpt, WedbStore};
use wval::TtlCodec;

/// 物理写镜像记录 (key, val, tombstone)
type MirrorLog = Arc<Mutex<Vec<(Vec<u8>, Vec<u8>, bool)>>>;
/// purge 端口记录 (ns, db, 用户键, expire_at_ms)
type PurgeLog = Arc<Mutex<Vec<(u64, u64, Vec<u8>, u64)>>>;

/// 构造独立临时库（4KB 页 / 16 页，GC 关闭避免后台物理删除干扰断言）
async fn open_store(tag: &str) -> aok::Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("ttl_purge_{tag}.db")),
  )?);
  let mut config = StoreConfig::new(1024, 4096, 16, 0.5)?;
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// 注入物理写镜像记录器（须在创建任何会话前调用）
fn mirror_listener(log: MirrorLog) -> wkv::WriteListenerFn {
  Arc::new(move |key, val, tombstone| {
    log
      .lock()
      .unwrap()
      .push((key.to_vec(), val.to_vec(), tombstone));
  })
}

/// 注入 purge 端口记录器
fn purge_listener(log: PurgeLog) -> wkv::TtlPurgeListenerFn {
  Arc::new(move |ns, db, key, expire_at_ms| {
    log
      .lock()
      .unwrap()
      .push((ns, db, key.to_vec(), expire_at_ms));
  })
}

/// 测试 1：purge 端口未注册——purge 链保持现状两条物理墓碑镜像（回归锁：
/// 嵌入式无 AOF 场景行为与历史完全一致）
#[test]
fn purge_without_listener_keeps_two_physical_mirrors() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("no_port").await?;
    let log: MirrorLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_write_listener(mirror_listener(Arc::clone(&log))));
    let session = store.new_session()?;
    session.set_context(5, 2);

    let key = b"k:legacy";
    session.upsert(key, b"v").await?;
    // 直接写入已过期的 TTL 记录（免 sleep），惰性清除走 check_expired 路径
    let past = now_ms() - 1_000;
    session.put_ttl(key, past).await?;
    let ttl_k = session.ttl_key(key);
    let str_k = session.session_string_key(key);

    let before = log.lock().unwrap().len();
    // 读路径惰性过期：物理清除（TTL 记录 + 数据两条墓碑）
    assert_eq!(session.read(key).await?, None);
    assert_eq!(session.pttl_ms(key).await?, -2);

    let mirrors = &log.lock().unwrap()[before..].to_vec();
    assert_eq!(
      mirrors,
      &vec![
        (ttl_k.as_slice().to_vec(), Vec::new(), true),
        (str_k.as_slice().to_vec(), Vec::new(), true),
      ],
      "端口未注册时 purge 链必须保持两条物理墓碑镜像"
    );
    OK
  })
}

/// 测试 2：purge 端口注册——链内物理 notify 被抑制、端口恰好一次且参数正确、
/// purge 后普通写镜像不受影响（抑制标志零残留）
#[test]
fn purge_with_listener_suppresses_mirror_and_fires_once() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("with_port").await?;
    let log: MirrorLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_write_listener(mirror_listener(Arc::clone(&log))));
    let purges: PurgeLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_ttl_purge_listener(purge_listener(Arc::clone(&purges))));
    let session = store.new_session()?;
    session.set_context(5, 2);

    let key = b"k:lazy";
    session.upsert(key, b"v").await?;
    let past = now_ms() - 1_000;
    session.put_ttl(key, past).await?;

    let before = log.lock().unwrap().len();
    // 读路径惰性过期：两条物理墓碑镜像被抑制，端口恰好触发一次
    assert_eq!(session.read(key).await?, None);
    assert_eq!(
      log.lock().unwrap().len(),
      before,
      "purge 链内不得泄漏任何物理写镜像"
    );
    assert_eq!(
      *purges.lock().unwrap(),
      vec![(5, 2, key.to_vec(), past)],
      "purge 端口必须恰好收到一次且 (ns, db, key, expire_at_ms) 正确"
    );

    // 抑制标志零残留：purge 之后的普通写镜像照常触发（镜像为物理键，含 ns/db 前缀）
    session.upsert(b"k:after", b"v2").await?;
    let after_k = session.session_string_key(b"k:after");
    let mirrors = log.lock().unwrap();
    let (k, v, tomb) = mirrors.last().unwrap();
    assert_eq!(
      (k.as_slice(), v.as_slice(), *tomb),
      (after_k.as_slice(), &b"v2"[..], false),
      "purge 后普通写镜像必须照常触发（标志零残留）"
    );
    OK
  })
}

/// 测试 3：expire_at 过去时间戳（返回 2）与 persist 过期分支（返回 0）同样
/// 经端口单条化，到期值取调用方传入/记录中的精确时间戳
#[test]
fn purge_port_covers_expire_at_past_and_persist_branches() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("branches").await?;
    let log: MirrorLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_write_listener(mirror_listener(Arc::clone(&log))));
    let purges: PurgeLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_ttl_purge_listener(purge_listener(Arc::clone(&purges))));
    let session = store.new_session()?;

    // expire_at 过去时间戳分支：立即物理删除，端口携带该过去时间戳
    let past = now_ms() - 2_000;
    session.upsert(b"k:exp", b"v").await?;
    let before = log.lock().unwrap().len();
    assert_eq!(session.expire_at(b"k:exp", past, TtlOpt::NONE).await?, 2);
    assert_eq!(
      log.lock().unwrap().len(),
      before,
      "expire_at 过去分支必须零镜像"
    );
    assert_eq!(
      *purges.lock().unwrap(),
      vec![(0, 0, b"k:exp".to_vec(), past)]
    );
    purges.lock().unwrap().clear();

    // persist 过期分支：惰性清除后返回 0，端口携带 TTL 记录中的到期值
    let expired = now_ms() - 1_000;
    session.upsert(b"k:persist", b"v").await?;
    session.put_ttl(b"k:persist", expired).await?;
    assert_eq!(
      session.read_raw(&session.ttl_key(b"k:persist")).await?,
      Some(TtlCodec::encode(expired).to_vec())
    );
    let before = log.lock().unwrap().len();
    assert_eq!(session.persist(b"k:persist").await?, 0);
    assert_eq!(
      log.lock().unwrap().len(),
      before,
      "persist 过期分支必须零镜像"
    );
    assert_eq!(
      *purges.lock().unwrap(),
      vec![(0, 0, b"k:persist".to_vec(), expired)]
    );
    OK
  })
}
