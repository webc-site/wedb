//! 同桶偶合自死锁防护回归（票 task/ing/wkv-ephemeral-latch-selfdeadlock）
//!
//! reviv 开启态，外层 user_key 桶闩（`RmwWindow` / ttl.rs 的 KeyLatch）与内层
//! 记录物理键（`prefix|KeyTag|user_key`）桶闩偶合同桶时，内层取闩失败重试环
//! 曾无界纯同步自旋——持有者正是本调用自身未返回的外层窗口，窗口 Drop
//! 不可达，命令永久挂死。修复后四处环（inplace.rs 三写内核环与 read.rs
//! `drive_mem_read` 驱动环）统一有界预算（INNER_LATCH_RETRY_BUDGET），耗尽即
//! 回 `Error::Index(LockTimeout)` 上抛退窗（窗口随栈 Drop 放闩），客户端得
//! 可重试错误而非挂死。
//!
//! 对标 C# InternalRMW/InternalUpsert/InternalRead 取闩失败即整操作
//! RETRY_LATER 上抛交外层重试——重试发起时本操作不持有任何其他闩，单一物理
//! 键形态无嵌套双取，挂死在 C# 结构上不可表达；rust 双锁基并存的偶合面由
//! 预算机制兜底（可用性残缺如实保留：偶合键确定性报错，根治留后续议题）。
//!
//! 用例形态：固定序确定性搜索同桶偶合键（fast_hash 种子 0 跨进程确定性），
//! 断言 INCR/EXPIRE/PERSIST 面有限时间内返回可重试错误或正常完成（挂死
//! 护栏：独立工作线程 + 主线程超时收案）；reviv 关闭默认档同偶合键全族
//! 正常完成（内层环免锁零触达，行为零变化）。

use std::{sync::mpsc, thread, time::Duration};

use aok::{OK, Void};
use compio::runtime::Runtime;
use wbase::{align::DEFAULT_SECTOR_SIZE, time::now_ticks};
use wdev::Device;
use whasher::fast_hash;
use wkv::{Error, StoreSession, TtlOpt};
use wval::KeyTag;

use crate::support::{config, open_store};

/// 挂死护栏：工作闭包在独立线程执行，主测试线程限时收案——预算环未生效时
/// 工作线程无界自旋，护栏侧超时失败（红即达意，无需回收工作线程）
fn bounded<T: Send + 'static>(name: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
  let (tx, rx) = mpsc::channel();
  thread::spawn(move || {
    let _ = tx.send(f());
  });
  rx.recv_timeout(Duration::from_secs(60))
    .unwrap_or_else(|_| panic!("{name} 有限时间内未返回：内层闩无界自旋回归"))
}

/// 固定序确定性搜索「user_key 主桶（窗口/TTL 键闩 scoped 口径，会话物理前缀
/// 种子）与指定标签记录键主桶（裸键物理域）偶合同桶」的用户键
///（`whasher::scoped_hash` 与标签记录键 `fast_hash` 均固定种子跨进程确定性，
/// 搜索纯哈希计算零 I/O；每键五标签偶合概率约标签数/桶数，1024 桶下百万域内
/// 必中）。同时要求其余断言面标签异桶——单一偶合面（EXPIRE 臂的存活判定读
/// String 记录键、INCR 臂读 Ttl 门，该键偶合时错误来自另一面而非断言面）
fn find_collided_key<D: Device>(session: &StoreSession<D>, tag: KeyTag) -> Vec<u8> {
  let index = session.store.index.load();
  let prefix = session.session_prefix();
  let bucket_of = |tag: KeyTag, k: &[u8]| {
    index.bucket_index_for_hash(fast_hash(session.session_tag_key(tag, k).as_slice()))
  };
  for i in 0..1_000_000u32 {
    let k = format!("selfdl_{tag:?}_{i:06}").into_bytes();
    let user = index.bucket_index_for_hash(whasher::scoped_hash(prefix.as_slice(), &k));
    // 其余断言面标签不偶合：INCR 面排除 Ttl 桶、EXPIRE/PERSIST 面排除 String 桶
    if [KeyTag::String, KeyTag::Ttl]
      .iter()
      .any(|t| *t != tag && bucket_of(*t, &k) == user)
    {
      continue;
    }
    if bucket_of(tag, &k) == user {
      return k;
    }
  }
  panic!("搜索域内无单标签偶合键");
}

/// 序列 A（INCR 自死锁面）：String 记录键桶与 user_key 桶偶合。RmwWindow 持闩
/// 期内 try_rmw_sync 的内层 upsert X-Latch 恒失败——预算耗尽即退窗上抛，
/// 窗口随栈 Drop 放闩后桶可复取（无泄漏自持）
#[test]
fn rmw_upsert_collided_key_bounded() -> Void {
  let env = open_store(
    "selfdl_rmw.db",
    config(1024, DEFAULT_SECTOR_SIZE, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  bounded("INCR 面偶合键预算环", move || {
    Runtime::new().unwrap().block_on(async {
      let session = store.new_session()?;
      let key = find_collided_key(&session, KeyTag::String);
      // 初值盲写（无外层窗口，内层闩首试即得）成功——偶合只在双锁基嵌套时成立
      session.upsert(&key, b"1").await?;
      let batch = session.enter_batch();
      let window = batch.try_rmw_window(&key).expect("无人竞争取窗必成");
      // 预算耗尽：可重试 LockTimeout 上抛（不在持闩窗口内消化、无窗口内二次自旋）
      assert!(
        matches!(window.try_rmw_sync(b"2"), Err(Error::Index(_))),
        "偶合键 RMW 写回应答可重试错误"
      );
      drop(window);
      drop(batch);
      // 放闩复核：窗口 Drop 已释放 user_key 桶闩，重取必成（杜绝泄漏自持）
      let batch = session.enter_batch();
      assert!(batch.try_rmw_window(&key).is_some(), "退窗后桶闩必须可复取");
      OK
    })
  })
}

/// 序列 B（EXPIRE 自死锁面）：Ttl 记录键桶与 user_key 桶偶合。expire_at 的
/// KeyLatch 持闩期内 put_ttl 落笔（upsert 环）与 ttl_of 重读
/// （drive_mem_read S-Latch 环）均撞自桶——两臂各自预算耗尽退窗上抛
#[test]
fn expire_persist_collided_key_bounded() -> Void {
  let env = open_store(
    "selfdl_expire.db",
    config(1024, DEFAULT_SECTOR_SIZE, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  bounded("EXPIRE/PERSIST 面偶合键预算环", move || {
    Runtime::new().unwrap().block_on(async {
      let session = store.new_session()?;
      let key = find_collided_key(&session, KeyTag::Ttl);
      session.upsert(&key, b"v").await?;
      // 臂一（upsert 环）：KeyLatch 内 put_ttl 落笔撞自桶，EXPIRE 应答可重试错误
      assert!(
        matches!(
          session
            .expire_at(&key, now_ticks() + 600_000_000, TtlOpt::NONE)
            .await,
          Err(Error::Index(_))
        ),
        "偶合键 EXPIRE 应答可重试错误而非挂死"
      );
      // 臂二（drive_mem_read 环）：窗外预置 TTL 记录（无外层窗可落笔）后，
      // KeyLatch 内 ttl_of 重读 S-Latch 撞自桶，PERSIST 同应答可重试错误
      session.put_ttl(&key, now_ticks() + 600_000_000).await?;
      assert!(
        matches!(session.persist(&key).await, Err(Error::Index(_))),
        "偶合键 PERSIST 应答可重试错误而非挂死"
      );
      OK
    })
  })
}

/// reviv 关闭默认档回归：同一批偶合键（哈希确定性，跨进程同一批）上
/// RMW/EXPIRE/PERSIST 全族正常完成——内层环免锁零触达，行为零变化
#[test]
fn collided_key_reviv_off_default_unchanged() -> Void {
  let env = open_store(
    "selfdl_reviv_off.db",
    config(1024, DEFAULT_SECTOR_SIZE, 16)?,
  )?;
  Runtime::new()?.block_on(async {
    let store = env.store;
    let session = store.new_session()?;
    let rmw_k = find_collided_key(&session, KeyTag::String);
    let ttl_k = find_collided_key(&session, KeyTag::Ttl);
    // INCR 面：窗口持闩期内层免锁，写回闭环
    session.upsert(&rmw_k, b"1").await?;
    let batch = session.enter_batch();
    let window = batch.try_rmw_window(&rmw_k).expect("取窗必成");
    assert!(
      window.try_rmw_sync(b"2")?.is_ok(),
      "默认档 RMW 写回正常完成"
    );
    drop(window);
    drop(batch);
    // EXPIRE/PERSIST 面：KeyLatch 持闩期 TTL 落笔与重读均免锁闭环
    session.upsert(&ttl_k, b"v").await?;
    assert_eq!(
      session
        .expire_at(&ttl_k, now_ticks() + 600_000_000, TtlOpt::NONE)
        .await?,
      1,
      "默认档 EXPIRE 正常落笔"
    );
    assert_eq!(session.persist(&ttl_k).await?, 1, "默认档 PERSIST 正常移除");
    assert_eq!(session.read(&rmw_k).await?, Some(b"2".to_vec()));
    assert_eq!(session.read(&ttl_k).await?, Some(b"v".to_vec()));
    OK
  })
}
