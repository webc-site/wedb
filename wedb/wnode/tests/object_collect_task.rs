//! 周期过期对象收集任务集成测试
//!
//! 对标 C# libs/server/StoreWrapper.cs:ObjectCollectTaskAsync +
//! TryStartObjectCollectTask（周期驱动 ExecuteObjectCollection → 全库
//! Hash/ZSet 对象 HCOLLECT/ZCOLLECT，清除对象内已过期字段/成员）：
//! - 写入带短 TTL 字段/成员的 hash/zset，过期后不调 HCOLLECT/ZCOLLECT
//!   命令，断言周期任务自动回收；
//! - 频率禁用（expired-object-collection-freq = 0）时任务不拉起；
//! - CONFIG SET 运行时启用（C# ReconcilePrimaryTask(ObjectCollectTask)
//!   链路）→ 任务拉起并生效。

use std::{path::Path, sync::Arc, time::Duration};

use compio::{runtime::Runtime, time::sleep};
use wacl::AccessControlList;
use wdev::SegmentedDevice;
use wkv::StoreConfig;
use wnode::{
  MessageConsumerFace, PrimaryTasks, SessionProviderFace, WireFormat,
  resp::{
    RespSessionConsumer, garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions,
  },
  service::StorageSessionProvider,
};
use wtest_base::test_store_config;

/// 会话装配钩子（生产 decorate 形态：无集群切面的单机会话）
fn decorate(id: u64, api: StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> {
  Some(RespSessionConsumer::new(
    id,
    RespServerSessionOptions::default(),
    Arc::new(api),
  ))
}

fn open_provider(
  dir: &Path,
) -> StorageSessionProvider<
  impl Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>,
> {
  let config: StoreConfig = test_store_config();
  let acl = Arc::new(AccessControlList::new("").expect("new acl"));
  StorageSessionProvider::open_with_config(config, dir.join("collect.db"), decorate)
    .expect("open provider")
    .with_acl(acl)
}

fn frame(args: &[&str]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", args.len());
  for a in args {
    out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
  }
  out.into_bytes()
}

/// 命令收发（复刻生产泵 [`wnode/src/net/handler/drive.rs`] 消费循环语义：
/// 消费一批 → 会话挂起体取走 await 闭环、应答按流水线序并入 → 续消费至
/// 整帧消化）。HLEN/ZCARD 信封水位越线落物化矫正臂（collection.md §6.3）
/// 时同步段 `Ok(false)` 挂 SlowWait 停消费（core.rs 游标先推进后 break，
/// 帧整段消费而应答延迟产出），无人驱动即空应答（同 23cc330d 口径的
/// slow-wait 唯一通道，禁第二条回写路径）；无挂起须整段消化否则必红不掩盖
async fn exec(c: &mut RespSessionConsumer, args: &[&str]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  loop {
    let consumed = c.try_consume_messages_into(&mut resp);
    wnode_test::drive_pending_parks_consumer(c, &mut resp).await;
    let Some(slow) = c.take_slow_wait() else {
      assert_eq!(consumed, Some(0), "会话应整段消费并产出应答: {args:?}");
      return resp;
    };
    let reply = slow.resolve().await;
    c.resolve_slow_wait_into(&reply, &mut resp);
  }
}

#[test]
fn periodic_collect_reclaims_expired_fields_without_hcollect() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let provider = Arc::new(open_provider(dir.path()));
    let mut c = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("首会话装配");

    // 写入 hash 与 zset，各带一个短 TTL 字段/成员与一个常驻字段/成员
    let r = exec(&mut c, &["HSET", "h", "keep", "v", "gone", "v"]).await;
    assert_eq!(r, b":2\r\n");
    assert_eq!(
      exec(&mut c, &["HPEXPIRE", "h", "150", "FIELDS", "1", "gone"]).await,
      b"*1\r\n:1\r\n"
    );
    assert_eq!(
      exec(&mut c, &["ZADD", "z", "1", "stay", "2", "die"]).await,
      b":2\r\n"
    );
    assert_eq!(
      exec(&mut c, &["ZPEXPIRE", "z", "150", "MEMBERS", "1", "die"]).await,
      b"*1\r\n:1\r\n"
    );

    // 周期任务未启用（默认 freq = 0）且未拉起
    assert!(
      !provider.primary_tasks().object_collect_running(),
      "前置：默认禁用，任务不应拉起"
    );

    // 等待字段过期后经 CONFIG SET 启用周期收集（频率 1s，C#
    // ReconcilePrimaryTask(ObjectCollectTask) 运行时链路）；runtime 内定时
    // 等待（await 驱动 timer 期间 spawn 的任务得以被 poll）
    sleep(Duration::from_millis(250)).await;
    assert_eq!(
      exec(
        &mut c,
        &["CONFIG", "SET", "expired-object-collection-freq", "1"]
      )
      .await,
      b"+OK\r\n"
    );
    assert!(
      provider.primary_tasks().object_collect_running(),
      "CONFIG SET freq>0 必须拉起周期收集任务"
    );

    // 不调 HCOLLECT/ZCOLLECT，轮询等周期任务自动回收（首轮间隔 1s）
    for _ in 0..32 {
      sleep(Duration::from_millis(50)).await;
      if exec(&mut c, &["HLEN", "h"]).await == b":1\r\n" {
        break;
      }
    }
    assert_eq!(
      exec(&mut c, &["HLEN", "h"]).await,
      b":1\r\n",
      "过期字段应被周期收集"
    );
    assert_eq!(exec(&mut c, &["HGET", "h", "keep"]).await, b"$1\r\nv\r\n");
    assert_eq!(
      exec(&mut c, &["ZCARD", "z"]).await,
      b":1\r\n",
      "过期成员应被周期收集"
    );
    assert_eq!(exec(&mut c, &["ZSCORE", "z", "stay"]).await, b"$1\r\n1\r\n");

    // CONFIG SET 禁用 → 任务自退出（频率槽位每轮重读，对标 CancelAsync）
    assert_eq!(
      exec(
        &mut c,
        &["CONFIG", "SET", "expired-object-collection-freq", "0"]
      )
      .await,
      b"+OK\r\n"
    );
    let tasks = provider.primary_tasks();
    for _ in 0..30 {
      if !tasks.object_collect_running() {
        break;
      }
      sleep(Duration::from_millis(100)).await;
    }
    assert!(
      !tasks.object_collect_running(),
      "禁用后循环必须自退出（对标 C# CancelAsync）"
    );
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}

#[test]
fn disable_enable_quick_flip_does_not_lose_task() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let provider = Arc::new(open_provider(dir.path()));
    let mut c = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("首会话装配");
    let tasks = provider.primary_tasks();

    assert_eq!(exec(&mut c, &["HSET", "h", "gone", "v"]).await, b":1\r\n");
    assert_eq!(
      exec(&mut c, &["HPEXPIRE", "h", "150", "FIELDS", "1", "gone"]).await,
      b"*1\r\n:1\r\n"
    );

    // 启用（首轮立即收集后进入 1s 轮次睡眠）
    assert_eq!(
      exec(
        &mut c,
        &["CONFIG", "SET", "expired-object-collection-freq", "1"]
      )
      .await,
      b"+OK\r\n"
    );
    assert!(tasks.object_collect_running(), "前置：任务在跑");

    // 等旧任务进入轮次睡眠后禁用（旧任务最迟 1s 后在退出决定点自退出），
    // 不等其翻转 started 标志立即重新启用——disable→enable 快速连续操作
    // 正是 started 竞态的命中序列（swap 失败 + 旧任务随后落 false = 任务
    // 永久丢失）
    sleep(Duration::from_millis(1200)).await;
    assert_eq!(
      exec(
        &mut c,
        &["CONFIG", "SET", "expired-object-collection-freq", "0"]
      )
      .await,
      b"+OK\r\n"
    );
    assert_eq!(
      exec(
        &mut c,
        &["CONFIG", "SET", "expired-object-collection-freq", "1"]
      )
      .await,
      b"+OK\r\n"
    );

    // 任务必须恢复运行并真实推进收集（若竞态丢任务，标志与收集双双停滞）
    let mut recovered = false;
    for _ in 0..50 {
      if tasks.object_collect_running() {
        recovered = true;
        break;
      }
      sleep(Duration::from_millis(100)).await;
    }
    assert!(
      recovered,
      "disable→enable 快速翻转后任务必须恢复运行（started 翻转竞态回归）"
    );
    for _ in 0..32 {
      sleep(Duration::from_millis(50)).await;
      if exec(&mut c, &["HLEN", "h"]).await == b":0\r\n" {
        break;
      }
    }
    assert!(tasks.object_collect_running(), "收集周期内任务必须持续在跑");
    assert_eq!(
      exec(&mut c, &["HLEN", "h"]).await,
      b":0\r\n",
      "翻转后周期收集必须真实推进（过期字段被回收）"
    );
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}

#[test]
fn disabled_freq_never_starts_task() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let provider = Arc::new(open_provider(dir.path()));
    let mut c = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("首会话装配");

    assert_eq!(exec(&mut c, &["HSET", "h", "gone", "v"]).await, b":1\r\n");
    assert_eq!(
      exec(&mut c, &["HPEXPIRE", "h", "100", "FIELDS", "1", "gone"]).await,
      b"*1\r\n:1\r\n"
    );

    // 频率禁用（默认 0）下跨过多个假设轮次：任务不拉起；计数精度不依赖
    // 周期任务——HLEN 计数前先走堆序惰性剔除（collection.md §6.3），唯一
    // 到期成员出账即计数归零并删空自愈
    sleep(Duration::from_millis(1200)).await;
    assert!(
      !provider.primary_tasks().object_collect_running(),
      "禁用配置下任务不得拉起"
    );
    assert_eq!(
      exec(&mut c, &["HLEN", "h"]).await,
      b":0\r\n",
      "无周期任务时计数仍精确（惰性剔除兜底）"
    );
    assert_eq!(
      exec(&mut c, &["EXISTS", "h"]).await,
      b":0\r\n",
      "全字段到期 HLEN 触发删空自愈"
    );
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}

#[test]
fn replica_gate_suspends_and_resume_restarts() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let provider = Arc::new(open_provider(dir.path()));
    let store = provider.store();

    let mut c = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("首会话装配");
    // 前置：GC 扫描经唯一启停真值源拉起（expired-key-deletion-scan-freq 槽位，
    // 对标 C# StoreWrapper.cs:994-999 TryStartExpiredKeyDeletionTask 现取
    // runtimeConfig.GetInt 同槽；GcConfig.enabled 只是该槽位经
    // apply_config_reconcile 写入的引擎镜像，装配不越权默认开后台任务）
    assert_eq!(
      exec(
        &mut c,
        &["CONFIG", "SET", "expired-key-deletion-scan-freq", "1"]
      )
      .await,
      b"+OK\r\n"
    );
    assert!(store.gc_running(), "前置：GC 扫描在跑");

    assert_eq!(
      exec(
        &mut c,
        &["CONFIG", "SET", "expired-object-collection-freq", "1"]
      )
      .await,
      b"+OK\r\n"
    );
    let tasks: Arc<PrimaryTasks> = provider.primary_tasks();
    assert!(tasks.object_collect_running(), "前置：周期收集任务在跑");

    // 降副本挂起（ClusterProvider.suspend_primary_tasks 的任务域侧语义）：
    // 周期收集任务停 + GC 扫描停
    tasks.suspend();
    store.stop_gc();
    assert!(tasks.is_replica());
    assert!(!store.gc_running(), "降副本后 Primary 类 GC 扫描必须停止");

    // 挂起期间过期字段不被主动回收（任务轮空）
    assert_eq!(exec(&mut c, &["HSET", "h2", "gone", "v"]).await, b":1\r\n");
    assert_eq!(
      exec(&mut c, &["HPEXPIRE", "h2", "100", "FIELDS", "1", "gone"]).await,
      b"*1\r\n:1\r\n"
    );
    // 等待 100ms 到期（余量 50ms）
    sleep(Duration::from_millis(150)).await;
    // 挂起期计数精度不受任务挂起影响：HLEN 计数前惰性剔除（collection.md
    // §6.3）由命令驱动，与周期任务无关；唯一成员到期即计数归零并删空自愈
    assert_eq!(
      exec(&mut c, &["HLEN", "h2"]).await,
      b":0\r\n",
      "副本挂起期 HLEN 惰性剔除照常精确（唯一到期成员出账即删空）"
    );
    assert_eq!(exec(&mut c, &["EXISTS", "h2"]).await, b":0\r\n");

    // 升主恢复（resume_primary_tasks 的任务域侧语义）：GC 重启 + 任务复跑
    tasks.resume(&store);
    store.start_gc();
    assert!(store.gc_running(), "升主后 GC 扫描必须恢复");
    assert!(tasks.object_collect_running(), "升主后周期收集任务必须恢复");
    // 复跑可观测面：恢复后新挂一枚到期字段，等一个收集周期由任务物理出账
    assert_eq!(exec(&mut c, &["HSET", "h3", "gone", "v"]).await, b":1\r\n");
    assert_eq!(
      exec(&mut c, &["HPEXPIRE", "h3", "100", "FIELDS", "1", "gone"]).await,
      b"*1\r\n:1\r\n"
    );
    for _ in 0..32 {
      sleep(Duration::from_millis(50)).await;
      if exec(&mut c, &["HLEN", "h3"]).await == b":0\r\n" {
        break;
      }
    }
    assert_eq!(
      exec(&mut c, &["HLEN", "h3"]).await,
      b":0\r\n",
      "升主后周期收集应回收挂起恢复后过期的字段"
    );
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}
