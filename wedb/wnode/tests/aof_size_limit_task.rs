//! AOF 体积超限自动检查点 + 截断任务端到端回归
//!
//! 对标 C# libs/server/StoreWrapper.cs:AutoCheckpointBasedOnAofSizeLimitAsync
//!（libs/server/TaskManager/TaskType.cs:AofSizeLimitTask 执行体；配置面
//! Options.cs:256/260 aof-size-limit + aof-size-limit-enforce-frequency，
//! 默认关闭）与其驱动臂
//! libs/server/Databases/SingleDatabaseManager.cs:TaskCheckpointBasedOnAofSizeLimitAsync
//!（尺寸预判 → 取暂停闸门 → 副本角色门 → 打检查点 → 还闸）语义：
//! 1. 写入使 AOF 超过注入的小阈值；
//! 2. 等待周期任务 → 检查点产生（wcpr checkpoint）+ AOF 物理截断（检查点
//!    内核 take_database_checkpoint_async 的 TruncateUntil 环节，total_size 回落）；
//! 3. 重启恢复（检查点 + AOF 增量重放）后数据一致；
//! 4. 臂门控次序回归：副本角色轮空、暂停闸门占用轮空、未超限不取闸不打点。

use std::{
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering::Relaxed},
  },
  time::Duration,
};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use tempfile::tempdir;
use wconf::ServerConfigType;
use wcpr::list_checkpoints;
use wnode::service::{StorageSessionProvider, spawn_aof_size_limit_task};
use wnode_test::{SessionFactory, session_factory};
use wtest_base::test_store_config;

/// 注入的小阈值字节（4 KB：少量写入即超限，避免放大测试时长）
const LIMIT_BYTES: u64 = 4 * 1024;
/// 写入条数与值长（128 条 × 256B ≈ 32KB + 键开销，稳超阈值）
const WRITE_COUNT: usize = 128;
const VALUE_LEN: usize = 256;
/// 周期任务检查间隔秒（注入最小周期，加快触发）
const FREQUENCY_SECS: u64 = 1;
/// 触发轮询超时（秒）
const WAIT_SECS: u64 = 15;
/// 轮空观察窗口秒数（≥3 个检查周期，确认任务多轮驱动均不触发）
const SKIP_WATCH_SECS: u64 = 3;
/// 「不超限不取闸」轮询节拍毫秒
const GATE_POLL_MS: u64 = 10;

type Provider = StorageSessionProvider<SessionFactory>;

fn key(i: usize) -> String {
  format!("aof-limit:key:{i}")
}

fn value(i: usize) -> Vec<u8> {
  vec![b'v' + (i % 26) as u8; VALUE_LEN]
}

/// 装配 AOF 点亮的单机 provider（fn 指针协变统一 F，避免 fn item 类型分歧）
fn open_aof_provider(data_path: &Path) -> Arc<Provider> {
  let provider: Arc<Provider> = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      data_path,
      None,
      None,
      session_factory as SessionFactory,
    )
    .expect("open with aof"),
  );
  provider
}

/// 写入 count 条键值（写监听端口自动镜像 AOF）
async fn write_keys(provider: &Provider, count: usize) {
  let session = provider.store().new_session().expect("new session");
  for i in 0..count {
    session
      .upsert(key(i).as_bytes(), &value(i))
      .await
      .expect("upsert");
  }
}

/// 轮询等待周期任务完成一轮打点（检查点产生 + AOF 截断回落）
async fn await_checkpoint_fire(provider: &Provider, peak: i64) -> bool {
  let aof = provider.aof().expect("aof enabled");
  for _ in 0..WAIT_SECS * 10 {
    sleep(Duration::from_millis(100)).await;
    if !list_checkpoints(&provider.checkpoint_dir)
      .expect("list checkpoints")
      .is_empty()
      && aof.total_size() < peak
    {
      return true;
    }
  }
  false
}

/// 检查点目录须为空（轮空断言与公共前置）
fn assert_no_checkpoint(provider: &Provider) {
  assert!(
    list_checkpoints(&provider.checkpoint_dir)
      .expect("list checkpoints")
      .is_empty(),
    "不应存在检查点"
  );
}

/// AOF 超限 → 周期任务触发检查点 + 截断 → 重启恢复数据一致
#[test]
fn aof_size_limit_task_checkpoints_truncates_and_recovers() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("aof_limit.db");
  let config = test_store_config();

  let provider: Arc<Provider> = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      config.clone(),
      &data_path,
      None,
      None,
      session_factory as SessionFactory,
    )
    .expect("open with aof"),
  );

  rt.block_on(async {
    // 拉起周期执行体（生产路径经 with_aof_size_limit + 首会话惰性装配，
    // 此处直接驱动同一 spawn 入口验证执行体语义）
    provider
      .runtime_config
      .try_set(
        ServerConfigType::AofSizeLimitEnforceFrequency,
        &FREQUENCY_SECS.to_string(),
      )
      .unwrap();
    spawn_aof_size_limit_task(
      Arc::clone(&provider.database_manager),
      LIMIT_BYTES,
      Some(Arc::clone(&provider.runtime_config)),
    );

    // 触发前目录无检查点
    assert_no_checkpoint(&provider);

    // 写入使 AOF 超限
    write_keys(&provider, WRITE_COUNT).await;
    let aof = provider.aof().expect("aof enabled");
    let peak = aof.total_size();
    assert!(
      peak as u64 > LIMIT_BYTES,
      "写入后 AOF 体积({peak})须超过注入阈值({LIMIT_BYTES})"
    );

    // 轮询等待周期任务：检查点产生 + AOF 截断回落
    assert!(
      await_checkpoint_fire(&provider, peak).await,
      "周期任务须在 {WAIT_SECS}s 内产生检查点并截断 AOF"
    );
    let truncated = aof.total_size();
    assert!(
      (truncated as u64) < LIMIT_BYTES,
      "检查点后 AOF 须物理截断（当前 {truncated}，峰值 {peak}，阈值 {LIMIT_BYTES}）"
    );
    // 后置断言：LastSave 由内核 update_last_save 单点回填；轮次结束
    // 暂停闸门已归还（还闸无条件执行）
    assert!(
      provider.database_manager.last_save_ms() > 0,
      "超限打点后须回填 LastSave"
    );
    assert!(
      !provider.database_manager.db.checkpoint_paused.load(Relaxed),
      "轮次结束暂停闸门须已归还"
    );

    // 截断后的数据仍可读（AOF 丢弃段已持久化进检查点，内存视图不受影响）
    let session = provider.store().new_session().expect("new session");
    for i in 0..WRITE_COUNT {
      assert_eq!(
        session.read(key(i).as_bytes()).await.expect("read"),
        Some(value(i)),
        "截断后键 {} 仍须可读",
        key(i)
      );
    }
  });

  // 宿主释放 → 周期任务随弱引用收敛退出
  drop(provider);

  // 重启恢复：检查点 + AOF 增量重放，数据一致
  rt.block_on(async {
    let recovered = StorageSessionProvider::open_recovered_with_config_and_aof(
      config,
      &data_path,
      None,
      None,
      session_factory,
    )
    .await
    .expect("recover");
    let session = recovered.store().new_session().expect("new session");
    for i in 0..WRITE_COUNT {
      assert_eq!(
        session.read(key(i).as_bytes()).await.expect("read"),
        Some(value(i)),
        "重启恢复后键 {} 数据不一致",
        key(i)
      );
    }
  });
}

/// 竞争写入轮数（4 倍键集 × 分批节奏 = 多轮检查点窗口内持续超阈值写入）
const RACE_WRITE_COUNT: usize = WRITE_COUNT * 4;
/// 每批写入条数（写满一批让渡执行权，拉长竞争窗口至秒级）
const RACE_BATCH: usize = 20;
/// 批间节拍毫秒（全程约 2.5s，覆盖 size-limit 任务 ≥ 2 轮）
const RACE_BATCH_PAUSE_MS: u64 = 100;

/// size-limit 任务与手动 SAVE 并发竞争不丢段（P0 回归：检查点互斥）
///
/// 两路检查点（size-limit 周期任务臂 vs SAVE 的
/// try_acquire_checkpoint_guard + take_checkpoint）必须经同一
/// checkpoint_paused CAS 锁位互斥：若无互斥，旧检查点路径后执行的
/// AOF 截断会截掉新检查点边界的前缀，恢复重放出现缺口。
/// 竞争全程检查点令牌单调，结束后内存视图与重启恢复全量一致。
#[test]
fn aof_size_limit_task_concurrent_with_save_recovers_consistently() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("aof_limit_race.db");
  let config = test_store_config();

  let provider: Arc<Provider> = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      config.clone(),
      &data_path,
      None,
      None,
      session_factory as SessionFactory,
    )
    .expect("open with aof"),
  );

  rt.block_on(async {
    provider
      .runtime_config
      .try_set(
        ServerConfigType::AofSizeLimitEnforceFrequency,
        &FREQUENCY_SECS.to_string(),
      )
      .unwrap();
    spawn_aof_size_limit_task(
      Arc::clone(&provider.database_manager),
      LIMIT_BYTES,
      Some(Arc::clone(&provider.runtime_config)),
    );

    let stop = Arc::new(AtomicBool::new(false));

    // 写入任务：分批推进 AOF 反复越过阈值，制造持续的检查点触发窗口
    let writer_stop = Arc::clone(&stop);
    let writer_store = provider.store().clone();
    let writer = spawn(async move {
      let session = writer_store.new_session().expect("new session");
      for i in 0..RACE_WRITE_COUNT {
        if writer_stop.load(Relaxed) {
          break;
        }
        session
          .upsert(key(i).as_bytes(), &value(i))
          .await
          .expect("upsert");
        if i % RACE_BATCH == RACE_BATCH - 1 {
          sleep(Duration::from_millis(RACE_BATCH_PAUSE_MS)).await;
        }
      }
    });

    // 手动 SAVE 任务：与 SAVE 命令同一互斥路径（guard + take_checkpoint）
    // 高频触发，与 size-limit 周期任务正面竞争
    let saver_stop = Arc::clone(&stop);
    let saver_dm = Arc::clone(&provider.database_manager);
    let saver = spawn(async move {
      let mut saves = 0usize;
      while !saver_stop.load(Relaxed) {
        if let Some(guard) = saver_dm.try_acquire_checkpoint_guard() {
          let taken = saver_dm.take_checkpoint(false).await;
          drop(guard);
          taken.expect("手动 SAVE 不得失败");
          saves += 1;
        }
        sleep(Duration::from_millis(5)).await;
      }
      saves
    });

    writer.await.expect("写入任务");
    stop.store(true, Relaxed);
    let saves = saver.await.expect("SAVE 任务");
    assert!(saves > 0, "竞争窗口内手动 SAVE 至少完成一次");

    // 检查点令牌单调（两路共用递增 token 通道）
    let tokens = list_checkpoints(&provider.checkpoint_dir).expect("list checkpoints");
    assert!(
      tokens.len() > 1,
      "竞争窗口须产生多轮检查点，实际 {} 轮",
      tokens.len()
    );
    assert!(
      tokens.windows(2).all(|w| w[0] < w[1]),
      "检查点令牌须严格递增: {tokens:?}"
    );

    // 竞争结束后内存视图全量可读
    let session = provider.store().new_session().expect("new session");
    for i in 0..RACE_WRITE_COUNT {
      assert_eq!(
        session.read(key(i).as_bytes()).await.expect("read"),
        Some(value(i)),
        "竞争后键 {} 须可读",
        key(i)
      );
    }
  });

  // 宿主释放 → 周期任务随弱引用收敛退出
  drop(provider);

  // 重启恢复：检查点 + AOF 增量重放。若两路检查点竞态截断丢段，
  // 重放缺口在此暴露为键缺失
  rt.block_on(async {
    let recovered = StorageSessionProvider::open_recovered_with_config_and_aof(
      config,
      &data_path,
      None,
      None,
      session_factory,
    )
    .await
    .expect("recover");
    let session = recovered.store().new_session().expect("new session");
    for i in 0..RACE_WRITE_COUNT {
      assert_eq!(
        session.read(key(i).as_bytes()).await.expect("read"),
        Some(value(i)),
        "重启恢复后键 {} 数据不一致",
        key(i)
      );
    }
  });
}

/// 副本角色超限轮空，升主后同一常驻任务自动打点
///
/// 对标 C# 臂内闸门段 IsReplica 早退（C# AofSizeLimitTask 属 Primary 放置
/// 类别、副本不注册；rust 沿用本仓常驻 + 臂内轮空形态，经
/// PrimaryTasks.suspend/set_replica 角色位承接）：副本检查点会截断本地
/// AOF 破坏主从推流衔接，超限也绝不执行；升主即恢复打点。
#[test]
fn aof_size_limit_task_skips_replica_and_fires_after_promotion() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("aof_limit_replica.db");

  let provider = open_aof_provider(&data_path);

  rt.block_on(async {
    // 副本角色位挂起（任务未注入角色域的裸构造形态恒视为主，此处经
    // provider 共享同一 PrimaryTasks 域）
    provider.primary_tasks().suspend();
    provider
      .runtime_config
      .try_set(
        ServerConfigType::AofSizeLimitEnforceFrequency,
        &FREQUENCY_SECS.to_string(),
      )
      .unwrap();
    spawn_aof_size_limit_task(
      Arc::clone(&provider.database_manager),
      LIMIT_BYTES,
      Some(Arc::clone(&provider.runtime_config)),
    );

    assert_no_checkpoint(&provider);
    write_keys(&provider, WRITE_COUNT).await;
    let aof = provider.aof().expect("aof enabled");
    assert!(
      aof.total_size() as u64 > LIMIT_BYTES,
      "写入后 AOF 体积须超过注入阈值"
    );

    // 副本轮空 ≥ SKIP_WATCH_SECS 轮：无检查点、AOF 未截断、LastSave 未回填
    sleep(Duration::from_secs(SKIP_WATCH_SECS)).await;
    assert_no_checkpoint(&provider);
    assert_eq!(
      provider.database_manager.last_save_ms(),
      0,
      "副本角色下不得回填 LastSave"
    );

    // 升主：同一常驻任务下一轮自动打点
    provider.primary_tasks().set_replica(false);
    let peak = aof.total_size();
    assert!(
      await_checkpoint_fire(&provider, peak).await,
      "升主后周期任务须在 {WAIT_SECS}s 内打点并截断"
    );
    assert!(
      provider.database_manager.last_save_ms() > 0,
      "升主打点后须回填 LastSave"
    );
  });

  drop(provider);
}

/// 暂停闸门占用中超限轮空，还闸后下一轮即打点
///
/// 对标 C# TryPauseCheckpoints 取闸失败早退；rust 差异：占用中本轮轮空
/// （C# 忙等续等），重投由下一检查周期承接，最坏延迟一个周期。
#[test]
fn aof_size_limit_task_skips_while_checkpoints_paused() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("aof_limit_paused.db");

  let provider = open_aof_provider(&data_path);

  rt.block_on(async {
    // 测试侧占闸（模拟 SAVE/BGSAVE 或他轮检查点持有暂停闸门）
    assert!(
      provider.database_manager.try_pause_checkpoints(),
      "测试侧占闸须成功"
    );
    provider
      .runtime_config
      .try_set(
        ServerConfigType::AofSizeLimitEnforceFrequency,
        &FREQUENCY_SECS.to_string(),
      )
      .unwrap();
    spawn_aof_size_limit_task(
      Arc::clone(&provider.database_manager),
      LIMIT_BYTES,
      Some(Arc::clone(&provider.runtime_config)),
    );

    assert_no_checkpoint(&provider);
    write_keys(&provider, WRITE_COUNT).await;
    let aof = provider.aof().expect("aof enabled");
    assert!(
      aof.total_size() as u64 > LIMIT_BYTES,
      "写入后 AOF 体积须超过注入阈值"
    );

    sleep(Duration::from_secs(SKIP_WATCH_SECS)).await;
    assert_no_checkpoint(&provider);
    assert_eq!(
      provider.database_manager.last_save_ms(),
      0,
      "闸门占用中不得打点回填"
    );

    // 还闸：下一轮即打点
    provider.database_manager.resume_checkpoints();
    let peak = aof.total_size();
    assert!(
      await_checkpoint_fire(&provider, peak).await,
      "还闸后周期任务须在 {WAIT_SECS}s 内打点并截断"
    );
  });

  drop(provider);
}

/// 未超限不打点、不回填，且暂停闸门完全未被触碰
///
/// 锁死臂次序「尺寸预判在取闸门之前」（C# 同构）：旧拓扑每轮先抢
/// 暂停闸门再判尺寸，本用例高频轮询 checkpoint_paused 会翻车。
#[test]
fn aof_size_limit_task_ignores_aof_below_limit() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("aof_limit_below.db");

  let provider = open_aof_provider(&data_path);

  rt.block_on(async {
    // 限额 1 TiB：写入量远不可达
    provider
      .runtime_config
      .try_set(
        ServerConfigType::AofSizeLimitEnforceFrequency,
        &FREQUENCY_SECS.to_string(),
      )
      .unwrap();
    spawn_aof_size_limit_task(
      Arc::clone(&provider.database_manager),
      1 << 40,
      Some(Arc::clone(&provider.runtime_config)),
    );

    assert_no_checkpoint(&provider);
    write_keys(&provider, WRITE_COUNT).await;

    // 覆盖 ≥ SKIP_WATCH_SECS 个检查周期的高频轮询：闸门恒未置位
    let paused = &provider.database_manager.db.checkpoint_paused;
    for _ in 0..SKIP_WATCH_SECS * 1000 / GATE_POLL_MS {
      assert!(
        !paused.load(Relaxed),
        "未超限轮次不得触碰暂停闸门（尺寸预判须在取闸之前）"
      );
      sleep(Duration::from_millis(GATE_POLL_MS)).await;
    }
    assert_no_checkpoint(&provider);
    assert_eq!(
      provider.database_manager.last_save_ms(),
      0,
      "未超限不得回填 LastSave"
    );
  });

  drop(provider);
}
