//! 数据目录排他锁（flock）进程级互斥端到端
//!
//! 回归 task/ing/wnode-datadir-flock-claimed-but-unimplemented：deviations
//! 第 11 条与 `bind_reuseport` 注释宣称的「防多实例并发双写由存储目录排他锁
//! 承担」此前全仓零实现，同机同 UID 误启第二实例指向同一 --dir 即两进程
//! O_RDWR 互踩段文件族静默损坏存储。本测试验证锁实装后：
//!
//! 1. 双进程同 --dir 第二实例启动即失败，错误点名数据目录与锁文件；
//! 2. 不同 --dir 并发启动互不影响；
//! 3. 第一进程被 kill -9 后目录立即可被新实例获取（fd 内核回收）；
//! 4. 单机与集群两二进制同型各一断言（两者同经
//!    `ServerBootstrap::run_async` 的同一锁装配位，泛型 C 不触达锁路径，
//!    此处以集群泛型装配形态 `with_cluster_provider` 对位验证）；
//! 5. 不同 --dir 显式共享同一 --wal-dir 的交叉双写同被锁拦；
//! 6. 不同 --dir 显式共享同一外置 --checkpoint-dir 的交叉双写同被锁拦；
//! 7. 检查点基目录落数据目录内时行为不变与统一去重。
//!
//! 进程级形态：子进程 self-reinvoke（current_exe + `--exact` 单测试），
//! 就绪/结果经文件系统信号交换——assemble 回调在锁获取之后执行，其内
//! 落盘的 ready 文件即「锁已持有」证据；拒启实例把启动错误写入 res 文件。

use std::{
  env::{current_exe, var, var_os},
  fs::{create_dir_all, read_to_string, remove_file, write},
  path::{Path, PathBuf},
  process::{Child, Command, Stdio, exit, id},
  sync::Arc,
  time::Duration,
};

use wconf::NodeArgs;
use wnode::{
  ClusterProvider, DataDirLock, MessageConsumerFace, NoopClusterProvider, ServerBootstrap,
  SessionProviderFace, WireFormat,
};

/// self-reinvoke 目标测试名（--exact 精确匹配）
const TEST_NAME: &str = "datadir_flock_process_exclusive";
/// 子进程模式标记（存在即子进程形态）
const MODE_ENV: &str = "WEDB_FLOCK_CHILD_MODE";
const DIR_ENV: &str = "WEDB_FLOCK_CHILD_DIR";
const WAL_ENV: &str = "WEDB_FLOCK_CHILD_WAL";
const CHECKPOINT_ENV: &str = "WEDB_FLOCK_CHILD_CHECKPOINT";
const READY_ENV: &str = "WEDB_FLOCK_CHILD_READY";
const RES_ENV: &str = "WEDB_FLOCK_CHILD_RES";

/// 子进程就绪/结果轮询时限与节拍（慢机 CI 余量）
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(50);

/// 子进程哑会话域：不触存储与网络协议，仅驱动 bootstrap 走完锁装配位
struct NullConsumer;

impl MessageConsumerFace for NullConsumer {
  fn try_consume_messages_into(&mut self, _resp_buf: &mut Vec<u8>) -> Option<usize> {
    Some(0)
  }
  fn take_recv_scratch(&mut self) -> Vec<u8> {
    Vec::new()
  }
  fn return_recv_scratch(&mut self, _buf: Vec<u8>) {}
  fn dispose(&mut self) {}
}

struct NullProvider;

impl SessionProviderFace for NullProvider {
  type Consumer = NullConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<NullConsumer> {
    Some(NullConsumer)
  }
}

/// 集群泛型装配形态探针（trait 全默认实现，与真集群二进制共用
/// `ServerBootstrap::run_async` 的同一锁装配位）
#[derive(Clone, Copy)]
struct ProbeCluster;

impl ClusterProvider for ProbeCluster {}

/// 子进程入口：按 env 标记驱动 bootstrap，永不返回
fn child_entry() -> ! {
  let mode = var(MODE_ENV).unwrap();
  let dir = PathBuf::from(var(DIR_ENV).unwrap());
  let wal = var(WAL_ENV).ok().map(PathBuf::from);
  let checkpoint = var(CHECKPOINT_ENV).ok().map(PathBuf::from);
  let ready = PathBuf::from(var(READY_ENV).unwrap());
  let res = var(RES_ENV).unwrap();

  let node = NodeArgs {
    bind: Some("127.0.0.1".into()),
    port: 0,
    dir,
    wal_dir: wal,
    checkpoint_dir: checkpoint,
    threads: Some(1),
    quiet: true,
    ..Default::default()
  };
  // assemble 在排他锁获取之后执行：ready 文件落盘即「锁已持有」信号；
  // 第一实例成功形态随后阻塞于 wait_for_shutdown 持锁存活
  let outcome = if mode == "cluster" {
    ServerBootstrap::new(node)
      .with_cluster_provider(ProbeCluster)
      .banner("flock-probe-cluster")
      .run_async(|_a: NodeArgs, _c: ProbeCluster| async {
        let _ = write(&ready, id().to_string());
        Ok(Arc::new(NullProvider))
      })
  } else {
    ServerBootstrap::new(node)
      .banner("flock-probe-standalone")
      .run_async(|_a: NodeArgs, _c: NoopClusterProvider| async {
        let _ = write(&ready, id().to_string());
        Ok(Arc::new(NullProvider))
      })
  };
  let ok = outcome.is_ok();
  let _ = write(&res, outcome.map(|_| "OK").unwrap_err().to_string());
  exit(i32::from(!ok) * 2);
}

/// 拉起子进程实例（模式 standalone / cluster；tag 区分同 work 下信号文件）
fn spawn_child(
  mode: &str,
  dir: &Path,
  wal: Option<&Path>,
  checkpoint: Option<&Path>,
  work: &Path,
  tag: &str,
) -> Child {
  let ready = work.join(format!("{tag}.ready"));
  let res = work.join(format!("{tag}.res"));
  let _ = remove_file(&ready);
  let _ = remove_file(&res);
  let mut cmd = Command::new(current_exe().unwrap());
  cmd
    .env(MODE_ENV, mode)
    .env(DIR_ENV, dir)
    .env(READY_ENV, &ready)
    .env(RES_ENV, &res)
    .args(["--exact", TEST_NAME, "--test-threads=1", "--nocapture"])
    .stdout(Stdio::null())
    .stderr(Stdio::inherit());
  if let Some(w) = wal {
    cmd.env(WAL_ENV, w);
  }
  if let Some(cp) = checkpoint {
    cmd.env(CHECKPOINT_ENV, cp);
  }
  cmd.spawn().unwrap()
}

/// 等子进程到达锁持有点（ready 文件）；提前退出即启动失败，连错误内容上抛
fn wait_ready(work: &Path, tag: &str, child: &mut Child) {
  let ready = work.join(format!("{tag}.ready"));
  let res = work.join(format!("{tag}.res"));
  wtest_base::wait_assert_sync(
    || {
      if let Some(status) = child.try_wait().unwrap() {
        let err = read_to_string(&res).unwrap_or_default();
        panic!("子进程 {tag} 未就绪即退出 {status}: {err}");
      }
      ready.exists()
    },
    CHILD_TIMEOUT,
    POLL,
    format!("子进程 {tag} 就绪超时"),
  );
}

/// 等子进程写入启动结果（拒启错误文本）
fn wait_result(work: &Path, tag: &str) -> String {
  let res = work.join(format!("{tag}.res"));
  wtest_base::wait_assert_sync(
    || res.exists(),
    CHILD_TIMEOUT,
    POLL,
    format!("子进程 {tag} 结果超时"),
  );
  read_to_string(&res).unwrap()
}

/// 强杀并收割（SIGKILL，验证 kill -9 后 fd 内核回收路径时勿用 wait 替代）
fn kill_reap(child: &mut Child) {
  let _ = child.kill();
  let _ = child.wait();
}

#[test]
fn datadir_flock_process_exclusive() {
  if var_os(MODE_ENV).is_some() {
    child_entry();
  }

  // —— 段 0：锁 API 直验（同进程内 flock 归属打开文件描述，二次获取同拒）——
  {
    let work = tempfile::tempdir().unwrap();
    let dir = work.path().join("d");
    let wal = work.path().join("w");
    let ck = work.path().join("ck");
    create_dir_all(&dir).unwrap();
    create_dir_all(&wal).unwrap();
    create_dir_all(&ck).unwrap();

    let g1 = DataDirLock::acquire(&dir, &wal, None).unwrap();
    let files: Vec<_> = g1.lock_files().collect();
    assert_eq!(files.len(), 2, "主目录与独立 WAL 目录各一把锁");
    assert!(files[0].ends_with("wedb.lock"));
    assert!(files[1].ends_with("wedb.lock"));
    let err = DataDirLock::acquire(&dir, &wal, None)
      .unwrap_err()
      .to_string();
    assert!(err.contains("wedb.lock"), "错误须点名锁文件: {err}");
    drop(g1);
    DataDirLock::acquire(&dir, &wal, None).unwrap();

    // 外置检查点基目录三参加锁
    let g2 = DataDirLock::acquire(&dir, &wal, Some(&ck)).unwrap();
    let files: Vec<_> = g2.lock_files().collect();
    assert_eq!(
      files.len(),
      3,
      "主目录、独立 WAL 与外置检查点基目录各一把锁"
    );
    assert!(files[0].ends_with("wedb.lock"));
    assert!(files[1].ends_with("wedb.lock"));
    assert!(files[2].ends_with("wedb.lock"));
    assert_eq!(files[2], ck.join("wedb.lock"));
    let err = DataDirLock::acquire(&dir, &wal, Some(&ck))
      .unwrap_err()
      .to_string();
    assert!(err.contains("wedb.lock"));
    drop(g2);

    // 检查点基目录落数据目录内或与 WAL 目录同路径时统一去重
    let g_dup_dir = DataDirLock::acquire(&dir, &wal, Some(&dir)).unwrap();
    assert_eq!(
      g_dup_dir.lock_files().count(),
      2,
      "与数据目录同路径时去重跳过"
    );
    drop(g_dup_dir);

    let g_dup_wal = DataDirLock::acquire(&dir, &wal, Some(&wal)).unwrap();
    assert_eq!(
      g_dup_wal.lock_files().count(),
      2,
      "与 WAL 目录同路径时去重跳过"
    );
    drop(g_dup_wal);
  }

  // —— 段 1：双进程同 --dir 第二实例拒启，错误点名数据目录与锁文件 ——
  {
    let work = tempfile::tempdir().unwrap();
    let dir = work.path().join("d");
    let mut c1 = spawn_child("standalone", &dir, None, None, work.path(), "a1");
    wait_ready(work.path(), "a1", &mut c1);
    let mut c2 = spawn_child("standalone", &dir, None, None, work.path(), "a2");
    let err = wait_result(work.path(), "a2");
    assert!(
      err.contains(&dir.display().to_string()),
      "错误须点名数据目录: {err}"
    );
    assert!(err.contains("wedb.lock"), "错误须点名锁文件: {err}");
    let _ = c2.wait();
    kill_reap(&mut c1);
  }

  // —— 段 2：不同 --dir 并发启动互不影响 ——
  {
    let work = tempfile::tempdir().unwrap();
    let dir_a = work.path().join("a");
    let dir_b = work.path().join("b");
    let mut ca = spawn_child("standalone", &dir_a, None, None, work.path(), "b1");
    let mut cb = spawn_child("standalone", &dir_b, None, None, work.path(), "b2");
    wait_ready(work.path(), "b1", &mut ca);
    wait_ready(work.path(), "b2", &mut cb);
    kill_reap(&mut ca);
    kill_reap(&mut cb);
  }

  // —— 段 3：第一进程 kill -9 后目录立即可被新实例获取 ——
  {
    let work = tempfile::tempdir().unwrap();
    let dir = work.path().join("d");
    let mut c1 = spawn_child("standalone", &dir, None, None, work.path(), "c1");
    wait_ready(work.path(), "c1", &mut c1);
    c1.kill().unwrap();
    c1.wait().unwrap();
    let mut c2 = spawn_child("standalone", &dir, None, None, work.path(), "c2");
    wait_ready(work.path(), "c2", &mut c2);
    kill_reap(&mut c2);
  }

  // —— 段 4：单机与集群两二进制同型各一断言 ——
  {
    // 单机占 → 集群泛型形态同 dir 拒启
    let work = tempfile::tempdir().unwrap();
    let dir = work.path().join("d");
    let mut s = spawn_child("standalone", &dir, None, None, work.path(), "d1");
    wait_ready(work.path(), "d1", &mut s);
    let mut c = spawn_child("cluster", &dir, None, None, work.path(), "d2");
    let err = wait_result(work.path(), "d2");
    assert!(err.contains("wedb.lock"), "集群形态同 dir 须拒启: {err}");
    let _ = c.wait();
    kill_reap(&mut s);
    // 集群占 → 单机同 dir 拒启
    let work2 = tempfile::tempdir().unwrap();
    let dir2 = work2.path().join("d");
    let mut k = spawn_child("cluster", &dir2, None, None, work2.path(), "d3");
    wait_ready(work2.path(), "d3", &mut k);
    let mut t = spawn_child("standalone", &dir2, None, None, work2.path(), "d4");
    let err = wait_result(work2.path(), "d4");
    assert!(err.contains("wedb.lock"), "单机形态同 dir 须拒启: {err}");
    let _ = t.wait();
    kill_reap(&mut k);
  }

  // —— 段 5：不同 --dir 显式共享同一 --wal-dir 交叉双写同被拦 ——
  {
    let work = tempfile::tempdir().unwrap();
    let wal = work.path().join("wal");
    let dir_a = work.path().join("a");
    let dir_b = work.path().join("b");
    let mut ca = spawn_child("standalone", &dir_a, Some(&wal), None, work.path(), "e1");
    wait_ready(work.path(), "e1", &mut ca);
    let mut cb = spawn_child("standalone", &dir_b, Some(&wal), None, work.path(), "e2");
    let err = wait_result(work.path(), "e2");
    assert!(
      err.contains(&wal.display().to_string()),
      "错误须点名共享 WAL 目录: {err}"
    );
    let _ = cb.wait();
    kill_reap(&mut ca);
  }

  // —— 段 6：不同 --dir 显式共享同一外置 --checkpoint-dir 交叉双写同被拦 ——
  {
    let work = tempfile::tempdir().unwrap();
    let ck = work.path().join("ck");
    let dir_a = work.path().join("a");
    let dir_b = work.path().join("b");
    let mut ca = spawn_child("standalone", &dir_a, None, Some(&ck), work.path(), "f1");
    wait_ready(work.path(), "f1", &mut ca);
    let mut cb = spawn_child("standalone", &dir_b, None, Some(&ck), work.path(), "f2");
    let err = wait_result(work.path(), "f2");
    assert!(
      err.contains(&ck.display().to_string()),
      "错误须点名共享外置检查点目录: {err}"
    );
    assert!(err.contains("wedb.lock"), "错误须点名锁文件: {err}");
    let _ = cb.wait();
    kill_reap(&mut ca);
  }

  // —— 段 7：外置检查点目录落数据目录内时行为不变 ——
  {
    let work = tempfile::tempdir().unwrap();
    let dir = work.path().join("d");
    let ck = dir.join("checkpoints");
    let mut ca = spawn_child("standalone", &dir, None, Some(&ck), work.path(), "g1");
    wait_ready(work.path(), "g1", &mut ca);
    let mut cb = spawn_child("standalone", &dir, None, Some(&ck), work.path(), "g2");
    let err = wait_result(work.path(), "g2");
    assert!(
      err.contains(&dir.display().to_string()) || err.contains(&ck.display().to_string()),
      "错误须点名被占目录: {err}"
    );
    assert!(err.contains("wedb.lock"), "错误须点名锁文件: {err}");
    let _ = cb.wait();
    kill_reap(&mut ca);
  }
}
