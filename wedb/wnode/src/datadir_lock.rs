//! 数据目录排他锁（flock）——防多实例并发双写的唯一防线
//!
//! [`crate::net::socket_opt::bind_reuseport`] 的 SO_REUSEPORT 架构偏差
//! （doc/zh/deviations.md 第 11 条）使网络层无法像 C#
//! GarnetServerTcp.cs:111-114 显式关闭端口复用那样在 bind 面拦下第二
//! 实例；同机同 UID 误启第二实例指向同一 `--dir` 时，两进程的
//! SegmentedDevice 将以 O_RDWR 互踩段文件族（`wedb.db.N` / `wal.log.N`），
//! 写入静默交错损坏存储。本锁在 [`crate::server::ServerBootstrap::run_async`]
//! 目录就绪后、设备打开前，对数据目录与 WAL 目录各建锁文件取进程级
//! 排他锁，第二实例获取即拒启并点名数据目录与锁文件路径。
//!
//! 生命周期：锁随持锁 fd 存活，守卫随 bootstrap 存活，drop 即释放；
//! 进程被 kill -9 时内核回收 fd 自动释放，锁文件无需清理（重启复用既有
//! 文件，互斥判据是内核 flock 而非文件存在性）。嵌入式直构
//! （`open_node_with_config` 绕过 bootstrap）不持锁，属嵌入宿主自管域。
//!
//! 已知限制：flock 互斥仅在本地文件系统与 Linux NFS+NLM/lockd（>=2.6.37）
//! 在位形态成立，nolocks/SMB 及 darwin 网络卷静默退化为本地锁、跨客户端
//! 不设防。

use std::{
  fs::{File, OpenOptions},
  io::{self, Seek, SeekFrom, Write},
  path::{Path, PathBuf},
  process::id,
};

use itoa::Buffer;

use crate::Error;

/// 锁文件基名（`{dir}/wedb.lock`，处于段文件族 `wedb.db.N` / `wal.log.N`
/// 的命名域之外，不与设备段文件冲突）
const LOCK_FILE_NAME: &str = "wedb.lock";

#[cfg(unix)]
use nix::fcntl::Flock;

/// 持锁句柄：unix 为 RAII Flock（drop 显式 UNLOCK），非 unix 平台无
/// flock 能力、仅持 fd 留痕（见 [`lock_exclusive`] 的告警分派）
#[cfg(unix)]
type HeldLock = Flock<File>;
#[cfg(not(unix))]
type HeldLock = File;

/// 数据目录排他锁守卫
///
/// 持有期内各锁文件的排他锁不释放，drop（随 bootstrap 结束或进程退出）
/// 即全量放行。锁互斥依平台语义（Linux flock 锁归属打开文件描述而非进程，
/// 本进程异 fd 二次获取同样被拦；darwin flock 经 fcntl 承接归属进程，
/// 同进程异 fd 不互斥，依赖 [`same_path`] 守卫去重）。
#[derive(Debug)]
pub struct DataDirLock {
  held: Vec<(PathBuf, HeldLock)>,
}

impl DataDirLock {
  /// 对数据目录、WAL 目录与外置检查点基目录取排他锁
  ///
  /// - 数据目录锁必然获取（空目录形态由调用侧跳过，无目录即无互踩面）；
  /// - WAL 目录与外置检查点基目录经 `same_path` 统一去重跳过（Linux 下同一进程
  ///   对同一锁文件二次 flock 因 open file description 互斥会被自身首锁阻塞，产生
  ///   EWOULDBLOCK 误拒启；darwin 经 fcntl 承接虽不阻塞亦藉此对齐行为）；
  ///   默认 `<dir>/wal` 位于数据目录内时锁文件各别无冲突，照常加锁——防
  ///   「不同 `--dir` 显式共享同一 `--wal-dir`」的交叉双写；
  /// - 外置检查点基目录（`--checkpoint-dir` 不落数据目录内）同形复用
  ///   [`lock_exclusive`] 取 `wedb.lock` 排他锁——防「不同 `--dir`
  ///   共享同一外置 `--checkpoint-dir`」时并发双写检查点文件族与集群复制历史。
  ///
  /// 失败即拒启：锁被活实例持有（EWOULDBLOCK）报 [`Error::DataDirLocked`]
  /// 点名目录与锁文件路径；其余 IO 错误经 [`Error::Io`] 原样上抛。
  pub fn acquire(
    dir: &Path,
    wal_dir: &Path,
    checkpoint_base_dir: Option<&Path>,
  ) -> crate::Result<Self> {
    let mut held = Vec::with_capacity(3);
    let mut locked_dirs: Vec<&Path> = Vec::with_capacity(3);

    let candidates = [
      Some(dir),
      (!wal_dir.as_os_str().is_empty()).then_some(wal_dir),
      checkpoint_base_dir.filter(|p| !p.as_os_str().is_empty()),
    ];

    for path in candidates.into_iter().flatten() {
      if !locked_dirs.iter().any(|&prev| same_path(prev, path)) {
        held.push(lock_exclusive(path)?);
        locked_dirs.push(path);
      }
    }
    Ok(Self { held })
  }

  /// 已持有的锁文件路径（数据目录在前，独立 WAL 目录与外置检查点基目录在后；诊断面）
  pub fn lock_files(&self) -> impl Iterator<Item = &Path> + '_ {
    self.held.iter().map(|(p, _)| p.as_path())
  }
}

/// 对目录建锁文件并取非阻塞排他锁
///
/// 锁文件内容为持有者 PID（仅运维诊断留痕，互斥判据是内核 flock 而非
/// 文件内容；截断旧 PID 的时机严格在自身 flock 成功之后，拒启实例不
/// 抹除活实例的留痕）
fn lock_exclusive(dir: &Path) -> crate::Result<(PathBuf, HeldLock)> {
  let lock_file = dir.join(LOCK_FILE_NAME);
  let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        // 不截断：旧持有者 PID 只能由 flock 成功后的自身覆写，拒启实例不抹痕
        .truncate(false)
        .open(&lock_file)?;
  #[cfg(unix)]
  let mut held = {
    use nix::{
      errno::Errno,
      fcntl::{Flock, FlockArg},
    };
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
      Ok(lock) => lock,
      // EWOULDBLOCK 与 EAGAIN 同值：锁被活实例持有
      Err((_, Errno::EWOULDBLOCK)) => {
        return Err(Error::DataDirLocked {
          dir: dir.display().to_string(),
          lock_file: lock_file.display().to_string(),
        });
      }
      Err((_, e)) => return Err(Error::Io(io::Error::from(e))),
    }
  };
  // 非 unix 平台无 flock：按平台能力单点分派为仅告警不锁（禁静默伪装
  // 已锁），多实例同目录并发双写风险显式留痕
  #[cfg(not(unix))]
  let mut held = {
    log::warn!(
      "当前平台无数据目录排他锁能力，多实例同目录并发双写不设防（锁文件仅留痕）: {}",
      lock_file.display()
    );
    file
  };
  // PID 留痕尽力而为：写失败（如磁盘满）不影响锁正确性，不据此拒启
  let mut pid = Buffer::new();
  let _ = held
    .set_len(0)
    .and_then(|_| held.seek(SeekFrom::Start(0)))
    .and_then(|_| held.write_all(pid.format(id()).as_bytes()));
  Ok((lock_file, held))
}

/// 两路径是否指向同一目录（canonicalize 解析符号链接；任一失败回退
/// 字面比较，误判 false 至多多取一把锁，无互斥漏面）
fn same_path(a: &Path, b: &Path) -> bool {
  match (a.canonicalize(), b.canonicalize()) {
    (Ok(a), Ok(b)) => a == b,
    _ => a == b,
  }
}
