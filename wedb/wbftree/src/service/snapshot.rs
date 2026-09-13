//! CPR 快照与恢复 (1:1 对标 Garnet BfTreeService 的 cpr_snapshot / recover 系列)

use std::{
  fs,
  panic::{self, AssertUnwindSafe},
  path::{Path, PathBuf},
  ptr::null_mut,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering},
  },
};

use bf_tree::BfTree;
use parking_lot::RwLock;

use super::{BfTreeService, MIN_MAX_RECORD_SIZE, config_error_to_string, file_has_cpr_magic};
use crate::{
  error::{Error, Result},
  types::StorageBackendType,
};

/// 构造统一格式的快照恢复错误（零临时字符串分配）
fn format_recovery_err(prefix: &str, path: &Path) -> Error {
  use core::fmt::Write;
  let mut msg = String::with_capacity(prefix.len() + path.as_os_str().len());
  let _ = write!(msg, "{prefix}{}", path.display());
  Error::Recovery(msg)
}

/// 快照文件缺失错误
#[inline]
fn snapshot_missing(path: &Path) -> Error {
  format_recovery_err("快照文件不存在: ", path)
}

impl BfTreeService {
  /// 触发 CPR 快照 (非阻塞并发 CPR 快照语义，零锁直调)
  ///
  /// 与 insert/delete 并发安全：引擎 CPR 采用阶段协议 (REST → PREPARE →
  /// IN_PROGRESS → SWEEP)，在途写者按当前快照版本把触碰的页自行拷入快照文件，
  /// 快照不阻塞写、写不阻塞快照 (较旧版「屏障排空再快照」的改动：屏障会让
  /// 大树 checkpoint 期间全部写入停摆且在 compio 核上自旋空转)。
  /// 同一棵树的并发快照互斥由 [`crate::RangeIndexManager`] 的 per-tree claim
  /// 承担——引擎对并发快照静默 no-op，宿主必须串行化。
  ///
  /// 底层 bf-tree 在未启用 use_snapshot 等异常场景下直接 panic 而非返回错误，
  /// 此处 catch_unwind 拦截转换为 Err (1:1 对标 Garnet 原生互操作层的处理方式)。
  /// 注意：release 构建全局 `panic = "abort"`，panic 路径实际以进程终止收场
  /// （检查点元数据未发布，重启一致性不受影响）；catch_unwind 仅在
  /// unwind 构建（dev/test）下生效，且 panic 点位于任何写入之前，无部分写入副作用。
  pub fn cpr_snapshot(&self, snapshot_path: impl AsRef<Path>) -> Result<()> {
    let tree = self.tree_arc()?;
    let p = snapshot_path.as_ref();
    if let Some(parent) = p.parent()
      && !parent.as_os_str().is_empty()
    {
      fs::create_dir_all(parent)?;
    }
    panic::catch_unwind(AssertUnwindSafe(|| tree.cpr_snapshot(p)))
      .map_err(|_| Error::Snapshot("底层引擎异常 (快照未启用或内部状态异常)".into()))
  }

  /// 通过原生句柄直接执行 CPR 快照 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:CprSnapshotByPtr)
  ///
  /// # Safety
  /// 调用方必须确保 `tree_ptr` 指向有效且未被释放的 `BfTree` 实例。
  pub unsafe fn cpr_snapshot_by_ptr(tree_ptr: u64, snapshot_path: impl AsRef<Path>) -> Result<()> {
    if tree_ptr == 0 {
      return Err(Error::InvalidArgument("原生树句柄为空".into()));
    }
    let tree = unsafe { &*(tree_ptr as usize as *const BfTree) };
    let p = snapshot_path.as_ref();
    if let Some(parent) = p.parent()
      && !parent.as_os_str().is_empty()
    {
      fs::create_dir_all(parent)?;
    }
    panic::catch_unwind(AssertUnwindSafe(|| tree.cpr_snapshot(p)))
      .map_err(|_| Error::Snapshot("底层引擎异常 (快照未启用或内部状态异常)".into()))
  }

  /// 从 CPR 快照原地换入恢复树 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:RestoreTree 的句柄重锚定)
  ///
  /// 恢复流程（保证任何时刻都不破坏仍存活的旧树，且任一步失败状态自洽）：
  /// 1. 快照拷贝至 `work_path.recovering` 临时文件（绝不覆盖旧树正在使用的 `work_path`）；
  /// 2. 从临时文件恢复出新树（其活动基文件即该临时 inode）；
  /// 3. 临时 inode 原子 rename 至 `work_path`——旧树 fd 指向原 inode 不受换名影响；
  ///    此后工作路径命名即新快照态，下次启动可凭魔数直接恢复，且 purge 回收
  ///    token 目录绝不伤及活动树；
  /// 4. 屏障内排空在途写者后写锁下换树——换树并发窗口内不存在「写入旧树成功
  ///    应答却被换树丢弃」的丢失写（屏障前已应答的写入随恢复回滚属既定语义）；
  ///    旧树摘除后放入 retired 延缓释放（旧树基文件为已换名的原 inode，经 fd 访问全程有效，
  ///    在途极短读绝无 UAF 风险）。
  ///
  /// 所有持有 `Arc<BfTreeService>` 的使用方（如 ACL 存储）无需重绑即透明使用恢复后的树。
  pub fn recover_in_place(&self, snapshot_path: &Path, work_path: &Path) -> Result<()> {
    if !snapshot_path.exists() {
      return Err(snapshot_missing(snapshot_path));
    }
    // 预建工作路径父目录：保证同目录 rename 不因目录缺失而失败
    if let Some(parent) = work_path.parent()
      && !parent.as_os_str().is_empty()
    {
      fs::create_dir_all(parent)?;
    }
    let mut tmp_os = work_path.as_os_str().to_os_string();
    tmp_os.push(".recovering");
    let tmp_path = PathBuf::from(tmp_os);

    // 1. 预置临时文件（覆盖上一轮可能的残留）；恢复失败时清理残留后原样上抛
    fs::copy(snapshot_path, &tmp_path)?;
    let recovered = match Self::recover_from_cpr_snapshot(&tmp_path, true, StorageBackendType::Disk)
    {
      Ok(tree) => tree,
      Err(e) => {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
      }
    };
    if self.disposed.load(Ordering::Acquire) {
      let _ = fs::remove_file(&tmp_path);
      return Err(Error::Disposed);
    }

    // 2. 先换名再换树：rename 失败时树未换、盘上态未动，清理残留即可原样上抛；
    //    成功后 work_path 命名即新快照 inode（旧树经 fd 继续访问原 inode，不受影响）
    if let Err(e) = fs::rename(&tmp_path, work_path) {
      let _ = fs::remove_file(&tmp_path);
      return Err(e.into());
    }

    // 3. 写锁换树：写锁内仅做指针替换
    let old_tree = {
      let mut guard = self.arc_tree.write();
      // 写锁内复查：并发 dispose 抢先释放时放弃换树，保持已释放语义
      //（recovered 树随栈变量析构关闭，旧树槽位保持 None）
      if self.disposed.load(Ordering::Acquire) {
        return Err(Error::Disposed);
      }
      let old_tree = guard.take();
      let new_tree = recovered.arc_tree.write().take();
      let raw_ptr = new_tree
        .as_ref()
        .map(|t| Arc::as_ptr(t) as *mut BfTree)
        .unwrap_or(null_mut());
      self.raw_tree.store(raw_ptr, Ordering::SeqCst);
      *guard = new_tree;
      // 缓冲上限随树在同一写锁临界段内发布（先于释放）：读侧在 tree_ref 借到新树后，
      // 经锁的 happens-before 必然读到新上限，杜绝「按旧(小)上限选缓冲却撞上新树大记录」的越界
      self
        .max_record_size
        .store(recovered.max_record_size(), Ordering::Release);
      old_tree
    };
    if let Some(t) = old_tree {
      self.retired_trees.write().push(t);
    }
    self
      .storage_backend
      .store(recovered.storage_backend() as u8, Ordering::Release);
    *self.file_path.write() = Some(work_path.to_string_lossy().into_owned());
    Ok(())
  }

  /// 从 CPR 快照文件恢复创建全新的 BfTreeService (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:RecoverFromCprSnapshot)
  ///
  /// 调引擎前先做魔数预检：损坏/非快照文件走结构化 [`Error::Recovery`]，
  /// 不依赖 unwind 拦截 (release 构建 panic = "abort" 下 catch_unwind 无效)。
  /// catch_unwind 仅兜底引擎内部的断言异常 (1:1 对标 Garnet 原生互操作层
  /// bftree_new_from_cpr_snapshot 的处理方式)，dev/test 构建下生效。
  pub fn recover_from_cpr_snapshot(
    recovery_path: impl AsRef<Path>,
    enable_snapshots: bool,
    storage_backend: impl Into<StorageBackendType>,
  ) -> Result<Self> {
    let p = recovery_path.as_ref();
    if !p.exists() {
      return Err(snapshot_missing(p));
    }
    if !file_has_cpr_magic(p) {
      return Err(format_recovery_err(
        "快照文件损坏或格式非法 (魔数不匹配): ",
        p,
      ));
    }
    let backend = storage_backend.into();
    let use_snapshot = enable_snapshots;
    match panic::catch_unwind(AssertUnwindSafe(|| {
      BfTree::new_from_cpr_snapshot(p, use_snapshot, None, None, None)
    })) {
      Ok(Ok(tree)) => {
        let max_record_size = tree
          .config()
          .get_cb_max_record_size()
          .max(MIN_MAX_RECORD_SIZE);
        let tree_arc = Arc::new(tree);
        let raw_ptr = Arc::as_ptr(&tree_arc) as *mut BfTree;
        Ok(Self {
          raw_tree: AtomicPtr::new(raw_ptr),
          arc_tree: RwLock::new(Some(tree_arc)),
          retired_trees: RwLock::new(Vec::new()),
          storage_backend: AtomicU8::new(backend as u8),
          file_path: RwLock::new(Some(p.to_string_lossy().into_owned())),
          max_record_size: AtomicUsize::new(max_record_size),
          disposed: AtomicBool::new(false),
          barriers: AtomicUsize::new(0),
        })
      }
      Ok(Err(e)) => Err(Error::Recovery(config_error_to_string(e))),
      Err(_) => Err(format_recovery_err("快照文件损坏或格式非法: ", p)),
    }
  }
}
