//! Unix 域套接字治理：路径预处理、过期套接字清理与 RAII 自动释放

#[cfg(unix)]
use std::{
  fs::{create_dir_all, remove_file},
  io,
  path::{Path, PathBuf},
};

#[cfg(unix)]
use compio::net::UnixListener;

/// Unix 域套接字路径守卫（RAII 析构时安全清理套接字文件）
#[cfg(unix)]
pub struct UdsGuard {
  path: PathBuf,
}

#[cfg(unix)]
impl UdsGuard {
  /// 预处理路径并绑定 Unix 域套接字监听器
  pub async fn bind(path: impl AsRef<Path>) -> io::Result<(UnixListener, Self)> {
    let path = path.as_ref();
    if path.exists() {
      let _ = remove_file(path);
    }
    if let Some(parent) = path.parent()
      && !parent.as_os_str().is_empty()
    {
      let _ = create_dir_all(parent);
    }
    let listener = UnixListener::bind(path).await?;
    let guard = Self {
      path: path.to_path_buf(),
    };
    Ok((listener, guard))
  }

  /// 获取套接字路径
  pub fn path(&self) -> &Path {
    &self.path
  }
}

#[cfg(unix)]
impl Drop for UdsGuard {
  fn drop(&mut self) {
    if self.path.exists() {
      let _ = remove_file(&self.path);
    }
  }
}
