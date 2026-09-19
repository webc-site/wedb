//! Unix 域套接字治理：路径预处理、过期套接字清理与 RAII 自动释放

#[cfg(unix)]
use std::{
  fs::{self, create_dir_all, remove_file, set_permissions},
  io,
  os::unix::fs::PermissionsExt,
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
  /// 预处理路径并绑定 Unix 域套接字监听器，按配置收紧套接字文件权限
  ///
  /// 对标 libs/server/Servers/GarnetServerTcp.cs:Start：Bind 之后非默认权限
  /// （C# `unixSocketPermission != default` 跳过臂 → rust `Option::None`）时
  /// File.SetUnixFileMode（:151）即 `set_permissions`；perm 为 wconf
  /// `NodeArgs::unix_socket_mode` 折算后的真实模式位，绑定侧不设二次校验，
  /// 权限收紧失败点名路径上抛不静默
  pub async fn bind(path: impl AsRef<Path>, perm: Option<u32>) -> io::Result<(UnixListener, Self)> {
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
    if let Some(mode) = perm {
      set_permissions(path, fs::Permissions::from_mode(mode)).map_err(|e| {
        io::Error::new(
          e.kind(),
          format!(
            "设置 Unix 套接字文件权限失败 {} {:o}: {e}",
            path.display(),
            mode
          ),
        )
      })?;
    }
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
