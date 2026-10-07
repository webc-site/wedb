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
  /// 权限收紧失败点名路径上抛不静默。
  ///
  /// 前置清理与校验（对标 C# GarnetServer.cs:289-292 / GarnetServerTcp.cs:95-97）：
  /// 1. 空白路径显式拒绝（对标 C# ArgumentException.ThrowIfNullOrWhiteSpace）；
  /// 2. 已存在文件清理：失败改错误透传（路径为目录等非可忽略错误点名路径和真实 errno 上抛）；
  /// 3. 父目录自动创建：失败改错误透传（若父目录已存在且为目录则 Ok，非目录或建目录失败点名路径和真实 errno 上抛，杜绝吞错）。
  pub async fn bind(path: impl AsRef<Path>, perm: Option<u32>) -> io::Result<(UnixListener, Self)> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() || path.to_str().map(|s| s.trim().is_empty()).unwrap_or(false) {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("Unix 域套接字路径不能为空: {:?}", path),
      ));
    }
    if (path.exists() || path.is_symlink())
      && let Err(e) = remove_file(path)
      && e.kind() != io::ErrorKind::NotFound
    {
      return Err(io::Error::new(
        e.kind(),
        format!("清理旧 Unix 套接字文件失败 {}: {e}", path.display()),
      ));
    }
    if let Some(parent) = path.parent()
      && !parent.as_os_str().is_empty()
      && let Err(e) = create_dir_all(parent)
      && !parent.is_dir()
    {
      return Err(io::Error::new(
        e.kind(),
        format!("创建 Unix 套接字父目录失败 {}: {e}", parent.display()),
      ));
    }
    let listener = UnixListener::bind(path).await.map_err(|e| {
      io::Error::new(
        e.kind(),
        format!("绑定 Unix 套接字失败 {}: {e}", path.display()),
      )
    })?;
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
    if self.path.exists() || self.path.is_symlink() {
      let _ = remove_file(&self.path);
    }
  }
}
