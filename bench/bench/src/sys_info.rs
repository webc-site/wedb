use std::{env, ffi::CString, mem::zeroed, path::Path};

use sysinfo::{CpuRefreshKind, Disks, MemoryRefreshKind, RefreshKind, System};

/// 机器硬件与系统环境配置
#[derive(Debug, Clone, serde::Serialize)]
pub struct MachineInfo {
  /// CPU 型号名称
  pub cpu_brand: String,
  /// CPU 物理核心数
  pub physical_cores: usize,
  /// CPU 逻辑核心数 (线程数)
  pub logical_cores: usize,
  /// 架构体系 (如 aarch64, x86_64)
  pub arch: &'static str,
  /// 内存总大小 (GiB)
  pub total_memory_gib: f64,
  /// 磁盘介质类型 (如 NVMe SSD, SSD)
  pub disk_type: String,
  /// 评测数据目录绝对路径 (--data-path 指定，缺省 OS 临时目录)
  pub data_dir: String,
  /// 数据目录所在文件系统类型 (如 ext4, apfs, tmpfs)
  pub data_fs: String,
  /// 操作系统名称与版本
  pub os_info: String,
  /// 操作系统内核版本
  pub kernel_version: String,
}

impl MachineInfo {
  /// 自动探测当前机器的环境配置
  ///
  /// bench_dir 仅用于物理盘介质探测；data_dir 为评测数据目录（已由调用方确保存在），
  /// 两者分开记录，不互相混淆
  pub fn detect(bench_dir: &Path, data_dir: &Path) -> Self {
    let mut sys = System::new_with_specifics(
      RefreshKind::nothing()
        .with_cpu(CpuRefreshKind::everything())
        .with_memory(MemoryRefreshKind::everything()),
    );
    sys.refresh_cpu_all();
    sys.refresh_memory();

    let cpu_brand = sys
      .cpus()
      .first()
      .map(|c| c.brand().trim().to_string())
      .filter(|s| !s.is_empty())
      .unwrap_or_else(|| "Unknown CPU".to_string());

    let physical_cores = System::physical_core_count().unwrap_or(sys.cpus().len());
    let logical_cores = sys.cpus().len();
    let arch = env::consts::ARCH;

    let total_memory_gib = sys.total_memory() as f64 / (1024.0 * 1024.0 * 1024.0);

    let disk_type = detect_disk_type(bench_dir);

    let data_dir = data_dir
      .canonicalize()
      .unwrap_or_else(|_| data_dir.to_path_buf());
    let data_fs = detect_fs_type(&data_dir);

    let os_name = System::name().unwrap_or_else(|| env::consts::OS.to_string());
    let os_ver = System::os_version().unwrap_or_default();
    let os_info = if os_ver.is_empty() {
      os_name
    } else {
      format!("{os_name} {os_ver}")
    };

    let kernel_version = System::kernel_version().unwrap_or_else(|| "Unknown".to_string());

    Self {
      cpu_brand,
      physical_cores,
      logical_cores,
      arch,
      total_memory_gib,
      disk_type,
      data_dir: data_dir.display().to_string(),
      data_fs,
      os_info,
      kernel_version,
    }
  }
}

/// 自动探测磁盘介质类型 (NVMe SSD / SSD / HDD)
fn detect_disk_type(_bench_dir: &Path) -> String {
  #[cfg(target_os = "macos")]
  {
    use std::process::Command;
    if let Ok(out) = Command::new("system_profiler")
      .args(["SPNVMeDataType"])
      .output()
    {
      let text = String::from_utf8_lossy(&out.stdout);
      if text.contains("NVMExpress") || text.contains("NVMe") || text.contains("Apple SSD") {
        return "NVMe SSD".to_string();
      }
    }
    if let Ok(out) = Command::new("diskutil").args(["info", "/"]).output() {
      let text = String::from_utf8_lossy(&out.stdout);
      if text.contains("Solid State:               Yes") {
        return "NVMe SSD".to_string();
      }
    }
  }

  #[cfg(target_os = "linux")]
  {
    use std::fs;
    if let Ok(entries) = fs::read_dir("/sys/block") {
      let mut is_nvme = false;
      let mut is_ssd = false;
      for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("nvme") {
          is_nvme = true;
          break;
        }
        if let Ok(rot) = fs::read_to_string(entry.path().join("queue/rotational"))
          && rot.trim() == "0"
        {
          is_ssd = true;
        }
      }
      if is_nvme {
        return "NVMe SSD".to_string();
      }
      if is_ssd {
        return "SSD".to_string();
      }
    }
  }

  let disks = Disks::new_with_refreshed_list();
  for disk in &disks {
    match disk.kind() {
      sysinfo::DiskKind::SSD => return "SSD".to_string(),
      sysinfo::DiskKind::HDD => return "HDD".to_string(),
      _ => {}
    }
  }

  "NVMe SSD".to_string()
}

/// 判断文件系统类型是否为内存盘 (数据不落物理盘)
pub fn is_ram_backed_fs(fs: &str) -> bool {
  fs == "tmpfs" || fs == "ramfs"
}

/// 探测指定目录所在文件系统类型名 (如 ext4, apfs, tmpfs)，失败返回 "unknown"
///
/// - Linux: 优先解析 /proc/mounts 挂载点最长前缀匹配取 fs 名，失败退 statfs f_type 魔数；
/// - macOS: statfs f_fstypename 直接取文件系统名
pub fn detect_fs_type(dir: &Path) -> String {
  #[cfg(target_os = "linux")]
  {
    if let Some(fs) = fs_type_via_mounts(dir) {
      return fs;
    }
    if let Some(fs) = fs_type_via_statfs(dir) {
      return fs;
    }
  }

  #[cfg(target_os = "macos")]
  {
    let mut st: libc::statfs = unsafe { zeroed() };
    if let Ok(c) = CString::new(dir.as_os_str().as_encoded_bytes())
      && unsafe { libc::statfs(c.as_ptr(), &mut st) } == 0
    {
      let name = st
        .f_fstypename
        .iter()
        .take_while(|&&b| b != 0)
        .map(|&b| b as u8)
        .collect::<Vec<_>>();
      if !name.is_empty() {
        return String::from_utf8_lossy(&name).into_owned();
      }
    }
  }

  "unknown".to_string()
}

/// Linux: 解析 /proc/mounts，取挂载点最长前缀匹配条目的文件系统名
#[cfg(target_os = "linux")]
fn fs_type_via_mounts(dir: &Path) -> Option<String> {
  let content = std::fs::read_to_string("/proc/mounts").ok()?;
  let target = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
  let mut best: Option<(usize, &str)> = None;
  for line in content.lines() {
    let mut parts = line.split(' ');
    // 格式: 设备 挂载点 fs类型 选项 dump pass；挂载点中空格转义为 \040
    let Some(mount) = parts.nth(1) else { continue };
    let Some(fstype) = parts.next() else { continue };
    let mount = mount.replace("\\040", " ");
    let mount = Path::new(&mount);
    if target.starts_with(mount)
      && best
        .as_ref()
        .is_none_or(|(len, _)| mount.as_os_str().len() > *len)
    {
      best = Some((mount.as_os_str().len(), fstype));
    }
  }
  best.map(|(_, fs)| fs.to_string())
}

/// Linux: statfs f_type 魔数兜底，仅覆盖判定内存盘与常见本地文件系统
#[cfg(target_os = "linux")]
fn fs_type_via_statfs(dir: &Path) -> Option<String> {
  const EXT_MAGIC: i64 = 0xef53; // ext2/3/4 共用
  const XFS_MAGIC: i64 = 0x5846_5342;
  const BTRFS_MAGIC: i64 = 0x9123_683e;
  const TMPFS_MAGIC: i64 = 0x0102_1994;
  const RAMFS_MAGIC: i64 = 0x8584_58f6;

  let c = CString::new(dir.as_os_str().as_encoded_bytes()).ok()?;
  let mut st: libc::statfs = unsafe { zeroed() };
  if unsafe { libc::statfs(c.as_ptr(), &mut st) } != 0 {
    return None;
  }
  let name = match st.f_type {
    TMPFS_MAGIC => "tmpfs",
    RAMFS_MAGIC => "ramfs",
    EXT_MAGIC => "ext4",
    XFS_MAGIC => "xfs",
    BTRFS_MAGIC => "btrfs",
    _ => return None,
  };
  Some(name.to_string())
}

use std::cell::RefCell;

thread_local! {
  static MEM_SYS: RefCell<System> = RefCell::new(System::new());
}

/// 精准测量当前进程真实物理常驻内存 (字节)
///
/// - Linux: 直接读取 `/proc/self/statm` 获取常驻页数并换算为字节（零堆分配开销）；
/// - 其他平台: 线程局部复用 `sysinfo::System` 单进程采样，杜绝每轮测量反复分配宿主拓扑结构体的开销。
pub fn get_process_physical_memory() -> u64 {
  #[cfg(target_os = "linux")]
  {
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
      let mut parts = statm.split_ascii_whitespace();
      let _vsize = parts.next();
      if let Some(rss_pages) = parts.next().and_then(|s| s.parse::<u64>().ok()) {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page_size = if page_size > 0 {
          page_size as u64
        } else {
          4096
        };
        return rss_pages * page_size;
      }
    }
  }

  let pid = sysinfo::get_current_pid().ok();
  if let Some(pid) = pid {
    return MEM_SYS.with(|cell| {
      let mut sys = cell.borrow_mut();
      sys.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        true,
        sysinfo::ProcessRefreshKind::nothing().with_memory(),
      );
      sys.process(pid).map(|p| p.memory()).unwrap_or(0)
    });
  }
  0
}
