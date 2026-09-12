use std::{env, path::Path};

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
  /// 操作系统名称与版本
  pub os_info: String,
  /// 操作系统内核版本
  pub kernel_version: String,
}

impl MachineInfo {
  /// 自动探测当前机器的环境配置
  pub fn detect(bench_dir: &Path) -> Self {
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
