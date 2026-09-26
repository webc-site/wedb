use std::{env, path::Path};

use aok::{OK, Void};
use bench::sys_info::{MachineInfo, detect_fs_type, is_ram_backed_fs};
use log::info;

#[test]
fn test() -> Void {
  info!("> test {}", 123456);
  OK
}

#[test]
fn detect_fs_type_probes_real_filesystem() {
  // 真实 statfs / /proc/mounts 系统调用，非 mock
  let fs = detect_fs_type(&env::temp_dir());
  assert!(!fs.is_empty());
  assert_ne!(fs, "unknown");
}

#[test]
fn is_ram_backed_fs_matches_memory_filesystems() {
  assert!(is_ram_backed_fs("tmpfs"));
  assert!(is_ram_backed_fs("ramfs"));
  assert!(!is_ram_backed_fs("ext4"));
  assert!(!is_ram_backed_fs("xfs"));
  assert!(!is_ram_backed_fs("apfs"));
}

#[test]
fn machine_info_records_data_dir_and_fs() {
  let data_dir = env::temp_dir();
  let info = MachineInfo::detect(Path::new("."), &data_dir);
  assert_eq!(
    info.data_dir,
    data_dir.canonicalize().unwrap().display().to_string()
  );
  assert!(!info.data_fs.is_empty());
  // 物理盘介质探测与数据目录 fs 类型分开记录，两者共存不混淆
  assert!(!info.disk_type.is_empty());
}
