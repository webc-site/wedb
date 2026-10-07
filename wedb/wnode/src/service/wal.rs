use super::*;

/// WAL 物理日志基名（分段设备的路径前缀，段文件按 `<基名>.<段号>` 命名，
/// 对标 C# GetAofDevice 的 "aof.log" 描述符 + StorageDeviceBase.cs:
/// GetSegmentFilename 的 `aof.log.<segmentId>` 段名）
const WAL_BASE_FILE_NAME: &str = "wal.log";

/// WAL 物理日志装配（优先显式 wal_dir，未指定时默认 `<data>/wal`），
/// 返回（解析后的目录，日志句柄）
///
/// 段容量与页/窗口尺寸由已校验的 [`AofSettings`] 注入（C# GetAofSettings →
/// TsavoriteLogSettings.SegmentSizeBits → AllocatorBase.Initialize
/// `LogDevice.Initialize(1L << SegmentSizeBits, ...)` 的投影链）：设备按
/// 段字节尺寸分段装配（段文件 `<基名>.<段号>`），历史段随检查点截断真实
/// 物理删除，磁盘空间可回收；日志设置经 [`AofSettings::wal_config`] 同批
/// 装载 aof-memory / aof-page-size 两尺寸
pub(super) fn open_wal(
  data_path: &Path,
  wal_dir: Option<&Path>,
  settings: &AofSettings,
) -> crate::Result<(PathBuf, Arc<WalLog<SegmentedDevice>>)> {
  let wal_dir = match wal_dir {
    Some(dir) => dir.to_path_buf(),
    None => data_path
      .parent()
      .unwrap_or_else(|| Path::new("."))
      .join("wal"),
  };
  wdev::ensure_dir_persistent(&wal_dir)?;
  let (segment_size, sector_size) = settings.device_segment();
  let wal_device = Arc::new(SegmentedDevice::new(
    wal_dir.join(WAL_BASE_FILE_NAME),
    segment_size,
    sector_size,
  )?);
  let wal = Arc::new(WalLog::new(wal_device, settings.wal_config())?);
  Ok((wal_dir, wal))
}
