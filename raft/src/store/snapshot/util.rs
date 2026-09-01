use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

use log::info;

use crate::types::{CompactSnapshotMeta, Snapshot, SnapshotMeta, decode, encode};

pub fn snapshot_dump_file(snapshot_id_dir: &Path) -> PathBuf {
  snapshot_id_dir.join("dump")
}

pub fn snapshot_meta_file(snapshot_id_dir: &Path) -> PathBuf {
  snapshot_id_dir.join("meta")
}

pub fn snapshot_data_file(snapshot_id_dir: &Path) -> PathBuf {
  snapshot_id_dir.join("snapshot")
}

pub fn snapshot_last_snapshot_id_file(snapshot_dir: &Path) -> PathBuf {
  snapshot_dir.join("last_snapshot_id")
}

pub fn snapshot_id_dir(snapshot_dir: &Path, snapshot_id: &str) -> PathBuf {
  snapshot_dir.join(snapshot_id)
}

pub async fn save_last_snapshot_id_file(
  snapshot_dir: &Path,
  last_snapshot_id: &str,
) -> io::Result<()> {
  let last_snapshot_id_file = snapshot_last_snapshot_id_file(snapshot_dir);
  fs::write(&last_snapshot_id_file, last_snapshot_id.as_bytes())?;
  Ok(())
}

pub(crate) async fn get_last_snapshot_id(snapshot_dir: &Path) -> io::Result<String> {
  let last_snapshot_file = snapshot_last_snapshot_id_file(snapshot_dir);
  fs::read_to_string(&last_snapshot_file)
}

pub async fn save_snapshot_meta(snapshot_id_dir: &Path, meta: SnapshotMeta) -> io::Result<()> {
  let meta_file = snapshot_meta_file(snapshot_id_dir);
  let compact = CompactSnapshotMeta::from(&meta);
  let data = encode(&compact)?;
  fs::write(&meta_file, &data)
}

pub async fn get_snapshot_meta(meta_file_path: &Path) -> io::Result<SnapshotMeta> {
  let data = fs::read(meta_file_path)?;
  let compact: CompactSnapshotMeta = decode(&data)?;
  Ok(compact.into())
}

pub async fn get_current_snapshot(snapshot_dir: &Path) -> io::Result<Option<Snapshot>> {
  let snapshot_id = match get_last_snapshot_id(snapshot_dir).await {
    Ok(id) => id,
    Err(e) if e.kind() == ErrorKind::NotFound => {
      info!("No snapshot found, returning None");
      return Ok(None);
    }
    Err(e) => return Err(e),
  };

  let snapshot_id_dir = snapshot_id_dir(snapshot_dir, &snapshot_id);

  let snapshot_meta_file = snapshot_meta_file(&snapshot_id_dir);
  let snapshot_meta = match get_snapshot_meta(&snapshot_meta_file).await {
    Ok(meta) => meta,
    Err(e) if e.kind() == ErrorKind::NotFound => {
      info!("Snapshot metadata not found for snapshot_id={snapshot_id}, returning None");
      return Ok(None);
    }
    Err(e) => return Err(e),
  };

  let snapshot_data_file = snapshot_data_file(&snapshot_id_dir);
  let res = match fs::File::open(&snapshot_data_file) {
    Ok(file) => file,
    Err(e) if e.kind() == ErrorKind::NotFound => {
      info!("Snapshot file not found for snapshot_id={snapshot_id}, returning None");
      return Ok(None);
    }
    Err(e) => return Err(e),
  };

  Ok(Some(Snapshot {
    meta: snapshot_meta,
    snapshot: res,
  }))
}
