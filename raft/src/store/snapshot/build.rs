use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use compio::runtime::spawn;
use futures_channel::oneshot;
use log::{error, info};
use zenoh_raft::SnapshotMeta;

use super::util::{
  save_last_snapshot_id_file, save_snapshot_meta, snapshot_data_file, snapshot_dump_file,
  snapshot_id_dir,
};
use crate::engine::FjallEngine;
use crate::store::key::{SM_DATA_FAMILY, SM_META_FAMILY, TTL_KEY_PREFIX};
use crate::types::{LogId, Snapshot, StoredMembership, read_logs_err};
use crate::util::now_millis;

pub async fn build_snapshot(
  engine: &Arc<FjallEngine>,
  snapshot_dir: &Path,
  last_applied_log_id: Option<LogId>,
  last_membership: StoredMembership,
) -> Result<Snapshot, io::Error> {
  let snapshot_idx = now_millis();

  let snapshot_id = if let Some(last) = last_applied_log_id {
    let leader_id = last.committed_leader_id();
    let idx = last.index();
    format!("{leader_id}-{idx}-{snapshot_idx}")
  } else {
    format!("0-0-{snapshot_idx}")
  };

  let snapshot_id_dir = snapshot_id_dir(snapshot_dir, &snapshot_id);
  fs::create_dir_all(&snapshot_id_dir)?;

  let meta = SnapshotMeta {
    last_log_id: last_applied_log_id,
    last_membership,
  };

  let engine_clone = engine.clone();
  let snapshot_id_dir_clone = snapshot_id_dir.clone();

  let (tx, rx) = oneshot::channel();
  thread::spawn(move || {
    let res = (|| -> io::Result<()> {
      let snapshot_id_dir = snapshot_id_dir_clone;
      let cf_data = engine_clone
        .keyspace(SM_DATA_FAMILY)
        .map_err(|e| io::Error::other(e.to_string()))?;

      let dump_file_name = snapshot_dump_file(&snapshot_id_dir);
      let dump_file = fs::File::create(&dump_file_name)?;

      let mut encoder = zstd::Encoder::new(dump_file, 3)?;
      // 复用写缓冲区：每条记录打包为 tag + key_len + key + val_len + val 一次写入
      let mut write_buf = Vec::with_capacity(4096);

      #[inline]
      fn write_entry(
        encoder: &mut zstd::Encoder<'_, fs::File>,
        buf: &mut Vec<u8>,
        tag: u8,
        key: &[u8],
        val: &[u8],
      ) -> io::Result<()> {
        buf.clear();
        buf.push(tag);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
        buf.extend_from_slice(val);
        encoder.write_all(buf)
      }

      for g in cf_data.iter() {
        let (key, val) = g.into_inner().map_err(read_logs_err)?;
        write_entry(&mut encoder, &mut write_buf, 0, &key, &val)?;
      }

      if let Ok(cf_meta) = engine_clone.keyspace(SM_META_FAMILY) {
        for g in cf_meta.prefix(TTL_KEY_PREFIX) {
          let (key, val) = g.into_inner().map_err(read_logs_err)?;
          write_entry(&mut encoder, &mut write_buf, 1, &key, &val)?;
        }
      }

      encoder.finish()?;

      let snapshot_file_name = snapshot_data_file(&snapshot_id_dir);
      fs::rename(&dump_file_name, &snapshot_file_name)?;

      Ok(())
    })();
    let _ = tx.send(res);
  });

  let res = rx.await.map_err(|e| io::Error::other(e.to_string()))?;
  if let Err(e) = res {
    error!("Failed to build snapshot data file for snapshot id={snapshot_id}: {e}");
    return Err(e);
  }

  if let Err(e) = save_snapshot_meta(&snapshot_id_dir, meta.clone()).await {
    error!("Failed to save snapshot meta file for snapshot id={snapshot_id}: {e}");
    return Err(e);
  }

  if let Err(e) = save_last_snapshot_id_file(snapshot_dir, &snapshot_id).await {
    error!("Failed to save last snapshot id file for snapshot id={snapshot_id}: {e}");
    return Err(e);
  }

  let res = fs::File::open(snapshot_data_file(&snapshot_id_dir))?;

  let snapshot_dir_owned = snapshot_dir.to_path_buf();
  let snapshot_id_clone = snapshot_id.clone();
  spawn(async move {
    if let Err(e) = vacuum_snapshot_files(snapshot_dir_owned, snapshot_id_clone) {
      error!("Failed to cleanup old snapshot files: {e}");
    }
  })
  .detach();

  info!("Snapshot build completed successfully for snapshot_id={snapshot_id}");

  Ok(Snapshot {
    meta,
    snapshot: res,
  })
}

fn vacuum_snapshot_files(snapshot_dir: PathBuf, last_snapshot_id: String) -> Result<(), io::Error> {
  if !snapshot_dir.exists() {
    return Ok(());
  }

  let entries = fs::read_dir(&snapshot_dir)?;
  for entry in entries.flatten() {
    let path = entry.path();
    let file_name = entry.file_name();
    let name_str = file_name.to_string_lossy();

    if path.is_dir() {
      if name_str != last_snapshot_id {
        info!("Vacuuming old snapshot directory: {path:?}");
        if let Err(e) = fs::remove_dir_all(&path)
          && e.kind() != io::ErrorKind::NotFound
        {
          error!("Failed to remove old snapshot dir {path:?}: {e}");
        }
      }
    } else if name_str.ends_with("_incomplete") {
      info!("Vacuuming leftover incomplete snapshot file: {path:?}");
      if let Err(e) = fs::remove_file(&path)
        && e.kind() != io::ErrorKind::NotFound
      {
        error!("Failed to remove incomplete snapshot file {path:?}: {e}");
      }
    }
  }

  Ok(())
}
