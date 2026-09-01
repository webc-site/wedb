use std::fs::File as StdFile;
use std::io::{self, ErrorKind, Read};
use std::sync::Arc;
use std::thread;

use futures_channel::oneshot;
use log::{error, info};

use crate::engine::FjallEngine;
use crate::store::FjallStateMachine;
use crate::store::batch_remove_guards;
use crate::store::key::{SM_DATA_FAMILY, SM_META_FAMILY, TTL_IDX_KEY_PREFIX, TTL_KEY_PREFIX};
use crate::types::{Snapshot, read_logs_err};

pub async fn recover_snapshot(
  engine: &Arc<FjallEngine>,
  snapshot: Snapshot,
) -> Result<(), io::Error> {
  let snapshot_id = snapshot
    .meta
    .last_log_id
    .map(|id| id.to_string())
    .unwrap_or_else(|| "0".to_string());

  info!("Starting to recover from snapshot, snapshot_id={snapshot_id}");

  let snapshot_file = snapshot.snapshot;
  let engine_clone = engine.clone();

  let (tx, rx) = oneshot::channel();
  thread::spawn(move || {
    let res = do_recover_snapshot_blocking(&engine_clone, snapshot_file);
    let _ = tx.send(res);
  });

  let res = rx.await.map_err(read_logs_err)?;
  if let Err(e) = res {
    error!("Failed to recover snapshot from snapshot_id={snapshot_id}: {e:?}");
    return Err(e);
  }

  info!("Snapshot recovery completed successfully for snapshot_id={snapshot_id}");
  Ok(())
}

fn do_recover_snapshot_blocking(
  engine: &Arc<FjallEngine>,
  mut snapshot_file: StdFile,
) -> Result<(), io::Error> {
  use std::io::{Seek, SeekFrom};
  snapshot_file.seek(SeekFrom::Start(0))?;

  let cf_data = engine
    .keyspace(SM_DATA_FAMILY)
    .map_err(|e| io::Error::new(ErrorKind::NotFound, e.to_string()))?;
  let cf_meta = engine
    .keyspace(SM_META_FAMILY)
    .map_err(|e| io::Error::new(ErrorKind::NotFound, e.to_string()))?;

  // 分批清理旧 cf_data 与旧 TTL 元数据（防止大数据量下单 batch 过大）
  batch_remove_guards(engine.db(), &cf_data, cf_data.iter(), 5000)?;
  batch_remove_guards(engine.db(), &cf_meta, cf_meta.prefix(TTL_KEY_PREFIX), 5000)?;
  batch_remove_guards(
    engine.db(),
    &cf_meta,
    cf_meta.prefix(TTL_IDX_KEY_PREFIX),
    5000,
  )?;

  let mut decoder =
    zstd::Decoder::new(snapshot_file).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

  let mut count = 0u64;
  let mut tag_buf = [0u8; 1];
  let mut len_buf = [0u8; 4];
  let mut batch = engine.db().batch();
  // 预分配可复用缓冲区，避免每条记录都分配堆内存
  let mut key_buf = Vec::with_capacity(256);
  let mut val_buf = Vec::with_capacity(4096);

  loop {
    match decoder.read_exact(&mut tag_buf) {
      Ok(_) => {}
      Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
      Err(e) => return Err(e),
    }
    let tag = tag_buf[0];

    decoder.read_exact(&mut len_buf)?;
    let key_len = u32::from_le_bytes(len_buf) as usize;

    key_buf.clear();
    key_buf.resize(key_len, 0);
    decoder.read_exact(&mut key_buf)?;

    decoder.read_exact(&mut len_buf)?;
    let value_len = u32::from_le_bytes(len_buf) as usize;

    val_buf.clear();
    val_buf.resize(value_len, 0);
    decoder.read_exact(&mut val_buf)?;

    match tag {
      1 => {
        batch.insert(&cf_meta, &key_buf, &val_buf);
        if key_buf.starts_with(TTL_KEY_PREFIX)
          && let Some(&buf) = val_buf.first_chunk::<8>()
        {
          let user_key = &key_buf[TTL_KEY_PREFIX.len()..];
          let expire_at = u64::from_be_bytes(buf);
          FjallStateMachine::with_ttl_idx_key(expire_at, user_key, |idx_key| {
            batch.insert(&cf_meta, idx_key, []);
          });
        }
      }
      _ => batch.insert(&cf_data, &key_buf, &val_buf),
    }
    count += 1;

    if count.is_multiple_of(1000) {
      batch.commit().map_err(io::Error::other)?;
      batch = engine.db().batch();
      info!("[metadata] Recovered {count} entries so far...");
    }
  }

  batch.commit().map_err(io::Error::other)?;
  engine.persist().map_err(io::Error::other)?;

  info!("Successfully recovered {count} entries from snapshot");
  Ok(())
}
