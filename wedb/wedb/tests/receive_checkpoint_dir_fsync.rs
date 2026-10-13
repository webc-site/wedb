#![recursion_limit = "256"]
//! 检查点接收面新建文件目录项屏障判别用例（wdev「持久化发布双屏障口径」
//! 消费面：数据 sync_all 后补父目录项 wdev::sync_dir，与主侧 wcpr 发布面
//! sync_dir_tree + sync_checkpoint_dir 次序对位）
//!
//! fsync 语义掉电不可模拟，按仓内持久化测试惯例判别（真帧接收臂 + 真设备
//! 落盘，无替身；replica_receive_checkpoint_retry.rs / checkpoint_import.rs
//! 同源骨架）：
//! 1. 三臂（index ckpt / RI 快照 .bftree / 提交 meta）接收 EOF 哨兵即收尾
//!    刷盘点——目录屏障错位（路径缺失等）当帧即 IOERR 红灯；
//! 2. 接收完成即文件集与三级目录链（{dir}/{token_b32}/rangeindex/*.bftree）
//!    完整，逐字节与主端源一致；
//! 3. 导入收口（基座独占：purge 后检查点目录仅存导入文件集）后冷开——
//!    全新设备实例重开同一路径，recover_latest 必须自该文件集恢复出全量
//!    视图（文件集残破即此处红灯）。

use std::path::PathBuf;

#[path = "common/ckpt_files.rs"]
mod ckpt_files;
#[path = "common/ckpt_node.rs"]
mod ckpt_node;
use std::{fs, sync::Arc};

use ckpt_files::{
  begin_recover_exchange, emit_file_stream, emit_hlog_index, put_str,
  read_primary_checkpoint_files, read_str, snapshot_data, take_primary_checkpoint,
};
use ckpt_node::{open_node, wired_provider};
use compio::runtime::Runtime;
use wbftree::{RangeIndexManager, StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wedb::server::{replication::checkpoint_entry::CheckpointFileType, worker::NodeRole};
use wedb_test::{cluster_consumer::cluster_consumer, resp_drive_scratch::drive};
use wkv::WedbStore;

/// 测试节点身份
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00C1;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00C2;

/// db 设备文件路径（ckpt_node::open_node 以 {tag}.db 命名 db 文件，冷开臂
/// 以全新设备实例重开同一路径）
fn db_path(node: &ckpt_node::CkptNodeStorage, tag: &str) -> PathBuf {
  node.base._dir.path().join(format!("{tag}.db"))
}

/// 全链：三臂接收落盘 → 文件集与三级目录链完整 → 导入收口基座独占 →
/// 冷开 recover_latest 自导入文件集恢复全量视图
#[test]
fn checkpoint_stream_lands_complete_file_set_and_cold_reopens() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 主端：建库（string + RI 树）+ 快照 + 文件集快照
    let primary = open_node("primary");
    let primary_provider =
      wired_provider(&primary, PRIMARY_ID, 7000, NodeRole::Primary, PRIMARY_ID);
    put_str(&primary.store, b"fence_key_a", b"value_a").await;
    // RI 键值长度契约：field+value 总长须落在 [min_record_size, max_record_size]
    let field_a = [b'f'; 32];
    let field_b = [b'g'; 32];
    let value_a = [b'a'; 40];
    let value_b = [b'b'; 40];
    {
      let session = primary.store.new_session().unwrap();
      session
        .range_index_create(b"ri_key", StorageBackendType::Disk, TreeTuning::default())
        .await
        .unwrap();
      session
        .range_index_set(b"ri_key", &field_a, &value_a)
        .await
        .unwrap();
      session
        .range_index_set(b"ri_key", &field_b, &value_b)
        .await
        .unwrap();
    }
    let (token, entry, covered) =
      take_primary_checkpoint(&primary.store, &primary.checkpoint_dir, &primary_provider).await;
    let ri_files =
      RangeIndexManager::enumerate_checkpoint_snapshots(&primary.checkpoint_dir, token).unwrap();
    assert_eq!(ri_files.len(), 1, "检查点必须落盘 RI 快照树文件");
    let (ri_key_id, ri_source_path) = ri_files[0].clone();
    let ri_content = fs::read(&ri_source_path).unwrap();
    let files = read_primary_checkpoint_files(&primary.store, &primary.checkpoint_dir, token).await;
    assert!(!files.hlog.is_empty(), "主端 hlog 段源非空");

    // ===== 副本：空库 + 全接线 provider
    let replica = open_node("replica");
    let provider = wired_provider(&replica, REPLICA_ID, 7001, NodeRole::Replica, PRIMARY_ID);
    let rm = provider.replication_manager().unwrap();
    let mut consumer = cluster_consumer(&provider);
    let old_store = Arc::clone(&replica.store);

    // ===== 段流接收：hlog → index → RI 快照（头帧 key_id → 段流 → 空收尾）
    // → meta（元数据最后落盘 = 提交标记）
    emit_hlog_index(&rt, &mut consumer, token, &files);
    let ri_header_frame = snapshot_data(
      token,
      CheckpointFileType::StoreRangeindexSnapshot,
      -1,
      &ri_key_id.to_le_bytes(),
    );
    assert_eq!(drive(&rt, &mut consumer, &ri_header_frame), b"+OK\r\n");
    emit_file_stream(
      &rt,
      &mut consumer,
      token,
      CheckpointFileType::StoreRangeindexSnapshot,
      &ri_content,
      0,
    );
    let meta_frame = snapshot_data(token, CheckpointFileType::StoreSnapshot, -1, &files.meta);
    assert_eq!(drive(&rt, &mut consumer, &meta_frame), b"+OK\r\n");

    // ===== 接收完成即文件集与目录结构完整（wcpr 命名零改名）
    let meta_path = replica.checkpoint_dir.join(wcpr::meta_filename(token));
    let index_path = replica.checkpoint_dir.join(wcpr::index_filename(token));
    let ri_path =
      RangeIndexManager::checkpoint_snapshot_path_in(&replica.checkpoint_dir, token, ri_key_id);
    let ri_dir = RangeIndexManager::token_snapshot_dir(&replica.checkpoint_dir, token);
    assert_eq!(
      fs::read(&meta_path).unwrap(),
      files.meta,
      "meta 整包逐字节落盘"
    );
    assert_eq!(
      fs::read(&index_path).unwrap(),
      files.index,
      "index ckpt 逐字节落盘"
    );
    assert_eq!(
      ri_path.parent().unwrap(),
      ri_dir,
      "RI 快照必须落在标准三级目录链 dir/token_b32/rangeindex"
    );
    assert_eq!(
      fs::read(&ri_path).unwrap(),
      ri_content,
      "RI 快照树文件逐字节落盘"
    );
    assert!(
      replica.store.device.get_file_size(0).unwrap() >= files.hlog.len() as u64,
      "设备文件幅面不足"
    );

    begin_recover_exchange(&rm, &rt, &mut consumer, &entry, covered);
    let new_store = provider.try_store().unwrap();
    assert!(!Arc::ptr_eq(&new_store, &old_store), "导入必须置换在线引擎");
    assert_eq!(
      read_str(&new_store, b"fence_key_a").await.unwrap(),
      b"value_a"
    );
    {
      let session = new_store.new_session().unwrap();
      assert!(
        session.range_index_exists(b"ri_key").await.unwrap(),
        "已导入 RI 树必须在副本重建"
      );
    }
    let mut landed: Vec<String> = fs::read_dir(&replica.checkpoint_dir)
      .unwrap()
      .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
      .collect();
    landed.sort();
    let mut expect = vec![
      wcpr::meta_filename(token),
      wcpr::index_filename(token),
      wcpr::token_to_base32(token).as_str().to_string(),
    ];
    expect.sort();
    assert_eq!(
      landed, expect,
      "导入收口后检查点目录仅存导入文件集（基座独占）"
    );

    // ===== 二次打开可 recover：进程重启仿真（全新设备实例重开同一路径），
    // recover_latest 必须自导入文件集恢复出全量视图
    drop(new_store);
    drop(consumer);
    drop(rm);
    drop(provider);
    let cold = Arc::new(
      WedbStore::recover_latest(
        &replica.checkpoint_dir,
        Arc::new(SegmentedDevice::single_file(db_path(&replica, "replica")).unwrap()),
      )
      .await
      .unwrap(),
    );
    assert_eq!(
      cold.recovered_checkpoint_token(),
      Some(token),
      "冷开必须命中导入 token"
    );
    assert_eq!(read_str(&cold, b"fence_key_a").await.unwrap(), b"value_a");
    let session = cold.new_session().unwrap();
    assert!(session.range_index_exists(b"ri_key").await.unwrap());
    assert_eq!(
      session.range_index_get(b"ri_key", &field_a).await.unwrap(),
      Some(value_a.to_vec())
    );
    assert_eq!(
      session.range_index_get(b"ri_key", &field_b).await.unwrap(),
      Some(value_b.to_vec())
    );
  });
}
