//! Snapshot 检查点恢复基线只读隔离回归（zcode-r19-wcpr 发现一收口验证）
//!
//! 契约（对标 C# SnapshotCheckpointSMTask 的快照文件隔离性）：
//! Snapshot 型检查点恢复出的实例，其快照字节在作为恢复基线期间全程只读——
//! rust 无独立快照文件（主日志即快照字节唯一载体），隔离性由
//! `read_only_address == tail_address` 单语义等价达成：恢复实例对检查点内
//! 已有键的非幂等写（INCR）必须降级 RCU 尾部追加，绝不原位覆写
//! [begin, tail) 快照区间。二次崩溃恢复（recover_latest 回退原 Token）装载
//! 干净基线后由 AOF 重放恰好一次承接增量，非幂等命令绝不双重生效。
//!
//! 实证红相（修复前）：恢复实例 ro = CalculateReadOnlyAddress(head, tail) < tail，
//! INCR 经原位臂改写驻留页字节并随 flush 落盘，二次恢复装载污染页（快照
//! 重插物化 5→8 的效果）且 AOF 重放再 +3，计数器 11 ≠ 8（静默双算）。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use waof::{WalConfig, WalLog};
use wbase::{align::DEFAULT_SECTOR_SIZE, time::now_ms};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  aof::waof_sublog::single_log_aof,
  database::{DatabaseManagerBase, GarnetDatabase},
  resp::{basic_commands::IncrCmd, resp_server_session::RespServerSession},
  service::NodeService,
};

type NodeEnv = (
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<WalLog<SegmentedDevice>>,
);

/// 打开节点（store + 1MB 环形 wal，保障检查点窗口期写入不挤出）
fn open_node(name: &str) -> aok::Result<NodeEnv> {
  let dir = tempdir()?;
  let store_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{name}.db")),
  )?);
  let wal_device = Arc::new(SegmentedDevice::new(
    dir.path().join(format!("{name}.wal")),
    64 * 1024,
    DEFAULT_SECTOR_SIZE,
  )?);
  let mut config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, store_device)?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::new(1 << 20))?);
  Ok((dir, store, wal))
}

#[test]
fn snapshot_recovery_inplace_isolation_nonidempotent_exactly_once() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store, wal) = open_node("snap_isolation")?;
    let aof = single_log_aof(Arc::clone(&wal), &RuntimeServerOptions::default())
      .expect("装配 single_log_aof");
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&aof))?;
    let ckpt_dir = dir.path().join("ckpt");
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      ckpt_dir.clone(),
      Some(Arc::clone(&aof)),
    ));
    let base = DatabaseManagerBase::new(ckpt_dir.clone());

    // 1. 基线写：SET ctr 5（先于检查点，效果物化进快照，AOF 随截断被覆盖）
    service.session().upsert(b"ctr", b"5").await?;
    wal.commit().await?;

    // 2. 生产漏斗拍 Snapshot 型检查点（生产默认 use_fold_over=false）：
    //    快照发布后 AOF 截断至 covered，单一物理日志域截断与数据同域
    let taken = base.take_database_checkpoint_async(&db).await?;
    assert!(taken, "首拍检查点必须发起成功");
    assert!(
      wcpr::find_latest_checkpoint(&ckpt_dir)?.is_some(),
      "检查点必须已发布"
    );

    // 3. 崩溃重启恢复：版本基线推进至恢复 Token + 重放 AOF 尾部（截断后近空）
    let recovered = base
      .recover_database_checkpoint_async(&db, None)
      .await?
      .expect("快照必须恢复成功");
    assert_eq!(
      recovered.read_only_address(),
      recovered.tail_address(),
      "Snapshot 恢复基线必须全程只读（ro=tail 隔离语义）"
    );
    let recovered_db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&recovered),
      Arc::clone(&store.device),
      ckpt_dir.clone(),
      Some(Arc::clone(&aof)),
    ));
    base.replay_database_aof(&recovered_db, u64::MAX).await?;
    {
      let s = recovered.new_session()?;
      assert_eq!(s.read(b"ctr").await?, Some(b"5".to_vec()), "基线计数 5");
    }

    // 4. 恢复实例上对检查点内已有键 INCR ×3：记录落在快照区间 [begin, tail)
    //    内（可变区驻留），隔离契约要求降级 RCU 追加、绝不原位覆写快照字节
    let rec_service = NodeService::new(Arc::clone(&recovered), Arc::clone(&aof))?;
    {
      let session = rec_service.session();
      let batch = session.enter_batch();
      let mut resp = RespServerSession::default();
      let mut out = Vec::new();
      for expect in [6i64, 7, 8] {
        out.clear();
        assert!(
          resp
            .network_increment(IncrCmd::Incr, &[b"ctr"], &batch, &mut out)
            .expect("INCR 执行"),
          "INCR 必须命中计数器"
        );
        assert_eq!(out, format!(":{expect}\r\n").into_bytes(), "INCR 增量生效");
      }
    }
    wal.commit().await?;

    // 5. 强制落盘 + 全量驱逐：任何原位污染（若隔离失效）此刻均已定稿到设备
    recovered.flush_and_evict_all().await?;

    // 6. 二次崩溃：丢弃恢复实例（不落任何新检查点），AOF 停留在含 3 条 INCR
    //    的状态
    drop(rec_service);
    drop(recovered_db);
    drop(recovered);
    db.update_last_save(now_ms());

    // 7. 二次恢复（recover_latest 回退原 Token）：干净快照基线 + AOF 重放
    let redead = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      ckpt_dir.clone(),
      Some(Arc::clone(&aof)),
    ));
    let recovered2 = base
      .recover_database_checkpoint_async(&redead, None)
      .await?
      .expect("二次恢复必须成功");
    let redead2 = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&recovered2),
      Arc::clone(&store.device),
      ckpt_dir.clone(),
      Some(Arc::clone(&aof)),
    ));
    base.replay_database_aof(&redead2, u64::MAX).await?;

    // 8. 严格终值：5（快照基线）+ 3（AOF 重放恰一次）= 8。
    //    隔离失效即 11：快照页被 INCR 原位污染至 8，重放再 +3
    let s = recovered2.new_session()?;
    assert_eq!(
      s.read(b"ctr").await?,
      Some(b"8".to_vec()),
      "非幂等 INCR 二次崩溃恢复必须恰好生效一次：5 + 3 = 8（快照基线污染即 11）"
    );

    OK
  })
}
