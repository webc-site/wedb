//! 检查点版本戳与窗口位同源一致性回归（zcode-r19-wcpr 发现二收口验证）
//!
//! 契约（对标 C# StateTransitions.cs SystemState 的 version+phase 单 8 字节
//! 原子 Word）：版本推进窗口的开合与存储版本共享同一 64 位原子字
//! （`whlog::HybridLog::version_shift`），检查点线程经 `begin_version_shift`
//! 单次 store 同时开窗 + 推版本，写侧经 `write_window_snapshot` 单次读同取
//! 「记录头纪元位」与「AOF 条目 store_version」——「带位 ⟹ 新版本戳」成为
//! 结构性蕴含，恢复期「undoNextVersion 回滚剔除 + AOF 版本闸重放」严格互补，
//! 绝无「带位旧版本戳」（丢写）或「无位新版本戳」（双算）的撕裂记录。
//!
//! 实证红相（修复前）：窗口位（version_shift_floor）与版本戳（current_version）
//! 是两个无互斥原子，写侧两点独立读可分别落在开窗/推版本的间隙两侧。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use waof::{AofAddress, AofHeader, WalConfig, WalLog};
use wbase::{align::DEFAULT_SECTOR_SIZE, time::now_ms};
use wconf::RuntimeServerOptions;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  aof::waof_sublog::single_log_aof,
  database::{DatabaseManagerBase, GarnetDatabase, checkpoint_version},
  resp::{basic_commands::IncrCmd, resp_server_session::RespServerSession},
  service::NodeService,
};

type NodeEnv = (
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<WalLog<SegmentedDevice>>,
);

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

/// 窗口期写入的版本戳必须与检查点新版本严格一致（位/戳同源单原子字），
/// 恢复期回滚剔除与 AOF 重放严格互补、非幂等 INCR 恰好生效一次
#[test]
fn window_writes_carry_shifted_version_stamp_exactly_once() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store, wal) = open_node("ver_stamps")?;
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

    // 1. 基线写（版本 0）：效果物化进快照，AOF 条目随截断被覆盖
    service.session().upsert(b"ctr", b"5").await?;
    wal.commit().await?;

    // 2. 生产漏斗同序：covered 采样先行 → begin_version_shift 单原子开窗 +
    //    推版本（返回值即模糊区地板 index_start，三者同源同字）
    let floor = wcpr::find_latest_checkpoint(&ckpt_dir)?.unwrap_or(0);
    let token = wcpr::next_token_above(floor);
    let new_ver = checkpoint_version(token);
    let covered = AofAddress::create(1, aof.log().tail_address().max());
    let index_start = store.begin_version_shift(new_ver as u64);
    assert!(new_ver > 0, "新版本投影必须为正");

    // 3. 窗口期非幂等写：INCR ×3（ctr 5→8）+ 新键 SET w1（绝对值形态）
    {
      let session = service.session();
      let batch = session.enter_batch();
      let mut resp = RespServerSession::default();
      let mut out = Vec::new();
      for expect in [6i64, 7, 8] {
        out.clear();
        assert!(
          resp
            .network_increment(IncrCmd::Incr, &[b"ctr"], &batch, &mut out)
            .expect("INCR 窗口执行")
        );
        assert_eq!(out, format!(":{expect}\r\n").into_bytes());
      }
      out.clear();
      assert!(
        resp
          .network_set(&[b"w1", b"a"], &batch, None, &mut out)
          .expect("SET w1 窗口执行")
      );
    }
    wal.commit().await?;

    // 4. 版本戳同源断言：扫描 AOF 全量，covered 位点之后的条目（窗口期写）
    //    store_version 必须严格等于检查点新版本——位与戳取自同一合字读，
    //    绝无旧戳条目
    let covered_addr = covered.get(0).unwrap_or(0) as u64;
    let mut stamped: Vec<(String, i64)> = Vec::new();
    let mut iter = aof.log().scan_single_iter(0, 0, i64::MAX);
    loop {
      match iter.next().await {
        Ok(Some(r)) => {
          if waof::is_commit_frame(&r.payload) || r.address < covered_addr {
            continue;
          }
          let header = AofHeader::parse(&r.payload).expect("header decodable");
          let body = &r.payload[AofHeader::TOTAL_SIZE..];
          let key_len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
          let key = body[4..4 + key_len].to_vec();
          stamped.push((
            String::from_utf8(key).expect("utf8 key"),
            header.store_version,
          ));
        }
        Ok(None) => break,
        Err(err) => panic!("AOF 扫描失败: {err:?}"),
      }
    }
    assert!(
      stamped
        .iter()
        .any(|(k, v)| k.ends_with("ctr") && *v == new_ver),
      "窗口期 INCR 条目版本戳必须 = 检查点新版本 {new_ver}，实际 {stamped:?}"
    );
    assert!(
      stamped
        .iter()
        .any(|(k, v)| k.ends_with("w1") && *v == new_ver),
      "窗口期 SET 条目版本戳必须 = 检查点新版本 {new_ver}，实际 {stamped:?}"
    );
    assert!(
      stamped.iter().all(|(_, v)| *v == new_ver),
      "全部窗口期条目版本戳必须同代（无旧戳撕裂），实际 {stamped:?}"
    );

    // 5. 快照落盘（窗口记录同时物化进快照与 AOF 尾部）→ 关窗 → 截断
    let cp_res = store
      .create_checkpoint_with_token(&ckpt_dir, CheckpointType::Snapshot, token, index_start)
      .await;
    store.end_version_shift();
    cp_res?;
    aof.log().truncate_until_async(&covered).await;
    aof.log().commit_async().await;
    db.update_last_save(now_ms());

    // 6. 崩溃恢复 + 重放：带位窗口副本回滚剔除，仅凭 AOF 重放恰好一次
    let recovered = base
      .recover_database_checkpoint_async(&db, None)
      .await?
      .expect("快照必须恢复成功");
    let recovered_db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&recovered),
      Arc::clone(&store.device),
      ckpt_dir.clone(),
      Some(Arc::clone(&aof)),
    ));
    base.replay_database_aof(&recovered_db, u64::MAX).await?;
    let s = recovered.new_session()?;
    assert_eq!(
      s.read(b"ctr").await?,
      Some(b"8".to_vec()),
      "INCR ×3 必须恰好生效一次：5 + 3 = 8（回滚缺失即 11、重放缺失即 5）"
    );
    assert_eq!(
      s.read(b"w1").await?,
      Some(b"a".to_vec()),
      "窗口期 SET 必须经重放恰一次生效"
    );

    OK
  })
}
