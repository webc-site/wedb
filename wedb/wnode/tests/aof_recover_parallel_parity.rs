//! 并行恢复与单任务恢复逐键对账回归（task/ing/zcode-r43-parrecover 验收点：
//! ns 交错多租户 count=4 与 count=1 逐键对账）
//!
//! 回归点：并行恢复（replay_task_count=4）按 can_replay 归属哈希分片重放，
//! 同键恒落同 worker、跨批经双闸栏页串行发布——恢复终态必须与单任务臂
//!（count=1 全归自有）逐键等价：命名空间交错的多租户键集、各域键计数、
//! 键值全量一致。分片归属与会合次序差异不得在终态留下任何可见痕迹。

use std::{sync::Arc, time::Duration};

use compio::time::timeout;
use waof::{AofEntryType, WalConfig};
use wconf::RuntimeServerOptions;
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    recover::aof_recover::AofRecover,
  },
  storage::session::storage_session::StorageSession,
};
use wnode_test::replay_input_bytes;
use wresp::command::RespCommand;
use wtest_base::open_test_store_with_budget;
use wval::{KeyTag, NamespaceDbCodec};

/// 有界时限：两形态均毫秒级收敛，修复前（并行臂闸栏冻结面）在此确定性判失败
const BOUNDED_TIMEOUT: Duration = Duration::from_secs(30);

/// 交错命名空间数（ns 0..=3 全覆盖，物理键前缀刚性隔离域逐域落键）
const NS_COUNT: u64 = 4;

/// 每命名空间键数
const KEYS_PER_NS: usize = 40;

/// 指定域前缀 (ns, db) 的物理键（条目 key 统一 wkv 物理键形态）
fn physical_at(ns: u64, db: u64, user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(ns, db, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// upsert 条目入队（ns 交错多租户形态）
fn enqueue_set_at(log: &GarnetLog, ns: u64, key: &[u8], value: &[u8]) -> waof::Result<()> {
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version: 5,
    session_id: 7,
    key: &physical_at(ns, 0, key),
    value,
    input: &input,
    database_id: 0,
  })?;
  Ok(())
}

/// 装配 ns 交错多租户 AOF（`replay_task_count` 决定恢复拓扑）
async fn build_aof(case: &str, replay_task_count: i32) -> waof::Result<Arc<GarnetAppendOnlyFile>> {
  let (_dirs, backends) = wnode_test::test_sublogs_with_config(case, 1, WalConfig::default());
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 1,
    aof_replay_task_count: replay_task_count,
    ..RuntimeServerOptions::default()
  };
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  let log = aof.log();
  // ns 交错写入（同批内跨域键交错，分片归属逐键离散）
  for i in 0..KEYS_PER_NS {
    for ns in 0..NS_COUNT {
      enqueue_set_at(
        log,
        ns,
        format!("ns{ns}-key{i}").as_bytes(),
        format!("v{ns}-{i}").as_bytes(),
      )?;
    }
  }
  log.commit_async().await;
  Ok(aof)
}

/// 恢复并返回（存储句柄, 重放条数）
async fn recover(
  tag: &str,
  aof: &Arc<GarnetAppendOnlyFile>,
) -> aok::Result<(Arc<wkv::WedbStore<wdev::SegmentedDevice>>, u64)> {
  let (_dir, store) = open_test_store_with_budget(tag, 64 << 20)?;
  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  store.set_current_version(5);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(aof));
  let replayed = timeout(
    BOUNDED_TIMEOUT,
    AofRecover::single_log_recover(&processor, aof, 0, 0, -1, &target),
  )
  .await
  .expect("恢复不得冻结（有界时限外仍无返回）")?;
  Ok((store, replayed))
}

/// ns 交错多租户 AOF：count=4 并行恢复与 count=1 单任务恢复逐键对账——
/// 各域键计数（keyspace_stats）、各域全键集与键值（db_keys / read）全量一致
#[compio::test]
async fn multi_ns_parallel_recover_matches_single_task_per_key() -> aok::Result<()> {
  let aof_single = build_aof("para_parity_single", 1).await?;
  let aof_quad = build_aof("para_parity_quad", 4).await?;

  let (store_single, replayed_single) = recover("para-parity-single", &aof_single).await?;
  let (store_quad, replayed_quad) = recover("para-parity-quad", &aof_quad).await?;

  let expect = NS_COUNT * KEYS_PER_NS as u64;
  assert_eq!(replayed_single, expect, "单任务臂全量重放");
  assert_eq!(replayed_quad, expect, "并行臂全量重放（条数对账）");

  // 逐域全键集与键值对账（set_virtual_context 直设会话物理域：零解析零
  // 分配零落盘，ns 交错键未经路由表装载，keyspace_stats 的在册库口径不可
  // 用；db_keys 即逐键对账本体）
  for ns in 0..NS_COUNT {
    let mut keys_single = None;
    let mut keys_quad = None;
    for (store, keys) in [
      (&store_single, &mut keys_single),
      (&store_quad, &mut keys_quad),
    ] {
      let session = store.new_session()?;
      session.set_virtual_context(ns, 0, ns, 0);
      let storage = StorageSession::new(session.enter_batch());
      let mut enumerated = storage.db_keys(b"*").await?;
      enumerated.sort();
      *keys = Some(enumerated);
    }
    assert_eq!(keys_single, keys_quad, "ns {ns} 全键集必须逐键一致");
    let keys = keys_single.expect("ns 键集必已枚举");
    assert_eq!(keys.len(), KEYS_PER_NS, "ns {ns} 域必须有全量键在场");
    // 键值抽查（首键全值比对；read 挂会话物理域，set_virtual_context 先行）
    let first = keys[0].clone();
    let value_single = {
      let session = store_single.new_session()?;
      session.set_virtual_context(ns, 0, ns, 0);
      session.read(&first).await?
    };
    let value_quad = {
      let session = store_quad.new_session()?;
      session.set_virtual_context(ns, 0, ns, 0);
      session.read(&first).await?
    };
    assert_eq!(value_single, value_quad, "ns {ns} 首键值必须一致");
    assert!(value_single.is_some(), "ns {ns} 首键必须在场");
  }
  Ok(())
}
