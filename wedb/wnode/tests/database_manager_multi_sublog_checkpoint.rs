//! 多子日志检查点 AOF 覆盖地址向量保真回归（对标 C# 检查点位点采样语义：
//! 见 libs/server/Databases/DatabaseManagerBase.cs 的 TakeCheckpointAsync 重载中
//! `AofAddress.Create(AofPhysicalSublogCount, 0)` 开维 + `Log.TailAddress`
//! 原生向量采样、GarnetCheckpointManager.cs:GetCookie 多子日志全向量序列化、
//! GarnetLog.cs:TruncateUntil/InitializeIf 逐子日志位点）
//!
//! 回归点：两物理子日志写入倾斜（sublog 0 写 50 条、sublog 1 写 10 条）下——
//! 1. 检查点元数据覆盖位点必须保存完整向量 `[尾0, 尾1]`（修复前坍缩为
//!    子日志 0 标量）；
//! 2. 检查点截断必须把两个子日志的 begin 各自推进（段文件回收；修复前
//!    truncate_until_async 对子日志 1 取 unwrap_or(0) 恒不删段，磁盘段无界
//!    泄漏）；
//! 3. 恢复期覆盖位点按向量逐子日志还原，子日志 1 尾位点绝不被子日志 0
//!    拔高（修复前 AofAddress::create(size, 标量) 广播制造幽灵空洞）；
//! 4. 崩溃重启后检查点物化面 + 残余增量重放恰一次收敛，全键值完整。

use std::{path::Path, sync::Arc};

use aok::{OK, Void};
use waof::{AofAddress, AofEntryType, SequenceNumberGenerator, WalConfig, WalLog};
use wbase::align::DEFAULT_SECTOR_SIZE;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    recover::aof_recover::AofRecover,
    waof_sublog::AofSublog,
  },
  database::{DatabaseManagerBase, GarnetDatabase},
  storage::session::storage_session::StorageSession,
};
use wnode_test::replay_input_bytes;
use wresp::command::RespCommand;
use wtest_base::test_store_config_with_budget;
use wval::{KeyTag, NamespaceDbCodec};

/// 段容量 128KB：倾斜两子日志均须跨段，截断回收才可观测
const SEGMENT_BYTES: u64 = 128 * 1024;
/// 环形窗口 128KB：单条记录须完整落入窗口
const RING_BYTES: usize = 128 * 1024;
/// 单条值 16KB：sublog 1 十条即跨段
const VALUE_BYTES: usize = 16 * 1024;
const SUBLOG0_ENTRIES: usize = 50;
const SUBLOG1_ENTRIES: usize = 10;

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 枚举用户键直至凑齐指定物理子日志的写入清单（分片判据与写路径同源：
/// GetPhysicalSublogIdx(hash(物理键))）
fn keys_for_sublog(log: &GarnetLog, prefix: &str, sublog_idx: usize, want: usize) -> Vec<Vec<u8>> {
  let mut out = Vec::with_capacity(want);
  let mut i = 0u32;
  while out.len() < want {
    let user = format!("{prefix}{i}");
    let pk = physical(user.as_bytes());
    if log.get_physical_sublog_idx(GarnetLog::hash(&pk)) == sublog_idx {
      out.push(user.into_bytes());
    }
    i += 1;
  }
  out
}

/// 生产双写语义的低层对偶：store 物化 + AOF 镜像条目（RESP 写路径的
/// 编排次序同形——先落 store 再追加日志）
async fn dual_write<D: wdev::Device>(
  storage: &StorageSession<'_, D>,
  log: &GarnetLog,
  version: i64,
  key: &[u8],
  value: &[u8],
) -> aok::Result<()> {
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  storage.upsert_string(key, value).await?;
  log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version,
    session_id: 7,
    key: &physical(key),
    value,
    input: &input,
    database_id: 0,
  })?;
  Ok(())
}

/// 段式真实设备子日志（段容量 [`SEGMENT_BYTES`]，物理截断 / 跨段扫描走真段文件）
fn seg_sublog(tag: &str) -> (tempfile::TempDir, Arc<AofSublog>) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(
    SegmentedDevice::new(
      dir.path().join(format!("{tag}.wal")),
      SEGMENT_BYTES,
      DEFAULT_SECTOR_SIZE,
    )
    .unwrap(),
  );
  let wal = WalLog::new(device, WalConfig::new(RING_BYTES)).unwrap();
  (dir, Arc::new(AofSublog::new(Arc::new(wal))))
}

/// 重开同一磁盘段设备（崩溃重启同盘恢复形态）
fn seg_sublog_reopen(dir: &Path, file: &str) -> Arc<AofSublog> {
  let device =
    Arc::new(SegmentedDevice::new(dir.join(file), SEGMENT_BYTES, DEFAULT_SECTOR_SIZE).unwrap());
  let wal = WalLog::new(device, WalConfig::new(RING_BYTES)).unwrap();
  Arc::new(AofSublog::new(Arc::new(wal)))
}

/// 两物理子日志拓扑装配（段式设备 + 分片选项 + 序列号生成器）
fn open_sharded_log(
  backends: Vec<Arc<AofSublog>>,
) -> aok::Result<(Arc<GarnetAppendOnlyFile>, RuntimeServerOptions)> {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 2,
    aof_replay_task_count: 1,
    ..RuntimeServerOptions::default()
  };
  let seq_gen = Some(Arc::new(SequenceNumberGenerator::new(0)));
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, seq_gen.clone())?),
    &options,
    seq_gen,
  ));
  Ok((aof, options))
}

#[compio::test]
async fn multi_sublog_checkpoint_truncates_and_recovers_per_sublog() -> Void {
  // 0. 装配：store 与多子日志 AOF（生产RESP 双写语义的两面）
  let store_dir = tempfile::tempdir()?;
  let store_device = Arc::new(SegmentedDevice::single_file(
    store_dir.path().join("gen1.db"),
  )?);
  let store = WedbStore::open_shared(
    test_store_config_with_budget(256u64 << 20),
    Arc::clone(&store_device),
  )?;
  let checkpoint_dir = store_dir.path().join("ckpt");
  let pairs: Vec<(tempfile::TempDir, Arc<AofSublog>)> = (0..2)
    .map(|i| seg_sublog(&format!("dbm_vec_ck_{i}")))
    .collect();
  let (dirs, backends): (Vec<tempfile::TempDir>, Vec<Arc<AofSublog>>) = pairs.into_iter().unzip();
  let (aof, _options) = open_sharded_log(backends)?;
  let log = aof.log();
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    Arc::clone(&store_device),
    checkpoint_dir.clone(),
    Some(Arc::clone(&aof)),
  ));
  let mgr = DatabaseManagerBase::new(checkpoint_dir.clone());

  // 1. 倾斜双写 + 全子日志提交 + 设备面收敛（写入会话随块退出纪元——
  //    自持纪元使检查点排空屏障永不完成，检查点须在会话作用域外驱动）
  let (keys0, keys1, value) = {
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch());
    let r = write_skewed(&storage, log, 1).await?;
    drop(storage);
    drop(session);
    r
  };
  log.commit_async().await;
  log.recover_async().await.expect("设备面收敛");

  // 倾斜基线：两子日志尾位点均跨段（截断回收可观测前提），且显著不均
  let tail_pre = log.tail_address();
  let (t0, t1) = (
    tail_pre.get(0).expect("sublog0 tail"),
    tail_pre.get(1).expect("sublog1 tail"),
  );
  assert!(t0 > SEGMENT_BYTES as i64, "sublog0 须跨段: {t0}");
  assert!(t1 > SEGMENT_BYTES as i64, "sublog1 须跨段: {t1}");
  assert!(t0 > t1 * 4, "写入倾斜须成立: {t0} vs {t1}");
  assert_eq!(
    log.begin_address().get(1),
    Some(0),
    "截断前子日志 1 begin 在段起点"
  );

  // 2. 生产检查点内核（单机臂：无 cluster 句柄）——covered 向量采样 →
  //    元数据补写 → 全量截断 → 提交
  assert!(mgr.take_database_checkpoint_async(&db).await?);

  // 断言一：元数据覆盖位点按完整向量持久化（修复前坍缩为子日志 0 标量）
  let (_, meta) = wcpr::latest_checkpoint_meta(&checkpoint_dir).expect("检查点元数据必须落盘");
  let covered_meta = meta.checkpoint_aof_address.expect("覆盖位点必须补写");
  assert_eq!(covered_meta.len(), 2, "两子日志位点必须逐位保存");
  assert!(covered_meta[0] > covered_meta[1], "倾斜位点须保真");
  assert!(covered_meta[1] > 0);

  // 断言二：全量截断——两个子日志 begin 各自推进（段回收；修复前子日志 1
  // 截断目标 unwrap_or(0) 恒不删段，begin 永钉 0）
  let begin = log.begin_address();
  let (b0, b1) = (
    begin.get(0).expect("sublog0 begin"),
    begin.get(1).expect("sublog1 begin"),
  );
  assert!(b0 >= SEGMENT_BYTES as i64, "sublog0 段须回收: {b0}");
  assert!(
    b1 >= SEGMENT_BYTES as i64,
    "子日志 1 段必须被截断回收（修复前恒 0）: {b1}"
  );

  // 3. 崩溃重启：同盘重建设备面 → 位点扫描恢复 → 覆盖位点按向量逐子日志
  //    还原（服务恢复臂 open_recovered_with_config_and_aof 同形）→ initialize_if
  let backends2: Vec<_> = (0..2)
    .map(|i| seg_sublog_reopen(dirs[i].path(), &format!("dbm_vec_ck_{i}.wal")))
    .collect();
  let (aof2, _) = open_sharded_log(backends2)?;
  let log2 = aof2.log();
  log2.recover_async().await.expect("重启设备面恢复");

  // 断言三：恢复后各子日志位点独立保真——子日志 1 尾位点保持自身水位，
  // 绝不被子日志 0 标量广播拔高成幽灵空洞
  let tail_rec = log2.tail_address();
  let r1 = tail_rec.get(1).expect("恢复后 sublog1 tail");
  assert!(
    r1 <= t1 + VALUE_BYTES as i64 + SEGMENT_BYTES as i64,
    "子日志 1 尾位点须保持自身水位（修复前被广播拔高至 {t0} 档制造幽灵空洞）: {r1}"
  );
  assert!(
    tail_rec.get(0).unwrap_or(0) > SEGMENT_BYTES as i64,
    "子日志 0 倾斜水位须保持"
  );

  let mut safe = AofAddress::new(log2.size() as i32);
  for (i, &addr) in covered_meta.iter().enumerate().take(safe.length() as usize) {
    safe.set(i, addr as i64);
  }
  log2.initialize_if(&safe);

  // 4. 恢复闭环：检查点恢复（60 键随快照物化）→ 残余重放（旧代条目版本
  //    闸跳过）→ 全键值读回。恢复宿主重开第一代数据文件（重启同盘形态：
  //    快照数据在其上物化）
  let boot_device = Arc::new(SegmentedDevice::single_file(
    store_dir.path().join("gen1.db"),
  )?);
  let bootstrap = WedbStore::open_shared(
    test_store_config_with_budget(256u64 << 20),
    boot_device.clone(),
  )?;
  let db2 = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&bootstrap),
    boot_device,
    checkpoint_dir.clone(),
    Some(Arc::clone(&aof2)),
  ));
  let recovered = mgr
    .recover_database_checkpoint_async(&db2, None)
    .await?
    .expect("检查点必须恢复成功");
  let session2 = recovered.new_session()?;
  let storage2 = StorageSession::new(session2.enter_batch());
  let target = ReplayTarget {
    session: &storage2,
    store: Arc::clone(&recovered),
    aof_floor: recovered
      .recovered_aof_floor()
      .iter()
      .map(|&a| a as i64)
      .collect(),
  };
  let processor = AofProcessor::new(Arc::clone(&aof2));
  let until = AofAddress::create(2, -1);
  let replayed = AofRecover::multi_log_recover(&processor, &aof2, 0, &until, &target).await?;
  // 计数口径 = 重放链消化条目数（含版本闸跳过的截断残余尾部条目），不卡
  // 精确值；正确性闸在键级——快照物化面零缺失、残余收敛由版本闸承接
  assert!(
    replayed < (SUBLOG0_ENTRIES + SUBLOG1_ENTRIES) as u64,
    "快照已物化条目不得重复重放: {replayed}"
  );
  for k in keys0.iter().chain(&keys1) {
    assert_eq!(
      storage2.read_string(k).await?.as_deref(),
      Some(value.as_slice()),
      "键 {k:?} 须凭快照物化"
    );
  }
  OK
}

/// 倾斜写入：sublog 0 写 [`SUBLOG0_ENTRIES`] 条、sublog 1 写 [`SUBLOG1_ENTRIES`] 条
async fn write_skewed<D: wdev::Device>(
  storage: &StorageSession<'_, D>,
  log: &GarnetLog,
  version: i64,
) -> aok::Result<(Vec<Vec<u8>>, Vec<Vec<u8>>, Vec<u8>)> {
  let keys0 = keys_for_sublog(log, "s0_", 0, SUBLOG0_ENTRIES);
  let keys1 = keys_for_sublog(log, "s1_", 1, SUBLOG1_ENTRIES);
  let value = vec![b'v'; VALUE_BYTES];
  for k in &keys0 {
    dual_write(storage, log, version, k, &value).await?;
  }
  for k in &keys1 {
    dual_write(storage, log, version, k, &value).await?;
  }
  Ok((keys0, keys1, value))
}
