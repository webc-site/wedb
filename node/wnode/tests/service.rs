//! NodeService 编排集成测试：apply → log 顺序、提交后回放与条目分发

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;
use wkv::{StorageBackend, StoreConfig, WedbStore};
use wnode::{AofOp, NodeService, StoreSession, TreeTuning};

/// 与 wkv/tests/range_index_scan.rs TUNE 对齐的合法调优参数
const TUNE: wkv::TreeTuning = wkv::TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

type TestEnv = (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<WalLog<SegmentedDevice>>,
);

/// 在临时目录装配 存储引擎 + 预写日志 双设备
fn open_node(name: &str) -> aok::Result<TestEnv> {
  let dir = tempdir()?;
  let store_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{name}.db")),
  )?);
  let wal_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{name}.wal")),
  )?);
  let mut config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, store_device)?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default())?);
  Ok((dir, store, wal))
}

/// 回放收集器：记录操作类型与键，断言用
#[derive(Default)]
struct CollectingReplay {
  seen: Vec<(AofOp, Vec<u8>)>,
}

impl wnode::Replay for CollectingReplay {
  fn on_entry(&mut self, entry: wnode::AofEntryRef<'_>) -> wnode::AofResult<()> {
    self.seen.push((entry.op, entry.key.to_vec()));
    Ok(())
  }
}

#[test]
fn ri_ops_apply_then_log_and_replay() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_replay")?;
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&wal))?;

    service
      .ri_create(b"idx", StorageBackend::Memory, TUNE)
      .await?;
    service.ri_set(b"idx", b"field-1", b"value-0001").await?;
    service.ri_set(b"idx", b"field-2", b"value-0002").await?;
    service.ri_del(b"idx", b"field-2").await?;
    wal.commit().await?;

    let mut replay = CollectingReplay::default();
    let count = service.replay(&mut replay).await?;
    assert_eq!(count, 4);
    assert_eq!(
      replay.seen,
      vec![
        (AofOp::RiCreate, b"idx".to_vec()),
        (AofOp::RiSet, b"idx".to_vec()),
        (AofOp::RiSet, b"idx".to_vec()),
        (AofOp::RiDel, b"idx".to_vec()),
      ]
    );
    OK
  })
}

#[test]
fn ri_del_missing_field_still_logs() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_skip")?;
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&wal))?;

    service
      .ri_create(b"idx", StorageBackend::Memory, TUNE)
      .await?;
    let deleted = service.ri_del(b"idx", b"absent").await?;
    assert!(deleted);
    wal.commit().await?;

    let mut replay = CollectingReplay::default();
    assert_eq!(service.replay(&mut replay).await?, 2);
    assert_eq!(replay.seen[0].0, AofOp::RiCreate);
    assert_eq!(replay.seen[1].0, AofOp::RiDel);
    OK
  })
}

#[test]
fn ri_set_reaches_bftree() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_data")?;
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&wal))?;

    service.ri_create(b"idx", StorageBackend::Std, TUNE).await?;
    service.ri_set(b"idx", b"field-1", b"value-0001").await?;

    let got = service
      .session()
      .range_index_get(b"idx", b"field-1")
      .await?;
    assert_eq!(got.as_deref(), Some(&b"value-0001"[..]));
    OK
  })
}

/// 读取一行帧头/定长段（`... \r\n`），返回 (行内容, 剩余字节)
fn read_frame_line(rest: &[u8]) -> Option<(&[u8], &[u8])> {
  let pos = rest.iter().position(|&b| b == b'\r')?;
  if rest.get(pos + 1) != Some(&b'\n') {
    return None;
  }
  Some((&rest[..pos], &rest[pos + 2..]))
}

/// 最小 RESP2 bulk-array 参数解析：仅支持 [`wnode::resp`] 产出的规范帧
/// (`*N\r\n` 头 + N 个 `$len\r\n<bytes>\r\n` 段)，供回放端从协议载荷提取
/// 命令参数（对标 C# AofProcessor 经 SessionParser 重新解析 AOF 中的命令）
fn parse_resp_args(frame: &[u8]) -> Option<Vec<&[u8]>> {
  let (count_line, mut rest) = read_frame_line(frame.strip_prefix(b"*")?)?;
  let count: usize = core::str::from_utf8(count_line).ok()?.parse().ok()?;
  let mut args = Vec::with_capacity(count);
  for _ in 0..count {
    let (len_line, after_len) = read_frame_line(rest.strip_prefix(b"$")?)?;
    let len: usize = core::str::from_utf8(len_line).ok()?.parse().ok()?;
    let data = after_len.get(..len)?;
    rest = after_len.get(len..)?.strip_prefix(b"\r\n")?;
    args.push(data);
  }
  Some(args)
}

/// 已解码待重放的 AOF 条目（owned 化，脱离 WAL 扫描的借用期）
struct PendingOp {
  op: AofOp,
  key: Vec<u8>,
  blob: Vec<u8>,
}

/// WAL 条目收集器：第一遍解码提交流（on_entry 为同步契约，引擎副作用
/// 留到第二遍按序重放）
#[derive(Default)]
struct AofCollector {
  ops: Vec<PendingOp>,
}

impl wnode::Replay for AofCollector {
  fn on_entry(&mut self, entry: wnode::AofEntryRef<'_>) -> wnode::AofResult<()> {
    self.ops.push(PendingOp {
      op: entry.op,
      key: entry.key.to_vec(),
      blob: entry.blob.to_vec(),
    });
    Ok(())
  }
}

/// 把单条 AOF 条目按 [`AofOp`] 分发重新作用到（从节点的）引擎会话
///
/// 对标 C# AofProcessor 的条目分发语义：RICREATE/RISET/RIDEL 按扫描顺序
/// 重放为引擎操作；KvUpsert/KvDelete 为预留位（service 尚未接入 KV 编排），
/// 回放端跳过
async fn replay_apply(session: &StoreSession<SegmentedDevice>, entry: &PendingOp) -> Void {
  /// 解析 RESP 帧中的数字参数（帧体即若干 `$len\r\n<bytes>\r\n` 段）
  fn num(args: &[&[u8]], i: usize) -> aok::Result<usize> {
    core::str::from_utf8(args[i])
      .ok()
      .and_then(|s| s.parse().ok())
      .ok_or_else(|| aok::Error::msg("malformed RI.CREATE numeric arg"))
  }
  match entry.op {
    AofOp::RiCreate => {
      // *11 RI.CREATE key BACKEND CACHESIZE n MINRECORD n MAXRECORD n MAXKEYLEN n [PAGESIZE n]
      let args =
        parse_resp_args(&entry.blob).ok_or_else(|| aok::Error::msg("malformed RI.CREATE frame"))?;
      let tuning = TreeTuning {
        cache_size: num(&args, 4)?,
        min_record_size: num(&args, 6)?,
        max_record_size: num(&args, 8)?,
        max_key_len: num(&args, 10)?,
        leaf_page_size: if args.len() > 12 { num(&args, 12)? } else { 0 },
      };
      let backend = if args[2] == b"MEMORY" {
        StorageBackend::Memory
      } else {
        StorageBackend::Std
      };
      session
        .range_index_create(&entry.key, backend, tuning)
        .await?;
    }
    AofOp::RiSet => {
      // *4 RI.SET key field value
      let args =
        parse_resp_args(&entry.blob).ok_or_else(|| aok::Error::msg("malformed RI.SET frame"))?;
      session
        .range_index_set(&entry.key, args[2], args[3])
        .await?;
    }
    AofOp::RiDel => {
      // *3 RI.DEL key field
      let args =
        parse_resp_args(&entry.blob).ok_or_else(|| aok::Error::msg("malformed RI.DEL frame"))?;
      session.range_index_del(&entry.key, args[2]).await?;
    }
    // 预留位：wnode 尚未接入 KV 编排，回放端按 UnknownOp 兜底语义跳过
    AofOp::KvUpsert | AofOp::KvDelete => {}
  }
  OK
}

/// RI.GET 便捷读取（错误抹平进 anyhow，断言用）
async fn ri_get(
  session: &StoreSession<SegmentedDevice>,
  key: &[u8],
  field: &[u8],
) -> aok::Result<Option<Vec<u8>>> {
  Ok(session.range_index_get(key, field).await?)
}

/// RIAofReplayTest 移植 (对标 C# RespRangeIndexTests.RIAofReplayTest)
///
/// C# 流程：主节点写基态 (RI.CREATE + key1..key3) → SAVE 建立检查点基线 →
/// 提交检查点后变更 (key4 新增 / key1 更新 / key2 删除) → COMMITAOF →
/// 重启恢复，AOF 重放后验证四项 RI.GET。
///
/// Rust 版以「从节点/新实例 WAL 回放」承担恢复语义：主节点 apply→log 写入
/// RI 数据并提交（检查点后变更仅存在于 WAL 提交流），从节点为全新引擎实例
/// （零内存状态），回放主节点 WAL 提交流并把 RICREATE/RISET/RIDEL 逐条重新
/// 作用到本端引擎（AofProcessor 语义），最终验证 range_index_get 数据一致：
/// key1 为更新值、key2 已删除、key3 为基态值、key4 为新增值。
#[test]
fn ri_aof_replay_converges_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, wal) = open_node("ri_aof_primary")?;
    let service = NodeService::new(Arc::clone(&store), Arc::clone(&wal))?;

    // 基态数据 (C# 段：RI.CREATE + RI.SET key1..key3 + SAVE)
    service
      .ri_create(b"aoftest", StorageBackend::Std, TUNE)
      .await?;
    service.ri_set(b"aoftest", b"key1", b"val1").await?;
    service.ri_set(b"aoftest", b"key2", b"val2").await?;
    service.ri_set(b"aoftest", b"key3", b"val3").await?;
    wal.commit().await?;

    // 检查点后变更——仅存在于 WAL 提交流 (C# 段：COMMITAOF 之前的三笔变更)
    service.ri_set(b"aoftest", b"key4", b"val4").await?;
    service.ri_set(b"aoftest", b"key1", b"val1-updated").await?;
    service.ri_del(b"aoftest", b"key2").await?;
    wal.commit().await?;

    // 从节点：全新引擎实例 + 独立 WAL，零内存状态
    let (_replica_dir, replica_store, replica_wal) = open_node("ri_aof_replica")?;
    let replica = NodeService::new(Arc::clone(&replica_store), Arc::clone(&replica_wal))?;

    // 对标 AofProcessor：解码主节点 WAL 提交流，按序重放到从节点引擎
    let mut collector = AofCollector::default();
    let count = service.replay(&mut collector).await?;
    assert_eq!(count, 7, "create + 5 sets + 1 del");
    for entry in &collector.ops {
      replay_apply(replica.session(), entry).await?;
    }

    // 数据一致性断言 (对标 C# 四项 RI.GET 断言)
    let primary = service.session();
    let replica = replica.session();
    let key1 = ri_get(primary, b"aoftest", b"key1").await?;
    let key2 = ri_get(primary, b"aoftest", b"key2").await?;
    let key3 = ri_get(primary, b"aoftest", b"key3").await?;
    let key4 = ri_get(primary, b"aoftest", b"key4").await?;

    // key1 应为回放后的更新值
    assert_eq!(key1.as_deref(), Some(&b"val1-updated"[..]));
    // key2 应已被回放删除
    assert_eq!(key2, None);
    // key3 应保有基态值
    assert_eq!(key3.as_deref(), Some(&b"val3"[..]));
    // key4 应为回放新增
    assert_eq!(key4.as_deref(), Some(&b"val4"[..]));

    // 主从两端逐字段一致
    for (field, expect) in [
      (b"key1".as_slice(), Some(&b"val1-updated"[..])),
      (b"key2", None),
      (b"key3", Some(&b"val3"[..])),
      (b"key4", Some(&b"val4"[..])),
    ] {
      assert_eq!(
        ri_get(primary, b"aoftest", field).await?,
        ri_get(replica, b"aoftest", field).await?,
        "field {field:?} diverged between primary and replica"
      );
      assert_eq!(
        ri_get(replica, b"aoftest", field).await?,
        expect.map(<[u8]>::to_vec)
      );
    }
    OK
  })
}
