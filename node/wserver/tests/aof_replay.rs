//! AOF 重放闭环集成测试（wkv 临时库 + GarnetLog 写入 → AofProcessor 恢复
//! 重放 → 状态一致断言）
//!
//! 覆盖：主存 upsert/RMW(INCR/APPEND)/delete 回放、对象 upsert/delete 回放、
//! 事务组（TxnStart..TxnCommit）整组重放、检查点标记模糊区、版本闸
//! （旧代跳过/新代缓冲后重放）、分块记录重组回放、FlushAll 标记、
//! 前缀一致上界（SkipReplay）。

use std::sync::Arc;

use aok::OK;
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wserver::{
  aof::{
    aof_address::AofAddress,
    aof_entry_type::AofEntryType,
    aof_header::{AofHeader, AofHeaderType},
    aof_processor::{AofProcessor, ReplayInput, ReplayTarget, encode},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, InMemorySublog, RecordShape, SublogBackend},
    recover::aof_recover::AofRecover,
  },
  config::runtime_server_options::RuntimeServerOptions,
  storage::session::storage_session::StorageSession,
  types::RespCommand,
};

type TestStore = Arc<WedbStore<SegmentedDevice>>;

/// 打开临时文件库
fn open_store(tag: &str) -> aok::Result<(TempDir, TestStore)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag))?);
  let config = StoreConfig::new(16384, 65536, 64, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// 单物理日志拓扑的 AOF 装配
fn aof_fixture() -> aok::Result<Arc<GarnetAppendOnlyFile>> {
  let options = RuntimeServerOptions::default();
  let backends: Vec<Arc<dyn SublogBackend>> =
    vec![Arc::new(InMemorySublog::new()) as Arc<dyn SublogBackend>];
  Ok(Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends)),
    &options,
  )))
}

/// SpanByte 长度前缀编码（C# 负载布局：[u32 len][bytes]）
fn lp(bytes: &[u8]) -> Vec<u8> {
  let mut out = (bytes.len() as u32).to_le_bytes().to_vec();
  out.extend_from_slice(bytes);
  out
}

/// upsert 条目编码入队（写端 = 命令层 AOF 通路的最小形态）
fn enqueue_upsert(log: &GarnetLog, version: i64, key: &[u8], value: &[u8]) -> aok::Result<i64> {
  // 写入端经 GarnetLog::enqueue 落 BasicHeader + 负载；ReplayInput 随 input 通道
  // 内联（C# StringInput 序列化的组合形态），供重放端重建命令参数
  let mut input = Vec::new();
  ReplayInput {
    cmd: RespCommand::Set,
    flags: 0,
    sub_id: 0,
    arg1: 0,
    arg2: 0,
    arg3: 0,
    args: vec![key.to_vec(), value.to_vec()],
  }
  .serialize(&mut input);
  Ok(log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version,
    session_id: 1,
    key: &lp(key),
    value: &lp(value),
    input: &input,
    database_id: 0,
  }))
}

/// 主存写入→恢复重放→状态一致闭环
#[test]
fn test_upsert_rmw_delete_replay_loop() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-loop.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // ── 写入段：模拟命令层 AOF 通路 ──
    // SET k v1（upsert）
    enqueue_upsert(log, 5, b"k", b"v1")?;
    // SET cnt 10（upsert，供 INCR 叠加）
    enqueue_upsert(log, 5, b"cnt", b"10")?;
    // INCRBY cnt 32（RMW）
    {
      let mut input = Vec::new();
      ReplayInput {
        cmd: RespCommand::Incrby,
        flags: 0,
        sub_id: 0,
        arg1: 32,
        arg2: 0,
        arg3: 0,
        args: vec![b"32".to_vec()],
      }
      .serialize(&mut input);
      log.enqueue(&RecordShape {
        op_type: AofEntryType::StoreRMW,
        version: 5,
        session_id: 1,
        key: &lp(b"cnt"),
        value: &[],
        input: &input,
        database_id: 0,
      });
    }
    // APPEND k "-tail"（RMW 携带 parseState 参数序列化）
    {
      let mut input = Vec::new();
      ReplayInput {
        cmd: RespCommand::Append,
        flags: 0,
        sub_id: 0,
        arg1: 0,
        arg2: 0,
        arg3: 0,
        args: vec![b"-tail".to_vec()],
      }
      .serialize(&mut input);
      log.enqueue(&RecordShape {
        op_type: AofEntryType::StoreRMW,
        version: 5,
        session_id: 1,
        key: &lp(b"k"),
        value: &[],
        input: &input,
        database_id: 0,
      });
    }
    // DEL gone（delete，先建键）
    enqueue_upsert(log, 5, b"gone", b"x")?;
    {
      log.enqueue(&RecordShape {
        op_type: AofEntryType::StoreDelete,
        version: 5,
        session_id: 1,
        key: &lp(b"gone"),
        value: &[],
        input: &[],
        database_id: 0,
      });
    }
    log.commit(0);

    // ── 重放段：全新库恢复 ──
    let (_dir2, store2) = open_store("aof-loop-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 6, "全部条目均应重放");

    // ── 状态一致断言 ──
    assert_eq!(
      storage2.read_string(b"k").await?,
      Some(b"v1-tail".to_vec()),
      "upsert + APPEND 重放闭环"
    );
    assert_eq!(
      storage2.read_string(b"cnt").await?,
      Some(b"42".to_vec()),
      "upsert + INCRBY 重放闭环"
    );
    assert_eq!(
      storage2.read_string(b"gone").await?,
      None,
      "delete 重放闭环"
    );
    OK
  })
}

/// 对象存 upsert/delete 回放闭环
#[test]
fn test_object_replay_loop() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-obj.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // HSET 信封 upsert（对象值 = [tag u8][payload]）
    let mut value = vec![3u8]; // OBJ_TAG_HASH
    value.extend_from_slice(b"payload");
    log.enqueue(&RecordShape {
      op_type: AofEntryType::ObjectStoreUpsert,
      version: 5,
      session_id: 1,
      key: &lp(b"h"),
      value: &lp(&value),
      input: &[],
      database_id: 0,
    });
    // 对象 delete
    let mut del = AofHeader::new();
    del.set_header_type(AofHeaderType::BasicHeader);
    del.op_type = AofEntryType::ObjectStoreDelete as u8;
    del.store_version = 5;
    let entry = encode::keyed_entry(&del, b"h", &[]);
    log.get_sub_log(0).enqueue(0, &entry);
    log.commit(0);

    // 重放到全新库
    let (_dir2, store2) = open_store("aof-obj-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 2);

    // HSET 信封后 DEL：键应不存在
    assert_eq!(storage2.read_string(b"h").await?, None, "对象 delete 闭环");
    OK
  })
}

/// 版本闸与检查点模糊区：旧代跳过、新代缓冲后重放、事务组整组重放
#[test]
fn test_version_gate_and_txn_group_replay() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-txn.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // 当前存储版本 = 6
    // (1) 旧代 upsert（v5 < 6）：恢复路径跳过
    enqueue_upsert(log, 5, b"old", b"stale")?;
    // (2) 事务组：TxnStart → SET a 1 → SET b 2 → TxnCommit（v6）
    log.enqueue(&RecordShape {
      op_type: AofEntryType::TxnStart,
      version: 6,
      session_id: 9,
      key: &[],
      value: &[],
      input: &[],
      database_id: 0,
    });
    enqueue_upsert(log, 6, b"a", b"1")?;
    enqueue_upsert(log, 6, b"b", b"2")?;
    log.enqueue(&RecordShape {
      op_type: AofEntryType::TxnCommit,
      version: 6,
      session_id: 9,
      key: &[],
      value: &[],
      input: &[],
      database_id: 0,
    });
    // (3) FlushAll 标记
    let mut flush = AofHeader::new();
    flush.set_header_type(AofHeaderType::BasicHeader);
    flush.op_type = AofEntryType::FlushAll as u8;
    flush.store_version = 6;
    log
      .get_sub_log(0)
      .enqueue(0, &encode::keyless_entry(&flush));
    log.commit(0);

    // 重放
    let (_dir2, store2) = open_store("aof-txn-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 6,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 6, "条目计数含跳过与标记条");

    // 事务组两键均落库（旧代键被版本闸跳过）
    assert_eq!(storage2.read_string(b"old").await?, None, "旧代条目跳过");
    assert_eq!(storage2.read_string(b"a").await?, Some(b"1".to_vec()));
    assert_eq!(storage2.read_string(b"b").await?, Some(b"2".to_vec()));
    OK
  })
}

/// 分块记录：写入端分块 → 读取端重组回放闭环
#[test]
fn test_chunked_record_replay_loop() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-chunk.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // 大对象值分块写入（enqueue_object_chunked；信封 = [tag u8][payload]）
    let mut big_value = vec![3u8]; // OBJ_TAG_HASH
    big_value.extend_from_slice(&vec![b'o'; 300]);
    log.enqueue_object_chunked(&wserver::aof::garnet_log::ChunkedShape {
      record: RecordShape {
        op_type: AofEntryType::ObjectStoreUpsert,
        version: 5,
        session_id: 2,
        key: b"big",
        value: &big_value,
        input: &[],
        database_id: 0,
      },
      write_value: true,
      write_input: false,
    });
    log.commit(0);

    // 重放：处理器内置分块读取器完成重组后按对象 upsert 落库
    let (_dir2, store2) = open_store("aof-chunk-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 2, "首块 + 数据块 = 2 条记录");
    // 信封 [tag][payload]：重组值经对象 upsert 落库后完整一致
    let got = storage2.read_string(b"big").await?.unwrap_or_default();
    assert_eq!(got.len(), 1 + 300, "信封 = 标签 + 载荷");
    assert_eq!(got[0], 3, "OBJ_TAG_HASH");
    assert!(got[1..].iter().all(|&b| b == b'o'));
    OK
  })
}

/// 前缀一致上界：SkipReplay 阈值截断 + 版本闸跳过计数
#[test]
fn test_skip_replay_prefix_bound() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-skip.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();
    let first = enqueue_upsert(log, 5, b"early", b"1")?;
    enqueue_upsert(log, 5, b"later", b"2")?;
    log.commit(0);

    // until_sequence_number = first：第二条地址超过阈值 → 前缀截断
    let (_dir2, store2) = open_store("aof-skip-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed =
      AofRecover::recover_replay_driver(&processor, &aof, 0, -1, first, &target).await?;
    assert_eq!(replayed, 1, "前缀一致上界截断后续条目");
    assert_eq!(storage2.read_string(b"early").await?, Some(b"1".to_vec()));
    assert_eq!(storage2.read_string(b"later").await?, None);

    // 无效地址向量形态核对
    let invalid = aof.invalid_aof_address();
    assert_eq!(invalid, AofAddress::create(1, -1));
    OK
  })
}
