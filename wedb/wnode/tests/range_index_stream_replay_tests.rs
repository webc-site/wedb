//! 范围索引流重组与清理集成测试
//!
//! 1:1 对标 garnet/test/standalone/Garnet.test/RangeIndexStreamReplayTests.cs：
//! 测试 AOF 流重组记账、未完成流清理、首块重置、格式损坏处理、末块违例检测与异常回收。

use std::{fs, io::Cursor, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::{
  RANGE_INDEX_STUB_SIZE, RangeIndexChunkedSerializer, RangeIndexManager as Engine,
  RangeIndexMigrationReader,
};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wnode::{
  GarnetAppendOnlyFile, GarnetLog,
  aof::aof_processor::AofProcessor,
  range_index::range_index_manager_replication::{
    RangeIndexChunkArgs, RangeIndexManagerReplication, ReplicationError,
  },
};
use wval::GarnetObjectType;

async fn process_chunk(
  replication: &RangeIndexManagerReplication,
  key: &[u8],
  chunk: &[u8],
  is_first: bool,
  is_last: bool,
) -> Result<(), ReplicationError> {
  replication
    .process_stream_chunk::<SegmentedDevice>(
      None,
      RangeIndexChunkArgs {
        key,
        obj_type: GarnetObjectType::Null,
        chunk,
        is_first,
        is_last,
        replace: false,
        next_expiry: i64::MAX,
      },
    )
    .await
}

/// 构造固定尺寸的测试 Stub 字节
///
/// test/standalone/Garnet.test/RangeIndexStreamReplayTests.cs:MakeStub
fn make_stub() -> [u8; RANGE_INDEX_STUB_SIZE] {
  let mut stub = [0u8; RANGE_INDEX_STUB_SIZE];
  for (i, byte) in stub.iter_mut().enumerate() {
    *byte = (0xC0 + i) as u8;
  }
  stub
}

/// 生成确定性伪随机测试数据
///
/// test/standalone/Garnet.test/RangeIndexStreamReplayTests.cs:RandomBytes
fn random_bytes(n: usize) -> Vec<u8> {
  let mut b = vec![0u8; n];
  for (i, x) in b.iter_mut().enumerate() {
    *x = ((i * 37 + 1234) % 256) as u8;
  }
  b
}

/// 按照生产 RangeIndexMigrationReader 将完整流分块为 (chunk, is_first, is_last) 元组序列
///
/// test/standalone/Garnet.test/RangeIndexStreamReplayTests.cs:BuildStreamChunks
fn build_stream_chunks(
  key: &[u8],
  stub: &[u8],
  file_data: &[u8],
  chunk_size: usize,
) -> Vec<(Vec<u8>, bool, bool)> {
  let serializer = RangeIndexChunkedSerializer::new(key, stub, file_data.len() as u64);
  let mut reader = RangeIndexMigrationReader::new(
    serializer,
    Cursor::new(file_data.to_vec()),
    None,
    chunk_size,
  )
  .expect("构造 RangeIndexMigrationReader");

  let mut result = Vec::new();
  let mut dest = vec![0u8; chunk_size];
  let mut is_first = true;

  while !reader.is_complete() {
    let written = reader.read_next_chunk(&mut dest).expect("读取下一个分块");
    result.push((dest[..written].to_vec(), is_first, reader.is_complete()));
    is_first = false;
  }

  result
}

/// test/standalone/Garnet.test/RangeIndexStreamReplayTests.cs:PartialStreamIsPendingThenCleanedUp
///
/// 仅推送第一块后处于进行中状态，调用 dispose_incomplete_stream_reassembly 能够清理未完成重组与临时文件
#[test]
fn partial_stream_is_pending_then_cleaned_up() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let engine = Arc::new(
      Engine::new(dir.path().join("ri"), dir.path().join("cpr")).expect("构造 RangeIndexManager"),
    );
    let replication = RangeIndexManagerReplication::new(Arc::clone(&engine));
    let key = b"k1";
    let stub = make_stub();
    let file_data = random_bytes(8192);
    let chunks = build_stream_chunks(key, &stub, &file_data, 512);
    assert!(chunks.len() > 2, "测试需要多块流");

    // 仅喂第一块：重组状态正在进行中
    let res = process_chunk(&replication, key, &chunks[0].0, chunks[0].1, chunks[0].2).await;
    assert!(res.is_ok());
    assert_eq!(replication.pending_stream_reassembly_count(), 1);

    // 验证临时迁移目录中存在未完成的临时文件
    let temp_files_before: Vec<_> = fs::read_dir(dir.path().join("ri").join("migration-tmp"))
      .unwrap()
      .flatten()
      .collect();
    assert_eq!(temp_files_before.len(), 1);

    // 重放结束/异常清理：丢弃未完成的重组，临时文件被完全清理
    replication.dispose_incomplete_stream_reassembly();
    assert_eq!(replication.pending_stream_reassembly_count(), 0);

    let temp_files_after: Vec<_> = fs::read_dir(dir.path().join("ri").join("migration-tmp"))
      .unwrap()
      .flatten()
      .collect();
    assert_eq!(temp_files_after.len(), 0);
  });
}

/// test/standalone/Garnet.test/RangeIndexStreamReplayTests.cs:RetryFirstChunkResetsStalePartialReassembly
///
/// 重试流的首块必须重置陈旧的半完成重组状态
#[test]
fn retry_first_chunk_resets_stale_partial_reassembly() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let engine = Arc::new(
      Engine::new(dir.path().join("ri"), dir.path().join("cpr")).expect("构造 RangeIndexManager"),
    );
    let replication = RangeIndexManagerReplication::new(Arc::clone(&engine));
    let key = b"k1";
    let stub = make_stub();
    let file_data = random_bytes(8192);
    let chunks = build_stream_chunks(key, &stub, &file_data, 512);
    assert!(chunks.len() > 3, "测试需要多块流");

    // 尝试 1：仅喂第一块后中断
    let res = process_chunk(&replication, key, &chunks[0].0, true, false).await;
    assert!(res.is_ok());
    assert_eq!(replication.pending_stream_reassembly_count(), 1);

    // 尝试 2 (重试)：首块 (is_first=true) 必须重置同键的陈旧状态
    for (i, chunk) in chunks.iter().take(chunks.len() - 1).enumerate() {
      let res = process_chunk(&replication, key, &chunk.0, i == 0, false).await;
      assert!(res.is_ok());
    }

    assert_eq!(
      replication.pending_stream_reassembly_count(),
      1,
      "重试首块重置后应正常重组"
    );

    // 清理未完成流
    replication.dispose_incomplete_stream_reassembly();
    assert_eq!(replication.pending_stream_reassembly_count(), 0);
  });
}

/// test/standalone/Garnet.test/RangeIndexStreamReplayTests.cs:MalformedChunkIsDropped
///
/// 损坏分块导致反序列化失败后立即摘除并释放重组状态
#[test]
fn malformed_chunk_is_dropped() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let engine = Arc::new(
      Engine::new(dir.path().join("ri"), dir.path().join("cpr")).expect("构造 RangeIndexManager"),
    );
    let replication = RangeIndexManagerReplication::new(Arc::clone(&engine));
    let key = b"k1";

    // 前 4 字节为 0 的损坏块（键长度非法 <= 0）
    let malformed = [0u8; 16];
    let res = process_chunk(&replication, key, &malformed, true, false).await;
    assert!(res.is_err());
    assert_eq!(replication.pending_stream_reassembly_count(), 0);

    // 临时文件被删除
    let temp_files: Vec<_> = fs::read_dir(dir.path().join("ri").join("migration-tmp"))
      .unwrap()
      .flatten()
      .collect();
    assert_eq!(temp_files.len(), 0);
  });
}

/// test/standalone/Garnet.test/RangeIndexStreamReplayTests.cs:FinalFlagOnIncompleteStreamDropsReassembly
///
/// 未完成流被置 is_last 标志视为流畸形/截断，抛出错误并丢弃清理
#[test]
fn final_flag_on_incomplete_stream_drops_reassembly() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let engine = Arc::new(
      Engine::new(dir.path().join("ri"), dir.path().join("cpr")).expect("构造 RangeIndexManager"),
    );
    let replication = RangeIndexManagerReplication::new(Arc::clone(&engine));
    let key = b"k1";
    let stub = make_stub();
    let file_data = random_bytes(8192);
    let chunks = build_stream_chunks(key, &stub, &file_data, 512);
    assert!(chunks.len() > 2, "测试需要多块流");

    // 喂入首块但置 is_last=true
    let res = process_chunk(&replication, key, &chunks[0].0, true, true).await;
    assert!(res.is_err());
    assert_eq!(replication.pending_stream_reassembly_count(), 0);

    // 临时文件被删除
    let temp_files: Vec<_> = fs::read_dir(dir.path().join("ri").join("migration-tmp"))
      .unwrap()
      .flatten()
      .collect();
    assert_eq!(temp_files.len(), 0);
  });
}

/// test/standalone/Garnet.test/RangeIndexStreamReplayTests.cs:PublishFailureOnCompleteStreamThrows
///
/// 完整流完成时若发布失败（例如未提供 session 或发布异常），必须报错并清理丢弃重组状态
#[test]
fn publish_failure_on_complete_stream_throws() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let engine = Arc::new(
      Engine::new(dir.path().join("ri"), dir.path().join("cpr")).expect("构造 RangeIndexManager"),
    );
    let replication = RangeIndexManagerReplication::new(Arc::clone(&engine));
    let key = b"k1";
    let stub = make_stub();
    let file_data = random_bytes(8192);
    let chunks = build_stream_chunks(key, &stub, &file_data, 512);

    // 喂入除末块外的所有分块
    for chunk in chunks.iter().take(chunks.len() - 1) {
      let res = process_chunk(&replication, key, &chunk.0, chunk.1, chunk.2).await;
      assert!(res.is_ok());
    }

    assert_eq!(replication.pending_stream_reassembly_count(), 1);

    // 喂入末块但无 session（发布失败）
    let last = chunks.last().unwrap();
    let res = process_chunk(&replication, key, &last.0, last.1, last.2).await;
    assert!(res.is_err());
    assert_eq!(replication.pending_stream_reassembly_count(), 0);

    // 临时文件被删除
    let temp_files: Vec<_> = fs::read_dir(dir.path().join("ri").join("migration-tmp"))
      .unwrap()
      .flatten()
      .collect();
    assert_eq!(temp_files.len(), 0);
  });
}

/// 超时或管理器析构时自动回收未完成流重组
#[test]
fn timeout_and_manager_drop_cleans_incomplete_streams() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let migration_tmp = dir.path().join("ri").join("migration-tmp");

    {
      let engine = Arc::new(
        Engine::new(dir.path().join("ri"), dir.path().join("cpr")).expect("构造 RangeIndexManager"),
      );
      let replication = RangeIndexManagerReplication::new(Arc::clone(&engine));
      let key = b"k1";
      let stub = make_stub();
      let file_data = random_bytes(8192);
      let chunks = build_stream_chunks(key, &stub, &file_data, 512);

      // 喂入第一块
      process_chunk(&replication, key, &chunks[0].0, true, false)
        .await
        .unwrap();
      assert_eq!(replication.pending_stream_reassembly_count(), 1);

      // 临时文件已存在
      let temp_files: Vec<_> = fs::read_dir(&migration_tmp).unwrap().flatten().collect();
      assert_eq!(temp_files.len(), 1);

      // replication 在作用域结束时 drop，触发 Drop::drop -> dispose_incomplete_stream_reassembly
    }

    // 作用域结束析构后，临时文件已被完全清理
    let temp_files: Vec<_> = fs::read_dir(&migration_tmp).unwrap().flatten().collect();
    assert_eq!(temp_files.len(), 0);
  });
}

/// 内存 AOF 用于构造 AofProcessor
fn memory_aof() -> Arc<wnode::GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = wnode_test::test_sublogs("ri_stream", 1);
          backends
        },
        None,
      )
      .expect("构造 GarnetLog"),
    ),
    &options,
    None,
  ))
}

/// AofProcessor 析构时触发范围索引流清理
#[test]
fn aof_processor_drop_cleans_range_index_reassembly() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let engine = Arc::new(
      Engine::new(dir.path().join("ri"), dir.path().join("cpr")).expect("构造 RangeIndexManager"),
    );
    let replication = Arc::new(RangeIndexManagerReplication::new(Arc::clone(&engine)));
    let migration_tmp = engine.migration_temp_dir().to_path_buf();

    let key = b"k1";
    let stub = make_stub();
    let file_data = random_bytes(8192);
    let chunks = build_stream_chunks(key, &stub, &file_data, 512);

    {
      let mut processor = AofProcessor::new(memory_aof());
      processor.set_range_index_manager(Arc::clone(&replication));

      process_chunk(&replication, key, &chunks[0].0, true, false)
        .await
        .unwrap();
      assert_eq!(replication.pending_stream_reassembly_count(), 1);

      let temp_files: Vec<_> = fs::read_dir(&migration_tmp).unwrap().flatten().collect();
      assert_eq!(temp_files.len(), 1);

      // processor drop 触发 DisposeIncompleteStreamReassembly
    }

    assert_eq!(replication.pending_stream_reassembly_count(), 0);
    let temp_files: Vec<_> = fs::read_dir(&migration_tmp).unwrap().flatten().collect();
    assert_eq!(temp_files.len(), 0);
  });
}
