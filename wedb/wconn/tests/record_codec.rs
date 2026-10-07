#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::slice::from_ref;

use compio::runtime::Runtime;
use wconn::{
  Error,
  record::{
    BatchItem, CONTINUATION_FLAG, MigrateVal, MigrateVectorElement, MigrationDomainContext,
    MigrationFrame, MigrationRecord, MigrationRecordKind, encode_dbmeta_frame,
    encode_frame_payload, encode_migration_payload, encode_range_index_stream_payload_into,
    encode_vector_set_element_payload, encode_vector_set_index_payload, parse_migration_payload,
    prepend_migration_frame, send_chunked_record,
  },
};

/// 完整记录帧金字节：编码输出逐字节钉死 + 同字节解码往返等价
#[test]
fn record_frame_golden_bytes_roundtrip() {
  let item = BatchItem {
    key: b"ka",
    val: MigrateVal::Str(b"vv".to_vec()),
    expire_ticks: 5,
  };
  let payload = encode_migration_payload(&[item]);
  let mut expect = Vec::new();
  expect.extend_from_slice(&1u32.to_le_bytes());
  expect.push(MigrationRecordKind::String as u8);
  expect.extend_from_slice(&2u32.to_le_bytes());
  expect.extend_from_slice(b"ka");
  expect.extend_from_slice(&2u32.to_le_bytes());
  expect.extend_from_slice(b"vv");
  expect.extend_from_slice(&5i64.to_le_bytes());
  assert_eq!(payload, expect);

  let (count, frames) = parse_migration_payload(&payload).unwrap();
  assert_eq!(count, 1);
  assert_eq!(
    frames[0],
    MigrationFrame::Record(MigrationRecord::Str {
      key: b"ka",
      val: b"vv",
      expire_ticks: 5,
    })
  );
}

/// 分块出帧金字节：48B 单帧切 32+16 两块，首块置续块位、末块不置，
/// 两块载荷拼接 = 完整单条记录帧（下沉前后帧字节口径）
#[test]
fn send_chunked_record_frame_golden_bytes() {
  let item = BatchItem {
    key: b"k",
    val: MigrateVal::Str(vec![7u8; 30]),
    expire_ticks: 1,
  };
  let total = item.frame_len();
  assert_eq!(total, 48);
  let full = encode_migration_payload(from_ref(&item))[4..].to_vec();

  let mut frames: Vec<Vec<u8>> = Vec::new();
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    send_chunked_record(&item, 32, async |chunk| {
      frames.push(chunk.to_vec());
      Ok::<(), ()>(())
    })
    .await
    .unwrap();
  });
  assert_eq!(frames.len(), 2);
  let head0 = u32::from_le_bytes(frames[0][5..9].try_into().unwrap());
  assert_eq!(head0, 32 | CONTINUATION_FLAG);
  let head1 = u32::from_le_bytes(frames[1][5..9].try_into().unwrap());
  assert_eq!(head1, 16);
  for frame in &frames {
    assert_eq!(&frame[..4], &1u32.to_le_bytes());
    assert_eq!(frame[4], MigrationRecordKind::Chunked as u8);
  }
  let joined: Vec<u8> = frames.iter().flat_map(|f| f[9..].iter().copied()).collect();
  assert_eq!(joined, full);
}

/// 巨值计数短帧协议违约回归：伪造 count=0xFFFFFFFF 的 5 字节帧在解析
/// 循环前具名拒绝（RecordCountExceedsPayload），绝无按 u32 全量预分配
///（分配失败 abort 全进程的协议面 DoS 守卫）；计数恰等可证下界
///（每记录 1 字节 kind）的载荷不得被违约门误伤，仍由逐帧截断收口
#[test]
fn parse_rejects_forged_record_count_upfront() {
  let mut forged = Vec::new();
  forged.extend_from_slice(&u32::MAX.to_le_bytes());
  forged.push(MigrationRecordKind::String as u8);
  let err = parse_migration_payload(&forged).unwrap_err();
  assert!(
    matches!(err, Error::RecordCountExceedsPayload(u32::MAX, 1)),
    "巨值计数须循环前具名违约，不 abort: {err:?}"
  );

  let mut tight = Vec::new();
  tight.extend_from_slice(&2u32.to_le_bytes());
  tight.extend_from_slice(&[
    MigrationRecordKind::String as u8,
    MigrationRecordKind::Envelope as u8,
  ]);
  let err = parse_migration_payload(&tight).unwrap_err();
  assert!(
    matches!(err, Error::InvalidRecord(_)),
    "计数=可证下界时放行入循环，由逐帧截断收口: {err:?}"
  );
}

/// RangeIndex 分块流帧（含流元三元组金字节）与向量集帧的编解码往返
#[test]
fn range_index_and_vector_frames_roundtrip() {
  let mut buf = Vec::new();
  encode_range_index_stream_payload_into(b"chunk", 5, i64::MAX, 1234, &mut buf);
  // 帧体金字节：kind 后紧跟 17B 流元（obj_type + next_expiry + expire），
  // 再 [u32 LE len][chunk]
  let mut golden = Vec::new();
  golden.extend_from_slice(&1u32.to_le_bytes());
  golden.push(MigrationRecordKind::RangeIndex as u8);
  golden.push(5);
  golden.extend_from_slice(&i64::MAX.to_le_bytes());
  golden.extend_from_slice(&1234i64.to_le_bytes());
  golden.extend_from_slice(&5u32.to_le_bytes());
  golden.extend_from_slice(b"chunk");
  assert_eq!(buf, golden);
  let (count, frames) = parse_migration_payload(&buf).unwrap();
  assert_eq!(count, 1);
  assert_eq!(
    frames[0],
    MigrationFrame::RangeIndexStream {
      obj_type: 5,
      next_expiry: i64::MAX,
      expire_ticks: 1234,
      bytes: b"chunk"
    }
  );
  // 流元段截断拒绝
  let mut short = golden.clone();
  short.truncate(golden.len() - 3);
  assert!(parse_migration_payload(&short).is_err());

  let payload = encode_vector_set_index_payload(b"vk", b"idx56");
  let (_, frames) = parse_migration_payload(&payload).unwrap();
  assert_eq!(
    frames[0],
    MigrationFrame::VectorSetIndex {
      key: b"vk",
      value: b"idx56"
    }
  );

  let elem = MigrateVectorElement {
    key: b"vk".to_vec(),
    element: b"e1".to_vec(),
    values: vec![1, 2, 3, 4],
    attributes: b"a".to_vec(),
  };
  let payload = encode_vector_set_element_payload(&[elem]);
  let (_, frames) = parse_migration_payload(&payload).unwrap();
  assert_eq!(
    frames[0],
    MigrationFrame::VectorSetElement {
      key: b"vk",
      element: b"e1",
      values: &[1, 2, 3, 4],
      attributes: b"a"
    }
  );
}

/// 跨域扩展帧（kind=7 落域上下文 / kind=8 DbMeta 映射）编解码往返与
/// 域上下文前插：前插后帧数 +1、首帧即上下文帧、原帧字节原样保留
#[test]
fn domain_context_and_dbmeta_frames_roundtrip() {
  let ctx = MigrationDomainContext {
    vns: 3,
    vdb: 9,
    ns: 7,
    db: 2,
  };
  let ctx_frame = ctx.encode_frame().to_vec();
  let meta_frame = encode_dbmeta_frame(&[0x01, 1, 2, 3, 4, 5, 6, 7, 8], &9u64.to_be_bytes());

  let payload = encode_frame_payload(&[ctx_frame.clone(), meta_frame]);
  let (count, frames) = parse_migration_payload(&payload).unwrap();
  assert_eq!(count, 2);
  assert_eq!(frames[0], MigrationFrame::DomainContext(ctx));
  assert_eq!(
    frames[1],
    MigrationFrame::DbMeta {
      key: &[0x01, 1, 2, 3, 4, 5, 6, 7, 8],
      value: &9u64.to_be_bytes()
    }
  );

  // 前插：原载荷（单记录帧）帧数头 +1，首帧为上下文帧，其余字节不动
  let item = BatchItem {
    key: b"ka",
    val: MigrateVal::Str(b"vv".to_vec()),
    expire_ticks: 5,
  };
  let payload = encode_migration_payload(&[item]);
  let prepended = prepend_migration_frame(&ctx_frame, &payload);
  let (count, frames) = parse_migration_payload(&prepended).unwrap();
  assert_eq!(count, 2);
  assert_eq!(frames[0], MigrationFrame::DomainContext(ctx));
  assert_eq!(&prepended[4 + ctx_frame.len()..], &payload[4..]);

  // 帧体截断拒绝
  let short = [MigrationRecordKind::DomainContext as u8, 0, 0, 0];
  let mut bad = Vec::new();
  bad.extend_from_slice(&1u32.to_le_bytes());
  bad.extend_from_slice(&short);
  assert!(parse_migration_payload(&bad).is_err());
}
