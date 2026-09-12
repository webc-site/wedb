use std::{str::from_utf8, sync::Arc};

use wnode::resp::vector::{
  disk_ann_service::DiskAnnInsertResult,
  hnsw::HnswConfig,
  resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
  vector_manager::{
    CONTEXT_METADATA_SIZE, METADATA_NAMESPACE, RECORD_TYPE, VectorAddArgs, VectorManager,
    VectorManagerOptions, VectorManagerResult, VectorSearchOptions, unpack_length_prefixed,
  },
  vector_manager_index::Index,
  vector_types::{
    VectorDistanceMetricType, VectorIdFormat, VectorQuantType, VectorSetFlags, VectorValueType,
  },
};

fn session() -> RespServerSessionVectors {
  let manager = Arc::new(VectorManager::new(VectorManagerOptions {
    is_enabled: true,
    ..Default::default()
  }));
  RespServerSessionVectors::new(manager)
}

fn manager() -> VectorManager {
  VectorManager::new(VectorManagerOptions {
    is_enabled: true,
    ..Default::default()
  })
}

fn fresh_index(context: u64, dims: u32) -> Index {
  Index {
    context,
    index_ptr: 1,
    dimensions: dims,
    reduce_dims: 0,
    num_links: 8,
    build_exploration_factor: 64,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    flags: VectorSetFlags::NONE,
  }
}

fn s(bytes: &[u8]) -> &str {
  from_utf8(bytes).unwrap()
}

fn err_text(r: VectorReply) -> String {
  match r {
    VectorReply::Error(e) => s(&e).to_owned(),
    other => panic!("期望错误应答，实际 {other:?}"),
  }
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn seed_index(sess: &RespServerSessionVectors, key: &[u8], dims: u32) -> [u8; 56] {
  let context = sess.manager.next_vector_set_context(0).unwrap();
  sess.manager.service.create_index(
    context,
    HnswConfig::new(
      dims,
      0,
      VectorQuantType::NoQuant,
      VectorDistanceMetricType::L2,
      64,
      8,
    ),
  );
  let index = Index {
    context,
    index_ptr: 1,
    dimensions: dims,
    reduce_dims: 0,
    num_links: 8,
    build_exploration_factor: 64,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    ..Index::default()
  };
  let bytes = index.to_bytes();
  sess.manager.write_stored_index(key, &bytes);
  bytes
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VADD
#[test]
fn vadd_parse_and_defaults() {
  let sess = session();

  // 参数不足
  let r = sess.network_vadd(&[b"k", b"FP32"]);
  assert!(err_text(r).contains("wrong number of arguments"));

  // VALUES 形式 + 默认 Q8/200/16/L2（成功应答为整数 1）
  let r = sess.network_vadd(&[b"k", b"VALUES", b"2", b"1.5", b"-2.5", b"elem1", b"CAS"]);
  assert_eq!(r, VectorReply::Integer(1));
  let index = sess.read_index(b"k").unwrap();
  assert_eq!(index.dimensions, 2);
  assert_eq!(index.quant_type, VectorQuantType::Q8);
  assert_eq!(index.num_links, 16);
  assert_eq!(index.build_exploration_factor, 200);
  assert_eq!(index.distance_metric, VectorDistanceMetricType::L2);
  assert!(sess.manager.is_member(&index.to_bytes(), b"elem1"));

  // 重复元素 → 整数 0（非错误，对齐 C# Duplicate 分支）
  let r = sess.network_vadd(&[b"k", b"VALUES", b"2", b"1.5", b"-2.5", b"elem1"]);
  assert_eq!(r, VectorReply::Integer(0));

  // FP32 形式 + 全选项（新键）
  let r = sess.network_vadd(&[
    b"k2",
    b"FP32",
    &f32_bytes(&[3.0, 4.0]),
    b"elem2",
    b"NOQUANT",
    b"EF",
    b"64",
    b"SETATTR",
    b"{\"a\":1}",
    b"M",
    b"8",
    b"XDISTANCE_METRIC",
    b"COSINE",
  ]);
  assert_eq!(r, VectorReply::Integer(1));
  let index = sess.read_index(b"k2").unwrap();
  assert_eq!(index.quant_type, VectorQuantType::NoQuant);
  assert_eq!(index.num_links, 8);
  assert_eq!(index.build_exploration_factor, 64);
  assert_eq!(index.distance_metric, VectorDistanceMetricType::Cosine);
  assert_eq!(
    sess.manager.service.get_attribute(index.context, b"elem2"),
    Some(b"{\"a\":1}".to_vec())
  );

  // REDUCE + FP32：降维集合
  let r = sess.network_vadd(&[
    b"kr",
    b"REDUCE",
    b"1",
    b"FP32",
    &f32_bytes(&[3.0, 4.0]),
    b"e0",
  ]);
  assert_eq!(r, VectorReply::Integer(1));
  let index = sess.read_index(b"kr").unwrap();
  assert_eq!(index.reduce_dims, 1);

  // REDUCE 超过向量维度
  let r = sess.network_vadd(&[
    b"kr2",
    b"REDUCE",
    b"5",
    b"FP32",
    &f32_bytes(&[1.0, 2.0]),
    b"e",
  ]);
  assert_eq!(
    err_text(r),
    "ERR REDUCE dimension must be <= vector dimensions"
  );

  // X 系量化 + REDUCE → quantization mismatch（对齐 C# BadParams 空文案回落）
  let r = sess.network_vadd(&[
    b"kq",
    b"REDUCE",
    b"1",
    b"FP32",
    &f32_bytes(&[1.0, 2.0]),
    b"e",
    b"XNOQUANT_U8",
  ]);
  assert_eq!(
    err_text(r),
    "ERR asked quantization mismatch with existing vector set"
  );

  // 重复选项报错（量化 / EF / M / SETATTR / CAS / XDISTANCE_METRIC）
  let v1 = f32_bytes(&[1.0]);
  let dup_sets: Vec<Vec<&[u8]>> = vec![
    vec![b"k", b"FP32", &v1, b"x", b"NOQUANT", b"Q8"],
    vec![b"k", b"FP32", &v1, b"x", b"EF", b"8", b"EF", b"9"],
    vec![b"k", b"FP32", &v1, b"x", b"M", b"8", b"M", b"9"],
    vec![b"k", b"FP32", &v1, b"x", b"SETATTR", b"a", b"SETATTR", b"b"],
    vec![b"k", b"FP32", &v1, b"x", b"CAS", b"CAS"],
    vec![
      b"k",
      b"FP32",
      &v1,
      b"x",
      b"XDISTANCE_METRIC",
      b"L2",
      b"XDISTANCE_METRIC",
      b"COSINE",
    ],
  ];
  for dup in dup_sets {
    let r = sess.network_vadd(&dup);
    assert!(matches!(r, VectorReply::Error(_)), "重复选项应报错: {r:?}");
  }
  assert_eq!(
    err_text(sess.network_vadd(&[b"k", b"FP32", &v1, b"x", b"NOQUANT", b"Q8"])),
    "Quantization specified multiple times"
  );

  // M 越界
  let r = sess.network_vadd(&[b"k", b"FP32", &f32_bytes(&[1.0]), b"x", b"M", b"2"]);
  assert_eq!(err_text(r), "ERR M must be an integer between 4 and 4096");

  // EF 越界 + 非法度量 + 未知选项 + 元素后 REDUCE
  assert_eq!(
    err_text(sess.network_vadd(&[b"k", b"FP32", &v1, b"x", b"EF", b"0"])),
    "ERR EF must be an integer between 1 and 1000000"
  );
  assert_eq!(
    err_text(sess.network_vadd(&[b"k", b"FP32", &v1, b"x", b"XDISTANCE_METRIC", b"DOT"])),
    "ERR invalid XDISTANCE_METRIC"
  );
  assert_eq!(
    err_text(sess.network_vadd(&[b"k", b"FP32", &v1, b"x", b"WHAT"])),
    "ERR invalid option after element"
  );
  assert_eq!(
    err_text(sess.network_vadd(&[b"k", b"FP32", &v1, b"x", b"REDUCE", b"2"])),
    "ERR invalid option after element"
  );

  // 向量格式非法 + 空键
  assert_eq!(
    err_text(sess.network_vadd(&[b"k", b"FP32", b"123", b"x"])),
    "ERR invalid vector specification"
  );
  assert_eq!(
    err_text(sess.network_vadd(&[b"", b"FP32", &v1, b"x"])),
    "ERR Vector Set key cannot be empty"
  );

  // 严格整数：前导零拒绝（C# allowLeadingZeros: false）
  let r = sess.network_vadd(&[b"k", b"VALUES", b"02", b"1.0", b"2.0", b"e"]);
  assert_eq!(err_text(r), "ERR invalid vector specification");
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VSIM
#[test]
fn vsim_options_and_output() {
  let sess = session();

  sess
    .manager
    .try_add(
      b"vs",
      &seed_index(&sess, b"vs", 2),
      &VectorAddArgs::new(
        b"near",
        VectorValueType::FP32,
        &f32_bytes(&[1.0, 0.0]),
        b"{\"n\":1}",
      ),
    )
    .unwrap();
  sess
    .manager
    .try_add(
      b"vs",
      &sess.manager.read_stored_index(b"vs").unwrap(),
      &VectorAddArgs::new(
        b"far",
        VectorValueType::FP32,
        &f32_bytes(&[9.0, 9.0]),
        b"{\"n\":2}",
      ),
    )
    .unwrap();

  // RESP2 + WITHSCORES（id/score 交错扁平数组）
  let r = sess.network_vsim(&[
    b"vs",
    b"FP32",
    &f32_bytes(&[1.0, 0.0]),
    b"WITHSCORES",
    b"COUNT",
    b"2",
  ]);
  let mut encoded = Vec::new();
  r.encode_resp2(&mut encoded);
  let text = s(&encoded);
  assert!(text.starts_with("*4\r\n"), "扁平数组 id/score 成对: {text}");

  // RESP3 + WITHSCORES → map（id → distance）
  let r3 = sess.network_vsim_impl(
    &[
      b"vs",
      b"FP32",
      &f32_bytes(&[1.0, 0.0]),
      b"WITHSCORES",
      b"COUNT",
      b"2",
    ],
    true,
  );
  let mut encoded3 = Vec::new();
  r3.encode_resp3(&mut encoded3);
  let text3 = s(&encoded3);
  assert!(text3.starts_with("%2\r\n"), "RESP3 map 头: {text3}");

  // RESP3 无选项 → 普通数组
  let r3 = sess.network_vsim_impl(&[b"vs", b"ELE", b"near", b"COUNT", b"1"], true);
  let mut encoded3 = Vec::new();
  r3.encode_resp3(&mut encoded3);
  assert!(
    s(&encoded3).starts_with("*1\r\n"),
    "RESP3 普通数组: {}",
    s(&encoded3)
  );

  // FILTER 过滤 far
  let r = sess.network_vsim(&[
    b"vs",
    b"FP32",
    &f32_bytes(&[1.0, 0.0]),
    b"FILTER",
    b".n > 1",
    b"COUNT",
    b"2",
  ]);
  let mut encoded = Vec::new();
  r.encode_resp2(&mut encoded);
  let text = s(&encoded);
  assert!(text.contains("far") && !text.contains("near"));

  // ELE 形式
  let r = sess.network_vsim(&[b"vs", b"ELE", b"near", b"COUNT", b"1"]);
  let mut encoded = Vec::new();
  r.encode_resp2(&mut encoded);
  assert!(s(&encoded).contains("near"));

  // COUNT 重复
  let r = sess.network_vsim(&[
    b"vs",
    b"FP32",
    &f32_bytes(&[1.0, 0.0]),
    b"COUNT",
    b"1",
    b"COUNT",
    b"2",
  ]);
  assert_eq!(err_text(r), "COUNT specified multiple times");

  // 未知选项（对齐 C# "Unknown option"）
  assert_eq!(
    err_text(sess.network_vsim(&[b"vs", b"ELE", b"near", b"WHAT"])),
    "Unknown option"
  );

  // 非法 kind（对齐 C# "VSIM expected ELE, FP32, or VALUES"）
  assert_eq!(
    err_text(sess.network_vsim(&[b"vs", b"WHAT", b"x"])),
    "VSIM expected ELE, FP32, or VALUES"
  );

  // VALUES 分量非法（对齐 C# "VALUES value must be valid float"）
  assert_eq!(
    err_text(sess.network_vsim(&[b"vs", b"VALUES", b"2", b"1.0", b"abc"])),
    "VALUES value must be valid float"
  );

  // COUNT 选项缺值 → 参数数量错误（对齐 C# AbortWithWrongNumberOfArguments）
  assert!(
    err_text(sess.network_vsim(&[b"vs", b"ELE", b"near", b"COUNT"]))
      .contains("wrong number of arguments")
  );

  // 元素不在集合中 → "Element not in Vector Set"
  assert_eq!(
    err_text(sess.network_vsim(&[b"vs", b"ELE", b"ghost"])),
    "Element not in Vector Set"
  );

  // 缺键 → 空数组（对齐 C# NOTFOUND → TryWriteEmptyArray）
  assert_eq!(
    sess.network_vsim(&[b"nope", b"ELE", b"x"]),
    VectorReply::Array(Vec::new())
  );

  // EPSILON / FILTER-EF 解析与越界
  assert_eq!(
    err_text(sess.network_vsim(&[b"vs", b"ELE", b"near", b"EPSILON", b"-1"])),
    "EPSILON must be float > 0"
  );
  assert_eq!(
    err_text(sess.network_vsim(&[b"vs", b"ELE", b"near", b"FILTER-EF", b"3"])),
    "ERR FILTER-EF must be an integer between 4 and 256"
  );
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VEMB_FP32Storage
#[test]
fn auxiliary_commands() {
  let sess = session();
  sess
    .manager
    .try_add(
      b"aux",
      &seed_index(&sess, b"aux", 2),
      &VectorAddArgs::new(
        b"e1",
        VectorValueType::FP32,
        &f32_bytes(&[5.0, 6.0]),
        b"{\"tag\":\"x\"}",
      ),
    )
    .unwrap();
  sess
    .manager
    .try_add(
      b"aux",
      &sess.manager.read_stored_index(b"aux").unwrap(),
      &VectorAddArgs::new(b"e2", VectorValueType::FP32, &f32_bytes(&[1.0, 1.0]), b""),
    )
    .unwrap();

  assert_eq!(sess.network_vcard(&[b"aux"]), VectorReply::Integer(2));
  assert_eq!(sess.network_vdim(&[b"aux"]), VectorReply::Integer(2));
  assert_eq!(
    sess.network_vismember(&[b"aux", b"e1"]),
    VectorReply::Integer(1)
  );
  assert_eq!(
    sess.network_vismember(&[b"aux", b"zz"]),
    VectorReply::Integer(0)
  );
  assert_eq!(
    sess.network_vismember_impl(&[b"aux", b"e1"], true),
    VectorReply::Boolean(true)
  );

  // VEMB / VEMB RAW（NoQuant → fp32 通道）
  let emb = sess.network_vemb(&[b"aux", b"e1"]);
  let mut encoded = Vec::new();
  emb.encode_resp2(&mut encoded);
  let text = s(&encoded);
  assert!(text.contains('5'), "嵌入值包含 5: {text}");

  // VEMB RAW：NoQuant → [fp32, 原始 f32 字节, 范数]（原始字节非 UTF-8，按结构断言）
  let raw = sess.network_vemb(&[b"aux", b"e1", b"RAW"]);
  let VectorReply::Array(items) = raw else {
    panic!("RAW 应为数组: {raw:?}");
  };
  assert_eq!(items.len(), 3);
  assert_eq!(items[0], VectorReply::Simple(b"fp32".to_vec()));
  assert_eq!(items[1], VectorReply::Bulk(Some(f32_bytes(&[5.0, 6.0]))));

  // VEMB 非法选项
  assert_eq!(
    err_text(sess.network_vemb(&[b"aux", b"e1", b"BAD"])),
    "Unexpected option to VEMB"
  );
  // VEMB 缺失元素 → 空数组（对齐 C# NOTFOUND）
  assert_eq!(
    sess.network_vemb(&[b"aux", b"ghost"]),
    VectorReply::Array(Vec::new())
  );

  // VGETATTR / VSETATTR（成功 1 / 缺失 0，非错误）
  assert_eq!(
    sess.network_vgetattr(&[b"aux", b"e1"]),
    VectorReply::Bulk(Some(b"{\"tag\":\"x\"}".to_vec()))
  );
  assert_eq!(
    sess.network_vsetattr(&[b"aux", b"e1", b"{}"]),
    VectorReply::Integer(1)
  );
  assert_eq!(
    sess.network_vsetattr(&[b"aux", b"ghost", b"{}"]),
    VectorReply::Integer(0)
  );
  assert_eq!(
    sess.network_vgetattr(&[b"aux", b"e1"]),
    VectorReply::Bulk(Some(b"{}".to_vec()))
  );

  // VLINKS（缺失元素 → null）
  assert!(matches!(
    sess.network_vlinks(&[b"aux", b"e1"]),
    VectorReply::Array(_)
  ));
  assert_eq!(
    sess.network_vlinks(&[b"aux", b"ghost"]),
    VectorReply::Bulk(None)
  );
  assert_eq!(
    err_text(sess.network_vlinks(&[b"aux", b"e1", b"BAD"])),
    "ERR Unexpected option"
  );

  // VRANDMEMBER
  assert!(matches!(
    sess.network_vrandmember(&[b"aux", b"2"]),
    VectorReply::Array(_)
  ));
  assert_eq!(
    err_text(sess.network_vrandmember(&[b"aux", b"x"])),
    "ERR expected integer count"
  );
  assert_eq!(
    sess.network_vrandmember(&[b"ghost", b"2"]),
    VectorReply::Array(Vec::new())
  );
  assert_eq!(
    sess.network_vrandmember(&[b"ghost"]),
    VectorReply::Bulk(None)
  );

  // VINFO（对齐 C# 布局：14 项 + 小写枚举名）
  let info = sess.network_vinfo(&[b"aux"]);
  let mut encoded = Vec::new();
  info.encode_resp2(&mut encoded);
  let text = s(&encoded);
  assert!(text.starts_with("*14\r\n"), "VINFO 14 项: {text}");
  assert!(text.contains("input-vector-dimensions"));
  assert!(text.contains("reduced-dimensions"));
  assert!(text.contains("f32"));
  assert!(text.contains("l2"));
  assert_eq!(sess.network_vinfo(&[b"ghost"]), VectorReply::NullArray);

  // VREM
  assert_eq!(sess.network_vrem(&[b"aux", b"e2"]), VectorReply::Integer(1));
  assert_eq!(sess.network_vrem(&[b"aux", b"e2"]), VectorReply::Integer(0));
  assert_eq!(sess.network_vcard(&[b"aux"]), VectorReply::Integer(1));

  // VDIM 缺键 → "ERR Key not found"
  assert_eq!(
    err_text(sess.network_vdim(&[b"ghost"])),
    "ERR Key not found"
  );
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:DisabledWithFeatureFlag
#[test]
fn disabled_and_reply_encoding() {
  let manager = Arc::new(VectorManager::new(VectorManagerOptions {
    is_enabled: false,
    ..Default::default()
  }));
  let sess = RespServerSessionVectors::new(manager);

  for r in [
    sess.network_vadd(&[b"k", b"FP32", &[0; 4], b"e"]),
    sess.network_vsim(&[b"k", b"ELE", b"e"]),
    sess.network_vcard(&[b"k"]),
    sess.network_vrem(&[b"k", b"e"]),
  ] {
    assert!(matches!(r, VectorReply::Error(_)), "未启用应拒绝");
  }

  // WRONGTYPE 文案（无句点，对齐 AbortVectorSetWrongType）
  assert!(sess.abort_vector_set_wrong_type(b"somekey").is_none());

  // RESP2/RESP3 编码差异（含 map / null array）
  let reply = VectorReply::Array(vec![
    VectorReply::Bulk(Some(b"a".to_vec())),
    VectorReply::Double(2.0),
    VectorReply::Boolean(true),
  ]);
  let mut r2 = Vec::new();
  reply.encode_resp2(&mut r2);
  assert_eq!(s(&r2), "*3\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\n1\r\n");
  let mut r3 = Vec::new();
  reply.encode_resp3(&mut r3);
  assert_eq!(s(&r3), "*3\r\n$1\r\na\r\n,2\r\n#t\r\n");

  let map = VectorReply::Map(vec![(
    VectorReply::Bulk(Some(b"id".to_vec())),
    VectorReply::Double(1.5),
  )]);
  let mut m2 = Vec::new();
  map.encode_resp2(&mut m2);
  assert_eq!(s(&m2), "*2\r\n$2\r\nid\r\n$3\r\n1.5\r\n");
  let mut m3 = Vec::new();
  map.encode_resp3(&mut m3);
  assert_eq!(s(&m3), "%1\r\n$2\r\nid\r\n,1.5\r\n");

  let mut na = Vec::new();
  VectorReply::NullArray.encode_resp2(&mut na);
  assert_eq!(s(&na), "*-1\r\n");
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VSIMWithAttributeFiltering
#[test]
fn result_writers_honor_bitmap_and_count() {
  let ids = vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()];
  let distances = [0.1f32, 0.2, 0.3];
  // 位图：a、c 通过
  let bitmap = [0b101u8];

  // RESP2：输出上限 = min(count, popcount)
  let out =
    RespServerSessionVectors::write_resp2_result(10, &ids, &distances, &bitmap, None, false);
  let mut encoded = Vec::new();
  out.encode_resp2(&mut encoded);
  assert_eq!(s(&encoded), "*2\r\n$1\r\na\r\n$1\r\nc\r\n");

  // RESP3 无 score/attr → 普通数组
  let out =
    RespServerSessionVectors::write_resp3_result(10, &ids, &distances, &bitmap, None, false);
  let mut encoded = Vec::new();
  out.encode_resp3(&mut encoded);
  assert_eq!(s(&encoded), "*2\r\n$1\r\na\r\n$1\r\nc\r\n");

  // RESP3 + WITHSCORES + WITHATTRIBS → map，值为 [score, attr] 数组
  let attrs = vec![b"x".as_slice(), b"".as_slice(), b"z".as_slice()];
  let out =
    RespServerSessionVectors::write_resp3_result(10, &ids, &distances, &bitmap, Some(&attrs), true);
  let mut encoded = Vec::new();
  out.encode_resp3(&mut encoded);
  let text = s(&encoded);
  assert!(text.starts_with("%2\r\n"), "{text}");
  assert!(
    text.contains("$1\r\na\r\n*2\r\n,0.10000000149011612\r\n$1\r\nx\r\n"),
    "{text}"
  );
  assert!(
    text.contains("$1\r\nc\r\n*2\r\n,0.30000001192092896\r\n$1\r\nz\r\n"),
    "{text}"
  );

  // 空属性在 RESP3 下写 NULL
  let attrs_one = vec![b"".as_slice()];
  let out = RespServerSessionVectors::write_resp3_result(
    10,
    &[b"e".as_slice()],
    &[0.5],
    &[],
    Some(&attrs_one),
    true,
  );
  let mut encoded = Vec::new();
  out.encode_resp3(&mut encoded);
  assert_eq!(s(&encoded), "%1\r\n$1\r\ne\r\n*2\r\n,0.5\r\n$-1\r\n");

  // count 截断
  let out = RespServerSessionVectors::write_resp2_result(1, &ids, &distances, &[], None, false);
  let mut encoded = Vec::new();
  out.encode_resp2(&mut encoded);
  assert_eq!(s(&encoded), "*1\r\n$1\r\na\r\n");
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VADDVariableLengthElementIds
#[test]
fn id_format_is_length_prefixed() {
  assert_eq!(VectorIdFormat::I32LengthPrefixed as i32, 1);
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VREM
#[test]
fn add_remove_and_query_lifecycle() {
  let manager = manager();
  manager.service.create_index(
    16,
    HnswConfig::new(
      2,
      0,
      VectorQuantType::NoQuant,
      VectorDistanceMetricType::L2,
      64,
      8,
    ),
  );
  let index = fresh_index(16, 2).to_bytes();

  // VADD
  let res = manager.try_add(
    b"set",
    &index,
    &VectorAddArgs::new(
      b"elem-a",
      VectorValueType::FP32,
      &f32_bytes(&[1.0, 1.0]),
      b"{\"k\":1}",
    ),
  );
  assert_eq!(res.unwrap(), VectorManagerResult::OK);

  // 重复 VADD → Duplicate
  let res = manager.try_add(
    b"set",
    &index,
    &VectorAddArgs::new(
      b"elem-a",
      VectorValueType::FP32,
      &f32_bytes(&[2.0, 2.0]),
      b"",
    ),
  );
  assert_eq!(res.unwrap(), VectorManagerResult::Duplicate);

  // 维度不匹配
  let res = manager.try_add(
    b"set",
    &index,
    &VectorAddArgs::new(b"elem-b", VectorValueType::FP32, &f32_bytes(&[1.0]), b""),
  );
  assert_eq!(res.unwrap_err().result, VectorManagerResult::BadParams);

  // M 不匹配
  let bad_m_bytes = f32_bytes(&[1.0, 2.0]);
  let mut bad_m = VectorAddArgs::new(b"elem-b", VectorValueType::FP32, &bad_m_bytes, b"");
  bad_m.num_links = 4;
  let res = manager.try_add(b"set", &index, &bad_m);
  assert_eq!(
    res.unwrap_err().message,
    b"ERR asked M value mismatch with existing vector set".to_vec()
  );

  // 成员判定 / 属性 / 嵌入
  assert!(manager.is_member(&index, b"elem-a"));
  assert!(!manager.is_member(&index, b"elem-z"));
  assert_eq!(
    manager.fetch_single_vector_element_attributes(&index, b"elem-a"),
    VectorManagerResult::OK
  );

  let emb = manager.try_get_embedding(&index, b"elem-a").unwrap();
  assert_eq!(emb, vec![1.0, 1.0]);

  let (raw, quant, norm, range) = manager.try_get_raw_embedding(&index, b"elem-a").unwrap();
  assert_eq!(quant, VectorQuantType::NoQuant);
  assert!((norm - 1.0).abs() < f64::EPSILON);
  assert!(range.is_none());
  assert_eq!(raw, f32_bytes(&[1.0, 1.0]));

  // VREM
  assert_eq!(
    manager.try_remove(&index, b"elem-a"),
    VectorManagerResult::OK
  );
  assert_eq!(
    manager.try_remove(&index, b"elem-a"),
    VectorManagerResult::MissingElement
  );
  assert!(!manager.is_member(&index, b"elem-a"));
  assert_eq!(manager.try_get_embedding(&index, b"elem-a"), None);
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VSIMWithAdvancedFiltering
#[test]
fn value_similarity_with_post_filter() {
  let manager = manager();
  manager.service.create_index(
    32,
    HnswConfig::new(
      2,
      0,
      VectorQuantType::NoQuant,
      VectorDistanceMetricType::L2,
      64,
      8,
    ),
  );
  let index = fresh_index(32, 2).to_bytes();

  for (name, v, attr) in [
    ("near", vec![1.0, 0.0], "{\"n\": 1}"),
    ("far", vec![5.0, 5.0], "{\"n\": 100}"),
    ("mid", vec![2.0, 0.0], "{\"n\": 5}"),
  ] {
    manager
      .try_add(
        b"set",
        &index,
        &VectorAddArgs::new(
          name.as_bytes(),
          VectorValueType::FP32,
          &f32_bytes(&v),
          attr.as_bytes(),
        ),
      )
      .unwrap();
  }

  // 无过滤：返回按距离排序的全部
  let out = manager
    .value_similarity(
      &index,
      VectorValueType::FP32,
      &f32_bytes(&[1.0, 0.0]),
      &VectorSearchOptions {
        count: 3,
        search_exploration_factor: 32,
        include_attributes: true,
        ..Default::default()
      },
    )
    .unwrap();
  assert_eq!(out.found, 3);
  assert_eq!(out.id_format, VectorIdFormat::I32LengthPrefixed);
  assert!(out.output_distances[0] < out.output_distances[2]);

  // EPSILON：距离上限截断（near 距离 0，mid 距离 1，far 距离 32）
  let out = manager
    .value_similarity(
      &index,
      VectorValueType::FP32,
      &f32_bytes(&[1.0, 0.0]),
      &VectorSearchOptions {
        count: 3,
        search_exploration_factor: 32,
        delta: 2.0,
        ..Default::default()
      },
    )
    .unwrap();
  assert_eq!(out.found, 2);
  let ids: Vec<&[u8]> = unpack_length_prefixed(&out.output_ids);
  assert!(ids.contains(&b"near".as_slice()) && ids.contains(&b"mid".as_slice()));

  // 过滤 .n > 1：near 被排除
  let out = manager
    .value_similarity(
      &index,
      VectorValueType::FP32,
      &f32_bytes(&[1.0, 0.0]),
      &VectorSearchOptions {
        count: 3,
        search_exploration_factor: 32,
        filter: b".n > 1",
        max_filtering_effort: 16,
        include_attributes: true,
        ..Default::default()
      },
    )
    .unwrap();
  assert_eq!(out.found, 2);
  let ids: Vec<&[u8]> = unpack_length_prefixed(&out.output_ids);
  assert!(ids.contains(&b"far".as_slice()) && ids.contains(&b"mid".as_slice()));
  let passed = out
    .filter_bitmap
    .iter()
    .map(|b| b.count_ones())
    .sum::<u32>();
  assert_eq!(passed, 2);

  // 过滤编译失败
  let out = manager.value_similarity(
    &index,
    VectorValueType::FP32,
    &f32_bytes(&[1.0, 0.0]),
    &VectorSearchOptions {
      count: 3,
      search_exploration_factor: 32,
      filter: b".n > >",
      max_filtering_effort: 16,
      ..Default::default()
    },
  );
  assert_eq!(out.unwrap_err().message, b"ERR Compiling filter failed");
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VSIMWithAttribs
#[test]
fn element_similarity_and_attributes_batch() {
  let manager = manager();
  manager.service.create_index(
    48,
    HnswConfig::new(
      1,
      0,
      VectorQuantType::NoQuant,
      VectorDistanceMetricType::L2,
      64,
      8,
    ),
  );
  let index = fresh_index(48, 1).to_bytes();

  manager
    .try_add(
      b"set",
      &index,
      &VectorAddArgs::new(
        b"x",
        VectorValueType::FP32,
        &f32_bytes(&[0.0]),
        b"{\"a\":1}",
      ),
    )
    .unwrap();
  manager
    .try_add(
      b"set",
      &index,
      &VectorAddArgs::new(b"y", VectorValueType::FP32, &f32_bytes(&[10.0]), b""),
    )
    .unwrap();

  let out = manager
    .element_similarity(
      &index,
      b"x",
      &VectorSearchOptions {
        count: 2,
        search_exploration_factor: 32,
        ..Default::default()
      },
    )
    .unwrap();
  assert_eq!(out.found, 2);

  // 缺失元素 → MissingElement + "Element not in Vector Set"（对齐 C# 出参文案）
  let err = manager
    .element_similarity(
      &index,
      b"ghost",
      &VectorSearchOptions {
        count: 2,
        search_exploration_factor: 32,
        ..Default::default()
      },
    )
    .unwrap_err();
  assert_eq!(err.result, VectorManagerResult::MissingElement);
  assert_eq!(err.message, b"Element not in Vector Set".to_vec());

  // 批量属性
  let mut ids = Vec::new();
  ids.extend_from_slice(&1i32.to_le_bytes());
  ids.extend_from_slice(b"x");
  ids.extend_from_slice(&1i32.to_le_bytes());
  ids.extend_from_slice(b"y");
  let attrs = manager.fetch_vector_element_attributes(48, &ids);
  let parsed = unpack_length_prefixed(&attrs);
  assert_eq!(parsed[0], b"{\"a\":1}".as_slice());
  assert_eq!(parsed[1], b"".as_slice());

  // VSETATTR
  assert!(manager.try_set_attribute(&index, b"y", b"{\"b\":2}"));
  assert!(!manager.try_set_attribute(&index, b"none", b"{}"));
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:DeleteVectorSet
#[test]
fn request_deletion_respects_suppress_cleanup() {
  let manager = manager();
  let mut index = fresh_index(64, 2);
  manager.service.create_index(
    64,
    HnswConfig::new(
      2,
      0,
      VectorQuantType::NoQuant,
      VectorDistanceMetricType::L2,
      16,
      4,
    ),
  );
  assert_eq!(
    manager
      .service
      .insert(64, b"e", &f32_bytes(&[0.0, 0.0]), b""),
    DiskAnnInsertResult::True
  );

  // 正常删除：发布清理请求 + 丢弃索引
  manager.request_deletion(&index.to_bytes());
  assert!(manager.request_cleanup_task_channel.has_pending());
  assert_eq!(manager.request_cleanup_task_channel.try_read(), Some(64));
  assert_eq!(manager.service.card(64), 0);

  // SuppressCleanup：删除被忽略
  manager.service.create_index(
    66,
    HnswConfig::new(
      2,
      0,
      VectorQuantType::NoQuant,
      VectorDistanceMetricType::L2,
      16,
      4,
    ),
  );
  assert_eq!(
    manager
      .service
      .insert(66, b"e", &f32_bytes(&[0.0, 0.0]), b""),
    DiskAnnInsertResult::True
  );
  index.context = 66;
  index.flags = VectorSetFlags::SUPPRESS_CLEANUP;
  manager.request_deletion(&index.to_bytes());
  assert!(!manager.request_cleanup_task_channel.has_pending());
  assert_eq!(manager.service.card(66), 1);

  // 尺寸不符：忽略
  manager.request_deletion(&[0u8; 10]);
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:RepeatedVectorSetDeletes
#[test]
fn drop_in_memory_index_flow() {
  let manager = manager();
  manager.service.create_index(
    80,
    HnswConfig::new(
      2,
      0,
      VectorQuantType::NoQuant,
      VectorDistanceMetricType::L2,
      16,
      4,
    ),
  );
  assert_eq!(
    manager
      .service
      .insert(80, b"e", &f32_bytes(&[0.0, 0.0]), b""),
    DiskAnnInsertResult::True
  );

  let mut index = fresh_index(80, 2);
  let key = b"dropkey".to_vec();

  // 请求丢弃：登记 + 信号
  manager.request_drop_in_memory_index(&key, &index.to_bytes());
  assert!(manager.requested_drops.lock().contains_key(&key));
  assert!(manager.request_drop_task_channel.has_pending());

  // 重复请求被拒
  manager.request_drop_in_memory_index(&key, &index.to_bytes());
  assert_eq!(manager.requested_drops.lock().len(), 1);

  // SuppressCleanup 忽略
  index.flags = VectorSetFlags::SUPPRESS_CLEANUP;
  manager.request_drop_in_memory_index(b"other", &index.to_bytes());
  assert!(
    !manager
      .requested_drops
      .lock()
      .contains_key(b"other".as_slice())
  );

  // 直接丢弃
  assert_eq!(manager.service.card(80), 1);
  manager.drop_in_memory_index(&index.to_bytes());
  assert_eq!(manager.service.card(80), 0);
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:RecreateIndexesOnRestoreAsync
#[test]
fn recovery_reconciliation_flow() {
  let manager = manager();

  let ctx = manager.next_vector_set_context(1).unwrap();

  let record = fresh_index(ctx, 4).to_bytes();
  manager.recovered_vector_set_index_key(&record);

  let meta_bytes = manager.context_metadatas.lock()[0].to_bytes();
  assert!(manager.recovered_context_metadata(&0i32.to_le_bytes(), &meta_bytes));
  assert!(!manager.recovered_context_metadata(&0i32.to_le_bytes(), &meta_bytes));
  assert!(manager.recovered_context_metadata(&0i32.to_le_bytes(), &[0u8; 8]));
  assert!(manager.recovered_context_metadata(&[0u8; 8], &meta_bytes));

  assert!(manager.reconcile_recovered_state(false));

  manager.request_deletion(&[0u8; 3]);
  manager.recovered_vector_set_index_key(&[0u8; 3]);

  let manager2 = VectorManager::new(VectorManagerOptions {
    is_enabled: true,
    ..Default::default()
  });
  let _ = manager2.next_vector_set_context(0);
  assert!(!manager2.reconcile_recovered_state(true));
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:HideInternalRecordsAsync
#[test]
fn sanitize_ingested_records() {
  let manager = manager();

  let mut meta = [0u8; CONTEXT_METADATA_SIZE];
  assert!(manager.sanitize_and_track_ingested_record_if_applicable(
    false,
    Some(&[METADATA_NAMESPACE]),
    0,
    &0i32.to_le_bytes(),
    &mut meta,
  ));

  let mut value = fresh_index(96, 2).to_bytes();
  assert!(manager.sanitize_and_track_ingested_record_if_applicable(
    false,
    None,
    RECORD_TYPE,
    b"k",
    &mut value
  ));
  assert!(manager.recovered_indexes.lock().contains_key(&96));
  assert_eq!(u64::from_le_bytes(value[8..16].try_into().unwrap()), 0);

  let mut value2 = fresh_index(98, 2).to_bytes();
  assert!(manager.sanitize_and_track_ingested_record_if_applicable(
    true,
    None,
    RECORD_TYPE,
    b"k",
    &mut value2
  ));
  assert!(!manager.recovered_indexes.lock().contains_key(&98));
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VectorSetOpacity
#[test]
fn storage_session_assertion() {
  let manager = manager();
  manager.assert_have_storage_session();
  assert!(VectorManager::index_has_suppress_cleanup(&Index {
    flags: VectorSetFlags::SUPPRESS_CLEANUP,
    ..Index::default()
  }));
}
