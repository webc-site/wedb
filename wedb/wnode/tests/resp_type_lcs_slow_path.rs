//! 端到端集成测试：TYPE / LCS 慢路径分派臂（落盘冷键降级承接）
//!
//! 分派表缺 C::Type / C::Lcs 臂时，落盘冷键上的 TYPE / LCS 会把内部哨兵
//! RESP_ERR_ASYNC_REQUIRED（"ERR command requires asynchronous completion"）
//! 直泄客户端——C# NetworkTYPE / NetworkLCS 经 Read_UnifiedStore pending 就地
//! CompletePendingForUnifiedStoreSession 闭环，终态恒类型名 / 结果帧 /
//! WRONGTYPE，零该错误形态。本测试以快路径热键应答为字节基准，flush_and_evict
//! 冷化后断言慢路径应答逐字节一致（对标 garnet/test 的 TYPE / LCS 用例族）
use std::{iter::once, sync::Arc};

use compio::runtime::Runtime;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
    slow_path::SlowWait,
  },
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wnode_test::pump;
use wresp::command::RespCommand;
use wtest_base::test_store_config;

/// 冷键用例（RespCommand + 协议命令名 + 参数）；热基准与冷重放共用同一组
type Case = (RespCommand, &'static [u8], &'static [&'static [u8]]);

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭），
/// 同时保留慢路径直答句柄与存储句柄（flush_and_evict 冷化构造磁盘候选）
fn consumer_with_store() -> (
  RespSessionConsumer,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
) {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("type_lcs.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let session = store.new_session().unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(session));
  (
    RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone()),
    api,
    store,
  )
}

/// 组 RESP 请求数组帧（cmd + args）
fn frame_of(cmd: &[u8], args: &[&[u8]]) -> Vec<u8> {
  let mut frame = format!("*{}\r\n", args.len() + 1).into_bytes();
  for token in once(cmd).chain(args.iter().copied()) {
    frame.extend_from_slice(format!("${}\r\n", token.len()).as_bytes());
    frame.extend_from_slice(token);
    frame.extend_from_slice(b"\r\n");
  }
  frame
}

/// 单命令往返（同步快路径）
fn roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// 冷键降级往返：快路径同步段必须挂起 SlowWait（缺失即未降级，基准失效），
/// block_on 承担网络泵角色 await 慢路径后拼回应答
fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  let slow = c
    .take_slow_wait()
    .expect("冷键快路径应降级挂起 SlowWait，字节基准失效");
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// 慢路径直答（不经会话快路径降级快照；解析失败面与不可降级面同径）
fn slow_reply(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  rt.block_on(async {
    SlowWait::for_command(
      api,
      cmd,
      args.iter().map(|a| a.to_vec()).collect(),
      wconf::DEFAULT_RESP_VERSION,
    )
    .resolve()
    .await
  })
}

/// 冷键 TYPE / LCS 慢路径应答与热键快路径逐字节一致（修复前内部哨兵外泄）
#[test]
fn type_lcs_cold_key_slow_path_matches_fast() {
  let rt = Runtime::new().unwrap();
  let (mut c, api, store) = consumer_with_store();

  // 键布局：LCS 值对（公共子序列 abc）、hash 对象键（信封域）、
  // RI 键（升阶 Meta 域）、直写过期的 TTL 键（过期裁决面）
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SET", &[b"k1", b"abcdef"])),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SET", &[b"k2", b"abcxyz"])),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"HSET", &[b"hk", b"f", b"v"])),
    b":1\r\n"
  );
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      &frame_of(b"RI.CREATE", &[b"rix", b"MEMORY", b"CACHESIZE", b"65536"])
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"SET", &[b"ek", b"v"])),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, &frame_of(b"PEXPIRE", &[b"ek", b"100"])),
    b":1\r\n"
  );
  // RESP 面无法自然构造「过期未清」态：put_ttl_sync 裸写内核原位覆写
  //（同步 TTL 门在内存对过期记录就地裁决视同缺失，过期面须叠加磁盘候选
  // 才真正降级——冷化后 TYPE ek / LCS ek k2 走慢路径惰性清除）
  {
    let probe = store.new_session().unwrap();
    let batch = probe.enter_batch();
    put_ttl_sync(&batch, b"ek", now_ticks() - TICKS_PER_SECOND).unwrap();
  }

  // 降级用例组（冷化后快路径读降级挂 SlowWait）：TYPE 覆盖 string /
  // 信封 hash / 升阶 Meta rangeindex / 过期 none 四域；LCS 覆盖默认/
  // LEN/IDX/WITHMATCHLEN+MINMATCHLEN/WRONGTYPE/缺键/过期空串七应答形态
  let degrade_cases: &[Case] = &[
    (RespCommand::Type, b"TYPE", &[b"k1"]),
    (RespCommand::Type, b"TYPE", &[b"hk"]),
    (RespCommand::Type, b"TYPE", &[b"rix"]),
    (RespCommand::Type, b"TYPE", &[b"ek"]),
    (RespCommand::Lcs, b"LCS", &[b"k1", b"k2"]),
    (RespCommand::Lcs, b"LCS", &[b"k1", b"k2", b"LEN"]),
    (RespCommand::Lcs, b"LCS", &[b"k1", b"k2", b"IDX"]),
    (
      RespCommand::Lcs,
      b"LCS",
      &[b"k1", b"k2", b"IDX", b"WITHMATCHLEN", b"MINMATCHLEN", b"2"],
    ),
    (RespCommand::Lcs, b"LCS", &[b"k1", b"hk"]),
    (RespCommand::Lcs, b"LCS", &[b"k1", b"miss"]),
    (RespCommand::Lcs, b"LCS", &[b"ek", b"k2"]),
  ];

  // 冷化前快路径字节基准（过期键 ek 的同步 TTL 门就地裁决视同缺失，
  // 基准值与冷化后慢路径期望天然同值）
  let baseline: Vec<(Vec<u8>, Vec<u8>)> = degrade_cases
    .iter()
    .map(|(_, name, args)| {
      let frame = frame_of(name, args);
      let hot = roundtrip(&mut c, &frame);
      (frame, hot)
    })
    .collect();

  // 冷化落盘：内存记录全部驱逐为磁盘候选 → 快路径三域读/双键读降级
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 逐用例冷键重放：慢路径应答必须与热键快路径逐字节一致
  //（修复前 TYPE/LCS 落兜底臂回内部哨兵 "ERR command requires
  // asynchronous completion"，本断言即失败）
  for ((_, name, args), (frame, want)) in degrade_cases.iter().zip(&baseline) {
    assert_eq!(
      slow_roundtrip(&rt, &mut c, frame),
      *want,
      "冷键慢路径应答与热键快路径不一致: {name:?} {args:?}"
    );
  }

  // 确证缺键无降级面（三域皆缺快路径直答，不经降级），直答慢路径对位
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Type, &[b"miss"]),
    b"+none\r\n",
    "缺失键慢路径 TYPE 应答 none"
  );
  assert_eq!(
    slow_reply(&rt, &api, RespCommand::Lcs, &[b"miss1", b"miss2"]),
    b"$0\r\n\r\n",
    "缺失键慢路径 LCS 应答空串"
  );

  // 纯解析失败面不触达存储（LEN+IDX 互斥 / 未知选项 / MINMATCHLEN 非整数 /
  // 缺参）：慢直答与热快路径逐字节一致（选项解析单源 parse_lcs_options）
  let parse_cases: &[Case] = &[
    (RespCommand::Lcs, b"LCS", &[b"k1", b"k2", b"LEN", b"IDX"]),
    (RespCommand::Lcs, b"LCS", &[b"k1", b"k2", b"BAD"]),
    (
      RespCommand::Lcs,
      b"LCS",
      &[b"k1", b"k2", b"MINMATCHLEN", b"abc"],
    ),
    (RespCommand::Lcs, b"LCS", &[b"k1", b"k2", b"MINMATCHLEN"]),
  ];
  for (cmd, name, args) in parse_cases {
    let frame = frame_of(name, args);
    let hot = roundtrip(&mut c, &frame_of(name, args));
    assert_eq!(
      slow_reply(&rt, &api, *cmd, args),
      hot,
      "解析失败面慢直答应与快路径一致: {frame:?}"
    );
  }
}
