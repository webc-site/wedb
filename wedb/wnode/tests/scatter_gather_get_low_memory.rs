//! 低内存（磁盘后备）形态 scatter-gather GET 行为对位（对标
//! garnet/test/standalone/Garnet.test/RespGetLowMemoryTests.cs）
//!
//! 四锚（行为值断言，旋钮只改批处理路径、不改应答字节）：
//! - ScatterGatherGet(:37)：批量写后大规模重复单 GET，SG 开/关两态逐键同值
//! - ScatterGatherMixedGetSetPipeline(:71)：每条 GET 紧随一条 SET，SG 前瞻
//!   嗅到非 GET 即拒绝跨命令批处理，交错应答与下发序逐字节一致
//! - ScatterGatherMixedGetTtlPipeline(:100)：GET 后紧随同形 3 参 TTL 不得
//!   误批为 GET，TTL 落自身处理臂回剩余秒数 / -1
//! - ScatterGatherMGet(:140) + RespMGetLowMemoryNoSgTests(:221)：MGET 全命中
//!   与混缺失两形态和逐键单 GET 同答；MGET 不依赖 SgGet 旋钮，冷盘批量读
//!   经慢路径照常完整完成

use core::str::from_utf8;
use std::sync::Arc;

use wconf::{RuntimeServerConfig, ServerConfigType};
use wdev::SegmentedDevice;
use wnode::resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSession};
use wnode_test::{bulk_bytes as bulk, test_env};
use wtest_base::resp_frame;

/// 装配指定 SgGet 旋钮态的测试会话（存储会话另行预热与驱动）
fn session_with_sg(
  sg: bool,
) -> (
  tempfile::TempDir,
  wkv::StoreSession<SegmentedDevice>,
  RespServerSession,
) {
  let (dir, session, mut resp) = test_env(false);
  resp.runtime_config = Arc::new(RuntimeServerConfig::with_defaults());
  if !sg {
    resp
      .runtime_config
      .try_set(ServerConfigType::SgGet, "no")
      .unwrap();
    assert!(!resp.runtime_config.get_bool(ServerConfigType::SgGet));
  }
  (dir, session, resp)
}

/// 批量写入口预热键值（batch 随函数退出收敛）
fn seed(store: &wkv::StoreSession<SegmentedDevice>, kvs: &[(Vec<u8>, Vec<u8>)]) {
  let batch = store.enter_batch();
  for (k, v) in kvs {
    batch.try_upsert_sync(k, v).unwrap().unwrap();
  }
}

/// 投喂整段流水线字节并复位游标
fn feed(s: &mut RespServerSession, bytes: &[u8]) {
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(bytes);
  s.bytes_read = bytes.len();
  s.read_head = 0;
  s.end_read_head = 0;
}

/// 泵驱整条流水线至消化完毕：同步段应答与挂起慢路径应答按流水线序聚合
async fn pump_all(s: &mut RespServerSession) -> Vec<u8> {
  let mut out = Vec::new();
  loop {
    let remaining = s.try_consume_messages();
    assert!(remaining.is_some(), "协议违规断流");
    s.take_output_into(&mut out, true);
    let mut had_slow = false;
    if let Some(reply) = s.take_slow_wait() {
      out.extend_from_slice(&reply.resolve().await);
      had_slow = true;
    }
    if remaining == Some(0) && !had_slow {
      return out;
    }
  }
}

/// nil 应答帧（RESP2 GET/MGET 缺键形态）
const NIL: &[u8] = b"$-1\r\n";

/// 十进制键/值字节（garnet 各用例 i.ToString() 同形）
fn fmt(i: usize) -> Vec<u8> {
  i.to_string().into_bytes()
}

/// 对标 RespGetLowMemoryTests.ScatterGatherGet(:37)：批量写入 30 键后大规模
/// 重复单 GET（步长 7 与键数互素的确定性抽样替代 C# 固定种子随机），SG 开/
/// 关两态逐键同值
#[compio::test]
async fn scatter_gather_get_values_match() -> aok::Result<()> {
  const N: usize = 30;
  for sg in [true, false] {
    let (dir, store, mut s) = session_with_sg(sg);
    let kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..N).map(|i| (fmt(i), fmt(i))).collect();
    seed(&store, &kvs);
    s.set_garnet_api(Arc::new(StoreGarnetApi::new(store)));

    let picks: Vec<usize> = (0..4 * N).map(|j| (j * 7 + 3) % N).collect();
    let mut req = Vec::new();
    let mut expected = Vec::new();
    for &i in &picks {
      let k = fmt(i);
      req.extend(resp_frame(&[b"GET", k.as_slice()]));
      expected.extend(bulk(k.as_slice()));
    }
    feed(&mut s, &req);
    assert_eq!(pump_all(&mut s).await, expected, "sg={sg} 值须逐键对答");
    drop(dir);
  }
  aok::OK
}

/// 对标 ScatterGatherMixedGetSetPipeline(:71)：每条 GET 紧随一条 SET，SG 前瞻
/// 拒绝跨 SET 批处理，交错应答与下发序逐字节一致；SET 侧 MGET 全量回读落库
#[compio::test]
async fn scatter_gather_mixed_get_set_pipeline() -> aok::Result<()> {
  const N: u32 = 100;
  let (dir, store, mut s) = session_with_sg(true);
  let kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..N)
    .map(|i| (format!("r{i}").into_bytes(), format!("val{i}").into_bytes()))
    .collect();
  seed(&store, &kvs);
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store)));

  let mut req = Vec::new();
  let mut expected = Vec::new();
  for i in 0..N {
    let rk = format!("r{i}");
    let (wk, wv) = (format!("w{i}"), format!("x{i}"));
    req.extend(resp_frame(&[b"GET", rk.as_bytes()]));
    req.extend(resp_frame(&[b"SET", wk.as_bytes(), wv.as_bytes()]));
    expected.extend(bulk(format!("val{i}").as_bytes()));
    expected.extend_from_slice(b"+OK\r\n");
  }
  feed(&mut s, &req);
  assert_eq!(pump_all(&mut s).await, expected, "交错应答须与下发序一致");

  // 写侧真落库：MGET w0..w99 全量回读
  let writes: Vec<Vec<u8>> = (0..N).map(|i| format!("w{i}").into_bytes()).collect();
  let mut args: Vec<&[u8]> = vec![b"MGET"];
  args.extend(writes.iter().map(|k| k.as_slice()));
  let mut expected = format!("*{N}\r\n").into_bytes();
  for i in 0..N {
    expected.extend(bulk(format!("x{i}").as_bytes()));
  }
  feed(&mut s, &resp_frame(&args));
  assert_eq!(pump_all(&mut s).await, expected);

  drop(dir);
  aok::OK
}

/// 对标 ScatterGatherMixedGetTtlPipeline(:100)：偶数键带 10 分钟过期（对标
/// db.KeyExpire），GET+TTL 交错流水线下 TTL 落自身处理臂——偶数回剩余秒数
/// (0,600]、奇数无过期回 -1；GET 值帧逐条精确
#[compio::test]
async fn scatter_gather_mixed_get_ttl_pipeline() -> aok::Result<()> {
  const N: usize = 100;
  let (dir, store, mut s) = session_with_sg(true);
  let kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..N)
    .map(|i| (format!("r{i}").into_bytes(), format!("val{i}").into_bytes()))
    .collect();
  seed(&store, &kvs);
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store)));

  // 偶数键挂过期
  let mut req = Vec::new();
  for i in (0..N).step_by(2) {
    let k = format!("r{i}");
    req.extend(resp_frame(&[b"EXPIRE", k.as_bytes(), b"600"]));
  }
  feed(&mut s, &req);
  assert_eq!(pump_all(&mut s).await, b":1\r\n".repeat(N / 2));

  // GET + TTL 交错流水线
  let mut req = Vec::new();
  for i in 0..N {
    let k = format!("r{i}");
    req.extend(resp_frame(&[b"GET", k.as_bytes()]));
    req.extend(resp_frame(&[b"TTL", k.as_bytes()]));
  }
  feed(&mut s, &req);
  let out = pump_all(&mut s).await;

  // 帧游标逐帧校验：GET 帧精确比对，TTL 帧整数域判定
  let mut pos = 0usize;
  for i in 0..N {
    let frame = bulk(format!("val{i}").as_bytes());
    assert_eq!(
      &out[pos..pos + frame.len()],
      frame.as_slice(),
      "r{i} GET 值帧错位"
    );
    pos += frame.len();
    assert_eq!(out[pos], b':', "r{i} 应答应为 TTL 整数帧");
    let end = pos + out[pos..].iter().position(|&c| c == b'\r').unwrap();
    let ttl: i64 = from_utf8(&out[pos + 1..end]).unwrap().parse().unwrap();
    if i % 2 == 0 {
      assert!(
        (1..=600).contains(&ttl),
        "r{i} 剩余秒数应落在 (0,600]: {ttl}"
      );
    } else {
      assert_eq!(ttl, -1, "r{i} 无过期应回 -1");
    }
    pos = end + 2;
  }
  assert_eq!(pos, out.len(), "应答帧总量与期望不符");

  drop(dir);
  aok::OK
}

/// 对标 ScatterGatherMGet(:140) 首档 30 键：全命中与混入缺失（2N..3N 未写
/// 键，确定性交错替代 C# 固定种子 Shuffle）两形态下，MGET 与逐键单 GET 同答
#[compio::test]
async fn scatter_gather_mget_hits_and_misses() -> aok::Result<()> {
  const N: usize = 30;
  let (dir, store, mut s) = session_with_sg(true);
  let kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..N).map(|i| (fmt(i), fmt(i))).collect();
  seed(&store, &kvs);
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store)));

  // 混排序列：命中与缺失交错，key < N 即命中
  let mixed: Vec<usize> = (0..N).flat_map(|i| [i, 2 * N + i]).collect();

  // 逐键单 GET 管线
  let mut req = Vec::new();
  let mut expected = Vec::new();
  for &key in &mixed {
    let k = fmt(key);
    req.extend(resp_frame(&[b"GET", k.as_slice()]));
    if key < N {
      expected.extend(bulk(k.as_slice()));
    } else {
      expected.extend_from_slice(NIL);
    }
  }
  feed(&mut s, &req);
  assert_eq!(pump_all(&mut s).await, expected, "单 GET 须逐键对答");

  // 同序列 MGET
  let keys: Vec<Vec<u8>> = mixed.iter().map(|&k| fmt(k)).collect();
  let mut args: Vec<&[u8]> = vec![b"MGET"];
  args.extend(keys.iter().map(|k| k.as_slice()));
  let mut expected = format!("*{}\r\n", mixed.len()).into_bytes();
  for &key in &mixed {
    if key < N {
      expected.extend(bulk(fmt(key).as_slice()));
    } else {
      expected.extend_from_slice(NIL);
    }
  }
  feed(&mut s, &resp_frame(&args));
  assert_eq!(pump_all(&mut s).await, expected, "MGET 须与单 GET 同答");

  drop(dir);
  aok::OK
}

/// 对标 RespMGetLowMemoryNoSgTests.MGetOnDiskCompletesWithoutScatterGatherFlag
/// (:221)：MGET 恒走批量读口、不依赖 SgGet 旋钮——关闭后冷盘（显式全驱逐
/// 替代 C# 大容量溢出）经慢路径批量读照常完整完成，全键值正确
#[compio::test]
async fn mget_completes_without_sg_flag_on_cold_store() -> aok::Result<()> {
  const N: usize = 30;
  let (dir, store, mut s) = session_with_sg(false);
  let kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..N).map(|i| (fmt(i), fmt(i))).collect();
  seed(&store, &kvs);
  store.store.flush_and_evict_all().await?;
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store)));

  let keys: Vec<Vec<u8>> = (0..N).map(fmt).collect();
  let mut args: Vec<&[u8]> = vec![b"MGET"];
  args.extend(keys.iter().map(|k| k.as_slice()));
  let mut expected = format!("*{N}\r\n").into_bytes();
  for i in 0..N {
    expected.extend(bulk(fmt(i).as_slice()));
  }
  feed(&mut s, &resp_frame(&args));
  assert_eq!(pump_all(&mut s).await, expected, "冷盘 MGET 须完整交付");

  drop(dir);
  aok::OK
}
