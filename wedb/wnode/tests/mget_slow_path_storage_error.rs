//! MGET / GET_SG 慢路径磁盘收割失败的 RESP 成帧回归
//!
//! 票面命题：wkv 批量读口 `session/raw/batch.rs:read_batch_raw_with` 的次序契约是
//! 「任一磁盘候选收割失败即以 `Err` 中止交付，首个磁盘候选之前的内存项已先行交付、
//! 无法回滚，调用方须整体丢弃本批部分结果」。慢路径两臂（`array_commands.rs`
//! 的 `slow::mget_inner`、`garnet_api/slow.rs` 的 `C::Get` 臂）曾未履行该丢弃契约，
//! 直接把错误帧追加在未回滚的 `*N` 数组头与部分元素之后，产出长度与内容不符的
//! 畸形 bulk/array 帧（MGET）与 N 命令 M+1 帧的缺帧错位（SG GET 流水线）。
//!
//! 故障注入为真设备真故障（非 mock）：冷落盘键所在段文件被截断为空，冷读经
//! `wdev` 读口以 `UnexpectedEof` 上抛（同仓 `wkv/tests/checkpoint/fault_defense.rs`
//! 同款物理介质故障口径）。
//!
//! 对标 C#：`ArrayCommands.cs:NetworkMGET` 先写数组头再 `ReadWithPrefetch` +
//! `MGetReadArgBatch.cs:CompletePending` 顺序补写元素，磁盘收割异常一律抛至
//! `RespServerSession.cs:ProcessMessages` 的 catch 通道（绝无「部分数组 + 错误帧加连接继续服务」形态）；
//! `BasicCommands.cs:NetworkGET_SG` 每键各是一条独立命令、逐键独立成帧。rust 慢路径对错误帧的承接形态即本文件断言目标。

use std::{fs::OpenOptions, sync::Arc};

use tempfile::TempDir;
use wconf::RuntimeServerConfig;
use wnode::resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSession};
use wnode_test::{complete_len, err_frame, test_env};
use wresp::cmd_strings::RESP_ERR_SLOW_PATH_STORAGE;
use wtest_base::resp_frame;

/// `n` 条存储错误帧的期望线面字节（单帧经 `wresp` 成帧单点派生，期望值全部由
/// 协议帧公式推导，非实测抄录）
fn err_frames(n: usize) -> Vec<u8> {
  err_frame(RESP_ERR_SLOW_PATH_STORAGE).repeat(n)
}

/// 装配「内存热键 + 冷落盘键」混合会话，并把段文件截断为空注入磁盘读故障
///
/// 冷键先写后 `flush_and_evict_all`（地址落入 head 之下 → 读侧恒判磁盘候选），
/// 热键在淘汰之后新写（地址落入可变区 → 恒内存直读），随后清空段文件：
/// 批量读必先正常交付热键帧、再在冷键收割处失败上抛，正是「部分结果不可回滚」
/// 契约的触发形态
async fn cold_fault_env() -> (TempDir, RespServerSession) {
  let (dir, session, mut s) = test_env(false);
  s.runtime_config = Arc::new(RuntimeServerConfig::with_defaults());

  {
    let batch = session.enter_batch();
    batch
      .try_upsert_sync(b"mf:cold1", b"v_cold1")
      .unwrap()
      .unwrap();
    batch
      .try_upsert_sync(b"mf:cold2", b"v_cold2")
      .unwrap()
      .unwrap();
  }
  session.store.flush_and_evict_all().await.unwrap();
  {
    let batch = session.enter_batch();
    batch
      .try_upsert_sync(b"mf:hot1", b"v_hot1")
      .unwrap()
      .unwrap();
    batch
      .try_upsert_sync(b"mf:hot2", b"v_hot2")
      .unwrap()
      .unwrap();
  }

  OpenOptions::new()
    .write(true)
    .open(session.store().device.segment_path(0))
    .expect("段文件应已随 store 打开创建")
    .set_len(0)
    .expect("截断段文件注入磁盘故障");

  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));
  (dir, s)
}

/// 驱动一轮消费并回线面应答字节，返回 `(应答字节, 是否走慢路径)`
///
/// 慢路径应答经产线冲出口 [`RespServerSession::resolve_slow_wait_into`] 并入，与
/// 真实网络泵同一写出面（该口先冲会话累积应答再按流水线顺序并尾部）
async fn pump(session: &mut RespServerSession, input: &[u8]) -> (Vec<u8>, bool) {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(input);
  session.bytes_read = input.len();
  session.read_head = 0;
  session.end_read_head = 0;
  session.output.clear();
  assert_eq!(
    session.try_consume_messages(),
    Some(0),
    "输入命令应整段消费完毕"
  );

  let mut wire = Vec::new();
  match session.take_slow_wait() {
    Some(slow) => {
      let reply = slow.resolve().await;
      session.resolve_slow_wait_into(&reply, &mut wire);
      (wire, true)
    }
    None => {
      session.take_output_into(&mut wire);
      (wire, false)
    }
  }
}

/// MGET 慢路径磁盘收割失败：单条命令只回一条完整错误帧，零数组头与元素残留
#[compio::test]
async fn mget_slow_path_storage_error_writes_single_conforming_frame() -> aok::Result<()> {
  let (_dir, mut s) = cold_fault_env().await;

  // 热键在前（批量口先行交付）、冷键在后（磁盘候选收割失败中止本批）
  let input = resp_frame(&[b"MGET", b"mf:hot1", b"mf:cold1"]);
  let (wire, went_slow) = pump(&mut s, &input).await;
  assert!(
    went_slow,
    "混批须降级慢路径批量读口，否则本用例不触达磁盘收割失败臂"
  );
  // MGET 是一条命令，错误整命令替换：既无 `*2` 数组头，也无 `$6/v_hot1` 残帧
  assert_eq!(wire, err_frames(1));
  // 帧完整性自证：全部线面字节恰构成一条完整 RESP 应答，无尾随半帧
  assert_eq!(complete_len(&wire), Some(wire.len()));

  // 连接续用无应答错位
  let (ping, _) = pump(&mut s, &resp_frame(&[b"PING"])).await;
  assert_eq!(ping, b"+PONG\r\n");
  // 后续纯内存 MGET 仍正常成帧（数组头 + N 元素，回滚未破坏会话输出状态）
  let (ok, went_slow) = pump(&mut s, &resp_frame(&[b"MGET", b"mf:hot1", b"mf:hot2"])).await;
  assert!(!went_slow, "全内存批须走快路径");
  assert_eq!(ok, b"*2\r\n$6\r\nv_hot1\r\n$6\r\nv_hot2\r\n");
  Ok(())
}

/// SG GET 流水线磁盘收割失败：N 条独立命令须回 N 条独立错误帧
#[compio::test]
async fn get_sg_pipeline_storage_error_writes_one_frame_per_key() -> aok::Result<()> {
  let (_dir, mut s) = cold_fault_env().await;

  // 2 条 GET 合并为一批 SG 冷读：首键内存命中先行交付，次键冷读失败
  let input = [
    resp_frame(&[b"GET", b"mf:hot1"]),
    resp_frame(&[b"GET", b"mf:cold1"]),
  ]
  .concat();
  let (wire, went_slow) = pump(&mut s, &input).await;
  assert!(
    went_slow,
    "SG 冷读批须整批降级 C::Get 臂，否则本用例不触达磁盘收割失败臂"
  );
  // N 键 N 帧，且不得混入热键的先行交付帧
  assert_eq!(wire, err_frames(2));
  // 结构自证：首帧恰为一条完整错误帧，余下字节再构成一条完整错误帧
  let one = err_frame(RESP_ERR_SLOW_PATH_STORAGE);
  assert_eq!(complete_len(&wire), Some(one.len()));
  assert_eq!(&wire[one.len()..], one.as_slice());

  // 单键 GET 慢路径退化为单帧（C# NetworkGET_SG 单命令同形）
  let (single, went_slow) = pump(&mut s, &resp_frame(&[b"GET", b"mf:cold2"])).await;
  assert!(went_slow, "冷键 GET 须降级慢路径");
  assert_eq!(single, err_frames(1));

  // 连接续用无应答错位
  let (ping, _) = pump(&mut s, &resp_frame(&[b"PING"])).await;
  assert_eq!(ping, b"+PONG\r\n");
  Ok(())
}
