//! 异值大载荷 HSET 批的升阶容量门回归（票 tiered-hset-mixed-large-value-storage-err-repro）
//!
//! 现象（修复前红相）：1MB 页配置下 1000 字段 × 600B 分块灌入，异值（内容随
//! i%64 变化）自第二块起每块回 `-ERR slow path storage error`，键永久卡死——
//! 中间信封整值记录 1,209,952B 超页容量被 whlog `validate_append_args` 拒
//! （`HLog(RecordTooLarge { size: 1209952, page_size: 1048576 })`），而升阶
//! 内存阈（wcol::TIERED_PROMOTE_BYTES = 4MB heap）未达，失败写不推进状态，
//! 永远够不到升阶。同值载荷因 bitcode 对重复内容的折叠（同 1000 条目 blob
//! 79,428B vs 异值 604,427B）侥幸绕过——非契约，掩盖了该面。
//!
//! 裁决：记录不可跨页是两侧共口径（whlog `validate_append_args` ⇄ C#
//! AllocatorBase.cs:TryAllocate "Entry does not fit on page" 硬抛），但 C#
//! 默认 16m 页（ServerOptions.cs PageSize）且无升阶机制，信封恒可长到页容量；
//! rust 页容量随预算收缩（[64KB,16MB]），页 < 升阶阈的死区系本仓升阶架构自身
//! 盲区。修复面收窄在写回判定臂（rmw_helpers `envelope_overflow` 容量门）：
//! 信封记录超页视同超阈，走既有升阶臂（wbftree 逐成员入表，无单记录上限），
//! 异值批与同值批同命。

use std::{iter::repeat_n, mem::take, sync::Arc};

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

const VALUE_BYTES: usize = 600;
const TOTAL: usize = 8000;
const CHUNK: usize = 1000;
/// 值内容变化模数（异值源：头缀取 i%64 十进制串，余部 'm' 填充）
const VALUE_MODULUS: usize = 64;

fn open_env(
  tag: &str,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  // 1MB 页对齐现役体积维用例（collection_adaptive_tiering / tiered_background_demote），
  // 页容量 < 升阶内存阈 4MB，正是死区暴露面
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

fn auto_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 异值成员值：`[i%64 十进制串]['m' 填充至 VALUE_BYTES]`（同长异内容，
/// 剥离 bitcode 同值折叠的侥幸通道）
fn mixed_value(i: usize, buf: &mut ItoaBuffer) -> Vec<u8> {
  let head = buf.format(i % VALUE_MODULUS);
  let mut v = Vec::with_capacity(VALUE_BYTES);
  v.extend_from_slice(head.as_bytes());
  v.extend(repeat_n(b'm', VALUE_BYTES - head.len()));
  v
}

#[test]
fn hset_mixed_large_value_chunks_tier_via_capacity_gate() {
  let (rt, api, store, _dir) = open_env("tiered-hset-mixed.db");
  let mut s = session_with(&api);
  let mut buf = ItoaBuffer::new();

  // 分块灌入：每块回帧必须是整数计数——存储层拒绝当场红，不留「未升阶」假象
  for chunk_start in (1..=TOTAL).step_by(CHUNK) {
    let chunk_end = (chunk_start + CHUNK - 1).min(TOTAL);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"mixed_hash".to_vec());
    for i in chunk_start..=chunk_end {
      let mut field = b"f".to_vec();
      field.extend_from_slice(buf.format(i).as_bytes());
      args.push(field);
      args.push(mixed_value(i, &mut buf));
    }
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    let rep = auto_exec(&api, &rt, &mut s, RespCommand::Hset, &slices);
    assert_eq!(
      rep,
      format!(":{CHUNK}\r\n").as_bytes(),
      "异值灌入批 {chunk_start}..{chunk_end} 回帧异常（修复前第二块起此处为 \
       -ERR slow path storage error）：{:?}",
      String::from_utf8_lossy(&rep)
    );
  }

  // 全量计数一致（容量门升阶后树内直写，无丢块）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"mixed_hash"]),
    format!(":{TOTAL}\r\n").as_bytes()
  );

  // 分层存根在场：条目数 8000 < 65536、异值 heap 未及 4MB 阈，升阶由页容量门
  // 触发即本票修复面；元计数与 HLEN 一致
  let sess = store.new_session().unwrap();
  let stub_opt = rt
    .block_on(sess.load_collection_stub(b"mixed_hash"))
    .unwrap();
  assert!(stub_opt.is_some(), "异值大载荷应经容量门升阶为分层树");
  let (meta, _) = stub_opt.unwrap();
  assert_eq!(meta.collection_type, GarnetObjectType::Hash);
  assert_eq!(meta.size as usize, TOTAL);

  // 树内点查逐字节回读异值成员（首/中/尾三抽样）
  for i in [1usize, TOTAL / 2, TOTAL] {
    let field = format!("f{i}");
    let expected = mixed_value(i, &mut ItoaBuffer::new());
    let rep = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hget,
      &[b"mixed_hash", field.as_bytes()],
    );
    let mut want = format!("${}\r\n", expected.len()).into_bytes();
    want.extend_from_slice(&expected);
    want.extend_from_slice(b"\r\n");
    assert_eq!(
      rep, want,
      "字段 {field} 异值内容须逐字节回读（头缀 i%{VALUE_MODULUS}）"
    );
  }
}
