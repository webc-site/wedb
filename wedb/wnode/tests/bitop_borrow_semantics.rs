//! BITOP 源键回调折叠语义回归测试
//!
//! 快路径逐源读改为切片借用折叠（BitOpAccumulator，零逐源克隆）后的
//! 行为等价回归：大源（1MB 级）AND/OR/XOR/DIFF/NOT 与逐字节参考实现
//! 对拍；全缺失、DIFF 单命中源、对象键短路等应答分支保持字节级不变。

use std::{str::from_utf8, sync::Arc};

use tempfile::tempdir;
use wbitmap::BitmapOperation;
use wdev::SegmentedDevice;
use wkv::{BatchStoreSession, WedbStore};
use wnode::resp::resp_server_session::RespServerSession;
use wresp::cmd_strings as cs;
use wtest_base::test_store_config;
use wval::KeyTag;

type TestBatch<'a> = BatchStoreSession<'a, SegmentedDevice>;

fn with_test_env(f: impl FnOnce(&mut RespServerSession, &TestBatch)) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("bitop.db")).unwrap());
  // 4MB 页 × 8 页：容纳 1MB 级大源记录（默认 64KB 页拒收超页记录）
  let mut config = test_store_config();
  config.page_size = 4 * 1024 * 1024;
  config.num_pages = 8;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let mut resp = RespServerSession::default();
  f(&mut resp, &batch);
}

/// 确定性伪随机填充（LCG，无外部依赖）
fn fill(seed: u64, len: usize) -> Vec<u8> {
  let mut state = seed;
  (0..len)
    .map(|_| {
      state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
      (state >> 33) as u8
    })
    .collect()
}

/// 逐字节参考实现（与 C# InvokeBitOperationUnsafe 语义同构：
/// AND 耗尽清零，其余耗尽恒等）
fn naive(op: BitmapOperation, srcs: &[&[u8]]) -> Vec<u8> {
  let max = srcs.iter().map(|s| s.len()).max().unwrap_or(0);
  (0..max)
    .map(|i| {
      let mut b = if srcs[0].len() > i { srcs[0][i] } else { 0 };
      for s in &srcs[1..] {
        b = if s.len() > i {
          match op {
            BitmapOperation::And => b & s[i],
            BitmapOperation::Or => b | s[i],
            BitmapOperation::Xor => b ^ s[i],
            BitmapOperation::Diff => b & !s[i],
            _ => b,
          }
        } else if op == BitmapOperation::And {
          0
        } else {
          b
        };
      }
      b
    })
    .collect()
}

/// 写键并执行 BITOP，回 (应答字节, 目的键当前值)
fn bitop_and_read(
  s: &mut RespServerSession,
  batch: &TestBatch,
  op: BitmapOperation,
  keys: &[&[u8]],
) -> (Vec<u8>, Vec<u8>) {
  let mut out = Vec::new();
  s.network_string_bit_operation(op, keys, batch, None, &mut out)
    .unwrap();
  let mut got = Vec::new();
  s.network_get(&[keys[0]], batch, &mut got).unwrap();
  // 解析 bulk string 应答：$<len>\r\n<payload>\r\n；nil（$-1）回空
  let payload = if got.first() == Some(&b'$') {
    let nl = got.iter().position(|&b| b == b'\n').unwrap();
    let len: i64 = from_utf8(&got[1..nl - 1]).unwrap().parse().unwrap();
    if len < 0 {
      Vec::new()
    } else {
      got[nl + 1..nl + 1 + len as usize].to_vec()
    }
  } else {
    Vec::new()
  };
  (out, payload)
}

const MB: usize = 1 << 20;

/// BITOP 用例（运算种类，命令键序列，源内容对拍基准）
type OpCase<'a> = (BitmapOperation, &'a [&'a [u8]], &'a [&'a [u8]]);

#[test]
fn big_sources_match_naive_for_all_ops() {
  with_test_env(|s, batch| {
    let a = fill(0xdead_beef, MB);
    let b = fill(0x1234_5678, MB);
    // 短源覆盖越界语义（AND 清零尾段 / OR/XOR/DIFF 恒等承接）
    let c = fill(0x00c0_ffee, MB - 400 * 1024);
    batch.try_upsert_sync(b"src:a", &a).unwrap().unwrap();
    batch.try_upsert_sync(b"src:b", &b).unwrap().unwrap();
    batch.try_upsert_sync(b"src:c", &c).unwrap().unwrap();

    let cases: &[OpCase] = &[
      (
        BitmapOperation::And,
        &[b"dst", b"src:a", b"src:b"],
        &[&a, &b],
      ),
      (
        BitmapOperation::Or,
        &[b"dst", b"src:a", b"src:b"],
        &[&a, &b],
      ),
      (
        BitmapOperation::Xor,
        &[b"dst", b"src:a", b"src:b"],
        &[&a, &b],
      ),
      (
        BitmapOperation::Diff,
        &[b"dst", b"src:a", b"src:b"],
        &[&a, &b],
      ),
      // 长短混合 + 三源（首短后长：折叠器补段路径）
      (
        BitmapOperation::And,
        &[b"dst", b"src:c", b"src:a", b"src:b"],
        &[&c, &a, &b],
      ),
      (
        BitmapOperation::Or,
        &[b"dst", b"src:c", b"src:a", b"src:b"],
        &[&c, &a, &b],
      ),
      (
        BitmapOperation::Xor,
        &[b"dst", b"src:a", b"src:c", b"src:b"],
        &[&a, &c, &b],
      ),
      (
        BitmapOperation::Diff,
        &[b"dst", b"src:a", b"src:c", b"src:b"],
        &[&a, &c, &b],
      ),
    ];
    for (op, keys, srcs) in cases {
      let (out, dst_val) = bitop_and_read(s, batch, *op, keys);
      let want_len = srcs.iter().map(|s| s.len()).max().unwrap();
      assert_eq!(
        out,
        format!(":{want_len}\r\n").into_bytes(),
        "{op:?} 应答长度"
      );
      assert_eq!(dst_val, naive(*op, srcs), "{op:?} 目的键内容");
    }

    // NOT 一元（大源逐字节取反）
    let (out, dst_val) = bitop_and_read(s, batch, BitmapOperation::Not, &[b"dst", b"src:a"]);
    assert_eq!(out, format!(":{}\r\n", a.len()).into_bytes());
    assert_eq!(dst_val, a.iter().map(|b| !b).collect::<Vec<u8>>());
  });
}

#[test]
fn response_branches_unchanged() {
  with_test_env(|s, batch| {
    batch
      .try_upsert_sync(b"alive", b"\x01\x02\x03\x04")
      .unwrap()
      .unwrap();

    // 全缺失：回 0 且不建目的键
    let (out, _) = bitop_and_read(s, batch, BitmapOperation::Or, &[b"dst", b"nope1", b"nope2"]);
    assert_eq!(out, b":0\r\n");
    let mut probe = Vec::new();
    s.network_get(&[b"dst"], batch, &mut probe).unwrap();
    assert_eq!(probe, b"$-1\r\n", "全缺失不得写目的键");

    // DIFF 命中源被缺失吞并至单源：通用错误（C# GarnetException 对位）
    let (out, _) = bitop_and_read(
      s,
      batch,
      BitmapOperation::Diff,
      &[b"dst", b"alive", b"nope"],
    );
    assert!(out.starts_with(b"-ERR"), "DIFF 单命中源须回错误帧");

    // 正常 DIFF 双命中源：回最长源长度且目的键为首源清除其余后的内容
    let x = [0xffu8; 6];
    let y = [0x0fu8; 4];
    batch.try_upsert_sync(b"dx", &x).unwrap().unwrap();
    batch.try_upsert_sync(b"dy", &y).unwrap().unwrap();
    let (out, dst_val) = bitop_and_read(s, batch, BitmapOperation::Diff, &[b"dst", b"dx", b"dy"]);
    assert_eq!(out, b":6\r\n");
    assert_eq!(dst_val, [0xf0, 0xf0, 0xf0, 0xf0, 0xff, 0xff]);

    // 对象键短路：整体 WRONGTYPE 且目的键不写（信封域物理键，值内容任意）
    batch
      .try_upsert_tag_sync(b"obj:key", KeyTag::ObjectEnvelope, b"envelope-payload")
      .unwrap()
      .unwrap();
    let mut out = Vec::new();
    s.network_string_bit_operation(
      BitmapOperation::And,
      &[b"dst", b"alive", b"obj:key"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    let wrong_type = cs::RESP_ERR_WRONG_TYPE.as_bytes();
    assert!(
      out.windows(wrong_type.len()).any(|w| w == wrong_type),
      "对象键源须整体 WRONGTYPE 短路"
    );
  });
}
