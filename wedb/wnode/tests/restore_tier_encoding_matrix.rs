#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! DUMP/RESTORE 长度编码三档位矩阵与跨界 roundtrip 回归
//!
//! 对标 C# test/standalone/Garnet.test/RespTests.cs：
//! - SingleDump6Bit（:282）：len<64 → 1 字节前缀（garnet 期望 `0x00 0x03`）
//! - SingleDump14Bit（:320）：64≤len≤16383 → 2 字节前缀（`0x00 0x7F 0xFE`
//!   即 16382）
//! - SingleDump32Bit（:371）：len≥16384 → 5 字节前缀（`0x00 0x80 0x00 0x00
//!   0x40 0x00` 即 16384）
//!
//! 本档补三档长度前缀字节精确断言 + 两处跨界（63→64、16383→16384）两侧
//! 逐字节档位锁定 + 跨界 DUMP→DEL→RESTORE→GET roundtrip；crc64 按 rust
//! 口径自洽复算（类型字节起算，doc/zh/deviations.md §21），前缀与 RDB
//! 版本字节与 garnet 期望逐字节一致。夹具形态同
//! restore_10byte_payload_slice.rs（真存储真协议帧，无 mock）。

use std::sync::Arc;

use compio::runtime::Runtime;
use wbase::crc64::hash as crc64_hash;
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::{reply_bulk, roundtrip};
use wresp::length::try_read_length;
use wtest_base::open_test_store;

/// 6 位档上限（含）——1 字节前缀
const TIER6_MAX: usize = 63;
/// 14 位档上限（含）——2 字节前缀
const TIER14_MAX: usize = 16_383;
/// RDB 版本尾（2 字节小端，同 garnet 0x0B 0x00）
const RDB_VERSION: u16 = 11;
/// crc64 尾长
const CRC_LEN: usize = 8;

/// 装配带真存储的会话消费者
fn harness(tag: &str) -> (Runtime, RespSessionConsumer) {
  let rt = Runtime::new().unwrap();
  let (_dir, store) = open_test_store(tag).unwrap();
  let c = RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  (rt, c)
}

/// SET + DUMP 取载荷（应答面已含 +OK 前置泵入，此处单命令往返）
fn dump_of(c: &mut RespSessionConsumer, rt: &Runtime, key: &[u8], val: &[u8]) -> Vec<u8> {
  assert_eq!(
    roundtrip(rt, c, &[b"SET", key, val]),
    b"+OK\r\n",
    "SET 预置应成功"
  );
  let out = roundtrip(rt, c, &[b"DUMP", key]);
  reply_bulk(&out).expect("DUMP 应答必为 bulk")
}

/// 三档位长度前缀字节精确断言 + DUMP 帧结构（类型 / 前缀 / 载荷 / 版本 /
/// crc）逐段锁定；跨界两侧（63/64、16383/16384）档位各归其位
/// （garnet SingleDump6Bit/14Bit/32Bit + 跨界矩阵对位）
#[test]
fn dump_length_prefix_tiers_and_boundaries() {
  let (rt, mut c) = harness("restore-tier-dump.db");

  // (值长, 期望长度前缀字节)——garnet 三档期望的 rust 同构口径：
  // len=3 → [0x03]（garnet :296）；len=63 → [0x3F]（6 档顶）；
  // len=64 → [0x40,0x40]（14 档底）；
  // len=16383 → [0x7F,0xFF]（14 档顶）；len=16384 → [0x80,0x00,0x00,0x40,0x00]
  //（garnet :369 32 档底 0x4000 大端）
  let cases: &[(usize, &[u8])] = &[
    (3, &[0x03]),
    (TIER6_MAX, &[0x3F]),
    (TIER6_MAX + 1, &[0x40, 0x40]),
    (TIER14_MAX, &[0x7F, 0xFF]),
    (TIER14_MAX + 1, &[0x80, 0x00, 0x00, 0x40, 0x00]),
  ];
  for (idx, (len, prefix)) in cases.iter().enumerate() {
    let val = vec![b'a'; *len];
    let key = format!("dk{idx}").into_bytes();
    let dump = dump_of(&mut c, &rt, &key, &val);

    // 帧结构：类型 0x00 + 前缀 + 载荷 + 版本 2B + crc 8B
    assert_eq!(dump[0], 0x00, "len={len} 首字节必为值类型 0x00");
    assert_eq!(
      &dump[1..1 + prefix.len()],
      *prefix,
      "len={len} 长度前缀档位字节漂移"
    );
    // 前缀自洽解码（wresp 单源读回）
    let (decoded, consumed) =
      try_read_length(&dump[1..]).unwrap_or_else(|| panic!("len={len} 前缀解码失败"));
    assert_eq!(
      (decoded as usize, consumed),
      (*len, prefix.len()),
      "len={len} 前缀解码须回读同值同宽"
    );
    // 载荷区间逐字节
    let body_start = 1 + prefix.len();
    assert_eq!(
      &dump[body_start..body_start + len],
      val.as_slice(),
      "len={len} 载荷区字节漂移"
    );
    // RDB 版本 2 字节小端 11（garnet 0x0B 0x00）
    assert_eq!(
      &dump[body_start + len..body_start + len + 2],
      &RDB_VERSION.to_le_bytes(),
      "len={len} RDB 版本尾漂移"
    );
    // crc64（rust 口径：类型字节起算至版本尾，§21）
    let crc_start = dump.len() - CRC_LEN;
    assert_eq!(
      &dump[crc_start..],
      &crc64_hash(&dump[..crc_start]),
      "len={len} crc64 尾漂移"
    );
    assert_eq!(dump.len(), 1 + prefix.len() + len + 2 + CRC_LEN);
  }

  // 大小递增不破坏会话
  assert_eq!(roundtrip(&rt, &mut c, &[b"PING"]), b"+PONG\r\n");
}

/// 跨界 DUMP→DEL→RESTORE→GET roundtrip：两处档位边界（63/64、16383/
/// 16384）两侧的 DUMP 载荷经 RESTORE 复原原值（garnet
/// SingleRestore6BitWithoutTtl 的跨界推广）
#[test]
fn restore_roundtrip_across_tier_boundaries() {
  let (rt, mut c) = harness("restore-tier-roundtrip.db");

  for (idx, len) in [3usize, TIER6_MAX, TIER6_MAX + 1, TIER14_MAX, TIER14_MAX + 1]
    .into_iter()
    .enumerate()
  {
    let val = vec![b'b'; len];
    let src = format!("rk{idx}").into_bytes();
    let dst = format!("rs{idx}").into_bytes();
    let dump = dump_of(&mut c, &rt, &src, &val);

    assert_eq!(
      roundtrip(&rt, &mut c, &[b"DEL", &dst]),
      b":0\r\n",
      "目标键预检应不存在"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"RESTORE", &dst, b"0", &dump]),
      b"+OK\r\n",
      "len={len} 档位载荷 RESTORE 应放行"
    );
    let got = reply_bulk(&roundtrip(&rt, &mut c, &[b"GET", &dst]))
      .unwrap_or_else(|| panic!("len={len} RESTORE 后 GET 应有值"));
    assert_eq!(got, val, "len={len} roundtrip 值漂移");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"STRLEN", &dst]),
      format!(":{len}\r\n").as_bytes(),
      "len={len} STRLEN 应复原原长"
    );
  }
}
