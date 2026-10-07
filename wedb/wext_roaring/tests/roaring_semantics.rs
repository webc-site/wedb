#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! R.* 命令四语义回归（对标 garnet/test/standalone/Garnet.test/
//! RespRoaringBitmapTests.cs 的四锚，语义锚定在 wext_roaring 命令面：
//! [`RoaringCommand`] 静态清单 + wcustom 注册描述 + 四接口执行体）
//!
//! - 键隔离(:162 StringKeyAndCustomObjectKey_AreSeparate)
//! - 大偏移块界(:242 LargeOffsetsAndChunkBoundaries)
//! - 稠密晋升(:293 DenseBitmapPromotion_StaysCorrect)
//! - 删建(:225 DeleteAndRecreate)
//!
//! 存储信封分域的 WRONGTYPE 门与整键回收在 wnode 侧闭环（wnode/tests/
//! resp_roaring_bitmap_tests.rs 已有 RESP 级同锚用例）；本文件锚定扩展 crate
//! 自持面：注册身份单源、载荷域闭包、位图值语义与删空公理。

use wcustom::{CommandType, KeyScope};
use wext_roaring::RoaringCommand;
use wval::CustomObjectType;

/// RESP 版本（各用例统一 RESP2）
const VER: u8 = 2;

/// 载荷解码失败帧（roaring_bitmap_commands.rs ERR_DECODE 常量的帧形）
const DECODE_ERR_FRAME: &[u8] = b"-ERR RoaringBitmap object decode failed\r\n";

/// 键隔离：统一存储分域的 rust 面承载（对标 :162）
///
/// C# 侧断言 string 键与 custom-object 键互不侵占（R.SETBIT 打 string 键回
/// WRONGTYPE、string 值完好）；rust 拆两层——信封标签 WRONGTYPE 门在 wnode
/// 侧，本 crate 自持两道身份面：
/// 1. 注册身份单源：TYPE 注册名 + 信封标签 + 四命令单键作用域（键参数域
///    首参），string/object 两域据此互斥分域；
/// 2. 载荷域闭包：string 值字节作载荷喂入读写臂一律解码失败闭门，永不误读
///    为位图内容。
#[test]
fn string_key_and_object_key_stay_separate() {
  let entry = RoaringCommand::OBJECT_ENTRY;
  assert_eq!(entry.type_name, "GarnetRoaringBitmap");
  assert!(
    matches!(entry.tag, CustomObjectType::Roaring),
    "信封标签须为 Roaring 分配位"
  );

  // 四命令键作用域恒单键、读写类型与 C# Register 一致
  //（R.SETBIT RMW arity 4，三条读命令 Read arity 3/2/-3）
  let expected: &[(&[u8], bool)] = &[
    (b"R.SETBIT", true),
    (b"R.GETBIT", false),
    (b"R.BITCOUNT", false),
    (b"R.BITPOS", false),
  ];
  for &(name, is_rmw) in expected {
    let Some(meta) = (entry.match_command)(name) else {
      panic!("{name:?} 须在注册清单");
    };
    assert!(
      matches!(meta.key_scope, KeyScope::Single),
      "{name:?} 恒单键"
    );
    if is_rmw {
      assert!(matches!(meta.command_type, CommandType::ReadModifyWrite));
    } else {
      assert!(matches!(meta.command_type, CommandType::Read));
    }
  }

  // String 域值字节作载荷：读写臂一律解码失败闭门并落错误帧
  let get = RoaringCommand::GetBit.fns();
  let set = RoaringCommand::SetBit.fns();
  for payload in [&b"hello"[..], &b"garbage"[..]] {
    let mut out = Vec::new();
    assert!(
      !(get.reader)(payload, &[b"42"], &mut out, VER),
      "string 字节不得被误读为位图"
    );
    assert_eq!(out, DECODE_ERR_FRAME);

    let mut p = payload.to_vec();
    let mut out = Vec::new();
    assert!(
      !(set.updater)(&mut p, &[b"42", b"1"], &mut out, VER),
      "string 字节不得被误写为位图"
    );
    assert_eq!(out, DECODE_ERR_FRAME);
  }
}

/// 大偏移与块界（对标 :242 LargeOffsetsAndChunkBoundaries）：65536 容器块界
/// 七偏移（含 int.MaxValue / uint.MaxValue）置位读回，BITCOUNT = 7，
/// BITPOS 1 全局首置位 0、自 2^31 起搜到 uint.MaxValue
#[test]
fn large_offsets_and_chunk_boundaries() {
  let set = RoaringCommand::SetBit.fns();
  let get = RoaringCommand::GetBit.fns();
  let mut payload = Vec::new();
  let mut out = Vec::new();

  let interesting: [u32; 7] = [0, 65535, 65536, 131071, 131072, i32::MAX as u32, u32::MAX];
  for off in interesting {
    let ob = off.to_string().into_bytes();
    out.clear();
    assert!((set.updater)(
      &mut payload,
      &[ob.as_slice(), b"1"],
      &mut out,
      VER
    ));
    assert_eq!(out, b":0\r\n", "offset {off} 首置旧值应 0");
    out.clear();
    assert!((get.reader)(&payload, &[ob.as_slice()], &mut out, VER));
    assert_eq!(out, b":1\r\n", "offset {off} 读回应 1");
  }

  out.clear();
  assert!((RoaringCommand::BitCount.fns().reader)(
    &payload,
    &[],
    &mut out,
    VER
  ));
  assert_eq!(out, b":7\r\n");

  out.clear();
  assert!((RoaringCommand::BitPos.fns().reader)(
    &payload,
    &[b"1"],
    &mut out,
    VER
  ));
  assert_eq!(out, b":0\r\n");

  out.clear();
  let from = (i32::MAX as u32 + 1).to_string();
  assert!((RoaringCommand::BitPos.fns().reader)(
    &payload,
    &[b"1", from.as_bytes()],
    &mut out,
    VER
  ));
  assert_eq!(out, format!(":{}\r\n", u32::MAX).as_bytes());
}

/// 稠密晋升（对标 :293 DenseBitmapPromotion_StaysCorrect）：单块连置 5000 位
/// 触发 array→bitmap 容器晋升，回收到 4096 后计数值、晋升前后边界位仍正确
#[test]
fn dense_bitmap_promotion_stays_correct() {
  let set = RoaringCommand::SetBit.fns();
  let get = RoaringCommand::GetBit.fns();
  let count = RoaringCommand::BitCount.fns();
  let mut payload = Vec::new();
  let mut out = Vec::new();

  for i in 0..5000u32 {
    let ib = i.to_string().into_bytes();
    out.clear();
    assert!((set.updater)(
      &mut payload,
      &[ib.as_slice(), b"1"],
      &mut out,
      VER
    ));
  }
  out.clear();
  assert!((count.reader)(&payload, &[], &mut out, VER));
  assert_eq!(out, b":5000\r\n");

  // 降级回收 4096..5000：每次清位旧值恒 1
  for i in 4096..5000u32 {
    let ib = i.to_string().into_bytes();
    out.clear();
    assert!((set.updater)(
      &mut payload,
      &[ib.as_slice(), b"0"],
      &mut out,
      VER
    ));
    assert_eq!(out, b":1\r\n");
  }
  out.clear();
  assert!((count.reader)(&payload, &[], &mut out, VER));
  assert_eq!(out, b":4096\r\n");

  out.clear();
  assert!((get.reader)(&payload, &[b"4095"], &mut out, VER));
  assert_eq!(out, b":1\r\n");
  out.clear();
  assert!((get.reader)(&payload, &[b"4096"], &mut out, VER));
  assert_eq!(out, b":0\r\n");
}

/// 删建（对标 :225 DeleteAndRecreate）：置位-清空即键回收（wedb 删空公理：
/// 空载荷整键回收，读走 NotFound 臂），重建后旧位不复活
#[test]
fn delete_and_recreate() {
  let set = RoaringCommand::SetBit.fns();
  let get = RoaringCommand::GetBit.fns();
  let count = RoaringCommand::BitCount.fns();
  let mut payload = Vec::new();
  let mut out = Vec::new();

  // R.SETBIT rb 5 1 → BITCOUNT 1
  assert!((set.updater)(&mut payload, &[b"5", b"1"], &mut out, VER));
  assert_eq!(out, b":0\r\n");
  out.clear();
  assert!((count.reader)(&payload, &[], &mut out, VER));
  assert_eq!(out, b":1\r\n");

  // DEL 的 rust 等价形态：清空末位 → 载荷腾空 → is_empty 成立交存储整键回收
  out.clear();
  assert!((set.updater)(&mut payload, &[b"5", b"0"], &mut out, VER));
  assert_eq!(out, b":1\r\n");
  assert!(payload.is_empty(), "清空末位后载荷必须腾空（删空公理）");
  assert!((set.is_empty)(&payload), "空对象判定面须同答");

  // 键已回收：读走 NotFound 臂——BITCOUNT 0 / GETBIT 0
  out.clear();
  (count.not_found)(&[], &mut out, VER);
  assert_eq!(out, b":0\r\n");
  out.clear();
  (get.not_found)(&[b"5"], &mut out, VER);
  assert_eq!(out, b":0\r\n");

  // 重建：空载荷上 R.SETBIT rb 9 1，旧位 5 不复活
  out.clear();
  assert!((set.updater)(&mut payload, &[b"9", b"1"], &mut out, VER));
  assert_eq!(out, b":0\r\n");
  out.clear();
  assert!((get.reader)(&payload, &[b"5"], &mut out, VER));
  assert_eq!(out, b":0\r\n");
  out.clear();
  assert!((get.reader)(&payload, &[b"9"], &mut out, VER));
  assert_eq!(out, b":1\r\n");
}
