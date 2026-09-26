//! RESP null 一族帧型单源对位测试
//!（task/ing/resp-null-protocol-single-source 验收 4）
//!
//! 同一位点在 RESP2 会话下逐字节保持既有 `$-1\r\n` / `*-1\r\n`（不回退），
//! 在 HELLO 3 后的会话（此处直接置 `RespServerSession::resp_protocol_version`，
//! 与 garnet_etag / hash_ttl / resp_list 族既有测试同法）一律为 `_\r\n`，
//! 对位 C# RespServerSessionOutput.cs:WriteNull / WriteNullArray 的版本裁决。
//! 自定义对象执行体（JSON / Roaring）与 vector 应答编码器同臂对照：
//! 三者与命令面共用 wresp::ext::RespVecExt::{write_resp_null_ver,
//! write_resp_null_array_ver} 两处入口，形态不得分叉。

#![cfg(all(feature = "json", feature = "roaring"))]

use wext_json::JsonCommand;
use wext_roaring::RoaringCommand;
use wnode::resp::vector::resp_server_session_vectors::VectorReply;
use wnode_test::with_batch;

/// nil bulk 帧（RESP3 `_`，RESP2 `$-1`）
fn nil_bulk(resp3: bool) -> &'static [u8] {
  if resp3 { b"_\r\n" } else { b"$-1\r\n" }
}

/// nil 数组帧（RESP3 `_`，RESP2 `*-1`）
fn nil_array(resp3: bool) -> &'static [u8] {
  if resp3 { b"_\r\n" } else { b"*-1\r\n" }
}

/// 命令面 + 扩展执行体 + vector 编码器的 null 位点逐条对位
fn assert_null_forms(resp3: bool) {
  let nil = nil_bulk(resp3);
  let ver = if resp3 { 3 } else { 2 };
  with_batch(|s, batch| {
    s.resp_protocol_version = ver;

    // GET 缺键（C# BasicCommands.cs:GETNOTFOUND → WriteNull）
    let mut out = Vec::new();
    s.network_get(&[b"missing_str"], batch, &mut out).unwrap();
    assert_eq!(out, nil, "GET 缺键 @resp{ver}");

    // GETDEL 缺键（C# KeyAdminCommands.cs:GETDEL → WriteNull）
    let mut out = Vec::new();
    s.network_getdel(&[b"missing_str"], batch, &mut out)
      .unwrap();
    assert_eq!(out, nil, "GETDEL 缺键 @resp{ver}");

    // MEMORY USAGE 缺键（C# BasicCommands.cs:NetworkMemoryUsage → WriteNull）
    let mut out = Vec::new();
    s.network_memory_usage(&[b"missing_str"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, nil, "MEMORY USAGE 缺键 @resp{ver}");

    // SET NX / XX 条件失败（C# SETEXNX 同一 nil 出口）
    let mut out = Vec::new();
    s.network_set(&[b"exists_key", b"v1"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    let mut out = Vec::new();
    s.network_set(&[b"exists_key", b"v2", b"NX"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, nil, "SET NX 条件失败 @resp{ver}");
    let mut out = Vec::new();
    s.network_set(&[b"absent_key", b"v1", b"XX"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, nil, "SET XX 条件失败 @resp{ver}");

    // MGET 含缺键（数组头 + 逐键 nil，版本随会话）
    let mut out = Vec::new();
    s.network_mget(&[b"exists_key", b"missing_str"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      [b"*2\r\n".as_slice(), b"$2\r\nv1\r\n".as_slice(), nil].concat(),
      "MGET 缺键 @resp{ver}"
    );

    // HMGET 缺键（count 元素全 nil 数组；C# NOTFOUND → 逐元素 WriteNull）
    let mut out = Vec::new();
    s.hash_get_multiple(&[b"missing_hash", b"f1", b"f2"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      [b"*2\r\n".as_slice(), nil, nil].concat(),
      "HMGET 缺键 @resp{ver}"
    );

    // BITFIELD 越界读不是 null 位点：C# GetBitfield 起点越过当前值长度恒返 0
    //（Resp/Bitmap/BitmapManagerBitfield.cs:336），BitFieldExecute 的 GET 臂 error
    // 恒 false（:460），故 Storage/Functions/MainStore/PrivateMethods.cs:240
    // CopyRespNumber 出整数；会话侧数组长度先落线
    //（Resp/Bitmap/BitmapCommands.cs:585）→ 两版本同为 `*1\r\n:0\r\n`。
    // C# 对位用例：test/standalone/.../GarnetBitmapTests.cs:1196
    // BitFieldMaxOffsetGetAsync（同断言整数 0，非 null）
    let mut out = Vec::new();
    s.network_set(&[b"bf_key", b"abc"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    let mut out = Vec::new();
    s.string_bit_field(&[b"bf_key", b"GET", b"u8", b"8000"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n", "BITFIELD 越界读 @resp{ver}");

    // BITFIELD 写子命令 OVERFLOW FAIL 溢出才是本域 null 位点：C#
    // Storage/Functions/MainStore/RMWMethods.cs:648-651 溢出臂
    // CopyDefaultResp(functionsState.nilResp)（读臂同型 PrivateMethods.cs:243），
    // 数组元素位 null 随会话版本（同一写口 write_resp_null_ver，不分叉）
    let mut out = Vec::new();
    s.string_bit_field(
      &[
        b"bf_key",
        b"INCRBY",
        b"u8",
        b"0",
        b"300",
        b"OVERFLOW",
        b"FAIL",
      ],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(
      out,
      [b"*1\r\n".as_slice(), nil].concat(),
      "BITFIELD 溢出 FAIL @resp{ver}"
    );

    // JSON.GET 缺键（wext_json 执行体经会话版本入参，不自存第二份版本状态）
    let mut out = Vec::new();
    (JsonCommand::Get.fns().not_found)(&[], &mut out, ver);
    assert_eq!(out, nil, "JSON.GET 缺键 @resp{ver}");

    // R.SETBIT 缺键兜底（wext_roaring 执行体，同上）
    let mut out = Vec::new();
    (RoaringCommand::SetBit.fns().not_found)(&[], &mut out, ver);
    assert_eq!(out, nil, "R.SETBIT 缺键兜底 @resp{ver}");
  });

  // vector 应答编码器：Bulk None 为 nil bulk、NullArray 为 nil 数组
  let mut out = Vec::new();
  VectorReply::Bulk(None).encode_resp(&mut out, resp3);
  assert_eq!(out, nil, "vector Bulk None @resp{ver}");
  let mut out = Vec::new();
  VectorReply::NullArray.encode_resp(&mut out, resp3);
  assert_eq!(out, nil_array(resp3), "vector NullArray @resp{ver}");
}

/// RESP2 会话：null 一族既有帧逐字节不回退
#[test]
fn resp2_null_forms_unchanged() {
  assert_null_forms(false);
}

/// RESP3 会话：null 一族与 C# 同型（首字节 `_`）
#[test]
fn resp3_null_forms_match_csharp() {
  assert_null_forms(true);
}
