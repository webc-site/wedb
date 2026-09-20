//! ETag 族集成测试（对标 test/standalone/Garnet.test/RespEtagTests.cs）
//!
//! 覆盖：SETWITHETAG etag 推进与 EX/PX、SETIFMATCH/SETIFGREATER 条件
//! 命中/不命中/初始插入、GETWITHETAG/GETIFNOTMATCH 应答数组、DELIFGREATER
//! 条件删除、NOGET、负值/非数值拒绝、DEL 后重建 etag 归零、普通 SET
//! 覆写保留 etag、TTL 保留/清除语义。

use core::str;

use wnode::resp::{
  key_admin_commands::{ExpireCmd, TtlCmd},
  resp_server_session::RespServerSession,
};
use wnode_test::{Batch, err_frame, with_batch};
use wresp::cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR;

fn parse_resp_int(out: &[u8]) -> i64 {
  let text = str::from_utf8(&out[1..out.len() - 2]).unwrap();
  text.parse().unwrap()
}

/// RESP2 nil bulk string
const NIL: &[u8] = b"$-1\r\n";

/// 提取 [etag, value] 数组中的 etag 整数
fn etag_of_array(out: &[u8]) -> i64 {
  // *2\r\n:<etag>\r\n...
  let rest = &out[4..];
  let end = rest.windows(2).position(|w| w == b"\r\n").unwrap();
  parse_resp_int(&rest[..end + 2])
}

type Ctx<'a> = (&'a mut RespServerSession, &'a Batch<'a>);

fn setwithetag(ctx: Ctx<'_>, args: &[&[u8]]) -> i64 {
  let (s, batch) = ctx;
  let mut out = Vec::new();
  s.network_setwithetag(args, batch, &mut out).unwrap();
  parse_resp_int(&out)
}

fn getwithetag(ctx: Ctx<'_>, key: &[u8]) -> Vec<u8> {
  let (s, batch) = ctx;
  let mut out = Vec::new();
  s.network_getwithetag(&[key], batch, &mut out).unwrap();
  out
}

fn setif(ctx: Ctx<'_>, greater: bool, args: &[&[u8]]) -> Vec<u8> {
  let (s, batch) = ctx;
  let mut out = Vec::new();
  if greater {
    s.network_setifgreater(args, batch, &mut out).unwrap();
  } else {
    s.network_setifmatch(args, batch, &mut out).unwrap();
  }
  out
}

fn delifgreater(ctx: Ctx<'_>, key: &[u8], etag: &[u8]) -> i64 {
  let (s, batch) = ctx;
  let mut out = Vec::new();
  s.network_delifgreater(&[key, etag], batch, &mut out)
    .unwrap();
  parse_resp_int(&out)
}

fn ttl(ctx: Ctx<'_>, key: &[u8]) -> i64 {
  let (s, batch) = ctx;
  let mut out = Vec::new();
  s.network_ttl(TtlCmd::Ttl, &[key], batch, &mut out).unwrap();
  parse_resp_int(&out)
}

fn get(ctx: Ctx<'_>, key: &[u8]) -> Vec<u8> {
  let (s, batch) = ctx;
  let mut out = Vec::new();
  s.network_get(&[key], batch, &mut out).unwrap();
  out
}

/// RespEtagTests.cs:SETReturnsEtagForNewData
#[test]
fn set_returns_etag_for_new_data() {
  with_batch(|s, batch| {
    assert_eq!(setwithetag((s, batch), &[b"rizz", b"buzz"]), 1);
  });
}

/// RespEtagTests.cs:GetWithEtagReturnsValAndEtagForKey
#[test]
fn get_with_etag_returns_val_and_etag_for_key() {
  with_batch(|s, batch| {
    // 不存在的键 → null
    assert_eq!(getwithetag((s, batch), b"florida"), NIL);

    assert_eq!(setwithetag((s, batch), &[b"florida", b"hkhalid"]), 1);
    // [etag, value]
    assert_eq!(
      getwithetag((s, batch), b"florida"),
      b"*2\r\n:1\r\n$7\r\nhkhalid\r\n"
    );
  });
}

/// RespEtagTests.cs:GetWithEtagOnNonEtagDataReturns0ForEtagAndCorrectData
#[test]
fn get_with_etag_on_non_etag_data_returns_0() {
  with_batch(|s, batch| {
    s.network_set(&[b"h", b"k"], batch, &mut Vec::new())
      .unwrap();
    assert_eq!(getwithetag((s, batch), b"h"), b"*2\r\n:0\r\n$1\r\nk\r\n");
  });
}

/// RespEtagTests.cs:GetIfNotMatchReturnsDataWhenEtagDoesNotMatch
#[test]
fn get_if_not_match_returns_data_when_etag_does_not_match() {
  with_batch(|s, batch| {
    // 不存在的键 → null
    let mut out = Vec::new();
    s.network_getifnotmatch(&[b"florida", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, NIL);

    assert_eq!(setwithetag((s, batch), &[b"florida", b"maximus"]), 1);

    // 匹配 → [etag, nil]
    let mut out = Vec::new();
    s.network_getifnotmatch(&[b"florida", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n$-1\r\n");

    // 不匹配 → [etag, value]
    let mut out = Vec::new();
    s.network_getifnotmatch(&[b"florida", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n$7\r\nmaximus\r\n");
  });
}

/// RespEtagTests.cs:GetIfNotMatchOnNonEtagDataReturnsNilForEtagAndCorrectData
#[test]
fn get_if_not_match_on_non_etag_data() {
  with_batch(|s, batch| {
    s.network_set(&[b"h", b"k"], batch, &mut Vec::new())
      .unwrap();
    let mut out = Vec::new();
    s.network_getifnotmatch(&[b"h", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:0\r\n$1\r\nk\r\n");
  });
}

/// RespEtagTests.cs:SetIfMatchReturnsNewValueAndEtagWhenEtagMatches
#[test]
fn set_if_match_returns_new_value_and_etag_when_etag_matches() {
  with_batch(|s, batch| {
    let initial = setwithetag((s, batch), &[b"florida", b"one"]);
    assert_eq!(initial, 1);

    // etag 不命中：回 [existing, 旧值]
    let out = setif((s, batch), false, &[b"florida", b"nextone", b"1738"]);
    assert_eq!(out, b"*2\r\n:1\r\n$3\r\none\r\n");

    // etag 命中：回 [newEtag = given + 1, nil]
    let out = setif((s, batch), false, &[b"florida", b"nextone", b"1"]);
    assert_eq!(out, b"*2\r\n:2\r\n$-1\r\n");

    let out = setif((s, batch), false, &[b"florida", b"nextnextone", b"2"]);
    assert_eq!(out, b"*2\r\n:3\r\n$-1\r\n");

    // 再次不命中：回 [existing, 旧值]
    let out = setif((s, batch), false, &[b"florida", b"lastOne", b"1738"]);
    assert_eq!(out, b"*2\r\n:3\r\n$11\r\nnextnextone\r\n");

    let out = setif((s, batch), false, &[b"florida", b"lastOne", b"3"]);
    assert_eq!(out, b"*2\r\n:4\r\n$-1\r\n");

    // DEL 后普通 SET 重建（无 etag）：不命中回 [0, 旧值]
    s.network_del(&[b"florida"], batch, &mut Vec::new())
      .unwrap();
    s.network_set(&[b"florida", b"one"], batch, &mut Vec::new())
      .unwrap();
    let out = setif((s, batch), false, &[b"florida", b"lastOne", b"1738"]);
    assert_eq!(out, b"*2\r\n:0\r\n$3\r\none\r\n");
  });
}

/// RespEtagTests.cs:SetIfGreaterWorksWithInitialETag
#[test]
fn set_if_greater_works_with_initial_etag() {
  with_batch(|s, batch| {
    assert_eq!(setwithetag((s, batch), &[b"meow-key", b"m"]), 1);

    // 0 不严格大于 1：不命中回 [existing, 旧值]
    let out = setif((s, batch), true, &[b"meow-key", b"diggity", b"0"]);
    assert_eq!(out, b"*2\r\n:1\r\n$1\r\nm\r\n");

    // 2 > 1：命中，新 etag 直取 given（不 +1）
    let out = setif((s, batch), true, &[b"meow-key", b"meow", b"2"]);
    assert_eq!(out, b"*2\r\n:2\r\n$-1\r\n");

    // 5 > 2：命中（缩短值同样成立）
    let out = setif((s, batch), true, &[b"meow-key", b"m", b"5"]);
    assert_eq!(out, b"*2\r\n:5\r\n$-1\r\n");
  });
}

/// RespEtagTests.cs:SetIfGreaterWorksWithoutInitialETag
#[test]
fn set_if_greater_works_without_initial_etag() {
  with_batch(|s, batch| {
    s.network_set(&[b"meow-key", b"m"], batch, &mut Vec::new())
      .unwrap();

    // 无 etag 键 existing = 0：0 不严格大于 0 → 不命中
    let out = setif((s, batch), true, &[b"meow-key", b"check", b"0"]);
    assert_eq!(out, b"*2\r\n:0\r\n$1\r\nm\r\n");

    let out = setif((s, batch), true, &[b"meow-key", b"meow", b"2"]);
    assert_eq!(out, b"*2\r\n:2\r\n$-1\r\n");

    let out = setif((s, batch), true, &[b"meow-key", b"m", b"5"]);
    assert_eq!(out, b"*2\r\n:5\r\n$-1\r\n");
  });
}

/// RespEtagTests.cs:SetIfMatchSetsKeyValueOnNonExistingKey /
/// SetIfGreaterSetsKeyValueOnNonExistingKey
#[test]
fn set_if_sets_key_value_on_non_existing_key() {
  with_batch(|s, batch| {
    // 键不存在：无条件写入。SETIFMATCH → given + 1；SETIFGREATER → given
    let out = setif(
      (s, batch),
      false,
      &[b"key", b"valueanother", b"1", b"EX", b"3"],
    );
    assert_eq!(out, b"*2\r\n:2\r\n$-1\r\n");

    let out = setif(
      (s, batch),
      true,
      &[b"key2", b"valueanother", b"1", b"EX", b"3"],
    );
    assert_eq!(out, b"*2\r\n:1\r\n$-1\r\n");

    assert_eq!(get((s, batch), b"key"), b"$12\r\nvalueanother\r\n");
  });
}

/// RespEtagTests.cs:SetIfMatchOnNonEtagDataReturnsNewEtagAndNoValue
#[test]
fn set_if_match_on_non_etag_data() {
  with_batch(|s, batch| {
    s.network_set(&[b"h", b"k"], batch, &mut Vec::new())
      .unwrap();

    // 无 etag 键 existing = 0：given 0 命中 → [1, nil]
    let out = setif((s, batch), false, &[b"h", b"t", b"0"]);
    assert_eq!(out, b"*2\r\n:1\r\n$-1\r\n");
  });
}

/// RespEtagTests.cs:SetIfMatchReturnsNewEtagButNoValueWhenUsingNoGet
#[test]
fn set_if_match_with_noget() {
  with_batch(|s, batch| {
    s.network_set(&[b"h", b"k"], batch, &mut Vec::new())
      .unwrap();

    let out = setif((s, batch), false, &[b"h", b"t", b"0", b"NOGET"]);
    assert_eq!(out, b"*2\r\n:1\r\n$-1\r\n");

    // NOGET + etag 不命中：回 [existing, nil]（不回旧值）
    let out = setif((s, batch), false, &[b"h", b"t", b"2", b"NOGET"]);
    assert_eq!(out, b"*2\r\n:1\r\n$-1\r\n");
  });
}

/// RespEtagTests.cs:SetWithEtagClearsTTLWhenNoExpiryProvided
#[test]
fn set_with_etag_clears_ttl_when_no_expiry_provided() {
  with_batch(|s, batch| {
    // EX 100 写入 → TTL 存在
    assert_eq!(
      setwithetag((s, batch), &[b"mykey", b"val1", b"EX", b"100"]),
      1
    );
    assert!((95..=100).contains(&ttl((s, batch), b"mykey")));

    // 无 EX/PX 再写 → TTL 清除（SET 语义）
    assert_eq!(setwithetag((s, batch), &[b"mykey", b"val2"]), 2);
    assert_eq!(ttl((s, batch), b"mykey"), -1);
  });
}

/// RespEtagTests.cs:SetIfMatchWorksWithExpiration
#[test]
fn set_if_match_works_with_expiration() {
  with_batch(|s, batch| {
    // 键先无过期
    assert_eq!(setwithetag((s, batch), &[b"florida", b"one"]), 1);

    // EX 100 命中 → 过期加入
    let out = setif(
      (s, batch),
      false,
      &[b"florida", b"nextone", b"1", b"EX", b"100"],
    );
    assert_eq!(etag_of_array(&out), 2);
    assert!((95..=100).contains(&ttl((s, batch), b"florida")));

    // 无 expiry 再命中 → TTL 保留（C# CopyUpdate 保留 srcRecord.Expiration）
    let out = setif(
      (s, batch),
      false,
      &[b"florida", b"nextoneeexpretained", b"2"],
    );
    assert_eq!(etag_of_array(&out), 3);
    assert!((95..=100).contains(&ttl((s, batch), b"florida")));

    s.network_del(&[b"florida"], batch, &mut Vec::new())
      .unwrap();

    // 键先有过期（PX）
    assert_eq!(
      setwithetag((s, batch), &[b"florida", b"one", b"PX", b"100000"]),
      1
    );
    assert!((90..=100).contains(&ttl((s, batch), b"florida")));

    // 命中且无 expiry → 保留
    let out = setif((s, batch), false, &[b"florida", b"nextone", b"1"]);
    assert_eq!(etag_of_array(&out), 2);
    assert!((90..=100).contains(&ttl((s, batch), b"florida")));

    // 命中且带 EX → 换新过期
    let out = setif(
      (s, batch),
      false,
      &[b"florida", b"nextoneeexpretained", b"2", b"EX", b"100"],
    );
    assert_eq!(etag_of_array(&out), 3);
    assert!((95..=100).contains(&ttl((s, batch), b"florida")));
  });
}

/// RespEtagTests.cs:DelIfGreaterOnAnAlreadyExistingKeyWithEtagWorks /
/// DelIfGreaterOnAnAlreadyExistingKeyWithoutEtagWorks /
/// DelIfGreaterOnNonExistingKeyWorks
#[test]
fn del_if_greater_works() {
  with_batch(|s, batch| {
    // 不存在的键 → 0
    assert_eq!(delifgreater((s, batch), b"nonexistingkey", b"10"), 0);

    // 有 etag 键
    assert_eq!(setwithetag((s, batch), &[b"meow-key", b"m"]), 1);
    // 等于不删
    assert_eq!(delifgreater((s, batch), b"meow-key", b"1"), 0);
    assert_eq!(get((s, batch), b"meow-key"), b"$1\r\nm\r\n");
    // 严格大于才删
    assert_eq!(delifgreater((s, batch), b"meow-key", b"2"), 1);
    assert_eq!(get((s, batch), b"meow-key"), NIL);

    // 无 etag 键（existing = 0）：0 不删，1 删
    s.network_set(&[b"plain", b"m"], batch, &mut Vec::new())
      .unwrap();
    assert_eq!(delifgreater((s, batch), b"plain", b"0"), 0);
    assert_eq!(get((s, batch), b"plain"), b"$1\r\nm\r\n");
    assert_eq!(delifgreater((s, batch), b"plain", b"2"), 1);
    assert_eq!(get((s, batch), b"plain"), NIL);

    // DEL 级联清理 etag：重建后 etag 归 1 而非残留
    s.network_set(&[b"meow-key", b"m"], batch, &mut Vec::new())
      .unwrap();
    assert_eq!(setwithetag((s, batch), &[b"meow-key", b"m2"]), 1);
  });
}

/// RespEtagTests.cs:SETWITHETAGOnAlreadyExistingSETDataOverridesItButUpdatesEtag
#[test]
fn set_with_etag_increments_on_each_write() {
  with_batch(|s, batch| {
    assert_eq!(setwithetag((s, batch), &[b"rizz", b"buzz"]), 1);

    let out = setif((s, batch), false, &[b"rizz", b"fixx", b"1"]);
    assert_eq!(etag_of_array(&out), 2);

    // 再写推进 3
    assert_eq!(setwithetag((s, batch), &[b"rizz", b"meow"]), 3);

    let out = setif((s, batch), false, &[b"rizz", b"fooo", b"3"]);
    assert_eq!(etag_of_array(&out), 4);

    assert_eq!(setwithetag((s, batch), &[b"rizz", b"oneofus"]), 5);
  });
}

/// RespEtagTests.cs:SETWITHETAGOnAlreadyExistingNonEtagDataOverridesItToInitialEtag
#[test]
fn set_with_etag_on_non_etag_data_initializes_to_1() {
  with_batch(|s, batch| {
    s.network_set(&[b"rizz", b"used"], batch, &mut Vec::new())
      .unwrap();
    assert_eq!(setwithetag((s, batch), &[b"rizz", b"buzz"]), 1);

    // DEL 后重建：etag 归零重来（级联清理不留残值）
    s.network_del(&[b"rizz"], batch, &mut Vec::new()).unwrap();
    s.network_set(&[b"rizz", b"my"], batch, &mut Vec::new())
      .unwrap();
    assert_eq!(setwithetag((s, batch), &[b"rizz", b"some"]), 1);
  });
}

/// 负值与非数值 etag 拒绝（对标 C# :259-264 与 :93-96 的
/// RESP_ERR_INVALID_ETAG 文案）
#[test]
fn etag_negative_or_non_numeric_rejected() {
  with_batch(|s, batch| {
    let invalid = b"-ETAG must be a numerical value greater than or equal to 0\r\n";
    let not_int = b"-ERR value is not an integer or out of range.\r\n";

    // SETIFMATCH / SETIFGREATER / DELIFGREATER 负值拒绝
    let mut out = Vec::new();
    s.network_setifmatch(&[b"k", b"v", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, invalid);
    let mut out = Vec::new();
    s.network_setifgreater(&[b"k", b"v", b"-5", b"NOGET"], batch, &mut out)
      .unwrap();
    assert_eq!(out, invalid);
    let mut out = Vec::new();
    s.network_delifgreater(&[b"k", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, invalid);

    // 非数值 etag 拒绝（SETIF* 回 INVALID_ETAG；GETIFNOTMATCH 回 not integer）
    let mut out = Vec::new();
    s.network_setifmatch(&[b"k", b"v", b"abc"], batch, &mut out)
      .unwrap();
    assert_eq!(out, invalid);
    let mut out = Vec::new();
    s.network_delifgreater(&[b"k", b"xyz"], batch, &mut out)
      .unwrap();
    assert_eq!(out, invalid);
    let mut out = Vec::new();
    s.network_getifnotmatch(&[b"k", b"zzz"], batch, &mut out)
      .unwrap();
    assert_eq!(out, not_int);

    // etag 错误文案覆盖先前的选项错误（C# :259-264 顺序）
    let mut out = Vec::new();
    s.network_setifmatch(&[b"k", b"v", b"-1", b"GARBAGE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, invalid);
  });
}

/// EX/PX 校验链（对标 C# SETWITHETAG :154-181 与 SETIF* :229-257）
#[test]
fn expiry_options_validation() {
  with_batch(|s, batch| {
    let syntax = err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR);
    let not_int = b"-ERR value is not an integer or out of range.\r\n";
    let invalid_exp = b"-ERR invalid expire time in 'set' command\r\n";

    // SETWITHETAG：未知 token → syntax error
    let mut out = Vec::new();
    s.network_setwithetag(&[b"k", b"v", b"GARBAGE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, syntax);

    // EX 缺 expiry → not integer
    let mut out = Vec::new();
    s.network_setwithetag(&[b"k", b"v", b"EX"], batch, &mut out)
      .unwrap();
    assert_eq!(out, not_int);

    // EX 非整数 → not integer
    let mut out = Vec::new();
    s.network_setwithetag(&[b"k", b"v", b"EX", b"abc"], batch, &mut out)
      .unwrap();
    assert_eq!(out, not_int);

    // EX 0 → invalid expire
    let mut out = Vec::new();
    s.network_setwithetag(&[b"k", b"v", b"EX", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, invalid_exp);

    // KEEPTtl 等其余过期形式 → syntax error（仅接受 EX/PX）
    let mut out = Vec::new();
    s.network_setwithetag(&[b"k", b"v", b"KEEPTTL"], batch, &mut out)
      .unwrap();
    assert_eq!(out, syntax);

    // SETIFMATCH：未知 token → syntax error
    let mut out = Vec::new();
    s.network_setifmatch(&[b"k", b"v", b"0", b"GARBAGE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, syntax);

    // NOGET 重复 → syntax error
    let mut out = Vec::new();
    s.network_setifmatch(&[b"k", b"v", b"0", b"NOGET", b"NOGET"], batch, &mut out)
      .unwrap();
    assert_eq!(out, syntax);

    // EX 缺 expiry → not integer
    let mut out = Vec::new();
    s.network_setifmatch(&[b"k", b"v", b"0", b"EX"], batch, &mut out)
      .unwrap();
    assert_eq!(out, not_int);

    // PX 非正 → invalid expire
    let mut out = Vec::new();
    s.network_setifmatch(&[b"k", b"v", b"0", b"PX", b"-3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, invalid_exp);
  });
}

/// 参数个数校验（对标 C# arity：GETWITHETAG 1、GETIFNOTMATCH 2、
/// DELIFGREATER 2、SETWITHETAG 2..4、SETIF* 3..6）
#[test]
fn etag_wrong_number_of_arguments() {
  with_batch(|s, batch| {
    let wrong = |cmd: &str| {
      Vec::from(format!("-ERR wrong number of arguments for '{cmd}' command\r\n").as_bytes())
    };
    let mut out = Vec::new();
    s.network_getwithetag(&[], batch, &mut out).unwrap();
    assert_eq!(out, wrong("GETWITHETAG"));

    let mut out = Vec::new();
    s.network_getifnotmatch(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, wrong("GETIFNOTMATCH"));

    let mut out = Vec::new();
    s.network_delifgreater(&[b"k", b"1", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, wrong("DELIFGREATER"));

    let mut out = Vec::new();
    s.network_setwithetag(&[b"k"], batch, &mut out).unwrap();
    assert_eq!(out, wrong("SETWITHETAG"));

    let mut out = Vec::new();
    s.network_setifmatch(&[b"k", b"v"], batch, &mut out)
      .unwrap();
    assert_eq!(out, wrong("SETIFMATCH"));

    let mut out = Vec::new();
    // 7 参超 arity 上限（6 参以内的未知 token 走选项循环的 syntax error，
    // 对标 C# 先选项解析后统一报错的顺序）
    s.network_setifgreater(
      &[b"k", b"v", b"1", b"NOGET", b"EX", b"1", b"x"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, wrong("SETIFGREATER"));
  });
}

/// 普通 SET 覆写保留 etag（对标 C# CopyUpdater SET 的 TryCopyOptionals）
#[test]
fn plain_set_overwrite_keeps_etag() {
  with_batch(|s, batch| {
    assert_eq!(setwithetag((s, batch), &[b"k", b"v1"]), 1);

    // 普通 SET 覆写：etag 不动
    s.network_set(&[b"k", b"v2"], batch, &mut Vec::new())
      .unwrap();
    assert_eq!(getwithetag((s, batch), b"k"), b"*2\r\n:1\r\n$2\r\nv2\r\n");

    // etag 命令继续推进为 2
    assert_eq!(setwithetag((s, batch), &[b"k", b"v3"]), 2);
  });
}

/// RespEtagTests.cs:PersistTTLTestForEtagSetData（TTL/PERSIST 不动 etag）
#[test]
fn ttl_operations_keep_etag() {
  with_batch(|s, batch| {
    assert_eq!(setwithetag((s, batch), &[b"expireKey", b"v"]), 1);

    s.network_expire(
      ExpireCmd::Expire,
      &[b"expireKey", b"100"],
      batch,
      &mut Vec::new(),
    )
    .unwrap();
    assert_eq!(
      getwithetag((s, batch), b"expireKey"),
      b"*2\r\n:1\r\n$1\r\nv\r\n"
    );

    s.network_persist(&[b"expireKey"], batch, &mut Vec::new())
      .unwrap();
    assert_eq!(
      getwithetag((s, batch), b"expireKey"),
      b"*2\r\n:1\r\n$1\r\nv\r\n"
    );
  });
}

/// etag 数组解析辅助自检：`[etag, nil]`
#[test]
fn etag_array_parser() {
  assert_eq!(etag_of_array(b"*2\r\n:12\r\n$-1\r\n"), 12);
  assert_eq!(etag_of_array(b"*2\r\n:0\r\n$3\r\nabc\r\n"), 0);
}

/// RENAME 搬迁 etag（对标 C# RENAME 整记录拷贝 TryCopyFrom 连同可选 ETag
/// 字段一体迁移；AOF 侧经 ETag 写端口以「新键直设 + 旧键清除」两跳闭环）
#[test]
fn rename_migrates_etag_to_new_key() {
  with_batch(|s, batch| {
    assert_eq!(setwithetag((s, batch), &[b"oldk", b"v1"]), 1);
    // 推进 etag 至 2 验证绝对值搬迁
    assert_eq!(setwithetag((s, batch), &[b"oldk", b"v2"]), 2);

    s.network_rename(&[b"oldk", b"newk"], batch, None, &mut Vec::new())
      .unwrap();

    assert_eq!(
      getwithetag((s, batch), b"newk"),
      b"*2\r\n:2\r\n$2\r\nv2\r\n",
      "新键须携带旧键的值与绝对 etag"
    );
    assert_eq!(getwithetag((s, batch), b"oldk"), NIL, "旧键整体消失");
  });
}

/// RENAME 旧键无 etag 时清退新键残留 etag（C# 整记录拷贝语义：新记录
/// etag = 旧记录 etag = NoETag，而非保留新键旧 etag）
#[test]
fn rename_without_etag_clears_stale_etag_on_new_key() {
  with_batch(|s, batch| {
    // 新键先带 etag
    assert_eq!(setwithetag((s, batch), &[b"dst", b"v2"]), 1);
    // 旧键为无 etag 的普通字符串
    s.network_set(&[b"src", b"v1"], batch, &mut Vec::new())
      .unwrap();

    s.network_rename(&[b"src", b"dst"], batch, None, &mut Vec::new())
      .unwrap();

    assert_eq!(
      getwithetag((s, batch), b"dst"),
      b"*2\r\n:0\r\n$2\r\nv1\r\n",
      "新键 etag 须随整记录拷贝归零（无残留）"
    );
  });
}

/// expiry 位宽对齐 C# parseState.TryGetInt（int32 域，BasicEtagCommands.cs
/// :167,:239）：超 int 范围解析失败回 not integer；i32::MAX 边界内正常接受。
/// etag 值仍为 long（i64）域，不受影响
#[test]
fn expiry_rejects_out_of_i32_range() {
  with_batch(|s, batch| {
    let not_int = b"-ERR value is not an integer or out of range.\r\n";

    // SETIFMATCH：EX 50 亿秒超 int32 → not integer
    let mut out = Vec::new();
    s.network_setifmatch(&[b"k", b"v", b"0", b"EX", b"5000000000"], batch, &mut out)
      .unwrap();
    assert_eq!(out, not_int);

    // SETWITHETAG：EX 2147483648（i32::MAX + 1）→ not integer
    let mut out = Vec::new();
    s.network_setwithetag(&[b"k", b"v", b"EX", b"2147483648"], batch, &mut out)
      .unwrap();
    assert_eq!(out, not_int);

    // 边界内 i32::MAX：SETWITHETAG 正常接受
    assert_eq!(
      setwithetag((s, batch), &[b"kk", b"v", b"EX", b"2147483647"]),
      1
    );

    // 边界内 i32::MAX：SETIFGREATER PX 正常接受（键不存在 → etag = given）
    let out = setif((s, batch), true, &[b"km", b"v", b"7", b"PX", b"2147483647"]);
    assert_eq!(etag_of_array(&out), 7);
  });
}

/// RESP3 会话下 [etag, nil] 的 nil 元素与键缺失 null 均为 RESP3 形态（nilResp 语义见 resp/basic_etag_commands.rs）；
/// 值元素（bulk string）不受协议影响
#[test]
fn etag_nil_follows_resp3_protocol() {
  with_batch(|s, batch| {
    s.resp_protocol_version = 3;
    const RESP3_NIL: &[u8] = b"_\r\n";

    // GETWITHETAG / GETIFNOTMATCH 键缺失 → RESP3 null
    assert_eq!(getwithetag((s, batch), b"florida"), RESP3_NIL);
    let mut out = Vec::new();
    s.network_getifnotmatch(&[b"florida", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, RESP3_NIL);

    assert_eq!(setwithetag((s, batch), &[b"florida", b"maximus"]), 1);

    // GETIFNOTMATCH etag 命中 → [etag, _]
    let mut out = Vec::new();
    s.network_getifnotmatch(&[b"florida", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n_\r\n");

    // SETIFMATCH 不命中（无 NOGET）→ [existing, 旧值]；NOGET → [existing, _]
    let out = setif((s, batch), false, &[b"florida", b"next", b"9"]);
    assert_eq!(out, b"*2\r\n:1\r\n$7\r\nmaximus\r\n");
    let out = setif((s, batch), false, &[b"florida", b"next", b"9", b"NOGET"]);
    assert_eq!(out, b"*2\r\n:1\r\n_\r\n");
    let out = setif((s, batch), false, &[b"florida", b"next", b"1"]);
    assert_eq!(out, b"*2\r\n:2\r\n_\r\n");

    // SETIFGREATER 命中（NOGET 同径）→ [newEtag, _]
    let out = setif((s, batch), true, &[b"florida", b"zz", b"5", b"NOGET"]);
    assert_eq!(out, b"*2\r\n:5\r\n_\r\n");

    // 不命中且无 NOGET → [existing, 旧值]（值元素仍为 bulk string）
    let out = setif((s, batch), false, &[b"florida", b"ww", b"99"]);
    assert_eq!(out, b"*2\r\n:5\r\n$2\r\nzz\r\n");
  });
}
