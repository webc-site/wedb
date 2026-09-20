//! TTL 写路径语义回归测试（RMW 保留 / SET 清除 / STORE 族清除）
//!
//! 对标 garnet：
//! - libs/server/Storage/Functions/UnifiedStore/VarLenInputMethods.cs:GetRMWModifiedFieldInfo
//!   （RMW 保留 HasExpiration）
//! - libs/server/Storage/Functions/MainStore/RMWMethods.cs（SETRANGE/APPEND 分支
//!   "not changing the presence of ETag or Expiration"）
//! - libs/server/Storage/Functions/ObjectStore/SetOps.cs / SortedSetOps.cs
//!   （STORE 族目标键 SET 语义清 TTL；SMOVE dst 全 RMW 保留）
//! - EXPIRE 过去时间戳：rust 物理删回 :1（C# 惰性过期同回 :1，终态等价）

use std::sync::Arc;

use tempfile::tempdir;
use wbase::{
  convert::{TICKS_PER_SECOND, expire_at_seconds_to_ticks},
  time::now_ticks,
};
use wdev::SegmentedDevice;
use wkv::{BatchStoreSession, WedbStore};
use wnode::{
  resp::{
    basic_commands::IncrCmd,
    key_admin_commands::{ExpireCmd, TtlCmd},
    objects::sorted_set_geo_commands::GeoSearchCommandKind,
    resp_server_session::RespServerSession,
  },
  storage::session::common::ttl_sync::{del_ttl_sync, put_ttl_sync, ttl_of_sync},
};
use wtest_base::test_store_config;

type TestBatch<'a> = BatchStoreSession<'a, SegmentedDevice>;

fn with_test_env(f: impl FnOnce(&mut RespServerSession, &TestBatch)) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let mut resp = RespServerSession::default();
  f(&mut resp, &batch);
}

/// 建字符串键并设 60s TTL，返回落盘的过期 ticks（helper 直调 put_ttl_sync
/// 裸写内核，值未粗化——粗化只发生在命令入口 network_expire / wkv expire_at）
fn seed_ttl_key(batch: &TestBatch, key: &[u8], val: &[u8]) -> i64 {
  batch.try_upsert_sync(key, val).unwrap().unwrap();
  put_ttl_sync(batch, key, now_ticks() + 60 * TICKS_PER_SECOND).unwrap();
  ttl_of_sync(batch, key)
    .unwrap()
    .value()
    .unwrap()
    .expect("TTL 在场")
}

/// 给已存在键（任意域）设 60s TTL，返回落盘的过期 ticks（put_ttl_sync
/// 裸写内核，不粗化）。对象/HLL 键专用：TTL 记录为键级旁路，不触碰数据域
fn seed_ttl_on_key(batch: &TestBatch, key: &[u8]) -> i64 {
  put_ttl_sync(batch, key, now_ticks() + 60 * TICKS_PER_SECOND).unwrap();
  ttl_of_sync(batch, key)
    .unwrap()
    .value()
    .unwrap()
    .expect("TTL 在场")
}

/// 断言键 TTL 记录原样保留（RMW 语义）
fn assert_ttl_kept(batch: &TestBatch, key: &[u8], expected: i64) {
  assert_eq!(
    ttl_of_sync(batch, key).unwrap().value(),
    Some(Some(expected)),
    "RMW 写回必须保留 key 级 TTL（{key:?}）"
  );
}

/// 断言键 TTL 记录已清除（SET 语义）
fn assert_ttl_cleared(batch: &TestBatch, key: &[u8]) {
  assert_eq!(
    ttl_of_sync(batch, key).unwrap().value(),
    Some(None),
    "SET 语义写入必须清除 key 级 TTL（{key:?}）"
  );
}

/// 字符串 RMW 族保留既有 TTL：INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT/APPEND/
/// SETRANGE/SETBIT/BITFIELD SET/PFADD（对标 GetRMWModifiedFieldInfo 保留）
#[test]
fn string_rmw_commands_preserve_ttl() {
  with_test_env(|s, batch| {
    // INCR/DECR 族（同键连续增减）
    let exp = seed_ttl_key(batch, b"rmw:cnt", b"10");
    let mut out = Vec::new();
    s.network_increment(IncrCmd::Incr, &[b"rmw:cnt"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":11\r\n");
    out.clear();
    s.network_increment(IncrCmd::Decr, &[b"rmw:cnt"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":10\r\n");
    out.clear();
    s.network_increment(IncrCmd::IncrBy, &[b"rmw:cnt", b"4"], batch, &mut out)
      .unwrap();
    out.clear();
    s.network_increment(IncrCmd::DecrBy, &[b"rmw:cnt", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":12\r\n");
    assert_ttl_kept(batch, b"rmw:cnt", exp);

    // INCRBYFLOAT
    out.clear();
    let exp = seed_ttl_key(batch, b"rmw:flt", b"1.5");
    s.network_increment_by_float(&[b"rmw:flt", b"0.25"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$4\r\n1.75\r\n");
    assert_ttl_kept(batch, b"rmw:flt", exp);

    // APPEND（命中与未命中两臂）
    let exp = seed_ttl_key(batch, b"rmw:app", b"ab");
    out.clear();
    s.network_append(&[b"rmw:app", b"cd"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":4\r\n");
    assert_ttl_kept(batch, b"rmw:app", exp);
    let exp = seed_ttl_key(batch, b"rmw:appnew", b"");
    out.clear();
    s.network_append(&[b"rmw:appnew", b"x"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    assert_ttl_kept(batch, b"rmw:appnew", exp);

    // SETRANGE（命中与未命中两臂）
    let exp = seed_ttl_key(batch, b"rmw:sr", b"abcdef");
    out.clear();
    s.network_set_range(&[b"rmw:sr", b"1", b"XY"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":6\r\n");
    assert_ttl_kept(batch, b"rmw:sr", exp);
    let exp = seed_ttl_key(batch, b"rmw:srnew", b"");
    out.clear();
    s.network_set_range(&[b"rmw:srnew", b"2", b"Z"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":3\r\n");
    assert_ttl_kept(batch, b"rmw:srnew", exp);

    // SETBIT
    let exp = seed_ttl_key(batch, b"rmw:bit", b"\x00");
    out.clear();
    s.network_string_set_bit(&[b"rmw:bit", b"0", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
    assert_ttl_kept(batch, b"rmw:bit", exp);

    // BITFIELD SET 写子命令（先建值，设 TTL 后再写第二字节）
    out.clear();
    s.string_bit_field(&[b"rmw:bf", b"SET", b"u8", b"0", b"7"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n", "BITFIELD SET 回旧值");
    let exp = seed_ttl_on_key(batch, b"rmw:bf");
    out.clear();
    s.string_bit_field(&[b"rmw:bf", b"SET", b"u8", b"8", b"3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n");
    assert_ttl_kept(batch, b"rmw:bf", exp);

    // PFADD
    s.hyper_log_log_add(&[b"rmw:hll", b"e1"], batch, &mut out)
      .unwrap();
    let exp = seed_ttl_on_key(batch, b"rmw:hll");
    out.clear();
    s.hyper_log_log_add(&[b"rmw:hll", b"e2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    assert_ttl_kept(batch, b"rmw:hll", exp);
  });
}

/// PFMERGE 目标键保留既有 TTL（对标 C# HyperLogLogMerge 走 RMW 面）
#[test]
fn pfmerge_preserves_dest_ttl() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();
    s.hyper_log_log_add(&[b"pm:src", b"a"], batch, &mut out)
      .unwrap();
    s.hyper_log_log_add(&[b"pm:dst", b"b"], batch, &mut out)
      .unwrap();
    let exp = seed_ttl_on_key(batch, b"pm:dst");
    out.clear();
    s.hyper_log_log_merge(&[b"pm:dst", b"pm:src"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    assert_ttl_kept(batch, b"pm:dst", exp);
  });
}

/// 集合对象 RMW 回写保留既有 TTL：ZADD/HSET/SADD/LPUSH（信封域默认保留）
#[test]
fn object_rmw_writeback_preserves_ttl() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();

    s.sorted_set_add(&[b"obj:z", b"1", b"m1"], batch, &mut out)
      .unwrap();
    let exp = seed_ttl_on_key(batch, b"obj:z");
    out.clear();
    s.sorted_set_add(&[b"obj:z", b"2", b"m2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    assert_ttl_kept(batch, b"obj:z", exp);

    s.hash_set(&[b"obj:h", b"f1", b"v1"], batch, &mut out)
      .unwrap();
    let exp = seed_ttl_on_key(batch, b"obj:h");
    out.clear();
    s.hash_set(&[b"obj:h", b"f2", b"v2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    assert_ttl_kept(batch, b"obj:h", exp);

    s.set_add(&[b"obj:s", b"a"], batch, &mut out).unwrap();
    let exp = seed_ttl_on_key(batch, b"obj:s");
    out.clear();
    s.set_add(&[b"obj:s", b"b"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");
    assert_ttl_kept(batch, b"obj:s", exp);

    s.list_push(&[b"obj:l", b"e1"], batch, &mut out, true)
      .unwrap();
    let exp = seed_ttl_on_key(batch, b"obj:l");
    out.clear();
    s.list_push(&[b"obj:l", b"e2"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":2\r\n");
    assert_ttl_kept(batch, b"obj:l", exp);
  });
}

/// SMOVE 目标键保留 TTL（对标 C# 存储侧 SetMove 全 RMW 链）
#[test]
fn smove_preserves_dst_ttl() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"mv:src", b"m"], batch, &mut out).unwrap();
    s.set_add(&[b"mv:dst", b"x"], batch, &mut out).unwrap();
    let exp = seed_ttl_on_key(batch, b"mv:dst");
    out.clear();
    s.set_move(&[b"mv:src", b"mv:dst", b"m"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    assert_ttl_kept(batch, b"mv:dst", exp);
  });
}

/// SET 语义清 TTL：SET / GETSET 覆写后 TTL 移除；SETNX 对存活键失败不动 TTL
#[test]
fn set_semantics_clears_ttl() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();
    seed_ttl_key(batch, b"set:k", b"old");
    s.network_set(&[b"set:k", b"new"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");
    assert_ttl_cleared(batch, b"set:k");

    seed_ttl_key(batch, b"set:getset", b"old");
    out.clear();
    s.network_getset(&[b"set:getset", b"new"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$3\r\nold\r\n");
    assert_ttl_cleared(batch, b"set:getset");

    seed_ttl_key(batch, b"set:nx", b"old");
    out.clear();
    s.network_setnx(&[b"set:nx", b"new"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n", "SETNX 对存活键失败");
    // 失败不写不清：TTL 原样
    let exp = ttl_of_sync(batch, b"set:nx")
      .unwrap()
      .value()
      .unwrap()
      .unwrap();
    assert_ttl_kept(batch, b"set:nx", exp);
  });
}

/// STORE 族目标键 SET 语义清 TTL：ZRANGESTORE / ZINTERSTORE / ZUNIONSTORE /
/// ZDIFFSTORE / SINTERSTORE / SUNIONSTORE / SDIFFSTORE / GEOSEARCHSTORE
///（对标 C# SetOps SET 收尾与 SortedSetRangeStore 先 Delete dst 再 ZADD）
#[test]
fn store_family_dest_clears_ttl() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();

    // 源集合：st:s1 = {a:1}，st:s2 = {a:2}
    s.sorted_set_add(&[b"st:s1", b"1", b"a"], batch, &mut out)
      .unwrap();
    s.sorted_set_add(&[b"st:s2", b"2", b"a"], batch, &mut out)
      .unwrap();

    // ZRANGESTORE
    s.sorted_set_add(&[b"st:zr", b"9", b"old"], batch, &mut out)
      .unwrap();
    let _ = seed_ttl_key(batch, b"st:zr", b"");
    out.clear();
    s.sorted_set_range_store(&[b"st:zr", b"st:s1", b"0", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    assert_ttl_cleared(batch, b"st:zr");

    // ZINTERSTORE / ZUNIONSTORE / ZDIFFSTORE
    for dst in ["st:zi", "st:zu", "st:zd"] {
      s.sorted_set_add(&[dst.as_bytes(), b"9", b"old"], batch, &mut out)
        .unwrap();
      let _ = seed_ttl_key(batch, dst.as_bytes(), b"");
    }
    out.clear();
    s.sorted_set_intersect_store(&[b"st:zi", b"2", b"st:s1", b"st:s2"], batch, &mut out)
      .unwrap();
    s.sorted_set_union_store(&[b"st:zu", b"2", b"st:s1", b"st:s2"], batch, &mut out)
      .unwrap();
    s.sorted_set_difference_store(&[b"st:zd", b"2", b"st:s1", b"st:s2"], batch, &mut out)
      .unwrap();
    for dst in ["st:zi", "st:zu", "st:zd"] {
      assert_ttl_cleared(batch, dst.as_bytes());
    }

    // SINTERSTORE / SUNIONSTORE / SDIFFSTORE
    s.set_add(&[b"st:c1", b"a"], batch, &mut out).unwrap();
    s.set_add(&[b"st:c2", b"a"], batch, &mut out).unwrap();
    for dst in ["st:si", "st:su", "st:sd"] {
      s.set_add(&[dst.as_bytes(), b"old"], batch, &mut out)
        .unwrap();
      let _ = seed_ttl_key(batch, dst.as_bytes(), b"");
    }
    out.clear();
    s.set_intersect_store(&[b"st:si", b"st:c1", b"st:c2"], batch, &mut out)
      .unwrap();
    s.set_union_store(&[b"st:su", b"st:c1", b"st:c2"], batch, &mut out)
      .unwrap();
    s.set_diff_store(&[b"st:sd", b"st:c1", b"st:c2"], batch, &mut out)
      .unwrap();
    for dst in ["st:si", "st:su", "st:sd"] {
      assert_ttl_cleared(batch, dst.as_bytes());
    }

    // GEOSEARCHSTORE
    s.geo_add(
      &[b"st:gsrc", b"15.08", b"37.30", b"catania"],
      batch,
      &mut out,
    )
    .unwrap();
    s.sorted_set_add(&[b"st:gdst", b"9", b"old"], batch, &mut out)
      .unwrap();
    let _ = seed_ttl_key(batch, b"st:gdst", b"");
    out.clear();
    s.geo_search_commands(
      &[
        b"st:gdst",
        b"st:gsrc",
        b"FROMLONLAT",
        b"15.08",
        b"37.30",
        b"BYRADIUS",
        b"200",
        b"km",
        b"ASC",
      ],
      batch,
      &mut out,
      GeoSearchCommandKind::GeoSearchStore,
    )
    .unwrap();
    assert_eq!(out, b":1\r\n", "GEOSEARCHSTORE 回命中数");
    assert_ttl_cleared(batch, b"st:gdst");
  });
}

/// EXPIRE 过去时间戳：物理删键回 :1（C# 惰性过期同回 :1，终态等价）；
/// 重放 DEL 幂等（AOF 镜像重放面同形）
#[test]
fn expire_past_timestamp_deletes_and_replay_is_idempotent() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();
    seed_ttl_key(batch, b"exp:past", b"v");

    // EXPIRE k 0：过期时刻即当前，is_expired_or_now 含相等 → 物理删除
    s.network_expire(ExpireCmd::Expire, &[b"exp:past", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n", "过去时间戳必须回 :1（Redis 7.4 语义）");
    out.clear();
    s.network_exists(&[b"exp:past"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n", "过去时间戳后键必须不可见");
    assert_eq!(
      ttl_of_sync(batch, b"exp:past").unwrap().value(),
      Some(None),
      "过去时间戳后 TTL 记录必须随键清除"
    );

    // AOF 重放幂等：主端已物理删除时重放删除面零副作用仍闭环
    let replay = batch.try_delete_sync(b"exp:past").unwrap();
    assert!(replay.is_ok(), "重放 DEL 必须同步闭环");
    out.clear();
    s.network_ttl(TtlCmd::Ttl, &[b"exp:past"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-2\r\n");
  });
}

/// EXPIRE 未来时间戳正常落 TTL 记录（对照臂）；del_ttl_sync 收口不受影响
#[test]
fn expire_future_timestamp_stores_ttl() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();
    seed_ttl_key(batch, b"exp:live", b"v");
    out.clear();
    s.network_expire(ExpireCmd::Expire, &[b"exp:live", b"120"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    let exp = ttl_of_sync(batch, b"exp:live")
      .unwrap()
      .value()
      .unwrap()
      .expect("未来时刻 TTL 必须在场");
    assert!(exp > now_ticks(), "TTL 必须在未来");
    assert!(del_ttl_sync(batch, b"exp:live").unwrap());
    assert_eq!(ttl_of_sync(batch, b"exp:live").unwrap().value(), Some(None));
  });
}

// ---- 粗化单源收口回归（键级 4-bit coarse 只在命令/会话入口各施加一次：
// 同步快路径 network_expire、异步外部路径 wkv expire_at；存储内核
// put_ttl_sync / put_ttl 裸写不判。绝对域秒/毫秒→ticks 换算天然 16 对齐，
// 粗化对之为恒等，焊死与换算单点逐位相等即同域锁定）

/// 主端 EXPIRE/PEXPIRE 相对域落库低 4 位恒零（network_expire 命令边界粗化，
/// 对标 C# NetworkEXPIRE 打包侧 ExpirationWithOption.cs:22-23）；EXPIREAT
/// 绝对域落库与 wbase::convert 换算单点逐位相等（AOF 重放端同函数同域）
#[test]
fn expire_family_stored_ticks_coarse_at_command_entry() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();
    // 相对域：now_ticks() + 秒/毫秒偏移，输入低 4 位随机非零，落库必须清零
    for (cmd, key, arg) in [
      (ExpireCmd::Expire, &b"coarse:exp"[..], &b"60"[..]),
      (ExpireCmd::Pexpire, &b"coarse:pexp"[..], &b"60000"[..]),
    ] {
      batch.try_upsert_sync(key, b"v").unwrap().unwrap();
      out.clear();
      s.network_expire(cmd, &[key, arg], batch, &mut out).unwrap();
      assert_eq!(out, b":1\r\n");
      let stored = ttl_of_sync(batch, key)
        .unwrap()
        .value()
        .unwrap()
        .expect("TTL 在场");
      assert_eq!(stored & 0xF, 0, "{key:?} 落库低 4 位必须为粗化清零");
    }
    // 绝对域：秒→ticks 恒 16 对齐，落库与换算单点逐位相等
    const UNIX_SECS: i64 = 2_500_000_000;
    batch
      .try_upsert_sync(b"coarse:reat", b"v")
      .unwrap()
      .unwrap();
    out.clear();
    s.network_expire(
      ExpireCmd::Expireat,
      &[b"coarse:reat", b"2500000000"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":1\r\n");
    assert_eq!(
      ttl_of_sync(batch, b"coarse:reat")
        .unwrap()
        .value()
        .unwrap()
        .expect("TTL 在场"),
      expire_at_seconds_to_ticks(UNIX_SECS),
      "EXPIREAT 落库必须与换算单点逐位相等（粗化恒等）"
    );
  });
}

/// RENAME 存量 TTL 逐位相等迁移、不套粗化（对标 C# UnifiedStoreOps.cs:363
/// 旧记录 expiration optional 随 logRecord 原样迁移）：种子 TTL 特意取低 4
/// 位非零的裸值（put_ttl_sync 裸写内核直落），迁移后必须一位不差
#[test]
fn rename_migrates_ttl_bitwise_without_coarsening() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();
    batch.try_upsert_sync(b"cg:old", b"v").unwrap().unwrap();
    let seed = now_ticks() + 60 * TICKS_PER_SECOND + 0b1011;
    put_ttl_sync(batch, b"cg:old", seed).unwrap();
    out.clear();
    s.network_rename(&[b"cg:old", b"cg:new"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    assert_eq!(
      ttl_of_sync(batch, b"cg:new")
        .unwrap()
        .value()
        .unwrap()
        .expect("新键 TTL 在场"),
      seed,
      "RENAME 迁移必须逐位相等，不得二次粗化"
    );
    assert_eq!(
      ttl_of_sync(batch, b"cg:old").unwrap().value(),
      Some(None),
      "旧键 TTL 须随键删除"
    );
  });
}

/// SET EX / GETEX 落库不被粗化（对应 C# MainStore/RMWMethods.cs
/// TrySetExpiration / EvaluateExpire* 裸 ticks 路径，判据边界与
/// network_expire 粗化单点互斥）：相对域命令落库时刻低 4 位非零概率 15/16，
/// 多样本至少出现一次非零即证无掩码铺到 SET 族（防后续有人「顺手」一律粗化）；
/// EXPIRE 族同臂对照：低 4 位恒零
#[test]
fn set_ex_getex_stored_ticks_not_coarsened() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();
    let mut saw_low_bits_set = 0usize;
    // SET key val EX 60：expiry_ticks_from_now 相对域，低 4 位随机
    for key in [b"nocg:s0".as_slice(), b"nocg:s1", b"nocg:s2", b"nocg:s3"] {
      out.clear();
      s.network_set(&[key, b"v", b"EX", b"60"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");
      let stored = ttl_of_sync(batch, key)
        .unwrap()
        .value()
        .unwrap()
        .expect("SET EX 后 TTL 在场");
      assert!(stored > now_ticks(), "落库时刻必须在未来");
      saw_low_bits_set += usize::from(stored & 0xF != 0);
    }
    // GETEX key EX 60：compute_relative_expiry 相对域，同样裸 ticks
    for key in [b"nocg:g0".as_slice(), b"nocg:g1", b"nocg:g2", b"nocg:g3"] {
      batch.try_upsert_sync(key, b"v").unwrap().unwrap();
      out.clear();
      s.network_getex(&[key, b"EX", b"60"], batch, &mut out)
        .unwrap();
      let stored = ttl_of_sync(batch, key)
        .unwrap()
        .value()
        .unwrap()
        .expect("GETEX EX 后 TTL 在场");
      assert!(stored > now_ticks(), "落库时刻必须在未来");
      saw_low_bits_set += usize::from(stored & 0xF != 0);
    }
    // 8 样本全 16 对齐的概率 (1/16)^8 ≈ 2.3e-10，可视为确定性判别
    assert!(
      saw_low_bits_set > 0,
      "SET/GETEX 相对域落库必须保持全精度（出现低 4 位非零样本）"
    );
    // 对照臂：EXPIRE 族同域命令落库低 4 位恒零（粗化只归 EXPIRE 族）
    batch.try_upsert_sync(b"nocg:e0", b"v").unwrap().unwrap();
    out.clear();
    s.network_expire(ExpireCmd::Expire, &[b"nocg:e0", b"60"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    let stored = ttl_of_sync(batch, b"nocg:e0")
      .unwrap()
      .value()
      .unwrap()
      .expect("EXPIRE 后 TTL 在场");
    assert_eq!(stored & 0xF, 0, "EXPIRE 族落库低 4 位必须为粗化清零");
  });
}

/// GT 判定与落盘同域（确定性绝对域构造）：EXPIREAT 固定秒置入 16 对齐存量值，
/// 再连发两次同值 EXPIREAT GT，输入换算与落库比较逐位相等，按严格大于口径
/// 两次一致拒绝——判定与写入同用粗化后值域，无「一次 :1 一次 :0」的值域分叉
#[test]
fn expire_gt_same_value_rejected_same_domain() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();
    batch.try_upsert_sync(b"gt:rep", b"v").unwrap().unwrap();
    out.clear();
    s.network_expire(
      ExpireCmd::Expireat,
      &[b"gt:rep", b"2500000000"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":1\r\n");
    for round in 1..=2 {
      out.clear();
      s.network_expire(
        ExpireCmd::Expireat,
        &[b"gt:rep", b"2500000000", b"GT"],
        batch,
        &mut out,
      )
      .unwrap();
      assert_eq!(out, b":0\r\n", "第 {round} 轮同值 GT 必须按严格大于拒绝");
    }
  });
}

// ---- 过期重建组（写入即幽灵回归：RMW 对已过期键重建必须清退残留 TTL，
// 新值立即可见且无 TTL。对标 C# CheckExpiry → ExpireAndResume 后转
// InitialUpdater，初始记录无 Expiration）

/// 给键设已过期的 TTL 记录（过去时刻，put_ttl_sync 裸写直落，仍为过去）
fn seed_expired_ttl(batch: &TestBatch, key: &[u8]) {
  put_ttl_sync(batch, key, now_ticks() - TICKS_PER_SECOND).unwrap();
}

/// 断言键 TTL 记录已随过期重建清退（残留 TTL 若在，新值写完立即可判过期）
fn assert_expired_ttl_purged(batch: &TestBatch, key: &[u8]) {
  assert_eq!(
    ttl_of_sync(batch, key).unwrap().value(),
    Some(None),
    "过期重建必须清退残留 TTL 记录（{key:?}）"
  );
}

/// 字符串 RMW 族对已过期键重建：新值无 TTL 且 GET 可见（INCR 回 :1 /
/// INCRBYFLOAT / APPEND / SETRANGE / SETBIT / BITFIELD / PFADD）
#[test]
fn rmw_after_expiry_rebuilds_without_ttl() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();

    // INCR：SETEX 种子过期后回 :1，重建值 :1 且 GET 可见
    batch.try_upsert_sync(b"exp:cnt", b"9").unwrap().unwrap();
    seed_expired_ttl(batch, b"exp:cnt");
    out.clear();
    s.network_increment(IncrCmd::Incr, &[b"exp:cnt"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n", "过期键 INCR 必须按缺失重建回 :1");
    assert_expired_ttl_purged(batch, b"exp:cnt");
    out.clear();
    s.network_get(&[b"exp:cnt"], batch, &mut out).unwrap();
    assert_eq!(out, b"$1\r\n1\r\n", "重建后 GET 必须立即可见");

    // INCRBYFLOAT：过期键按缺失自 0 重建（0 + 0.25）
    batch.try_upsert_sync(b"exp:flt", b"1.5").unwrap().unwrap();
    seed_expired_ttl(batch, b"exp:flt");
    out.clear();
    s.network_increment_by_float(&[b"exp:flt", b"0.25"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$4\r\n0.25\r\n", "过期键 INCRBYFLOAT 必须自 0 重建");
    assert_expired_ttl_purged(batch, b"exp:flt");
    out.clear();
    s.network_get(&[b"exp:flt"], batch, &mut out).unwrap();
    assert_eq!(out, b"$4\r\n0.25\r\n");

    // APPEND（命中臂读侧判缺失 → Missing 臂重建）
    batch.try_upsert_sync(b"exp:app", b"ab").unwrap().unwrap();
    seed_expired_ttl(batch, b"exp:app");
    out.clear();
    s.network_append(&[b"exp:app", b"cd"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n", "过期键 APPEND 按缺失重建（只含新增段）");
    assert_expired_ttl_purged(batch, b"exp:app");
    out.clear();
    s.network_get(&[b"exp:app"], batch, &mut out).unwrap();
    assert_eq!(out, b"$2\r\ncd\r\n");

    // SETRANGE
    batch
      .try_upsert_sync(b"exp:sr", b"abcdef")
      .unwrap()
      .unwrap();
    seed_expired_ttl(batch, b"exp:sr");
    out.clear();
    s.network_set_range(&[b"exp:sr", b"1", b"XY"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":3\r\n");
    assert_expired_ttl_purged(batch, b"exp:sr");
    out.clear();
    s.network_get(&[b"exp:sr"], batch, &mut out).unwrap();
    // 过期重建零填充：旧值 ab cdef 已亡，[0, 'X', 'Y']
    assert_eq!(out, b"$3\r\n\x00XY\r\n");

    // SETBIT
    batch.try_upsert_sync(b"exp:bit", b"\x00").unwrap().unwrap();
    seed_expired_ttl(batch, b"exp:bit");
    out.clear();
    s.network_string_set_bit(&[b"exp:bit", b"0", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n", "过期键 SETBIT 旧位按缺失回 :0");
    assert_expired_ttl_purged(batch, b"exp:bit");
    out.clear();
    s.network_get(&[b"exp:bit"], batch, &mut out).unwrap();
    // bit 0 为字节最高位（大端位序）→ 0x80
    assert_eq!(out, b"$1\r\n\x80\r\n");

    // BITFIELD SET 写子命令
    batch.try_upsert_sync(b"exp:bf", b"\x00").unwrap().unwrap();
    seed_expired_ttl(batch, b"exp:bf");
    out.clear();
    s.string_bit_field(&[b"exp:bf", b"SET", b"u8", b"0", b"7"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n", "过期键 BITFIELD 旧值按缺失回 :0");
    assert_expired_ttl_purged(batch, b"exp:bf");

    // PFADD（对象域 HLL 快路径同走 RMW 收口）
    s.hyper_log_log_add(&[b"exp:hll", b"e1"], batch, &mut out)
      .unwrap();
    seed_expired_ttl(batch, b"exp:hll");
    out.clear();
    s.hyper_log_log_add(&[b"exp:hll", b"e2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    assert_expired_ttl_purged(batch, b"exp:hll");
  });
}

/// 集合对象 RMW 对已过期键重建（对象面信封写收口）：HSET/SADD 重建后 HGET/
/// SMEMBERS 可见且 TTL 清退
#[test]
fn object_rmw_after_expiry_rebuilds_without_ttl() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();

    s.hash_set(&[b"expobj:h", b"f1", b"v1"], batch, &mut out)
      .unwrap();
    seed_expired_ttl(batch, b"expobj:h");
    out.clear();
    s.hash_set(&[b"expobj:h", b"f2", b"v2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n", "过期对象键 HSET 按缺失重建");
    assert_expired_ttl_purged(batch, b"expobj:h");
    out.clear();
    s.hash_get(&[b"expobj:h", b"f2"], batch, &mut out).unwrap();
    assert_eq!(out, b"$2\r\nv2\r\n", "重建后 HGET 必须立即可见");
    out.clear();
    s.hash_get(&[b"expobj:h", b"f1"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n", "旧字段随过期重建消失");

    s.set_add(&[b"expobj:s", b"a"], batch, &mut out).unwrap();
    seed_expired_ttl(batch, b"expobj:s");
    out.clear();
    s.set_add(&[b"expobj:s", b"b"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");
    assert_expired_ttl_purged(batch, b"expobj:s");
  });
}

/// 无 TTL 记录键 RMW 零探针回归：过期清退门对无 TTL 键只付单次哈希探针，
/// 行为不变（写入成功、无 TTL 记录产生）
#[test]
fn rmw_without_ttl_record_untouched() {
  with_test_env(|s, batch| {
    let mut out = Vec::new();
    batch.try_upsert_sync(b"plain:cnt", b"1").unwrap().unwrap();
    out.clear();
    s.network_increment(IncrCmd::Incr, &[b"plain:cnt"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");
    assert_eq!(
      ttl_of_sync(batch, b"plain:cnt").unwrap().value(),
      Some(None),
      "无 TTL 键 RMW 不得产生 TTL 记录"
    );
    out.clear();
    s.network_get(&[b"plain:cnt"], batch, &mut out).unwrap();
    assert_eq!(out, b"$1\r\n2\r\n");
  });
}
