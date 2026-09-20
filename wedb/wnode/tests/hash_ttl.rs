//! hash 字段级 TTL 全链路集成测试（信封路径，对标 Redis 7.4 HEXPIRE 系 /
//! Garnet test/standalone/Garnet.test.collections/RespHashTests.cs 的
//! CanDoHashExpire / CanDoHashExpireWithOptions）
//!
//! 覆盖：HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT 四命令逐字段返回码（1/0/-1/-2）
//! 与 NX/XX/GT/LT 四条件矩阵、HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME 读回、
//! HPERSIST 四态、到期字段读路径惰性 purge（不可见 + 计数缩减）、
//! 过去时间戳立即删字段（返回 2）、条件拒绝后字段值原样保留、WRONGTYPE 传播。

use std::{thread::sleep, time::Duration};

use wbase::{
  convert::{TICKS_PER_MILLISECOND, TICKS_PER_SECOND, UNIX_EPOCH_TICKS},
  time::now_ticks,
};
use wnode_test::with_batch;

/// 当前 unix 秒（.NET ticks → unix 纪元换算）
fn unix_now_sec() -> i64 {
  (now_ticks() - UNIX_EPOCH_TICKS) / TICKS_PER_SECOND
}

/// 当前 unix 毫秒
fn unix_now_ms() -> i64 {
  (now_ticks() - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND
}

/// 解析 `*N` + `:int` 应答为整数数组
fn parse_int_array(frame: &[u8]) -> Vec<i64> {
  let mut items = Vec::new();
  let mut pos = match frame.iter().position(|&b| b == b'\n') {
    Some(p) => p + 1,
    None => return items,
  };
  while pos < frame.len() && frame[pos] == b':' {
    let end = frame[pos..].iter().position(|&b| b == b'\r').unwrap() + pos;
    items.push(
      str::from_utf8(&frame[pos + 1..end])
        .unwrap()
        .parse()
        .unwrap(),
    );
    pos = end + 2;
  }
  items
}

/// 解析 `*N` + 成对批量串应答为 (field, value) 数组
fn parse_pair_array(frame: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
  let mut items = Vec::new();
  let mut pos = match frame.iter().position(|&b| b == b'\n') {
    Some(p) => p + 1,
    None => return items,
  };
  while pos < frame.len() && frame[pos] == b'$' {
    let read_bulk = |pos: &mut usize| -> Vec<u8> {
      let len_end = frame[*pos..].iter().position(|&b| b == b'\n').unwrap() + *pos;
      let len: usize = str::from_utf8(&frame[*pos + 1..len_end - 1])
        .unwrap()
        .parse()
        .unwrap();
      let start = len_end + 1;
      *pos = start + len + 2;
      frame[start..start + len].to_vec()
    };
    let field = read_bulk(&mut pos);
    if pos < frame.len() && frame[pos] == b'$' {
      let value = read_bulk(&mut pos);
      items.push((field, value));
    } else {
      items.push((field, Vec::new()));
    }
  }
  items
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHashExpire
/// 四命令设过期 + 四命令读回 + HPERSIST 四态 + 到期惰性 purge。
#[test]
fn can_do_hash_expire() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[
        b"myhash", b"field1", b"hello", b"field2", b"world", b"field3", b"value3", b"field4",
        b"value4", b"field5", b"value5", b"field6", b"value6",
      ],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":6\r\n");
    out.clear();

    // HEXPIRE：存活字段 1、不存在字段 -2
    s.hash_expire(
      "HEXPIRE",
      &[
        b"myhash",
        b"60",
        b"FIELDS",
        b"3",
        b"field1",
        b"field5",
        b"nonexistfield",
      ],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![1, 1, -2]);
    out.clear();

    // HPEXPIRE
    s.hash_expire(
      "HPEXPIRE",
      &[
        b"myhash",
        b"60000",
        b"FIELDS",
        b"2",
        b"field2",
        b"nonexistfield",
      ],
      batch,
      &mut out,
      true,
      false,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![1, -2]);
    out.clear();

    // HEXPIREAT（unix 秒）
    let at = unix_now_sec() + 60;
    s.hash_expire(
      "HEXPIREAT",
      &[
        b"myhash",
        at.to_string().as_bytes(),
        b"FIELDS",
        b"2",
        b"field3",
        b"nonexistfield",
      ],
      batch,
      &mut out,
      false,
      true,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![1, -2]);
    out.clear();

    // HPEXPIREAT（unix 毫秒）
    let at_ms = unix_now_ms() + 60_000;
    s.hash_expire(
      "HPEXPIREAT",
      &[
        b"myhash",
        at_ms.to_string().as_bytes(),
        b"FIELDS",
        b"2",
        b"field4",
        b"nonexistfield",
      ],
      batch,
      &mut out,
      true,
      true,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![1, -2]);
    out.clear();

    // HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME 读回（窗口断言）
    s.hash_time_to_live(
      "HTTL",
      &[b"myhash", b"FIELDS", b"2", b"field1", b"nonexistfield"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    let ttl = parse_int_array(&out);
    assert_eq!(ttl.len(), 2);
    assert!(ttl[0] > 1 && ttl[0] <= 60, "HTTL window, got {}", ttl[0]);
    assert_eq!(ttl[1], -2);
    out.clear();

    s.hash_time_to_live(
      "HPTTL",
      &[b"myhash", b"FIELDS", b"2", b"field1", b"nonexistfield"],
      batch,
      &mut out,
      true,
      false,
    )
    .unwrap();
    let ttl = parse_int_array(&out);
    assert!(
      ttl[0] > 1000 && ttl[0] <= 60_000,
      "HPTTL window, got {}",
      ttl[0]
    );
    out.clear();

    let now_sec = unix_now_sec();
    s.hash_time_to_live(
      "HEXPIRETIME",
      &[b"myhash", b"FIELDS", b"2", b"field1", b"nonexistfield"],
      batch,
      &mut out,
      false,
      true,
    )
    .unwrap();
    let exp = parse_int_array(&out);
    assert!(
      exp[0] > now_sec && exp[0] <= now_sec + 60,
      "HEXPIRETIME window, got {} exp={:?} now={now_sec}",
      exp[0],
      exp
    );
    out.clear();

    let now_ms = unix_now_ms();
    s.hash_time_to_live(
      "HPEXPIRETIME",
      &[b"myhash", b"FIELDS", b"2", b"field1", b"nonexistfield"],
      batch,
      &mut out,
      true,
      true,
    )
    .unwrap();
    let exp = parse_int_array(&out);
    assert!(
      exp[0] > now_ms && exp[0] <= now_ms + 60_000,
      "HPEXPIRETIME window, got {}",
      exp[0]
    );
    out.clear();

    // HPERSIST：已设 1 / 未设 -1 / 不存在 -2
    s.hash_persist(
      &[
        b"myhash",
        b"FIELDS",
        b"3",
        b"field5",
        b"field6",
        b"nonexistfield",
      ],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![1, -1, -2]);
    out.clear();

    // field1-4 统一短过期 → 惰性 purge 后仅剩 field5/field6
    //（HPERSIST 已移除 field5 过期、field6 从未设置）
    s.hash_expire(
      "HEXPIRE",
      &[
        b"myhash", b"1", b"FIELDS", b"4", b"field1", b"field2", b"field3", b"field4",
      ],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![1, 1, 1, 1]);
    out.clear();
    sleep(Duration::from_millis(1200));

    s.hash_get_all(&[b"myhash"], batch, &mut out).unwrap();
    let mut items = parse_pair_array(&out);
    items.sort();
    assert_eq!(
      items,
      vec![
        (b"field5".to_vec(), b"value5".to_vec()),
        (b"field6".to_vec(), b"value6".to_vec()),
      ],
      "过期字段必须对读取不可见"
    );
    out.clear();

    // 过期时长 0 / 过去时间戳 → 立即删字段，返回 2
    s.hash_expire(
      "HEXPIRE",
      &[b"myhash", b"0", b"FIELDS", b"1", b"field5"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![2]);
    out.clear();

    let past = unix_now_sec() - 1;
    s.hash_expire(
      "HEXPIREAT",
      &[
        b"myhash",
        past.to_string().as_bytes(),
        b"FIELDS",
        b"1",
        b"field6",
      ],
      batch,
      &mut out,
      false,
      true,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![2]);
    out.clear();

    s.hash_get_all(&[b"myhash"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n", "全字段过期后集合为空");
  });
}

/// 命令形态行：(命令名, 毫秒口径, 时间戳口径, 短 TTL, 长 TTL, 待验 TTL)
type ExpireForm<'a> = (&'a str, bool, bool, &'a str, &'a str, &'a str);
/// 条件期望行：(条件, [field1, field2(无 TTL), field3] 应答)
type OptionExpect<'a> = (&'a [u8], [i64; 3]);

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHashExpireWithOptions
/// NX/XX/GT/LT 条件矩阵 × HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT 四命令形态。
#[test]
fn can_do_hash_expire_with_options() {
  with_batch(|s, batch| {
    let forms: &[ExpireForm] = &[
      ("HEXPIRE", false, false, "2", "6", "4"),
      ("HPEXPIRE", true, false, "2000", "6000", "4000"),
      ("HEXPIREAT", false, true, "2", "6", "4"),
      ("HPEXPIREAT", true, true, "2000", "6000", "4000"),
    ];
    let matrix: &[OptionExpect] = &[
      (b"NX", [0, 1, 0]),
      (b"XX", [1, 0, 1]),
      (b"GT", [1, 0, 0]),
      (b"LT", [0, 1, 1]),
    ];

    for (cmd_name, is_ms, is_ts, t1, t3, t_new) in forms {
      for (option, expected) in matrix {
        let key = format!(
          "h:{}:{}-{}",
          cmd_name,
          unsafe { str::from_utf8_unchecked(option) },
          t1
        );
        let mut out = Vec::new();
        s.hash_set(
          &[
            key.as_bytes(),
            b"field1",
            b"hello",
            b"field2",
            b"world",
            b"field3",
            b"welcome",
            b"field4",
            b"back",
          ],
          batch,
          &mut out,
        )
        .unwrap();
        assert_eq!(out, b":4\r\n");
        out.clear();

        // 先为 field1 / field3 设短/长两档 TTL
        let (arg1, arg3, arg_new) = if *is_ts {
          // 时间戳形态：秒/毫秒 unix 时刻
          let scale = if *is_ms {
            TICKS_PER_MILLISECOND
          } else {
            TICKS_PER_SECOND
          };
          let now = now_ticks() / scale;
          let (a1, a3, an) = (
            t1.parse::<i64>().unwrap(),
            t3.parse::<i64>().unwrap(),
            t_new.parse::<i64>().unwrap(),
          );
          (
            (now + a1).to_string().into_bytes(),
            (now + a3).to_string().into_bytes(),
            (now + an).to_string().into_bytes(),
          )
        } else {
          (
            t1.as_bytes().to_vec(),
            t3.as_bytes().to_vec(),
            t_new.as_bytes().to_vec(),
          )
        };

        s.hash_expire(
          cmd_name,
          &[key.as_bytes(), &arg1, b"FIELDS", b"1", b"field1"],
          batch,
          &mut out,
          *is_ms,
          *is_ts,
        )
        .unwrap();
        assert_eq!(parse_int_array(&out), vec![1], "{cmd_name:?} 预设 field1");
        out.clear();
        s.hash_expire(
          cmd_name,
          &[key.as_bytes(), &arg3, b"FIELDS", b"1", b"field3"],
          batch,
          &mut out,
          *is_ms,
          *is_ts,
        )
        .unwrap();
        assert_eq!(parse_int_array(&out), vec![1], "{cmd_name:?} 预设 field3");
        out.clear();

        // 带条件设置：field1(短 TTL) / field2(无 TTL) / field3(长 TTL)
        s.hash_expire(
          cmd_name,
          &[
            key.as_bytes(),
            &arg_new,
            option,
            b"FIELDS",
            b"3",
            b"field1",
            b"field2",
            b"field3",
          ],
          batch,
          &mut out,
          *is_ms,
          *is_ts,
        )
        .unwrap();
        assert_eq!(
          parse_int_array(&out),
          expected.to_vec(),
          "{cmd_name:?} {option:?} 条件矩阵"
        );
        out.clear();

        // 条件拒绝后字段值原样保留（绝不误删）
        s.hash_get(&[key.as_bytes(), b"field1"], batch, &mut out)
          .unwrap();
        assert_eq!(
          out, b"$5\r\nhello\r\n",
          "{cmd_name:?} {option:?} field1 值必须保留"
        );
        out.clear();

        // field2 全程无 TTL：C# SetExpiration 的 XX/GT 拒绝臂因
        // GetValueRefOrAddDefault 先插 0 值幻影项、字段随后被判恒过期而
        // 消失（HashObject.cs:581-610），rust 只读探测不复刻该缺陷，
        // 断言值与 HLEN 存活，任何改回 C# 形态的改动都须在此撞红
        s.hash_get(&[key.as_bytes(), b"field2"], batch, &mut out)
          .unwrap();
        assert_eq!(
          out, b"$5\r\nworld\r\n",
          "{cmd_name:?} {option:?} field2（无 TTL）值必须保留，C# 拒绝臂会幻影删除，rust 不复刻"
        );
        out.clear();

        s.hash_length(&[key.as_bytes()], batch, &mut out).unwrap();
        assert_eq!(
          out, b":4\r\n",
          "{cmd_name:?} {option:?} 条件拒绝后 HLEN 必须仍为 4（零字段丢失）"
        );
      }
    }
  });
}

/// 过期字段视同不存在：HEXPIRE / HPERSIST 对已过期字段返回 -2 且惰性 purge
/// （对标 C# IsExpired 语义 + 原存储层等价测试的 RESP 面投影）
#[test]
fn expired_field_treated_as_missing() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"ht:ef", b"live", b"v1", b"dead", b"v2"], batch, &mut out)
      .unwrap();
    out.clear();

    s.hash_expire(
      "HEXPIRE",
      &[b"ht:ef", b"1", b"FIELDS", b"1", b"dead"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![1]);
    sleep(Duration::from_millis(1200));
    out.clear();

    // 已过期字段对 HEXPIRE / HPERSIST 视同不存在（-2）
    s.hash_expire(
      "HEXPIRE",
      &[b"ht:ef", b"60", b"FIELDS", b"1", b"dead"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![-2]);
    out.clear();

    s.hash_persist(&[b"ht:ef", b"FIELDS", b"1", b"dead"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_int_array(&out), vec![-2]);
    out.clear();

    // HGET 同样不可见
    s.hash_get(&[b"ht:ef", b"dead"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
    out.clear();

    // HLEN 只计存活字段
    s.hash_length(&[b"ht:ef"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// HPERSIST / HTTL 的信封落盘验证（C# HashOps.HashPersist →
/// RMWObjectStoreOperation 落盘路径；HashTimeToLive 的 DeleteExpiredItems
/// 剔除对标常驻对象经 checkpoint 序列化收敛）。
/// 同 batch 内二次命令经 obj_load_typed_sync 重读存储载荷，可观测写回。
#[test]
fn hpersist_and_ttl_purge_persist_to_envelope() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(
      &[b"ht:pb", b"live", b"v1", b"ephemeral", b"v2"],
      batch,
      &mut out,
    )
    .unwrap();
    out.clear();

    s.hash_expire(
      "HEXPIRE",
      &[b"ht:pb", b"100", b"FIELDS", b"1", b"live"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![1]);
    out.clear();

    s.hash_expire(
      "HEXPIRE",
      &[b"ht:pb", b"1", b"FIELDS", b"1", b"ephemeral"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![1]);
    out.clear();

    sleep(Duration::from_millis(1200));

    // HPERSIST live：清除其过期元数据须落盘（元数据写命令，C# 为 RMW 路径），
    // 否则重装载后 live 在原到期时刻仍会过期消失
    s.hash_persist(&[b"ht:pb", b"FIELDS", b"1", b"live"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_int_array(&out), vec![1]);
    out.clear();

    // 重读存储载荷：live 已无过期元数据（-1），而非残余 ticks（>0）
    s.hash_time_to_live(
      "HTTL",
      &[b"ht:pb", b"FIELDS", b"1", b"live"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![-1]);
    out.clear();

    // HTTL 读回触发 ephemeral 惰性剔除：live 持久（-1）、ephemeral 不存在（-2）
    s.hash_time_to_live(
      "HTTL",
      &[b"ht:pb", b"FIELDS", b"2", b"live", b"ephemeral"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(parse_int_array(&out), vec![-1, -2]);
    out.clear();
  });
}

/// WRONGTYPE 传播：字符串键上的 HEXPIRE / HTTL / HPERSIST 显式报错
///（对标 RespHashTests.CheckCommandOnWrongTypeObject）
#[test]
fn hash_expire_on_wrong_type_errors() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"k", b"v"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");
    out.clear();

    s.hash_expire(
      "HEXPIRE",
      &[b"k", b"60", b"FIELDS", b"1", b"f"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert!(out.starts_with(b"-WRONGTYPE"), "got {out:?}");
    out.clear();

    s.hash_time_to_live(
      "HTTL",
      &[b"k", b"FIELDS", b"1", b"f"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert!(out.starts_with(b"-WRONGTYPE"), "got {out:?}");
    out.clear();

    s.hash_persist(&[b"k", b"FIELDS", b"1", b"f"], batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"-WRONGTYPE"), "got {out:?}");
  });
}

/// HEXPIRE 与 HTTL 族参数错误提示命令名对标真实命令名（对标 C# command.ToString()）
#[test]
fn test_hash_expire_and_ttl_error_command_name() {
  with_batch(|s, batch| {
    let mut out = Vec::new();

    for cmd in ["HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT"] {
      out.clear();
      s.hash_expire(cmd, &[b"k", b"10"], batch, &mut out, false, false)
        .unwrap();
      assert_eq!(
        out,
        format!("-ERR wrong number of arguments for '{cmd}' command\r\n").as_bytes()
      );
    }

    for cmd in ["HTTL", "HPTTL", "HEXPIRETIME", "HPEXPIRETIME"] {
      out.clear();
      s.hash_time_to_live(cmd, &[b"k", b"FIELDS"], batch, &mut out, false, false)
        .unwrap();
      assert_eq!(
        out,
        format!("-ERR wrong number of arguments for '{cmd}' command\r\n").as_bytes()
      );
    }
  });
}

/// FIELDS 0 零字段合法形态对位（C# HashCommands.cs:HashExpire 门序无 num 值域门：
/// NX 形态 Count==currIdx+0 放行、逐字段循环零次回 *0；多余实参/负数/极值落
/// must-match 臂；键缺失走 C# NOTFOUND 同型 *0；RESP2/RESP3 帧头一致）
#[test]
fn hash_expire_fields_zero_parity() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.hash_set(&[b"myhash", b"f1", b"v1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // HEXPIRE myhash 60 NX FIELDS 0 → *0（计数吻合，零字段执行）
    out.clear();
    s.hash_expire(
      "HEXPIRE",
      &[b"myhash", b"60", b"NX", b"FIELDS", b"0"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(out, b"*0\r\n");

    // 键缺失同回 *0（C# NOTFOUND 臂数组长度取 numFields=0）
    out.clear();
    s.hash_expire(
      "HEXPIRE",
      &[b"nokey", b"60", b"NX", b"FIELDS", b"0"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(out, b"*0\r\n");

    // 多余实参 → must match（不得抢答 greater-than-0）
    out.clear();
    s.hash_expire(
      "HEXPIRE",
      &[b"myhash", b"60", b"NX", b"FIELDS", b"0", b"x"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(
      out,
      b"-The `numFields` parameter must match the number of arguments\r\n"
    );

    // 负数与 i32 极值：debug 构建溢出护栏，稳定 must match
    for num in [
      b"-1".as_slice(),
      b"-2147483648".as_slice(),
      b"2147483647".as_slice(),
    ] {
      out.clear();
      s.hash_expire(
        "HEXPIRE",
        &[b"myhash", b"60", b"NX", b"FIELDS", num],
        batch,
        &mut out,
        false,
        false,
      )
      .unwrap();
      assert_eq!(
        out,
        b"-The `numFields` parameter must match the number of arguments\r\n"
      );
    }

    // RESP3 会话同帧（数组帧头协议间不变）
    s.resp_protocol_version = 3;
    out.clear();
    s.hash_expire(
      "HEXPIRE",
      &[b"myhash", b"60", b"XX", b"FIELDS", b"0"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}
