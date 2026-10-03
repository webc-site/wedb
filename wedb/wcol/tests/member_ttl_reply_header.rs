//! 成员级 TTL 容器读臂「声明数恒等写出数」回归（zcode-r27-wcolstruct 发现二）
//!
//! 原缺陷形：purge_expired_len（delete_expired_items 单次采样 t1 物理摘除）后
//! 按存活数写 RESP 头，迭代臂 is_expired 逐项重采样 t_i 过滤——expiry 落于
//! [t1, t_i) 的成员被计入头部声明却被跳过，声明数大于实际项数，客户端按声明
//! 读取即 RESP 流永久错位（C# WriteMapLength(Count()) + foreach IsExpired 双
//! 采样同窗；HRANDFIELD/ZRANDMEMBER 窗口内存活不足时 ElementAt 越界抛断流）。
//!
//! 修复形态：purge 后主容器已无 expiry<t1 存留项，读臂删除二次重采样过滤
//! 直接输出全部条目，声明数恒等写出数（修复型偏离，doc/zh/deviations.md）。
//!
//! 自研回归锁: 计数声明面行为锁（时钟窗口错位本身不可稳定注入，锁恒等不变式）

use std::{str::from_utf8, sync::Arc};

use wbase::time::now_ticks;
use wcol::{
  HashObject, HashOperation, ObjectOutput, SetObject, SetOperation, SortedSetObject,
  SortedSetOperation,
};
use wresp::options::ExpireOption;

const LIVE_SPAN: i64 = 1_000_000_000;
const EXPIRED_SPAN: i64 = 1_000;

/// 解析 RESP2 bulk 数组应答 → 项序列
fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
  let header_end = frame.iter().position(|&b| b == b'\n').expect("缺数组头");
  let length: usize = from_utf8(&frame[1..header_end - 1])
    .unwrap()
    .parse()
    .unwrap();
  let mut rest = &frame[header_end + 1..];
  let mut items = Vec::with_capacity(length);
  for _ in 0..length {
    let len_end = rest.iter().position(|&b| b == b'\n').expect("缺 bulk 头");
    let len: usize = from_utf8(&rest[1..len_end - 1]).unwrap().parse().unwrap();
    let body = &rest[len_end + 1..len_end + 1 + len];
    items.push(body.to_vec());
    rest = &rest[len_end + 1 + len + 2..];
  }
  assert!(
    rest.is_empty(),
    "应答项数与头部声明不符（尾随 {}/2 项未消费）",
    rest.len()
  );
  items
}

fn hash_with(fields: &[&[u8]]) -> HashObject {
  let mut hash = HashObject::new();
  for f in fields {
    hash.hash.insert(Arc::from(f.to_vec()), b"v".to_vec());
  }
  hash
}

/// 挂账混合态：a 存活挂期、b 陈旧过期（HGETALL/HKEYS/HVALS purge 时物理摘除）
fn hash_ttl_mixed() -> HashObject {
  let mut hash = hash_with(&[b"a", b"b", b"c", b"d"]);
  let now = now_ticks();
  hash.set_expiration(b"a", now + LIVE_SPAN, ExpireOption::NONE);
  // 过期字段经装载单点直挂过去刻度（set_expiration 对过去刻度直接删除条目）
  hash.insert_expiration(Arc::from(b"b".to_vec()), now - EXPIRED_SPAN);
  hash
}

#[test]
fn hgetall_header_always_matches_written_pairs() {
  let mut hash = hash_ttl_mixed();

  let mut sink = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut sink);
    hash.operate(HashOperation::Hgetall as u8, &[], 0, 0, &mut output, 2);
  }
  let items = parse_bulk_array(&sink);
  assert_eq!(items.len(), 6, "3 存活对 → RESP2 扁平 6 项，声明恒等写出");
  let mut fields: Vec<Vec<u8>> = items.iter().step_by(2).cloned().collect();
  // HashMap 迭代序非插入序（C# Dictionary 同），不变式只锁集合形态
  fields.sort();
  assert_eq!(fields, vec![b"a".to_vec(), b"c".to_vec(), b"d".to_vec()]);
}

#[test]
fn hkeys_hvals_header_always_matches_written_items() {
  let mut hash = hash_ttl_mixed();

  for op in [HashOperation::Hkeys, HashOperation::Hvals] {
    let mut sink = Vec::new();
    {
      let mut output = ObjectOutput::mount(&mut sink);
      hash.operate(op as u8, &[], 0, 0, &mut output, 2);
    }
    let items = parse_bulk_array(&sink);
    assert_eq!(items.len(), 3, "{op:?} 声明数恒等写出数");
    match op {
      HashOperation::Hkeys => {
        let mut sorted = items.clone();
        // HashMap 迭代序非插入序（C# Dictionary 同），不变式只锁集合形态
        sorted.sort();
        assert_eq!(sorted, vec![b"a".to_vec(), b"c".to_vec(), b"d".to_vec()]);
      }
      _ => assert!(items.iter().all(|i| i.as_slice() == b"v")),
    }
  }
}

/// ZRANDMEMBER 带 TTL 成员：purge 后放回采样 |count| 项恒满（迭代臂无二次
/// 过滤，声明数恒等写出数）
#[test]
fn zrandmember_ttl_header_always_matches_written_items() {
  let mut zset = SortedSetObject::new();
  for (i, m) in [b"m0", b"m1", b"m2", b"m3", b"m4"].into_iter().enumerate() {
    zset.add(m, (i + 1) as f64);
  }
  let now = now_ticks();
  zset.set_expiration(b"m0", now + LIVE_SPAN, ExpireOption::NONE);
  zset.insert_expiration(Arc::from(b"m1".to_vec()), now + LIVE_SPAN);
  // m0/m1 双路径挂存活账（set_expiration / insert_expiration）；m4 陈旧过期：
  // purge 物理摘除，存活 m0-m3 共 4
  zset.insert_expiration(Arc::from(b"m4".to_vec()), now - EXPIRED_SPAN);

  let mut sink = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut sink);
    // arg1 打包语义 count = arg1 >> 2（C# SortedSetObjectImpl.cs:665 同形），
    // -32_000 >> 2 = -8_000：负 count 放回采样 |count| 与基数脱钩
    zset.operate(
      SortedSetOperation::Zrandmember as u8,
      &[],
      -32_000,
      7,
      &mut output,
      2,
    );
  }
  let items = parse_bulk_array(&sink);
  assert_eq!(items.len(), 8_000, "放回采样声明 8000 项恒写出 8000 项");
  assert!(
    items
      .iter()
      .all(|m| m == b"m0" || m == b"m1" || m == b"m2" || m == b"m3"),
    "已摘除成员不得混入应答"
  );
}

/// SRANDMEMBER 空集负 count：nil 出口不受流式化改造影响
#[test]
fn srandmember_negative_count_on_empty_set_writes_nil() {
  let mut set = SetObject::new();
  let mut sink = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut sink);
    set.operate(SetOperation::Srandmember as u8, &[], -3, 7, &mut output, 2);
  }
  assert_eq!(sink, b"$-1\r\n", "空集负 count 应答 nil（RESP2 $-1）");
}

/// ZRANDMEMBER 成员级 TTL 全到期键（Present 存活零）三形态守卫锁
#[test]
fn zrandmember_all_expired_writes_nil_or_empty_array() {
  let pack = |count: i64, included: bool, with: bool| -> i32 {
    (((count as i32) << 1 | i32::from(included)) << 1) | i32::from(with)
  };
  // 三成员全挂过去刻度：purge_expired_len 物理摘除后存活数为零
  let all_expired = || {
    let mut zset = SortedSetObject::new();
    let past = now_ticks() - EXPIRED_SPAN;
    for (i, m) in [b"m1", b"m2", b"m3"].into_iter().enumerate() {
      zset.add(m, (i + 1) as f64);
      zset.insert_expiration(Arc::from(m.to_vec()), past);
    }
    zset
  };

  // (arg1, 形态, RESP2 帧, RESP3 帧)
  let cases: [(&str, i32, &[u8], &[u8]); 4] = [
    ("无 count", pack(1, false, false), b"$-1\r\n", b"_\r\n"),
    ("正 count", pack(5, true, false), b"*0\r\n", b"*0\r\n"),
    ("负 count", pack(-5, true, false), b"*0\r\n", b"*0\r\n"),
    (
      "正 count + WITHSCORES",
      pack(5, true, true),
      b"*0\r\n",
      b"*0\r\n",
    ),
  ];

  for (label, arg1, resp2, resp3) in cases {
    for ver in [2u8, 3] {
      let want = if ver >= 3 { resp3 } else { resp2 };
      let mut zset = all_expired();
      let mut sink = Vec::new();
      let result1 = {
        let mut output = ObjectOutput::mount(&mut sink);
        zset.operate(
          SortedSetOperation::Zrandmember as u8,
          &[],
          arg1,
          7,
          &mut output,
          ver,
        );
        output.result1
      };
      assert_eq!(sink, want, "{label} RESP{ver} 应答形");
      assert_eq!(result1, 0, "{label} RESP{ver} result1 应归零免 RMW 写回");
    }
  }
}

#[test]
fn srandmember_negative_count_header_matches_items() {
  let mut set = SetObject::new();
  for m in [b"x", b"y", b"z"] {
    set.add(m);
  }
  let mut sink = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut sink);
    set.operate(
      SetOperation::Srandmember as u8,
      &[],
      -100,
      7,
      &mut output,
      2,
    );
  }
  let items = parse_bulk_array(&sink);
  assert_eq!(items.len(), 100, "放回采样声明 100 项恒写出 100 项");
  assert!(items.iter().all(|m| m == b"x" || m == b"y" || m == b"z"));
}
