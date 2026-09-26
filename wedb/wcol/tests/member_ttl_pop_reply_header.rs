//! ZPOPMIN/ZPOPMAX 显式 count 形弹出臂「声明数恒等写出数」回归（zcode-r159c-zpopcnt）
//!
//! 原缺陷形：sorted_set_pop_min_or_max_count 环前 delete_expired_items 一次
//! 采样 t1 后按存活数钳制并先落 RESP 头，弹出循环每轮经 pop_min_or_max
//! 内部重复 delete_expired_items 重采样 t_i——expiry 落于 (t1, t_i] 的成员
//! 被中途物理摘除，树缩至头声明之下、pop 返回 None 早 break，声明数大于
//! 实写条目数即客户端按声明读取时 RESP 流永久错位（C#
//! SortedSetPopMinOrMaxCount 环前仅一次剔除、环体直摘 Min/Max 零重采样，
//! 声明恒等实写形于结构）。
//!
//! 修复形态：环前一次剔除、环内免重采样（弹出循环直调免采样内核并删
//! break 早停臂），环内到期成员视存活原样弹出并计入声明数，与 C# 逐字节
//! 全等（修复型偏离收口，doc/zh/deviations.md §53）。
//!
//! 自研回归锁: 计数声明面行为锁（时钟窗口错位本身不可稳定注入，锁恒等
//! 不变式：大基数弹出循环 O(count·log n) 必跨毫秒级到期刻，回退重采样形
//! 即环内被剔致尾随断言炸）

use std::{str::from_utf8, sync::Arc};

use wbase::time::now_ticks;
use wcol::{ObjectOutput, SortedSetObject, SortedSetOperation};
use wresp::options::ExpireOption;

/// 弹出基数（大到弹出循环必跨 1ms 到期刻）
const COUNT: i64 = 30_000;
/// 尾段到期窗成员数（挂 1ms 即刻到期：环前采样必存活计入钳制声明）
const TAIL_EXPIRING: i64 = 8;
const MILLI_SPAN: i64 = 10_000;
const LIVE_SPAN: i64 = 1_000_000_000;

/// N 员 zset（成员 mi 分值 i+1，弹出序即下标序），尾段 K 员挂 1ms 即刻到期
fn zset_with_expiring_tail() -> SortedSetObject {
  let mut zset = SortedSetObject::new();
  for i in 0..COUNT {
    let member = format!("m{i}");
    zset.add(member.as_bytes(), (i + 1) as f64);
  }
  let now = now_ticks();
  // 到期刻晚于环前采样（挂账与 operate 间为微秒级同步代码），尾段成员
  // 存活计入钳制；弹出循环期间落入到期窗，环内免重采样即按存活原样弹出
  for i in COUNT - TAIL_EXPIRING..COUNT {
    let member = format!("m{i}");
    zset.insert_expiration(Arc::from(member.into_bytes()), now + MILLI_SPAN);
  }
  zset
}

/// 解析 RESP2 bulk 数组应答 → 项序列（头声明数与实写不符即尾随断言炸）
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

/// 解析 RESP3 数组应答（每项为 [*2 member score] 嵌套对）→ 成员序列
fn parse_resp3_pair_array(frame: &[u8]) -> Vec<Vec<u8>> {
  let header_end = frame.iter().position(|&b| b == b'\n').expect("缺数组头");
  let length: usize = from_utf8(&frame[1..header_end - 1])
    .unwrap()
    .parse()
    .unwrap();
  let mut rest = &frame[header_end + 1..];
  let mut members = Vec::with_capacity(length);
  for _ in 0..length {
    assert!(rest.starts_with(b"*2\r\n"), "嵌套对应为 *2 头");
    rest = &rest[4..];
    let len_end = rest.iter().position(|&b| b == b'\n').expect("缺 bulk 头");
    let len: usize = from_utf8(&rest[1..len_end - 1]).unwrap().parse().unwrap();
    members.push(rest[len_end + 1..len_end + 1 + len].to_vec());
    rest = &rest[len_end + 1 + len + 2..];
    // 分值项为 RESP3 简单浮点串 `,…\r\n`
    let score_end = rest.iter().position(|&b| b == b'\n').expect("缺浮点头");
    assert_eq!(rest[0], b',', "分值应为 RESP3 浮点帧");
    rest = &rest[score_end + 1..];
  }
  assert!(rest.is_empty(), "应答对数与头部声明不符");
  members
}

/// RESP2 加倍头：声明 2·count 项恒写出 2·count 项，环内到期成员按存活原样弹出
#[test]
fn zpopmin_count_resp2_header_always_matches_written_items() {
  let mut zset = zset_with_expiring_tail();
  let mut sink = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut sink);
    zset.operate(
      SortedSetOperation::Zpopmin as u8,
      &[],
      COUNT as i32,
      0,
      &mut output,
      2,
    );
  }
  let header = format!("*{}\r\n", COUNT * 2);
  assert!(
    sink.starts_with(header.as_bytes()),
    "RESP2 外层头应先落加倍声明 {header:?}，实落 {:?}",
    &sink[..header.len().min(sink.len())]
  );
  let items = parse_bulk_array(&sink);
  assert_eq!(items.len(), (COUNT * 2) as usize, "声明数恒等写出数");
  // 弹出序 = 分值升序 = 下标序；尾段到期窗成员（m29992..）按存活原样弹出
  for (i, member) in items.iter().step_by(2).enumerate() {
    let expect = format!("m{i}");
    assert_eq!(
      member.as_slice(),
      expect.as_bytes(),
      "弹出序错位于第 {i} 员"
    );
  }
}

/// RESP3 头声明 count、每对嵌套数组：声明恒等写出，环内到期成员按存活原样弹出
#[test]
fn zpopmax_count_resp3_header_always_matches_written_pairs() {
  let mut zset = zset_with_expiring_tail();
  let mut sink = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut sink);
    zset.operate(
      SortedSetOperation::Zpopmax as u8,
      &[],
      COUNT as i32,
      3,
      &mut output,
      2,
    );
  }
  let header = format!("*{COUNT}\r\n");
  assert!(
    sink.starts_with(header.as_bytes()),
    "RESP3 外层头应先落声明 {header:?}，实落 {:?}",
    &sink[..header.len().min(sink.len())]
  );
  let members = parse_resp3_pair_array(&sink);
  assert_eq!(members.len(), COUNT as usize, "声明数恒等写出数");
  // ZPOPMAX 弹出序 = 分值降序 = 下标倒序；尾段到期窗成员按存活原样弹出
  for (i, member) in members.iter().rev().enumerate() {
    let expect = format!("m{i}");
    assert_eq!(
      member.as_slice(),
      expect.as_bytes(),
      "弹出序错位于第 {i} 员"
    );
  }
}

/// 缺省无计数形态（arg1 = -1 哨兵）：无外层头单元素弹出，RESP2/RESP3 帧形不感变更
#[test]
fn zpopmin_single_element_form_unchanged_across_versions() {
  for (arg2, expected) in [
    (0_i32, &b"*2\r\n$2\r\nm0\r\n$1\r\n1\r\n"[..]),
    (3, b"*2\r\n$2\r\nm0\r\n,1\r\n".as_slice()),
  ] {
    // 单元素弹出消耗成员，每版本独立构造
    let mut zset = SortedSetObject::new();
    for (i, m) in [b"m0", b"m1", b"m2"].into_iter().enumerate() {
      zset.add(m, (i + 1) as f64);
    }
    let mut sink = Vec::new();
    {
      let mut output = ObjectOutput::mount(&mut sink);
      zset.operate(
        SortedSetOperation::Zpopmin as u8,
        &[],
        -1,
        arg2,
        &mut output,
        2,
      );
    }
    assert_eq!(sink, expected, "单元素形（arg2={arg2}）帧形不得变更");
  }
}

/// 存活守卫对照：到期窗成员长期存活（LIVE_SPAN）时弹出臂行为与到期无关，
/// 锁头声明恒等实写不受挂账路径（set_expiration 守卫摘除臂）影响
#[test]
fn zpopmin_count_with_live_tail_header_always_matches_written_items() {
  let mut zset = zset_with_expiring_tail();
  let now = now_ticks();
  for i in COUNT - TAIL_EXPIRING..COUNT {
    let member = format!("m{i}");
    zset.set_expiration(member.as_bytes(), now + LIVE_SPAN, ExpireOption::NONE);
  }
  let mut sink = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut sink);
    zset.operate(
      SortedSetOperation::Zpopmin as u8,
      &[],
      COUNT as i32,
      0,
      &mut output,
      2,
    );
  }
  let items = parse_bulk_array(&sink);
  assert_eq!(items.len(), (COUNT * 2) as usize, "声明数恒等写出数");
  for (i, member) in items.iter().step_by(2).enumerate() {
    let expect = format!("m{i}");
    assert_eq!(member.as_slice(), expect.as_bytes());
  }
}
