#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::str::from_utf8;

use wcol::{ObjectOutput, SetObject, SetOperation, set::set_object_impl::NO_COUNT};

#[test]
fn test_set_pop_no_count() {
  let mut set = SetObject::default();
  let mut payload = Vec::new();

  // 空集单枚弹出：写 null，result1 为 1
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], NO_COUNT, 0, &mut output, 2);
    assert_eq!(output.result1, 1);
  }
  assert_eq!(payload, b"$-1\r\n");

  // 填充元素并同步更新内存记账
  set.set.insert(b"elem1".to_vec());
  set.update_size(b"elem1", true);
  set.set.insert(b"elem2".to_vec());
  set.update_size(b"elem2", true);

  // 弹出单枚
  payload.clear();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], NO_COUNT, 0, &mut output, 2);
    assert_eq!(output.result1, 1);
  }
  assert_eq!(set.set.len(), 1);

  // 再次弹出单枚
  payload.clear();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], NO_COUNT, 0, &mut output, 2);
    assert_eq!(output.result1, 1);
  }
  assert_eq!(set.set.len(), 0);

  // 弹空后再弹出：写 null，result1 为 1
  payload.clear();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], NO_COUNT, 0, &mut output, 2);
    assert_eq!(output.result1, 1);
  }
  assert_eq!(payload, b"$-1\r\n");
}

#[test]
fn test_set_pop_with_count() {
  let mut set = SetObject::default();
  set.set.insert(b"a".to_vec());
  set.update_size(b"a", true);
  set.set.insert(b"b".to_vec());
  set.update_size(b"b", true);
  set.set.insert(b"c".to_vec());
  set.update_size(b"c", true);

  let mut payload = Vec::new();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], 2, 0, &mut output, 2);
    assert_eq!(output.result1, 2);
  }
  assert_eq!(set.set.len(), 1);

  payload.clear();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    // count 超过当前剩余量
    set.operate(SetOperation::Spop as u8, &[], 5, 0, &mut output, 2);
    assert_eq!(output.result1, 5);
  }
  assert_eq!(set.set.len(), 0);
}

/// RESP2 bulk 数组应答 → 成员序列（与 random_member_sampling.rs 同形小解析器）
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
    let body_start = len_end + 1;
    items.push(rest[body_start..body_start + len].to_vec());
    rest = &rest[body_start + len + 2..];
  }
  items
}

/// 大基数 SPOP count 臂收口（互异下标一次产出 + 视图释放后统一剔除，替代
/// 逐弹 iter().nth 的 O(count·n) 线性扫描）：
/// ①弹出成员互异且全部出集、剩余基数与 heap 记账与逐枚 update_size 严格对账；
/// ②count 超剩余量时清空、heap 回容器基线、result1 恒为 count、帧体只出剩余量；
/// ③约 9×10^8 迭代步的旧形态 debug 下必超 5 秒上界
#[test]
fn test_set_pop_large_count_distinct_and_heap_accounting() {
  use std::time::{Duration, Instant};

  const N: usize = 60_000;
  const POP: usize = 30_000;
  let mut set = SetObject::new();
  let base = set.heap_memory_size;
  for i in 0..N {
    let m = format!("m{i:05}");
    set.add(m.as_bytes());
  }
  assert_eq!(set.len(), N);
  let full = set.heap_memory_size;
  // 定长成员：每成员记账份额恒等，full - base 按成员数整除
  let per_member = (full - base) / N as i64;

  let mut payload = Vec::new();
  let t = Instant::now();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], POP as i32, 0, &mut output, 2);
    assert_eq!(output.result1, POP as i64);
  }
  let elapsed = t.elapsed();

  let popped = parse_bulk_array(&payload);
  assert_eq!(popped.len(), POP, "弹出条数漂移");
  let mut distinct = popped;
  distinct.sort();
  distinct.dedup();
  assert_eq!(distinct.len(), POP, "SPOP 弹出成员必须互异");
  assert!(
    distinct.iter().all(|m| !set.set.contains(m)),
    "弹出成员必须已从集合剔除"
  );
  assert_eq!(set.len(), N - POP, "剩余基数守恒");
  assert_eq!(
    set.heap_memory_size,
    full - POP as i64 * per_member,
    "heap 记账须与逐枚 update_size 基线一致"
  );
  assert!(
    elapsed < Duration::from_secs(5),
    "SPOP 逐弹 nth 形态必超 5 秒上界: {elapsed:?}"
  );

  // count 超剩余量：清空、heap 回容器基线、result1 恒为 count
  payload.clear();
  {
    let mut output = ObjectOutput::mount(&mut payload);
    set.operate(SetOperation::Spop as u8, &[], 99_999, 0, &mut output, 2);
    assert_eq!(output.result1, 99_999);
  }
  assert_eq!(set.set.len(), 0);
  assert_eq!(set.heap_memory_size, base, "清空后 heap 回容器基线");
  assert_eq!(
    parse_bulk_array(&payload).len(),
    N - POP,
    "超量弹出帧体只出剩余量"
  );
}
