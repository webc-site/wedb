use wval::{SAMPLE_STACK_CAP, sample_distinct_indices};

/// 基本属性：边界、严格升序、无重复、值域合法
#[test]
fn test_sample_basic_properties() {
  // count == 0 / count >= total / count == 1 边界
  assert!(sample_distinct_indices(10, 0).is_empty());
  assert_eq!(sample_distinct_indices(5, 5), (0..5).collect::<Vec<_>>());
  assert_eq!(sample_distinct_indices(3, 9), vec![0, 1, 2]);
  let one = sample_distinct_indices(100, 1);
  assert_eq!(one.len(), 1);
  assert!(one[0] < 100);

  // 覆盖位掩码（N <= 64 / N <= 512）、栈数组去重与 Floyd 全部场景
  for (total, count) in [
    (2usize, 1usize),
    (64, 32),
    (64, 63),
    (65, 33),
    (512, 256),
    (512, 511),
    (513, 64),
    (513, 65),
    (1000, 10),
    (1000, 990),
    (100_000, 128),
  ] {
    let v = sample_distinct_indices(total, count);
    assert_eq!(v.len(), count, "total={total} count={count}");
    assert!(v.windows(2).all(|w| w[0] < w[1]), "必须严格升序且无重复");
    assert!(*v.first().unwrap() < total);
    assert!(*v.last().unwrap() < total);
  }
}

/// 栈上定长数组容量边界（恰好跨过 SAMPLE_STACK_CAP 触发 Floyd 回退）
#[test]
fn test_sample_stack_cap_boundary() {
  assert_eq!(SAMPLE_STACK_CAP, 64);
  for count in [SAMPLE_STACK_CAP - 1, SAMPLE_STACK_CAP, SAMPLE_STACK_CAP + 1] {
    let v = sample_distinct_indices(10_000, count);
    assert_eq!(v.len(), count);
    assert!(v.windows(2).all(|w| w[0] < w[1]));
    assert!(*v.last().unwrap() < 10_000);
  }
}
