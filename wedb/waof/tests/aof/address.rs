use waof::{AOF_ADDRESS_BYTES, AofAddress, MAX_SUBLOG_COUNT};

#[test]
fn span_roundtrip() {
  let mut a = AofAddress::create(3, 100);
  a.set(1, -5);
  a.set(2, 123456789);
  // 裸 8B LE 字节切片（生产复制线格式，无长度字节）。
  let mut bytes = Vec::with_capacity(AOF_ADDRESS_BYTES * 3);
  for i in 0..3 {
    bytes.extend_from_slice(&a[i].to_le_bytes());
  }
  assert_eq!(AofAddress::from_span(&bytes), a);
}

/// 带 1 字节长度前缀的二进制形往返与拒收锁（FAILREPLICATIONOFFSET
/// 请求线格式，对标 C# ToByteArray/FromByteArray 双端同形）
#[test]
fn binary_roundtrip_and_rejects() {
  let mut a = AofAddress::create(3, 100);
  a.set(1, -5);
  a.set(2, 12345678901234);
  let bytes = a.to_aof_binary();
  // 线形锁定：首字节 = 长度前缀，其后逐槽 8B 小端
  assert_eq!(bytes[0], 3);
  assert_eq!(bytes.len(), 1 + AOF_ADDRESS_BYTES * 3);
  assert_eq!(&bytes[1..9], 100i64.to_le_bytes().as_slice());
  assert_eq!(AofAddress::from_aof_binary(&bytes), Some(a));
  // 空载荷、前缀越界、前缀与实长不符一律拒（C# BinaryReader 抛异常面）
  assert_eq!(AofAddress::from_aof_binary(&[]), None);
  let mut over = a.to_aof_binary();
  over[0] = MAX_SUBLOG_COUNT as u8 + 1;
  assert_eq!(AofAddress::from_aof_binary(&over), None);
  assert_eq!(AofAddress::from_aof_binary(&bytes[..bytes.len() - 1]), None);
  assert_eq!(AofAddress::from_aof_binary(&[3, 0, 0]), None);
}

#[test]
fn string_roundtrip_and_rejects() {
  // "1,-2,300" 往返即负号逐段语义锁：逐段施加并复位 negative，get(1) == -2
  // （C# 上游逗号分支不施加不复位、负号仅末段生效，"1,-2,300" 解出
  // [1, 2, -300]，属已登记上游缺陷修复，见根 doc/zh/deviations.md §26）
  let a = AofAddress::from_string("1,-2,300").unwrap();
  assert_eq!(a.length(), 3);
  assert_eq!(a.get(1), Some(-2));
  assert_eq!(a.to_aof_string(), "1,-2,300");
  assert!(AofAddress::from_string("1,x,3").is_none());
  // 段数超上限拒绝（C# 构造仅 Debug.Assert，release 固定数组越界写，同条登记）
  assert!(AofAddress::from_string("1,2,3,4,5").is_none());
}

#[test]
fn compare_and_range_ops() {
  let a = AofAddress::create(2, 100);
  let b = AofAddress::create(2, 150);
  assert!(a.any_lesser(&b));
  assert_eq!(a.diff(&b).get(0), Some(-50));
  assert_eq!(a.aggregate_diff(&b), -100);
  assert_eq!(b.max(), 150);
  assert_eq!(b.min_value(), 0);

  let mut c = a;
  c.set_value(7);
  assert!(c.equals_all(&AofAddress::create(2, 7)));
}

/// 位点向量单调推进与取小推进：判据方向单点在 address.rs，回退写必须被拒
#[test]
fn monotonic_update_and_min_exchange() {
  let mut cur = AofAddress::create(3, 100);
  cur.set(1, 200);
  let mut next = AofAddress::create(2, 150);
  next.set(1, 180);
  cur.monotonic_update(&next);
  // 槽 0 取大写入、槽 1 回退值被拒、越出 update.length 的槽 2 原值不动
  assert_eq!((cur[0], cur[1], cur[2]), (150, 200, 100));

  // 单槽位推进经向量口径复现（C# 单槽重载在 rust 由同一判据的向量入口承接）：
  // 取大写入、回退值被拒、越出 update.length 的槽位不动
  cur.monotonic_update(&AofAddress::create(1, 160));
  cur.monotonic_update(&AofAddress::create(1, 155));
  assert_eq!((cur[0], cur[1]), (160, 200));

  let mut limit = AofAddress::create(3, i64::MAX);
  limit.min_exchange(&cur);
  assert_eq!((limit[0], limit[1], limit[2]), (160, 200, 100));
}
