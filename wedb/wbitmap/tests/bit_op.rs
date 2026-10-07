#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wbitmap::{BitOpAccumulator, BitmapOperation};

fn naive(op: BitmapOperation, srcs: &[&[u8]], dst_len: usize) -> Vec<u8> {
  let mut dst = vec![0u8; dst_len];
  for i in 0..dst_len {
    let mut b = if srcs[0].len() > i { srcs[0][i] } else { 0 };
    for s in &srcs[1..] {
      b = if s.len() > i {
        match op {
          BitmapOperation::And => b & s[i],
          BitmapOperation::Or => b | s[i],
          BitmapOperation::Xor => b ^ s[i],
          BitmapOperation::Diff => b & !s[i],
          _ => b,
        }
      } else if op == BitmapOperation::And {
        0
      } else {
        b
      };
    }
    dst[i] = b;
  }
  dst
}

fn run_accumulator(op: BitmapOperation, srcs: &[&[u8]]) -> Result<Vec<u8>, &'static str> {
  let mut acc = BitOpAccumulator::new(op);
  for s in srcs {
    acc.fold(s);
  }
  acc.finish()
}

#[test]
fn accumulator_matches_naive() {
  let bytes: Vec<u8> = (0..64).map(|i| (i * 37 + 11) as u8).collect();
  let lens: &[&[usize]] = &[
    &[1, 1],
    &[7, 7, 7],
    &[8, 8],
    &[16, 16],
    &[3, 8],
    &[8, 3],
    &[1, 15, 7],
    &[32, 16, 8, 4],
    &[4, 8, 16, 32],
    &[17, 33, 5],
  ];

  for op in [
    BitmapOperation::And,
    BitmapOperation::Or,
    BitmapOperation::Xor,
    BitmapOperation::Diff,
  ] {
    for seq in lens {
      let srcs: Vec<&[u8]> = seq.iter().map(|&l| &bytes[..l]).collect();
      let got = run_accumulator(op, &srcs).unwrap();
      let longest = seq.iter().copied().max().unwrap_or(0);
      let want = naive(op, &srcs, longest);
      assert_eq!(got, want, "{op:?} lens {seq:?}");
    }
  }
}

#[test]
fn not_and_single_source() {
  let a: &[u8] = &[0x0f, 0xf0, 0xff];
  // NOT 取反
  let mut acc = BitOpAccumulator::new(BitmapOperation::Not);
  acc.fold(a);
  assert_eq!(acc.finish().unwrap(), vec![0xf0, 0x0f, 0x00]);

  // 单源非 NOT：拷贝
  let mut acc = BitOpAccumulator::new(BitmapOperation::And);
  acc.fold(a);
  assert_eq!(acc.finish().unwrap(), a);

  // 单源 DIFF：非法
  let mut acc = BitOpAccumulator::new(BitmapOperation::Diff);
  acc.fold(a);
  assert!(acc.finish().is_err());
}

/// Redis 语义神谕（bitops.c:1294-1301 缺失=零长串：越界/缺失字节取 0
/// 参与运算，结果长 = 最长在场源）：`None` 缺席按全零串折算
fn naive_with_missing(op: BitmapOperation, srcs: &[Option<&[u8]>]) -> Vec<u8> {
  let longest = srcs.iter().flatten().map(|s| s.len()).max().unwrap_or(0);
  (0..longest)
    .map(|i| {
      let byte = |s: Option<&[u8]>| s.and_then(|s| s.get(i).copied()).unwrap_or(0);
      let mut b = byte(srcs[0]);
      for s in &srcs[1..] {
        let sb = byte(*s);
        b = match op {
          BitmapOperation::And => b & sb,
          BitmapOperation::Or => b | sb,
          BitmapOperation::Xor => b ^ sb,
          BitmapOperation::Diff => b & !sb,
          BitmapOperation::Not => b,
        };
      }
      b
    })
    .collect()
}

fn run_with_missing(op: BitmapOperation, srcs: &[Option<&[u8]>]) -> Result<Vec<u8>, &'static str> {
  let mut acc = BitOpAccumulator::new(op);
  for s in srcs {
    acc.fold(s.unwrap_or(&[]));
  }
  acc.finish()
}

#[test]
fn missing_source_folds_as_empty_slice() {
  let a = [0x55u8, 0xAA, 0xFF, 0x00];
  let b = [0x0Fu8, 0xF0];

  let cases: &[&[Option<&[u8]>]] = &[
    &[Some(&a), None],
    &[None, Some(&a)],
    &[Some(&a), None, Some(&b)],
    &[None, Some(&a), None, Some(&b)],
    &[None, None],
  ];

  for op in [
    BitmapOperation::And,
    BitmapOperation::Or,
    BitmapOperation::Xor,
    BitmapOperation::Diff,
  ] {
    for (idx, seq) in cases.iter().enumerate() {
      let got = run_with_missing(op, seq).unwrap();
      let want = naive_with_missing(op, seq);
      assert_eq!(got, want, "{op:?} case #{idx}");
    }
  }
}

#[test]
fn accumulator_edges() {
  // 空累加器（0 源）
  let acc = BitOpAccumulator::new(BitmapOperation::And);
  assert_eq!(acc.finish().unwrap(), Vec::<u8>::new());

  let acc = BitOpAccumulator::new(BitmapOperation::Diff);
  assert_eq!(acc.finish().unwrap(), Vec::<u8>::new());

  // 全零长源
  let empty: &[u8] = &[];
  let mut acc = BitOpAccumulator::new(BitmapOperation::Or);
  acc.fold(empty);
  acc.fold(empty);
  assert_eq!(acc.finish().unwrap(), Vec::<u8>::new());
}
