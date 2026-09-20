#[cfg(feature = "addr")]
#[test]
fn test_addr_primitives() {
  use wbase::addr::*;

  assert_eq!(ADDRESS_BITS, 48);
  assert_eq!(ADDRESS_MASK, 0x0000_FFFF_FFFF_FFFF);
  assert_eq!(READ_CACHE_BIT, 1u64 << 47);
  assert_eq!(ABSOLUTE_ADDRESS_MASK, 0x0000_7FFF_FFFF_FFFF);
  assert_eq!(INVALID_ADDRESS, 0);
  assert_eq!(TEMP_INVALID_ADDRESS, 1);

  let raw = 0x1234_5678_9ABC;
  assert!(is_valid(raw));
  assert!(!is_read_cache(raw));
  assert_eq!(to_absolute(raw), raw);

  let rc_addr = with_read_cache(raw);
  assert!(is_read_cache(rc_addr));
  assert_eq!(to_absolute(rc_addr), raw);
}

#[cfg(feature = "backoff")]
#[test]
fn test_backoff_stages() {
  use std::time::Duration;

  use wbase::backoff::*;

  let mut b = Backoff::new();
  assert_eq!(b.stage(), BackoffStage::Spin);
  assert!(b.stage().is_spin());

  for _ in 0..SPIN_LIMIT {
    b.advance();
  }
  assert_eq!(b.stage(), BackoffStage::Yield);

  for _ in SPIN_LIMIT..YIELD_LIMIT {
    b.advance();
  }
  assert_eq!(b.stage(), BackoffStage::Sleep);
  assert_eq!(SLEEP_DURATION, Duration::from_micros(50));

  b.reset();
  assert_eq!(b.stage(), BackoffStage::Spin);
  assert_eq!(b.step_count(), 0);
}

/// 阶段动作真源的派发契约：仅 Sleep 阶段触碰注入的定时器，忙等面全程不阻塞线程
#[cfg(all(feature = "backoff", feature = "future"))]
#[test]
fn test_backoff_stage_wait_dispatch() {
  use std::{cell::Cell, future::ready, time::Duration};

  use wbase::{backoff::*, future::block_on};

  let timer_hits = Cell::new(0u32);
  let sleeper = |_: Duration| {
    timer_hits.set(timer_hits.get() + 1);
    ready(())
  };

  for stage in [BackoffStage::Spin, BackoffStage::Yield, BackoffStage::Sleep] {
    // 三面各跑一轮：wait 在 Sleep 走 50μs 线程微睡，wait_busy 与异步面皆不阻塞线程
    stage.wait();
    stage.wait_busy();
    block_on(stage.wait_async(&sleeper));
  }
  // Spin / Yield 让核，唯 Sleep 让渡到注入定时器
  assert_eq!(timer_hits.get(), 1);
}

#[cfg(feature = "thread")]
#[test]
fn test_thread_id_uniqueness() {
  use std::{
    sync::{Arc, Mutex},
    thread,
  };

  use wbase::{map::HashSet, thread::*};

  let id1 = current_thread_id();
  let id2 = current_thread_id();
  assert_eq!(id1, id2, "Same thread should have stable ID");
  assert!(id1 > 0);

  let set = Arc::new(Mutex::new(HashSet::default()));
  let mut handles = Vec::new();

  for _ in 0..16 {
    let set = Arc::clone(&set);
    handles.push(thread::spawn(move || {
      let tid = current_thread_id();
      let mut lock = set.lock().unwrap();
      assert!(lock.insert(tid), "Thread ID must be globally unique");
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  assert_eq!(set.lock().unwrap().len(), 16);
}

#[cfg(feature = "crc")]
#[test]
fn test_crc_primitives() {
  use wbase::crc::*;

  let data = b"123456789";
  let expected = 0xCBF4_3926; // 标准 CRC-32 校验向量
  assert_eq!(crc32(data), expected);

  let mut hasher = Crc32Hasher::new();
  hasher.update(b"12345");
  hasher.update(b"6789");
  assert_eq!(hasher.finalize(), expected);

  let mut h2 = Crc32Hasher::new();
  h2.update_u64(0x0102_0304_0506_0708);
  assert_ne!(h2.finalize(), 0);
}

#[cfg(feature = "time")]
#[test]
fn test_time_primitives() {
  use wbase::time::*;

  let ms = now_ms();
  let nanos = now_nanos();

  assert!(ms > 0);
  assert!(nanos > 0);
  assert!(nanos >= ms * 1_000_000);
}

/// 计时源域纪律回归（对标 C# `System.Diagnostics.Stopwatch.GetTimestamp` 的
/// QueryPerformanceCounter 单调读数）：`now_stopwatch_ticks` 必须落在单调计时域，
/// 不得与 `now_nanos` 的实时域同源。旧实现取 `now_nanos() / NANOS_PER_TICK`，
/// 两域换算后差恒 < 100ns，本断言必红；换单调基后实时域读数领先计时域数十年
/// （与 NTP/手动回拨无关），断言恒绿且无需 sleep 等待
#[cfg(feature = "time")]
#[test]
fn test_stopwatch_ticks_anchored_on_monotonic_clock() {
  use wbase::time::{NANOS_PER_TICK, now_nanos, now_stopwatch_ticks};

  let ticks = now_stopwatch_ticks();
  let wall_ticks = now_nanos() / NANOS_PER_TICK;
  assert!(
    wall_ticks > ticks + 10_000_000,
    "计时源仍接在实时墙上钟域: wall_ticks={wall_ticks} mono_ticks={ticks}"
  );
  // 同域连续取时单调不减 → 区间作差恒非负，慢日志/直方图无回绕面
  assert!(now_stopwatch_ticks() >= ticks);
}

#[cfg(feature = "simd")]
#[test]
fn test_simd_fast_key_eq() {
  use wbase::simd::fast_key_eq;

  assert!(fast_key_eq(b"", b""));
  assert!(fast_key_eq(b"hello", b"hello"));
  assert!(!fast_key_eq(b"hello", b"world"));
  assert!(!fast_key_eq(b"short", b"shorter"));

  // 16 字节对齐与长键测试
  let k1 = b"0123456789abcdef_long_key_vector";
  let k2 = b"0123456789abcdef_long_key_vector";
  let k3 = b"0123456789abcdef_long_key_vectoX";
  assert!(fast_key_eq(k1, k2));
  assert!(!fast_key_eq(k1, k3));
}

/// `first_masked_eq`：一次 16B 载入 + 逐组掩码与 + 组内整向量全等的命中核
///
/// 向量核（aarch64 NEON / x86_64 SSE2 起步）与非向量目标的逐字节回落须逐输入
/// 同判，故按伪随机语料与标量参考实现全等校验
#[cfg(feature = "simd")]
#[test]
fn test_simd_first_masked_eq() {
  use wbase::simd::{MaskedGroup, first_masked_eq};

  const fn mask(len: usize) -> [u8; 16] {
    let mut m = [0u8; 16];
    let mut i = 0;
    while i < len {
      m[i] = 0xFF;
      i += 1;
    }
    m
  }

  // 13 字节档两项（尾 3 字节由掩码清零）、14 字节档一项、16 字节全等档一项
  static CAND_13: [[u8; 16]; 2] = [*b"*2\r\n$3\r\nGET\r\n\0\0\0", *b"*3\r\n$3\r\nSET\r\n\0\0\0"];
  static CAND_14: [[u8; 16]; 1] = [*b"*1\r\n$4\r\nPING\r\n\0\0"];
  static CAND_16: [[u8; 16]; 1] = [*b"*2\r\n$6\r\nEXISTS\r\n"];

  let groups = [
    MaskedGroup {
      mask: Some(mask(13)),
      candidates: &CAND_13,
      base: 0,
    },
    MaskedGroup {
      mask: Some(mask(14)),
      candidates: &CAND_14,
      base: 2,
    },
    MaskedGroup {
      mask: None,
      candidates: &CAND_16,
      base: 3,
    },
  ];

  // 命中回传全局序位（base + 组内下标），模式长度之后的输入字节不作约束
  assert_eq!(first_masked_eq(b"*2\r\n$3\r\nGET\r\njun", &groups), Some(0));
  assert_eq!(
    first_masked_eq(b"*3\r\n$3\r\nSET\r\n\x00\x00\x00", &groups),
    Some(1)
  );
  assert_eq!(first_masked_eq(b"*1\r\n$4\r\nPING\r\nXY", &groups), Some(2));
  assert_eq!(first_masked_eq(b"*2\r\n$6\r\nEXISTS\r\n", &groups), Some(3));
  // 16 字节档无掩码：末字节差异即失配
  assert_eq!(first_masked_eq(b"*2\r\n$6\r\nEXISTS\rX", &groups), None);
  // 掩码宽度内的差异即失配（13 字节档的第 13 字节属掩码内）
  assert_eq!(first_masked_eq(b"*2\r\n$3\r\nGET\rXjun", &groups), None);
  // 空候选组 / 空组集不参与比较
  assert_eq!(first_masked_eq(b"*2\r\n$3\r\nGET\r\njun", &[]), None);
  assert_eq!(
    first_masked_eq(
      b"*2\r\n$3\r\nGET\r\njun",
      &[MaskedGroup {
        mask: Some(mask(13)),
        candidates: &[],
        base: 9,
      }],
    ),
    None
  );

  // 标量参考实现：逐字节 (输入 & 掩码) == 候选，按组序与组内序首中
  fn reference(input: &[u8; 16], groups: &[MaskedGroup<'_>]) -> Option<usize> {
    for group in groups {
      for (idx, candidate) in group.candidates.iter().enumerate() {
        let hit = match group.mask {
          Some(mask) => (0..16).all(|i| (input[i] & mask[i]) == candidate[i]),
          None => input == candidate,
        };
        if hit {
          return Some(group.base + idx);
        }
      }
    }
    None
  }

  // 伪随机语料（LCG，进程内确定性）逐输入与参考实现同判，
  // 覆盖帧形态、掩码跨界字节差异与全随机噪声
  let mut state = 0x2545_F491_4F6C_DD1Du64;
  let mut next = move || {
    state = state
      .wrapping_mul(6364136223846793005)
      .wrapping_add(1442695040888963407);
    (state >> 33) as u8
  };
  for frame in [
    &b"*2\r\n$3\r\nGET\r\njun"[..],
    b"*3\r\n$3\r\nSET\r\n\xFF\xFF\xFF",
    b"*1\r\n$4\r\nPING\r\n\x00\x00",
    b"*2\r\n$6\r\nEXISTS\r\n",
    b"*9\r\n$9\r\nBOGUSBOG",
  ] {
    for _ in 0..200 {
      let mut input = [0u8; 16];
      input[..frame.len()].copy_from_slice(frame);
      for byte in &mut input {
        if *byte == 0 {
          *byte = next();
        }
      }
      // 随机翻转单字节 → 差异可落在掩码内或掩码外
      let flip = (next() as usize) % 16;
      input[flip] ^= 1 << (next() & 7);
      assert_eq!(
        first_masked_eq(&input, &groups),
        reference(&input, &groups),
        "输入 {input:?} 的掩码全等判定与标量参考不符"
      );
    }
  }
  for _ in 0..500 {
    let mut input = [0u8; 16];
    for byte in &mut input {
      *byte = next();
    }
    assert_eq!(first_masked_eq(&input, &groups), reference(&input, &groups));
  }
}

/// `masked_eq`：单候选一次 16B 载入 + 一次掩码与 + 整向量全等（会话 MRU 槽形态）
///
/// 向量核与非向量目标回落须逐输入同判，故按逐字节位翻转与伪随机语料和标量参考
/// 实现全等校验；掩码宽度之外的输入字节不参与判定
#[cfg(feature = "simd")]
#[test]
fn test_simd_masked_eq() {
  use wbase::simd::masked_eq;

  const fn mask(len: usize) -> [u8; 16] {
    let mut m = [0u8; 16];
    let mut i = 0;
    while i < len {
      m[i] = 0xFF;
      i += 1;
    }
    m
  }
  // 标量参考：逐字节 (输入 & 掩码) == 模式
  fn reference(input: &[u8; 16], mask: &[u8; 16], pattern: &[u8; 16]) -> bool {
    (0..16).all(|i| (input[i] & mask[i]) == pattern[i])
  }

  // 14 字节帧入槽（ECHO 一类）：模式尾部零填，消费长度之外由掩码清零
  let mut pattern = [0u8; 16];
  pattern[..14].copy_from_slice(b"*2\r\n$4\r\nECHO\r\n");

  // 掩码宽度内的逐字节位翻转：命中态与标量参考逐输入同判（宽度外翻转为噪声）
  for width in [13usize, 14, 15, 16] {
    let slot_mask = mask(width);
    for position in 0..16 {
      for bit in 0..8 {
        let mut input = pattern;
        if position < width {
          // 掩码内翻转：模式字节被改写 → 两臂须同判失配
          input[position] ^= 1 << bit;
        } else {
          // 掩码外翻转：填成与之不同的杂字节 → 两臂须同判命中
          input[position] = !pattern[position];
        }
        assert_eq!(
          masked_eq(&input, &slot_mask, &pattern),
          reference(&input, &slot_mask, &pattern),
          "宽度 {width} 第 {position} 字节翻第 {bit} 位"
        );
      }
    }
  }

  // 锚点：帧 + 掩码宽度之外的杂字节仍命中（14 字节档），同窗口在 16 字节档失配
  let mut noisy = [0u8; 16];
  noisy[..14].copy_from_slice(b"*2\r\n$4\r\nECHO\r\n");
  noisy[15] = b'Z';
  assert!(
    masked_eq(&noisy, &mask(14), &pattern),
    "掩码外杂字节不作约束"
  );
  assert!(
    !masked_eq(&noisy, &mask(16), &pattern),
    "16 字节档须整窗全等"
  );

  // 伪随机语料（LCG，进程内确定性）：全随机与帧嫁接两类窗口均与参考同判
  let mut state = 0x853C_49E6_748F_A39Du64;
  let mut next = move || {
    state = state
      .wrapping_mul(6364136223846793005)
      .wrapping_add(1442695040888963407);
    (state >> 33) as u8
  };
  let masks = [mask(13), mask(14), mask(15), mask(16)];
  for _ in 0..4096 {
    let mut input = [0u8; 16];
    for byte in &mut input {
      *byte = next();
    }
    for mask in masks {
      assert_eq!(
        masked_eq(&input, &mask, &pattern),
        reference(&input, &mask, &pattern),
        "全随机输入 {input:?}"
      );
    }
  }
  for _ in 0..4096 {
    let mut input = [0u8; 16];
    for byte in &mut input {
      *byte = next();
    }
    let keep = (next() as usize) % 17;
    input[..keep.min(14)].copy_from_slice(&pattern[..keep.min(14)]);
    for mask in masks {
      assert_eq!(
        masked_eq(&input, &mask, &pattern),
        reference(&input, &mask, &pattern),
        "前缀嫁接输入 {input:?}"
      );
    }
  }
}

#[cfg(feature = "base32")]
#[test]
fn test_base32_primitives() {
  use std::{ffi::OsStr, path::Path};

  use wbase::base32::*;

  // 0. 常量验证
  assert_eq!(BASE32_LEN_U64, 13);
  assert_eq!(BASE32_LEN_U128, 26);
  assert_eq!(BASE32_LOWER_TABLE.len(), 32);

  // 1. u64 编码与解码
  let val64 = 0x0123_4567_89ab_cdef_u64;
  let b32_64 = encode_u64(val64);
  assert_eq!(b32_64.len(), BASE32_LEN_U64);
  assert_eq!(decode_u64(&b32_64), Some(val64));
  // 零与极值
  assert_eq!(decode_u64(&encode_u64(0)), Some(0));
  assert_eq!(decode_u64(&encode_u64(u64::MAX)), Some(u64::MAX));
  // 大写容错解码
  let upper = b32_64.as_str().to_ascii_uppercase();
  assert_eq!(decode_u64(&upper), Some(val64));
  // AsRef 与 Deref 转换
  assert_eq!(b32_64.as_ref() as &Path, Path::new(b32_64.as_str()));
  assert_eq!(b32_64.as_ref() as &OsStr, OsStr::new(b32_64.as_str()));
  assert_eq!(b32_64.as_ref() as &str, b32_64.as_str());
  assert_eq!(b32_64.as_ref() as &[u8], b32_64.as_bytes());
  assert_eq!(&*b32_64, b32_64.as_str());
  // PartialEq 跨类型对比
  assert_eq!(b32_64, b32_64.as_str());
  assert_eq!(b32_64.as_str(), b32_64);
  assert_eq!(format!("{b32_64}"), b32_64.as_str());
  assert_eq!(format!("{b32_64:?}"), b32_64.as_str());

  // 2. u128 编码与解码
  let val128 = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210_u128;
  let b32_128 = encode_u128(val128);
  assert_eq!(b32_128.len(), BASE32_LEN_U128);
  assert_eq!(decode_u128(&b32_128), Some(val128));
  assert_eq!(decode_u128(&encode_u128(0)), Some(0));
  assert_eq!(decode_u128(&encode_u128(u128::MAX)), Some(u128::MAX));
  let upper128 = b32_128.as_str().to_ascii_uppercase();
  assert_eq!(decode_u128(&upper128), Some(val128));
  // AsRef 与 Deref 转换
  assert_eq!(b32_128.as_ref() as &Path, Path::new(b32_128.as_str()));
  assert_eq!(b32_128.as_ref() as &OsStr, OsStr::new(b32_128.as_str()));
  assert_eq!(b32_128.as_ref() as &str, b32_128.as_str());
  assert_eq!(b32_128.as_ref() as &[u8], b32_128.as_bytes());
  assert_eq!(&*b32_128, b32_128.as_str());
  assert_eq!(b32_128, b32_128.as_str());
  assert_eq!(b32_128.as_str(), b32_128);
  assert_eq!(format!("{b32_128}"), b32_128.as_str());
  assert_eq!(format!("{b32_128:?}"), b32_128.as_str());

  // 3. 严格保序性测试（数值递增 == 字符串字典序递增）
  let s1 = encode_u64(100);
  let s2 = encode_u64(101);
  let s3 = encode_u64(0xFFFF_FFFF_0000_0000);
  let s4 = encode_u64(0xFFFF_FFFF_0000_0001);
  assert!(s1.as_str() < s2.as_str());
  assert!(s2.as_str() < s3.as_str());
  assert!(s3.as_str() < s4.as_str());
  assert!(s1 < s2 && s2 < s3 && s3 < s4);

  let u1 = encode_u128(100);
  let u2 = encode_u128(101);
  let u3 = encode_u128(0xFFFF_FFFF_0000_0000_FFFF_FFFF_0000_0000);
  let u4 = encode_u128(0xFFFF_FFFF_0000_0000_FFFF_FFFF_0000_0001);
  assert!(u1.as_str() < u2.as_str());
  assert!(u2.as_str() < u3.as_str());
  assert!(u3.as_str() < u4.as_str());
  assert!(u1 < u2 && u2 < u3 && u3 < u4);

  // 4. 校验器（仅测试内保留校验逻辑）
  let is_base32 = |s: &str| {
    s.as_bytes()
      .iter()
      .all(|&b| BASE32_LOWER_TABLE.contains(&b.to_ascii_lowercase()))
  };
  assert!(is_base32(""));
  assert!(is_base32(b32_64.as_str()));
  assert!(is_base32("0123456789abcdefghijklmnopqrstuv"));
  assert!(is_base32("0123456789ABCDEFGHIJKLMNOPQRSTUV"));
  assert!(!is_base32("w")); // w 不在 Base32hex 字符集内
  assert!(!is_base32("xyz")); // x, y, z 不是 Base32hex 字符 (只有 0..=v)
  assert!(!is_base32("WXYZ"));
  assert!(!is_base32("0123 4567"));
  assert!(!is_base32("0123-4567"));

  // 5. 防溢出与异常长度防御断言
  // decode_u64: 长度非 13
  assert_eq!(decode_u64(""), None);
  assert_eq!(decode_u64("000000000000"), None); // 12 字符
  assert_eq!(decode_u64("00000000000000"), None); // 14 字符
  // decode_u64: 首字符高位溢出 (0x0F 以上为非法，'g' 为 16，'v' 为 31)
  assert_eq!(decode_u64("g000000000000"), None);
  assert_eq!(decode_u64("v000000000000"), None);
  assert_eq!(decode_u64("fvvvvvvvvvvvv"), Some(u64::MAX)); // 恰好最大值
  // decode_u64: 非法字符
  assert_eq!(decode_u64("000000000000w"), None);
  assert_eq!(decode_u64("000000000000z"), None);
  assert_eq!(decode_u64("000000-000000"), None);

  // decode_u128: 长度非 26
  assert_eq!(decode_u128(""), None);
  assert_eq!(decode_u128(&"0".repeat(25)), None);
  assert_eq!(decode_u128(&"0".repeat(27)), None);
  // decode_u128: 首字符高位溢出 (0x07 以上为非法，'8' 为 8，'v' 为 31)
  assert_eq!(decode_u128(&format!("8{}", "0".repeat(25))), None);
  assert_eq!(decode_u128(&format!("a{}", "0".repeat(25))), None);
  assert_eq!(decode_u128(&format!("v{}", "0".repeat(25))), None);
  assert_eq!(
    decode_u128(&format!("7{}", "v".repeat(25))),
    Some(u128::MAX)
  ); // 恰好最大值
  // decode_u128: 非法字符
  assert_eq!(decode_u128(&format!("{}w", "0".repeat(25))), None);
  assert_eq!(decode_u128(&format!("{}z", "0".repeat(25))), None);
}

#[cfg(feature = "striped")]
#[test]
fn test_striped_rwlock() {
  use std::{
    sync::{
      Arc,
      atomic::{AtomicUsize, Ordering},
    },
    thread,
  };

  use wbase::striped::*;

  // 1. 默认构造与容量断言
  let locks: StripedRwLock<(), 128> = StripedRwLock::new();
  assert_eq!(locks.len(), 128);
  assert!(!locks.is_empty());
  assert_eq!(StripedRwLock::<(), 128>::STRIPE_MASK, 127);

  // 2. 自定义初始化与索引访问
  let data_locks: StripedRwLock<usize, 64> = StripedRwLock::with_initializer(|i| i * 10);
  assert_eq!(data_locks.len(), 64);
  assert_eq!(*data_locks.read_at(3), 30);
  assert_eq!(*data_locks.read_at(67), 30); // 67 & 63 == 3

  // 3. 读写锁基本语义
  {
    let mut w = data_locks.write_at(3);
    *w = 999;
  }
  assert_eq!(*data_locks.read_at(3), 999);

  // 4. 哈希寻址
  let hash1 = 0x1234_5678_u64;
  let hash2 = hash1 + 64;
  assert_eq!(
    StripedRwLock::<(), 64>::stripe_index(hash1),
    StripedRwLock::<(), 64>::stripe_index(hash2)
  );

  // 5. 多线程并发读写压力测试
  let counter_locks = Arc::new(StripedRwLock::<AtomicUsize, 32>::new());
  let mut handles = Vec::new();

  for t in 0..16 {
    let cl = Arc::clone(&counter_locks);
    handles.push(thread::spawn(move || {
      for i in 0..100 {
        let hash = (t * 1000 + i) as u64;
        let guard = cl.read(hash);
        guard.fetch_add(1, Ordering::Relaxed);
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  let total: usize = (0..32)
    .map(|idx| counter_locks.read_at(idx).load(Ordering::Relaxed))
    .sum();
  assert_eq!(total, 16 * 100);
}

#[cfg(feature = "striped")]
#[test]
fn test_striped_counter() {
  use std::thread;

  use wbase::striped::StripedCounter;

  // 1. 本地实例测试：隔离性、容量、掩码与基础增减
  let counter = StripedCounter::<16>::new();
  assert_eq!(counter.len(), 16);
  assert!(!counter.is_empty());
  assert_eq!(counter.slots().len(), 16);
  assert_eq!(StripedCounter::<16>::STRIPE_MASK, 15);

  counter.add(0, 100);
  counter.add(16, 50); // 16 自动模 16 回绕到 0
  assert_eq!(counter.get(), 150);

  counter.add(1, 200);
  counter.sub(17, 30); // 17 模 16 回绕到 1
  assert_eq!(counter.get(), 320);
  assert_eq!(counter.get_positive(), 320);

  // 负值截断
  counter.sub(0, 500);
  assert_eq!(counter.get(), -180);
  assert_eq!(counter.get_positive(), 0);

  // 重置
  counter.reset();
  assert_eq!(counter.get(), 0);
  assert_eq!(counter.get_positive(), 0);

  let dbg_str = format!("{counter:?}");
  assert!(dbg_str.contains("StripedCounter"));
  assert!(dbg_str.contains("len: 16"));

  // 2. 静态常量构造与多线程高并发原子争用测试
  static GLOBAL_COUNTER: StripedCounter<32> = StripedCounter::new();
  GLOBAL_COUNTER.reset();
  assert_eq!(GLOBAL_COUNTER.len(), 32);
  assert_eq!(StripedCounter::<32>::STRIPE_MASK, 31);

  let threads: Vec<_> = (0..16)
    .map(|tid| {
      thread::spawn(move || {
        for i in 0..500 {
          let stripe = tid * 37 + i;
          GLOBAL_COUNTER.add(stripe, 10);
          GLOBAL_COUNTER.sub(stripe, 5);
        }
      })
    })
    .collect();

  for t in threads {
    t.join().unwrap();
  }

  // 16 线程 * 500 轮 * (10 - 5) = 40000
  assert_eq!(GLOBAL_COUNTER.get(), 16 * 500 * 5);
  assert_eq!(GLOBAL_COUNTER.get_positive(), 16 * 500 * 5);

  GLOBAL_COUNTER.reset();
  assert_eq!(GLOBAL_COUNTER.get(), 0);
}

#[cfg(feature = "varint")]
#[test]
fn test_varint_primitives() {
  use wbase::varint::*;

  // 1. 常量范围边界
  let boundary_samples = [
    0u64,
    1,
    127,
    128,
    129,
    16_511,
    16_512,
    16_513,
    2_113_663,
    2_113_664,
    2_113_665,
    270_549_119,
    270_549_120,
    270_549_121,
    u64::MAX - 1,
    u64::MAX,
  ];

  for &val in &boundary_samples {
    let expected_len = varint_len(val);
    let (arr, len) = encode_u64_to_array(val);
    assert_eq!(len, expected_len, "val={val} 编码长度不符");

    // 切片解码往返
    let (decoded, consumed) = decode_u64(&arr[..len]).unwrap();
    assert_eq!(decoded, val, "val={val} 解码还原不符");
    assert_eq!(consumed, len, "val={val} 消耗字节不符");

    // 写入目标切片
    let mut dst = [0u8; 16];
    let wlen = encode_u64(val, &mut dst).unwrap();
    assert_eq!(wlen, len);
    assert_eq!(&dst[..len], &arr[..len]);
  }

  // 2. 严格保序性（数值单调递增 == 二进制编码大端字典序单调递增）
  for i in 0..boundary_samples.len() - 1 {
    let a = boundary_samples[i];
    let b = boundary_samples[i + 1];
    let (arr_a, len_a) = encode_u64_to_array(a);
    let (arr_b, len_b) = encode_u64_to_array(b);
    assert!(arr_a[..len_a] < arr_b[..len_b], "保序失败: a={a} vs b={b}");
  }

  // 3. 错误与截断防御
  assert!(matches!(
    decode_u64(&[]),
    Err(VarintError::BufferTooShort {
      expected: 1,
      actual: 0
    })
  ));
  let (arr9, _) = encode_u64_to_array(1_000_000_000);
  assert!(matches!(
    decode_u64(&arr9[..5]),
    Err(VarintError::BufferTooShort { .. })
  ));

  // 4. 非规范编码防御（9字节形式但数值小于 270_549_120）
  let mut non_canonical = [0u8; 9];
  non_canonical[0] = VARINT_9B_MARKER;
  non_canonical[1..9].copy_from_slice(&100u64.to_be_bytes());
  assert_eq!(decode_u64(&non_canonical), Err(VarintError::NonCanonical));
}

#[cfg(feature = "pool")]
mod suite;

#[cfg(feature = "hex")]
#[test]
fn test_hex_primitives() {
  use wbase::hex::{HEX_CHARS_LOWER, HEX_CHARS_UPPER, hex_decode, hex_encode, hex_val};

  assert_eq!(hex_val(b'7'), Some(7));
  assert_eq!(hex_val(b'c'), Some(12));
  assert_eq!(hex_val(b'z'), None);

  let bytes = hex_decode::<2>(b"AbCd").unwrap();
  assert_eq!(bytes, [0xab, 0xcd]);
  assert!(hex_decode::<2>(b"abc").is_none());

  assert_eq!(hex_encode(&[0xab, 0xcd]), "abcd");
  assert_eq!(HEX_CHARS_LOWER[10], b'a');
  assert_eq!(HEX_CHARS_UPPER[10], b'A');
}

#[cfg(feature = "num")]
#[test]
fn test_num_primitives() {
  use wbase::num::*;

  let mut i32_val = 0;
  assert!(try_parse(b"123", &mut i32_val));
  assert_eq!(i32_val, 123);

  assert_eq!(strict_f64(b"+inf", true), Some(f64::INFINITY));
}

#[cfg(feature = "convert")]
#[test]
fn test_convert_primitives() {
  use wbase::convert::*;

  let ticks = unix_timestamp_in_seconds_to_ticks(1600000000);
  assert_eq!(unix_time_in_seconds_from_ticks(ticks), 1600000000);
  assert_eq!(unix_time_in_seconds_from_ticks(-1), -1);
  assert_eq!(unix_time_in_seconds_from_ticks(0), -1);

  let ms_ticks = unix_timestamp_in_milliseconds_to_ticks(1600000000123);
  assert_eq!(
    unix_time_in_milliseconds_from_ticks(ms_ticks),
    1600000000123
  );
  assert_eq!(unix_time_in_milliseconds_from_ticks(-1), -1);
  assert_eq!(unix_time_in_milliseconds_from_ticks(0), -1);

  let now_ticks = 100_000_000;
  assert_eq!(
    seconds_from_diff_ticks(now_ticks + 15_000_000, now_ticks),
    2
  );
  assert_eq!(
    seconds_from_diff_ticks(now_ticks + 14_999_999, now_ticks),
    1
  );
  assert_eq!(seconds_from_diff_ticks(now_ticks, now_ticks), -1);
  assert_eq!(seconds_from_diff_ticks(-1, now_ticks), -1);

  assert_eq!(
    milliseconds_from_diff_ticks(now_ticks + 50_000, now_ticks),
    5
  );
  assert_eq!(milliseconds_from_diff_ticks(now_ticks, now_ticks), -1);
}

#[cfg(feature = "hash_slot")]
#[test]
fn test_hash_slot_primitives() {
  use wbase::hash_slot::*;

  assert_eq!(CLUSTER_SLOT_COUNT, 16384);
  assert_eq!(SLOT_MASK, 16383);
}

#[cfg(feature = "hash")]
#[test]
fn test_hash_primitives() {
  use wbase::hash::*;

  let h2_64 = murmur_hash2_x64_a(b"test", 0);
  assert_ne!(h2_64, 0);
}

#[cfg(feature = "crc64")]
#[test]
fn test_crc64_primitives() {
  use wbase::crc64::*;

  let h = hash(b"123456789");
  assert_eq!(h.len(), 8);
}

/// 端点形态判定单源规则（入站监听解析与出站建连共读此一条）
#[cfg(feature = "endpoint")]
#[test]
fn test_endpoint_uds_path_rule() {
  use std::path::Path;

  use wbase::endpoint::uds_path;

  // 显式前缀形态：剥 `unix:` 取其后的路径
  assert_eq!(
    uds_path("unix:/var/run/wedb.sock"),
    Some(Path::new("/var/run/wedb.sock"))
  );
  assert_eq!(uds_path("unix:"), Some(Path::new("")));
  // 裸路径三形态：绝对路径、相对路径、`.sock` 后缀
  assert_eq!(
    uds_path("/tmp/wedb.sock"),
    Some(Path::new("/tmp/wedb.sock"))
  );
  assert_eq!(uds_path("/tmp/wedb"), Some(Path::new("/tmp/wedb")));
  assert_eq!(
    uds_path("./run/wedb.sock"),
    Some(Path::new("./run/wedb.sock"))
  );
  assert_eq!(
    uds_path("we/call/wedb.sock"),
    Some(Path::new("we/call/wedb.sock"))
  );
  // 前后空白归一
  assert_eq!(
    uds_path("  unix:/tmp/a.sock  "),
    Some(Path::new("/tmp/a.sock"))
  );
  // TCP 形态一律 None（含 `:port` 简写与裸主机串）
  assert_eq!(uds_path("127.0.0.1:6379"), None);
  assert_eq!(uds_path("[::1]:6379"), None);
  assert_eq!(uds_path(":6379"), None);
  assert_eq!(uds_path("localhost"), None);
  assert_eq!(uds_path(""), None);
}
