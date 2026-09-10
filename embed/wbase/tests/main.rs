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

  let log_addr = LogAddress::new(rc_addr);
  assert!(log_addr.is_valid());
  assert!(log_addr.is_read_cache());
  assert_eq!(log_addr.absolute().as_raw(), raw);
  assert_eq!(*log_addr, rc_addr);
  assert_eq!(log_addr.as_raw(), rc_addr);
  assert_eq!(u64::from(log_addr), rc_addr);
  assert_eq!(LogAddress::from(rc_addr), log_addr);

  // Display 对齐 libs/storage/Tsavorite/cs/src/core/Index/Common/LogAddress.cs:AddressString：rc:N / kInvalid / kTempInvalid / log:N（十进制）
  assert_eq!(format!("{log_addr}"), format!("rc:{raw}"));
  assert_eq!(
    format!("{}", LogAddress::from_raw(TEMP_INVALID_ADDRESS)),
    "kTempInvalid"
  );
  assert_eq!(format!("{}", LogAddress::new(raw)), format!("log:{raw}"));

  // 页号与页起始地址互转（对齐 C# GetPageOfAddress / GetLogicalAddressOfStartOfPage）
  let page_bits = 20; // 1MB 页
  assert_eq!(page_of_address(raw, page_bits), raw >> page_bits);
  assert_eq!(page_of_address(rc_addr, page_bits), raw >> page_bits);
  assert_eq!(address_of_page_start(3, page_bits), 3 << page_bits);
  assert_eq!(
    LogAddress::page_start(5, page_bits).page(page_bits),
    5,
    "页起始地址所在页号须还原为原页号"
  );
}

#[cfg(feature = "align")]
#[test]
fn test_align_primitives() {
  use wbase::align::*;

  assert_eq!(CACHELINE_BYTES, 64);
  assert_eq!(DEFAULT_SECTOR_SIZE, 4096);
  assert_eq!(MIN_SECTOR_SIZE, 512);

  assert!(is_aligned(4096, 4096));
  assert!(!is_aligned(4095, 4096));
  assert_eq!(align_down(4097, 4096), 4096);
  assert_eq!(align_up(4097, 4096), 8192);
  assert_eq!(align_up(4096, 4096), 4096);
  assert_eq!(checked_align_up(u64::MAX - 10, 4096), None);

  // 前置 2 的幂（对齐 libs/client/Utility.cs:PreviousPowerOf2）
  assert_eq!(prev_power_of2(0), 0);
  assert_eq!(prev_power_of2(1), 1);
  assert_eq!(prev_power_of2(2), 2);
  assert_eq!(prev_power_of2(3), 2);
  assert_eq!(prev_power_of2(4097), 4096);
  assert_eq!(prev_power_of2(u64::MAX), 1u64 << 63);
  assert_eq!(prev_power_of2(1u64 << 47), 1u64 << 47);

  assert!(is_cacheline_aligned(128));
  assert!(!is_cacheline_aligned(127));
  assert_eq!(align_to_cacheline(65), 128);

  assert!(is_valid_sector_size(512));
  assert!(is_valid_sector_size(4096));
  assert!(!is_valid_sector_size(300));
  assert!(!is_valid_sector_size(256));

  let range = SectorRange::calculate(100, 200, DEFAULT_SECTOR_SIZE).unwrap();
  assert_eq!(range.aligned_offset, 0);
  assert_eq!(range.aligned_len, 4096);
  assert_eq!(range.internal_offset, 100);
  assert_eq!(range.sector_count(DEFAULT_SECTOR_SIZE), 1);
  assert_eq!(range.sub_range(50), 100..150);
}

#[cfg(feature = "backoff")]
#[test]
fn test_backoff_stages() {
  use std::time::Duration;

  use wbase::backoff::*;

  let mut b = Backoff::new();
  assert_eq!(b.stage(), BackoffStage::Spin);
  assert!(b.stage().is_spin());
  assert!(!b.is_sleep());

  for _ in 0..SPIN_LIMIT {
    b.advance();
  }
  assert_eq!(b.stage(), BackoffStage::Yield);
  assert!(b.stage().is_yield());
  assert!(!b.is_sleep());

  for _ in SPIN_LIMIT..YIELD_LIMIT {
    b.advance();
  }
  assert_eq!(b.stage(), BackoffStage::Sleep);
  assert!(b.stage().is_sleep());
  assert!(b.is_sleep());
  assert_eq!(SLEEP_DURATION, Duration::from_micros(50));

  b.reset();
  assert_eq!(b.stage(), BackoffStage::Spin);
  assert_eq!(b.step_count(), 0);
}

#[cfg(feature = "thread")]
#[test]
fn test_thread_id_uniqueness() {
  use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    thread,
  };

  use wbase::thread::*;

  let id1 = current_thread_id();
  let id2 = current_thread_id();
  assert_eq!(id1, id2, "Same thread should have stable ID");
  assert!(id1 > 0);

  let set = Arc::new(Mutex::new(HashSet::new()));
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

#[cfg(feature = "align")]
#[test]
fn test_cache_padded_layout() {
  use core::mem::{align_of, size_of};

  use wbase::align::{CachePadded, CachePadded64};

  assert_eq!(align_of::<CachePadded<u64>>(), 128);
  assert!(size_of::<CachePadded<u64>>() >= 128);

  assert_eq!(align_of::<CachePadded64<u64>>(), 64);
  assert!(size_of::<CachePadded64<u64>>() >= 64);

  let mut padded = CachePadded::new(42u64);
  assert_eq!(*padded, 42);
  *padded = 100;
  assert_eq!(padded.into_inner(), 100);
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

  // 4. 校验器
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

#[cfg(feature = "float")]
#[test]
fn test_float_primitives() {
  use wbase::float::*;

  // 1. 常规浮点数编解码一致性
  let samples_f64 = [
    f64::NEG_INFINITY,
    -1e300,
    -1000.5,
    -1.0,
    -1e-10,
    -0.0,
    0.0,
    1e-10,
    1.0,
    1000.5,
    1e300,
    f64::INFINITY,
  ];

  for &v in &samples_f64 {
    let bytes = encode_f64(v);
    let decoded = decode_f64(bytes);
    if v == 0.0 {
      assert_eq!(decoded, 0.0);
    } else {
      assert_eq!(decoded.to_bits(), v.to_bits());
    }
  }

  // 2. 严格保序性（字节字典序与浮点数大小完全一致，-0.0 严格排在 +0.0 之前）
  for i in 0..samples_f64.len() - 1 {
    let a = samples_f64[i];
    let b = samples_f64[i + 1];
    let ba = encode_f64(a);
    let bb = encode_f64(b);
    assert!(ba < bb, "{a} vs {b}: {ba:?} not < {bb:?}");
  }

  // 3. 切片接口
  let mut dst64 = [0u8; 10];
  dst64[..8].copy_from_slice(&encode_f64(123.456789));
  assert_eq!(decode_f64_from_slice(&dst64), Some(123.456789));
  assert_eq!(decode_f64_from_slice(&dst64[..7]), None);

  // 4. f32 浮点数测试
  let samples_f32 = [
    f32::NEG_INFINITY,
    -1e30,
    -100.5,
    -1.0,
    -1e-5,
    -0.0,
    0.0,
    1e-5,
    1.0,
    100.5,
    1e30,
    f32::INFINITY,
  ];

  for &v in &samples_f32 {
    let bytes = encode_f32(v);
    let decoded = decode_f32(bytes);
    if v == 0.0 {
      assert_eq!(decoded, 0.0);
    } else {
      assert_eq!(decoded.to_bits(), v.to_bits());
    }
  }

  for i in 0..samples_f32.len() - 1 {
    let a = samples_f32[i];
    let b = samples_f32[i + 1];
    let ba = encode_f32(a);
    let bb = encode_f32(b);
    assert!(ba < bb, "{a} vs {b}: {ba:?} not < {bb:?}");
  }

  let mut dst32 = [0u8; 6];
  dst32[..4].copy_from_slice(&encode_f32(98.7654));
  assert_eq!(decode_f32_from_slice(&dst32), Some(98.7654));
  assert_eq!(decode_f32_from_slice(&dst32[..3]), None);

  // 5. 编译期 const fn 契约验证
  const C_F64_BYTES: [u8; 8] = encode_f64(123.456789);
  const C_F64_DEC: f64 = decode_f64(C_F64_BYTES);
  assert_eq!(C_F64_DEC, 123.456789);
  const C_F64_SLICE_DEC: Option<f64> = decode_f64_from_slice(&C_F64_BYTES);
  assert_eq!(C_F64_SLICE_DEC, Some(123.456789));

  const C_F32_BYTES: [u8; 4] = encode_f32(98.7654);
  const C_F32_DEC: f32 = decode_f32(C_F32_BYTES);
  assert_eq!(C_F32_DEC, 98.7654);
  const C_F32_SLICE_DEC: Option<f32> = decode_f32_from_slice(&C_F32_BYTES);
  assert_eq!(C_F32_SLICE_DEC, Some(98.7654));
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

#[cfg(feature = "glob")]
#[test]
fn test_glob_primitives() {
  use wbase::glob::*;

  // 1. 基础通配符
  assert!(!glob_match(b"*", b""));
  assert!(glob_match(b"*", b"hello"));
  assert!(glob_match(b"", b""));
  assert!(!glob_match(b"", b"a"));
  assert!(glob_match(b"h?llo", b"hello"));
  assert!(!glob_match(b"h?llo", b"hllo"));

  // 2. 集合与区间
  assert!(glob_match(b"[a-z]ello", b"hello"));
  assert!(!glob_match(b"[0-9]ello", b"hello"));
  assert!(!glob_match(b"[!0-9]ello", b"hello"));
  assert!(glob_match(b"[!0-9]ello", b"!ello"));
  assert!(glob_match(b"[^0-9]ello", b"hello"));
  assert!(glob_match(b"[\\]]", b"]"));
  assert!(glob_match(b"[\\\\\\\\]", b"\\"));
  assert!(glob_match(b"\\*hello", b"*hello"));
  assert!(!glob_match(b"\\*hello", b"foo_hello"));
  assert!(glob_match(b"[a-]", b"]"));
  assert!(glob_match(b"[a-]", b"a"));
  assert!(!glob_match(b"[a-]", b"b"));
  assert!(glob_match(b"[a-]x]", b"x"));
  assert!(glob_match(b"[a-]x]", b"]"));
  assert!(glob_match(b"[z-a]", b"m"));
  assert!(glob_match(b"[abc", b"a"));
  assert!(!glob_match(b"[abc", b"d"));
  assert!(!glob_match(b"[", b"["));

  // 3. 大小写忽略
  assert!(!glob_match(b"hello", b"HELLO"));
  assert!(glob_match_nocase(b"hello", b"HELLO"));
  assert!(glob_match_opt(b"h[a-z]llo", b"hEllo", true));
  assert!(!glob_match_nocase(b"[k-M]ello", b"lello"));
  assert!(!glob_match_nocase(b"[Z-a]ello", b"mello"));
  assert!(!glob_match_nocase(b"[a-Z]ello", b"mello"));

  // 4. 编译期 const fn 求值验证
  const { assert!(glob_match(b"h*o", b"hello")) };
  const { assert!(glob_match_nocase(b"h*O", b"hello")) };
}

#[cfg(feature = "buf")]
#[test]
fn test_buf_macro_primitives() {
  use wbase::stack_heap_buf;

  stack_heap_buf!(TestBuf, 16);

  // 1. 栈分配
  let s_buf: TestBuf = b"short".as_slice().into();
  assert!(s_buf.is_stack());
  assert!(!s_buf.is_heap());
  assert_eq!(s_buf.len(), 5);
  assert_eq!(&*s_buf, b"short");
  assert_eq!(s_buf.as_slice(), b"short");
  assert_eq!(s_buf, b"short".as_slice());
  assert_eq!(b"short".as_slice(), s_buf);
  assert_eq!(s_buf.into_vec(), b"short".to_vec());

  // 2. 堆分配
  let long_data = b"0123456789abcdef_long_overflow";
  let h_buf: TestBuf = long_data.as_slice().into();
  assert!(!h_buf.is_stack());
  assert!(h_buf.is_heap());
  assert_eq!(h_buf.len(), long_data.len());
  assert_eq!(&*h_buf, long_data);
  assert_eq!(h_buf, long_data.as_slice());
  assert_eq!(long_data.as_slice(), h_buf);
  assert_eq!(h_buf.into_vec(), long_data.to_vec());

  // 3. 边界 16 字节恰好为栈
  let exact_16 = b"0123456789abcdef";
  let exact_buf: TestBuf = exact_16.as_slice().into();
  assert!(exact_buf.is_stack());
  assert_eq!(exact_buf.len(), 16);

  // 4. 排序与比较
  let b1: TestBuf = b"aaa".as_slice().into();
  let b2: TestBuf = b"bbb".as_slice().into();
  assert!(b1 < b2);
  assert_eq!(b1, b1);
  assert_ne!(b1, b2);

  // 5. Default 与 Into<Vec<u8>>
  let def: TestBuf = Default::default();
  assert!(def.is_empty());
  assert_eq!(def.len(), 0);
  let v: Vec<u8> = b1.into();
  assert_eq!(v, b"aaa");
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
    assert_eq!(decode_u64_opt(&arr[..len]), Some((val, len)));

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

#[cfg(feature = "ascii")]
#[test]
fn test_ascii_primitives() {
  use wbase::ascii::*;

  assert!(is_between(b'B', b'A', b'Z'));
  assert!(!is_between(b'a', b'A', b'Z'));
  assert_eq!(to_lower(b'A'), b'a');
  assert_eq!(to_upper(b'b'), b'B');

  let mut cmd = b"hELLo".to_vec();
  to_upper_in_place(&mut cmd);
  assert_eq!(cmd, b"HELLO");

  let mut cmd = b"hELLo".to_vec();
  to_lower_in_place(&mut cmd);
  assert_eq!(cmd, b"hello");
}

#[cfg(feature = "num")]
#[test]
fn test_num_primitives() {
  use wbase::num::*;

  let mut is_neg = false;
  assert_eq!(count_digits(12345, &mut is_neg), 5);
  assert!(!is_neg);

  assert_eq!(count_digits(-987, &mut is_neg), 3);
  assert!(is_neg);

  let mut i32_val = 0;
  assert!(try_parse_i32(b"123", &mut i32_val));
  assert_eq!(i32_val, 123);

  let mut f64_val: f64 = 0.0;
  assert!(try_parse_with_infinity(b"+inf", &mut f64_val));
  assert!(f64_val.is_infinite() && f64_val.is_sign_positive());

  let mut bits = 0b0110u64;
  assert_eq!(get_next_offset(&mut bits), 1);
  assert_eq!(bits, 0b0100);

  let mut zero = 0u64;
  assert_eq!(get_next_offset(&mut zero), 64);
  assert_eq!(zero, 0);
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

  assert_eq!(hash_slot(b"123456789"), 0x31C3 & HASH_SLOT_MAX);
  assert_eq!(hash_slot(b"key{user1}data"), hash_slot(b"user1"));
}

#[cfg(feature = "hash")]
#[test]
fn test_hash_primitives() {
  use wbase::hash::*;

  let h = murmur_hash3_x64_a(b"test", 0);
  assert_ne!(h, 0);

  let (h1, h2) = murmur_hash3_x128(b"test123456789012", 0);
  assert_ne!(h1, 0);
  assert_ne!(h2, 0);

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
