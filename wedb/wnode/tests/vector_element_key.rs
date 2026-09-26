//! 向量元素键与索引元数据物理键单元测试（对标 C# VectorElementKeyTests.cs）
//!
//! 验证：
//! 1. 物理键（命名空间 + 键体）在相同与不同命名空间/键体下的相等性与哈希一致性；
//! 2. 动态长度命名空间（4 字节 / 8 字节）的编码正确性；
//! 3. VectorManager.Index 内存布局与序列化反序列化往返（56 字节固定布局）。

use whasher::fast_hash;
use wnode::resp::vector::vector_manager_index::{INDEX_SIZE, Index};
use wvector::{VectorDistanceMetricType, VectorQuantType, VectorSetFlags, store::namespace_bytes};

/// 物理键拼装：`[命名空间字节][键字节]`（生产键编码构件 namespace_bytes 的测试装配）
#[inline]
fn physical_key(context: u64, key: &[u8]) -> Vec<u8> {
  let (ns_len, ns_buf) = namespace_bytes(context);
  let mut out = Vec::with_capacity(ns_len + key.len());
  out.extend_from_slice(&ns_buf[..ns_len]);
  out.extend_from_slice(key);
  out
}

#[inline]
fn key_hash(bytes: &[u8]) -> u64 {
  fast_hash(bytes)
}

#[test]
fn basic_element_key_comparisons() {
  for ns_len in [1, 4, 8] {
    let mut ns0 = vec![0u8; ns_len];
    let mut ns1 = vec![0u8; ns_len];
    for i in 0..ns_len {
      ns0[i] = (i + 1) as u8;
      ns1[i] = (i + 2) as u8;
    }

    let make_key = |ns: &[u8], key: &[u8]| -> Vec<u8> {
      let mut out = Vec::with_capacity(ns.len() + key.len());
      out.extend_from_slice(ns);
      out.extend_from_slice(key);
      out
    };

    // All different
    {
      let k0 = make_key(&ns0, b"foo");
      let k1 = make_key(&ns1, b"bar");
      assert_ne!(k0, k1);
      assert_ne!(key_hash(&k0), key_hash(&k1));
    }

    // Same key, different namespace
    {
      let k0 = make_key(&ns0, b"foo");
      let k1 = make_key(&ns1, b"foo");
      assert_ne!(k0, k1);
      assert_ne!(key_hash(&k0), key_hash(&k1));
    }

    // Same namespace, different key
    {
      let k0 = make_key(&ns0, b"foo");
      let k1 = make_key(&ns0, b"bar");
      assert_ne!(k0, k1);
      assert_ne!(key_hash(&k0), key_hash(&k1));
    }

    // Same namespace, same key
    {
      let k0 = make_key(&ns0, b"foo");
      let k1 = make_key(&ns0, b"foo");
      assert_eq!(k0, k1);
      assert_eq!(key_hash(&k0), key_hash(&k1));
    }
  }
}

#[test]
fn make_vector_element_key_matches_csharp_semantics() {
  const CONTEXT_STARTS: [u64; 5] = [8, 128, 256, 65_535, 2_147_483_647];
  let keys: [&[u8]; 3] = [b"", b"abcd", b"fizzbuzz"];

  for &k0 in &keys {
    for &ctx0_start in &CONTEXT_STARTS {
      for i in 0..20 {
        let ctx0 = ctx0_start + i;
        let phys_key0 = physical_key(ctx0, k0);

        // 验证命名空间编码头
        let (ns_len, ns_buf) = namespace_bytes(ctx0);
        assert_eq!(&phys_key0[..ns_len], &ns_buf[..ns_len]);
        assert_eq!(&phys_key0[ns_len..], k0);

        for &k1 in &keys {
          for &ctx1_start in &CONTEXT_STARTS {
            for j in 0..5 {
              let ctx1 = ctx1_start + j;
              let phys_key1 = physical_key(ctx1, k1);

              let equal = phys_key0.as_slice() == phys_key1.as_slice();
              let expected = k0 == k1 && ctx0 == ctx1;
              assert_eq!(equal, expected);
            }
          }
        }
      }
    }
  }
}

#[test]
fn index_record_layout_roundtrip() {
  let sample = Index {
    context: 8,
    index_ptr: 0xDEAD_BEEF,
    dimensions: 32,
    reduce_dims: 0,
    num_links: 8,
    build_exploration_factor: 200,
    quant_type: VectorQuantType::Q8,
    distance_metric: VectorDistanceMetricType::InnerProduct,
    flags: VectorSetFlags::SUPPRESS_CLEANUP,
  };

  let bytes = sample.to_bytes();
  assert_eq!(bytes.len(), INDEX_SIZE);
  // C# 布局：Context[0..8] IndexPtr[8..16] Dimensions[16..20] ReduceDims[20..24]
  assert_eq!(u64::from_le_bytes(bytes[0..8].try_into().unwrap()), 8);
  assert_eq!(u32::from_le_bytes(bytes[16..20].try_into().unwrap()), 32);
  assert_eq!(bytes[40], 1);

  let back = Index::from_bytes(&bytes).expect("56 字节应当反序列化成功");
  assert_eq!(back, sample);

  // 尺寸异常拒绝
  assert!(Index::from_bytes(&[0u8; 55]).is_none());
  assert!(Index::from_bytes(&[0u8; 57]).is_none());
}
