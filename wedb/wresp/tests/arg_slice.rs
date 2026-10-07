#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
#[cfg(test)]
mod tests {
  use wresp::argslice::ArgSlice;

  #[test]
  fn test_arg_slice_resolve() {
    let payload = b"Hello, Garnet ArgSlice!";
    // 负载前预留 4 字节长度前缀位，验证区间解析
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&[0u8; 4]);
    buf.extend_from_slice(payload);
    let slice = ArgSlice::new(4, payload.len());
    assert_eq!(slice.resolve(&buf), payload);
    assert_eq!(slice.total_size(), payload.len() + 4);
    assert!(!slice.is_empty());
  }

  #[test]
  fn test_empty_arg_slice() {
    let slice = ArgSlice::new(0, 0);
    assert!(slice.is_empty());
    assert_eq!(slice.resolve(b"anything"), b"");
  }
}
