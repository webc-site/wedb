//! 密钥材料共享工具（对标 libs/server/ACL/SecretsUtility.cs）
//!
//! 在 garnet 中的相对路径: libs/server/ACL/SecretsUtility.cs(对标 C# SecretsUtility 密钥装载)

/// 常量时间 32 字节哈希值比较（4 个 u64，无分支展开，用于 SHA-256 口令哈希）
///
/// libs/server/ACL/SecretsUtility.cs:ConstantEquals
#[inline]
pub fn constant_equals(a: &[u8; 32], b: &[u8; 32]) -> bool {
  let (chunks_a, _) = a.as_chunks::<8>();
  let (chunks_b, _) = b.as_chunks::<8>();
  let mut diff = 0u64;
  for (ca, cb) in chunks_a.iter().zip(chunks_b.iter()) {
    diff |= u64::from_ne_bytes(*ca) ^ u64::from_ne_bytes(*cb);
  }
  diff == 0
}
