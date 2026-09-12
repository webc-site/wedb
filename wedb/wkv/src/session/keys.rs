//! 会话键编码域（对标 C# Garnet StorageSession 的物理键编码：ns/db 前缀 + KeyTag + 用户键）
//!
//! 全部为纯编码构造：会话绑定变体取 `session_prefix()`（ns/db 原子变量），静态变体
//! 基于显式前缀切片或默认 (ns=0, db=0)，零堆分配、零 I/O。

use wdev::Device;
use wval::{KeyTag, NamespaceDbCodec, TaggedKeyBuf};

use crate::session::StoreSession;

impl<D: Device> StoreSession<D> {
  /// 生成当前会话专属方案 A 集合元数据物理键
  #[inline(always)]
  pub fn session_meta_key(&self, user_key: &[u8]) -> TaggedKeyBuf {
    let prefix = self.session_prefix();
    NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::Meta, user_key)
  }

  /// 生成当前会话专属方案 A 普通字符串物理键 (KeyTag::String)
  #[inline(always)]
  pub fn session_string_key(&self, user_key: &[u8]) -> TaggedKeyBuf {
    let prefix = self.session_prefix();
    NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::String, user_key)
  }

  /// 静态辅助：构造默认命名空间与数据库 (ns=0, db=0) 的集合元数据物理键
  #[inline(always)]
  pub fn meta_key(user_key: &[u8]) -> TaggedKeyBuf {
    NamespaceDbCodec::encode_meta_key(0, 0, user_key)
  }

  /// 生成当前会话专属方案 A 集合分块子键 (定长 23B，栈优先 64B L1 Cache 对齐，零堆分配)
  #[inline(always)]
  pub fn chunk_key(&self, tag: KeyTag, key_id: u64, version: u64, chunk_id: u32) -> TaggedKeyBuf {
    let prefix = self.session_prefix();
    NamespaceDbCodec::encode_chunk_key_with_prefix(
      prefix.as_slice(),
      tag,
      key_id,
      version,
      chunk_id,
    )
  }

  /// 基于已知前缀切片生成方案 A 集合打平子键（定长 17B 刚性帧 + 字段字节，栈优先 64B L1 Cache 对齐，零堆分配）
  #[inline(always)]
  pub fn sub_key_with_prefix(
    prefix: &[u8],
    tag: KeyTag,
    key_id: u64,
    version: u64,
    field: &[u8],
  ) -> TaggedKeyBuf {
    NamespaceDbCodec::encode_sub_key_with_prefix(prefix, tag, key_id, version, field)
  }

  /// 生成当前会话专属方案 A 集合打平子键（定长 17B 刚性帧 + 字段字节，栈优先 64B L1 Cache 对齐，零堆分配）
  #[inline(always)]
  pub fn sub_key(&self, tag: KeyTag, key_id: u64, version: u64, field: &[u8]) -> TaggedKeyBuf {
    let prefix = self.session_prefix();
    Self::sub_key_with_prefix(prefix.as_slice(), tag, key_id, version, field)
  }

  /// 从完整 TTL 物理键反解 `(ns, db, 用户键)`（供后台过期扫描器逆解，零堆分配）
  ///
  /// 非 TTL 记录键（标签不符或前缀非法）安全返回 None
  #[inline]
  pub fn user_key_from_ttl_key(key: &[u8]) -> Option<(u64, u64, &[u8])> {
    let (ns, db, tag, user_key) = NamespaceDbCodec::decode_tagged_key(key).ok()?;
    (tag == KeyTag::Ttl).then_some((ns, db, user_key))
  }
}
