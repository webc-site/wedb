//! 会话键编码域（对标 C# Garnet StorageSession 的物理键编码：ns/db 前缀 + KeyTag + 用户键）
//!
//! 全部为纯编码构造：会话绑定变体取 `session_prefix()`（ns/db 原子变量），静态变体
//! 基于显式前缀切片或默认 (ns=0, db=0)，零堆分配、零 I/O。

use wdev::Device;
use wval::{KeyTag, NamespaceDbCodec, TaggedKeyBuf};

use crate::session::StoreSession;

impl<D: Device> StoreSession<D> {
  /// 生成当前会话专属指定标签物理键（`[prefix][tag][user_key]`，一处定义）
  ///
  /// 全部 `session_*_key` 变体的唯一编码内核：标签带外区分对象信封（ObjectEnvelope）、
  /// 普通字符串（String）、集合元数据（Meta）等记录域
  #[inline(always)]
  pub fn session_tag_key(&self, tag: KeyTag, user_key: &[u8]) -> TaggedKeyBuf {
    let prefix = self.session_prefix();
    Self::session_tag_key_with_prefix(prefix.as_slice(), tag, user_key)
  }

  /// 显式前缀编码指定标签物理键（循环前缀外提内核，transpile SKILL 工程准则；
  /// rust 工程优化无 c# 对应：批量遍历单次外提 `session_prefix()` 消除逐键
  /// 重读 ns/db 原子变量与重算 Varint）
  #[inline(always)]
  pub fn session_tag_key_with_prefix(prefix: &[u8], tag: KeyTag, user_key: &[u8]) -> TaggedKeyBuf {
    NamespaceDbCodec::encode_with_session_prefix(prefix, tag, user_key)
  }

  /// 生成当前会话专属方案 A 集合元数据物理键
  #[inline(always)]
  pub fn session_meta_key(&self, user_key: &[u8]) -> TaggedKeyBuf {
    self.session_tag_key(KeyTag::Meta, user_key)
  }

  /// 生成当前会话专属方案 A 普通字符串物理键 (KeyTag::String)
  #[inline(always)]
  pub fn session_string_key(&self, user_key: &[u8]) -> TaggedKeyBuf {
    self.session_tag_key(KeyTag::String, user_key)
  }

  /// 生成当前会话专属向量存储物理键（定长刚性帧隔离公理：[prefix][KeyTag::Vector][context: 8B be][key]）
  #[inline(always)]
  pub fn vector_key(&self, context: u64, key: &[u8]) -> TaggedKeyBuf {
    let prefix = self.session_prefix();
    Self::vector_key_with_prefix(prefix.as_slice(), context, key)
  }

  /// 显式前缀编码向量存储物理键（循环前缀外提内核，语义与
  /// [`Self::vector_key`] 完全一致；rust 工程优化无 c# 对应：向量批量读单次外提
  /// `session_prefix()` 消除逐键重读 ns/db 原子变量与重算 Varint）
  #[inline(always)]
  pub fn vector_key_with_prefix(prefix: &[u8], context: u64, key: &[u8]) -> TaggedKeyBuf {
    NamespaceDbCodec::encode_vector_key_with_prefix(prefix, context, key)
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
