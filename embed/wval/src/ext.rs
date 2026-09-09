use wrecord::{RecordMut, RecordRef};

use crate::{
  bftag::BfTag,
  error::Result,
  meta::{MetaValue, SubKeyRef},
  tag::KeyTag,
  zset::{ZMemberKeyRef, ZScoreKeyRef, ZSetSubKeyCodec},
};

/// 值层对记录视图的扩展访问
///
/// 记录格式层（wrecord）不感知任何值层类型；元数据与子键解析以扩展 trait
/// 形式挂回 RecordRef / RecordMut（对标 C# Garnet 在 server 侧以扩展方法
/// 桥接 SpanByte 与值对象），维持 `wval -> wrecord` 单向依赖。
pub trait RecordValueExt {
  /// 提取记录键的命名空间前缀标签（若键非空且首字节为有效标签）
  fn tag(&self) -> Option<KeyTag>;

  /// 提取记录键的 BfTree 物理前缀标签（若键非空且首字节为有效 BfTag）
  fn bftag(&self) -> Option<BfTag>;

  /// 若当前记录为集合元数据记录，从值切片解析 MetaValue
  fn meta_value(&self) -> Result<MetaValue>;

  /// 若当前记录为打平子键，从键切片零拷贝解析 SubKeyRef
  fn sub_key_ref(&self) -> Result<SubKeyRef<'_>>;

  /// 若当前记录为有序集合成员键 (BfTag::ZMember / 0)，从键切片零拷贝解析 ZMemberKeyRef
  fn zmember_key_ref(&self) -> Result<ZMemberKeyRef<'_>>;

  /// 若当前记录为有序集合分值键 (BfTag::ZScore / 1)，从键切片零拷贝解析 ZScoreKeyRef
  fn zscore_key_ref(&self) -> Result<ZScoreKeyRef<'_>>;
}

/// 从键首字节解析命名空间标签（空键返回 None）
#[inline]
fn key_tag(key: &[u8]) -> Option<KeyTag> {
  if let [first, ..] = key {
    KeyTag::from_repr(*first)
  } else {
    None
  }
}

/// 从键首字节解析 BfTree 物理前缀标签（空键返回 None）
#[inline]
fn key_bftag(key: &[u8]) -> Option<BfTag> {
  if let [first, ..] = key {
    BfTag::from_repr(*first)
  } else {
    None
  }
}

impl RecordValueExt for RecordRef<'_> {
  #[inline]
  fn tag(&self) -> Option<KeyTag> {
    key_tag(self.key)
  }

  #[inline]
  fn bftag(&self) -> Option<BfTag> {
    key_bftag(self.key)
  }

  #[inline]
  fn meta_value(&self) -> Result<MetaValue> {
    MetaValue::from_slice(self.value)
  }

  #[inline]
  fn sub_key_ref(&self) -> Result<SubKeyRef<'_>> {
    SubKeyRef::from_slice(self.key)
  }

  #[inline]
  fn zmember_key_ref(&self) -> Result<ZMemberKeyRef<'_>> {
    ZSetSubKeyCodec::decode_member_key(self.key)
  }

  #[inline]
  fn zscore_key_ref(&self) -> Result<ZScoreKeyRef<'_>> {
    ZSetSubKeyCodec::decode_score_key(self.key)
  }
}

impl RecordValueExt for RecordMut<'_> {
  #[inline]
  fn tag(&self) -> Option<KeyTag> {
    key_tag(self.key())
  }

  #[inline]
  fn bftag(&self) -> Option<BfTag> {
    key_bftag(self.key())
  }

  #[inline]
  fn meta_value(&self) -> Result<MetaValue> {
    MetaValue::from_slice(self.value())
  }

  #[inline]
  fn sub_key_ref(&self) -> Result<SubKeyRef<'_>> {
    SubKeyRef::from_slice(self.key())
  }

  #[inline]
  fn zmember_key_ref(&self) -> Result<ZMemberKeyRef<'_>> {
    ZSetSubKeyCodec::decode_member_key(self.key())
  }

  #[inline]
  fn zscore_key_ref(&self) -> Result<ZScoreKeyRef<'_>> {
    ZSetSubKeyCodec::decode_score_key(self.key())
  }
}

/// 值层对可变记录视图的原位写扩展
pub trait RecordValueMutExt {
  /// 原位更新集合元数据（要求值长度与 MetaValue 一致）
  fn update_meta_value(&mut self, meta: &MetaValue) -> Result<()>;
}

impl RecordValueMutExt for RecordMut<'_> {
  #[inline]
  fn update_meta_value(&mut self, meta: &MetaValue) -> Result<()> {
    Ok(self.update_value_in_place(&meta.to_bytes())?)
  }
}
