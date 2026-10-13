//! 持久化与存储底座回调交互
//!
//! 包含向量属性存取、全精度向量底层读取与日志回调通道。

use std::mem;

use bytemuck::cast_slice;

use super::data_provider::{ToDistanceComputer, WedbProvider};
use crate::{
  error::{StoreError, WedbProviderError},
  store::{Context, StoreCallbacks, Term, VectorSetId},
};

impl<T: ToDistanceComputer, S: StoreCallbacks> WedbProvider<T, S> {
  /// 外部 id → 内部 id（单点委托 [`Self::to_internal_id`]，缺失/读失败折叠为
  /// `None`，调用方统一映射 `StoreError::Read`）。
  #[inline]
  async fn resolve_iid(&self, context: &Context, id: &VectorSetId) -> Option<u32> {
    self.to_internal_id(context, id).await.ok()
  }

  /// 写入元素属性。
  pub async fn set_attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
    data: &[u8],
  ) -> Result<(), WedbProviderError> {
    let iid = self
      .resolve_iid(context, id)
      .await
      .ok_or(StoreError::Read)?;
    if self
      .callbacks
      .write_iid(&context.term(Term::Attributes), iid, data)
      .await
    {
      Ok(())
    } else {
      Err(StoreError::Write.into())
    }
  }

  /// 删除元素属性。
  pub async fn delete_attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> Result<(), WedbProviderError> {
    let iid = self
      .resolve_iid(context, id)
      .await
      .ok_or(StoreError::Read)?;
    if self
      .callbacks
      .delete_iid(&context.term(Term::Attributes), iid)
      .await
    {
      Ok(())
    } else {
      Err(StoreError::Delete.into())
    }
  }

  /// 读取元素属性（[`StoreCallbacks::read`] 契约：单读 false=缺失——属性项
  /// 缺失是合法稳态，映射 `Ok(None)` 照常导出空载荷；元素外部→内部 id 解析
  /// 失败对存活元素属异常，映射 Err 供调用方（迁移导出面）中止）。
  pub async fn get_attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> Result<Option<Vec<u8>>, WedbProviderError> {
    let iid = self
      .resolve_iid(context, id)
      .await
      .ok_or(StoreError::Read)?;
    Ok(
      self
        .callbacks
        .read_varsize_iid(&context.term(Term::Attributes), iid)
        .await,
    )
  }

  /// 读取元素完整全精度向量。
  pub async fn get_full_vector(
    &self,
    context: &Context,
    iid: u32,
  ) -> Result<Vec<T>, WedbProviderError> {
    let mut v = vec![T::default(); self.dim];
    if iid == 0 {
      let cache = self.start_point_cache.pin();
      let guard = match cache.get(&iid) {
        Some(r) => r,
        None => return Err(StoreError::Read.into()),
      };
      v.copy_from_slice(cast_slice::<u8, T>(guard));
      return Ok(v);
    }

    if !self
      .callbacks
      .read_single_iid(&context.term(Term::Vector), iid, &mut v)
      .await
    {
      return Err(StoreError::Read.into());
    }

    Ok(v)
  }

  #[inline]
  pub(crate) fn full_vector_size(&self) -> usize {
    self.dim * mem::size_of::<T>()
  }
}
