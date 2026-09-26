//! 持久化与存储底座回调交互
//!
//! 包含向量属性存取、全精度向量底层读取与日志回调通道。

use std::mem;

use bytemuck::{bytes_of_mut, cast_slice};

use super::data_provider::{ToDistanceComputer, WedbProvider};
use crate::{
  error::{StoreError, WedbProviderError},
  store::{Context, StoreCallbacks, Term, VectorSetId},
};

impl<T: ToDistanceComputer, S: StoreCallbacks> WedbProvider<T, S> {
  #[inline]
  async fn resolve_iid(&self, context: &Context, id: &VectorSetId) -> Option<u32> {
    let mut iid = u32::MAX;
    if self
      .callbacks
      .read_single_eid(&context.term(Term::IntMap), id, bytes_of_mut(&mut iid))
      .await
    {
      Some(iid)
    } else {
      None
    }
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

  /// 读取元素属性。
  pub async fn get_attributes(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>> {
    let iid = self.resolve_iid(context, id).await?;
    self
      .callbacks
      .read_varsize_iid(&context.term(Term::Attributes), iid)
      .await
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
