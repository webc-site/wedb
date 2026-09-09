//! 高级 API 面（对标 libs/server/API/IGarnetAdvancedApi.cs:IGarnetAdvancedApi）
//!
//! C# 侧为"仅供非普通客户端使用"的底层入口；Rust 侧以 [`IGarnetAdvancedApi`]
//! 关联函数委托 [`StorageSession`](crate::storage::session::storage_session::StorageSession)
//! 已实现的高级操作面。

use wdev::Device;

use crate::{
  api::garnet_status::GarnetStatus,
  storage::session::{
    mainstore::advanced_ops::{RmwResult, StringRMWOp},
    storage_session::StorageSession,
    unifiedstore::advanced_ops::UnifiedRMWOp,
  },
};

/// 高级 API 面
pub struct IGarnetAdvancedApi;

impl IGarnetAdvancedApi {
  /// libs/server/API/IGarnetAdvancedApi.cs:GET_WithPending
  pub async fn get__with_pending<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>, bool)> {
    ss.get_with_pending(key).await
  }

  /// libs/server/API/IGarnetAdvancedApi.cs:GET_CompletePending
  pub fn get__complete_pending<D: Device>(ss: &StorageSession<'_, D>) -> bool {
    ss.get_complete_pending()
  }

  /// libs/server/API/IGarnetAdvancedApi.cs:RMW_MainStore
  pub async fn rmw__main_store<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    op: StringRMWOp<'_>,
  ) -> wkv::Result<RmwResult> {
    ss.rmw_main_store(key, op).await
  }

  /// libs/server/API/IGarnetAdvancedApi.cs:Read_MainStore
  pub async fn read__main_store<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.read_main_store(key).await
  }

  /// libs/server/API/IGarnetAdvancedApi.cs:RMW_ObjectStore
  pub async fn rmw__object_store<D: Device, R>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    tag: u8,
    on_load: impl FnOnce(Option<Vec<u8>>) -> Option<(Vec<u8>, R)>,
  ) -> wkv::Result<Option<R>> {
    ss.rmw_object_store(key, tag, on_load).await
  }

  /// libs/server/API/IGarnetAdvancedApi.cs:Read_ObjectStore
  pub async fn read__object_store<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    tag: u8,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.read_object_store(key, tag).await
  }

  /// libs/server/API/IGarnetAdvancedApi.cs:RMW_UnifiedStore
  pub async fn rmw__unified_store<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
    op: UnifiedRMWOp<'_>,
  ) -> wkv::Result<GarnetStatus> {
    ss.rmw_unified_store(key, op).await
  }

  /// libs/server/API/IGarnetAdvancedApi.cs:Read_UnifiedStore
  pub async fn read__unified_store<D: Device>(
    ss: &StorageSession<'_, D>,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    ss.read_unified_store(key).await
  }

  /// libs/server/API/IGarnetAdvancedApi.cs:ReadWithPrefetch
  pub async fn read_with_prefetch<D: Device, K: AsRef<[u8]>>(
    ss: &StorageSession<'_, D>,
    keys: &[K],
    on_item: impl FnMut(usize, Option<&[u8]>),
  ) -> wkv::Result<()> {
    ss.read_with_prefetch(keys, on_item).await
  }
}
