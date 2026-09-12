//! 主存高级操作（对标 libs/server/Storage/Session/MainStore/AdvancedOps.cs，C# 为 StorageSession partial）

use std::str;

use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::{objects::types::object_output::ObjectOutput, types::GarnetStatus};

/// 主存读-改-写操作描述（对标 C# StringInput.header.cmd 分发）
#[derive(Debug, Clone, Copy)]
pub enum StringRMWOp<'k> {
  /// 整数增减（INCR/INCRBY/DECR/DECRBY）
  Incr { delta: i64 },
  /// 浮点增减（INCRBYFLOAT）
  IncrFloat { delta: f64 },
  /// 尾部追加（APPEND）
  Append(&'k [u8]),
  /// 指定偏移覆写（SETRANGE）
  SetRange { offset: usize, data: &'k [u8] },
}

/// RMW 结果：最终状态与整数值输出（INCR/APPEND/SETRANGE 共用）
pub type RmwResult = (GarnetStatus, Option<i64>);

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 主存读-改-写统一入口（按操作描述分发）
  ///
  /// libs/server/Storage/Session/MainStore/AdvancedOps.cs:RMW_MainStore
  pub async fn rmw_main_store(&self, key: &[u8], op: StringRMWOp<'_>) -> wkv::Result<RmwResult> {
    match op {
      StringRMWOp::Incr { delta } => {
        let (status, v) = self.increment(key, delta).await?;
        Ok((status, (status == GarnetStatus::Ok).then_some(v)))
      }
      StringRMWOp::IncrFloat { delta } => {
        let (status, new_val) = self.increment_by_float(key, delta).await?;
        Ok((status, new_val.map(|f| f as i64)))
      }
      StringRMWOp::Append(data) => {
        let (status, len) = self.append(key, data).await?;
        Ok((status, Some(len as i64)))
      }
      StringRMWOp::SetRange { offset, data } => {
        let (status, len) = self.setrange(key, offset, data).await?;
        Ok((status, Some(len as i64)))
      }
    }
  }

  /// 主存通用读入口
  ///
  /// libs/server/Storage/Session/MainStore/AdvancedOps.cs:Read_MainStore
  pub async fn read_main_store(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    self.read_with_unsafe_context(key).await
  }

  /// 批量预取读（对标 C# Tsavorite ContextReadWithPrefetch 12 项流水线预取，读一致性会话接入 PreBatchKeyConsistentRead / PostBatchKeyConsistentRead 校验）
  ///
  /// libs/server/Storage/Session/MainStore/AdvancedOps.cs:ReadWithPrefetch
  pub async fn read_with_prefetch<K: AsRef<[u8]>>(
    &self,
    keys: &[K],
    mut on_item: impl FnMut(usize, Option<&[u8]>),
  ) -> wkv::Result<()> {
    let mut collector = |i, opt: Option<&[u8]>| {
      self.record_read_outcome(opt.is_some());
      on_item(i, opt);
    };
    if let Some(ctx) = self.consistent_read_context() {
      ctx.read_batch_with(keys, &mut collector).await
    } else {
      self.batch.read_batch_with(keys, &mut collector).await
    }
  }

  /// 整数增减（INCR 族核心，值须可解析为 i64）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:Increment
  pub(crate) async fn increment(&self, key: &[u8], delta: i64) -> wkv::Result<(GarnetStatus, i64)> {
    let parsed = self
      .read_string_with(key, |v| {
        str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok())
      })
      .await?;
    let (current, delta_result) = match parsed {
      None => {
        self
          .upsert_string(key, itoa::Buffer::new().format(delta).as_bytes())
          .await?;
        return Ok((GarnetStatus::Ok, delta));
      }
      Some(None) => return Ok((GarnetStatus::WrongType, 0)),
      Some(Some(c)) => (c, c.checked_add(delta)),
    };
    let Some(updated) = delta_result else {
      return Ok((GarnetStatus::WrongType, current));
    };
    self
      .upsert_string(key, itoa::Buffer::new().format(updated).as_bytes())
      .await?;
    Ok((GarnetStatus::Ok, updated))
  }

  /// 浮点增减（INCRBYFLOAT 核心，支持科学计数法往返）
  pub async fn increment_by_float(
    &self,
    key: &[u8],
    delta: f64,
  ) -> wkv::Result<(GarnetStatus, Option<f64>)> {
    let parsed = self.read_string_with(key, parse_f64).await?;
    let updated = match parsed {
      None => {
        self
          .upsert_string(key, ObjectOutput::format_double(delta).as_bytes())
          .await?;
        return Ok((GarnetStatus::Ok, Some(delta)));
      }
      Some(None) => return Ok((GarnetStatus::WrongType, None)),
      Some(Some(c)) => c + delta,
    };
    self
      .upsert_string(key, ObjectOutput::format_double(updated).as_bytes())
      .await?;
    Ok((GarnetStatus::Ok, Some(updated)))
  }
}

/// 解析字节数组为 f64（容忍前导/尾随空白，对标 Redis string2ld 宽容口径）
pub(crate) fn parse_f64(bytes: &[u8]) -> Option<f64> {
  str::from_utf8(bytes)
    .ok()?
    .trim()
    .parse::<f64>()
    .ok()
    .filter(|v| v.is_finite())
}
