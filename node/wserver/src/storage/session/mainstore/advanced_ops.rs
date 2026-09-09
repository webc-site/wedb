//! 主存高级操作（对标 libs/server/Storage/Session/MainStore/AdvancedOps.cs，C# 为 StorageSession partial）

use std::{str, sync::atomic::Ordering::Relaxed};

use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::api::garnet_status::GarnetStatus;

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

impl<'a, D: Device> StorageSession<'a, D> {
  /// GET（带 pending 出参版）
  ///
  /// 缺口说明：C# 侧 `out bool pending` 表示 Tsavorite 异步 IO 未闭环；
  /// wkv 模型下磁盘候选在本调用内异步闭环，pending 恒为 false 由实现消化。
  ///
  /// libs/server/Storage/Session/MainStore/AdvancedOps.cs:GET_WithPending
  pub async fn get_with_pending(
    &self,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>, bool)> {
    let (status, val) = self.read_with_unsafe_context(key).await?;
    Ok((status, val, false))
  }

  /// 完成挂起的批量 GET 输出
  ///
  /// 缺口说明：C# 侧完成 Tsavorite pending 队列并回填输出数组；wkv 所有读
  /// 调用同步闭环（磁盘候选经异步路径在本调用内完成），恒无遗留 pending，
  /// 直接返回 true（全部输出已就绪）。
  ///
  /// libs/server/Storage/Session/MainStore/AdvancedOps.cs:GET_CompletePending
  pub fn get_complete_pending(&self) -> bool {
    true
  }

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

  /// 范围索引存储读取入口
  ///
  /// 缺口说明：C# 侧经 BfTree RangeIndexManager 读索引记录；wkv 将范围索引
  /// 内置于引擎（`WedbStore::range_index` / `scan_range_callback`），无按键直读
  /// 入口，此处降级为主存字符串读。
  ///
  /// libs/server/Storage/Session/MainStore/AdvancedOps.cs:Read_RangeIndex
  pub async fn read_range_index(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    self.read_with_unsafe_context(key).await
  }

  /// 批量预取读（回调逐键回吐，磁盘候选异步闭环）
  ///
  /// libs/server/Storage/Session/MainStore/AdvancedOps.cs:ReadWithPrefetch
  pub async fn read_with_prefetch<K: AsRef<[u8]>>(
    &self,
    keys: &[K],
    mut on_item: impl FnMut(usize, Option<&[u8]>),
  ) -> wkv::Result<()> {
    for (i, k) in keys.iter().enumerate() {
      let v = self.read_string(k.as_ref()).await?;
      on_item(i, v.as_deref());
    }
    Ok(())
  }

  /// 整数增减（INCR 族核心，值须可解析为 i64）
  pub(crate) async fn increment(&self, key: &[u8], delta: i64) -> wkv::Result<(GarnetStatus, i64)> {
    let Some(val) = self.read_string(key).await? else {
      self
        .upsert_string(key, itoa::Buffer::new().format(delta).as_bytes())
        .await?;
      return Ok((GarnetStatus::Ok, delta));
    };
    let Some(current) = str::from_utf8(&val)
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      return Ok((GarnetStatus::WrongType, 0));
    };
    let Some(updated) = current.checked_add(delta) else {
      return Ok((GarnetStatus::WrongType, current));
    };
    self
      .upsert_string(key, itoa::Buffer::new().format(updated).as_bytes())
      .await?;
    Ok((GarnetStatus::Ok, updated))
  }

  /// 浮点增减（INCRBYFLOAT 核心，支持科学计数法往返）
  pub(crate) async fn increment_by_float(
    &self,
    key: &[u8],
    delta: f64,
  ) -> wkv::Result<(GarnetStatus, Option<f64>)> {
    let Some(val) = self.read_string(key).await? else {
      self
        .upsert_string(key, format_delta(delta).as_bytes())
        .await?;
      return Ok((GarnetStatus::Ok, Some(delta)));
    };
    let Some(current) = parse_f64(&val) else {
      return Ok((GarnetStatus::WrongType, None));
    };
    let updated = current + delta;
    self
      .upsert_string(key, format_delta(updated).as_bytes())
      .await?;
    Ok((GarnetStatus::Ok, Some(updated)))
  }

  /// 未命中计数收纳（供本域各条件写路径复用）
  pub(crate) fn note_notfound(&self) {
    self.session_notfound.fetch_add(1, Relaxed);
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

/// f64 格式化为 Redis INCRBYFLOAT 口径（17 位有效数字最短表示）
pub(crate) fn format_delta(v: f64) -> String {
  format!("{v:.17}")
}
