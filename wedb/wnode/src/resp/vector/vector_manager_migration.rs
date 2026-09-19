//! 向量集合跨节点迁移承接（1:1 对标 libs/server/Resp/Vector/VectorManager.Migration.cs）
//!
//! rust 存储模型与 C# 的差异投影：C# 索引记录驻留主存储、元素数据驻留
//! DiskANN 磁盘命名空间，迁移先移元素后重建索引；rust 索引记录驻留域内
//! 登记表、HNSW 图驻留内存，迁移先建索引（目标端预留上下文上）后按 VADD
//! 语义重放元素插入（内存图 + 磁盘记录 + AOF 合成写复用既有执行域），
//! 收发两端帧语义对齐 C# VectorSetIndex / 迁移元素记录。

use std::collections::BTreeSet;

use gxhash::HashMap as GxHashMap;
use wvector::{element_data::native_format, store::StoreCallbacks};

use super::{
  ERR_MIGRATED_INDEX, ERR_VECTOR_SET_DISABLED,
  vector_manager::{
    ERR_VECTOR_SERVICE_RESPONSE, INDEX_SIZE_BYTES, VectorAddArgs, VectorManager,
    VectorManagerResult, VectorOpError,
  },
  vector_manager_index::Index,
  vector_manager_locking::registry_key,
  vector_manager_quantization::{QuantizationState, QuantizationStep},
};

/// 目标端上下文未预留文案（CLUSTER RESERVE VECTOR_SET_CONTEXTS 未先行）。
const ERR_CONTEXT_NOT_RESERVED: &[u8] = b"ERR Vector Set context was not reserved for migration";

/// 迁移导出元素条目（元素 id、原生格式向量字节、属性）。
pub struct MigratedElement {
  /// 元素 id。
  pub element: Vec<u8>,
  /// 原生格式向量字节（量化器稳态格式，目标端零转换直插）。
  pub values: Vec<u8>,
  /// 元素属性（无属性为空）。
  pub attributes: Vec<u8>,
}

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.Migration.cs:GetNamespacesForKeys
  /// 的槽位扫描对偶：按迁移槽位集合收集向量集键与其索引记录（SLOTS 迁移
  /// 发现面；registry 上下文命中槽位命名空间全集即收录）。
  ///
  /// 返回登记表复合键（含源端会话域）：源端删除与目标端导入分别经
  /// [`super::vector_manager_locking::split_registry_key`] /
  /// [`super::vector_manager_locking::domain_prefix`] 单点取域；迁移帧
  /// 口径恒为剥域用户键（C# 无域前缀，帧面单点剥离）。
  pub fn get_vector_set_keys_for_slots(
    &self,
    hash_slots: &BTreeSet<i32>,
  ) -> Vec<(Vec<u8>, [u8; INDEX_SIZE_BYTES])> {
    let contexts = self.get_namespaces_for_hash_slots(hash_slots);
    self
      .key_index_registry
      .pin()
      .iter()
      .filter(|(_, bytes)| {
        Index::from_bytes(bytes.as_slice()).is_some_and(|index| contexts.contains(&index.context))
      })
      .map(|(key, bytes)| (key.clone(), *bytes))
      .collect()
  }

  /// 读取迁移键的索引记录（发送侧导出面，源端登记表）。
  pub fn read_migrated_index(&self, prefix: &[u8], key: &[u8]) -> Option<[u8; INDEX_SIZE_BYTES]> {
    self.read_stored_index(prefix, key)
  }

  /// 枚举迁移元素的 (元素 id, 原生向量, 属性) 全集（发送侧导出面）。
  ///
  /// 元素枚举经随机取样通道（去重取样 count >= 基数即全集），向量与属性
  /// 经稳态读取通道；枚举期间索引由上层 sketch TRANSMITTING 门控保护。
  pub fn export_migration_elements(&self, index_value: &[u8]) -> Vec<MigratedElement> {
    let Some(index) = Index::from_bytes(index_value) else {
      return Vec::new();
    };
    let card = self.service.card(index.context) as usize;
    self
      .service
      .sample(index.context, card + 1)
      .into_iter()
      .map(|element| {
        let values = self
          .try_get_raw_embedding(index_value, &element)
          .map(|(bytes, ..)| bytes)
          .unwrap_or_default();
        let attributes = self
          .service
          .get_attribute(index.context, &element)
          .unwrap_or_default();
        MigratedElement {
          element,
          values,
          attributes,
        }
      })
      .collect()
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:HandleMigratedIndexKey
  ///
  /// 目标端导入迁移索引：上下文须已预留（CLUSTER RESERVE
  /// VECTOR_SET_CONTEXTS 置 in_use + migrating），创建内存索引并以
  /// index_ptr=1 写登记表，mark_migration_complete 绑定键槽位后落盘。
  /// 库级定槽（doc/zh/db.md 4.1）：`slot` 由 CLUSTER MIGRATE 头显式携带
  /// （C# 逐键 HashSlot(key) 推导随键级哈希废除）
  pub fn import_migrated_index(
    &self,
    prefix: &[u8],
    key: &[u8],
    index_value: &[u8],
    slot: u16,
  ) -> Result<(), &'static [u8]> {
    if !self.is_enabled() {
      return Err(ERR_VECTOR_SET_DISABLED);
    }
    let Some(mut index) = Index::from_bytes(index_value) else {
      return Err(ERR_MIGRATED_INDEX);
    };
    // C# 断言：迁移帧不得携带索引指针，上下文 0 保留不可指派
    if index.index_ptr != 0 || index.context == 0 {
      return Err(ERR_MIGRATED_INDEX);
    }

    // 上下文预留校验（C# DEBUG 断言的生产化）
    let (context_index, context_value) = Self::decompose_context(index.context);
    {
      let metas = self.context_metadatas.lock();
      let Some(meta) = metas.get(context_index) else {
        return Err(ERR_CONTEXT_NOT_RESERVED);
      };
      let allow_zero = context_index != 0;
      if !meta.is_in_use(allow_zero, context_value) || !meta.is_migrating(allow_zero, context_value)
      {
        return Err(ERR_CONTEXT_NOT_RESERVED);
      }
    }

    // 创建内存索引（磁盘记录随后随元素导入落盘）
    if self
      .service
      .create_index(index.context, index.index_config(), self.callbacks.clone())
      .is_err()
    {
      return Err(ERR_VECTOR_SERVICE_RESPONSE);
    }
    index.index_ptr = 1;
    self.write_stored_index(prefix, key, &index.to_bytes());
    // 索引条目合成写（对照 import_migrated_element 的
    // replicate_vector_set_add）：登记表不随检查点持久化，空向量集无元素
    // 条目可重放，缺此条目目标端重启后整键消失而源端已删
    self.replicate_vector_set_index(prefix, key, &index);

    // 迁移完成标记 + 元数据落盘（对齐 C# MarkMigrationComplete 段）
    {
      let mut metas = self.context_metadatas.lock();
      if let Some(meta) = metas.get_mut(context_index) {
        meta.mark_migration_complete(context_index != 0, context_value, slot);
      }
      drop(metas);
      self.dirty_context_metadatas.lock().insert(context_index);
      self.update_context_metadata();
    }

    // 建表请求调度（对齐 C# requestQuantization 段；量化载荷为复合键）
    if self.service.needs_quantization(index.context) {
      let rk = registry_key(prefix, key);
      let _ = self.quantization_channel.push(QuantizationState::new(
        rk.as_slice().to_vec(),
        QuantizationStep::BuildQuantizationTable,
        0,
      ));
    }
    Ok(())
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:HandleMigratedElementKey
  ///
  /// 目标端导入迁移元素：索引已建立（index 帧先行）时按 VADD 语义插入
  /// （内存图 + 磁盘记录），并合成 AOF 写（对标
  /// libs/server/Resp/Vector/VectorManager.Migration.cs:ReplicateMigratedElementKey
  /// 假写，C# 为 HandleMigratedElementKey 内嵌局部函数；rust 复用标准 VADD
  /// 十参通道，重放侧 remove+insert 幂等收敛）。
  pub fn import_migrated_element(
    &self,
    prefix: &[u8],
    key: &[u8],
    index_value: &[u8],
    element: &[u8],
    values: &[u8],
    attributes: &[u8],
  ) -> Result<(), &'static [u8]> {
    if !self.is_enabled() {
      return Err(ERR_VECTOR_SET_DISABLED);
    }
    let Some(index) = Index::from_bytes(index_value) else {
      return Err(ERR_MIGRATED_INDEX);
    };
    if self.read_stored_index(prefix, key).is_none() {
      // 索引未先行到达（协议乱序）
      return Err(ERR_MIGRATED_INDEX);
    }

    // 几何参数逐项取自索引记录（try_add 的一致性校验按同值放行）
    let args = VectorAddArgs {
      element,
      // 迁移载荷恒为量化器原生格式（发送侧导出面保证），零转换直插
      value_type: native_format(index.quant_type),
      values,
      attributes,
      reduce_dims: index.reduce_dims,
      quant_type: index.quant_type,
      num_links: index.num_links,
      distance_metric: index.distance_metric,
    };
    let inserted = match self.try_add(prefix, key, index_value, &args) {
      Ok(VectorManagerResult::OK) => true,
      // 竞态重放幂等
      Ok(VectorManagerResult::Duplicate) => false,
      Ok(other) => return Err(other.error_msg()),
      Err(VectorOpError { result, .. }) => return Err(result.error_msg()),
    };
    if inserted {
      self.replicate_vector_set_add(
        prefix,
        key,
        index.dimensions,
        index.build_exploration_factor,
        &args,
      );
    }
    Ok(())
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs 配套删除面
  ///（C# DeleteVectorSet 经 BasicGarnetApi.DELETE 触发删除回调；rust 索引
  /// 记录驻留域内登记表，直接登记清理 + 摘除登记表项，键随之消失）。
  /// `rk` 为登记表复合键（源端枚举产物，含源端会话域）。
  pub fn delete_migrated_vector_set_of(&self, rk: &[u8], index_value: &[u8]) {
    if self
      .stored_index_of(rk)
      .as_ref()
      .is_none_or(|arr| arr.as_slice() != index_value)
    {
      log::warn!("迁移删除时登记表记录已变更，按当前记录删除");
    }
    self.delete_vector_set_of(rk);
  }

  /// 迁移上下文重映射（发送侧
  /// libs/server/Resp/Vector/VectorManager.Index.cs:SetContextForMigration
  /// 对偶）：源索引记录按预留映射改写上下文并清零索引指针，目标端导入后
  /// 按磁盘数据重建。
  pub fn remap_index_for_migration(
    &self,
    index_value: &[u8],
    namespace_map: &GxHashMap<u64, u64>,
  ) -> Option<[u8; INDEX_SIZE_BYTES]> {
    let mut index = Index::from_bytes(index_value)?;
    index.context = namespace_map.get(&index.context).copied()?;
    index.index_ptr = 0;
    Some(index.to_bytes())
  }
}
