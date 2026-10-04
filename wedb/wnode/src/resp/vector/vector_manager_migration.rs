//! 向量集合跨节点迁移承接（1:1 对标 libs/server/Resp/Vector/VectorManager.Migration.cs）
//!
//! rust 存储模型与 C# 的差异投影：C# 索引记录驻留主存储、元素数据驻留
//! DiskANN 磁盘命名空间，迁移先移元素后重建索引；rust 索引记录驻留域内
//! 登记表、HNSW 图驻留内存，迁移先建索引（目标端预留上下文上）后按 VADD
//! 语义重放元素插入（内存图 + 磁盘记录 + AOF 合成写复用既有执行域），
//! 收发两端帧语义对齐 C# VectorSetIndex / 迁移元素记录。

use std::collections::BTreeSet;

use wbase::map::HashMap as GxHashMap;
use wvector::{element_data::native_format, error::StoreError, store::StoreCallbacks};

use super::{
  ERR_MIGRATED_INDEX, ERR_VECTOR_SET_DISABLED,
  vector_manager::{
    ERR_VECTOR_SERVICE_RESPONSE, INDEX_SIZE_BYTES, VectorAddArgs, VectorManager,
    VectorManagerResult, VectorOpError,
  },
  vector_manager_index::Index,
  vector_manager_locking::{registry_key, split_registry_key},
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
  /// 按槽位集合与动态槽位计算函数发现向量集键与其索引记录（单一真源现算单点）
  ///
  /// `slot_of` 闭包按登记表复合键解出的物理域 (vns, vdb) 现算槽位，覆盖面
  /// 判据由调用方按其域门禁口径供给（无盘 SYNC 快照按覆盖清单物理域直配、
  /// SLOTS 迁移按「仅默认域」门禁以根域物理号 (0,0) 直判、swapdb 面按逻辑
  /// 域反查），避免绝对信任建表时盖章。
  pub fn get_vector_set_keys_for_slots_with<F>(
    &self,
    hash_slots: &BTreeSet<i32>,
    mut slot_of: F,
  ) -> Vec<(Vec<u8>, [u8; INDEX_SIZE_BYTES])>
  where
    F: FnMut(u64, u64) -> Option<u16>,
  {
    self
      .key_index_registry
      .pin()
      .iter()
      .filter(|(key, _)| {
        let (domain, _) = split_registry_key(key.as_slice());
        slot_of(domain.vns, domain.vdb).is_some_and(|slot| hash_slots.contains(&i32::from(slot)))
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
  /// 元素枚举走 fsm 占用位精确扫描（O(块) 仅访占用 id，起点 0 排除）——
  /// 拒绝采样通道在高删除碎片集合上按已铸 id 空间分配洗牌向量、且每个死
  /// id 各触发一次 ExtMap 缺失读，导出成本随碎片线性膨胀；向量与属性经
  /// 稳态读取通道；枚举期间索引由上层 sketch TRANSMITTING 门控保护。
  ///
  /// 存储读失败一律 Err 上抛中止导出（store.rs 回调契约 + sync_transport
  /// 「严禁降级为空导出」口径）：占用位图读失败禁折叠空集静默丢整集，
  /// 存活元素的向量/id 映射读失败禁物化为空载荷照常导出；属性缺失是
  /// 合法稳态（`Ok(None)`→空载荷）。
  pub async fn export_migration_elements(
    &self,
    index_value: &[u8],
  ) -> Result<Vec<MigratedElement>, StoreError> {
    let Some(index) = Index::from_bytes(index_value) else {
      return Ok(Vec::new());
    };
    let elements = self.service.all_elements(index.context).await?;
    let mut exported = Vec::with_capacity(elements.len());
    for element in elements {
      let values = self
        .service
        .get_full_vector(index.context, &element)
        .await?;
      let attributes = self
        .service
        .get_attribute(index.context, &element)
        .await?
        .unwrap_or_default();
      exported.push(MigratedElement {
        element,
        values,
        attributes,
      });
    }
    Ok(exported)
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:HandleMigratedIndexKey
  ///
  /// 目标端导入迁移索引：上下文须已预留（CLUSTER RESERVE
  /// VECTOR_SET_CONTEXTS 置 in_use + migrating），创建内存索引并以
  /// index_ptr=1 写登记表，mark_migration_complete 绑定键槽位后落盘。
  /// 库级定槽（doc/zh/db.md 4.1）：`slot` 由 CLUSTER MIGRATE 头显式携带
  /// （C# 逐键 HashSlot(key) 推导随键级哈希废除）
  pub async fn import_migrated_index(
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
      .await
      .is_err()
    {
      return Err(ERR_VECTOR_SERVICE_RESPONSE);
    }
    index.index_ptr = 1;
    let index_bytes = index.to_bytes();
    if !self.write_stored_index(prefix, key, &index_bytes).await {
      // 写透失败臂回收刚建原生索引（对位 C# HandleMigratedIndexKey
      // writeRes != OK → Service.DropIndex 后 throw）；context in_use 与
      // migrating 位滞留同 C# throw 后形态，交既有重启 reconcile 弃迁臂收敛
      self.drop_in_memory_index(&index_bytes);
      return Err(ERR_VECTOR_SERVICE_RESPONSE);
    }
    // 索引条目合成写（对照 import_migrated_element 的 replicate_vector_set_add）：
    // 登记表虽经写透旁路记录持久化，但副本端无迁移帧，仍需合成写注入 AOF 复制链供副本同步
    if self
      .replicate_vector_set_index(prefix, key, &index)
      .is_err()
    {
      // 复制失败臂同形回收（登记已落、原生索引弃置，位滞留交重启 reconcile 收敛）
      self.drop_in_memory_index(&index_bytes);
      return Err(ERR_VECTOR_SERVICE_RESPONSE);
    }

    // 迁移完成标记 + 元数据落盘（对齐 C# MarkMigrationComplete 段）。
    // 元数据守卫块作用域精确限定，写透 `.await` 前已让位（parking_lot
    // 守卫绝不跨 await）
    {
      let mut metas = self.context_metadatas.lock();
      if let Some(meta) = metas.get_mut(context_index) {
        meta.mark_migration_complete(context_index != 0, context_value, slot);
      }
    }
    self.dirty_context_metadatas.lock().insert(context_index);
    self.update_context_metadata().await;

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
  /// libs/server/Resp/Vector/VectorManager.Migration.cs:CompletePending 的
  /// 合并承接：C# 两处局部 CompletePending（元素/索引写后 pending IO 收割，
  /// CompletePendingWithOutputs(wait: true)）在 rust 无 pending 态——存储
  /// 会话写内联完成即收割（冷读收割见 vector_store_callbacks 单点），本
  /// import 路径的同步写直返即 C# 收割后的完成语义。
  ///
  /// 目标端导入迁移元素：索引已建立（index 帧先行）时按 VADD 语义插入
  /// （内存图 + 磁盘记录），并合成 AOF 写（对标
  /// libs/server/Resp/Vector/VectorManager.Migration.cs:ReplicateMigratedElementKey
  /// 假写，C# 为 HandleMigratedElementKey 内嵌局部函数；rust 复用标准 VADD
  /// 十参通道，重放侧 remove+insert 幂等收敛）。
  pub async fn import_migrated_element(
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
    let inserted = match self.try_add(prefix, key, index_value, &args).await {
      Ok(VectorManagerResult::OK) => true,
      // 竞态重放幂等
      Ok(VectorManagerResult::Duplicate) => false,
      Ok(other) => return Err(other.error_msg()),
      Err(VectorOpError { result, .. }) => return Err(result.error_msg()),
    };
    if inserted {
      self
        .replicate_vector_set_add(
          prefix,
          key,
          index.dimensions,
          index.build_exploration_factor,
          &args,
        )
        .map_err(|_| ERR_VECTOR_SERVICE_RESPONSE)?;
    }
    Ok(())
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
