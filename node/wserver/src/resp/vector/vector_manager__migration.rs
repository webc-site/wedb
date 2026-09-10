//! 迁移接收路径（对标 libs/server/Resp/Vector/VectorManager.Migration.cs）
//!
//! 承接迁移过程中 Vector Set 记录的接收/复制与键序列化：
//!
//! - 元素键：命名空间恒以 4 字节展开（为改写目标命名空间留位），值为原样字节
//! - 索引键：仅元数据（DiskANN 不参与），context 直接生效
//!
//! 复制面以合成 VADD 写入日志（Rust 侧经 [`ReplicationRuntime`] 通道承接）。

use std::{
  collections::{BTreeSet, HashMap},
  ops::Range,
};

use super::{
  vector_manager::{
    CONTEXT_STEP, INDEX_SIZE_BYTES, MIGRATE_ELEMENT_KEY_LOG_ARG, MIGRATE_INDEX_KEY_LOG_ARG,
    VectorManager,
  },
  vector_manager__index::Index,
};

/// 已迁移元素键：命名空间 + 键 + 值的三元组视图。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigratedElementKey {
  /// 命名空间字节（反序列化后即为 4 字节视图）。
  pub namespace_bytes: Vec<u8>,
  /// 元素键字节。
  pub key_bytes: Vec<u8>,
  /// 值字节。
  pub value: Vec<u8>,
}

impl VectorManager {
  /// libs/server/Resp/Vector/VectorManager.Migration.cs:HandleMigratedElementKey
  ///
  /// 处理迁移接收的元素键：展开/收缩命名空间后 upsert，并合成复制写。
  pub fn handle_migrated_element_key(&self, data: &[u8]) -> Result<(), &'static str> {
    let record = Self::deserialize_migrated_element_key(data)?;

    // DEBUG 断言语义：迁移目标上下文必须在用且处于迁移中
    let context = Self::extract_context_from_namespaces(&record.namespace_bytes);
    let block = context & !(CONTEXT_STEP - 1);
    let (context_index, context_value) = Self::decompose_context(block);
    {
      let metas = self.context_metadatas.lock();
      if let Some(meta) = metas.get(context_index) {
        let allow_zero = context_index != 0;
        debug_assert!(
          meta.is_in_use(allow_zero, context_value),
          "不能迁移到未使用上下文"
        );
        debug_assert!(
          meta.is_migrating(allow_zero, context_value),
          "目标上下文未标记迁移中"
        );
        debug_assert!(
          !meta
            .get_need_cleanup()
            .is_some_and(|v| v.contains(&context_value)),
          "不能迁移进正在清理的上下文"
        );
      }
    }

    // 命名空间收缩为最短表示（迁移展开 4 字节，落库前还原）
    let mut ns_buf = [0u8; 4];
    let ns_len = Self::store_context_in_namespace(context, &mut ns_buf);

    // Upsert（登记到键值承接表）
    self
      .key_index_registry
      .lock()
      .insert(record.key_bytes.clone(), [0; INDEX_SIZE_BYTES]);

    // 合成复制写（对齐 C# ReplicateMigratedElementKey 的假写注入）
    self.replication.replicate(
      MIGRATE_ELEMENT_KEY_LOG_ARG,
      &ns_buf[..ns_len],
      &record.key_bytes,
      &record.value,
    );

    Ok(())
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:ReplicateMigratedElementKey
  ///
  /// 元素键迁移后的复制面注入（后迁移假写）。
  pub fn replicate_migrated_element_key(&self, key_bytes: &[u8], value: &[u8]) {
    // 命名空间按键迁移语义恒展开为 4 字节
    self
      .replication
      .replicate(MIGRATE_ELEMENT_KEY_LOG_ARG, &[0; 4], key_bytes, value);
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:HandleMigratedIndexKey
  ///
  /// 处理迁移接收的索引键（元数据；在全部元素键移动后调用）。
  pub fn handle_migrated_index_key(&self, key: &[u8], value: &[u8]) -> Result<(), &'static str> {
    let Some(index) = Index::from_bytes(value) else {
      return Err("migrated index value has invalid size");
    };
    debug_assert_eq!(index.index_ptr, 0, "迁移不应携带索引指针");

    // DEBUG 断言语义：目标上下文必须已分配且标记迁移中
    let (context_index, context_value) = Self::decompose_context(index.context);
    {
      let metas = self.context_metadatas.lock();
      if let Some(meta) = metas.get(context_index) {
        let allow_zero = context_index != 0;
        debug_assert!(
          meta.is_in_use(allow_zero, context_value),
          "迁移要求上下文已分配"
        );
        debug_assert!(
          meta.is_migrating(allow_zero, context_value),
          "迁移要求标记迁移中"
        );
      }
    }

    // 以 CREATE_INDEX_ARG 语义重建原生索引（指针置位）
    self.service.create_index(
      index.context,
      index.dimensions,
      index.reduce_dims,
      index.quant_type,
      index.build_exploration_factor,
      index.num_links,
      index.distance_metric,
    );
    let mut rebuilt = index;
    rebuilt.index_ptr = 1;
    self.write_stored_index(key, &rebuilt.to_bytes());

    // 合成复制写（索引键）
    self
      .replication
      .replicate(MIGRATE_INDEX_KEY_LOG_ARG, &[], key, value);
    Ok(())
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:ReplicateMigratedIndexKey
  ///
  /// 索引键迁移后的复制面注入。
  pub fn replicate_migrated_index_key(&self, key: &[u8], value: &[u8]) {
    self
      .replication
      .replicate(MIGRATE_INDEX_KEY_LOG_ARG, &[], key, value);
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:GetNamespacesForKeys
  ///
  /// 找出给定键中 Vector Set 占用的命名空间（迁移用）；
  /// 命中的键以 index_map 记录其索引 VALUE。
  pub fn get_namespaces_for_keys(
    &self,
    keys: &[Vec<u8>],
    vector_set_keys: &mut HashMap<Vec<u8>, [u8; INDEX_SIZE_BYTES]>,
  ) -> BTreeSet<u64> {
    let mut namespaces = BTreeSet::new();
    for key in keys {
      let Some(stored) = self.read_stored_index(key) else {
        continue;
      };
      let Some(index) = Index::from_bytes(&stored) else {
        continue;
      };
      // 上下文 0 非法（保留），不视为 Vector Set 记录
      if index.context == 0 {
        continue;
      }
      for i in 0..CONTEXT_STEP {
        namespaces.insert(index.context + i);
      }
      vector_set_keys.insert(key.clone(), stored);
    }
    namespaces
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:UpdateMigratedElementNamespaces
  ///
  /// 按新旧命名空间映射改写迁移读取输出中的命名空间字段。
  /// `read_output` 为序列化元素键记录（可变）。
  pub fn update_migrated_element_namespaces(
    old_to_new: &HashMap<u64, u64>,
    read_output: &mut [u8],
  ) {
    let Some((ns_range, old_ns)) = peek_serialized_namespace(read_output) else {
      return;
    };
    let Some(new_ns) = old_to_new.get(&old_ns) else {
      return;
    };
    debug_assert!(*new_ns <= u64::from(u32::MAX), "不应保留如此大的上下文");
    read_output[ns_range].copy_from_slice(&(*new_ns as u32).to_le_bytes());
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:GetMigratedElementKeySerializationSize
  ///
  /// 计算迁移元素键所需存储（命名空间恒展开 4 字节）。
  pub fn get_migrated_element_key_serialization_size(
    key_bytes: &[u8],
    aligned_value: &[u8],
  ) -> usize {
    4 + 4 + // 命名空间长度 + 命名空间（恒 4 字节，为改写留位）
      4 + key_bytes.len() + // 键长度 + 键
      4 + aligned_value.len() // 值长度 + 值
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:SerializeMigratedElementKey
  ///
  /// 序列化迁移元素键记录；返回实际写入长度。
  pub fn serialize_migrated_element_key(
    data_bytes: &mut [u8],
    namespace_bytes: &[u8],
    key_bytes: &[u8],
    aligned_value: &[u8],
  ) -> usize {
    let context = Self::extract_context_from_namespaces(namespace_bytes);
    let mut pos = 0;

    // 命名空间长度（恒 4）、命名空间
    data_bytes[pos..pos + 4].copy_from_slice(&4i32.to_le_bytes());
    pos += 4;
    data_bytes[pos..pos + 4].copy_from_slice(&(context as u32).to_le_bytes());
    pos += 4;

    // 键长度、键
    data_bytes[pos..pos + 4].copy_from_slice(&(key_bytes.len() as i32).to_le_bytes());
    pos += 4;
    data_bytes[pos..pos + key_bytes.len()].copy_from_slice(key_bytes);
    pos += key_bytes.len();

    // 值长度、值
    data_bytes[pos..pos + 4].copy_from_slice(&(aligned_value.len() as i32).to_le_bytes());
    pos += 4;
    data_bytes[pos..pos + aligned_value.len()].copy_from_slice(aligned_value);
    pos + aligned_value.len()
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:DeserializeMigratedElementKey
  ///
  /// 反序列化迁移元素键记录（命名空间恒为 4 字节）。
  pub fn deserialize_migrated_element_key(
    data_bytes: &[u8],
  ) -> Result<MigratedElementKey, &'static str> {
    let mut rest = data_bytes;
    if rest.len() < 4 {
      return Err("truncated namespace length");
    }
    let ns_length = i32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
    debug_assert_eq!(ns_length, 4, "反序列化时命名空间应恒为 4 字节");
    rest = &rest[4..];
    if rest.len() < ns_length {
      return Err("truncated namespace");
    }
    let namespace_bytes = rest[..ns_length].to_vec();
    rest = &rest[ns_length..];

    if rest.len() < 4 {
      return Err("truncated key length");
    }
    let key_length = i32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
    rest = &rest[4..];
    if rest.len() < key_length {
      return Err("truncated key");
    }
    let key_bytes = rest[..key_length].to_vec();
    rest = &rest[key_length..];

    if rest.len() < 4 {
      return Err("truncated value length");
    }
    let value_length = i32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
    rest = &rest[4..];
    if rest.len() < value_length {
      return Err("truncated value");
    }
    let value = rest[..value_length].to_vec();

    Ok(MigratedElementKey {
      namespace_bytes,
      key_bytes,
      value,
    })
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:GetMigratedIndexKeySerializationSize
  ///
  /// 计算迁移索引键所需存储。
  pub fn get_migrated_index_key_serialization_size(key_bytes: &[u8], value_bytes: &[u8]) -> usize {
    4 + key_bytes.len() + 4 + value_bytes.len()
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:SerializeMigratedIndexKey
  ///
  /// 序列化迁移索引键记录（值必须为索引记录）；返回实际写入长度。
  pub fn serialize_migrated_index_key(
    data_bytes: &mut [u8],
    key_bytes: &[u8],
    value_bytes: &[u8],
  ) -> usize {
    debug_assert_eq!(value_bytes.len(), INDEX_SIZE_BYTES, "仅应序列化索引记录");

    let mut pos = 0;
    data_bytes[pos..pos + 4].copy_from_slice(&(key_bytes.len() as i32).to_le_bytes());
    pos += 4;
    data_bytes[pos..pos + key_bytes.len()].copy_from_slice(key_bytes);
    pos += key_bytes.len();
    data_bytes[pos..pos + 4].copy_from_slice(&(value_bytes.len() as i32).to_le_bytes());
    pos += 4;
    data_bytes[pos..pos + value_bytes.len()].copy_from_slice(value_bytes);
    pos + value_bytes.len()
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:DeserializeMigratedIndexKey
  ///
  /// 反序列化迁移索引键记录 → (键, 值)。
  pub fn deserialize_migrated_index_key(data_bytes: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut rest = data_bytes;
    if rest.len() < 4 {
      return None;
    }
    let key_length = i32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
    rest = &rest[4..];
    if rest.len() < key_length {
      return None;
    }
    let key_bytes = rest[..key_length].to_vec();
    rest = &rest[key_length..];

    if rest.len() < 4 {
      return None;
    }
    let value_length = i32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
    rest = &rest[4..];
    if rest.len() < value_length {
      return None;
    }
    Some((key_bytes, rest[..value_length].to_vec()))
  }
}

/// 窥视序列化元素键记录的命名空间区间与值。
fn peek_serialized_namespace(data: &[u8]) -> Option<(Range<usize>, u64)> {
  if data.len() < 8 {
    return None;
  }
  let ns_length = i32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
  // 命名空间恒 4 字节（serialize 侧保证）；异常长度拒绝而非 panic
  if ns_length != 4 || data.len() < 4 + ns_length {
    return None;
  }
  let ns = u32::from_le_bytes(data[4..8].try_into().unwrap());
  Some(((4..8), u64::from(ns)))
}

#[cfg(test)]
mod tests {
  use super::{
    super::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_types::{VectorDistanceMetricType, VectorQuantType, VectorSetFlags},
    },
    *,
  };

  fn sample_index(context: u64) -> Index {
    Index {
      context,
      index_ptr: 0,
      dimensions: 4,
      reduce_dims: 0,
      num_links: 8,
      build_exploration_factor: 64,
      quant_type: VectorQuantType::NoQuant,
      distance_metric: VectorDistanceMetricType::L2,
      flags: VectorSetFlags::NONE,
    }
  }

  #[test]
  fn element_key_serialization_roundtrip() {
    let ns = [7u8];
    let key = b"element-1".to_vec();
    let value = b"vector-bytes".to_vec();

    let needed = VectorManager::get_migrated_element_key_serialization_size(&key, &value);
    let mut buf = vec![0u8; needed];
    let written = VectorManager::serialize_migrated_element_key(&mut buf, &ns, &key, &value);
    assert_eq!(written, needed);

    // 命名空间被恒定展开为 4 字节
    let record = VectorManager::deserialize_migrated_element_key(&buf).unwrap();
    assert_eq!(record.namespace_bytes, vec![7, 0, 0, 0]);
    assert_eq!(record.key_bytes, key);
    assert_eq!(record.value, value);

    // 截断输入报错
    assert!(VectorManager::deserialize_migrated_element_key(&buf[..needed - 1]).is_err());
    assert!(VectorManager::deserialize_migrated_element_key(&[]).is_err());
  }

  #[test]
  fn index_key_serialization_roundtrip() {
    let key = b"my-vector-set".to_vec();
    let value = sample_index(40).to_bytes();

    let needed = VectorManager::get_migrated_index_key_serialization_size(&key, &value);
    let mut buf = vec![0u8; needed];
    let written = VectorManager::serialize_migrated_index_key(&mut buf, &key, &value);
    assert_eq!(written, needed);

    let (back_key, back_value) = VectorManager::deserialize_migrated_index_key(&buf).unwrap();
    assert_eq!(back_key, key);
    assert_eq!(back_value, value.to_vec());
    assert!(VectorManager::deserialize_migrated_index_key(&buf[..3]).is_none());
  }

  #[test]
  fn namespace_rewrite_mapping() {
    let ns = [9u8];
    let key = b"k".to_vec();
    let value = b"v".to_vec();
    let mut buf =
      vec![0u8; VectorManager::get_migrated_element_key_serialization_size(&key, &value)];
    VectorManager::serialize_migrated_element_key(&mut buf, &ns, &key, &value);

    let mut old_to_new = HashMap::new();
    old_to_new.insert(9u64, 80000u64);
    VectorManager::update_migrated_element_namespaces(&old_to_new, &mut buf);

    let record = VectorManager::deserialize_migrated_element_key(&buf).unwrap();
    assert_eq!(record.namespace_bytes, 80000u32.to_le_bytes().to_vec());

    // 无映射 → 不改写
    let mut buf2 = buf.clone();
    old_to_new.remove(&9);
    VectorManager::update_migrated_element_namespaces(&old_to_new, &mut buf2);
    assert_eq!(buf2, buf);
  }

  #[test]
  fn handle_migrated_records_end_to_end() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });

    // 为迁移保留两个上下文（64 上下文块内）
    let reserved = manager.reserve_contexts_for_migration(2).unwrap();
    let target_ctx = reserved[0];
    assert!(target_ctx > 0);

    // 元素键迁移接收
    let mut buf =
      vec![0u8; VectorManager::get_migrated_element_key_serialization_size(b"e1", b"value")];
    VectorManager::serialize_migrated_element_key(
      &mut buf,
      &(target_ctx as u32).to_le_bytes(),
      b"e1",
      b"value",
    );
    manager.handle_migrated_element_key(&buf).unwrap();
    assert!(
      manager
        .key_index_registry
        .lock()
        .contains_key(b"e1".as_slice())
    );
    // 复制面已注入合成 VADD
    assert_eq!(manager.replication.replay_len(), 1);
    assert_eq!(manager.replication.last_arg(), MIGRATE_ELEMENT_KEY_LOG_ARG);

    // 索引键迁移接收
    let idx = sample_index(target_ctx);
    let idx_bytes = idx.to_bytes();
    let mut ib =
      vec![0u8; VectorManager::get_migrated_index_key_serialization_size(b"myset", &idx_bytes)];
    VectorManager::serialize_migrated_index_key(&mut ib, b"myset", &idx_bytes);
    manager
      .handle_migrated_index_key(b"myset", &idx_bytes)
      .unwrap();
    assert_eq!(manager.replication.replay_len(), 2);
    assert_eq!(manager.replication.last_arg(), MIGRATE_INDEX_KEY_LOG_ARG);
    assert_eq!(manager.service.card(target_ctx), 0); // 空索引已建

    // 直接复制面注入
    manager.replicate_migrated_index_key(b"myset", &idx_bytes);
    assert_eq!(manager.replication.replay_len(), 3);
  }

  #[test]
  fn namespaces_for_keys_detection() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });
    let ctx = manager.next_vector_set_context(0).unwrap();
    let idx = sample_index(ctx);
    manager.write_stored_index(b"vs-key", &idx.to_bytes());
    manager.write_stored_index(b"plain-key", &[0u8; 56]);

    let keys = vec![
      b"vs-key".to_vec(),
      b"plain-key".to_vec(),
      b"missing".to_vec(),
    ];
    let mut vector_set_keys = HashMap::new();
    let namespaces = manager.get_namespaces_for_keys(&keys, &mut vector_set_keys);

    // 上下文块展开为 CONTEXT_STEP 个命名空间
    assert_eq!(namespaces.len(), super::CONTEXT_STEP as usize);
    assert!(namespaces.contains(&(ctx)));
    assert!(vector_set_keys.keys().any(|k| k == b"vs-key"));
    // plain-key 全零（context=0）非法，不视为 Vector Set 记录
    assert_eq!(vector_set_keys.len(), 1);
  }
}
