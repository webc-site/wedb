use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::fs::{self, File};
use std::io::{self, Error};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use fjall::Keyspace;
use futures::{Stream, TryStreamExt};
use log::info;
use zenoh_raft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use zenoh_raft::{EntryPayload, Membership, OptionalSend};

use super::key::{
  LAST_APPLIED_LOG_KEY, NODES_KEY, SM_DATA_FAMILY, SM_META_FAMILY, TTL_IDX_KEY_PREFIX,
  TTL_KEY_PREFIX,
};
use super::snapshot::{build_snapshot, get_current_snapshot, recover_snapshot};
use crate::engine::FjallEngine;
use crate::types::{
  AppliedState, Cmd, CompactLogId, LogId, Node, NodeId, Operation, RaftCodec, Snapshot,
  SnapshotData, SnapshotMeta, StoredMembership, SysData, TxnReply, TxnReq, TypeConfig, UpsertKV,
  decode, encode, read_logs_err,
};
use crate::util::now_millis;

pub type KvPair = (Vec<u8>, Vec<u8>);

/// 延迟节点操作，在 apply 中统一合并到同一个 batch 原子提交
enum DeferredNodeOp {
  Add(Node),
  Remove(NodeId),
  SetNodes(BTreeMap<NodeId, Node>),
}

pub struct FjallStateMachine {
  engine: Arc<FjallEngine>,
  cf_meta: Keyspace,
  cf_data: Keyspace,
  snapshot_dir: PathBuf,
  sys_data: Arc<Mutex<SysData>>,
}

impl Debug for FjallStateMachine {
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    f.debug_struct("FjallStateMachine").finish()
  }
}

impl Clone for FjallStateMachine {
  fn clone(&self) -> Self {
    Self {
      engine: self.engine.clone(),
      cf_meta: self.cf_meta.clone(),
      cf_data: self.cf_data.clone(),
      snapshot_dir: self.snapshot_dir.clone(),
      sys_data: self.sys_data.clone(),
    }
  }
}

impl FjallStateMachine {
  pub fn create_snapshot_temp_file(&self, snapshot_id: &str) -> Result<File, io::Error> {
    File::create(self.snapshot_dir.join(format!("{snapshot_id}_incomplete")))
  }

  fn lock_sys_data(&self) -> Result<MutexGuard<'_, SysData>, io::Error> {
    self
      .sys_data
      .lock()
      .map_err(|e| Error::other(format!("Mutex lock failed: {e}")))
  }

  pub async fn new(
    engine: Arc<FjallEngine>,
    data_dir: PathBuf,
  ) -> Result<FjallStateMachine, Error> {
    let cf_meta = engine
      .keyspace(SM_META_FAMILY)
      .map_err(|e| Error::other(e.to_string()))?;
    let cf_data = engine
      .keyspace(SM_DATA_FAMILY)
      .map_err(|e| Error::other(e.to_string()))?;

    let snapshot_dir = data_dir.join("snapshot");
    fs::create_dir_all(&snapshot_dir)?;

    let sys_data = Self::recover_sys_data(&cf_meta)?;

    Ok(Self {
      engine,
      cf_meta,
      cf_data,
      snapshot_dir,
      sys_data: Arc::new(Mutex::new(sys_data)),
    })
  }

  fn compute_membership(nodes: &BTreeMap<NodeId, Node>) -> Result<StoredMembership, io::Error> {
    let node_ids: BTreeSet<NodeId> = nodes.keys().cloned().collect();
    if node_ids.is_empty() {
      return Ok(StoredMembership::default());
    }

    let membership = Membership::new(vec![node_ids], nodes.clone())
      .map_err(|e| io::Error::other(format!("Failed to create membership: {e}")))?;

    Ok(StoredMembership::new(None, membership))
  }

  fn recover_sys_data(cf_meta: &Keyspace) -> Result<SysData, io::Error> {
    let last_applied = match cf_meta.get(LAST_APPLIED_LOG_KEY) {
      Ok(Some(v)) => {
        let id = LogId::decode_from(&v).map_err(read_logs_err)?;
        Some(id)
      }
      Ok(None) => None,
      Err(e) => return Err(read_logs_err(e)),
    };

    let nodes: BTreeMap<NodeId, Node> = cf_meta
      .get(NODES_KEY)
      .map_err(read_logs_err)?
      .map(|bytes| decode(&bytes).map_err(read_logs_err))
      .transpose()?
      .unwrap_or_default();

    let membership = Self::compute_membership(&nodes)?;

    Ok(SysData {
      last_applied,
      nodes,
      membership,
    })
  }

  pub fn get_last_applied_log_id(&self) -> Result<Option<LogId>, io::Error> {
    Ok(self.lock_sys_data()?.last_applied)
  }

  pub fn get_last_membership(&self) -> Result<StoredMembership, io::Error> {
    Ok(self.lock_sys_data()?.membership.clone())
  }

  pub fn get_applied_state(&self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
    let sys_data = self.lock_sys_data()?;
    Ok((sys_data.last_applied, sys_data.membership.clone()))
  }

  /// 零堆分配的高性能 TTL 键前缀闭包封装（<= 120 字节直接在栈上构造）
  #[inline]
  pub fn with_ttl_key<R>(key: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
    let p_len = TTL_KEY_PREFIX.len();
    if key.len() <= (128 - p_len) {
      let mut buf = [0u8; 128];
      buf[..p_len].copy_from_slice(TTL_KEY_PREFIX);
      buf[p_len..p_len + key.len()].copy_from_slice(key);
      f(&buf[..p_len + key.len()])
    } else {
      let mut buf = Vec::with_capacity(p_len + key.len());
      buf.extend_from_slice(TTL_KEY_PREFIX);
      buf.extend_from_slice(key);
      f(&buf)
    }
  }

  /// 零堆分配的高性能 TTL 时间序索引键闭包封装（_ttl_idx:{expire_at_ms_be_bytes}:{key}）
  /// <= 110 字节直接在栈上构造（9 + 8 + 110 <= 127 字节）
  #[inline]
  pub fn with_ttl_idx_key<R>(expire_at_ms: u64, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
    let p_len = TTL_IDX_KEY_PREFIX.len();
    if key.len() <= (128 - p_len - 8) {
      let mut buf = [0u8; 128];
      buf[..p_len].copy_from_slice(TTL_IDX_KEY_PREFIX);
      buf[p_len..p_len + 8].copy_from_slice(&expire_at_ms.to_be_bytes());
      buf[p_len + 8..p_len + 8 + key.len()].copy_from_slice(key);
      f(&buf[..p_len + 8 + key.len()])
    } else {
      let mut buf = Vec::with_capacity(p_len + 8 + key.len());
      buf.extend_from_slice(TTL_IDX_KEY_PREFIX);
      buf.extend_from_slice(&expire_at_ms.to_be_bytes());
      buf.extend_from_slice(key);
      f(&buf)
    }
  }

  #[inline]
  fn remove_old_ttl_idx(&self, key_bytes: &[u8], batch: Option<&mut fjall::OwnedWriteBatch>) {
    if let Ok(Some(old_expire_at)) = self.get_ttl_expire_at_bytes(key_bytes) {
      Self::with_ttl_idx_key(old_expire_at, key_bytes, |old_idx| {
        if let Some(b) = batch {
          b.remove(&self.cf_meta, old_idx);
        } else {
          let _ = self.cf_meta.remove(old_idx);
        }
      });
    }
  }

  pub fn set_ttl(&self, key: &str, expire_at_ms: u64) -> Result<(), io::Error> {
    let key_bytes = key.as_bytes();
    self.remove_old_ttl_idx(key_bytes, None);
    Self::with_ttl_key(key_bytes, |meta_key| {
      self
        .cf_meta
        .insert(meta_key, expire_at_ms.to_be_bytes())
        .map_err(read_logs_err)
    })?;
    Self::with_ttl_idx_key(expire_at_ms, key_bytes, |idx_key| {
      self.cf_meta.insert(idx_key, []).map_err(read_logs_err)
    })?;
    Ok(())
  }

  #[inline]
  pub fn get_ttl_expire_at_bytes(&self, key: &[u8]) -> Result<Option<u64>, io::Error> {
    let val = Self::with_ttl_key(key, |meta_key| {
      self.cf_meta.get(meta_key).map_err(read_logs_err)
    })?;

    if let Some(val) = val
      && let Some(&bytes) = val.first_chunk::<8>()
    {
      return Ok(Some(u64::from_be_bytes(bytes)));
    }
    Ok(None)
  }

  #[inline]
  pub fn get_ttl_expire_at(&self, key: &str) -> Result<Option<u64>, io::Error> {
    self.get_ttl_expire_at_bytes(key.as_bytes())
  }

  pub fn remove_ttl(&self, key: &str) -> Result<bool, io::Error> {
    let key_bytes = key.as_bytes();
    self.remove_old_ttl_idx(key_bytes, None);
    Self::with_ttl_key(key_bytes, |meta_key| {
      let existed = self.cf_meta.contains_key(meta_key).map_err(read_logs_err)?;
      if existed {
        self.cf_meta.remove(meta_key).map_err(read_logs_err)?;
      }
      Ok(existed)
    })
  }

  #[inline]
  pub fn is_expired_bytes(&self, key: &[u8]) -> bool {
    if let Ok(Some(expire_at_ms)) = self.get_ttl_expire_at_bytes(key) {
      return now_millis() >= expire_at_ms;
    }
    false
  }

  #[inline]
  pub fn is_expired(&self, key: &str) -> bool {
    self.is_expired_bytes(key.as_bytes())
  }

  #[inline]
  pub fn get_kv(&self, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
    let slice = match self.cf_data.get(key).map_err(read_logs_err)? {
      Some(v) => v,
      None => return Ok(None),
    };
    if self.is_expired(key) {
      return Ok(None);
    }
    Ok(Some(slice.to_vec()))
  }

  /// 收集 KV 对，如果 has_ttl 为 true 则跳过已过期的条目
  fn collect_kv(
    &self,
    iter: impl Iterator<Item = fjall::Guard>,
    has_ttl: bool,
  ) -> Result<Vec<KvPair>, io::Error> {
    let mut results = Vec::new();
    for g in iter {
      let (k, v) = g.into_inner().map_err(read_logs_err)?;
      if !has_ttl || !self.is_expired_bytes(&k) {
        results.push((k.to_vec(), v.to_vec()));
      }
    }
    Ok(results)
  }

  pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvPair>, io::Error> {
    let has_ttl = Self::with_ttl_key(prefix, |meta_prefix| {
      self.cf_meta.prefix(meta_prefix).next().is_some()
    });
    self.collect_kv(self.cf_data.prefix(prefix), has_ttl)
  }

  pub fn scan_all(&self) -> Result<Vec<KvPair>, io::Error> {
    let has_ttl = self.cf_meta.prefix(TTL_KEY_PREFIX).next().is_some();
    self.collect_kv(self.cf_data.iter(), has_ttl)
  }

  /// 主动扫描并物理清理已过期的 TTL 键及对应数据
  /// 基于时间序索引 _ttl_idx:{expire_at_ms_be}:{key} 毫秒级有序扫描，未到期立即 break 剪枝
  pub fn sweep_expired_keys(&self, max_keys: usize) -> Result<usize, io::Error> {
    let now = now_millis();
    let mut batch = self.engine.db().batch();
    let mut count = 0;
    let p_len = TTL_IDX_KEY_PREFIX.len();

    for g in self.cf_meta.prefix(TTL_IDX_KEY_PREFIX) {
      let (idx_key, _) = g.into_inner().map_err(read_logs_err)?;
      if idx_key.len() >= p_len + 8 {
        let expire_at = u64::from_be_bytes(
          *idx_key[p_len..p_len + 8]
            .first_chunk::<8>()
            .expect("length already checked"),
        );

        // 核心剪枝：索引按时间升序严格排列，一旦遇到未过期的键，后续所有键必然均未过期！
        if now < expire_at {
          break;
        }

        let user_key = &idx_key[p_len + 8..];
        batch.remove(&self.cf_data, user_key);
        Self::with_ttl_key(user_key, |meta_key| {
          batch.remove(&self.cf_meta, meta_key);
        });
        batch.remove(&self.cf_meta, idx_key);

        count += 1;
        if count >= max_keys {
          break;
        }
      }
    }

    if count > 0 {
      batch.commit().map_err(read_logs_err)?;
    }
    Ok(count)
  }

  pub fn keyspace_data(&self) -> &Keyspace {
    &self.cf_data
  }

  pub fn set_last_applied_log_id(&self, log_id: Option<LogId>) -> Result<(), io::Error> {
    let mut sys_data = self.lock_sys_data()?;

    match log_id {
      Some(id) => {
        let compact = CompactLogId::from(&id);
        let data = encode(&compact).map_err(read_logs_err)?;
        self
          .cf_meta
          .insert(LAST_APPLIED_LOG_KEY, data)
          .map_err(read_logs_err)?;
        sys_data.last_applied = log_id;
      }
      None => {
        self
          .cf_meta
          .remove(LAST_APPLIED_LOG_KEY)
          .map_err(read_logs_err)?;
        sys_data.last_applied = None;
      }
    }
    Ok(())
  }

  pub fn get_nodes(&self) -> Result<BTreeMap<NodeId, Node>, io::Error> {
    Ok(self.lock_sys_data()?.nodes.clone())
  }

  pub fn contains_node(&self, node_id: NodeId) -> Result<bool, io::Error> {
    Ok(self.lock_sys_data()?.nodes.contains_key(&node_id))
  }

  pub fn add_node(&self, node: Node) -> Result<(), io::Error> {
    let mut sys_data = self.lock_sys_data()?;
    if let Some(existing) = sys_data.nodes.get(&node.node_id)
      && existing == &node
    {
      return Ok(());
    }
    sys_data.nodes.insert(node.node_id, node);
    self.persist_nodes(&mut sys_data)
  }

  pub fn set_nodes(&self, nodes: BTreeMap<NodeId, Node>) -> Result<(), io::Error> {
    let mut sys_data = self.lock_sys_data()?;
    if sys_data.nodes == nodes {
      return Ok(());
    }
    sys_data.nodes = nodes;
    self.persist_nodes(&mut sys_data)
  }

  pub fn remove_node(&self, node_id: NodeId) -> Result<(), io::Error> {
    let mut sys_data = self.lock_sys_data()?;
    sys_data.nodes.remove(&node_id);
    self.persist_nodes(&mut sys_data)
  }

  /// 重算 membership 并持久化节点列表
  fn persist_nodes(&self, sys_data: &mut SysData) -> Result<(), io::Error> {
    sys_data.membership = Self::compute_membership(&sys_data.nodes)?;
    let data = encode(&sys_data.nodes).map_err(read_logs_err)?;
    self.cf_meta.insert(NODES_KEY, data).map_err(read_logs_err)
  }

  fn apply_node_ops_to_batch(
    &self,
    ops: &[DeferredNodeOp],
    batch: &mut fjall::OwnedWriteBatch,
  ) -> Result<BTreeMap<NodeId, Node>, io::Error> {
    // 仅持锁克隆当前节点快照，立即释放
    let mut nodes = {
      let sys_data = self.lock_sys_data()?;
      sys_data.nodes.clone()
    };
    for op in ops {
      match op {
        DeferredNodeOp::Add(node) => {
          nodes.insert(node.node_id, node.clone());
        }
        DeferredNodeOp::Remove(node_id) => {
          nodes.remove(node_id);
        }
        DeferredNodeOp::SetNodes(new_nodes) => {
          nodes = new_nodes.clone();
        }
      }
    }
    // 序列化节点列表，写入 batch（与 KV 数据同一个原子 batch）
    let data = encode(&nodes).map_err(read_logs_err)?;
    batch.insert(&self.cf_meta, NODES_KEY, data);
    Ok(nodes)
  }

  #[inline]
  fn remove_old_ttl_idx_and_key(&self, key_bytes: &[u8], batch: &mut fjall::OwnedWriteBatch) {
    if let Ok(Some(old_expire_at)) = self.get_ttl_expire_at_bytes(key_bytes) {
      Self::with_ttl_idx_key(old_expire_at, key_bytes, |old_idx| {
        batch.remove(&self.cf_meta, old_idx);
      });
    }
    Self::with_ttl_key(key_bytes, |meta_key| {
      batch.remove(&self.cf_meta, meta_key);
    });
  }

  #[inline]
  fn apply_upsert_kv(&self, kv: &UpsertKV, batch: &mut fjall::OwnedWriteBatch) {
    let key_bytes = kv.key.as_bytes();
    match &kv.value {
      Operation::Update(value) => {
        batch.insert(&self.cf_data, key_bytes, value);
        if let Some(eat) = kv.expire_at_ms {
          self.remove_old_ttl_idx_and_key(key_bytes, batch);
          if eat > 0 {
            Self::with_ttl_key(key_bytes, |meta_key| {
              batch.insert(&self.cf_meta, meta_key, eat.to_be_bytes());
            });
            Self::with_ttl_idx_key(eat, key_bytes, |idx_key| {
              batch.insert(&self.cf_meta, idx_key, []);
            });
          }
        }
      }
      Operation::Delete => {
        batch.remove(&self.cf_data, key_bytes);
        self.remove_old_ttl_idx_and_key(key_bytes, batch);
      }
    }
  }

  /// 纯分片直写快速通道：无需 Raft 共识网络往返，直接原子批量写入本地存储（对标 Kvrocks 本地写吞吐）
  pub fn apply_batch_upsert_direct(&self, entries: &[UpsertKV]) -> Result<(), io::Error> {
    let mut batch = self.engine.db().batch();
    for kv in entries {
      self.apply_upsert_kv(kv, &mut batch);
    }
    batch.commit().map_err(read_logs_err)?;
    Ok(())
  }

  /// 纯分片事务评估与批处理写入逻辑
  fn execute_txn_to_batch(
    &self,
    req: &TxnReq,
    batch: &mut fjall::OwnedWriteBatch,
  ) -> Result<TxnReply, io::Error> {
    let mut all_conditions_met = true;
    for condition in &req.condition {
      let actual_value = self
        .cf_data
        .get(condition.key.as_bytes())
        .map_err(read_logs_err)?;
      let condition_met = condition.expected.evaluate(actual_value.as_deref());
      if !condition_met {
        all_conditions_met = false;
        break;
      }
    }

    let ops_to_exec = if all_conditions_met {
      &req.if_then
    } else {
      &req.else_then
    };

    let prev_values = if req.return_previous {
      let mut values = Vec::with_capacity(ops_to_exec.len());
      for kv in ops_to_exec {
        let old_val = self
          .cf_data
          .get(kv.key.as_bytes())
          .map_err(read_logs_err)?
          .map(|v| v.to_vec());
        values.push(old_val);
      }
      values
    } else {
      Vec::new()
    };

    for kv in ops_to_exec {
      self.apply_upsert_kv(kv, batch);
    }

    Ok(TxnReply::Success {
      branch: all_conditions_met,
      prev_values,
    })
  }

  /// 纯分片事务直写快速通道：直接在本地原子评估条件并执行分支操作
  pub fn apply_txn_direct(&self, req: TxnReq) -> Result<TxnReply, io::Error> {
    let mut batch = self.engine.db().batch();
    let reply = self.execute_txn_to_batch(&req, &mut batch)?;
    batch.commit().map_err(read_logs_err)?;
    Ok(reply)
  }
}

impl RaftSnapshotBuilder<TypeConfig> for FjallStateMachine {
  type SnapshotData = SnapshotData;

  async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
    let last_applied_log = self.get_last_applied_log_id()?;
    let last_membership = self.get_last_membership()?;

    build_snapshot(
      &self.engine,
      &self.snapshot_dir,
      last_applied_log,
      last_membership,
    )
    .await
  }
}

impl RaftStateMachine<TypeConfig> for FjallStateMachine {
  type SnapshotBuilder = Self;
  type SnapshotData = SnapshotData;

  async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
    self.get_applied_state()
  }

  async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
  where
    Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
  {
    let mut batch = self.engine.db().batch();
    let mut last_applied_log_id = None;
    let mut responses = Vec::new();
    // 延迟执行的节点操作，确保在 batch.commit() 成功后才写入元数据
    let mut deferred_node_ops: Vec<DeferredNodeOp> = Vec::new();

    while let Some((entry, responder)) = entries.try_next().await? {
      last_applied_log_id = Some(entry.log_id);

      let response = match entry.payload {
        EntryPayload::Blank => AppliedState::None,
        EntryPayload::Normal(req) => match req.cmd {
          Cmd::UpsertKV(kv) => {
            self.apply_upsert_kv(&kv, &mut batch);
            AppliedState::None
          }
          Cmd::BatchUpsertKV { entries } => {
            for kv in &entries {
              self.apply_upsert_kv(kv, &mut batch);
            }
            AppliedState::None
          }
          Cmd::AddNode { node, .. } => {
            let node_id = node.node_id;
            info!("Applying AddNode cmd for node {node_id} in state machine");
            deferred_node_ops.push(DeferredNodeOp::Add(node));
            AppliedState::None
          }
          Cmd::RemoveNode { node_id } => {
            deferred_node_ops.push(DeferredNodeOp::Remove(node_id));
            AppliedState::None
          }
          Cmd::Txn { req, .. } => {
            let reply = self.execute_txn_to_batch(&req, &mut batch)?;
            AppliedState::Txn(reply)
          }
        },
        EntryPayload::Membership(membership) => {
          info!("Applying membership: {membership:?}");
          let nodes = membership
            .nodes()
            .map(|(node_id, node)| (*node_id, node.clone()))
            .collect();
          deferred_node_ops.push(DeferredNodeOp::SetNodes(nodes));
          AppliedState::None
        }
      };

      if let Some(responder) = responder {
        responses.push((responder, response));
      }
    }

    if let Some(last_applied_log_id) = last_applied_log_id {
      let compact = CompactLogId::from(&last_applied_log_id);
      let data = encode(&compact).map_err(read_logs_err)?;
      batch.insert(&self.cf_meta, LAST_APPLIED_LOG_KEY, data);
    }

    // 将节点操作合并到同一个 batch 中原子提交，保证崩溃一致性
    let final_nodes = if !deferred_node_ops.is_empty() {
      Some(self.apply_node_ops_to_batch(&deferred_node_ops, &mut batch)?)
    } else {
      None
    };

    batch.commit().map_err(read_logs_err)?;

    // batch 提交成功后仅更新内存缓存（不再做额外 IO）
    {
      let mut sys_data = self.lock_sys_data()?;
      if let Some(last_applied_log_id) = last_applied_log_id {
        sys_data.last_applied = Some(last_applied_log_id);
      }
      if let Some(nodes) = final_nodes {
        sys_data.membership = Self::compute_membership(&nodes)?;
        sys_data.nodes = nodes;
      }
    }

    for (responder, response) in responses {
      responder.send(response);
    }

    Ok(())
  }

  async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
    self.clone()
  }

  async fn install_snapshot(
    &mut self,
    meta: &SnapshotMeta,
    snapshot: Self::SnapshotData,
  ) -> Result<(), io::Error> {
    recover_snapshot(
      &self.engine,
      Snapshot {
        meta: meta.clone(),
        snapshot,
      },
    )
    .await?;

    if let Some(log_id) = meta.last_log_id {
      self.set_last_applied_log_id(Some(log_id))?;
    }
    let nodes = meta
      .last_membership
      .membership()
      .nodes()
      .map(|(node_id, node)| (*node_id, node.clone()))
      .collect();
    self.set_nodes(nodes)?;

    Ok(())
  }

  async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
    let data = get_current_snapshot(&self.snapshot_dir).await?;

    match data {
      Some(snapshot) => {
        let last_applied = self.get_last_applied_log_id()?;
        match (last_applied, snapshot.meta.last_log_id) {
          // 初始状态但快照存在 → 返回快照
          (None, _) => Ok(Some(snapshot)),
          // 快照 log_id >= last_applied → 快照有效
          (Some(applied), Some(snap_id)) if snap_id >= applied => Ok(Some(snapshot)),
          // 快照 log_id < last_applied → 快照过期
          _ => Ok(None),
        }
      }
      None => Ok(None),
    }
  }
}
