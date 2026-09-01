use std::process::id as process_id;
use std::sync::Arc;

use super::context::{ConnectionContext, KeyComposer};
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::protocol::RespValue;
use crate::redis::resp_util::int_to_blob;
use crate::util::now_millis;
use rapidhash::v3::rapidhash_v3;
use wedb_embed::key_composer::{NS_NAME_PREFIX, is_default_namespace, ns_name_key, ns_token_key};
use wedb_raft::types::{BatchWriteReq, GetKVReq, ScanPrefixReq, UpsertKV};

/// 收集指定命名空间下某个 key 的所有底层物理条目（按需精准探测，避免盲目全量 15 前缀扫描）
pub async fn collect_key_storage_entries(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
) -> Result<Vec<(String, Vec<u8>)>> {
  let mut list = Vec::new();

  // 1. 原生键与独立实体键
  for single_k in [
    kc.raw_key(key),
    kc.json_key(key),
    kc.tdigest_key(key),
    kc.ts_key(key),
    kc.hll_key(key),
    kc.ft_key(key),
  ] {
    if let Some(v) = node
      .read(GetKVReq {
        key: single_k.clone(),
      })
      .await?
    {
      list.push((single_k, v));
    }
  }

  // 2. 所有数据类型的元数据键
  for meta_k in kc.all_meta_keys(key) {
    if let Some(v) = node
      .read(GetKVReq {
        key: meta_k.clone(),
      })
      .await?
    {
      list.push((meta_k, v));
    }
  }

  // 3. 复合前缀子项
  for prefix in kc.all_data_prefixes(key) {
    let subkeys = node.scan_prefix(ScanPrefixReq { prefix }).await?;
    for (sub_k, sub_v) in subkeys {
      list.push((unsafe { String::from_utf8_unchecked(sub_k) }, sub_v));
    }
  }

  Ok(list)
}

pub async fn handle_conn(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let sm = node.state_machine();
  let raft = node.raft();
  let kc = ctx.key_composer();

  match cmd {
    RedisCommand::Ping(msg) => match msg {
      Some(m) => Ok(RespValue::Blob(m.into_bytes())),
      None => Ok(RespValue::pong()),
    },
    RedisCommand::Echo(msg) => Ok(RespValue::Blob(msg.into_bytes())),
    RedisCommand::Hello(proto) => {
      let proto_version = proto.unwrap_or(3);
      let pairs = vec![
        (
          RespValue::Simple("server".to_string()),
          RespValue::Simple("redis".to_string()),
        ),
        (
          RespValue::Simple("version".to_string()),
          RespValue::Simple("7.2.0".to_string()),
        ),
        (
          RespValue::Simple("proto".to_string()),
          RespValue::Int(proto_version as i64),
        ),
        (
          RespValue::Simple("id".to_string()),
          RespValue::Int(node.conf.node_id as i64),
        ),
        (
          RespValue::Simple("mode".to_string()),
          RespValue::Simple("cluster".to_string()),
        ),
        (
          RespValue::Simple("role".to_string()),
          RespValue::Simple(if raft.is_leader() {
            "master".to_string()
          } else {
            "slave".to_string()
          }),
        ),
        (
          RespValue::Simple("modules".to_string()),
          RespValue::Arr(Vec::new()),
        ),
      ];
      Ok(RespValue::Map(pairs))
    }
    RedisCommand::Quit => Ok(RespValue::ok()),
    RedisCommand::Auth {
      username: _,
      password,
    } => {
      if let Some(ns_str) = node.meta_cache().get_namespace_by_token(&password) {
        ctx.set_namespace(ns_str);
        ctx.authenticated = true;
      } else {
        let token_key = ns_token_key(&password);
        if let Ok(Some(ns_bytes)) = node.read(GetKVReq { key: token_key }).await {
          let ns_str = String::from_utf8_lossy(&ns_bytes).into_owned();
          node.meta_cache().put(&ns_str, &password);
          ctx.set_namespace(ns_str);
          ctx.authenticated = true;
        } else {
          ctx.set_namespace("default");
          ctx.authenticated = true;
        }
      }
      Ok(RespValue::ok())
    }
    RedisCommand::Select(db) => {
      ctx.set_db(db);
      Ok(RespValue::ok())
    }
    RedisCommand::NamespaceAdd(ns, token) => {
      if is_default_namespace(&ns) {
        return Err(Error::invalid_data(
          "ERR forbidden to add the default namespace",
        ));
      }
      if token.is_empty() {
        return Err(Error::invalid_data("ERR token cannot be empty"));
      }
      let name_key = ns_name_key(&ns);
      let token_key = ns_token_key(&token);

      // 检查 namespace 是否已存在
      if let Ok(Some(existing_token_bytes)) = node
        .read(GetKVReq {
          key: name_key.clone(),
        })
        .await
      {
        let existing_token = String::from_utf8_lossy(&existing_token_bytes);
        if existing_token == token {
          node.meta_cache().put(&ns, &token);
          return Ok(RespValue::ok());
        }
        return Err(Error::invalid_data("ERR the namespace already exists"));
      }

      // 检查 token 是否已被其他 namespace 占用
      if let Ok(Some(_)) = node
        .read(GetKVReq {
          key: token_key.clone(),
        })
        .await
      {
        return Err(Error::invalid_data("ERR the token already exists"));
      }

      let entries = vec![
        UpsertKV::insert(token_key, ns.clone().into_bytes()),
        UpsertKV::insert(name_key, token.clone().into_bytes()),
      ];
      node.batch_write(BatchWriteReq { entries }).await?;
      node.meta_cache().put(&ns, &token);
      Ok(RespValue::ok())
    }
    RedisCommand::NamespaceSet(ns, token) => {
      if is_default_namespace(&ns) {
        return Err(Error::invalid_data(
          "ERR forbidden to add the default namespace",
        ));
      }
      if token.is_empty() {
        return Err(Error::invalid_data("ERR token cannot be empty"));
      }
      let name_key = ns_name_key(&ns);
      let token_key = ns_token_key(&token);

      // 检查 token 是否被其他 namespace 占用
      if let Ok(Some(existing_ns_bytes)) = node
        .read(GetKVReq {
          key: token_key.clone(),
        })
        .await
      {
        let existing_ns = String::from_utf8_lossy(&existing_ns_bytes);
        if existing_ns != ns {
          return Err(Error::invalid_data("ERR the token already exists"));
        }
      }

      let mut entries = Vec::new();
      let mut old_token_opt = None;
      if let Ok(Some(old_token_bytes)) = node
        .read(GetKVReq {
          key: name_key.clone(),
        })
        .await
      {
        let old_token = String::from_utf8_lossy(&old_token_bytes).into_owned();
        if old_token != token {
          entries.push(UpsertKV::delete(ns_token_key(&old_token)));
          old_token_opt = Some(old_token);
        }
      }

      entries.push(UpsertKV::insert(token_key, ns.clone().into_bytes()));
      entries.push(UpsertKV::insert(name_key, token.clone().into_bytes()));
      node.batch_write(BatchWriteReq { entries }).await?;
      if let Some(old_t) = old_token_opt {
        node.meta_cache().invalidate(&ns, Some(&old_t));
      }
      node.meta_cache().put(&ns, &token);
      Ok(RespValue::ok())
    }
    RedisCommand::NamespaceDel(ns) => {
      if is_default_namespace(&ns) {
        return Err(Error::invalid_data(
          "ERR forbidden to delete the default namespace",
        ));
      }
      let name_key = ns_name_key(&ns);
      let token_bytes = match node
        .read(GetKVReq {
          key: name_key.clone(),
        })
        .await?
      {
        Some(t) => t,
        None => return Err(Error::invalid_data("ERR the namespace was not found")),
      };
      let token = String::from_utf8_lossy(&token_bytes).into_owned();

      let mut entries = Vec::new();
      entries.push(UpsertKV::delete(ns_token_key(&token)));
      entries.push(UpsertKV::delete(name_key));

      let ns_prefix = KeyComposer::new(&ns).namespace_prefix();
      let subkeys = node
        .scan_prefix(ScanPrefixReq { prefix: ns_prefix })
        .await?;
      for (k, _) in subkeys {
        let k_str = unsafe { String::from_utf8_unchecked(k) };
        sm.remove_ttl(&k_str).ok();
        entries.push(UpsertKV::delete(k_str));
      }
      node.batch_write(BatchWriteReq { entries }).await?;
      node.meta_cache().invalidate(&ns, Some(&token));
      Ok(RespValue::ok())
    }
    RedisCommand::NamespaceGet(ns) => {
      if ns == "*" {
        let prefix = NS_NAME_PREFIX.as_bytes().to_vec();
        let results = node
          .scan_prefix(ScanPrefixReq {
            prefix: prefix.clone(),
          })
          .await?;
        let mut list = Vec::with_capacity((results.len() + 1) * 2);
        for (k, v) in results {
          let name = k[prefix.len()..].to_vec();
          list.push(RespValue::Blob(name));
          list.push(RespValue::Blob(v));
        }
        list.push(RespValue::Blob(b"default".to_vec()));
        list.push(RespValue::Blob(b"".to_vec()));
        Ok(RespValue::Arr(list))
      } else if is_default_namespace(&ns) {
        Ok(RespValue::Blob(Vec::new()))
      } else if let Some(cached_token) = node.meta_cache().get_token_by_namespace(&ns) {
        Ok(RespValue::Blob(cached_token.as_bytes().to_vec()))
      } else {
        let name_key = ns_name_key(&ns);
        let val = node.read(GetKVReq { key: name_key }).await?;
        match val {
          Some(v) => {
            let token_str = String::from_utf8_lossy(&v);
            node.meta_cache().put(&ns, &token_str);
            Ok(RespValue::Blob(v))
          }
          None => Ok(RespValue::Null),
        }
      }
    }
    RedisCommand::NamespaceCurrent => Ok(RespValue::Blob(ctx.namespace.as_bytes().to_vec())),
    RedisCommand::NamespaceId(target_ns) => {
      let ns_name = target_ns.as_deref().unwrap_or(&ctx.namespace);
      if is_default_namespace(ns_name) {
        return Ok(RespValue::Int(0));
      }
      let name_key = ns_name_key(ns_name);
      let id = if let Ok(Some(_)) = node.read(GetKVReq { key: name_key }).await {
        (rapidhash_v3(ns_name.as_bytes()) % 1_000_000 + 1) as i64
      } else {
        return Err(Error::invalid_data("ERR the namespace was not found"));
      };
      Ok(RespValue::Int(id))
    }
    RedisCommand::NamespaceRename(old_name, new_name) => {
      if is_default_namespace(&old_name) || is_default_namespace(&new_name) {
        return Err(Error::invalid_data(
          "ERR forbidden to rename default namespace",
        ));
      }
      let old_name_key = ns_name_key(&old_name);
      let new_name_key = ns_name_key(&new_name);

      let token_bytes = match node
        .read(GetKVReq {
          key: old_name_key.clone(),
        })
        .await?
      {
        Some(t) => t,
        None => return Err(Error::invalid_data("ERR the namespace was not found")),
      };
      if let Ok(Some(_)) = node
        .read(GetKVReq {
          key: new_name_key.clone(),
        })
        .await
      {
        return Err(Error::invalid_data(
          "ERR the target namespace already exists",
        ));
      }

      let token = String::from_utf8_lossy(&token_bytes).into_owned();
      let token_key = ns_token_key(&token);

      let entries = vec![
        UpsertKV::delete(old_name_key),
        UpsertKV::insert(new_name_key, token.clone().into_bytes()),
        UpsertKV::insert(token_key, new_name.clone().into_bytes()),
      ];
      node.batch_write(BatchWriteReq { entries }).await?;

      node.meta_cache().invalidate(&old_name, Some(&token));
      node.meta_cache().put(&new_name, &token);

      if ctx.namespace == old_name {
        ctx.set_namespace(new_name);
      }

      Ok(RespValue::ok())
    }
    RedisCommand::SwapDb(db1, db2) => {
      if db1 == db2 {
        return Ok(RespValue::ok());
      }
      let ns = &ctx.namespace;
      let kc1 = KeyComposer::new_with_db(ns, db1);
      let kc2 = KeyComposer::new_with_db(ns, db2);

      let all = sm.scan_all().map_err(|e| Error::internal(e.to_string()))?;
      let mut deletes = Vec::new();
      let mut inserts = Vec::new();

      for (k, v) in all {
        if kc1.is_key_in_ns(&k)
          && let Some(target_k) = kc1.transform_key_to_target(&k, &kc2)
        {
          let k_str = unsafe { String::from_utf8_unchecked(k) };
          if let Ok(Some(exp)) = sm.get_ttl_expire_at(&k_str) {
            sm.set_ttl(&target_k, exp).ok();
            sm.remove_ttl(&k_str).ok();
          }
          deletes.push(UpsertKV::delete(k_str));
          inserts.push(UpsertKV::insert(target_k, v));
        } else if kc2.is_key_in_ns(&k)
          && let Some(target_k) = kc2.transform_key_to_target(&k, &kc1)
        {
          let k_str = unsafe { String::from_utf8_unchecked(k) };
          if let Ok(Some(exp)) = sm.get_ttl_expire_at(&k_str) {
            sm.set_ttl(&target_k, exp).ok();
            sm.remove_ttl(&k_str).ok();
          }
          deletes.push(UpsertKV::delete(k_str));
          inserts.push(UpsertKV::insert(target_k, v));
        }
      }

      if !deletes.is_empty() || !inserts.is_empty() {
        let mut entries = deletes;
        entries.extend(inserts);
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::ok())
    }
    RedisCommand::Move(key, target_db) => {
      if target_db == ctx.db {
        return Ok(RespValue::Int(0));
      }
      let target_kc = KeyComposer::new_with_db(&ctx.namespace, target_db);

      let src_entries = collect_key_storage_entries(node, &kc, &key).await?;
      if src_entries.is_empty() {
        return Ok(RespValue::Int(0));
      }

      let dst_entries = collect_key_storage_entries(node, &target_kc, &key).await?;
      if !dst_entries.is_empty() {
        return Ok(RespValue::Int(0));
      }

      let mut entries = Vec::with_capacity(src_entries.len() * 2);
      for (src_k, v) in src_entries {
        if let Some(dst_k) = kc.transform_key_to_target(src_k.as_bytes(), &target_kc) {
          eprintln!(
            "MOVE transformed: src_k={:?} -> dst_k={:?}",
            src_k.as_bytes(),
            dst_k.as_bytes()
          );
          entries.push(UpsertKV::delete(src_k));
          entries.push(UpsertKV::insert(dst_k, v));
        } else {
          eprintln!("MOVE transform failed: src_k={:?}", src_k.as_bytes());
        }
      }

      let src_raw_k = kc.raw_key(&key);
      let dst_raw_k = target_kc.raw_key(&key);
      if let Ok(Some(exp)) = sm.get_ttl_expire_at(&src_raw_k) {
        sm.set_ttl(&dst_raw_k, exp).ok();
        sm.remove_ttl(&src_raw_k).ok();
      }

      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(1))
    }
    RedisCommand::MoveX {
      key,
      target,
      replace,
    } => {
      let ns = &ctx.namespace;
      let (target_ns, target_db): (String, u64) = if let Ok(db_num) = target.parse::<u64>() {
        (ns.to_string(), db_num)
      } else {
        let token_key = ns_token_key(&target);
        match node.read(GetKVReq { key: token_key }).await? {
          Some(ns_bytes) => (String::from_utf8_lossy(&ns_bytes).into_owned(), 0),
          None => {
            return Err(Error::invalid_data("ERR target namespace token not found"));
          }
        }
      };
      if target_ns == ns.as_str() && target_db == ctx.db {
        return Ok(RespValue::Int(0));
      }
      let target_kc = KeyComposer::new_with_db(&target_ns, target_db);

      let src_entries = collect_key_storage_entries(node, &kc, &key).await?;
      if src_entries.is_empty() {
        return Ok(RespValue::Int(0));
      }

      let dst_entries = collect_key_storage_entries(node, &target_kc, &key).await?;
      if !replace && !dst_entries.is_empty() {
        return Ok(RespValue::Int(0));
      }

      let mut entries = Vec::with_capacity(src_entries.len() * 2 + dst_entries.len());
      for (dst_k, _) in dst_entries {
        entries.push(UpsertKV::delete(dst_k));
      }
      for (src_k, v) in src_entries {
        if let Some(dst_k) = kc.transform_key_to_target(src_k.as_bytes(), &target_kc) {
          entries.push(UpsertKV::delete(src_k));
          entries.push(UpsertKV::insert(dst_k, v));
        }
      }

      let src_raw_k = kc.raw_key(&key);
      let dst_raw_k = target_kc.raw_key(&key);
      if let Ok(Some(exp)) = sm.get_ttl_expire_at(&src_raw_k) {
        sm.set_ttl(&dst_raw_k, exp).ok();
        sm.remove_ttl(&src_raw_k).ok();
      }

      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(1))
    }
    RedisCommand::Command => Ok(RespValue::Arr(Vec::new())),
    RedisCommand::ConfigGet(param) => {
      let pairs = vec![RespValue::Blob(param.into_bytes()), RespValue::Blob(vec![])];
      Ok(RespValue::Arr(pairs))
    }
    RedisCommand::ConfigSet(_, _) => Ok(RespValue::ok()),
    RedisCommand::Time => {
      let now_ms = now_millis();
      let sec = (now_ms / 1000) as i64;
      let micro = ((now_ms % 1000) * 1000) as i64;
      Ok(RespValue::Arr(vec![int_to_blob(sec), int_to_blob(micro)]))
    }
    RedisCommand::ClientId => Ok(RespValue::Int(1)),
    RedisCommand::ClientGetName => Ok(RespValue::Blob(b"wedb_client".to_vec())),
    RedisCommand::ClientSetName(_) => Ok(RespValue::ok()),
    RedisCommand::ClientList => {
      let db = ctx.db;
      let info = format!(
        "id=1 addr=127.0.0.1:0 fd=1 name=wedb age=0 idle=0 flags=N db={db} sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 events=r cmd=client\n"
      );
      Ok(RespValue::Blob(info.into_bytes()))
    }
    RedisCommand::ClientInfo => {
      let db = ctx.db;
      let info = format!("id=1 addr=127.0.0.1:0 fd=1 name=wedb db={db}\n");
      Ok(RespValue::Blob(info.into_bytes()))
    }
    RedisCommand::ClientKill(_) => Ok(RespValue::Int(0)),
    RedisCommand::ClientPause(_) => Ok(RespValue::ok()),
    RedisCommand::ClientUnpause => Ok(RespValue::ok()),
    RedisCommand::ClientUnblock { .. } => Ok(RespValue::Int(0)),
    RedisCommand::ClientTracking { .. } => Ok(RespValue::ok()),
    RedisCommand::ClientTrackingInfo => Ok(RespValue::Arr(vec![
      RespValue::Blob(b"flags".to_vec()),
      RespValue::Arr(vec![RespValue::Blob(b"off".to_vec())]),
      RespValue::Blob(b"redirect".to_vec()),
      RespValue::Int(-1),
      RespValue::Blob(b"prefixes".to_vec()),
      RespValue::Arr(Vec::new()),
    ])),
    RedisCommand::ClientGetRedir => Ok(RespValue::Int(-1)),
    RedisCommand::ClientSetInfo(_, _) => Ok(RespValue::ok()),
    RedisCommand::ClientNoTouch(_) => Ok(RespValue::ok()),
    RedisCommand::ClientNoEvict(_) => Ok(RespValue::ok()),
    RedisCommand::ClientReply(_) => Ok(RespValue::ok()),
    RedisCommand::ClientHelp => Ok(RespValue::Arr(vec![
      RespValue::Blob(b"CLIENT <subcmd> [<arg> [value] ...]. Subcmds are:".to_vec()),
      RespValue::Blob(b"ID -- Return the ID of the current connection.".to_vec()),
      RespValue::Blob(b"INFO -- Return information about the current client connection.".to_vec()),
      RespValue::Blob(b"KILL <ip:port> -- Close connections matching the filter.".to_vec()),
      RespValue::Blob(b"LIST -- Return information about client connections.".to_vec()),
      RespValue::Blob(b"GETNAME -- Return the name of the current connection.".to_vec()),
      RespValue::Blob(b"SETNAME <name> -- Set the name of the current connection.".to_vec()),
      RespValue::Blob(b"PAUSE <timeout> -- Suspend all client cmds.".to_vec()),
      RespValue::Blob(b"UNPAUSE -- Resume client cmds processing.".to_vec()),
      RespValue::Blob(b"HELP -- Print this help.".to_vec()),
    ])),
    RedisCommand::Info(_) => {
      let role_str = if raft.is_leader() { "master" } else { "slave" };
      let metrics = raft.metrics().borrow_watched().clone();
      let pid = process_id();
      let voter_count = metrics
        .membership_config
        .membership()
        .voter_ids()
        .count()
        .saturating_sub(1);
      let node_id = node.conf.node_id;
      let term = metrics.current_term;
      let last_log = metrics.last_log_index.unwrap_or(0);
      let last_applied = metrics.last_applied.map(|l| l.index).unwrap_or(0);
      let info_content = format!(
        "# Server\nredis_version:7.2.0\nredis_mode:distributed-raft\nprocess_id:{pid}\n\n# Replication\nrole:{role_str}\nconnected_slaves:{voter_count}\n\n# Cluster\ncluster_enabled:1\n\n# Raft\nnode_id:{node_id}\ncurrent_term:{term}\nlast_log_index:{last_log}\nlast_applied_index:{last_applied}\n"
      );
      Ok(RespValue::Blob(info_content.into_bytes()))
    }
    RedisCommand::Role => {
      let role_name = if raft.is_leader() { "master" } else { "slave" };
      let elements = vec![
        RespValue::Simple(role_name.to_string()),
        RespValue::Int(raft.metrics().borrow_watched().last_log_index.unwrap_or(0) as i64),
        RespValue::Arr(Vec::new()),
      ];
      Ok(RespValue::Arr(elements))
    }
    RedisCommand::Slowlog
    | RedisCommand::KProfile
    | RedisCommand::PerfLog
    | RedisCommand::Stats
    | RedisCommand::Latency(_) => Ok(RespValue::Arr(Vec::new())),
    RedisCommand::MemoryUsage(key) => {
      let raw_k = kc.raw_key(&key);
      if let Some(val) = node.read(GetKVReq { key: raw_k }).await? {
        Ok(RespValue::Int((val.len() + 64) as i64))
      } else {
        Ok(RespValue::Null)
      }
    }
    RedisCommand::Monitor | RedisCommand::Shutdown | RedisCommand::Reset => Ok(RespValue::ok()),
    RedisCommand::Debug(_)
    | RedisCommand::Disk(_)
    | RedisCommand::Rdb(_)
    | RedisCommand::Sst(_) => Ok(RespValue::ok()),
    RedisCommand::Compact | RedisCommand::FlushMemTable | RedisCommand::FlushBlockCache => {
      sm.keyspace_data().major_compact().ok();
      Ok(RespValue::ok())
    }
    RedisCommand::Bgsave | RedisCommand::FlushBackup => {
      let snapshot = node.trigger_snapshot().await?;
      let snap_idx = snapshot.map(|s| s.index).unwrap_or(0);
      Ok(RespValue::Simple(format!(
        "Background saving started: index={snap_idx}"
      )))
    }
    RedisCommand::Lastsave => {
      let now = ts_::sec();
      Ok(RespValue::Int(now as i64))
    }
    RedisCommand::SlaveOf(_, _) | RedisCommand::ReplicaOf(_, _) => Ok(RespValue::ok()),
    RedisCommand::ApplyBatch(_) | RedisCommand::PollUpdates => Ok(RespValue::ok()),
    RedisCommand::Dump(key) => {
      let raw_k = kc.raw_key(&key);
      if let Some(val) = node.read(GetKVReq { key: raw_k }).await? {
        Ok(RespValue::Blob(val))
      } else {
        Ok(RespValue::Null)
      }
    }
    RedisCommand::Restore {
      key,
      ttl,
      serialized,
      replace,
    } => {
      let raw_k = kc.raw_key(&key);
      if !replace && node.read(GetKVReq { key: raw_k.clone() }).await?.is_some() {
        return Err(Error::invalid_data(
          "BUSYKEY Target key name already exists.",
        ));
      }
      let entries = vec![UpsertKV::insert(raw_k.clone(), serialized)];
      if ttl > 0 {
        sm.set_ttl(&raw_k, now_millis() + ttl).ok();
      }
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    _ => Err(Error::redis("Command not matched in handle_conn")),
  }
}
