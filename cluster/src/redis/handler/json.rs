use sonic_rs::JsonContainerTrait;
use std::sync::Arc;

use super::context::{ConnectionContext, JsonMeta, KeyComposer};
use super::search::sync_search_indices_on_doc_update;
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::json_util::{
  format_json_value, json_arr_append, json_arr_index, json_arr_insert, json_arr_pop, json_arr_trim,
  json_clear, json_merge_patch, json_num_op, json_obj_keys, json_obj_len, json_path_del,
  json_path_query, json_path_replace, json_path_set, json_str_append, json_str_len, json_to_resp,
  json_toggle, json_type_str,
};
use crate::redis::protocol::RespValue;
use wedb_raft::types::{BatchWriteReq, GetKVReq, UpsertKV};

#[inline]
pub async fn read_json_doc(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
) -> Result<Option<(JsonMeta, sonic_rs::Value)>> {
  let j_k = kc.json_meta(key);
  let bytes = match node.read(GetKVReq { key: j_k }).await? {
    Some(b) => b,
    None => return Ok(None),
  };
  let (meta, payload) = match JsonMeta::decode(&bytes) {
    Some(res) => res,
    None => return Ok(None),
  };
  let root: sonic_rs::Value = match sonic_rs::from_slice(payload) {
    Ok(v) => v,
    Err(_) => sonic_rs::Value::from(()),
  };
  Ok(Some((meta, root)))
}

#[inline]
pub async fn write_json_doc(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  meta: &mut JsonMeta,
  root: &sonic_rs::Value,
) -> Result<()> {
  let json_bytes = sonic_rs::to_vec(root).map_err(|e| Error::internal(e.to_string()))?;
  meta.base.size = json_bytes.len() as u64;
  meta.base.version += 1;
  let mut val = meta.encode().to_vec();
  val.extend_from_slice(&json_bytes);
  let entries = vec![
    UpsertKV::insert(kc.json_meta(key), val),
    UpsertKV::insert(kc.raw_key(key), meta.base.encode().to_vec()),
  ];
  node.batch_write(BatchWriteReq { entries }).await?;

  sync_search_indices_on_doc_update(node, kc, key, Some(&json_bytes)).await?;
  Ok(())
}

/// RedisJSON 命令主调度处理器
pub async fn handle_json(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();

  match cmd {
    RedisCommand::JsonSet {
      key,
      path,
      value,
      nx,
      xx,
    } => {
      let parsed_v: sonic_rs::Value =
        sonic_rs::from_str(&value).map_err(|e| Error::invalid_data(e.to_string()))?;
      let existing = read_json_doc(node, &kc, &key).await?;

      let (mut meta, mut root) = match existing {
        Some((m, r)) => {
          if nx && (path == "$" || path == "." || path.is_empty()) {
            return Ok(RespValue::Null);
          }
          (m, r)
        }
        None => {
          if xx {
            return Ok(RespValue::Null);
          }
          (
            JsonMeta::new(0, 0, 0),
            if path == "$" || path == "." || path.is_empty() {
              parsed_v.clone()
            } else {
              sonic_rs::json!({})
            },
          )
        }
      };

      let ok = json_path_set(&mut root, &path, parsed_v, nx, xx);
      if !ok {
        return Ok(RespValue::Null);
      }

      write_json_doc(node, &kc, &key, &mut meta, &root).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::JsonGet {
      key,
      indent,
      newline,
      space,
      paths,
    } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (_, root) = match existing {
        Some(pair) => pair,
        None => return Ok(RespValue::Null),
      };

      if paths.is_empty() || (paths.len() == 1 && (paths[0] == "$" || paths[0] == ".")) {
        let formatted = format_json_value(
          &root,
          indent.as_deref(),
          newline.as_deref(),
          space.as_deref(),
        );
        return Ok(RespValue::Blob(formatted.into_bytes()));
      }

      if paths.len() == 1 {
        let nodes = json_path_query(&root, &paths[0]);
        if nodes.is_empty() {
          return Ok(RespValue::Null);
        }
        if nodes.len() == 1 {
          let formatted = format_json_value(
            nodes[0],
            indent.as_deref(),
            newline.as_deref(),
            space.as_deref(),
          );
          return Ok(RespValue::Blob(formatted.into_bytes()));
        } else {
          let mut arr = Vec::new();
          for n in nodes {
            arr.push(n.clone());
          }
          let arr_v = sonic_rs::Value::from(arr);
          let formatted = format_json_value(
            &arr_v,
            indent.as_deref(),
            newline.as_deref(),
            space.as_deref(),
          );
          return Ok(RespValue::Blob(formatted.into_bytes()));
        }
      }

      let mut map = sonic_rs::Object::new();
      for p in &paths {
        let nodes = json_path_query(&root, p);
        if nodes.is_empty() {
          map.insert(p.as_str(), sonic_rs::json!(null));
        } else if nodes.len() == 1 {
          map.insert(p.as_str(), nodes[0].clone());
        } else {
          let mut arr = Vec::new();
          for n in nodes {
            arr.push(n.clone());
          }
          map.insert(p.as_str(), sonic_rs::Value::from(arr));
        }
      }
      let map_v = sonic_rs::Value::from(map);
      let formatted = format_json_value(
        &map_v,
        indent.as_deref(),
        newline.as_deref(),
        space.as_deref(),
      );
      Ok(RespValue::Blob(formatted.into_bytes()))
    }
    RedisCommand::JsonDel { key, path } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => return Ok(RespValue::Int(0)),
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      if p == "$" || p == "." || p.is_empty() {
        let j_k = kc.json_meta(&key);
        let raw_k = kc.raw_key(&key);
        let entries = vec![UpsertKV::delete(j_k), UpsertKV::delete(raw_k)];
        node.batch_write(BatchWriteReq { entries }).await?;
        sync_search_indices_on_doc_update(node, &kc, &key, None).await?;
        return Ok(RespValue::Int(1));
      }

      let deleted = json_path_del(&mut root, &p);
      if deleted > 0 {
        write_json_doc(node, &kc, &key, &mut meta, &root).await?;
      }
      Ok(RespValue::Int(deleted as i64))
    }
    RedisCommand::JsonType { key, path } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (_, root) = match existing {
        Some(pair) => pair,
        None => return Ok(RespValue::Null),
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let is_v2 = p.starts_with('$');
      let nodes = json_path_query(&root, &p);
      if nodes.is_empty() {
        return Ok(RespValue::Null);
      }
      if !is_v2 {
        return Ok(RespValue::Simple(json_type_str(nodes[0]).to_string()));
      }
      let type_arr: Vec<RespValue> = nodes
        .into_iter()
        .map(|n| RespValue::Simple(json_type_str(n).to_string()))
        .collect();
      Ok(RespValue::Arr(type_arr))
    }
    RedisCommand::JsonNumIncrBy { key, path, number } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => {
          return Err(Error::invalid_data(
            "ERR could not perform this operation on a key that doesn't exist",
          ));
        }
      };

      let results = json_num_op(&mut root, &path, number, true)?;
      if results.is_empty() {
        return Err(Error::invalid_data("ERR path does not exist"));
      }
      write_json_doc(node, &kc, &key, &mut meta, &root).await?;

      let is_v2 = path.starts_with('$');
      if !is_v2 && results.len() == 1 {
        if let Some(ref v) = results[0] {
          let out = sonic_rs::to_vec(v).unwrap_or_default();
          return Ok(RespValue::Blob(out));
        }
        return Ok(RespValue::Null);
      }
      let out = sonic_rs::to_vec(&results).unwrap_or_default();
      Ok(RespValue::Blob(out))
    }
    RedisCommand::JsonNumMultBy { key, path, number } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => {
          return Err(Error::invalid_data(
            "ERR could not perform this operation on a key that doesn't exist",
          ));
        }
      };

      let results = json_num_op(&mut root, &path, number, false)?;
      if results.is_empty() {
        return Err(Error::invalid_data("ERR path does not exist"));
      }
      write_json_doc(node, &kc, &key, &mut meta, &root).await?;

      let is_v2 = path.starts_with('$');
      if !is_v2 && results.len() == 1 {
        if let Some(ref v) = results[0] {
          let out = sonic_rs::to_vec(v).unwrap_or_default();
          return Ok(RespValue::Blob(out));
        }
        return Ok(RespValue::Null);
      }
      let out = sonic_rs::to_vec(&results).unwrap_or_default();
      Ok(RespValue::Blob(out))
    }
    RedisCommand::JsonToggle { key, path } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => {
          return Err(Error::invalid_data(
            "ERR could not perform this operation on a key that doesn't exist",
          ));
        }
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let is_v2 = p.starts_with('$');
      let results = json_toggle(&mut root, &p);
      if results.is_empty() {
        return Err(Error::invalid_data("ERR path does not exist"));
      }
      write_json_doc(node, &kc, &key, &mut meta, &root).await?;

      if !is_v2 && results.len() == 1 {
        if let Some(b) = results[0] {
          return Ok(RespValue::Int(if b { 1 } else { 0 }));
        }
        return Ok(RespValue::Null);
      }
      let arr = results
        .into_iter()
        .map(|b_opt| {
          b_opt
            .map(|b| RespValue::Int(if b { 1 } else { 0 }))
            .unwrap_or(RespValue::Null)
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonClear { key, path } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => return Ok(RespValue::Int(0)),
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let count = json_clear(&mut root, &p);
      if count > 0 {
        write_json_doc(node, &kc, &key, &mut meta, &root).await?;
      }
      Ok(RespValue::Int(count as i64))
    }
    RedisCommand::JsonStrAppend { key, path, value } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => {
          return Err(Error::invalid_data(
            "ERR could not perform this operation on a key that doesn't exist",
          ));
        }
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let is_v2 = p.starts_with('$');
      let raw_val = if (value.starts_with('"') && value.ends_with('"')) && value.len() >= 2 {
        &value[1..value.len() - 1]
      } else {
        &value
      };
      let results = json_str_append(&mut root, &p, raw_val);
      if results.is_empty() {
        return Err(Error::invalid_data("ERR path does not exist"));
      }
      write_json_doc(node, &kc, &key, &mut meta, &root).await?;

      if !is_v2 && results.len() == 1 {
        if let Some(len) = results[0] {
          return Ok(RespValue::Int(len as i64));
        }
        return Ok(RespValue::Null);
      }
      let arr = results
        .into_iter()
        .map(|l_opt| {
          l_opt
            .map(|l| RespValue::Int(l as i64))
            .unwrap_or(RespValue::Null)
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonStrLen { key, path } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (_, root) = match existing {
        Some(pair) => pair,
        None => return Ok(RespValue::Null),
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let is_v2 = p.starts_with('$');
      let results = json_str_len(&root, &p);
      if results.is_empty() {
        return Ok(RespValue::Null);
      }
      if !is_v2 && results.len() == 1 {
        return Ok(
          results[0]
            .map(|l| RespValue::Int(l as i64))
            .unwrap_or(RespValue::Null),
        );
      }
      let arr = results
        .into_iter()
        .map(|l_opt| {
          l_opt
            .map(|l| RespValue::Int(l as i64))
            .unwrap_or(RespValue::Null)
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonArrAppend { key, path, values } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => {
          return Err(Error::invalid_data(
            "ERR could not perform this operation on a key that doesn't exist",
          ));
        }
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let is_v2 = p.starts_with('$');
      let parsed_vals: Vec<sonic_rs::Value> = values
        .iter()
        .map(|v| sonic_rs::from_str(v).unwrap_or_else(|_| sonic_rs::json!(v.as_str())))
        .collect();
      let results = json_arr_append(&mut root, &p, parsed_vals);
      if results.is_empty() {
        return Err(Error::invalid_data("ERR path does not exist"));
      }
      write_json_doc(node, &kc, &key, &mut meta, &root).await?;

      if !is_v2 && results.len() == 1 {
        if let Some(len) = results[0] {
          return Ok(RespValue::Int(len as i64));
        }
        return Ok(RespValue::Null);
      }
      let arr = results
        .into_iter()
        .map(|l_opt| {
          l_opt
            .map(|l| RespValue::Int(l as i64))
            .unwrap_or(RespValue::Null)
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonArrInsert {
      key,
      path,
      index,
      values,
    } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => {
          return Err(Error::invalid_data(
            "ERR could not perform this operation on a key that doesn't exist",
          ));
        }
      };

      let is_v2 = path.starts_with('$');
      let parsed_vals: Vec<sonic_rs::Value> = values
        .iter()
        .map(|v| sonic_rs::from_str(v).unwrap_or_else(|_| sonic_rs::json!(v.as_str())))
        .collect();
      let results = json_arr_insert(&mut root, &path, index, parsed_vals);
      if results.is_empty() {
        return Err(Error::invalid_data("ERR path does not exist"));
      }
      write_json_doc(node, &kc, &key, &mut meta, &root).await?;

      if !is_v2 && results.len() == 1 {
        if let Some(len) = results[0] {
          return Ok(RespValue::Int(len as i64));
        }
        return Ok(RespValue::Null);
      }
      let arr = results
        .into_iter()
        .map(|l_opt| {
          l_opt
            .map(|l| RespValue::Int(l as i64))
            .unwrap_or(RespValue::Null)
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonArrPop { key, path, index } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => return Ok(RespValue::Null),
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let is_v2 = p.starts_with('$');
      let idx = index.unwrap_or(-1);
      let results = json_arr_pop(&mut root, &p, idx);
      if results.is_empty() {
        return Ok(RespValue::Null);
      }
      write_json_doc(node, &kc, &key, &mut meta, &root).await?;

      if !is_v2 && results.len() == 1 {
        if let Some(ref v) = results[0] {
          let out = sonic_rs::to_vec(v).unwrap_or_default();
          return Ok(RespValue::Blob(out));
        }
        return Ok(RespValue::Null);
      }
      let arr = results
        .into_iter()
        .map(|v_opt| {
          v_opt
            .map(|v| RespValue::Blob(sonic_rs::to_vec(&v).unwrap_or_default()))
            .unwrap_or(RespValue::Null)
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonArrTrim {
      key,
      path,
      start,
      stop,
    } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => {
          return Err(Error::invalid_data(
            "ERR could not perform this operation on a key that doesn't exist",
          ));
        }
      };

      let is_v2 = path.starts_with('$');
      let results = json_arr_trim(&mut root, &path, start, stop);
      if results.is_empty() {
        return Err(Error::invalid_data("ERR path does not exist"));
      }
      write_json_doc(node, &kc, &key, &mut meta, &root).await?;

      if !is_v2 && results.len() == 1 {
        if let Some(len) = results[0] {
          return Ok(RespValue::Int(len as i64));
        }
        return Ok(RespValue::Null);
      }
      let arr = results
        .into_iter()
        .map(|l_opt| {
          l_opt
            .map(|l| RespValue::Int(l as i64))
            .unwrap_or(RespValue::Null)
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonArrLen { key, path } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (_, root) = match existing {
        Some(pair) => pair,
        None => return Ok(RespValue::Null),
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let is_v2 = p.starts_with('$');
      let nodes = json_path_query(&root, &p);
      if nodes.is_empty() {
        return Ok(RespValue::Null);
      }
      if !is_v2 && nodes.len() == 1 {
        return Ok(
          nodes[0]
            .as_array()
            .map(|a| RespValue::Int(a.len() as i64))
            .unwrap_or(RespValue::Null),
        );
      }
      let arr = nodes
        .into_iter()
        .map(|n| {
          n.as_array()
            .map(|a| RespValue::Int(a.len() as i64))
            .unwrap_or(RespValue::Null)
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonArrIndex { key, path, value } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (_, root) = match existing {
        Some(pair) => pair,
        None => {
          return Err(Error::invalid_data(
            "ERR could not perform this operation on a key that doesn't exist",
          ));
        }
      };

      let is_v2 = path.starts_with('$');
      let needle: sonic_rs::Value =
        sonic_rs::from_str(&value).unwrap_or_else(|_| sonic_rs::json!(value.as_str()));
      let results = json_arr_index(&root, &path, &needle, 0, 0);
      if results.is_empty() {
        return Err(Error::invalid_data("ERR path does not exist"));
      }
      if !is_v2 && results.len() == 1 {
        return Ok(results[0].map(RespValue::Int).unwrap_or(RespValue::Null));
      }
      let arr = results
        .into_iter()
        .map(|i_opt| i_opt.map(RespValue::Int).unwrap_or(RespValue::Null))
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonObjKeys { key, path } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (_, root) = match existing {
        Some(pair) => pair,
        None => return Ok(RespValue::Null),
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let is_v2 = p.starts_with('$');
      let results = json_obj_keys(&root, &p);
      if results.is_empty() {
        return Ok(RespValue::Null);
      }
      if !is_v2 && results.len() == 1 {
        if let Some(keys) = &results[0] {
          return Ok(RespValue::Arr(
            keys
              .iter()
              .map(|k| RespValue::Blob(k.as_bytes().to_vec()))
              .collect(),
          ));
        }
        return Ok(RespValue::Null);
      }
      let arr = results
        .into_iter()
        .map(|keys_opt| {
          keys_opt
            .map(|keys| {
              RespValue::Arr(
                keys
                  .into_iter()
                  .map(|k| RespValue::Blob(k.into_bytes()))
                  .collect(),
              )
            })
            .unwrap_or(RespValue::Null)
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonObjLen { key, path } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (_, root) = match existing {
        Some(pair) => pair,
        None => return Ok(RespValue::Null),
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let is_v2 = p.starts_with('$');
      let results = json_obj_len(&root, &p);
      if results.is_empty() {
        return Ok(RespValue::Null);
      }
      if !is_v2 && results.len() == 1 {
        return Ok(
          results[0]
            .map(|l| RespValue::Int(l as i64))
            .unwrap_or(RespValue::Null),
        );
      }
      let arr = results
        .into_iter()
        .map(|l_opt| {
          l_opt
            .map(|l| RespValue::Int(l as i64))
            .unwrap_or(RespValue::Null)
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonMerge { key, path, value } => {
      let parsed_patch: sonic_rs::Value =
        sonic_rs::from_str(&value).map_err(|e| Error::invalid_data(e.to_string()))?;
      let existing = read_json_doc(node, &kc, &key).await?;
      let (mut meta, mut root) = match existing {
        Some(pair) => pair,
        None => (JsonMeta::new(0, 0, 0), sonic_rs::json!({})),
      };

      if path == "$" || path == "." || path.is_empty() {
        json_merge_patch(&mut root, &parsed_patch);
      } else {
        json_path_replace(&mut root, &path, |target| {
          json_merge_patch(target, &parsed_patch);
        });
      }
      write_json_doc(node, &kc, &key, &mut meta, &root).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::JsonMGet { keys, path } => {
      let is_v2 = path.starts_with('$');
      let mut results = Vec::with_capacity(keys.len());
      for k in keys {
        if let Ok(Some((_, root))) = read_json_doc(node, &kc, &k).await {
          let nodes = json_path_query(&root, &path);
          if nodes.is_empty() {
            results.push(RespValue::Null);
          } else if !is_v2 && nodes.len() == 1 {
            let out = sonic_rs::to_vec(nodes[0]).unwrap_or_default();
            results.push(RespValue::Blob(out));
          } else {
            let out = sonic_rs::to_vec(&nodes).unwrap_or_default();
            results.push(RespValue::Blob(out));
          }
        } else {
          results.push(RespValue::Null);
        }
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::JsonMSet(entries) => {
      for (k, p, v) in entries {
        let parsed_v: sonic_rs::Value =
          sonic_rs::from_str(&v).map_err(|e| Error::invalid_data(e.to_string()))?;
        let existing = read_json_doc(node, &kc, &k).await?;
        let (mut meta, mut root) = match existing {
          Some((m, r)) => (m, r),
          None => (JsonMeta::new(0, 0, 0), sonic_rs::json!({})),
        };
        json_path_set(&mut root, &p, parsed_v, false, false);
        write_json_doc(node, &kc, &k, &mut meta, &root).await?;
      }
      Ok(RespValue::ok())
    }
    RedisCommand::JsonResp { key, path } => {
      let existing = read_json_doc(node, &kc, &key).await?;
      let (_, root) = match existing {
        Some(pair) => pair,
        None => return Ok(RespValue::Null),
      };

      let p = path.unwrap_or_else(|| "$".to_string());
      let nodes = json_path_query(&root, &p);
      if nodes.is_empty() {
        return Ok(RespValue::Null);
      }
      if nodes.len() == 1 {
        return Ok(json_to_resp(nodes[0]));
      }
      let arr = nodes.into_iter().map(json_to_resp).collect();
      Ok(RespValue::Arr(arr))
    }
    RedisCommand::JsonInfo(key) => {
      let j_k = kc.json_meta(&key);
      let res = node.read(GetKVReq { key: j_k }).await?;
      match res {
        Some(b) => {
          let size = if b.len() >= JsonMeta::ENCODED_SIZE {
            b.len() - JsonMeta::ENCODED_SIZE
          } else {
            b.len()
          };
          let info = vec![
            RespValue::Simple("bytes".to_string()),
            RespValue::Int(size as i64),
          ];
          Ok(RespValue::Arr(info))
        }
        None => Ok(RespValue::Null),
      }
    }
    RedisCommand::JsonDebug(_, _) => Ok(RespValue::ok()),
    _ => Err(Error::internal("unsupported json command")),
  }
}
