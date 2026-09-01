use rapidhash::RapidHashSet;
use sonic_rs::JsonValueTrait;
use std::cmp::min;
use std::str::from_utf8;
use std::sync::Arc;

use super::conn::collect_key_storage_entries;
use super::context::{ConnectionContext, KeyComposer};
use super::json::read_json_doc;
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::json_util::json_path_query;
use crate::redis::protocol::RespValue;
use crate::redis::resp_util::float_to_blob;
use crate::redis::search::{
  IndexField, IndexFieldType, IndexOnDataType, SearchIndexSchema, SearchQueryNode,
  decode_sortable_f64, encode_sortable_f64, explain_search_query, extract_doc_terms,
  parse_search_query,
};
use wedb_raft::types::{BatchWriteReq, GetKVReq, ScanPrefixReq, UpsertKV};

pub fn parse_schema_fields(fields_args: &[String]) -> Vec<IndexField> {
  let mut fields = Vec::new();
  let mut i = 0;
  while i < fields_args.len() {
    let name = &fields_args[i];
    i += 1;
    let mut alias = None;
    if i < fields_args.len() && fields_args[i].eq_ignore_ascii_case("as") {
      i += 1;
      if i < fields_args.len() {
        alias = Some(fields_args[i].clone());
        i += 1;
      }
    }
    let type_str = if i < fields_args.len() {
      let s = fields_args[i].to_ascii_uppercase();
      i += 1;
      s
    } else {
      "TEXT".to_string()
    };

    let mut field = match type_str.as_str() {
      "TAG" => IndexField::with_tag(name.clone(), None, false),
      "NUMERIC" => IndexField::with_numeric(name.clone(), false),
      _ => IndexField::new(name.clone(), IndexFieldType::Text),
    };
    field.alias = alias;

    while i < fields_args.len() {
      let opt = fields_args[i].to_ascii_uppercase();
      if opt == "SEPARATOR" && i + 1 < fields_args.len() {
        field.separator = fields_args[i + 1].chars().next();
        i += 2;
      } else if opt == "CASESENSITIVE" {
        field.case_sensitive = true;
        i += 1;
      } else if opt == "WEIGHT" && i + 1 < fields_args.len() {
        field.weight = fields_args[i + 1].parse().unwrap_or(1.0);
        i += 2;
      } else if opt == "SORTABLE" {
        field.sortable = true;
        i += 1;
      } else if opt == "NOINDEX" {
        field.noindex = true;
        i += 1;
      } else if matches!(opt.as_str(), "TEXT" | "TAG" | "NUMERIC" | "VECTOR" | "GEO") {
        i -= 1;
        break;
      } else {
        if i + 1 < fields_args.len()
          && matches!(
            fields_args[i + 1].to_ascii_uppercase().as_str(),
            "TEXT" | "TAG" | "NUMERIC" | "VECTOR" | "GEO" | "AS"
          )
        {
          break;
        }
        i += 1;
      }
    }
    fields.push(field);
  }
  fields
}

pub async fn sync_search_indices_on_doc_update(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  doc_key: &str,
  raw_doc: Option<&[u8]>,
) -> Result<()> {
  let prefix = kc.ft_schema_prefix();
  let subkeys = node.scan_prefix(ScanPrefixReq { prefix }).await?;
  if subkeys.is_empty() {
    return Ok(());
  }

  let mut entries = Vec::new();
  for (_, v) in subkeys {
    if let Ok(schema) = bitcode::decode::<SearchIndexSchema>(&v)
      && schema.matches_key(doc_key)
    {
      let idx_prefix = kc.ft_index_prefix(&schema.name);
      let postings = node
        .scan_prefix(ScanPrefixReq {
          prefix: idx_prefix.clone(),
        })
        .await?;
      for (pk, _) in postings {
        let remain = &pk[idx_prefix.len()..];
        let colon1 = memchr::memchr(b':', remain);
        let colon2 =
          colon1.and_then(|c1| memchr::memchr(b':', &remain[c1 + 1..]).map(|c2| c1 + 1 + c2));
        if let Some(c2) = colon2
          && &remain[c2 + 1..] == doc_key.as_bytes()
          && let Ok(pk_str) = String::from_utf8(pk)
        {
          entries.push(UpsertKV::delete(pk_str));
        }
      }

      if let Some(doc_bytes) = raw_doc {
        let terms = extract_doc_terms(&schema, doc_key, doc_bytes);
        for (field_name, term) in terms {
          let posting_k = kc.ft_index_key(&schema.name, &field_name, &term, doc_key);
          entries.push(UpsertKV::insert(posting_k, Vec::new()));
        }
      }
    }
  }

  if !entries.is_empty() {
    node.batch_write(BatchWriteReq { entries }).await?;
  }
  Ok(())
}

/// 全文检索查询参数配置
#[derive(Debug, Clone, Default)]
pub struct SearchQueryOptions {
  pub nocontent: bool,
  pub return_fields: Option<Vec<String>>,
  pub offset: usize,
  pub limit: usize,
  pub sortby: Option<(String, bool)>,
}

pub async fn exec_search_query(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  index: &str,
  schema: &SearchIndexSchema,
  query_str: &str,
  opts: SearchQueryOptions,
) -> Result<RespValue> {
  let ast = parse_search_query(query_str);
  let matching_doc_set = exec_search_ast(node, kc, index, schema, &ast).await?;

  let mut matching_docs: Vec<String> = matching_doc_set.into_iter().collect();

  if let Some((sort_field, is_asc)) = &opts.sortby {
    let mut doc_sort_keys: Vec<(String, String)> = Vec::with_capacity(matching_docs.len());
    for doc_id in &matching_docs {
      let sort_key = match schema.on_data_type {
        IndexOnDataType::Json => {
          if let Ok(Some((_, root))) = read_json_doc(node, kc, doc_id).await {
            let path = format!("$.{sort_field}");
            let q = json_path_query(&root, &path);
            if let Some(first) = q.first() {
              if let Some(num) = first.as_f64() {
                encode_sortable_f64(num)
              } else if let Some(s) = first.as_str() {
                s.to_string()
              } else {
                sonic_rs::to_string(first).unwrap_or_default()
              }
            } else {
              String::new()
            }
          } else {
            String::new()
          }
        }
        IndexOnDataType::Hash => {
          let h_k = kc.hash_key(doc_id, sort_field);
          if let Ok(Some(v)) = node.read(GetKVReq { key: h_k }).await {
            if let Ok(s) = String::from_utf8(v) {
              if let Ok(num) = s.parse::<f64>() {
                encode_sortable_f64(num)
              } else {
                s
              }
            } else {
              String::new()
            }
          } else {
            String::new()
          }
        }
      };
      doc_sort_keys.push((doc_id.clone(), sort_key));
    }

    doc_sort_keys.sort_unstable_by(|a, b| {
      if *is_asc {
        a.1.cmp(&b.1)
      } else {
        b.1.cmp(&a.1)
      }
    });
    matching_docs = doc_sort_keys.into_iter().map(|(id, _)| id).collect();
  } else {
    matching_docs.sort_unstable();
  }

  let total_count = matching_docs.len();
  let page_docs = if opts.offset < matching_docs.len() {
    matching_docs[opts.offset..min(opts.offset + opts.limit, matching_docs.len())].to_vec()
  } else {
    Vec::new()
  };

  let mut elements = Vec::with_capacity(page_docs.len() * 2 + 1);
  elements.push(RespValue::Int(total_count as i64));

  for doc_id in page_docs {
    elements.push(RespValue::Blob(doc_id.as_bytes().to_vec()));
    if !opts.nocontent {
      match schema.on_data_type {
        IndexOnDataType::Json => {
          if let Ok(Some((_, root))) = read_json_doc(node, kc, &doc_id).await {
            if let Some(ref req_fields) = opts.return_fields {
              let mut field_pairs = Vec::with_capacity(req_fields.len() * 2);
              for f in req_fields {
                let path = format!("$.{f}");
                let q = json_path_query(&root, &path);
                field_pairs.push(RespValue::Blob(f.as_bytes().to_vec()));
                if let Some(first) = q.first() {
                  let serialized = sonic_rs::to_vec(first).unwrap_or_default();
                  field_pairs.push(RespValue::Blob(serialized));
                } else {
                  field_pairs.push(RespValue::Null);
                }
              }
              elements.push(RespValue::Arr(field_pairs));
            } else {
              let out = sonic_rs::to_vec(&root).unwrap_or_default();
              elements.push(RespValue::Arr(vec![
                RespValue::Blob(b"$".to_vec()),
                RespValue::Blob(out),
              ]));
            }
          } else {
            elements.push(RespValue::Null);
          }
        }
        IndexOnDataType::Hash => {
          let hash_prefix = kc.hash_prefix(&doc_id);
          let subkeys = node
            .scan_prefix(ScanPrefixReq {
              prefix: hash_prefix.clone(),
            })
            .await?;
          let mut pairs = Vec::with_capacity(subkeys.len() * 2);
          for (k, v) in subkeys {
            let field_bytes = &k[hash_prefix.len()..];
            let field_str = String::from_utf8_lossy(field_bytes).into_owned();
            if let Some(ref req_fields) = opts.return_fields {
              if req_fields.iter().any(|rf| rf == &field_str) {
                pairs.push(RespValue::Blob(field_bytes.to_vec()));
                pairs.push(RespValue::Blob(v));
              }
            } else {
              pairs.push(RespValue::Blob(field_bytes.to_vec()));
              pairs.push(RespValue::Blob(v));
            }
          }
          elements.push(RespValue::Arr(pairs));
        }
      }
    }
  }

  Ok(RespValue::Arr(elements))
}

async fn exec_search_ast(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  index: &str,
  schema: &SearchIndexSchema,
  ast: &SearchQueryNode,
) -> Result<RapidHashSet<String>> {
  match ast {
    SearchQueryNode::Wildcard => {
      let prefix = kc.ft_index_prefix(index);
      let subkeys = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;
      let mut docs = RapidHashSet::default();
      for (k, _) in subkeys {
        let remain = &k[prefix.len()..];
        let colon1 = memchr::memchr(b':', remain);
        let colon2 =
          colon1.and_then(|c1| memchr::memchr(b':', &remain[c1 + 1..]).map(|c2| c1 + 1 + c2));
        if let Some(c2) = colon2
          && let Ok(doc_id) = from_utf8(&remain[c2 + 1..])
        {
          docs.insert(doc_id.to_string());
        }
      }
      Ok(docs)
    }
    SearchQueryNode::Term {
      field,
      term,
      is_prefix,
      ..
    } => {
      let mut matching_docs = RapidHashSet::default();
      let fields_to_search: Vec<&IndexField> = if let Some(f_name) = field {
        if let Some(f) = schema.get_field(f_name) {
          vec![f]
        } else {
          Vec::new()
        }
      } else {
        schema
          .fields
          .iter()
          .filter(|f| f.field_type == IndexFieldType::Text)
          .collect()
      };

      for f in fields_to_search {
        if *is_prefix {
          let prefix = kc.ft_index_term_scan_prefix(index, &f.name, term);
          let subkeys = node
            .scan_prefix(ScanPrefixReq {
              prefix: prefix.clone(),
            })
            .await?;
          for (k, _) in subkeys {
            let remain = &k[prefix.len()..];
            if let Some(colon_pos) = memchr::memchr(b':', remain)
              && let Ok(doc_id) = from_utf8(&remain[colon_pos + 1..])
            {
              matching_docs.insert(doc_id.to_string());
            }
          }
        } else {
          let prefix = kc.ft_index_term_prefix(index, &f.name, term);
          let subkeys = node
            .scan_prefix(ScanPrefixReq {
              prefix: prefix.clone(),
            })
            .await?;
          for (k, _) in subkeys {
            if let Ok(doc_id) = from_utf8(&k[prefix.len()..]) {
              matching_docs.insert(doc_id.to_string());
            }
          }
        }
      }
      Ok(matching_docs)
    }
    SearchQueryNode::Tag { field, tags } => {
      let mut matching_docs = RapidHashSet::default();
      for tag in tags {
        let prefix = kc.ft_index_term_prefix(index, field, tag);
        let subkeys = node
          .scan_prefix(ScanPrefixReq {
            prefix: prefix.clone(),
          })
          .await?;
        for (k, _) in subkeys {
          if let Ok(doc_id) = from_utf8(&k[prefix.len()..]) {
            matching_docs.insert(doc_id.to_string());
          }
        }
      }
      Ok(matching_docs)
    }
    SearchQueryNode::NumericRange {
      field,
      min,
      min_inclusive,
      max,
      max_inclusive,
    } => {
      let mut matching_docs = RapidHashSet::default();
      let prefix = kc.ft_index_field_prefix(index, field);
      let subkeys = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;
      for (k, _) in subkeys {
        let remain = &k[prefix.len()..];
        if let Some(colon_pos) = memchr::memchr(b':', remain)
          && let Ok(hex_str) = from_utf8(&remain[..colon_pos])
          && let Some(num) = decode_sortable_f64(hex_str)
        {
          let min_ok = if *min_inclusive {
            num >= *min
          } else {
            num > *min
          };
          let max_ok = if *max_inclusive {
            num <= *max
          } else {
            num < *max
          };
          if min_ok
            && max_ok
            && let Ok(doc_id) = from_utf8(&remain[colon_pos + 1..])
          {
            matching_docs.insert(doc_id.to_string());
          }
        }
      }
      Ok(matching_docs)
    }
    SearchQueryNode::And(nodes) => {
      let mut iter = nodes.iter();
      if let Some(first) = iter.next() {
        let mut current = Box::pin(exec_search_ast(node, kc, index, schema, first)).await?;
        for next_node in iter {
          let next_set = Box::pin(exec_search_ast(node, kc, index, schema, next_node)).await?;
          current.retain(|doc| next_set.contains(doc));
          if current.is_empty() {
            break;
          }
        }
        Ok(current)
      } else {
        Ok(RapidHashSet::default())
      }
    }
    SearchQueryNode::Or(nodes) => {
      let mut res = RapidHashSet::default();
      for n in nodes {
        let s = Box::pin(exec_search_ast(node, kc, index, schema, n)).await?;
        res.extend(s);
      }
      Ok(res)
    }
    SearchQueryNode::Not(inner) => {
      let excluded = Box::pin(exec_search_ast(node, kc, index, schema, inner)).await?;
      let all_docs = Box::pin(exec_search_ast(
        node,
        kc,
        index,
        schema,
        &SearchQueryNode::Wildcard,
      ))
      .await?;
      let res = all_docs.difference(&excluded).cloned().collect();
      Ok(res)
    }
    _ => Ok(RapidHashSet::default()),
  }
}

/// RediSearch 全文检索命令主调度处理器
pub async fn handle_search(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();

  match cmd {
    RedisCommand::FtCreate {
      index,
      on_data_type,
      prefixes,
      fields,
    } => {
      let ft_k = kc.ft_schema(&index);
      if node.read(GetKVReq { key: ft_k.clone() }).await?.is_some() {
        return Err(Error::invalid_data("Index already exists"));
      }

      let parsed_data_type = match on_data_type.to_ascii_uppercase().as_str() {
        "JSON" => IndexOnDataType::Json,
        _ => IndexOnDataType::Hash,
      };

      let field_defs = parse_schema_fields(&fields);
      let rs =
        SearchIndexSchema::with_full_spec(index.clone(), parsed_data_type, prefixes, field_defs);
      let encoded = bitcode::encode(&rs);
      let mut entries = vec![UpsertKV::insert(ft_k, encoded)];

      for prefix in &rs.prefixes {
        let doc_prefix = kc.raw_key_bytes(prefix.as_bytes()).into_owned();
        let docs = node
          .scan_prefix(ScanPrefixReq { prefix: doc_prefix })
          .await?;
        for (k, v) in docs {
          if let Some(user_k) = kc.extract_user_key(&k) {
            let doc_id = String::from_utf8_lossy(user_k).into_owned();
            let terms = extract_doc_terms(&rs, &doc_id, &v);
            for (field_name, term) in terms {
              let posting_k = kc.ft_index_key(&index, &field_name, &term, &doc_id);
              entries.push(UpsertKV::insert(posting_k, Vec::new()));
            }
          }
        }
      }

      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::FtSearch {
      index,
      query,
      nocontent,
      return_fields,
      offset,
      limit,
      sortby,
    } => {
      let real_index = match node
        .read(GetKVReq {
          key: kc.ft_alias(&index),
        })
        .await?
      {
        Some(b) => String::from_utf8(b).unwrap_or_else(|_| index.clone()),
        None => index.clone(),
      };

      let ft_k = kc.ft_schema(&real_index);
      let rs: SearchIndexSchema = match node.read(GetKVReq { key: ft_k }).await? {
        Some(b) => {
          bitcode::decode::<SearchIndexSchema>(&b).map_err(|e| Error::internal(e.to_string()))?
        }
        None => return Err(Error::invalid_data("Unknown Index name")),
      };

      exec_search_query(
        node,
        &kc,
        &real_index,
        &rs,
        &query,
        SearchQueryOptions {
          nocontent,
          return_fields,
          offset,
          limit,
          sortby,
        },
      )
      .await
    }
    RedisCommand::FtSearchSql { index, query } => {
      Box::pin(handle_search(
        node,
        ctx,
        RedisCommand::FtSearch {
          index,
          query,
          nocontent: false,
          return_fields: None,
          offset: 0,
          limit: 10,
          sortby: None,
        },
      ))
      .await
    }
    RedisCommand::FtExplain { index: _, query } => {
      let ast = parse_search_query(&query);
      let plan = explain_search_query(&ast);
      Ok(RespValue::Blob(plan.into_bytes()))
    }
    RedisCommand::FtExplainSql { index, query } => {
      Box::pin(handle_search(
        node,
        ctx,
        RedisCommand::FtExplain { index, query },
      ))
      .await
    }
    RedisCommand::FtInfo(index) => {
      let real_index = match node
        .read(GetKVReq {
          key: kc.ft_alias(&index),
        })
        .await?
      {
        Some(b) => String::from_utf8(b).unwrap_or_else(|_| index.clone()),
        None => index.clone(),
      };

      let ft_k = kc.ft_schema(&real_index);
      let rs: SearchIndexSchema = match node.read(GetKVReq { key: ft_k }).await? {
        Some(b) => {
          bitcode::decode::<SearchIndexSchema>(&b).map_err(|e| Error::internal(e.to_string()))?
        }
        None => return Err(Error::invalid_data("Unknown Index name")),
      };
      let idx_prefix = kc.ft_index_prefix(&real_index);
      let postings = node
        .scan_prefix(ScanPrefixReq { prefix: idx_prefix })
        .await?;
      let mut unique_docs = RapidHashSet::default();
      for (k, _) in &postings {
        if let Some(colon_pos) = memchr::memrchr(b':', k)
          && let Ok(doc_id) = from_utf8(&k[colon_pos + 1..])
        {
          unique_docs.insert(doc_id);
        }
      }

      let mut fields_info = Vec::with_capacity(rs.fields.len());
      for f in &rs.fields {
        let f_entry = vec![
          RespValue::Blob(f.name.as_bytes().to_vec()),
          RespValue::Simple("type".to_string()),
          RespValue::Simple(f.field_type.as_str().to_string()),
          RespValue::Simple("WEIGHT".to_string()),
          float_to_blob(f.weight),
        ];
        fields_info.push(RespValue::Arr(f_entry));
      }

      let info = vec![
        RespValue::Simple("index_name".to_string()),
        RespValue::Simple(rs.name.to_string()),
        RespValue::Simple("index_options".to_string()),
        RespValue::Arr(Vec::new()),
        RespValue::Simple("index_definition".to_string()),
        RespValue::Arr(vec![
          RespValue::Simple("key_type".to_string()),
          RespValue::Simple(rs.on_data_type.as_str().to_string()),
          RespValue::Simple("prefixes".to_string()),
          RespValue::Arr(
            rs.prefixes
              .iter()
              .map(|p| RespValue::Blob(p.as_bytes().to_vec()))
              .collect(),
          ),
        ]),
        RespValue::Simple("fields".to_string()),
        RespValue::Arr(fields_info),
        RespValue::Simple("num_docs".to_string()),
        RespValue::Int(unique_docs.len() as i64),
        RespValue::Simple("num_terms".to_string()),
        RespValue::Int(postings.len() as i64),
      ];
      Ok(RespValue::Arr(info))
    }
    RedisCommand::FtDropIndex { index, drop_docs } => {
      let ft_k = kc.ft_schema(&index);
      let rs_opt: Option<SearchIndexSchema> =
        match node.read(GetKVReq { key: ft_k.clone() }).await? {
          Some(b) => bitcode::decode::<SearchIndexSchema>(&b).ok(),
          None => return Err(Error::invalid_data("Unknown Index name")),
        };
      let rs = match rs_opt {
        Some(r) => r,
        None => return Err(Error::invalid_data("Unknown Index name")),
      };

      let mut entries = vec![UpsertKV::delete(ft_k)];
      let idx_prefix = kc.ft_index_prefix(&index);
      let postings = node
        .scan_prefix(ScanPrefixReq { prefix: idx_prefix })
        .await?;
      for (k, _) in postings {
        if let Ok(k_str) = String::from_utf8(k) {
          entries.push(UpsertKV::delete(k_str));
        }
      }

      if drop_docs {
        for prefix in &rs.prefixes {
          let doc_prefix = kc.raw_key_bytes(prefix.as_bytes()).into_owned();
          let docs = node
            .scan_prefix(ScanPrefixReq { prefix: doc_prefix })
            .await?;
          for (k, _) in docs {
            if let Some(user_k) = kc.extract_user_key(&k) {
              let doc_id = String::from_utf8_lossy(user_k).into_owned();
              let storage_entries = collect_key_storage_entries(node, &kc, &doc_id).await?;
              for (storage_k, _) in storage_entries {
                entries.push(UpsertKV::delete(storage_k));
              }
            }
          }
        }
      }

      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::FtList => {
      let prefix = kc.ft_schema_prefix();
      let subkeys = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;
      let mut list = Vec::with_capacity(subkeys.len());
      for (k, _) in subkeys {
        list.push(RespValue::Blob(k[prefix.len()..].to_vec()));
      }
      Ok(RespValue::Arr(list))
    }
    RedisCommand::FtAliasAdd { alias, index } => {
      let ft_k = kc.ft_schema(&index);
      if node.read(GetKVReq { key: ft_k }).await?.is_none() {
        return Err(Error::invalid_data("Unknown Index name"));
      }
      let alias_k = kc.ft_alias(&alias);
      let entries = vec![UpsertKV::insert(alias_k, index.into_bytes())];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::FtAliasDel { alias } => {
      let alias_k = kc.ft_alias(&alias);
      let entries = vec![UpsertKV::delete(alias_k)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::FtTagVals(index, field) => {
      let prefix = kc.ft_index_field_prefix(&index, &field);
      let postings = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;
      let mut tags = RapidHashSet::default();
      for (k, _) in postings {
        let remain = &k[prefix.len()..];
        if let Some(colon_pos) = memchr::memchr(b':', remain)
          && let Ok(tag_str) = from_utf8(&remain[..colon_pos])
        {
          tags.insert(tag_str.to_string());
        }
      }
      let mut list: Vec<String> = tags.into_iter().collect();
      list.sort_unstable();
      let results = list
        .into_iter()
        .map(|t| RespValue::Blob(t.into_bytes()))
        .collect();
      Ok(RespValue::Arr(results))
    }
    _ => Err(Error::internal("unsupported search command")),
  }
}
