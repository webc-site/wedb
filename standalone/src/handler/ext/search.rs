use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{Error, Result, WeDb, explain_search_query_cli, parse_search_query};
use wedb_resp::RespValue;

/// 处理所有 RediSearch (全文检索) 命令
pub async fn handle_search(_db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::FtCreate { .. } => Ok(RespValue::ok()),
    Cmd::FtSearch { .. } => Ok(RespValue::Arr(vec![RespValue::Int(0)])),
    Cmd::FtSearchSql { .. } => Ok(RespValue::Arr(vec![RespValue::Int(0)])),
    Cmd::FtExplain { query, .. } | Cmd::FtExplainSql { query, .. } => {
      let node = parse_search_query(&query);
      let explanation = explain_search_query_cli(&node);
      Ok(RespValue::Blob(explanation.into_bytes()))
    }
    Cmd::FtInfo(index) => Ok(RespValue::Arr(vec![
      RespValue::Simple("index_name".to_string()),
      RespValue::Blob(index.into_bytes()),
      RespValue::Simple("num_docs".to_string()),
      RespValue::Int(0),
    ])),
    Cmd::FtList => Ok(RespValue::Arr(Vec::new())),
    Cmd::FtDropIndex { .. } => Ok(RespValue::ok()),
    Cmd::FtAliasAdd { .. } | Cmd::FtAliasDel { .. } => Ok(RespValue::ok()),
    Cmd::FtTagVals(_, _) => Ok(RespValue::Arr(Vec::new())),
    _ => Err(Error::internal("unsupported search command")),
  }
}
