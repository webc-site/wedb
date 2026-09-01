use coarsetime::Clock;
use std::sync::Arc;
use webc_cmd::{Cmd, ConnectionContext};
use wedb_embed::{
  Error, NS_NAME_PREFIX, Result, WeDb, is_default_namespace, ns_name_key, ns_token_key,
};
use wedb_resp::RespValue;

/// 处理所有连接、认证、命名空间和系统管理命令
pub async fn handle_conn(
  db: &Arc<WeDb>,
  ctx: &mut ConnectionContext,
  cmd: Cmd,
) -> Result<RespValue> {
  match cmd {
        Cmd::Ping(msg) => match msg {
            Some(m) => Ok(RespValue::Blob(m.into_bytes())),
            None => Ok(RespValue::pong()),
        },
        Cmd::Echo(msg) => Ok(RespValue::Blob(msg.into_bytes())),
        Cmd::Hello(ver) => {
            let proto = ver.unwrap_or(2);
            Ok(RespValue::Map(vec![
                (
                    RespValue::Simple("server".to_string()),
                    RespValue::Simple("wedb".to_string()),
                ),
                (
                    RespValue::Simple("version".to_string()),
                    RespValue::Simple(env!("CARGO_PKG_VERSION").to_string()),
                ),
                (
                    RespValue::Simple("proto".to_string()),
                    RespValue::Int(proto as i64),
                ),
                (
                    RespValue::Simple("id".to_string()),
                    RespValue::Int(1),
                ),
                (
                    RespValue::Simple("mode".to_string()),
                    RespValue::Simple("standalone".to_string()),
                ),
                (
                    RespValue::Simple("role".to_string()),
                    RespValue::Simple("master".to_string()),
                ),
            ]))
        }
        Cmd::Quit => Ok(RespValue::ok()),
        Cmd::Select(db_num) => {
            let ns = db.select_db(db_num);
            ctx.set_namespace(ns.name());
            Ok(RespValue::ok())
        }
        Cmd::Auth { password, .. } => {
            let token_key = ns_token_key(&password);
            if let Some(ns_bytes) = db.meta_ns.get(token_key.as_bytes())? {
                let ns_str = String::from_utf8_lossy(&ns_bytes).into_owned();
                ctx.set_namespace(ns_str);
                ctx.become_user();
            } else {
                ctx.set_namespace("default");
                ctx.become_admin();
            }
            Ok(RespValue::ok())
        }
        Cmd::NamespaceAdd(ns, token) => {
            if is_default_namespace(&ns) {
                return Err(Error::invalid_data("ERR forbidden to add the default namespace"));
            }
            if token.is_empty() {
                return Err(Error::invalid_data("ERR token cannot be empty"));
            }
            let name_key = ns_name_key(&ns);
            let token_key = ns_token_key(&token);

            if let Some(existing_token_bytes) = db.meta_ns.get(name_key.as_bytes())? {
                let existing_token = String::from_utf8_lossy(&existing_token_bytes);
                if existing_token == token {
                    return Ok(RespValue::ok());
                }
                return Err(Error::invalid_data("ERR the namespace already exists"));
            }

            if db.meta_ns.contains_key(token_key.as_bytes())? {
                return Err(Error::invalid_data("ERR the token already exists"));
            }

            let mut batch = db.db.batch();
            batch.insert(&db.meta_ns, token_key.as_bytes(), ns.as_bytes());
            batch.insert(&db.meta_ns, name_key.as_bytes(), token.as_bytes());
            batch.commit()?;
            Ok(RespValue::ok())
        }
        Cmd::NamespaceSet(ns, token) => {
            if is_default_namespace(&ns) {
                return Err(Error::invalid_data("ERR forbidden to add the default namespace"));
            }
            if token.is_empty() {
                return Err(Error::invalid_data("ERR token cannot be empty"));
            }
            let name_key = ns_name_key(&ns);
            let token_key = ns_token_key(&token);

            if let Some(existing_ns_bytes) = db.meta_ns.get(token_key.as_bytes())? {
                let existing_ns = String::from_utf8_lossy(&existing_ns_bytes);
                if existing_ns != ns {
                    return Err(Error::invalid_data("ERR the token already exists"));
                }
            }

            let mut batch = db.db.batch();
            if let Some(old_token_bytes) = db.meta_ns.get(name_key.as_bytes())? {
                let old_token = String::from_utf8_lossy(&old_token_bytes);
                if old_token != token {
                    batch.remove(&db.meta_ns, ns_token_key(&old_token).as_bytes());
                }
            }

            batch.insert(&db.meta_ns, token_key.as_bytes(), ns.as_bytes());
            batch.insert(&db.meta_ns, name_key.as_bytes(), token.as_bytes());
            batch.commit()?;
            Ok(RespValue::ok())
        }
        Cmd::NamespaceDel(ns) => {
            if is_default_namespace(&ns) {
                return Err(Error::invalid_data("ERR forbidden to delete the default namespace"));
            }
            let name_key = ns_name_key(&ns);
            let token_bytes = match db.meta_ns.get(name_key.as_bytes())? {
                Some(t) => t,
                None => return Err(Error::invalid_data("ERR the namespace was not found")),
            };
            let token = String::from_utf8_lossy(&token_bytes);
            let token_key = ns_token_key(&token);

            let mut batch = db.db.batch();
            batch.remove(&db.meta_ns, name_key.as_bytes());
            batch.remove(&db.meta_ns, token_key.as_bytes());
            batch.commit()?;

            // 使用 Namespace 句柄清理物理数据
            db.namespace(&ns).clear()?;
            Ok(RespValue::ok())
        }
        Cmd::NamespaceGet(ns) => {
            if ns == "*" {
                let prefix = NS_NAME_PREFIX.as_bytes();
                let mut list = Vec::new();
                for item in db.meta_ns.prefix(prefix) {
                    let (k, v) = item.into_inner()?;
                    let name = String::from_utf8_lossy(&k[prefix.len()..]).into_owned();
                    let token = String::from_utf8_lossy(&v).into_owned();
                    list.push(RespValue::Blob(name.into_bytes()));
                    list.push(RespValue::Blob(token.into_bytes()));
                }
                list.push(RespValue::Blob(b"default".to_vec()));
                list.push(RespValue::Blob(b"".to_vec()));
                Ok(RespValue::Arr(list))
            } else if is_default_namespace(&ns) {
                Ok(RespValue::Blob(b"".to_vec()))
            } else {
                let name_key = ns_name_key(&ns);
                match db.meta_ns.get(name_key.as_bytes())? {
                    Some(token) => Ok(RespValue::Blob(token.to_vec())),
                    None => Ok(RespValue::Null),
                }
            }
        }
        Cmd::NamespaceCurrent => Ok(RespValue::Blob(ctx.namespace.as_bytes().to_vec())),
        Cmd::NamespaceId(target_ns) => {
            let ns_name = target_ns.as_deref().unwrap_or(&ctx.namespace);
            let id = db.ns_id(ns_name)?;
            Ok(RespValue::Int(id as i64))
        }
        Cmd::NamespaceRename(old_name, new_name) => {
            if is_default_namespace(&old_name) || is_default_namespace(&new_name) {
                return Err(Error::invalid_data("ERR forbidden to rename default namespace"));
            }
            let old_name_key = ns_name_key(&old_name);
            let new_name_key = ns_name_key(&new_name);

            let token_bytes = match db.meta_ns.get(old_name_key.as_bytes())? {
                Some(t) => t,
                None => return Err(Error::invalid_data("ERR the namespace was not found")),
            };
            if db.meta_ns.contains_key(new_name_key.as_bytes())? {
                return Err(Error::invalid_data("ERR the target namespace already exists"));
            }

            let token = String::from_utf8_lossy(&token_bytes).into_owned();
            let token_key = ns_token_key(&token);

            let mut batch = db.db.batch();
            batch.remove(&db.meta_ns, old_name_key.as_bytes());
            batch.insert(&db.meta_ns, new_name_key.as_bytes(), token.as_bytes());
            batch.insert(&db.meta_ns, token_key.as_bytes(), new_name.as_bytes());
            batch.commit()?;

            db.rename_namespace(&old_name, &new_name)?;

            if ctx.namespace == old_name {
                ctx.set_namespace(new_name);
            }

            Ok(RespValue::ok())
        }
        Cmd::Command => Ok(RespValue::Arr(vec![RespValue::ok()])),
        Cmd::ConfigGet(param) => Ok(RespValue::Arr(vec![
            RespValue::Blob(param.into_bytes()),
            RespValue::Blob(b"".to_vec()),
        ])),
        Cmd::ConfigSet(_, _) => Ok(RespValue::ok()),
        Cmd::Time => {
            let now = Clock::now_since_epoch();
            let sec = now.as_secs();
            let usec = now.as_micros() % 1_000_000;
            let mut sec_buf = itoa::Buffer::new();
            let mut usec_buf = itoa::Buffer::new();
            Ok(RespValue::Arr(vec![
                RespValue::Blob(sec_buf.format(sec).as_bytes().to_vec()),
                RespValue::Blob(usec_buf.format(usec).as_bytes().to_vec()),
            ]))
        }
        Cmd::ClientId => Ok(RespValue::Int(1)),
        Cmd::ClientGetName => Ok(RespValue::Null),
        Cmd::ClientSetName(_) => Ok(RespValue::ok()),
        Cmd::ClientList => Ok(RespValue::Blob(b"id=1 addr=127.0.0.1:0 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client\n".to_vec())),
        Cmd::ClientInfo => Ok(RespValue::Blob(b"id=1 addr=127.0.0.1:0\n".to_vec())),
        Cmd::ClientKill(_) => Ok(RespValue::Int(1)),
        Cmd::ClientPause(_) | Cmd::ClientUnpause => Ok(RespValue::ok()),
        Cmd::ClientUnblock { .. } => Ok(RespValue::Int(0)),
        Cmd::ClientTracking { .. } => Ok(RespValue::ok()),
        Cmd::ClientTrackingInfo => Ok(RespValue::Arr(vec![
            RespValue::Simple("flags".to_string()),
            RespValue::Arr(vec![RespValue::Simple("off".to_string())]),
        ])),
        Cmd::ClientGetRedir => Ok(RespValue::Int(-1)),
        Cmd::ClientSetInfo(_, _) => Ok(RespValue::ok()),
        Cmd::ClientNoTouch(_) | Cmd::ClientNoEvict(_) => Ok(RespValue::ok()),
        Cmd::ClientReply(_) => Ok(RespValue::ok()),
        Cmd::ClientHelp => Ok(RespValue::Arr(vec![
            RespValue::Simple("CLIENT <subcommand> [<arg> [value] ...]".to_string()),
        ])),
        Cmd::Info(_) => Ok(RespValue::Blob(b"# Server\nwedb_version:0.1.0\n# Replication\nrole:master\nconnected_slaves:0\n".to_vec())),
        Cmd::Role => Ok(RespValue::Arr(vec![
            RespValue::Simple("master".to_string()),
            RespValue::Int(0),
            RespValue::Arr(Vec::new()),
        ])),
        Cmd::Slowlog => Ok(RespValue::Arr(Vec::new())),
        Cmd::MemoryUsage(key) => {
            let exists = db.exists(&[key.as_bytes()])?;
            if exists > 0 {
                Ok(RespValue::Int(64))
            } else {
                Ok(RespValue::Null)
            }
        }
        Cmd::Reset => {
            ctx.reset_multi();
            Ok(RespValue::Simple("RESET".to_string()))
        }
        Cmd::Stats => Ok(RespValue::Arr(Vec::new())),
        _ => Err(Error::internal("unsupported connection command")),
    }
}
