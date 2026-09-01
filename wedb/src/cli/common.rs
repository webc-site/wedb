use std::net::SocketAddr;
use std::path::Path;

use clap_args::{
  arg,
  clap::{ArgMatches, Command, parser::ValueSource, value_parser},
};

use super::conf_file::ConfFile;

/// 格式化服务地址或端口（若仅为纯端口号则与指定 IP 拼接）
#[inline]
pub fn format_endpoint(raw: &str, default_ip: &str) -> String {
  if raw.contains(':') {
    raw.to_string()
  } else {
    format!("{default_ip}:{raw}")
  }
}

/// 优先从命令行获取显式设置的参数，否则尝试从配置文件获取，最后回退到命令行默认值
#[inline]
pub fn get_arg_or_conf<T: Clone + Send + Sync + 'static>(
  matches: &ArgMatches,
  id: &str,
  conf_val: Option<T>,
) -> T {
  if matches.value_source(id) == Some(ValueSource::CommandLine) {
    matches.get_one::<T>(id).cloned().unwrap()
  } else {
    conf_val.unwrap_or_else(|| matches.get_one::<T>(id).cloned().unwrap())
  }
}

/// 优先从命令行获取可选参数，否则尝试从配置文件获取
#[inline]
pub fn get_opt_arg_or_conf<T: Clone + Send + Sync + 'static>(
  matches: &ArgMatches,
  id: &str,
  conf_val: Option<T>,
) -> Option<T> {
  if matches.value_source(id) == Some(ValueSource::CommandLine) {
    matches.get_one::<T>(id).cloned()
  } else {
    conf_val.or_else(|| matches.get_one::<T>(id).cloned())
  }
}

/// 解析逗号分隔的 IP:PORT 字符串，严格校验每个项为标准 SocketAddr
pub fn parse_comma_separated_addrs(raw: &str) -> aok::Result<Vec<String>> {
  let mut addrs = Vec::new();
  for item in raw.split(',') {
    let trimmed = item.trim();
    if !trimmed.is_empty() {
      if let Err(e) = trimmed.parse::<SocketAddr>() {
        return Err(aok::anyhow!(
          "Invalid join address '{trimmed}': must be standard IP:PORT format ({e})"
        ));
      }
      addrs.push(trimmed.to_string());
    }
  }
  Ok(addrs)
}

/// 校验地址列表中的每一项均符合标准 SocketAddr 格式
pub fn validate_socket_addrs(raw_li: &[String]) -> aok::Result<Vec<String>> {
  let mut addrs = Vec::with_capacity(raw_li.len());
  for item in raw_li {
    for sub in item.split(',') {
      let trimmed = sub.trim();
      if !trimmed.is_empty() {
        if let Err(e) = trimmed.parse::<SocketAddr>() {
          return Err(aok::anyhow!(
            "Invalid join address '{trimmed}': must be standard IP:PORT format ({e})"
          ));
        }
        addrs.push(trimmed.to_string());
      }
    }
  }
  Ok(addrs)
}

/// 单机与集群通用命令行参数
#[derive(Debug, Clone)]
pub struct CommonCliArgs {
  pub conf_path: Option<String>,
  pub conf_file: Option<ConfFile>,
  pub ip: Option<String>,
  pub addr: String,
  pub data_dir: String,
  pub compression: String,
  pub journal_compression: String,
  pub manual_journal_persist: Option<bool>,
  pub cache_size: Option<usize>,
  pub worker_threads: Option<usize>,
  pub max_journaling_size: Option<usize>,
}

impl CommonCliArgs {
  #[inline]
  pub fn to_fjall_conf(&self) -> super::conf_file::FjallConf {
    super::conf_file::FjallConf {
      data_path: self.data_dir.clone(),
      cache_size: self.cache_size,
      compression: Some(self.compression.clone()),
      journal_compression: Some(self.journal_compression.clone()),
      manual_journal_persist: self.manual_journal_persist,
      worker_threads: self.worker_threads,
      max_journaling_size: self.max_journaling_size,
      max_cached_files: None,
    }
  }

  pub fn add_args(cmd: Command) -> Command {
    cmd.arg(
            arg!(-f --conf <PATH> "Configuration file path (NestedText format, e.g. wedb.nt)")
                .visible_alias("config"),
        )
        .arg(
            arg!(--ip <IP> "Common IP/host address for services (e.g. 127.0.0.1, 0.0.0.0)")
                .visible_alias("bind"),
        )
        .arg(
            arg!(-p --addr <PORT_OR_ADDR> "Service listening address or port (e.g. 6379, 127.0.0.1:6379)")
                .default_value("4909")
                .visible_alias("port")
                .visible_alias("redis_addr")
                .visible_alias("redis-addr"),
        )
        .arg(
            arg!(-d --data_dir <PATH> "Data storage directory path for Fjall engine (defaults to 'wedb' in current directory)")
                .default_value("wedb")
                .visible_alias("data-dir"),
        )
        .arg(
            arg!(-c --compression <TYPE> "Data compression for Fjall engine (lz4, none)")
                .default_value("lz4"),
        )
        .arg(
            arg!(--journal_compression <TYPE> "WAL Journal compression for Fjall engine (none, lz4)")
                .default_value("none")
                .visible_alias("journal-compression"),
        )
        .arg(
            arg!(--manual_journal_persist <FLAG> "Manual journal persist flag (true, false)")
                .value_parser(value_parser!(bool))
                .visible_alias("manual-journal-persist"),
        )
        .arg(
            arg!(--cache_size <BYTES> "Block cache size in bytes (optional)")
                .value_parser(value_parser!(usize))
                .visible_alias("cache-size"),
        )
        .arg(
            arg!(--worker_threads <NUM> "Number of background worker threads for Fjall engine (optional)")
                .value_parser(value_parser!(usize))
                .visible_alias("worker-threads"),
        )
        .arg(
            arg!(--max_journaling_size <BYTES> "Maximum size of all journals in bytes for Fjall engine (optional)")
                .value_parser(value_parser!(usize))
                .visible_alias("max-journaling-size"),
        )
  }

  pub fn extract(matches: &ArgMatches) -> Self {
    let conf_path = matches.get_one::<String>("conf").cloned();
    let conf_file = conf_path
      .as_deref()
      .and_then(|p| ConfFile::load_from_file(p).ok())
      .or_else(|| {
        if Path::new("wedb.nt").exists() {
          ConfFile::load_from_file("wedb.nt").ok()
        } else {
          None
        }
      });

    let ip = matches
      .get_one::<String>("ip")
      .cloned()
      .or_else(|| conf_file.as_ref().and_then(|c| c.get_ip()));

    let addr_is_user_set = matches.value_source("addr") == Some(ValueSource::CommandLine);
    let raw_addr = matches.get_one::<String>("addr").cloned().unwrap();
    let default_ip = ip.as_deref().unwrap_or("127.0.0.1");

    let addr = if addr_is_user_set {
      format_endpoint(&raw_addr, default_ip)
    } else {
      conf_file
        .as_ref()
        .and_then(|c| c.get_addr())
        .unwrap_or_else(|| format_endpoint(&raw_addr, default_ip))
    };

    let data_dir = get_arg_or_conf(
      matches,
      "data_dir",
      conf_file.as_ref().and_then(|c| c.get_data_dir()),
    );
    let compression = get_arg_or_conf(
      matches,
      "compression",
      conf_file.as_ref().and_then(|c| c.get_compression()),
    );
    let journal_compression = get_arg_or_conf(
      matches,
      "journal_compression",
      conf_file.as_ref().and_then(|c| c.get_journal_compression()),
    );
    let manual_journal_persist = get_opt_arg_or_conf(
      matches,
      "manual_journal_persist",
      conf_file
        .as_ref()
        .and_then(|c| c.get_manual_journal_persist()),
    );
    let cache_size = get_opt_arg_or_conf(
      matches,
      "cache_size",
      conf_file.as_ref().and_then(|c| c.get_cache_size()),
    );
    let worker_threads = get_opt_arg_or_conf(
      matches,
      "worker_threads",
      conf_file.as_ref().and_then(|c| c.get_worker_threads()),
    );
    let max_journaling_size = get_opt_arg_or_conf(
      matches,
      "max_journaling_size",
      conf_file.as_ref().and_then(|c| c.get_max_journaling_size()),
    );

    Self {
      conf_path,
      conf_file,
      ip,
      addr,
      data_dir,
      compression,
      journal_compression,
      manual_journal_persist,
      cache_size,
      worker_threads,
      max_journaling_size,
    }
  }
}
