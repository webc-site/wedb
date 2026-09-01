use clap_args::{
  arg,
  clap::{ArgMatches, Command, parser::ValueSource, value_parser},
};

use super::common::{
  CommonCliArgs, format_endpoint, get_arg_or_conf, get_opt_arg_or_conf,
  parse_comma_separated_addrs, validate_socket_addrs,
};

/// 分布式集群模式命令行参数
#[derive(Debug, Clone)]
pub struct ClusterCliArgs {
  pub common: CommonCliArgs,
  pub node_id: u64,
  pub raft: String,
  pub join: Vec<String>,
  pub heartbeat: u64,
  pub region: Option<String>,
  pub zone: Option<String>,
  pub rack: Option<String>,
  pub host: Option<String>,
  pub weight: Option<u32>,
}

use wedb_cluster::conf::{ClusterMode, Conf, Endpoint, RaftConf, RedisConf, TopologyConf};

impl ClusterCliArgs {
  #[inline]
  pub fn to_topology_conf(&self) -> Option<TopologyConf> {
    if self.region.is_some()
      || self.zone.is_some()
      || self.rack.is_some()
      || self.host.is_some()
      || self.weight.is_some()
    {
      Some(TopologyConf {
        region: self.region.clone(),
        zone: self.zone.clone(),
        rack: self.rack.clone(),
        host: self.host.clone(),
        weight: self.weight,
        redis_port: None,
      })
    } else {
      None
    }
  }

  #[inline]
  pub fn to_cluster_conf(&self) -> aok::Result<Conf> {
    let endpoint: Endpoint = self.raft.parse()?;
    Ok(Conf {
      node_id: self.node_id,
      mode: ClusterMode::default(),
      topology: self.to_topology_conf(),
      raft: RaftConf {
        endpoint: endpoint.clone(),
        advertise_endpoint: endpoint,
        join: self.join.clone(),
        heartbeat_interval: Some(self.heartbeat),
        election_timeout_min: None,
        election_timeout_max: None,
      },
      fjall: self.common.to_fjall_conf(),
      redis: RedisConf {
        addr: self.common.addr.clone(),
        enabled: true,
      },
    })
  }
  pub fn add_args(cmd: Command) -> Command {
    let cmd = cmd
            .about("High-performance Distributed Redis Cluster backed by OpenRaft and LSM Fjall Engine")
            .arg(
                arg!(-i --node_id <ID> "Node ID in the Raft cluster")
                    .default_value("1")
                    .value_parser(value_parser!(u64))
                    .visible_alias("node-id"),
            )
            .arg(
                arg!(-r --raft <ADDR> "Raft RPC service listening address or port (e.g. 4910, 127.0.0.1:4910)")
                    .default_value("4910")
                    .visible_alias("raft_addr")
                    .visible_alias("raft-addr")
                    .visible_alias("cluster_addr")
                    .visible_alias("cluster-addr"),
            )
            .arg(
                arg!(-j --join <ADDRS> "Comma-separated peer Raft IP:PORT address list to join on startup (e.g. --join 127.0.0.1:4910,127.0.0.1:4911)")
            )
            .arg(
                arg!(--heartbeat <MS> "Raft heartbeat interval in milliseconds")
                    .default_value("50")
                    .value_parser(value_parser!(u64)),
            )
            .arg(
                arg!(--region <REGION> "Node geographic region (e.g. cn-beijing, us-west)")
            )
            .arg(
                arg!(--zone <ZONE> "Node availability zone / datacenter (e.g. zone-a, dc-01)")
                    .visible_alias("dc")
                    .visible_alias("az"),
            )
            .arg(
                arg!(--rack <RACK> "Node rack / cabinet identifier (e.g. rack-01)")
            )
            .arg(
                arg!(--host <HOST> "Node physical machine host identifier (e.g. 10.0.1.10)")
            )
            .arg(
                arg!(-w --weight <WEIGHT> "Node capacity weight for weighted sharding (default 100)")
                    .value_parser(value_parser!(u32)),
            );
    CommonCliArgs::add_args(cmd)
  }

  pub fn command() -> Command {
    Self::add_args(Command::new("wedb-cluster"))
  }

  pub fn from_matches(matches: &ArgMatches) -> aok::Result<Self> {
    let common = CommonCliArgs::extract(matches);
    let conf_file = common.conf_file.as_ref();

    let node_id = get_arg_or_conf(matches, "node_id", conf_file.and_then(|c| c.get_node_id()));

    let default_ip = common.ip.as_deref().unwrap_or("127.0.0.1");
    let raft_is_user_set = matches.value_source("raft") == Some(ValueSource::CommandLine);
    let raw_raft = matches.get_one::<String>("raft").cloned().unwrap();
    let raft = if raft_is_user_set {
      format_endpoint(&raw_raft, default_ip)
    } else {
      conf_file
        .and_then(|c| c.get_raft_addr())
        .unwrap_or_else(|| format_endpoint(&raw_raft, default_ip))
    };

    let join = if matches.value_source("join") == Some(ValueSource::CommandLine) {
      matches
        .get_one::<String>("join")
        .map(|s| parse_comma_separated_addrs(s))
        .transpose()?
        .unwrap_or_default()
    } else if let Some(conf_join) = conf_file.and_then(|c| c.get_join()) {
      validate_socket_addrs(&conf_join)?
    } else {
      matches
        .get_one::<String>("join")
        .map(|s| parse_comma_separated_addrs(s))
        .transpose()?
        .unwrap_or_default()
    };

    let heartbeat = get_arg_or_conf(
      matches,
      "heartbeat",
      conf_file.and_then(|c| c.get_heartbeat()),
    );
    let region = get_opt_arg_or_conf(matches, "region", conf_file.and_then(|c| c.get_region()));
    let zone = get_opt_arg_or_conf(matches, "zone", conf_file.and_then(|c| c.get_zone()));
    let rack = get_opt_arg_or_conf(matches, "rack", conf_file.and_then(|c| c.get_rack()));
    let host = get_opt_arg_or_conf(matches, "host", conf_file.and_then(|c| c.get_host()));
    let weight = get_opt_arg_or_conf(matches, "weight", conf_file.and_then(|c| c.get_weight()));

    Ok(Self {
      common,
      node_id,
      raft,
      join,
      heartbeat,
      region,
      zone,
      rack,
      host,
      weight,
    })
  }

  pub fn parse() -> aok::Result<Option<Self>> {
    let Some(matches) = clap_args::parse!(|cmd| { Self::add_args(cmd) }) else {
      return Ok(None);
    };

    Self::from_matches(&matches).map(Some)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_cluster_cli_single_join() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from(["wedb-cluster", "--join", "127.0.0.1:4910"])
      .unwrap();
    let args = ClusterCliArgs::from_matches(&matches).unwrap();
    assert_eq!(args.join, vec!["127.0.0.1:4910"]);
  }

  #[test]
  fn test_cluster_cli_comma_separated_multiple_join() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from([
        "wedb-cluster",
        "--join",
        "127.0.0.1:4910,127.0.0.1:4911,10.0.1.12:4912",
      ])
      .unwrap();
    let args = ClusterCliArgs::from_matches(&matches).unwrap();
    assert_eq!(
      args.join,
      vec!["127.0.0.1:4910", "127.0.0.1:4911", "10.0.1.12:4912"]
    );
  }

  #[test]
  fn test_cluster_cli_comma_separated_with_spaces() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from([
        "wedb-cluster",
        "-j",
        " 127.0.0.1:4910 ,  127.0.0.1:4911 , 10.0.1.12:4912 ",
      ])
      .unwrap();
    let args = ClusterCliArgs::from_matches(&matches).unwrap();
    assert_eq!(
      args.join,
      vec!["127.0.0.1:4910", "127.0.0.1:4911", "10.0.1.12:4912"]
    );
  }

  #[test]
  fn test_cluster_cli_consecutive_and_trailing_commas() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from(["wedb-cluster", "--join", ",127.0.0.1:4910,,127.0.0.1:4911,"])
      .unwrap();
    let args = ClusterCliArgs::from_matches(&matches).unwrap();
    assert_eq!(args.join, vec!["127.0.0.1:4910", "127.0.0.1:4911"]);
  }

  #[test]
  fn test_cluster_cli_ipv6_join() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from(["wedb-cluster", "--join", "[::1]:4910,[fe80::1]:4911"])
      .unwrap();
    let args = ClusterCliArgs::from_matches(&matches).unwrap();
    assert_eq!(args.join, vec!["[::1]:4910", "[fe80::1]:4911"]);
  }

  #[test]
  fn test_cluster_cli_multiple_join_flags_rejected() {
    // 严格不支持多次传入 -j 参数，必须使用逗号隔开的单一参数
    let res = ClusterCliArgs::command().try_get_matches_from([
      "wedb-cluster",
      "-j",
      "127.0.0.1:4910",
      "-j",
      "127.0.0.1:4911",
    ]);
    assert!(res.is_err(), "Multiple -j flags must be rejected by clap");
  }

  #[test]
  fn test_cluster_cli_invalid_join_with_node_id_prefix_fails() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from(["wedb-cluster", "--join", "1=127.0.0.1:4910"])
      .unwrap();
    let err = ClusterCliArgs::from_matches(&matches).unwrap_err();
    let err_msg = err.to_string();
    assert!(
      err_msg.contains("1=127.0.0.1:4910"),
      "Error should include invalid parameter, got: {err_msg}"
    );
    assert!(
      err_msg.contains("must be standard IP:PORT format"),
      "Error should indicate failure to parse as socket address, got: {err_msg}"
    );
  }

  #[test]
  fn test_cluster_cli_invalid_join_domain_name_fails() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from(["wedb-cluster", "--join", "localhost:4910"])
      .unwrap();
    let err = ClusterCliArgs::from_matches(&matches).unwrap_err();
    let err_msg = err.to_string();
    assert!(
      err_msg.contains("localhost:4910"),
      "Error should include invalid domain address, got: {err_msg}"
    );
  }

  #[test]
  fn test_cluster_cli_invalid_join_missing_port_fails() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from(["wedb-cluster", "--join", "127.0.0.1"])
      .unwrap();
    let err = ClusterCliArgs::from_matches(&matches).unwrap_err();
    let err_msg = err.to_string();
    assert!(
      err_msg.contains("127.0.0.1"),
      "Error should include missing port address, got: {err_msg}"
    );
  }

  #[test]
  fn test_cluster_cli_invalid_join_invalid_ip_fails() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from(["wedb-cluster", "--join", "999.999.999.999:4910"])
      .unwrap();
    let err = ClusterCliArgs::from_matches(&matches).unwrap_err();
    let err_msg = err.to_string();
    assert!(
      err_msg.contains("999.999.999.999:4910"),
      "Error should include invalid IP address, got: {err_msg}"
    );
  }

  #[test]
  fn test_cluster_cli_invalid_join_port_overflow_fails() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from(["wedb-cluster", "--join", "127.0.0.1:99999"])
      .unwrap();
    let err = ClusterCliArgs::from_matches(&matches).unwrap_err();
    let err_msg = err.to_string();
    assert!(
      err_msg.contains("127.0.0.1:99999"),
      "Error should include overflow port, got: {err_msg}"
    );
  }

  #[test]
  fn test_cluster_cli_default_data_dir_is_wedb() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from(["wedb-cluster"])
      .unwrap();
    let args = ClusterCliArgs::from_matches(&matches).unwrap();
    assert_eq!(args.common.data_dir, "wedb");
  }

  #[test]
  fn test_cluster_cli_custom_data_dir() {
    let matches = ClusterCliArgs::command()
      .try_get_matches_from(["wedb-cluster", "--data_dir", "/mnt/nvme/wedb_data"])
      .unwrap();
    let args = ClusterCliArgs::from_matches(&matches).unwrap();
    assert_eq!(args.common.data_dir, "/mnt/nvme/wedb_data");
  }
}
