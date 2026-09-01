use mimalloc::MiMalloc;
use wedb::cli::ClusterCliArgs;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[compio::main]
async fn main() -> aok::Result<()> {
  log_init::init();

  let Some(args) = ClusterCliArgs::parse()? else {
    return Ok(());
  };

  let conf = args.to_cluster_conf()?;
  log::info!(
    "Starting WeDb Cluster Node #{} (Addr: {}, Raft: {})",
    conf.node_id,
    conf.redis.addr,
    conf.raft.endpoint
  );

  wedb_cluster::run_cluster_node_with_conf(&conf).await?;

  Ok(())
}
