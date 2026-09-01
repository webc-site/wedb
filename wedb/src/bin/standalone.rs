use mimalloc::MiMalloc;
use wedb::cli::StandaloneCliArgs;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[compio::main]
async fn main() -> aok::Result<()> {
  log_init::init();

  let Some(args) = StandaloneCliArgs::parse()? else {
    return Ok(());
  };

  let addr = &args.common.addr;
  log::info!("Starting WeDb Standalone Server on {addr}");
  let conf = args.common.to_fjall_conf();
  wedb_standalone::run_server_with_conf(addr, &conf).await?;

  Ok(())
}
