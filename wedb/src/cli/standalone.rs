use super::common::CommonCliArgs;

/// 单机模式命令行参数
#[derive(Debug, Clone)]
pub struct StandaloneCliArgs {
  pub common: CommonCliArgs,
}

impl StandaloneCliArgs {
  pub fn parse() -> aok::Result<Option<Self>> {
    let Some(matches) = clap_args::parse!(|cmd| {
      let cmd = cmd.about("High-performance Standalone Redis Server backed by LSM Fjall Engine");
      CommonCliArgs::add_args(cmd)
    }) else {
      return Ok(None);
    };

    let common = CommonCliArgs::extract(&matches);
    Ok(Some(Self { common }))
  }
}
