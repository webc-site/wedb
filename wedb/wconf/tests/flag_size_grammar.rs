use clap::Parser;
use wconf::node_options::{NodeArgs, NodeOptionsError};

#[test]
fn test_flag_size_grammar() {
  // 测试拒启 t/p 档
  for flag in [
    "--aof-page-size=1t",
    "--aof-size-limit=4t",
    "--aof-memory=2t",
    "--index-max-size=1p",
    "--lua-script-memory-limit=3t",
  ] {
    let args = NodeArgs::try_parse_from([
      "wedb",
      flag,
      "--aof=true",
      "--lua-memory-management-mode=tracked",
    ])
    .unwrap();
    match args.validate() {
      Err(NodeOptionsError::InvalidSizeStr(_, raw)) => {
        assert!(raw.ends_with('t') || raw.ends_with('p'));
      }
      res => panic!("Expected InvalidSizeStr, got {:?}", res),
    }
  }

  // 测试正常通过的格式
  for val in ["128m", "1g", "1GB", "64kb"] {
    let args = NodeArgs::try_parse_from([
      "wedb",
      &format!("--aof-page-size={}", val),
      &format!("--aof-size-limit={}", val),
      &format!("--aof-memory={}", val),
      &format!("--index-max-size={}", val),
      &format!("--lua-script-memory-limit={}", val),
      "--aof=true",
      "--lua-memory-management-mode=tracked",
    ])
    .unwrap();
    assert!(args.validate().is_ok());
  }

  // 空串哨兵不报错（豁免）
  let args = NodeArgs::try_parse_from([
    "wedb",
    "--aof-size-limit=",
    "--index-max-size=",
    "--lua-script-memory-limit=",
    "--aof=true",
  ])
  .unwrap();
  assert!(args.validate().is_ok());
}
