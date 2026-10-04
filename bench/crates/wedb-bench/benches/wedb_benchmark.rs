//! 自家引擎单列表格（对标 redb 的 `redb_benchmark`）：
//! 只跑编译进来的 hash / bftree，不带第三方引擎的原生构建依赖。

use std::{env::args, process::exit};

use wedb_bench::{engines::wedb_specs, main_logic};

fn main() {
  let argv: Vec<String> = args().skip(1).collect();
  if let Err(error) = main_logic(&wedb_specs(), &argv) {
    eprintln!("{error}");
    exit(1);
  }
}
