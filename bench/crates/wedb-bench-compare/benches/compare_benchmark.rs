//! 五列同表的对比评测入口：`cargo bench -p wedb-bench-compare -- --quick`。

use std::{env::args, process::exit};

use wedb_bench::runner::main_logic;
use wedb_bench_compare::engines::compare_specs;

fn main() {
  let specs = compare_specs();
  let argv: Vec<String> = args().skip(1).collect();
  if let Err(detail) = main_logic(&specs, &argv) {
    eprintln!("{detail}");
    exit(1);
  }
}
