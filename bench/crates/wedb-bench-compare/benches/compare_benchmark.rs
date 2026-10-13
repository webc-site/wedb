//! 五列同表的对比评测入口：`cargo bench -p wedb-bench-compare -- --quick`。

use std::{env::args, process::exit, thread::Builder};

use wedb_bench::runner::main_logic;
use wedb_bench_compare::engines::compare_specs;

fn main() {
  // 被测列的整棵跑测搬到显式大栈线程：bf-tree 的 ScanIter::next 在「跳过已删记录」
  // 和「叶尾换叶」两支都是自递归（bf-tree-0.5.6 range_scan.rs:157/218），递归深度随
  // 单次扫描走过的记录数线性增长，默认 8 MiB 主线程栈在百万键档（装载后有半数墓碑）
  // 必然打爆，整列凭空调 N/A。抬栈深只改可运行的深度上限，不改被测语义：
  // 同一进程、同一分配器、同一线程数、同一段计时
  let worker = Builder::new()
    .stack_size(1 << 28)
    .spawn(|| {
      let specs = compare_specs();
      let argv: Vec<String> = args().skip(1).collect();
      if let Err(detail) = main_logic(&specs, &argv) {
        eprintln!("{detail}");
        exit(1);
      }
    })
    .expect("为评测线程分配大栈失败");
  if worker.join().is_err() {
    // 工作线程 panic 或被信号打死：如实以非零码退出，让 runner 把该列记成 crashed
    exit(101);
  }
}
