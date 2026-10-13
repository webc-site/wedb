#!/usr/bin/env -S bun

import "zx/globals";
import { mkdir, rm } from "node:fs/promises";

$.verbose = false;

cd(import.meta.dirname);

process.env.CARGO_TARGET_DIR = "/tmp/wedb_bench_target";

const DATA_DIR = "regress/data",
  arg = process.argv[2];

await mkdir(DATA_DIR, { recursive: true });

if (arg === "--clean") {
  console.log("==> 清空回归历史记录...");
  await rm(DATA_DIR + "/history.json", { force: true });
  await rm(DATA_DIR + "/latest.json", { force: true });
  console.log("已清理历史基线数据。");
  process.exit(0);
}

if (arg === "--help" || arg === "-h") {
  console.log(`用法: ./regress.js [选项]

选项:
  (无参数)      一键运行快速回归测试，输出最近 5 次提交的吞吐与延迟变化对比
  --bench, -b   运行 Criterion 全量统计学基准测试 (详尽采样)
  --clean       清空已有历史记录，从当前提交开始重新建立基线
  --help, -h    显示本帮助信息

配置说明:
  配置文件: regress/config.json
  历史数据: regress/data/history.json
  报告生成: regress/README.md`);
  process.exit(0);
}

if (arg === "--bench" || arg === "-b") {
  await within(async () => {
    cd("regress");
    await $({ stdio: "inherit" })`cargo bench`;
  });
  process.exit(0);
}

await within(async () => {
  cd("regress");
  await $({ quiet: true })`cargo run --release -q --bin regress-run`;
});

const res = await $({ stdio: "inherit" })`bun regress/report.js`.nothrow();
if (res.exitCode !== 0) {
  process.exit(res.exitCode);
}
