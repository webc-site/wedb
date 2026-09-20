#!/usr/bin/env -S bun
import { $ } from "zx";
import { which } from "bun";
import { dirname } from "node:path";

$.verbose = 1;

const REPO_ROOT = dirname(import.meta.dirname),
  PROJECT_LI = ["wedb", "bench", "regress"],
  precommit = async () => {
    process.chdir(REPO_ROOT);
    if (!which("fixrs")) {
      console.log("未检测到 fixrs，正在自动全局安装...");
      await $`cargo install fixrs`;
    }
    await $`fixrs`;
    for (const project of PROJECT_LI) {
      await $`cargo fmt --manifest-path ${project}/Cargo.toml --all`;
    }
    await $`codegraph sync`;
    // 只回添「已暂存且被 fixrs/fmt 改动」的文件：两者交集恰为本提交需要的格式化回添。
    // 用全部已暂存文件会在多代理共享暂存区的并发窗口卷入他人文件；用全部工作区改动会卷入未暂存内容。
    const staged = new Set(
      (await $`git diff --cached --name-only`).stdout.trim().split("\n").filter(Boolean),
    );
    const worktreeDirty = (await $`git diff --name-only`).stdout.trim().split("\n").filter(Boolean);
    const toReadd = worktreeDirty.filter((f) => staged.has(f));
    if (toReadd.length > 0) {
      await $`git add -u ${toReadd}`;
    }
  };

await precommit();
