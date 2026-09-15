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
    await $`git add -u`;
  };

await precommit();
