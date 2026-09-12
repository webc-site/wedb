#!/usr/bin/env -S bun
import { $ } from "@3-/zx";
import { dirname } from "node:path";

$.verbose = 1;

const REPO_ROOT = dirname(import.meta.dirname),
  PROJECT_LI = ["wedb", "bench", "regress"],
  precommit = async () => {
    process.chdir(REPO_ROOT);
    for (const project of PROJECT_LI) {
      await $`cargo fmt --manifest-path ${project}/Cargo.toml --all`;
    }
    await $`codegraph sync`;
    await $`git add -u`;
  };

await precommit();
