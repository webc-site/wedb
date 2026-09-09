#!/usr/bin/env bun
import { $, which } from "bun";
import { resolve } from "node:path";

const REPO_ROOT = resolve(import.meta.dirname, "..");

const precommit = async () => {
  $.verbose = 1;
  $.cwd = REPO_ROOT;
  if (!which("moon")) await $`bun i -g @moonrepo/cli`.nothrow();
  const head_ok = (await $`git rev-parse --verify HEAD`.quiet().nothrow()).exitCode === 0;
  if (head_ok) await $`moon run :clippy --affected`.nothrow();
  if (which("codegraph")) await $`codegraph sync`.nothrow();
  await $`git add -u`.nothrow();
};

export default await precommit();
