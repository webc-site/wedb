#!/usr/bin/env -S bun
import { $ } from "zx";
import { which } from "bun";
import { dirname } from "node:path";

$.verbose = 1;

const REPO_ROOT = dirname(import.meta.dirname),
  PROJECT_LI = ["wedb", "bench", "regress"],
  precommit = async () => {
    process.chdir(REPO_ROOT);
    // fmt 前快照：fixrs/cargo fmt 执行前已脏的文件，其工作区脏内容含作者未暂存编辑，
    // 之后不得整文件回添（否则未暂存 hunk 被静默提级入库）。
    const pre = new Set(
      (await $`git diff --name-only`).stdout.trim().split("\n").filter(Boolean),
    );
    if (!which("fixrs")) {
      console.log("未检测到 fixrs，正在自动全局安装...");
      await $`cargo install fixrs`;
    }
    await $`fixrs`;
    for (const project of PROJECT_LI) {
      await $`cargo fmt --manifest-path ${project}/Cargo.toml --all`;
    }
    await $`codegraph sync`;
    // 只回添「已暂存且被本次 fixrs/fmt 净改动」的文件：worktreeDirty 相对 pre 的新增脏 ∩ staged。
    // 用全部已暂存文件会在多代理共享暂存区的并发窗口卷入他人文件；用全部工作区改动会卷入未暂存内容；
    // pre 已脏且 staged 的文件含作者未暂存编辑，跳过回添并留痕，由作者自行拆笔。
    const staged = new Set(
        (await $`git diff --cached --name-only`).stdout.trim().split("\n").filter(Boolean),
      ),
      worktreeDirty = (await $`git diff --name-only`).stdout.trim().split("\n").filter(Boolean),
      toReadd = worktreeDirty.filter((f) => staged.has(f) && !pre.has(f));
    for (const f of worktreeDirty.filter((f) => staged.has(f) && pre.has(f))) {
      console.log(
        "[pre-commit] 跳过回添 " + f + "：fmt 前工作区已脏（含未暂存编辑），请自行拆笔后分别暂存提交。",
      );
    }
    // fmt 序列完成后落内容基线，git add -u 前对回添候选逐文件末验一次哈希，漂移即排除（TOCTOU 封窗）
    const fileHash = async (f) => {
        const file = Bun.file(f);
        return (await file.exists()) ? Bun.hash(await file.bytes()) : null;
      },
      baseline = new Map();
    for (const f of toReadd) {
      baseline.set(f, await fileHash(f));
    }
    const verified = [];
    for (const f of toReadd) {
      const curHash = await fileHash(f),
        baseHash = baseline.get(f);
      if (curHash !== null && curHash === baseHash) {
        verified.push(f);
      } else {
        console.log(
          "[pre-commit] 跳过回添 " + f + "：fmt 后内容发生漂移（疑似并发编辑），请自行拆笔后分别暂存提交。",
        );
      }
    }
    if (verified.length > 0) {
      await $`git add -u ${verified}`;
    }
  };

await precommit();
