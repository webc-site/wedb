#!/usr/bin/env -S bun

import { readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { execSync } from "node:child_process";
import yaml from "yaml";

const ROOT_DIR = resolve(import.meta.dirname, "..");
const GARNET_DIR = join(ROOT_DIR, "garnet");
const FIXED_YML_PATH = join(ROOT_DIR, "fixed.yml");

// 1. 读取 fixed.yml 中的记录
async function loadFixedCommits() {
  const fixedMap = new Map();
  try {
    const content = await readFile(FIXED_YML_PATH, "utf-8");
    const list = yaml.parse(content) || [];
    for (const item of list) {
      if (item && item.commit) {
        const hash = String(item.commit).trim().toLowerCase();
        fixedMap.set(hash, item);
      }
    }
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.error(`加载 fixed.yml 失败: ${err.message}`);
    }
  }
  return fixedMap;
}

// 2. 检查是否为无关提交（文档、网站、CI脚本、依赖自动升级）
function isIgnoredCommit(subject, files) {
  const s = subject.toLowerCase();
  if (
    s.startsWith("build(deps") ||
    s.startsWith("bump ") ||
    s.includes("dependabot") ||
    s.startsWith("docs:") ||
    s.startsWith("doc:") ||
    s.includes("readme") ||
    s.startsWith("merge:") ||
    s.startsWith("merge branch") ||
    s.startsWith("merge pull request")
  ) {
    return true;
  }

  // 若改动文件全部在非核心目录
  if (files.length > 0) {
    const isAllIgnored = files.every((f) => {
      const lf = f.toLowerCase();
      return (
        lf.endsWith(".md") ||
        lf.startsWith("website/") ||
        lf.startsWith(".github/") ||
        lf.startsWith("docs/") ||
        lf.startsWith(".git") ||
        lf.includes("license") ||
        lf.endsWith(".png") ||
        lf.endsWith(".svg")
      );
    });
    if (isAllIgnored) return true;
  }

  return false;
}

// 3. 判断是否为 Bug 修复相关
function isBugFix(subject) {
  const s = subject.toLowerCase();
  const keywords = [
    "fix", "bug", "crash", "overflow", "leak", "race", "hang",
    "deadlock", "error", "exception", "fault", "invalid", "issue",
    "correct", "prevent", "missing", "bounds", "assert"
  ];
  return keywords.some((k) => s.includes(k));
}

// 4. 判断是否涉及核心协议与存储
function isCoreGarnet(files) {
  return files.some((f) => {
    return (
      f.startsWith("libs/server/") ||
      f.startsWith("libs/storage/") ||
      f.startsWith("libs/cluster/") ||
      f.startsWith("libs/common/")
    );
  });
}

// 5. 从 git log 获取提交历史
function getGarnetCommits(maxCount = 2000) {
  let raw = "";
  try {
    raw = execSync(
      `git -C "${GARNET_DIR}" log --name-only --pretty=format:"COMMIT:%H|%h|%as|%s" -n ${maxCount}`,
      { maxBuffer: 32 * 1024 * 1024, encoding: "utf-8" }
    );
  } catch (e) {
    console.error(`无法获取 garnet git 日志: ${e.message}`);
    return [];
  }

  const lines = raw.split("\n");
  const commits = [];
  let current = null;

  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.startsWith("COMMIT:")) {
      if (current) {
        commits.push(current);
      }
      const parts = trimmed.substring(7).split("|");
      current = {
        hash: parts[0],
        shortHash: parts[1],
        date: parts[2],
        subject: parts.slice(3).join("|"),
        files: [],
      };
    } else if (trimmed && current) {
      current.files.push(trimmed);
    }
  }
  if (current) {
    commits.push(current);
  }
  return commits;
}

async function main() {
  const args = process.argv.slice(2);
  const showAll = args.includes("--all");
  const onlyBugfix = args.includes("--bugfix") || !showAll;
  const limitArg = args.find((a, i) => args[i - 1] === "--limit");
  const limit = limitArg ? parseInt(limitArg, 10) : 30;

  const fixedMap = await loadFixedCommits();
  const allCommits = getGarnetCommits(2000);

  const pendingCommits = [];
  let ignoredCount = 0;
  let fixedCount = 0;

  for (const c of allCommits) {
    // 检查 fixed.yml 中是否已包含
    const isFixed =
      fixedMap.has(c.shortHash.toLowerCase()) ||
      fixedMap.has(c.hash.toLowerCase()) ||
      Array.from(fixedMap.keys()).some((k) => c.hash.startsWith(k));

    if (isFixed) {
      fixedCount++;
      continue;
    }

    if (isIgnoredCommit(c.subject, c.files)) {
      ignoredCount++;
      continue;
    }

    const bugfix = isBugFix(c.subject);
    const core = isCoreGarnet(c.files);

    if (onlyBugfix && !bugfix && !core) {
      continue;
    }

    pendingCommits.push({
      ...c,
      bugfix,
      core,
    });
  }

  console.log("==================================================");
  console.log("       Garnet 提交对标与修复检查 (wedb)           ");
  console.log("==================================================");
  console.log(`Garnet 提交总数:       ${allCommits.length}`);
  console.log(`已在 fixed.yml 记录:   ${fixedCount}`);
  console.log(`忽略提交 (文档/CI等):  ${ignoredCount}`);
  console.log(`待核验/待对标提交:     ${pendingCommits.length}`);
  console.log("==================================================\n");

  const displayList = pendingCommits.slice(0, limit);

  if (displayList.length === 0) {
    console.log("🎉 没有发现待对标的提交！所有关键提交均已在 fixed.yml 覆盖。");
    return;
  }

  console.log(`候选修订提交清单 (显示前 ${displayList.length} 条):`);
  for (let i = 0; i < displayList.length; i++) {
    const item = displayList[i];
    const tag = item.bugfix ? "[BUGFIX]" : item.core ? "[CORE]  " : "[FEAT]  ";
    console.log(`${(i + 1).toString().padStart(2, " ")}. ${tag} ${item.shortHash} (${item.date})`);
    console.log(`    标题: ${item.subject}`);
    const coreFiles = item.files
      .filter((f) => f.endsWith(".cs"))
      .slice(0, 3)
      .join(", ");
    if (coreFiles) {
      console.log(`    源文件: ${coreFiles}${item.files.length > 3 ? ` ...等 ${item.files.length} 个文件` : ""}`);
    }
  }

  console.log("\n💡 使用方法:");
  console.log("  bun js/check_commits.js             # 查看待核验的 Bugfix & Core 提交");
  console.log("  bun js/check_commits.js --all       # 查看所有待对标提交");
  console.log("  bun js/check_commits.js --limit 50  # 调整显示条数");
}

main().catch(console.error);
