#!/usr/bin/env -S bun
// js/check.js 语料读取路径的自检：非法 YAML 必须被报出、非空语料不得被无声删除、
// 缺省回落必须可见。fixture 全部落在临时目录，不触碰 js/check/ignore/ 真实语料。
//
// 用法：bun ./js/check_selftest.js

import { mkdir, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative, resolve } from "node:path";
import yaml from "yaml";
import { ignoreLoadAndPrune, corpusFailLines } from "./check.js";

const IGNORE_DIR = resolve(import.meta.dirname, "check/ignore"),
  FIXTURE_DIR = join(tmpdir(), "wedb-check-selftest-" + process.pid);

let fail_count = 0,
  case_count = 0;

const expect = (name, cond, detail = "") => {
  case_count++;
  if (cond) {
    console.log("  ok   " + name);
  } else {
    fail_count++;
    console.log("  FAIL " + name + (detail ? "\n         → " + detail : ""));
  }
};

const exists = async (path) => await Bun.file(path).exists();

const writeFixture = async (name, content) => {
  const path = join(FIXTURE_DIR, name);
  await mkdir(join(FIXTURE_DIR, join(name, "..")), { recursive: true });
  await writeFile(path, content);
  return path;
};

// 复刻事故形状：明文标量里含 ": "，整份 YAML 解析失败
const BAD_YML = "- 文件:\n    - libs/common/AccidentShape.cs\n  理由: 需要保持 allowLeadingZeros: false 行为\n",
  GOOD_YML = "- 文件:\n    - libs/common/Healthy.cs\n  理由: 自检 fixture，健康条目\n",
  MAPPING_YML = "文件:\n  - libs/common/Bar.cs\n理由: 顶层是 mapping 而非 list\n",
  EMPTY_LIST_YML = "[]\n",
  BLANK_YML = "\n   \n",
  NO_REASON_YML = "- 文件:\n    - libs/common/NoReason.cs\n",
  NESTED_BAD_YML = "- 文件:\n    - libs/common/NestedDir.cs\n  理由: 同样非法 nested: true 的写法\n";

const run = async () => {
  await mkdir(FIXTURE_DIR, { recursive: true });

  const bad_path = await writeFixture("bad.yml", BAD_YML),
    good_path = await writeFixture("good.yml", GOOD_YML),
    mapping_path = await writeFixture("mapping.yml", MAPPING_YML),
    empty_list_path = await writeFixture("emptylist.yml", EMPTY_LIST_YML),
    blank_path = await writeFixture("blank.yml", BLANK_YML),
    no_reason_path = await writeFixture("noreason.yml", NO_REASON_YML),
    nested_bad_path = await writeFixture("sub/bad.yml", NESTED_BAD_YML);

  // 空 fn_map / 空文档集：不触发既有剪枝，只考察语料读取本身
  const [file_ignore_map, , diag] = await ignoreLoadAndPrune(
    new Map(),
    {},
    {},
    FIXTURE_DIR
  );

  const fail_files = diag.parse_fail_li.map(({ path }) => path).sort(),
    bad_shape_files = diag.bad_shape_li.map(({ path }) => path).sort(),
    report = corpusFailLines(diag).join("\n"),
    rel = (p) => relative(FIXTURE_DIR, p);

  console.log("# 1 非法 YAML 必须被报出（旧行为：静默 continue）");
  expect(
    "两份非法文件均进入语料失效清单",
    JSON.stringify(fail_files.map(rel)) === JSON.stringify(["bad.yml", "sub/bad.yml"]),
    JSON.stringify(fail_files.map(rel))
  );
  expect(
    "报出内容带 YAML 错误信息",
    diag.parse_fail_li.every(({ msg }) => msg.includes("Nested mappings")),
    diag.parse_fail_li.map(({ msg }) => msg.split("\n")[0]).join(" | ")
  );
  expect(
    "汇总输出含文件路径与语料失效标题",
    report.includes("bad.yml") && report.includes("sub/bad.yml") && report.includes("语料失效"),
    report
  );

  console.log("# 2 非法语料不影响其它文件判定");
  expect(
    "同目录合法 yml 的条目仍被采信",
    file_ignore_map.get("libs/common/Healthy.cs") === true,
    [...file_ignore_map.keys()].join(",")
  );
  expect("非法 yml 的条目未被采信", !file_ignore_map.has("libs/common/AccidentShape.cs"));
  expect("fixture 文件均未被删除", await Promise.all(
    [bad_path, nested_bad_path, good_path, no_reason_path].map(exists)
  ).then((li) => li.every(Boolean)));

  console.log("# 3 破坏性自动删除收紧");
  expect(
    "解析成 mapping 的非空语料被保留并报错",
    await exists(mapping_path) && bad_shape_files.map(rel).includes("mapping.yml"),
    "exists=" + (await exists(mapping_path)) + " bad_shape=" + JSON.stringify(bad_shape_files.map(rel))
  );
  expect(
    "解析成空数组的非空语料被保留并报错",
    await exists(empty_list_path) && bad_shape_files.map(rel).includes("emptylist.yml"),
    "exists=" + (await exists(empty_list_path))
  );
  expect(
    "仅空白内容的文件仍按既有语义自动淘汰",
    !(await exists(blank_path)) && diag.deleted_li.some(({ path }) => rel(path) === "blank.yml"),
    "deleted=" + JSON.stringify(diag.deleted_li.map(({ path }) => rel(path)))
  );
  expect(
    "汇总输出提示保留与回滚",
    report.includes("文件已保留") && report.includes("git checkout"),
    report
  );

  console.log("# 4 理由缺省回落可见");
  expect(
    "无\"理由\"条目命中计数为 1",
    diag.no_reason_li.length === 1 && rel(diag.no_reason_li[0]) === "noreason.yml",
    JSON.stringify(diag.no_reason_li.map(rel))
  );
  expect(
    "缺省回落仍参与判定（不改变算法）",
    file_ignore_map.get("libs/common/NoReason.cs") === true
  );
  expect(
    "汇总输出含命中条目数与文件名",
    report.includes("缺省回落命中 1 条") && report.includes("noreason.yml"),
    report
  );

  await rm(FIXTURE_DIR, { recursive: true, force: true });

  console.log("# 5 真实语料只读取证（不写不删）");
  const walk = async (dir_path) => {
    const li = [];
    for (const entry of await readdir(dir_path, { withFileTypes: true })) {
      const p = join(dir_path, entry.name);
      if (entry.isDirectory()) li.push(...await walk(p));
      else if (/\.ya?ml$/.test(entry.name)) li.push(p);
    }
    return li;
  };
  const real_li = await walk(IGNORE_DIR);
  let parse_fail = 0, bad_shape = 0, no_reason = 0, rule_total = 0;
  for (const p of real_li) {
    let d;
    try {
      d = yaml.parse(await Bun.file(p).text());
    } catch {
      parse_fail++;
      continue;
    }
    if (!Array.isArray(d) || d.length === 0) {
      bad_shape++;
      continue;
    }
    for (const r of d) {
      if (!r || typeof r !== "object") continue;
      rule_total++;
      if (r["理由"] === undefined && r.reason === undefined) no_reason++;
    }
  }
  console.log(
    "  语料 " + real_li.length + " 份 / 条目 " + rule_total + " 条：解析失败 " + parse_fail +
    "、形状异常 " + bad_shape + "、缺理由 " + no_reason
  );
  expect("真实语料无解析失败", parse_fail === 0, parse_fail + " 份");
  expect("真实语料无形状异常", bad_shape === 0, bad_shape + " 份");
  expect("真实语料缺理由命中为 0（据此可判为错误）", no_reason === 0, no_reason + " 条");
};

await run().catch(async (err) => {
  console.error(err);
  fail_count++;
  await rm(FIXTURE_DIR, { recursive: true, force: true });
});

console.log(fail_count === 0
  ? "\n自检通过：" + case_count + " 项断言"
  : "\n自检失败：" + fail_count + "/" + case_count + " 项断言未过");
process.exit(fail_count === 0 ? 0 : 1);
