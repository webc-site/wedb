#!/usr/bin/env -S bun

// qcode.design 条 9 机制位：映射注释「符号存在性」断言。
// 解析全部 rust 注释（含行内 // 与块注释）中的 `libs/….cs:符号` 锚点，
// 对被引 C# 文件做词法存在性断言（\b 边界），升级条 6 的「路径存在性」口径：
// 1) 被引文件不存在 → 路径失真；
// 2) 文件存在但符号不在文件文本中 → 符号失真；按 garnetScan 函数索引区分
//    「错挂他文件」（给出真实落点候选）与「全树不存在（虚构名）」。
// 违规由 check.js 汇总并硬失败（exit 1），防回潮。
// 叙述性假阳性（如 ReadCache.cs:DRAM、RespCommand.cs:OBJECT_ 前缀）经甄别后
// 登记 js/check/symbolignore.yml（anchor + 理由），不复用 ignore 目录——
// ignore 语料会把「已在注释中出现」的条目自动淘汰，语义不同。

import { readdir } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import yaml from "yaml";
import { CS_REF_REGEX, csPathNormalize, rsWalk } from "./rustScan.js";

const EXEMPT_FILE = join(import.meta.dirname, "symbolignore.yml");

const exemptLoad = async (exempt_file = EXEMPT_FILE) => {
  const exempt_set = new Set(),
    diag = { parse_fail: null };

  const file = Bun.file(exempt_file);
  if (!(await file.exists())) return [exempt_set, diag];

  let rule_li;
  try {
    rule_li = yaml.parse(await file.text());
  } catch (err) {
    diag.parse_fail = err.message;
    return [exempt_set, diag];
  }

  if (!Array.isArray(rule_li)) {
    diag.parse_fail = "顶层必须是列表";
    return [exempt_set, diag];
  }

  for (const rule of rule_li) {
    const anchor = rule?.["锚点"] ?? rule?.anchor;
    if (typeof anchor !== "string" || anchor === "") continue;
    const idx = anchor.lastIndexOf(":");
    if (idx > 0 && anchor.slice(0, idx).endsWith(".cs")) {
      exempt_set.add(csPathNormalize(anchor.slice(0, idx)) + ":" + anchor.slice(idx + 1));
    }
  }
  return [exempt_set, diag];
};

// 从一行文本中取注释域内容：整行注释取 // 之后；行尾注释同样取首个 // 之后；
// 块注释内容行（* 开头或含 /* ）取 /* 之后与行尾。锚点正则要求 ".cs:符号"，
// 对误把字符串里的 // 当注释无假阳性风险。
const commentText = (line) => {
  const trimmed = line.trimStart();
  if (trimmed.startsWith("*")) return trimmed.slice(1);
  const i = line.indexOf("//");
  if (i !== -1) return line.slice(i + 2);
  const j = line.indexOf("/*");
  if (j !== -1) return line.slice(j + 2);
  return "";
};

const anchorWalk = async (root_dir) => {
  const file_li = await rsWalk(root_dir),
    anchor_li = [];

  for (const file_path of file_li) {
    const text = await Bun.file(file_path).text(),
      file_rel = relative(root_dir, file_path);

    let in_block = false;
    const lines = text.split("\n");
    for (let idx = 0; idx < lines.length; idx++) {
      const line = lines[idx];
      let seg;
      if (in_block) {
        seg = line;
        if (line.includes("*/")) in_block = false;
      } else {
        seg = commentText(line);
        if (line.includes("/*") && !line.includes("*/")) in_block = true;
      }
      if (!seg.includes(".cs")) continue;
      for (const m of seg.matchAll(CS_REF_REGEX)) {
        anchor_li.push({
          file: file_rel,
          line: idx + 1,
          raw_path: m[1],
          sym: m[2]
        });
      }
    }
  }
  return anchor_li;
};

const symbolCheck = async (
  root_dir = resolve(import.meta.dirname, "../.."),
  garnet_dir = resolve(import.meta.dirname, "../../garnet"),
  pre_scan = null
) => {
  const [anchor_li, [exempt_set, exempt_diag]] = await Promise.all([
      anchorWalk(root_dir),
      exemptLoad()
    ]),
    // 反查索引：符号 → 全 garnet 树中定义该方法的文件（区分错挂/虚构用）
    def_index = new Map();

  const buildDefIndex = async () => {
    const [fn_map, test_map] =
      pre_scan ?? await (await import("./garnetScan.js")).default(garnet_dir);
    for (const [path, li] of [...Object.entries(fn_map), ...Object.entries(test_map)]) {
      for (const name of li) {
        let set = def_index.get(name);
        if (!set) def_index.set(name, (set = new Set()));
        set.add(path);
      }
    }
  };
  await buildDefIndex();

  const file_text_cache = new Map(),
    violation_li = [],
    stat = { total: 0, unique: 0, exempt_hit: 0, bare_skip: 0, tier_a: 0, tier_b: 0 };

  const loadFile = async (cs_path) => {
    if (file_text_cache.has(cs_path)) return file_text_cache.get(cs_path);
    const f = Bun.file(join(garnet_dir, cs_path));
    const text = (await f.exists()) ? await f.text() : null;
    file_text_cache.set(cs_path, text);
    return text;
  };

  const seen = new Set();
  for (const { file, line, raw_path, sym } of anchor_li) {
    stat.total++;
    const cs_path = csPathNormalize(raw_path),
      key = file + ":" + line + ":" + cs_path + ":" + sym;
    if (seen.has(key)) continue;
    seen.add(key);
    stat.unique++;

    // 条 9 口径：断言对象为层次路径锚点（libs/… 等，与条 6 路径存在性同源）；
    // 裸文件名锚点（"TsavoriteLog.cs:Enqueue"）不参与，属另一族口径待另条收编
    if (!cs_path.includes("/")) {
      stat.bare_skip++;
      continue;
    }

    const anchor = cs_path + ":" + sym;
    if (exempt_set.has(anchor)) {
      stat.exempt_hit++;
      continue;
    }

    // 分层：libs/ 全路径族（条 9 普查 3429 处口径）为 A 层硬断言，违规即 check.js 红；
    // 其余层次路径（test/ 族、截断路径、lib/ 拼写漂移等）为 B 层存量口径外族，
    // 列出但不拦截，避免把其他在飞条目（条 6 等）的地盘卷进本机制的红灯
    const tier = cs_path.startsWith("libs/") ? "A" : "B";

    const text = await loadFile(cs_path);
    if (text === null) {
      const def_set = def_index.get(sym);
      violation_li.push({
        tier,
        kind: "路径失真",
        loc: file + ":" + line,
        anchor,
        hint: def_set ? "该符号真实落点: " + [...def_set].slice(0, 3).join(", ") : "符号全树亦不存在"
      });
      continue;
    }
    if (!new RegExp("\\b" + sym + "\\b").test(text)) {
      const def_set = def_index.get(sym);
      violation_li.push({
        tier,
        kind: def_set ? "符号错挂" : "虚构符号",
        loc: file + ":" + line,
        anchor,
        hint: def_set
          ? "该符号真实落点: " + [...def_set].slice(0, 3).join(", ")
          : "全 garnet 树（含测试）不存在此符号"
      });
    }
  }

  violation_li.sort((a, b) => a.loc.localeCompare(b.loc));
  stat.tier_a = violation_li.filter((v) => v.tier === "A").length;
  stat.tier_b = violation_li.length - stat.tier_a;
  return [violation_li, stat, exempt_diag];
};

export default symbolCheck;
export { anchorWalk, commentText, exemptLoad };

if (import.meta.main) {
  const t0 = performance.now(),
    [violation_li, stat, exempt_diag] = await symbolCheck();

  if (exempt_diag.parse_fail) {
    console.error("\x1b[31m[symbolCheck] symbolignore.yml 解析失败：" + exempt_diag.parse_fail + "\x1b[0m");
    process.exit(1);
  }

  console.log(
    "[symbolCheck] 耗时 " + (performance.now() - t0).toFixed(1) + "ms，锚点命中 " +
      stat.total + " 处（去重 " + stat.unique + "），豁免 " + stat.exempt_hit +
      "，裸文件名跳过 " + stat.bare_skip + "，违规 " + violation_li.length +
      "（A 层 libs 族 " + stat.tier_a + "，B 层口径外 " + stat.tier_b + "）"
  );
  for (const v of violation_li) {
    console.log("  [" + v.tier + " " + v.kind + "] " + v.loc + " " + v.anchor + " → " + v.hint);
  }
  if (stat.tier_a > 0) process.exit(1);
}
