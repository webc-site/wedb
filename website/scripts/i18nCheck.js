#!/usr/bin/env node
// 多语言一致性检查：词条 parity、占位符对齐、代码引用的 key 是否真存在、有没有裸落英文的翻译。
//
//   node scripts/i18nCheck.js
//
// 以 src/lib/locales/en.js 为基准（缺失词条运行时逐条回退英文），任一硬伤 exit 1。
// CI 在 website 构建前调用：改文案、加语言、加 workload 段都在门禁里露出来，
// 而不是等上线后用户看到裸 key。

import { readFileSync, readdirSync, existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const LOCALES = join(ROOT, "src", "lib", "locales");
const SRC = join(ROOT, "src");
const BASELINE = "en";

// 只认这些命名空间的字面量，避免把 "0.0.1"、CSS 值之类当成词条 key
const NAMESPACE =
  /^(meta|nav|hero|section|pane|bars|trend|table|status|commit|foot|bench)\.[a-z0-9_.]+$/;

// 模板串拼出来的 key 族：源码里以反引号 + 前缀出现，按族核对而非逐个字面量
const DYNAMIC_FAMILIES = ["status.", "bench.row."];

// 本该各语言同形的词条（品牌名、函数名、命令名），不算「没翻译」
const COGNATE = new Set([
  "meta.title",
  "nav.github",
  "bench.row.len",
  "bench.row.retain",
  "bench.row.extract_if",
  "bench.row.pop",
  "table.utc",
]);

const errors = [];
const warnings = [];

async function loadDefault(path, label) {
  try {
    const module = await import(pathToFileURL(path).href);
    return module.default ?? module;
  } catch (error) {
    errors.push(`${label} 读取失败：${error.message}`);
    return null;
  }
}

/// CODE.js / NAME.js 是具名导出（语言码与自称数组）
async function loadNamed(path, export_name, label) {
  try {
    const module = await import(pathToFileURL(path).href);
    if (!Array.isArray(module[export_name])) {
      errors.push(`${label} 里没有数组导出 ${export_name}`);
      return null;
    }
    return module[export_name];
  } catch (error) {
    errors.push(`${label} 读取失败：${error.message}`);
    return null;
  }
}

function placeholders(text) {
  return [...String(text).matchAll(/\{(\w+)\}/g)].map((match) => match[1]).sort();
}

function walk(dir, files = []) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === "locales" || entry.name === "node_modules") continue;
      walk(path, files);
    } else if (/\.(svelte|js)$/.test(entry.name) && entry.name !== "benchData.js") {
      files.push(path);
    }
  }
  return files;
}

const code = await loadNamed(join(LOCALES, "CODE.js"), "CODE", "CODE.js");
const names = await loadNamed(join(LOCALES, "NAME.js"), "NAME", "NAME.js");
const baseline = await loadDefault(join(LOCALES, `${BASELINE}.js`), `${BASELINE}.js`);

if (code && names && baseline) {
  if (code.length !== names.length) {
    errors.push(`CODE.js 有 ${code.length} 个语言码，NAME.js 只有 ${names.length} 个名字`);
  }
  if (code[0] !== BASELINE) {
    errors.push(`CODE.js 的第一项必须是基准语言 ${BASELINE}`);
  }
  if (new Set(code).size !== code.length) errors.push("CODE.js 里有重复语言码");

  // 代码里引用到的字面量 key 与动态族
  const used = new Set();
  const families = new Set();
  for (const file of walk(SRC)) {
    const text = readFileSync(file, "utf8");
    for (const match of text.matchAll(/(["'`])([a-z0-9_]+(?:\.[a-z0-9_]+)+)\1/g)) {
      if (NAMESPACE.test(match[2])) used.add(match[2]);
    }
    for (const family of DYNAMIC_FAMILIES) {
      if (text.includes("`" + family)) families.add(family);
    }
  }

  for (const key of used) {
    if (!(key in baseline)) errors.push(`代码引用了 ${BASELINE}.js 里没有的词条：${key}`);
  }
  for (const family of families) {
    if (!Object.keys(baseline).some((key) => key.startsWith(family))) {
      errors.push(`代码按 ${family}* 动态取词条，但 ${BASELINE}.js 里一个都没有`);
    }
  }
  for (const key of Object.keys(baseline)) {
    if (used.has(key)) continue;
    if ([...families].some((family) => key.startsWith(family))) continue;
    warnings.push(`${BASELINE}.js 的 ${key} 在代码里没被引用（删掉还是接上界面？）`);
  }

  // 逐语言对账：缺 / 多 / 空值 / 占位符不一致 / 疑似没翻
  for (const [index, lang] of code.entries()) {
    if (lang === BASELINE) continue;
    const path = join(LOCALES, `${lang}.js`);
    if (!existsSync(path)) {
      errors.push(`${lang}：缺文件 src/lib/locales/${lang}.js`);
      continue;
    }
    const dict = (await loadDefault(path, `${lang}.js`)) ?? {};
    const missing = Object.keys(baseline).filter((key) => !(key in dict));
    const extra = Object.keys(dict).filter((key) => !(key in baseline));
    const empty = Object.entries(dict).filter(([, value]) => value === "" || value == null);
    const shape = Object.entries(dict).filter(
      ([key, value]) =>
        key in baseline && placeholders(value).join(",") !== placeholders(baseline[key]).join(",")
    );
    const untranslated = Object.entries(dict).filter(
      ([key, value]) => value === baseline[key] && !COGNATE.has(key)
    );

    if (missing.length) errors.push(`${lang}：缺 ${missing.length} 条 → ${missing.join(", ")}`);
    if (extra.length) errors.push(`${lang}：多 ${extra.length} 条 → ${extra.join(", ")}`);
    if (empty.length) {
      errors.push(`${lang}：空值 ${empty.length} 条 → ${empty.map(([key]) => key).join(", ")}`);
    }
    if (shape.length) {
      errors.push(
        `${lang}：占位符与 ${BASELINE} 不一致 ${shape.length} 条 → ` +
          shape.map(([key, value]) => `${key}{${placeholders(value).join(",")}}`).join(", ")
      );
    }
    if (untranslated.length) {
      warnings.push(
        `${lang}：${untranslated.length} 条与英文一字不差（可能是没翻）→ ` +
          untranslated.map(([key]) => key).slice(0, 8).join(", ")
      );
    }
    if (!names[index]) errors.push(`${lang}：NAME.js 缺该语言的自称`);
  }

  // 表格行名要覆盖机读数据里真出现过的段，否则非英文界面会混出一列英文行名
  const history_path = join(ROOT, "data", "history.json");
  if (existsSync(history_path)) {
    try {
      const history = JSON.parse(readFileSync(history_path, "utf8"));
      const keys = new Set();
      for (const run of (history.reports ?? []).flatMap((report) => report.runs ?? [])) {
        for (const engine of run.engines ?? []) {
          for (const row of engine.rows ?? []) keys.add(row.key);
        }
      }
      const uncovered = [...keys].filter((key) => !(`bench.row.${key}` in baseline));
      if (uncovered.length) {
        errors.push(`表格段没有对应词条：${uncovered.join(", ")}（在 en.js 补 bench.row.*）`);
      }
    } catch (error) {
      warnings.push(`data/history.json 解析失败，跳过表格段覆盖核对：${error.message}`);
    }
  }

  if (errors.length) {
    for (const line of errors) console.error(`✗ ${line}`);
  }
}

for (const line of warnings) console.warn(`注意：${line}`);

if (errors.length) {
  console.error(`i18n 检查未通过：${errors.length} 项。`);
  process.exit(1);
}

console.log(
  `i18n 检查通过：${code.length} 个语言 × ${Object.keys(baseline).length} 条基准词条。`
);
