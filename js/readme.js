#!/usr/bin/env node
// README 派生器。README.md 与 readme/{en,zh}/bench.md 是生成物，不允许手改：
//
//   node js/readme.js --report <report.json> --tables <tables.md> [选项]
//
// --tables 是 `benchreport merge --markdown` 的产物：表体数字全部由 Rust 侧渲染，
// 本脚本只搬运表格并补外层文字（版本标注、死列与口径备注），不重排单元格也不改数值。
// --report 提供版本与机器信息（commit / branch / 时间 / CPU / 内存 / 备注 / 列状态）。
//
// 选项：
//   --representative NAME  主文件内嵌的代表平台，缺省 linux-arm64
//   --skip-missing         代表平台本轮没数据时什么都不写（退出码 3，供 CI 跳过回写）
//   --check                只校验落盘内容与重新生成的结果是否逐字节一致
//   --verify               不带任何输入，只校验 README.md 确实是两份分章的装配产物
//   --root DIR             仓库根，缺省脚本所在目录的上一级

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");

const BEGIN = "WEDB-BENCH:BEGIN";
const END = "WEDB-BENCH:END";

// 与 Rust 侧 report::column_label 同源：JSON 里的 engines[].name 仍是内部 id
// （feature 名、--only 取值、产物名都挂它），只有落到纸面的列名换成产品名。
const COLUMN_LABEL = { hash: "wkv", bftree: "wbftree" };
const column_label = (name) => COLUMN_LABEL[name] ?? name;

const TEXT = {
  en: {
    run_line: (identity, branch, date) =>
      `> Latest run: \`${identity}\` (\`${branch}\`), ${date} UTC.`,
    machine: (platform, cpu, cores, gib) =>
      `## ${platform} — ${cpu} (${cores} logical cores / ${gib} GiB RAM)`,
    note: (text) => `- Harness note: ${text}`,
    dead: (name, status, detail) => `- Column \`${name}\`: ${status} — ${detail}`,
  },
  zh: {
    run_line: (identity, branch, date) =>
      `> 最新一轮：\`${identity}\`（\`${branch}\`），${date} UTC。`,
    machine: (platform, cpu, cores, gib) =>
      `## ${platform} — ${cpu}（${cores} 逻辑核 / ${gib} GiB 内存）`,
    note: (text) => `- 备注：${text}`,
    dead: (name, status, detail) => `- 列 \`${name}\`：${status} — ${detail}`,
  },
};

function parse_args(argv) {
  const options = {
    report: null,
    tables: null,
    representative: "linux-arm64",
    skip_missing: false,
    check: false,
    verify: false,
    root: ROOT,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    const value = () => {
      index += 1;
      if (index >= argv.length) throw new Error(`${flag} 缺少取值`);
      return argv[index];
    };
    switch (flag) {
      case "--report":
        options.report = resolve(value());
        break;
      case "--tables":
        options.tables = resolve(value());
        break;
      case "--representative":
        options.representative = value();
        break;
      case "--skip-missing":
        options.skip_missing = true;
        break;
      case "--check":
        options.check = true;
        break;
      case "--verify":
        options.verify = true;
        break;
      case "--root":
        options.root = resolve(value());
        break;
      case "--help":
      case "-h":
        console.log(
          "用法：node js/readme.js --report REPORT.json --tables TABLES.md [--representative NAME] [--skip-missing] [--check]\n" +
            "     node js/readme.js --verify"
        );
        process.exit(0);
      default:
        throw new Error(`未知参数：${flag}`);
    }
  }
  if (!options.verify && (!options.report || !options.tables)) {
    throw new Error("--report 与 --tables 都为必填（或改用 --verify）");
  }
  return options;
}

/// benchreport 的 markdown 以 `## <platform> — ...` 分平台；这里只取表体行，
/// 平台标题与备注由脚本按语言重写，避免中英两版共用一套中文小节标题
function split_tables(markdown) {
  const blocks = new Map();
  let current = null;
  for (const line of markdown.split("\n")) {
    const head = /^## (\S+) — /.exec(line);
    if (head) {
      current = [];
      blocks.set(head[1], current);
      continue;
    }
    if (current !== null && line.startsWith("|")) current.push(line);
  }
  return blocks;
}

function utc_day(unix) {
  return new Date(unix * 1000).toISOString().slice(0, 10);
}

/// 一个平台一节：机器标题 + Rust 表体 + 死列与口径备注
function render_run(run, table_lines, language) {
  const text = TEXT[language];
  const lines = [
    text.machine(
      run.platform,
      run.machine.cpu_brand,
      run.machine.logical_cores,
      run.machine.total_memory_gib.toFixed(1)
    ),
    "",
    ...table_lines,
    "",
  ];
  const dead = run.engines.filter((engine) => engine.status !== "ok");
  for (const engine of dead)
    lines.push(text.dead(column_label(engine.name), engine.status, engine.detail ?? ""));
  for (const note of run.notes) lines.push(text.note(note));
  if (dead.length > 0 || run.notes.length > 0) lines.push("");
  return lines.join("\n");
}

function table_for(tables, platform) {
  const lines = tables.get(platform);
  if (!lines || lines.length === 0) throw new Error(`${platform} 在 markdown 里没有对应表体`);
  return lines;
}

function render_chapter(context, language) {
  const intro = readFileSync(
    join(context.root, `readme/${language}/bench-intro.md`),
    "utf8"
  ).trimEnd();
  const blocks = context.list
    .map((run) => render_run(run, table_for(context.tables, run.platform), language))
    .join("\n");
  return `${intro}\n\n${blocks}`;
}

function render_fence(context, language) {
  const text = TEXT[language];
  const run = context.by_platform.get(context.representative);
  const head = text.run_line(run.version || run.commit.slice(0, 7), run.branch, utc_day(run.generated_at_unix));
  return `${head}\n\n${render_run(run, table_for(context.tables, run.platform), language)}`;
}

/// 把生成块塞进围栏之间；围栏缺失说明分章被改过，直接报错而不是悄悄补
function splice(source, block, path) {
  const begin = source.indexOf(`<!-- ${BEGIN}`);
  const end = source.indexOf(`<!-- ${END}`);
  if (begin < 0 || end < begin) throw new Error(`${path} 缺少 WEDB-BENCH 围栏`);
  const head = source.slice(0, source.indexOf("-->", begin) + 3);
  return `${head}\n${block}\n\n${source.slice(end)}`;
}

function assemble_readme(en, zh) {
  return [
    "[English](#en) | [中文](#zh)",
    "",
    "---",
    "",
    '<a name="en"></a>',
    "",
    en.trimEnd(),
    "",
    "---",
    "",
    '<a name="zh"></a>',
    "",
    zh.trimEnd(),
    "",
  ].join("\n");
}

function build(options) {
  const report = JSON.parse(readFileSync(options.report, "utf8"));
  const context = {
    root: options.root,
    tables: split_tables(readFileSync(options.tables, "utf8")),
    representative: options.representative,
    list: report.runs,
    by_platform: new Map(report.runs.map((run) => [run.platform, run])),
  };
  if (!context.by_platform.has(options.representative)) {
    if (!options.skip_missing) throw new Error(`代表平台 ${options.representative} 没有数据`);
    return { skipped: `代表平台 ${options.representative} 本轮没有结果，README 不回写` };
  }

  const targets = [];
  for (const language of ["en", "zh"]) {
    const chapter_path = `readme/${language}.md`;
    const chapter = readFileSync(join(options.root, chapter_path), "utf8");
    const spliced = splice(chapter, render_fence(context, language), chapter_path);
    targets.push({ path: chapter_path, content: spliced });
    targets.push({ path: `readme/${language}/bench.md`, content: render_chapter(context, language) });
  }
  const chapter_of = (language) =>
    targets.find((item) => item.path === `readme/${language}.md`).content;
  targets.push({ path: "README.md", content: assemble_readme(chapter_of("en"), chapter_of("zh")) });
  return targets;
}

/// 只校验装配关系：README.md 必须等于两份分章的装配产物。
/// 表格数字那一层的校验要靠当轮 report，交给 CI 的 --check（见 bench.yml）
function verify_assembly(options) {
  const readme = readFileSync(join(options.root, "README.md"), "utf8");
  const en = readFileSync(join(options.root, "readme/en.md"), "utf8");
  const zh = readFileSync(join(options.root, "readme/zh.md"), "utf8");
  if (readme === assemble_readme(en, zh)) {
    console.log("README.md 与分章装配一致");
    return;
  }
  console.error(
    "README.md 不是 readme/en.md + readme/zh.md 的装配产物：README 是生成物，" +
      "请改分章后运行 node js/readme.js --report ... --tables ... 重新生成。"
  );
  process.exit(1);
}

function main(argv) {
  const options = parse_args(argv);
  if (options.verify) {
    verify_assembly(options);
    return;
  }
  const built = build(options);
  if (built.skipped !== undefined) {
    console.log(built.skipped);
    process.exit(3);
  }
  const targets = built;
  const drifted = targets.filter(
    (item) => readFileSync(join(options.root, item.path), "utf8") !== item.content
  );
  if (options.check) {
    if (drifted.length > 0) {
      console.error(
        `README 派生物与生成结果不一致：\n${drifted.map((item) => `  - ${item.path}`).join("\n")}\n` +
          "请运行 node js/readme.js --report ... --tables ... 重新生成后再提交。"
      );
      process.exit(1);
    }
    console.log(`README 派生物一致（${targets.length} 个文件）`);
    return;
  }
  for (const item of drifted) {
    writeFileSync(join(options.root, item.path), item.content);
    console.log(`已更新 ${item.path}`);
  }
  if (drifted.length === 0) console.log("无需改动");
}

try {
  main(process.argv.slice(2));
} catch (error) {
  console.error(`readme: ${error.message}`);
  process.exit(1);
}
