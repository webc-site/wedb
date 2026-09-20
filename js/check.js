#!/usr/bin/env -S bun

import { mkdir, readdir, rm } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import yaml from "yaml";
import garnetScan from "./check/garnetScan.js";
import rustScan, { CS_REF_REGEX, csPathNormalize } from "./check/rustScan.js";
import symbolCheck from "./check/symbolCheck.js";

const BASE_DIR = resolve(import.meta.dirname, ".."),
  ROOT_DIR = process.env.ROOT_DIR || BASE_DIR,
  GARNET_DIR = join(BASE_DIR, "garnet"),
  IGNORE_DIR = join(import.meta.dirname, "check/ignore"),
  MISS_DIR_LI = [
    join(import.meta.dirname, "check/miss"),
    join(ROOT_DIR, "check/miss")
  ];

const ymlWalk = async (dir_path) => {
  const file_li = [];
  try {
    const entry_li = await readdir(dir_path, { withFileTypes: true });
    for (const entry of entry_li) {
      const { name } = entry,
        full_path = join(dir_path, name);
      if (entry.isDirectory()) {
        const sub_li = await ymlWalk(full_path);
        file_li.push(...sub_li);
      } else if (name.endsWith(".yml") || name.endsWith(".yaml")) {
        file_li.push(full_path);
      }
    }
  } catch {}
  return file_li;
};

const emptyDirClean = async (dir_path, is_root = true) => {
  try {
    const entry_li = await readdir(dir_path, { withFileTypes: true });
    for (const entry of entry_li) {
      if (entry.isDirectory()) {
        await emptyDirClean(join(dir_path, entry.name), false);
      }
    }
    if (!is_root) {
      const remain_li = await readdir(dir_path);
      if (remain_li.length === 0) {
        await rm(dir_path, { recursive: true, force: true });
      }
    }
  } catch {}
};

const missSync = async (miss_dir, active_miss_map) => {
  await mkdir(miss_dir, { recursive: true });
  const existing_file_li = await ymlWalk(miss_dir);

  for (const file_path of existing_file_li) {
    const rel_path = relative(miss_dir, file_path);
    if (!active_miss_map.has(rel_path)) {
      await rm(file_path, { force: true });
    }
  }

  for (const [rel_path, data] of active_miss_map.entries()) {
    const target_file = join(miss_dir, rel_path),
      content = yaml.stringify(data),
      target_file_obj = Bun.file(target_file);

    if (await target_file_obj.exists()) {
      const old_content = await target_file_obj.text();
      if (old_content === content) continue;
    } else {
      await mkdir(dirname(target_file), { recursive: true });
    }
    await Bun.write(target_file, content);
  }

  await emptyDirClean(miss_dir);
};

const ignoreLoadAndPrune = async (doc_file_fn_map, fn_map, test_map, ignore_dir = IGNORE_DIR) => {
  const file_ignore_map = new Map(),
    global_ignore_set = new Set(),
    yml_file_li = await ymlWalk(ignore_dir),
    // 语料健康度：解析失败 / 形状异常 / 缺省回落命中 / 实际删除，统一交由 check() 汇总
    diag = { parse_fail_li: [], bad_shape_li: [], no_reason_li: [], deleted_li: [] };

  let has_deleted_files = false;

  for (const yml_path of yml_file_li) {
    const content = await Bun.file(yml_path).text();
    let rule_li;
    try {
      rule_li = yaml.parse(content);
    } catch (err) {
      // 此处过去是静默 continue：一份非法 YAML（common.yml 曾因明文标量含 ": " 被解析成
      // mapping）会让整份语料凭空失效，凭空报出 46 项"实现缺失"并污染后续甄别。
      // 现在记录后由 check() 大声失败；本文件条目不参与判定，其余文件判定语义不变。
      diag.parse_fail_li.push({ path: yml_path, msg: err.message });
      continue;
    }

    // 自动淘汰只允许针对"内容确实为空 / 仅空白"的文件（既有语义）。
    // 原始字节非空而解析结果不是非空数组时，一律保留文件并报错：
    // 这类文件里有真实甄别成果，无声 rm 会让工作不可回滚。
    if (content.trim() === "") {
      await rm(yml_path, { force: true });
      diag.deleted_li.push({ path: yml_path, why: "内容为空/仅空白" });
      has_deleted_files = true;
      continue;
    }

    if (!Array.isArray(rule_li) || rule_li.length === 0) {
      const shape = rule_li === null || rule_li === undefined
        ? "空"
        : Array.isArray(rule_li) ? "空数组" : typeof rule_li;
      diag.bad_shape_li.push({ path: yml_path, shape });
      continue;
    }

    let file_modified = false;
    const new_rule_li = [];

    for (const rule of rule_li) {
      if (!rule || typeof rule !== "object") continue;
      // 缺省回落：无"理由"的条目仍按"无需实现"参与判定（改硬失败会让历史语料把工具变砖），
      // 但命中一律记入 diag，运行结束时汇总打印条目数与文件名并计入非零退出，
      // 让"靠默认值遮蔽"可见。实测 tip 语料 45 份文件 / 438 条条目命中 0。
      const raw_reason = rule["理由"] ?? rule.reason,
        reason = raw_reason ?? "无需实现",
        file_entry_li = rule["文件"] ?? rule.files ?? rule.file;

      if (raw_reason === undefined) diag.no_reason_li.push(yml_path);

      if (!file_entry_li) continue;

      let rule_modified = false;
      const new_file_li = [];

      const fileIgnoreHandle = (raw_cs_path, fn_li) => {
        const cs_path = csPathNormalize(raw_cs_path),
          documented_set = doc_file_fn_map?.get(cs_path);

        if (!fn_li) {
          // 整文件忽略
          const all_cs_fns = [
            ...(fn_map?.[cs_path] ?? []),
            ...(test_map?.[cs_path] ?? [])
          ];
          if (all_cs_fns.length > 0 && documented_set && documented_set.size > 0) {
            const remain_fn_li = all_cs_fns.filter((f) => !documented_set.has(f));
            if (remain_fn_li.length === 0) {
              rule_modified = true;
              return;
            } else if (remain_fn_li.length < all_cs_fns.length) {
              rule_modified = true;
              new_file_li.push({ [cs_path]: remain_fn_li.sort() });
              let set = file_ignore_map.get(cs_path);
              if (set !== true) {
                if (!set) {
                  set = new Set();
                  file_ignore_map.set(cs_path, set);
                }
                for (const fn of remain_fn_li) set.add(fn);
              }
              return;
            }
          }
          new_file_li.push(cs_path);
          file_ignore_map.set(cs_path, true);
        } else {
          // 指定函数列表忽略
          const clean_fn_li = Array.isArray(fn_li) ? fn_li : [fn_li],
            remain_fn_li = clean_fn_li.filter((f) => !documented_set?.has(f));
          if (remain_fn_li.length < clean_fn_li.length) {
            rule_modified = true;
          }
          if (remain_fn_li.length > 0) {
            new_file_li.push({ [cs_path]: remain_fn_li.sort() });
            let set = file_ignore_map.get(cs_path);
            if (set !== true) {
              if (!set) {
                set = new Set();
                file_ignore_map.set(cs_path, set);
              }
              for (const fn of remain_fn_li) set.add(fn);
            }
          }
        }
      };

      if (Array.isArray(file_entry_li)) {
        for (const entry of file_entry_li) {
          if (typeof entry === "string") {
            fileIgnoreHandle(entry, null);
          } else if (typeof entry === "object" && entry !== null) {
            for (const [p, fns] of Object.entries(entry)) {
              fileIgnoreHandle(p, fns);
            }
          }
        }
      } else if (typeof file_entry_li === "object" && file_entry_li !== null) {
        for (const [p, fns] of Object.entries(file_entry_li)) {
          fileIgnoreHandle(p, fns);
        }
      }

      if (rule_modified) {
        file_modified = true;
      }

      if (new_file_li.length > 0) {
        new_rule_li.push({
          "文件": new_file_li,
          "理由": reason
        });
      } else {
        file_modified = true;
      }
    }

    if (new_rule_li.length === 0) {
      // 既有语义：条目全部已在文档注释中登记后，整文件自动淘汰
      await rm(yml_path, { force: true });
      diag.deleted_li.push({ path: yml_path, why: "条目已全部文档化，整文件忽略自动淘汰" });
      has_deleted_files = true;
    } else if (file_modified) {
      await Bun.write(yml_path, yaml.stringify(new_rule_li));
    }
  }

  if (has_deleted_files) {
    await emptyDirClean(ignore_dir);
  }

  const root_ignore = join(import.meta.dirname, "check/ignore.yml"),
    root_file = Bun.file(root_ignore);

  if (await root_file.exists()) {
    let root_data;
    try {
      root_data = yaml.parse(await root_file.text());
    } catch (err) {
      diag.parse_fail_li.push({ path: root_ignore, msg: err.message });
      root_data = null;
    }
    if (Array.isArray(root_data)) {
      for (const item of root_data) {
        if (typeof item === "string") global_ignore_set.add(item);
      }
    } else if (typeof root_data === "object" && root_data !== null) {
      for (const key of Object.keys(root_data)) {
        global_ignore_set.add(key);
      }
    }
  }

  return [file_ignore_map, global_ignore_set, diag];
};

// 语料失效汇总：全部走 stderr + 红色，不污染 stdout 的判定输出
const corpusFailLines = (diag) => {
  const line_li = [];

  if (diag.parse_fail_li.length > 0) {
    line_li.push("# 语料失效：ignore YAML 解析失败，该文件的条目全部未被采信");
    for (const { path, msg } of diag.parse_fail_li) {
      line_li.push("  - " + path);
      line_li.push(...msg.split("\n").map((l) => "      " + l));
    }
    line_li.push("  注意：明文标量里出现 \": \" （如 理由: ... allowLeadingZeros: false ...）会被解析成嵌套映射，");
    line_li.push("  整份文件随之报错。此类值请写折叠块标量 理由: >- 或整体加引号。");
  }

  if (diag.bad_shape_li.length > 0) {
    line_li.push("# 语料失效：原始字节非空但解析结果不是非空列表（文件已保留，未自动删除）");
    for (const { path, shape } of diag.bad_shape_li) {
      line_li.push("  - " + path + " → " + shape);
    }
    line_li.push("  只有内容确实为空/仅空白的文件才会被自动淘汰；如确要淘汰请清空后重跑，");
    line_li.push("  否则按上面的形状提示改回列表。改动前可用 git diff 核对，必要时 git checkout -- <path> 回滚。");
  }

  if (diag.no_reason_li.length > 0) {
    const file_count_map = new Map();
    for (const path of diag.no_reason_li) {
      file_count_map.set(path, (file_count_map.get(path) ?? 0) + 1);
    }
    line_li.push("# 缺省回落命中 " + diag.no_reason_li.length + " 条（无\"理由\"字段，已按\"无需实现\"计入判定）");
    for (const [path, count] of [...file_count_map.entries()].sort()) {
      line_li.push("  - " + path + ": " + count + " 条");
    }
    line_li.push("  请为这些条目补显式理由（归并去向或\"按 transpile 规范不实现\"）。");
  }

  return line_li;
};

const corpusDeletedLines = (diag) =>
  diag.deleted_li.map(({ path, why }) => "# 已自动淘汰 ignore: " + path + "（" + why + "）");

// C# 源语料降级汇报：与上面的 YAML 侧 corpus_invalid 分开，因为二者处置手段不同。
// YAML 解析失败是本仓自己写坏的数据、可修且修前判定必错，故并入 corpus_invalid 硬失败；
// C# AST 断裂是已装语法包（@2h2d/tree-sitter-wasms 0.2.1，已是该包最新版）的固有
// 能力边界，且已由 garnetScan 的词法兜底补回名录——判据本身是可信的。若也并入
// corpus_invalid，则 194 > 0 恒成立，missSync 与符号断言被永久跳过，
// SKILL.md:97「直到 check.js 没有缺失的输出」这条验收口径反而彻底失效。
// 因此这里大声报（每次运行、stderr、非零判定可见），但不硬失败。
const csDegradedLines = (cs_health) => {
  const degraded_li = cs_health?.degraded_li ?? [];
  if (degraded_li.length === 0) return [];

  const recovered = degraded_li.reduce((total, item) => total + item.total_count - item.ast_count, 0),
    silent_li = degraded_li.filter((item) => item.total_count === 0),
    line_li = [
      "# C# 语料降级：" + degraded_li.length + "/" + cs_health.cs_file_count +
        " 个 .cs 的 tree-sitter AST 断裂（unsafe 指针、部分 #if 块等语法不覆盖），" +
        "已用词法兜底补回 " + recovered + " 个方法名"
    ];

  for (const item of [...degraded_li].sort((a, b) => b.error_nodes - a.error_nodes).slice(0, 5)) {
    line_li.push("  - " + item.path + "：ERROR " + item.error_nodes +
      " 处，AST 提出 " + item.ast_count + " → 兜底后 " + item.total_count + " 个");
  }

  if (silent_li.length > 0) {
    line_li.push("  兜底后仍零名录（该文件对本门禁不可见，需人工核对是否真无方法）：");
    for (const item of silent_li) line_li.push("    ! " + item.path);
  }

  line_li.push("  语法不覆盖的构造详见 js/check/README.md 第 1 节；这些文件的方法名录由" +
    "词法兜底给出，只用于补齐缺失判定，不参与 test/非 test 之外的语义推断。");
  return line_li;
};


// miss 判定唯一口径（导出供 selftest 断言防回潮）：C# 符号已文档化，当且仅当 rust 注释存在
// 指向该符号所在路径的规范锚（doc_file_fn_map，按归一路径为 key）。
// doc_set 词元面（任意注释出现过的同名裸词：叙述性提名、跨路径同名锚等）不得参与判定，
// 否则「注释顺带提名、实现却缺失」的符号被永久遮蔽，构成 miss 判定的结构性漏报死角；
// 词元面降级为提示性输出（见 tokenHintLines），供人工甄别误伤面。
const isDocumentedAnchor = (doc_file_fn_map, rel_path, name) =>
  doc_file_fn_map?.get(rel_path)?.has(name) ?? false;

// 「仅词元提及」提示层：这些符号 miss 判定为缺失，但某条 rust 注释出现过同名裸词
// （叙述性提名或跨路径锚）。词元面不再豁免判定，只提示人工甄别：
// 属叙述性误伤的走 ignore 登记或改写注释，属真缺失的按实现票处理。
const TOKEN_HINT_SAMPLE = 3,
  TOKEN_HINT_FILE_LI_MAX = 10;

const tokenHintLines = (token_only_map) => {
  const total = [...token_only_map.values()].reduce((sum, li) => sum + li.length, 0);
  if (total === 0) return [];

  const line_li = ["# 仅词元提及 " + total + " 个（不再作为已文档化判据；叙述性误伤请走 ignore 登记或改写注释）"],
    file_path_li = [...token_only_map.keys()].sort();

  for (const rel_path of file_path_li.slice(0, TOKEN_HINT_FILE_LI_MAX)) {
    const name_li = token_only_map.get(rel_path),
      sample_li = name_li.slice(0, TOKEN_HINT_SAMPLE),
      more = name_li.length - sample_li.length;
    line_li.push("  - " + rel_path + ": " + sample_li.join(", ") + (more > 0 ? " (+" + more + ")" : ""));
  }
  if (file_path_li.length > TOKEN_HINT_FILE_LI_MAX) {
    line_li.push("  - … 共 " + file_path_li.length + " 个文件");
  }
  return line_li;
};

const dupDefFind = (fn_doc_li) => {
  const cs_ref_map = new Map();

  for (const item of fn_doc_li) {
    const { doc, file, fn, fn_path, line } = item;
    if (!doc) continue;

    const seen_set = new Set();
    for (const match of doc.matchAll(CS_REF_REGEX)) {
      const [, raw_cs_path, fn_name] = match,
        cs_path = csPathNormalize(raw_cs_path),
        key = cs_path + ":" + fn_name;

      if (seen_set.has(key)) continue;
      seen_set.add(key);

      const loc_li = cs_ref_map.get(key) ?? [];
      loc_li.push({ file, fn, fn_path, line });
      cs_ref_map.set(key, loc_li);
    }
  }

  const dup_li = [];
  for (const [key, loc_li] of cs_ref_map.entries()) {
    if (loc_li.length > 1) {
      loc_li.sort((a, b) => a.file.localeCompare(b.file) || a.line - b.line);
      dup_li.push([key, loc_li]);
    }
  }

  dup_li.sort(([a], [b]) => a.localeCompare(b));
  return dup_li;
};

const dupDefFormat = (dup_li) => {
  if (dup_li.length === 0) return [];

  const line_li = ["# 重复定义"];

  for (const [cs_ref, loc_li] of dup_li) {
    line_li.push(cs_ref + ":");
    for (const loc of loc_li) {
      const fn_desc = loc.fn_path ? " (" + loc.fn_path + ")" : "";
      line_li.push("  - " + loc.file + ":" + loc.line + fn_desc);
    }
  }

  return line_li;
};

const nodeFormat = (node, indent = "") => {
  const key_li = Object.keys(node).sort(),
    line_li = [];

  for (const key of key_li) {
    const sub_node = node[key],
      is_dir = Object.keys(sub_node).length > 0;

    if (is_dir) {
      let curr_node = sub_node,
        sub_key_li = Object.keys(curr_node),
        combined_key = key;

      while (sub_key_li.length === 1 && Object.keys(curr_node[sub_key_li[0]]).length > 0) {
        combined_key += "/" + sub_key_li[0];
        curr_node = curr_node[sub_key_li[0]];
        sub_key_li = Object.keys(curr_node);
      }

      line_li.push(indent + combined_key + "/");
      line_li.push(...nodeFormat(curr_node, indent + "  "));
    } else {
      line_li.push(indent + key.replace(/\.ya?ml$/, ""));
    }
  }
  return line_li;
};

const pathTreeFormat = (path_li) => {
  const root = {};
  for (const file_path of path_li) {
    const part_li = file_path.split("/");
    let curr = root;
    for (const part of part_li) {
      curr[part] = curr[part] ?? {};
      curr = curr[part];
    }
  }
  return nodeFormat(root);
};

const check = async () => {
  const [fn_map, test_map, cs_health] = await garnetScan(GARNET_DIR),
    [doc_set, doc_file_fn_map, fn_doc_li] = await rustScan(ROOT_DIR),
    [file_ignore_map, global_ignore_set, corpus_diag] = await ignoreLoadAndPrune(doc_file_fn_map, fn_map, test_map),
    isIgnored = (rel_path, name) => {
      if (global_ignore_set.has(name)) return true;
      const ignore_entry = file_ignore_map.get(rel_path);
      if (!ignore_entry) return false;
      if (ignore_entry === true) return true;
      if (ignore_entry instanceof Set && ignore_entry.has(name)) return true;
      return false;
    },
    isDocumented = (rel_path, name) => isDocumentedAnchor(doc_file_fn_map, rel_path, name);

  const all_file_set = new Set([
    ...Object.keys(fn_map),
    ...Object.keys(test_map)
  ]),
    active_miss_map = new Map(),
    token_only_map = new Map();

  for (const rel_path of all_file_set) {
    const fn_li = fn_map[rel_path] ?? [],
      test_li = test_map[rel_path] ?? [],
      miss_judge = (name) => !isDocumented(rel_path, name) && !isIgnored(rel_path, name),
      miss_fn_li = fn_li.filter(miss_judge),
      miss_test_li = test_li.filter(miss_judge),
      token_only_li = [...miss_fn_li, ...miss_test_li].filter((name) => doc_set.has(name));

    if (token_only_li.length > 0) token_only_map.set(rel_path, token_only_li);

    if (miss_fn_li.length === 0 && miss_test_li.length === 0) continue;

    const yml_rel_path = rel_path.replace(/\.cs$/, ".yml"),
      out_data = {};
    if (miss_fn_li.length > 0) out_data.fn = miss_fn_li;
    if (miss_test_li.length > 0) out_data.test = miss_test_li;

    active_miss_map.set(yml_rel_path, out_data);
  }

  const fail_line_li = corpusFailLines(corpus_diag),
    corpus_invalid = fail_line_li.length > 0;

  // 语料失效期间不写 miss：假缺失一旦落盘，就会被后续代理当成真实缺口去"补映射/撤条目"
  if (!corpus_invalid) {
    for (const miss_dir of MISS_DIR_LI) {
      await missSync(miss_dir, active_miss_map);
    }
  }

  const dup_li = dupDefFind(fn_doc_li),
    dup_line_li = dupDefFormat(dup_li),
    miss_file_li = [...active_miss_map.keys()].sort(),
    tree_li = pathTreeFormat(miss_file_li),
    out_li = [];

  if (dup_line_li.length > 0) {
    out_li.push(dup_line_li.join("\n"));
  }
  if (tree_li.length > 0) {
    out_li.push(["# 实现缺失", ...tree_li].join("\n"));
  }

  // 条 9 机制位：映射注释符号存在性断言（升级条 6 路径存在性口径）。
  // A 层（libs/ 全路径锚点）违规硬失败防回潮；B 层为口径外存量族仅提示。
  // 语料失效期间跳过（豁免表不可信时判定无意义），与 miss 同步同纪律。
  let symbol_fail = false;
  if (!corpus_invalid) {
    const [sym_violation_li, sym_stat, sym_diag] = await symbolCheck(ROOT_DIR, GARNET_DIR, [fn_map, test_map]);

    if (sym_diag.parse_fail) {
      console.error("\x1b[31m# 语料失效：symbolignore.yml 解析失败，符号断言未采信：" + sym_diag.parse_fail + "\x1b[0m");
      symbol_fail = true;
    }

    const sym_a_li = sym_violation_li.filter((v) => v.tier === "A");
    if (sym_a_li.length > 0) {
      const line_li = ["# 虚构锚点（符号存在性断言失败，libs 族 A 层）"];
      for (const v of sym_a_li) {
        line_li.push("  [" + v.kind + "] " + v.loc + " " + v.anchor + " → " + v.hint);
      }
      out_li.push(line_li.join("\n"));
      symbol_fail = true;
    }
    if (sym_stat.tier_b > 0) {
      console.error("\x1b[33m# 符号断言 B 层提示：" + sym_stat.tier_b +
        " 处非 libs 族锚点未过词法断言（test/ 族、截断路径等，条 9 口径外存量），详见 bun js/check/symbolCheck.js\x1b[0m");
    }
  }

  if (out_li.length > 0) {
    console.log(out_li.join("\n\n"));
  }

  for (const line of corpusDeletedLines(corpus_diag)) {
    console.error("\x1b[33m" + line + "\x1b[0m");
  }

  // 词元面提示：stderr 黄色，不进退出码，不参与 miss 落盘
  for (const line of tokenHintLines(token_only_map)) {
    console.error("\x1b[33m" + line + "\x1b[0m");
  }

  // C# 源语料降级：每次运行都大声报，避免读 check.js 输出的代理把它当无损完备性证明
  for (const line of csDegradedLines(cs_health)) {
    console.error("\x1b[33m" + line + "\x1b[0m");
  }

  if (corpus_invalid || symbol_fail) {
    if (corpus_invalid) {
      console.error("\x1b[31m" + fail_line_li.join("\n") + "\x1b[0m");
      console.error("\x1b[31m  语料失效期间 miss 目录未同步，上面的判定不可作为甄别依据。\x1b[0m");
    }
    process.exit(1);
  }
};

export default check;
export { ignoreLoadAndPrune, corpusFailLines, csDegradedLines, isDocumentedAnchor };

if (import.meta.main) {
  await check();
}
