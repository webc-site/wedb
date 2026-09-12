#!/usr/bin/env -S bun

import { mkdir, readdir, rm } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import yaml from "yaml";
import garnetScan from "./check/garnetScan.js";
import rustScan, { CS_REF_REGEX, csPathNormalize } from "./check/rustScan.js";

const ROOT_DIR = resolve(import.meta.dirname, ".."),
  GARNET_DIR = join(ROOT_DIR, "garnet"),
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

const ignoreLoadAndPrune = async (doc_file_fn_map, fn_map, test_map) => {
  const file_ignore_map = new Map(),
    global_ignore_set = new Set(),
    yml_file_li = await ymlWalk(IGNORE_DIR);

  let has_deleted_files = false;

  for (const yml_path of yml_file_li) {
    const content = await Bun.file(yml_path).text();
    let rule_li;
    try {
      rule_li = yaml.parse(content);
    } catch {
      continue;
    }

    if (!rule_li || !Array.isArray(rule_li) || rule_li.length === 0) {
      await rm(yml_path, { force: true });
      has_deleted_files = true;
      continue;
    }

    let file_modified = false;
    const new_rule_li = [];

    for (const rule of rule_li) {
      if (!rule || typeof rule !== "object") continue;
      const reason = rule["理由"] ?? rule.reason ?? "无需实现",
        file_entry_li = rule["文件"] ?? rule.files ?? rule.file;

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
      await rm(yml_path, { force: true });
      has_deleted_files = true;
    } else if (file_modified) {
      await Bun.write(yml_path, yaml.stringify(new_rule_li));
    }
  }

  if (has_deleted_files) {
    await emptyDirClean(IGNORE_DIR);
  }

  const root_ignore = join(import.meta.dirname, "check/ignore.yml"),
    root_file = Bun.file(root_ignore);

  if (await root_file.exists()) {
    const root_data = yaml.parse(await root_file.text());
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

  return [file_ignore_map, global_ignore_set];
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
  const [fn_map, test_map] = await garnetScan(GARNET_DIR),
    [doc_set, doc_file_fn_map, fn_doc_li] = await rustScan(ROOT_DIR),
    [file_ignore_map, global_ignore_set] = await ignoreLoadAndPrune(doc_file_fn_map, fn_map, test_map),
    isIgnored = (rel_path, name) => {
      if (global_ignore_set.has(name)) return true;
      const ignore_entry = file_ignore_map.get(rel_path);
      if (!ignore_entry) return false;
      if (ignore_entry === true) return true;
      if (ignore_entry instanceof Set && ignore_entry.has(name)) return true;
      return false;
    },
    isDocumented = (rel_path, name) => {
      if (doc_file_fn_map?.get(rel_path)?.has(name)) return true;
      if (doc_set.has(name)) return true;
      return false;
    };

  const all_file_set = new Set([
    ...Object.keys(fn_map),
    ...Object.keys(test_map)
  ]),
    active_miss_map = new Map();

  for (const rel_path of all_file_set) {
    const fn_li = fn_map[rel_path] ?? [],
      test_li = test_map[rel_path] ?? [],
      miss_fn_li = fn_li.filter((name) => !isDocumented(rel_path, name) && !isIgnored(rel_path, name)),
      miss_test_li = test_li.filter((name) => !isDocumented(rel_path, name) && !isIgnored(rel_path, name));

    if (miss_fn_li.length === 0 && miss_test_li.length === 0) continue;

    const yml_rel_path = rel_path.replace(/\.cs$/, ".yml"),
      out_data = {};
    if (miss_fn_li.length > 0) out_data.fn = miss_fn_li;
    if (miss_test_li.length > 0) out_data.test = miss_test_li;

    active_miss_map.set(yml_rel_path, out_data);
  }

  for (const miss_dir of MISS_DIR_LI) {
    await missSync(miss_dir, active_miss_map);
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

  if (out_li.length > 0) {
    console.log(out_li.join("\n\n"));
  }
};

export default check;

if (import.meta.main) {
  await check();
}
