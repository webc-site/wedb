#!/usr/bin/env -S bun

import { mkdir, readdir, rm } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import yaml from "yaml";
import garnetScan from "./check/garnetScan.js";
import rustScan from "./check/rustScan.js";

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

const ignoreLoadAndPrune = async (doc_file_fn_map) => {
  const file_ignore_map = new Map(),
    global_ignore_set = new Set(),
    yml_file_li = await ymlWalk(IGNORE_DIR);

  let has_deleted_files = false;

  for (const yml_path of yml_file_li) {
    const content = await Bun.file(yml_path).text();
    let data;
    try {
      data = yaml.parse(content);
    } catch {
      continue;
    }

    if (!data) {
      await rm(yml_path, { force: true });
      has_deleted_files = true;
      continue;
    }

    const rel_path = relative(IGNORE_DIR, yml_path),
      cs_path = rel_path.replace(/\.cs\.ya?ml$/, ".cs").replace(/\.ya?ml$/, ".cs"),
      documented_set = doc_file_fn_map?.get(cs_path);

    // 标准化为字典结构：{ [函数名]: 为什么无需实现 }
    const dict = {};
    if (Array.isArray(data)) {
      for (const item of data) {
        if (typeof item === "string") dict[item] = "无需实现";
      }
    } else if (typeof data === "object") {
      if (Array.isArray(data.fn)) {
        for (const item of data.fn) {
          if (typeof item === "string") dict[item] = "无需实现";
        }
      }
      if (Array.isArray(data.test)) {
        for (const item of data.test) {
          if (typeof item === "string") dict[item] = "测试函数无需实现";
        }
      }

      for (const [key, val] of Object.entries(data)) {
        if (key === "fn" || key === "test") continue;
        if (typeof val === "string") {
          dict[key] = val;
        } else if (Array.isArray(val)) {
          for (const item of val) {
            if (typeof item === "string") dict[item] = "无需实现";
          }
        } else if (val === null || val === undefined) {
          dict[key] = "无需实现";
        }
      }
    }

    // 如已经实现并有文档注释，自动从 ignore 中剔除
    let modified = false;
    for (const fn_name of Object.keys(dict)) {
      if (documented_set?.has(fn_name)) {
        delete dict[fn_name];
        modified = true;
      }
    }

    const remaining_keys = Object.keys(dict);
    if (remaining_keys.length === 0) {
      await rm(yml_path, { force: true });
      has_deleted_files = true;
    } else {
      if (modified || Array.isArray(data) || data.fn || data.test) {
        await Bun.write(yml_path, yaml.stringify(dict));
      }
      file_ignore_map.set(cs_path, new Set(remaining_keys));
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

const check = async () => {
  const [fn_map, test_map] = await garnetScan(GARNET_DIR),
    [doc_set, doc_file_fn_map] = await rustScan(ROOT_DIR),
    [file_ignore_map, global_ignore_set] = await ignoreLoadAndPrune(doc_file_fn_map),
    isIgnored = (rel_path, name) => {
      if (global_ignore_set.has(name)) return true;
      const file_set = file_ignore_map.get(rel_path);
      if (file_set?.has(name)) return true;
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

  const miss_file_li = [...active_miss_map.keys()];
  miss_file_li.sort();

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

    return nodeFormat(root);
  };

  const tree_li = pathTreeFormat(miss_file_li);
  if (tree_li.length > 0) {
    console.log(tree_li.join("\n"));
  }
};

export default check;

if (import.meta.main) {
  await check();
}
