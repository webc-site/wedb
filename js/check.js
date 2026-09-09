#!/usr/bin/env -S bun

import { mkdir, readdir, rm } from "node:fs/promises";
import { basename, dirname, join, relative, resolve } from "node:path";
import yaml from "yaml";
import garnetScan from "./check/garnetScan.js";
import rustScan from "./check/rustScan.js";

const ROOT_DIR = resolve(import.meta.dirname, ".."),
  GARNET_DIR = join(ROOT_DIR, "garnet"),
  IGNORE_DIR = join(import.meta.dirname, "check/ignore"),
  MISS_DIR = join(ROOT_DIR, "check/miss");

const walkYml = async (dir_path) => {
  const file_li = [];
  try {
    const entry_li = await readdir(dir_path, { withFileTypes: true });
    for (const entry of entry_li) {
      const full_path = join(dir_path, entry.name);
      if (entry.isDirectory()) {
        const sub_li = await walkYml(full_path);
        file_li.push(...sub_li);
      } else if (entry.name.endsWith(".yml") || entry.name.endsWith(".yaml")) {
        file_li.push(full_path);
      }
    }
  } catch {}
  return file_li;
};

const ignoreLoad = async () => {
  const file_ignore_map = new Map(),
    global_ignore_set = new Set(),
    yml_file_li = await walkYml(IGNORE_DIR);

  for (const yml_path of yml_file_li) {
    const content = await Bun.file(yml_path).text(),
      data = yaml.parse(content);

    if (!data) continue;

    const rel_path = relative(IGNORE_DIR, yml_path),
      cs_path = rel_path.replace(/\.cs\.ya?ml$/, ".cs").replace(/\.ya?ml$/, ".cs"),
      fn_set = new Set();

    if (Array.isArray(data)) {
      for (const item of data) {
        if (typeof item === "string") fn_set.add(item);
      }
    } else if (typeof data === "object") {
      if (Array.isArray(data.fn)) {
        for (const item of data.fn) {
          if (typeof item === "string") fn_set.add(item);
        }
      }
      if (Array.isArray(data.test)) {
        for (const item of data.test) {
          if (typeof item === "string") fn_set.add(item);
        }
      }

      for (const [key, val] of Object.entries(data)) {
        if (key === "fn" || key === "test") continue;
        if (Array.isArray(val)) {
          for (const item of val) {
            if (typeof item === "string") fn_set.add(item);
          }
        } else if (typeof val === "string") {
          fn_set.add(val);
        }
      }
    }

    file_ignore_map.set(cs_path, fn_set);
  }

  const root_ignore = join(import.meta.dirname, "check/ignore.yml"),
    root_file = Bun.file(root_ignore);

  if (await root_file.exists()) {
    const root_data = yaml.parse(await root_file.text());
    if (Array.isArray(root_data)) {
      for (const item of root_data) {
        if (typeof item === "string") global_ignore_set.add(item);
      }
    }
  }

  return {
    file_ignore_map,
    global_ignore_set
  };
};

const check = async () => {
  const { fn_map, test_map } = await garnetScan(GARNET_DIR),
    { doc_set, doc_file_fn_map } = await rustScan(ROOT_DIR),
    { file_ignore_map, global_ignore_set } = await ignoreLoad(),
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

  await rm(MISS_DIR, { recursive: true, force: true });
  await mkdir(MISS_DIR, { recursive: true });

  const all_file_set = new Set([
    ...Object.keys(fn_map),
    ...Object.keys(test_map)
  ]),
    miss_file_li = [];

  for (const rel_path of all_file_set) {
    const fn_li = fn_map[rel_path] ?? [],
      test_li = test_map[rel_path] ?? [],
      miss_fn_li = fn_li.filter((name) => !isDocumented(rel_path, name) && !isIgnored(rel_path, name)),
      miss_test_li = test_li.filter((name) => !isDocumented(rel_path, name) && !isIgnored(rel_path, name));

    if (miss_fn_li.length === 0 && miss_test_li.length === 0) continue;

    const out_data = {};
    if (miss_fn_li.length > 0) out_data.fn = miss_fn_li;
    if (miss_test_li.length > 0) out_data.test = miss_test_li;

    const yml_rel_path = rel_path.replace(/\.cs$/, ".yml"),
      target_file = join(MISS_DIR, yml_rel_path),
      target_dir = dirname(target_file);

    await mkdir(target_dir, { recursive: true });
    await Bun.write(target_file, yaml.stringify(out_data));

    miss_file_li.push(relative(ROOT_DIR, target_file));
  }

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
          line_li.push(indent + key);
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
