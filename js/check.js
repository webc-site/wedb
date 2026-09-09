#!/usr/bin/env -S bun

import { mkdir, readdir, rm } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
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
  const global_set = new Set(),
    file_map = new Map(),
    yml_file_li = await walkYml(IGNORE_DIR);

  const root_ignore = join(import.meta.dirname, "check/ignore.yml");
  if (!yml_file_li.includes(root_ignore)) {
    const root_file = Bun.file(root_ignore);
    if (await root_file.exists()) {
      yml_file_li.push(root_ignore);
    }
  }

  for (const yml_path of yml_file_li) {
    const content = await Bun.file(yml_path).text(),
      data = yaml.parse(content);

    if (!data) continue;

    if (Array.isArray(data)) {
      for (const item of data) {
        if (typeof item === "string") global_set.add(item);
      }
    } else if (typeof data === "object") {
      if (Array.isArray(data.fn)) {
        for (const item of data.fn) {
          if (typeof item === "string") global_set.add(item);
        }
      }
      if (Array.isArray(data.test)) {
        for (const item of data.test) {
          if (typeof item === "string") global_set.add(item);
        }
      }

      for (const [key, val] of Object.entries(data)) {
        if (key === "fn" || key === "test") continue;
        if (Array.isArray(val)) {
          const fn_set = file_map.get(key) ?? new Set();
          for (const item of val) {
            if (typeof item === "string") fn_set.add(item);
          }
          file_map.set(key, fn_set);
        } else if (typeof val === "string") {
          global_set.add(val);
        }
      }
    }
  }

  return {
    global_set,
    file_map
  };
};

const check = async () => {
  const t0 = performance.now();

  console.log("[check] Scanning Garnet C# source...");
  const { fn_map, test_map } = await garnetScan(GARNET_DIR);

  console.log("[check] Scanning Rust functions doc comments...");
  const { doc_set } = await rustScan(ROOT_DIR);

  console.log("[check] Loading ignore rules...");
  const { global_set, file_map } = await ignoreLoad(),
    isIgnored = (rel_path, name) => {
      if (global_set.has(name)) return true;
      if (file_map.get(rel_path)?.has(name)) return true;
      const base_name = basename(rel_path);
      if (file_map.get(base_name)?.has(name)) return true;
      return false;
    };

  await rm(MISS_DIR, { recursive: true, force: true });
  await mkdir(MISS_DIR, { recursive: true });

  const all_file_set = new Set([
    ...Object.keys(fn_map),
    ...Object.keys(test_map)
  ]);

  let miss_file_count = 0,
    total_miss_fn = 0,
    total_miss_test = 0;

  for (const rel_path of all_file_set) {
    const fn_li = fn_map[rel_path] ?? [],
      test_li = test_map[rel_path] ?? [],
      miss_fn_li = fn_li.filter((name) => !doc_set.has(name) && !isIgnored(rel_path, name)),
      miss_test_li = test_li.filter((name) => !doc_set.has(name) && !isIgnored(rel_path, name));

    if (miss_fn_li.length === 0 && miss_test_li.length === 0) continue;

    const out_data = {};
    if (miss_fn_li.length > 0) out_data.fn = miss_fn_li;
    if (miss_test_li.length > 0) out_data.test = miss_test_li;

    const yml_rel_path = rel_path.replace(/\.cs$/, ".yml"),
      target_file = join(MISS_DIR, yml_rel_path),
      target_dir = dirname(target_file);

    await mkdir(target_dir, { recursive: true });
    await Bun.write(target_file, yaml.stringify(out_data));

    ++miss_file_count;
    total_miss_fn += miss_fn_li.length;
    total_miss_test += miss_test_li.length;
  }

  const elapsed_ms = (performance.now() - t0).toFixed(1);

  console.log(`[check] Completed in ${elapsed_ms}ms:`);
  console.log(`  - Garnet files examined: ${all_file_set.size}`);
  console.log(`  - Rust doc tokens indexed: ${doc_set.size}`);
  console.log(`  - Ignored global symbols: ${global_set.size}`);
  console.log(`  - Files with missing items: ${miss_file_count}`);
  console.log(`  - Missing regular functions: ${total_miss_fn}`);
  console.log(`  - Missing test functions: ${total_miss_test}`);
  console.log(`  - Output directory: ${MISS_DIR}`);
};

export default check;

if (import.meta.main) {
  await check();
}
