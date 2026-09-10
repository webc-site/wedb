#!/usr/bin/env -S bun

import { readdir } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import { csParser } from "./treeSitter.js";

const TEST_ATTR_SET = new Set([
  "Test", "TestCase", "TestCaseSource", "Theory", "Fact", "TestMethod"
]);

const csExtract = (code, parser) => {
  const tree = parser.parse(code),
    fn_set = new Set(),
    test_set = new Set(),
    node_li = tree.rootNode.descendantsOfType([
      "method_declaration",
      "local_function_statement"
    ]);

  for (const node of node_li) {
    const fn_name = node.childForFieldName("name")?.text;
    if (!fn_name) continue;

    let is_test = false;
    for (const child of node.children) {
      if (child.type === "attribute_list") {
        for (const attr of child.children) {
          if (attr.type === "attribute") {
            const attr_name = attr.childForFieldName("name")?.text;
            if (attr_name && TEST_ATTR_SET.has(attr_name)) {
              is_test = true;
              break;
            }
          }
        }
      }
      if (is_test) break;
    }

    if (is_test) test_set.add(fn_name);
    else fn_set.add(fn_name);
  }

  tree.delete();
  return [[...fn_set], [...test_set]];
};

const csWalk = async (dir_path) => {
  const entry_li = await readdir(dir_path, { withFileTypes: true }),
    file_li = [];

  for (const entry of entry_li) {
    const { name } = entry;
    if (name === "bin" || name === "obj" || name.startsWith(".")) continue;
    const full_path = join(dir_path, name);
    if (entry.isDirectory()) {
      const sub_li = await csWalk(full_path);
      file_li.push(...sub_li);
    } else if (name.endsWith(".cs")) {
      file_li.push(full_path);
    }
  }
  return file_li;
};

const garnetScan = async (garnet_dir = resolve(import.meta.dirname, "../../garnet")) => {
  const file_li = await csWalk(garnet_dir),
    fn_map = {},
    test_map = {},
    parser = await csParser();

  for (const file_path of file_li) {
    const code = await Bun.file(file_path).text(),
      [fn_li, test_li] = csExtract(code, parser),
      rel_path = relative(garnet_dir, file_path);

    if (fn_li.length > 0) fn_map[rel_path] = fn_li;
    if (test_li.length > 0) test_map[rel_path] = test_li;
  }

  parser.delete();

  return [fn_map, test_map];
};

export default garnetScan;

if (import.meta.main) {
  const t0 = performance.now(),
    [fn_map, test_map] = await garnetScan(),
    fn_count = Object.values(fn_map).reduce((total, sub_li) => total + sub_li.length, 0),
    test_count = Object.values(test_map).reduce((total, sub_li) => total + sub_li.length, 0),
    file_count = new Set([...Object.keys(fn_map), ...Object.keys(test_map)]).size,
    elapsed_ms = (performance.now() - t0).toFixed(1);

  console.log(
    "[garnetScan] 耗时 " +
      elapsed_ms +
      "ms，扫描 " +
      file_count +
      " 个文件：" +
      fn_count +
      " 个普通函数，" +
      test_count +
      " 个测试函数"
  );
}
