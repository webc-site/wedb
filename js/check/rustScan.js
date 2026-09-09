#!/usr/bin/env -S bun

import { readdir } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import { rsParser } from "./treeSitter.js";

const rsWalk = async (dir_path) => {
  const entry_li = await readdir(dir_path, { withFileTypes: true }),
    file_li = [];

  for (const entry of entry_li) {
    const { name } = entry;
    if (
      name === "target" ||
      name === ".git" ||
      name === "node_modules" ||
      name === "garnet" ||
      name === "scratch" ||
      name.startsWith(".moon")
    ) {
      continue;
    }
    const full_path = join(dir_path, name);
    if (entry.isDirectory()) {
      const sub_li = await rsWalk(full_path);
      file_li.push(...sub_li);
    } else if (name.endsWith(".rs")) {
      file_li.push(full_path);
    }
  }
  return file_li;
};

const CS_REF_REGEX =
  /(?:^|[^\w./])([a-zA-Z0-9_\-./]+\.cs)::?([A-Za-z0-9_]+)/g;

const docTokenExtract = (text, doc_set, doc_file_fn_map) => {
  const word_li = text.match(/[A-Za-z_][A-Za-z0-9_]*/g);
  word_li?.forEach((word) => doc_set.add(word));

  for (const match of text.matchAll(CS_REF_REGEX)) {
    const [, raw_cs_path, fn_name] = match;
    let cs_path = raw_cs_path;

    const idx = cs_path.indexOf("garnet/");
    if (idx !== -1) {
      cs_path = cs_path.slice(idx + 7);
    }
    cs_path = cs_path.replace(/^\.?\//, "");

    const fn_set = doc_file_fn_map.get(cs_path) ?? new Set();
    fn_set.add(fn_name);
    doc_file_fn_map.set(cs_path, fn_set);
    doc_set.add(fn_name);
  }
};

const rsDocExtract = (code, file_rel, doc_file_fn_map, parser) => {
  const tree = parser.parse(code);
  const fn_doc_li = [];
  const doc_set = new Set();
  const all_doc_li = [];

  const comments = tree.rootNode.descendantsOfType(["line_comment", "block_comment"]);
  for (const c of comments) {
    let text = c.text;
    if (c.type === "line_comment") {
      text = text.replace(/^\/\/[/!]?\s*/, "");
    } else if (c.type === "block_comment") {
      text = text.replace(/^\/\*+\s*/, "").replace(/\s*\*+\/$/, "");
    }
    text = text.trim();
    if (text) {
      all_doc_li.push(text);
      docTokenExtract(text, doc_set, doc_file_fn_map);
    }
  }

  const functions = tree.rootNode.descendantsOfType("function_item");
  for (const fn of functions) {
    const nameNode = fn.childForFieldName("name");
    if (!nameNode) continue;
    const fn_name = nameNode.text;

    const doc_parts = [];
    let curr = fn.previousSibling;
    let last_row = fn.startPosition.row;

    while (curr) {
      if (curr.type === "attribute_item") {
        last_row = curr.startPosition.row;
        curr = curr.previousSibling;
        continue;
      }

      if (curr.type !== "line_comment" && curr.type !== "block_comment") {
        break;
      }

      if (last_row - curr.endPosition.row > 1) {
        break;
      }

      const text = curr.text;
      if (curr.type === "line_comment" && (text.startsWith("///") || text.startsWith("//!"))) {
        doc_parts.unshift(text.replace(/^\/\/[/!]\s*/, ""));
      } else if (curr.type === "block_comment" && text.startsWith("/**")) {
        doc_parts.unshift(text.replace(/^\/\*\*+\s*/, "").replace(/\s*\*+\/$/, "").trim());
      }

      last_row = curr.startPosition.row;
      curr = curr.previousSibling;
    }

    fn_doc_li.push({
      file: file_rel,
      fn: fn_name,
      doc: doc_parts.join("\n")
    });
  }

  tree.delete();
  return [fn_doc_li, doc_set, all_doc_li.join("\n")];
};

const rustScan = async (root_dir = resolve(import.meta.dirname, "../..")) => {
  const file_li = await rsWalk(root_dir),
    fn_doc_li = [],
    doc_set = new Set(),
    doc_file_fn_map = new Map(),
    text_li = [];

  const parser = await rsParser();

  for (const file_path of file_li) {
    const code = await Bun.file(file_path).text(),
      file_rel = relative(root_dir, file_path),
      [sub_fn_doc_li, sub_doc_set, doc_text] = rsDocExtract(code, file_rel, doc_file_fn_map, parser);

    fn_doc_li.push(...sub_fn_doc_li);
    for (const token of sub_doc_set) doc_set.add(token);
    text_li.push(doc_text);
  }

  parser.delete();
  return [doc_set, doc_file_fn_map, fn_doc_li, text_li.join("\n")];
};

export default rustScan;

if (import.meta.main) {
  const t0 = performance.now(),
    [doc_set, doc_file_fn_map, fn_doc_li] = await rustScan(),
    doc_fn_count = fn_doc_li.filter((doc_item) => doc_item.doc.length > 0).length,
    elapsed_ms = (performance.now() - t0).toFixed(1);

  console.log(
    "[rustScan] 耗时 " +
      elapsed_ms +
      "ms，扫描 " +
      fn_doc_li.length +
      " 个函数（" +
      doc_fn_count +
      " 个含文档注释），提取 " +
      doc_set.size +
      " 个文档符号，识别 " +
      doc_file_fn_map.size +
      " 个精准映射文件"
  );
}
