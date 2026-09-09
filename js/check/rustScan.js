#!/usr/bin/env -S bun

import { readdir } from "node:fs/promises";
import { join, relative, resolve } from "node:path";

const FN_REGEX = /(?:^|\s)(?:pub(?:\([^)]+\))?\s+)?(?:default\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?(?:extern(?:\s+"[^"]+")?\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)/;

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

const rsDocExtract = (code, file_rel, doc_file_fn_map) => {
  const line_li = code.split("\n"),
    fn_doc_li = [],
    doc_set = new Set(),
    all_doc_li = [];

  let pending_doc_li = [],
    in_block_doc = false,
    block_buf_li = [];

  for (let i = 0; i < line_li.length; ++i) {
    const trimmed = line_li[i].trim();

    if (in_block_doc) {
      if (trimmed.includes("*/")) {
        in_block_doc = false;
        const part = trimmed.slice(0, trimmed.indexOf("*/")).trim();
        block_buf_li.push(part);
        const full_block = block_buf_li.join(" ");
        pending_doc_li.push(full_block);
        all_doc_li.push(full_block);
        docTokenExtract(full_block, doc_set, doc_file_fn_map);
        block_buf_li = [];
      } else {
        block_buf_li.push(trimmed);
      }
      continue;
    }

    if (trimmed.startsWith("/**")) {
      if (trimmed.includes("*/")) {
        const doc = trimmed.slice(3, trimmed.indexOf("*/")).trim();
        pending_doc_li.push(doc);
        all_doc_li.push(doc);
        docTokenExtract(doc, doc_set, doc_file_fn_map);
      } else {
        in_block_doc = true;
        block_buf_li = [trimmed.slice(3).trim()];
      }
      continue;
    }

    if (trimmed.startsWith("///") || trimmed.startsWith("//!")) {
      const doc = trimmed.replace(/^\/\/[/!]\s*/, "");
      pending_doc_li.push(doc);
      all_doc_li.push(doc);
      docTokenExtract(doc, doc_set, doc_file_fn_map);
      continue;
    }

    if (trimmed.startsWith("//")) {
      const comment = trimmed.replace(/^\/\/\s*/, "");
      all_doc_li.push(comment);
      docTokenExtract(comment, doc_set, doc_file_fn_map);
      continue;
    }

    if (trimmed.startsWith("#[")) {
      continue;
    }

    if (trimmed === "") {
      pending_doc_li = [];
      continue;
    }

    const fn_match = trimmed.match(FN_REGEX);
    if (fn_match) {
      const fn_name = fn_match[1],
        doc_str = pending_doc_li.join("\n");

      fn_doc_li.push({
        file: file_rel,
        fn: fn_name,
        doc: doc_str
      });
      pending_doc_li = [];
    } else {
      pending_doc_li = [];
    }
  }

  return [fn_doc_li, doc_set, all_doc_li.join("\n")];
};

const rustScan = async (root_dir = resolve(import.meta.dirname, "../..")) => {
  const file_li = await rsWalk(root_dir),
    fn_doc_li = [],
    doc_set = new Set(),
    doc_file_fn_map = new Map(),
    text_li = [];

  for (const file_path of file_li) {
    const code = await Bun.file(file_path).text(),
      file_rel = relative(root_dir, file_path),
      [sub_fn_doc_li, sub_doc_set, doc_text] = rsDocExtract(code, file_rel, doc_file_fn_map);

    fn_doc_li.push(...sub_fn_doc_li);
    for (const token of sub_doc_set) doc_set.add(token);
    text_li.push(doc_text);
  }

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
