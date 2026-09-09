#!/usr/bin/env -S bun

import { readdir } from "node:fs/promises";
import { join, relative, resolve } from "node:path";

const FN_REGEX = /(?:^|\s)(?:pub(?:\([^)]+\))?\s+)?(?:default\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?(?:extern(?:\s+"[^"]+")?\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)/;

const walkRs = async (dir_path) => {
  const entry_li = await readdir(dir_path, { withFileTypes: true }),
    file_li = [];

  for (const entry of entry_li) {
    if (
      entry.name === "target" ||
      entry.name === ".git" ||
      entry.name === "node_modules" ||
      entry.name === "garnet" ||
      entry.name === "scratch" ||
      entry.name.startsWith(".moon")
    ) {
      continue;
    }
    const full_path = join(dir_path, entry.name);
    if (entry.isDirectory()) {
      const sub_li = await walkRs(full_path);
      file_li.push(...sub_li);
    } else if (entry.name.endsWith(".rs")) {
      file_li.push(full_path);
    }
  }
  return file_li;
};

const CS_REF_REGEX =
  /(?:^|[^\w./])([a-zA-Z0-9_\-./]+\.cs)::?([A-Za-z0-9_]+)/g;

const extractDocTokens = (text, doc_set, doc_file_fn_map) => {
  const word_li = text.match(/[A-Za-z_][A-Za-z0-9_]*/g);
  if (word_li) {
    for (const w of word_li) doc_set.add(w);
  }
  for (const m of text.matchAll(CS_REF_REGEX)) {
    let cs_path = m[1];
    const fn_name = m[2];

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

  let pending_doc = [],
    in_block_doc = false,
    block_buf = [];

  for (let i = 0; i < line_li.length; ++i) {
    const trimmed = line_li[i].trim();

    if (in_block_doc) {
      if (trimmed.includes("*/")) {
        in_block_doc = false;
        const part = trimmed.slice(0, trimmed.indexOf("*/")).trim();
        block_buf.push(part);
        const full_block = block_buf.join(" ");
        pending_doc.push(full_block);
        all_doc_li.push(full_block);
        extractDocTokens(full_block, doc_set, doc_file_fn_map);
        block_buf = [];
      } else {
        block_buf.push(trimmed);
      }
      continue;
    }

    if (trimmed.startsWith("/**")) {
      if (trimmed.includes("*/")) {
        const doc = trimmed.slice(3, trimmed.indexOf("*/")).trim();
        pending_doc.push(doc);
        all_doc_li.push(doc);
        extractDocTokens(doc, doc_set, doc_file_fn_map);
      } else {
        in_block_doc = true;
        block_buf = [trimmed.slice(3).trim()];
      }
      continue;
    }

    if (trimmed.startsWith("///") || trimmed.startsWith("//!")) {
      const doc = trimmed.replace(/^\/\/[/!]\s*/, "");
      pending_doc.push(doc);
      all_doc_li.push(doc);
      extractDocTokens(doc, doc_set, doc_file_fn_map);
      continue;
    }

    if (trimmed.startsWith("//")) {
      const comment = trimmed.replace(/^\/\/\s*/, "");
      all_doc_li.push(comment);
      extractDocTokens(comment, doc_set, doc_file_fn_map);
      continue;
    }

    if (trimmed.startsWith("#[")) {
      continue;
    }

    if (trimmed === "") {
      pending_doc = [];
      continue;
    }

    const m = trimmed.match(FN_REGEX);
    if (m) {
      const fn_name = m[1],
        doc_str = pending_doc.join("\n");

      fn_doc_li.push({
        file: file_rel,
        fn: fn_name,
        doc: doc_str
      });
      pending_doc = [];
    } else {
      pending_doc = [];
    }
  }

  return {
    fn_doc_li,
    doc_set,
    doc_text: all_doc_li.join("\n")
  };
};

const rustScan = async (root_dir = resolve(import.meta.dirname, "../..")) => {
  const file_li = await walkRs(root_dir),
    fn_doc_li = [],
    doc_set = new Set(),
    doc_file_fn_map = new Map(),
    text_li = [];

  for (const file_path of file_li) {
    const code = await Bun.file(file_path).text(),
      file_rel = relative(root_dir, file_path),
      res = rsDocExtract(code, file_rel, doc_file_fn_map);

    fn_doc_li.push(...res.fn_doc_li);
    for (const token of res.doc_set) doc_set.add(token);
    text_li.push(res.doc_text);
  }

  return {
    fn_doc_li,
    doc_set,
    doc_file_fn_map,
    doc_text: text_li.join("\n")
  };
};

export default rustScan;

if (import.meta.main) {
  const t0 = performance.now(),
    { fn_doc_li, doc_set, doc_file_fn_map } = await rustScan(),
    doc_fn_count = fn_doc_li.filter((x) => x.doc.length > 0).length,
    elapsed_ms = (performance.now() - t0).toFixed(1);

  console.log(
    `[rustScan] 耗时 ${elapsed_ms}ms，扫描 ${fn_doc_li.length} 个函数（${doc_fn_count} 个含文档注释），提取 ${doc_set.size} 个文档符号，识别 ${doc_file_fn_map.size} 个精准映射文件`
  );
}
