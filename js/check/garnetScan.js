#!/usr/bin/env -S bun

import { readdir } from "node:fs/promises";
import { join, relative, resolve } from "node:path";

const KEYWORD_SET = new Set([
  "if", "else", "for", "foreach", "while", "do", "switch", "case", "default",
  "using", "lock", "fixed", "sizeof", "typeof", "nameof", "checked", "unchecked",
  "throw", "return", "new", "await", "yield", "var", "class", "struct", "interface",
  "record", "enum", "namespace", "get", "set", "init", "add", "remove", "value",
  "catch", "finally", "try", "base", "this", "true", "false", "null", "operator"
]),
TEST_ATTR_SET = new Set([
  "Test", "TestCase", "TestCaseSource", "Theory", "Fact", "TestMethod"
]),
INVALID_PREV_SET = new Set([
  ".", "=", "==", "!=", "+", "-", "*", "/", "%", "&", "|", "^", "!", "~",
  ",", "[", "(", "?", ":", "return", "throw", "await", "new", "case",
  "class", "struct", "interface", "record", "enum", "namespace", "delegate"
]);

const codeTokenize = (code) => {
  let i = 0;
  const code_len = code.length,
    token_li = [];

  while (i < code_len) {
    const ch = code[i];
    if (/\s/.test(ch)) {
      ++i;
      continue;
    }
    if (ch === "/" && code[i + 1] === "/") {
      while (i < code_len && code[i] !== "\n") ++i;
      continue;
    }
    if (ch === "/" && code[i + 1] === "*") {
      i += 2;
      while (i < code_len && !(code[i] === "*" && code[i + 1] === "/")) ++i;
      i += 2;
      continue;
    }
    if (ch === "#") {
      while (i < code_len && code[i] !== "\n") ++i;
      continue;
    }
    if (ch === "@" && code[i + 1] === "\"") {
      i += 2;
      while (i < code_len) {
        if (code[i] === "\"" && code[i + 1] === "\"") i += 2;
        else if (code[i] === "\"") {
          ++i;
          break;
        } else ++i;
      }
      token_li.push({ type: "str", val: "" });
      continue;
    }
    if (ch === "$" && code[i + 1] === "@" && code[i + 2] === "\"") {
      i += 3;
      while (i < code_len) {
        if (code[i] === "\"" && code[i + 1] === "\"") i += 2;
        else if (code[i] === "\"") {
          ++i;
          break;
        } else ++i;
      }
      token_li.push({ type: "str", val: "" });
      continue;
    }
    if (ch === "\"" && code[i + 1] === "\"" && code[i + 2] === "\"") {
      i += 3;
      while (i < code_len && !(code[i] === "\"" && code[i + 1] === "\"" && code[i + 2] === "\"")) ++i;
      i += 3;
      token_li.push({ type: "str", val: "" });
      continue;
    }
    if (ch === "\"") {
      ++i;
      while (i < code_len && code[i] !== "\"") {
        if (code[i] === "\\") ++i;
        ++i;
      }
      ++i;
      token_li.push({ type: "str", val: "" });
      continue;
    }
    if (ch === "\x27") {
      ++i;
      while (i < code_len && code[i] !== "\x27") {
        if (code[i] === "\\") ++i;
        ++i;
      }
      ++i;
      token_li.push({ type: "char", val: "" });
      continue;
    }
    if (/[A-Za-z_]/.test(ch)) {
      const start = i;
      while (i < code_len && /[A-Za-z0-9_]/.test(code[i])) ++i;
      token_li.push({ type: "ident", val: code.slice(start, i) });
      continue;
    }
    if (/[0-9]/.test(ch)) {
      while (i < code_len && /[0-9A-Fa-fxXULulfd_.]/.test(code[i])) ++i;
      token_li.push({ type: "num", val: "" });
      continue;
    }
    if (ch === "=" && code[i + 1] === ">") {
      token_li.push({ type: "punct", val: "=>" });
      i += 2;
      continue;
    }
    if (ch === "=" && code[i + 1] === "=") {
      token_li.push({ type: "punct", val: "==" });
      i += 2;
      continue;
    }
    if (ch === "!" && code[i + 1] === "=") {
      token_li.push({ type: "punct", val: "!=" });
      i += 2;
      continue;
    }
    if (ch === ":" && code[i + 1] === ":") {
      token_li.push({ type: "punct", val: "::" });
      i += 2;
      continue;
    }
    token_li.push({ type: "punct", val: ch });
    ++i;
  }
  return token_li;
};

const csExtract = (code) => {
  const token_li = codeTokenize(code),
    fn_set = new Set(),
    test_set = new Set(),
    token_len = token_li.length;

  for (let idx = 0; idx < token_len; ++idx) {
    const token = token_li[idx];
    if (token.type === "ident" && !KEYWORD_SET.has(token.val)) {
      let next_idx = idx + 1;
      if (next_idx >= token_len) break;

      if (token_li[next_idx].val === "<") {
        let depth = 1;
        ++next_idx;
        while (next_idx < token_len && depth > 0) {
          if (token_li[next_idx].val === "<") ++depth;
          else if (token_li[next_idx].val === ">") --depth;
          ++next_idx;
        }
      }

      if (next_idx < token_len && token_li[next_idx].val === "(") {
        let p_depth = 1,
          p_idx = next_idx + 1;
        while (p_idx < token_len && p_depth > 0) {
          if (token_li[p_idx].val === "(") ++p_depth;
          else if (token_li[p_idx].val === ")") --p_depth;
          ++p_idx;
        }

        if (p_idx < token_len) {
          let after_p = p_idx;
          if (token_li[after_p]?.val === ":") {
            while (after_p < token_len && token_li[after_p].val !== "{" && token_li[after_p].val !== "=>" && token_li[after_p].val !== ";") {
              ++after_p;
            }
          }
          while (after_p < token_len && token_li[after_p].val === "where") {
            while (after_p < token_len && token_li[after_p].val !== "{" && token_li[after_p].val !== "=>" && token_li[after_p].val !== ";") {
              ++after_p;
            }
          }

          if (after_p < token_len && (token_li[after_p].val === "{" || token_li[after_p].val === "=>" || token_li[after_p].val === ";")) {
            const prev_idx = idx - 1;
            if (prev_idx >= 0) {
              const prev = token_li[prev_idx];
              if (!INVALID_PREV_SET.has(prev.val)) {
                let is_delegate = false,
                  is_test = false,
                  back_idx = idx - 1;
                while (back_idx >= 0 && token_li[back_idx].val !== ";" && token_li[back_idx].val !== "}" && token_li[back_idx].val !== "{") {
                  if (token_li[back_idx].val === "delegate") is_delegate = true;
                  if (TEST_ATTR_SET.has(token_li[back_idx].val)) is_test = true;
                  --back_idx;
                }
                if (!is_delegate) {
                  if (is_test) test_set.add(token.val);
                  else fn_set.add(token.val);
                }
              }
            }
          }
        }
      }
    }
  }

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
    test_map = {};

  for (const file_path of file_li) {
    const code = await Bun.file(file_path).text(),
      [fn_li, test_li] = csExtract(code),
      rel_path = relative(garnet_dir, file_path);

    if (fn_li.length > 0) fn_map[rel_path] = fn_li;
    if (test_li.length > 0) test_map[rel_path] = test_li;
  }

  return [fn_map, test_map];
};

export default garnetScan;

if (import.meta.main) {
  const t0 = performance.now(),
    [fn_map, test_map] = await garnetScan(),
    fn_count = Object.values(fn_map).reduce((total, list) => total + list.length, 0),
    test_count = Object.values(test_map).reduce((total, list) => total + list.length, 0),
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
