#!/usr/bin/env -S bun

import { join } from "node:path";
import TreeSitter from "web-tree-sitter";

const WASM_DIR = join(import.meta.dirname, "../../node_modules/tree-sitter-wasms/out");

let init_promise, cs_lang, rs_lang;

const tsInit = () => (init_promise ??= TreeSitter.init());

export const csParser = async () => {
    await tsInit();
    cs_lang ??= await TreeSitter.Language.load(join(WASM_DIR, "tree-sitter-c_sharp.wasm"));
    const parser = new TreeSitter();
    parser.setLanguage(cs_lang);
    return parser;
  },
  rsParser = async () => {
    await tsInit();
    rs_lang ??= await TreeSitter.Language.load(join(WASM_DIR, "tree-sitter-rust.wasm"));
    const parser = new TreeSitter();
    parser.setLanguage(rs_lang);
    return parser;
  };

