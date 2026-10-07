#!/usr/bin/env -S bun

import { Language, Parser } from "web-tree-sitter";

let init_promise, cs_lang, rs_lang;

const tsInit = () => (init_promise ??= Parser.init());

export const csParser = async () => {
    await tsInit();
    cs_lang ??= await Language.load(
      new URL(import.meta.resolve("@2h2d/tree-sitter-wasms/wasm/tree-sitter-c-sharp.wasm"))
    );
    const parser = new Parser();
    parser.setLanguage(cs_lang);
    return parser;
  },
  rsParser = async () => {
    await tsInit();
    rs_lang ??= await Language.load(
      new URL(import.meta.resolve("@2h2d/tree-sitter-wasms/wasm/tree-sitter-rust.wasm"))
    );
    const parser = new Parser();
    parser.setLanguage(rs_lang);
    return parser;
  };

