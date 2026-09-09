import { join } from "node:path";

const TreeSitter = require("web-tree-sitter");

let _init;

const tsInit = () => {
  if (!_init) {
    _init = TreeSitter.init();
  }
  return _init;
};

const WASM_DIR = join(import.meta.dirname, "../../node_modules/tree-sitter-wasms/out");

let _csLang, _rsLang;

export const csParser = async () => {
  await tsInit();
  if (!_csLang) {
    _csLang = await TreeSitter.Language.load(join(WASM_DIR, "tree-sitter-c_sharp.wasm"));
  }
  const parser = new TreeSitter();
  parser.setLanguage(_csLang);
  return parser;
};

export const rsParser = async () => {
  await tsInit();
  if (!_rsLang) {
    _rsLang = await TreeSitter.Language.load(join(WASM_DIR, "tree-sitter-rust.wasm"));
  }
  const parser = new TreeSitter();
  parser.setLanguage(_rsLang);
  return parser;
};
