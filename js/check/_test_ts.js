#!/usr/bin/env -S bun
import { join } from "node:path";

const TreeSitter = require("web-tree-sitter");

await TreeSitter.init();

const WASM_DIR = join(import.meta.dirname, "../../node_modules/tree-sitter-wasms/out");

const csLang = await TreeSitter.Language.load(join(WASM_DIR, "tree-sitter-c_sharp.wasm"));
const rsLang = await TreeSitter.Language.load(join(WASM_DIR, "tree-sitter-rust.wasm"));

const csParser = new TreeSitter();
csParser.setLanguage(csLang);

const rsParser = new TreeSitter();
rsParser.setLanguage(rsLang);

const csCode = `using System;

namespace Test {
  [Test]
  public class Foo {
    public void Bar(int x) {
      Console.WriteLine(x);
    }

    [TestCase("hello")]
    private async Task<int> Baz<T>(string s) where T : class {
      return 0;
    }

    public delegate void MyDelegate(int x);
  }
}
`;

const csTree = csParser.parse(csCode);
const printTree = (node, indent = "", depth = 0) => {
  if (depth > 4) return;
  const text = node.text?.length > 40 ? node.text.slice(0, 40) + "..." : node.text;
  console.log(`${indent}${node.type}${node.isNamed ? "" : " (anon)"}  "${text?.replace(/\n/g, "\\n")}"`);
  for (const child of node.children) {
    printTree(child, indent + "  ", depth + 1);
  }
};

console.log("=== C# AST (depth <= 4) ===");
printTree(csTree.rootNode);

const rsCode = `/// This is a doc comment about foo
/// References garnet/test.cs::SomeFunc
pub fn foo(x: i32) -> i32 {
    x + 1
}

// regular comment about bar
fn bar() {}

/// Another doc
pub async fn baz<T: Clone>(val: T) -> T {
    val
}
`;

const rsTree = rsParser.parse(rsCode);
console.log("\n=== Rust AST (depth <= 4) ===");
printTree(rsTree.rootNode);

csTree.delete();
rsTree.delete();
csParser.delete();
rsParser.delete();
