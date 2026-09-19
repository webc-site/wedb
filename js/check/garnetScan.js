#!/usr/bin/env -S bun

import { readdir } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import { csParser } from "./treeSitter.js";

const TEST_ATTR_SET = new Set([
  "Test", "TestCase", "TestCaseSource", "Theory", "Fact", "TestMethod"
]);

const TEST_LIFECYCLE_ATTR_SET = new Set([
  "SetUp", "TearDown", "OneTimeSetUp", "OneTimeTearDown",
  "TestInitialize", "TestCleanup", "ClassInitialize", "ClassCleanup",
  "AssemblyInitialize", "AssemblyCleanup",
  "GlobalSetup", "GlobalCleanup", "IterationSetup", "IterationCleanup"
]);

const TEST_LIFECYCLE_FN_SET = new Set([
  "Setup", "SetUp", "TearDown", "Teardown", "OneTimeSetUp", "OneTimeTearDown",
  "OnTearDown", "BaseSetup", "BaseTearDown",
  "GlobalSetup", "GlobalCleanup", "IterationSetup", "IterationCleanup",
  "DeleteDirectory", "CleanDirectory", "DeleteTestDataPath"
]);

// ── C# 语料降级兜底（词法声明提取）────────────────────────────────────────
// tree-sitter 的 c-sharp 语法（@2h2d/tree-sitter-wasms，已装 0.2.1 = 该包最新版，
// 无更新版可升）遇到 unsafe 指针写法、部分 #if 条件块等构造时，会把该处往后的
// 整棵 AST 退化成 ERROR 碎片，其后的 method_declaration 全部丢失。实测全仓
// 1425 个 .cs 中 194 个 rootNode.hasError、ERROR 节点 7506 处；例如
// libs/server/Storage/Functions/MainStore/PrivateMethods.cs 只在断裂前提出 1 个名字，
// :396 与 :473 两个重载的 TryInPlaceUpdateNumber 全丢；
// libs/server/Resp/Vector/VectorManager.Callbacks.cs 提出 0 个，整文件对门禁隐形。
//
// 丢失的名字结构性地不可能进 miss，也进不了 documented 比对，因此门禁对这 194 个
// 文件永不上报。这里在 hasError 的文件上补一遍词法声明提取，口径对齐
// js/check/symbolCheck.js 的词法断言：只补 method_declaration 名录（并入既有
// 桶），不据以做任何 test/非 test 之外的语义判断——语法缺失不等于语义结论。
const DECL_MODIFIER_SET = new Set([
  "public", "private", "protected", "internal", "static", "virtual", "override",
  "sealed", "abstract", "partial", "async", "unsafe", "extern", "new", "readonly",
  "ref", "in", "out", "params", "fixed"
]);

// 与声明同形的控制流/成员访问起点，防把 if ( … ) 一类读成声明
const CONTROL_KEYWORD_SET = new Set([
  "if", "for", "foreach", "while", "switch", "catch", "lock", "using", "return",
  "do", "else", "throw", "when", "operator", "value", "get", "set", "init"
]);

const TYPE_DECL_RX = /\b(class|record|struct|interface|enum)\b/,
  // 修饰符序列 + 返回类型 + 名字 + (  ；返回类型为必填段，据此天然排除构造函数
  CS_DECL_RX = /^[ \t]*((?:(?:public|private|protected|internal|static|virtual|override|sealed|abstract|partial|async|unsafe|extern|new|readonly|ref)\s+)+)([A-Za-z_][\w<>,[\].?]*)(?:<[^<>]*>)?\s+(\w+)\s*(?:<[^<>]*>)?\(/gm;

const csDeclFallback = (code) => {
  const name_set = new Set();
  let match;
  CS_DECL_RX.lastIndex = 0;

  while ((match = CS_DECL_RX.exec(code)) !== null) {
    const [, modifier_seg, return_type, fn_name] = match;

    // 主构造函数 `public class Foo(int x)` 与字段声明
    // `private static readonly (int A, int B)[] Table = …` 与真声明同形：
    // 前者返回类型位上是类型关键字，后者被正则回溯拆成「修饰符=类型名」。
    // 实测剔除这两形后，在 1231 个 AST 完好文件上的假阳性为 0。
    if (TYPE_DECL_RX.test(modifier_seg + return_type)) continue;
    if (DECL_MODIFIER_SET.has(return_type) || DECL_MODIFIER_SET.has(fn_name)) continue;
    if (CONTROL_KEYWORD_SET.has(fn_name)) continue;

    name_set.add(fn_name);
  }
  return name_set;
};

const csExtract = (code, parser, file_path = "") => {
  const tree = parser.parse(code),
    fn_set = new Set(),
    test_set = new Set(),
    is_test_file = /(?:[/\\]test|[/\\]tests|[/\\]benchmark|[/\\]benchmarks|[a-z0-9]Tests?\.cs$)/i.test(file_path),
    node_li = tree.rootNode.descendantsOfType([
      "method_declaration",
      "local_function_statement"
    ]);

  for (const node of node_li) {
    const fn_name = node.childForFieldName("name")?.text;
    if (!fn_name) continue;

    if (is_test_file && TEST_LIFECYCLE_FN_SET.has(fn_name)) {
      continue;
    }

    let is_test = false;
    let is_lifecycle = false;
    for (const child of node.children) {
      if (child.type === "attribute_list") {
        for (const attr of child.children) {
          if (attr.type === "attribute") {
            const raw_name = attr.childForFieldName("name")?.text;
            if (raw_name) {
              const attr_name = raw_name.endsWith("Attribute") ? raw_name.slice(0, -9) : raw_name;
              if (TEST_ATTR_SET.has(attr_name)) {
                is_test = true;
                break;
              }
              if (TEST_LIFECYCLE_ATTR_SET.has(attr_name)) {
                is_lifecycle = true;
                break;
              }
            }
          }
        }
      }
      if (is_test || is_lifecycle) break;
    }

    if (is_lifecycle) continue;
    if (is_test) test_set.add(fn_name);
    else fn_set.add(fn_name);
  }

  const error_node_li = tree.rootNode.descendantsOfType("ERROR"),
    degraded = tree.rootNode.hasError,
    ast_count = fn_set.size + test_set.size;

  // 兜底只在 AST 已断裂的文件上跑：tree-sitter 完好时它是权威口径，
  // 完好文件上词法提取反而读不到属性（测试分类）与局部函数的作用域。
  if (degraded) {
    const bucket_set = is_test_file ? test_set : fn_set;
    for (const fn_name of csDeclFallback(code)) {
      // 测试生命周期方法按既有口径整族排除，否则 SetUp/TearDown 这类必然的
      // 「不转写」条目会被兜底灌成语料、凭空报成缺失（首轮实测漏进 3 个文件）
      if (is_test_file && TEST_LIFECYCLE_FN_SET.has(fn_name)) continue;
      if (!fn_set.has(fn_name) && !test_set.has(fn_name)) bucket_set.add(fn_name);
    }
  }

  tree.delete();
  return [
    [...fn_set],
    [...test_set],
    degraded ? { error_nodes: error_node_li.length, ast_count } : null
  ];
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
    parser = await csParser(),
    // 语料健康度：AST 断裂文件清单交 check() 汇总上报，不静默
    degraded_li = [];

  for (const file_path of file_li) {
    const code = await Bun.file(file_path).text(),
      [fn_li, test_li, degrade_info] = csExtract(code, parser, file_path),
      rel_path = relative(garnet_dir, file_path);

    if (fn_li.length > 0) fn_map[rel_path] = fn_li;
    if (test_li.length > 0) test_map[rel_path] = test_li;

    if (degrade_info) {
      degraded_li.push({
        path: rel_path,
        error_nodes: degrade_info.error_nodes,
        ast_count: degrade_info.ast_count,
        total_count: fn_li.length + test_li.length
      });
    }
  }

  parser.delete();

  return [fn_map, test_map, { cs_file_count: file_li.length, degraded_li }];
};

export default garnetScan;
export { csDeclFallback, TEST_LIFECYCLE_FN_SET };

if (import.meta.main) {
  const t0 = performance.now(),
    [fn_map, test_map, health] = await garnetScan(),
    fn_count = Object.values(fn_map).reduce((total, sub_li) => total + sub_li.length, 0),
    test_count = Object.values(test_map).reduce((total, sub_li) => total + sub_li.length, 0),
    file_count = new Set([...Object.keys(fn_map), ...Object.keys(test_map)]).size,
    elapsed_ms = (performance.now() - t0).toFixed(1),
    recovered = health.degraded_li.reduce((total, d) => total + d.total_count - d.ast_count, 0);

  console.log(
    "[garnetScan] 耗时 " +
      elapsed_ms +
      "ms，扫描 " +
      health.cs_file_count +
      " 个 .cs 文件，其中 " +
      file_count +
      " 个有名录：" +
      fn_count +
      " 个普通函数，" +
      test_count +
      " 个测试函数"
  );

  if (health.degraded_li.length > 0) {
    const sorted_li = [...health.degraded_li].sort((a, b) => b.error_nodes - a.error_nodes);
    console.log(
      "[garnetScan] 语料降级：" + sorted_li.length + " 个 .cs 的 tree-sitter AST 断裂，" +
        "已用词法兜底补回 " + recovered + " 个方法名（按 ERROR 节点数取前 10）"
    );
    for (const item of sorted_li.slice(0, 10)) {
      console.log(
        "  ERROR " + item.error_nodes + " 处，AST " + item.ast_count +
          " → 兜底后 " + item.total_count + "  " + item.path
      );
    }
  }
}
