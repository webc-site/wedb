import { join, relative, resolve } from "node:path";
import yaml from "yaml";

const file_ignore_map = new Map();
const IGNORE_DIR = resolve("js/check/ignore");
const yml_path = resolve("js/check/ignore/test/standalone/BfTreeInterop.test/BfTreeInteropTests.yml");
const content = await Bun.file(yml_path).text();
const data = yaml.parse(content);
console.log("YAML data:", data);

const rel_path = relative(IGNORE_DIR, yml_path);
const cs_path = rel_path.replace(/\.cs\.ya?ml$/, ".cs").replace(/\.ya?ml$/, ".cs");
console.log("cs_path from ignoreLoad:", cs_path);

const fn_set = new Set();
if (Array.isArray(data.test)) {
  for (const item of data.test) {
    if (typeof item === "string") fn_set.add(item);
  }
}
for (const [key, val] of Object.entries(data)) {
  if (key === "fn" || key === "test") continue;
  if (Array.isArray(val)) {
    for (const item of val) {
      if (typeof item === "string") fn_set.add(item);
    }
  } else if (typeof val === "string") {
    fn_set.add(val);
  }
}
console.log("fn_set:", [...fn_set]);
file_ignore_map.set(cs_path, fn_set);

const test_rel_path = "test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs";
const file_set = file_ignore_map.get(test_rel_path);
console.log("file_set exists?", !!file_set);
console.log("isIgnored Setup?", file_set?.has("Insert_InvalidArguments_ReturnsInvalidArguments"));
