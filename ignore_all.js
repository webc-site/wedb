import fs from "fs/promises";
import { join, dirname } from "path";
import yaml from "yaml";
import garnetScan from "./js/check/garnetScan.js";

const [fn_map, test_map] = await garnetScan();
const TARGETS = ["test", "benchmark", "main", "playground", "samples", "metrics", "modules", "hosting", "libs"];

const all_files = [...Object.keys(fn_map), ...Object.keys(test_map)];

for (const file of all_files) {
  if (TARGETS.some(t => file.startsWith(t + "/"))) {
    const yml_path = "js/check/ignore/" + file.replace(/\.cs$/, ".yml");
    await fs.mkdir(dirname(yml_path), { recursive: true });

    const fns = fn_map[file] || [];
    const tests = test_map[file] || [];
    
    const obj = {};
    if (fns.length) obj.fn = fns;
    if (tests.length) obj.test = tests;
    
    if (Object.keys(obj).length > 0) {
      await fs.writeFile(yml_path, yaml.stringify(obj));
    }
  }
}
