import { readdir } from "node:fs/promises";
import { join, relative, resolve, dirname } from "node:path";
import yaml from "yaml";

const file_ignore_map = new Map();
const IGNORE_DIR = resolve("js/check/ignore");
const yml_path = resolve("js/check/ignore/test/standalone/Garnet.test/RespTests.yml");
const content = await Bun.file(yml_path).text();
const data = yaml.parse(content);
console.log("data:", Object.keys(data));
const rel_path = relative(IGNORE_DIR, yml_path);
const cs_path = rel_path.replace(/\.cs\.ya?ml$/, ".cs").replace(/\.ya?ml$/, ".cs");
console.log("rel_path:", rel_path);
console.log("cs_path:", cs_path);
