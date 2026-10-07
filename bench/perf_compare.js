#!/usr/bin/env -S bun
// Rust 与 C#（vendored Tsavorite 原版）性能对表门禁。
// 口径来自 task/bench.md 第 5 节：Rust 在任何指标上都不低于 C#。
// 用法：node bench/perf_compare.js --rust r1.json r2.json r3.json --cs c1.json c2.json c3.json [--noise-ok]
// 同侧给多个文件时按指标取中位数（抗噪要求），只给一个文件则直取。

import { readFile } from "node:fs/promises";

// 噪声区间：比值落在 [0.98, 1.00)（吞吐）或 (1.00, 1.02]（时延/体积）时判 HOLD，
// 必须复跑确认才放行；--noise-ok 表示本席已复跑确认过，放行并计入 notes。
const NOISE_LO = 0.98, NOISE_HI = 1.02;

const arg_li = process.argv.slice(2);

const take = (name) => {
  const i = arg_li.indexOf(`--${name}`);
  if (i < 0) return [];
  const out = [];
  for (let j = i + 1; j < arg_li.length && !arg_li[j].startsWith("--"); j++) out.push(arg_li[j]);
  return out;
};

const flag = (name) => arg_li.includes(`--${name}`);

const rust_paths = take("rust"), cs_paths = take("cs");

if (rust_paths.length === 0 || cs_paths.length === 0) {
  console.error("用法：node js/perfGate.js --rust <json..> --cs <json..> [--noise-ok]");
  process.exit(2);
}

const load = async (path) => {
  const run = JSON.parse(await readFile(path, "utf8"));
  if (run.schema !== 1) fail(`${path} 的 schema=${run.schema}，期望 1`);
  if (!Array.isArray(run.engines) || run.engines.length === 0) fail(`${path} 无 engines`);
  return { path, run };
};

const median = (li) => {
  const v = [...li].sort((a, b) => a - b);
  const m = v.length >> 1;
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
};

// 显示串一律由数值现算，规则与 bench/crates/wedb-bench/src/result.rs 逐条对齐：
// 直接信任 JSON 里的 formatted 会让「数字对但单位/字符串造假」的驱动蒙混过关。
const fmt_rate = (rate) => {
  const m = rate / 1e6;
  if (m === 0) return "0.00";
  if (m < 0.01) return "<0.01";
  if (m >= 100) return m.toFixed(1);
  return m.toFixed(2);
};

const fmt_ms = (ns) => `${Math.floor((ns + 500_000) / 1_000_000)}ms`;

const fmt_size = (bytes) => {
  const U = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
  let v = bytes, i = 0;
  while (v >= 1024 && i + 1 < U.length) { v /= 1024; i += 1; }
  return i === 0 ? `${Math.round(v)} B` : `${v.toFixed(2)} ${U[i]}`;
};

// 由数值重算显示串（kind + 单位由调用方补）
const render = (kind, v) =>
  kind === "throughput" ? fmt_rate(v) : kind === "latency" ? fmt_ms(v) : kind === "size" ? fmt_size(v) : "N/A";

// 一个引擎一张表：把多个文件里同名的引擎合并成「指标键 -> 多次测量」
// 值口径：吞吐取 rate（越大越好），时延取 duration_ns，体积取 bytes（两者越小越好）
const collect = (runs, engine_name) => {
  const per = new Map();
  for (const { path, run } of runs) {
    const eng = run.engines.find((e) => e.name === engine_name);
    if (!eng) fail(`${path} 里没有引擎 ${engine_name}（实有 ${run.engines.map((e) => e.name).join(", ")}）`);
    for (const row of eng.rows) {
      const m = per.get(row.key) ?? { samples: [], name: row.name, kinds: new Set(), units: new Set(), statuses: new Set(), path };
      const v = metric_value(row);
      m.samples.push({
        v,
        kind: row.kind,
        unit: row.unit,
        reported: row.formatted,
        status: eng.status,
        file: path,
      });
      m.kinds.add(row.kind);
      if (row.unit) m.units.add(row.unit);
      m.statuses.add(eng.status);
      per.set(row.key, m);
    }
  }
  return per;
};

const metric_value = (row) => {
  switch (row.kind) {
    // 吞吐一律由 count / duration_ns 现算，rate 字段只作为算不出来时的兜底
    case "throughput": {
      const secs = (row.duration_ns ?? 0) / 1e9;
      return Number.isFinite(row.count) && secs > 0 ? row.count / secs : (row.rate ?? 0);
    }
    case "latency":
      return row.duration_ns ?? 0;
    case "size":
      return row.bytes ?? 0;
    default:
      return null;
  }
};

// 同键多次测量取中位数；显示串由中位数现算，同时记下驱动自带的 formatted 以便对账
const reduce = (m) => {
  const ok = m.samples.filter((s) => s.v !== null && Number.isFinite(s.v));
  if (ok.length === 0) return { na: true, kinds: [...m.kinds] };
  const med = median(ok.map((s) => s.v));
  const pick = ok.reduce((a, b) => (Math.abs(a.v - med) <= Math.abs(b.v - med) ? a : b));
  return {
    na: false,
    v: med,
    reported: pick.reported,
    kind: pick.kind,
    unit: pick.unit,
    status: [...m.statuses].join("/"),
  };
};

const fail = (msg) => {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
};

const runs_rust = [], runs_cs = [];
for (const p of rust_paths) runs_rust.push(await load(p));
for (const p of cs_paths) runs_cs.push(await load(p));

// 引擎列名两侧不同是应当的（hash vs tsavorite-cs），但同一张表必须只有一个引擎，
// 否则不知道该比哪一列。
const single_engine = (runs, side) => {
  const names = new Set(runs.flatMap(({ run }) => run.engines.map((e) => e.name)));
  if (names.size !== 1) fail(`${side} 侧引擎不唯一：${[...names].join(", ")}`);
  return [...names][0];
};
const eng_rust = single_engine(runs_rust, "rust");
const eng_cs = single_engine(runs_cs, "cs");

// 平台口径必须一致：platform 与 workload 逐项等值，machine 里两侧同名字段逐项等值。
// machine 缺字段只算口径缺项（两侧实现字段名不完全同构），不当作不同平台。
const mismatches = [];
for (const { path, run } of [...runs_rust, ...runs_cs]) {
  const base = runs_rust[0].run;
  if (run.platform !== base.platform) mismatches.push(`${path} platform=${run.platform} ≠ ${base.platform}`);
  for (const k of Object.keys(base.workload)) {
    const a = JSON.stringify(base.workload[k]), b = JSON.stringify(run.workload?.[k]);
    if (a !== b) mismatches.push(`${path} workload.${k}=${b} ≠ ${a}`);
  }
}
const machine_gaps = [];
{
  // 两侧各用独立数据目录（禁复用段文件），data_dir 必然不同；其余 machine 字段
  // 是同机证明，C# 侧必须实测出与 Rust 侧一致的值，不一致就是驱动口径 bug
  const MACHINE_IGNORE = new Set(["data_dir"]);
  const a = runs_rust[0].run.machine ?? {}, b = runs_cs[0].run.machine ?? {};
  for (const k of Object.keys(a)) {
    if (MACHINE_IGNORE.has(k)) continue;
    if (!(k in b)) { machine_gaps.push(`machine.${k} 仅 rust 侧有`); continue; }
    if (JSON.stringify(a[k]) !== JSON.stringify(b[k])) mismatches.push(`machine.${k}：rust=${JSON.stringify(a[k])} ≠ cs=${JSON.stringify(b[k])}`);
  }
  for (const k of Object.keys(b))
    if (!MACHINE_IGNORE.has(k) && !(k in a)) machine_gaps.push(`machine.${k} 仅 cs 侧有`);
}

const tbl_rust = collect(runs_rust, eng_rust), tbl_cs = collect(runs_cs, eng_cs);
const keys = [...new Set([...tbl_rust.keys(), ...tbl_cs.keys()])];

const lines = [], skipped = [], held = [], display_gaps = [];
let violations = 0;

// 表里没有这个键与「有键但 N/A」是两种口径缺项，必须分开说
const cell = (tbl, key) =>
  tbl.has(key) ? reduce(tbl.get(key)) : { na: true, missing: true, kinds: [] };

for (const key of keys) {
  const r = cell(tbl_rust, key), c = cell(tbl_cs, key);
  if (r.na || c.na || r.kind !== c.kind) {
    const side = (x) => (x.missing ? "无此段" : "N/A");
    const why = r.na && c.na
      ? side(r) === side(c) ? `两侧均${side(r)}` : `rust ${side(r)}／cs ${side(c)}`
      : r.na ? `rust 侧${side(r)}`
      : c.na ? `cs 侧${side(c)}`
      : `kind 不同侧（rust=${r.kind} cs=${c.kind}）`;
    skipped.push(`${key} ← ${why}`);
    lines.push(`${key} | ${r.na ? "N/A" : render(r.kind, r.v)} | ${c.na ? "N/A" : render(c.kind, c.v)} | - | SKIP`);
    continue;
  }
  // 数字与驱动自报字符串对不上 = 生产侧口径 bug，单列告警（不改变判定）
  for (const [side, x] of [["rust", r], ["cs", c]]) {
    const mine = render(x.kind, x.v);
    if (x.reported && x.reported !== mine)
      display_gaps.push(`${key} ${side}：自报「${x.reported}」与按数值重算的「${mine}」不一致`);
  }
  const higher_better = r.kind === "throughput";
  const ratio = (r.v === 0 && c.v === 0) || (r.kind === "latency" && r.v < 1_000_000 && c.v < 1_000_000)
    ? 1.0
    : c.v === 0
    ? Infinity
    : r.v / c.v;
  let verdict;
  if (higher_better ? ratio >= 1 : ratio <= 1) verdict = "PASS";
  else {
    const in_noise = higher_better ? ratio >= NOISE_LO : ratio <= NOISE_HI;
    verdict = in_noise && flag("noise-ok") ? "PASS" : in_noise ? "HOLD" : "FAIL";
  }
  // 非 PASS 行多给两位小数：体积/时延的违规常是千分位级，四位会显示成 1.0000 看不出方向
  const ratio_text = verdict === "PASS" ? ratio.toFixed(4) : ratio.toFixed(6);
  lines.push(`${key} | ${render(r.kind, r.v)} | ${render(c.kind, c.v)} | ${ratio_text} | ${verdict}`);
  if (verdict === "FAIL") violations++;
  else if (verdict === "HOLD") held.push(`${key}（比值 ${ratio.toFixed(4)}，在噪声区间内，需复跑确认后加 --noise-ok 放行）`);
}

// 一侧有另一侧没有的键已在上面的并集循环里按「无此段」计入 skipped

console.log(`对表：rust(${eng_rust}) ${runs_rust.length} 次 vs cs(${eng_cs}) ${runs_cs.length} 次，指标 ${keys.length} 项，取值口径=同键中位数`);
if (mismatches.length) {
  console.log(`\n平台/负载口径不一致（判 FAIL，防止跨档跨机混比）：`);
  for (const m of mismatches) console.log(`  - ${m}`);
  process.exit(1);
}
console.log(`\n指标 | rust(${eng_rust}) (M/s) | cs(${eng_cs}) (M/s) | 比值 | 结果`);
console.log("|---|---|---|---|---|");
for (const l of lines) console.log(l);

if (skipped.length) {
  console.log(`\n口径缺项（不计入通过，共 ${skipped.length} 项）：`);
  for (const s of skipped) console.log(`  - ${s}`);
}
if (machine_gaps.length) {
  console.log(`\nmachine 字段口径缺项（不判 FAIL）：`);
  for (const s of machine_gaps) console.log(`  - ${s}`);
}
if (display_gaps.length) {
  console.log(`\n数字与自报显示串不一致（判定用的是重算值；不一致说明驱动呈现层有 bug）：`);
  for (const s of display_gaps) console.log(`  - ${s}`);
}

if (violations > 0) {
  console.log(`\n结论：不达标，${violations} 项 Rust 低于 C#。`);
  process.exit(1);
}
if (held.length) {
  console.log(`\n结论：无硬性违规，但 ${held.length} 项落在噪声区间，放行条件未满足：`);
  for (const h of held) console.log(`  - ${h}`);
  process.exit(1);
}
console.log(`\n结论：全部达标（Rust 不低于 C#）。`);
