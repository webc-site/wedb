// 站点数据的读取与刻度格式化：表格显示文本直接用工厂端（Rust）产出的 formatted 字段，
// 图表才用数值，避免在 JS 里重抄一遍表格口径。

export const ENGINE_COLORS = {
  hash: "#0969da",
  bftree: "#1a7f37",
  fjall: "#bc4c00",
  rocksdb: "#8250df",
  sqlite: "#cf222e",
};

export const PLATFORM_NAMES = {
  "linux-x64": "Linux · x64",
  "linux-arm64": "Linux · ARM64",
  "macos-arm64": "macOS · ARM64",
  "macos-x64": "macOS · x64",
  "windows-x64": "Windows · x64",
  "windows-arm64": "Windows · ARM64",
};

export function engineColor(name) {
  return ENGINE_COLORS[name] ?? "#57606a";
}

export function platformLabel(platform) {
  return PLATFORM_NAMES[platform] ?? platform;
}

/// 全部 run 按时间升序展平
export function allRuns(history) {
  const runs = (history?.reports ?? []).flatMap((report) => report.runs ?? []);
  return runs.slice().sort((a, b) => a.generated_at_unix - b.generated_at_unix);
}

/// 每个平台最近一次 run：最新表格与柱状图的口径来源
export function latestRuns(history) {
  const by_platform = new Map();
  for (const run of allRuns(history)) {
    by_platform.set(run.platform, run);
  }
  return [...by_platform.values()];
}

/// 表的行序以该平台的第一个活列为准（同一 harness 产出，各列同行序）
export function rowSpecs(run) {
  const engine = run.engines.find((item) => item.status === "ok") ?? run.engines[0];
  return (engine?.rows ?? []).map((row) => ({ key: row.key, name: row.name, unit: row.unit }));
}

export function rowsByName(run) {
  const map = new Map();
  for (const engine of run.engines) {
    map.set(engine.name, new Map(engine.rows.map((row) => [row.key, row])));
  }
  return map;
}

/// 图表口径：只画吞吐段（key/s · txn/s · scan/s）。时延和占用绝对量级不同，
/// 混进同一根轴会互相压扁，留在表格里对比。
/// 选项按各列并集取：某一段在参考列里是 N/A 时，别列的实测值仍然要能选出来
export function throughputOptions(run) {
  const seen = new Map();
  for (const engine of run?.engines ?? []) {
    for (const row of engine.rows) {
      if (row.kind === "throughput" && !seen.has(row.key)) {
        seen.set(row.key, { key: row.key, label: `${row.name} (${row.unit})`, unit: row.unit });
      }
    }
  }
  return [...seen.values()];
}

/// 吞吐单元格的可比数值；非吞吐（时延/占用/N-A）一律不进图
export function throughputValue(row) {
  return row?.kind === "throughput" ? row.rate : null;
}

export function shortCommit(commit) {
  return commit && commit !== "local" ? commit.slice(0, 7) : "本地";
}

/// 版本标注：CI 记进机读结果的 version 优先（有 tag 就是 vX.Y.Z），
/// 本地跑或历史里没这个字段时退回 commit 短哈希
export function versionLabel(run) {
  if (!run) return "—";
  return run.version || shortCommit(run.commit);
}

/// 趋势轴上的版本日期：光有 commit 认不出版本先后
export function formatDay(unix) {
  if (!unix) return "—";
  return new Date(unix * 1000).toISOString().slice(0, 10);
}

/// 条目数量的口语写法：百万以上用 M，其余用 K，避免 0.0M 这种看不出量级的读数
export function formatCount(count) {
  if (!Number.isFinite(count)) return "—";
  if (count >= 1e6) return `${(count / 1e6).toFixed(count % 1e6 === 0 ? 0 : 1)}M`;
  if (count >= 1e3) return `${Math.round(count / 1e3)}K`;
  return `${count}`;
}

const UNITS = ["", "K", "M", "G", "T"];

/// 与 Rust 侧 format_rate 同一量级阶梯的紧凑刻度（轴标签用，不做三档有效数字）
export function formatTick(value) {
  if (!Number.isFinite(value)) return "—";
  if (value === 0) return "0";
  let index = 0;
  let scaled = Math.abs(value);
  while (scaled >= 1000 && index < UNITS.length - 1) {
    scaled /= 1000;
    index += 1;
  }
  const digits = scaled >= 100 ? 0 : scaled >= 10 ? 1 : 2;
  return `${scaled.toFixed(digits)}${UNITS[index]}`;
}

export function formatBytesValue(bytes) {
  if (!Number.isFinite(bytes)) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = bytes;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
}

export function formatUnix(unix) {
  if (!unix) return "—";
  return new Date(unix * 1000).toISOString().slice(0, 16).replace("T", " ");
}

export function formatGib(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) return "—";
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GiB`;
}
