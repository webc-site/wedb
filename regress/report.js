import { resolve } from "node:path";

const BASE_DIR = import.meta.dirname,
  CONFIG_PATH = resolve(BASE_DIR, "config.json"),
  DEFAULT_CONFIG = {
    max_history: 20,
    display_commits: 5,
    regression_threshold_pct: 10.0,
    data_file: "data/history.json",
    markdown_output: "README.md",
  },
  ANSI_RESET = "\x1b[0m",
  ANSI_BOLD = "\x1b[1m",
  ANSI_GREEN = "\x1b[32m",
  ANSI_RED = "\x1b[31m",
  ANSI_CYAN = "\x1b[36m",
  ANSI_GRAY = "\x1b[90m";

const ENGINE_TABLES_LI = [
  {
    name: "wkv",
    metric_li: [
      { key: "wkv_upsert", label: "单点写入 (Upsert)" },
      { key: "wkv_get_hot", label: "热点点查 (Get Hot)" },
      { key: "wkv_delete", label: "单点删除 (Delete)" },
    ],
  },
  {
    name: "wbftree",
    metric_li: [
      { key: "wbftree_insert", label: "单点插入 (Insert)" },
      { key: "wbftree_read", label: "单点点查 (Read)" },
      { key: "wbftree_scan_10", label: "范围扫描 (Range Scan 10)" },
      { key: "wbftree_delete", label: "单点删除 (Delete)" },
    ],
  },
];

const configLoad = async () => {
  const file = Bun.file(CONFIG_PATH);
  if (await file.exists()) {
    try {
      const custom = await file.json();
      return { ...DEFAULT_CONFIG, ...custom };
    } catch (_) {
      return DEFAULT_CONFIG;
    }
  }
  return DEFAULT_CONFIG;
};

const stripAnsi = (str) => str.replace(/\x1b\[[0-9;]*m/g, "");

const charDisplayWidth = (code) => {
  if (code < 128) return 1;
  if (
    (code >= 0x1100 && code <= 0x115f) ||
    (code >= 0x2e80 && code <= 0xa4cf) ||
    (code >= 0xac00 && code <= 0xd7a3) ||
    (code >= 0xf900 && code <= 0xfaff) ||
    (code >= 0xfe10 && code <= 0xfe19) ||
    (code >= 0xfe30 && code <= 0xfe6f) ||
    (code >= 0xff00 && code <= 0xff60) ||
    (code >= 0xffe0 && code <= 0xffe6)
  ) {
    return 2;
  }
  return 1;
};

const stringDisplayWidth = (str) => {
  const clean = stripAnsi(str);
  let w = 0;
  for (let i = 0; i < clean.length; i++) {
    w += charDisplayWidth(clean.charCodeAt(i));
  }
  return w;
};

const padAnsi = (str, target_len, pad_start = true) => {
  const visible_len = stringDisplayWidth(str);
  if (visible_len >= target_len) return str;
  const pad = " ".repeat(target_len - visible_len);
  return pad_start ? pad + str : str + pad;
};

const numFormat = (num) => {
  if (num === undefined || num === null || Number.isNaN(num)) return "-";
  if (num >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(2)}B`;
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(2)}K`;
  return num.toFixed(1);
};

const diffCalc = (curr, prev) => {
  if (prev === undefined || prev === null || prev === 0 || !curr) return null;
  return ((curr - prev) / prev) * 100;
};

const diffFormatAnsi = (pct, higher_is_better = true) => {
  if (pct === null) return `${ANSI_GRAY}(base)${ANSI_RESET}`;
  const sign = pct >= 0 ? "+" : "",
    val_str = `${sign}${pct.toFixed(1)}%`,
    good = higher_is_better ? pct >= 0 : pct <= 0;
  if (Math.abs(pct) < 1.0) {
    return `${ANSI_GRAY}${val_str}${ANSI_RESET}`;
  }
  if (good) {
    return `${ANSI_GREEN}${val_str} ↑${ANSI_RESET}`;
  }
  return `${ANSI_RED}${val_str} ↓${ANSI_RESET}`;
};

const diffFormatMd = (pct, higher_is_better = true) => {
  if (pct === null) return "-";
  const sign = pct >= 0 ? "+" : "",
    val_str = `${sign}${pct.toFixed(1)}%`,
    good = higher_is_better ? pct >= 0 : pct <= 0;
  if (Math.abs(pct) < 1.0) return val_str;
  return good ? `**${val_str}** 🟢` : `**${val_str}** 🔴`;
};

const historyLoad = async (history_path) => {
  const file = Bun.file(history_path);
  if (await file.exists()) {
    try {
      const data = await file.json();
      return Array.isArray(data) ? data : [];
    } catch (_) {
      return [];
    }
  }
  return [];
};

const historySave = async (history_path, history_li) => {
  await Bun.write(history_path, JSON.stringify(history_li, null, 2) + "\n");
};

const historyMerge = (history_li, current_record, max_history) => {
  const copy_li = [...history_li],
    idx = copy_li.findIndex((r) => r.commit === current_record.commit);
  if (idx >= 0) {
    copy_li[idx] = current_record;
  } else {
    copy_li.push(current_record);
  }
  if (copy_li.length > max_history) {
    return copy_li.slice(copy_li.length - max_history);
  }
  return copy_li;
};

const terminalReportPrint = (display_li, threshold_pct) => {
  console.log(`\n${ANSI_BOLD}WeDB 性能回归测试报告 (最近 ${display_li.length} 次提交)${ANSI_RESET}\n`);

  console.log(`${ANSI_BOLD}提交版本列表:${ANSI_RESET}`);
  display_li.forEach((rec, i) => {
    const is_latest = i === display_li.length - 1,
      tag = is_latest ? `${ANSI_GREEN}* (当前 HEAD)${ANSI_RESET}` : " ";
    console.log(
      `  ${tag} ${ANSI_CYAN}${rec.commit}${ANSI_RESET}  ${rec.date}  ${ANSI_GRAY}${rec.message || "(no msg)"}${ANSI_RESET}`
    );
  });

  const col_width = 20,
    label_width = 28;

  let has_regression = false,
    regression_warnings_li = [];

  for (const group of ENGINE_TABLES_LI) {
    console.log(`\n${ANSI_BOLD}${ANSI_CYAN}${group.name}${ANSI_RESET}`);
    const header_str =
      padAnsi("评测指标全称", label_width, false) +
      display_li.map((r) => padAnsi(r.commit, col_width, true)).join("");
    console.log(header_str);

    for (const m of group.metric_li) {
      let row_str = padAnsi(m.label, label_width, false);
      for (let i = 0; i < display_li.length; i++) {
        const curr_rec = display_li[i],
          val = curr_rec.metrics?.[m.key]?.ops ?? 0,
          prev_rec = i > 0 ? display_li[i - 1] : null,
          prev_val = prev_rec?.metrics?.[m.key]?.ops ?? 0,
          diff = prev_rec ? diffCalc(val, prev_val) : null;

        if (i === display_li.length - 1 && diff !== null && diff < -threshold_pct) {
          has_regression = true;
          regression_warnings_li.push(
            `⚠️ ${group.name} - ${m.label} 较上个提交下降了 ${Math.abs(diff).toFixed(1)}% (阈值 ${threshold_pct}%)`
          );
        }

        const diff_str = diffFormatAnsi(diff, true),
          val_str = `${numFormat(val)} ${diff_str}`;
        row_str += padAnsi(val_str, col_width, true);
      }
      console.log(row_str);
    }
  }

  if (has_regression) {
    console.log(`\n${ANSI_BOLD}${ANSI_RED}❌ 检测到性能显著回退警告 (Regression Alert):${ANSI_RESET}`);
    for (const warn of regression_warnings_li) {
      console.log(`  ${ANSI_RED}${warn}${ANSI_RESET}`);
    }
  } else {
    console.log(`\n${ANSI_BOLD}${ANSI_GREEN}✅ 性能平稳，未触发回退警报 (阈值: -${threshold_pct}%)${ANSI_RESET}`);
  }
  console.log("");

  return !has_regression;
};

const markdownReportGenerate = (display_li, threshold_pct) => {
  let md = `# WeDB 性能回归演进报告\n\n`;
  md += `> 自动生成时间: ${new Date().toISOString()}  \n`;
  md += `> 回归警戒阈值: 下降超过 **${threshold_pct}%**  \n\n`;

  md += `## 提交版本列表\n\n`;
  display_li.forEach((r, i) => {
    const is_curr = i === display_li.length - 1 ? " (当前 HEAD)" : "";
    md += `- \`${r.commit}\`${is_curr} · ${r.date} · ${r.message || "-"}\n`;
  });
  md += `\n`;

  for (const group of ENGINE_TABLES_LI) {
    md += `## ${group.name} 性能演进 (吞吐 ops/s)\n\n`;
    md += `| 评测指标全称 | ` + display_li.map((r) => `\`${r.commit}\``).join(" | ") + ` | 较前次变动 |\n`;
    md += `| :--- | ` + display_li.map(() => `:---:`).join(" | ") + ` | :---: |\n`;

    for (const m of group.metric_li) {
      const last_idx = display_li.length - 1,
        curr_val = display_li[last_idx].metrics?.[m.key]?.ops ?? 0,
        prev_val = last_idx > 0 ? display_li[last_idx - 1].metrics?.[m.key]?.ops ?? 0 : null,
        latest_diff = prev_val ? diffCalc(curr_val, prev_val) : null;

      const row_vals = display_li.map((r) => {
        const v = r.metrics?.[m.key]?.ops ?? 0;
        return numFormat(v);
      });

      md += `| **${m.label}** | ${row_vals.join(" | ")} | ${diffFormatMd(latest_diff, true)} |\n`;
    }
    md += `\n`;

    md += `### ${group.name} 延迟表现 (平均延迟 μs)\n\n`;
    md += `| 评测指标全称 | ` + display_li.map((r) => `\`${r.commit}\``).join(" | ") + ` |\n`;
    md += `| :--- | ` + display_li.map(() => `:---:`).join(" | ") + ` |\n`;

    for (const m of group.metric_li) {
      const row_vals = display_li.map((r) => {
        const lat = r.metrics?.[m.key]?.latency_us;
        return lat !== undefined ? `${lat.toFixed(3)} μs` : "-";
      });
      md += `| **${m.label}** | ${row_vals.join(" | ")} |\n`;
    }
    md += `\n`;
  }

  return md;
};

const run = async () => {
  const config = await configLoad(),
    history_path = resolve(BASE_DIR, config.data_file),
    latest_file_path = resolve(BASE_DIR, "data/latest.json"),
    latest_file = Bun.file(latest_file_path);

  if (!(await latest_file.exists())) {
    console.error(`未找到当前指标采集文件: ${latest_file_path}`);
    process.exit(1);
  }

  const current_record = await latest_file.json(),
    history_li = await historyLoad(history_path),
    merged_history_li = historyMerge(history_li, current_record, config.max_history);

  await historySave(history_path, merged_history_li);

  const display_count = Math.min(config.display_commits, merged_history_li.length),
    display_li = merged_history_li.slice(merged_history_li.length - display_count),
    is_ok = terminalReportPrint(display_li, config.regression_threshold_pct),
    md_content = markdownReportGenerate(display_li, config.regression_threshold_pct),
    md_path = resolve(BASE_DIR, config.markdown_output);

  await Bun.write(md_path, md_content);

  if (!is_ok) {
    process.exit(2);
  }
};

await run();
