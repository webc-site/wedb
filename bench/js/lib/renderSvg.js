import { metricByKey, metricSummary } from "./data.js";

// 配色参考 fastalp：浅色高雅明亮风格，无边框
const ENGINE_STYLE = {
  wkv: {
    grad_id: "grad_wkv",
    grad_start: "#3b82f6",
    grad_end: "#1d4ed8",
    color: "#1d4ed8",
    badge: "wedb",
    badge_bg: "#2563eb",
    badge_color: "#ffffff",
  },
  wbftree: {
    grad_id: "grad_wbftree",
    grad_start: "#a855f7",
    grad_end: "#6d28d9",
    color: "#6d28d9",
    badge: "wedb",
    badge_bg: "#7c3aed",
    badge_color: "#ffffff",
  },
  redb: {
    grad_id: "grad_redb",
    grad_start: "#fb923c",
    grad_end: "#ea580c",
    color: "#c2410c",
    badge: "",
    badge_bg: "#cbd5e1",
    badge_color: "#475569",
  },
  fjall: {
    grad_id: "grad_fjall",
    grad_start: "#34d399",
    grad_end: "#059669",
    color: "#047857",
    badge: "",
    badge_bg: "#cbd5e1",
    badge_color: "#475569",
  },
  rocksdb: {
    grad_id: "grad_rocksdb",
    grad_start: "#facc15",
    grad_end: "#ca8a04",
    color: "#a16207",
    badge: "",
    badge_bg: "#cbd5e1",
    badge_color: "#475569",
  },
  sqlite: {
    grad_id: "grad_sqlite",
    grad_start: "#94a3b8",
    grad_end: "#64748b",
    color: "#475569",
    badge: "",
    badge_bg: "#cbd5e1",
    badge_color: "#475569",
  },
},
xmlEscape = (str) => {
  if (typeof str !== "string") return str ?? "";
  return str
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
},
barWidthByRatio = (val, min_val, max_val, max_width = 300) => {
  if (!val || val <= 0 || !max_val || max_val <= 0) return 6;
  const ratio = val / (min_val > 0 ? min_val : 1),
    max_ratio = max_val / (min_val > 0 ? min_val : 1);

  if (max_ratio > 30) {
    const log_ratio = Math.log10(ratio + 1),
      log_max = Math.log10(max_ratio + 1),
      pct = Math.max(0.04, log_ratio / log_max);
    return Math.round(pct * max_width);
  }
  const pct = Math.max(0.05, val / max_val);
  return Math.round(pct * max_width);
},
// 文本折行辅助函数
textWrap = (text, max_chars = 52) => {
  const line_li = [];
  let cur = "";
  for (const char of text) {
    cur += char;
    if (cur.length >= max_chars && (char === " " || char === "，" || char === "；" || char === "。")) {
      line_li.push(cur.trim());
      cur = "";
    }
  }
  if (cur.trim().length > 0) {
    line_li.push(cur.trim());
  }
  return line_li;
},
metricGroupRender = (
  engine_li,
  metric_key,
  metric_title,
  unit_label,
  start_x,
  start_y,
  content_w,
  is_size_metric = false,
  lang = "zh"
) => {
  const summary = metricSummary(engine_li, metric_key),
    row_h = 28,
    bar_track_w = 310,
    bar_x = start_x + 140;

  let group_svg = `
    <!-- Metric Header -->
    <g class="metric-head" transform="translate(0, ${start_y})">
      <circle cx="${start_x + 6}" cy="10" r="3.5" fill="#2563eb"/>
      <text x="${start_x + 16}" y="14" font-size="13.5" font-weight="700" fill="#0f172a">${xmlEscape(metric_title)}</text>
      <text x="${start_x + content_w - 6}" y="14" font-size="11" font-weight="500" fill="#64748b" text-anchor="end">${xmlEscape(unit_label)}</text>
    </g>
  `;

  engine_li.forEach((eng, idx) => {
    const m = metricByKey(eng, metric_key),
      row_y = start_y + 24 + idx * row_h,
      style = ENGINE_STYLE[eng.name] ?? ENGINE_STYLE.sqlite,
      is_best = summary.best_idx === idx,
      is_wedb = eng.name === "wkv" || eng.name === "wbftree",
      is_na = !m || m.type === "na";

    let bar_w = 6,
      mult_str = "1.00X",
      val_str = is_na ? "N/A" : m.formatted;

    if (!is_na) {
      if (m.type === "throughput") {
        bar_w = barWidthByRatio(m.rate, summary.min_rate, summary.max_rate, bar_track_w);
        if (summary.min_rate > 0 && m.rate > 0) {
          mult_str = (m.rate / summary.min_rate).toFixed(2) + "X";
        }
      } else if (m.type === "latency") {
        const dur = Math.max(0.001, m.duration_ms ?? 0),
          inv_val = 1 / dur,
          inv_min = summary.max_lat > 0 ? 1 / summary.max_lat : 0,
          inv_max = summary.min_lat > 0 ? 1 / Math.max(0.001, summary.min_lat) : 0;
        bar_w = barWidthByRatio(inv_val, inv_min, inv_max, bar_track_w);
        if (summary.max_lat > 0) {
          mult_str = (summary.max_lat / dur).toFixed(2) + "X";
        }
      } else if (m.type === "size") {
        const min_bytes = summary.min_bytes || 1,
          max_bytes = summary.max_bytes || 1;
        // 内存与磁盘占用：物理大小越小越短，越小越优；最小的标为领先加星
        bar_w = barWidthByRatio(m.bytes, min_bytes, max_bytes, bar_track_w);
        if (min_bytes > 0 && m.bytes > 0) {
          const ratio = (m.bytes / min_bytes).toFixed(2);
          mult_str = is_best ? (lang === "zh" ? "★ 最小" : "★ Min") : ratio + "X";
        }
      }
    }

    // 无边框行背景与装饰元素
    const row_bg = is_best
        ? `<rect x="${start_x}" y="${row_y - 2}" width="${content_w}" height="${row_h - 2}" rx="6" fill="#eff6ff"/>`
        : idx % 2 === 1
        ? `<rect x="${start_x}" y="${row_y - 2}" width="${content_w}" height="${row_h - 2}" rx="6" fill="#f8fafc"/>`
        : "",
      wedb_badge = style.badge
        ? `<rect x="${start_x + 72}" y="${row_y + 1}" width="34" height="15" rx="3.5" fill="${style.badge_bg}"/><text x="${start_x + 89}" y="${row_y + 12}" font-size="9" font-weight="700" fill="#ffffff" text-anchor="middle">${style.badge}</text>`
        : "",
      star_x = start_x + (style.badge ? 116 : 76),
      star_svg = is_best
        ? `<path d="M 0,-5.8 L 1.7,-1.8 L 6.0,-1.5 L 2.8,1.4 L 3.8,5.5 L 0,3.1 L -3.8,5.5 L -2.8,1.4 L -6.0,-1.5 L -1.7,-1.8 Z" fill="#ea580c" transform="translate(${star_x}, ${row_y + 8.5})"/>`
        : "",
      bar_elem = is_na
        ? `<text x="${bar_x + 8}" y="${row_y + 14}" font-size="11.5" font-weight="500" fill="#94a3b8">N/A</text>`
        : `
        <!-- Track (无边框) -->
        <rect x="${bar_x}" y="${row_y + 4}" width="${bar_track_w}" height="12" rx="6" fill="#f1f5f9"/>
        <!-- Fill Bar -->
        <rect x="${bar_x}" y="${row_y + 4}" width="${Math.max(bar_w, 8)}" height="12" rx="6" fill="url(#${style.grad_id})"/>
        <!-- Values & Multipliers -->
        <text x="${bar_x + bar_track_w + 12}" y="${row_y + 14}" font-size="12" font-weight="${is_best ? "800" : "600"}" fill="${is_best ? "#1d4ed8" : "#1e293b"}">${xmlEscape(val_str)}</text>
        <text x="${start_x + content_w - 8}" y="${row_y + 14}" font-size="11.5" font-weight="${is_best ? "800" : "500"}" fill="${is_best ? "#ea580c" : "#64748b"}" text-anchor="end">${xmlEscape(mult_str)}</text>
      `;

    group_svg += `
      <g class="bar-row">
        ${row_bg}
        <!-- Engine Name -->
        <text x="${start_x + 10}" y="${row_y + 13}" font-size="12.5" font-weight="${is_wedb ? "800" : "600"}" fill="${is_wedb ? style.color : "#334155"}">${xmlEscape(eng.name)}</text>
        ${wedb_badge}
        ${star_svg}
        ${bar_elem}
      </g>
    `;
  });

  const total_group_h = 30 + engine_li.length * row_h;
  return [group_svg, total_group_h];
};

export const svgRender = (bench_data, i18n, lang = "zh") => {
  const { machine, config, engines: engine_li } = bench_data,
    svg_w = 760,
    margin = 20,
    content_w = svg_w - 2 * margin,
    card_pad = 16,
    read_metric_li = [
      { key: "random_reads", title: i18n.benchmarks.random_reads, unit: lang === "zh" ? "点查吞吐 · 越高越优" : "Point Read · Higher is better" },
      { key: "random_range_reads", title: i18n.benchmarks.random_range_reads, unit: lang === "zh" ? "范围扫描 · 越高越优" : "Range Scan · Higher is better" },
      { key: "random_reads_8", title: i18n.benchmarks.random_reads_8, unit: lang === "zh" ? "并发读吞吐 · 越高越优" : "8-Thread Read · Higher is better" },
      { key: "random_reads_4", title: i18n.benchmarks.random_reads_4, unit: lang === "zh" ? "并发读吞吐 · 越高越优" : "4-Thread Read · Higher is better" },
    ],
    write_metric_li = [
      { key: "individual_writes", title: i18n.benchmarks.individual_writes, unit: lang === "zh" ? "逐笔写入 · 越高越优" : "Individual Write · Higher is better" },
      { key: "bulk_load", title: i18n.benchmarks.bulk_load, unit: lang === "zh" ? "批量导入 · 越高越优" : "Bulk Load · Higher is better" },
      { key: "batch_writes", title: i18n.benchmarks.batch_writes, unit: lang === "zh" ? "事务写入 · 越高越优" : "Batch Write · Higher is better" },
      { key: "removals", title: i18n.benchmarks.removals, unit: lang === "zh" ? "记录删除 · 越高越优" : "Removals · Higher is better" },
    ],
    space_metric_li = [
      { key: "uncompacted_size", title: i18n.benchmarks.uncompacted_size, unit: lang === "zh" ? "磁盘占用 · 越小越优" : "Disk Usage · Lower is better", is_size: true },
      { key: "memory", title: i18n.benchmarks.memory ?? i18n.benchmarks.peak_memory, unit: lang === "zh" ? "内存占用 · 越小越优" : "Memory Usage · Lower is better", is_size: true },
    ];

  let cur_y = 20;

  // 1. 标题区：顶部别加高亮的线，标题别加背景色
  const title_text = xmlEscape(i18n.title),
    subtitle_text = xmlEscape(i18n.subtitle ?? "wkv 与 wbftree 对比主流嵌入式引擎综合性能评测"),
    header_svg = `
    <!-- Header (无背景色，无顶部高亮线) -->
    <g class="header" transform="translate(${margin}, ${cur_y})">
      <text x="4" y="24" font-size="21" font-weight="800" fill="#0f172a" letter-spacing="0.3">${title_text}</text>
      <text x="4" y="46" font-size="13" font-weight="500" fill="#64748b">${subtitle_text}</text>
    </g>
  `;
  cur_y += 58;

  // 2. 图例放开头
  const legend_svg = `
    <!-- Legend (放开头，无边框) -->
    <g class="legend" transform="translate(${margin}, ${cur_y})">
      <rect x="0" y="0" width="${content_w}" height="36" rx="8" fill="#f8fafc"/>
      <circle cx="20" cy="18" r="4.5" fill="#2563eb"/>
      <text x="30" y="22" font-size="11.5" font-weight="700" fill="#0f172a">wkv</text>
      <circle cx="85" cy="18" r="4.5" fill="#7c3aed"/>
      <text x="95" y="22" font-size="11.5" font-weight="700" fill="#0f172a">wbftree</text>
      <circle cx="170" cy="18" r="4.5" fill="#ea580c"/>
      <text x="180" y="22" font-size="11.5" font-weight="600" fill="#334155">redb</text>
      <circle cx="240" cy="18" r="4.5" fill="#059669"/>
      <text x="250" y="22" font-size="11.5" font-weight="600" fill="#334155">fjall</text>
      <circle cx="305" cy="18" r="4.5" fill="#ca8a04"/>
      <text x="315" y="22" font-size="11.5" font-weight="600" fill="#334155">rocksdb</text>
      <circle cx="380" cy="18" r="4.5" fill="#64748b"/>
      <text x="390" y="22" font-size="11.5" font-weight="600" fill="#334155">sqlite</text>
      <path d="M 0,-4.8 L 1.4,-1.5 L 4.8,-1.2 L 2.3,1.1 L 3.0,4.4 L 0,2.5 L -3.0,4.4 L -2.3,1.1 L -4.8,-1.2 L -1.4,-1.5 Z" fill="#ea580c" transform="translate(${content_w - 90}, 18)"/>
      <text x="${content_w - 78}" y="22" font-size="11" font-weight="700" fill="#ea580c">${xmlEscape(i18n.chart.lead ?? "领先")}</text>
    </g>
  `;
  cur_y += 48;

  // Helper for rendering section card (无边框，浅色底卡)
  const cardRender = (title, metric_li) => {
    const card_start_y = cur_y;
    let card_inner_y = card_start_y + 36,
      card_content_svg = "";

    metric_li.forEach((m_def) => {
      const [grp_svg, grp_h] = metricGroupRender(
        engine_li,
        m_def.key,
        m_def.title,
        m_def.unit,
        margin + card_pad,
        card_inner_y,
        content_w - 2 * card_pad,
        m_def.is_size,
        lang
      );
      card_content_svg += grp_svg;
      card_inner_y += grp_h + 6;
    });

    const card_h = card_inner_y - card_start_y + 8;
    cur_y = card_inner_y + 14;

    return `
      <!-- Card Section (无边框) -->
      <g class="card">
        <rect x="${margin}" y="${card_start_y}" width="${content_w}" height="${card_h}" rx="10" fill="#ffffff"/>
        <text x="${margin + card_pad}" y="${card_start_y + 22}" font-size="15" font-weight="800" fill="#0f172a" letter-spacing="0.2">${xmlEscape(title)}</text>
        ${card_content_svg}
      </g>
    `;
  };

  // 3. 读取性能（读放到写前面）
  const card1_svg = cardRender(i18n.chart.read_section, read_metric_li),
    // 4. 写入性能
    card2_svg = cardRender(i18n.chart.write_section, write_metric_li),
    // 5. 存储与内存
    card3_svg = cardRender(i18n.chart.space_section, space_metric_li),
    // 6. 机器配置放最后，配置放图的结尾，加上描述 (无边框)
    cache_mib = Math.round(config.cache_size / (1024 * 1024)),
    cfg_card_y = cur_y,
    note_line_li = textWrap(i18n.notes, lang === "zh" ? 48 : 72),
    col_w = (content_w - 2 * card_pad) / 4,
    cfg_grid_y = cfg_card_y + 36,
    cfg_item_li = [
      { label: i18n.key_size, val: config.key_size + " B" },
      { label: i18n.value_size, val: config.value_size + " B" },
      { label: i18n.cache_size, val: cache_mib + " MiB" },
      { label: i18n.elements, val: config.bulk_elements.toLocaleString() },
    ],
    cfg_grid_svg = cfg_item_li
      .map((item, idx) => {
        const cx = margin + card_pad + idx * col_w;
        return `
        <rect x="${cx}" y="${cfg_grid_y}" width="${col_w - 8}" height="42" rx="6" fill="#f8fafc"/>
        <text x="${cx + 10}" y="${cfg_grid_y + 17}" font-size="11" font-weight="500" fill="#64748b">${xmlEscape(item.label)}</text>
        <text x="${cx + 10}" y="${cfg_grid_y + 34}" font-size="13" font-weight="700" fill="#0f172a">${xmlEscape(item.val)}</text>
      `;
      })
      .join(""),
    mach_grid_y = cfg_grid_y + 50,
    mach_item_li = [
      { label: i18n.cpu, val: machine.cpu_brand },
      { label: i18n.cores, val: machine.physical_cores + " 物理 / " + machine.logical_cores + " 逻辑" },
      { label: i18n.memory, val: machine.total_memory_gib.toFixed(1) + " GiB RAM" },
      { label: i18n.disk_type, val: machine.disk_type },
    ],
    mach_grid_svg = mach_item_li
      .map((item, idx) => {
        const cx = margin + card_pad + idx * col_w;
        return `
        <rect x="${cx}" y="${mach_grid_y}" width="${col_w - 8}" height="42" rx="6" fill="#f8fafc"/>
        <text x="${cx + 10}" y="${mach_grid_y + 17}" font-size="11" font-weight="500" fill="#64748b">${xmlEscape(item.label)}</text>
        <text x="${cx + 10}" y="${mach_grid_y + 34}" font-size="12" font-weight="700" fill="#0f172a">${xmlEscape(item.val)}</text>
      `;
      })
      .join("");

  let desc_cur_y = mach_grid_y + 54,
    params_desc_svg = "";

  const params_desc_li = i18n.params_desc ?? [];
  if (params_desc_li.length > 0) {
    params_desc_svg += `<text x="${margin + card_pad}" y="${desc_cur_y + 14}" font-size="13" font-weight="700" fill="#0f172a">${xmlEscape(i18n.params_desc_title ?? "配置参数说明")}</text>`;
    desc_cur_y += 24;

    for (const item of params_desc_li) {
      const full_text = item.label + "：" + item.desc,
        line_li = textWrap(full_text, lang === "zh" ? 48 : 72);
      for (let i = 0; i < line_li.length; ++i) {
        const line = line_li[i];
        if (i === 0) {
          params_desc_svg += `
            <circle cx="${margin + card_pad + 6}" cy="${desc_cur_y + 10}" r="2.5" fill="#3b82f6"/>
            <text x="${margin + card_pad + 16}" y="${desc_cur_y + 14}" font-size="11.5" font-weight="500" fill="#334155">${xmlEscape(line)}</text>
          `;
        } else {
          params_desc_svg += `
            <text x="${margin + card_pad + 16}" y="${desc_cur_y + 14}" font-size="11.5" font-weight="500" fill="#64748b">${xmlEscape(line)}</text>
          `;
        }
        desc_cur_y += 18;
      }
      desc_cur_y += 2;
    }
    desc_cur_y += 6;
  }

  const notes_y = desc_cur_y,
    notes_svg = note_line_li
      .map(
        (line, idx) =>
          `<text x="${margin + card_pad + 6}" y="${notes_y + 14 + idx * 18}" font-size="11" font-weight="500" fill="#94a3b8">${xmlEscape(line)}</text>`
      )
      .join(""),
    cfg_card_h = notes_y + 14 + note_line_li.length * 18 + 14 - cfg_card_y,
    config_section_svg = `
    <!-- Benchmark Parameters, Machine Spec & Description (机器配置与评测说明置底，无边框) -->
    <g class="config-card">
      <rect x="${margin}" y="${cfg_card_y}" width="${content_w}" height="${cfg_card_h}" rx="10" fill="#ffffff"/>
      <text x="${margin + card_pad}" y="${cfg_card_y + 22}" font-size="14.5" font-weight="800" fill="#0f172a">${xmlEscape(i18n.params_title)} ＆ ${xmlEscape(i18n.system_info_title)}</text>
      ${cfg_grid_svg}
      ${mach_grid_svg}
      ${params_desc_svg}
      ${notes_svg}
    </g>
  `,
    total_h = cfg_card_y + cfg_card_h + 16;

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${svg_w} ${total_h}" width="${svg_w}" height="${total_h}">
  <defs>
    <linearGradient id="grad_wkv" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#3b82f6"/>
      <stop offset="100%" stop-color="#1d4ed8"/>
    </linearGradient>
    <linearGradient id="grad_wbftree" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#a855f7"/>
      <stop offset="100%" stop-color="#6d28d9"/>
    </linearGradient>
    <linearGradient id="grad_redb" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#fb923c"/>
      <stop offset="100%" stop-color="#ea580c"/>
    </linearGradient>
    <linearGradient id="grad_fjall" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#34d399"/>
      <stop offset="100%" stop-color="#059669"/>
    </linearGradient>
    <linearGradient id="grad_rocksdb" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#facc15"/>
      <stop offset="100%" stop-color="#ca8a04"/>
    </linearGradient>
    <linearGradient id="grad_sqlite" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#94a3b8"/>
      <stop offset="100%" stop-color="#64748b"/>
    </linearGradient>
  </defs>
  <style>
    text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }
  </style>
  <!-- Background Canvas: 白色背景，无边框 -->
  <rect width="100%" height="100%" fill="#ffffff"/>
  ${header_svg}
  ${legend_svg}
  ${card1_svg}
  ${card2_svg}
  ${card3_svg}
  ${config_section_svg}
</svg>`;
},
renderSvg = svgRender;

export default svgRender;

