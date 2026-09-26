import { metricByKey, metricSummary } from "./data.js";

const METRIC_ORDER_LI = [
  // 1. 读取性能
  "random_reads",
  "random_range_reads",
  "random_reads_8",
  "random_reads_4",
  "random_reads_16",
  "random_reads_32",
  "len",
  // 2. 写入性能
  "individual_writes",
  "bulk_load",
  "batch_writes",
  "nosync_writes",
  "removals",
  // 3. 空间与内存
  "uncompacted_size",
  "compacted_size",
  "memory",
];

export const mdRender = (bench_data, i18n, cdn_url = "") => {
    const { machine, config, engines: engine_li } = bench_data;
    let md = "# " + i18n.title + "\n\n";

    // 1. 置顶放置 SVG 柱状图
    if (cdn_url) {
      md += '<p align="center">\n  <img src="' + cdn_url + '" alt="' + i18n.title + '" width="100%">\n</p>\n\n';
    }

    // 2. 性能评测结果表格（读放到写前面）
    if (engine_li.length > 0) {
      const header_li = engine_li.map((e) => "[" + e.name + "](" + e.url + ")");

      md += "| " + i18n.metric_col + " | " + header_li.join(" | ") + " |\n";
      md += "|:---|" + engine_li.map(() => "---:").join("|") + "|\n";

      METRIC_ORDER_LI.forEach((key) => {
        const metric_name = i18n.benchmarks[key] ?? key,
          summary = metricSummary(engine_li, key),
          cell_li = engine_li.map((eng, idx) => {
            const m = metricByKey(eng, key);
            if (!m || m.type === "na") return "N/A";

            const is_best = summary.best_idx === idx;

            if (m.type === "throughput") {
              const mult =
                summary.min_rate > 0 && m.rate > 0
                  ? (m.rate / summary.min_rate).toFixed(2) + "X"
                  : "1.00X";
              return is_best
                ? "**" + mult + "**<br>**" + m.formatted + "**"
                : mult + "<br>" + m.formatted;
            }

            if (m.type === "latency") {
              const mult =
                summary.max_lat > 0
                  ? (summary.max_lat / Math.max(0.001, m.duration_ms ?? 0)).toFixed(2) + "X"
                  : "1.00X";
              return is_best
                ? "**" + mult + "**<br>**" + m.formatted + "**"
                : mult + "<br>" + m.formatted;
            }

            return is_best ? "**" + m.formatted + "**" : m.formatted;
          });

        md += "| " + metric_name + " | " + cell_li.join(" | ") + " |\n";
      });

      md += "\n> " + i18n.notes + "\n\n";
    }

    // 3. 评测配置
    const cache_mib = Math.round(config.cache_size / (1024 * 1024)),
      params_desc_li = i18n.params_desc ?? [];

    md += "## " + i18n.params_title + "\n\n";
    md += "| " + i18n.param_col + " | " + i18n.val_col + " |\n|:---|:---|\n";
    md += "| **" + i18n.key_size + "** | " + config.key_size + " B |\n";
    md += "| **" + i18n.value_size + "** | " + config.value_size + " B |\n";
    md += "| **" + i18n.cache_size + "** | " + cache_mib + " MiB |\n";
    md += "| **" + i18n.elements + "** | " + config.bulk_elements + " |\n\n";

    if (params_desc_li.length > 0) {
      md += "### " + (i18n.params_desc_title ?? "配置参数说明") + "\n\n";
      for (const item of params_desc_li) {
        md += "- **" + item.label + "**：" + item.desc + "\n";
      }
      md += "\n";
    }

    // 4. 测试环境
    const cores_str = i18n.cores_format
      .replaceAll("{physical}", machine.physical_cores)
      .replaceAll("{logical}", machine.logical_cores);

    md += "## " + i18n.system_info_title + "\n\n";
    md += "| " + i18n.hardware_col + " | " + i18n.spec_col + " |\n|:---|:---|\n";
    md += "| **" + i18n.cpu + "** | " + machine.cpu_brand + " |\n";
    md += "| **" + i18n.cores + "** | " + cores_str + " |\n";
    md += "| **" + i18n.arch + "** | " + machine.arch + " |\n";
    md += "| **" + i18n.memory + "** | " + machine.total_memory_gib.toFixed(2) + " GiB |\n";
    md += "| **" + i18n.disk_type + "** | " + machine.disk_type + " |\n";
    md += "| **" + i18n.os + "** | " + machine.os_info + " |\n";
    md += "| **" + i18n.kernel + "** | " + machine.kernel_version + " |\n\n";

    const durability_li = i18n.durability_li ?? [];
    if (durability_li.length > 0) {
      md += "## " + (i18n.durability_title ?? "存储架构与持久化配置说明") + "\n\n";
      if (i18n.durability_desc) {
        md += i18n.durability_desc + "\n\n";
      }
      for (const item of durability_li) {
        const link_str = item.url ? "[" + item.engine + "](" + item.url + ")" : item.engine;
        md += "- **" + link_str + "**\n";
        md += "  - **" + i18n.durability_col_arch + "**：" + item.arch + "\n";
        md += "  - **" + i18n.durability_col_sync + "**：" + item.sync + "\n";
        md += "  - **" + i18n.durability_col_bulk + "**：" + item.bulk + "\n";
        md += "  - **" + i18n.durability_col_nosync + "**：" + item.nosync + "\n";
        md += "  - **" + i18n.durability_col_verify + "**：" + item.verify + "\n";
      }
      md += "\n";
    }

    return md;
  },
  renderMd = mdRender;

export default mdRender;
