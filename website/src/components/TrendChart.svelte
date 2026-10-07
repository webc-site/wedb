<script>
  import MetricSelect from "./MetricSelect.svelte";
  import {
    engineColor,
    engineLabel,
    formatDay,
    formatTick,
    throughputOptions,
    throughputValue,
    versionLabel,
  } from "../lib/data.js";
  import { t, tf } from "../lib/i18n.svelte.js";

  // 平台选择由父层持有（与柱状图、表格共用同一个 tab 组），这里只画跨提交的纵轴
  let { runs = [], platform = "" } = $props();

  const series_runs = $derived(
    runs
      .filter((run) => run.platform === platform)
      .slice()
      .sort((a, b) => a.generated_at_unix - b.generated_at_unix)
  );

  const metrics = $derived(throughputOptions(series_runs.at(-1)));
  let metric = $state("");
  const active_metric = $derived(metrics.find((item) => item.key === metric) ?? metrics[0]);

  const engine_ids = $derived([
    ...new Set(series_runs.flatMap((run) => run.engines.map((engine) => engine.name))),
  ]);

  // 一个观测 = 某轮某列在某段的实测吞吐。N/A 不进数据，线按 segment 分组断开，
  // 绝不做插值——缺口本身就是「那一列当轮没出值」的读数。
  const observations = $derived.by(() => {
    if (!active_metric) return [];
    const rows = [];
    for (const name of engine_ids) {
      let segment = 0;
      let previous = null;
      series_runs.forEach((run, index) => {
        const engine = run.engines.find((item) => item.name === name);
        const row = engine?.rows.find((item) => item.key === active_metric.key);
        const value = throughputValue(row);
        if (value === null) {
          segment += 1;
          previous = null;
          return;
        }
        const delta =
          previous === null
            ? t("trend.tip_na")
            : `${t("trend.tip_delta")} ${value >= previous ? "+" : ""}${(((value - previous) / previous) * 100).toFixed(1)}%`;
        rows.push({
          index,
          value,
          label: engineLabel(name),
          segment: `${name}#${segment}`,
          version: versionLabel(run),
          date: formatDay(run.generated_at_unix),
          formatted: row.formatted,
          unit: active_metric.unit,
          delta,
        });
        previous = value;
      });
    }
    return rows;
  });

  const domain = $derived([...new Set(observations.map((row) => row.index))].sort((a, b) => a - b));
  const color_labels = $derived(engine_ids.map(engineLabel));
  const color_range = $derived(engine_ids.map(engineColor));
  const version_by_index = $derived(
    Object.fromEntries(series_runs.map((run, index) => [index, versionLabel(run)]))
  );

  let host = $state();
  let lib = $state(null);

  // G2 侧 gzip 约 300KB：动态 import 只是不进首屏 chunk，挂上就拉仍会抢带宽，
  // 所以等这一区真的滚进视口再取
  $effect(() => {
    if (lib || !host || series_runs.length === 0) return;
    let cancelled = false;
    const observer = new IntersectionObserver(
      (entries) => {
        if (!entries.some((entry) => entry.isIntersecting)) return;
        observer.disconnect();
        import("@antv/g2")
          .then(({ Chart }) => {
            if (!cancelled) lib = { Chart };
          })
          .catch(() => {
            lib = null;
          });
      },
      { rootMargin: "160px" }
    );
    observer.observe(host);
    return () => {
      cancelled = true;
      observer.disconnect();
    };
  });

  $effect(() => {
    if (!lib || !host || observations.length === 0) return;
    const { Chart } = lib;
    const tooltip = {
      title: (row) => `${row.version} · ${row.date}`,
      items: [(row) => ({ name: row.label, value: `${row.formatted} ${row.unit} · ${row.delta}` })],
    };
    const chart = new Chart({
      container: host,
      autoFit: true,
      height: 340,
    });
    chart.options({
      type: "view",
      data: observations,
      // 图例由 pane-head 自己画（要跟站点配色与词条走），G2 那份关掉；
      // 留着它还会因为 size/color 的推断走 continuous 分支炸渲染
      legend: false,
      scale: {
        x: { type: "point", domain },
        y: { nice: true },
        color: { domain: color_labels, range: color_range },
      },
      axis: {
        x: {
          title: false,
          labelFormatter: (index) => version_by_index[index] ?? "",
          labelFill: "#656d76",
          labelFontSize: 10.5,
          tickStroke: "#d0d7de",
        },
        y: {
          title: false,
          labelFormatter: (value) => formatTick(value),
          labelFill: "#656d76",
          labelFontSize: 10.5,
          grid: true,
          gridStroke: "#eeeeee",
          gridLineDash: [4, 4],
        },
      },
      children: [
        {
          // tooltip 挂在折线上：线才是可靠的命中目标（lineAppendWidth 给了命中宽度），
          // 挂在点上则要点中那个小圆才出提示；标记 tooltip:false 的点只负责画点
          type: "line",
          encode: { x: "index", y: "value", color: "label", series: "segment" },
          style: { lineWidth: 2, lineAppendWidth: 8 },
          tooltip,
        },
        {
          type: "point",
          encode: { x: "index", y: "value", color: "label", size: 4 },
          style: { strokeWidth: 1.5, stroke: "#ffffff" },
          tooltip: false,
        },
      ],
      interaction: {
        tooltip: { shared: true },
      },
    });
    chart.render();
    return () => chart.destroy();
  });
</script>

{#if series_runs.length === 0}
  <p class="muted">{t("trend.empty")}</p>
{:else}
  <div class="chips" style="margin-bottom:12px">
    <MetricSelect {metrics} bind:value={metric} />
  </div>

  <div class="box-card">
    <div class="pane-head">
      <span class="label">{t("pane.trend")}</span>
      <span class="chips legend">
        {#each color_labels as label}
          <span class="legend-item">
            <i style="background:{color_range[color_labels.indexOf(label)]}"></i>{label}
          </span>
        {/each}
      </span>
      <span style="margin-left:auto">{active_metric?.unit ?? ""}</span>
    </div>

    <div class="chart-host" role="img" aria-label={t("trend.aria")} bind:this={host}></div>

    <p class="muted plot-note">{tf("trend.dots", { n: series_runs.length })}</p>
  </div>
{/if}
