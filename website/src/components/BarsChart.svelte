<script>
  import MetricSelect from "./MetricSelect.svelte";
  import {
    engineColor,
    engineLabel,
    statusLabel,
    throughputOptions,
    throughputValue,
  } from "../lib/data.js";
  import { t } from "../lib/i18n.svelte.js";

  // 平台与段的选择：平台由父层的 PlatformTabs 持有（三区共用），这里只管自己的一段
  let { run = null } = $props();

  const SVG_WIDTH = 840,
    LABEL_X = 16,
    BAR_START_X = 220,
    MAX_BAR_WIDTH = 430,
    ROW_HEIGHT = 46,
    BAR_HEIGHT = 20,
    TOP_Y = 40;

  const metrics = $derived(throughputOptions(run));
  let metric = $state("");
  const active_metric = $derived(metrics.find((item) => item.key === metric) ?? metrics[0]);

  const bars = $derived.by(() => {
    if (!run || !active_metric) return { rows: [], best: null, unit: "" };
    const items = run.engines
      .map((engine) => {
        const row = engine.rows.find((item) => item.key === active_metric.key);
        return { name: engine.name, row, value: throughputValue(row), status: engine.status };
      })
      .filter((item) => item.row);
    const numeric = items.filter((item) => item.value !== null);
    const max = Math.max(...numeric.map((item) => item.value), 0);
    const better = numeric.find((item) => item.row.winner);
    // 吞吐段越长越好，直接按值降序排，最右那条就是冠军
    const sorted = items.slice().sort((a, b) => (b.value ?? -1) - (a.value ?? -1));
    return {
      rows: sorted.map((item, index) => ({
        ...item,
        y: TOP_Y + index * ROW_HEIGHT,
        bar_w:
          item.value !== null && max > 0
            ? Math.max(16, (item.value / max) * MAX_BAR_WIDTH)
            : 0,
      })),
      best: better?.name ?? null,
      unit: active_metric.unit,
    };
  });

  const height = $derived(TOP_Y + (bars.rows?.length ?? 0) * ROW_HEIGHT + 16);
</script>

{#if !run}
  <p class="muted">{t("bars.empty")}</p>
{:else}
  <div class="box-card">
    <div class="pane-head">
      <span class="label">{t("pane.bars")}</span>
      <MetricSelect {metrics} bind:value={metric} />
      <span style="margin-left:auto">
        {t("bars.higher_better")}{bars.unit ? ` · ${bars.unit}` : ""}
      </span>
    </div>

    <svg
      class="chart-svg"
      viewBox="0 0 {SVG_WIDTH} {height}"
      width="100%"
      height={height}
      role="img"
      aria-label={t("bars.aria")}
    >
      {#each bars.rows as row}
        <text class="row-label" x={LABEL_X} y={row.y + 14}>{engineLabel(row.name)}</text>
        <text class="row-note" x={LABEL_X} y={row.y + 30}>
          {row.status !== "ok" ? statusLabel(row.status) : ""}
        </text>
        <rect
          class="bar-track"
          x={BAR_START_X}
          y={row.y}
          width={MAX_BAR_WIDTH}
          height={BAR_HEIGHT}
          rx="6"
        />
        {#if row.bar_w > 0}
          <rect
            class="bar-rect"
            x={BAR_START_X}
            y={row.y}
            width={row.bar_w}
            height={BAR_HEIGHT}
            rx="6"
            fill={engineColor(row.name)}
          />
        {/if}
        <text
          class="val-text"
          x={BAR_START_X + row.bar_w + 14}
          y={row.y + 10}
          fill={row.name === bars.best ? "#1f2328" : "#57606a"}
        >
          {row.row.formatted}{row.name === bars.best ? " ★" : ""}
          {#if row.name === bars.best}
            <title>{t("bars.best_hint")}</title>
          {/if}
        </text>
      {/each}
    </svg>
  </div>
{/if}
