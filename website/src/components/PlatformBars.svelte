<script>
  import { engineColor, platformLabel, throughputOptions, versionLabel, throughputValue } from "../lib/data.js";

  let { runs = [] } = $props();

  const SVG_WIDTH = 840,
    LABEL_X = 16,
    BAR_START_X = 220,
    MAX_BAR_WIDTH = 430,
    ROW_HEIGHT = 46,
    BAR_HEIGHT = 20,
    TOP_Y = 40;

  // 平台选择以最新一次为准；默认落在数据最全的那个平台
  const ordered = $derived(runs.slice().sort((a, b) => b.generated_at_unix - a.generated_at_unix));
  let platform = $derived(
    ordered.reduce(
      (best, run) => (best === null || run.engines.length > best.engines.length ? run : best),
      null
    )?.platform ?? ""
  );
  let metric = $state("");

  const current = $derived(ordered.find((run) => run.platform === platform) ?? ordered[0]);

  const metrics = $derived(throughputOptions(current));

  const active_metric = $derived(metrics.find((item) => item.key === metric) ?? metrics[0]);

  const bars = $derived.by(() => {
    if (!current || !active_metric) return { rows: [], best: null, unit: "" };
    const items = current.engines
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

{#if !current}
  <p class="muted">还没有可对比的平台数据。</p>
{:else}
  <div class="chips" style="margin-bottom:12px">
    {#each ordered as run}
      <button
        class="btn-pill"
        class:active={run.platform === current.platform}
        onclick={() => (platform = run.platform)}
      >
        {platformLabel(run.platform)}
      </button>
    {/each}
    <span class="muted" style="margin-left:auto">
      <span class="mono">{versionLabel(current)}</span>
    </span>
  </div>

  <div class="box-card">
    <div class="pane-head">
      <span class="label">单段横向对比</span>
      <select value={active_metric?.key ?? ""} oninput={(event) => (metric = event.target.value)}>
        {#each metrics as item}
          <option value={item.key} selected={item.key === active_metric?.key}>{item.label}</option>
        {/each}
      </select>
      <span style="margin-left:auto">越高越好{bars.unit ? ` · ${bars.unit}` : ""}</span>
    </div>

    <svg
      class="chart-svg"
      viewBox="0 0 {SVG_WIDTH} {height}"
      width="100%"
      height={height}
      role="img"
      aria-label="各引擎在所选 workload 段的横向柱状对比"
    >
      {#each bars.rows as row}
        <text class="row-label" x={LABEL_X} y={row.y + 14}>{row.name}</text>
        <text class="row-note" x={LABEL_X} y={row.y + 30}>
          {row.status !== "ok" ? row.status : ""}
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
        </text>
      {/each}
    </svg>
  </div>
{/if}
