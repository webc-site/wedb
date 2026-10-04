<script>
  import {
    allRuns,
    engineColor,
    formatDay,
    formatTick,
    platformLabel,
    versionLabel,
    throughputOptions,
    throughputValue,
  } from "../lib/data.js";

  let { history } = $props();

  const PLOT_LEFT = 72,
    PLOT_RIGHT = 812,
    PLOT_TOP = 24,
    PLOT_BOTTOM = 268,
    WIDTH = 840,
    HEIGHT = 320;

  const runs = $derived(allRuns(history));
  const platforms = $derived([...new Set(runs.map((run) => run.platform))]);
  let platform = $state("");
  let metric = $state("");

  const active_platform = $derived(
    platforms.includes(platform) ? platform : (platforms.at(-1) ?? "")
  );

  const series_runs = $derived(
    runs.filter((run) => run.platform === active_platform).sort((a, b) => a.generated_at_unix - b.generated_at_unix)
  );

  // 行名在同一 harness 下各列一致，取窗口里最新一次的吞吐行序
  const metrics = $derived(throughputOptions(series_runs.at(-1)));

  const active_metric = $derived(metrics.find((item) => item.key === metric) ?? metrics[0]);

  const engine_names = $derived([...new Set(series_runs.flatMap((run) => run.engines.map((e) => e.name)))]);

  const chart = $derived.by(() => {
    if (!active_metric) return { lines: [], ticks: [], points: [], max: 0, unit: "" };
    const per_engine = engine_names.map((name) => {
      const points = series_runs.map((run, index) => {
        const engine = run.engines.find((item) => item.name === name);
        const row = engine?.rows.find((item) => item.key === active_metric.key);
        const value = throughputValue(row);
        return {
          index,
          value,
          formatted: row?.formatted ?? "N/A",
          commit: run.commit,
          version: versionLabel(run),
          generated_at_unix: run.generated_at_unix,
        };
      });
      const max = Math.max(...points.map((point) => point.value ?? 0), 0);
      // N/A 断线：把连续有值的段落各自连成一条 polyline，不做插值伪造
      const segments = [];
      let current = [];
      for (const point of points) {
        if (point.value === null) {
          if (current.length) segments.push(current);
          current = [];
        } else {
          current.push(point);
        }
      }
      if (current.length) segments.push(current);
      return { name, points, max, segments };
    });
    const overall = Math.max(...per_engine.map((engine) => engine.max), 0);
    const step = (x) =>
      series_runs.length <= 1 ? (PLOT_LEFT + PLOT_RIGHT) / 2 : PLOT_LEFT + (x * (PLOT_RIGHT - PLOT_LEFT)) / (series_runs.length - 1);
    const y = (value) =>
      overall > 0 ? PLOT_BOTTOM - (value / overall) * (PLOT_BOTTOM - PLOT_TOP) : PLOT_BOTTOM;
    const ticks = [0, 0.25, 0.5, 0.75, 1].map((ratio) => ({
      value: overall * ratio,
      y: y(overall * ratio),
    }));
    return {
      per_engine,
      ticks,
      points: series_runs.map((run, index) => ({
        x: step(index),
        commit: run.commit,
        version: versionLabel(run),
        generated_at_unix: run.generated_at_unix,
      })),
      step,
      y,
      max: overall,
      unit: active_metric.unit,
    };
  });

  const label_stride = $derived(Math.max(1, Math.ceil(series_runs.length / 8)));
</script>

{#if runs.length === 0}
  <p class="muted">还没有历史数据：主分支每跑完一轮 Benchmark 就会多一个点。</p>
{:else}
  <div class="chips" style="margin-bottom:12px">
    {#each platforms as item}
      <button
        class="btn-pill"
        class:active={item === active_platform}
        onclick={() => (platform = item)}
      >
        {platformLabel(item)}
      </button>
    {/each}
    <select class="metric-pick" value={active_metric?.key ?? ""} oninput={(event) => (metric = event.target.value)}>
      {#each metrics as item}
        <option value={item.key} selected={item.key === active_metric?.key}>{item.label}</option>
      {/each}
    </select>
  </div>

  <div class="box-card">
    <div class="pane-head">
      <span class="label">同一平台跨提交趋势</span>
      <span class="chips legend">
        {#each engine_names as name}
          <span class="legend-item"><i style="background:{engineColor(name)}"></i>{name}</span>
        {/each}
      </span>
      <span style="margin-left:auto">{chart.unit}</span>
    </div>

    {#if series_runs.length === 0}
      <p class="muted" style="padding:16px">该平台还没有数据。</p>
    {:else}
      <svg
        class="chart-svg"
        viewBox="0 0 {WIDTH} {HEIGHT}"
        width="100%"
        height={HEIGHT}
        role="img"
        aria-label="所选平台各引擎在所选 workload 段的跨提交趋势"
      >
        {#each chart.ticks as tick}
          <line class="grid" x1={PLOT_LEFT} y1={tick.y} x2={PLOT_RIGHT} y2={tick.y} />
          <text class="axis" x={PLOT_LEFT - 10} y={tick.y + 4} text-anchor="end">
            {formatTick(tick.value)}
          </text>
        {/each}

        {#each chart.per_engine as engine}
          {#each engine.segments as segment}
            <polyline
              class="series"
              stroke={engineColor(engine.name)}
              points={segment
                .map((point) => `${chart.step(point.index)},${chart.y(point.value)}`)
                .join(" ")}
            />
          {/each}
          {#each engine.points as point}
            {#if point.value !== null}
              <circle
                class="dot"
                cx={chart.step(point.index)}
                cy={chart.y(point.value)}
                r="3"
                fill={engineColor(engine.name)}
              >
                <title>
                  {engine.name} · {point.version}（{formatDay(point.generated_at_unix)}）· {point.formatted}
                </title>
              </circle>
            {/if}
          {/each}
        {/each}

        <line class="axis-line" x1={PLOT_LEFT} y1={PLOT_BOTTOM} x2={PLOT_RIGHT} y2={PLOT_BOTTOM} />
        {#each chart.points as point, index}
          {#if index % label_stride === 0}
            <text class="axis" x={point.x} y={PLOT_BOTTOM + 18} text-anchor="middle">
              {point.version}
            </text>
            <text class="axis axis-date" x={point.x} y={PLOT_BOTTOM + 32} text-anchor="middle">
              {formatDay(point.generated_at_unix)}
            </text>
          {/if}
        {/each}
      </svg>
      <p class="muted plot-note">
        {series_runs.length} 个点，横轴每个点标那一轮的版本号（commit 短哈希 + 跑测日期）·
        缺口表示那一列当轮没出值（崩溃、超时或未参与），线在缺口处断开，不做插值。
      </p>
    {/if}
  </div>
{/if}

<style>
  .metric-pick {
    margin-left: auto;
    height: 32px;
    padding: 0 10px;
    border: 1px solid #d0d7de;
    border-radius: 6px;
    background: #ffffff;
    color: #1f2328;
    font-size: 13px;
  }

  .legend {
    margin-left: 12px;
  }

  .legend-item {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-size: 12px;
    color: #57606a;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }

  .legend-item i {
    width: 10px;
    height: 10px;
    border-radius: 2px;
    display: inline-block;
  }

  .grid {
    stroke: #eeeeee;
    stroke-width: 1;
  }

  .axis-line {
    stroke: #d0d7de;
    stroke-width: 1;
  }

  .axis {
    font-size: 10.5px;
    fill: #656d76;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }

  .axis-date {
    fill: #8b949e;
    font-size: 9.5px;
  }

  .series {
    fill: none;
    stroke-width: 2;
    stroke-linejoin: round;
    stroke-linecap: round;
  }

  .dot {
    stroke: #ffffff;
    stroke-width: 1.5;
  }

  .plot-note {
    padding: 0 16px 14px;
    margin: 0;
  }
</style>
