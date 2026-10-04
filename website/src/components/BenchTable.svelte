<script>
  import {
    rowsByName,
    rowSpecs,
    platformLabel,
    shortCommit,
    formatUnix,
    formatGib,
    formatCount,
  } from "../lib/data.js";

  let { run } = $props();

  // 单位只写在行标题里（与 Rust 侧 results_table_markdown 同一口径）：
  // 该列在这一行是 N/A 时没有单位，所以取第一个真跑出值的引擎的单位
  const rows = $derived.by(() => {
    const by_name = rowsByName(run);
    return rowSpecs(run).map((spec) => {
      let unit = null;
      const cells = run.engines.map((engine) => {
        const row = by_name.get(engine.name)?.get(spec.key);
        if (!unit && row?.unit) unit = row.unit;
        return { engine: engine.name, row };
      });
      return { key: spec.key, name: spec.name, unit, cells };
    });
  });

  const machine = $derived(
    [
      run.machine?.cpu_brand,
      `${run.machine?.physical_cores ?? "?"}C${run.machine?.logical_cores ?? "?"}T`,
      `${(run.machine?.total_memory_gib ?? 0).toFixed(1)} GiB`,
      run.machine?.disk_type,
      run.machine?.data_fs,
      run.machine?.os_info,
    ]
      .filter(Boolean)
      .join(" · ")
  );

  const workload = $derived.by(() => {
    const w = run.workload;
    if (!w) return "";
    return `${formatCount(w.bulk_elements)} 装载 · ${formatCount(w.sorted_elements)} 有序 · ${formatCount(
      w.num_reads
    )} 随机读 · ${formatCount(w.num_scans)}×${w.scan_len} 范围读 · ${w.key_size}B 键 · ${w.value_size}B 值 · 缓存 ${formatGib(
      w.cache_size
    )}`;
  });

  const read_iterations = $derived(run.workload?.read_iterations ?? 3);
</script>

<div class="box-card">
  <div class="pane-head">
    <span class="label">{platformLabel(run.platform)}</span>
    <span class="mono">{shortCommit(run.commit)}</span>
    <span class="mono">{formatUnix(run.generated_at_unix)} UTC</span>
  </div>

  <div class="meta">
    <div class="meta-line">{machine}</div>
    <div class="meta-line">{workload}</div>
    {#if read_iterations > 1}
      <div class="meta-line">读类段落取 {read_iterations} 次运行的中位数；每列一个子进程，崩溃或超时的列整列折 N/A</div>
    {/if}
  </div>

  <div class="table-scroll">
    <table class="bench">
      <thead>
        <tr>
          <th></th>
          {#each run.engines as engine}
            <th>
              {engine.name}
              {#if engine.status !== "ok"}
                <span class="badge warn" title={engine.detail ?? ""}>{engine.status}</span>
              {/if}
            </th>
          {/each}
        </tr>
      </thead>
      <tbody>
        {#each rows as row}
          <tr>
            <td>
              <span class="row-name">{row.name}</span>
              {#if row.unit}
                <span class="row-unit"> ({row.unit})</span>
              {/if}
            </td>
            {#each row.cells as cell}
              <td class={cell.row?.winner ? "best" : cell.row?.kind === "na" ? "na" : ""}>
                {cell.row?.formatted ?? "N/A"}
              </td>
            {/each}
          </tr>
        {/each}
      </tbody>
    </table>
  </div>

  {#if run.notes?.length}
    <ul class="note-list">
      {#each run.notes as note}
        <li>{note}</li>
      {/each}
    </ul>
  {/if}
</div>

<style>
  .meta {
    padding: 10px 16px;
    border-bottom: 1px solid #eeeeee;
  }

  .meta-line {
    font-size: 12px;
    color: #656d76;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  }

  .badge {
    margin-left: 6px;
  }
</style>
