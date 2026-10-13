<script>
  import {
    engineLabel,
    formatCount,
    formatGib,
    formatUnix,
    platformLabel,
    rowsByName,
    rowSpecs,
    statusLabel,
    versionLabel,
  } from "../lib/data.js";
  import { rowLabel, t, tf } from "../lib/i18n.svelte.js";

  let { run = null } = $props();

  // 单位统一为 M/s 移至列名
  const rows = $derived.by(() => {
    if (!run) return [];
    const by_name = rowsByName(run);
    return rowSpecs(run).map((spec) => {
      const cells = run.engines.map((engine) => {
        const row = by_name.get(engine.name)?.get(spec.key);
        return { engine: engine.name, row };
      });
      return { spec, label: rowLabel(spec), cells };
    });
  });

  const machine = $derived.by(() => {
    if (!run) return "";
    return [
      run.machine?.cpu_brand,
      `${run.machine?.physical_cores ?? "?"}C${run.machine?.logical_cores ?? "?"}T`,
      `${(run.machine?.total_memory_gib ?? 0).toFixed(1)} GiB`,
      run.machine?.disk_type,
      run.machine?.data_fs,
      run.machine?.os_info,
    ]
      .filter(Boolean)
      .join(" · ");
  });

  const workload = $derived.by(() => {
    const w = run?.workload;
    if (!w) return "";
    return tf("table.workload", {
      bulk: formatCount(w.bulk_elements),
      sorted: formatCount(w.sorted_elements),
      reads: formatCount(w.num_reads),
      scans: formatCount(w.num_scans),
      scan_len: w.scan_len,
      key: w.key_size,
      value: w.value_size,
      cache: formatGib(w.cache_size),
    });
  });

  const read_iterations = $derived(run?.workload?.read_iterations ?? 3);
</script>

{#if !run}
  <p class="muted">{t("table.empty")}</p>
{:else}
  <div class="box-card">
    <div class="pane-head">
      <span class="label">{platformLabel(run.platform)}</span>
      <span class="mono">{versionLabel(run)}</span>
      <span class="mono">{formatUnix(run.generated_at_unix)} {t("table.utc")}</span>
    </div>

    <div class="meta">
      <div class="meta-line">{machine}</div>
      <div class="meta-line">{workload}</div>
      {#if read_iterations > 1}
        <div class="meta-line">{tf("table.median_note", { n: read_iterations })}</div>
      {/if}
    </div>

    <div class="table-scroll">
      <table class="bench">
        <thead>
          <tr>
            <th></th>
            {#each run.engines as engine}
              <th>
                {engineLabel(engine.name)} (M/s)
                {#if engine.status !== "ok"}
                  <span class="badge warn" title={engine.detail ?? ""}>
                    {statusLabel(engine.status)}
                  </span>
                {/if}
              </th>
            {/each}
          </tr>
        </thead>
        <tbody>
          {#each rows as row}
            <tr>
              <td>
                <span class="row-name">{row.label}</span>
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
{/if}
