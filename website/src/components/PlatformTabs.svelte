<script>
  import { platformLabel, versionLabel } from "../lib/data.js";

  // 表格、柱状图、趋势共用同一个平台选择：value 由父层持有并双向绑定，
  // 三处标签页因此永远同步，不必每个区各自记一份状态
  let { runs = [], value = $bindable("") } = $props();

  const current = $derived(runs.find((run) => run.platform === value));
</script>

<div class="chips" style="margin-bottom:12px">
  {#each runs as run}
    <button
      class="btn-pill"
      class:active={run.platform === value}
      aria-pressed={run.platform === value}
      onclick={() => (value = run.platform)}
    >
      {platformLabel(run.platform)}
    </button>
  {/each}
  {#if current}
    <span class="muted" style="margin-left:auto">
      <span class="mono">{versionLabel(current)}</span>
    </span>
  {/if}
</div>
