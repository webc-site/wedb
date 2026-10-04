<script>
  import {
    platformLabel,
    shortCommit,
    formatUnix,
    formatCount,
  } from "../lib/data.js";

  let { runs = [], history } = $props();

  const headline = $derived.by(() => {
    const standard = runs.find((run) => run.notes.every((note) => !note.includes("缩放")));
    const workload = (standard ?? runs[0])?.workload;
    return workload
      ? `${formatCount(workload.bulk_elements)} 装载 · ${formatCount(workload.sorted_elements)} 有序 · ${workload.key_size}B 键 · ${workload.value_size}B 值`
      : "redb 标准档";
  });

  const engines = $derived(
    [...new Set(runs.flatMap((run) => run.engines.map((engine) => engine.name)))]
  );

  const newest = $derived(
    runs.slice().sort((a, b) => b.generated_at_unix - a.generated_at_unix)[0]
  );

  const scale_note = $derived(
    history?.reports?.length ? null : "还没有评测数据：主分支跑完一轮 Benchmark 后这里就会有内容。"
  );
</script>

<section class="hero wrap">
  <h1>redb 同构基准下的 wedb</h1>
  <p>
    表格口径、18 段 workload 与呈现规则全部对齐 redb-bench：同样的行序、同样的速率单位、
    同样的行内最优加粗。被测的是 wedb 自己的 <code>hash</code>（wkv 混合日志 KV）与
    <code>bftree</code>（wbftree 有序索引），对照列是 fjall、rocksdb 与 sqlite。
  </p>
  {#if scale_note}
    <p class="muted" style="margin-top:14px">{scale_note}</p>
  {:else}
    <div class="chips" style="margin-top:18px;justify-content:center">
      <span class="badge">{headline}</span>
      <span class="badge">{engines.length} 列 · {runs.length} 平台</span>
      {#if newest}
        <span class="badge mono">{shortCommit(newest.commit)}</span>
        <span class="badge mono">{platformLabel(newest.platform)}</span>
        <span class="badge mono">{formatUnix(newest.generated_at_unix)} UTC</span>
      {/if}
    </div>
  {/if}
</section>
