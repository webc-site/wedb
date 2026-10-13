<script>
  import { formatCount, formatUnix, platformLabel, versionLabel } from "../lib/data.js";
  import { t, tf } from "../lib/i18n.svelte.js";

  let { runs = [], history } = $props();

  // 「缩放」是工厂端写进 notes 的中文口径串，这里是匹配数据出处，不是界面文案
  const headline = $derived.by(() => {
    const standard = runs.find((run) => run.notes.every((note) => !note.includes("缩放")));
    const workload = (standard ?? runs[0])?.workload;
    return workload
      ? tf("hero.headline", {
          bulk: formatCount(workload.bulk_elements),
          sorted: formatCount(workload.sorted_elements),
          key: workload.key_size,
          value: workload.value_size,
        })
      : t("hero.scale_standard");
  });

  const engines = $derived(
    [...new Set(runs.flatMap((run) => run.engines.map((engine) => engine.name)))]
  );

  const newest = $derived(
    runs.slice().sort((a, b) => b.generated_at_unix - a.generated_at_unix)[0]
  );

  const scale_note = $derived(history?.reports?.length ? null : t("hero.empty"));
</script>

<section class="hero wrap">
  <h1>{t("hero.title")}</h1>
  <p>{t("hero.body")}</p>
  {#if scale_note}
    <p class="muted" style="margin-top:14px">{scale_note}</p>
  {:else}
    <div class="chips" style="margin-top:18px;justify-content:center">
      <span class="badge">{headline}</span>
      <span class="badge">{tf("hero.columns_platforms", { columns: engines.length, platforms: runs.length })}</span>
      {#if newest}
        <span class="badge mono">{versionLabel(newest)}</span>
        <span class="badge mono">{platformLabel(newest.platform)}</span>
        <span class="badge mono">{formatUnix(newest.generated_at_unix)} {t("table.utc")}</span>
      {/if}
    </div>
  {/if}
</section>
