<script>
  import { HISTORY } from "./lib/benchData.js";
  import { allRuns, defaultPlatform, latestRuns } from "./lib/data.js";
  import { t } from "./lib/i18n.svelte.js";
  import Nav from "./components/Nav.svelte";
  import Hero from "./components/Hero.svelte";
  import Section from "./components/Section.svelte";
  import PlatformTabs from "./components/PlatformTabs.svelte";
  import BarsChart from "./components/BarsChart.svelte";
  import TrendChart from "./components/TrendChart.svelte";
  import BenchTable from "./components/BenchTable.svelte";
  import Footer from "./components/Footer.svelte";

  const runs = $derived(latestRuns(HISTORY));
  const all = $derived(allRuns(HISTORY));

  // 三区共用一个平台选择：柱状图与表格各摆一组 tab（同一状态，切哪组都同步），
  // 趋势图跟着走，不再各自记一份。默认档在挂载时补齐，避免只捕到 runs 的初值。
  let platform = $state("");
  $effect(() => {
    if (platform === "" && runs.length > 0) platform = defaultPlatform(runs) ?? "";
  });
  const current = $derived(runs.find((run) => run.platform === platform) ?? null);
</script>

<Nav />
<Hero {runs} history={HISTORY} />

<main class="wrap">
  <Section id="bars" title={t("section.bars.title")} desc={t("section.bars.desc")}>
    <PlatformTabs {runs} bind:value={platform} />
    <BarsChart run={current} />
  </Section>

  <Section id="trend" title={t("section.trend.title")} desc={t("section.trend.desc")}>
    <TrendChart runs={all} {platform} />
  </Section>

  <Section id="table" title={t("section.table.title")} desc={t("section.table.desc")}>
    <PlatformTabs {runs} bind:value={platform} />
    <BenchTable run={current} />
  </Section>
</main>

<Footer />

<style>
  main.wrap {
    padding-top: 8px;
    padding-bottom: 24px;
  }
</style>
