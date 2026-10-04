<script>
  import { HISTORY } from "./lib/benchData.js";
  import { allRuns, latestRuns } from "./lib/data.js";
  import Nav from "./components/Nav.svelte";
  import Hero from "./components/Hero.svelte";
  import BenchTable from "./components/BenchTable.svelte";
  import PlatformBars from "./components/PlatformBars.svelte";
  import TrendChart from "./components/TrendChart.svelte";
  import Footer from "./components/Footer.svelte";

  const runs = $derived(latestRuns(HISTORY));
  const all = $derived(allRuns(HISTORY));
  // 表格按列数多的平台在前，一屏内先看到最完整的一张表
  const per_platform = $derived(
    runs.slice().sort((a, b) => {
      const left = a.engines.filter((engine) => engine.status === "ok").length;
      const right = b.engines.filter((engine) => engine.status === "ok").length;
      return right - left || b.generated_at_unix - a.generated_at_unix;
    })
  );
</script>

<Nav />
<Hero runs={runs} history={HISTORY} />

<main class="wrap">
  <section class="section" id="latest">
    <div class="section-head">
      <h2>最新表格</h2>
      <p>每个平台最近一次评测的完整表：行序、单位与加粗规则和 redb 公布的表逐字同构，可与它的表并列阅读。</p>
    </div>
    {#if per_platform.length === 0}
      <div class="box-card"><p class="muted" style="padding:16px;margin:0">还没有评测数据。</p></div>
    {:else}
      <div class="stack">
        {#each per_platform as run}
          <BenchTable {run} />
        {/each}
      </div>
    {/if}
  </section>

  <section class="section" id="bars">
    <div class="section-head">
      <h2>分平台横向对比</h2>
      <p>选一段 workload 看同一平台上各引擎的相对位置；条长就是该段的数值，★ 标出行内最优。</p>
    </div>
    <PlatformBars runs={runs} />
  </section>

  <section class="section" id="trend">
    <div class="section-head">
      <h2>历史趋势</h2>
      <p>同一平台跨提交的纵向变化：每轮主分支评测落一个点，用来盯回归而不是看单轮快照。</p>
    </div>
    <TrendChart history={HISTORY} />
  </section>
</main>

<Footer />

<style>
  .stack {
    display: flex;
    flex-direction: column;
    gap: 24px;
  }

  main.wrap {
    padding-top: 8px;
    padding-bottom: 24px;
  }
</style>
