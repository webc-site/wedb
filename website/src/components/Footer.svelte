<script>
  const repo = "https://github.com/webc-site/wedb";
</script>

<footer class="foot">
  <div class="wrap">
    <p>
      表格由 bench 工厂端（Rust）产出：18 段 workload、单位标注与行内最优加粗的规则都在
      <span class="mono">crates/wedb-bench</span> 里，站点只消费
      <a href="./data/history.json">机读 JSON</a>，不在浏览器里重抄一遍格式化口径。
    </p>
    <p>
      流水线：<a href="{repo}/actions/workflows/bench.yml">Benchmark</a> 在
      Linux x64 / Linux ARM64 / macOS / Windows 四类镜像上按引擎分列跑，
      <span class="mono">benchreport merge</span> 并成平台表并累积历史，
      <a href="{repo}/actions/workflows/website.yml">Website</a> 把站点与历史发到
      <span class="mono">gh-pages</span>。
    </p>
    <p class="mono">
      wedb · <a href={repo}>GitHub</a> · 数据每轮主分支评测后更新 · MulanPSL-2.0
    </p>
  </div>
</footer>
