<script>
  import { REPO, SITE } from "../lib/site.js";
  import { t } from "../lib/i18n.svelte.js";
</script>

<footer class="foot">
  <div class="wrap">
    <p>{t("foot.line1")}</p>
    <p>{t("foot.line2")}</p>
    <div class="chips foot-links">
      <span class="label">{t("foot.links")}</span>
      <a class="btn-pill" href={REPO} rel="noopener">{t("nav.github")}</a>
      <a
        class="btn-pill"
        href="{REPO}/actions/workflows/bench.yml"
        rel="noopener">{t("foot.workflow_bench")}</a
      >
      <a
        class="btn-pill"
        href="{REPO}/actions/workflows/website.yml"
        rel="noopener">{t("foot.workflow_website")}</a
      >
      <a class="btn-pill" href="./data/history.json" rel="noopener">{t("nav.data")}</a>
      <a class="btn-pill mono" href={SITE} rel="noopener">{SITE}</a>
    </div>
    <p class="mono foot-sign">
      wedb · {t("foot.line3")} · MulanPSL-2.0
    </p>
  </div>
</footer>
