<script>
  import LangSelect from "./LangSelect.svelte";
  import { SITE, REPO } from "../lib/site.js";
  import { t } from "../lib/i18n.svelte.js";
</script>

<nav class="nav">
  <div class="wrap nav-inner">
    <div class="nav-left">
      <a class="nav-brand" href={SITE} rel="noopener">
        <img src="./favicon.svg" alt="" width="22" height="22" />
        <span>WeDB Bench</span>
      </a>
      <div class="nav-links">
        <a class="hide-sm" href="#bars">{t("nav.section.bars")}</a>
        <a class="hide-sm" href="#trend">{t("nav.section.trend")}</a>
        <a class="hide-sm" href="#table">{t("nav.section.table")}</a>
      </div>
    </div>
    <div class="nav-actions">
      <LangSelect />
      <a class="btn-pill hide-sm" href="./data/history.json" rel="noopener">{t("nav.data")}</a>
      <a class="btn-pill" href={REPO} rel="noopener">{t("nav.github")}</a>
    </div>
  </div>
</nav>
