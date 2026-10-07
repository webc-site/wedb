<script>
  import { i18n, languageSet, LANGUAGES, t } from "../lib/i18n.svelte.js";

  let open = $state(false);
  let root = $state();

  const current_name = $derived(
    LANGUAGES.find((item) => item.code === i18n.lang)?.name ?? i18n.lang
  );

  function pick(code) {
    open = false;
    languageSet(code, true);
  }

  // 点到菜单外任意处收起：监听挂在 window，蒙层不再需要吃掉一次点击
  $effect(() => {
    if (!open) return;
    const on_pointer = (event) => {
      if (event.target instanceof Node && root?.contains(event.target)) return;
      open = false;
    };
    const on_keydown = (event) => {
      if (event.key === "Escape") open = false;
    };
    window.addEventListener("click", on_pointer);
    window.addEventListener("keydown", on_keydown);
    return () => {
      window.removeEventListener("click", on_pointer);
      window.removeEventListener("keydown", on_keydown);
    };
  });
</script>

<div class="lang" bind:this={root}>
  <button
    class="btn-pill"
    type="button"
    aria-haspopup="listbox"
    aria-expanded={open}
    aria-label={t("nav.lang")}
    onclick={() => (open = !open)}
  >
    <svg class="lang-icon" viewBox="0 0 16 16" width="14" height="14" aria-hidden="true">
      <circle cx="8" cy="8" r="6.4" fill="none" stroke="currentColor" stroke-width="1.2" />
      <ellipse cx="8" cy="8" rx="2.8" ry="6.4" fill="none" stroke="currentColor" stroke-width="1.2" />
      <line x1="1.6" y1="8" x2="14.4" y2="8" stroke="currentColor" stroke-width="1.2" />
    </svg>
    <span>{current_name}</span>
  </button>

  {#if open}
    <div class="lang-menu" role="listbox" aria-label={t("nav.lang")}>
      {#each LANGUAGES as item}
        <button
          class="lang-item"
          class:active={item.code === i18n.lang}
          type="button"
          role="option"
          aria-selected={item.code === i18n.lang}
          onclick={() => pick(item.code)}
        >
          {item.name}
        </button>
      {/each}
    </div>
  {/if}
</div>
