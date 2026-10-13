<script>
  let { id, title, desc = "", children } = $props();
</script>

<section class="section" id={id}>
  <div class="section-head">
    <h2>{title}</h2>
    {#if desc}
      <p>{desc}</p>
    {/if}
  </div>
  {@render children()}
</section>
