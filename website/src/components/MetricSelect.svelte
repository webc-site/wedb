<script>
  // 段选择器：柱状图与趋势图共用一份「吞吐段」选项口径（throughputOptions）
  let { metrics = [], value = $bindable("") } = $props();

  // 初值为空、或切平台后段列表变了，都会让 select 落不到任何 option 而显示空白，
  // 这里统一兜回第一档
  $effect(() => {
    if (metrics.length > 0 && !metrics.some((item) => item.key === value)) value = metrics[0].key;
  });
</script>

<select class="metric-pick" value={value} oninput={(event) => (value = event.target.value)}>
  {#each metrics as item}
    <option value={item.key}>{item.label}</option>
  {/each}
</select>
