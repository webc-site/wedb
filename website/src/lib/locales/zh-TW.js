export default {
  "meta.title": "WeDB Bench",
  "meta.description":
    "wedb 的 redb 同構基準：wkv、wbftree 對比 fjall、rocksdb、sqlite，四類 runner 映像上的表格、橫向對比圖與跨提交趨勢。",

  "nav.section.bars": "對比圖表",
  "nav.section.trend": "歷史趨勢",
  "nav.section.table": "最新表格",
  "nav.data": "機讀資料",
  "nav.github": "GitHub",
  "nav.lang": "語言",

  "hero.title": "按 redb 的口徑測 wedb",
  "hero.body":
    "行序、單位與加粗規則全部對齊 redb-bench：同樣的 18 段 workload、同樣的速率單位、同樣的行內最佳標註。受測的是 wedb 自己的 wkv（雜湊日誌 KV）與 wbftree（有序索引），對照欄是 fjall、rocksdb 與 sqlite。",
  "hero.scale_standard": "redb 標準檔",
  "hero.headline": "{bulk} 裝載 · {sorted} 有序 · {key}B 鍵 · {value}B 值",
  "hero.columns_platforms": "{columns} 欄 · {platforms} 平台",
  "hero.empty": "還沒有評測資料：主分支跑完一輪 Benchmark 後這裡就會有內容。",

  "section.bars.title": "分平台橫向對比",
  "section.bars.desc": "選一段 workload 看同一平台上各引擎的相對位置；條長就是該段的數值，★ 標出行內最佳。",
  "section.trend.title": "歷史趨勢",
  "section.trend.desc":
    "同一平台跨提交的縱向變化：每輪主分支評測落一個點，用來盯迴歸而不是看單輪快照。把滑鼠移到點上可以看到引擎、版本與環比。",
  "section.table.title": "最新表格",
  "section.table.desc":
    "所選平台最近一次評測的完整表：行序、單位與加粗規則和 redb 公布的表逐字同構，可與它的表並列閱讀。",

  "pane.bars": "單段對比",
  "pane.trend": "跨提交趨勢",
  "bars.higher_better": "越高越好",
  "bars.empty": "還沒有可對比的平台資料。",
  "bars.aria": "各引擎在所選 workload 段的橫向柱狀對比",
  "bars.best_hint": "★ 該段最快",

  "trend.aria": "所選平台各引擎在所選 workload 段的跨提交趨勢",
  "trend.empty": "還沒有歷史點：主分支每跑完一輪 Benchmark 就會多一個點。",
  "trend.dots":
    "{n} 個點 · 橫軸每個點標那一輪的版本號 · 缺口表示該欄當輪沒出值（崩潰、逾時或未參與），線在缺口處斷開，不做插值。",
  "trend.tip_delta": "較上輪",
  "trend.tip_na": "無資料",

  "table.empty": "該平台這一輪還沒有資料。",
  "table.utc": "UTC",
  "table.workload":
    "{bulk} 裝載 · {sorted} 有序 · {reads} 隨機讀 · {scans}×{scan_len} 範圍讀 · {key}B 鍵 · {value}B 值 · 快取 {cache}",
  "table.median_note":
    "讀類段落取 {n} 次執行的中位數；每欄一個子行程，崩潰或逾時的欄整欄折為 N/A。",
  "status.crashed": "崩潰",
  "status.timeout": "逾時",
  "commit.local": "本機",

  "foot.line1":
    "表格由 bench 工廠端（Rust）產出：18 段 workload、單位標註與行內最佳加粗的規則都在 crates/wedb-bench 裡，網站只消費機讀 JSON，不在瀏覽器裡重抄一遍格式化口徑。",
  "foot.line2":
    "流水線：Benchmark 工作流在四類 runner 映像上按引擎分欄跑，benchreport merge 併成平台表並累積歷史，Website 工作流把網站連同歷史發布到 gh-pages。",
  "foot.line3": "資料每輪主分支評測後更新。",
  "foot.links": "連結",
  "foot.workflow_bench": "Benchmark 工作流",
  "foot.workflow_website": "Website 工作流",

  "bench.row.bulk_load": "批次裝載",
  "bench.row.individual_writes": "單條寫入",
  "bench.row.small_batch_writes": "小批寫入",
  "bench.row.sorted_inserts": "有序插入",
  "bench.row.nosync_writes": "免同步寫入",
  "bench.row.len": "len()",
  "bench.row.random_reads": "隨機讀",
  "bench.row.random_range_reads": "範圍隨機讀",
  "bench.row.random_reads_4_threads": "隨機讀（4 執行緒）",
  "bench.row.random_reads_8_threads": "隨機讀（8 執行緒）",
  "bench.row.random_reads_16_threads": "隨機讀（16 執行緒）",
  "bench.row.random_reads_32_threads": "隨機讀（32 執行緒）",
  "bench.row.removals": "刪除",
  "bench.row.retain": "retain",
  "bench.row.extract_if": "extract_if",
  "bench.row.pop": "pop",
  "bench.row.uncompacted_size": "未壓實體積",
  "bench.row.compacted_size": "壓實後體積",
};
