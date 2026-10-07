export default {
  "meta.title": "WeDB Bench",
  "meta.description":
    "wedb 的 redb 同构基准：wkv、wbftree 对比 fjall、rocksdb、sqlite，四类 runner 镜像上的表格、横向对比图与跨提交趋势。",

  "nav.section.bars": "对比图表",
  "nav.section.trend": "历史趋势",
  "nav.section.table": "最新表格",
  "nav.data": "机读数据",
  "nav.github": "GitHub",
  "nav.lang": "语言",

  "hero.title": "按 redb 的口径测 wedb",
  "hero.body":
    "行序、单位与加粗规则全部对齐 redb-bench：同样的 18 段 workload、同样的速率单位、同样的行内最优标注。被测的是 wedb 自己的 wkv（混合日志 KV）与 wbftree（有序索引），对照列是 fjall、rocksdb 与 sqlite。",
  "hero.scale_standard": "redb 标准档",
  "hero.headline": "{bulk} 装载 · {sorted} 有序 · {key}B 键 · {value}B 值",
  "hero.columns_platforms": "{columns} 列 · {platforms} 平台",
  "hero.empty": "还没有评测数据：主分支跑完一轮 Benchmark 后这里就会有内容。",

  "section.bars.title": "分平台横向对比",
  "section.bars.desc": "选一段 workload 看同一平台上各引擎的相对位置；条长就是该段的数值，★ 标出行内最优。",
  "section.trend.title": "历史趋势",
  "section.trend.desc":
    "同一平台跨提交的纵向变化：每轮主分支评测落一个点，用来盯回归而不是看单轮快照。把鼠标放到点上能看到引擎、版本与环比。",
  "section.table.title": "最新表格",
  "section.table.desc":
    "所选平台最近一次评测的完整表：行序、单位与加粗规则和 redb 公布的表逐字同构，可与它的表并列阅读。",

  "pane.bars": "单段对比",
  "pane.trend": "跨提交趋势",
  "bars.higher_better": "越高越好",
  "bars.empty": "还没有可对比的平台数据。",
  "bars.aria": "各引擎在所选 workload 段的横向柱状对比",
  "bars.best_hint": "★ 该段最快",

  "trend.aria": "所选平台各引擎在所选 workload 段的跨提交趋势",
  "trend.empty": "还没有历史点：主分支每跑完一轮 Benchmark 就会多一个点。",
  "trend.dots":
    "{n} 个点 · 横轴每个点标那一轮的版本号 · 缺口表示那一列当轮没出值（崩溃、超时或未参与），线在缺口处断开，不做插值。",
  "trend.tip_delta": "较上轮",
  "trend.tip_na": "无数据",

  "table.empty": "该平台这一轮还没有数据。",
  "table.utc": "UTC",
  "table.workload":
    "{bulk} 装载 · {sorted} 有序 · {reads} 随机读 · {scans}×{scan_len} 范围读 · {key}B 键 · {value}B 值 · 缓存 {cache}",
  "table.median_note":
    "读类段落取 {n} 次运行的中位数；每列一个子进程，崩溃或超时的列整列折 N/A",
  "status.crashed": "崩溃",
  "status.timeout": "超时",
  "commit.local": "本地",

  "foot.line1":
    "表格由 bench 工厂端（Rust）产出：18 段 workload、单位标注与行内最优加粗的规则都在 crates/wedb-bench 里，站点只消费机读 JSON，不在浏览器里重抄一遍格式化口径。",
  "foot.line2":
    "流水线：Benchmark 工作流在四类 runner 镜像上按引擎分列跑，benchreport merge 并成平台表并累积历史，Website 工作流把站点连同历史发到 gh-pages。",
  "foot.line3": "数据每轮主分支评测后更新。",
  "foot.links": "链接",
  "foot.workflow_bench": "Benchmark 工作流",
  "foot.workflow_website": "Website 工作流",

  "bench.row.bulk_load": "批量装载",
  "bench.row.individual_writes": "单条写入",
  "bench.row.small_batch_writes": "小批写入",
  "bench.row.sorted_inserts": "有序插入",
  "bench.row.nosync_writes": "免同步写入",
  "bench.row.len": "len()",
  "bench.row.random_reads": "随机读",
  "bench.row.random_range_reads": "范围随机读",
  "bench.row.random_reads_4_threads": "随机读（4 线程）",
  "bench.row.random_reads_8_threads": "随机读（8 线程）",
  "bench.row.random_reads_16_threads": "随机读（16 线程）",
  "bench.row.random_reads_32_threads": "随机读（32 线程）",
  "bench.row.removals": "删除",
  "bench.row.retain": "保留 retain",
  "bench.row.extract_if": "抽取 extract_if",
  "bench.row.pop": "弹出 pop",
  "bench.row.uncompacted_size": "未压实体积",
  "bench.row.compacted_size": "压实后体积",
};
