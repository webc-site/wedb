export default {
  "meta.title": "WeDB Bench",
  "meta.description":
    "wedbs Benchmark im redb-Format: wkv und wbftree gegen fjall, rocksdb und sqlite auf vier Runner-Images — Tabellen, Balkenvergleiche und Trends pro Commit.",

  "nav.section.bars": "Diagramme",
  "nav.section.trend": "Verlauf",
  "nav.section.table": "Tabellen",
  "nav.data": "Rohdaten",
  "nav.github": "GitHub",
  "nav.lang": "Sprache",

  "hero.title": "wedb, gemessen wie redb sich selbst misst",
  "hero.body":
    "Zeilenreihenfolge, Einheiten und Fettmarkierungsregeln folgen dem redb-bench-Harness: dieselben 18 Workload-Segmente, dieselben Rateeinheiten, dieselbe Markierung der schnellsten Spalte pro Zeile. Getestet werden wedbs eigenes wkv (Hash-Log-KV) und wbftree (geordneter B+-Baum), verglichen mit fjall, rocksdb und sqlite.",
  "hero.scale_standard": "redb-Standardmaßstab",
  "hero.headline": "{bulk} Bulk · {sorted} sortiert · {key}B-Schlüssel · {value}B-Werte",
  "hero.columns_platforms": "{columns} Spalten · {platforms} Plattformen",
  "hero.empty":
    "Noch keine Benchmark-Daten: Sie erscheinen, sobald ein Benchmark-Durchlauf auf dem Hauptzweig abgeschlossen ist.",

  "section.bars.title": "Direktvergleich pro Plattform",
  "section.bars.desc":
    "Wähle ein Workload-Segment, um die Engines auf einer Plattform zu vergleichen. Balkenlänge ist der Wert des Segments; ★ markiert die schnellste.",
  "section.trend.title": "Trend über Commits",
  "section.trend.desc":
    "Dieselbe Plattform über die Zeit, ein Punkt pro Hauptzweig-Durchlauf — um Regressionen zu beobachten statt eines einzelnen Schnappschusses. Maus über einen Punkt zeigt Engine, Version und Delta.",
  "section.table.title": "Neueste Tabellen",
  "section.table.desc":
    "Die vollständige Tabelle des neuesten Durchlaufs der gewählten Plattform: gleiche Zeilenreihenfolge, Einheiten und Fettmarkierungsregeln wie die von redb veröffentlichte Tabelle, sodass sich beide nebeneinander lesen lassen.",

  "pane.bars": "Einzelnes Segment",
  "pane.trend": "Über Commits",
  "bars.higher_better": "Höher ist besser",
  "bars.empty": "Noch keine Plattformdaten zum Vergleich.",
  "bars.aria": "Waagerechter Balkenvergleich der Engines im gewählten Workload-Segment",
  "bars.best_hint": "★ schnellste Engine im Segment",

  "trend.aria": "Commit-Trend der Engines im gewählten Workload-Segment",
  "trend.empty": "Noch keine Trenddaten: jeder abgeschlossene Hauptzweig-Durchlauf ergänzt einen Punkt.",
  "trend.dots":
    "{n} Punkte · die Achse beschriftet jeden Durchlauf mit seiner Version · eine Lücke bedeutet, dass diese Spalte in diesem Durchlauf keinen Wert erzeugt hat (abgestürzt, Timeout oder nicht Teil der Matrix), die Linie bricht dort und wird nie interpoliert.",
  "trend.tip_delta": "ggü. vorherigem Durchlauf",
  "trend.tip_na": "keine Daten",

  "table.empty": "Diese Plattform hat für den gewählten Durchlauf keine Daten.",
  "table.utc": "UTC",
  "table.workload":
    "{bulk} Bulk · {sorted} sortiert · {reads} Zufallsreads · {scans}×{scan_len} Bereichsreads · {key}B-Schlüssel · {value}B-Werte · {cache} Cache",
  "table.median_note":
    "Read-Segmente sind der Median aus {n} Durchläufen; jede Spalte läuft in einem eigenen Unterprozess, ein Absturz oder Timeout faltet deshalb die ganze Spalte zu N/A.",
  "status.crashed": "abgestürzt",
  "status.timeout": "Zeitüberschreitung",
  "commit.local": "lokal",

  "foot.line1":
    "Die Tabellen erzeugt die Bench-Fabrikseite (Rust): die 18 Workload-Segmente, die Einheitenbeschriftungen und die Fettmarkierung der besten Spalte pro Zeile leben in crates/wedb-bench. Die Seite konsumiert nur das maschinenlesbare JSON und reimplementiert diese Formatierung nie nach.",
  "foot.line2":
    "Pipeline: der Benchmark-Workflow lässt auf vier Runner-Images pro Engine je eine Spalte laufen, benchreport merge fügt sie zu Plattformtabellen zusammen und hängt die Historie an, und der Website-Workflow veröffentlicht die Seite samt dieser Historie auf gh-pages.",
  "foot.line3": "Daten aktualisieren sich nach jedem Benchmark-Durchlauf auf dem Hauptzweig.",
  "foot.links": "Weblinks",
  "foot.workflow_bench": "Benchmark-Workflow",
  "foot.workflow_website": "Website-Workflow",

  "bench.row.bulk_load": "Massenladen",
  "bench.row.individual_writes": "Einzelne Schreibvorgänge",
  "bench.row.small_batch_writes": "Kleine Batch-Schreibvorgänge",
  "bench.row.sorted_inserts": "Sortiertes Einfügen",
  "bench.row.nosync_writes": "Schreibvorgänge ohne Sync",
  "bench.row.len": "len()",
  "bench.row.random_reads": "Zufällige Lesezugriffe",
  "bench.row.random_range_reads": "Zufällige Bereichslesungen",
  "bench.row.random_reads_4_threads": "Zufällige Lesezugriffe (4 Threads)",
  "bench.row.random_reads_8_threads": "Zufällige Lesezugriffe (8 Threads)",
  "bench.row.random_reads_16_threads": "Zufällige Lesezugriffe (16 Threads)",
  "bench.row.random_reads_32_threads": "Zufällige Lesezugriffe (32 Threads)",
  "bench.row.removals": "Entfernen",
  "bench.row.retain": "retain",
  "bench.row.extract_if": "extract_if",
  "bench.row.pop": "pop",
  "bench.row.uncompacted_size": "Größe ohne Verdichtung",
  "bench.row.compacted_size": "Größe nach Verdichtung",
};
