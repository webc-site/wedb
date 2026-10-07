export default {
  "meta.title": "WeDB Bench",
  "meta.description":
    "redb-совместимый бенчмарк wedb: wkv и wbftree против fjall, rocksdb и sqlite на четырёх образах раннеров — таблицы, сравнение полосами и тренды по коммитам.",

  "nav.section.bars": "Диаграммы",
  "nav.section.trend": "Тренды",
  "nav.section.table": "Таблицы",
  "nav.data": "Сырые данные",
  "nav.github": "GitHub",
  "nav.lang": "Язык",

  "hero.title": "wedb, измеряемый так же, как redb измеряет себя",
  "hero.body":
    "Порядок строк, единицы и правила жирного шрифта повторяют redb-bench: те же 18 сегментов нагрузки, те же единицы скорости, та же отметка лучшего в строке. Проверяемые колонки — собственные wkv (hash log KV) и wbftree (упорядоченное B+ дерево) из wedb, сравнение с fjall, rocksdb и sqlite.",
  "hero.scale_standard": "стандартная шкала redb",
  "hero.headline": "{bulk} пакетно · {sorted} сортированно · ключи {key}B · значения {value}B",
  "hero.columns_platforms": "{columns} колонок · {platforms} платформ",
  "hero.empty":
    "Данных бенчмарка пока нет: они появятся, как только завершится прогон Benchmark в основной ветке.",

  "section.bars.title": "Сравнение по платформам",
  "section.bars.desc":
    "Выберите сегмент нагрузки, чтобы сравнить движки на одной платформе. Длина полосы — значение сегмента; ★ отмечает самый быстрый.",
  "section.trend.title": "Тренд по коммитам",
  "section.trend.desc":
    "Одна платформа во времени, по точке на каждый прогон основной ветки — чтобы следить за регрессиями, а не за одним снимком. Наведите на точку: движок, версия и изменение к прежнему прогону.",
  "section.table.title": "Последние таблицы",
  "section.table.desc":
    "Полная таблица последнего прогона выбранной платформы: тот же порядок строк, единицы и правила выделения лучшего, что и в таблицах redb, — их можно читать рядом.",

  "pane.bars": "Один сегмент",
  "pane.trend": "По коммитам",
  "bars.higher_better": "Больше — лучше",
  "bars.empty": "Пока нет данных платформ для сравнения.",
  "bars.aria": "Горизонтальное сравнение полос движков на выбранном сегменте нагрузки",
  "bars.best_hint": "★ самый быстрый в сегменте",

  "trend.aria": "Тренд движков по коммитам на выбранном сегменте нагрузки",
  "trend.empty": "Трендов пока нет: каждый завершённый прогон основной ветки добавляет точку.",
  "trend.dots":
    "{n} точек · ось подписывает каждый прогон его версией · разрыв означает, что колонка не дала значения в этом прогоне (сбой, тайм-аут или вне матрицы), линия там прерывается и никогда не интерполируется.",
  "trend.tip_delta": "к прежнему прогону",
  "trend.tip_na": "нет данных",

  "table.empty": "У этой платформы нет данных за выбранный прогон.",
  "table.utc": "UTC",
  "table.workload":
    "{bulk} пакетно · {sorted} сортированно · {reads} случайных чтений · {scans}×{scan_len} диапазонных чтений · ключи {key}B · значения {value}B · кэш {cache}",
  "table.median_note":
    "Сегменты чтения — медиана {n} прогонов; каждая колонка выполняется в собственном подпроцессе, поэтому сбой или тайм-аут сводит всю колонку к N/A.",
  "status.crashed": "сбой",
  "status.timeout": "тайм-аут",
  "commit.local": "локальный",

  "foot.line1":
    "Таблицы формирует сторона стенда (Rust): 18 сегментов нагрузки, подписи единиц и выделение лучшего в строке живут в crates/wedb-bench. Сайт только потребляет машиночитаемый JSON и заново это форматирование не реализует.",
  "foot.line2":
    "Конвейер: workflow Benchmark гонит по одной колонке на движок на четырёх образах раннеров, benchreport merge сводит их в таблицы по платформам и дописывает историю, а workflow Website публикует сайт вместе с историей на gh-pages.",
  "foot.line3": "Данные обновляются после каждого прогона бенчмарка в основной ветке.",
  "foot.links": "Ссылки",
  "foot.workflow_bench": "Workflow Benchmark",
  "foot.workflow_website": "Workflow Website",

  "bench.row.bulk_load": "пакетная загрузка",
  "bench.row.individual_writes": "поодиночные записи",
  "bench.row.small_batch_writes": "малые пакетные записи",
  "bench.row.sorted_inserts": "сортированные вставки",
  "bench.row.nosync_writes": "записи без синхронизации",
  "bench.row.len": "len()",
  "bench.row.random_reads": "случайные чтения",
  "bench.row.random_range_reads": "случайные диапазонные чтения",
  "bench.row.random_reads_4_threads": "случайные чтения (4 потока)",
  "bench.row.random_reads_8_threads": "случайные чтения (8 потоков)",
  "bench.row.random_reads_16_threads": "случайные чтения (16 потоков)",
  "bench.row.random_reads_32_threads": "случайные чтения (32 потока)",
  "bench.row.removals": "удаления",
  "bench.row.retain": "retain",
  "bench.row.extract_if": "extract_if",
  "bench.row.pop": "pop",
  "bench.row.uncompacted_size": "размер без компакции",
  "bench.row.compacted_size": "размер после компакции",
};
