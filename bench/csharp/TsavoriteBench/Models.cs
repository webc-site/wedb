// Copyright (c) wedb authors.
// 结果值、格式化与 JSON 侧车：与 Rust 侧 result.rs / json.rs 同构（schema=1）。

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Wedb.Bench.Tsavorite
{
    /// <summary>吞吐计数单位（result.rs::ThroughputUnit）。</summary>
    internal enum ThroughputUnit
    {
        Key,
        Transaction,
        Scan,
    }

    internal static class ThroughputUnitExtensions
    {
        /// <summary>abbrev：统一为 M/s。</summary>
        public static string Abbreviation(this ThroughputUnit u) => "M/s";

        /// <summary>json.rs::to_result 反解用。</summary>
        public static ThroughputUnit FromSlug(string unit) => ThroughputUnit.Key;
    }

    /// <summary>
    /// 一段测量的结果值（result.rs::ResultType）。
    /// 时长统一存纳秒（ulong）——Rust 侧是 Duration/u128 ns，Stopwatch 换算是 100ns 粒度不够，
    /// 直接用 ns 保证 duration_ns 与 rate 同源。
    /// </summary>
    internal readonly struct ResultType
    {
        public enum KindOf { ThroughputKind, LatencyKind, SizeKind, NaKind }

        public readonly KindOf Type;
        public readonly ulong Count;
        public readonly ulong DurationNs;
        public readonly ThroughputUnit Unit;
        public readonly ulong Bytes;

        ResultType(KindOf t, ulong count, ulong ns, ThroughputUnit unit, ulong bytes)
        {
            Type = t; Count = count; DurationNs = ns; Unit = unit; Bytes = bytes;
        }

        public static ResultType Keys(ulong count, ulong ns) => new(KindOf.ThroughputKind, count, ns, ThroughputUnit.Key, 0);
        public static ResultType Txns(ulong count, ulong ns) => new(KindOf.ThroughputKind, count, ns, ThroughputUnit.Transaction, 0);
        public static ResultType Scans(ulong count, ulong ns) => new(KindOf.ThroughputKind, count, ns, ThroughputUnit.Scan, 0);
        public static ResultType Latency(ulong ns) => new(KindOf.LatencyKind, 0, ns, ThroughputUnit.Key, 0);
        public static ResultType SizeInBytes(ulong bytes) => new(KindOf.SizeKind, 0, 0, ThroughputUnit.Key, bytes);
        public static ResultType NA => new(KindOf.NaKind, 0, 0, ThroughputUnit.Key, 0);

        /// <summary>result.rs::rate()：count / 秒；非吞吐为 0。</summary>
        public double Rate() => Type == KindOf.ThroughputKind && DurationNs > 0
            ? Count / ((double)DurationNs / 1_000_000_000.0)
            : 0.0;

        public string KindString() => Type switch
        {
            KindOf.ThroughputKind => "throughput",
            KindOf.LatencyKind => "latency",
            KindOf.SizeKind => "size",
            _ => "na",
        };

        /// <summary>Display 等价文本（不含单位）。</summary>
        public string Format() => Type switch
        {
            KindOf.NaKind => "N/A",
            KindOf.ThroughputKind => Formats.FormatRate(Rate()),
            KindOf.LatencyKind => Formats.FormatDuration(DurationNs),
            _ => Formats.FormatSize(Bytes),
        };

        /// <summary>with_unit() 等价：吞吐把单位写进文本。</summary>
        public string WithUnit() => Type == KindOf.ThroughputKind ? Format() + " " + Unit.Abbreviation() : Format();

        /// <summary>result.rs::is_better_than：吞吐大者优，时延/尺寸小者优；N/A 永不占优。</summary>
        public bool IsBetterThan(in ResultType other) => (Type, other.Type) switch
        {
            (KindOf.ThroughputKind, KindOf.ThroughputKind) => Rate() > other.Rate(),
            (KindOf.LatencyKind, KindOf.LatencyKind) => DurationNs < other.DurationNs,
            (KindOf.SizeKind, KindOf.SizeKind) => Bytes < other.Bytes,
            _ => false,
        };

        /// <summary>json.rs::JsonRow::to_result 的还原侧。</summary>
        public static ResultType FromRow(JsonRow row) => row.Kind switch
        {
            "throughput" => new ResultType(KindOf.ThroughputKind, row.Count ?? 0, row.DurationNs ?? 0,
                ThroughputUnitExtensions.FromSlug(row.Unit ?? "key/s"), 0),
            "latency" => Latency(row.DurationNs ?? 0),
            "size" => SizeInBytes(row.Bytes ?? 0),
            _ => NA,
        };
    }

    /// <summary>
    /// 与 Rust 侧逐字符一致的呈现格式：format_rate（三位有效+SI）、format_duration（整毫秒）、
    /// format_size（byte-unit Binary 口径，两位小数）、metric_key（小写+下划线）。
    /// </summary>
    internal static class Formats
    {
        /// <summary>统一以 M (百万/秒) 为基准呈现吞吐速率，保持各操作在同一量纲可比。</summary>
        public static string FormatRate(double rate)
        {
            double m = rate / 1_000_000.0;
            if (m == 0.0) return "0.00";
            if (m < 0.01) return "<0.01";
            int precision = m >= 100.0 ? 1 : 2;
            double rounded = Math.Round(m, precision, MidpointRounding.AwayFromZero);
            if (rounded.Equals(-0.0))
                rounded = 0.0;
            return rounded.ToString("F" + precision.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        }

        /// <summary>result.rs::format_duration：纳秒四舍五入到整毫秒。</summary>
        public static string FormatDuration(ulong ns)
        {
            ulong millis = (ns + 500_000) / 1_000_000;
            return millis.ToString(CultureInfo.InvariantCulture) + "ms";
        }

        /// <summary>
        /// result.rs::Display 的 SizeInBytes 分支：byte-unit 5.2.6
        /// `Byte::from_u64(n).get_appropriate_unit(Binary)` 后 `{:.2}`——
        /// 取不超过值的最大二进制单位（KiB/MiB/GiB…），值恒显两位小数；
        /// 不足 1024 字节时单位为 B，byte-unit 对 B 走 `{value}` 常规 Display（f64 最短可回读形式）。
        /// </summary>
        public static string FormatSize(ulong bytes)
        {
            string[] units = ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];
            int unitIndex = -1; // -1 表示 B
            for (int i = 0; i < units.Length; i++)
            {
                double divisor = Math.Pow(1024.0, i + 1);
                if ((double)bytes >= divisor)
                    unitIndex = i;
                else
                    break;
            }
            if (unitIndex < 0)
                return bytes.ToString(CultureInfo.InvariantCulture) + " B";
            double scaled = (double)bytes / Math.Pow(1024.0, unitIndex + 1);
            double rounded = Math.Round(scaled, 2, MidpointRounding.AwayFromZero);
            return rounded.ToString("F2", CultureInfo.InvariantCulture) + " " + units[unitIndex];
        }

        /// <summary>result.rs::metric_key：小写字母数字保留，其余折叠成单个下划线，尾部下划线裁掉。</summary>
        public static string MetricKey(string name)
        {
            var sb = new System.Text.StringBuilder(name.Length);
            bool lastUnderscore = false;
            foreach (char ch in name)
            {
                if (char.IsAsciiLetterOrDigit(ch))
                {
                    sb.Append(char.ToLowerInvariant(ch));
                    lastUnderscore = false;
                }
                else if (!lastUnderscore)
                {
                    sb.Append('_');
                    lastUnderscore = true;
                }
            }
            while (sb.Length > 0 && sb[^1] == '_')
                sb.Length--;
            return sb.ToString();
        }
    }

    // ===== JSON 侧车（json.rs schema=1；字段名、顺序与 null 语义一致） =====

    internal sealed class JsonRow
    {
        [JsonPropertyName("key")] public string Key { get; set; } = "";
        [JsonPropertyName("name")] public string Name { get; set; } = "";
        [JsonPropertyName("kind")] public string Kind { get; set; } = "";
        [JsonPropertyName("unit")] public string? Unit { get; set; }
        [JsonPropertyName("count")] public ulong? Count { get; set; }
        [JsonPropertyName("duration_ns")] public ulong? DurationNs { get; set; }
        [JsonPropertyName("bytes")] public ulong? Bytes { get; set; }
        [JsonPropertyName("formatted")] public string Formatted { get; set; } = "";
        [JsonPropertyName("rate")] public double? Rate { get; set; }
        [JsonPropertyName("winner")] public bool Winner { get; set; }

        /// <summary>json.rs::JsonRow::from_result。</summary>
        public static JsonRow FromResult(string name, in ResultType result)
        {
            var row = new JsonRow
            {
                Key = Formats.MetricKey(name),
                Name = name,
                Kind = result.KindString(),
                Formatted = result.Format(),
            };
            switch (result.Type)
            {
                case ResultType.KindOf.ThroughputKind:
                    row.Count = result.Count;
                    row.DurationNs = result.DurationNs;
                    row.Rate = result.Rate();
                    row.Unit = result.Unit.Abbreviation();
                    break;
                case ResultType.KindOf.LatencyKind:
                    row.DurationNs = result.DurationNs;
                    break;
                case ResultType.KindOf.SizeKind:
                    row.Bytes = result.Bytes;
                    break;
            }
            return row;
        }
    }

    internal sealed class JsonEngine
    {
        [JsonPropertyName("name")] public string Name { get; set; } = "";
        [JsonPropertyName("source")] public string Source { get; set; } = "";
        [JsonPropertyName("status")] public string Status { get; set; } = "ok";
        [JsonPropertyName("detail")] public string? Detail { get; set; }
        [JsonPropertyName("peak_memory_bytes")] public ulong? PeakMemoryBytes { get; set; }
        [JsonPropertyName("rows")] public List<JsonRow> Rows { get; set; } = [];
    }

    /// <summary>runner.rs::ChildPayload——子进程回传的列结果（{"engine":{...}}）。</summary>
    internal sealed class ChildPayload
    {
        [JsonPropertyName("engine")] public JsonEngine? Engine { get; set; }
    }

    internal sealed class JsonRun
    {
        [JsonPropertyName("schema")] public uint Schema { get; set; } = 1;
        [JsonPropertyName("generated_at_unix")] public ulong GeneratedAtUnix { get; set; }
        [JsonPropertyName("commit")] public string Commit { get; set; } = "";
        [JsonPropertyName("branch")] public string Branch { get; set; } = "";
        [JsonPropertyName("version")] public string Version { get; set; } = "";
        [JsonPropertyName("platform")] public string Platform { get; set; } = "";
        [JsonPropertyName("machine")] public MachineInfo? Machine { get; set; }
        [JsonPropertyName("workload")] public WorkloadDto? Workload { get; set; }
        [JsonPropertyName("notes")] public List<string> Notes { get; set; } = [];
        [JsonPropertyName("engines")] public List<JsonEngine> Engines { get; set; } = [];

        /// <summary>json.rs::mark_winners：同平台每行标出最优（并列全标）；单引擎直返。</summary>
        public void MarkWinners()
        {
            if (Engines.Count < 2)
                return;
            int rowCount = Engines[0].Rows.Count;
            for (int i = 0; i < rowCount; i++)
            {
                var results = new ResultType[Engines.Count];
                for (int j = 0; j < Engines.Count; j++)
                    results[j] = ResultType.FromRow(Engines[j].Rows[i]);
                int best = -1;
                for (int j = 0; j < results.Length; j++)
                {
                    if (results[j].Type == ResultType.KindOf.NaKind)
                        continue;
                    if (best < 0 || results[j].IsBetterThan(results[best]))
                        best = j;
                }
                if (best >= 0)
                {
                    string winnerText = results[best].Format();
                    foreach (var e in Engines)
                        if (e.Rows[i].Formatted == winnerText)
                            e.Rows[i].Winner = true;
                }
            }
        }

        public string ToJson() => JsonSerializer.Serialize(this, JsonWriteOptions.Pretty);

        /// <summary>json.rs::unix_now。</summary>
        public static ulong UnixNow() => (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    }

    internal static class JsonWriteOptions
    {
        /// <summary>serde_json::to_string_pretty 等价：2 空格缩进；null 字段照写；非 ASCII 不转义（serde 原样输出 UTF-8）。</summary>
        public static readonly JsonSerializerOptions Pretty = new()
        {
            WriteIndented = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.Never,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        };

        /// <summary>serde_json::to_string 等价：紧凑无缩进（子进程载荷）。</summary>
        public static readonly JsonSerializerOptions Compact = new()
        {
            WriteIndented = false,
            DefaultIgnoreCondition = JsonIgnoreCondition.Never,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        };
    }

    /// <summary>
    /// config.rs::Workload 的 serde 镜像：字段名与 Rust 完全一致，供 JSON 同构比较。
    /// Rust serde 对 usize/u64 写无引号整数，C# 统一 ulong；thread_counts 为数组。
    /// </summary>
    internal sealed class WorkloadDto
    {
        [JsonPropertyName("read_iterations")] public int ReadIterations { get; set; }
        [JsonPropertyName("bulk_elements")] public ulong BulkElements { get; set; }
        [JsonPropertyName("sorted_elements")] public ulong SortedElements { get; set; }
        [JsonPropertyName("individual_writes")] public ulong IndividualWrites { get; set; }
        [JsonPropertyName("nosync_writes")] public ulong NosyncWrites { get; set; }
        [JsonPropertyName("batch_writes")] public ulong BatchWrites { get; set; }
        [JsonPropertyName("batch_size")] public ulong BatchSize { get; set; }
        [JsonPropertyName("scan_iterations")] public int ScanIterations { get; set; }
        [JsonPropertyName("num_reads")] public ulong NumReads { get; set; }
        [JsonPropertyName("num_scans")] public ulong NumScans { get; set; }
        [JsonPropertyName("scan_len")] public int ScanLen { get; set; }
        [JsonPropertyName("pop_removals")] public ulong PopRemovals { get; set; }
        [JsonPropertyName("pop_sample_removals")] public ulong PopSampleRemovals { get; set; }
        [JsonPropertyName("slow_pop_sample_limit_ms")] public ulong SlowPopSampleLimitMs { get; set; }
        [JsonPropertyName("key_size")] public int KeySize { get; set; }
        [JsonPropertyName("value_size")] public int ValueSize { get; set; }
        [JsonPropertyName("rng_seed")] public ulong RngSeed { get; set; }
        [JsonPropertyName("cache_size")] public ulong CacheSize { get; set; }
        [JsonPropertyName("thread_counts")] public List<int> ThreadCounts { get; set; } = [];

        public static WorkloadDto FromWorkload(Workload w) => new()
        {
            ReadIterations = w.ReadIterations,
            BulkElements = (ulong)w.BulkElements,
            SortedElements = (ulong)w.SortedElements,
            IndividualWrites = (ulong)w.IndividualWrites,
            NosyncWrites = (ulong)w.NosyncWrites,
            BatchWrites = (ulong)w.BatchWrites,
            BatchSize = (ulong)w.BatchSize,
            ScanIterations = w.ScanIterations,
            NumReads = (ulong)w.NumReads,
            NumScans = (ulong)w.NumScans,
            ScanLen = w.ScanLen,
            PopRemovals = (ulong)w.PopRemovals,
            PopSampleRemovals = (ulong)w.PopSampleRemovals,
            SlowPopSampleLimitMs = w.SlowPopSampleLimitMs,
            KeySize = w.KeySize,
            ValueSize = w.ValueSize,
            RngSeed = w.RngSeed,
            CacheSize = w.CacheSize,
            ThreadCounts = new List<int>(w.ThreadCounts),
        };

        /// <summary>解析 runner 传入的 --workload-json（serde 形态）。</summary>
        public static Workload ToWorkload(WorkloadDto d) => new()
        {
            ReadIterations = d.ReadIterations,
            BulkElements = (long)d.BulkElements,
            SortedElements = (long)d.SortedElements,
            IndividualWrites = (long)d.IndividualWrites,
            NosyncWrites = (long)d.NosyncWrites,
            BatchWrites = (long)d.BatchWrites,
            BatchSize = (long)d.BatchSize,
            ScanIterations = d.ScanIterations,
            NumReads = (long)d.NumReads,
            NumScans = (long)d.NumScans,
            ScanLen = d.ScanLen,
            PopRemovals = (long)d.PopRemovals,
            PopSampleRemovals = (long)d.PopSampleRemovals,
            SlowPopSampleLimitMs = d.SlowPopSampleLimitMs,
            KeySize = d.KeySize,
            ValueSize = d.ValueSize,
            RngSeed = d.RngSeed,
            CacheSize = d.CacheSize,
            ThreadCounts = d.ThreadCounts.Count > 0 ? d.ThreadCounts.ToArray() : [4, 8, 16, 32],
        };
    }
}
