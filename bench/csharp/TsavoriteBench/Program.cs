// Copyright (c) wedb authors.
// 入口：命令行契约与 bench/perf_vs_cs.sh 传给 Rust 侧的一致
// （--scale/--data-path/--json/--commit/--branch，另补 --quick/--cache-mb/--workload-json/
// --version/--list/--rng-selftest/--help），组装与 Rust json.rs schema=1 同构的侧车。
// 任何段崩溃/超时都不吞成 N/A 了事：engines[0].status 记 crashed/timeout 并写 detail。

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Threading;

namespace Wedb.Bench.Tsavorite
{
    internal static class Program
    {
        const string DefaultHashName = "tsavorite-cs";
        const string DefaultHashSource = "https://github.com/microsoft/garnet/tree/main/libs/storage/Tsavorite";
        const string DefaultBfTreeName = "bftree-cs";
        const string DefaultBfTreeSource = "https://github.com/microsoft/garnet/tree/main/libs/native/bftree-garnet";

        const string Usage = @"用法：TsavoriteBench [选项]

  --engine NAME      评测引擎：hash（默认）或 bftree
  --scale F          按 F 缩放条目量级（redb 标准档为 1.0）
  --quick            等价 --scale 0.02，用于冒烟与 CI 调试
  --cache-mb N       覆盖引擎缓存预算（默认 4096 MiB，按物理内存自动收敛）
  --timeout-secs N   墙钟预算，超时按 timeout 记账（默认由负载字节数折算，下限 900s）
  --data-path DIR    评测数据目录（默认 OS 临时目录下 wedb-bench-cs；每次跑新建独立子目录，禁复用旧段文件）
  --json FILE        写出机读结果（schema=1，与 Rust 侧同构）
  --commit SHA       标注提交（默认取 GITHUB_SHA）
  --branch NAME      标注分支（默认取 GITHUB_REF_NAME）
  --version VER      标注版本身份（默认取 WEDB_BENCH_VERSION）
  --workload-json V  负载覆盖：JSON 串或 JSON 文件路径（serde 形态，走 WorkloadDto）
  --list             打印段名清单
  --rng-selftest     打印 WyRand 前 8 对键值 hex（与 Rust fastrand 2.5.0 同种子对账）
";

        static int Main(string[] rawArgv)
        {
            var argv = new List<string>(rawArgv);
            try
            {
                return Run(argv);
            }
            catch (UsageErrorException e)
            {
                Console.Error.WriteLine($"{e.Message}\n{Usage}");
                return 1;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"致命错误：{e}");
                return 1;
            }
        }

        static int Run(List<string> argv)
        {
            // ===== 解析参数（与 Rust runner.rs::parse_args 同风格） =====
            string engineType = "hash";
            double? scale = null;
            bool quick = false, list = false, rngSelftest = false, help = false;
            int? cacheMb = null;
            ulong? timeoutSecs = null;
            string? dataPath = null, jsonOut = null, commit = null, branch = null, version = null, workloadJson = null;

            for (int i = 0; i < argv.Count; i++)
            {
                string TakeValue(string name)
                {
                    i++;
                    if (i >= argv.Count)
                        throw new UsageErrorException($"{name} 缺少取值");
                    return argv[i];
                }
                switch (argv[i])
                {
                    case "--engine": engineType = TakeValue("--engine").ToLowerInvariant(); break;
                    case "--scale":
                        if (!double.TryParse(TakeValue("--scale"), NumberStyles.Float, CultureInfo.InvariantCulture, out double s))
                            throw new UsageErrorException("--scale 取值无效");
                        scale = s;
                        break;
                    case "--quick": quick = true; break;
                    case "--cache-mb":
                        if (!int.TryParse(TakeValue("--cache-mb"), out int mb) || mb <= 0)
                            throw new UsageErrorException("--cache-mb 取值无效");
                        cacheMb = mb;
                        break;
                    case "--timeout-secs":
                        if (!ulong.TryParse(TakeValue("--timeout-secs"), out ulong t) || t == 0)
                            throw new UsageErrorException("--timeout-secs 取值无效");
                        timeoutSecs = t;
                        break;
                    case "--data-path": dataPath = TakeValue("--data-path"); break;
                    case "--json": jsonOut = TakeValue("--json"); break;
                    case "--commit": commit = TakeValue("--commit"); break;
                    case "--branch": branch = TakeValue("--branch"); break;
                    case "--version": version = TakeValue("--version"); break;
                    case "--workload-json": workloadJson = TakeValue("--workload-json"); break;
                    case "--list": list = true; break;
                    case "--rng-selftest": rngSelftest = true; break;
                    case "--help" or "-h": help = true; break;
                    default:
                        throw new UsageErrorException($"未知参数：{argv[i]}");
                }
            }

            string engineName;
            string engineSource;
            bool isBfTree;
            switch (engineType)
            {
                case "bftree" or "bftree-cs":
                    isBfTree = true;
                    engineName = DefaultBfTreeName;
                    engineSource = DefaultBfTreeSource;
                    break;
                case "hash" or "tsavorite" or "tsavorite-cs":
                    isBfTree = false;
                    engineName = DefaultHashName;
                    engineSource = DefaultHashSource;
                    break;
                default:
                    throw new UsageErrorException($"未知引擎：{engineType}（支持 hash 或 bftree）");
            }

            if (help)
            {
                Console.WriteLine(Usage);
                return 0;
            }
            if (list)
            {
                foreach (string name in RowNames(Workload.Default()))
                    Console.WriteLine(name);
                return 0;
            }
            if (rngSelftest)
            {
                // 与 Rust harness::random_pair 同种子同尺寸：seed=3、key 24B、value 150B
                var w = Workload.Default();
                Console.WriteLine($"WyRand selftest: seed={w.RngSeed} key_size={w.KeySize} value_size={w.ValueSize} pairs=8");
                Console.Write(WyRand.SelfTest(w.RngSeed, w.KeySize, w.ValueSize, 8));
                return 0;
            }

            // ===== 数据目录：根 = --data-path / WEDB_BENCHMARK_DIR / 临时目录；每次跑用独立子目录（禁复用旧段文件） =====
            string dataRoot = dataPath ?? Environment.GetEnvironmentVariable("WEDB_BENCHMARK_DIR")
                ?? Path.Combine(Path.GetTempPath(), "wedb-bench-cs");
            Directory.CreateDirectory(dataRoot);

            var machine = MachineInfo.Detect(dataRoot, dataRoot);

            // ===== 负载解析（resolve_workload 同式） =====
            var notes = new List<string>();
            double resolvedScale = scale ?? (quick ? 0.02 : 1.0);
            Workload workload;
            if (workloadJson is not null)
            {
                string text = workloadJson.TrimStart().StartsWith('{')
                    ? workloadJson
                    : File.ReadAllText(workloadJson);
                WorkloadDto dto = JsonSerializer.Deserialize<WorkloadDto>(text)
                    ?? throw new UsageErrorException("--workload-json 解析失败");
                workload = WorkloadDto.ToWorkload(dto);
                notes.Add("负载由 --workload-json 指定");
            }
            else
            {
                workload = Workload.Scaled(resolvedScale);
                if (cacheMb is not null)
                    workload.CacheSize = (ulong)cacheMb.Value * 1024 * 1024;
                if (workload.CapCacheToMemory(machine.TotalMemoryGib))
                    notes.Add(string.Create(CultureInfo.InvariantCulture,
                        $"缓存预算按物理内存收敛到 {workload.CacheSize / (1024.0 * 1024.0 * 1024.0):F2} GiB（redb 标准档为 4 GiB）"));
                if (resolvedScale != 1.0)
                    notes.Add(string.Create(CultureInfo.InvariantCulture, $"负载按 {resolvedScale}× 缩放，非 redb 标准档"));
            }
            if (machine.DataFs is "tmpfs" or "ramfs")
                notes.Add($"数据目录 {machine.DataDir} 位于 {machine.DataFs}（内存盘），尺寸段不反映物理盘占用");

            // 两侧配置差异（task/bench.md §3 映射表；perfGate 口径说明）
            if (isBfTree)
            {
                notes.Add("bftree-garnet 原生 C# P/Invoke 驱动（Garnet.server.BfTreeInterop）");
                notes.Add("nosync: bftree 无事务提交持久化档位，记 N/A");
                notes.Add("retain/extract_if/pop 无对应托管 API，记 N/A");
            }
            else
            {
                notes.Add("持久化映射：individual/small batch/removals 用 Upsert+CompletePending(wait)+Log.Flush(wait) 作提交屏障，bulk 末尾一次屏障，nosync 不等屏障——与 Rust 侧 write txn commit(+fsync) 同位不同机制");
                notes.Add("随机范围读锚点为日志物理序（ReadInfo.Address + Log.Scan 步进），非键序；与 Rust hash 引擎 find_tag+hlog 顺序扫同口径，与 btree 类键序扫存在机制差异");
                notes.Add("compact 段 = FoldOver 检查点 + ShiftBeginAddress(tail 对齐 1GB 段界, truncateLog)；Rust 侧段界 64MiB，尺寸两段存在段粒度差异");
                notes.Add("retain/extract_if/pop 无对应 API，记 N/A（不回填，与 Rust 不支持路径同形）");
            }

            ulong budgetSecs = timeoutSecs ?? workload.DerivedTimeoutSecs();

            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"平台 {machine.Platform} · {machine.LogicalCores} 核 · {machine.TotalMemoryGib:F1} GiB · 数据目录 {machine.DataDir} ({machine.DataFs})"));
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"负载 bulk {workload.BulkElements} / sorted {workload.SortedElements} / reads {workload.NumReads} · 缓存 {workload.CacheSize / (1024.0 * 1024.0 * 1024.0):F2} GiB · 单引擎预算 {budgetSecs}s"));
            Console.WriteLine($"=== {engineName} ({engineSource}) ===");

            // ===== 跑 18 段：独立工作线程 + 墙钟预算看门狗；崩溃/超时如实记账 =====
            string workDir = Path.Combine(dataRoot, $"{engineName}-{Environment.ProcessId}-{Guid.NewGuid():N}");
            Directory.CreateDirectory(workDir);

            List<(string Name, ResultType Result)>? rows = null;
            Exception? failure = null;
            using var done = new ManualResetEventSlim(false);
            var worker = new Thread(() =>
            {
                try
                {
                    if (isBfTree)
                    {
                        ulong ringCap = RingCapacity(workload.CacheSize);
                        using var bftree = new Garnet.server.BfTreeInterop.BfTreeService(
                            Garnet.server.BfTreeInterop.StorageBackendType.Disk,
                            Path.Combine(workDir, "bftree.data"),
                            enableSnapshots: false,
                            cbSizeByte: ringCap,
                            cbMinRecordSize: 4,
                            cbMaxRecordSize: 4096,
                            cbMaxKeyLen: 512,
                            leafPageSize: 16 * 1024);
                        rows = new BftreeRunner(bftree, workDir, workload, notes).RunAll();
                        bftree.Dispose();
                    }
                    else
                    {
                        using var engine = new TsavoriteEngine(workDir, workload, notes);
                        rows = new Runner(engine, workload, notes).RunAll();
                        engine.Dispose(); // 先落盘释放再采样峰值内存
                    }
                    peakMemoryBytes = SamplePeakMemory();
                }
                catch (Exception e)
                {
                    failure = e;
                }
                finally
                {
                    done.Set();
                }
            })
            { IsBackground = true, Name = $"{engineName}-runner" };

            var wallStart = Stopwatch.GetTimestamp();
            worker.Start();
            bool finished = done.Wait(TimeSpan.FromSeconds(budgetSecs));
            double wallSecs = (Stopwatch.GetTimestamp() - wallStart) / (double)Stopwatch.Frequency;

            JsonEngine engineResult;
            if (!finished)
            {
                engineResult = NaEngine(workload, engineName, engineSource, "timeout",
                    $"超过 {budgetSecs}s 墙钟预算被终止（runner 线程未落定，进程随之退出；日志见 stdout）");
                FinishAndPrint(machine, workload, notes, engineResult, commit, branch, version, jsonOut, wallSecs);
                // 后台线程仍挂着：直接终止进程，语义与 Rust parent kill 子进程一致
                Environment.Exit(0);
            }
            else if (failure is not null)
            {
                Console.Error.WriteLine($"{engineName} 崩溃：{failure}");
                string detail = $"{failure.GetType().Name}: {failure.Message}";
                if (failure.InnerException is not null)
                    detail += $" <- {failure.InnerException.GetType().Name}: {failure.InnerException.Message}";
                engineResult = NaEngine(workload, engineName, engineSource, "crashed", detail.ReplaceLineEndings(" "));
                FinishAndPrint(machine, workload, notes, engineResult, commit, branch, version, jsonOut, wallSecs);
            }
            else
            {
                engineResult = new JsonEngine
                {
                    Name = engineName,
                    Source = engineSource,
                    Status = "ok",
                    Detail = null,
                    PeakMemoryBytes = peakMemoryBytes,
                    // 本地不变式：worker 线程仅在 RunAll 正常返回后才给 rows 赋非空值，
                    // 且赋值先于 finally 的 done.Set()；走到此分支必有 finished && failure 为空，
                    // 事件等待构成 happens-before，rows 在此处必非空。
                    Rows = [.. rows!.ConvertAll(r => JsonRow.FromResult(r.Name, r.Result))],
                };
                Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"--- {engineName} 用时 {wallSecs:F1}s"));
                FinishAndPrint(machine, workload, notes, engineResult, commit, branch, version, jsonOut, wallSecs);
            }

            TryRemoveDir(workDir);
            return 0;
        }

        static ulong? peakMemoryBytes;

        /// <summary>组装 JsonRun（schema=1）→ MarkWinners → 同构表格 → 落盘 --json。</summary>
        static void FinishAndPrint(MachineInfo machine, Workload w, List<string> notes,
            JsonEngine engineResult, string? commit, string? branch, string? version, string? jsonOut, double wallSecs)
        {
            var run = new JsonRun
            {
                Schema = 1,
                GeneratedAtUnix = JsonRun.UnixNow(),
                Commit = EnvOr("GITHUB_SHA", commit, "local"),
                Branch = EnvOr("GITHUB_REF_NAME", branch, "local"),
                Version = EnvOr("WEDB_BENCH_VERSION", version, ""),
                Platform = machine.Platform,
                Machine = machine,
                Workload = WorkloadDto.FromWorkload(w),
                Notes = notes,
                Engines = [engineResult],
            };
            run.MarkWinners();

            PrintTable(run.Engines[0]);
            if (run.Engines[0].Status != "ok")
                Console.WriteLine($"列 {run.Engines[0].Name}：{run.Engines[0].Status} — {run.Engines[0].Detail}");
            foreach (string note in run.Notes)
                Console.WriteLine($"备注：{note}");

            if (jsonOut is not null)
            {
                string? parent = Path.GetDirectoryName(Path.GetFullPath(jsonOut));
                if (!string.IsNullOrEmpty(parent))
                    Directory.CreateDirectory(parent);
                File.WriteAllText(jsonOut, run.ToJson());
                Console.WriteLine($"机读结果：{jsonOut}");
            }
        }

        /// <summary>与 Rust table.rs 同构的 markdown 表（单引擎列：无从比较不标最优）。</summary>
        static void PrintTable(JsonEngine engine)
        {
            Console.WriteLine();
            Console.WriteLine($"|  | {engine.Name} (M/s) |");
            Console.WriteLine("| - | - |");
            foreach (var row in engine.Rows)
            {
                Console.WriteLine($"| {row.Name} | {row.Formatted} |");
            }
        }

        /// <summary>harness.rs::row_names——与 Rust 逐字同名同序。</summary>
        static List<string> RowNames(Workload w)
        {
            var names = new List<string>
            {
                "bulk load", "individual writes", "small batch writes", "sorted inserts",
                "nosync writes", "len()", "random reads", "random range reads",
            };
            foreach (int t in w.ThreadCounts)
                names.Add($"random reads ({t} threads)");
            names.AddRange(["removals", "retain", "extract_if", "pop", "uncompacted size", "compacted size"]);
            return names;
        }

        /// <summary>崩溃/超时列：整列 N/A 垫行（runner.rs::na_engine 同形）。</summary>
        static JsonEngine NaEngine(Workload w, string engineName, string engineSource, string status, string detail)
        {
            var rows = new List<JsonRow>();
            foreach (string name in RowNames(w))
                rows.Add(JsonRow.FromResult(name, ResultType.NA));
            return new JsonEngine
            {
                Name = engineName,
                Source = engineSource,
                Status = status,
                Detail = detail,
                PeakMemoryBytes = null,
                Rows = rows,
            };
        }

        static ulong RingCapacity(ulong cacheSize)
        {
            const ulong minRing = 2 * 16 * 1024;
            if (cacheSize <= minRing) return minRing;
            if (cacheSize % 2 != 0 && cacheSize < 1024 * 1024)
                return 32 * 1024 * 1024;
            int leadingZeros = System.Numerics.BitOperations.LeadingZeroCount(cacheSize);
            ulong floor = 1UL << (63 - leadingZeros);
            return Math.Max(floor, minRing);
        }

        static string EnvOr(string var, string? explicitValue, string fallback)
            => explicitValue ?? Environment.GetEnvironmentVariable(var) ?? fallback;

        static ulong SamplePeakMemory()
        {
            try
            {
                using var p = Process.GetCurrentProcess();
                return (ulong)p.PeakWorkingSet64;
            }
            catch
            {
                return (ulong)Environment.WorkingSet;
            }
        }

        static void TryRemoveDir(string dir)
        {
            try
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
            }
            catch { /* 与 Rust WorkDir drop 同：清理失败忽略 */ }
        }

        sealed class UsageErrorException(string message) : Exception(message);
    }
}
