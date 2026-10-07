// Copyright (c) wedb authors.
// 18 段驱动：与 Rust harness.rs 同名、同序、同单位；键值序列与主 rng 消耗次序逐字节同源。
// 持久化映射（task/bench.md §3）：
//   individual = Upsert + 同步等 flush 屏障；nosync = 仅 Upsert 不 flush；
//   bulk = 全程 Upsert 末尾一次 flush；batch = 1000 Upsert + 1 flush；
//   removals = Delete + flush；len = 全表遍历；random reads = Read + CompletePending 直到落定；
//   random range reads = 点读取锚点地址后 Log.Scan 步进 10。

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core;

namespace Wedb.Bench.Tsavorite
{
#pragma warning disable IDE0065 // Misplaced using directive
    // 与 Engine.cs 相同的泛型装配别名（using 别名是文件作用域，须各自声明）
    using BenchStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;
    using BenchAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>>;
#pragma warning restore IDE0065

    /// <summary>
    /// 逐段驱动 Tsavorite 引擎，返回 [(行名, 结果)]；行名与 harness.rs::row_names 逐字一致。
    /// </summary>
    internal sealed class Runner
    {
        readonly TsavoriteEngine engine;
        readonly Workload w;
        readonly List<string> notes;

        // 主 rng：与 Rust 侧同一份、同一消耗次序（bulk→individual→batch→nosync→sorted values）
        readonly WyRand rng;

        // 复用缓冲：写段与读段都按对填充，零分配热路径
        byte[] keyBuf;
        byte[] valBuf;

        // 读段批量槽：pinned，供异步落定后回读
        const int ReadBatchSlots = 256;

        public Runner(TsavoriteEngine engine, Workload w, List<string> notes)
        {
            this.engine = engine;
            this.w = w;
            this.notes = notes;
            rng = new WyRand(w.RngSeed);
            keyBuf = new byte[w.KeySize];
            valBuf = new byte[w.ValueSize];
        }

        static ulong ElapsedNs(long startTicks)
        {
            // Stopwatch.Frequency 在 arm64 macOS 为 1e9（tick=ns），通用换算保精度
            long delta = Stopwatch.GetTimestamp() - startTicks;
            return (ulong)(delta * 1_000_000_000L / Stopwatch.Frequency);
        }

        public List<(string Name, ResultType Result)> RunAll()
        {
            var results = new List<(string, ResultType)>();
            var (session, functions) = engine.NewMainSession();
            var bc = session.BasicContext;

            // ===== 1. bulk load：全程 Upsert，末尾一次提交屏障 =====
            long start = Stopwatch.GetTimestamp();
            for (long i = 0; i < w.BulkElements; i++)
            {
                NextPair();
                bc.Upsert(BenchKey.FromSpan(keyBuf), valBuf);
            }
            bc.CompletePending(true);
            engine.FlushBarrier();
            ulong ns = ElapsedNs(start);
            Log($"Bulk loaded {w.BulkElements} items in {DurationMs(ns)}ms");
            results.Add(("bulk load", ResultType.Keys((ulong)w.BulkElements, ns)));

            // ===== 2. individual writes：逐条 Upsert + 同步等提交屏障 =====
            start = Stopwatch.GetTimestamp();
            for (long i = 0; i < w.IndividualWrites; i++)
            {
                NextPair();
                bc.Upsert(BenchKey.FromSpan(keyBuf), valBuf);
                bc.CompletePending(true);
                engine.FlushBarrier();
            }
            ns = ElapsedNs(start);
            Log($"Wrote {w.IndividualWrites} individual items in {DurationMs(ns)}ms");
            results.Add(("individual writes", ResultType.Txns((ulong)w.IndividualWrites, ns)));

            // ===== 3. small batch writes：1000 Upsert + 1 次提交屏障 =====
            start = Stopwatch.GetTimestamp();
            for (long b = 0; b < w.BatchWrites; b++)
            {
                for (long k = 0; k < w.BatchSize; k++)
                {
                    NextPair();
                    bc.Upsert(BenchKey.FromSpan(keyBuf), valBuf);
                }
                bc.CompletePending(true);
                engine.FlushBarrier();
            }
            ns = ElapsedNs(start);
            Log($"Wrote {w.BatchWrites} batches of {w.BatchSize} items in {DurationMs(ns)}ms");
            results.Add(("small batch writes", ResultType.Keys((ulong)(w.BatchWrites * w.BatchSize), ns)));

            // 有序装载必须最后跑，以免污染尺寸测量；但结果行要放回写段之间
            int sortedInsertsRow = results.Count;

            // ===== 4. nosync writes：仅 Upsert，不等提交（引擎支持该档位） =====
            start = Stopwatch.GetTimestamp();
            for (long i = 0; i < w.NosyncWrites; i++)
            {
                NextPair();
                bc.Upsert(BenchKey.FromSpan(keyBuf), valBuf);
                if ((i & 1023) == 1023)
                    bc.CompletePending(false);
            }
            bc.CompletePending(true);
            ns = ElapsedNs(start);
            Log($"Wrote {w.NosyncWrites} individual items in {DurationMs(ns)}ms, nosync");
            results.Add(("nosync writes", ResultType.Txns((ulong)w.NosyncWrites, ns)));

            long elements = w.LoadedElements;

            // ===== 5. len()：全表真实遍历 =====
            start = Stopwatch.GetTimestamp();
            long len = engine.TraverseCount();
            ns = ElapsedNs(start);
            if (len != elements)
                throw new InvalidOperationException($"len() 遍历计数 {len} != 装载条目 {elements}");
            Log($"len() = {len} in {DurationMs(ns)}ms");
            results.Add(("len()", ResultType.Latency(ns)));

            // ===== 6. random reads：种子重放 + checksum 断言，取 read_iterations 轮中位数 =====
            var readDurations = new ulong[w.ReadIterations];
            for (int r = 0; r < w.ReadIterations; r++)
            {
                var replay = new WyRand(w.RngSeed);
                start = Stopwatch.GetTimestamp();
                RunRandomReads(bc, replay, w.NumReads, "random reads");
                readDurations[r] = ElapsedNs(start);
            }
            ns = Median(readDurations);
            Log($"Random read {w.NumReads} items in {DurationMs(ns)}ms, median of {w.ReadIterations} runs");
            results.Add(("random reads", ResultType.Keys((ulong)w.NumReads, ns)));

            // ===== 7. random range reads：点读取锚点，日志序步进 scan_len =====
            var scanDurations = new ulong[w.ScanIterations];
            for (int r = 0; r < w.ScanIterations; r++)
            {
                var replay = new WyRand(w.RngSeed);
                start = Stopwatch.GetTimestamp();
                RunRangeReads(bc, functions, replay, w.NumScans);
                scanDurations[r] = ElapsedNs(start);
            }
            ns = Median(scanDurations);
            Log($"Random range read {w.NumScans} x {w.ScanLen} elements in {DurationMs(ns)}ms");
            results.Add(("random range reads", ResultType.Scans((ulong)w.NumScans, ns)));

            // ===== 8-11. random reads (N threads) =====
            foreach (int numThreads in w.ThreadCounts)
            {
                // 与 Rust 侧同口径：3 轮取中位（每轮全新分片，同种子重放同键序），
                // 压短窗对同机负载瞬态的敏感度
                ulong[] threadDurations = new ulong[w.ReadIterations];
                for (int round = 0; round < w.ReadIterations; round++)
                {
                    var shards = WyRand.MakeRngShards(w.RngSeed, numThreads, elements, w.KeySize, w.ValueSize);
                    var barrier = new Barrier(numThreads);
                    var errors = new Exception?[numThreads];
                    start = Stopwatch.GetTimestamp();
                    var threads = new Thread[numThreads];
                    for (int t = 0; t < numThreads; t++)
                    {
                        int idx = t;
                        WyRand shard = shards[idx];
                        threads[idx] = new Thread(() =>
                        {
                            try
                            {
                                using var s = engine.NewThreadSession();
                                barrier.SignalAndWait();
                                RunRandomReads(s.BasicContext, shard, elements / numThreads, $"random reads {numThreads}t");
                            }
                            catch (Exception e)
                            {
                                errors[idx] = e;
                            }
                        });
                        threads[idx].Start();
                    }
                    foreach (var th in threads)
                        th.Join();
                    barrier.Dispose();
                    threadDurations[round] = (ulong)ElapsedNs(start);
                    for (int t = 0; t < numThreads; t++)
                        if (errors[t] is not null)
                            throw new InvalidOperationException($"random reads ({numThreads} threads) 线程 {t} 失败", errors[t]);
                }
                ns = Median(threadDurations);
                Log($"Random read ({numThreads} threads) {elements} items in {DurationMs(ns)}ms, median of {w.ReadIterations} runs");
                results.Add(($"random reads ({numThreads} threads)", ResultType.Keys((ulong)elements, ns)));
            }

            // ===== 12. removals：单事务删 elements/2，末尾一次提交屏障 =====
            long deletes = elements / 2;
            start = Stopwatch.GetTimestamp();
            {
                var rmRng = new WyRand(w.RngSeed);
                for (long i = 0; i < deletes; i++)
                {
                    NextPairInto(rmRng);
                    bc.Delete(BenchKey.FromSpan(keyBuf));
                    if ((i & 1023) == 1023)
                        bc.CompletePending(false);
                }
                bc.CompletePending(true);
                engine.FlushBarrier();
            }
            ns = ElapsedNs(start);
            Log($"Removed {deletes} items in {DurationMs(ns)}ms");
            results.Add(("removals", ResultType.Keys((ulong)deletes, ns)));

            // ===== 13. retain：引擎不支持，记 N/A，不做回填（与 Rust 同：主 rng 不消耗） =====
            Log("retain unsupported，记 N/A");
            results.Add(("retain", ResultType.NA));

            // ===== 14. extract_if：同上 =====
            Log("extract_if unsupported，记 N/A");
            results.Add(("extract_if", ResultType.NA));

            // ===== 15. pop：同上 =====
            Log("pop unsupported，记 N/A");
            results.Add(("pop", ResultType.NA));

            // ===== 16. uncompacted size：目录树字节和 =====
            ulong uncompacted = TsavoriteEngine.DirectoryTreeSize(engine.DataDir);
            results.Add(("uncompacted size", ResultType.SizeInBytes(uncompacted)));

            // ===== 17. compacted size：Checkpoint + ShiftBeginAddress(truncateLog) 后重走目录树 =====
            start = Stopwatch.GetTimestamp();
            if (engine.Compact())
            {
                ns = ElapsedNs(start);
                Log($"Compacted in {DurationMs(ns)}ms");
                ulong compacted = TsavoriteEngine.DirectoryTreeSize(engine.DataDir);
                results.Add(("compacted size", ResultType.SizeInBytes(compacted)));
            }
            else
            {
                results.Add(("compacted size", ResultType.NA));
            }

            // ===== 18. sorted inserts：键 0xFF×key_size + i 的 8 字节大端；值续用主 rng =====
            // 先 drop 旧会话（引擎已 CloseSessions），重开一条连接再装载
            var (sortedSession, _) = engine.NewMainSession();
            var sbc = sortedSession.BasicContext;
            int sortedKeyLen = w.KeySize + 8;
            byte[] sortedKey = new byte[sortedKeyLen];
            for (int i = 0; i < w.KeySize; i++)
                sortedKey[i] = 0xFF;
            var sortedValues = new byte[w.ValueSize];
            // 与 Rust 同：值在计时区外预生成（rng.fill(&mut value)），键按索引大端编码
            byte[] pregenerated = new byte[(long)w.SortedElements * w.ValueSize];
            for (long i = 0; i < w.SortedElements; i++)
            {
                rng.Fill(new Span<byte>(pregenerated, (int)(i * w.ValueSize), w.ValueSize));
            }
            start = Stopwatch.GetTimestamp();
            for (long i = 0; i < w.SortedElements; i++)
            {
                MemoryMarshal.Write(sortedKey.AsSpan(w.KeySize), in i); // 前 8 字节即大端尾段需转
                ReverseToBe(sortedKey, w.KeySize);
                var valueSpan = new Span<byte>(pregenerated, (int)(i * w.ValueSize), w.ValueSize);
                sbc.Upsert(BenchKey.FromSpan(sortedKey), valueSpan);
                if ((i & 1023) == 1023)
                    sbc.CompletePending(false);
            }
            sbc.CompletePending(true);
            engine.FlushBarrier();
            ns = ElapsedNs(start);
            Log($"Loaded {w.SortedElements} sorted items in {DurationMs(ns)}ms");
            results.Insert(sortedInsertsRow, ("sorted inserts", ResultType.Keys((ulong)w.SortedElements, ns)));

            return results;
        }

        // ===== 段内热循环 =====

        /// <summary>写段配对：与 random_pair 同（先 fill(key) 再 fill(value)），但零分配。</summary>
        void NextPair()
        {
            rng.Fill(keyBuf);
            rng.Fill(valBuf);
        }

        void NextPairInto(WyRand r)
        {
            r.Fill(keyBuf);
            r.Fill(valBuf);
        }

        /// <summary>
        /// 随机点读：Read + CompletePendingWithOutputs 直到状态落定，
        /// checksum 取回读值首字节、与预生成值首字节逐条对账（wrapping add，与 Rust release 同）。
        /// </summary>
        void RunRandomReads(BasicContext<BenchKey, PinnedSpanByte, SpanByteAndMemory, Empty, BenchSessionFunctions, BenchStoreFunctions, BenchAllocator> bc,
            WyRand r, long count, string label)
        {
            if (count <= 0)
                return;
            int valueSize = w.ValueSize;
            // 键/值缓冲必须按调用（即按线程）私有：共享 this.keyBuf 会让并发分片互相覆写刚生成的键
            byte[] localKey = new byte[w.KeySize];
            byte[] localVal = new byte[valueSize];
            var slots = GC.AllocateArray<byte>(ReadBatchSlots * valueSize, pinned: true);
            ulong checksum = 0;
            ulong expected = 0;
            ref byte slotsBase = ref MemoryMarshal.GetArrayDataReference(slots);

            long issuedInBatch = 0;
            long k = 0;
            while (k < count)
            {
                r.Fill(localKey);
                r.Fill(localVal);
                expected = unchecked(expected + localVal[0]);

                int slot = (int)(issuedInBatch % ReadBatchSlots);
                Span<byte> outSpan = MemoryMarshal.CreateSpan(ref Unsafe.Add(ref slotsBase, slot * valueSize), valueSize);
                var output = SpanByteAndMemory.FromPinnedSpan(outSpan);
                var st = bc.Read(BenchKey.FromSpan(localKey), ref output);
                if (!st.IsPending)
                {
                    if (!st.Found)
                        throw new InvalidOperationException($"{label}：键未命中（第 {k} 次读）");
                    checksum = unchecked(checksum + outSpan[0]);
                }
                issuedInBatch++;
                k++;

                if (issuedInBatch == ReadBatchSlots)
                {
                    DrainPending(bc, ref slotsBase, ref checksum, valueSize, label);
                    issuedInBatch = 0;
                }
            }
            if (issuedInBatch > 0)
                DrainPending(bc, ref slotsBase, ref checksum, valueSize, label);

            if (checksum != expected)
                throw new InvalidOperationException($"{label}：checksum {checksum} != expected {expected}");
        }

        static void DrainPending(
            BasicContext<BenchKey, PinnedSpanByte, SpanByteAndMemory, Empty, BenchSessionFunctions, BenchStoreFunctions, BenchAllocator> bc,
            ref byte slotsBase, ref ulong checksum, int valueSize, string label)
        {
            bc.CompletePendingWithOutputs(out var completed, wait: true);
            while (completed.Next())
            {
                ref var cr = ref completed.Current;
                if (!cr.Status.Found)
                    throw new InvalidOperationException($"{label}：异步读未命中");
                checksum = unchecked(checksum + cr.Output.SpanByte.Span[0]);
                cr.Output.Dispose();
            }
            completed.Dispose();
        }

        /// <summary>
        /// 随机范围读：点读同一键拿逻辑锚点（Reader 回调记录 ReadInfo.Address），
        /// 再从锚点沿日志序步进 scan_len 条存活记录，累加 value[0]；断言和 &gt; 0。
        /// 与 Rust hash 引擎 range_from 的 find_tag+hlog 顺序扫同机制（日志序而非键序，入 notes）。
        /// </summary>
        void RunRangeReads(
            BasicContext<BenchKey, PinnedSpanByte, SpanByteAndMemory, Empty, BenchSessionFunctions, BenchStoreFunctions, BenchAllocator> bc,
            BenchSessionFunctions functions, WyRand r, long count)
        {
            int valueSize = w.ValueSize;
            var slot = GC.AllocateArray<byte>(valueSize, pinned: true);
            ref byte slotBase = ref MemoryMarshal.GetArrayDataReference(slot);
            ulong valueSum = 0;
            long misses = 0;
            for (long i = 0; i < count; i++)
            {
                r.Fill(keyBuf);
                r.Fill(valBuf);
                Span<byte> outSpan = MemoryMarshal.CreateSpan(ref slotBase, valueSize);
                var output = SpanByteAndMemory.FromPinnedSpan(outSpan);
                functions.LastReadAddress = -1;
                var st = bc.Read(BenchKey.FromSpan(keyBuf), ref output);
                if (st.IsPending)
                {
                    bc.CompletePending(wait: true);
                }
                long anchor = functions.LastReadAddress;
                if (anchor >= 0)
                    valueSum += engine.RangeScanValueSum(anchor, w.ScanLen);
                else
                    misses++;
            }
            if (valueSum == 0)
                throw new InvalidOperationException($"random range reads：value_sum 为 0（misses={misses}）");
        }

        static void ReverseToBe(byte[] key, int offset)
        {
            // MemoryMarshal.Write 写的是本机小端 i；Rust 侧键尾段是 i 的大端字节，这里就地翻转 8 字节
            for (int a = offset, b = offset + 7; a < b; a++, b--)
                (key[a], key[b]) = (key[b], key[a]);
        }

        static ulong Median(ulong[] durations)
        {
            // harness::median_duration：排序后取 durations[len/2]（偶数样本取靠后的中间值）
            Array.Sort(durations);
            return durations[durations.Length / 2];
        }

        static ulong DurationMs(ulong ns) => (ns + 500_000) / 1_000_000;

        static void Log(string message) => Console.WriteLine($"tsavorite-cs: {message}");
    }
}
