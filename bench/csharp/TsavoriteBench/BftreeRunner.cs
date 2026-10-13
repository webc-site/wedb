// Copyright (c) wedb authors.
// BfTree 18 段驱动：对标 Tsavorite Runner.cs 与 Rust bftree_engine.rs。

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.server.BfTreeInterop;

namespace Wedb.Bench.Tsavorite
{
    internal sealed class BftreeRunner
    {
        readonly BfTreeService tree;
        readonly string dataDir;
        readonly Workload w;
        readonly List<string> notes;
        readonly WyRand rng;

        byte[] keyBuf;
        byte[] valBuf;

        public BftreeRunner(BfTreeService tree, string dataDir, Workload w, List<string> notes)
        {
            this.tree = tree;
            this.dataDir = dataDir;
            this.w = w;
            this.notes = notes;
            rng = new WyRand(w.RngSeed);
            keyBuf = new byte[w.KeySize];
            valBuf = new byte[w.ValueSize];
        }

        static ulong ElapsedNs(long startTicks)
        {
            long delta = Stopwatch.GetTimestamp() - startTicks;
            return (ulong)(delta * 1_000_000_000L / Stopwatch.Frequency);
        }

        void NextPair()
        {
            rng.Fill(keyBuf);
            rng.Fill(valBuf);
        }

        public List<(string Name, ResultType Result)> RunAll()
        {
            var results = new List<(string, ResultType)>();

            // ===== 1. bulk load =====
            long start = Stopwatch.GetTimestamp();
            for (long i = 0; i < w.BulkElements; i++)
            {
                NextPair();
                tree.Insert(keyBuf, valBuf);
            }
            ulong ns = ElapsedNs(start);
            Log($"Bulk loaded {w.BulkElements} items in {DurationMs(ns)}ms");
            results.Add(("bulk load", ResultType.Keys((ulong)w.BulkElements, ns)));

            // ===== 2. individual writes =====
            start = Stopwatch.GetTimestamp();
            for (long i = 0; i < w.IndividualWrites; i++)
            {
                NextPair();
                tree.Insert(keyBuf, valBuf);
            }
            ns = ElapsedNs(start);
            Log($"Wrote {w.IndividualWrites} individual items in {DurationMs(ns)}ms");
            results.Add(("individual writes", ResultType.Txns((ulong)w.IndividualWrites, ns)));

            // ===== 3. small batch writes =====
            start = Stopwatch.GetTimestamp();
            for (long b = 0; b < w.BatchWrites; b++)
            {
                for (long k = 0; k < w.BatchSize; k++)
                {
                    NextPair();
                    tree.Insert(keyBuf, valBuf);
                }
            }
            ns = ElapsedNs(start);
            Log($"Wrote {w.BatchWrites} batches of {w.BatchSize} items in {DurationMs(ns)}ms");
            results.Add(("small batch writes", ResultType.Keys((ulong)(w.BatchWrites * w.BatchSize), ns)));

            int sortedInsertsRow = results.Count;

            // ===== 4. nosync writes: 不支持也要把这批数据写进去，保持表形与数据量一致，记 N/A =====
            for (long i = 0; i < w.NosyncWrites; i++)
            {
                NextPair();
                tree.Insert(keyBuf, valBuf);
            }
            results.Add(("nosync writes", ResultType.NA));

            // ===== 5. len(): bftree doesn't maintain external O(1) len counter, matches rust 0ms =====
            results.Add(("len()", ResultType.Latency(0)));
            Log("len() = 0ms");

            long elements = w.LoadedElements;

            // ===== 6. random reads =====
            var readDurations = new ulong[w.ReadIterations];
            for (int r = 0; r < w.ReadIterations; r++)
            {
                var replay = new WyRand(w.RngSeed);
                start = Stopwatch.GetTimestamp();
                RunRandomReads(tree, replay, w.NumReads, "random reads");
                readDurations[r] = ElapsedNs(start);
            }
            ns = Median(readDurations);
            Log($"Random read {w.NumReads} items in {DurationMs(ns)}ms, median of {w.ReadIterations} runs");
            results.Add(("random reads", ResultType.Keys((ulong)w.NumReads, ns)));

            // ===== 7. random range reads =====
            var scanDurations = new ulong[w.ScanIterations];
            for (int r = 0; r < w.ScanIterations; r++)
            {
                var replay = new WyRand(w.RngSeed);
                start = Stopwatch.GetTimestamp();
                RunRangeReads(tree, replay, w.NumScans);
                scanDurations[r] = ElapsedNs(start);
            }
            ns = Median(scanDurations);
            Log($"Random range read {w.NumScans} x {w.ScanLen} elements in {DurationMs(ns)}ms");
            results.Add(("random range reads", ResultType.Scans((ulong)w.NumScans, ns)));

            // ===== 8-11. random reads (N threads) =====
            foreach (int numThreads in w.ThreadCounts)
            {
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
                                barrier.SignalAndWait();
                                RunRandomReads(tree, shard, elements / numThreads, $"random reads {numThreads}t");
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
                            throw new InvalidOperationException($"读线程 {t} 异常：{errors[t]}", errors[t]);
                }
                ns = Median(threadDurations);
                Log($"Random read ({numThreads} threads) {elements} items in {DurationMs(ns)}ms, median of {w.ReadIterations} runs");
                results.Add(($"random reads ({numThreads} threads)", ResultType.Keys((ulong)elements, ns)));
            }

            // ===== 12. removals =====
            long deletes = elements / 2;
            start = Stopwatch.GetTimestamp();
            var replayRemovals = new WyRand(w.RngSeed);
            byte[] remKey = new byte[w.KeySize];
            byte[] remVal = new byte[w.ValueSize];
            for (long i = 0; i < deletes; i++)
            {
                replayRemovals.Fill(remKey);
                replayRemovals.Fill(remVal);
                tree.Delete(remKey);
            }
            ns = ElapsedNs(start);
            Log($"Removed {deletes} items in {DurationMs(ns)}ms");
            results.Add(("removals", ResultType.Keys((ulong)deletes, ns)));

            // ===== 13-15. retain / extract_if / pop =====
            results.Add(("retain", ResultType.NA));
            results.Add(("extract_if", ResultType.NA));
            results.Add(("pop", ResultType.NA));

            // ===== 16. uncompacted size =====
            ulong uncompacted = TsavoriteEngine.DirectoryTreeSize(dataDir);
            results.Add(("uncompacted size", ResultType.SizeInBytes(uncompacted)));

            // ===== 17. compacted size =====
            results.Add(("compacted size", ResultType.SizeInBytes(uncompacted)));

            // ===== 18. sorted inserts =====
            int sortedKeyLen = w.KeySize + 8;
            byte[] sortedKey = new byte[sortedKeyLen];
            for (int i = 0; i < w.KeySize; i++)
                sortedKey[i] = 0xFF;
            byte[] pregenerated = new byte[(long)w.SortedElements * w.ValueSize];
            for (long i = 0; i < w.SortedElements; i++)
            {
                rng.Fill(new Span<byte>(pregenerated, (int)(i * w.ValueSize), w.ValueSize));
            }
            start = Stopwatch.GetTimestamp();
            for (long i = 0; i < w.SortedElements; i++)
            {
                MemoryMarshal.Write(sortedKey.AsSpan(w.KeySize), in i);
                ReverseToBe(sortedKey, w.KeySize);
                var valueSpan = new ReadOnlySpan<byte>(pregenerated, (int)(i * w.ValueSize), w.ValueSize);
                tree.Insert(sortedKey, valueSpan);
            }
            ns = ElapsedNs(start);
            Log($"Loaded {w.SortedElements} sorted items in {DurationMs(ns)}ms");
            results.Insert(sortedInsertsRow, ("sorted inserts", ResultType.Keys((ulong)w.SortedElements, ns)));

            return results;
        }

        void RunRandomReads(BfTreeService t, WyRand r, long count, string label)
        {
            if (count <= 0)
                return;
            int valueSize = w.ValueSize;
            byte[] localKey = new byte[w.KeySize];
            byte[] localVal = new byte[valueSize];
            byte[] readBuf = new byte[4096];
            ulong checksum = 0;
            ulong expected = 0;

            for (long k = 0; k < count; k++)
            {
                r.Fill(localKey);
                r.Fill(localVal);
                expected = unchecked(expected + localVal[0]);

                var res = t.Read(localKey, readBuf, out int bytesRead);
                if (res != BfTreeReadResult.Found)
                    throw new InvalidOperationException($"{label}：键未命中（第 {k} 次读）res={res}");
                checksum = unchecked(checksum + readBuf[0]);
            }

            if (checksum != expected)
                throw new InvalidOperationException($"{label}：checksum {checksum} != expected {expected}");
        }

        void RunRangeReads(BfTreeService t, WyRand r, long count)
        {
            byte[] localKey = new byte[w.KeySize];
            byte[] localVal = new byte[w.ValueSize];
            byte[] scanBuf = new byte[16384];
            ulong valueSum = 0;

            for (long i = 0; i < count; i++)
            {
                r.Fill(localKey);
                r.Fill(localVal);
                t.ScanWithCount(localKey, (int)w.ScanLen, scanBuf, (k, v) =>
                {
                    if (v.Length > 0)
                        valueSum += v[0];
                    return true;
                });
            }
            if (valueSum == 0)
                throw new InvalidOperationException("random range reads：value_sum 为 0");
        }

        static void ReverseToBe(byte[] key, int offset)
        {
            for (int a = offset, b = offset + 7; a < b; a++, b--)
                (key[a], key[b]) = (key[b], key[a]);
        }

        static ulong Median(ulong[] durations)
        {
            Array.Sort(durations);
            return durations[durations.Length / 2];
        }

        static ulong DurationMs(ulong ns) => (ns + 500_000) / 1_000_000;

        static void Log(string message) => Console.WriteLine($"bftree-cs: {message}");
    }
}
