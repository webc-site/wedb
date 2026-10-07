// Copyright (c) wedb authors.
// Tsavorite（vendored C# 原版）引擎封装：装配范式抄 garnet 的 KV.benchmark，
// 持久化语义按 task/bench.md §3 的映射表实现；本文件不修改 garnet 任何源码。

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core;

namespace Wedb.Bench.Tsavorite
{
#pragma warning disable IDE0065 // Misplaced using directive
    // 与 KV.benchmark 相同的泛型装配别名：SpanByte 值 + 变长键（IKey）
    using BenchStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;
    using BenchAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>>;
#pragma warning restore IDE0065

    /// <summary>
    /// 变长键：前 <see cref="Length"/> 字节参与哈希与比较（SpanByteComparer 只看 KeyBytes）。
    /// 显式布局 36B：4B 长度 + 32B 数据区；随机键 24B、有序键 32B 都放得下。
    /// IsPinned=false 与 KvKey 同法——指针只在本条操作栈帧内有效，落盘由引擎复制。
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    internal unsafe struct BenchKey : IKey
    {
        /// <summary>键数据区容量（随机段 24B、有序段 32B 共用）。</summary>
        public const int DataCapacity = 32;

        [FieldOffset(0)]
        public int Length;

        [FieldOffset(4)]
        public fixed byte Data[DataCapacity];

        /// <summary>从 byte[] 装载（每次拷贝；调用点在计时循环外准备或就地写入栈槽）。</summary>
        public static BenchKey FromBytes(byte[] bytes)
        {
            BenchKey k = default;
            k.Length = bytes.Length;
            fixed (byte* src = bytes)
                Buffer.MemoryCopy(src, k.Data, DataCapacity, bytes.Length);
            return k;
        }

        /// <summary>从栈上 span 装载（多线程热路径避免 byte[] 分配）。</summary>
        public static BenchKey FromSpan(ReadOnlySpan<byte> bytes)
        {
            BenchKey k = default;
            k.Length = bytes.Length;
            bytes.CopyTo(new Span<byte>(k.Data, bytes.Length));
            return k;
        }

        public readonly bool IsPinned => false;

        public readonly bool IsEmpty => false;

        /// <summary>数据区起始于结构体偏移 4（FieldOffset），KeyBytes 必须指数据区而非结构体首地址。</summary>
        public readonly ReadOnlySpan<byte> KeyBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                fixed (byte* p = Data)
                    return new ReadOnlySpan<byte>(p, Length);
            }
        }

        public readonly bool HasNamespace => false;

        public readonly ReadOnlySpan<byte> NamespaceBytes => [];
    }

    /// <summary>
    /// 会话回调：Reader 记录 readInfo.Address（范围读锚点），其余走 SpanByteFunctions 默认复制。
    /// </summary>
    internal sealed class BenchSessionFunctions : SpanByteFunctions<Empty>
    {
        /// <summary>最近一次 Reader 回调对应的逻辑地址；范围读用它做 Seek 锚点。</summary>
        public long LastReadAddress = -1;

        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            LastReadAddress = readInfo.Address;
            return base.Reader(in srcLogRecord, ref input, ref output, ref readInfo);
        }
    }

    /// <summary>
    /// Tsavorite 引擎封装：设备/store/session 装配、flush 屏障、尺寸遍历、压缩程序。
    /// </summary>
    internal sealed class TsavoriteEngine : IDisposable
    {
        /// <summary>garnet 生产默认（defaults.conf）：页 16MB / 段 1GB / 内联值上限 16KB / 可变区 0.9。</summary>
        public const long GarnetPageSize = 16L << 20;
        public const long GarnetSegmentSize = 1L << 30;
        public const int GarnetMaxInlineValueSize = 16 << 10;
        public const double GarnetMutableFraction = 0.9;

        /// <summary>windex 桶字节数（Rust 侧同值）：C# Tsavorite 主索引桶同为 8 槽 64B……见 notes 口径说明。</summary>
        public const int IndexBucketBytes = 64;

        /// <summary>哈希桶数按 2 键/7 槽外推（与 Rust recommended_index_size 同式：buckets=next_pow2(ceil(2*keys/7))）。</summary>
        public const int MinIndexBuckets = 65_536;
        public const int MaxIndexBuckets = 16_777_216;

        readonly IDevice device;
        readonly TsavoriteKV<BenchStoreFunctions, BenchAllocator> store;
        readonly List<IDisposable> sessions = [];

        public readonly string DataDir;
        public readonly string CheckpointDir;

        /// <summary>引擎侧推导出的实际配置，写进 notes。</summary>
        public readonly string ConfigNote;

        public TsavoriteEngine(string dataDir, Workload w, List<string> notes)
        {
            DataDir = dataDir;
            Directory.CreateDirectory(dataDir);
            CheckpointDir = Path.Combine(dataDir, "cpr");
            Directory.CreateDirectory(CheckpointDir);

            long loaded = w.LoadedElements + w.SortedElements;
            var (indexBytes, logMemoryBytes) = PlanMemory(w.CacheSize, loaded);

            // 装配范式抄 KV.benchmark：managed LogDevice + SpanByte 值 + ObjectAllocator
            string logPath = Path.Combine(dataDir, "hlog");
            device = Devices.CreateLogDevice(logPath);

            var kvSettings = new KVSettings
            {
                IndexSize = indexBytes,
                LogDevice = device,
                LogMemorySize = logMemoryBytes,
                PageSize = GarnetPageSize,
                SegmentSize = GarnetSegmentSize,
                MaxInlineValueSize = GarnetMaxInlineValueSize,
                MutableFraction = GarnetMutableFraction,
                CheckpointDir = CheckpointDir,
                PreallocateLog = false,
                ReadCacheEnabled = false,
            };

            store = new TsavoriteKV<BenchStoreFunctions, BenchAllocator>(
                kvSettings,
                StoreFunctions.Create(SpanByteComparer.Instance, new SpanByteRecordTriggers()),
                (allocSettings, sf) => new BenchAllocator(allocSettings, sf));

            ConfigNote = $"Tsavorite 配置：IndexSize={indexBytes}B LogMemorySize={logMemoryBytes}B PageSize={GarnetPageSize}B SegmentSize={GarnetSegmentSize}B MaxInlineValueSize={GarnetMaxInlineValueSize}B MutableFraction={GarnetMutableFraction} managed设备 CheckpointDir=cpr";
            notes.Add("C# 侧 " + ConfigNote);
        }

        /// <summary>
        /// 与 Rust StoreConfig::from_memory_budget_with_keys(cache, Some(loaded_keys)) 同口径：
        /// 索引桶 = next_pow2(ceil(2*keys/7)) 钳制 [65536, 16777216]，但再受预算钳制
        /// （索引 ≤ min(budget/2, budget-16*page)，向下取 2 的幂）；日志 = 预算余量向下取 2 的幂。
        /// </summary>
        public static (long indexBytes, long logMemoryBytes) PlanMemory(ulong cacheSize, long expectedKeys)
        {
            ulong budget = Math.Max(cacheSize, 32UL << 20);
            long page = GarnetPageSize;

            // Rust：max_index_bytes = budget - 16*page；capped = min(budget/2, max_index_bytes)
            ulong maxIndexBytes = budget > (ulong)(16 * page) ? budget - (ulong)(16 * page) : 0;
            ulong capped = Math.Min(budget / 2, maxIndexBytes);
            capped = Math.Max(capped, (ulong)MinIndexBuckets * IndexBucketBytes);
            int maxBuckets = (int)Math.Clamp((long)(capped / (ulong)IndexBucketBytes), MinIndexBuckets, MaxIndexBuckets);
            long maxIndexSize = PrevPow2((ulong)maxBuckets);

            // recommended_index_size(keys) = next_pow2(ceil(2*keys/7)) 钳制 [MIN, MAX]
            ulong need = ((ulong)expectedKeys * 2 + 6) / 7;
            need = Math.Min(need, MaxIndexBuckets);
            long recommended = NextPow2(need);
            long buckets = Math.Min(recommended, maxIndexSize);
            buckets = Math.Clamp(buckets, MinIndexBuckets, MaxIndexBuckets);

            long indexBytes = buckets * IndexBucketBytes;
            ulong logBudget = budget > (ulong)indexBytes ? budget - (ulong)indexBytes : (ulong)(16 * page);
            long logMemory = PrevPow2(logBudget);
            if (logMemory < 16 * page)
                logMemory = 16 * page; // MIN_NUM_PAGES * page 兜底（Rust 同式）
            return (indexBytes, logMemory);
        }

        static long PrevPow2(ulong v)
        {
            if (v == 0) return 0;
            long p = 1;
            while (p << 1 <= (long)v && (p << 1) > 0) p <<= 1;
            return p;
        }

        static long NextPow2(ulong v)
        {
            long p = 1;
            while ((ulong)p < v && p < (1L << 62)) p <<= 1;
            return p;
        }

        /// <summary>主写会话（bulk/individual/batch/nosync/removals/sorted 与单线程读共用）。</summary>
        public (ClientSession<BenchKey, PinnedSpanByte, SpanByteAndMemory, Empty, BenchSessionFunctions, BenchStoreFunctions, BenchAllocator> Session, BenchSessionFunctions Functions) NewMainSession()
        {
            var functions = new BenchSessionFunctions();
            var session = store.NewSession<BenchKey, PinnedSpanByte, SpanByteAndMemory, Empty, BenchSessionFunctions>(functions);
            sessions.Add(session);
            return (session, functions);
        }

        /// <summary>多线程读会话（每线程一个）。</summary>
        public ClientSession<BenchKey, PinnedSpanByte, SpanByteAndMemory, Empty, BenchSessionFunctions, BenchStoreFunctions, BenchAllocator> NewThreadSession()
        {
            var session = store.NewSession<BenchKey, PinnedSpanByte, SpanByteAndMemory, Empty, BenchSessionFunctions>(new BenchSessionFunctions());
            sessions.Add(session);
            return session;
        }

        /// <summary>
        /// 提交屏障：本 vendored 版本没有 session 级 Commit/持久化选项，
        /// 与 bench.md §3 映射表一致，用 Log.Flush(wait:true)（ShiftReadOnlyAddress 到 tail）作为 flush 屏障。
        /// </summary>
        public void FlushBarrier() => store.Log.Flush(true);

        /// <summary>释放全部会话（压缩段前必须；对应 Rust drop(connection)）。</summary>
        public void CloseSessions()
        {
            foreach (var s in sessions)
                s.Dispose();
            sessions.Clear();
        }

        /// <summary>全表遍历计数：len() 段用真实遍历，不做任何 O(1) 短路。</summary>
        public long TraverseCount()
        {
            long count = 0;
            using ITsavoriteScanIterator iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress);
            while (iter.GetNext())
                count++;
            return count;
        }

        /// <summary>
        /// 范围读：从锚点地址向后步进 <paramref name="steps"/> 条记录，返回 value 首字节之和。
        /// 锚点由 Reader 回调记录（对应 Rust hash 引擎的 find_tag + hlog 顺序扫，非键序——两侧同口径，见 notes）。
        /// </summary>
        public ulong RangeScanValueSum(long anchorAddress, int steps)
        {
            if (anchorAddress < 0)
                return 0;
            long begin = Math.Max(anchorAddress, store.Log.BeginAddress);
            ulong sum = 0;
            int taken = 0;
            using ITsavoriteScanIterator iter = store.Log.Scan(begin, store.Log.TailAddress);
            while (taken < steps && iter.GetNext())
            {
                if (iter.Info.IsClosed)
                    continue;
                sum += iter.ValueSpan.Length > 0 ? iter.ValueSpan[0] : (byte)0;
                taken++;
            }
            return sum;
        }

        /// <summary>
        /// 压缩程序（bench.md §3 第 3 条）：Checkpoint(FoldOver) → ShiftBeginAddress(tail 对齐段界, truncateLog:true)
        /// → 目录树字节和。返回 false 表示未压缩（本引擎支持，恒为 true）。
        /// </summary>
        public bool Compact()
        {
            CloseSessions();
            store.Log.Flush(true);
            // FoldOver 全量检查点（索引+日志元数折叠到 cpr）
            var (success, _) = store.TakeFullCheckpointAsync(CheckpointType.FoldOver).AsTask().GetAwaiter().GetResult();
            if (!success)
                return false;
            // tail 向下对齐段界（Rust compact 用 64MiB 段界，本侧 1GB 段界；见 notes 口径差异）
            long tail = store.Log.TailAddress;
            long until = tail / GarnetSegmentSize * GarnetSegmentSize;
            if (until > store.Log.BeginAddress)
                store.Log.ShiftBeginAddress(until, snapToPageStart: false, truncateLog: true);
            store.Log.Truncate();
            return true;
        }

        /// <summary>
        /// 目录树字节和（含目录项自身，对应 WalkDir 对每个 entry 取 metadata().len()）。
        /// 与 Rust 同来源：POSIX st_size——Unix 上 FileInfo.Length 即 st_size；
        /// 目录 inode 的 st_size C# 目录 API 不暴露，用 /usr/bin/stat 同字段补齐。
        /// </summary>
        public static ulong DirectoryTreeSize(string path)
        {
            ulong size = 0;
            var stack = new Stack<string>();
            stack.Push(path);
            var dirs = new List<string>();
            while (stack.Count > 0)
            {
                string dir = stack.Pop();
                dirs.Add(dir);
                try
                {
                    foreach (string entry in Directory.EnumerateFileSystemEntries(dir))
                    {
                        if (Directory.Exists(entry))
                            stack.Push(entry);
                        else
                            try
                            {
                                size += (ulong)new FileInfo(entry).Length; // st_size，与 metadata().len() 同字段
                            }
                            catch
                            {
                                // 竞态删除：按 0 计
                            }
                    }
                }
                catch
                {
                    // 目录不可枚举：与 WalkDir err 一致按 0 计
                }
            }
            // 目录 inode 自身的 st_size（含根目录，WalkDir 对每个 entry 含目录都累加）
            if (dirs.Count > 0 && TryStatDirSizes(dirs, out ulong dirBytes))
                size += dirBytes;
            else if (dirs.Count > 0)
                size += (ulong)dirs.Count * 4096; // stat 不可用平台的近似（macOS/Linux 均不会走到）
            return size;
        }

        static bool TryStatDirSizes(List<string> dirs, out ulong total)
        {
            total = 0;
            try
            {
                // 单进程批量取 st_size：%z = byte size，%N = 路径（BSD stat 与本机同源字段）
                var psi = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "/usr/bin/stat",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                };
                psi.ArgumentList.Add("-f");
                psi.ArgumentList.Add("%z\t%N");
                foreach (string d in dirs)
                    psi.ArgumentList.Add(d);
                using var p = System.Diagnostics.Process.Start(psi);
                if (p is null)
                    return false;
                string stdout = p.StandardOutput.ReadToEnd();
                p.WaitForExit();
                if (p.ExitCode != 0)
                    return false;
                ulong sum = 0;
                foreach (string line in stdout.Split('\n'))
                {
                    int tab = line.IndexOf('\t');
                    if (tab <= 0)
                        continue;
                    if (ulong.TryParse(line.AsSpan(0, tab), out ulong v))
                        sum += v;
                }
                total = sum;
                return true;
            }
            catch
            {
                return false;
            }
        }

        public void Dispose()
        {
            CloseSessions();
            try { store?.Dispose(); } catch { /* 与 KV.benchmark 同法：清理失败吞掉 */ }
            try { device?.Dispose(); } catch { /* 同上 */ }
        }
    }
}
