// Copyright (c) wedb authors.
// 确定性随机数与负载参数：与 Rust 侧 bench 逐字节同源的 WyRand 复刻。

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Wedb.Bench.Tsavorite
{
    /// <summary>
    /// fastrand 2.5.0 的 WyRand 逐字节复刻（Rust 侧 harness 用同一 PRNG 生成键值）。
    /// 只有字节序列完全一致，C# 读段重放的键才与 Rust 装载的键同源，checksum 断言才有意义。
    /// </summary>
    internal sealed class WyRand
    {
        // fastrand::Rng 常量（src/lib.rs）：WY_CONST_0 / WY_CONST_1
        const ulong WyConst0 = 0x2d35_8dcc_aa6c_78a5UL;
        const ulong WyConst1 = 0x8bb8_4b93_962e_acc9UL;

        ulong state;

        /// <summary>fastrand `Rng::with_seed(seed)` 直接以 seed 为状态（恒等映射，无混淆）。</summary>
        public WyRand(ulong seed) => state = seed;

        /// <summary>复制当前状态：分片生成时每个分片都从同一母种子快进。</summary>
        public WyRand Clone() => new(state);

        /// <summary>gen_u64：s += C0; t = (u128)s * (s ^ C1); return (u64)t ^ (u64)(t &gt;&gt; 64)。</summary>
        public ulong NextU64()
        {
            state = unchecked(state + WyConst0);
            UInt128 t = (UInt128)state * (UInt128)(state ^ WyConst1);
            return unchecked((ulong)t ^ (ulong)(t >> 64));
        }

        /// <summary>
        /// fastrand `fill`：每 8 字节写一个 gen_u64 的 native-endian（本平台小端）字节序，
        /// 尾部不足 8 字节时再取一个完整块的前 len 个字节。
        /// </summary>
        public void Fill(Span<byte> dst)
        {
            int len = dst.Length;
            int i = 0;
            while (len - i >= 8)
            {
                WriteLe64(NextU64(), dst[i..]);
                i += 8;
            }
            int remainder = len - i;
            if (remainder > 0)
            {
                // Rust: let n = self.next_u64().to_ne_bytes(); dst[..remainder].copy_from_slice(&n[..remainder])
                Span<byte> block = stackalloc byte[8];
                WriteLe64(NextU64(), block);
                block.Slice(0, remainder).CopyTo(dst[i..]);
            }
        }

        static void WriteLe64(ulong v, Span<byte> dst)
        {
            // BinaryPrimitives.WriteUInt64LittleEndian 等价，这里保持与 to_ne_bytes 在 arm64/x64 上一致
            for (int j = 0; j < 8; j++)
                dst[j] = (byte)(v >> (8 * j));
        }

        /// <summary>一对随机键值：先 fill(key)，再 fill(value)——与 harness::random_pair 同序。</summary>
        public (byte[] Key, byte[] Value) RandomPair(int keySize, int valueSize)
        {
            byte[] key = new byte[keySize];
            byte[] value = new byte[valueSize];
            Fill(key);
            Fill(value);
            return (key, value);
        }

        /// <summary>快进 n 个 pair（与 Rust make_rng_shards 的循环消耗完全一致）。</summary>
        public void AdvancePairs(long n, int keySize, int valueSize)
        {
            for (long i = 0; i < n; i++)
                RandomPair(keySize, valueSize);
        }

        /// <summary>
        /// 多线程分片 rng：复刻 harness::make_rng_shards——每个分片重建 seed=3 的 rng，
        /// 再快进 i * (elements / shards) 个 pair（整数除法）。
        /// </summary>
        public static List<WyRand> MakeRngShards(ulong seed, int shards, long elements, int keySize, int valueSize)
        {
            var rngs = new List<WyRand>(shards);
            long elementsPerShard = elements / shards; // 与 Rust 一样用整数除法
            for (int i = 0; i < shards; i++)
            {
                WyRand rng = new(seed);
                rng.AdvancePairs((long)i * elementsPerShard, keySize, valueSize);
                rngs.Add(rng);
            }
            return rngs;
        }

        /// <summary>
        /// 自检：把 seed 下前 count 个 pair 的键与值首字节打印成 hex，供与 Rust 侧同种子输出比对。
        /// </summary>
        public static string SelfTest(ulong seed, int keySize, int valueSize, int count)
        {
            WyRand rng = new(seed);
            var sb = new System.Text.StringBuilder();
            for (int i = 0; i < count; i++)
            {
                var (key, value) = rng.RandomPair(keySize, valueSize);
                string keyHex = Convert.ToHexString(key).ToLowerInvariant();
                string valueHex = Convert.ToHexString(value).ToLowerInvariant();
                sb.Append("pair ").Append(i)
                  .Append(" key=").Append(keyHex)
                  .Append(" value=").Append(valueHex)
                  .AppendLine();
            }
            return sb.ToString();
        }
    }

    /// <summary>
    /// 负载参数：默认值与 Rust 侧 config.rs::Workload::default() 逐项一致。
    /// </summary>
    internal sealed class Workload
    {
        public int ReadIterations = 3;
        public long BulkElements = 5_000_000;
        public long SortedElements = 1_000_000;
        public long IndividualWrites = 1_000;
        public long NosyncWrites = 50_000;
        public long BatchWrites = 100;
        public long BatchSize = 1_000;
        public int ScanIterations = 3;
        public long NumReads = 1_000_000;
        public long NumScans = 500_000;
        public int ScanLen = 10;
        public long PopRemovals = 500_000;
        public long PopSampleRemovals = 5_000;
        public ulong SlowPopSampleLimitMs = 1_000;
        public int KeySize = 24;
        public int ValueSize = 150;
        public ulong RngSeed = 3;
        public ulong CacheSize = 4UL * 1024 * 1024 * 1024; // REDB_CACHE_SIZE = 4 GiB
        public int[] ThreadCounts = [4, 8, 16, 32];

        /// <summary>config.rs::scaled(scale)：bulk/sorted/reads/scans/pop 按比例缩放，写段小量级不动。</summary>
        public static Workload Scaled(double scale)
        {
            if (!double.IsFinite(scale) || scale <= 0.0)
                scale = 1.0;
            var w = new Workload
            {
                BulkElements = Math.Max(1, (long)(5_000_000.0 * scale)),
                SortedElements = Math.Max(1, (long)(1_000_000.0 * scale)),
                NumReads = Math.Max(1, (long)(1_000_000.0 * scale)),
                NumScans = Math.Max(1, (long)(500_000.0 * scale)),
                PopRemovals = Math.Max(2, (long)(500_000.0 * scale)),
            };
            w.PopSampleRemovals = Math.Min(w.PopRemovals, 5_000);
            return w;
        }

        /// <summary>默认档：与 Rust Workload::default() 一致。</summary>
        public static Workload Default() => new();

        /// <summary>config.rs::loaded_elements() = bulk + individual + batch_size*batch_writes + nosync。</summary>
        public long LoadedElements => BulkElements + IndividualWrites + BatchSize * BatchWrites + NosyncWrites;

        /// <summary>config.rs::cap_cache_to_memory()：缓存收敛到物理内存一半以内，返回是否发生收敛。</summary>
        public bool CapCacheToMemory(double totalMemoryGib)
        {
            if (totalMemoryGib <= 0.0)
                return false;
            ulong limit = (ulong)(totalMemoryGib * 0.5 * 1024.0 * 1024.0 * 1024.0);
            if (CacheSize > limit)
            {
                CacheSize = limit;
                return true;
            }
            return false;
        }

        /// <summary>config.rs::workload_bytes()：按近似字节量折算默认超时。</summary>
        public ulong WorkloadBytes()
        {
            ulong pair = (ulong)(KeySize + ValueSize);
            ulong writeUnits = (ulong)LoadedElements + (ulong)SortedElements;
            ulong readUnits = (ulong)(NumReads + NumScans * ScanLen);
            foreach (int t in ThreadCounts)
                readUnits += (ulong)(LoadedElements / t * t);
            ulong mutateUnits = ((ulong)LoadedElements / 2) * 4;
            return (writeUnits + readUnits + mutateUnits) * pair;
        }

        /// <summary>config.rs::derived_timeout_secs()：3600s 基线按字节折算，下限 900s。</summary>
        public ulong DerivedTimeoutSecs()
        {
            const double BaselineBytes = 3_872_000_000.0;
            double scaled = 3600.0 * ((double)WorkloadBytes() / BaselineBytes);
            return (ulong)Math.Max(900.0, scaled);
        }
    }
}
