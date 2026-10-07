// Copyright (c) wedb authors.
// 机器环境探测：字段名、取值来源与 Rust 侧 bench/crates/wedb-bench/src/machine.rs
// 逐项对齐（sysinfo 0.39.6 在 macOS 上读 sysctl 的哪些 MIB，这里就调同一批 sysctl，
// 保证同机两侧输出逐字段相等；仅 data_dir 因两侧各用独立数据目录而不同）。

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Wedb.Bench.Tsavorite
{
    /// <summary>
    /// serde_json 的 f64 呈现复刻：最短可回读数字，整数值也带小数点（64 → "64.0"）。
    /// ryu 与 .NET Core 3.0+ 的 double.ToString 同为 shortest-roundtrippable，数字相同；
    /// 这里只补 serde 对整数值的 ".0" 后缀，避免与 int 形态混淆。
    /// </summary>
    internal sealed class SerdeDoubleConverter : JsonConverter<double>
    {
        public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => reader.GetDouble();

        public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
        {
            string text = value.ToString(CultureInfo.InvariantCulture);
            if (!text.Contains('.') && !text.Contains('e') && !text.Contains('E')
                && !double.IsNaN(value) && !double.IsInfinity(value))
                text += ".0";
            writer.WriteRawValue(text, skipInputValidation: true);
        }
    }

    /// <summary>machine.rs::MachineInfo 的逐字段复刻（serde 名用小写下划线）。</summary>
    internal sealed class MachineInfo
    {
        [JsonPropertyName("cpu_brand")] public string CpuBrand { get; set; } = "";
        [JsonPropertyName("physical_cores")] public int PhysicalCores { get; set; }
        [JsonPropertyName("logical_cores")] public int LogicalCores { get; set; }
        [JsonPropertyName("arch")] public string Arch { get; set; } = "";
        [JsonPropertyName("platform")] public string Platform { get; set; } = "";
        [JsonPropertyName("total_memory_gib")]
        [JsonConverter(typeof(SerdeDoubleConverter))]
        public double TotalMemoryGib { get; set; }
        [JsonPropertyName("disk_type")] public string DiskType { get; set; } = "";
        [JsonPropertyName("data_dir")] public string DataDir { get; set; } = "";
        [JsonPropertyName("data_fs")] public string DataFs { get; set; } = "";
        [JsonPropertyName("os_info")] public string OsInfo { get; set; } = "";
        [JsonPropertyName("kernel_version")] public string KernelVersion { get; set; } = "";

        const int StatfsBufferSize = 4096;
        const int DarwinFstypenameOffset = 72; // 本机 sys/mount.h offsetof(statfs, f_fstypename)（C 探针实测）
        const int DarwinStatfsStructSize = 2168;

        /// <summary>machine.rs::detect(bench_dir, data_dir)：bench_dir 仅用于盘介质探测。</summary>
        public static MachineInfo Detect(string benchDir, string dataDir)
        {
            var m = new MachineInfo
            {
                Arch = RustArch(),
                Platform = PlatformSlug(),
            };

            if (OperatingSystem.IsMacOS())
            {
                m.CpuBrand = FirstNonEmpty(Sysctl("machdep.cpu.brand_string").Trim(), "Unknown CPU");
                int logical = ParseIntOr(Sysctl("hw.logicalcpu"), Environment.ProcessorCount);
                m.LogicalCores = logical;
                m.PhysicalCores = ParseIntOr(Sysctl("hw.physicalcpu"), logical); // sysinfo：hw.physicalcpu，失败回落 cpus().len()
                ulong memsize = ParseULongOr(Sysctl("hw.memsize"), 0);
                if (memsize == 0)
                    memsize = (ulong)Math.Max(0, GC.GetGCMemoryInfo().TotalAvailableMemoryBytes);
                m.TotalMemoryGib = memsize / (1024.0 * 1024.0 * 1024.0); // sysinfo 0.39 total_memory 单位为字节
                m.DiskType = DetectDiskTypeMac(benchDir);
                string osName = FirstNonEmpty(Sysctl("kern.ostype").Trim(), "Darwin"); // sysinfo name(): KERN_OSTYPE
                string osVer = Sysctl("kern.osproductversion").Trim();                  // sysinfo os_version(): kern.osproductversion
                m.OsInfo = string.IsNullOrEmpty(osVer) ? osName : osName + " " + osVer;
                m.KernelVersion = FirstNonEmpty(Sysctl("kern.osrelease").Trim(), "Unknown"); // sysinfo kernel_version(): KERN_OSRELEASE
            }
            else if (OperatingSystem.IsLinux())
            {
                m.CpuBrand = LinuxCpuBrand();
                int logical = Environment.ProcessorCount; // sysinfo：cpu 数即 /sys/devices/system/cpu 在线数
                m.LogicalCores = logical;
                m.PhysicalCores = LinuxPhysicalCores(logical);
                m.TotalMemoryGib = LinuxTotalMemoryKiB() / 1024.0 / 1024.0; // /proc/meminfo MemTotal(kB) → 字节 → GiB
                m.DiskType = DetectDiskTypeLinux();
                string osVer = ReadFirstLine("/proc/sys/kernel/osrelease");
                m.OsInfo = string.IsNullOrEmpty(osVer) ? "Linux" : "Linux " + osVer;
                m.KernelVersion = FirstNonEmpty(osVer, "Unknown");
            }
            else
            {
                m.CpuBrand = "Unknown CPU";
                m.LogicalCores = Environment.ProcessorCount;
                m.PhysicalCores = m.LogicalCores;
                m.TotalMemoryGib = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes / (1024.0 * 1024.0 * 1024.0);
                m.DiskType = "NVMe SSD";
                m.OsInfo = RuntimeInformation.OSDescription;
                m.KernelVersion = "Unknown";
            }

            string root = PlainRoot(Realpath(dataDir));
            m.DataDir = root;
            m.DataFs = DetectFsType(root);
            return m;
        }

        // ===== 来源与 machine.rs::platform_slug 同式 =====

        /// <summary>env::consts::ARCH 复刻：Arm64→aarch64、X64→x86_64。</summary>
        static string RustArch() => RuntimeInformation.ProcessArchitecture switch
        {
            Architecture.X64 => "x86_64",
            Architecture.Arm64 => "aarch64",
            Architecture.X86 => "x86",
            Architecture.Arm => "arm",
            _ => RuntimeInformation.ProcessArchitecture.ToString().ToLowerInvariant(),
        };

        static string PlatformSlug()
        {
            string arch = RustArch() switch { "x86_64" => "x64", "aarch64" => "arm64", var other => other };
            string os = OperatingSystem.IsWindows() ? "windows"
                : OperatingSystem.IsMacOS() ? "macos"
                : OperatingSystem.IsLinux() ? "linux"
                : RuntimeInformation.OSDescription.ToLowerInvariant();
            return os + "-" + arch;
        }

        // ===== disk_type：逐分支照抄 machine.rs::detect_disk_type =====

        static string DetectDiskTypeMac(string benchDir)
        {
            string out1 = RunProgram("/usr/sbin/system_profiler", ["SPNVMeDataType"]);
            if (out1.Contains("NVMExpress") || out1.Contains("NVMe") || out1.Contains("Apple SSD"))
                return "NVMe SSD";
            string out2 = RunProgram("/usr/sbin/diskutil", ["info", "/"]);
            if (out2.Contains("Solid State:               Yes")) // 与 Rust 同：按 diskutil 定宽两栏的精确串匹配
                return "NVMe SSD";
            // Rust 此处走 sysinfo Disks::kind()（读 IORegistry 介质），C# 无等价物，
            // 用 diskutil 的 Solid State/Rotational 近似 SSD/HDD 判定，见汇报口径说明。
            if (out2.Contains("Solid State:               Yes")) return "SSD";
            if (out2.Contains("Rotational:              Yes") || out2.Contains("Solid State:               No"))
                return out2.Contains("Rotational:              Yes") ? "HDD" : "SSD";
            return "NVMe SSD";
        }

        static string DetectDiskTypeLinux()
        {
            try
            {
                foreach (string dev in Directory.EnumerateDirectories("/sys/block"))
                {
                    string name = Path.GetFileName(dev);
                    if (name.StartsWith("nvme", StringComparison.Ordinal))
                        return "NVMe SSD";
                    try
                    {
                        if (File.ReadAllText(Path.Combine(dev, "queue/rotational")).Trim() == "0")
                            return "SSD";
                    }
                    catch { /* 单盘读失败继续 */ }
                }
            }
            catch { /* 无 /sys/block */ }
            return "NVMe SSD";
        }

        // ===== data_fs：machine.rs::detect_fs_type 同来源 =====

        /// <summary>macOS statfs(2) 的 f_fstypename；Linux /proc/mounts 最长前缀 + statfs 魔数。</summary>
        public static string DetectFsType(string dir)
        {
            if (OperatingSystem.IsMacOS())
                return FsTypeViaStatfsDarwin(dir) ?? "unknown";

            if (OperatingSystem.IsLinux())
            {
                string? viaMounts = FsTypeViaMounts(dir);
                if (viaMounts is not null)
                    return viaMounts;
                string? viaStatfs = FsTypeViaStatfsLinux(dir);
                if (viaStatfs is not null)
                    return viaStatfs;
            }
            return "unknown";
        }

        [DllImport("libc", EntryPoint = "statfs", SetLastError = true)]
        static extern int StatfsDarwin(string path, IntPtr buf);

        static string? FsTypeViaStatfsDarwin(string dir)
        {
            IntPtr buf = Marshal.AllocHGlobal(StatfsBufferSize);
            try
            {
                unsafe
                {
                    for (int b = 0; b < StatfsBufferSize; b++) ((byte*)buf)[b] = 0;
                    int rc = StatfsDarwin(dir, buf);
                    if (rc != 0)
                        return null;
                    // sizeof(struct statfs)==2168；f_fstypename 为 16 字节 NUL 结尾名
                    string name = Marshal.PtrToStringAnsi(buf + DarwinFstypenameOffset, DarwinStatfsStructSize - DarwinFstypenameOffset) ?? "";
                    int nul = name.IndexOf('\0');
                    if (nul >= 0) name = name[..nul];
                    return name.Length > 0 ? name : null;
                }
            }
            catch { return null; }
            finally { Marshal.FreeHGlobal(buf); }
        }

        [DllImport("libc", EntryPoint = "statfs", SetLastError = true)]
        static extern int StatfsLinux(string path, IntPtr buf);

        static string? FsTypeViaStatfsLinux(string dir)
        {
            IntPtr buf = Marshal.AllocHGlobal(StatfsBufferSize);
            try
            {
                if (StatfsLinux(dir, buf) != 0)
                    return null;
                long ftype = Marshal.ReadInt64(buf);
                return ftype switch
                {
                    0x0102_1994 => "tmpfs",
                    unchecked((long)0x8584_58f6) => "ramfs",
                    0xef53 => "ext4",
                    0x5846_5342 => "xfs",
                    unchecked((long)0x9123_683e) => "btrfs",
                    _ => null,
                };
            }
            catch { return null; }
            finally { Marshal.FreeHGlobal(buf); }
        }

        static string? FsTypeViaMounts(string dir)
        {
            string[] lines;
            try { lines = File.ReadAllLines("/proc/mounts"); }
            catch { return null; }
            string? bestFs = null;
            int bestLen = -1;
            foreach (string line in lines)
            {
                string[] parts = line.Split(' ');
                if (parts.Length < 3)
                    continue;
                string mount = parts[1].Replace("\\040", " ");
                string fs = parts[2];
                if (dir.StartsWith(mount, StringComparison.Ordinal) && mount.Length > bestLen)
                {
                    // 与 Rust 同：要求是完整路径段前缀，避免 /tmp 误配 /tmpfoo
                    if (mount == "/" || dir.Length == mount.Length || dir[mount.Length] == '/')
                    {
                        bestLen = mount.Length;
                        bestFs = fs;
                    }
                }
            }
            return bestFs;
        }

        // ===== 工具 =====

        /// <summary>data_dir 用 canonicalize 等价（macOS 上即 realpath，解 /var→/private/var 符号链接）。</summary>
        static string Realpath(string path)
        {
            string full;
            try { full = Path.GetFullPath(path); }
            catch { full = path; }
            string rp = OperatingSystem.IsWindows() ? "" : RunProgram("/bin/realpath", [full]);
            string trimmed = rp.Trim();
            if (trimmed.Length > 0 && trimmed.Contains('/') && Directory.Exists(trimmed))
                return trimmed;
            return full.TrimEnd(Path.DirectorySeparatorChar);
        }

        /// <summary>machine.rs::plain_root——剥 Windows verbatim 前缀；Unix 直传。</summary>
        static string PlainRoot(string path)
        {
            if (path.StartsWith(@"\\?\UNC\")) return @"\\" + path[8..];
            if (path.StartsWith(@"\\?\")) return path[4..];
            return path;
        }

        static string Sysctl(string name) => RunProgram("/usr/sbin/sysctl", ["-n", name]);

        internal static string RunProgram(string file, string[] args)
        {
            try
            {
                var psi = new ProcessStartInfo
                {
                    FileName = file,
                    RedirectStandardOutput = true,
                    RedirectStandardError = false,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                };
                foreach (var a in args)
                    psi.ArgumentList.Add(a);
                using var p = Process.Start(psi);
                if (p is null)
                    return "";
                string stdout = p.StandardOutput.ReadToEnd();
                p.WaitForExit();
                return stdout;
            }
            catch
            {
                return "";
            }
        }

        static string FirstNonEmpty(string value, string fallback) => string.IsNullOrWhiteSpace(value) ? fallback : value;

        static int ParseIntOr(string text, int fallback)
            => int.TryParse(text.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out int v) ? v : fallback;

        static ulong ParseULongOr(string text, ulong fallback)
            => ulong.TryParse(text.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong v) ? v : fallback;

        static string LinuxCpuBrand()
        {
            try
            {
                foreach (string line in File.ReadLines("/proc/cpuinfo"))
                {
                    if (line.StartsWith("model name"))
                    {
                        int eq = line.IndexOf(':');
                        if (eq >= 0)
                            return line[(eq + 1)..].Trim();
                    }
                }
            }
            catch { }
            return "Unknown CPU";
        }

        static int LinuxPhysicalCores(int fallback)
        {
            var seen = new HashSet<(string, string)>();
            try
            {
                string? phys = null, core = null;
                foreach (string line in File.ReadLines("/proc/cpuinfo"))
                {
                    if (line.StartsWith("physical id")) phys = line;
                    else if (line.StartsWith("core id")) core = line;
                    else if (line.Length == 0 && phys is not null && core is not null)
                    {
                        seen.Add((phys, core));
                        phys = core = null;
                    }
                }
                if (phys is not null && core is not null)
                    seen.Add((phys, core));
            }
            catch { }
            return seen.Count > 0 ? seen.Count : fallback;
        }

        static double LinuxTotalMemoryKiB()
        {
            try
            {
                foreach (string line in File.ReadLines("/proc/meminfo"))
                {
                    if (line.StartsWith("MemTotal:"))
                        return ParseULongOr(line.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)[1], 0);
                }
            }
            catch { }
            return 0;
        }

        static string ReadFirstLine(string path)
        {
            try
            {
                foreach (string line in File.ReadLines(path))
                    return line.Trim();
            }
            catch { }
            return "";
        }
    }
}
