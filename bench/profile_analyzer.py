#!/usr/bin/env python3
"""
Profile Analyzer for wedb-bench (supports samply and Gecko/Firefox profile format).
Parses .json.gz profile and optional .syms.json sidecar to produce:
1. Top Hotspot Functions (Self Time & Inclusive Time)
2. Demangled symbol names with source file and line numbers
3. Per-thread sample breakdown
"""

import bisect
import gzip
import json
import os
import sys
from collections import Counter, defaultdict


def analyze_profile(profile_path, top_n=20, thread_filter=None):
    if not os.path.exists(profile_path):
        print(f"错误：Profile 文件不存在: {profile_path}", file=sys.stderr)
        sys.exit(1)

    # 寻找 sidecar 符号文件
    syms_path = profile_path + ".syms.json"
    if not os.path.exists(syms_path):
        syms_path = profile_path.replace(".json.gz", ".json.syms.json")
    if not os.path.exists(syms_path):
        syms_path = None

    try:
        profile = json.load(gzip.open(profile_path))
    except Exception as e:
        print(f"读取 profile 压缩文件失败: {e}", file=sys.stderr)
        sys.exit(1)

    syms_data = None
    if syms_path and os.path.exists(syms_path):
        try:
            syms_data = json.load(open(syms_path))
        except Exception:
            pass

    # 构建符号与源码位置映射 (lib_name -> (rvas, sym_tab, str_table))
    lib_sym_tables = {}
    if syms_data and "data" in syms_data and "string_table" in syms_data:
        st = syms_data["string_table"]
        for lib in syms_data["data"]:
            lib_name = lib.get("debug_name", "")
            sym_tab = lib.get("symbol_table", [])
            rvas = [s.get("rva", 0) for s in sym_tab]
            lib_sym_tables[lib_name] = (rvas, sym_tab, st)

    def resolve_sym(lib_name, addr):
        if lib_name not in lib_sym_tables:
            return None, None
        rvas, sym_tab, st = lib_sym_tables[lib_name]
        idx = bisect.bisect_right(rvas, addr) - 1
        if idx >= 0:
            s = sym_tab[idx]
            name = st[s["symbol"]]
            frames = s.get("frames", [])
            loc = ""
            if frames:
                f0 = frames[0]
                file_idx = f0.get("file", 0)
                file_name = st[file_idx] if file_idx < len(st) else ""
                line_no = f0.get("line", 0)
                if file_name:
                    loc = f"{file_name}:{line_no}"
            return name, loc
        return None, None

    self_counts = Counter()
    total_counts = Counter()
    sym_locations = {}
    thread_stats = Counter()

    libs = profile.get("libs", [])

    for t in profile.get("threads", []):
        strings = t.get("stringArray", [])
        funcs = t.get("funcTable", {})
        frames = t.get("frameTable", {})
        stacks = t.get("stackTable", {})
        res = t.get("resourceTable", {})
        samples = t.get("samples", {})
        
        sample_count = samples.get("length", 0)
        if not sample_count:
            continue

        raw_tname = t.get("name", "unnamed")
        tname = strings[raw_tname] if isinstance(raw_tname, int) and raw_tname < len(strings) else str(raw_tname)
        thread_stats[tname] += sample_count

        if thread_filter and thread_filter.lower() not in tname.lower():
            continue

        stack_indices = samples.get("stack", [])
        for s_idx in stack_indices:
            if s_idx is None:
                continue
            curr = s_idx
            stack_funcs = []
            while curr is not None and curr < len(stacks.get("frame", [])):
                f_idx = stacks["frame"][curr]
                if f_idx >= len(frames.get("func", [])):
                    break
                func_idx = frames["func"][f_idx]
                fname = strings[funcs["name"][func_idx]] if func_idx < len(funcs.get("name", [])) else "unknown"
                r_idx = funcs["resource"][func_idx] if func_idx < len(funcs.get("resource", [])) else None
                lib_idx = res["lib"][r_idx] if r_idx is not None and r_idx < len(res.get("lib", [])) else None
                lib_name = libs[lib_idx]["name"] if lib_idx is not None and lib_idx < len(libs) else ""

                addr = frames["address"][f_idx] if f_idx < len(frames.get("address", [])) else None
                if addr is not None:
                    resolved_name, loc = resolve_sym(lib_name, addr)
                    if resolved_name:
                        fname = resolved_name
                        if loc:
                            sym_locations[fname] = loc

                stack_funcs.append(fname)
                prefix_list = stacks.get("prefix", [])
                curr = prefix_list[curr] if curr < len(prefix_list) else None

            if stack_funcs:
                # 栈顶为自用时间（Self Time）
                self_counts[stack_funcs[0]] += 1
                # 整调用栈包含时间（Inclusive Time）
                for fn in set(stack_funcs):
                    total_counts[fn] += 1

    total_samples = sum(self_counts.values())
    if total_samples == 0:
        print("未采集到有效 CPU 采样数据。")
        return

    print("═════════════════════════════════════════════════════════════════════════════════")
    print(f"📊 性能分析概要 (共 {total_samples} 个 CPU 采样，覆盖 {len(thread_stats)} 个活跃线程)")
    print("═════════════════════════════════════════════════════════════════════════════════\n")

    print("── 线程采样分布（前 5 个线程）──")
    for tname, cnt in thread_stats.most_common(5):
        pct = cnt / total_samples * 100
        print(f"  • {tname:<25} : {cnt:6d} 采样 ({pct:5.1f}%)")
    print()

    print(f"── 热点函数排行 (Top {top_n}，按 Self Time 降序) ──\n")
    print("| Self % | Total % | Samples | Function | Source Location |")
    print("|--------|---------|---------|----------|-----------------|")
    
    for fn, sc in self_counts.most_common(top_n):
        tc = total_counts[fn]
        sp = sc / total_samples * 100
        tp = tc / total_samples * 100
        loc = sym_locations.get(fn, "")
        if len(loc) > 42:
            loc = "..." + loc[-39:]
        fn_disp = fn if len(fn) <= 65 else fn[:62] + "..."
        print(f"| {sp:5.1f}% | {tp:6.1f}% | {sc:7d} | {fn_disp:<65} | {loc:<42} |")
    print()


def main():
    if len(sys.argv) < 2:
        print("用法: profile_analyzer.py <profile.json.gz> [top_n]")
        sys.exit(1)
    
    path = sys.argv[1]
    top_n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    analyze_profile(path, top_n)


if __name__ == "__main__":
    main()
