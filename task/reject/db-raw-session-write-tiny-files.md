裁决：整体拒绝 next/db-raw-session-write-tiny-files.md（wkv session/raw 写路径小文件合并）。
核销 2026-09-20。取证基线：主仓 dev 当下代码（3dccbd4 后）+ garnet C# 逐文件对照。
不建 worktree、不改代码。

票面主张：append.rs（40 行）、rmw.rs（105 行）、modify.rs（135 行）、
copy_to_tail.rs（现 193 行）多为单函数文件，应并入 write/mod.rs 或 raw/mod.rs。

拒绝理由一：票面自证与结论相悖，现状已与 C# 文件组织逐文件一一对应
票面引 C# Implementation/ 目录「按机制不按函数」平铺，而 rust 写路径恰是如此：
- garnet InternalUpsert.cs（396 行，异步驱逐重试环）↔ session/raw/write/append.rs
  的 upsert_raw/delete_raw 重试环（append.rs 头部锚点注释即指向 InternalUpsert.cs）
- garnet InternalRMW.cs（778 行）↔ session/raw/write/rmw.rs（try_rmw_sync 家族 +
  upsert_rmw 一条 RMW 机制）与 session/raw/modify.rs（try_modify_raw_in_place_unprotected/
  rmw_raw/rmw，闭包式读改写一族变体，头注「对标 C# Tsavorite InternalRMW &
  InPlaceUpdaterWorker」）
- garnet TryCopyToTail.cs ↔ copy_to_tail.rs（含 cas_mount_copied_frame 单点，
  读侧两条冷读晋升臂 raw/read.rs 也转调此处，见该文件头注）
- garnet InPlaceUpdater 快路径/BlockAllocate.cs retryNewLogicalAddress ↔ inplace.rs
- C# 中 upsert、rmw、copy-to-tail、in-place 本就是各不相同的机制文件；把
  append/rmw/copy_to_tail/modify 并成 700+ 行 mod.rs 恰是把四套机制混进一文件，
  反而背离票面自己引用的 C# 组织，属「添加额外复杂度」，与拆分类验收口径
  （对标 C# 文件组织）直接冲突。

拒绝理由二：「单函数一文件」指控不实
append.rs 含 2 个 pub fn；rmw.rs 含 4 个（try_rmw_sync 三变体 + upsert_rmw）；
modify.rs 含 3 个。均为同族机制多函数一文件，与 C# 一类一文件等粒度。

拒绝理由三：小文件是本仓既有组织惯例，非病灶
全仓 src 下 .rs 共 702 个，行数 <60 的 152 个（约 21.6%），wkv 内亦有 vdb/mod.rs
（10 行）、gc/vdb.rs（59 行）、session/keys.rs（65 行）、read_cache/cleanse.rs
（66 行）等大量同量级机制文件。按票面逻辑全仓需大并文件，显然荒谬。

拒绝理由四：与读侧收敛方向冲突
wkv 读路径刚完成单点收敛（dev cd44779：find_in_read_cache/trace_back_for_key_match/
drive_mem_read/read_probe），收敛后的机制锚点正是靠 read.rs 与写侧各机制文件的
一一对齐维系（copy_to_tail.rs 的 cas_mount_copied_frame 即读侧冷读晋升臂共用
单点）。把写侧机制文件并入 mod.rs 是拆散机制粒度的逆操作，同域在途票
next/db-read-cache-probe-unify 亦无需本票协调——本票不动任何文件，零冲突。

附注：票面行数已漂移（copy_to_tail.rs 票称 157 行、现 193 行），系对活代码的
快照式指控，进一步说明此类「行数审美」票不宜作为改造依据。
