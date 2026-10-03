# check.js 扫描漏排席位沙箱，副本树灌满「重复定义/实现缺失」噪声（主控亲办）

定级：P3（治理面：门禁判定被同源副本污染，甄别结果失真且输出量级膨胀三个数量级；无行为改动）

甄别结论：通过（2026-10-01 主树现码 + check.js 实跑双侧亲验，主控亲办收口）

## 问题分析

1. 席位沙箱自 /tmp 迁移入仓库内持久路径（`fork.sh` 注释自陈：/tmp 清理风暴两次连 worktree
   带未提交工作整窝端掉，故 worktree 落 `.forks/<名>`、私有 target 落 `.rs-targets/<名>`，
   二者均 .gitignore）。
2. `js/check/rustScan.js:rsWalk` 的跳过表只有 `target/.git/node_modules/garnet/scratch/.moon*`，
   不含 `.forks`/`.rs-targets`/`.bench_run`，于是每个席位副本（含 worktree-of-worktree 的
   嵌套链，实测最深 4 层 `.forks/g2a-resp/.forks/g2a-resp/.forks/g2b-mig2/...`）都被当作
   独立源参与映射锚点判定：
   - 同一 C# 函数在 11 份副本里各报一次「重复定义」；
   - 输出从 48 行涨到 64712 行，符号断言 B 层提示从 3 处涨到 45 处；
   - 门禁结果不可读，后续甄别会被噪声带偏。
3. 副本非重复定义的真身：`.forks/**` 与 `.rs-targets/**` 是同一 commit 的检出与编译缓存，
   按仓内口径本就不该参与源面判定（与 `target`、`node_modules` 同族）。

## 落地（单点，零行为改动）

`js/check/rustScan.js:rsWalk` 跳过表补三项，并写清理由（防后人当作冗余删除）：
`.forks`（席位 worktree 沙箱）、`.rs-targets`（席位私有 cargo target）、`.bench_run`（门禁日志落盘处）。

## 复验

1. `bun js/check.js`（主树）：EXIT=0，输出 64712 → 48 行；
   符号断言 B 层提示回到 3 处口径外存量；
   「重复定义」只剩真实一簇（`GarnetLatencyMetrics.cs:GetLatencyMetrics` 双 rust 锚，另案甄别）。
2. 沙箱实存核查：`.forks/`、`.rs-targets/` 均在 .gitignore:30-31，门禁日志目录 .bench_run/ 在:20，
   跳过表与实际布局一致，无漏排的新目录族。

## 遗留

- 「实现缺失」15 项目录级族与 1 簇真实重复定义的甄别另行开席，不在本登记票范围。
- 席位沙箱生命周期治理（孤儿 worktree 的 `.git/worktrees` 被外部压缩机制整目录摘除后，
  `.forks/*` 副本沦为不可回收的裸目录，既占盘又会被任何全仓扫描吃到）属工具面待议，
  本票只做扫描侧止血，不动沙箱本身。
