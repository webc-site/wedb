# SINTERCARD numkeys/LIMIT i32 过滤补下界（对位 C# TryReadInt32Safe 负溢出文案）

来源：next/glm.data.md 第 2 条。基线：主仓 dev。排队：须在
task/ing/resp3-command-layer-frame-parity.md 合并落地后再开工（同文件
wedb/wnode/src/resp/objects/set_commands.rs，避与其 SMEMBERS/SPOP/write_set_members 改动对撞）。

## 问题（已核实成立）

set_intersect_length（:612）内两处 i32 值域过滤只滤上界、缺下界：
- :624 numkeys：`.filter(|&n| n <= i64::from(i32::MAX))`
- :649 limit_val：`.filter(|&v| v <= i64::from(i32::MAX))`
超出 i32::MIN 的负值（如 -3000000000）通过过滤后落入后续语义检查：numkeys 报
"ERR numkeys should be greater than 0"、limit<0 报 "ERR LIMIT can't be negative"（:655）。
C# TryGetInt → RespReadUtils.TryReadInt32Safe 对 number>int.MaxValue（含负越界）判 overflow 返回 false，
两者均回 "ERR value is not an integer or out of range"。
同文件 :1183（另一 SINTERCARD 解析臂）已用 `(1..=i64::from(i32::MAX)).contains`、:1196 用
`v <= i32::MAX` 且注释自称「与同步段同口径」，实际同步段 :624/:649 反缺界，两侧对调，属疏漏。

## 修法

- :624 numkeys 改 `(1..=i64::from(i32::MAX)).contains(&n)`（numkeys 必正且 ≤ i32::MAX，越界/负值即拒 → 回 out-of-range）；
- :649 limit_val 改 `(i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&v)`，
  使越界负值在过滤阶段即被拒回 out-of-range，保留 :654 对「合法 i32 内的负 LIMIT」的 "can't be negative" 语义。
两处越界/非整数应统一走 C# 的 "value is not an integer or out of range" 早退（对齐同文件既有 out-of-range 臂的写法与文案常量）。

## 边界与验收

- 只动 set_commands.rs 的 set_intersect_length 这两处过滤；不改聚合语义、不碰 SMEMBERS/SPOP 帧（那是 resp3 票）。
- RESP2/RESP3 均只改参数校验文案路径，不改回复帧。
- 子代理仅在 fork worktree 开发，仅 cargo check（私有 CARGO_TARGET_DIR），禁 test.sh/clippy/fmt，禁碰主树，禁 git add -A。
- 报告附 git log dev..HEAD、rev-parse HEAD(40)、diff --name-only、REAL_EXIT。

## 细化方案（已对照 C# 甄别，2026-09-19）

甄别核实：
- RespReadUtils.cs:258 TryReadInt32Safe：负数绝对值 > int.MaxValue+1 判 overflow（i32::MIN 合法），
  正数 > int.MaxValue 判 overflow，overflow → false；ParseUtils.cs:47 TryReadInt 转调之。
- SetCommands.cs:159 SetIntersectLength：TryGetInt 失败（含负越界）→ RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER；
  域内 nKeys<1 → GenericErrShouldBeGreaterThanZero；域内 LIMIT<0 → GenericErrCantBeNegative。

票据修法修正一处：numkeys 不可用 `(1..=i32::MAX)`，否则 i32 域内 0/-5 被拦进 out-of-range，
而 C# 对域内非正数报 "numkeys should be greater than 0"（且 :629 分支变死码）。
正确口径：两处统一 i32 全域过滤（用 i32::try_from(n).is_ok()，等价 [i32::MIN, i32::MAX] 且更直白），
域内语义检查（<1 / <0）留给既有分支。定稿：
- :624 → .filter(|&n| i32::try_from(n).is_ok())，注释改「超 i32 值域（含负溢出）视为非整数」
- :649 → .filter(|&v| i32::try_from(v).is_ok())
- 不动 :1183/:1196 慢路径臂（票据边界只许两处；其 None→ASYNC_REQUIRED 防御语义不受影响）。

排队决策：resp3-command-layer-frame-parity 尚在 next/ 未认领未落地；其改动锚（SMEMBERS/SPOP/
write_set_members）与本票锚（set_intersect_length 两行 filter）不同函数零 hunk 重叠，merge 可自动
合并，且合并前 re-merge dev + cargo check 兜底，故不再等待，直接开工。

测试补充（tests/resp_set.rs sintercard_limit_align 追断言，锁口径防回归）：
- numkeys/LIMIT = -3000000000 → "-ERR value is not an integer or out of range.\r\n"
- numkeys = 0 与 -5 → RESP_ERR_GENERIC_NUMKEYS（域内非正不被 out-of-range 吞）
验收：worktree cargo check（含 --tests 编译）通过。
