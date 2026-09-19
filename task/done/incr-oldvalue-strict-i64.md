# INCR/DECR/INCRBY/DECRBY 旧值解析改用 strict_i64（对位 C# IsValidNumber 拒前导零）

来源：next/glm.data.md 第 1 条。基线：主仓 dev。

## 问题（已核实成立）

wedb/wnode/src/resp/basic_commands/incr.rs 的旧值读取闭包（network_increment 约 :96，
`read_user_sync(store, key, |v| v.as_str_safe().parse::<i64>().ok())`）用 Rust 宽松 std parse，
接受 "01"/"00" 等前导零；而同一函数参数路径（:78/:88）已用 wbase::num::strict_i64（拒前导零）。
两侧口径对调。C# 对位：旧值数值校验走 IsValidNumber → NumUtils.TryReadInt64，
len>1 且首字节 '0' 即判非整数，落 InvalidTypeError，回 "ERR value is not an integer or out of range" 且不写。
复现：SET k 01 → INCR k，C# 报错保值，rust 返回 2 并把值改写为 "2"。

## 修法

network_increment 旧值闭包改为 strict_i64：`v.as_str_safe().and_then(|s| wbase::num::strict_i64(s.as_bytes()))`
（strict_i64 已在 incr.rs 顶部导入；保持既有 None→InvalidTypeError 分支不变）。
顺带核对 network_increment_by_float（约 :133）旧值闭包是否同样用了宽松 std parse——若是，改用 strict_f64
（strict_f64 亦已导入），与 C# 浮点旧值校验口径对齐；若已是 strict_f64 则不动。
不改 RMW 并发/原子性面（另票），只改数值校验口径这一处。

## 边界与验收

- 只动 wedb/wnode/src/resp/basic_commands/incr.rs。
- 子代理仅在 fork worktree 开发，仅跑 cargo check（私有 CARGO_TARGET_DIR），
  禁跑 test.sh / sh/clippy.sh / cargo fmt，禁碰主工作树，禁 git add -A。
- 补一条最小单测：SET "01" 后 INCR 应报错（旧值非法）保值；对照 SET "1" INCR 正常。
  （集成测试非强制，能在 incr.rs 单测层断言 strict_i64 拒前导零即可。）
- 报告附 git log dev..HEAD、rev-parse HEAD(40)、diff --name-only、cargo check REAL_EXIT。

## 细化方案（已核实）

甄别通过：C# NumUtils.TryReadInt64（NumUtils.cs:216-225）`len>1 && *beg=='0'` 拒前导零；
PrivateMethods.cs:672 IsValidNumber → TryReadInt64，失败置 InvalidTypeError。
rust wbase::num::strict_i64（num.rs:94）语义吻合且参数路径已在用。
浮点侧（incr.rs:155）已是 try_parse_double=strict_f64，按票不动。

1. incr.rs:96 `|v| v.as_str_safe().parse::<i64>().ok()` → 直接传 `strict_i64`
   （与 :155 传 try_parse_double 同风格，函数指针零包装）。
   as_str_safe 全文件仅此一处 → 同步收窄导入 `RespSliceExt` 只留 `RespVecExt`；
   :95 注释补 C# IsValidNumber 对位说明。
2. 测试：resp_tests.rs simple_increment_invalid_value 追加
   SET k "01" → INCR 报 not-integer 且 GET 保值 "01"；SET "1" → INCR :2 正常对照。
   （strict_i64 拒前导零单测 wbase/num.rs strict_i64_strict 已有，不重复。）
