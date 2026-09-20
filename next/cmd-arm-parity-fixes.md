# 命令臂四处行为对齐:SETEX/PSETEX/SET-EX 宽度、DECRBY 边界、PFMERGE 原子性

来源:next/zcode-r8-sample-a.md 条目 1-4(已随本票认领从 next 移除)。
四条均为对照 C# 精读发现的极端入参/错误路径差异,逐条对齐 C# 为裁判。

## 1. SETEX/PSETEX 过期参数解析宽度(rust i64 vs C# int32)

wedb/wnode/src/resp/basic_commands/set.rs parse_setex_args(约 :325)用
strict_i64(全 i64 值域);C# NetworkSETEX(:533)TryGetInt 超 int32 直接失败。
SETEX k 2147483648 v:C# 回 "ERR value is not an integer" 不写键;
rust 落 TTL 回 +OK。应答与副作用双差。PSETEX 同体同受累。
修法:解析改 strict_i32 口径(或 strict_i64 后加 int32 值域门),
错误文案对齐 C# not-integer。

## 2. SET/SETEXNX 的 EX/PX 数值同根差异

parse_set_options(约 :738)strict_i64;C# :605/:653 int32。
SET k v EX 2147483648:C# not-integer;rust +OK 落 TTL。
修法同上:EX/PX 值域门 int32。注意不要波及 EXPIRE 族(C# 该族本就是 long,
r8-sample-a 条 26 已核一致)。

## 3. DECRBY i64::MIN 取负:C# unchecked 回绕 vs rust saturating

parse_incr_args(incr.rs 约 :65)sign().saturating_mul 把 -i64::MIN 饱和为
+i64::MAX;C# RMWMethods.cs:559 `-decrBy` 在 unchecked 上下文回绕仍得
i64::MIN。DECRBY k -9223372036854775808、旧值 0:C# 写回 MIN 回 :1;
rust 写回 +MAX。终值符号翻转级发散。
修法:对齐 C# wrapping_neg(wrapping 语义,C# release 无 checked),
终值与 C# 逐位一致;补边界回归(DECRBY k -9223372036854775808 旧值 0)。

## 4. PFMERGE 错误路径原子性:C# 部分提交 vs rust 零写

hyper_log_log_commands.rs(约 :503 慢路径 :338)先装载全部源、任一源
WRONGTYPE/非法载荷即零写返回;C# HyperLogLogMerge(:96/:191)逐源
GET→SET_Conditional 即时合并,后续源错误时 finally 仍 Commit——此前源已
持久合并(dest 缺失被建键),客户端收错但 dest 已部分变更。
修法:对齐 C#(逐源即时合并+错误照发),或登记「rust 零写为有意增强」
(ignore/doc)。倾向对齐 C#(SKILL 1:1);二选一写明证据。

## 纪律

1. 每条先读 rust 现状与 C# 原码核实票面差异仍在(可能已被并发票改动);
   不成立写 reject 给证据。
2. 四条相互独立,可分 commit;回归测试各一条(极端入参直证)。
3. 禁止顺手扩面到 EXPIRE 族等其他解析面;不需要向下兼容。

## 验收

1. cargo check -p wnode 零 error 零 warning;四条定向回归绿。
2. 逐条写明:对齐 C# / 有意登记 的裁决与证据。

## 门禁

只跑 cargo check(-p 收窄)与定向测试。严禁 ./test.sh 与 ./sh/clippy.sh。
