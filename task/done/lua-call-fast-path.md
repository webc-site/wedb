lua.call SET/GET 快路径补 number 形参臂

C# 参考
garnet/libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting
SET 分支 key string 臂 :3176、number 臂 :3180-3186（失败 OutOfMemory）、其它 ErrBadArg；
value string 臂 :3194、number 臂 :3198-3204；GET 分支 key string 臂 :3228、number 臂 :3232-3238。
number 臂即 state.TryNumberToString(idx, out span)，就地把栈槽数值转为字符串后照常取用。

Rust 现状
wedb/wlua/src/functions/redis.rs:try_fast_path_set / try_fast_path_get 仅走
known_string_to_slice，number 形参落 ERR_BAD_ARG，与自身 fallback
（prepare_and_check_resp_request 已支持 number）及 C# 三处分叉。
转换件 try_number_to_string_at（wedb/wlua/src/state.rs，1:1 对标 C# TryNumberToString）
与同型先例 sha1_hex 的 number 臂俱在。

修法（单一机制，不抽新 helper）
两快路径各按 C# 判定顺序 string → number → 其它：
先逐槽 state.type_name 分派，number 槽就地带 state.try_number_to_string_at(slot)，
失败即回 ConstantStrings::OUT_OF_MEMORY（lua_wrapped_error_view），其它类型回 ERR_BAD_ARG；
全部就地转换结束后再统一 known_string_to_slice 取 key/value 切片供 session.set /
session.get 使用（避免 &mut state 与切片借用打架，切片存活期内无栈操作，
现两函数仅用毕后 clear_stack，次序满足）。

测试
新增集成测试（wedb/wlua/tests，记录型 ScriptingApi 假会话）：
redis.call("SET", 123, 456) 快路径落 set("123","456")，与字符串形参结果一致；
redis.call("GET", 123) 快路径落 get("123")；
bool/nil/table 形参仍报 ERR wrong type（fast-path 错误文案），pcall 形态断言。

验收
cargo check 零 error/warning；同参数快路径与 fallback 口径一致。
