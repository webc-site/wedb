redis.acl_check_cmd 撤第二套命令硬清单，有效性判定与权限检查收口会话侧单点

来源：next/glm.data.md 条 1（该文件已清空删除）。逐条按主仓 HEAD 复核后判定成立且待做。

结论
wlua 侧用一张 33 名的静态字符串清单判命令有效性，而全量命令表、子命令表、arity 字段与
解析单点在 rust 都已就位，会话侧也已有 ACL 门单点。这是同一件事的第二套实现，且两套口径
互相矛盾（会话侧未知名放行、wlua 侧未知名报错），后果是绝大多数命令在
redis.acl_check_cmd 下直接报无效命令、子命令形态完全不可用。修法即 SKILL 的一处定义：
撤硬清单，按 C# 三臂补子命令族与 arity 宽容，判定下沉会话侧既有单点。

现状（主仓 HEAD 实测行号）

一、硬清单与消费点：wedb/wlua/src/functions/redis.rs:25 KNOWN_ACL_COMMANDS（33 名），
:234 以 `!KNOWN_ACL_COMMANDS.contains(...)` 判有效性，未命中即 :235 报 ERR_INVALID_COMMAND。
清单 :55 的 "SORT" 在 C# 与 rust 两侧命令枚举里都不存在（garnet/libs/server/Resp/Parser/
RespCommand.cs 与 wedb/wresp/src/command.rs grep Sort 零命中），属虚设名；反之 HSET 等
大量在册命令不在清单内，redis.call 侧照跑、acl_check_cmd 侧报无效。

二、子命令族覆盖面：wedb/wlua/src/functions/redis.rs:249 只有 is_bit_op_parent 一臂，
:252-258 手抄五枚 BITOP_ 名为常量。C# 的 BITOP 展开有单点（wedb/wresp/src/catalog/mod.rs:279
expand_for_acls，活消费者 wedb/wacl/src/command_permission_set.rs:265、:285），本处是第三份。
父命令带子命令但未提供子命令时「逐子命令全查、全通过才 true」这一臂在 rust 整体缺失
（redis.rs:262 之后无该分支），"ACL|SETUSER" 一类子命令形态不可用。
arity 裁剪/补齐（C# 的宽容计数）同样缺失：redis.rs:238 取的 provided_resp_arg_count 只喂给
:249 的 BITOP 判据，未参与任何成帧计数。

三、口径分叉：会话侧单点 wedb/wnode/src/resp/resp_server_session.rs:2556 acl_allows_command
（:2557 经 wedb/wnode/src/resp/resp_commands_info_data.rs:18 全枚举映射，落到 :2541 acl_permits
位图门），对解不出的名字在 :2559 返回 true（宽松）；wlua 侧对解不出的名字硬报错。两侧同为一件事。
acl_allows_command 的消费面实测只有 wlua 端口一处（resp_server_session.rs:2763 的
ScriptingApi impl，端口声明 wedb/wlua/src/api.rs:25、函数表 wedb/wlua/src/runner/host.rs:121）
与一例测试 wedb/wnode/tests/resp_server_session_tests.rs:704，主循环 ACL 门走的是
wedb/wnode/src/resp/admin_commands.rs:91 check_acl_permissions(RespCommand)，收口不牵动主循环。

四、缺失语义的现成件（redis.rs:24 与 :231-232 注释自陈「resp 域就绪后切 RespCommandsInfo」，
该前提已达成）：全量表按名取元数据 wedb/wresp/src/catalog/commands_info.rs:700
try_get_resp_command_info_by_name(cmd_name, external_only, include_sub_commands)，arity 字段
:215、sub_commands 字段 :233；成帧后解析单点 wedb/wnode/src/resp/parser/resp_command.rs:409
parse_resp_command_buffer；RESP 成帧单点 wedb/wlua/src/functions/redis.rs:279
prepare_and_check_resp_request（其 number 臂 :300-318 已在用，勿另起一套）。

C# 参考

garnet/libs/server/Lua/LuaRunner.Functions.cs:2803 AclCheckCommand
:2827 RespCommandsInfo.TryGetRespCommandInfo(cmdStr, externalOnly: false, includeSubCommands: true)
:2834-2836 isBitOpParent / hasSubCommands / providesSubCommand 三判据
:2851 RespCommand.BITOP.ExpandForACLs() 逐子命令成帧检查
:2887 hasSubCommands && !providesSubCommand 全查臂（:2913 foreach info.SubCommands）
:3004 局部 PrepareAndCheckRespRequest：arity 裁剪补齐在 :3015-3019，成帧解析在 :3064

修法

1. 有效性判定换成 try_get_resp_command_info_by_name(name, false, true)，删 KNOWN_ACL_COMMANDS
   整块与随之无用的导入（redis.rs:6-8 的 LazyLock / gxhash::HashSet）。
2. 按 C#:2834-2836 建三判据，分支顺序对齐 C#：BITOP 父无参臂改走 expand_for_acls 单点取子命令
   （删 redis.rs:252-258 手抄常量）；新增 hasSubCommands && !providesSubCommand 的逐子命令全查臂
   （子命令名取自 info.sub_commands，不再新增名字常量表）；其余走直检臂。
3. 新增 arity 宽容的成帧解析单点：按 info.arity 取 min/max 裁补参数后成帧，转调
   parse_resp_command_buffer 解出 RespCommand，再以 acl_permits 判权——即把 wlua 侧的
   「有效性 + 权限」两问一次交回会话侧，acl_allows_command(:2556) 收为按 C# INVALID 语义
   报无效（:2559 的恒真分支随之撤除），全仓不留第二套清单。
4. wedb/wnode/tests/resp_server_session_tests.rs:704 一例断言按新口径校正，并补 C# 三臂的
   对照用例（HSET 等清单外命令可用、BITOP 无参全查、ACL 父命令不带子命令时逐子命令全查、
   arity 不足的宽容计数），用例按 SKILL 归集成测试面。
5. 不保留兼容路径：删清单后不留「清单为空则退回旧行为」之类的开关。

优先级
重复/多套架构（wlua 硬清单是命令有效性判定的第二套实现，且与会话侧口径相反、BITOP 子命令族
为第三份手抄；redis.rs:24 的「待切换」注记已过期）。功能缺口居次。

验收
1. grep KNOWN_ACL_COMMANDS 全仓零命中；wlua 侧不再出现命令名字面量数组。
2. redis.acl_check_cmd("HSET", ...) 与 "ACL|SETUSER" 形态行为与 C# 一致。
3. 命令有效性与权限判定各只有一处定义（catalog 表单点 + acl_permits 位图门）。
4. clippy 无新增告警（禁写 allow）。

细化方案（实现代理按主仓 HEAD 复核后追加，行号已重取）

复核结论：票述全部成立。补充事实：
- parse_resp_command_buffer（wnode/src/resp/parser/resp_command.rs:409，&mut self）
  现无生产消费者（仅 tests/resp_command_parse.rs:374 一例）；本次接入即其生产消费面。
- ConstantStrings::AND/OR/XOR/NOT/DIFF（wlua/src/strings.rs:153-162，对标 C# constStrs
  同名注册 LuaRunner.Strings.cs:247-251）现无消费者，BITOP 臂成为其消费面，不必新造常量。
- wlua 已依赖 wresp（Cargo.toml wresp.workspace），catalog 与 RespCommand 枚举可直接消费。
- acl_allows_command 消费面确实仅 ScriptingApi impl（resp_server_session.rs:2767）+ 测试
  :703 一例；resp_command_from_cs_name 另有 resp_command_docs.rs:385、basic_commands/mod.rs
  :298/:312 消费，不随本票死。
- command_table.rs:437 BITOP_SUBTABLE 五名与 expand_for_acls 的 EXPANDED_BITOP 五枚一一
  对应，"BITOP AND" 成帧可解出 BitopAnd。
- ScriptingApi 另有 wlua/tests/redis_call_fast_path.rs:18 MockSession 实现体，随 trait 改签名。

改动面（1:1 对标 C# AclCheckCommand :2803-3064）：

1. wlua/src/api.rs：trait 撤 `check_acl_permissions(&str)`，按 C# RespCommand 重载改
   `check_acl_permissions(&self, RespCommand) -> bool`；新增
   `parse_resp_command_buffer(&mut self, &[u8]) -> Option<RespCommand>`（C# 同名方法，
   LuaRunner 直调会话的形状经 trait 承接）。host.rs vtable 两条同步。
2. wlua/src/functions/redis.rs:219 acl_check_command 重写：
   - 有效性判定换 try_get_resp_command_info_by_name(name, false, true)，None →
     ERR_INVALID_COMMAND；删 KNOWN_ACL_COMMANDS 整块与 LazyLock/gxhash::HashSet 导入。
   - 三判据 is_bit_op_parent / has_sub_commands / provides_sub_command 对齐 C# :2834-2836。
   - 臂一 BITOP 无参：expand_for_acls(RespCommand::Bitop) 取子命令枚举，短名经
     ConstantStrings::AND 等映射（C# :2851-2862 switch 同构），成帧 "BITOP <sub>" 后
     parse+判权；删 :252-258 手抄五枚全名常量。
   - 臂二 has_sub_commands && !provides_sub_command：逐 info.sub_commands，子名取
     name '|' 后段（C# :2915），arity 用子命令 info，全过才 true。
   - 臂三直检：父/子 info arity 宽容成帧后 parse+判权。
   - 成帧单点：改造既有 prepare_and_check_resp_request（redis.rs:279）签名承接
     (provided, actual) 两计数，actual 按 C# :3016-3018 clamp(provided, |arity|-1,
     arity<0 ? MAX : arity-1)，缺位补空参（C# WriteArgument(default)）；fallback 调用点
     传 (provided, provided) 行为不变。badArg → ERR_BAD_ARG，parse None →
     ERR_INVALID_COMMAND。
   - 无会话臂（runner 模式）保留恒 true。
   - 快路径 SET/GET（:347/:423）改传 RespCommand::Set/Get。
3. wnode/src/resp/resp_server_session.rs：ScriptingApi impl 两方法落
   self.0.parse_resp_command_buffer / self.0.acl_permits；删 acl_allows_command(:2560)
   及其恒真分支；resp_commands_info_data.rs:8 头注「脚本 ACL 路径」表述同步清理。
4. wnode/tests/resp_server_session_tests.rs:703 改按新 API 断言；wnode/tests/
   lua_script_tests.rs（ACL 会话 harness 已备 :274）补三臂对照用例：清单外命令
   （HSET）放行、BITOP 无参全查、ACL 父不带子命令逐子全查、arity 不足宽容补位、
   未知命令报 ERR Invalid command。
5. wlua/tests/redis_call_fast_path.rs MockSession 随 trait 改签名（parse 返回 None 即可，
   快路径用例不触该面）。

实现态（f24-lua-acl-catalog @ 988448f9，2026-09-19）

已完成并自验：九文件改动全部落地（见细化方案 1-5），分支独立 cargo check
--workspace --tests 全绿；两次 merge dev 复核本面（wlua/wnode 含 tests）亦全绿，
并顺带验证了与 sunsubscribe 目录新增条目的共存。

未合入 dev：dev 基线损坏（wext_json/src/json_object.rs 25 红 +
wext_json/tests/json_commands_test.rs 25 红 + wext_roaring/src/
roaring_bitmap_commands.rs 42 红，全为 E0061 实参计数，与本票面零交集；
de9f7d3a 自记「dev 侧 wext_json/wext_roaring arity 红待对账」、3b4fc4c4 自记
「待 dev 基线修复合入」）。按规程第 10 步回退分支上的 dev merge、保留分支。

接力：dev 基线修复后，在 /tmp/fork/f24-lua-acl-catalog（若已被清理则
fork.sh f24-lua-acl-catalog 重建，分支在）merge dev → cargo check --workspace
--tests → 主仓 checkpoint + merge --no-ff → 票移 done → 清 worktree/分支。
