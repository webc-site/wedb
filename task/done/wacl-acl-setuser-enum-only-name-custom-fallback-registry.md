ACL 命令名对照为 C# Enum.TryParse 全枚举成员形、rust 目录单源形：DELIFEXPIM/RIPROMOTE/RIRESTORE 类「枚举在场、目录缺席」内名在 rust 坠自定义命令名通道（默认档应答文案发散、无扩展 feature 档静默收受残留，Enum.TryParse 怪癖族第三形零登记）

问题分析：
1. C# 契约对齐
C# ACL 加减命令臂（garnet/libs/server/ACL/ACLParser.cs:223-255）经 TryParseCommandForAcl（:274-318）以 Enum.TryParse(effectiveName, ignoreCase: true)（:281）对照全枚举成员名——含目录（RespCommandsInfo.json）缺席的内部枚举成员 DELIFEXPIM（garnet/libs/server/Resp/Parser/RespCommand.cs:40）、RIPROMOTE/RIRESTORE（:94-95，本三者在两仓 JSON 目录均零条目，grep 实测 0 命中）。该三形 IsValidParse（:324-327）不拒（非 NONE/INVALID、无数字）、IsInvalidCommandToAcl（:330-331）不拒（NormalizeForACLs 恒等），故 TryParseCommandForAcl 判为真命令返回；随后 User.AddCommand/RemoveCommand（garnet/libs/server/ACL/User.cs:187-190、:316-319）以 TryGetRespCommandInfo 查 FlattenedRespCommandInfo（该表仅由 JSON 构建，garnet/libs/server/Resp/RespCommandsInfo.cs:328-344）失配，抛 ACLException("Unable to obtain ACL information, this shouldn't be possible")，被 NetworkAclSetUser 捕获（garnet/libs/server/Resp/ACLCommands.cs:228）回 -ERR 错误帧。即 C# 现形：此类名字既不走命令位图、也不入自定义名轨，一律失败关闭。
2. 工程现状确证
rust 命令名解析单源走 ACL 目录（JSON 内嵌投影）：wedb/wacl/src/acl_parser.rs:213-254 try_parse_command_for_acl → :258-261 lookup_command → wedb/wresp/src/catalog/mod.rs:322 try_get_by_cs_name（entries 仅含 JSON 条目，mod.rs:22/233-252）。DELIFEXPIM/RIPROMOTE/RIRESTORE 目录零条目 → 查表 None → 落 :188-191 自定义命令名回落臂（is_valid_custom_command_name 纯字母名恒过）→ apply_custom_command 入 custom_allowed/custom_denied 集并写入描述串。SETUSER 线面另有新增名校验臂（wedb/wnode/src/resp/acl_commands.rs:372-384）：默认 wnode features（wedb/wnode/Cargo.toml:19 default=["roaring","json"]）下 is_custom_command_registered=Some（acl_commands.rs:794），该名不在扩展命令清单 → 回 -ERR "Unknown custom command 'DELIFEXPIM' (not registered with any loaded module)"，失败先于 store.write 零残留（§109 事务形态在案）；--no-default-features 构建下该臂为 None（:796）→ 判 +OK 且幻影条目随记录持久化、进 ACL LIST/GETUSER 描述并随复制/AOF 记录传播。查重：doc/zh/deviations.md 全册——Enum.TryParse 怪癖族仅在册两形（§105 空白修剪第二形、§106 a) 数字回退第一形；§134 系 byte.Parse HexNumber 族第三形），本「全枚举成员对照形」零登记；DELIFEXPIM 仅 §347 前后 AOF 域提及，与 ACL 名对照面无涉。
3. 逻辑危害确证
具体命令序列：AUTH 后执行 ACL SETUSER u off -delifexpim（+ripromote/-rirestore 同形）。C# 回 -ERR Unable to obtain ACL information, this shouldn't be possible；rust 默认构建回 -ERR Unknown custom command 'DELIFEXPIM' (not registered with any loaded module)——双侧皆拒但文案发散，对拍夹具必红无据可查；rust 无扩展 feature 构建回 +OK，此后 ACL LIST u 显示 user u off -delifexpim 幻影条目并经复制链传播，C# 对照态永不产生该形记录。无权限放大（该三命令线不可达、幻影名不参与任何门判定，见 wedb/wnode/src/resp/admin_commands.rs:44-46 按名轨仅对 Customobjcmd 生效）；危害为应答面对账发散、features-off 形态收受/拒绝分叉与残留、治理面三件：其一，acl_parser.rs:256 注「目录名对照（Enum.TryParse(ignoreCase)…）」易被后续席误读为全量镜像怪癖而按「对齐原型」给 lookup_command 补全枚举名对照——那会把 C# 域内名（含 rust 已删除的 RIPROMOTE/RIRESTORE，rust 枚举无此成员，wedb/wresp/src/command.rs:63-64 编号空缺注在案）引成本仓第二套名法；其二，同类怪癖已两度立项在册（§105/§106），族谱缺第三形则怪癖面全貌不可考。定级 P3 登记级（对齐 §106 先例：不改码不改行为，只补台账）。

涉及代码：
rust 文件与函数：
wedb/wacl/src/acl_parser.rs:try_parse_command_for_acl / lookup_command / apply_acl_op_to_user 加减命令臂
wedb/wresp/src/catalog/mod.rs:try_get_by_cs_name / project_entries
wedb/wnode/src/resp/acl_commands.rs:apply_set_user 新增自定义名校验臂
wedb/wnode/Cargo.toml default features
wedb/wresp/src/command.rs RespCommand 枚举（Delifexpim = 9；63/64 空缺注）

对应 c# 文件与函数：
garnet/libs/server/ACL/ACLParser.cs:ApplyACLOpToUser / TryParseCommandForAcl / IsValidParse / IsInvalidCommandToAcl
garnet/libs/server/ACL/User.cs:AddCommand / RemoveCommand
garnet/libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandInfo
garnet/libs/server/Resp/Parser/RespCommand.cs:DELIFEXPIM / RIPROMOTE / RIRESTORE
garnet/libs/server/Resp/ACLCommands.cs:NetworkAclSetUser catch 臂

精炼执行方案：
登记为主，零行为改动。一机制：维持 rust 目录单源名法（拒全枚举镜像，rust 现形更贴 Redis 上游「非命令名即拒」），doc/zh/deviations.md 新增一节登记 Enum.TryParse 怪癖族第三形（全枚举成员对照、目录缺席内名坠自定义名轨形；编号按落笔当日册尾实况顺编、撞号让位不写死），条内回指 §105/§106 a) 两形并归 §134 族题谱系，钉死两形态实测文案（默认 features 回 Unknown custom command 文案且零残留；双侧命令名对照集差异即本条），并随票在 acl_parser.rs:256 lookup_command 文档注释补「不镜像全枚举成员对照、怪癖族第三形见本条」一句回指锚。测试验证点：新增锁测（挂 wedb/wacl/tests 既有怪癖族锁面旁）：a) parse_acl_rule("user u off -delifexpim") 纯解析面锁定该坠自定义名轨事实——custom_denied 含 DELIFEXPIM、描述串回 -delifexpim、有效权限与 "-@all" 用户 is_equivalent_to 恒真（证幻影零权限效应）；b) 默认 features 下 ACL SETUSER 活链 "off -delifexpim" 回 -ERR Unknown custom command 'DELIFEXPIM' 精确文案且存储零记录（GETUSER 回 nil），锁定与 C# 的文案分叉现形；c) 正对照 -get/-config/get 等目录在名授权链不受影响。严禁按 C# 全枚举形态给名对照补枚举回退臂。

## 销号注记（2026-09-28 主控）
立案已由 task/done/wacl-acl-setuser-enum-only-name-custom-fallback.md 收口
（合并 70c2d351：判定集改 RespCommand::from_cs_name strum 单源，63/64 按 C# 同值回填占位成员，
三名解析命中后由 User::apply_command 查目录失败关闭回 C# 同文案；151/151 绿）。
