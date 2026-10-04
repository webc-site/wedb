# wacl-acl-setuser-enum-only-name-custom-fallback（P2，源自 task/issue/wacl-acl-setuser-enum-only-name-custom-fallback-registry.md）

## 甄别结论：通过（2026-09-28 主控现码复验）
C# 侧亲验：ACLParser.cs:274-281 TryParseCommandForAcl 走 Enum.TryParse 全枚举成员
（RespCommand.cs:40 DELIFEXPIM=9、:94 RIPROMOTE=63、:95 RIRESTORE=64 枚举在场），
User.cs:187-190 AddCommand 查目录 RespCommandsInfo 失配即抛
"Unable to obtain ACL information, this shouldn't be possible"（:318 RemoveCommand
同形），被 NetworkAclSetUser 捕获回 -ERR——C# 现形为失败关闭。JSON 目录亲验零条目
（grep garnet/libs/resources/RespCommandsInfo.json 计 0）。rust 侧亲验：
acl_parser.rs:259 lookup_command 目录单源，None 时 :188 落
is_valid_custom_command_name 回落臂（纯字母恒过）。deviations.md 现册
Enum.TryParse 怪癖族仅 §105/§106 两形，本「枚举在场、目录缺席」第三形零登记。

## 危害
默认档：-ERR 文案与 C# 发散（Unknown custom command vs Unable to obtain ACL
information）；--no-default-features 档：is_custom_command_registered 返 None
（acl_commands.rs:796 形），幻影名静默 +OK 入 custom_allowed 并持久化、随复制/AOF
传播——权限面伪授权。

## 方案
以 C# 为准：目录失配且名称命中 RespCommand 枚举成员名集（编译期单源，如
strum/const 名表，禁运行时新字典）时不走自定义回落臂，判失败并回 C# 同文案
-ERR（thiserror 面按仓内既有 ACL 错误枚举扩展）；真自定义名（枚举亦无）回落形
不变。deviations.md 不登记（本票为对齐修复非偏差）。

## 验证
wacl 单测/wnode 集成锁测：DELIFEXPIM/RIPROMOTE/RIRESTORE 三名 SETUSER +名/-名
两档均 -ERR 且用户权限零变化、零持久残留；正对照未知纯字母名仍走自定义轨；
子代理只跑 cargo check 与相关专测，门禁归主控。

## 收口记录（2026-09-28 主控）
- 席：`wacl-enum-fallback`（起点 51c9c9ae，tip 50c5f494）→ dev 合并 **70c2d351**（`--no-ff`）。
- 收口形态：判定集改 `RespCommand::from_cs_name`（strum 编译期单源，即 C#
  `Enum.TryParse(ignoreCase)` 对位，非新建字典/新锁）；目录反查臂
  `catalog::try_get_by_cs_name` 按零死代码纪律撤除（唯一消费者即本臂，全仓 grep 归零），
  失败关闭顺延 `User::apply_command` 查目录失配回 C# 逐字同文案；
  DELIFEXPIM=9 原已在员，RIPROMOTE=63 / RIRESTORE=64 按 C# 同值回填 1:1 占位成员
  （全仓零分发臂，亲验：`cs: "RIPROMOTE"` 目录条目 0，RIPROMOTE 全仓提及均为 RMW 语义注释）。
- 主控独立复核：`cargo check -q --workspace --all-targets` EXIT 0（席暖 target 复用）；
  `cargo nextest run -p wacl -p wresp` **151/151 PASS**；
  `-p wnode --test acl_setuser_enum_only_fail_close` 1/1 PASS；
  `bun js/check.js` 合并后 实现缺失 0、重复定义簇无新增；
  `bun js/check/symbolCheck.js` 违规 1→0（唯一违规系同日 egress 票折行锚，主控另笔 3fde7d1c 修）。
- 票面差集核验补充：goldens 普查 118→120/364→366，洞位断言翻回填断言并在册记由；
  MODULE 族（254..=256）与 §106 数值回退拒形不动，deviations 无需新登记
  （63/64 回填在 `resp_command_registered_diffs_are_as_documented` 测试面登记并锁形）。
- 未尽项（另票候选，本票不扩面）：no-default 档未知名静默入自定义轨系 C# `ccm==null` 既有对位面。
