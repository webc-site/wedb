# r9 波空手甄别档：wacl 认证/授权/命名空间绑定缝（2026-10-01）

本档为**已甄别、不立案**项的登记，非待执行任务。来源：只读甄别席（`wedb/wacl/**` 及其在 `wedb/wnode/src/resp/` 的
认证/授权消费面 + 命名空间绑定/切库面），主控按「席线索≠案」口径抽查承重锚后判空手收口。
基线 1e42558。立案数 0，淘汰备案 5，全阴性谱见下。

## 淘汰项（判据 + 主控复核结论）

1. `wacl/src/acl_parser.rs::try_parse_command_for_acl` 子命令「枚举在场 / 目录缺席」时 rust 回 None 走自定义回落、
   C# `ACLParser.cs::TryParseCommandForAcl` 抛 `ACLException`：**不可达**——触发需「名含 `|`、折叠成下划线后恰为某
   `RespCommand` 成员、且该成员缺席 `RespCommandsInfo`」；在册目录缺席名（`DELIFEXPIM`/`RIPROMOTE`/`RIRESTORE`）
   名中无下划线，`X|Y → X_Y` 无法命中。且属 §105/§106/§109「`Enum.TryParse` 怪癖族」在册裁决面（`done/wacl-acl-setuser-enum-only-name-custom-fallback{,-registry}`）。
2. `acl_parser.rs::get_name_by_acl_category` 多比特（非 ALL）分类返回 `"unknown"`、C# 反查缺项抛 KeyNotFound：
   仅描述串生成面，调用方 `User::apply_category` 恒以单类别位或 ALL 传入，多比特形在正常规则流不可达，软降级不越权。
3. `wacl/src/auth/garnet_acl_authenticator.rs::acl_password_check` 对停用账号先返回、跳过口令哈希比对：
   rust 与 C# `GarnetAclWithPasswordAuthenticator.cs::AuthenticateInternal`（`user.IsEnabled && ValidatePassword(...)`）
   **判定顺序同构**，非 rust 偏离；§98 在册。
4. `wacl/src/command_permission_set.rs::can_run_custom_command` 用 `eq_ignore_ascii_case`、C# 用 `FrozenSet(OrdinalIgnoreCase)`：
   自定义名经 `acl_parser.rs::is_valid_custom_command_name` 限 ASCII 字母数字 + `. _ - |`，两折叠对纯 ASCII 语义一致；
   臂序（All→denied→allowed→泛型位回落）逐臂同构。
5. GENPASS / `describe_user` / 各 `with_capacity` 分配面：`bits` 先经 `<=0 || >4096` 上界校验再折算容量（≤1024），
   其余以已存在数据长度为上界，无「客户端可控参数 × 常数」放大形（对照本波 BLMPOP 票的判据口径）。

## 已核清确认对齐（阴性证据，后席勿重复勘查）

- 恒定时间比较：`wacl/src/secrets_utility.rs::constant_equals`（4×u64 异或累加无早退）+ `acl_password.rs::PartialEq` +
  `user.rs::validate_password`（`fold` 非短路全遍历）↔ C# `SecretsUtility.cs::ConstantEquals`/`User.cs::ValidatePassword`。
- 命令位图：`command_permission_set.rs::bit_on/set_bit/apply_cmd_bit/can_run_command/is_equivalent_to`
  （`>>6`/`&63`、越界 `word<len` 失败关闭、All/None 哨兵身份与 `copy()` 物化）逐位对齐 C# 同名件。
- 认证主链判定顺序：`acl_commands.rs::authenticate_user_via_store`（解析→跨租门禁→读前采样代数→点查→
  **在场即唯一真源一律 Denied 绝不回落**，仅 `NoRecord`+ns0 回落引导单例）与 `resp_server_session/auth.rs::authenticate_user`
  的 ns0 兜底门 ↔ C# `GarnetACLAuthenticator.cs:58-79`；§98/§90/§159 在册。
- 预门三态：`admin_commands.rs::check_acl_permissions`（park→custom→`acl_permits`）与 C#
  `AdminCommands.cs::CheckACLPermissions`(:124-146) 臂序同构；Parked 不执行仅回退游标（`core.rs` break 臂）；
  `write_acl_permission_error(is_some)` 的 NOPERM/NOAUTH 择定对齐。
- 命名空间绑定/切库：`user.rs::parse_user_namespace_with_default`（`split_once('#')`，空/二 `#`/非数字前缀拒）+
  `acl_commands.rs::foreign_namespace_denied`（裸名恒落 caller_ns）+ ns0 超管豁免——符合 `doc/zh/db.md` §2.2，
  `0#user`/`12#user`/裸名三形均无法落 ns0，逃逸面闭合。
- 规则解析臂序：`acl_parser.rs::apply_acl_op_to_user`（on/off/nopass/reset/resetpass→`><#!`→`+@/-@`→`+/-`→
  `~*/allkeys/resetkeys`→else 未知）与 C# `ACLParser.cs::ApplyACLOpToUser` 一致。
- 错误折叠谱：全缝仅 `user.rs::describe_user` 一处 `let _ = write!`（写 String，Infallible）；
  存储读失败一律失败关闭（`StorageError` / `RESP_ERR_ACL_STORE_SCAN_FAILED` 关框），未折成「无权限」。

## 后备锁测（若日后重开本缝，先补这三条阴性锁再立案）

1. ns=5 会话 `AUTH 0#default <pw>` 与 `AUTH default <requirepass>` 恒 WRONGPASS（租户逃逸闭合）。
2. `SETUSER default >新` 落盘后旧 requirepass `AUTH default` 恒 Denied（§98 无回落）。
3. `SETUSER +FOO|BAR`（扩展清单缺席的自定义名）失败关闭。
落点：`wedb/wacl/tests/`（既有 `acl_setuser_enum_only_fail_close.rs`、`access_control_list_tests.rs`、
`command_permission_set_tests.rs` 为天然锚册）与 wnode 侧 ACL/多租会话 tests 册。
