优先级：低

问题：键级 TTL（EXPIRE/PEXPIRE）允许 NX/XX 与 GT/LT 双选项组合（仅放行 XXGT/XXLT），字段级 TTL（HEXPIRE/ZEXPIRE）仅支持单选项，不支持复合。两边 rust 与 C# 行为一致，但该约束只散落在各命令实现里，wresp 选项定义处没有集中说明，后续重构容易把字段级 TTL 误放开复合选项（那将与 C# 分叉）。

取证（rust）：
- 键级：wedb/wnode/src/resp/key_admin_commands/keys.rs:168 `network_expire`，count>3 时 `merged == ExpireOption::XXGT || merged == ExpireOption::XXLT` 才放行双选项，其余组合报 not compatible
- 字段级：wedb/wnode/src/resp/objects/hash_commands.rs `hash_expire` 与 wedb/wnode/src/resp/objects/sorted_set_commands/write.rs `zadd_expire` 均只解析单个选项词元
- 选项定义：wedb/wresp/src/options.rs:77 `ExpireOption` bitflags（含 XXGT/XXLT 复合常量），定义处无「键级双选 / 字段级单选」的口径说明

C# 对标（同口径）：
- garnet/libs/server/Resp/KeyAdminCommands.cs:364 NetworkEXPIRE（TryGetExpireOption 两参合并，兼容对限定）
- garnet/libs/server/Resp/Objects/HashCommands.cs HashExpire、garnet/libs/server/Resp/Objects/SortedSetCommands.cs SetExpiration（单选项解析）

修法建议：在 wedb/wresp/src/options.rs 的 ExpireOption 文档注释补一段「键级 TTL（EXPIRE/PEXPIRE 族）允许 XX+GT / XX+LT 双选项组合（仅 XXGT/XXLT 放行）；字段级 TTL（HEXPIRE/ZEXPIRE 族）仅单选项，C# 同口径（KeyAdminCommands vs HashCommands/SortedSetCommands），重构禁将字段级放开复合」。仅注释，不改解析行为。

落地（2026-09-19，docs-data-comment-batch 棒）
裁决：成立。wresp/src/options.rs 的 ExpireOption 结构体文档注释补「键级与字段级 TTL 的选项组合口径」
整段：键级 EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT 可给两选项但只放行 XX+GT、XX+LT（即 XXGT/XXLT
复合常量），其余组合回 not compatible；字段级 HEXPIRE 族与 ZEXPIRE 族只解析单个选项词元、不支持
复合；并写明「禁把键级双选口子放到字段级（将与 C# 分叉），也禁反过来收掉键级复合」与判据现状
（network_expire、parse_hash_expire_args、sorted_set_expire）。XXGT、XXLT 两个常量各补一行适用面。
try_get_expire_option 的单词元解析、network_expire 的 merged 兼容判定、字段级两处 curr_idx 推进
全部零改动。
票面取证订正：字段级 ZEXPIRE 的 rust 实现名是 sorted_set_expire（objects/sorted_set_commands/
write.rs:590），票面写的 zadd_expire 查无此名；主张本身经核成立（C# SortedSetCommands.cs:1772 与
HashCommands.cs:603 都只试读一个选项词元，rust 同形）。
门禁：cargo check 零错误零警告；锚点集合与改前逐条相同（`Garnet.server:ExpireOption` 等既有写法
未动，新增文本一律不带 `.cs:名` 形态）。提交：695969b；回合 dev fast-forward 至 9c06e3b。
