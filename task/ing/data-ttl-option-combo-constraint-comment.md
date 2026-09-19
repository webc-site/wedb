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
