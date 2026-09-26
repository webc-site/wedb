拒绝结论：判净（核对 C# KeyAdminCommands.cs 与 SessionParseStateExtensions.cs，EXPIRE 族 NX/XX/GT/LT 及复合选项错误帧与互斥矩阵逐字节对齐，键不存在/无 TTL/有 TTL 比值返回及立即删除逻辑完全闭环，大值钳制合规，SET 复合选项对齐，无缺陷无分叉）

EXPIRE 族 GT/LT/GTE 与带 EX 复合命令选项边界及状态流转深度审查

核查概述与划界基线：
1. 划界说明：本审查严格遵守划界约定，expireopts（deviations.md §143 命令边界粗化单点化）已落地，getexwatch（GETEX 独立 checked 换算与 WATCH 推进）已判净，不重复立项。
2. 核查要点一：wedb/wnode/src/resp/key_admin_commands/ 中 EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT 的 NX/XX/GT/LT/XXGT/XXLT 选项解析与校验。
3. 核查要点二：键不存在、键存活无 TTL、存活已有 TTL 与新 TTL 比较（GT/LT 严格大小判定，等值拒绝）的返回值（0 与 1），以及负过期时间拦截与极大时间戳饱和钳制。
4. 核查要点三：复合命令（SET ... EX/PX/KEEPTTL 与 GETEX ... EX/PX/EXAT/PXAT/PERSIST）与 EXPIRE 族的语义一致性及 Garnet/Redis 7.0 契约对标。
5. 核查要点四：核验 doc/zh/deviations.md 既有在册条款（§4 a/b、§75、§123、§143 等），确认既定架构改良与崩溃防御机制合规生效。

核查逐项确证：

核查项一：EXPIRE 族选项解析与兼容性校验
涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/key_admin_commands/keys.rs:parse_expire_args
wedb/wresp/src/options.rs:try_get_expire_option
对应 c# 文件与函数：
garnet/libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRE
garnet/libs/server/SessionParseStateExtensions.cs:TryGetExpireOption
确证结果：
1. 参数个数 2..=4 严格对标 C# parseState.Count < 2 || count > 4。
2. 选项词元解析仅认 NX/XX/GT/LT（wresp::options::try_get_expire_option），非法选项回写 ERR Unsupported option %s，逐字对齐 C# GenericErrUnsupportedOption。
3. 双选项仅放行 XX+GT（XXGT）与 XX+LT（XXLT），其余任意互斥组合（如 NX+XX、GT+LT、同选项重复等）均回写 ERR NX and XX, GT or LT options at the same time are not compatible，逐字节对齐 C# KeyAdminCommands.cs:415。
4. 关于 GTE 疑问：Redis 7.0 契约与 Garnet 原型均无 GTE（大于等于）选项，GT/LT 语义严格取严格大于与严格小于，等值场景双方均拒绝，判定完全闭环合规。

核查项二：键状态判定、返回值矩阵及过去时间戳物理删除
涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/key_admin_commands/keys.rs:expire_apply_sync
wedb/wnode/src/resp/key_admin_commands/slow.rs:key_admin_slow
wedb/wkv/src/ttl.rs:StoreSession::expire_at
对应 c# 文件与函数：
garnet/libs/server/Storage/Functions/SessionFunctionsUtils.cs:EvaluateExpire
garnet/libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:InPlaceUpdaterWorker
garnet/libs/server/Storage/Functions/UnifiedStore/PrivateMethods.cs:EvaluateExpireInPlace
确证结果：
1. 键不存在时：快慢两路（ttl_write_alive 返回 Some(false) 或 wkv expire_at 返回 -2）均返回 0，对齐 C# status != OK。
2. 键已存在但无 TTL 时：
- 无选项：设置 TTL，返回 1。
- NX 选项：满足条件，设置 TTL，返回 1。
- XX 选项：不满足条件，返回 0。
- GT 选项：按 Redis 7.0 契约与 Garnet 规范，无 TTL 视同无限 TTL（infinite），任何有限值均不大于无限，故拒绝并返回 0。
- LT 选项：有限值严格小于无限，满足条件，设置 TTL，返回 1。
- XXGT / XXLT 复合选项：因缺乏既有 TTL，XX 前置门拒绝，均返回 0。
3. 键已存在且具 TTL 时：
- NX 选项：已有 TTL 故拒绝，返回 0。
- XX 选项：已有 TTL 满足条件，更新 TTL，返回 1。
- GT 选项：新 TTL <= 当前 TTL 拒绝（返回 0），新 TTL > 当前 TTL 更新（返回 1）。
- LT 选项：新 TTL >= 当前 TTL 拒绝（返回 0），新 TTL < 当前 TTL 更新（返回 1）。
- 等值情况：expire_at_ticks == c 时，GT 与 LT 均判定为条件不满足，返回 0，无边界歧义。
4. 过去时间戳立即物理删除：
- is_expired_or_now 判定（<= now_ticks），先删 TTL 记录再删数据记录，回执 :1 并推进 WATCH 版本。
- 若条件选项不满足（如已有 TTL 的键给更小的过去戳但带 GT），条件门先于过去时间戳删除门拦截，返回 0，绝不误删数据。

核查项三：负数与大值溢出保护机制
涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/key_admin_commands/keys.rs:parse_expire_args
wedb/wbase/src/convert.rs:expire_after_to_ticks
wedb/wbase/src/convert.rs:expire_at_seconds_to_ticks
对应 c# 文件与函数：
garnet/libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRE
garnet/libs/common/ConvertUtils.cs:UnixTimestampInSecondsToTicks
确证结果：
1. 负数过期时间：expiration < 0 时直接拦截，回写 ERR invalid expire time, must be >= 0，对标 C# RESP_ERR_INVALID_EXPIRE_TIME。
2. 相对大值溢出：EXPIRE/PEXPIRE 相对大秒数通过 saturating_add 钳制到 i64::MAX ticks 并正常返回 :1，消除 C# DateTimeOffset.UtcNow.AddSeconds 越界抛异常掐断连接的缺陷，属 doc/zh/deviations.md §4 a) 在册防御改良。
3. 绝对 Unix 时间戳回绕：EXPIREAT/PEXPIREAT 采用 clamp(0, MAX_UNIX_TIMESTAMP_SECONDS/MILLISECONDS) 确定性钳制，消除 C# unchecked 乘加溢出回绕垃圾值写进 TTL 的缺陷，属 doc/zh/deviations.md §4 b) 在册防御改良。

核查项四：复合命令（SET ... EX/PX/KEEPTTL 与 GETEX）语义一致性
涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/basic_commands/set.rs:parse_set_options
wedb/wnode/src/resp/basic_commands/set.rs:apply_set_with_expiry
wedb/wnode/src/resp/basic_commands/get.rs:network_getex
wedb/wnode/src/resp/basic_commands/ttl.rs:try_get_absolute_expiry_ticks
对应 c# 文件与函数：
garnet/libs/server/Resp/BasicCommands.cs:NetworkSETEXNX
garnet/libs/server/Resp/BasicCommands.cs:NetworkGETEX
确证结果：
1. SET 命令选项集严格对标 C# NetworkSETEXNX：仅放行 EX/PX/KEEPTTL，出现 EXAT/PXAT 命中过期解析器但被可接受集过滤并报错 ERR syntax error，双侧契约完全逐字一致。
2. SET ... EX/PX 过期数值校验为正 int32（v <= 0 报错 ERR invalid expire time in 'set' command），大值由 (i64::MAX - current_ticks) / ticks_per_unit 守卫，杜绝算术溢出。
3. SET ... KEEPTTL 在 RMW 窗口内读旧 TTL 并安全回填，写值与 TTL 维护原子收口。
4. GETEX 支持 EX/PX/EXAT/PXAT/PERSIST，绝对/相对换算收敛于 wbase checked 单源，溢出安全报错，WATCH 推进已闭环（deviations.md §143 案二在册）。
5. 粗化单点边界一致：EXPIRE 族经由 ExpirationWithOption 在命令入口进行 4-bit coarse 粗化，而 SET/GETEX 全程保存裸 ticks，会话入口统一恒等直通，符合 deviations.md §143 裁决。

核查结论：
本席经对 Rust 代码库全链路（keys.rs, slow.rs, set.rs, ttl.rs, options.rs, convert.rs）与 C# 原型（KeyAdminCommands.cs, SessionFunctionsUtils.cs, RMWMethods.cs, BasicCommands.cs）深度逐行对账，确认 EXPIRE 族各选项、条件比较逻辑、返回值矩阵、溢出保护及复合命令一致性均已完备收敛，且与 deviations.md 既有在册条款完全自洽，未见逻辑分叉与余缝遗留。

视角结论:已穷尽
