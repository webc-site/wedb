优先级：低

问题：RESTORE 的 ttl 参数按秒解释且仅收 3 参数，与 Redis 规范（毫秒 + REPLACE/ABSTTL/IDLETIME/FREQ 修饰符）存在真实分叉，但当前代码注释只写了对标 C# 的换算口径，未向维护者与客户端文档标清这一 Garnet 继承偏差。按 Redis 规范传毫秒（如 5000 表示 5 秒）会被解释成 5000 秒，TTL 放大 1000 倍；覆盖已存在键时仅能回 BUSYKEY，无 REPLACE 通路。行为必须维持对标 Garnet 不动，仅补注释防误读。

取证（rust）：
- wedb/wnode/src/resp/key_admin_commands/types.rs:31 `network_restore`，`unpack_args` 仅解包 `[key, expiry_raw, value]` 三参
- 同函数内 `expire_after_to_ticks(now_ticks(), expiry)` 按秒换算，注释仅写「C#：DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expiry).Ticks；换算单点与 EXPIRE 同源」
- 键存在时回 `cs::RESP_ERR_BUSSYKEY`，无 REPLACE 分支

C# 对标（两边同口径，非 rust 回归）：
- garnet/libs/server/Resp/KeyAdminCommands.cs:25 `NetworkRESTORE`，`parseState.Count != 3` 即回参数数错误
- 同函数 `DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expiry).Ticks`，SETEXNX 条件写入，无 REPLACE

修法建议：在 `network_restore` 文档注释中补一句「Redis 规范 ttl 为毫秒且支持 REPLACE/ABSTTL 等修饰符，Garnet 为秒 + 3 参数，本实现 1:1 对标 Garnet，勿按 Redis 规范改单位或加参数」（transpile SKILL 1:1 对标原则，禁自行扩参数面）。

落地（2026-09-19，docs-data-comment-batch 棒）
裁决：成立，行为维持对标 Garnet 不动，仅补注释。wnode/src/resp/key_admin_commands/types.rs 的
network_restore 头部文档注释补「与 Redis 规范的已知差异」两条：ttl 按秒解释（Redis 缺省为相对
空闲毫秒、带 ABSTTL 为绝对 Unix 毫秒，按 Redis 传 5000 表「5 秒」会被解释成 5000 秒、放大 1000 倍）；
只收三参 key、ttl、value，不支持 ABSTTL/IDLETIME/FREQ 与 REPLACE，覆盖已存在键只能回 BUSYKEY。
末段写明两点均属 Garnet 继承分叉而非 rust 回归、本实现 1:1 对标 Garnet、禁改单位或扩参数面。
另在 expire_after_to_ticks 换算处补一行「口径为秒、非 Redis 毫秒」并指向函数头，防逐行读码误判。
unpack_args 三参、BUSYKEY 分支、SETEXNX 条件写入逻辑零改动（票面主张经现刻复核全部成立）。
门禁：cargo check 零错误零警告；`KeyAdminCommands.cs:NetworkRESTORE` 与 `:RDB_VERSION` 两条既有
锚点原样保留，锚点集合与改前逐条相同。提交：2e07393；回合 dev fast-forward 至 9c06e3b。
