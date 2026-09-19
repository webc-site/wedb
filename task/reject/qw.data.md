qw.data.md 第 11 轮拒绝档案（数据命令 / TTL 参数面角度，分拣代理 2026-09-19）

条 1 LASTSAVE / SAVE / BGSAVE 的 DBID 参数臂解析后丢弃，跨库应答与 C# 分叉

原文要点
rust 会话层 network_lastsave / network_bgsave 只做 try_parse_database_id 校验，随后 route_slow_command
把原样 parse_state 投递慢路径，慢路径 Lastsave 臂直接读 ctx.database_manager.last_save_ms()，既不看 args
也不带 db 维度；SingleDatabaseManager::last_save_ms 只有唯一 self.db，故 LASTSAVE 3 与 LASTSAVE 0 应答恒等，
多库形态下 DBID 臂形同虚设。修法要求「慢路径按 args 取库」，或退一步在文档明确 DBID 只作校验不作分派。
C# 对位 garnet/libs/server/Resp/AdminCommands.cs:1044-1069 NetworkLASTSAVE（:1061
TryGetOrAddDatabase(dbId) → :1064 db.LastSaveTime）。

拒绝原因（作为「按库分派的 DBID 臂缺失」这一功能缺口不成立）

一、本仓架构无每库独立落盘时刻这一形态，按 args 取库要新造不存在的机制。js/check/ignore/server.yml:833-876
已裁定 C# MultiDatabaseManager.cs 整面（含 TakeCheckpointAsync/TryGetOrAddDatabase/UpdateLastSaveData）
不转写，理由「C# 独立 TsavoriteKV 实例多库架构；rust 统一采用共享单日志多库模型（wkv 单 HybridLog 内以
16-bit database ID / tag 隔离），运行时由 SingleDatabaseManager 直接装配」。取证：检查点全链无 db 维度
——wedb/wnode/src/database/single_database_manager.rs:225 take_checkpoint(&self, _background) 与 :413
take_checkpoint_async 无形参库号，wedb/wkv/src/store/cpr_host.rs:48 create_checkpoint、
wedb/wcpr/src/manager/create.rs:71 create_checkpoint 亦无；同文件对比面 flush_database
（single_database_manager.rs:295 持 db_id）说明库维度只在数据面存在、不在落盘面存在。
故 LASTSAVE 3 == LASTSAVE 0 是架构事实而非漏读参数，按票面补分派等于为一条命令凭空引入 per-db checkpoint
子系统，违 .agents/skills/transpile/SKILL.md「实现复杂度要对标 c#，而不是添加额外的复杂度」。

二、C# 缺省形态同口径。garnet/libs/server/Databases/SingleDatabaseManager.cs:47-49
TryGetOrAddDatabase 对 dbId != 0 直接 ThrowIfNotEqual，:116-119 TakeCheckpointAsync 对 dbId != -1 && != 0
抛「SingleDatabaseManager only supports dbId 0」。即 C# 单库装配（本仓唯一装配形态）下 DBID 臂同样只作
校验、不产生分派差异。rust 侧既有注释亦已声明该口径：wedb/wnode/src/resp/garnet_api/slow.rs:1026-1029
「rust 共享存储单 WAL，全库共用一条物理日志…与 C# SingleDatabaseManager 忽略 dbId 同口径」。

三、票面「形同虚设」的观测面已被在册校验链覆盖：wedb/wnode/src/resp/admin_commands.rs:582 network_lastsave
（:587 arity ..=1、:589 try_parse_database_id）、:598 network_bgsave（:603 arity ..=2、:607-613
SCHEDULE/DBID 令牌序）、:624 try_parse_database_id（i32 严格口径 + MaxDatabases 范围门，对位
AdminCommands.cs:TryParseDatabaseId）。非法 DBID 已拒，合法 DBID 回全局落盘时刻，与 C# 单库管理器行为一致。

四、票面自己的退一步修法（明确 DBID 只作校验不作分派的注释口径）成立且已在途，不另立 ing 票以免双花。
验尸：git branch --list 有 lastsave-dbid-routing、git worktree list 有 /private/tmp/fork/lastsave-dbid-routing，
其工作区对 wedb/wnode/src/resp/admin_commands.rs 与 wedb/wnode/src/resp/garnet_api/slow.rs 有未提交改动，
内容正是该口径注释（admin_commands.rs:586 段「DBID 仅为兼容面校验，不作分派」+ slow.rs:990 段同措辞），
非僵尸分支。落地后由该票把 doc/zh/db.md §1 单日志多库口径引全，本处不再重复动代码。
