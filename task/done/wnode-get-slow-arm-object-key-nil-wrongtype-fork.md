甄别结论：通过（2026-09-29 主控甄别，定级 P2——慢臂 C::Get 唯走 read_string_batch_into，wkv batch.rs:53-54 恒单域探针，对象键慢臂恒 nil+notfound；快臂三域折叠（ttl_sync.rs:418-458）出 WrongType，分叉坐实。C# BasicCommands.cs:81-82 单一入口 WRONGTYPE。修复：判型收口 wnode 漏斗层、wkv 保持单域，与快臂三域内核同源零新机制，天然兼容 §133 采序）

审核结论：通过（2026-09-29 甲轮34-A，P2 级）。全链锚复核成立（快臂 ttl_sync.rs:418-462 三域折叠 vs 慢臂 batch.rs:29-77 恒 KeyTag::String 单域）；object_envelope_regression.rs:249-252 既有「GET 集合键须回 WRONGTYPE 不是 nil」锚在降级面失守坐实；MGET 快臂 nil 口径双臂一致系 Redis 语义刻意，票面射程划定正确。判型收口在 wnode 漏斗层、wkv 保持单域，与快臂同源零新机制；SG「N 键 N 帧」契约与既有 Err 臂逐键补帧形态兼容。无修正意见。

原票面：
GET 慢臂（SG 批量冷读口）对对象信封键回 nil 并计 notfound，快臂与 C# 恒回 -WRONGTYPE 且零入账（review.md 4.2 多路径行为同构违例）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# GET 单一入口（garnet/libs/server/Resp/BasicCommands.cs:61 NetworkGET）：status == GarnetStatus.WRONGTYPE 即 WriteError(RESP_ERR_WRONG_TYPE)，记录在盘或在内存不影响该判定——C# Tsavorite pending 读收割后走同一 Reader 判型，快慢（同步/pending）两通道无第二形态。计数面对位 C# MainStoreOps GET：Found → incr_session_found / else → incr_session_notfound / WrongType 双臂均不计数（仓内单点注记见 wnode/src/storage/session/common/user_read.rs fold_outcome 头注「WrongType 双臂均不计数」）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
快臂 network_get（wedb/wnode/src/resp/basic_commands/get.rs:38 network_get）经 read_user_sync 双域折叠（wedb/wnode/src/storage/session/common/ttl_sync.rs read_adjudicated_user_sync_with_prefix：String 缺失 → 探 ObjectEnvelope 命中 → UserRead::WrongType）出 -WRONGTYPE，簿记 WrongType 静默。信封域磁盘候选（冷对象键）折叠为 UserRead::Deferred → 整体降级慢臂；SG 流水线混合批（network_get_sg Deferred 出口 output.truncate(start_len) 整批回滚重放，sg_batched_keys 快照全量键）同理整批转慢臂。
慢臂 C::Get（wedb/wnode/src/resp/garnet_api/slow.rs:538 exec_slow_impl C::Get 臂）只走 read_string_batch_into（wedb/wnode/src/storage/session/storage_session.rs:656）→ wkv read_batch_with（wedb/wkv/src/session/raw/batch.rs:29，stack_keys 恒以 KeyTag::String 编码探针，batch_read_probes → windex prefetch_batch_probes 单域探针）——该口无信封/Meta 域续探：对象键在 String 物理域恒 NotFound，emit 闭包（storage_session.rs:671 record_read_outcome(val_opt.is_some())）按 None 出 nil 帧并计 notfound。同一逻辑态（对象键）两种应答：信封驻内存 -WRONGTYPE、信封落盘或 SG 混合批重放 nil；簿记两态：零入账 vs notfound +1。快臂测试锚 wedb/wnode/tests/object_envelope_regression.rs:249（GET 集合键须回 WRONGTYPE 不是 nil）在降级面失守。MGET 双臂同走批量口（快臂 do_network_mget 对 WrongType 亦写 nil 计 notfound，array_commands.rs:391-397）不受本票波及——分叉仅 GET 单命令面（含 SG 多键 GET，garnet_api/mod.rs take sg_batched_keys 快照承接）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
协议可见应答分叉：客户端对同一集合键 GET，冷态（信封页被驱逐/刚重启未装载）得 nil 误判键不存在，热态得 -WRONGTYPE——同键两次读应答矛盾，破坏 review.md 5.1「内部多态存储对同一数据集返回逐字节全等应答」契约。SG 混合批降级重放时，批内已答 -WRONGTYPE 的热对象键被 truncate 后改答 nil，同批内帧形不一致可复现。会话 found/notfound 采样指标双臂失真（慢臂虚增 notfound）。无崩溃/数据损坏面；层级 P2（应答帧与簿记分叉，触发面为冷对象键与 SG 混合批）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/garnet_api/slow.rs:StoreGarnetApi::exec_slow_impl（C::Get 臂，慢臂唯一落点）
wedb/wnode/src/storage/session/storage_session.rs:StorageSession::read_string_batch_into（emit 闭包无判型通道）
wedb/wkv/src/session/raw/batch.rs:StoreSession::read_batch_with / read_batch_raw_with（KeyTag::String 单域探针）
wedb/wnode/src/resp/basic_commands/get.rs:RespServerSession::network_get / network_get_sg（快臂对照面与降级快照装配）
wedb/wnode/src/storage/session/common/ttl_sync.rs:read_adjudicated_user_sync_with_prefix（快臂双域折叠单源）
对应 c# 文件与函数：
garnet/libs/server/Resp/BasicCommands.cs:NetworkGET（:74-90 WRONGTYPE → WriteError 单一入口）
garnet/libs/server/Resp/BasicCommands.cs:NetworkGET_SG（逐键独立成帧，同一 reader 判型）

精炼执行方案：
1 C::Get 慢臂补判型通道：批量读口保留 String 域取值，读前（或 emit 内 None 臂）对候选缺失键补一次双域折叠复验（慢臂对偶 read_user_quiet_with_prefix / 异步三域探针），信封或 Meta 域命中改出 -WRONGTYPE 且该键簿记静默（对位快臂 fold_outcome None），禁在 wkv 批量口内另起第二套判型（wkv 层保持单域，判型收口在 wnode 漏斗层，与 read_user_quiet 三域内核同源）
2 SG 多键快照逐键独立成帧语义保持：N 键 N 帧序不变，仅 WrongType 键帧形由 nil 改错误帧（对位 C# NetworkGET_SG 逐键独立 WRONGTYPE 同形）
3 测试验证点：集合键 + EXPIRE 驱逐或 debug flushan devilict 冷化后 GET 回 -WRONGTYPE 非 nil；SG 管线「热集合键 + 冷字符串键」混合批降级重放后各键帧形与快臂逐字节一致；簿记断言 WrongType 键不增 notfound

来源：甲轮28-B 交叉复审席（2026-09-29），主控亲验（快臂双域折叠 ttl_sync.rs / 慢臂批量口 batch.rs 单域探针 / C# NetworkGET WRONGTYPE 单入口 / MGET 双臂同批量口不涉）。复审维度：task/review.md 4.2 多路径行为同构（快慢双臂 parity 单面）

终态注记（2026-09-29 执行席）：
合入 9585286（分支 fix-get-slow-wrongtype，开发提交 4fc2735）。收口形态：判型收口 wnode 漏斗层——StorageSession 新增 read_user_batch_into（GET 慢臂专用批量漏斗：wkv 批量口保持 KeyTag::String 单域取值不动，String 域确认缺失键批量闭环后经 object_kind_alive_with_prefix 续探信封/Meta 两域，命中即 nil 占位帧原位替换为 WRONGTYPE 错误帧、倒序替换帧偏移恒准、N 键 N 帧序零漂移、簿记 WrongType 静默；批量口 Err 中止本地计数随部分帧整体丢弃零入账，与快臂 network_get_sg Deferred 出口同形）；object_kind_alive_with_prefix 为 read_user_quiet_with_prefix 三域折叠后两腿单源抽出（两处共用，零第二套机制），续探走 read_tag_quiet_with_prefix 静默内核自带 TTL 门与磁盘候选异步冷读闭环，§133「已过期对象键答 nil」采序天然兼容；slow.rs C::Get 臂切新漏斗，MGET 刻意仍走 read_string_batch_into（Redis MGET 非字符串键答 nil 双臂一致，票面射程外）。测试 wnode/tests/get_slow_arm_object_wrongtype.rs 两用例（全真存储真协议帧无 mock，object_read_accounting.rs 同形基建）：①慢臂直驱（SlowWait 同径）+ flush_and_evict_all 冷化——信封对象键/升阶 Meta 键（65546 字段灌水升阶）GET 答 WRONGTYPE 且与快臂热形逐字节等、WrongType 零入账（found/notfound 四键合计恰 1+1）、冷字符串命中不回归、真缺失仍 nil；②热字符串+冷对象键+缺失键 SG 流水线混合批整批判停重放——N 键 N 帧序不漂移、批内帧形与逐键应答逐字节一致、簿记同律。worktree 内 cargo check --all-targets 通过；test.sh/clippy 由主控集成门禁统一执行。MigrationBusy（windex 迁移期）面未单测（信封/Meta 两域续探通道已被冷化面锁死，迁移面共享同一 read_tag_quiet 内核，无独立判型分支）。
