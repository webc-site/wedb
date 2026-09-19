优先级：高（门禁全绿前置）

R5 红四枚归因修复（来源：r4 红票代办 merge 时 wnode 全量 1092 例暴露；四枚在 dev 基线 detached 树 /tmp/gate-r4 复现同红，非 r4 载荷所致；R4 基线时此四枚为绿，系 R4 之后并发合入打红）

失败清单（现刻复现于 dev 4b59438+，单跑套件秒级红）：
1. wnode::range_index_wrongtype_gate ri_key_rename_not_wrongtyped
2. wnode::resp_commandstats_session commandstats_calls_failed_rejected_end_to_end
3. wnode::resp_pubsub pub_sub_mode_resp2_whitelist_commands
4. wnode::tiered_field_ttl tiered_hash_expire_sets_and_reads_back（同套件其余 5 枚绿）

嫌疑区间（R4 基线 22548fc 之后的合入，按域对齐）：
- 枚1 疑 windex 2pl 收口（bfbd1e0：table.rs 双套闩删并、ttl.rs 改持 try_lock_key_exclusive）与 range-index 迁移面合并处置；
- 枚2/枚3 疑 slow-path 臂并入（c629653 slow.rs 分派臂）改到 commandstats/pubsub 白名单回包口径；
- 枚4 疑 tiered-tombstone 收口 f24966c（成员级 TTL 全走整值重灌）。
逐枚归因用 `git log 22548fc..dev -- <套件的被测源文件>` 加 checkout 基点复跑，必要时 bisect。

修法二选一按 C# 终裁（同 r4 票范式）：行为正确→断言随契约迁移并逐条给 C# 行实；行为破 C#→修实现留测试。只留一套机制。
门禁：私有 target 的 wnode 全域 nextest 复绿 + 触及包 workspace check；禁主仓 test.sh/clippy.sh。

---

## 判词（r5 棒）

取证口径：R4 基线 22548fc 另开 detached 树 /tmp/base-r5（私有 target /tmp/ct-r5-base）复跑四套件
＝ 23/23 全绿，坐实「基线四枚绿」；红态在 19664cd（私有 target /tmp/ct-r5）逐枚复现同形失败，均为秒级
assert，非调度抖动、非环境红。票面三条嫌疑判否两条：枚1 与 bfbd1e0（windex 2pl）无关，枚2/枚3
与 c629653（slow.rs 分派臂）无关。

同根并一：枚2/枚3 同根 3460e1e；枚1/枚4 各为独立单点。四枚的复绿载荷已由并发棒
fix-obj-arg-reparse 落地（工作提交 eaa27ea，经 0bcf574 入 dev），本棒逐枚对 C# 语料终裁复核其
「二选一」取向是否成立，并对未在册的行实补齐；复绿判据与门禁数字见文末。

### 枚1 wnode::range_index_wrongtype_gate ri_key_rename_not_wrongtyped —— 断言过强，改断言

- 根因 sha：3a13489（并包载荷改写 range_index_wrongtype_gate.rs 44 行）。迁移本体由 05df0e3
  （wkv/src/range_index/migration.rs:129 rename_range_index）落地后，该用例从「只钉门方向、
  不钉回包值」升格为钉迁移本体与 RENAMENX 三态，其中 `RENAMENX idx2 idx2`（源=目标、键存活）
  被写成 `:0`；实现 wnode/src/resp/key_admin_commands/keys.rs:424-431 的 C# 同键早退臂回 `:1`，
  红在断言不在实现。
- C# 行实：garnet/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:241-250
  （`result = -1;` 之后第一段即 `if (oldKeySlice.ReadOnlySpan.SequenceEqual(newKeySlice.ReadOnlySpan))
  { result = 1; return GarnetStatus.OK; }`，同名早退**先于** isNX 存在性判定）；同函数 :300-305
  （异名且 GET(newKey) 命中 → `result = 0; abortTransaction = true; return OK`）；回包面
  libs/server/Resp/KeyAdminCommands.cs:255-277（:268-270 注释「1 if key was renamed / 0 if newkey
  already exists」+ `TryWriteInt32(result)`）。
- 修法（eaa27ea）：断言随 C# 行实改——同名自改段改判 `:1`，另补真·NX 段（先 seed_ri 造
  idx_existing，`RENAMENX idx2 idx_existing` 判 `:0`），两态各钉一次，未削强度。
- 本棒复核：rust 侧同键早退臂与 C# :241-250 逐条件对齐（nx→`:1`、非 nx→`+OK`），成立。

### 枚2 wnode::resp_commandstats_session commandstats_calls_failed_rejected_end_to_end —— 实现破 C#，修实现留测试

- 根因 sha：3460e1e（dead-batch-two「PING/ASKING/ECHO 分派臂改转调 basic_commands 单一定义，删内联副本」）。
  基线时会话侧内联臂（22548fc:resp_server_session.rs:1376-1388）arity 失败走
  `self.abort_wrong_num_args("PING")`，该方法置 command_error_written；转调后落
  basic_commands/mod.rs:94 的 `check_arg_count!`，宏在 wresp 层纯写帧（wresp/src/check_args.rs:76-81），
  无会话副作用 → arity 错误不再计 failed_calls（实测 INFO 行 `failed_calls=0`）。此为**实现破 C#**。
- C# 行实：libs/server/Resp/ArrayCommands.cs:398-404（`if (count > 1) return
  AbortWithWrongNumberOfArguments(nameof(RespCommand.PING));`）→
  libs/server/Resp/Objects/ObjectStoreUtils.cs:21-27（`AbortWithWrongNumberOfArguments` 组帧后
  转 :47-51 `AbortWithErrorMessage`，`commandErrorWritten = true;` 写在错误帧之前）→
  libs/server/Resp/RespServerSession.cs:683-691
  （`commandStats.IncrementCalls(cmd); if (commandErrorWritten) { IncrementFailed; commandErrorWritten = false; }`）。
  即 arity 拒绝必须先置失败标志、再由同一核算点计 failed_calls。
- 修法（eaa27ea）：修实现、测试原样保留（`assert_cmdstat(&info, "ping", 2, 1, 1)` 未改）。
  核算点 wnode/src/resp/resp_server_session.rs:1230-1238 判据并上「本命令输出段以 `-` 起帧」：
  `if self.command_error_written || self.output[orig_output_len..].starts_with(b"-")`。
- 本棒复核：仍是一处计数、一条口径（标志与帧形态在同一核算点取并集，未新增第二处 IncrementFailed），
  未开兼容层；残留面为「错误帧不在本命令输出段首字节」时只靠标志兜，与 C# 语义差在写帧入口，
  属后续单源化收口题，不在本票射程。

### 枚3 wnode::resp_pubsub pub_sub_mode_resp2_whitelist_commands —— 契约已迁移，改断言

- 根因 sha：同枚2 的 3460e1e（同根并一）。转调前内联臂零参恒写 `+PONG`（无订阅模式分支），
  转调后落 network_ping 的订阅会话分支（basic_commands/mod.rs:98-101），RESP2 订阅会话回
  SUSCRIBE_PONG 整帧 —— 该分支自 22548fc 即在位，此前被内联副本遮蔽。
- C# 行实：libs/server/Resp/BasicCommands.cs:989-999（`if (isSubscriptionSession &&
  respProtocolVersion == 2) TryWriteDirect(CmdStrings.SUSCRIBE_PONG) else RESP_PONG`）+
  libs/server/Resp/CmdStrings.cs:190（`SUSCRIBE_PONG => "*2\r\n$4\r\npong\r\n$0\r\n\r\n"`）+
  C# 自身用例 test/standalone/Garnet.test/RespPubSubTests.cs:285-287（订阅态 PING 逐字节断该整帧）。
- 修法（eaa27ea）：断言随 C# 行实改，resp_pubsub.rs:261-263 期望帧改 `*2\r\n$4\r\npong\r\n$0\r\n\r\n`
  并登记 RespPubSubTests.cs:285 行实；其余白名单/拦截段（RESET、SUNSUBSCRIBE、SUBSCRIBE、QUIT、
  GET/SET/PUBLISH 拦截、RESP3 不拦）保持原断言，未放宽。
- 本棒复核：行为与 C# 逐字节一致，判「契约迁移」成立，非实现破口。

### 枚4 wnode::tiered_field_ttl tiered_hash_expire_sets_and_reads_back —— 断言过强，改断言

- 根因 sha：f24966c（tiered-tombstone 收口：成员级 TTL 面全走整值重灌 expire_sweep_or_rebuild /
  穿透物化写回）。重灌按树扫描序重插成员，信封哈希迭代序由 f1,f2,f3 变 f1,f3,f2；
  实测失败帧为 `*6` 内三对全在（无丢值、无多余成员），HLEN=3、HTTL/HPERSIST 段绿，
  同套件其余 5 枚绿。
- C# 行实：C# 侧对 HGETALL 迭代序零承诺——test/standalone/Garnet.test.collections/RespHashTests.cs:236-239
  （`AreEqual(hashEntries.Length, result.Length)` + `AreEqual(Length, result.Select(r => r.Name).Distinct().Count())`
  + `IsTrue(hashEntries.OrderBy(e => e.Name).SequenceEqual(result.OrderBy(r => r.Name)))`），
  同型断言另见 :252-255、:259-261、:264-266 —— 全部先排序后比、并以长度+Distinct 双计数钉「无缺无重」。
- 修法（eaa27ea）：断言随 C# 行实改，tiered_field_ttl.rs:219-232 改 `*6\r\n` 长度钉 + 逐对
  (field, value) 包含校验（与 C# OrderBy-SequenceEqual 同强度：数量、无重、成员全）。
- 枚级取证：f24966c 的前一态复跑旧严格断言绿、f24966c 后红（数字见文末），墓碑/重灌之外无其它源。

### 门禁与复绿数字（私有 target /tmp/ct-r5，树 /tmp/fork/r5-red）
