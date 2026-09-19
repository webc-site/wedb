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
