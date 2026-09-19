优先级：高（dev 测试基线红，计数轮前置）

单题：向量集登记键的键域编码在 dev 上多出一段 2 字节前缀，导致登记键查找/迁移类用例成片红。

红测试与实测证据（2026-09-19 dev 6e008a33 复跑，确定性失败）
1 wnode::vector_set_rename::rename_onto_string_and_vector_dest_cleans_displaced —
  wnode/tests/vector_set_rename.rs:171 「登记键应为新名」
  left: [0, 0, b"dst"] / right: b"dst"
2 wnode::vector_set_rename::rename_to_absent_key_migrates_registry —
  wnode/tests/vector_set_rename.rs:171 「登记键应为新名」
  left: [0, 0, b"vs_dst"] / right: b"vs_dst"
3 wnode::vector_key_domain_ops::vector_set_nsdb_isolation_across_dbs —
  wnode/tests/vector_key_domain_ops.rs:411
  assertion failed: scan_db1.windows(10).any(|w| w == b"vs_only_db1")
4 wnode::vector_key_domain_ops::vector_set_flushdb_registry_reclaim —
  wnode/tests/vector_key_domain_ops.rs:481 「清库后同键名重建应落新域登记」
5 wedb::cluster_migration::vector_set_discovery_for_slots —
  wedb/tests/cluster_migration.rs:2742 left: [0, 0, b"vs:disc:1"] / right: b"vs:disc:1"
6 wnode::resp_vector_set::drop_in_memory_index_flow — wnode/tests/resp_vector_set.rs:1368
  assertion failed: manager.requested_drops.contains(&key)

判读方向（须自行核实，不当结论）
四条用例的 left 都是「两字节零前缀 + 裸键名」，right 是裸键名，形态一致，指向同一处编码变更。
两字节零段是库号/命名空间维度（同 C# 的 namespace prefix 口径），怀疑向量集登记键近期被改走带库号
前缀的物理键投影，而登记侧的读回路径仍按裸名比对。先用 git log -p 追 wnode/src/resp/vector/
vector_manager.rs 的登记键构造与 wkv 侧前缀投影，判明是产线把登记键写进了错误的键域（登记应是
库内逻辑名，不该带库号前缀）、还是用例的比对面该随投影更新。第 6 条 drop_in_memory_index_flow
是同域（登记/清退）还是独立（内存索引丢弃请求未登记），分开定性。

改动域
wedb/wnode/src/resp/vector/**、向量登记键构造点、上述测试文件；如需读 wkv 前缀投影只做只读核实，
wkv 自身的 dbmeta/NS_MAP/ReadCache 属另一票（qw13-red-wkv-dbmeta-readcache-cluster），不得越界改。

## 落地补记（二棒 sv-vecreg，载荷 5e689b3e 收尸）

红 6 条（票面 1~6 号），全部定性为**用例比对面滞后**，产线零改动；复跑实测：
`cargo test -p wnode --test vector_set_rename`（6/6 ok）、`--test vector_key_domain_ops`
（6/6 ok）、`--test resp_vector_set`（14/14 ok）、`-p wedb --test cluster_migration`
（34/34 ok，含 vector_set_discovery_for_slots 与 migrate_vector_set_keys_e2e）。

产线判据（逐处对 C# 核实，非采信一棒自述）：
- 登记表条目复合键 `[NsVarint][DbVarint]+用户键` 是拼装单点
  `wnode/src/resp/vector/vector_manager_locking.rs:55 registry_key` /
  读端对偶 `:76 split_registry_key`，对偶 C# 每库一实例
  `libs/server/Resp/Vector/VectorManager.cs:177 VectorManager(int dbId, …)`
  （同文件 `:161 Service { get; } = new DiskANNService()` 每实例一份）——C# 键面天然无域前缀，
  rust 单例以域前缀直拼达成 doc/zh/db.md §1.1 同等刚性隔离，非自造偏离。
- RENAME 登记域随键搬运：`vector_manager.rs:633 rename_vector_set` 旧/新名同 prefix 复合、
  快照→开窗→新名注册→旧名摘除（C# MarkSuppressCleanup/SetFlags 窗口对偶），用例
  `assert_registry_exactly` 以 `registry_domain` 恰一项 + 域值 (0,0) + 剥域用户键钉死，无幽灵。
- 迁移帧口径恒剥域：`wedb/src/server/sync_transport.rs:195`、
  `wedb/src/server/migration/migrate_session_vector_set.rs:75`、
  `replication/diskless_replication/replication_snapshot_iterator.rs:240` 三处消费面均
  `split_registry_key` 剥域上线，源端删除按复合键（`delete_migrated_vector_set_of`）、
  目标端按本端会话域重新复合（e2e 用例两侧 `read_migrated_index(ROOT, key)` 实证）。
- FLUSHDB 换号回收：`vector_manager.rs:556 reclaim_registry_domain` 按 `split_registry_key`
  域值匹配摘除，死域整域清零由 `registry_domain_count` 钉死。
- 丢弃通道载荷为复合键（`vector_manager.rs:682 registry_key(prefix, key)` → `requested_drops`），
  与 C# `VectorManager.cs:707 RequestDropInMemoryIndex` 的 `requestedDrops.TryAdd(key.ToArray())`
  同形（C# per-db 实例的 key 即 rust 的复合键），SuppressCleanup 早退分支同 C# `:720`。

推翻一棒（5e689b3e）之处：其 5 处改动的等强或更强版本已由并发棒 a1360096 落 dev，本票不复写、
不降级；唯一残留假绿通道在红 3：一棒只把永假的 `windows(10)` 改为用户键长的**裸子串**比对
（dev 亦同），而复合键以用户键为后缀——命令面若漏剥域仍恒真，dev 注释自称「bulk 长度恰等于
用户键长，携带登记域前缀即破形」而代码未断 bulk 头。已收口为定长 bulk 帧断言
`assert_projects_stripped_bulk`（`$<len>\r\n<user_key>\r\n` 逐字节），KEYS 与 SCAN 两投影面同口径。
负控取证：临时将 `for_each_domain_user_key` 改为直发复合键，本用例 FAILED 且打出
`$13\r\n\0\u{1}vs_only_db1`（子串断言在原口径下仍会放行），改判产线剥域单点正确、断言有判别力。

门禁：`cargo check --workspace --all-targets` exit 0 / 0 warning；`bun js/check.js` exit 0 且
报告与本票改前逐字节相同（回写的 js/check/ignore/{common,storage}.yml 无关改动已 `git checkout --` 还原）。
