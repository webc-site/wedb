向量登记表库域隔离：(ns,db) 复合登记键 + FLUSH 域回收联动

来源：next/vector-registry-nsdb-isolation.md 移交（原档由二次接管代理按脏树 diff 重建，
其挂靠的 /tmp/fork 未提交基线不在主仓 HEAD，按「未提交即未落地」重立本档，取证全部按
主仓 dev HEAD 重做）。

问题

SKILL 转写规范的「物理键前缀刚性隔离 [NsVarint]+[DbVarint]+[KeyTag]+[Payload]」与
「FLUSHDB 瞬间换号」两条在向量登记表上未打通。rust 的 VectorManager 是全服务器单例，
登记表 key_index_registry 以裸用户键为键，全部消费点不带 (ns,db) 域。后果一：切库后
同名向量键跨库互命中（登记表、DiskANN context、WRONGTYPE 门、TYPE/EXISTS 向量臂全部
串扰），DBSIZE/KEYS/SCAN 把他库向量键算进本库。后果二：FLUSHDB/FLUSHALL 换号只隐藏
wkv 数据域，登记表条目与底层索引 context 无回收联动，换号后同键名 VADD 复用旧 context，
清库语义被架空。属功能缺口（正确性），非打磨。

rust 现状取证（主仓 HEAD，工作树与 HEAD 一致）

- 单例：VectorManager::new 全仓非测试唯一装配点 wnode/src/service.rs:716（node_components
  内，service.rs:708 定义），Arc<VectorManager> 经 provider.vector_manager
  （wnode/src/service.rs:860）灌给全部会话；构造签名 wnode/src/resp/vector/
  vector_manager.rs:213 `pub fn new(options, callbacks)` 无库身份参数，而 C# 构造器
  首参即 dbId。
- 登记表裸键：wnode/src/resp/vector/vector_manager.rs:178
  `pub(crate) key_index_registry: ConcurrentMap<Vec<u8>, [u8; INDEX_SIZE_BYTES]>`；
  读写单点 wnode/src/resp/vector/vector_manager_locking.rs:416 read_stored_index(key)
  与 :421 write_stored_index(key, ..) 首参均裸 &[u8]，无任何域参数。
- 复合键原语在 HEAD 完全不存在：全仓 grep registry_key / split_registry_key /
  reclaim_registry_domain / RegistryReclaim / registry_domain_count /
  registry_domain_user_keys 零命中。
- 消费点裸键直查（逐项核实）：向量子命令面 wnode/src/resp/vector/
  resp_server_session_vectors.rs:263、:877、:949（read_index 于 :262 收裸 key，
  :949 直接用 args[0]）；WRONGTYPE/写门 wnode/src/resp/garnet_api/raw.rs:38
  set_vector_guard；TYPE/DEL 臂 wnode/src/resp/array_commands.rs:512；
  EXISTS/基本命令臂 wnode/src/resp/basic_commands/mod.rs:614、:639；
  RENAME 臂 wnode/src/resp/key_admin_commands/keys.rs:435、:633；
  BITOP 臂 wnode/src/resp/bitmap/bitmap_commands.rs:378；
  DBSIZE/KEYS/SCAN 慢路径 wnode/src/resp/garnet_api/slow.rs:193（本库 db_size 直接加
  全局 key_index_registry.pin().len()）、:207（KEYS 把全登记表键推入本库结果）、
  :260（SCAN cursor==0 同上）——三处即跨域计数的实证。
- 迁移面同样裸键：wnode/src/resp/vector/vector_manager_migration.rs:41
  get_vector_set_keys_for_slots、:47 直接 pin key_index_registry、:58
  read_migrated_index(key)、:140 write_stored_index、:189/:222。
- AOF 合成条目键定值硬编码：wnode/src/resp/vector/vector_manager_replication.rs:59
  `NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, key)`——所有向量合成条目
  恒以 ns=0/db=0 入账，重放端无从切回真实域。
- 域感知探针已具三面、独缺登记表一面：wnode/src/storage/session/common/ttl_sync.rs:448
  probe_alive_with_registry(session, prefix, key, vector) 收显式 prefix，其 String /
  ObjectEnvelope / Meta 三域臂一律以 prefix + key 复合寻址
  （probe_alive_with_prefix 同文件 :425 起，域臂 :482），唯独第四态 :459 写成
  `vm.read_stored_index(key)` 裸键——同一探针内前缀口径自相矛盾，是登记表跨域最清
  楚的实证。消费方已按该形态接线：wnode/src/resp/key_admin_commands/keys.rs:482、:765。
- FLUSH 无联动：wnode/src/database/single_database_manager.rs:271 flush_database 仅
  store.flush_database + safe_flush_aof(FlushDb)；:286 flush_namespace、:312
  flush_all_databases、:325 reset 同形；wnode/src/database/ 全域 grep vector 仅命中
  recover_vector_sets（:168，当前为返回 Ok(0) 的桩）与 i_database_manager.rs:139 声明，
  登记表/context 无任何回收通道。

C# 对位（路径行号按本 checkout 核实）

- garnet/libs/server/GarnetDatabase.cs:78「Per-DB VectorManager」注释 + :83
  `public readonly VectorManager VectorManager;` 为每库实例字段；:130 构造赋值。
- garnet/libs/host/GarnetServer.cs:420 `new VectorManager(dbId, serverOptions, ...)`
  在按 dbId 建库的路径内，:429 随该库 GarnetDatabase 一起返回——每库一实例。
- garnet/libs/server/Resp/Vector/VectorManager.cs:173 `private readonly int dbId;`、
  :178 构造器首参 dbId、:190 日志名带 dbId；后台会话一律钉本库：
  VectorManager.cs:271、VectorManager.Cleanup.cs:144/:210/:277、
  VectorManager.Quantization.cs:85 均 TrySwitchActiveDatabaseSession(dbId)；
  VectorManager.Replication.cs:348-350 集群形态显式要求 dbId==0。
- C# 侧索引记录随所属库的独立 hybrid log 存亡（每 dbId 一套 store/AOF/检查点目录：
  GarnetServer.cs:426 CreateStore(dbId,..)、:427 CreateAOF(dbId)），故 FLUSH 物理截断
  即整体失效，无需额外清理编排。rust 按 SKILL 收为共享单日志 + 前缀隔离 + 虚拟号换号，
  域隔离必须由登记表自己承担：这是同等隔离语义在 rust 拓扑下的唯一实现，不是新增复杂度。

修订方案

1. 登记表键单点带会话域。新增合成/剥离唯一入口（registry_key / split_registry_key），
   物理会话前缀字节直拼用户键（不带 KeyTag、不加冒号，遵 SKILL「前缀用 enum u8」），
   前缀取自 wkv StoreSession::session_prefix()（wedb/wkv/src/session/mod.rs:268），与
   数据条目物理键前缀字节同源；编码器复用
   wval NamespaceDbCodec::encode_with_session_prefix
   （wedb/wval/src/ns_codec.rs:508，已有调用先例 wkv/src/session/keys.rs:27）。
   唯一性由变长前缀自定界保证（[NsVarint][DbVarint] 段自闭合），对偶读端 split 需
   NamespaceDbCodec::decode_varint 由私有升公开（wval/src/ns_codec.rs:496 现为
   `const fn decode_varint`，无 pub），并在两侧注释钉住该论证。
2. 读写收口：read_stored_index / write_stored_index
   （vector_manager_locking.rs:416/:421）签名收 (prefix, key)，内部保留直读直写单点
   （lookup/put/remove_registry 语义），vector_manager_locking.rs 为合成唯一入口；
   锁协议、量化通道、丢弃通道、AOF 合成条目键全程工作在复合登记键域。禁止散点裸键直查，
   上列「消费点」每一项都要经收口函数，不得在调用侧自行拼前缀。
3. AOF 入账键改带域：vector_manager_replication.rs:59 的 encode_tagged_key(0,0,..) 改
   encode_with_session_prefix(prefix,..)，重放端以 KeyContextGuard 切域后的
   session.batch.session_prefix() 传入各 replay_vector_set_* 前缀，使登记表域随条目域
   收敛，消除恒 0,0 的跨域错入账。
4. FLUSH 域回收单点漏斗 reclaim_registry_domain，三臂 RegistryReclaim::{Database,
   Namespace,All}：逐条目走 split 比对域值，复用既有 request_deletion
   （vector_manager.rs:465）+ delete_vector_set（:492）/ remove_registry 通道，
   不新造第二套清理编排。挂点：single_database_manager.rs:271 flush_database、
   :286 flush_namespace、:312 flush_all_databases、:325 reset（主端执行段，载荷域值与
   广播条目同源）+ aof_processor 的 FlushDb/FlushNs/FlushAll 重放臂 + CLUSTER RESET 族。
5. 命令面消费点适配：network_v* / try_add / delete_vector_set / rename_vector_set /
   import_migrated_* / read_migrated_index（vector_manager_migration.rs:58）/
   delete_migrated_vector_set（:222）/ mark_suppress_cleanup（vector_manager.rs:509）/
   request_drop_in_memory_index（:554）全部带 prefix 首参；garnet_api dispatch、
   garnet_api/raw.rs:38 门、keys.rs RENAME 臂、basic_commands/mod.rs EXISTS、
   array_commands.rs DEL 与 TYPE、bitmap_commands.rs:378 BITOP、slow.rs:193/:207/:260
   的 DBSIZE/KEYS/SCAN 改域内计数与域内用户键投影（registry 域内计数/枚举单点）。
6. get_vector_set_keys_for_slots（vector_manager_migration.rs:41）返回剥域用户键——
   迁移帧口径恒为裸用户键，目标端按本端会话域重新复合。
7. 域判定单点：登记表域命中判定与 TTL 第四态探针同源（探针实现在
   wnode/src/storage/session/common/ttl_sync.rs:448 probe_alive_with_registry，另见
   task/done/vector-key-ttl-fourth-domain.md:61），严禁出现第二套 (ns,db) 拼装；该单点
   同时承接该档「八处 read_stored_index 在隔离档落地后彻底归一进探针」的挂账。
8. 不做向下兼容：旧无域裸键面直接删除，无旧数据迁移——登记表不随检查点持久化，
   重启经 AOF 重放按域重建（SKILL「不需要向下兼容，不需要旧版数据迁移，直接删除」）。

刻意不做

- 不把 VectorManager 改为 (ns,db) 实例表。C# 每库一实例的前提是每库一套 Tsavorite
  日志；rust 的库为会话前缀编号、按需冷加载且 FLUSHDB 即时换号（见
  wnode/src/database/garnet_database.rs:1-8 拓扑声明），实例表会引入 context 空间、
  DiskANN 索引文件与检查点目录的按库派生，是 C# 没有的额外机制。本档以复合键达成同等
  隔离语义。若后续要真按库分实例，另立项并先给 C# 对位论证。
- 不顺手做 DEL/SET 覆写向量键的重放端登记幽灵清理（task/done/
  vector-key-ttl-fourth-domain.md 五、范围声明已另立待办）。
- 不改 recover_vector_sets 桩语义（single_database_manager.rs:168 的向量引擎域缺口属
  另一域）。

优先级

功能缺口（跨库串扰与清库语义失效），排在死代码与重复/多套架构类待办之后、打磨类之前；
与 task/done/vector-key-ttl-fourth-domain.md（本轮已落 done，写闸与重放臂已在位）有登记表
消费点交叠：其 :61 明载「八处 read_stored_index 在隔离档落地后彻底归一进探针」，该义务即本档方案 7。

验收

1. cargo check --workspace --all-targets 绿。
2. cargo test -p wnode vector 全绿，并扩 wedb/wnode/tests/vector_key_domain_ops.rs
   （现 331 行、4 个用例，零 SELECT/切库形态）：db0/db1 同名键 VADD 互不可见、
   TYPE/EXISTS/DBSIZE/KEYS/SCAN 按库判定、FLUSHDB 后本库登记与 context 回收而他库不受影响。
3. 重启 rebuild 路径按域恢复：vector_replication_replay 与 vector_set_interrupt_delete_recovery
   经带域条目重放后逐库可观测。
4. FLUSH 三臂（库/命名空间/全域）与 CLUSTER RESET 族均有登记回收联动的断言。
5. delta 恰为本域文件，不回退 tiered-promote-aof、库级定槽、flushall-bus、
   vector-registry-write-gate（写门已落地，实证为 raw.rs:38 set_vector_guard 与
   resp_server_session_vectors.rs:275 abort_vector_set_wrong_type）等他人改动。
6. ./sh/clippy.sh 零警告、./test.sh 全绿、./js/check.js 无新增缺失（如需 ignore 按 SKILL
   配置，禁 #[allow]）。

涉及文件（以实际改动点为准）

wedb/wnode/src/resp/vector/vector_manager.rs、vector_manager_locking.rs、
vector_manager_migration.rs、vector_manager_replication.rs、
vector_manager_cleanup.rs、resp_server_session_vectors.rs、
wedb/wnode/src/resp/garnet_api/raw.rs、garnet_api/slow.rs、
resp/key_admin_commands/keys.rs、resp/basic_commands/mod.rs、resp/array_commands.rs、
resp/bitmap/bitmap_commands.rs、wedb/wnode/src/storage/session/common/ttl_sync.rs、
wedb/wnode/src/database/single_database_manager.rs、wedb/wnode/src/aof/aof_processor.rs、
wedb/wval/src/ns_codec.rs（decode_varint 升公开）、wedb/wkv 侧仅复用既有
session_prefix/encode_with_session_prefix、
wedb/wnode/tests/vector_key_domain_ops.rs 及上列 vector 相关测试。

落地方式：./fork.sh vector-registry-nsdb-isolation 新起分支，基线为主仓 dev HEAD（不接管
任何 /tmp/fork 现场），按 SKILL 与 .agents/skills/rust_review/SKILL.md 自查后并回。

细化方案（f25-vec-registry 实施档，2026-09-19 按主仓 dev HEAD 05dc7ec9 复核定稿）

甄别结论：原方案 8 条全部成立，取证逐项复核命中。两处订正：
a) 原「CLUSTER RESET 族」挂点不成立——C# ClusterReset/TryReset 只清集群拓扑状态
   不清库数据（slow_path.rs 仅 HasKeysInSlots 检查，无 flush 面），登记表无回收需求，
   不挂；FLUSH 三臂 + reset 四处已全覆盖清库语义。
b) 与 task/ing/aof-replay-virtual-domain-context.md 边界维持该档自述「两单互不覆盖」：
   本档只管登记表键域与回收联动，KeyContextGuard 的 set_context/set_virtual_context
   口径归那档；本档重放端统一取 session.batch.session_prefix()，域值语义随那档落地
   自动收敛，不预先改它的面。

实施定稿：

1. 复合登记键 [NsVarint][DbVarint]+user_key（无 KeyTag，varint 段自定界）。
   合成/剥离单点 vector_manager_locking.rs：registry_key(prefix, key) -> TaggedKeyBuf
   （StackHeapBuf<62>，超限堆退化，用户键无硬限）；split_registry_key(composite)
   -> Option<(&[u8] prefix, &[u8] user_key)>。wval/src/ns_codec.rs decode_varint
   const fn 升 pub（split 单点用，两侧注释钉自定界论证）。
2. read_stored_index / write_stored_index 签名收 (prefix, key)（locking.rs:416/:421
   语义位不变，内部 registry_key 合成单点）。下列面全部工作在复合键域（键锁、
   requested_drops、量化通道载荷 QuantizationState.key、cleanup pending）：
   read_vector_index_core / read_or_create_vector_index / create_or_recreate_under_exclusive
   / recreate_index_locked / create_index_locked / try_add / delete_vector_set /
   rename_vector_set / mark_suppress_cleanup / request_drop_in_memory_index /
   drop_requested / wait_for_disk_ann_index_drop 全收 (prefix, key)；量化任务消费端
   split 一次取 (prefix, user_key)。
3. 命令面（分发面 StoreGarnetApi::exec 向量臂先取 self.session.session_prefix() 传入）：
   RespServerSessionVectors::read_index 与 network_v* 全族加 prefix 首参；
   raw.rs dispatch 内 set_vector_guard / vector_registry_gate 加 prefix 参数
   （batch.session_prefix() 已在域）；basic_commands MEMORY USAGE/OBJECT、
   array_commands TYPE、bitmap BITOP、keys.rs RENAME 两臂（rename_vector_set_sync
   同步改）——各点 store: &BatchStoreSession 均可 session_prefix()。
4. AOF 入账带域：VectorAofSink::enqueue 加 prefix 参数，encode_tagged_key(0,0,..) 改
   encode_with_session_prefix(prefix, KeyTag::String, key)；replicate_vector_set_add/
   remove/set_attribute/rename/index 加 prefix 首参。重放端不改 store_rmw 签名：内部
   向量臂取 session.batch.session_prefix() 传给 replay_vector_set_*（分块回放
   aof_processor_chunk_replay.rs:110/:125 经同一 store_rmw 自动覆盖）；
   replay_vector_set_add/remove/set_attribute/rename/index 加 prefix 首参。
5. FLUSH 域回收单点：RegistryReclaim::{Database{vns,vdb}, Namespace{vns}, All} +
   VectorManager::reclaim_registry_domain（枚举复合键 → split 比对域 → 收集命中后
   逐条 request_deletion + 摘除，复用 delete 通道不造第二套编排）。挂点：
   single_database_manager.rs flush_database（(vns, domain_db)）/ flush_namespace
   （domain_vns）/ flush_all_databases / reset 四处；SingleDatabaseManager 加
   Option<Arc<VectorManager>> 注入通道（attach 形态，service.rs 四个构造点装配）。
   aof_processor.rs FlushDb/FlushNs/FlushAll 重放臂经 append_only_file.vector_manager()
   现成通道调 reclaim（域值取条目载荷，与数据条目同源）。
6. 迁移面：get_vector_set_keys_for_slots 枚举复合键返回 (复合键, index)（源端删除
   delete_migrated_vector_set 收复合键内部 split）；帧装帧处 split 取剥域用户键
   （迁移帧口径恒裸键）；目标端 frame_import 以目标端会话域 prefix 调
   import_migrated_index/import_migrated_element/read_migrated_index；
   live_value.rs probe_live_key_kind 三点 read_migrated_index 带 storage 会话 prefix；
   diskless 快照迭代器同帧口径。
7. DBSIZE/KEYS/SCAN 域内投影：registry 域内枚举单点 for_each_domain_user_key(prefix, f)
   + registry_domain_count(prefix)（slow.rs:193/:207/:260 三处改用，域内 glob 投影、
   域内计数；慢路径 O(全表) 扫描与 KEYS 同价，可接受）。
8. probe_alive_with_registry 第四态改 read_stored_index(prefix, key)（前缀口径与
   三域臂对齐，ttl_sync.rs:459 唯一改点）。
9. 测试面：现存裸键调用（vector_set_rename / resp_vector_set / resp_vector_set_
   wrong_type / vector_replication_replay / vector_set_interrupt_delete_recovery /
   vector_set_cleanup_vs_reset_race / cluster_migration / diskless_sync_ri_vector）
   统一改根域 prefix（SessionPrefixBuf::new(0,0)）；扩 vector_key_domain_ops.rs：
   db0/db1 同名键互不可见、TYPE/EXISTS/DBSIZE/KEYS/SCAN 按库判定、FLUSHDB 后本库
   回收他库不受影响（本流程只过 cargo check 编译，不跑 test.sh）。
10. 只跑 cargo check --workspace --all-targets（fixloop 约束）；clippy/test.sh/check.js
    由后续统一质量档承接。

落地记录（f25-vec-registry，2026-09-19）

已实现并合回 dev（877f6768）：
- 复合登记键单点 registry_key/split_registry_key（vector_manager_locking.rs），
  wval decode_varint 升 pub、SessionPrefixBuf::ROOT 常量新增
- read/write_stored_index 收 (prefix, key)；stored_index_of/put_stored_index/
  remove_stored_index 内部轴键单点；锁协议、量化通道载荷、丢弃通道全复合键域
- AOF 合成条目 encode_with_session_prefix 带域（消恒 0,0 入账）；重放端
  store_rmw 向量臂 session_prefix 域收敛（分块重放同覆盖）
- FLUSH 域回收：RegistryReclaim 三臂 + reclaim_registry_domain 单点；主端
  四挂点（flush_database/flush_namespace/flush_all_databases/reset，经
  attach_vector_manager 注入）+ AOF Flush 三重放臂回收
- DBSIZE/KEYS/SCAN 域内投影（for_each_domain_user_key/registry_domain_count，
  复合键字节前缀比对，OPPV 自定界保证零误命中）
- 迁移面：源端枚举复合键、帧口径恒剥域用户键、目标端按本端会话域复合、
  live_value 探测按会话域复合（KEYS 链收集同步复合）
- TTL 第四态探针 read_stored_index(prefix, key) 前缀口径对齐三域臂
- 测试：vector_key_domain_ops.rs 扩两用例（跨库同名键互不可见/TYPE/EXISTS/
  DBSIZE/KEYS/SCAN 按库判定 + FLUSHDB 本库回收他库存活），consumer 装配
  SingleDatabaseManager 注入回收链；全部现存向量测试改根域前缀

验证：cargo check -p wval -p wkv -p wnode -p wedb --all-targets 绿（独立
target；共享 CARGO_TARGET_DIR=/tmp/rust_reviv_coalescing 有跨 worktree 缓存
互踩假错，须独立 target 验证）。workspace 全量 all-targets 在合并时点由他人
wext_json/wext_roaring 刚合入的半成品测试面报错（dev 基线自带，非本档引入，
主链 cargo check --workspace 绿）。

遗留（另立待办，非本档）：
- CLUSTER RESET 无数据清空语义（C# TryReset 只清拓扑），无回收挂点需求
- MIGRATE KEYS 链 storage 会话为根域，非根域键不参与该链（与 wkv 三域探测
  口径一致，跨域串扰已随本档消除）
