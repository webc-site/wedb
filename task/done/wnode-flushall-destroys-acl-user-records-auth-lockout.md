归档注记：合入 d9d15102，FLUSHALL 截断前恒根域住户重挂+0x05 水位盘上补挂，锁序 dbmeta→acl 单向，三路调用面单点覆盖

甄别结论：通过（甄别席 J5，2026-09-27，定级 P1——ns0 常规运维命令静默销毁全租户认证真源，重连 WRONGPASS 永锁+default 旧口令复活绕过+旧句柄静默永续，四害齐备）。亲验截断链：slow.rs:1248-1251 ns0 臂 → keyspace.rs flush_all_databases（:453-461，票面 :449-457 微漂）持 lock_dbmeta 后 shift_begin_address(tail) 无任何 Acl/DbMeta 豁免臂；read.rs:694-696 链低于 begin 恒 NotFound 不可逆；auth.rs :460-468 ns0 回落臂与 :503-510 非 0 租户禁回落、acl_commands.rs:704-712 NoRecord 臂与 §98「记录在场即唯一真源」互恰亲验；compact.rs:303-320 ACL/DbMeta/VectorRegistry 豁免自陈「误伤即全量丢失」仅护紧缩死域判定、不覆截断臂，保护意图在册而 FLUSHALL 漏接。C# 对照亲验：StoreWrapper.cs:625-628 仅转发、AccessControlList.cs:24 _userHandles 与存储彻底分离；ClearUsers :109-111 全仓零调用（审核订正票面成立）。审核裁定方案（重挂+锁序单向+acl_generation bump+VectorRegistry 同格）可落，现码无第二镜像。派沙箱席 c01f。

审核结论：通过（P1 成立）

审核席裁定（2026-09-27，独立亲验非背书票面）：
1 真实性：成立。截断链四层逐锚复核一致：slow.rs:1248-1251 ns0 臂 → single_database_manager.rs:594-601 → database_manager_base.rs:562-573 → keyspace.rs:449-457 flush_all_databases 持 lock_dbmeta 串行闸后 shift_begin_address(tail)，无 Acl/DbMeta 豁免臂；whlog/src/hlog/shift.rs:113-171 确认逻辑 begin 全量推进（:160）物理删段按检查点地板钳制（:165），但读路径 read.rs:694-696 低于 begin 恒 NotFound，逻辑失效不可逆；AOF 先截断后入队（database_manager_base.rs:565-571），副本回放臂 aof_processor.rs:565-602 同调截断。acl_commands.rs:707-709 NoRecord 臂、auth.rs:505-508 非 0 租户禁回落、auth.rs:462-465 ns0 回落引导单例均亲验在册。
2 反证排查（翻案尝试，不成立）：acl_store.rs:9 自陈「存储引擎为 ACL 唯一真实数据源，无全局用户字典」，读路径 :56-74 全走 wkv 点查；引导内存认证器仅 ns0 default/requirepass 形态、装配期不落盘、不重建命名用户（auth.rs:153-159 挂载刷新面同证「命名用户句柄本就取自记录，无记录即用户已删」）；无任何启动扫描/内存镜像第二持久源；AOF 已截断无可回放；compact.rs:303-325 豁免仅覆盖紧缩死域判定 is_deleted，不覆盖 begin 截断臂——该豁免自陈「误杀即整租户用户丢失」恰证保护意图在册而 FLUSHALL 臂漏接，反证保护意图成立而非豁免已覆盖。翻案无据。
3 C# 对照：逐锚属实。AccessControlList.cs:24 _userHandles ConcurrentDictionary 与 Tsavorite 存储彻底分离；StoreWrapper.cs:625-628 FlushAllDatabases 仅转发 databaseManager，全程不触 ACL；删用户唯一活口是 AccessControlList.RemoveUser 的 _userHandles.TryRemove。一处锚点订正不翻案：ClearUsers（AccessControlList.cs:109-111）全仓零调用方系死代码，票面「仅 ACL RESET 流程」表述不实，实质不变。
4 互恰：§98（deviations.md:1285-1297）回落臂系登记内保留面，其安全前提「记录在场即存储唯一真源」被 FLUSHALL 物理拆除——NoRecord 臂成为绕过 §98 收口的新入口，两节互恰、本案正是 §98 焊缝的前提破坏形。
5 六项判定：真实性成立；架构单向分层不破（重挂落 wkv 既有扫描口，无反向依赖）；单机制不破（复用 scan+upsert 与 generation bump，明禁第二内存镜像）；数据面零开销不破（FLUSHALL 控制面冷路径，O(1) 截断主体保持）；方案可落（见文末裁定方案补两处收口）；格式纯粹（纯文本合规，仅上述一处 C# 锚点订正）。
6 定级 P1：认证真源被 ns0 常规运维命令静默销毁 + 全租户锁死 + default 旧口令复活认证绕过面 + 旧句柄静默永续窗，四害齐备，非深僻路径。

问题分析：
1 Garnet 契约对齐：C# ACL 用户表与存储彻底分离——AccessControlList 内 ConcurrentDictionary 用户句柄（garnet/libs/server/ACL/AccessControlList.cs:24），StoreWrapper.FlushAllDatabases（garnet/libs/server/StoreWrapper.cs:625-628）仅走 databaseManager.FlushAllDatabases 截断 Tsavorite 存储，全程不触 ACL；唯一清用户口是 ClearUsers()（AccessControlList.cs:109-111，仅 ACL RESET 流程）。C# FLUSHALL 后全部用户（口令、启停、权限规则）原样保留可继续 AUTH。
2 工程现状确证：rust ACL 记录驻存储（自研改良，恒驻逻辑域根 vdb 0，wkv/src/compact.rs:303-317 紧缩面为 ACL 设显式豁免并自陈「误杀即整租户用户丢失」，acl_store.rs:7-9 声明恒驻不随前缀漂移——保护意图在册），但 ns0 FLUSHALL 链无对应豁免：wedb/wnode/src/resp/garnet_api/slow.rs:1248-1251 → single_database_manager.rs:594-600 → database_manager_base.rs:562-564 → wedb/wkv/src/store/keyspace.rs:449-457 flush_all_databases 直接 shift_begin_address(tail) 全域物理截断，无 Acl/DbMeta 豁免臂；whlog shift 推进 head/begin 并按检查点地板物理删段，此后读路径 wkv/src/session/raw/read.rs:690-696 对链头低于 begin 的键恒 NotFound，ACL 记录不可逆丢失（AOF 亦已先截至尾）。认证面连锁：点查 None → NoRecord（acl_commands.rs:707-709）→ 非 0 租户 store_auth_reject 恒 WRONGPASS 且禁回落（auth.rs:505-508，锁死无自救）；ns0 会话坠入回落臂（auth.rs:462-464 仅 ns0 回落引导内存认证器）——被 SETUSER 改密/停用的 default 经启动期旧 requirepass 单例复活，正是 §98「记录在场即存储为唯一真源绝不回落」要焊死的失效缝，其前提被 FLUSHALL 物理破坏。且该清除不经 AclStore::delete（无墓碑、不 bump acl_generation），在途会话挂载代数不落后无撤权广播，静默窗内已删用户凭旧句柄全权操作。副本/重启经 FlushAll 回放臂（aof_processor.rs:565-602 同调）一致丢用户。对照：FLUSHDB/FLUSHNS 走 vdb 换号不截日志 ACL 存活，唯 FLUSHALL 一臂破契约。
3 逻辑危害确证：ns0 运维常规 FLUSHALL（Redis 语义仅清键空间）静默销毁全部租户认证配置——所有租户在途会话下次 ACL 变更后集体 NOAUTH、重连 WRONGPASS 永锁；ns0 侧被改密/停用的 default 复活构成认证绕过面；已挂载被删用户句柄的会话在无 ACL 写的静默期权限永续。

涉及代码：
rust 文件与函数：
wedb/wkv/src/store/keyspace.rs:flush_all_databases（:449-457 无豁免 shift_begin_address）
wedb/wnode/src/resp/garnet_api/slow.rs:flush_command_slow（:1248-1251 ns0 臂）
wedb/wnode/src/resp/resp_server_session/auth.rs:回落臂（:462-464）、store_auth_reject（:505-508）、refresh_acl_mount_if_stale（:153-159）
wedb/wkv/src/compact.rs:ACL 豁免反证注记（:303-317）
wedb/wkv/src/session/raw/read.rs:低于 begin 恒 NotFound（:690-696）
wedb/wedb/src/server/cluster_session/aof_processor.rs:FlushAll 回放臂（:565-602）

对应 c# 文件与函数：
garnet/libs/server/ACL/AccessControlList.cs:_userHandles（:24）、ClearUsers（:109-111）
garnet/libs/server/StoreWrapper.cs:FlushAllDatabases（:625-628）

精炼执行方案：
1 keyspace.rs flush_all_databases 截断前把 [begin, tail) 区间 KeyTag::Acl（及 DbMeta/VectorRegistry 同格恒根域住户，视各自 reset 补偿面裁定范围）经既有扫描口重挂至截断后新日志（单遍 scan+upsert），保持 O(1) 截断主体；严禁第二套 ACL 内存镜像
2 或最小方案：ns0 FLUSHALL 臂回错误拒绝直至方案 1 落地（device-contaminated 门形），二择一倾向 1
3 补 acl_generation bump 与 DELUSER 同判据，消除静默期旧句柄永续窗
4 锁测：SETUSER 建户→ns0 FLUSHALL→AUTH 仍 +OK；default 改密/停用→FLUSHALL→旧口令必 WRONGPASS；副本与重启后用户集与主端全等

审核裁定执行方案（审核席整理，供 task/fix.md 消费，采纳原方案 1 并补两处收口）：
1 落点与主体：wkv keyspace.rs flush_all_databases 持 lock_dbmeta 串行闸内、shift_begin_address 之前，单遍顺序扫描 [begin, tail) 收集 KeyTag::Acl 存活记录（链首地址校验 + 非墓碑，口径同 acl_store.rs for_each_user），逐条 upsert 至当前 tail（新地址 >= 旧 tail），随后 shift_begin_address(取样旧 tail)——原记录与副本物理分居截断线两侧，截断后仅副本存活，O(1) 截断主体不变，严禁第二套 ACL 内存镜像；扫描属控制面冷路径，量级同 INFO KEYSPACE 全日志扫描既有先例
2 竞态封死（审核席新增，原票未覆盖）：scan 与 shift 之间并发 SETUSER/DELUSER 落盘的增量记录会被截断吞掉。FLUSHALL 编排全程须与 lock_acl 互斥；两锁嵌套为全仓新锁序点，须按锁序册单向定级（lock_dbmeta → lock_acl，严禁反向）并在锁序登记处补记，或以 shift 前 tail 二次采样复扫兜底窗内迟到记录
3 acl_generation bump：重挂完成后按 DELUSER 同判据复用 store.bump_acl_generation 单点推进，在途挂载会话下一拍收敛（命名用户记录在场则重挂无感、已删用户 revoke），消除旧句柄静默永续窗
4 住户范围裁定：VectorRegistry::Metadata 同为唯一持久源被截断销毁（compact.rs:311-317 自陈重启回建失据），一并纳入重挂集；DbMeta 换号映射随 FLUSHALL 零号态语义由 vdb.reset 重置不需重挂，但 0x05 分配水位盘上镜像是否随截断丢失须执行时核实（flush_database.rs:176 仅锁内存水位），丢失则补挂水位记录
5 锁测：保留原票三点（SETUSER 建户 → ns0 FLUSHALL → AUTH 仍 +OK；default 改密/停用 → FLUSHALL → 旧口令必 WRONGPASS；副本与重启后用户集与主端全等），追加两点：FLUSHALL 与并发 SETUSER 竞态窗锁测（截断后窗内新户仍可 AUTH）；重启后 next_virtual_id 不回绕锁测
6 原方案 2（拒绝臂）降级为方案 1 落地前的临时门，不替代方案 1
