# 数据库隔离与命名空间架构

## 1. 数据库隔离机制

wedb 采用共享单日志多库模型（单 HybridLog + 单哈希索引），通过物理键前缀编码实现逻辑隔离。

与 Garnet C# 每个 Database 独立 Tsavorite 实例、独立 ObjectStore、独占 AOF 不同，wedb 所有 DB 共享物理存储。

### 1.1 物理键结构

代码路径：wedb/wval/src/ns_codec.rs

记录物理键布局：

```
[VirtualNsVarint] + [VirtualDbVarint] + [KeyTag: 1B] + [Payload]
```

VirtualNsVarint：虚拟命名空间 ID（变长 OPPV 编码，映射租户版本）
VirtualDbVarint：虚拟数据库 ID（变长 OPPV 编码，映射逻辑库版本）
KeyTag：1 字节类型标签（String 0x00、Meta 0x01、Ttl 0x09、Vector 0x0A、Etag 0x0B、ObjectEnvelope 0x0C、Acl 0x0D、DbMeta 0x0E、VectorRegistry 0x0F）
Payload：用户原始键或子键

VectorRegistry 0x0F（`wedb/wval/src/tag.rs`）：向量集合登记旁路记录，键 = 会话前缀 + 本标签 + [1B 子标签][负载]。子标签由基座 `wval::tag::VectorRegistrySubTag` 单点定义（`wnode` 侧 `resp::vector::vector_registry_recovery` 消费其编解码做写透与回建）：Index (0x01) = 索引登记（负载 = 集合用户键，值 = Index 字节）；Metadata (0x02) = 上下文元数据（负载 = i32 LE 元数据下标，值 = ContextMetadata 字节）。对标 C# `VectorManager.RecordType` 索引记录与 `MetadataNamespace` 元数据记录（两者在 C# 驻 Tsavorite 主存随检查点持久）；rust 登记表驻内存，经该标签写透存储域达成同等持久性。

常见场景（vns < 128 且 vdb < 128）下，前缀仅占用 2 字节。

### 1.2 会话上下文与读写链路

代码路径：wedb/wkv/src/session/mod.rs

StoreSession 为连接私有上下文（Connection-Local）。
客户端连接内严格单线程串行执行，会话即单写者。
上下文字段以原生 64 位标量存取（namespace、active_db、active_vns、
active_vdb、last_generation 为 u64，is_virtual 为 bool），经 Relaxed 原子
原语实现：对齐 64 位单指令寄存器读写，无锁、无争用、无字节撕裂，
耗时与纯标量等同，零堆分配。
Relaxed 载体仅是 &self 会话 API（批处理上下文借用会话、共享分派句柄
持会话）下的内部可变性形态，不表示支持多写者并发：六个上下文字段
不整体原子更新，（逻辑库，虚拟库，代数）跨字段一致性由单写者纪律
承接。跨任务共享会话严禁 set_context / set_active_db /
set_virtual_context 等上下文变更；共享长持有的唯一许可形态是装配期
固化上下文的只读持有者（如向量磁盘存储回调），其执行期仅允许
session_prefix 的虚库换代幂等刷新收敛（清库换号后重对齐新物理域）。

session_prefix() 在栈上构造 19 字节定长 SessionPrefixBuf，纯栈分配，零堆内存开销。
所有 CRUD（read / upsert / delete）自动携带会话前缀，不同库键天然互斥。

### 1.3 64 位库 ID 与自由切库

切库重构：
移除旧 database_sessions 数组。
消灭大库 ID 内存膨胀隐患。
会话状态收敛为标量直接赋值。
零堆分配。

无限制切库：
彻底删除 allow_multi_db 配置项。
移除集群模式切库限制。
单机与集群原生支持 SELECT。

命令实现：
SELECT（wedb/wnode/src/resp/array_commands.rs）：
调用 parse_db_index 解析数字。
直接更新会话标量 session.active_db。

FLUSHDB（wedb/wkv/src/vdb/flush.rs、wedb/wkv/src/vdb/routing.rs、wedb/wnode/src/resp/basic_commands/mod.rs）：
秒级虚拟 ID 换号。
单次 O(1) 原子替换当前库 virtual_db_id 槽位单元格。
新库瞬时清空。
旧 ID 压入 GC 队列。
内存换号段耗时小于 1 微秒（DbMeta 同步落盘批不在此承诺内）。

SWAPDB（wedb/wkv/src/session/swap.rs）：
集群拓扑门禁：
单机模式直接允许执行。
集群模式校验两库所属槽位是否均由当前本地节点掌管，且槽位状态均为 Stable。
若分属不同物理节点（或本节点不持有任一所属槽位，或任一所属槽位处于
MIGRATING / IMPORTING 迁移窗口），直接拦截报错
（RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE：ERR SWAPDB databases are not served by this node）。
迁移窗口拦截的必要性：换号只改 logic_db → virtual_db_id 指针、物理数据原地，
MIGRATING 槽的 eff 属主恒为本地，仅判属主会放行源端换库，使在途搬迁键集与
另一库归属对调，破坏换号与迁移的互斥。
原子互换两库虚拟 ID 映射：
基于 per-logic_db 槽位级单元格路由表，两格各自单指令 ArcSwap 原子换指。
零物理数据搬移、零整表克隆。
同步原子落盘。
瞬时完成逻辑互换。

KEYS / SCAN / DBSIZE（wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs）：
调用 strip_session_prefix 剥离前缀。
高效过滤当前 (ns, db) 键。
向量登记表域（deviations.md 第 75 条：VectorManager::key_index_registry，不驻 wkv 值域）经
merge_vector_keys 投影并入键面：SCAN 首页按 COUNT 页上界剩余额度补投、满额截断不加码，
该域不承载游标位段（续扫页不复现，非第二套游标编码，票 zcode-r147c-hscanmt 案二口径）；
KEYS/DBSIZE 恒全量投影；逐键分配走 try_push_key 平滑单源，失败折单会话存储错误帧。

FLUSHALL（wedb/wnode/src/resp/basic_commands/mod.rs、wedb/wkv/src/vdb/flush.rs）：
多租户秒级清库。
单次 O(1) 原子替换当前租户 virtual_ns_id。
当前租户下所有逻辑库瞬间全部失效。
零逐库写放大。
旧 virtual_ns_id 整体压入 GC 队列。
命名空间 0 超管支持执行底层物理截断。

### 1.4 虚拟空间与虚拟库映射与延时回收机制

代码路径：wedb/wkv/src/vdb/（目录模块：routing.rs 路由表、meta_record.rs DbMeta 记录编解码、
gc_dead.rs 死亡号账本、manager.rs 管理器、flush.rs 清库换号）、wedb/wkv/src/gc/（目录模块：内置 GC 驱动与换号清扫）

架构原理：
双层虚拟化解耦：
用户操作的租户为 logic_ns: u64，库号为 logic_db: u64。
底层物理记录键前缀存储映射后的 virtual_ns_id 与 virtual_db_id。

内存槽位级单元格路由表与无锁读：
空间映射：logic_ns -> AtomicU64(virtual_ns_id)
库级路由：virtual_ns_id -> Arc<TenantRouting>（papaya 并发字典承载）。
TenantRouting 内含 DbRoutingTable 槽位级路由表：papaya 并发字典承载
logic_db -> 单元格（Arc<ArcSwap<u64>>），每格独立原子承载一个 virtual_db_id。
读路径经表内单点访问器点查逻辑库单元格并 load() 取指向，零锁零争用（< 1ns），
禁散点直取。
修改与互换（FLUSHDB / SWAPDB）：
FLUSHDB 对目标逻辑库单元格单指令 swap 原子换指新号并摘回旧号。
SWAPDB 双格各自单指令原子换指完成互换（换号串行锁内成对执行，互换中
每库只指向自身换号前/后两个合法域之一；盘上双指向撕裂中间态由 0x06
成对记录单条原子杜绝）。
换号成本恒定 O(1)，与租户在册库数无关，零整表克隆、零 CAS 重试。
初次访问与清库后回源：
字典槽位经 papaya get_or_insert 无锁抢占，并发首访仅一者胜出插入。
映射落盘固定根域前缀 (ns 0, db 0)，与重建扫描、点查装载、GC 墓碑
删除同一物理布局，杜绝前缀漂移导致点查失准。
并发落盘由会话各自同步快路径成批闭环（一次翻页后整批降级异步回放），
跨表换号（FLUSHDB / FLUSHNS / SWAPDB）全程由换号串行锁互斥，
防并发改号时旧水位覆盖新映射。
读路径零分配零拷贝；换号写面仅目标单元格一次定宽 Arc 分配（建格时
一次单元格堆分配），杜绝按库数放大的整表拷贝。

秒级清库与清空间：
FLUSHDB：分配全新自增 virtual_db_id，单次 O(1) 原子换指目标逻辑库单元格，新库瞬间为空。
FLUSHALL：分配全新自增 virtual_ns_id，单次 O(1) 原子更新空间映射，租户全库瞬间为空。
内存换号段耗时均小于 1 微秒（DbMeta 同步落盘批不在此承诺内）。

即时原子提交：
系统专属标签：KeyTag::DbMeta (0x0E)。
底层持久化存储布局：
当前空间映射：[ns: 0][db: 0][DbMeta][0x01][ns: 8B be] -> [virtual_ns_id: 8B be]
当前库级映射：[ns: 0][db: 0][DbMeta][0x02][vns: 8B be][logic_db: 8B be] -> [virtual_db_id: 8B be]
待回收空间项：[ns: 0][db: 0][DbMeta][0x03][expired_at: 8B be][old_vns: 8B be] -> [tail_address: 8B be]
待回收库级项：[ns: 0][db: 0][DbMeta][0x04][expired_at: 8B be][vns: 8B be][old_vdb: 8B be] -> [tail_address: 8B be]
全局自增标量：[ns: 0][db: 0][DbMeta][0x05][b"next_virtual_id"] -> [id: 8B be]
互换成对记录：[ns: 0][db: 0][DbMeta][0x06][vns: 8B be][logic_db1: 8B be][logic_db2: 8B be] -> [db1 新指向: 8B be][db2 新指向: 8B be]
原子批处理：换号落盘按新映射、旧 ID 墓碑登记、自增水位的安全顺序成组为
单批：引擎单条日志追加即原子提交单元，同步快路径逐条追加、一次翻页后
整批降级异步回放；SWAPDB 以 0x06 成对记录先行、两条同值库级映射记录随落。
任意崩溃前缀最坏旧域泄漏，绝不数据复活或撞号。
杜绝延迟持久化。
严防断电状态撕裂与幽灵键。
重启扫描 DbMeta，重构死亡账本、分配水位与根域映射表，无缝恢复清理任务。

写路径过期 ID 防护（写落盘前不逐条查墓碑）：
会话按开启时捕获的路由快照与写前缀代数落盘，换号不追溯改写已开会话；
换号后首次写入按代数-解析不可交换口径重解析最新映射，自愈收敛命令边界。
set_context 仅为首次创建的目标落新映射，同会话重复绑定零落盘。
纪元内慢窗产生的死域孤儿对读写与统计面均不可见：逻辑路由只解析当前
映射，退役域键不经任何逻辑库可达，统计面按死域死空间判定过滤。
纪元屏障跟踪活跃读写会话，物理清退由屏障后排尾水位推进与紧缩死域跳过
承接：后台回收并查过期时刻与保留水位，紧缩死域判定比对墓碑角色与过期，
DbMeta 记录豁免于业务紧缩谓词、仅经自身退役墓碑退出。
残余窗口如实声明：死域孤儿最长滞留一个回收延迟周期，绝不数据复活或撞号。

惰性过滤与紧缩顺带回收：
严禁后台主动全表扫描哈希索引（消除 O(N) 性能风暴）。
读路径不做墓碑点查：逻辑路由只解析当前映射，退役域键不可达即不可见，
无需在点查路径校验墓碑。
紧缩回收（Compaction-Driven GC）：
LogCompactor 顺序扫描物理日志。
遇到属于已注销到期虚拟 ID 的记录，直接物理跳过不写回。
顺手调用 CompareAndRemove 摘除对应哈希索引指针。
零主动扫表，零多余访存开销。
配置参数：
db_gc_reclaim_delay_secs：延时回收等待时长，默认 86400 秒（24 小时）。
到期弹出与空闲析构均随内置 GC 物理回收轮次推进（后台扫描循环在跑时轮次间隔即引擎 gc.scan_interval_ms，循环缺席时由常驻物理回收轮次兜底推进），紧缩判定与之同轮评估、不设独立周期旋钮，库级回收亦不设独立周期参数。
安全纪元双覆盖：换号摘除哈希索引登记的旧域树文件（RI/升阶树数据文件，集合内容唯一持久副本）同受本期限门控——待释放队列条目入队即携到期时刻，消费轮次只释放已到期条目，期限内绝不 unlink，杜绝「删除先于换号批持久化镜像落地」的崩溃回滚窗口。
route_idle_evict_secs：租户路由快照空闲析构期限，默认 300 秒（0 = 引用归零即期）。
高低水位熔断：旧垃圾积压超过高水位阈值时，自动触发加速紧缩，防止磁盘膨胀。

冷租户按需加载与零全局常驻内存：
冷数据全留磁盘：
启动期重建不全量灌入映射：仅装载 ns 标量基线（logic_ns -> vns，
16 字节级）、根域 (0, 0) 完整路由表与近期即将到期的死亡账本；
非根域库级路由表零常驻，仅抬升分配水位防撞号。
首次访问点查底层磁盘 KeyTag::DbMeta 装载既有映射（映射权威在磁盘，
绝不换号），点查未命中才分配新号并原子持久化。
反查面（由物理域求逻辑域）同须先装载：库级定槽以逻辑域现算，而回放面
条目只带物理前缀，故向量族重放定槽前按条目物理租户经 load_routes_of_vns
在协议库号地址空间内逐格点查磁盘回建该租户路由快照（与首访点查同一
0x02 记录内核、同一 insert_db_mapping 写入口，不建第二套装载机制）。
此前置为必需：非根域库级路由表重建时刻意不装载，而早于恢复基线的
DbMeta 镜像条目又被 AOF 版本闸挡在应用面之外，重启 / 检查点基线后回放
即处于「映射权威在磁盘、内存快照为空」形态。回建后仍无格可查即本节点
对该域确无逻辑入口（映射已随退役注销），显式失败上抛留痕，禁静默以
物理号冒充逻辑号定槽——错槽一经登记项盖章即不可自愈，该向量集在按槽
迁移枚举中恒漏发。
连接会话为严格上下文态：切换到未装载库时同步域拒绝盲分配（set_context
返回未物化），由协议层挂起异步点查装载（resolve_context，同步域内严禁
await 磁盘）后重放；全新租户（ns 未在册）直接同步创建，零挂起开销。
内部与统计会话维持纯内存原语语义；重放会话切域亦为纯内存直设，仅上述
定槽反查前经磁盘点查回建路由快照。
空闲自动析构：
连接断开、长期空闲的租户路由快照引用计数归零（会话绑定/解绑维护
每租户引用计数），解绑即登记空闲析构期限（route_idle_evict_secs，
默认 300 秒）。
内置 GC 轮次弹出到期候选，摘除-回插协议保证绑定期间快照绝不缺席
（摘除后引用复归则原样回插），引用归零方真正释放，内存占用归零。
析构绝不变更任何映射，后续访问点查磁盘装载回建。
极小基线与轻量堆：
单库映射仅占用 16 字节纯标量数字（装载后常驻）。
全量历史墓碑留存底层磁盘，内存仅加载近期即将到期的小根堆。
死亡账本按 expired_at 升序小根堆索引，sweep 只弹到期前缀，
扫描成本与到期量成正比，与历史租户数彻底脱钩。
彻底杜绝海量租户导致的内存泄漏与膨胀。

主从物理镜像与异步屏障：
物理日志复制与 Checkpoint 直接镜像主库的 KeyTag::DbMeta 与数据记录。
从库完全继承主库的映射体系，不进行本地二次映射。
GC 时序同步：主库换号（FlushDb/FlushNs 条目）即屏障，不新增独立屏障日志条目。
从库回放线程接收换号条目时，将释放任务投递至从库本地后台 GC 异步队列，不阻塞复制流水线，继续推进位点。
由从库后台线程等待本地 Epoch Guard 排空活跃读后，再执行物理块释放。
严防长读事务卡死主从同步与访问野指针。


## 2. 命名空间架构

### 2.1 体系层级

四层层级：

```
命名空间
  └─ 逻辑数据库
       └─ 类型与旁路标签
            └─ 用户载荷
```

### 2.2 协议层认证格式与会话绑定

登录格式：
`<ns>#用户名`
例如：
0#alice：命名空间为 0，用户名为 alice
1#bob：命名空间为 1，用户名为 bob
无 `#` 字符（如 alice、default）：缺省为会话当前绑定的命名空间（未认证会话即 ns 0）
用户名本身禁止包含 `#`。前缀非法或用户名为空时直接报错拦截。
缺省口径与 3.5 门禁闭环：namespace != 0 的连接严禁携带 `#`，裸名必落当前租户——非 ns0 会话写裸名即指本租户用户，ns0 会话写裸名即指 ns 0；显式 `<ns>#` 前缀仅 ns0 跨租户管理可用。

会话自动绑定：
AUTH 或 HELLO 认证成功后，会话立即执行 store.session.set_context(target_ns, active_db)。
当前连接后续所有命令自动附带 target_ns 物理前缀，协议层正式打通多租户租界隔离。


## 3. 访问控制数据库持久化与零全局内存设计

### 3.1 彻底废弃配置文件

彻底删除从 users.acl 文件读写的逻辑。
删除 --acl-file 启动参数与 acl_configuration_file 配置项。
存储引擎为 ACL 的唯一真实数据源（Source of Truth）。

### 3.2 物理存储布局

代码路径：wedb/wval/src/tag.rs (KeyTag::Acl = 0x0D)

ACL 用户规则以物理键形式保存在底层存储：

```
物理键: [NsVarint] + [DbVarint: 0] + [KeyTag::Acl: 0x0D] + [username]
值内容: 紧凑编码的用户规则（密码哈希、命令分类与白名单位图）
```

不同 Namespace 的 ACL 用户天然物理隔离，不会发生越界穿透。

### 3.3 零全局内存与连接本地生命周期

消除常驻大字典：
全局不维护 ConcurrentMap 用户大字典，避免数万租户与海量用户吞噬内存。
海量用户数据留存在底层存储冷热分层结构与持久化索引中。

按需点查认证：
客户端 AUTH <ns>#<user> <pass> 时，按 (ns, 0, KeyTag::Acl, user) 点查哈希索引。
密码校验通过后，在当前连接生成一份轻量只读权限句柄 Arc<UserHandle>。

连接本地持有（Connection-Local）：
每条命令鉴权仅读取连接本地持有句柄的位图，耗时数纳秒，零全局锁、零查表、零分配。
连接断开时，句柄随连接析构自动释放。
全服 ACL 内存开销严格与当前在线连接数挂钩，与注册用户总数完全脱钩。

### 3.4 ACL 管理命令直通存储

ACL SETUSER <ns>#<user> ...：直接写底层存储 KeyTag::Acl 记录。
ACL DELUSER <ns>#<user>：直接向底层存储写入墓碑删除。
ACL GETUSER <ns>#<user>：点查底层存储并反序列化输出。
ACL LIST / ACL USERS：单遍流式扫描当前 Namespace 的 KeyTag::Acl 记录并就地收成小快照——扫描所见的用户名（USERS）或逐条解码后渲染的规则正文（LIST）先入快照，再由同一快照写出应答数组长度与元素，数组头与元素数同源强一致：扫描起讫区间取调用时刻的日志尾，起扫后本命名空间的并发 SETUSER/DELUSER 无从令符头与条数背离（ACL 用户量级小，快照只是整份应答的提前驻留，应答正文本就全量缓冲于会话输出缓冲，与零大字典设计不冲突）。不可解码的记录在写数组头之前失败关闭，只回一条错误帧、不留半截数组框。
ACL SAVE / ACL LOAD：由于数据即时持久化，直接返回 +OK。

### 3.5 零号命名空间超管权限门禁

处于 namespace == 0 的连接具有跨租户管理权限，允许在命令中指定 <ns># 执行跨空间 ACL 管理。
处于 namespace != 0 的普通连接严禁携带 `#`，仅允许管理所属本地 Namespace 的用户。
同理，多租户集群总线收令帧（如 CLUSTER FLUSHALL_NS 跨主节点换号清库）属超管管理面，
仅节点间连接（gossip 建链确立的在册对端节点）或 namespace == 0 的会话可发起，
其余已认证租户会话一律按权限错误拒绝。


## 4. 集群模式与数据库亲和性架构

### 4.1 彻底废除键哈希分片

废除 Key Hash：
完全移除针对单个用户 Key 的哈希计算（CRC16 与 Hash Tag 彻底废弃）。
集群不再按键寻址分片，彻底杜绝单库内部键被打散到不同机器的缺陷。

### 4.2 数据库级分片原则

分片基准：
集群仅以 namespace -> db 的数据库实体为最小也是唯一的拓扑分片单元。

槽位映射规则：
同一个 DB 是同一个槽位：给定 (namespace, db)，其集群槽位恒定唯一，库内所有数据 100% 收敛在同一台物理机器。
同一个 Namespace 的不同 DB 可以是不同槽位：例如 (ns=1, db=0) 与 (ns=1, db=1) 哈希后映射到不同槽位，由不同集群节点承载，实现多库分布式并发与负载均衡。

### 4.3 槽位计算机制

设计不变量：给定 (namespace, db)，槽位唯一确定；确定性映射函数已随 rust 执行域
门禁统一收口落地（wbase::hash_slot::slot_of 单点，执行域门禁见
cluster_manager_slot_gate；C# HashSlotUtils.cs 的键级 CRC16 槽与 CROSSSLOT
裁决已随库级分片一并废除，见本仓 4.4）。

槽位计算算法（设计口径）：
原生 64 位整数寄存器哈希混合器（Integer XOR-Shift Mixer，结合 Knuth 黄金分割常数、Murmur 质数与 Stafford Mix13）：
Slot = Mixer(namespace, db) & (CLUSTER_SLOT_COUNT - 1)  // 单周期位与：& 0x3FFF

性能与设计优势：
零序列化：直接输入标量数字，废除 LEB128 变长字节编码。
零查表与访存：全寄存器运算，彻底消除 CRC16 查表带来的 Cacheline Miss。
指令级并行：ns 与 db 双路乘法超标量并行发射，纯寄存器无分支流水线，耗时小于 2 纳秒。
完美雪崩效应：连续自增的数据库 ID 在 16384 个槽位中均匀离散分布。

执行时机：
会话执行任何数据命令或事务时，直接依据会话绑定的 (session.namespace, session.active_db) 取得槽位。
无需解析命令中的任何键，零参数解析开销，零内存分配。

### 4.4 架构收益与特性保障

彻底消除跨槽报错：
同库内部的多键命令（MGET / MSET / DEL）、MULTI/EXEC 事务与 Lua 脚本天然在同一节点执行，彻底根除 CROSSSLOT 错误。

高效本地运维：
FLUSHDB 单节点流式清空，零跨网络 RPC。
KEYS / SCAN / DBSIZE 单节点完成。
集群扩缩容再平衡时以整库 (namespace, db) 为粒度迁移，杜绝孤儿键碎片。

### 4.5 数据库切换与集群广播协议设计

会话切库（SELECT）：
连接本地纯标量切换。
会话更新 active_db，无需全网广播。
后续读写命令按会话绑定的 (namespace, db) 定位槽位归属。
若槽位归属本地节点，直接执行。
若槽位归属远程节点，响应 -MOVED <slot> <ip:port>，驱动智能客户端向目标节点建连。

单库秒清（FLUSHDB）：
客户端直接路由至掌管该槽位的主节点。
若发往非掌管节点，返回 -MOVED 重定向。
主节点秒级更新虚拟库 ID，通过物理日志复制流向从库同步 KeyTag::DbMeta 换号批次。

全租户秒清（FLUSHALL）集群广播协议：
租户的多库打散于集群不同主节点。
客户端连接主节点发起 FLUSHALL 时，该节点作为协调者。
协调者本地更新 virtual_ns_id。
集群总线广播（Cluster Bus Broadcast）：
协调者经集群 gossip 连接向全网其余活跃主节点并行扇出换号命令帧（不存在独立
控制帧结构体，即 wresp 的 RespCommand::ClusterFlushallNs）：
CLUSTER FLUSHALL_NS <ns> <origin 32hex> <epoch>
origin 为协调者 u128 节点 ID 的 32 位十六进制文本，epoch 为其 config_epoch。
各主节点收帧先过同步守卫（拒 ns 0、拒 origin 回声、拒非 Primary、拒 origin
不可知或 epoch 陈旧），通过即在本地原子替换该租户 virtual_ns_id 并入队待回收，
回 +OK ack；副本不经总线，经 AOF FlushNs 回放条目收敛。
全网租户多库分布式微秒级清空。
协调者收齐全部 +OK ack 后向客户端响应 +OK，任一节点失败或超时即报错上抛。

拓扑同步与槽位确定性：
槽位分配由 (namespace, db) 的确定性映射唯一确定。
集群节点间通过 Gossip 协议定期同步 16384 槽位归属（Slots 到 Nodes 映射）。
无需为单个数据库广播路由元数据，零广播风暴。