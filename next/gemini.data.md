# 数据类型、Redis 命令规范与 TTL 深度审查待办清单

1. [P0] TTL 旁路键架构导致命中 TTL 标签全量击穿内存直读快路径并强制降级异步
   位置：wedb/wkv/src/session/raw/read.rs:204-207（StoreSession::try_read_tag_sync_unprotected）；wedb/wkv/src/ttl.rs:115（StoreSession::has_ttl_tag_unprotected）
   对标：garnet/libs/server/Storage/Functions/MainStore/ReadMethods.cs:37-38（SingleReader、ConcurrentReader）；garnet/libs/server/Storage/Functions/LogRecordUtils.cs:18-20（LogRecordUtils.CheckExpiry）；garnet/libs/server/Storage/Functions/MainStore/PrivateMethods.cs:64-70
   C# 实现：Garnet 将 8 字节绝对过期时间戳（.NET Ticks）以内联形式存储在每个物理日志记录的 DataHeader / RecordInfo 头部中，通过 srcLogRecord.DataHeader.HasExpiration 与 srcLogRecord.Expiration 访问。同步读路径在 SingleReader / ConcurrentReader 中直接在同栈内联比对 CheckExpiry(in srcLogRecord)。若未过期，零开销直接返回记录数据切片（命中 OperationStatus.SUCCESS），完全在快路径完成；若已过期，原地标记 GarnetStatus.NOTFOUND 与 StatusCode.Expired，直接向客户端输出 nil / $-1。
   问题：wedb 将 TTL 剥离为独立的旁路物理键（KeyTag::Ttl）。在 try_read_tag_sync_unprotected（以及外层的 try_read_sync_unprotected、try_read_tag_sync_with_size）中，硬编码了 if self.has_ttl_tag_unprotected(user_key)? { return Ok(None); }。注释声称“同步内存路径无法执行异步过期物理删除，降级异步读裁决”。这意味着系统中任何被设置了 TTL 的键（哪怕过期时间在 1 小时或 10 年后），在执行同步读取时，100% 必然被 has_ttl_tag_unprotected 拦截，直接返回 Ok(None) 并强制击穿内存直读快路径，退化为异步调度、运行时任务派发与线程上下文切换，造成极大的读延迟增加与 CPU 资源浪费。
   方案：
   API 签名：
   pub fn try_read_tag_sync_unprotected<R>(&self, user_key: &[u8], tag: KeyTag, f: impl FnOnce(&[u8]) -> R) -> Result<Option<Option<R>>>
   算法流程：
   1. 探测 TTL 键时，不再仅做布尔型 has_ttl 检查，而是同栈调用同步原语 try_read_tag_in_memory_unprotected 读取该键对应的 KeyTag::Ttl 记录值（8 字节大端 i64 ticks）。
   2. 若 TTL 记录在内存中命中：比对 exp 与 now_ticks()。若 exp >= now_ticks()，判定为存活且未过期，立即放行并继续在内存中直接零拷贝读取该数据标签（KeyTag::String 或 KeyTag::ObjectEnvelope），闭包消费数据并返回 Ok(Some(Some(result)))，完整保全内存直读快路径。
   3. 若 exp < now_ticks()：判定键已在内存中物理过期。直接返回 Ok(Some(None))（对标 NOTFOUND），快路径直接回复客户端 nil / $-1，严禁触发异步降级；被动物理清理留给后续写路径或由后台纪元回收线程异步批量清理。
   4. 仅当 TTL 旁路键或数据键确实处于冷数据区（返回 None）时，才返回 Ok(None) 降级异步慢路径。

2. [P0] 读命中已过期键时强制降级走异步慢路径而非快路径直接返回 NOTFOUND
   位置：wedb/wnode/src/storage/session/common/ttl_sync.rs:128-130,144-150（ttl_gate_sync、read_adjudicated_tag_sync）；wedb/wnode/src/resp/key_admin_commands.rs:742,809（probe_alive）
   对标：garnet/libs/server/Storage/Functions/MainStore/ReadMethods.cs:37-38（SingleReader）；garnet/libs/server/Storage/Functions/ObjectStore/ReadMethods.cs:34-36
   C# 实现：Garnet 在内存读操作中发现 LogRecordUtils.CheckExpiry(in srcLogRecord) 为 true 时，直接将返回状态置为 GarnetStatus.NOTFOUND（并自增 notfound 计数器），快路径立即向 RESP 缓冲区写入 RESP_ERR_GENERIC_NOSUCHKEY 或 WriteNull()，绝不发起异步慢路径转接。
   问题：wnode 的 ttl_gate_sync 在检测到 exp < now_ticks() 时返回 false，导致 read_adjudicated_tag_sync 抛弃已读到的内存命中数据并返回 Ok(None)。同时 probe_alive 也在 exp < now_ticks() 时返回 Ok(None)。这导致对已过期键的所有读命令（GET、HGET、TTL、EXISTS 等）和条件写命令（SET NX/XX、RESTORE）强行将请求抛给异步慢路径引擎，在异步队列排队并唤醒后台任务后再次做过期判定才返回 NOTFOUND，凭空增加系统排队延迟与 CPU 线程切换开销。
   方案：
   数据结构：
   pub enum AdjudicatedStatus<T> { Hit(T), NotFound, Degrade }
   pub enum TtlStatus { Alive, Expired, Degrade }
   API 签名：
   fn ttl_gate_sync<D: Device>(session: &BatchStoreSession<'_, D>, key: &[u8]) -> Result<TtlStatus>
   算法流程：
   1. 当 ttl_status 为 TtlStatus::Alive 时，执行读取闭包返回 AdjudicatedStatus::Hit(val)。
   2. 当 ttl_status 为 TtlStatus::Expired 时，直接返回 AdjudicatedStatus::NotFound，外部调用方直接向 output 写入 null 或 0（如 TTL 返回 -2，EXISTS 返回 0，GET 返回 null），无需降级。
   3. 仅当 ttl_status 为 TtlStatus::Degrade（磁盘候选或页缺失）时才返回 AdjudicatedStatus::Degrade。

3. [P0] HLL 磁盘冷数据与 TTL 待裁决状态在 load_hll 中被错误吞没视为空键导致数据盲插覆盖
   位置：wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:38-40（RespServerSession::load_hll）
   对标：garnet/libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:25-80（HyperLogLogAdd）；garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:70-110（GET_Slow）
   C# 实现：Garnet 处理 HyperLogLog 命令（如 PFADD）时，先通过快路径读取记录；若返回 GarnetStatus.NOTFOUND 则确认为全新键；若状态为 GarnetStatus.PENDING（存在磁盘候选页）或存储层等待，Garnet 会挂起当前协程，调用 CompletePending 或 GET_Slow 从持久化日志层加载冷数据页并重构记录。绝不允许在冷数据未确认的情况下盲目初始化空 HLL。
   问题：load_hll 函数在调用 store.try_read_tag_sync(key, KeyTag::String, ...) 时，当返回 Ok(None)（表示该键在磁盘冷数据区有候选页或 TTL 处于待裁决状态）时，代码竟然与键不存在的分支合并，直接当作 None 返回，随后上层逻辑将其判定为“新键”，初始化一个全零的 12KB 空 HLL 结构。接着 PFADD 执行 try_upsert_sync 将空头加新元素直接盲插覆盖写入存储，导致磁盘上的历史几万、几十万基数记录被瞬间覆写毁灭；PFCOUNT 和 PFMERGE 在冷数据场景下也会因读不到冷数据而返回失真的 0 或错误并集。
   方案：
   数据结构：
   pub(crate) enum HllLoad { Degrade, Missing, Present(HyperLogLog) }
   API 签名：
   pub(crate) fn load_hll_sync<D: Device>(session: &BatchStoreSession<'_, D>, key: &[u8], output: &mut Vec<u8>) -> HllLoad
   算法流程：
   1. 调用 read_adjudicated_tag_sync 读取 KeyTag::String。
   2. 若返回 Ok(None)（磁盘候选），load_hll_sync 返回 HllLoad::Degrade，hyper_log_log_add 同步快路径立即返回 Ok(false)，通知上层框架挂起当前会话并切入慢路径异步调度。
   3. 在慢路径中使用 read_raw_with 或 store_session.read 异步完成磁盘 IO 加载并校验 TTL。
   4. 只有在存储层明确返回 Ok(Some(None))（物理确认不存在）时，才创建全新 HyperLogLog。

4. [P0] MSET 与 MSETNX 缺少全局原子事务保证且单键失败引发部分写入状态撕裂
   位置：wedb/wnode/src/resp/array_commands.rs:207-216,236-257（RespServerSession::network_mset、network_msetnx）
   对标：garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:349-399（MSET_Conditional）；garnet/libs/server/Resp/ArrayCommands.cs:57-64,85
   C# 实现：Garnet 在执行 MSETNX 时，通过 MSET_Conditional 进入 txnManager 事务管理器，将涉及的所有键按顺序加上排他锁（SaveKeyEntryToLock(srcKey, LockType.Exclusive)），在原子事务上下文内先校验所有键是否全部不存在；一旦发现任意键存在，立即放弃写入并返回 0；仅当全部不存在时才批量执行 SET 并原子提交。若 MSET 中遇到异构类型键，会提升为跨域事务先 DELETE 再 SET。
   问题：wedb 的 network_msetnx 采用两段式裸循环：前一个循环用 probe_alive 无锁扫描各键是否存在，后一个循环逐个调用 try_upsert_sync 写入。这在并发场景下完全破坏了原子性：如果并发连接在两个循环间隙插入了某键，后半段循环依然会覆盖写入；更严重的是，在后半段循环中，如果第 1 个键写入成功，而第 2 个键因环形缓冲区翻转返回 Ok(Err(_)) 导致返回 Ok(false) 降级慢路径，第 1 个键已经被持久写入了存储！慢路径重试时 probe_alive 发现第 1 个键已存在，直接判定失败并返回 0！最终导致部分键写入成功但命令却返回失败（0），严重破坏 Redis 的全有或全无原子契约。
   方案：
   API 签名：
   pub fn msetnx_transactional<'a, D: Device>(store: &BatchStoreSession<'a, D>, pairs: &[(&[u8], &[u8])]) -> Result<bool>
   算法流程：
   1. 对输入键进行去重并按字典序排序，依次获取各键条带锁，杜绝并发死锁。
   2. 在持有条带锁的保护下，再次校验所有键的探针 probe_alive。
   3. 若任意键存在，释放锁并直接返回 0。
   4. 若均不存在，在同个锁临界区内一次性完成所有键值的写入。若遇到页翻转或内存限制，在事务管理器中回滚已写入的内存键（写墓碑）并整体移交异步慢路径事务执行，彻底保障原子性。

5. [P1] FLUSHDB 与 FLUSHALL 的 ASYNC 选项被静默丢弃且在网络线程同步阻塞执行
   位置：wedb/wnode/src/resp/garnet_api.rs:536（StoreGarnetApi::flush_db）；wedb/wnode/src/resp/basic_commands.rs:446（RespServerSession::network_flushdb）
   对标：garnet/libs/server/Resp/BasicCommands.cs:1913-1915（FlushDb）；garnet/libs/server/Databases/SingleDatabaseManager.cs:120-145（FlushDatabase）；garnet/libs/server/Databases/DatabaseManagerBase.cs:282-310
   C# 实现：Garnet 在解析到 FLUSHDB 或 FLUSHALL 带有 ASYNC 选项时，在 StoreWrapper / DatabaseManagerBase 中将实际的存储截断与集合清空逻辑（如 ShiftBeginAddress 与树文件销毁）封装为异步 Task 丢入后台线程池排队执行，网络会话立刻向客户端发送 +OK\r\n 响应；仅在无 ASYNC（即 SYNC 模式）下才同步等待任务执行完成。
   问题：wnode 中解析出了 FlushOptions { async_flush: true, .. }，但在 StoreGarnetApi::flush_db 入口处直接执行了 let _ = opts.async_flush; 静默丢弃！随后在当前网络会话线程上同步调用 database_manager.flush_database。由于清库涉及扫描键空间、逐键生成墓碑或逐树卸载，会卡住 compio 网络工作线程数秒乃至数分钟，导致该工作线程承载的所有其他客户端连接全部超时断连。
   方案：
   API 签名：
   pub async fn flush_database_async(&self, db_id: usize) -> Result<()>
   pub fn flush_database_background(&self, db_id: usize) -> Result<()>
   算法流程：
   1. 在 network_flushdb 解析到 ASYNC 修饰符时，通过后台工作线程池或异步 runtime spawn 派发异步清库任务。
   2. 网络会话在派发成功后立即调用 output.write_resp_simple_string("OK") 并返回 Ok(true)，实现无阻塞清库。
   3. 若无 ASYNC 修饰符，则当前命令降级到异步执行流中 await flush_database_async(db_id)，等待清库完毕后再写出 +OK。

6. [P1] COMMITAOF 为固定文本无落盘操作导致持久化与复制位点脱节
   位置：wedb/wnode/src/resp/admin_commands.rs:136-138,152,166（RespServerSession::network_commitaof）；wedb/wnode/src/config_owner.rs:32（ConfigReconcile::CommitTask）
   对标：garnet/libs/server/Resp/AdminCommands.cs:703-725（NetworkCOMMITAOF）；garnet/libs/server/StoreWrapper.cs:CommitAOFAsync；garnet/libs/server/AOF/AofHeader.cs:CommitTo
   C# 实现：Garnet 收到 COMMITAOF 命令时，获取当前 AOF 的 safeTailAddress，调用 StoreWrapper.CommitAOFAsync(safeTailAddress)，触发物理 IO 设备将日志缓冲页强制落盘并调用 Flush，更新 flushedUntilAddress，确认落盘成功后才写入 +AOF file committed\r\n。
   问题：network_commitaof 仅对参数和 db_id 做了基础检验，随后注释标注未接入提交，直接无条件硬编码 output.extend_from_slice(b"+AOF file committed\r\n")；而在后台配置调停中，周期提交任务 CommitTask 更是直接被空操作抛弃。客户端收到成功的提交回执，但操作系统崩溃或断电后最新数据在磁盘中完全不存在。
   方案：
   API 签名：
   pub async fn commit_aof_async(&self) -> Result<u64>
   算法流程：
   1. network_commitaof 必须走慢路径异步执行。
   2. 获取当前 aof_writer 的 safe_tail 地址，驱动底层设备的 write_async 与 sync_data_async。
   3. 待提交偏移量安全持久化至存储介质并推进 committed_offset 后，再向客户端回复 +AOF file committed\r\n；若刷盘失败返回 -ERR committing AOF: {err}。

7. [P1] SINTERCARD 命令在提供 LIMIT 时未短路截断且多键装载未判空短路
   位置：wedb/wnode/src/resp/objects/set_commands.rs:122-133,665-670（RespServerSession::set_intersect_cardinality）；wedb/wcol/src/set/set_object.rs:64-77（SetObject::intersect）
   对标：garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:938-978（SetIntersectCardinality）；garnet/libs/server/Objects/Set/SetObject.cs:IntersectCardinality
   C# 实现：Garnet 在执行 SetIntersectCardinality 时：首先获取所有输入集合的头信息，若任意集合不存在或基数为 0，直接短路返回 0；随后按集合基数升序排序，以最小集合作为外层循环源；遍历最小集合元素并在后续集合执行 Contains 判断，同时维护计数器 count，一旦 limit > 0 且 count >= limit，立即 break 跳出循环提前终止，时间复杂度降至 O(min_len * (N-1))。
   问题：wedb 的实现完全无视 LIMIT 传入的阈值，不管三七二十一，先通过 load_many 加载所有集合（哪怕前几个集合为空集也不短路），然后无条件调用 intersect_sets 执行全量集合交集求值（涉及多轮内存克隆与 HashSet 重新构建），生成包含全量交集元素的临时 SetObject，最后在出口处才通过 min(intersection.len(), limit) 做截断，完全丧失了 Redis 引入 SINTERCARD LIMIT 来防止千万级大集合交集计算导致 CPU 跑满的设计初衷。
   方案：
   API 签名：
   pub fn intersect_cardinality(sets: &[&SetObject], limit: usize) -> usize
   算法流程：
   1. 在 set_intersect_cardinality 中，先收集各键引用，若 load_sync 发现任何一个键 Missing 或其 set.is_empty()，立即向 output 写入 :0\r\n 并直接返回 Ok(true)。
   2. 将所有集合引用按 set.len() 从小到大排序。
   3. 获取基数最小的集合 min_set，遍历其元素；对每个元素，依次在剩下的 sets[1..] 中执行 set.contains(item)；若全部包含则 matched_count += 1。
   4. 当 limit > 0 且 matched_count == limit 时，立即提前 return limit。
   5. 整个过程零堆分配，不创建任何临时 HashSet。

8. [P1] 集合与有序集合 *STORE 族命令未清理目标键既有 TTL 与多域物理冲突
   位置：wedb/wnode/src/resp/objects/set_commands.rs:610,816（combine_store）；wedb/wnode/src/resp/objects/sorted_set_commands.rs:1360-1410（zunionstore/zinterstore/zdiffstore）；wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:618
   对标：garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:880-920（SetIntersectStore）；garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:1150-1200（SortedSetUnionStore）
   C# 实现：Garnet 在将计算结果写入 destination 目标键前，会调用 EXPIRE(destination, 0) 或在两套存储域（MainStore 与 ObjectStore）中先执行显式的 DELETE(destination)；确保彻底清除目标键上残存的旧 TTL 时间戳，同时销毁目标键可能存在的异构数据类型（如目标键原先是字符串键），实现 Redis 标准的覆写清 TTL 语义。
   问题：wedb 的 combine_store 与 zset_save_or_gc 在将结果存入目标键时，仅直接调用了 try_upsert_tag_sync(dst, KeyTag::ObjectEnvelope, &val)。这存在两大严重漏洞：第一，如果目标键原先设置过过期时间，其 KeyTag::Ttl 记录未被清除，导致新建的集合莫名其妙地继承了旧的 TTL 并提前失效蒸发；第二，如果目标键原先是一个字符串键（KeyTag::String），该写操作未删除字符串域数据，导致该键在 String 域和 ObjectEnvelope 域同时存在，引发后续 TYPE、EXISTS 及读取命令的域冲突混乱。
   方案：
   API 签名：
   pub fn obj_overwrite_destination<D: Device>(store: &BatchStoreSession<'_, D>, dst_key: &[u8], tag: u8, payload: &[u8]) -> Result<bool>
   算法流程：
   1. 在写入目标键前，先调用 del_ttl_sync 清理 KeyTag::Ttl 旁路记录。
   2. 同时调用 store.try_delete_sync(dst_key) 对目标键进行双域盲删，确保清除 String 域残留墓碑。
   3. 然后再调用 try_upsert_tag_sync 写入新的 ObjectEnvelope 载荷。
   4. 保证目标键作为全新生命周期存在，符合 Redis 标准协议。

9. [P2] SINTER、SUNION、SDIFF 集合算子与结果输出存在多轮全量对象深拷贝
   位置：wedb/wnode/src/resp/objects/set_commands.rs:767-831（RespServerSession::set_intersect_internal、load_many、write_set_members）
   对标：garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:810-860（SetIntersect）；garnet/libs/server/Objects/Set/SetObjectImpl.cs:SetIntersect
   C# 实现：Garnet 在执行集合交并差时，直接借用已处于内存中的 HashSet<byte[]> 的引用迭代器，在写出阶段通过 SpanByte 或直接将借用的 byte[] 写入网络 session 的响应流（dcurr / dend），全程不存在全量对象的 Clone 或重复的中间容器转换。
   问题：load_many 在加载每个集合时，使用了 set_obj.clone() 将每个源键对应的整套 HashSet<Vec<u8>> 全量深拷贝生成 Vec<SetObject>；在 intersect_sets 中，对集合中的每个 byte 数组又执行 item.clone() 拷贝插入新 HashSet；最后在 write_set_members 中，再次调用 result.to_members() 生成第三份 Vec<Vec<u8>>。对一个包含 10 万字符串元素的集合，一次交集计算会造成 3 次全量深拷贝，产生数百兆无谓堆内存分配，极易触发系统 OOM。
   方案：
   API 签名：
   fn load_many_borrowed<'a, D: Device>(store: &'a BatchStoreSession<'a, D>, keys: &[&[u8]], output: &mut Vec<u8>) -> Result<Option<Vec<&'a SetObject>>, ()>
   pub fn intersect_borrowed<'a>(sets: &[&'a SetObject]) -> HashSet<&'a [u8]>
   算法流程：
   1. 存储层直接返回 &SetObject 借用（在 BatchStoreSession 纪元生命周期内受保护）。
   2. 集合运算基于借用切片 &[u8] 进行哈希运算，中间结果容器仅为 HashSet<&'a [u8]>。
   3. 写出函数 write_set_members 直接接收 HashSet<&'a [u8]>，逐个元素直接作为 bulk string 写出到 RESP 缓冲区，彻底消除对象深拷贝。

10. [P2] ZRANGE 默认按排名区间参数误用浮点解析且底层在 BTreeSet 上 O(N) 线性扫描
    位置：wedb/wcol/src/zset/sorted_set_object_impl.rs:564-604（SortedSetObject::range_by_index_internal）；wedb/wnode/src/resp/objects/sorted_set_commands.rs:512-528
    对标：garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:400-470（SortedSetRange）；garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:RangeByIndex
    C# 实现：Garnet 在处理 ZRANGE 默认的按排名区间（ByRank / ByIndex）时，严格使用 parseState.TryGetInt 解析 start 与 stop 参数（禁止小数输入，且支持 i64 大整数）；其跳表结构（SkipList）内嵌了每层的 span 步长索引，支持在 O(log N) 时间内通过累加跨度直接跳转到指定 rank 的节点，随后按需遍历取 count 个元素。
    问题：wedb 在解析 ZRANGE 排名参数时误用了 try_parse_parameter（包含浮点容错），允许了类似 1.5 这种不合法的排名输入，且超过 53 位的超大整数在转为 f64 后会丢失精度；而在存储结构上，SortedSetObject 仅使用了标准库的 BTreeSet<(f64, Vec<u8>)>，缺乏排名索引，计算 offset 时只能使用 set.iter().skip(min_index).take(...) 从头进行单链表式的线性扫描。在包含数百万元素的 ZSET 中查询末尾区间（如 -100 -1），耗时高达数百毫秒，发生严重的 O(N) 性能退化。
    方案：
    数据结构：
    pub struct RankedSkipList { .. }
    impl RankedSkipList {
      pub fn get_node_by_rank(&self, rank: usize) -> Option<(&[u8], f64)>
    }
    算法流程：
    1. 在 sorted_set_commands.rs 中，对默认的 ByRank 分支，强制使用 strict_i64 精确解析整数参数，非法整数字符串直接报错。
    2. 针对负数 rank（如 -1），根据集合长度 len 做标准化换算：if start < 0 { start = len + start }。
    3. 寻址时直接利用跳表的跨度指针在 O(log N) 内跳转至 start 节点，然后迭代输出 count 个元素，消除线性遍历。

11. [P2] ZCARD 纯只读计数命令走 RMW 写路径引发条带写锁与事务争用
    位置：wedb/wnode/src/resp/objects/sorted_set_commands.rs:278-300（RespServerSession::sorted_set_length）
    对标：garnet/libs/server/Resp/Objects/SortedSetCommands.cs:200-220（SortedSetLength）；garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetLength
    C# 实现：Garnet 的 SortedSetLength 走的是只读 API（READ），获取存储对象的共享只读引用后，直接读取 sortedSetDict.Count 并写出 RESP 整数，全程无需申请任何写锁、不走 RMW 状态机，更不会进入 AOF 事务日志流。
    问题：sorted_set_length 内部调用了 self.zset_rmw(key, SortedSetOperation::Zcard, ...)。ZSET 的 RMW 通道是为写操作设计的，会触发一系列复杂的条带排他锁争用、分配事务上下文、检查回写脏标以及生成 AOF 预备条目。这导致高并发的 ZCARD 只读查询与高频的 ZADD 写操作产生严重的锁争用，吞吐量断崖式暴跌。
    方案：
    API 签名：
    pub fn sorted_set_length<'a, D: Device>(&mut self, parse_state: &[&[u8]], store: &BatchStoreSession<'a, D>, output: &mut Vec<u8>) -> Result<bool>
    算法流程：
    1. 参数校验完毕后，调用 zset_load_sync 获取只读借用。
    2. 若为 ZsetLoad::Present(ref obj)，读取 obj.sorted_set_dict.len()，调用 output.write_resp_int(len as i64)。
    3. 若为 ZsetLoad::Missing，调用 output.write_resp_int(0)。
    4. 若为 ZsetLoad::Degrade，返回 Ok(false) 切入异步只读路径，彻底杜绝调用 zset_rmw。

12. [P2] LPOP 与 RPOP 参数数量大于 2 时未返回语法错误而是静默以 count=1 执行
    位置：wedb/wnode/src/resp/objects/list_commands.rs:265-280（RespServerSession::list_pop）
    对标：garnet/libs/server/Resp/Objects/ListCommands.cs:120-140（ListPop）；garnet/libs/server/Resp/RespCommand.cs
    C# 实现：Garnet 在 ListPop 中对参数个数严格断言：if (parseState.Count < 1 || parseState.Count > 2) return AbortWithWrongNumberOfArguments(nameof(RespCommand.LPOP))。若传入多于 2 个参数，立即中断并回复客户端协议语法错误。
    问题：wedb 的 list_pop 在解析 parse_state 时，只用 check_arg_count 校验了长度非空（>= 1）；在提取可选的 count 参数时，代码使用 parse_count 尝试读取下标 1 的参数；若参数数量大于 2，代码并没有返回 wrong number of arguments，而是静默忽略了多余的所有参数，并退化为单元素 pop（count=1）默默执行。客户端传入错误命令（例如把参数拼错的多参数请求），服务端非但不报错，反而直接静默修改了列表数据并返回错误结果，埋下严重的数据不一致隐患。
    方案：
    算法流程：
    1. 入口处直接判断：if parse_state.is_empty() || parse_state.len() > 2 { self.abort_with_wrong_number_of_arguments(cmd_name, output); return Ok(true); }。
    2. 若 parse_state.len() == 2，调用 strict_i32(parse_state[1]) 解析 count；若解析失败或 count < 0，回复 cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER 并返回 Ok(true)。
    3. 严格禁止静默回退到 count=1 执行。

13. [P2] BITOP 在位操作运算前将所有源键全量克隆到内存引发内存峰值放大
    位置：wedb/wnode/src/resp/bitmap/bitmap_commands.rs:395-410（RespServerSession::string_bit_operation）
    对标：garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:310-380（NetworkStringBitOperation）；garnet/libs/server/Storage/Session/MainStore/BitmapOps.cs
    C# 实现：Garnet 在执行 NetworkStringBitOperation 时，通过 PinnedSpanByte 直接借用存储层各源键底层内存的只读指针，按 64 位（ulong）机器字长对齐，以流式向量化 SIMD / ulong 循环直接在输出缓冲区上进行按位 AND/OR/XOR/NOT 运算，中途绝不分配任何存放源键拷贝的中间托管数组。
    问题：wedb 的 string_bit_operation 在搜集源键的值时，循环内部执行了 if let Ok(Some(Some(v))) = ... { sources.push(v.to_vec()); }，将所有源键的数据全部克隆到堆上生成 Vec<Vec<u8>>。对于多个 512MB 的大位图运算，该操作瞬间占用数 GB 的物理内存，不仅触发大量的 malloc/memcpy 开销，更极易引发系统 OOM 崩溃。
    方案：
    API 签名：
    fn bitwise_op_stream(op: BitOp, sources: &[&[u8]], max_len: usize, dest: &mut [u8])
    算法流程：
    1. 在当前 BatchStoreSession 纪元保护下，收集各源键在内存中的只读切片引用 &[u8]。
    2. 仅为目标键分配大小为 max_len 的单一目标缓冲区。
    3. 以 8 字节（u64）为步长，流式从各源切片按位读取并计算出目标值，最后写入目标键，彻底消除源键全量堆克隆。

14. [P2] SETRANGE 负载上限硬编码 512MB 与运行时 proto-max-bulk-len 配置脱节
    位置：wedb/wnode/src/resp/basic_commands.rs:503-506（RespServerSession::network_setrange）
    对标：garnet/libs/server/Resp/BasicCommands.cs:810-825（NetworkSetRange）；garnet/libs/server/ServerConfig.cs:MaxBulkLength
    C# 实现：Garnet 在 NetworkSetRange 中，校验最终字符串长度是否超出限制时，使用的是动态配置项 opts.MaxBulkLength（通过 ServerConfig 维护，默认 512MB，可通过配置文件或参数动态调小或调大）。
    问题：wedb 在 network_setrange 中硬编码写死校验：offset + value.len() > 512 * 1024 * 1024。如果运维通过配置文件将系统最大载荷限制为 64MB 以保护内存，SETRANGE 依然允许写入最高 512MB 的数据，直接绕过系统安全保护边界；反之若系统支持超大载荷配置，该命令又会被非法拦截。
    方案：
    算法流程：
    1. 从 RespServerSession 关联的 ServerOptions / GarnetServerOptions 中读取 max_bulk_len。
    2. 校验逻辑改为：let limit = self.options.max_bulk_len.unwrap_or(512 * 1024 * 1024); if offset.saturating_add(value.len()) > limit { cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_STRING_EXCEEDS_MAX_SIZE); return Ok(true); }。

15. [P2] CONFIG SET 内存与索引扩容合法参数无条件报错降级为伪实现
    位置：wedb/wnode/src/resp/config_commands.rs:310-350（RespServerSession::network_config_set）
    对标：garnet/libs/server/ServerConfig.cs:1120-1180（HandleMemorySizeChange、HandleIndexSizeChangeAsync）；garnet/libs/server/Storage/SizeTracker/CacheSizeTracker.cs
    C# 实现：Garnet 在收到 CONFIG SET memory 或 readcache-memory 时，调用 HandleMemorySizeChange 动态更新 CacheSizeTracker 的 targetSize，并在存储层动态调整 HybridLog 与 ReadCache 的淘汰高低水位；收到 index 扩容时调用 HandleIndexSizeChangeAsync 动态触发哈希表异步扩容与溢出桶分裂。
    问题：wedb 的 network_config_set 在匹配到 memory、readcache-memory、index 等核心运行时调优参数时，直接无条件匹配通配错误分支返回 ERR failed to set configuration parameter，将 Garnet 原本具备的运行时热调优能力彻底降级为不支持的假实现。
    方案：
    API 签名：
    pub fn update_memory_limit(&self, target_bytes: usize) -> Result<()>
    pub async fn grow_index(&self, new_size_power: u8) -> Result<()>
    算法流程：
    1. 在 ConfigManager 与 CacheSizeTracker 中打通动态更新通道。
    2. 在 network_config_set 中接入上述方法，解析合法的内存数值单位（b/k/m/g），更新全局容量参数并安全返回 +OK\r\n。

16. [P2] CONFIG REWRITE 仅返回固定文本未将配置落盘且集群模式未同步配置
    位置：wedb/wnode/src/resp/config_commands.rs:178-192（RespServerSession::network_config_rewrite）
    对标：garnet/libs/server/ServerConfig.cs:650-690（NetworkCONFIG_REWRITE）；garnet/libs/server/ServerConfig.cs:WriteConfigFile
    C# 实现：Garnet 在 NetworkCONFIG_REWRITE 中，会读取当前服务器运行时的所有最新配置项，以原子方式重写回原启动配置文件（通过临时文件写完后原子替换），如果处于集群模式同时调用 clusterProvider.FlushConfig() 将集群配置落盘，成功后才返回 +OK。
    问题：wedb 的 network_config_rewrite 函数在校验参数个数为 0 后，直接向输出写入 output.extend_from_slice(b"+OK\r\n")，没有任何文件序列化与磁盘刷盘逻辑，集群配置也未刷新，为纯粹的空壳假实现。
    方案：
    API 签名：
    pub fn rewrite_config_file(&self) -> Result<()>
    算法流程：
    1. 在 ConfigManager 增加原子重写方法。
    2. 获取当前全局配置快照，格式化为标准 conf 文本，利用 tempfile 原子替换原配置文件。
    3. 在集群模式下驱动 cluster_manager 持久化节点状态，出错时返回明确的 IO 错误信息。

17. [P2] EXPDELSCAN 主动过期扫描在未开启时跳过检查但在配置开启时缺少自愈调度
    位置：wedb/wnode/src/resp/admin_commands.rs:551-553（RespServerSession::network_expdelscan）
    对标：garnet/libs/server/Resp/AdminCommands.cs:1016-1020；garnet/libs/server/Storage/Session/MainStore/ExpDelScanTask.cs
    C# 实现：Garnet 支持 EXPDELSCAN 命令手动触发或驱动后台主动过期键清理任务（ExpDelScanTask），遍历哈希表桶检查过期时间并回收已过期键。
    问题：wedb 注释声称后台扫描恒未启用跳过检查，代码内部没有接入真实的主动扫描状态机；当用户配置开启过期扫描时，不仅无法通过该命令进行主动清理，后台也缺乏自愈重试调度。
    方案：
    算法流程：
    1. 对标 Garnet 实现 ExpDelScanTask 周期迭代器。
    2. 通过 HashBucket 游标分批扫描主存与对象存，遇过期键批量写入墓碑回收。

18. [P2] DEBUG FLUSHANDEVICT 与 FORCEGC 伪实现与 HELP 文案残留欺骗用户
    位置：wedb/wnode/src/resp/admin_commands.rs:377-414（RespServerSession::network_debug）
    对标：garnet/libs/server/Resp/AdminCommands.cs:772-810（NetworkDEBUG）
    C# 实现：Garnet 的 DEBUG FLUSHANDEVICT 调用存储引擎将内存脏页强制刷盘并驱逐到磁盘冷区；DEBUG FORCEGC 触发 CLR 全量垃圾回收与内存整理。
    问题：DEBUG FLUSHANDEVICT 直接硬编码返回 ERR Unable to flush and evict；DEBUG FORCEGC 直接返回 +OK 但实际上什么都没做（Rust 下未调用 jemalloc 的 epoch 清理或 malloc_trim）；且 DEBUG HELP 中列出该命令，给用户造成实现完备的假象。
    方案：
    算法流程：
    1. FLUSHANDEVICT 接入 HybridLog::shift_read_cache_to_disk 与 CacheSizeTracker 强制驱逐。
    2. FORCEGC 接入全局 allocator 的 purge/trim 接口释放空闲物理内存。
    3. 对于无法完全模拟的命令在 HELP 中做准确文案说明或明确标注不支持。
