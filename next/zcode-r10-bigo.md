轮10 大对象视角:单命令时间/空间复杂度界(zcode-r10-bigo)

方法
对「元素数 n 或值长 L 可达十万/百 MB 量级」的命令逐族推演 rust 实现的时间/空间复杂度,与 C# 对位。与 r9-load 的区别:r9 立并发闩持时长(大对象闩内全量序列化的负载形态),本轮只立单命令自身的复杂度阶与额外超线性步骤。已立不复述:r9-load 场景3.1 信封态整对象闩持路径、r3-perf 静态拷贝、r4-objimpl 算法对标、r6-scan 迭代器、collection.md §8 分层 zset 范围/排名/写族的 O(N) 折损登记、r3-perf 的 KEYS/DBSIZE 全量物化(在途票 task/ing/dbsize-keys-single-pass-scan.md)。

问题1 树态 hash/set/list 三族穿透臂:删除/随机/扫描/聚合/成员TTL 每命令 O(整对象) 五次全量转换,AOF 侧整树重发,文档仅登记 zset 面

命令面
HDEL / HEXPIRE / HTTL / HPERSIST / HSCAN / HRANDFIELD / HCOLLECT;
SREM / SPOP / SSCAN / SMOVE / SUNION / SUNIONSTORE / SDIFF / SDIFFSTORE / SINTER / SINTERSTORE / SRANDMEMBER;
LPOP / RPOP / LREM / LTRIM / LINSERT / LSET;
zset 侧同型(ZPOPMIN/ZPOPMAX/ZREM/ZREMRANGEBY* /ZRANDMEMBER/GEOADD/ZRANGESTORE/成员 TTL 族/ZUNION/ZINTER/ZDIFF)已被 collection.md §8.2 登记,本条不重复计。

rust 复杂度(升阶态,N = 成员数)
单命令五段全量:1 树全扫(wbftree 页级)→ 2 bitcode 编码 blob(tiered_materialize_blob_sealed)→ 3 bitcode 解码建 wcol 对象(from_blob)→ 4 对象层操作本体(HDEL 单字段本步仅 O(1))→ 5 export_entries + bulk_load 重建新树(promote_collection_to_bftree,先建后拆原子换入)。时间 O(N) 扫 + O(N) 编解码 + O(N log 页) 建树;空间峰值 O(N) 对象 + 新旧双树并存窗口;AOF 侧重灌走 RangeIndexStreamChunk 整树字节流(service.rs on_aof_store_event 的 StoreEvent::RangeIndexStream 臂),单条 HDEL 在 AOF 中产生 O(整树字节) 记录量。
连续删除放大:业务循环 HDEL k 个字段 = k 次 O(N) 全量往返,总 O(k·N log 页);LPOP/RPOP 队列消费场景(每弹一个)同型,k 次消费总 O(k·N log 页)。降至死区(N≤32768 且体积≤2MB)前每条删除命令都全额付费。

C# 复杂度
HDEL/SREM/ZREM/LPOP 等就地改内存对象 O(字段)(garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:119 Unsafe.As<IGarnetObject>(logRecord.ValueObject).Operate,活对象常驻零反序列化);AOF 镜像 ObjectStoreRMW 仅 O(命令参数)。数量级差:O(1) vs O(N log 页),且 AOF 记录量 O(参数) vs O(整树字节)。

证据
- 穿透清单:wedb/wnode/src/resp/objects/tiered_collection_ops/hash.rs:621-627(_ => Ok(false) 臂)、set.rs:263-272、list.rs:306 与 list.rs 模块头注「LPOP/RPOP 与中段删改均无树内臂,一律经 tiered_materialize_blob 物化后整树重灌」、zset.rs:614-621。
- 五段链:wedb/wnode/src/resp/objects/rmw_helpers.rs run_async_rmw 物化降级通道(198-212 行封窗装载)→ apply_rmw_post_operate(rmw_helpers.rs:363)第 394-397 行 `tiered && !obj.should_demote()` 臂恒走 promote_to_bftree;wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs expire_sweep_or_rebuild 头注确认换入原语同一。
- AOF 整树重发:wedb/wnode/src/service.rs:343-380 replicate_range_index_stream 分块流;collection.md §2 表:AOF 记录行自认「升阶/重灌树数据走 RangeIndexStreamChunk」。
- 登记缺口:doc/zh/collection.md 第 8 节标题与 8.1/8.2/8.3 全部仅覆盖「分层有序集合」;hash/set/list 三族的穿透代价(含 AOF 整树重发)无任何复杂度声明;tiered_collection_ops/mod.rs 头注只写「一律穿透」机制,未写代价。

为何不算已立
r9-load 场景3.2 判定「切换后稳态走树内原生臂 O(字段)」只对覆盖面内命令(HSET 族/点查族/zset 范围族)成立;删除族是生产高频面,升阶后反而比信封态(r9 场景3.1,单 blob 写回 + AOF O(参数))多出建树 log 因子、双树峰值与 AOF 整树重发放大——升阶对删除重负载是负收益,该定性两处均未登记。collection.md §8.2 的 O(N) 物化 + O(N log N) 重建 + AOF 全量重发字样仅存在于 zset 节。

触发门槛
N = 10 万字段的树态 hash(约几 MB 树体积),HDEL 单字段:五段全量在数十 ms 级,AOF 追加整树字节流(等于把整个 hash 重写一遍持久化);每秒 100 次 HDEL 的热键 = 每秒整树重灌百次 + AOF 放大 GB 级。C# 同负载每 HDEL 亚微秒。LPOP 高频消费 10 万元素列表:每弹一次整树重建。

问题2 AOF 单帧静态容量门已被装配校验闭合,无增量(排查记录)

排查链:waof/src/wal/pipeline.rs reserve_address 第 211 行 `required_end - start_aligned > buf_cap` 为单帧硬门(单帧扇区圆整后超过环形窗即永久 BufferFull);但 wedb/wnode/src/aof/aof_settings.rs from_options 校验一(aof-memory ≥ 2×aof-page)与校验三(aof-page ≥ 2×主存页)已把「单帧 > 窗」的组合在装配期拒启,且 whlog 单记录上限 = 页容量(whlog/src/hlog/mod.rs:549-570 validate_append_args),注释明言 C# TryAllocate "Entry does not fit on page" 硬抛两侧行为一致。默认配置(主存页 16MiB、aof-page ≥ 32MiB、aof-memory ≥ 2×aof-page)下大值 SET 的 AOF 帧恒可入窗。非问题,登记排查结论。

已核实双侧同级面(无增量确认)

1. HGETALL/HKEYS/HVALS:对象层单遍迭代直写应答,O(n) 零中间集合(wedb/wcol/src/hash/hash_object_impl.rs hash_get_all 对位 garnet/libs/server/Objects/Hash/HashObjectImpl.cs:61 HashGetAll 的 foreach 直写)。
2. SMEMBERS:单遍直写 O(n)(wedb/wcol/src/set/set_object_impl.rs set_members 对位 SetObjectImpl.cs:SetMembers)。
3. ZRANGE 0 -1(byIndex):skip/take 迭代直出 O(start+count);rust 借用收集 Vec<(f64,&[u8])> 每元素 24B 指针级,与 RESP 输出帧同阶,无数量级差(wedb/wcol/src/zset/sorted_set_object_impl.rs sorted_set_range 对位 SortedSetObjectImpl.cs:522 iterator.Skip(minIndex).Take(n))。
4. LRANGE 全量与中段:iter().skip(start).take(count) O(start+count),C# 同型(wedb/wcol/src/list/list_object_impl.rs list_range 对位 ListObjectImpl.cs:ListRange)。
5. GET 大值:借用闭包直写应答零拷贝,GETRANGE 归一化后切片直写(wedb/wnode/src/resp/basic_commands/get.rs network_get/network_get_range),C# SpanByte 直写同型。
6. HRANDFIELD/SRANDMEMBER/SPOP count/ZRANDMEMBER:下标采样 iter().nth(index) 每下标 O(n) 迭代,总 O(k·n);C# Set.ElementAt/HashObject.ElementAt(LINQ 对 Dictionary/HashSet 同为线性遍历)同阶(wedb/wcol/src/hash/hash_object.rs element_at 对位 garnet/libs/server/Objects/Hash/HashObject.cs:659 ElementAt、SetObjectImpl.cs:138 Set.ElementAt)。pick_k_random_indexes 的 k/n 阈值分派与 C# RandomUtils 同构。
7. LTRIM 中段删除:rust LinkedList::remove(index) O(index)、总 O(k·n);C# list.Remove(node) 按值查找 LinkedList 同 O(n)/次(总同阶 O(n²) 上限双侧一致),C# 还额外 O(n) 快照 List(哈 wedb 侧 doomed Vec<usize> 同阶)(wedb/wcol/src/list/list_object_impl.rs list_trim 对位 ListObjectImpl.cs:ListTrim)。
8. ZRANGE BYSCORE/BYLEX/LIMIT:range 哨兵下界裁剪 + break 上界先收集 O(区间) 后 skip/take 切片,C# GetViewBetween + scoredElements.Skip/Take 同序同阶,含 reverse 先收集再倒序(wedb/wcol/src/zset/sorted_set_object_impl.rs get_elements_in_range_by_score/get_elements_in_range_by_lex 对位 SortedSetObjectImpl.cs:1086/975)。
9. ZREMRANGEBYSCORE/ZREMRANGEBYRANK/ZREMRANGEBYLEX:命中收集 + 逐删两趟,C# 同型(GetElementsInRangeByScore rem 臂 / Skip().Take().ToList()),rust doomed 拷贝与 C# 引用 List 差一常数(wedb/wcol/src/zset/sorted_set_object_impl.rs:845/779 对位 SortedSetObjectImpl.cs:623/656)。
10. ZCOUNT/ZLEXCOUNT/ZRANK/ZREVRANK:range 扫描到界即 break / 计名次线性扫,O(区间)/O(rank),C# 同型。
11. HSET/HMSET/HDEL/ZADD/ZREM/SADD/SREM 批量:每元素均摊 O(1)(双索引/单容器散列直插),无每元素整对象成本,与 C# 同;信封态的整对象 from_blob/to_blob 包裹属 r9-load 已立面不复述。
12. SUNION/SINTER/SDIFF(信封态):首集复制 + retain/extend,O(n1) 或 O(总元素),与 C# SetIntersect(首集复制 + IntersectWith)/SetUnion(UnionWith)逐行同构(wedb/wnode/src/resp/objects/set_commands/write.rs intersect_sets/union_sets/diff_sets 对位 garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:442/612);中间无 2n 峰值。PFMERGE/PFCOUNT:12KB 稠密寄存器逐字节择大折叠,双侧同型(wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs slow_hll_merge 对位 HyperLogLogOps.cs:HyperLogLogMerge)。
13. BITOP:快路径逐源借用折叠峰值 O(最长源)(wedb/wnode/src/resp/bitmap/bitmap_commands.rs:308-315 注释明示不逐源克隆,对位 BitmapOps.cs:70 srcBitmapPtrs 指针收集);慢路径(冷盘降级)逐源 to_vec 瞬时峰值 O(2×最长源) 同阶,时间面多一趟同阶 memcpy 无数量级差(wedb/wnode/src/resp/basic_commands/slow.rs slow_bit_operation)。BITFIELD 单值快照 O(L) 双侧同型。
14. LCS:len 形态 rust 滚动行 O(min(M,N)) 空间优于 C# 完整 DP 表(ComputeLCSLength 调 GetLcsDpTable 全表);IDX 形态双侧同 O(M·N) 表(wedb/wnode/src/storage/session/mainstore/main_store_ops.rs:34/63 对位 MainStoreOps.cs:ComputeLCSLength/GetLcsDpTable)。
15. SORT 命令:C# Garnet 未实现(SORT/BY/GET 修饰面均无),rust 同未实现,无对位面。
16. AOF 帧与回放:字符串/信封大值整帧入 AOF(StoreEvent::Write enqueue_raw 整值),回放单帧峰值 O(帧),双侧同型;升阶/重灌树数据 RangeIndexStreamChunk 分块(O(chunk_size) 内存,回放端 deserializer 累积走临时文件,见 wedb/wnode/src/range_index/range_index_manager_replication.rs process_stream_chunk),无整树内存峰值;帧/页/窗/段容量联动已在装配期三重校验闭合(见问题2)。
17. 树态覆盖面内命令(逃过整对象路径的正面清单):HSET/HMGET/HINCRBY 族、SADD、ZADD/ZINCRBY、LPUSH/RPUSH 树内点查/批量折叠 O(log N)+O(参数);HGETALL/HKEYS/HVALS/SMEMBERS/LRANGE/ZRANGE 族/ZCOUNT/ZRANK 树内流式扫描;扫描族 HSCAN/SSCAN/ZSCAN 树内(exec_tiered_scan)。该覆盖面边界即问题1 的穿透面,两侧清点互为镜像。

视角结论:有增量
