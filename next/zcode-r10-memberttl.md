轮10 成员级 TTL 转写自造面甄别(r10-memberttl)

背景纠偏:本仓 ./garnet 参照版不是上游原版,hash 字段级 TTL 与 zset 成员级 TTL 双双在场,属裁决面(a)「C# 有 → rust 必须对标」,非自造扩展。逐面核查结论如下。

一、C# 对位裁决(三面)

1. hash 字段 TTL:C# 有。
证据:garnet/libs/server/Resp/Parser/RespCommand.cs:55-59(HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT/HPERSIST)、:189-192(HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME);RespCommandHashLookupData.cs:112-120;对象内核 libs/server/Objects/Hash/HashObject.cs(expirationTimes + expirationQueue 双结构)与 HashObjectImpl.cs:HashExpire/HashTimeToLive/HashPersist;AOF 操作码 HashOperation.HEXPIRE=1/HTTL=2/HPERSIST=3(HashObject.cs:28-33)。
HGETEX/HGETDEL(redis 8 族):C# 全仓零命中,rust 亦无,双侧一致缺席,非缺口。

2. zset 成员 TTL:C# 有。
证据:RespCommandHashLookupData.cs:202-210(ZTTL/ZPTTL/ZEXPIRE/ZPEXPIRE/ZEXPIREAT/ZPEXPIREAT/ZEXPIRETIME/ZPEXPIRETIME/ZPERSIST);SortedSetObject.cs:151-161(同款双结构)、SortedSetOperation.ZEXPIRE=24/ZTTL=25/ZPERSIST=26(SortedSetObject.cs:50-52)。

3. set 成员 TTL:C# 无(SetObject.cs 全文无过期结构,命令表无 S 族成员 TTL)。rust set_object.rs 同样无(全文无 expiry/ledger 命中),双侧一致缺席。轮次产出所提「到期重灌」「降阶洗牌」均指分层树整值重灌/懒降阶机制,与 set 成员 TTL 无关,不构成自造面。

4. 后台周期收集:C# 有。StoreWrapper.cs:722 ObjectCollectTaskAsync → ExecuteObjectCollection(ExecuteHashCollect + ExecuteSortedSetCollect),频率 ExpiredObjectCollectionFrequencySecs;显式命令 HCOLLECT/ZCOLLECT(HashOperation.HCOLLECT=0、SortedSetOperation.ZCOLLECT=27)。rust 有对应(primary_tasks.rs:spawn_object_collect_task + garnet_api/objects.rs:object_collect_all,Hash/ZSet 两族先后,频率槽同配置位)。

二、rust 镜像面闭环核查(全部走通,未发现断链)

1. 命令面:wnode/src/resp/parser/command_table.rs:86-107(hash 8 条)、:242-275(zset 8 条),与 C# 一一对应。分发 wnode/src/resp/garnet_api/raw.rs:260-268、:339-347,(is_milliseconds, is_timestamp) 组合与 C# HashCommands.cs 逐条同构。解析单点 object_store_utils.rs:parse_expire_elements_args(单选项词元 + FIELDS numFields + 计数校验),对位 HashCommands.cs:HashExpire/HashTimeToLive/HashPersist 三段重复解析。键缺失应答:C# NOTFOUND 会话层直写 numFields 个 -2;rust 以空对象执行逐字段回 -2(HashExpireResult::KeyNotFound=-2),线格式等价,且 should_write_back 的 !existed && is_empty 臂保证不落幻键、不发幻 AOF 记录(hash_commands/mod.rs:should_write_back)。
2. 选项打包:ExpirationWithOption 1:1(粗化 >>4 + 低 4 位选项,word head/tail 双 i32 透传):wresp/src/options.rs:127-174 对位 garnet/libs/server/ExpirationWithOption.cs:20-40。ExpireOption 位定义与「字段级只解析单选项、键级才有 XXGT/XXLT 复合」分界在 options.rs:79-100 成文,双侧一致。
3. 过期语义 1:1:条件闸门 NX/XX/GT/LT、expiration <= now 即删成员回 2(KeyAlreadyExpired)、ContainsKey 过滤已过期后判 -2(wcol/src/hash/hash_object.rs:set_expiration/persist/get_expiration 对位 HashObject.cs:562-684)。ZADD 更新分支清成员 TTL(zset/sorted_set_object_impl.rs:sorted_set_add 对位 SortedSetObjectImpl.cs SortedSetAdd 的 TryRemoveExpiration 臂);HSET 覆写清字段 TTL(hash_object_impl.rs:hash_set remove_expiration 臂);HINCRBY/HINCRBYFLOAT 保留存活成员既有 TTL,分层树臂同(old_expiry 透传,tiered_collection_ops/hash.rs:449-533、zset.rs:369-398)。
4. 存储单点:C# 双份同体代码(Hash/SortedSet 各一份)收敛为 wcol/src/types/expiry_ledger.rs(字典 + 最小堆 + 记账单点,Redis 语序的陈旧堆项以字典为真值),堆结构 expiration_queue.rs(BinaryHeap 最小堆对位 PriorityQueue)。符合 SKILL「一处定义」纪律,无双轨(grep 全仓无第二套成员过期堆)。
5. 惰性剔除:全部写面与 HEXPIRE/HTTL/HPERSIST/HCOLLECT/ZCOLLECT 入口先 delete_expired_items(hash_object_impl.rs:249/351/398/440/468/501,zset 同),对位 C# 各 Operate 入口。读面(HGET/HGETALL/HKEYS/HVALS/HSCAN/HRANDFIELD/ZRANGE 族)逐项 is_expired 过滤不物删,C# 同口径。
6. 落盘格式:信封 bitcode HashWire{entries, expirations} + 头部 4B 存活计数 + 8B 最早到期水位,写时过滤已到期(hash_object.rs:serialize_wire,sorted_set_object.rs 同);装载丢弃已到期并置 mutated_by_ttl 闭环(from_blob),对位 C# DoSerialize/构造函数的 ExpirationBitMask 逐条带 TTL + 装载即弃。分层树记录以 wcol/src/types/member_ttl.rs 单点 codec(1B 旗标 + 8B 大端 .NET Ticks),升阶导出防续命、物化还原防复活、树内读写臂共用。
7. 升降阶保真:export_entries 写时过滤 + 存活挂期成员随记录落刻度(wcol/src/types/garnet_object.rs:121-132);升阶/重灌水位 earliest_expiry 单点随灌入批同帧落盘(wkv/src/range_index/promote.rs:64-77,杜绝 i64::MAX 假水位);HEXPIRE 族在分层态穿透物化降级 → wcol 对象层单源求值 → 整值重灌(tiered_collection_ops/hash.rs:621-627、zset.rs:638-644),树内零墓碑;懒降阶 keep_ttl=true 不碰键 TTL,删空 keep_ttl=false 随键清 TTL(rmw_helpers.rs:apply_rmw_post_operate)。
8. AOF/复制镜像:信封域 RMW 增量条目 ObjectStoreRMW 携 op_code+arg1/arg2+args(run_sync_rmw,rmw_helpers.rs:790-806,对位 C# WriteLogRMW),副本经 aof/aof_processor_object_replay.rs:object_store_rmw → operate(sub_id, args, arg1, arg2) 重放,HEXPIRE 的绝对 ticks 随条目过河,确定性等价;整值通道 ObjectStoreUpsert 信封自带 expirations;分层稳态写镜像仅 HSET/ZADD 写义命令(tiered_hash_writes,hash.rs:53-62),到期出账经重灌流 RangeIndexStream + next_expiry 随首块 ReplayInput.arg2 重建副本水位(wkv/src/range_index/migration.rs:25-41、wnode/src/service.rs:351-374)。三通道成员 TTL 保真,无双份入账。
9. 键 TTL 与成员 TTL 叠加:键级 TTL 走独立 KeyTag::Ttl 旁路记录(wkv/src/ttl.rs 两套载体总览),成员 TTL 命令不触键 TTL;信封写回入口前置 rmw_ttl_rebuild_sync 清退过期残留 TTL(storage/session/common/ttl_sync.rs:321-334,对位 RMWMethods.cs CheckExpiry → ExpireAndResume),杜绝「写完即幽灵」;键过期清退不涉成员级(键整体消亡,成员随之)。

三、发现(增量)

1. HEXPIRE 条件拒绝臂幻影项修复:行为偏离 C# 且仅代码内声明,未登记 doc。
机制:HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT 带 XX/GT(及 GT 数值不满足)时,C# HashObject.cs:SetExpiration 以 CollectionsMarshal.GetValueRefOrAddDefault 先插入 0 值过期项再回 ExpireConditionNotMet,被拒字段自此在 expirationTimes 留 0 值幻影 → IsExpired 恒真 → 字段在 HGET/HGETALL/HLEN 各方眼中失活(客户端可见的数据消失)且记账缺项;rust wcol/src/types/expiry_ledger.rs:set_expiration(:175-177)只读探测现值,拒绝臂零副作用,字段保持存活。
判定:刻意修复 C# 缺陷,合理;但该差异是客户端可见行为差,登记仅在代码注释,doc/zh/collection.md 与 SKILL 偏差清单均无。建议在 collection.md 补一行偏差登记(同文件 §6 已有 C# Count 非恒 O(1) 的同 spirit 登记,体例现成)。

2. SortedSet Equals 重复谓词缺陷 1:1 保留,与发现 1 的裁决纪律相反,且注释表述失准。
机制:C# SortedSetObject.cs:335-338 两处 IsExpired(key.Key) 均比较 self 侧(语义上应为 self/other 各一),效果是 self 侧已过期成员被跳过相等比较、other 侧过期状态完全不参与;rust wcol/src/zset/sorted_set_object.rs:equals(:387-407)照搬并自注「1:1 保留 C# 原文的重复谓词形态(两处 IsExpired 调用等价于一次)」——「等价于一次」只对代码字面成立,对 C# 原意(self vs other)不成立,保留的实为残缺语义。
判定:低危(全仓无非测试消费方,C# 侧亦仅 Equals 挂基类),但同一轮次内对 C# 缺陷一个修(发现 1)一个留,无裁决准则成文;且这是成员级过期参与对象相等比较的唯一位置。建议:或修 + ignore 登记 C# 原文缺陷,或在注释与 doc 写明保留理由与影响面(当前注释的「等价于一次」说法会误导后来者)。

3. 周期收集两处刻意差异仅代码声明,未入 doc(轻)。
机制一:零变更门控——C# 周期收集每轮对全库 hash/zset 信封无条件重序列化进 AOF(InPlaceUpdater 恒置 NeedAofLog),rust 以头部存活计数 == 收集后计数短路(garnet_api/objects.rs:collect_hash_key/collect_sorted_set_key 门控段,代码已自注「C# 无对应物,不得称等价」);机制二:失败粒度——C# 单族未知异常记 CRITICAL 后整个收集任务退出不再重启,rust 单族失败 warn 留痕进入下一轮(primary_tasks.rs:spawn_object_collect_task 注释)。
判定:均为合理工程裁决且代码内声明充分;但两处都改变副本流量与故障语义,与发现 1 同属「doc 零登记」面。可与发现 1 合并在 collection.md 开「与 C# 的成员 TTL 刻意差异」小节一次收口。

四、删空自愈(成员级)

信封域:惰性剔除发生 → mutated_by_ttl 升格写回;剔空 → operate 尾 REMOVE_KEY(hash_object.rs:294-296,sorted_set_object.rs:485-488,对位 C# HashObject.cs:299-302/SortedSetObject.cs:452-455)→ apply_rmw_post_operate is_empty 臂 delete_string/drain 整键回收并随键清 TTL(rmw_helpers.rs:330-360)。
分层域:expire_sweep_or_rebuild 存活全集为空 → handle_bftree_drain_and_delete(keep_ttl=false) 键消亡(tiered_collection_ops/common.rs:359-368)。
双侧终态一致:C# 经后台收集与写面到达,rust 额外允许 HLEN/ZCARD 物化矫正臂在读路径触发(信封架构必需的自造闭环,杜绝已剔字段重装载复活,代码与 collection.md §6.3 已登记)。

五、计数一致性(HLEN/ZCARD 口径)

redis 语义(HEXPIRE 后 HLEN 立即减):C# 以 Count() 遍历 expirationTimes 过滤已过期实现「应答立即减、物删延后」(HashObject.cs:612-619、SortedSetObject.cs:610-619,O(T) 只读);rust 以 purge_expired_len 先堆序物删再直读 len,应答值逐值一致(hash_object_impl.rs:462、sorted_set_object_impl.rs:900-908),偏差已登记 collection.md §6。信封稳态 O(1) 快道由头部计数 + 水位门 gate_head_length 承接,越线降级矫正一次(object_store_utils.rs:622-634);分层态 tiered_count 水位内共享读锁直读 size、越线升级出账(tiered_collection_ops/common.rs:tiered_count),每到期纪元至多一扫,off-by-one 已在 `<=` 收口。口径与 O(1) 契约双侧闭环。

无增量确认(已核实面)
- 命令面 8+8 与 C# 一一对应,HGETEX/HGETDEL 双侧缺席,set 成员 TTL 双侧缺席。
- ExpirationWithOption 打包/粗化、ExpireOption 位义与键级/字段级复合选项分界。
- set_expiration 四态应答(-2/0/1/2)与 KeyAlreadyExpired 删成员臂。
- ExpiryLedger 记账口径(SLOT/EXPIRY_STRUCT_BASE)与 C# UpdateExpirationSize 逐臂对位。
- 惰性剔除触发点全集与读面过滤口径;HRANDFIELD/ZRANDMEMBER 过期采样口径。
- 信封 bitcode 线格式、水位写时前移、from_blob 装载即弃 + mutated_by_ttl 闭环。
- 分层 member_ttl codec 单点、升阶导出/物化还原/整值重灌的成员 TTL 保真。
- AOF 三通道(RMW 增量/Upsert 整值/重灌流+next_expiry)成员 TTL 镜像与副本水位重建。
- 键 TTL 与成员 TTL 叠加、rmw_ttl_rebuild_sync 过期残留清退。
- 删空自愈信封/分层双域及 REMOVE_KEY 传播链。
- HLEN/ZCARD/HLEN 矫正臂与 tiered_count 水位门、off-by-one 收口。
- 周期收集任务调度/频率槽/副本挂起/HCOLLECT·ZCOLLECT 单执行体。
- 后台降阶评估轮搭车收集节拍(collection.md 3.3 登记)。

视角结论:有增量
